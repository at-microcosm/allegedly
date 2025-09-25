use crate::{CLIENT, Dt, ExportPage, Op};
use async_compression::tokio::bufread::GzipDecoder;
use async_compression::tokio::write::GzipEncoder;
use core::pin::pin;
use reqwest::Url;
use std::future::Future;
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
    sync::mpsc,
};
use tokio_stream::wrappers::LinesStream;
use tokio_util::compat::FuturesAsyncReadCompatExt;

const WEEK_IN_SECONDS: i64 = 7 * 86_400;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Week(i64);

impl Week {
    pub const fn from_n(n: i64) -> Self {
        Self(n)
    }
    pub fn range(r: impl RangeBounds<Week>) -> Vec<Self> {
        let first = match r.start_bound() {
            Bound::Included(week) => *week,
            Bound::Excluded(week) => week.next(),
            Bound::Unbounded => panic!("week range must have a defined start bound"),
        };
        let last = match r.end_bound() {
            Bound::Included(week) => *week,
            Bound::Excluded(week) => week.prev(),
            Bound::Unbounded => Self(Self::nullification_cutoff()).prev(),
        };
        let mut out = Vec::new();
        let mut current = first;
        while current <= last {
            out.push(current);
            current = current.next();
        }
        out
    }
    pub fn n_ago(&self) -> i64 {
        let now = chrono::Utc::now().timestamp();
        (now - self.0) / WEEK_IN_SECONDS
    }
    pub fn n_until(&self, other: Week) -> i64 {
        let Self(until) = other;
        (until - self.0) / WEEK_IN_SECONDS
    }
    pub fn next(&self) -> Week {
        Self(self.0 + WEEK_IN_SECONDS)
    }
    pub fn prev(&self) -> Week {
        Self(self.0 - WEEK_IN_SECONDS)
    }
    /// whether the plc log for this week outside the 72h nullification window
    ///
    /// plus one hour for safety (week must have ended > 73 hours ago)
    pub fn is_immutable(&self) -> bool {
        self.next().0 <= Self::nullification_cutoff()
    }
    fn nullification_cutoff() -> i64 {
        const HOUR_IN_SECONDS: i64 = 3600;
        let now = chrono::Utc::now().timestamp();
        now - (73 * HOUR_IN_SECONDS)
    }
}

impl From<Dt> for Week {
    fn from(dt: Dt) -> Self {
        let ts = dt.timestamp();
        let truncated = (ts / WEEK_IN_SECONDS) * WEEK_IN_SECONDS;
        Week(truncated)
    }
}

impl From<Week> for Dt {
    fn from(week: Week) -> Dt {
        let Week(ts) = week;
        Dt::from_timestamp(ts, 0).expect("the week to be in valid range")
    }
}

pub trait BundleSource: Clone {
    fn reader_for(
        &self,
        week: Week,
    ) -> impl Future<Output = anyhow::Result<impl AsyncRead + Send>> + Send;
}

#[derive(Debug, Clone)]
pub struct FolderSource(pub PathBuf);
impl BundleSource for FolderSource {
    async fn reader_for(&self, week: Week) -> anyhow::Result<impl AsyncRead> {
        let FolderSource(dir) = self;
        let path = dir.join(format!("{}.jsonl.gz", week.0));
        log::debug!("opening folder source: {path:?}");
        let file = File::open(path)
            .await
            .inspect_err(|e| log::error!("failed to open file: {e}"))?;
        Ok(file)
    }
}

#[derive(Debug, Clone)]
pub struct HttpSource(pub Url);
impl BundleSource for HttpSource {
    async fn reader_for(&self, week: Week) -> anyhow::Result<impl AsyncRead> {
        use futures::TryStreamExt;
        let HttpSource(base) = self;
        let url = base.join(&format!("{}.jsonl.gz", week.0))?;
        Ok(CLIENT
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .bytes_stream()
            .map_err(futures::io::Error::other)
            .into_async_read()
            .compat())
    }
}

pub async fn pages_to_weeks(
    mut rx: mpsc::Receiver<ExportPage>,
    dir: PathBuf,
    clobber: bool,
) -> anyhow::Result<()> {
    pub use std::time::Instant;

    // ...there is certainly a nicer way to write this
    let mut current_week: Option<Week> = None;
    let dummy_file = File::create(dir.join("_dummy")).await?;
    let mut encoder = GzipEncoder::new(dummy_file);

    let mut total_ops = 0;
    let total_t0 = Instant::now();
    let mut week_ops = 0;
    let mut week_t0 = total_t0;

    while let Some(page) = rx.recv().await {
        for mut s in page.ops {
            let Ok(op) = serde_json::from_str::<Op>(&s)
                .inspect_err(|e| log::error!("failed to parse plc op, ignoring: {e}"))
            else {
                continue;
            };
            let op_week = op.created_at.into();
            if current_week.map(|w| w != op_week).unwrap_or(true) {
                encoder.shutdown().await?;
                let now = Instant::now();

                log::info!(
                    "done week {:3 } ({:10 }): {week_ops:7 } ({:5.0 }/s) ops, {:5 }k total ({:5.0 }/s)",
                    current_week.map(|w| -w.n_ago()).unwrap_or(0),
                    current_week.unwrap_or(Week(0)).0,
                    (week_ops as f64) / (now - week_t0).as_secs_f64(),
                    total_ops / 1000,
                    (total_ops as f64) / (now - total_t0).as_secs_f64(),
                );
                let path = dir.join(format!("{}.jsonl.gz", op_week.0));
                let file = if clobber {
                    File::create(path).await?
                } else {
                    File::create_new(path).await?
                };
                encoder = GzipEncoder::with_quality(file, async_compression::Level::Best);
                current_week = Some(op_week);
                week_ops = 0;
                week_t0 = now;
            }
            s.push('\n'); // hack
            log::trace!("writing: {s}");
            encoder.write_all(s.as_bytes()).await?;
            total_ops += 1;
            week_ops += 1;
        }
    }

    // don't forget the final file
    encoder.shutdown().await?;
    let now = Instant::now();
    log::info!(
        "done week {:3 } ({:10 }): {week_ops:7 } ({:5.0 }/s) ops, {:5 }k total ({:5.0 }/s)",
        current_week.map(|w| -w.n_ago()).unwrap_or(0),
        current_week.unwrap_or(Week(0)).0,
        (week_ops as f64) / (now - week_t0).as_secs_f64(),
        total_ops / 1000,
        (total_ops as f64) / (now - total_t0).as_secs_f64(),
    );

    Ok(())
}

pub async fn week_to_pages(
    source: impl BundleSource,
    week: Week,
    dest: mpsc::Sender<ExportPage>,
) -> anyhow::Result<()> {
    use futures::TryStreamExt;
    let reader = source.reader_for(week)
        .await
        .inspect_err(|e| log::error!("week_to_pages reader failed: {e}"))?;
    let decoder = GzipDecoder::new(BufReader::new(reader));
    let mut chunks = pin!(LinesStream::new(BufReader::new(decoder).lines()).try_chunks(10000));

    while let Some(chunk) = chunks
        .try_next()
        .await
        .inspect_err(|e| log::error!("failed to get next chunk: {e}"))?
    {
        let ops: Vec<String> = chunk.into_iter().collect();
        let page = ExportPage { ops };
        dest
            .send(page)
            .await
            .inspect_err(|e| log::error!("failed to send page: {e}"))?;
    }
    Ok(())
}
