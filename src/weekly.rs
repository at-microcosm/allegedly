use crate::{Dt, ExportPage, Op};
use async_compression::tokio::write::GzipEncoder;
use std::path::PathBuf;
use tokio::{fs::File, io::AsyncWriteExt};

const WEEK_IN_SECONDS: i64 = 7 * 86400;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Week(i64);

impl Week {
    pub fn from_n(n: i64) -> Self {
        Self(n)
    }
    pub fn n_ago(&self) -> i64 {
        let Self(us) = self;
        let Self(cur) = chrono::Utc::now().into();
        (cur - us) / WEEK_IN_SECONDS
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

pub async fn pages_to_weeks(
    rx: flume::Receiver<ExportPage>,
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

    while let Ok(page) = rx.recv_async().await {
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
