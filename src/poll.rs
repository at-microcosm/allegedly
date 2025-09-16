use crate::{CLIENT, Dt, ExportPage, Op};
use std::time::Duration;
use thiserror::Error;
use url::Url;

const UPSTREAM_REQUEST_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Error)]
pub enum GetPageError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    ReqwestMiddleware(#[from] reqwest_middleware::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

/// ops are primary-keyed by (did, cid)
/// plc orders by `created_at` but does not guarantee distinct times per op
/// we assume that the order will at least be deterministic: this may be unsound
#[derive(Debug, PartialEq)]
pub struct LastOp {
    pub created_at: Dt,   // any op greater is definitely not duplicated
    pk: (String, String), // did, cid
}

impl From<Op<'_>> for LastOp {
    fn from(op: Op) -> Self {
        Self {
            created_at: op.created_at,
            pk: (op.did.to_string(), op.cid.to_string()),
        }
    }
}

impl From<Dt> for LastOp {
    fn from(dt: Dt) -> Self {
        Self {
            created_at: dt,
            pk: ("".to_string(), "".to_string()),
        }
    }
}

impl ExportPage {
    /// this is a (slightly flawed) op deduplicator
    fn only_after_last(&mut self, last_op: &LastOp) {
        loop {
            let Some(s) = self.ops.first().cloned() else {
                break;
            };
            let Ok(op) = serde_json::from_str::<Op>(&s) else {
                log::warn!(
                    "deduplication failed op parsing ({s:?}), bailing for downstream to deal with."
                );
                break;
            };
            if op.created_at > last_op.created_at {
                break;
            }
            log::trace!("dedup: dropping an op");
            self.ops.remove(0);
            if Into::<LastOp>::into(op) == *last_op {
                log::trace!("dedup: found exact op, keeping all after here");
                break;
            }
        }
    }
}

pub async fn get_page(url: Url) -> Result<(ExportPage, Option<LastOp>), GetPageError> {
    log::trace!("Getting page: {url}");

    let ops: Vec<String> = CLIENT
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?
        .trim()
        .split('\n')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() { None } else { Some(s) }
        })
        .map(Into::into)
        .collect();

    let last_op = ops
        .last()
        .filter(|s| !s.is_empty())
        .map(|s| serde_json::from_str::<Op>(s))
        .transpose()?
        .map(Into::into)
        .inspect(|at| log::trace!("new last op: {at:?}"));

    Ok((ExportPage { ops }, last_op))
}

pub async fn poll_upstream(
    after: Option<Dt>,
    base: Url,
    dest: flume::Sender<ExportPage>,
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(UPSTREAM_REQUEST_INTERVAL);
    let mut prev_last: Option<LastOp> = after.map(Into::into);
    loop {
        tick.tick().await;

        let mut url = base.clone();
        if let Some(ref pl) = prev_last {
            url.query_pairs_mut()
                .append_pair("after", &pl.created_at.to_rfc3339());
        };

        let (mut page, next_last) = get_page(url).await?;
        if let Some(ref pl) = prev_last {
            page.only_after_last(pl);
        }
        if !page.is_empty() {
            match dest.try_send(page) {
                Ok(()) => {}
                Err(flume::TrySendError::Full(page)) => {
                    log::warn!("export: destination channel full, awaiting...");
                    dest.send_async(page).await?;
                }
                e => e?,
            };
        }

        prev_last = next_last.or(prev_last);
    }
}
