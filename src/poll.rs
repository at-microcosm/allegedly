use crate::{ExportPage, OpPeek};
use chrono::{DateTime, Utc};
use std::time::Duration;
use thiserror::Error;
use url::Url;

const UPSTREAM_REQUEST_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Error)]
pub enum GetPageError {
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),
}

pub async fn get_page(
    client: &reqwest::Client,
    url: Url,
) -> Result<(ExportPage, Option<DateTime<Utc>>), GetPageError> {
    let ops: Vec<String> = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?
        .trim()
        .split('\n')
        .map(Into::into)
        .collect();

    let last_at = ops
        .last()
        .map(|s| serde_json::from_str::<OpPeek>(s))
        .transpose()?
        .map(|o| o.created_at);

    Ok((ExportPage { ops }, last_at))
}

pub async fn poll_upstream(
    client: &reqwest::Client,
    after: Option<DateTime<Utc>>,
    base: Url,
    dest: flume::Sender<ExportPage>,
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(UPSTREAM_REQUEST_INTERVAL);
    let mut after = after;
    loop {
        tick.tick().await;
        let mut url = base.clone();
        if let Some(a) = after {
            url.query_pairs_mut().append_pair("after", &a.to_rfc3339());
        };
        let (page, next_after) = get_page(client, url).await?;
        dest.send_async(page).await?;
        after = next_after;
    }
}
