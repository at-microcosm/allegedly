use crate::ExportPage;
use url::Url;

use async_compression::futures::bufread::GzipDecoder;
use futures::{AsyncBufReadExt, StreamExt, TryStreamExt, io};

pub async fn week_to_pages(
    client: &reqwest::Client,
    url: Url,
    dest: flume::Sender<ExportPage>,
) -> anyhow::Result<()> {
    let reader = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .bytes_stream()
        .map_err(io::Error::other)
        .into_async_read();

    let decoder = GzipDecoder::new(io::BufReader::new(reader));

    let mut chunks = io::BufReader::new(decoder).lines().chunks(1000);

    while let Some(chunk) = chunks.next().await {
        let ops = chunk.into_iter().collect::<Result<Vec<_>, io::Error>>()?;
        let page = ExportPage { ops };
        dest.send_async(page).await?;
    }
    Ok(())
}
