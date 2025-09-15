use async_compression::futures::bufread::GzipDecoder;
use futures::{AsyncBufReadExt, StreamExt, TryStreamExt, io};

#[tokio::main]
async fn main() {
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            "allegedly (blah) v",
            env!("CARGO_PKG_VERSION"),
            " (from @microcosm.blue; contact @bad-example.com)"
        ))
        .build()
        .unwrap();

    let reader = client
        .get("https://plc.t3.storage.dev/plc.directory/1699488000.jsonl.gz")
        // .get("https://plc.t3.storage.dev/plc.directory/1669248000.jsonl.gz")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .bytes_stream()
        .map_err(io::Error::other)
        .into_async_read();

    let decoder = GzipDecoder::new(io::BufReader::new(reader));
    let mut chunks = io::BufReader::new(decoder).lines().chunks(1000);
    while let Some(ref _chunk) = chunks.next().await {
        print!(".");
    }
    println!();
}
