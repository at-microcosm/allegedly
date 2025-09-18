use allegedly::{HttpSource, Week, week_to_pages};
use std::io::Write;

#[tokio::main]
async fn main() {
    let url: url::Url = "https://plc.t3.storage.dev/plc.directory/".parse().unwrap();
    let source = HttpSource(url);
    // let source = FolderSource("./weekly/".into());
    let week = Week::from_n(1699488000);

    let (tx, rx) = flume::bounded(32);

    tokio::task::spawn(async move {
        week_to_pages(source, week, tx).await.unwrap();
    });

    let mut n = 0;

    print!("receiving");
    while let Ok(page) = rx.recv_async().await {
        print!(".");
        std::io::stdout().flush().unwrap();
        n += page.ops.len();
    }
    println!();

    println!("bye ({n})");

    // let reader = CLIENT
    //     .get("https://plc.t3.storage.dev/plc.directory/1699488000.jsonl.gz")
    //     // .get("https://plc.t3.storage.dev/plc.directory/1669248000.jsonl.gz")
    //     .send()
    //     .await
    //     .unwrap()
    //     .error_for_status()
    //     .unwrap()
    //     .bytes_stream()
    //     .map_err(io::Error::other)
    //     .into_async_read();

    // let decoder = GzipDecoder::new(io::BufReader::new(reader));
    // let mut chunks = io::BufReader::new(decoder).lines().chunks(1000);
    // while let Some(ref _chunk) = chunks.next().await {
    //     print!(".");
    // }
    // println!();
}
