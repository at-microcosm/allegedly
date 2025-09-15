use allegedly::{bin_init, poll_upstream};
use clap::Parser;
use url::Url;

#[derive(Parser)]
struct Args {
    /// Upstream PLC server to poll
    ///
    /// default: https://plc.directory
    #[arg(long, env)]
    #[clap(default_value = "https://plc.directory")]
    upstream: Url,
}

#[tokio::main]
async fn main() {
    bin_init("tail");

    let mut url = Args::parse().upstream;
    url.set_path("/export");
    let now = chrono::Utc::now();

    let (tx, rx) = flume::bounded(0); // rendezvous
    tokio::task::spawn(async move {
        if let Err(e) = poll_upstream(Some(now), url, tx).await {
            log::error!("polling failed: {e}");
        } else {
            log::warn!("poller finished ok (weird?)");
        }
    });

    while let Ok(page) = rx.recv_async().await {
        for op in page.ops {
            println!("{op}");
        }
    }
    log::warn!("recv failed, bye");
}
