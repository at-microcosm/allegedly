use allegedly::{bin_init, pages_to_weeks, poll_upstream};
use clap::Parser;
use std::path::PathBuf;
use url::Url;

const PAGE_QUEUE_SIZE: usize = 128;

#[derive(Parser)]
struct Args {
    /// Upstream PLC server to poll
    ///
    /// default: https://plc.directory
    #[arg(long, env)]
    #[clap(default_value = "https://plc.directory")]
    upstream: Url,
    /// Directory to save gzipped weekly bundles
    ///
    /// default: ./weekly/
    #[arg(long, env)]
    #[clap(default_value = "./weekly/")]
    dir: PathBuf,
    /// The week to start from
    ///
    /// Must be a week-truncated unix timestamp
    #[arg(long, env)]
    start_at: Option<u64>, // TODO!!
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bin_init("weekly");
    let args = Args::parse();

    let mut url = args.upstream;
    url.set_path("/export");

    log::trace!("ensure weekly output directory exists");
    std::fs::create_dir_all(&args.dir)?;

    let (tx, rx) = flume::bounded(PAGE_QUEUE_SIZE);

    tokio::task::spawn(async move {
        if let Err(e) = poll_upstream(None /*todo*/, url, tx).await {
            log::error!("polling failed: {e}");
        } else {
            log::warn!("poller finished ok (weird?)");
        }
    });

    pages_to_weeks(rx, args.dir).await?;

    Ok(())
}
