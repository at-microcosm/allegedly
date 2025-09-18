use allegedly::{Dt, bin_init, pages_to_weeks, poll_upstream};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use url::Url;

#[derive(Debug, Parser)]
struct Cli {
    /// Upstream PLC server
    #[arg(short, long, env)]
    #[clap(default_value = "https://plc.directory")]
    upstream: Url,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Scrape a PLC server, collecting ops into weekly bundles
    ///
    /// Bundles are gzipped files named `<WEEK>.jsonl.gz` where WEEK is a unix
    /// timestamp rounded down to a multiple of 604,800 (one week in seconds).
    ///
    /// Will stop by default at floor((now - 73hrs) / one week) * one week. PLC
    /// operations can be invalidated within 72 hrs, so stopping before that
    /// time ensures that the bundles are (hopefully) immutable.
    Bundle {
        /// Where to save the bundled files
        #[arg(short, long)]
        #[clap(default_value = "./weekly/")]
        dest: PathBuf,
        /// Start the export from this time. Should be a week boundary.
        #[arg(short, long)]
        #[clap(default_value = "2022-11-17T00:00:00Z")]
        after: Dt,
        /// Overwrite existing files, if present
        #[arg(long, action)]
        clobber: bool,
    },
    /// Poll an upstream PLC server and log new ops to stdout
    Tail {
        /// Begin tailing from a specific timestamp for replay or wait-until
        #[arg(short, long)]
        after: Option<Dt>,
    },
}

#[tokio::main]
async fn main() {
    bin_init("main");

    let args = Cli::parse();

    match args.command {
        Commands::Bundle {
            dest,
            after,
            clobber,
        } => {
            let mut url = args.upstream;
            url.set_path("/export");
            let (tx, rx) = flume::bounded(32); // read ahead if gzip stalls for some reason
            tokio::task::spawn(async move { poll_upstream(Some(after), url, tx).await.unwrap() });
            log::trace!("ensuring output directory exists");
            std::fs::create_dir_all(&dest).unwrap();
            pages_to_weeks(rx, dest, clobber).await.unwrap();
        }
        Commands::Tail { after } => {
            let mut url = args.upstream;
            url.set_path("/export");
            let start_at = after.or_else(|| Some(chrono::Utc::now()));
            let (tx, rx) = flume::bounded(0); // rendezvous, don't read ahead
            tokio::task::spawn(async move { poll_upstream(start_at, url, tx).await.unwrap() });
            loop {
                for op in rx.recv_async().await.unwrap().ops {
                    println!("{op}")
                }
            }
        }
    }
}
