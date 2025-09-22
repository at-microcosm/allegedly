use allegedly::{
    Db, Dt, ExportPage, FolderSource, HttpSource, backfill, bin_init, pages_to_weeks,
    poll_upstream, write_bulk as pages_to_pg,
};
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
    /// Use weekly bundled ops to get a complete directory mirror FAST
    Backfill {
        /// Remote URL prefix to fetch bundles from
        #[arg(long)]
        #[clap(default_value = "https://plc.t3.storage.dev/plc.directory/")]
        http: Url,
        /// Local folder to fetch bundles from (overrides `http`)
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Parallel bundle fetchers
        ///
        /// Default: 4 for http fetches, 1 for local folder
        #[arg(long)]
        source_workers: Option<usize>,
        /// Bulk load into did-method-plc-compatible postgres instead of stdout
        ///
        /// Pass a postgres connection url like "postgresql://localhost:5432"
        #[arg(long)]
        to_postgres: Option<Url>,
        /// Delete all operations from the postgres db before starting
        ///
        /// only used if `--to-postgres` is present
        #[arg(long, action)]
        postgres_reset: bool,
        /// Stop at the week ending before this date
        #[arg(long)]
        until: Option<Dt>,
    },
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

async fn pages_to_stdout(rx: flume::Receiver<ExportPage>) -> Result<(), flume::RecvError> {
    while let Ok(page) = rx.recv_async().await {
        for op in page.ops {
            println!("{op}")
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    bin_init("main");

    let args = Cli::parse();

    match args.command {
        Commands::Backfill {
            http,
            dir,
            source_workers,
            to_postgres,
            postgres_reset,
            until,
        } => {
            let (tx, rx) = flume::bounded(32); // big pages
            tokio::task::spawn(async move {
                if let Some(dir) = dir {
                    log::info!("Reading weekly bundles from local folder {dir:?}");
                    backfill(FolderSource(dir), tx, source_workers.unwrap_or(1), until)
                        .await
                        .unwrap();
                } else {
                    log::info!("Fetching weekly bundles from from {http}");
                    backfill(HttpSource(http), tx, source_workers.unwrap_or(4), until)
                        .await
                        .unwrap();
                }
            });
            if let Some(url) = to_postgres {
                let db = Db::new(url.as_str()).await.unwrap();
                pages_to_pg(db, rx, postgres_reset).await.unwrap();
            } else {
                pages_to_stdout(rx).await.unwrap();
            }
        }
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
            let (tx, rx) = flume::bounded(1);
            tokio::task::spawn(async move { poll_upstream(start_at, url, tx).await.unwrap() });
            pages_to_stdout(rx).await.unwrap();
        }
    }
}
