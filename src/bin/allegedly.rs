use allegedly::{
    Db, Dt, ExportPage, FolderSource, HttpSource, PageBoundaryState, backfill, backfill_to_pg,
    bin_init, pages_to_pg, pages_to_weeks, poll_upstream,
};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tokio::sync::oneshot;
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
        /// After the weekly imports, poll upstream until we're caught up
        #[arg(long, action)]
        catch_up: bool,
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

async fn pages_to_stdout(
    rx: flume::Receiver<ExportPage>,
    notify_last_at: Option<oneshot::Sender<Option<Dt>>>,
) -> Result<(), flume::RecvError> {
    let mut last_at = None;
    while let Ok(page) = rx.recv_async().await {
        for op in &page.ops {
            println!("{op}");
        }
        if notify_last_at.is_some()
            && let Some(s) = PageBoundaryState::new(&page)
        {
            last_at = last_at.filter(|&l| l >= s.last_at).or(Some(s.last_at));
        }
    }
    if let Some(notify) = notify_last_at {
        log::trace!("notifying last_at: {last_at:?}");
        if notify.send(last_at).is_err() {
            log::error!("receiver for last_at dropped, can't notify");
        };
    }
    Ok(())
}

/// page forwarder who drops its channels on receipt of a small page
///
/// PLC will return up to 1000 ops on a page, and returns full pages until it
/// has caught up, so this is a (hacky?) way to stop polling once we're up.
fn full_pages(rx: flume::Receiver<ExportPage>) -> flume::Receiver<ExportPage> {
    let (tx, fwd) = flume::bounded(0);
    tokio::task::spawn(async move {
        while let Ok(page) = rx.recv_async().await
            && page.ops.len() > 900
        {
            tx.send_async(page).await.unwrap();
        }
    });
    fwd
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
            catch_up,
        } => {
            let (tx, rx) = flume::bounded(32); // these are big pages
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

            // postgres writer will notify us as soon as the very last op's time is known
            // so we can start catching up while pg is restoring indexes and stuff
            let (notify_last_at, rx_last) = if catch_up {
                let (tx, rx) = oneshot::channel();
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };

            let to_postgres_url_bulk = to_postgres.clone();
            let bulk_out_write = tokio::task::spawn(async move {
                if let Some(ref url) = to_postgres_url_bulk {
                    let db = Db::new(url.as_str()).await.unwrap();
                    backfill_to_pg(db, postgres_reset, rx, notify_last_at)
                        .await
                        .unwrap();
                } else {
                    pages_to_stdout(rx, notify_last_at).await.unwrap();
                }
            });

            if let Some(rx_last) = rx_last {
                let mut upstream = args.upstream;
                upstream.set_path("/export");
                // wait until the time for `after` is known
                let last_at = rx_last.await.unwrap();
                log::info!("beginning catch-up from {last_at:?} while the writer finalizes stuff");
                let (tx, rx) = flume::bounded(256);
                tokio::task::spawn(
                    async move { poll_upstream(last_at, upstream, tx).await.unwrap() },
                );
                bulk_out_write.await.unwrap();
                log::info!("writing catch-up pages");
                let full_pages = full_pages(rx);
                if let Some(url) = to_postgres {
                    let db = Db::new(url.as_str()).await.unwrap();
                    pages_to_pg(db, full_pages).await.unwrap();
                } else {
                    pages_to_stdout(full_pages, None).await.unwrap();
                }
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
            pages_to_stdout(rx, None).await.unwrap();
        }
    }
}
