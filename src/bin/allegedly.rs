use allegedly::{
    Db, Dt, ExportPage, FolderSource, HttpSource, ListenConf, PageBoundaryState, backfill,
    backfill_to_pg, bin_init, pages_to_pg, pages_to_weeks, poll_upstream, serve,
};
use clap::{CommandFactory, Parser, Subcommand};
use reqwest::Url;
use std::{net::SocketAddr, path::PathBuf, time::Instant};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Parser)]
struct Cli {
    /// Upstream PLC server
    #[arg(short, long, global = true, env = "ALLEGEDLY_UPSTREAM")]
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
        #[arg(long, env = "ALLEGEDLY_TO_POSTGRES")]
        to_postgres: Option<Url>,
        /// Cert for postgres (if needed)
        #[arg(long)]
        postgres_cert: Option<PathBuf>,
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
    /// Wrap a did-method-plc server, syncing upstream and blocking op submits
    Mirror {
        /// the wrapped did-method-plc server
        #[arg(long, env = "ALLEGEDLY_WRAP")]
        wrap: Url,
        /// the wrapped did-method-plc server's database (write access required)
        #[arg(long, env = "ALLEGEDLY_WRAP_PG")]
        wrap_pg: Url,
        /// path to tls cert for the wrapped postgres db, if needed
        #[arg(long, env = "ALLEGEDLY_WRAP_PG_CERT")]
        wrap_pg_cert: Option<PathBuf>,
        /// wrapping server listen address
        #[arg(short, long, env = "ALLEGEDLY_BIND")]
        #[clap(default_value = "127.0.0.1:8000")]
        bind: SocketAddr,
        /// obtain a certificate from letsencrypt
        ///
        /// for now this will force listening on all interfaces at :80 and :443
        /// (:80 will serve an "https required" error, *will not* redirect)
        #[arg(
            long,
            conflicts_with("bind"),
            requires("acme_cache_path"),
            env = "ALLEGEDLY_ACME_DOMAIN"
        )]
        acme_domain: Vec<String>,
        /// which local directory to keep the letsencrypt certs in
        #[arg(long, requires("acme_domain"), env = "ALLEGEDLY_ACME_CACHE_PATH")]
        acme_cache_path: Option<PathBuf>,
        /// which public acme directory to use
        ///
        /// eg. letsencrypt staging: "https://acme-staging-v02.api.letsencrypt.org/directory"
        #[arg(long, requires("acme_domain"), env = "ALLEGEDLY_ACME_DIRECTORY_URL")]
        #[clap(default_value = "https://acme-v02.api.letsencrypt.org/directory")]
        acme_directory_url: Url,
    },
    /// Poll an upstream PLC server and log new ops to stdout
    Tail {
        /// Begin tailing from a specific timestamp for replay or wait-until
        #[arg(short, long)]
        after: Option<Dt>,
    },
}

async fn pages_to_stdout(
    mut rx: mpsc::Receiver<ExportPage>,
    notify_last_at: Option<oneshot::Sender<Option<Dt>>>,
) -> anyhow::Result<()> {
    let mut last_at = None;
    while let Some(page) = rx.recv().await {
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
fn full_pages(mut rx: mpsc::Receiver<ExportPage>) -> mpsc::Receiver<ExportPage> {
    let (tx, fwd) = mpsc::channel(1);
    tokio::task::spawn(async move {
        while let Some(page) = rx.recv().await
            && page.ops.len() > 900
        {
            tx.send(page).await.expect("to be able to forward a page");
        }
    });
    fwd
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let matches = Cli::command().get_matches();
    let name = matches.subcommand().map(|(name, _)| name).unwrap_or("???");
    bin_init(name);

    let t0 = Instant::now();
    match args.command {
        Commands::Backfill {
            http,
            dir,
            source_workers,
            to_postgres,
            postgres_cert,
            postgres_reset,
            until,
            catch_up,
        } => {
            let (tx, rx) = mpsc::channel(32); // these are big pages
            tokio::task::spawn(async move {
                if let Some(dir) = dir {
                    log::info!("Reading weekly bundles from local folder {dir:?}");
                    backfill(FolderSource(dir), tx, source_workers.unwrap_or(1), until)
                        .await
                        .expect("to source bundles from a folder");
                } else {
                    log::info!("Fetching weekly bundles from from {http}");
                    backfill(HttpSource(http), tx, source_workers.unwrap_or(4), until)
                        .await
                        .expect("to source bundles from http");
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
            let pg_cert = postgres_cert.clone();
            let bulk_out_write = tokio::task::spawn(async move {
                if let Some(ref url) = to_postgres_url_bulk {
                    let db = Db::new(url.as_str(), pg_cert)
                        .await
                        .expect("to get db for bulk out write");
                    backfill_to_pg(db, postgres_reset, rx, notify_last_at)
                        .await
                        .expect("to backfill to pg");
                } else {
                    pages_to_stdout(rx, notify_last_at)
                        .await
                        .expect("to backfill to stdout");
                }
            });

            if let Some(rx_last) = rx_last {
                let mut upstream = args.upstream;
                upstream.set_path("/export");
                // wait until the time for `after` is known
                let last_at = rx_last.await.expect("to get the last log's createdAt");
                log::info!("beginning catch-up from {last_at:?} while the writer finalizes stuff");
                let (tx, rx) = mpsc::channel(256); // these are small pages
                tokio::task::spawn(async move {
                    poll_upstream(last_at, upstream, tx)
                        .await
                        .expect("polling upstream to work")
                });
                bulk_out_write.await.expect("to wait for bulk_out_write");
                log::info!("writing catch-up pages");
                let full_pages = full_pages(rx);
                if let Some(url) = to_postgres {
                    let db = Db::new(url.as_str(), postgres_cert)
                        .await
                        .expect("to connect pg for catchup");
                    pages_to_pg(db, full_pages)
                        .await
                        .expect("to write catch-up pages to pg");
                } else {
                    pages_to_stdout(full_pages, None)
                        .await
                        .expect("to write catch-up pages to stdout");
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
            let (tx, rx) = mpsc::channel(32); // read ahead if gzip stalls for some reason
            tokio::task::spawn(async move {
                poll_upstream(Some(after), url, tx)
                    .await
                    .expect("to poll upstream")
            });
            log::trace!("ensuring output directory exists");
            std::fs::create_dir_all(&dest).expect("to ensure output dir exists");
            pages_to_weeks(rx, dest, clobber)
                .await
                .expect("to write bundles to output files");
        }
        Commands::Mirror {
            wrap,
            wrap_pg,
            wrap_pg_cert,
            bind,
            acme_domain,
            acme_cache_path,
            acme_directory_url,
        } => {
            let db = Db::new(wrap_pg.as_str(), wrap_pg_cert)
                .await
                .expect("to connect to pg for mirroring");
            let latest = db
                .get_latest()
                .await
                .expect("to query for last createdAt")
                .expect("there to be at least one op in the db. did you backfill?");

            let (tx, rx) = mpsc::channel(2);
            // upstream poller
            let mut url = args.upstream.clone();
            tokio::task::spawn(async move {
                log::info!("starting poll reader...");
                url.set_path("/export");
                tokio::task::spawn(async move {
                    poll_upstream(Some(latest), url, tx)
                        .await
                        .expect("to poll upstream for mirror sync")
                });
            });
            // db writer
            let poll_db = db.clone();
            tokio::task::spawn(async move {
                log::info!("starting db writer...");
                pages_to_pg(poll_db, rx)
                    .await
                    .expect("to write to pg for mirror");
            });

            let listen_conf = match (bind, acme_domain.is_empty(), acme_cache_path) {
                (_, false, Some(cache_path)) => ListenConf::Acme {
                    domains: acme_domain,
                    cache_path,
                    directory_url: acme_directory_url.to_string(),
                },
                (bind, true, None) => ListenConf::Bind(bind),
                (_, _, _) => unreachable!(),
            };

            serve(&args.upstream, wrap, listen_conf)
                .await
                .expect("to be able to serve the mirror proxy app");
        }
        Commands::Tail { after } => {
            let mut url = args.upstream;
            url.set_path("/export");
            let start_at = after.or_else(|| Some(chrono::Utc::now()));
            let (tx, rx) = mpsc::channel(1);
            tokio::task::spawn(async move {
                poll_upstream(start_at, url, tx)
                    .await
                    .expect("to poll upstream")
            });
            pages_to_stdout(rx, None)
                .await
                .expect("to write pages to stdout");
        }
    }
    log::info!("whew, {:?}. goodbye!", t0.elapsed());
}
