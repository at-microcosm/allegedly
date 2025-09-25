use allegedly::{
    Db, Dt, FolderSource, HttpSource, backfill, backfill_to_pg, bin::GlobalArgs, bin_init,
    full_pages, pages_to_pg, pages_to_stdout, poll_upstream,
};
use clap::Parser;
use reqwest::Url;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, clap::Args)]
pub struct Args {
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
}

pub async fn run(
    GlobalArgs { upstream }: GlobalArgs,
    Args {
        http,
        dir,
        source_workers,
        to_postgres,
        postgres_cert,
        postgres_reset,
        until,
        catch_up,
    }: Args,
) -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel(32); // these are big pages
    tokio::task::spawn(async move {
        if let Some(dir) = dir {
            log::info!("Reading weekly bundles from local folder {dir:?}");
            backfill(FolderSource(dir), tx, source_workers.unwrap_or(1), until)
                .await
                .inspect_err(|e| log::error!("backfill from folder problem: {e}"))
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
        let mut upstream = upstream;
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
    Ok(())
}

#[derive(Debug, Parser)]
struct CliArgs {
    #[command(flatten)]
    globals: GlobalArgs,
    #[command(flatten)]
    args: Args,
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = CliArgs::parse();
    bin_init("backfill");
    run(args.globals, args.args).await?;
    Ok(())
}
