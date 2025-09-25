use allegedly::{Db, ListenConf, bin::GlobalArgs, bin_init, pages_to_pg, poll_upstream, serve};
use clap::Parser;
use reqwest::Url;
use std::{net::SocketAddr, path::PathBuf};
use tokio::sync::mpsc;

#[derive(Debug, clap::Args)]
pub struct Args {
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
}

pub async fn run(
    GlobalArgs { upstream }: GlobalArgs,
    Args {
        wrap,
        wrap_pg,
        wrap_pg_cert,
        bind,
        acme_domain,
        acme_cache_path,
        acme_directory_url,
    }: Args,
) -> anyhow::Result<()> {
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
    let mut url = upstream.clone();
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

    serve(&upstream, wrap, listen_conf)
        .await
        .expect("to be able to serve the mirror proxy app");
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
    bin_init("mirror");
    run(args.globals, args.args).await?;
    Ok(())
}
