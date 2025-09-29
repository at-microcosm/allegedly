use allegedly::{
    Db, ExperimentalConf, ListenConf, bin::GlobalArgs, bin_init, pages_to_pg, poll_upstream, serve,
};
use clap::Parser;
use reqwest::Url;
use std::{net::SocketAddr, path::PathBuf};
use tokio::{fs::create_dir_all, sync::mpsc, task::JoinSet};

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
    /// try to listen for ipv6
    #[arg(long, action, requires("acme_domain"), env = "ALLEGEDLY_ACME_IPV6")]
    acme_ipv6: bool,
    /// only accept experimental requests at this hostname
    ///
    /// a cert will be provisioned for it from letsencrypt. if you're not using
    /// acme (eg., behind a tls-terminating reverse proxy), open a feature request.
    #[arg(
        long,
        requires("acme_domain"),
        env = "ALLEGEDLY_EXPERIMENTAL_ACME_DOMAIN"
    )]
    experimental_acme_domain: Option<String>,
    /// accept writes! by forwarding them upstream
    #[arg(long, action, env = "ALLEGEDLY_EXPERIMENTAL_WRITE_UPSTREAM")]
    experimental_write_upstream: bool,
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
        acme_ipv6,
        experimental_acme_domain,
        experimental_write_upstream,
    }: Args,
) -> anyhow::Result<()> {
    let db = Db::new(wrap_pg.as_str(), wrap_pg_cert).await?;

    // TODO: allow starting up with polling backfill from beginning?
    log::debug!("getting the latest op from the db...");
    let latest = db
        .get_latest()
        .await?
        .expect("there to be at least one op in the db. did you backfill?");

    let listen_conf = match (bind, acme_domain.is_empty(), acme_cache_path) {
        (_, false, Some(cache_path)) => {
            log::info!("configuring acme for https at {acme_domain:?}...");
            create_dir_all(&cache_path).await?;
            ListenConf::Acme {
                domains: acme_domain,
                cache_path,
                directory_url: acme_directory_url.to_string(),
                ipv6: acme_ipv6,
            }
        }
        (bind, true, None) => ListenConf::Bind(bind),
        (_, _, _) => unreachable!(),
    };

    let experimental_conf = ExperimentalConf {
        acme_domain: experimental_acme_domain,
        write_upstream: experimental_write_upstream,
    };

    let mut tasks = JoinSet::new();

    let (send_page, recv_page) = mpsc::channel(8);

    let mut poll_url = upstream.clone();
    poll_url.set_path("/export");

    tasks.spawn(poll_upstream(Some(latest), poll_url, send_page));
    tasks.spawn(pages_to_pg(db.clone(), recv_page));
    tasks.spawn(serve(
        upstream,
        wrap,
        listen_conf,
        experimental_conf,
        db.clone(),
    ));

    while let Some(next) = tasks.join_next().await {
        match next {
            Err(e) if e.is_panic() => {
                log::error!("a joinset task panicked: {e}. bailing now. (should we panic?)");
                return Err(e.into());
            }
            Err(e) => {
                log::error!("a joinset task failed to join: {e}");
                return Err(e.into());
            }
            Ok(Err(e)) => {
                log::error!("a joinset task completed with error: {e}");
                return Err(e);
            }
            Ok(Ok(name)) => {
                log::trace!("a task completed: {name:?}. {} left", tasks.len());
            }
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
    bin_init("mirror");
    run(args.globals, args.args).await?;
    Ok(())
}
