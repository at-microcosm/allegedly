use allegedly::{Dt, bin_init, poll_upstream};
use clap::{Parser, Subcommand};
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
        Commands::Tail { after } => {
            let mut url = args.upstream;
            url.set_path("/export");
            let start_at = after.or_else(|| Some(chrono::Utc::now()));
            let (tx, rx) = flume::bounded(0); // rendezvous
            tokio::task::spawn(async move { poll_upstream(start_at, url, tx).await.unwrap() });
            loop {
                for op in rx.recv_async().await.unwrap().ops {
                    println!("{op}")
                }
            }
        }
    }
}
