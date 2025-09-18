use clap::{Parser, Subcommand};
use url::Url;
use allegedly::{Dt, bin_init, poll_upstream};

#[derive(Debug, Parser)]
struct Cli {
    /// Upstream PLC server
    #[arg(long, env)]
    #[clap(default_value = "https://plc.directory")]
    upstream: Url,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Poll an upstream PLC server and log new ops to stdout
    Tail {
        /// Begin replay from a specific timestamp
        #[arg(long)]
        after: Option<Dt>,
    }
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
