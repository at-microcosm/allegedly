use reqwest::Url;

#[derive(Debug, Clone, clap::Args)]
pub struct GlobalArgs {
    /// Upstream PLC server
    #[arg(short, long, global = true, env = "ALLEGEDLY_UPSTREAM")]
    #[clap(default_value = "https://plc.directory")]
    pub upstream: Url,
}

#[allow(dead_code)]
fn main() {
    panic!("this is not actually a module")
}
