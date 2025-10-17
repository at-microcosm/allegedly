use reqwest::Url;

#[derive(Debug, Clone, clap::Args)]
pub struct GlobalArgs {
    /// Upstream PLC server
    #[arg(short, long, global = true, env = "ALLEGEDLY_UPSTREAM")]
    #[clap(default_value = "https://plc.directory")]
    pub upstream: Url,
    /// Self-rate-limit upstream request interval
    ///
    /// plc.directory's rate limiting is 500 requests per 5 mins (600ms)
    #[arg(long, global = true, env = "ALLEGEDLY_UPSTREAM_THROTTLE_MS")]
    #[clap(default_value = "600")]
    pub upstream_throttle_ms: u64,
}

#[allow(dead_code)]
fn main() {
    panic!("this is not actually a module")
}
