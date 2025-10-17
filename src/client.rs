use reqwest::Client;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use std::sync::LazyLock;

pub const UA: &str = concat!(
    "allegedly, v",
    env!("CARGO_PKG_VERSION"),
    " (from @microcosm.blue; contact @bad-example.com)"
);

pub static CLIENT: LazyLock<ClientWithMiddleware> = LazyLock::new(|| {
    let inner = Client::builder()
        .user_agent(UA)
        .gzip(true)
        .build()
        .expect("reqwest client to build");

    let policy = ExponentialBackoff::builder().build_with_max_retries(12);

    ClientBuilder::new(inner)
        .with(RetryTransientMiddleware::new_with_policy(policy))
        .build()
});
