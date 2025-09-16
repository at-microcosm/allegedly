use reqwest::Client;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use std::sync::LazyLock;

pub static CLIENT: LazyLock<ClientWithMiddleware> = LazyLock::new(|| {
    let inner = Client::builder()
        .user_agent(concat!(
            "allegedly, v",
            env!("CARGO_PKG_VERSION"),
            " (from @microcosm.blue; contact @bad-example.com)"
        ))
        .build()
        .unwrap();

    let policy = ExponentialBackoff::builder().build_with_max_retries(12);

    ClientBuilder::new(inner)
        .with(RetryTransientMiddleware::new_with_policy(policy))
        .build()
});
