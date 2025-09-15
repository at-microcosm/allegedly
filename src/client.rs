use reqwest::Client;
use std::sync::LazyLock;

pub static CLIENT: LazyLock<Client> = LazyLock::new(|| {
    Client::builder()
        .user_agent(concat!(
            "allegedly, v",
            env!("CARGO_PKG_VERSION"),
            " (from @microcosm.blue; contact @bad-example.com)"
        ))
        .build()
        .unwrap()
});
