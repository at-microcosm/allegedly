use serde::Deserialize;

mod backfill;
mod client;
mod poll;

pub use backfill::week_to_pages;
pub use client::CLIENT;
pub use poll::poll_upstream;

pub type Dt = chrono::DateTime<chrono::Utc>;

/// One page of PLC export
///
/// Not limited, but expected to have up to about 1000 lines
pub struct ExportPage {
    pub ops: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpPeek {
    pub created_at: Dt,
}

pub fn bin_init(name: &str) {
    use env_logger::{Builder, Env};
    Builder::from_env(Env::new().filter_or("RUST_LOG", "info")).init();

    log::info!(
        r"

    \    |  |                         |  |
   _ \   |  |   -_)   _` |   -_)   _` |  |  |  |    ({name})
 _/  _\ _| _| \___| \__, | \___| \__,_| _| \_, |    (v{})
                     ____|                  __/
",
        env!("CARGO_PKG_VERSION")
    );
}
