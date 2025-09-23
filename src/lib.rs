use serde::Deserialize;

mod backfill;
mod client;
mod mirror;
mod plc_pg;
mod poll;
mod ratelimit;
mod weekly;

pub use backfill::backfill;
pub use client::CLIENT;
pub use mirror::serve;
pub use plc_pg::{Db, backfill_to_pg, pages_to_pg};
pub use poll::{PageBoundaryState, get_page, poll_upstream};
pub use ratelimit::GovernorMiddleware;
pub use weekly::{BundleSource, FolderSource, HttpSource, Week, pages_to_weeks, week_to_pages};

pub type Dt = chrono::DateTime<chrono::Utc>;

/// One page of PLC export
///
/// plc.directory caps /export at 1000 ops; backfill tasks may send more in a page.
#[derive(Debug)]
pub struct ExportPage {
    pub ops: Vec<String>,
}

impl ExportPage {
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// A fully-deserialized plc operation
///
/// including the plc's wrapping with timestmap and nullified state
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Op<'a> {
    pub did: &'a str,
    pub cid: &'a str,
    pub created_at: Dt,
    pub nullified: bool,
    #[serde(borrow)]
    pub operation: &'a serde_json::value::RawValue,
}

/// Database primary key for an op
#[derive(Debug, PartialEq)]
pub struct OpKey {
    pub did: String,
    pub cid: String,
}

impl From<&Op<'_>> for OpKey {
    fn from(Op { did, cid, .. }: &Op<'_>) -> Self {
        Self {
            did: did.to_string(),
            cid: cid.to_string(),
        }
    }
}

pub fn logo(name: &str) -> String {
    format!(
        r"

    \    |  |                         |  |
   _ \   |  |   -_)   _` |   -_)   _` |  |  |  |    ({name})
 _/  _\ _| _| \___| \__, | \___| \__,_| _| \_, |    (v{})
                     ____|                  __/
",
        env!("CARGO_PKG_VERSION"),
    )
}

pub fn bin_init(name: &str) {
    if std::env::var_os("RUST_LOG").is_none() {
        unsafe { std::env::set_var("RUST_LOG", "info") };
    }
    let filter = tracing_subscriber::EnvFilter::from_default_env();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .init();

    log::info!("{}", logo(name));
}
