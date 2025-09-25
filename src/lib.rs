use serde::{Deserialize, Serialize};

mod backfill;
mod client;
mod mirror;
mod plc_pg;
mod poll;
mod ratelimit;
mod weekly;

pub use backfill::backfill;
pub use client::{CLIENT, UA};
pub use mirror::{ListenConf, serve};
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
    pub ops: Vec<Op>,
}

impl ExportPage {
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// A fully-deserialized plc operation
///
/// including the plc's wrapping with timestmap and nullified state
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Op {
    pub did: String,
    pub cid: String,
    pub created_at: Dt,
    pub nullified: bool,
    pub operation: Box<serde_json::value::RawValue>,
}

#[cfg(test)]
impl PartialEq for Op {
    fn eq(&self, other: &Self) -> bool {
        self.did == other.did
            && self.cid == other.cid
            && self.created_at == other.created_at
            && self.nullified == other.nullified
            && serde_json::from_str::<serde_json::Value>(self.operation.get()).unwrap()
                == serde_json::from_str::<serde_json::Value>(other.operation.get()).unwrap()
    }
}

/// Database primary key for an op
#[derive(Debug, PartialEq)]
pub struct OpKey {
    pub did: String,
    pub cid: String,
}

impl From<&Op> for OpKey {
    fn from(Op { did, cid, .. }: &Op) -> Self {
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
