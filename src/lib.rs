use serde::Deserialize;

mod backfill;
mod client;
mod plc_pg;
mod poll;
mod weekly;

pub use backfill::week_to_pages;
pub use client::CLIENT;
pub use plc_pg::Db;
pub use poll::{get_page, poll_upstream};
pub use weekly::{Week, pages_to_weeks};

pub type Dt = chrono::DateTime<chrono::Utc>;

/// One page of PLC export
///
/// Expected to have up to around 1000 lines of raw json ops
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
