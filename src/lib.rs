use serde::Deserialize;

mod backfill;
mod poll;

pub use backfill::week_to_pages;
pub use poll::poll_upstream;

/// One page of PLC export
///
/// Not limited, but expected to have up to about 1000 lines
pub struct ExportPage {
    pub ops: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpPeek {
    pub created_at: chrono::DateTime<chrono::Utc>,
}
