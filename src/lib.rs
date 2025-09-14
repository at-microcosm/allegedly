mod backfill;

pub use backfill::week_to_pages;

/// One page of PLC export
///
/// Not limited, but expected to have up to about 1000 lines
pub struct ExportPage {
    pub ops: Vec<String>,
}
