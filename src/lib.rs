mod backfill;

pub use backfill::PageForwarder;

/// One page of PLC export
///
/// should have maximum length of 1000 lines.
/// A bulk export consumer should chunk ops into pages of max 1000 ops.
///
/// leading and trailing whitespace should be trimmed.
pub struct ExportPage {
    pub ops: String,
}
