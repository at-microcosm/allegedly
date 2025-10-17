use allegedly::{ExportPage, poll_upstream};

#[tokio::main]
async fn main() {
    // set to `None` to replay from the beginning of the PLC history
    let after = Some(chrono::Utc::now());

    // the PLC server to poll for new ops
    let upstream = "https://plc.wtf/export".parse().unwrap();

    // self-rate-limit (plc.directory's limit interval is 600ms)
    let throttle = std::time::Duration::from_millis(300);

    // pages are sent out of the poller via a tokio mpsc channel
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    // spawn a tokio task to run the poller
    tokio::task::spawn(poll_upstream(after, upstream, throttle, tx));

    // receive pages of plc ops from the poller
    while let Some(ExportPage { ops }) = rx.recv().await {
        println!("received {} plc ops", ops.len());

        for op in ops {
            // in this example we're alerting when changes are found for one
            // specific identity
            if op.did == "did:plc:hdhoaan3xa3jiuq4fg4mefid" {
                println!(
                    "Update found for {}! cid={}\n -> operation: {}",
                    op.did, op.cid, op.operation.get()
                );
            }
        }
    }
}
