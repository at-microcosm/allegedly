use clap::Parser;
use serde::Deserialize;
use std::time::Duration;
use tokio_postgres::NoTls;
use url::Url;

const EXPORT_PAGE_QUEUE_SIZE: usize = 32;
const UPSTREAM_REQUEST_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Parser)]
struct Args {
    /// Upstream PLC server to mirror
    ///
    /// default: plc.directory
    #[arg(long, env)]
    #[clap(default_value = "https://plc.directory")]
    upstream: Url,
    /// Mirror PLC's postgres database
    ///
    /// URI string with credentials etc
    #[arg(long, env)]
    postgres: String,
}

/// One page of PLC export
///
/// should have maximum length of 1000 lines.
/// A bulk export consumer should chunk ops into pages of max 1000 ops.
///
/// leading and trailing whitespace should be trimmed.
struct ExportPage {
    pub ops: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OpPeek {
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Op<'a> {
    pub did: &'a str,
    pub cid: &'a str,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub nullified: bool,
    #[serde(borrow)]
    pub operation: &'a serde_json::value::RawValue,
}

async fn export_upstream(
    upstream: Url,
    tx: flume::Sender<ExportPage>,
    latest: Option<chrono::DateTime<chrono::Utc>>,
) {
    let mut upstream = upstream;
    upstream.set_path("/export");
    let mut after = latest;
    let mut tick = tokio::time::interval(UPSTREAM_REQUEST_INTERVAL);
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            "allegedly v",
            env!("CARGO_PKG_VERSION"),
            " (part of @microcosm.blue; contact @bad-example.com)"
        ))
        .timeout(Duration::from_secs(4))
        .build()
        .unwrap();

    loop {
        tick.tick().await;
        let mut url = upstream.clone();
        if let Some(ref after) = after {
            url.query_pairs_mut()
                .append_pair("after", &after.to_rfc3339());
        }
        let ops = client
            .get(url)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap()
            .trim()
            .to_string();

        let Some((_, last_line)) = ops.rsplit_once('\n') else {
            log::trace!("no ops in response page, nothing to do");
            continue;
        };

        let op: OpPeek = serde_json::from_str(last_line).unwrap();
        after = Some(op.created_at);

        log::trace!("got some ops until {after:?}, sending them...");
        tx.send_async(ExportPage { ops }).await.unwrap();
    }
}

async fn write_pages(
    rx: flume::Receiver<ExportPage>,
    mut pg_client: tokio_postgres::Client,
) -> Result<(), anyhow::Error> {
    let upsert_did = &pg_client
        .prepare(
            r#"
        INSERT INTO dids (did) VALUES ($1)
            ON CONFLICT DO NOTHING"#,
        )
        .await
        .unwrap();

    let insert_op = &pg_client
        .prepare(
            r#"
        INSERT INTO operations (did, operation, cid, nullified, "createdAt")
        VALUES ($1, $2, $3, $4, $5)"#,
        ) // TODO: check that it hasn't changed
        .await
        .unwrap();

    while let Ok(page) = rx.recv_async().await {
        log::info!("got a page...");

        let mut tx = pg_client.transaction().await.unwrap();

        // TODO: probably figure out postgres COPY IN
        // for now just write everything into a transaction

        log::info!("setting up inserts...");
        for op_line in page.ops.lines() {
            let op: Op = serde_json::from_str(op_line).unwrap();
            let client = &tx;

            client.execute(upsert_did, &[&op.did]).await.unwrap();

            let sp = tx.savepoint("op").await.unwrap();
            if let Err(e) = sp
                .execute(
                    insert_op,
                    &[
                        &op.did,
                        &tokio_postgres::types::Json(op.operation),
                        &op.cid,
                        &op.nullified,
                        &op.created_at,
                    ],
                )
                .await
            {
                if e.code() != Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
                    anyhow::bail!(e);
                }
                // TODO: assert that the row has not changed
                log::warn!("ignoring dup");
            } else {
                sp.commit().await.unwrap();
            }
        }

        tx.commit().await.unwrap();

        log::info!("hi from writer! (done page)");
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    env_logger::init();
    log::info!(concat!("📜 Allegedly v", env!("CARGO_PKG_VERSION")));

    let args = Args::parse();

    log::trace!("connecting postgres...");
    let (pg_client, connection) = tokio_postgres::connect(&args.postgres, NoTls)
        .await
        .unwrap();

    // send the connection away to do the actual communication work
    // TODO: error and shutdown handling
    let conn_task = tokio::task::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    let latest = pg_client
        .query_opt(
            r#"SELECT "createdAt" FROM operations
            ORDER BY "createdAt" DESC LIMIT 1"#,
            &[],
        )
        .await
        .unwrap()
        .map(|r| r.get(0));

    log::info!("connected! latest: {latest:?}");

    let (tx, rx) = flume::bounded::<ExportPage>(EXPORT_PAGE_QUEUE_SIZE);

    let export_task = tokio::task::spawn(export_upstream(args.upstream, tx, latest));
    let writer_task = tokio::task::spawn(write_pages(rx, pg_client));

    tokio::select! {
        z = conn_task => log::warn!("connection task ended: {z:?}"),
        z = export_task => log::warn!("export task ended: {z:?}"),
        z = writer_task => log::warn!("writer task ended: {z:?}"),
    };

    log::error!("todo: shutdown");
}
