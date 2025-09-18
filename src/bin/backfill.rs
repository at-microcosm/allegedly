use clap::Parser;
use std::time::Duration;
use url::Url;

use allegedly::{Db, Dt, ExportPage, Op, bin_init, poll_upstream};

const EXPORT_PAGE_QUEUE_SIZE: usize = 0; // rendezvous for now
const WEEK_IN_SECONDS: u64 = 7 * 86400;

#[derive(Parser)]
struct Args {
    /// Upstream PLC server to mirror
    ///
    /// default: https://plc.directory
    #[arg(long, env)]
    #[clap(default_value = "https://plc.directory")]
    upstream: Url,
    /// Bulk export source prefix
    ///
    /// Must be a prefix for urls ending with {WEEK_TIMESTAMP}.jsonl.gz
    ///
    /// default: https://plc.t3.storage.dev/plc.directory/
    ///
    /// pass "off" to skip fast bulk backfilling
    #[arg(long, env)]
    #[clap(default_value = "https://plc.t3.storage.dev/plc.directory/")]
    upstream_bulk: Url,
    /// The oldest available bulk upstream export timestamp
    ///
    /// Must be a week-truncated unix timestamp
    ///
    /// plc.directory's oldest week is `1668643200`; you probably don't want to change this.
    #[arg(long, env)]
    #[clap(default_value = "1668643200")]
    bulk_epoch: u64,
    /// Mirror PLC's postgres database
    ///
    /// URI string with credentials etc
    #[arg(long, env)]
    postgres: String,
}

async fn bulk_backfill((_upstream, epoch): (Url, u64), _tx: flume::Sender<ExportPage>) {
    let immutable_cutoff = std::time::SystemTime::now() - Duration::from_secs((7 + 4) * 86400);
    let immutable_ts = (immutable_cutoff.duration_since(std::time::SystemTime::UNIX_EPOCH))
        .unwrap()
        .as_secs();
    let _immutable_week = (immutable_ts / WEEK_IN_SECONDS) * WEEK_IN_SECONDS;
    let _week = epoch;
    let _week_n = 0;
    todo!();
    // while week < immutable_week {
    //     log::info!("backfilling week {week_n} ({week})");
    //     let url = upstream.join(&format!("{week}.jsonl.gz")).unwrap();
    //     week_to_pages(url, tx.clone()).await.unwrap();
    //     week_n += 1;
    //     week += WEEK_IN_SECONDS;
    // }
}

async fn export_upstream(
    upstream: Url,
    bulk: (Url, u64),
    tx: flume::Sender<ExportPage>,
    pg_client: tokio_postgres::Client,
) {
    let latest = get_latest(&pg_client).await;

    if latest.is_none() {
        bulk_backfill(bulk, tx.clone()).await;
    }
    let mut upstream = upstream;
    upstream.set_path("/export");
    poll_upstream(latest, upstream, tx).await.unwrap();
}

async fn write_pages(
    rx: flume::Receiver<ExportPage>,
    mut pg_client: tokio_postgres::Client,
) -> Result<(), anyhow::Error> {
    // TODO: one big upsert at the end from select distinct on the other table

    // let upsert_did = &pg_client
    //     .prepare(
    //         r#"
    //     INSERT INTO dids (did) VALUES ($1)
    //         ON CONFLICT DO NOTHING"#,
    //     )
    //     .await
    //     .unwrap();

    let insert_op = &pg_client
        .prepare(
            r#"
        INSERT INTO operations (did, operation, cid, nullified, "createdAt")
        VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (did, cid) DO UPDATE
           SET nullified = excluded.nullified,
               "createdAt" = excluded."createdAt"
         WHERE operations.nullified = excluded.nullified
            OR operations."createdAt" = excluded."createdAt""#,
        ) // idea: op is provable via cid, so leave it out. after did/cid (pk) that leaves nullified and createdAt
        // that we want to notice changing.
        // normal insert: no conflict, rows changed = 1
        // conflict (exact match): where clause passes, rows changed = 1
        // conflict (mismatch): where clause fails, rows changed = 0 (detect this and warn!)
        .await
        .unwrap();

    while let Ok(page) = rx.recv_async().await {
        log::trace!("got a page...");

        let tx = pg_client.transaction().await.unwrap();

        // TODO: probably figure out postgres COPY IN
        // for now just write everything into a transaction

        log::trace!("setting up inserts...");
        for op_line in page
            .ops
            .into_iter()
            .flat_map(|s| {
                s.replace("}{", "}\n{")
                    .split('\n')
                    .map(|s| s.trim())
                    .map(Into::into)
                    .collect::<Vec<String>>()
            })
            .filter(|s| !s.is_empty())
        {
            let Ok(op) = serde_json::from_str::<Op>(&op_line)
                .inspect_err(|e| log::error!("failing! at the {op_line}! {e}"))
            else {
                log::error!("ayeeeee just ignoring this error for now......");
                continue;
            };
            // let client = &tx;

            // client.execute(upsert_did, &[&op.did]).await.unwrap();

            // let sp = tx.savepoint("op").await.unwrap();
            let inserted = tx
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
                .unwrap();
            if inserted != 1 {
                log::warn!(
                    "possible log modification: {inserted} rows changed after upserting {op:?}"
                );
            }
            // {
            //     if e.code() != Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION) {
            //         anyhow::bail!(e);
            //     }
            //     // TODO: assert that the row has not changed
            //     log::warn!("ignoring dup");
            // }
        }

        tx.commit().await.unwrap();
    }
    Ok(())
}

async fn get_latest(pg_client: &tokio_postgres::Client) -> Option<Dt> {
    pg_client
        .query_opt(
            r#"SELECT "createdAt" FROM operations
            ORDER BY "createdAt" DESC LIMIT 1"#,
            &[],
        )
        .await
        .unwrap()
        .map(|r| r.get(0))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    bin_init("main");
    let args = Args::parse();
    let db = Db::new(&args.postgres);
    let (tx, rx) = flume::bounded(EXPORT_PAGE_QUEUE_SIZE);

    log::trace!("connecting postgres for export task...");
    let pg_client = db.connect().await?;
    let export_task = tokio::task::spawn(export_upstream(
        args.upstream,
        (args.upstream_bulk, args.bulk_epoch),
        tx,
        pg_client,
    ));

    log::trace!("connecting postgres for writer task...");
    let pg_client = db.connect().await?;
    let writer_task = tokio::task::spawn(write_pages(rx, pg_client));

    tokio::select! {
        z = export_task => log::warn!("export task ended: {z:?}"),
        z = writer_task => log::warn!("writer task ended: {z:?}"),
    };

    log::error!("todo: shutdown");

    Ok(())
}
