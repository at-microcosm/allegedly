use crate::{Dt, ExportPage, Op, PageBoundaryState};
use std::pin::pin;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::{
    Client, Error as PgError, NoTls,
    binary_copy::BinaryCopyInWriter,
    connect,
    types::{Json, Type},
};

/// a little tokio-postgres helper
///
/// it's clone for easiness. it doesn't share any resources underneath after
/// cloning at all so it's not meant for
#[derive(Debug, Clone)]
pub struct Db {
    pg_uri: String,
}

impl Db {
    pub async fn new(pg_uri: &str) -> Result<Self, anyhow::Error> {
        // we're going to interact with did-method-plc's database, so make sure
        // it's what we expect: check for db migrations.
        log::trace!("checking migrations...");
        let (client, connection) = connect(pg_uri, NoTls).await?;
        let connection_task = tokio::task::spawn(async move {
            connection
                .await
                .inspect_err(|e| log::error!("connection ended with error: {e}"))
                .unwrap();
        });
        let migrations: Vec<String> = client
            .query("SELECT name FROM kysely_migration ORDER BY name", &[])
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(
            &migrations,
            &[
                "_20221020T204908820Z",
                "_20230223T215019669Z",
                "_20230406T174552885Z",
                "_20231128T203323431Z",
            ]
        );
        drop(client);
        // make sure the connection worker thing doesn't linger
        connection_task.await?;
        log::info!("db connection succeeded and plc migrations appear as expected");

        Ok(Self {
            pg_uri: pg_uri.to_string(),
        })
    }

    pub async fn connect(&self) -> Result<Client, PgError> {
        log::trace!("connecting postgres...");
        let (client, connection) = connect(&self.pg_uri, NoTls).await?;

        // send the connection away to do the actual communication work
        // apparently the connection will complete when the client drops
        tokio::task::spawn(async move {
            connection
                .await
                .inspect_err(|e| log::error!("connection ended with error: {e}"))
                .unwrap();
        });

        Ok(client)
    }

    pub async fn get_latest(&self) -> Result<Option<Dt>, PgError> {
        let client = self.connect().await?;
        let dt: Option<Dt> = client
            .query_opt(
                r#"SELECT "createdAt"
                           FROM operations
                          ORDER BY "createdAt" DESC
                          LIMIT 1"#,
                &[],
            )
            .await?
            .map(|row| row.get(0));
        Ok(dt)
    }
}

pub async fn pages_to_pg(db: Db, mut pages: mpsc::Receiver<ExportPage>) -> Result<(), PgError> {
    let mut client = db.connect().await?;

    let ops_stmt = client
        .prepare(
            r#"INSERT INTO operations (did, operation, cid, nullified, "createdAt")
               VALUES ($1, $2, $3, $4, $5)
                   ON CONFLICT do nothing"#,
        )
        .await?;
    let did_stmt = client
        .prepare(r#"INSERT INTO dids (did) VALUES ($1) ON CONFLICT do nothing"#)
        .await?;

    let t0 = Instant::now();
    let mut ops_inserted = 0;
    let mut dids_inserted = 0;

    while let Some(page) = pages.recv().await {
        log::trace!("writing page with {} ops", page.ops.len());
        let tx = client.transaction().await?;
        for s in page.ops {
            let Ok(op) = serde_json::from_str::<Op>(&s) else {
                log::warn!("ignoring unparseable op {s:?}");
                continue;
            };
            ops_inserted += tx
                .execute(
                    &ops_stmt,
                    &[
                        &op.did,
                        &Json(op.operation),
                        &op.cid,
                        &op.nullified,
                        &op.created_at,
                    ],
                )
                .await?;
            dids_inserted += tx.execute(&did_stmt, &[&op.did]).await?;
        }
        tx.commit().await?;
    }

    log::info!(
        "no more pages. inserted {ops_inserted} ops and {dids_inserted} dids in {:?}",
        t0.elapsed()
    );
    Ok(())
}

/// Dump rows into an empty operations table quickly
///
/// you must run this after initializing the db with kysely migrations from the
/// typescript app, but before inserting any content.
///
/// it's an invasive process: it will drop the indexes that kysely created (and
/// restore them after) in order to get the backfill in as quickly as possible.
///
/// fails: if the backfill data violates the primary key constraint (unique did*cid)
///
/// panics: if the operations or dids tables are not empty, unless reset is true
///
/// recommended postgres setting: `max_wal_size=4GB` (or more)
pub async fn backfill_to_pg(
    db: Db,
    reset: bool,
    mut pages: mpsc::Receiver<ExportPage>,
    notify_last_at: Option<oneshot::Sender<Option<Dt>>>,
) -> Result<(), PgError> {
    let mut client = db.connect().await?;

    let t0 = Instant::now();
    let tx = client.transaction().await?;
    tx.execute("SET LOCAL synchronous_commit = off", &[])
        .await?;

    let t_step = Instant::now();
    for table in ["operations", "dids"] {
        if reset {
            let n = tx.execute(&format!("DELETE FROM {table}"), &[]).await?;
            if n > 0 {
                log::warn!("postgres reset: deleted {n} from {table}");
            }
        } else {
            let n: i64 = tx
                .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                .await?
                .get(0);
            if n > 0 {
                panic!("postgres: {table} table was not empty and `reset` not requested");
            }
        }
    }
    log::trace!("tables clean: {:?}", t_step.elapsed());

    let t_step = Instant::now();
    tx.execute("ALTER TABLE operations SET UNLOGGED", &[])
        .await?;
    tx.execute("ALTER TABLE dids SET UNLOGGED", &[]).await?;
    log::trace!("set tables unlogged: {:?}", t_step.elapsed());

    let t_step = Instant::now();
    tx.execute(r#"DROP INDEX "operations_createdAt_index""#, &[])
        .await?;
    tx.execute("DROP INDEX operations_did_createdat_idx", &[])
        .await?;
    log::trace!("indexes dropped: {:?}", t_step.elapsed());

    let t_step = Instant::now();
    log::trace!("starting binary COPY IN...");
    let types = &[
        Type::TEXT,
        Type::JSONB,
        Type::TEXT,
        Type::BOOL,
        Type::TIMESTAMPTZ,
    ];
    let sync = tx
        .copy_in(
            r#"COPY operations (did, operation, cid, nullified, "createdAt") FROM STDIN BINARY"#,
        )
        .await?;
    let mut writer = pin!(BinaryCopyInWriter::new(sync, types));
    let mut last_at = None;
    while let Some(page) = pages.recv().await {
        for s in &page.ops {
            let Ok(op) = serde_json::from_str::<Op>(s) else {
                log::warn!("ignoring unparseable op: {s:?}");
                continue;
            };
            writer
                .as_mut()
                .write(&[
                    &op.did,
                    &Json(op.operation),
                    &op.cid,
                    &op.nullified,
                    &op.created_at,
                ])
                .await?;
        }
        if notify_last_at.is_some()
            && let Some(s) = PageBoundaryState::new(&page)
        {
            last_at = last_at.filter(|&l| l >= s.last_at).or(Some(s.last_at));
        }
    }

    if let Some(notify) = notify_last_at {
        log::trace!("notifying last_at: {last_at:?}");
        if notify.send(last_at).is_err() {
            log::error!("receiver for last_at dropped, can't notify");
        };
    }

    let n = writer.as_mut().finish().await?;
    log::trace!("COPY IN wrote {n} ops: {:?}", t_step.elapsed());

    // CAUTION: these indexes MUST match up exactly with the kysely ones we dropped
    let t_step = Instant::now();
    tx.execute(
        r#"CREATE INDEX operations_did_createdat_idx ON operations (did, "createdAt")"#,
        &[],
    )
    .await?;
    tx.execute(
        r#"CREATE INDEX "operations_createdAt_index" ON operations ("createdAt")"#,
        &[],
    )
    .await?;
    log::trace!("indexes recreated: {:?}", t_step.elapsed());

    let t_step = Instant::now();
    let n = tx
        .execute(
            r#"INSERT INTO dids SELECT distinct did FROM operations"#,
            &[],
        )
        .await?;
    log::trace!("INSERT wrote {n} dids: {:?}", t_step.elapsed());

    let t_step = Instant::now();
    tx.execute("ALTER TABLE dids SET LOGGED", &[]).await?;
    tx.execute("ALTER TABLE operations SET LOGGED", &[]).await?;
    log::trace!("set tables LOGGED: {:?}", t_step.elapsed());

    tx.commit().await?;
    log::info!("total backfill time: {:?}", t0.elapsed());

    Ok(())
}
