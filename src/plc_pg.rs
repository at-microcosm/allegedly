use crate::{ExportPage, Op};
use std::pin::pin;
use std::time::Instant;
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
}

pub async fn write_bulk(db: Db, pages: flume::Receiver<ExportPage>) -> Result<(), PgError> {
    let mut client = db.connect().await?;

    // TODO: maybe we want to be more cautious
    client
        .execute(
            r#"
        DROP TABLE IF EXISTS allegedly_backfill"#,
            &[],
        )
        .await?;

    let tx = client.transaction().await?;

    tx.execute(
        r#"
        CREATE UNLOGGED TABLE allegedly_backfill (
            did text not null,
            cid text not null,
            operation jsonb not null,
            nullified boolean not null,
            createdAt timestamptz not null
        )"#,
        &[],
    )
    .await?;

    let types = &[
        Type::TEXT,
        Type::TEXT,
        Type::JSONB,
        Type::BOOL,
        Type::TIMESTAMPTZ,
    ];
    let t0 = Instant::now();

    let sync = tx
        .copy_in("COPY allegedly_backfill FROM STDIN BINARY")
        .await?;
    let mut writer = pin!(BinaryCopyInWriter::new(sync, types));

    while let Ok(page) = pages.recv_async().await {
        for s in page.ops {
            let Ok(op) = serde_json::from_str::<Op>(&s) else {
                log::warn!("ignoring unparseable op: {s:?}");
                continue;
            };
            writer
                .as_mut()
                .write(&[
                    &op.did,
                    &op.cid,
                    &Json(op.operation),
                    &op.nullified,
                    &op.created_at,
                ])
                .await?;
        }
    }

    let n = writer.as_mut().finish().await?;
    log::info!("copied in {n} rows");

    tx.commit().await?;

    let dt = t0.elapsed();
    log::info!("backfill time: {dt:?}");

    Ok(())
}
