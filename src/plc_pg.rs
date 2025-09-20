use crate::{ExportPage, Op};
use tokio_postgres::{Client, types::{Type, Json}, Error as PgError, NoTls, connect, binary_copy::BinaryCopyInWriter};
use std::pin::pin;


/// a little tokio-postgres helper
///
/// it's clone for easiness. it doesn't share any resources underneath after
/// cloning at all so it's not meant for
#[derive(Debug, Clone)]
pub struct Db {
    pg_uri: String,
}

impl Db {
    pub fn new(pg_uri: &str) -> Self {
        Self {
            pg_uri: pg_uri.to_string(),
        }
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

pub async fn write_bulk(
    db: Db,
    pages: flume::Receiver<ExportPage>,
) -> Result<(), PgError> {
    let mut client = db.connect().await?;
    let tx = client.transaction().await?;

    tx
        .execute(r#"
        CREATE TABLE backfill (
            did text not null,
            cid text not null,
            operation jsonb not null,
            nullified boolean not null,
            createdAt timestamptz not null
        )"#, &[])
        .await?;


    let types = &[
        Type::TEXT,
        Type::TEXT,
        Type::JSONB,
        Type::BOOL,
        Type::TIMESTAMPTZ,
    ];

    let sync = tx.copy_in("COPY backfill FROM STDIN BINARY").await?;
    let mut writer = pin!(BinaryCopyInWriter::new(sync, types));

    while let Ok(page) = pages.recv_async().await {
        for s in page.ops {
            let Ok(op) = serde_json::from_str::<Op>(&s) else {
                log::warn!("ignoring unparseable op: {s:?}");
                continue;
            };
            writer.as_mut().write(&[
                &op.did,
                &op.cid,
                &Json(op.operation),
                &op.nullified,
                &op.created_at,
            ]).await?;
        }
    }

    let n = writer.as_mut().finish().await?;
    log::info!("copied in {n} rows");

    tx.commit().await?;

    Ok(())
}
