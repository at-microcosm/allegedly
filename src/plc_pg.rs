use tokio_postgres::{Client, Error as PgError, NoTls, connect};

/// a little tokio-postgres helper
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
