use crate::{BundleSource, Dt, ExportPage, Week, week_to_pages};
use tokio::task::JoinSet;

const FIRST_WEEK: Week = Week::from_n(1668643200);

pub async fn backfill(
    source: impl BundleSource + Send + 'static,
    dest: flume::Sender<ExportPage>,
    source_workers: usize,
) -> anyhow::Result<()> {
    // queue up the week bundles that should be available
    let (week_tx, week_rx) = flume::bounded(1024); // work queue
    let mut week = FIRST_WEEK;
    while week.is_immutable() {
        week_tx.try_send(week)?; // if this fails, something has gone really wrong or we're farrrr in the future
        week = week.next();
    }

    let mut workers: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // spin up the fetchers to work in parallel
    for w in 0..source_workers {
        let weeks = week_rx.clone();
        let dest = dest.clone();
        let source = source.clone();
        workers.spawn(async move {
            while let Ok(week) = weeks.recv_async().await {
                log::info!(
                    "worker {w}: fetching week {} (-{})",
                    Into::<Dt>::into(week).to_rfc3339(),
                    week.n_ago(),
                );
                week_to_pages(source.clone(), week, dest.clone()).await?;
            }
            Ok(())
        });
    }

    // wait for them to finish
    while let Some(res) = workers.join_next().await {
        res??;
    }

    Ok(())
}
