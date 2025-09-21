use crate::{BundleSource, Dt, ExportPage, Week, week_to_pages};
use std::sync::Arc;
use tokio::{sync::Mutex, task::JoinSet};

const FIRST_WEEK: Week = Week::from_n(1668643200);

pub async fn backfill(
    source: impl BundleSource + Send + 'static,
    dest: flume::Sender<ExportPage>,
    source_workers: usize,
) -> anyhow::Result<()> {
    // queue up the week bundles that should be available
    let weeks = Arc::new(Mutex::new(Week::range(FIRST_WEEK..)));
    weeks.lock().await.reverse();

    let mut workers: JoinSet<anyhow::Result<()>> = JoinSet::new();

    // spin up the fetchers to work in parallel
    for w in 0..source_workers {
        let weeks = weeks.clone();
        let dest = dest.clone();
        let source = source.clone();
        workers.spawn(async move {
            log::info!("about to get weeks...");

            while let Some(week) = weeks.lock().await.pop() {
                log::info!(
                    "worker {w}: fetching week {} (-{})",
                    Into::<Dt>::into(week).to_rfc3339(),
                    week.n_ago(),
                );
                week_to_pages(source.clone(), week, dest.clone()).await?;
                log::info!("done a week");
            }
            log::info!("done with the weeks ig");
            Ok(())
        });
    }

    // TODO: handle missing/failed weeks

    // wait for them to finish
    while let Some(res) = workers.join_next().await {
        res??;
    }

    Ok(())
}
