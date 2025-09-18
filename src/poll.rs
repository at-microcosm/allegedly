use crate::{CLIENT, Dt, ExportPage, Op, OpKey};
use std::time::Duration;
use thiserror::Error;
use url::Url;

const UPSTREAM_REQUEST_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Debug, Error)]
pub enum GetPageError {
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    #[error(transparent)]
    ReqwestMiddleware(#[from] reqwest_middleware::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

/// ops are primary-keyed by (did, cid)
/// plc orders by `created_at` but does not guarantee distinct times per op
/// we assume that the order will at least be deterministic: this may be unsound
#[derive(Debug, PartialEq)]
pub struct LastOp {
    pub created_at: Dt,   // any op greater is definitely not duplicated
    pk: (String, String), // did, cid
}

impl From<Op<'_>> for LastOp {
    fn from(op: Op) -> Self {
        Self {
            created_at: op.created_at,
            pk: (op.did.to_string(), op.cid.to_string()),
        }
    }
}

impl From<Dt> for LastOp {
    fn from(dt: Dt) -> Self {
        Self {
            created_at: dt,
            pk: ("".to_string(), "".to_string()),
        }
    }
}

/// PLC
struct PageBoundaryState {
    last_at: Dt,
    keys_at: Vec<OpKey>, // expected to ~always be length one
}

// ok so this is silly.
//
// i think i had some idea that deferring parsing to later steps would make it
// easier to do things like sometimes not parsing at all (where the output is
// also json lines), and maybe avoid some memory shuffling.
// but since the input already has to be split into lines, keeping them as line
// strings is probably the worst option: space-inefficient, allows garbage, and
// leads to, well, this impl.
//
// it almost could have been slick if the *original* was just reused, and the
// parsed ops were just kind of on the side referencing into it, but i'm lazy
// and didn't get it there.
//
// should unrefactor to make Op own its data again, parse (and deal with errors)
// upfront, and probably greatly simplify everything downstream. simple.
impl PageBoundaryState {
    fn new(page: &mut ExportPage) -> Option<Self> {
        // grab the very last op
        let (last_at, last_key) = loop {
            let Some(s) = page.ops.last().cloned() else {
                // there are no ops left? oop. bail.
                // last_at and existing keys remain in tact if there was no later op
                return None;
            };
            if s.is_empty() {
                // annoying: trim off any trailing blank lines
                page.ops.pop();
                continue;
            }
            let Ok(op) = serde_json::from_str::<Op>(&s)
                .inspect_err(|e| log::warn!("deduplication failed last op parsing ({s:?}: {e}), ignoring for downstream to deal with."))
            else {
                // doubly annoying: skip over trailing garbage??
                continue;
            };
            break (op.created_at, Into::<OpKey>::into(&op));
        };

        // set initial state
        let mut me = Self {
            last_at,
            keys_at: vec![last_key],
        };

        // and make sure all keys at this time are captured from the back
        me.capture_nth_last_at(page, last_at);

        Some(me)
    }
    fn apply_to_next(&mut self, page: &mut ExportPage) {
        // walk ops forward, kicking previously-seen ops until created_at advances
        let to_remove: Vec<usize> = page
            .ops
            .iter()
            .map(|s| serde_json::from_str::<Op>(s).inspect_err(|e|
                log::warn!("deduplication failed op parsing ({s:?}: {e}), bailing for downstream to deal with.")))
            .enumerate()
            .take_while(|(_, opr)| opr.as_ref().map(|op| op.created_at == self.last_at).unwrap_or(false))
            .filter_map(|(i, opr)| {
                if self.keys_at.contains(&(&opr.expect("any Errs were filtered by take_while")).into()) {
                    Some(i)
                } else { None }
            })
            .collect();

        // actually remove them. last to first to indices don't shift
        for dup_idx in to_remove.into_iter().rev() {
            page.ops.remove(dup_idx);
        }

        // grab the very last op
        let (last_at, last_key) = loop {
            let Some(s) = page.ops.last().cloned() else {
                // there are no ops left? oop. bail.
                // last_at and existing keys remain in tact if there was no later op
                return;
            };
            if s.is_empty() {
                // annoying: trim off any trailing blank lines
                page.ops.pop();
                continue;
            }
            let Ok(op) = serde_json::from_str::<Op>(&s)
                .inspect_err(|e| log::warn!("deduplication failed last op parsing ({s:?}: {e}), ignoring for downstream to deal with."))
            else {
                // doubly annoying: skip over trailing garbage??
                continue;
            };
            break (op.created_at, Into::<OpKey>::into(&op));
        };

        // reset state (as long as time actually moved forward on this page)
        if last_at > self.last_at {
            self.last_at = last_at;
            self.keys_at = vec![last_key];
        } else {
            // weird cases: either time didn't move (fine...) or went backwards (not fine)
            assert_eq!(last_at, self.last_at, "time moved backwards on a page");
        }
        // and make sure all keys at this time are captured from the back
        self.capture_nth_last_at(page, last_at);
    }

    /// walk backwards from 2nd last and collect keys until created_at changes
    fn capture_nth_last_at(&mut self, page: &mut ExportPage, last_at: Dt) {
        page.ops
            .iter()
            .rev()
            .skip(1) // we alredy added the very last one
            .map(|s| serde_json::from_str::<Op>(s).inspect_err(|e|
                log::warn!("deduplication failed op parsing ({s:?}: {e}), bailing for downstream to deal with.")))
            .take_while(|opr| opr.as_ref().map(|op| op.created_at == last_at).unwrap_or(false))
            .for_each(|opr| {
                let op = &opr.expect("any Errs were filtered by take_while");
                self.keys_at.push(op.into());
            });
    }
}

pub async fn get_page(url: Url) -> Result<(ExportPage, Option<LastOp>), GetPageError> {
    log::trace!("Getting page: {url}");

    let ops: Vec<String> = CLIENT
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?
        .trim()
        .split('\n')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() { None } else { Some(s) }
        })
        .map(Into::into)
        .collect();

    let last_op = ops
        .last()
        .filter(|s| !s.is_empty())
        .map(|s| serde_json::from_str::<Op>(s))
        .transpose()?
        .map(Into::into)
        .inspect(|at| log::trace!("new last op: {at:?}"));

    Ok((ExportPage { ops }, last_op))
}

pub async fn poll_upstream(
    after: Option<Dt>,
    base: Url,
    dest: flume::Sender<ExportPage>,
) -> anyhow::Result<()> {
    let mut tick = tokio::time::interval(UPSTREAM_REQUEST_INTERVAL);
    let mut prev_last: Option<LastOp> = after.map(Into::into);
    let mut boundary_state: Option<PageBoundaryState> = None;
    loop {
        tick.tick().await;

        let mut url = base.clone();
        if let Some(ref pl) = prev_last {
            url.query_pairs_mut()
                .append_pair("after", &pl.created_at.to_rfc3339());
        };

        let (mut page, next_last) = get_page(url).await?;
        if let Some(ref mut state) = boundary_state {
            state.apply_to_next(&mut page);
        } else {
            boundary_state = PageBoundaryState::new(&mut page);
        }
        if !page.is_empty() {
            match dest.try_send(page) {
                Ok(()) => {}
                Err(flume::TrySendError::Full(page)) => {
                    log::warn!("export: destination channel full, awaiting...");
                    dest.send_async(page).await?;
                }
                e => e?,
            };
        }

        prev_last = next_last.or(prev_last);
    }
}
