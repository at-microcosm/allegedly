use crate::logo;

use governor::{
    NotUntil, Quota, RateLimiter,
    clock::{Clock, DefaultClock},
    state::keyed::DefaultKeyedStateStore,
};
use poem::{Endpoint, Middleware, Request, Response, Result, http::StatusCode};
use std::{
    convert::TryInto,
    net::{IpAddr, Ipv6Addr},
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::sync::oneshot;

static CLOCK: LazyLock<DefaultClock> = LazyLock::new(DefaultClock::default);

const IP6_64_MASK: Ipv6Addr = Ipv6Addr::from_bits(0xFFFF_FFFF_FFFF_FFFF_0000_0000_0000_0000);
type IP6_56 = [u8; 7];
type IP6_48 = [u8; 6];

fn scale_quota(quota: Quota, factor: u32) -> Option<Quota> {
    let period = quota.replenish_interval() / factor;
    let burst = quota
        .burst_size()
        .checked_mul(factor.try_into().unwrap())
        .unwrap();
    Quota::with_period(period).map(|q| q.allow_burst(burst))
}

#[derive(Debug)]
struct IpLimiters {
    per_ip: RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>,
    ip6_56: RateLimiter<IP6_56, DefaultKeyedStateStore<IP6_56>, DefaultClock>,
    ip6_48: RateLimiter<IP6_48, DefaultKeyedStateStore<IP6_48>, DefaultClock>,
}

impl IpLimiters {
    pub fn new(quota: Quota) -> Self {
        Self {
            per_ip: RateLimiter::keyed(quota),
            ip6_56: RateLimiter::keyed(scale_quota(quota, 8).unwrap()),
            ip6_48: RateLimiter::keyed(scale_quota(quota, 256).unwrap()),
        }
    }
    pub fn check_key(&self, ip: IpAddr) -> Result<(), Duration> {
        let asdf = |n: NotUntil<_>| n.wait_time_from(CLOCK.now());
        match ip {
            addr @ IpAddr::V4(_) => self.per_ip.check_key(&addr).map_err(asdf),
            IpAddr::V6(a) => {
                // always check all limiters
                let check_ip = self
                    .per_ip
                    .check_key(&IpAddr::V6(a & IP6_64_MASK))
                    .map_err(asdf);
                let check_56 = self
                    .ip6_56
                    .check_key(a.octets()[..7].try_into().unwrap())
                    .map_err(asdf);
                let check_48 = self
                    .ip6_48
                    .check_key(a.octets()[..6].try_into().unwrap())
                    .map_err(asdf);
                check_ip.and(check_56).and(check_48)
            }
        }
    }
}

/// Once the rate limit has been reached, the middleware will respond with
/// status code 429 (too many requests) and a `Retry-After` header with the amount
/// of time that needs to pass before another request will be allowed.
#[derive(Debug)]
pub struct GovernorMiddleware {
    #[allow(dead_code)]
    stop_on_drop: oneshot::Sender<()>,
    limiters: Arc<IpLimiters>,
}

impl GovernorMiddleware {
    /// Limit request rates
    ///
    /// a little gross but this spawns a tokio task for housekeeping:
    /// https://docs.rs/governor/latest/governor/struct.RateLimiter.html#keyed-rate-limiters---housekeeping
    pub fn new(quota: Quota) -> Self {
        let limiters = Arc::new(IpLimiters::new(quota));
        let (stop_on_drop, mut stopped) = oneshot::channel();
        tokio::task::spawn({
            let limiters = limiters.clone();
            async move {
                loop {
                    tokio::select! {
                        _ = &mut stopped => break,
                        _ = tokio::time::sleep(Duration::from_secs(60)) => {},
                    };
                    log::debug!(
                        "limiter sizes before housekeeping: {}/ip {}/v6_56 {}/v6_48",
                        limiters.per_ip.len(),
                        limiters.ip6_56.len(),
                        limiters.ip6_48.len(),
                    );
                    limiters.per_ip.retain_recent();
                    limiters.ip6_56.retain_recent();
                    limiters.ip6_48.retain_recent();
                }
            }
        });
        Self {
            stop_on_drop,
            limiters,
        }
    }
}

impl<E: Endpoint> Middleware<E> for GovernorMiddleware {
    type Output = GovernorMiddlewareImpl<E>;
    fn transform(&self, ep: E) -> Self::Output {
        GovernorMiddlewareImpl {
            ep,
            limiters: self.limiters.clone(),
        }
    }
}

pub struct GovernorMiddlewareImpl<E> {
    ep: E,
    limiters: Arc<IpLimiters>,
}

impl<E: Endpoint> Endpoint for GovernorMiddlewareImpl<E> {
    type Output = E::Output;

    async fn call(&self, req: Request) -> Result<Self::Output> {
        let remote = req
            .remote_addr()
            .as_socket_addr()
            .unwrap_or_else(|| panic!("failed to get request's remote addr")) // TODO
            .ip();

        log::trace!("remote: {remote}");

        match self.limiters.check_key(remote) {
            Ok(_) => {
                log::debug!("allowing remote {remote}");
                self.ep.call(req).await
            }
            Err(d) => {
                let wait_time = d.as_secs();

                log::debug!("rate limit exceeded for {remote}, quota reset in {wait_time}s");

                let res = Response::builder()
                    .status(StatusCode::TOO_MANY_REQUESTS)
                    .header("x-ratelimit-after", wait_time)
                    .header("retry-after", wait_time)
                    .body(booo());
                Err(poem::Error::from_response(res))
            }
        }
    }
}

fn booo() -> String {
    format!(
        r#"{}

You're going a bit too fast.

Tip: check out the `x-ratelimit-after` response header.
"#,
        logo("mirror 429")
    )
}
