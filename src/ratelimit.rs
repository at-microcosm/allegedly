use crate::logo;

use governor::{
    Quota, RateLimiter,
    clock::{Clock, DefaultClock},
    state::keyed::DefaultKeyedStateStore,
};
use poem::{Endpoint, Middleware, Request, Response, Result, http::StatusCode};
use std::{
    convert::TryInto, error::Error, net::IpAddr, num::NonZeroU32, sync::Arc, sync::LazyLock,
    time::Duration,
};

static CLOCK: LazyLock<DefaultClock> = LazyLock::new(DefaultClock::default);

/// Once the rate limit has been reached, the middleware will respond with
/// status code 429 (too many requests) and a `Retry-After` header with the amount
/// of time that needs to pass before another request will be allowed.
#[derive(Debug, Clone)]
pub struct GovernorMiddleware {
    limiter: Arc<RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>>,
}

impl GovernorMiddleware {
    /// Constructs a rate-limiting middleware from a [`Duration`] that allows one request in the given time interval.
    ///
    /// If the time interval is zero, returns `None`.
    #[must_use]
    pub fn with_period(duration: Duration) -> Option<Self> {
        Some(Self {
            limiter: Arc::new(RateLimiter::<IpAddr, _, _>::keyed(Quota::with_period(
                duration,
            )?)),
        })
    }

    /// Constructs a rate-limiting middleware that allows a specified number of requests every second.
    ///
    /// Returns an error if `times` can't be converted into a [`NonZeroU32`].
    pub fn per_second<T>(times: T) -> Result<Self>
    where
        T: TryInto<NonZeroU32>,
        T::Error: Error + Send + Sync + 'static,
    {
        Ok(Self {
            limiter: Arc::new(RateLimiter::<IpAddr, _, _>::keyed(Quota::per_second(
                times.try_into().unwrap(), // TODO
            ))),
        })
    }

    /// Constructs a rate-limiting middleware that allows a specified number of requests every minute.
    ///
    /// Returns an error if `times` can't be converted into a [`NonZeroU32`].
    pub fn per_minute<T>(times: T) -> Result<Self>
    where
        T: TryInto<NonZeroU32>,
        T::Error: Error + Send + Sync + 'static,
    {
        Ok(Self {
            limiter: Arc::new(RateLimiter::<IpAddr, _, _>::keyed(Quota::per_minute(
                times.try_into().unwrap(), // TODO
            ))),
        })
    }

    /// Constructs a rate-limiting middleware that allows a specified number of requests every hour.
    ///
    /// Returns an error if `times` can't be converted into a [`NonZeroU32`].
    pub fn per_hour<T>(times: T) -> Result<Self>
    where
        T: TryInto<NonZeroU32>,
        T::Error: Error + Send + Sync + 'static,
    {
        Ok(Self {
            limiter: Arc::new(RateLimiter::<IpAddr, _, _>::keyed(Quota::per_hour(
                times.try_into().unwrap(), // TODO
            ))),
        })
    }
}

impl<E: Endpoint> Middleware<E> for GovernorMiddleware {
    type Output = GovernorMiddlewareImpl<E>;
    fn transform(&self, ep: E) -> Self::Output {
        GovernorMiddlewareImpl {
            ep,
            limiter: self.limiter.clone(),
        }
    }
}

pub struct GovernorMiddlewareImpl<E> {
    ep: E,
    limiter: Arc<RateLimiter<IpAddr, DefaultKeyedStateStore<IpAddr>, DefaultClock>>,
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

        match self.limiter.check_key(&remote) {
            Ok(_) => {
                log::debug!("allowing remote {remote}");
                self.ep.call(req).await
            }
            Err(negative) => {
                let wait_time = negative.wait_time_from(CLOCK.now()).as_secs();

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
