use crate::{
    CachedValue, CreatePlcOpLimiter, Db, Dt, Fetcher, GovernorMiddleware, IpLimiters, UA, logo,
};
use futures::TryStreamExt;
use governor::Quota;
use poem::{
    Body, Endpoint, EndpointExt, Error, IntoResponse, Request, Response, Result, Route, Server,
    get, handler,
    http::{StatusCode, header::HOST, header::USER_AGENT},
    listener::{Listener, TcpListener, acme::AutoCert},
    middleware::{AddData, CatchPanic, Compression, Cors, Tracing},
    web::{Data, Json, Path},
};
use reqwest::{Client, Url};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Clone)]
struct State {
    client: Client,
    plc: Url,
    upstream: Url,
    latest_at: CachedValue<Dt, GetLatestAt>,
    upstream_status: CachedValue<PlcStatus, CheckUpstream>,
    experimental: ExperimentalConf,
}

#[handler]
fn hello(Data(State { upstream, .. }): Data<&State>) -> String {
    format!(
        r#"{}

This is a PLC[1] mirror running Allegedly in mirror mode. Mirror mode wraps and
synchronizes a local PLC reference server instance[2] (why?[3]).


Configured upstream:

    {upstream}


Available APIs:

    - GET  /_health  Health and version info

    - GET  /*        Proxies to wrapped server; see PLC API docs:
                     https://web.plc.directory/api/redoc

    - POST /*        Always rejected. This is a mirror.


    tip: try `GET /{{did}}` to resolve an identity


Allegedly is a suite of open-source CLI tools from for working with PLC logs,
from microcosm:

    https://tangled.org/@microcosm.blue/Allegedly

    https://microcosm.blue


[1] https://web.plc.directory
[2] https://github.com/did-method-plc/did-method-plc
[3] https://updates.microcosm.blue/3lz7nwvh4zc2u
"#,
        logo("mirror")
    )
}

#[handler]
fn favicon() -> impl IntoResponse {
    include_bytes!("../favicon.ico").with_content_type("image/x-icon")
}

fn failed_to_reach_named(name: &str) -> String {
    format!(
        r#"{}

Failed to reach the {name} server. Sorry.
"#,
        logo("mirror 502 :( ")
    )
}

fn bad_create_op(reason: &str) -> Response {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(format!(
            r#"{}

NooOOOooooo: {reason}
"#,
            logo("mirror 400 >:( ")
        ))
}

type PlcStatus = (bool, serde_json::Value);

async fn plc_status(url: &Url, client: &Client) -> PlcStatus {
    use serde_json::json;

    let mut url = url.clone();
    url.set_path("/_health");

    let Ok(response) = client.get(url).timeout(Duration::from_secs(3)).send().await else {
        return (false, json!({"error": "cannot reach plc server"}));
    };

    let status = response.status();

    let Ok(text) = response.text().await else {
        return (false, json!({"error": "failed to read response body"}));
    };

    let body = match serde_json::from_str(&text) {
        Ok(json) => json,
        Err(_) => serde_json::Value::String(text.to_string()),
    };

    if status.is_success() {
        (true, body)
    } else {
        (
            false,
            json!({
                "error": "non-ok status",
                "status": status.as_str(),
                "status_code": status.as_u16(),
                "response": body,
            }),
        )
    }
}

#[derive(Clone)]
struct GetLatestAt(Db);
impl Fetcher<Dt> for GetLatestAt {
    async fn fetch(&self) -> Result<Dt, Box<dyn std::error::Error>> {
        let now = self.0.get_latest().await?.ok_or(anyhow::anyhow!(
            "expected to find at least one thing in the db"
        ))?;
        Ok(now)
    }
}

#[derive(Clone)]
struct CheckUpstream(Url, Client);
impl Fetcher<PlcStatus> for CheckUpstream {
    async fn fetch(&self) -> Result<PlcStatus, Box<dyn std::error::Error>> {
        Ok(plc_status(&self.0, &self.1).await)
    }
}

#[handler]
async fn health(
    Data(State {
        plc,
        client,
        latest_at,
        upstream_status,
        ..
    }): Data<&State>,
) -> impl IntoResponse {
    let mut overall_status = StatusCode::OK;
    let (ok, wrapped_status) = plc_status(plc, client).await;
    if !ok {
        overall_status = StatusCode::BAD_GATEWAY;
    }
    let (ok, upstream_status) = upstream_status.get().await.expect("plc_status infallible");
    if !ok {
        overall_status = StatusCode::BAD_GATEWAY;
    }
    let latest = latest_at.get().await.ok();
    (
        overall_status,
        Json(serde_json::json!({
            "server": "allegedly (mirror)",
            "version": env!("CARGO_PKG_VERSION"),
            "wrapped_plc": wrapped_status,
            "upstream_plc": upstream_status,
            "latest_at": latest,
        })),
    )
}

fn proxy_response(res: reqwest::Response) -> Response {
    let http_res: poem::http::Response<reqwest::Body> = res.into();
    let (parts, reqw_body) = http_res.into_parts();

    let parts = poem::ResponseParts {
        status: parts.status,
        version: parts.version,
        headers: parts.headers,
        extensions: parts.extensions,
    };

    let body = http_body_util::BodyDataStream::new(reqw_body)
        .map_err(|e| std::io::Error::other(Box::new(e)));

    Response::from_parts(parts, poem::Body::from_bytes_stream(body))
}

#[handler]
async fn proxy(req: &Request, Data(state): Data<&State>) -> Result<Response> {
    let mut target = state.plc.clone();
    target.set_path(req.uri().path());
    let wrapped_res = state
        .client
        .get(target)
        .timeout(Duration::from_secs(3)) // should be low latency to wrapped server
        .headers(req.headers().clone())
        .send()
        .await
        .map_err(|e| {
            log::error!("upstream req fail: {e}");
            Error::from_string(
                failed_to_reach_named("wrapped reference PLC"),
                StatusCode::BAD_GATEWAY,
            )
        })?;

    Ok(proxy_response(wrapped_res))
}

#[handler]
async fn forward_create_op_upstream(
    Data(State {
        upstream,
        client,
        experimental,
        ..
    }): Data<&State>,
    Path(did): Path<String>,
    req: &Request,
    body: Body,
) -> Result<Response> {
    if let Some(expected_domain) = &experimental.acme_domain {
        let Some(found_host) = req.header(HOST) else {
            log::debug!(
                "expected experimental domain but missing host header. {:?}; {:?}",
                req.header(HOST),
                req.headers()
            );
            log::debug!("does it get put into uri??? {:?}", req.uri());
            return Ok(bad_create_op(&format!(
                "missing `Host` header, expected {expected_domain:?} for experimental requests."
            )));
        };
        if found_host != expected_domain {
            return Ok(bad_create_op(&format!(
                "experimental requests must be made to {expected_domain:?}, but this request's `Host` header was {found_host}"
            )));
        }
    }

    // adjust proxied headers
    let mut headers: reqwest::header::HeaderMap = req.headers().clone();
    log::trace!("original request headers: {headers:?}");
    headers.insert("Host", upstream.host_str().unwrap().parse().unwrap());
    let client_ua = headers
        .get(USER_AGENT)
        .map(|h| h.to_str().unwrap())
        .unwrap_or("unknown");
    headers.insert(
        USER_AGENT,
        format!("{UA} (forwarding from {client_ua:?})")
            .parse()
            .unwrap(),
    );
    log::trace!("adjusted request headers: {headers:?}");

    let mut target = upstream.clone();
    target.set_path(&did);
    let upstream_res = client
        .post(target)
        .timeout(Duration::from_secs(15)) // be a little generous
        .headers(headers)
        .body(reqwest::Body::wrap_stream(body.into_bytes_stream()))
        .send()
        .await
        .map_err(|e| {
            log::warn!("upstream write fail: {e}");
            Error::from_string(
                failed_to_reach_named("upstream PLC"),
                StatusCode::BAD_GATEWAY,
            )
        })?;

    Ok(proxy_response(upstream_res))
}

#[handler]
async fn nope(Data(State { upstream, .. }): Data<&State>) -> (StatusCode, String) {
    (
        StatusCode::BAD_REQUEST,
        format!(
            r#"{}

Sorry, this server does not accept POST requests.

You may wish to try sending that to our upstream: {upstream}.

If you operate this server, try running with `--experimental-write-upstream`.
"#,
            logo("mirror (nope)")
        ),
    )
}

#[derive(Debug)]
pub enum ListenConf {
    Acme {
        domains: Vec<String>,
        cache_path: PathBuf,
        directory_url: String,
        ipv6: bool,
    },
    Bind(SocketAddr),
}

#[derive(Debug, Clone)]
pub struct ExperimentalConf {
    pub acme_domain: Option<String>,
    pub write_upstream: bool,
}

pub async fn serve(
    upstream: Url,
    plc: Url,
    listen: ListenConf,
    experimental: ExperimentalConf,
    db: Db,
) -> anyhow::Result<&'static str> {
    log::info!("starting server...");

    // not using crate CLIENT: don't want the retries etc
    let client = Client::builder()
        .user_agent(UA)
        .timeout(Duration::from_secs(10)) // fallback
        .build()
        .expect("reqwest client to build");

    let latest_at = CachedValue::new(GetLatestAt(db), Duration::from_secs(2));
    let upstream_status = CachedValue::new(
        CheckUpstream(upstream.clone(), client.clone()),
        Duration::from_secs(6),
    );

    let state = State {
        client,
        plc,
        upstream: upstream.clone(),
        latest_at,
        upstream_status,
        experimental: experimental.clone(),
    };

    let mut app = Route::new()
        .at("/", get(hello))
        .at("/favicon.ico", get(favicon))
        .at("/_health", get(health));

    if experimental.write_upstream {
        log::info!("enabling experimental write forwarding to upstream");

        let ip_limiter = IpLimiters::new(Quota::per_hour(10.try_into().unwrap()));
        let did_limiter = CreatePlcOpLimiter::new(Quota::per_hour(4.try_into().unwrap()));

        let upstream_proxier = forward_create_op_upstream
            .with(GovernorMiddleware::new(did_limiter))
            .with(GovernorMiddleware::new(ip_limiter));

        app = app.at("/:any", get(proxy).post(upstream_proxier));
    } else {
        app = app.at("/:any", get(proxy).post(nope));
    }

    let app = app
        .with(AddData::new(state))
        .with(Cors::new().allow_credentials(false))
        .with(Compression::new())
        .with(GovernorMiddleware::new(IpLimiters::new(Quota::per_minute(
            3000.try_into().expect("ratelimit middleware to build"),
        ))))
        .with(CatchPanic::new())
        .with(Tracing);

    match listen {
        ListenConf::Acme {
            domains,
            cache_path,
            directory_url,
            ipv6,
        } => {
            rustls::crypto::aws_lc_rs::default_provider()
                .install_default()
                .expect("crypto provider to be installable");

            let mut auto_cert = AutoCert::builder()
                .directory_url(directory_url)
                .cache_path(cache_path);
            for domain in domains {
                auto_cert = auto_cert.domain(domain);
            }
            let auto_cert = auto_cert.build().expect("acme config to build");

            log::trace!("auto_cert: {auto_cert:?}");

            let notice_task = tokio::task::spawn(run_insecure_notice(ipv6));
            let listener = TcpListener::bind(if ipv6 { "[::]:443" } else { "0.0.0.0:443" });
            let app_res = run(app, listener.acme(auto_cert)).await;
            log::warn!("server task ended, aborting insecure server task...");
            notice_task.abort();
            app_res?;
            notice_task.await??;
        }
        ListenConf::Bind(addr) => run(app, TcpListener::bind(addr)).await?,
    }

    Ok("server (uh oh?)")
}

async fn run<A, L>(app: A, listener: L) -> std::io::Result<()>
where
    A: Endpoint + 'static,
    L: Listener + 'static,
{
    Server::new(listener)
        .name("allegedly (mirror)")
        .run(app)
        .await
}

/// kick off a tiny little server on a tokio task to tell people to use 443
async fn run_insecure_notice(ipv6: bool) -> Result<(), std::io::Error> {
    #[handler]
    fn oop_plz_be_secure() -> (StatusCode, String) {
        (
            StatusCode::BAD_REQUEST,
            format!(
                r#"{}

You probably want to change your request to use HTTPS instead of HTTP.
"#,
                logo("mirror (tls on 443 please)")
            ),
        )
    }

    let app = Route::new()
        .at("/", get(oop_plz_be_secure))
        .at("/favicon.ico", get(favicon))
        .with(Tracing);
    Server::new(TcpListener::bind(if ipv6 {
        "[::]:80"
    } else {
        "0.0.0.0:80"
    }))
    .name("allegedly (mirror:80 helper)")
    .run(app)
    .await
}
