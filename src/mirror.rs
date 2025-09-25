use crate::{GovernorMiddleware, UA, logo};
use futures::TryStreamExt;
use governor::Quota;
use poem::{
    Endpoint, EndpointExt, Error, IntoResponse, Request, Response, Result, Route, Server, get,
    handler,
    http::StatusCode,
    listener::{Listener, TcpListener, acme::AutoCert},
    middleware::{AddData, CatchPanic, Compression, Cors, Tracing},
    web::{Data, Json},
};
use reqwest::{Client, Url};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Debug, Clone)]
struct State {
    client: Client,
    plc: Url,
    upstream: Url,
}

#[handler]
fn hello(Data(State { upstream, .. }): Data<&State>) -> String {
    format!(
        r#"{}

This is a PLC[1] mirror running Allegedly[2] in mirror mode. Allegedly synchronizes and proxies to a downstream PLC reference server instance[3] (why?[4]).


Configured upstream:

    {upstream}


Available APIs:

    - All PLC GET requests [5].
    - Rejects POSTs. This is a mirror.

    try `GET /{{did}}` to resolve an identity


[1] https://web.plc.directory
[2] https://tangled.org/@microcosm.blue/Allegedly
[3] https://github.com/did-method-plc/did-method-plc
[4] https://updates.microcosm.blue/3lz7nwvh4zc2u
[5] https://web.plc.directory/api/redoc

"#,
        logo("mirror")
    )
}

fn failed_to_reach_wrapped() -> String {
    format!(
        r#"{}

Failed to reach the wrapped reference PLC server. Sorry.
"#,
        logo("mirror 502 :( ")
    )
}

async fn plc_status(url: &Url, client: &Client) -> (bool, serde_json::Value) {
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

#[handler]
async fn health(
    Data(State {
        plc,
        client,
        upstream,
    }): Data<&State>,
) -> impl IntoResponse {
    let mut overall_status = StatusCode::OK;
    let (ok, wrapped_status) = plc_status(plc, client).await;
    if !ok {
        overall_status = StatusCode::BAD_GATEWAY;
    }
    let (ok, upstream_status) = plc_status(upstream, client).await;
    if !ok {
        overall_status = StatusCode::BAD_GATEWAY;
    }
    (
        overall_status,
        Json(serde_json::json!({
            "server": "allegedly (mirror)",
            "version": env!("CARGO_PKG_VERSION"),
            "wrapped_plc": wrapped_status,
            "upstream_plc": upstream_status,
        })),
    )
}

#[handler]
async fn proxy(req: &Request, Data(state): Data<&State>) -> Result<impl IntoResponse> {
    let mut target = state.plc.clone();
    target.set_path(req.uri().path());
    let upstream_res = state
        .client
        .get(target)
        .timeout(Duration::from_secs(3)) // should be low latency to wrapped server
        .headers(req.headers().clone())
        .send()
        .await
        .map_err(|e| {
            log::error!("upstream req fail: {e}");
            Error::from_string(failed_to_reach_wrapped(), StatusCode::BAD_GATEWAY)
        })?;

    let http_res: poem::http::Response<reqwest::Body> = upstream_res.into();
    let (parts, reqw_body) = http_res.into_parts();

    let parts = poem::ResponseParts {
        status: parts.status,
        version: parts.version,
        headers: parts.headers,
        extensions: parts.extensions,
    };

    let body = http_body_util::BodyDataStream::new(reqw_body)
        .map_err(|e| std::io::Error::other(Box::new(e)));

    Ok(Response::from_parts(
        parts,
        poem::Body::from_bytes_stream(body),
    ))
}

#[handler]
async fn nope(Data(State { upstream, .. }): Data<&State>) -> (StatusCode, String) {
    (
        StatusCode::BAD_REQUEST,
        format!(
            r#"{}

Sorry, this server does not accept POST requests.

You may wish to try upstream: {upstream}
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
    },
    Bind(SocketAddr),
}

pub async fn serve(upstream: &Url, plc: Url, listen: ListenConf) -> std::io::Result<()> {
    // not using crate CLIENT: don't want the retries etc
    let client = Client::builder()
        .user_agent(UA)
        .timeout(Duration::from_secs(10)) // fallback
        .build()
        .expect("reqwest client to build");

    let state = State {
        client,
        plc,
        upstream: upstream.clone(),
    };

    let app = Route::new()
        .at("/", get(hello))
        .at("/_health", get(health))
        .at("/:any", get(proxy).post(nope))
        .with(AddData::new(state))
        .with(Cors::new().allow_credentials(false))
        .with(Compression::new())
        .with(GovernorMiddleware::new(Quota::per_minute(
            3000.try_into().expect("ratelimit middleware to build"),
        )))
        .with(CatchPanic::new())
        .with(Tracing);

    match listen {
        ListenConf::Acme {
            domains,
            cache_path,
            directory_url,
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

            run_insecure_notice();
            run(app, TcpListener::bind("0.0.0.0:443").acme(auto_cert)).await
        }
        ListenConf::Bind(addr) => run(app, TcpListener::bind(addr)).await,
    }
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
fn run_insecure_notice() {
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

    let app = Route::new().at("/", get(oop_plz_be_secure)).with(Tracing);
    let listener = TcpListener::bind("0.0.0.0:80");
    tokio::task::spawn(async move {
        Server::new(listener)
            .name("allegedly (mirror:80 helper)")
            .run(app)
            .await
    });
}
