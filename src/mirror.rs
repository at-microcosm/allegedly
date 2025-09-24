use crate::{GovernorMiddleware, UA, logo};
use futures::TryStreamExt;
use governor::Quota;
use poem::{
    EndpointExt, Error, IntoResponse, Request, Response, Result, Route, Server, get, handler,
    http::StatusCode,
    listener::TcpListener,
    middleware::{AddData, CatchPanic, Compression, Cors, Tracing},
    web::{Data, Json},
};
use reqwest::{Client, Url};
use std::{net::SocketAddr, time::Duration};

#[derive(Debug, Clone)]
struct State {
    upstream_client: Client,
    wrapped_client: Client,
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

    let Ok(response) = client.get(url).send().await else {
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
        wrapped_client,
        upstream,
        upstream_client,
    }): Data<&State>,
) -> impl IntoResponse {
    let mut overall_status = StatusCode::OK;
    let (ok, wrapped_status) = plc_status(plc, wrapped_client).await;
    if !ok {
        overall_status = StatusCode::BAD_GATEWAY;
    }
    let (ok, upstream_status) = plc_status(upstream, upstream_client).await;
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
        .upstream_client
        .get(target)
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

pub async fn serve(upstream: &Url, plc: Url, bind: SocketAddr) -> std::io::Result<()> {
    let wrapped_client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let upstream_client = Client::builder()
        .user_agent(UA)
        .timeout(Duration::from_secs(6))
        .build()
        .unwrap();

    let state = State {
        wrapped_client,
        upstream_client,
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
            3000.try_into().unwrap(),
        )))
        .with(CatchPanic::new())
        .with(Tracing);

    Server::new(TcpListener::bind(bind)).run(app).await
}
