use crate::{GovernorMiddleware, logo};
use futures::TryStreamExt;
use governor::Quota;
use poem::{
    EndpointExt, Error, IntoResponse, Request, Response, Result, Route, Server, get, handler,
    http::StatusCode,
    listener::TcpListener,
    middleware::{AddData, CatchPanic, Compression, Cors, Tracing},
    web::Data,
};
use reqwest::{Client, Url};
use std::{net::SocketAddr, time::Duration};

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

#[handler]
async fn proxy(req: &Request, Data(state): Data<&State>) -> Result<impl IntoResponse> {
    let mut target = state.plc.clone();
    target.set_path(req.uri().path());
    let upstream_res = state
        .client
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
    let wrapped_req_client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();

    let state = State {
        client: wrapped_req_client,
        plc,
        upstream: upstream.clone(),
    };

    let app = Route::new()
        .at("/", get(hello))
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
