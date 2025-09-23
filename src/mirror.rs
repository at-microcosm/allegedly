use crate::logo;
use poem::{
    EndpointExt, Error, IntoResponse, Request, Response, Result, Route, Server, get, handler,
    http::{StatusCode, Uri},
    listener::TcpListener,
    middleware::{AddData, CatchPanic, Compression, Cors, Tracing},
    web::Data,
};
use reqwest::{Client, Url};
use std::net::SocketAddr;
use std::time::Duration;

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
    let mut res = Response::default();
    upstream_res.headers().iter().for_each(|(k, v)| {
        res.headers_mut().insert(k, v.to_owned());
    });
    res.set_status(upstream_res.status());
    res.set_version(upstream_res.version());
    res.set_body(upstream_res.bytes().await.unwrap());
    Ok(res)
}

#[handler]
async fn nope(uri: &Uri) -> Result<impl IntoResponse> {
    log::info!("ha nope, {uri:?}");
    Ok(())
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
        .with(CatchPanic::new())
        .with(Tracing);

    Server::new(TcpListener::bind(bind)).run(app).await
}
