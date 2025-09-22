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
}

#[handler]
fn hello() -> String {
    logo("mirror")
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
            Error::from_string("request to plc server failed", StatusCode::BAD_GATEWAY)
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

pub async fn serve(plc: Url, bind: SocketAddr) -> std::io::Result<()> {
    let client = Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let state = State { client, plc };

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
