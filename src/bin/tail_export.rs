use allegedly::OpPeek;
use url::Url;

async fn get_page(client: &reqwest::Client, url: Url) -> Vec<String> {
    client
        .get(url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap()
        .trim()
        .split('\n')
        .map(Into::into)
        .collect()
}

#[tokio::main]
async fn main() {
    let client = reqwest::Client::builder()
        .user_agent(concat!(
            "allegedly (export) v",
            env!("CARGO_PKG_VERSION"),
            " (from @microcosm.blue; contact @bad-example.com)"
        ))
        .build()
        .unwrap();

    let mut url = Url::parse("https://plc.directory/export").unwrap();
    let ops = get_page(&client, url.clone()).await;

    println!("first: {:?}", ops.first());

    if let Some(last_line) = ops.last() {
        let x: OpPeek = serde_json::from_str(last_line).unwrap();
        url.query_pairs_mut()
            .append_pair("after", &x.created_at.to_rfc3339());
        let ops2 = get_page(&client, url).await;
        println!("2nd: {:?}", ops2.first());
    }
}
