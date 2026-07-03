use bytes::Bytes;
use futures_util::{Stream, StreamExt};

use super::client::KuboClient;

pub async fn stream_cat(
    kubo: &KuboClient,
    cid: &str,
    range: Option<(u64, u64)>,
) -> Result<
    impl Stream<Item = Result<Bytes, std::io::Error>>,
    Box<dyn std::error::Error + Send + Sync>,
> {
    let url = if let Some((start, end)) = range {
        format!(
            "{}/api/v0/cat?arg={cid}&bytes={start}-{}",
            kubo.base_url(),
            end.saturating_sub(1)
        )
    } else {
        format!("{}/api/v0/cat?arg={cid}", kubo.base_url())
    };

    let resp = kubo.http().post(&url).send().await?;
    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("cat failed: {text}").into());
    }

    let stream = resp
        .bytes_stream()
        .map(|r| r.map_err(std::io::Error::other));
    Ok(stream)
}

#[allow(dead_code)]
pub async fn cat_to_vec(
    kubo: &KuboClient,
    cid: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let stream = stream_cat(kubo, cid, None).await?;
    tokio::pin!(stream);
    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_stream_cat_returns_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello world"))
            .mount(&server)
            .await;

        let client = KuboClient::new(server.uri());
        let result = cat_to_vec(&client, "QmTest").await.unwrap();
        assert_eq!(result, b"hello world");
    }
}
