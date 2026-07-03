use bytes::Bytes;
use futures_util::{Stream, TryStreamExt};
use reqwest::Body as ReqwestBody;
use reqwest::multipart;
use serde::Deserialize;

use super::client::KuboClient;

#[derive(Debug, Deserialize)]
struct AddResponse {
    #[serde(rename = "Hash")]
    pub hash: String,
    #[serde(rename = "Size", default)]
    #[allow(dead_code)]
    pub size: String,
}

pub async fn stream_add<S, E>(
    kubo: &KuboClient,
    stream: S,
    cid_version: u8,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>>
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
{
    // Kubo /api/v0/add requires multipart/form-data with a file part.
    let mapped = stream.map_err(|e| {
        let boxed: Box<dyn std::error::Error + Send + Sync> = e.into();
        boxed
    });
    let body = ReqwestBody::wrap_stream(mapped);

    let part = multipart::Part::stream(body)
        .file_name("object")
        .mime_str("application/octet-stream")?;
    let form = multipart::Form::new().part("file", part);

    let url = format!(
        "{}/api/v0/add?cid-version={cid_version}&pin=false&wrap-with-directory=false",
        kubo.base_url()
    );

    let resp = kubo.http().post(&url).multipart(form).send().await?;
    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("add failed: {text}").into());
    }

    let text = resp.text().await?;
    let mut last_hash: Option<String> = None;
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: AddResponse =
            serde_json::from_str(line).map_err(|e| format!("parse add response: {e}"))?;
        last_hash = Some(parsed.hash);
    }
    last_hash.ok_or_else(|| "empty add response".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::stream;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_stream_add_parses_cid() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmRoot\",\"Size\":\"100\"}\n"),
            )
            .mount(&server)
            .await;

        let client = KuboClient::new(server.uri());
        let data: Vec<Result<Bytes, std::io::Error>> = vec![Ok(Bytes::from("hello world"))];
        let s = stream::iter(data);

        let result = stream_add(&client, s, 1).await.unwrap();
        assert_eq!(result, "QmRoot");
    }

    #[tokio::test]
    async fn test_stream_add_error_on_non_200() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let client = KuboClient::new(server.uri());
        let data: Vec<Result<Bytes, std::io::Error>> = vec![Ok(Bytes::from("hello world"))];
        let s = stream::iter(data);

        let result = stream_add(&client, s, 1).await;
        assert!(result.is_err());
    }
}
