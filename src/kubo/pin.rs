use super::client::KuboClient;

pub async fn pin_add(
    kubo: &KuboClient,
    cid: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/api/v0/pin/add?arg={cid}", kubo.base_url());
    let resp = kubo.http().post(&url).send().await?;
    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("pin add failed: {text}").into());
    }
    Ok(())
}

pub async fn pin_rm(
    kubo: &KuboClient,
    cid: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!("{}/api/v0/pin/rm?arg={cid}", kubo.base_url());
    let resp = kubo.http().post(&url).send().await?;
    if !resp.status().is_success() {
        let text = resp.text().await.unwrap_or_default();
        return Err(format!("pin rm failed: {text}").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn test_pin_add_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmTest\"]}"))
            .mount(&server)
            .await;

        let client = KuboClient::new(server.uri());
        let result = pin_add(&client, "QmTest").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_pin_rm_success() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/rm"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[\"QmTest\"]}"))
            .mount(&server)
            .await;

        let client = KuboClient::new(server.uri());
        let result = pin_rm(&client, "QmTest").await;
        assert!(result.is_ok());
    }
}
