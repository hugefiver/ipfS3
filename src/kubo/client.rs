#[derive(Clone)]
pub struct KuboClient {
    #[allow(dead_code)]
    base_url: std::sync::Arc<str>,
    http: reqwest::Client,
}

impl KuboClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(300))
                .build()
                .expect("failed to build reqwest client"),
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }
}
