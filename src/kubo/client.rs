const CONTROL_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
/// Bounds the gap between received response body chunks without capping total
/// duration. Only meaningful for downloads: see [`KuboClient::upload_http`].
const STREAM_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

#[derive(Clone)]
pub struct KuboClient {
    #[allow(dead_code)]
    base_url: std::sync::Arc<str>,
    http: reqwest::Client,
    upload_http: reqwest::Client,
    download_http: reqwest::Client,
    stream_idle_timeout: std::time::Duration,
}

impl KuboClient {
    pub fn new(base_url: String) -> Self {
        Self::new_with_request_timeout(base_url, CONTROL_REQUEST_TIMEOUT)
    }

    /// Build a client whose bounded control-plane requests use `request_timeout`.
    ///
    /// Neither streaming client is bounded by a whole-request deadline, because
    /// `add` and `cat` carry object payloads of arbitrary size. They differ in
    /// their post-connect liveness policy; see [`Self::upload_http`] and
    /// [`Self::download_http`].
    pub fn new_with_request_timeout(
        base_url: String,
        request_timeout: std::time::Duration,
    ) -> Self {
        Self::new_with_timeouts(base_url, request_timeout, STREAM_IDLE_TIMEOUT)
    }

    /// As [`Self::new_with_request_timeout`], but with an injectable download
    /// idle bound so tests can prove a stalled download aborts — and that the
    /// upload client is *not* subject to that bound.
    pub fn new_with_timeouts(
        base_url: String,
        request_timeout: std::time::Duration,
        download_idle_timeout: std::time::Duration,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .timeout(request_timeout)
                .build()
                .expect("failed to build reqwest client"),
            upload_http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .expect("failed to build reqwest upload client"),
            download_http: reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .read_timeout(download_idle_timeout)
                .build()
                .expect("failed to build reqwest download client"),
            stream_idle_timeout: download_idle_timeout,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Bounded client for short control-plane calls such as `pin/add` and `pin/rm`.
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Client for uploading payloads (`add`): connection setup is bounded, and
    /// nothing else is.
    ///
    /// It deliberately carries **no** post-connect liveness bound — neither a
    /// whole-request `timeout` nor a `read_timeout`. `read_timeout` cannot be
    /// used here: in reqwest 0.13 it is a one-shot deadline during the pending
    /// phase (armed at dispatch, never reset until response headers arrive), so
    /// it would abort an upload that is making continuous progress. Callers of
    /// Kubo's `progress=true` response streams enforce the configured idle bound
    /// while incrementally parsing those response frames.
    pub fn upload_http(&self) -> &reqwest::Client {
        &self.upload_http
    }

    /// Client for downloading payloads (`cat`): no whole-request deadline, but
    /// connection setup and inter-chunk idle time are bounded.
    ///
    /// `read_timeout` is a genuine inter-chunk bound here: the request itself is
    /// small, so the pending phase is short, and once the response body starts
    /// arriving reqwest resets the deadline on every received frame.
    pub fn download_http(&self) -> &reqwest::Client {
        &self.download_http
    }

    /// Inter-frame liveness bound for Kubo RPC response streams.
    pub(crate) fn stream_idle_timeout(&self) -> std::time::Duration {
        self.stream_idle_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::KuboClient;
    use futures_util::StreamExt;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn neither_streaming_client_has_a_whole_request_deadline() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/slow"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("done")
                    .set_delay(Duration::from_millis(300)),
            )
            .mount(&server)
            .await;

        let client = KuboClient::new_with_request_timeout(server.uri(), Duration::from_millis(20));
        let url = format!("{}/slow", client.base_url());

        let bounded = client.http().post(&url).send().await;
        assert!(
            bounded.is_err(),
            "the bounded control-plane client must time out on a slow response"
        );

        let uploaded = client
            .upload_http()
            .post(&url)
            .send()
            .await
            .expect("the upload client must not impose a whole-request deadline");
        assert_eq!(uploaded.status().as_u16(), 200);

        let downloaded = client
            .download_http()
            .post(&url)
            .send()
            .await
            .expect("the download client must not impose a whole-request deadline");
        assert_eq!(downloaded.status().as_u16(), 200);
    }

    #[tokio::test]
    async fn the_download_client_bounds_inter_chunk_idle_time() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                socket.read_exact(&mut byte).await.unwrap();
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nfirst\r\n")
                .await
                .unwrap();
            // Keep the second chunk pending longer than the injected idle
            // timeout. This fixture has already delivered headers and a body
            // chunk, unlike a whole-response wiremock delay.
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = KuboClient::new_with_timeouts(
            endpoint,
            Duration::from_secs(300),
            Duration::from_millis(50),
        );
        let url = format!("{}/stalled", client.base_url());

        let response = client
            .download_http()
            .post(&url)
            .send()
            .await
            .expect("the initial response headers and first chunk must arrive");
        let mut body = response.bytes_stream();
        let first = tokio::time::timeout(Duration::from_secs(2), body.next())
            .await
            .expect("the first body chunk must arrive")
            .expect("response must contain a first body chunk")
            .expect("the first body chunk must be readable");
        assert_eq!(first.as_ref(), b"first");

        let error = tokio::time::timeout(Duration::from_secs(2), body.next())
            .await
            .expect("the download client must abort an inter-chunk stall, not hang")
            .expect("the stalled body must resolve to an error")
            .expect_err("the delayed second body chunk must time out");
        assert!(
            error.is_timeout(),
            "expected an idle timeout, got: {error:?}"
        );
        server.abort();
        let _ = server.await;
    }

    /// Regression: `read_timeout` is a one-shot deadline before response headers
    /// arrive, so applying it to uploads aborted large objects that Kubo was
    /// still ingesting. The upload client must carry no such bound.
    #[tokio::test]
    async fn the_upload_client_tolerates_a_response_that_is_slow_to_arrive() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/slow-ingest"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("done")
                    .set_delay(Duration::from_secs(2)),
            )
            .mount(&server)
            .await;

        // The injected idle bound is 20x shorter than the response delay: under
        // load the abort can only come later, never can the endpoint answer early.
        let client = KuboClient::new_with_timeouts(
            server.uri(),
            Duration::from_secs(300),
            Duration::from_millis(100),
        );
        let url = format!("{}/slow-ingest", client.base_url());

        let response = tokio::time::timeout(
            Duration::from_secs(30),
            client.upload_http().post(&url).send(),
        )
        .await
        .expect("the upload client must not hang the test harness")
        .expect("the upload client must not abort an object that is slow to ingest");
        assert_eq!(response.status().as_u16(), 200);
    }

    #[tokio::test]
    async fn both_streaming_clients_still_bound_connection_setup() {
        let client = KuboClient::new("http://127.0.0.1:1".to_owned());

        for (label, http) in [
            ("upload", client.upload_http()),
            ("download", client.download_http()),
        ] {
            let error = http
                .post("http://127.0.0.1:1/api/v0/cat")
                .send()
                .await
                .expect_err("a refused connection must fail rather than hang");
            assert!(
                error.is_connect() || error.is_timeout(),
                "unexpected {label} client error: {error}"
            );
        }
    }
}
