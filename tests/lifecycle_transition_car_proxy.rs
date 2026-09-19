//! Test-only streaming Kubo reverse proxy that measures DAG-import CAR part bytes.

use std::sync::{Arc, Mutex};

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    response::Response,
    routing::{get, post},
};
use bytes::Bytes;
use futures_util::StreamExt as _;
use http::{HeaderMap, StatusCode, header};
use serde::Serialize;

const BIND_ENV: &str = "IPFS_S3_TRANSITION_CAR_PROXY_BIND";
const UPSTREAM_ENV: &str = "IPFS_S3_TRANSITION_CAR_PROXY_UPSTREAM_URL";
const HEADER_LIMIT: usize = 64 * 1024;

#[derive(Clone)]
struct ProxyState {
    upstream: String,
    client: reqwest::Client,
    metrics: Arc<Mutex<Metrics>>,
}

#[derive(Default)]
struct Metrics {
    import_requests: u64,
    completed_car_bytes: Vec<u64>,
    parse_failures: u64,
    in_flight: u64,
}

#[derive(Serialize)]
struct MetricsSnapshot {
    import_requests: u64,
    completed_car_bytes: Vec<u64>,
    total_car_bytes: u64,
    max_car_bytes: u64,
    parse_failures: u64,
    in_flight: u64,
}

impl Metrics {
    fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            import_requests: self.import_requests,
            completed_car_bytes: self.completed_car_bytes.clone(),
            total_car_bytes: self.completed_car_bytes.iter().sum(),
            max_car_bytes: self.completed_car_bytes.iter().copied().max().unwrap_or(0),
            parse_failures: self.parse_failures,
            in_flight: self.in_flight,
        }
    }
}

struct ImportCompletion {
    metrics: Arc<Mutex<Metrics>>,
    finished: bool,
}

impl ImportCompletion {
    fn start(metrics: Arc<Mutex<Metrics>>) -> Self {
        {
            let mut metrics = metrics.lock().expect("CAR proxy metrics lock");
            metrics.import_requests += 1;
            metrics.in_flight += 1;
        }
        Self {
            metrics,
            finished: false,
        }
    }

    fn complete(&mut self, car_bytes: u64) {
        let mut metrics = self.metrics.lock().expect("CAR proxy metrics lock");
        metrics.completed_car_bytes.push(car_bytes);
        metrics.in_flight -= 1;
        self.finished = true;
    }

    fn fail(&mut self) {
        let mut metrics = self.metrics.lock().expect("CAR proxy metrics lock");
        metrics.parse_failures += 1;
        metrics.in_flight -= 1;
        self.finished = true;
    }
}

impl Drop for ImportCompletion {
    fn drop(&mut self) {
        if !self.finished {
            self.fail();
        }
    }
}

enum MultipartPhase {
    Headers,
    Car,
    Done,
    Invalid,
}

struct CarPartCounter {
    phase: MultipartPhase,
    pending: Vec<u8>,
    delimiter: Vec<u8>,
    car_bytes: u64,
}

impl CarPartCounter {
    fn new(boundary: &str) -> Self {
        Self {
            phase: MultipartPhase::Headers,
            pending: Vec::new(),
            delimiter: format!("\r\n--{boundary}").into_bytes(),
            car_bytes: 0,
        }
    }

    fn observe(&mut self, bytes: &[u8]) {
        match self.phase {
            MultipartPhase::Headers => {
                self.pending.extend_from_slice(bytes);
                if let Some(index) = find_bytes(&self.pending, b"\r\n\r\n") {
                    let data = self.pending.split_off(index + 4);
                    let headers = std::mem::take(&mut self.pending);
                    if find_bytes(&headers, b"filename=\"export.car\"").is_none() {
                        self.phase = MultipartPhase::Invalid;
                        return;
                    }
                    self.phase = MultipartPhase::Car;
                    self.observe_car(&data);
                } else if self.pending.len() > HEADER_LIMIT {
                    self.phase = MultipartPhase::Invalid;
                }
            }
            MultipartPhase::Car => self.observe_car(bytes),
            MultipartPhase::Done | MultipartPhase::Invalid => {}
        }
    }

    fn observe_car(&mut self, bytes: &[u8]) {
        let mut combined = std::mem::take(&mut self.pending);
        combined.extend_from_slice(bytes);
        if let Some(index) = find_bytes(&combined, &self.delimiter) {
            self.car_bytes += index as u64;
            self.phase = MultipartPhase::Done;
            return;
        }

        let retained = self.delimiter.len().saturating_sub(1).min(combined.len());
        let counted = combined.len() - retained;
        self.car_bytes += counted as u64;
        self.pending.extend_from_slice(&combined[counted..]);
    }

    fn finish(self) -> Result<u64, &'static str> {
        match self.phase {
            MultipartPhase::Done => Ok(self.car_bytes),
            MultipartPhase::Headers => Err("multipart file headers were incomplete"),
            MultipartPhase::Car => Err("multipart closing boundary was incomplete"),
            MultipartPhase::Invalid => Err("multipart file part was invalid"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "long-lived test utility: parent runner must terminate the process"]
async fn lifecycle_transition_car_proxy() {
    let bind = required_env(BIND_ENV);
    let upstream = required_env(UPSTREAM_ENV).trim_end_matches('/').to_owned();
    let state = ProxyState {
        upstream,
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("build CAR proxy client"),
        metrics: Arc::new(Mutex::new(Metrics::default())),
    };
    let app = Router::new()
        .route("/__car_proxy/health", get(health))
        .route("/__car_proxy/metrics", get(metrics))
        .route("/__car_proxy/metrics/reset", post(reset_metrics))
        .fallback(proxy)
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .unwrap_or_else(|error| panic!("bind CAR proxy at {bind}: {error}"));
    eprintln!("[LIFECYCLE-CAR-PROXY] ready bind={bind}");
    axum::serve(listener, app)
        .await
        .expect("serve lifecycle CAR proxy");
}

async fn health() -> &'static str {
    "ok"
}

async fn metrics(State(state): State<ProxyState>) -> axum::Json<MetricsSnapshot> {
    let snapshot = state
        .metrics
        .lock()
        .expect("CAR proxy metrics lock")
        .snapshot();
    axum::Json(snapshot)
}

async fn reset_metrics(State(state): State<ProxyState>) -> StatusCode {
    let mut metrics = state.metrics.lock().expect("CAR proxy metrics lock");
    if metrics.in_flight != 0 {
        return StatusCode::CONFLICT;
    }
    *metrics = Metrics::default();
    StatusCode::NO_CONTENT
}

async fn proxy(State(state): State<ProxyState>, request: Request) -> Response {
    match forward(state, request).await {
        Ok(response) => response,
        Err(error) => Response::builder()
            .status(StatusCode::BAD_GATEWAY)
            .body(Body::from(format!("CAR proxy upstream error: {error}")))
            .expect("build proxy error response"),
    }
}

async fn forward(state: ProxyState, request: Request) -> Result<Response, String> {
    let is_import = request.uri().path() == "/api/v0/dag/import";
    let boundary = if is_import {
        Some(multipart_boundary(request.headers())?)
    } else {
        None
    };
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let url = format!("{}{path_and_query}", state.upstream);
    let method = request.method().clone();
    let headers = request.headers().clone();
    let source = request.into_body().into_data_stream();

    let upload = if let Some(boundary) = boundary {
        let metrics = Arc::clone(&state.metrics);
        reqwest::Body::wrap_stream(async_stream::stream! {
            let mut source = source;
            let mut counter = CarPartCounter::new(&boundary);
            let mut completion = ImportCompletion::start(metrics);
            while let Some(item) = source.next().await {
                match item {
                    Ok(bytes) => {
                        counter.observe(&bytes);
                        yield Ok::<Bytes, axum::Error>(bytes);
                    }
                    Err(error) => {
                        completion.fail();
                        yield Err(error);
                        return;
                    }
                }
            }
            match counter.finish() {
                Ok(car_bytes) => completion.complete(car_bytes),
                Err(_) => completion.fail(),
            }
        })
    } else {
        reqwest::Body::wrap_stream(source)
    };
    let mut upstream = state.client.request(method, url).body(upload);
    for (name, value) in &headers {
        if !is_hop_by_hop(name) {
            upstream = upstream.header(name, value);
        }
    }
    let response = upstream.send().await.map_err(|error| error.to_string())?;
    let status = response.status();
    let response_headers = response.headers().clone();
    let body = Body::from_stream(response.bytes_stream());
    let mut downstream = Response::builder().status(status);
    for (name, value) in &response_headers {
        if !is_hop_by_hop(name) {
            downstream = downstream.header(name, value);
        }
    }
    downstream.body(body).map_err(|error| error.to_string())
}

fn multipart_boundary(headers: &HeaderMap) -> Result<String, String> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .ok_or_else(|| "DAG import omitted Content-Type".to_owned())?
        .to_str()
        .map_err(|_| "DAG import Content-Type was not ASCII".to_owned())?;
    let boundary = content_type
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("boundary="))
        .map(|value| value.trim_matches('"'))
        .filter(|value| !value.is_empty() && value.len() <= 200)
        .ok_or_else(|| "DAG import multipart boundary was missing or invalid".to_owned())?;
    Ok(boundary.to_owned())
}

fn is_hop_by_hop(name: &http::HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "host"
    )
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("NOT RUN: {name} is required"))
}

#[cfg(test)]
mod tests {
    use super::CarPartCounter;

    #[test]
    fn counts_only_fragmented_car_file_part_bytes() {
        let car = b"actual-car\0bytes\r\ninside";
        let multipart = [
            b"--test-boundary\r\nContent-Disposition: form-data; name=\"file\"; filename=\"export.car\"\r\nContent-Type: application/vnd.ipld.car\r\n\r\n".as_slice(),
            car,
            b"\r\n--test-boundary--\r\n",
        ]
        .concat();
        let mut counter = CarPartCounter::new("test-boundary");
        for fragment in multipart.chunks(7) {
            counter.observe(fragment);
        }

        assert_eq!(
            counter.finish().expect("complete multipart"),
            car.len() as u64
        );
    }
}
