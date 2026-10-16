//! Loopback PSA account and bounded Kubo protocol fixture; not a Noop adapter.

use super::{
    BenchResult, Case, RPS,
    http::{self, Server},
};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const MAX_BODY: usize = 64 * 1024;
const MAX_EVENTS: usize = 2048;
const MAX_CONTENT_BYTES: usize = 512 * 1024;
const MAX_MULTIPART_OVERHEAD: usize = 1024;
const HOLD_MS: u64 = 650;

#[derive(Clone, Serialize)]
pub struct Event {
    pub provider: usize,
    pub correlation: Option<String>,
    pub process: usize,
    pub class: String,
    pub item: usize,
    pub method: String,
    pub start_us: u64,
    pub end_us: Option<u64>,
    pub status: u16,
    pub retry_after_seconds: Option<u64>,
}

#[derive(Default)]
struct Data {
    files: HashMap<String, Vec<u8>>,
    resources: HashMap<String, Value>,
    accepted: [VecDeque<u64>; 2],
    events: Vec<Event>,
    current: [usize; 3],
    peak: [usize; 3],
    peak_combined: usize,
    protocol_errors: usize,
    rejected_by_resource_bound: usize,
    content_bytes: usize,
    leaf_content_bytes: usize,
    resource_bound_categories: BTreeMap<&'static str, usize>,
}

#[derive(Clone)]
pub struct Fixture {
    origin: Instant,
    expected: Arc<BTreeMap<String, (String, usize)>>,
    data: Arc<Mutex<Data>>,
    provider: usize,
    case: Case,
    source_archive: Arc<(String, usize)>,
}

struct Active {
    fixture: Fixture,
    process: usize,
    event: usize,
}

impl Active {
    fn response_constructed(&self, status: u16) {
        let mut data = self.fixture.data.lock().unwrap();
        data.events[self.event].status = status;
        data.events[self.event].retry_after_seconds = (status == 429).then_some(1);
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        let mut data = self.fixture.data.lock().unwrap();
        data.current[self.process] -= 1;
        data.events[self.event].end_us = Some(self.fixture.now_us());
    }
}

impl Fixture {
    pub async fn start_for(case: Case) -> BenchResult<(Self, Server)> {
        let archive = http::archive_with_counts(case.outputs, case.distinct);
        let source_archive = Arc::new((http::cid(&archive), archive.len()));
        let expected = [
            ("bulk", case.distinct),
            ("ordinary", 4),
            ("recovery", 2),
            ("cleanup", usize::from(case.mixed)),
        ]
        .into_iter()
        .flat_map(|(class, count)| {
            (0..count).map(move |index| {
                (
                    http::cid(&http::payload(class, index)),
                    (class.to_owned(), index),
                )
            })
        })
        .collect();
        let fixture = Self {
            origin: Instant::now(),
            expected: Arc::new(expected),
            data: Arc::new(Mutex::new(Data::default())),
            provider: 0,
            case,
            source_archive,
        };
        let account = |state: Fixture| {
            Router::new()
                .route("/v1/ipfs/pins", post(submit).get(find).delete(forbidden))
                .route("/v1/ipfs/pins/{id}", get(get_pin).delete(forbidden))
                .with_state(state)
        };
        let mut router = account(fixture.clone());
        if case.providers == 2 {
            let mut replica = fixture.clone();
            replica.provider = 1;
            router = router.nest("/replica", account(replica));
        }
        let router = router
            .route("/api/v0/{*op}", post(kubo))
            .fallback(forbidden)
            .layer(DefaultBodyLimit::max(MAX_BODY))
            .with_state(fixture.clone());
        Ok((fixture, Server::start(router).await?))
    }

    pub fn now_us(&self) -> u64 {
        self.origin.elapsed().as_micros() as u64
    }
    pub fn identity(&self, cid: &str) -> Option<(&str, usize)> {
        self.expected
            .get(cid)
            .map(|(class, item)| (class.as_str(), *item))
    }
    pub fn events(&self) -> Vec<Event> {
        self.data.lock().unwrap().events.clone()
    }
    pub fn counts(&self) -> Value {
        let data = self.data.lock().unwrap();
        json!({"actual_peak_in_flight_combined":data.peak_combined, "actual_peak_in_flight_per_process":&data.peak[..2], "protocol_errors":data.protocol_errors,
            "provider_handler_hold_ms":HOLD_MS, "fixture_global_acceptance_budget_per_rolling_second":RPS,
            "acceptance_budget_scope":"each loopback simulated account, NOT a gateway cross-process budget implementation",
            "resource_bounds":{"events":MAX_EVENTS,"kubo_files":self.case.distinct+8,"provider_json_body_bytes":MAX_BODY,
                "kubo_leaf_framed_body_bytes":MAX_BODY,"kubo_leaf_content_pool_bytes":MAX_CONTENT_BYTES,
                "kubo_source_archive_files":1,"kubo_source_archive_content_bytes":self.source_archive.1,"kubo_multipart_framing_bytes":MAX_MULTIPART_OVERHEAD,
                "kubo_add_max_framed_body_bytes":self.max_add_body_bytes(),"kubo_total_content_bytes":MAX_CONTENT_BYTES+self.source_archive.1,"remote_resources":self.case.expected_allocations()},
            "actual_resources":{"events":data.events.len(),"kubo_files":data.files.len(),"kubo_content_bytes":data.content_bytes,"kubo_leaf_content_bytes":data.leaf_content_bytes,
                "kubo_source_archive_content_bytes":data.files.get(&self.source_archive.0).map_or(0,Vec::len),"remote_resources":data.resources.len()},
            "resource_bound_rejections":data.rejected_by_resource_bound,
            "resource_bound_rejection_categories":data.resource_bound_categories,
            "seed_http_calls":data.events.iter().filter(|e| e.process == 2).count()})
    }
    pub fn reject(&self) -> Response {
        self.data.lock().unwrap().protocol_errors += 1;
        (StatusCode::BAD_REQUEST, "fixture protocol rejected").into_response()
    }
    fn max_add_body_bytes(&self) -> usize {
        MAX_BODY.max(self.source_archive.1 + MAX_MULTIPART_OVERHEAD)
    }
    fn reject_bound(&self, category: &'static str) -> Response {
        let mut data = self.data.lock().unwrap();
        data.rejected_by_resource_bound += 1;
        *data.resource_bound_categories.entry(category).or_default() += 1;
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            "fixture resource limit exceeded",
        )
            .into_response()
    }

    fn enter(
        &self,
        headers: &HeaderMap,
        cid: &str,
        method: &str,
        correlation: Option<&str>,
    ) -> Option<(Active, bool)> {
        // Fake tokens attribute processes, not accounts; the route selects the
        // simulated account. Never read user credentials or invent token scopes.
        let process = match headers.get("authorization")?.to_str().ok()? {
            "Bearer offline-fixture-process-0" => 0,
            "Bearer offline-fixture-process-1" => 1,
            "Bearer offline-fixture-seed" => 2,
            _ => return None,
        };
        let (class, item) = self.expected.get(cid)?;
        let now = self.now_us();
        let mut data = self.data.lock().unwrap();
        if data.events.len() >= MAX_EVENTS {
            data.rejected_by_resource_bound += 1;
            *data
                .resource_bound_categories
                .entry("provider_events")
                .or_default() += 1;
            return None;
        }
        let accepted = &mut data.accepted[self.provider];
        while accepted
            .front()
            .is_some_and(|time| now.saturating_sub(*time) >= 1_000_000)
        {
            accepted.pop_front();
        }
        let limited = process < 2 && accepted.len() >= RPS as usize;
        if process < 2 && !limited {
            accepted.push_back(now);
        }
        let event = data.events.len();
        // Zero means canceled/no response constructed, never a guessed success.
        data.events.push(Event {
            provider: self.provider,
            correlation: correlation
                .filter(|v| super::metrics::safe_identity(v))
                .map(str::to_owned),
            process,
            class: class.clone(),
            item: *item,
            method: method.into(),
            start_us: now,
            end_us: None,
            status: 0,
            retry_after_seconds: None,
        });
        data.current[process] += 1;
        data.peak[process] = data.peak[process].max(data.current[process]);
        data.peak_combined = data.peak_combined.max(data.current[0] + data.current[1]);
        Some((
            Active {
                fixture: self.clone(),
                process,
                event,
            },
            limited,
        ))
    }
}

async fn forbidden(State(fixture): State<Fixture>) -> Response {
    fixture.reject()
}

async fn delay(limited: bool) -> Option<Response> {
    tokio::time::sleep(Duration::from_millis(HOLD_MS)).await;
    limited.then(|| {
        (
            StatusCode::TOO_MANY_REQUESTS,
            [("retry-after", "1")],
            Json(json!({"error":"offline shared account rate budget"})),
        )
            .into_response()
    })
}

async fn submit(
    State(fixture): State<Fixture>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let Some(cid) = body["cid"].as_str() else {
        return fixture.reject();
    };
    if body["meta"]["gateway_job_id"].as_str().is_none() {
        return fixture.reject();
    }
    let Some((active, limited)) = fixture.enter(
        &headers,
        cid,
        "POST",
        body["meta"]["gateway_job_id"].as_str(),
    ) else {
        return fixture.reject();
    };
    if let Some(response) = delay(limited).await {
        active.response_constructed(429);
        return response;
    }
    let resource = json!({"requestid":format!("fixture-{cid}"), "status":"pinned", "pin":{"cid":cid,"meta":body["meta"]}, "info":{}});
    let key = format!("{}:{cid}", fixture.provider);
    let mut data = fixture.data.lock().unwrap();
    if !data.resources.contains_key(&key)
        && data.resources.len() >= fixture.case.expected_allocations()
    {
        data.rejected_by_resource_bound += 1;
        *data
            .resource_bound_categories
            .entry("provider_resources")
            .or_default() += 1;
        drop(data);
        active.response_constructed(400);
        return fixture.reject();
    }
    data.resources.insert(key, resource.clone());
    drop(data);
    active.response_constructed(200);
    Json(resource).into_response()
}

async fn find(
    State(fixture): State<Fixture>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Some(cid) = query.get("cid") else {
        return fixture.reject();
    };
    let metadata: Value = match query.get("meta").and_then(|m| serde_json::from_str(m).ok()) {
        Some(m) => m,
        None => return fixture.reject(),
    };
    if metadata["gateway_job_id"].as_str().is_none() {
        return fixture.reject();
    }
    let Some((active, limited)) = fixture.enter(
        &headers,
        cid,
        "GET-find",
        metadata["gateway_job_id"].as_str(),
    ) else {
        return fixture.reject();
    };
    if let Some(response) = delay(limited).await {
        active.response_constructed(429);
        return response;
    }
    // A recovery result exists only after an actual successful historical HTTP
    // submission, and only with matching CID AND outbound correlation metadata.
    let data = fixture.data.lock().unwrap();
    let matching = data
        .resources
        .get(&format!("{}:{cid}", fixture.provider))
        .filter(|r| r["pin"]["meta"]["gateway_job_id"] == metadata["gateway_job_id"]);
    let results = matching.into_iter().cloned().collect::<Vec<_>>();
    drop(data);
    active.response_constructed(200);
    Json(json!({"count":results.len(), "results":results})).into_response()
}

async fn get_pin(
    State(fixture): State<Fixture>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let Some(cid) = id.strip_prefix("fixture-") else {
        return fixture.reject();
    };
    let Some((active, limited)) = fixture.enter(&headers, cid, "GET-status", None) else {
        return fixture.reject();
    };
    if let Some(response) = delay(limited).await {
        active.response_constructed(429);
        return response;
    }
    let resource = fixture
        .data
        .lock()
        .unwrap()
        .resources
        .get(&format!("{}:{cid}", fixture.provider))
        .cloned();
    match resource {
        Some(r) => {
            active.response_constructed(200);
            Json(r).into_response()
        }
        None => {
            active.response_constructed(400);
            fixture.reject()
        }
    }
}

enum BodyReadFailure {
    ResourceBound,
    Transport,
}

async fn bounded_body(body: Body, limit: usize) -> Result<Vec<u8>, BodyReadFailure> {
    let mut stream = body.into_data_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| BodyReadFailure::Transport)?;
        if bytes
            .len()
            .checked_add(chunk.len())
            .is_none_or(|next| next > limit)
        {
            return Err(BodyReadFailure::ResourceBound);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

async fn kubo(
    State(fixture): State<Fixture>,
    Path(op): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Body,
) -> Response {
    match op.as_str() {
        "add" => {
            let Some(content_type) = headers.get("content-type").and_then(|v| v.to_str().ok())
            else {
                return fixture.reject();
            };
            let Some(boundary) = content_type
                .strip_prefix("multipart/form-data; boundary=")
                .filter(|b| b.len() < 200)
            else {
                return fixture.reject();
            };
            let bytes = match bounded_body(body, fixture.max_add_body_bytes()).await {
                Ok(bytes) => bytes,
                Err(BodyReadFailure::ResourceBound) => {
                    return fixture.reject_bound("kubo_add_framed_body");
                }
                Err(BodyReadFailure::Transport) => return fixture.reject(),
            };
            let end = format!("\r\n--{boundary}--\r\n");
            let Some(separator) = bytes.windows(4).position(|b| b == b"\r\n\r\n") else {
                return fixture.reject();
            };
            if !bytes.starts_with(format!("--{boundary}\r\n").as_bytes())
                || !bytes.ends_with(end.as_bytes())
                || separator + 4 > bytes.len() - end.len()
            {
                return fixture.reject();
            }
            let content = &bytes[separator + 4..bytes.len() - end.len()];
            if bytes.len() - content.len() > MAX_MULTIPART_OVERHEAD {
                return fixture.reject_bound("kubo_multipart_framing");
            }
            let cid = http::cid(content);
            let size = content.len();
            // Only this case's exact generated source archive gets the archive
            // allowance. Unknown/oversized ZIPs cannot spend the leaf pool or
            // turn the larger bounded reader into a general large-object fixture.
            let source_archive =
                cid == fixture.source_archive.0 && size == fixture.source_archive.1;
            if !source_archive && bytes.len() > MAX_BODY {
                return fixture.reject_bound("kubo_leaf_framed_body");
            }
            let mut data = fixture.data.lock().unwrap();
            if !data.files.contains_key(&cid) && data.files.len() >= fixture.case.distinct + 8 {
                drop(data);
                return fixture.reject_bound("kubo_files");
            }
            let previous = data.files.get(&cid).map_or(0, Vec::len);
            let leaf_bytes = if source_archive {
                data.leaf_content_bytes
            } else {
                data.leaf_content_bytes - previous + size
            };
            if leaf_bytes > MAX_CONTENT_BYTES {
                drop(data);
                return fixture.reject_bound("kubo_leaf_content_pool");
            }
            data.leaf_content_bytes = leaf_bytes;
            data.content_bytes = data.content_bytes - previous + size;
            data.files.insert(cid.clone(), content.to_vec());
            (
                StatusCode::OK,
                format!(
                    "{}\n",
                    json!({"Name":"object", "Hash":cid, "Size":size.to_string()})
                ),
            )
                .into_response()
        }
        "cat" => {
            let content = query
                .get("arg")
                .and_then(|cid| fixture.data.lock().unwrap().files.get(cid).cloned());
            match content {
                Some(bytes) => bytes.into_response(),
                None => fixture.reject(),
            }
        }
        "pin/add" => {
            let Some(cid) = query.get("arg") else {
                return fixture.reject();
            };
            if !fixture.data.lock().unwrap().files.contains_key(cid) {
                return fixture.reject();
            }
            Json(json!({"Pins":[cid]})).into_response()
        }
        // Deliberately no pin/rm, GC, directory fabrication or external networking.
        _ => fixture.reject(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
    }

    async fn add(client: &reqwest::Client, port: u16, content: Vec<u8>) -> reqwest::Response {
        // The production Kubo add client uses a streamed multipart file named
        // object, including for the ZIP input uploaded before extraction.
        let stream =
            futures_util::stream::iter([Ok::<_, std::io::Error>(bytes::Bytes::from(content))]);
        let part = reqwest::multipart::Part::stream(reqwest::Body::wrap_stream(stream))
            .file_name("object")
            .mime_str("application/octet-stream")
            .unwrap();
        client
            .post(format!("http://127.0.0.1:{port}/api/v0/add"))
            .multipart(reqwest::multipart::Form::new().part("file", part))
            .send()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn source_archive_round_trips_for_formal_and_10k_without_spending_leaf_pool() {
        for case in [Case::formal(), Case::named("dual-provider-10k").unwrap()] {
            let (fixture, mut server) = Fixture::start_for(case).await.unwrap();
            let client = client();
            let archive = http::archive_with_counts(case.outputs, case.distinct);
            let cid = http::cid(&archive);
            let response = add(&client, server.port, archive.clone()).await;
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            let receipt: Value = response.json().await.unwrap();
            assert_eq!(receipt["Hash"], cid);
            assert_eq!(receipt["Size"], archive.len().to_string());
            let downloaded = client
                .post(format!(
                    "http://127.0.0.1:{}/api/v0/cat?arg={cid}",
                    server.port
                ))
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            assert_eq!(downloaded.as_ref(), archive.as_slice());

            // Replaying the same source input occupies one content-addressed slot.
            assert_eq!(
                add(&client, server.port, archive.clone()).await.status(),
                reqwest::StatusCode::OK
            );
            let leaf = http::payload("bulk", 0);
            assert_eq!(
                add(&client, server.port, leaf.clone()).await.status(),
                reqwest::StatusCode::OK
            );
            let resources = fixture.counts()["actual_resources"].clone();
            assert_eq!(resources["kubo_files"], 2);
            assert_eq!(
                resources["kubo_source_archive_content_bytes"],
                archive.len()
            );
            assert_eq!(resources["kubo_leaf_content_bytes"], leaf.len());
            assert_eq!(resources["kubo_content_bytes"], archive.len() + leaf.len());
            assert_eq!(fixture.counts()["resource_bound_rejections"], 0);
            assert!(server.shutdown().await);
        }
    }

    #[tokio::test]
    async fn oversized_source_and_framed_body_are_rejected_without_cache_growth() {
        let case = Case::named("dual-provider-10k").unwrap();
        let (fixture, mut server) = Fixture::start_for(case).await.unwrap();
        let client = client();
        let archive = http::archive_with_counts(case.outputs, case.distinct);
        assert_eq!(
            add(&client, server.port, archive.clone()).await.status(),
            reqwest::StatusCode::OK
        );
        let before = fixture.counts()["actual_resources"].clone();
        let mut oversized = archive;
        oversized.push(0);
        assert_eq!(
            add(&client, server.port, oversized).await.status(),
            reqwest::StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(fixture.counts()["actual_resources"], before);
        assert_eq!(
            add(
                &client,
                server.port,
                vec![0; fixture.max_add_body_bytes() + 1]
            )
            .await
            .status(),
            reqwest::StatusCode::PAYLOAD_TOO_LARGE
        );
        let counts = fixture.counts();
        assert_eq!(counts["actual_resources"], before);
        assert_eq!(counts["resource_bound_rejections"], 2);
        assert_eq!(
            counts["resource_bound_rejection_categories"]["kubo_leaf_framed_body"],
            1
        );
        assert_eq!(
            counts["resource_bound_rejection_categories"]["kubo_add_framed_body"],
            1
        );
        assert!(server.shutdown().await);
    }

    #[tokio::test]
    async fn leaf_pool_cannot_borrow_the_reserved_archive_allowance() {
        let case = Case::named("dual-provider-10k").unwrap();
        let (fixture, mut server) = Fixture::start_for(case).await.unwrap();
        let client = client();
        let archive = http::archive_with_counts(case.outputs, case.distinct);
        assert_eq!(
            add(&client, server.port, archive).await.status(),
            reqwest::StatusCode::OK
        );
        let leaf_size = MAX_BODY - MAX_MULTIPART_OVERHEAD;
        let accepted = MAX_CONTENT_BYTES / leaf_size;
        for index in 0..accepted {
            assert_eq!(
                add(&client, server.port, vec![index as u8; leaf_size])
                    .await
                    .status(),
                reqwest::StatusCode::OK
            );
        }
        let before = fixture.counts()["actual_resources"].clone();
        assert_eq!(
            add(&client, server.port, vec![accepted as u8; leaf_size])
                .await
                .status(),
            reqwest::StatusCode::PAYLOAD_TOO_LARGE
        );
        let counts = fixture.counts();
        assert_eq!(counts["actual_resources"], before);
        assert_eq!(
            counts["resource_bound_rejection_categories"]["kubo_leaf_content_pool"],
            1
        );
        assert!(server.shutdown().await);
    }
}
