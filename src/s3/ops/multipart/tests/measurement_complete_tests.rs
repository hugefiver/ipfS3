use super::*;
use axum::{
    Router,
    body::Body,
    extract::{Query, Request, State},
    routing::post,
};
use std::time::Instant;

const TOTAL_BYTES: usize = 30 * 1024 * 1024;
const CAT_LATENCY: Duration = Duration::from_millis(10);
const ROOT_CID: &str = "QmMeasurementRoot";

#[derive(Default)]
struct Observations {
    started: Option<Instant>,
    cat_requests: HashMap<String, usize>,
    cat_bytes: usize,
    completed_cats: HashMap<String, usize>,
    active_cats: usize,
    peak_cats: usize,
    add_requests: usize,
    first_add: Option<Duration>,
    parts_read_before_add: usize,
    boundary: String,
    root_body: Vec<u8>,
}

struct ProbeState {
    parts: HashMap<String, Bytes>,
    observations: Mutex<Observations>,
}

struct ActiveCat(Arc<ProbeState>);

impl Drop for ActiveCat {
    fn drop(&mut self) {
        self.0.observations.lock().unwrap().active_cats -= 1;
    }
}

async fn cat(
    State(state): State<Arc<ProbeState>>,
    Query(query): Query<HashMap<String, String>>,
) -> Body {
    let cid = query.get("arg").unwrap().clone();
    let bytes = state.parts.get(&cid).expect("selected part CID").clone();
    {
        let mut observed = state.observations.lock().unwrap();
        *observed.cat_requests.entry(cid.clone()).or_default() += 1;
        observed.active_cats += 1;
        observed.peak_cats = observed.peak_cats.max(observed.active_cats);
    }
    let active = ActiveCat(state.clone());
    // Delay headers, not fixture generation or individual TCP frames.
    tokio::time::sleep(CAT_LATENCY).await;
    Body::from_stream(async_stream::stream! {
        let _active = active;
        for offset in (0..bytes.len()).step_by(64 * 1024) {
            let chunk = bytes.slice(offset..(offset + 64 * 1024).min(bytes.len()));
            state.observations.lock().unwrap().cat_bytes += chunk.len();
            yield Ok::<_, std::io::Error>(chunk);
        }
        *state.observations.lock().unwrap().completed_cats.entry(cid).or_default() += 1;
    })
}

async fn add(State(state): State<Arc<ProbeState>>, request: Request) -> &'static str {
    let mut captured = {
        let mut observed = state.observations.lock().unwrap();
        observed.add_requests += 1;
        if observed.first_add.is_none() {
            observed.first_add = Some(observed.started.unwrap().elapsed());
            observed.parts_read_before_add = observed.completed_cats.len();
        }
        observed.boundary = request.headers()[http::header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .strip_prefix("multipart/form-data; boundary=")
            .unwrap()
            .to_owned();
        std::mem::take(&mut observed.root_body)
    };
    let mut body = request.into_body().into_data_stream();
    while let Some(chunk) = body.next().await {
        captured.extend_from_slice(&chunk.unwrap());
    }
    state.observations.lock().unwrap().root_body = captured;
    "{\"Hash\":\"QmMeasurementRoot\",\"Size\":\"31457280\"}\n"
}

fn fixture_parts(source: &Bytes, part_bytes: usize, encrypted: bool) -> HashMap<String, Bytes> {
    let key = crate::crypto::ObjectKey { bytes: [0x42; 32] };
    (0..source.len())
        .step_by(part_bytes)
        .enumerate()
        .map(|(index, offset)| {
            let plaintext = source.slice(offset..(offset + part_bytes).min(source.len()));
            let body = if encrypted {
                let mut body = Vec::new();
                for (chunk_index, chunk) in plaintext
                    .chunks(crate::crypto::chunker::CHUNK_SIZE)
                    .enumerate()
                {
                    let mut nonce = [0_u8; 12];
                    nonce[..4].copy_from_slice(&u32::try_from(index).unwrap().to_le_bytes());
                    nonce[4..12]
                        .copy_from_slice(&u64::try_from(chunk_index).unwrap().to_le_bytes());
                    body.extend_from_slice(
                        &crate::crypto::aes_gcm::encrypt_chunk(&key, &nonce, chunk).unwrap(),
                    );
                }
                Bytes::from(body)
            } else {
                plaintext
            };
            (format!("QmMeasurementPart{index}"), body)
        })
        .collect()
}

async fn measure_complete(source: &Bytes, part_bytes: usize, encrypted: bool) {
    let parts = fixture_parts(source, part_bytes, encrypted);
    let selected_wire_bytes: usize = parts.values().map(Bytes::len).sum();
    let probe = Arc::new(ProbeState {
        parts,
        observations: Mutex::new(Observations {
            root_body: Vec::with_capacity(selected_wire_bytes + 4096),
            ..Default::default()
        }),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/api/v0/cat", post(cat))
        .route("/api/v0/add", post(add))
        .route("/api/v0/pin/add", post(|| async { "{\"Pins\":[]}" }))
        .with_state(probe.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let state = test_state_with_bucket_and_kubo("test-bucket", endpoint).await;
    let mut create = multipart_create_request("test-bucket", "archive.zip");
    if encrypted {
        create.headers = valid_sse_c_headers();
    }
    let upload_id = create_multipart_upload(&state, create)
        .await
        .unwrap()
        .output
        .upload_id
        .unwrap();
    let mut selected = Vec::new();
    for (index, offset) in (0..source.len()).step_by(part_bytes).enumerate() {
        let cid = format!("QmMeasurementPart{index}");
        let number = i32::try_from(index + 1).unwrap();
        crate::store::multipart::upsert_part(
            state.store.db(),
            &upload_id,
            number,
            &cid,
            i64::try_from(part_bytes.min(source.len() - offset)).unwrap(),
            &cid,
        )
        .await
        .unwrap();
        selected.push(CompletedPart {
            part_number: Some(number),
            e_tag: Some(ETag::Strong(cid)),
            ..Default::default()
        });
    }
    let mut request = complete_request(&upload_id, "unused");
    request.input.multipart_upload.as_mut().unwrap().parts = Some(selected);
    if encrypted {
        request.headers = valid_sse_c_headers();
    }

    // Only Complete is timed: DB setup, source allocation and part encryption are excluded.
    // Wall time includes the HTTP mock's body capture; it is not CPU or memory evidence.
    let started = Instant::now();
    probe.observations.lock().unwrap().started = Some(started);
    let completed = complete_multipart_upload(&state, request).await;
    let total = started.elapsed();
    server.abort();
    let _ = server.await;
    assert_eq!(completed.unwrap().output.e_tag.unwrap().value(), ROOT_CID);

    let observed = probe.observations.lock().unwrap();
    let passes = if encrypted { 2 } else { 1 };
    let cat_count: usize = observed.cat_requests.values().sum();
    assert_eq!(cat_count, probe.parts.len() * passes);
    assert!(observed.cat_requests.values().all(|&count| count == passes));
    assert_eq!(observed.cat_bytes, selected_wire_bytes * passes);
    assert_eq!(observed.completed_cats.values().sum::<usize>(), cat_count);
    assert_eq!(observed.add_requests, 1);
    if encrypted {
        assert_eq!(observed.parts_read_before_add, probe.parts.len());
    }

    // Check the consumed add payload outside the timed interval, including decryption.
    let start = observed
        .root_body
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap()
        + 4;
    let trailer = format!("\r\n--{}--\r\n", observed.boundary);
    assert!(observed.root_body.ends_with(trailer.as_bytes()));
    let payload = &observed.root_body[start..observed.root_body.len() - trailer.len()];
    if encrypted {
        let key = crate::crypto::ObjectKey { bytes: [0x42; 32] };
        let mut offset = 0;
        for chunk in payload.chunks(crate::crypto::chunker::CHUNK_SIZE + 28) {
            let plaintext = crate::crypto::aes_gcm::decrypt_chunk(&key, chunk).unwrap();
            assert_eq!(
                plaintext.as_ref(),
                &source[offset..offset + plaintext.len()]
            );
            offset += plaintext.len();
        }
        assert_eq!(offset, source.len());
    } else {
        assert_eq!(payload, source.as_ref());
    }
    // EOF and concurrency describe mock responses, not client-side decrypt completion
    // or retained buffers. Corrupt-part regression tests cover the authentication barrier.
    eprintln!(
        "multipart_complete mode={} parts={} selected_plain_bytes={} selected_wire_bytes={} cat_requests={} cat_response_bytes={} add_requests={} selected_parts_eof_before_add={}/{} first_root_add_headers_ms={:.3} total_complete_ms={:.3} mock_cat_in_flight_peak={} cat_header_latency_ms={}",
        if encrypted { "sse-c" } else { "plain" },
        probe.parts.len(),
        source.len(),
        selected_wire_bytes,
        cat_count,
        observed.cat_bytes,
        observed.add_requests,
        observed.parts_read_before_add,
        probe.parts.len(),
        observed.first_add.unwrap().as_secs_f64() * 1000.0,
        total.as_secs_f64() * 1000.0,
        observed.peak_cats,
        CAT_LATENCY.as_millis(),
    );
}

#[tokio::test]
#[ignore = "opt-in multipart completion wall-clock observations"]
async fn multipart_complete_measurement_baseline() {
    measure_workloads(&[false, true]).await;
}

#[tokio::test]
#[ignore = "opt-in plaintext multipart completion wall-clock observations"]
async fn multipart_plain_complete_measurement_baseline() {
    measure_workloads(&[false]).await;
}

async fn measure_workloads(modes: &[bool]) {
    // Equal logical bytes, valid >=5 MiB nonfinal parts, distinct content across chunks.
    let source = Bytes::from(
        (0..TOTAL_BYTES)
            .map(|offset| (offset / crate::crypto::chunker::CHUNK_SIZE % 251) as u8)
            .collect::<Vec<_>>(),
    );
    for &encrypted in modes {
        for part_bytes in [5 * 1024 * 1024, 15 * 1024 * 1024] {
            measure_complete(&source, part_bytes, encrypted).await;
        }
    }
}
