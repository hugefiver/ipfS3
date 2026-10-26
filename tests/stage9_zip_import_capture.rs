//! R25 import admission captures intent before source I/O.
#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
    pub mod stage9_zip_replay;
    pub mod stage9_zip_replay_requests;
    pub mod stage9_zip_replay_source;
}

use futures_util::FutureExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{pinning::zip_policy::CapturedZipIntent, store::entities::import_job};
use std::panic::AssertUnwindSafe;
use support::{
    decompress::S3TestEndpoint,
    sigv4::send_sigv4,
    stage9_zip_replay::{BUCKET, Entry, KEY, Outcome, run},
    stage9_zip_replay_source::URL,
};

#[tokio::test]
async fn legacy_url_intake_persists_authenticated_capture_before_fetch() {
    run(Entry::Import, Outcome::Complete, |h| async move {
        let result = AssertUnwindSafe(async {
            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/xml"),
            );
            let response = send_sigv4(
                reqwest::Method::POST,
                h.endpoint(),
                BUCKET,
                KEY,
                &[("ipfs3-import", ""), ("decompress-zip", "out/")],
                format!("<IPFS3ImportRequest><URL>{URL}</URL></IPFS3ImportRequest>").into_bytes(),
                headers,
                "test",
            )
            .await;
            assert_eq!(response.status(), StatusCode::ACCEPTED);
            let id = response.headers()["x-ipfs3-import-job-id"]
                .to_str()
                .unwrap()
                .to_owned();
            let xml = response.text().await.unwrap();
            assert!(xml.contains(&id));
            let job = import_job::Entity::find_by_id(&id)
                .one(h.state.store.db())
                .await
                .unwrap()
                .unwrap();
            let capture = CapturedZipIntent::decode_json(
                job.zip_intent_json
                    .as_deref()
                    .expect("capture is durable at intake"),
            )
            .unwrap();
            assert_eq!(capture.origin().principal_id, "test");
            assert_eq!(capture.origin().request_id, id);
            assert_eq!(capture.target_prefix(), "out/");
            assert!(capture.controls().auto_pin_outputs);
            assert!(!capture.controls().targets.source);
            assert!(capture.controls().targets.extracted);
            assert_eq!(h.source.calls(), 0);
            assert_eq!(h.adds().await, 0);
        })
        .catch_unwind()
        .await;
        (h, result)
    })
    .await;
}
