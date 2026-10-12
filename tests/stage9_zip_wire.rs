//! Literal wire regressions sampled only from committed dc51927, never main.
#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
    pub mod stage9_zip_replay_source;
    pub mod stage9_zip_wire;
    pub mod stage9_zip_wire_golden;
    pub mod stage9_zip_wire_requests;
}

use futures_util::FutureExt;
use http::{HeaderMap, StatusCode};
use std::panic::AssertUnwindSafe;
use support::{
    stage9_zip_wire::{Entry, Outcome, run},
    stage9_zip_wire_golden::check,
    stage9_zip_wire_requests as request,
};

macro_rules! scenario {
    ($entry:expr, $outcome:expr, |mut $h:ident| $body:block) => { scenario!(@owned $entry, $outcome, [mut], $h, $body) };
    ($entry:expr, $outcome:expr, |$h:ident| $body:block) => { scenario!(@owned $entry, $outcome, [], $h, $body) };
    (@owned $entry:expr, $outcome:expr, [$($mutable:tt)*], $h:ident, $body:block) => {
        run($entry, $outcome, |$($mutable)* $h| async move {
            let result = AssertUnwindSafe(async $body).catch_unwind().await;
            ($h, result)
        }).await
    };
}

#[tokio::test]
async fn legacy_put_result_false_empty_wire_and_root_warning() {
    for (outcome, name) in [
        (Outcome::Complete, "legacy-put"),
        (Outcome::RootWarning, "legacy-put-warning"),
    ] {
        scenario!(Entry::Put, outcome, |h| {
            request::enable_versioning(&h).await;
            let wire = request::put(&h, outcome.archive(), HeaderMap::new()).await;
            check(name, &wire);
            assert_eq!(wire.status, StatusCode::OK);
            assert!(wire.body.is_empty());
        });
    }
}

#[tokio::test]
async fn legacy_mpu_complete_partial_original_xml_and_headers_replay_exactly() {
    scenario!(Entry::Mpu, Outcome::Partial, |h| {
        request::enable_versioning(&h).await;
        let created = request::create(&h, HeaderMap::new()).await;
        assert_eq!(created.status, StatusCode::OK);
        let id = created.field("UploadId");
        let etag = request::upload(&h, &id, Outcome::Partial.archive()).await;
        let first = request::complete(&h, &id, &etag).await;
        assert_eq!(first.id("x-ipfs-s3-zip-batch-id"), id);
        check("legacy-mpu-partial", &first);
        assert_eq!(first.status, StatusCode::OK);
        let replay = request::complete(&h, &id, &etag).await;
        check("legacy-mpu-partial", &replay);
        assert_eq!(replay.body, first.body);
        assert_eq!(replay.protocol(), first.protocol());
    });
}

#[tokio::test]
async fn legacy_url_import_accepted_queued_and_completed_status_xml() {
    scenario!(Entry::Import, Outcome::Complete, |mut h| {
        request::enable_versioning(&h).await;
        let accepted = request::import(&h).await;
        check("legacy-import-accepted", &accepted);
        assert_eq!(accepted.status, StatusCode::ACCEPTED);
        let id = accepted.id("x-ipfs3-import-job-id");
        let queued = request::status(&h, &id).await;
        assert_eq!(queued.field("JobId"), id);
        check("legacy-import-queued", &queued);
        assert_eq!(h.source.calls(), 0);
        h.start_worker();
        request::completed(&h, &id).await;
        let completed = request::status(&h, &id).await;
        assert_eq!(completed.field("JobId"), id);
        check("legacy-import-completed", &completed);
        assert_eq!(h.source.calls(), 1);
    });
}

#[tokio::test]
async fn v2_source_false_success_original_body_without_virtual_source_headers() {
    scenario!(Entry::Put, Outcome::Complete, |h| {
        request::enable_versioning(&h).await;
        let first = request::put(
            &h,
            Outcome::Complete.archive(),
            request::v2("plain-wire-success"),
        )
        .await;
        check("v2-source-false", &first);
        assert_eq!(first.status, StatusCode::OK);
        let replay = request::put(
            &h,
            Outcome::Complete.archive(),
            request::v2("plain-wire-success"),
        )
        .await;
        check("v2-source-false", &replay);
        assert_eq!(replay.body, first.body);
        assert_eq!(replay.protocol(), first.protocol());
    });
}

#[tokio::test]
async fn v2_zero_output_failed_direct_and_mpu_keep_original_error_wire_on_replay() {
    for entry in [Entry::Put, Entry::Mpu] {
        scenario!(entry, Outcome::Failed, |h| {
            let (name, first, replay) = match entry {
                Entry::Put => {
                    let first = request::put(
                        &h,
                        Outcome::Failed.archive(),
                        request::v2("plain-wire-failure"),
                    )
                    .await;
                    let replay = request::put(
                        &h,
                        Outcome::Failed.archive(),
                        request::v2("plain-wire-failure"),
                    )
                    .await;
                    ("v2-direct-failed", first, replay)
                }
                Entry::Mpu => {
                    let created = request::create(&h, request::v2("plain-wire-failure")).await;
                    assert_eq!(created.status, StatusCode::OK);
                    let id = created.field("UploadId");
                    let etag = request::upload(&h, &id, Outcome::Failed.archive()).await;
                    let first = request::complete(&h, &id, &etag).await;
                    let replay = request::complete(&h, &id, &etag).await;
                    ("v2-mpu-failed", first, replay)
                }
                Entry::Import => unreachable!(),
            };
            check(name, &first);
            check(name, &replay);
            assert_eq!(first.status, StatusCode::BAD_REQUEST);
            assert_eq!(replay.body, first.body);
            assert_eq!(replay.protocol(), first.protocol());
        });
    }
}
