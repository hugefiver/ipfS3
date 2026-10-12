//! R25 primary-review recovery regressions through authenticated production HTTP.
#[path = "support/stage9_zip_recovery_review.rs"]
mod review;
#[path = "support/stage9_zip_recovery_review_safety.rs"]
mod safety;
#[allow(dead_code)]
mod support;

use http::StatusCode;
use ipfs_s3_gateway::store::{multipart::v2_zip, zip::execution};
use review::*;

async fn assert_zero_mpu(empty: bool) {
    let (h, id, etag, first) = zero_mpu(empty).await;
    let expected = if empty { "empty" } else { "failed" };
    assert_eq!(first.headers["x-ipfs-s3-zip-batch-id"], id);
    assert_eq!(first.headers["x-ipfs-s3-zip-batch-status"], expected);
    assert!(!first.headers.contains_key(http::header::ETAG));
    assert!(!first.headers.contains_key("x-amz-version-id"));
    let intake = v2_zip::read_by_upload(h.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        serde_json::from_str::<serde_json::Value>(&intake.captured_options)
            .unwrap()
            .get("options")
            .is_some()
    );
    assert!(!intake.options().unwrap().publish_source);
    let terminal = execution::read(h.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.state, "completed");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(terminal.terminal_result.as_deref().unwrap())
            .unwrap()["status"],
        expected
    );
    let before = Effects::read(&h).await;
    for (table, n) in &before.rows {
        let expected = match *table {
            "zip_batches" => 1,
            "zip_v2_manifest" => i64::from(!empty),
            _ => 0,
        };
        assert_eq!(*n, expected, "{table}: {before:?}");
    }
    assert!(
        before
            .quota
            .iter()
            .all(|(_, bytes, pins)| *bytes == 0 && *pins == 0)
    );
    assert_eq!(count(&h, "multipart_uploads").await, 0);
    let get = status(&h, &id, KEY).await;
    eprintln!(
        "fresh MPU {expected}: Complete={} GET={}; counters={before:?}",
        first.status, get.status
    );
    assert_eq!(get.status, StatusCode::OK, "{get:?}");
    for marker in [
        format!("<State>{expected}</State>"),
        format!("<BatchStatus>{expected}</BatchStatus>"),
        "<SourcePublished>false</SourcePublished>".into(),
        "<RootStatus>empty</RootStatus>".into(),
    ] {
        assert!(get.body.contains(&marker), "{get:?}");
    }
    assert!(!get.body.contains("<RootCID>"));
    assert_eq!(
        status(&h, &id, "wrong-key.zip").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(complete(&h, &id, &etag).await, first);
    let repeated_status = status(&h, &id, KEY).await;
    assert_eq!(repeated_status.status, get.status);
    assert_eq!(repeated_status.body, get.body);
    // Status GET has a transport Date; Complete's saved receipt above does not.
    let mut original_headers = get.headers;
    let mut repeated_headers = repeated_status.headers;
    original_headers.remove(http::header::DATE);
    repeated_headers.remove(http::header::DATE);
    assert_eq!(repeated_headers, original_headers);
    assert_eq!(Effects::read(&h).await, before);
}

#[tokio::test]
async fn fresh_mpu_failed_receipt_get_is_200_and_repeats_original_400() {
    assert_zero_mpu(false).await;
}

#[tokio::test]
async fn fresh_mpu_empty_receipt_get_is_200_and_repeats_original_400() {
    assert_zero_mpu(true).await;
}

#[tokio::test]
async fn historical_bare_completed_mpu_status_remains_legal_but_bad_envelopes_do_not() {
    let (h, id, _, _) = zero_mpu(true).await;
    let intake = v2_zip::read_by_upload(h.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let original: serde_json::Value = serde_json::from_str(&intake.captured_options).unwrap();
    sql(
        &h,
        "UPDATE zip_v2_mpu_intakes SET captured_options=? WHERE original_upload_id=?",
        vec![original["options"].to_string().into(), id.clone().into()],
    )
    .await;
    assert_eq!(status(&h, &id, KEY).await.status, StatusCode::OK);
    let before = Effects::read(&h).await;
    for field in [
        "schema_version",
        "target_prefix",
        "rule_revision",
        "zip_intent",
    ] {
        let mut broken = original.clone();
        broken[field] = if field == "schema_version" {
            2.into()
        } else if field == "zip_intent" {
            serde_json::Value::Null
        } else {
            "wrong".into()
        };
        sql(
            &h,
            "UPDATE zip_v2_mpu_intakes SET captured_options=? WHERE original_upload_id=?",
            vec![broken.to_string().into(), id.clone().into()],
        )
        .await;
        let response = status(&h, &id, KEY).await;
        assert!(!response.status.is_success(), "bad {field}: {response:?}");
        assert!(!response.body.contains(ARCHIVE));
        assert_eq!(Effects::read(&h).await, before);
    }
}

#[tokio::test]
async fn pending_bound_matching_raw_body_recovers_original_artifact_without_new_archive_add() {
    let archive = support::decompress::legal_single_entry_zip();
    let (h, saved) = bound_pending("ordinary-recovery-token", &archive).await;
    let before = Effects::read(&h).await;
    let response = put(&h, &saved.token, archive.clone()).await;
    let after = Effects::read(&h).await;
    eprintln!(
        "bound pending matching: response={} before={before:?} after={after:?}",
        response.status
    );
    assert_eq!(response.status, StatusCode::OK, "{response:?}");
    assert_eq!(
        after.rpc_count("/api/v0/add") - before.rpc_count("/api/v0/add"),
        1
    );
    assert_eq!(
        after.rpc_count("/api/v0/cat") - before.rpc_count("/api/v0/cat"),
        1
    );
    assert_eq!(
        after.rpc_count("/api/v0/pin/add") - before.rpc_count("/api/v0/pin/add"),
        1
    );
    let finished = execution::read(h.state.store.db(), &saved.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.state, "completed");
    assert_eq!(finished.input_sha256, saved.input_sha256);
    assert_eq!(finished.input_art_cid, saved.input_art_cid);
    assert_eq!(finished.input_art_size, saved.input_art_size);
    assert_eq!(finished.captured_options, saved.captured_options);
    assert_eq!(finished.request_contract, saved.request_contract);
    assert_eq!(count(&h, "objects").await, 1);
    assert_eq!(count(&h, "object_versions").await, 1);
    let replay = put(&h, &saved.token, archive).await;
    assert_eq!(replay.status, response.status);
    assert_eq!(replay.body, response.body);
    assert_eq!(Effects::read(&h).await, after);
}

#[tokio::test]
async fn pending_bound_different_zip_comment_conflicts_with_zero_extra_rpc_or_publication() {
    for token in ["ordinary-token", "explicit-source-false-token"] {
        let mut archive = support::decompress::legal_single_entry_zip();
        if token == "explicit-source-false-token" {
            archive = with_comment(archive, b"same-size-original-comment");
        }
        let (h, saved) = bound_pending(token, &archive).await;
        let before = Effects::read(&h).await;
        let changed = if token == "explicit-source-false-token" {
            let last = archive.last_mut().unwrap();
            *last ^= 1;
            archive
        } else {
            with_comment(archive, b"same extracted entry, different raw ZIP")
        };
        let response = put(&h, token, changed).await;
        let after = Effects::read(&h).await;
        eprintln!(
            "bound pending raw mismatch: response={} before={before:?} after={after:?}",
            response.status
        );
        assert_eq!(response.status, StatusCode::CONFLICT, "{response:?}");
        assert_eq!(after, before);
        let remaining = execution::read(h.state.store.db(), &saved.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remaining.input_sha256, saved.input_sha256);
        assert_eq!(remaining.input_art_cid, saved.input_art_cid);
        assert_eq!(remaining.input_art_size, saved.input_art_size);
        assert_eq!(remaining.captured_options, saved.captured_options);
    }
}

#[tokio::test]
async fn partial_binding_and_null_original_capture_never_authorize_recovery() {
    for change in [
        "input_art_size=NULL",
        "captured_options=json_set(captured_options,'$.zip_intent',NULL)",
    ] {
        let archive = support::decompress::legal_single_entry_zip();
        let (h, saved) = bound_pending("incomplete-recovery", &archive).await;
        // Model a damaged historical row, without changing the production schema.
        sql(&h, "PRAGMA ignore_check_constraints=ON", vec![]).await;
        sql(
            &h,
            &format!("UPDATE zip_v2_executions SET {change} WHERE id=?"),
            vec![saved.id.into()],
        )
        .await;
        sql(&h, "PRAGMA ignore_check_constraints=OFF", vec![]).await;
        let before = Effects::read(&h).await;
        let response = put(&h, "incomplete-recovery", archive).await;
        assert!(!response.status.is_success(), "{response:?}");
        assert!(response.body.contains("needs_attention"), "{response:?}");
        assert!(!response.body.contains(ARCHIVE));
        assert_eq!(Effects::read(&h).await, before);
    }
}

#[tokio::test]
async fn missing_or_corrupt_bound_artifact_is_diagnostic_not_a_new_upload() {
    for artifact in ["missing", "oversized", "wrong-sha"] {
        let archive = support::decompress::legal_single_entry_zip();
        let (h, saved) = bound_pending("artifact-proof", &archive).await;
        if artifact == "missing" {
            wiremock::Mock::given(wiremock::matchers::path("/api/v0/cat"))
                .respond_with(wiremock::ResponseTemplate::new(404))
                .with_priority(1)
                .mount(&h.kubo)
                .await;
        } else if artifact == "oversized" {
            h.set_cat_body(
                ARCHIVE,
                with_comment(archive.clone(), b"corrupt stored bytes"),
            );
        } else {
            let mut corrupt = archive.clone();
            corrupt[0] ^= 1;
            h.set_cat_body(ARCHIVE, corrupt);
        }
        let before = Effects::read(&h).await;
        let response = put(&h, &saved.token, archive).await;
        assert!(!response.status.is_success(), "{response:?}");
        assert!(response.body.contains("needs_attention"), "{response:?}");
        assert!(!response.body.contains(ARCHIVE));
        let after = Effects::read(&h).await;
        assert_eq!(
            after.rpc_count("/api/v0/add"),
            before.rpc_count("/api/v0/add")
        );
        assert_eq!(
            after.rpc_count("/api/v0/pin/add"),
            before.rpc_count("/api/v0/pin/add")
        );
        assert_eq!(
            after.rpc_count("/api/v0/cat"),
            before.rpc_count("/api/v0/cat") + 1
        );
        assert_eq!(after.rows, before.rows);
        assert_eq!(after.quota, before.quota);
    }
}

#[tokio::test]
async fn pending_recovery_requires_fresh_exact_claim_and_unchanged_complete_binding() {
    for change in [
        "worker='successor-worker'",
        "input_art_cid='different-bound-artifact'",
        "input_art_size=input_art_size+1",
        "captured_options=json_set(captured_options,'$.rule_revision','different-capture')",
        "lease_until=datetime('now','-1 minute')",
    ] {
        let archive = support::decompress::legal_single_entry_zip();
        let (h, saved) = bound_pending("claim-race", &archive).await;
        // A competing writer changes durable evidence at the exact claim seam.
        sql(&h, &format!("CREATE TRIGGER recovery_claim_race AFTER UPDATE OF epoch ON zip_v2_executions BEGIN UPDATE zip_v2_executions SET {change} WHERE id=NEW.id; END"), vec![]).await;
        let before = Effects::read(&h).await;
        let response = put(&h, &saved.token, archive).await;
        assert_eq!(
            response.status,
            StatusCode::CONFLICT,
            "{change}: {response:?}"
        );
        assert_eq!(Effects::read(&h).await, before, "{change}");
    }
}

#[tokio::test]
async fn admitted_recovery_uses_only_frozen_manifest_and_whole_original_guard_group() {
    for lose_guard in [false, true] {
        let archive = support::decompress::legal_single_entry_zip();
        let (h, saved) = admitted_pending("frozen-admitted", &archive).await;
        // A frozen admitted manifest needs no source CAT, even when unavailable.
        wiremock::Mock::given(wiremock::matchers::path("/api/v0/cat"))
            .respond_with(wiremock::ResponseTemplate::new(404))
            .with_priority(1)
            .mount(&h.kubo)
            .await;
        if lose_guard {
            sql(
                &h,
                "DELETE FROM zip_v2_targets WHERE batch_id=?",
                vec![saved.id.clone().into()],
            )
            .await;
        }
        let before = Effects::read(&h).await;
        let response = put(&h, &saved.token, archive).await;
        let after = Effects::read(&h).await;
        assert_eq!(after.rpc, before.rpc);
        assert_eq!(after.quota, before.quota);
        let final_row = execution::read(h.state.store.db(), &saved.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_row.captured_options, saved.captured_options);
        if lose_guard {
            assert_eq!(response.status, StatusCode::CONFLICT, "{response:?}");
            assert_eq!(final_row.state, "fenced");
            assert_eq!(count(&h, "objects").await, 0);
            assert_eq!(count(&h, "zip_v2_targets").await, 0);
        } else {
            assert_eq!(response.status, StatusCode::OK, "{response:?}");
            assert_eq!(final_row.state, "completed");
            assert_eq!(count(&h, "objects").await, 1);
            assert_eq!(count(&h, "zip_v2_targets").await, 1);
        }
    }
}

#[tokio::test]
async fn pending_recovery_does_not_replace_the_current_s3_source_or_read_it_as_input() {
    let archive = support::decompress::legal_single_entry_zip();
    let (h, saved) = bound_pending("current-key-independent", &archive).await;
    wiremock::Mock::given(wiremock::matchers::path("/api/v0/add"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string("{\"Hash\":\"QmCurrentSource\",\"Size\":\"0\"}\n"),
        )
        .with_priority(1)
        .up_to_n_times(1)
        .mount(&h.kubo)
        .await;
    let current = request(
        &h,
        reqwest::Method::PUT,
        KEY,
        &[],
        b"current source is not the original ZIP".to_vec(),
        http::HeaderMap::new(),
    )
    .await;
    assert_eq!(current.status, StatusCode::OK, "{current:?}");
    let source_before =
        ipfs_s3_gateway::store::object::get_latest(h.state.store.db(), &h.bucket, KEY)
            .await
            .unwrap();
    let before = Effects::read(&h).await;
    let response = put(&h, &saved.token, archive).await;
    assert_eq!(response.status, StatusCode::OK, "{response:?}");
    assert!(!response.headers.contains_key(http::header::ETAG));
    assert!(!response.headers.contains_key("x-amz-version-id"));
    let after = Effects::read(&h).await;
    assert_eq!(
        after.rpc_count("/api/v0/cat"),
        before.rpc_count("/api/v0/cat") + 1
    );
    assert_eq!(after.rpc.last().unwrap().0, "/api/v0/pin/add");
    assert!(
        after
            .rpc
            .iter()
            .filter(|(path, _)| path == "/api/v0/cat")
            .all(|(_, query)| query == &format!("arg={ARCHIVE}"))
    );
    assert_eq!(
        ipfs_s3_gateway::store::object::get_latest(h.state.store.db(), &h.bucket, KEY)
            .await
            .unwrap(),
        source_before
    );
}

#[tokio::test]
async fn bound_pending_chunked_body_is_verified_to_eof_before_recovery_or_conflict() {
    for valid in [true, false] {
        let archive = support::decompress::legal_single_entry_zip();
        let (h, saved) = bound_pending("chunked-ordinary-token", &archive).await;
        let mut sent = archive.clone();
        if !valid {
            sent.push(b'x');
        }
        let split = sent.len() - 1;
        let before = Effects::read(&h).await;
        let response = Receipt::read(
            support::sigv4::send_sigv4_chunked_http1(
                reqwest::Method::PUT,
                &h.endpoint,
                &h.bucket,
                KEY,
                &[("decompress-zip", "out/")],
                vec![
                    bytes::Bytes::copy_from_slice(&sent[..split]),
                    bytes::Bytes::copy_from_slice(&sent[split..]),
                ],
                controls(&saved.token),
                "test",
            )
            .await,
        )
        .await;
        if valid {
            assert_eq!(response.status, StatusCode::OK, "{response:?}");
            assert_eq!(
                Effects::read(&h).await.rpc_count("/api/v0/add"),
                before.rpc_count("/api/v0/add") + 1
            );
        } else {
            assert_eq!(response.status, StatusCode::BAD_REQUEST, "{response:?}");
            assert_eq!(Effects::read(&h).await, before);
        }
    }
}

#[tokio::test]
async fn current_explicit_deny_rejects_bound_remote_pending_before_any_new_io() {
    use ipfs_s3_gateway::pinning::zip_policy::ZipRuleEffect;
    let mut fixture = safety::SafetyFixture::new(false).await;
    let saved = fixture
        .pending_direct("current-safety-bound", "extracted")
        .await;
    fixture
        .change_current_rule(ZipRuleEffect::Deny, "test-bkt", "out/")
        .await;
    for archive in [
        fixture.archive.clone(),
        with_comment(fixture.archive.clone(), b"different raw ZIP"),
    ] {
        let before = Effects::read(&fixture.h).await;
        let response = fixture.direct(&saved.token, "extracted", archive).await;
        assert_eq!(response.status, StatusCode::CONFLICT, "{response:?}");
        assert_eq!(Effects::read(&fixture.h).await, before);
        assert_eq!(
            execution::read(fixture.h.state.store.db(), &saved.id)
                .await
                .unwrap()
                .unwrap()
                .captured_options,
            saved.captured_options
        );
    }
    // A safety veto cannot bypass the raw body's clean EOF/trailer contract.
    let mut incomplete = fixture.archive.clone();
    incomplete.pop();
    let before = Effects::read(&fixture.h).await;
    assert_eq!(
        fixture
            .direct(&saved.token, "extracted", incomplete)
            .await
            .status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(Effects::read(&fixture.h).await, before);
}

#[tokio::test]
async fn current_explicit_deny_blocks_mpu_before_assembly_without_rewriting_capture() {
    use ipfs_s3_gateway::pinning::zip_policy::ZipRuleEffect;
    let mut fixture = safety::SafetyFixture::new(true).await;
    let (id, etag) = fixture.create_and_upload("current-safety-mpu").await;
    let saved = v2_zip::read_by_upload(fixture.h.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    fixture
        .change_current_rule(ZipRuleEffect::Deny, "test-bkt", "")
        .await;
    let before = Effects::read(&fixture.h).await;
    let response = complete(&fixture.h, &id, &etag).await;
    assert_eq!(response.status, StatusCode::CONFLICT, "{response:?}");
    assert_eq!(Effects::read(&fixture.h).await, before);
    let retained = v2_zip::read_by_upload(fixture.h.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.captured_options, saved.captured_options);
    assert_eq!(retained.captured_config, saved.captured_config);
    assert_eq!(retained.rule_revision, saved.rule_revision);
    assert_eq!(retained.request_fingerprint, saved.request_fingerprint);
    assert_eq!(retained.active_upload_id, saved.active_upload_id);
    assert_eq!(count(&fixture.h, "objects").await, 0);
    assert_eq!(count(&fixture.h, "multipart_uploads").await, 1);
}

#[tokio::test]
async fn current_unrelated_deny_or_allow_changes_do_not_freeze_original_capture() {
    use ipfs_s3_gateway::pinning::zip_policy::ZipRuleEffect;
    for (effect, bucket, prefix, targets) in [
        (ZipRuleEffect::Deny, "other-bucket", "", "extracted"),
        (ZipRuleEffect::Deny, "test-bkt", "unrelated/", "extracted"),
        (ZipRuleEffect::Allow, "test-bkt", "out/", "extracted"),
        (ZipRuleEffect::Deny, "test-bkt", "", "none"),
    ] {
        let mut fixture = safety::SafetyFixture::new(false).await;
        let saved = fixture
            .pending_direct("unrelated-safety-change", targets)
            .await;
        fixture.change_current_rule(effect, bucket, prefix).await;
        let response = fixture
            .direct(&saved.token, targets, fixture.archive.clone())
            .await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "{effect:?}/{bucket}/{prefix}/{targets}: {response:?}"
        );
        assert_eq!(
            execution::read(fixture.h.state.store.db(), &saved.id)
                .await
                .unwrap()
                .unwrap()
                .captured_options,
            saved.captured_options
        );
        if effect == ZipRuleEffect::Allow {
            use sea_orm::EntityTrait as _;
            let leases = ipfs_s3_gateway::store::entities::pin_lease::Entity::find()
                .all(fixture.h.state.store.db())
                .await
                .unwrap();
            assert_eq!(leases.len(), 1);
            assert_eq!(
                (leases[0].expires_at - leases[0].created_at).num_seconds(),
                3600
            );
        }
    }
}

#[tokio::test]
async fn original_deny_stays_local_only_not_a_new_safety_revocation() {
    use ipfs_s3_gateway::pinning::zip_policy::ZipRuleEffect;
    let mut fixture = safety::SafetyFixture::new(false).await;
    fixture
        .change_current_rule(ZipRuleEffect::Deny, "test-bkt", "out/")
        .await;
    let saved = fixture
        .pending_direct("original-deny-local", "extracted")
        .await;
    fixture
        .change_current_rule(ZipRuleEffect::Deny, "test-bkt", "out/file.txt")
        .await;
    let response = fixture
        .direct(&saved.token, "extracted", fixture.archive.clone())
        .await;
    assert_eq!(response.status, StatusCode::OK, "{response:?}");
    assert_eq!(count(&fixture.h, "objects").await, 1);
    assert_eq!(count(&fixture.h, "pin_leases").await, 0);
    assert_eq!(count(&fixture.h, "pin_jobs").await, 0);
    assert_eq!(
        execution::read(fixture.h.state.store.db(), &saved.id)
            .await
            .unwrap()
            .unwrap()
            .captured_options,
        saved.captured_options
    );
}

#[tokio::test]
async fn current_deny_does_not_reexecute_or_rewrite_published_direct_or_mpu_receipts() {
    use ipfs_s3_gateway::pinning::zip_policy::ZipRuleEffect;
    let mut direct = safety::SafetyFixture::new(false).await;
    let first = direct
        .direct(
            "terminal-safety-direct",
            "extracted",
            direct.archive.clone(),
        )
        .await;
    assert_eq!(first.status, StatusCode::OK, "{first:?}");
    direct
        .change_current_rule(ZipRuleEffect::Deny, "test-bkt", "")
        .await;
    let before = Effects::read(&direct.h).await;
    let repeated = direct
        .direct(
            "terminal-safety-direct",
            "extracted",
            direct.archive.clone(),
        )
        .await;
    assert_eq!(repeated.status, first.status);
    assert_eq!(repeated.body, first.body);
    assert_eq!(
        direct
            .direct(
                "terminal-safety-direct",
                "extracted",
                with_comment(direct.archive.clone(), b"changed after terminal")
            )
            .await
            .status,
        StatusCode::CONFLICT
    );
    assert_eq!(Effects::read(&direct.h).await, before);

    let mut mpu = safety::SafetyFixture::new(true).await;
    let (id, etag) = mpu.create_and_upload("terminal-safety-mpu").await;
    let first = complete(&mpu.h, &id, &etag).await;
    assert_eq!(first.status, StatusCode::OK, "{first:?}");
    mpu.change_current_rule(ZipRuleEffect::Deny, "test-bkt", "")
        .await;
    let before = Effects::read(&mpu.h).await;
    assert_eq!(complete(&mpu.h, &id, &etag).await, first);
    assert_eq!(
        complete(&mpu.h, &id, "\"different part\"").await.status,
        StatusCode::CONFLICT
    );
    assert_eq!(Effects::read(&mpu.h).await, before);
}

#[tokio::test]
async fn persisted_mpu_decode_contradictions_are_409_but_malformed_client_stays_400() {
    let fixture = safety::SafetyFixture::new(true).await;
    let (id, etag) = fixture.create_and_upload("persisted-decode-conflict").await;
    let saved = v2_zip::read_by_upload(fixture.h.state.store.db(), &id)
        .await
        .unwrap()
        .unwrap();
    let original: serde_json::Value = serde_json::from_str(&saved.captured_options).unwrap();
    let before = Effects::read(&fixture.h).await;
    for (pointer, value) in [
        ("/schema_version", serde_json::json!(2)),
        ("/options/token", serde_json::json!("other")),
        ("/options/publish_source", serde_json::json!(false)),
        ("/options/root_enabled", serde_json::json!(true)),
        ("/zip_intent/schema_version", serde_json::json!(2)),
        (
            "/zip_intent/source_decision/origin/principal_id",
            serde_json::json!("other"),
        ),
        ("/target_prefix", serde_json::json!("other/")),
        ("/rule_revision", serde_json::json!("other")),
    ] {
        let mut tampered = original.clone();
        *tampered.pointer_mut(pointer).unwrap() = value;
        for (table, identity) in [
            ("zip_v2_mpu_intakes", "original_upload_id"),
            ("zip_v2_executions", "id"),
        ] {
            sql(
                &fixture.h,
                &format!("UPDATE {table} SET captured_options=? WHERE {identity}=?"),
                vec![tampered.to_string().into(), id.clone().into()],
            )
            .await;
        }
        let response = complete(&fixture.h, &id, &etag).await;
        assert_eq!(
            response.status,
            StatusCode::CONFLICT,
            "{pointer}: {response:?}"
        );
        assert!(
            response.body.contains("<Code>OperationAborted</Code>"),
            "{response:?}"
        );
        assert_eq!(Effects::read(&fixture.h).await, before);
    }
    for (table, identity) in [
        ("zip_v2_mpu_intakes", "original_upload_id"),
        ("zip_v2_executions", "id"),
    ] {
        sql(
            &fixture.h,
            &format!("UPDATE {table} SET captured_options=? WHERE {identity}=?"),
            vec![saved.captured_options.clone().into(), id.clone().into()],
        )
        .await;
    }
    let mut headers = safety::remote_controls("malformed-client", "both", true);
    headers.insert("x-ipfs3-zip-publish-source", "not-a-bool".parse().unwrap());
    let response = request(
        &fixture.h,
        reqwest::Method::POST,
        KEY,
        &[("uploads", ""), ("decompress-zip", "out/")],
        vec![],
        headers,
    )
    .await;
    assert_eq!(response.status, StatusCode::BAD_REQUEST, "{response:?}");
    assert!(
        response.body.contains("<Code>InvalidRequest</Code>"),
        "{response:?}"
    );
    assert_eq!(Effects::read(&fixture.h).await, before);
}

async fn assert_p2_explicit_source_is_local_only(
    mpu: bool,
    authority: safety::LocalSourceAuthority,
) {
    use ipfs_s3_gateway::pinning::zip_policy::ZipRuleEffect;
    let mut fixture = safety::SafetyFixture::new(mpu).await;
    fixture.original_local_source(authority).await;
    let token = format!("p2-local-source-{mpu}-{authority:?}");
    let (saved, etag) = if mpu {
        let (id, etag) = fixture.create_and_upload_source_only(&token).await;
        (
            execution::read(fixture.h.state.store.db(), &id)
                .await
                .unwrap()
                .unwrap(),
            Some(etag),
        )
    } else {
        (fixture.pending_direct_source_only(&token).await, None)
    };
    let envelope: serde_json::Value = serde_json::from_str(&saved.captured_options).unwrap();
    let capture = ipfs_s3_gateway::pinning::zip_policy::CapturedZipIntent::decode_json(
        &envelope["zip_intent"].to_string(),
    )
    .unwrap();
    assert!(capture.controls().targets.source);
    assert!(!capture.controls().targets.extracted);
    assert!(capture.source_decision().effective_intents.is_empty());
    fixture
        .change_current_rule(ZipRuleEffect::Deny, "test-bkt", KEY)
        .await;
    let before = Effects::read(&fixture.h).await;
    let response = if let Some(etag) = &etag {
        complete(&fixture.h, &saved.id, etag).await
    } else {
        fixture.direct_source_only(&token).await
    };
    let after = Effects::read(&fixture.h).await;
    eprintln!(
        "P2 original local-only {mpu}/{authority:?}: {} before={before:?} after={after:?}",
        response.status
    );
    assert_eq!(response.status, StatusCode::OK, "{response:?}");
    assert_eq!(
        after.rpc_count("/api/v0/add"),
        before.rpc_count("/api/v0/add") + usize::from(mpu)
    );
    assert_eq!(
        after.rpc_count("/api/v0/pin/add"),
        before.rpc_count("/api/v0/pin/add") + usize::from(mpu)
    );
    assert_eq!(
        after.rpc_count("/api/v0/cat"),
        before.rpc_count("/api/v0/cat") + 1
    );
    assert_eq!(after.quota, before.quota);
    for table in [
        "pin_leases",
        "pin_lease_targets",
        "pin_jobs",
        "remote_pins",
        "zip_v2_manifest",
        "zip_v2_targets",
    ] {
        assert_eq!(count(&fixture.h, table).await, 0, "{table}: {after:?}");
    }
    assert_eq!(count(&fixture.h, "objects").await, 1);
    assert_eq!(count(&fixture.h, "object_versions").await, 1);
    assert_eq!(
        response.headers[http::header::ETAG]
            .to_str()
            .unwrap()
            .trim_matches('"'),
        ARCHIVE
    );
    let source = ipfs_s3_gateway::store::object::get_latest(
        fixture.h.state.store.db(),
        &fixture.h.bucket,
        KEY,
    )
    .await
    .unwrap();
    assert_eq!(source.cid, ARCHIVE);
    let finished = execution::read(fixture.h.state.store.db(), &saved.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(finished.state, "completed");
    assert_eq!(finished.captured_options, saved.captured_options);
    assert_eq!(finished.request_contract, saved.request_contract);
    assert_eq!(finished.request_fingerprint, saved.request_fingerprint);
    if !mpu {
        assert_eq!(finished.input_sha256, saved.input_sha256);
        assert_eq!(finished.input_art_cid, saved.input_art_cid);
        assert_eq!(finished.input_art_size, saved.input_art_size);
    }
    let get = status(&fixture.h, &saved.id, KEY).await;
    assert_eq!(get.status, StatusCode::OK, "{get:?}");
    assert!(
        get.body.contains("<SourcePublished>true</SourcePublished>"),
        "{get:?}"
    );
    let replay = if let Some(etag) = &etag {
        complete(&fixture.h, &saved.id, etag).await
    } else {
        fixture.direct_source_only(&token).await
    };
    assert_eq!(replay.status, response.status);
    assert_eq!(replay.body, response.body);
    assert_eq!(Effects::read(&fixture.h).await, after);
}

macro_rules! p2_local_source_case {
    ($name:ident, $mpu:expr, $authority:ident) => {
        #[tokio::test]
        async fn $name() {
            assert_p2_explicit_source_is_local_only($mpu, safety::LocalSourceAuthority::$authority)
                .await;
        }
    };
}
p2_local_source_case!(
    p2_direct_explicit_source_empty_rules_publishes_without_remote_leases,
    false,
    EmptyRules
);
p2_local_source_case!(
    p2_mpu_explicit_source_empty_rules_publishes_without_remote_leases,
    true,
    EmptyRules
);
p2_local_source_case!(
    p2_direct_explicit_source_no_matching_rule_publishes_without_remote_leases,
    false,
    NoMatchingRule
);
p2_local_source_case!(
    p2_mpu_explicit_source_no_matching_rule_publishes_without_remote_leases,
    true,
    NoMatchingRule
);
p2_local_source_case!(
    p2_direct_explicit_source_no_available_provider_publishes_without_remote_leases,
    false,
    NoAvailableProvider
);
p2_local_source_case!(
    p2_mpu_explicit_source_no_available_provider_publishes_without_remote_leases,
    true,
    NoAvailableProvider
);

async fn assert_p2_admitted_exact_outputs(mpu: bool, intersects: bool) {
    use ipfs_s3_gateway::pinning::zip_policy::ZipRuleEffect;
    let mut fixture = safety::SafetyFixture::with_archive(mpu, safety::public_entry_zip()).await;
    let token = format!("p2-exact-successes-{mpu}-{intersects}");
    let upload = if mpu {
        Some(fixture.create_and_upload(&token).await)
    } else {
        None
    };
    sql(&fixture.h, "CREATE TRIGGER safety_publication_rollback BEFORE INSERT ON object_versions BEGIN SELECT RAISE(ABORT, 'fixture admitted publication rollback'); END", vec![]).await;
    let first = if let Some((id, etag)) = &upload {
        complete(&fixture.h, id, etag).await
    } else {
        fixture
            .direct(&token, "extracted", fixture.archive.clone())
            .await
    };
    assert_eq!(first.status, StatusCode::INTERNAL_SERVER_ERROR, "{first:?}");
    sql(
        &fixture.h,
        "DROP TRIGGER safety_publication_rollback",
        vec![],
    )
    .await;
    let saved = execution::read_by_identity(
        fixture.h.state.store.db(),
        "test",
        if mpu { "mpu" } else { "direct" },
        &token,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(saved.state, "admitted");
    let batch = ipfs_s3_gateway::store::zip::snapshot(fixture.h.state.store.db(), &saved.id)
        .await
        .unwrap()
        .unwrap();
    assert!(batch.batch.manifest_prepared);
    assert_eq!(batch.batch.state, "open");
    assert_eq!(batch.entries.len(), 1);
    assert_eq!(
        batch.entries[0].object_key.as_deref(),
        Some("out/public/file.txt")
    );
    assert_eq!(batch.entries[0].cid.as_deref(), Some(OUTPUT));
    assert_eq!(batch.entries[0].version_row_id, None);
    assert_eq!(count(&fixture.h, "objects").await, 0);
    assert_eq!(count(&fixture.h, "object_versions").await, 0);
    expire(&fixture.h, &saved.id).await;
    fixture
        .change_current_rule(
            ZipRuleEffect::Deny,
            "test-bkt",
            if intersects {
                "out/public/"
            } else {
                "out/private/"
            },
        )
        .await;
    let before = Effects::read(&fixture.h).await;
    let response = if let Some((id, etag)) = &upload {
        complete(&fixture.h, id, etag).await
    } else {
        fixture
            .direct(&token, "extracted", fixture.archive.clone())
            .await
    };
    let after = Effects::read(&fixture.h).await;
    eprintln!(
        "P2 admitted exact success {mpu}/intersects={intersects}: {} before={before:?} after={after:?}",
        response.status
    );
    assert_eq!(
        after.rpc, before.rpc,
        "admitted must never CAT/extract/add/pin/root-fetch"
    );
    let remaining = execution::read(fixture.h.state.store.db(), &saved.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remaining.captured_options, saved.captured_options);
    assert_eq!(remaining.request_contract, saved.request_contract);
    assert_eq!(remaining.request_fingerprint, saved.request_fingerprint);
    assert_eq!(remaining.input_sha256, saved.input_sha256);
    assert_eq!(remaining.input_art_cid, saved.input_art_cid);
    assert_eq!(remaining.input_art_size, saved.input_art_size);
    if intersects {
        assert_eq!(response.status, StatusCode::CONFLICT, "{response:?}");
        assert_eq!(remaining.state, "admitted");
        assert_eq!(after, before);
    } else {
        assert_eq!(response.status, StatusCode::OK, "{response:?}");
        assert_eq!(remaining.state, "completed");
        assert_eq!(count(&fixture.h, "objects").await, if mpu { 2 } else { 1 });
        assert_eq!(
            count(&fixture.h, "object_versions").await,
            if mpu { 2 } else { 1 }
        );
        assert_eq!(
            count(&fixture.h, "pin_leases").await,
            if mpu { 2 } else { 1 }
        );
        assert_eq!(
            count(&fixture.h, "pin_lease_targets").await,
            if mpu { 2 } else { 1 }
        );
        assert_eq!(count(&fixture.h, "pin_jobs").await, if mpu { 2 } else { 1 });
        assert_eq!(
            count(&fixture.h, "remote_pins").await,
            if mpu { 2 } else { 1 }
        );
        assert_eq!(after.quota.len(), 1);
        assert_eq!(
            after.quota[0].1,
            18 + if mpu { fixture.archive.len() as i64 } else { 0 }
        );
        assert_eq!(after.quota[0].2, if mpu { 2 } else { 1 });
        assert_eq!(
            ipfs_s3_gateway::store::object::get_latest(
                fixture.h.state.store.db(),
                &fixture.h.bucket,
                "out/public/file.txt"
            )
            .await
            .unwrap()
            .cid,
            OUTPUT
        );
    }
}

#[tokio::test]
async fn p2_direct_admitted_disjoint_current_deny_publishes_exact_successes_without_io() {
    assert_p2_admitted_exact_outputs(false, false).await;
}
#[tokio::test]
async fn p2_mpu_admitted_disjoint_current_deny_publishes_exact_successes_without_io() {
    assert_p2_admitted_exact_outputs(true, false).await;
}
#[tokio::test]
async fn p2_direct_admitted_intersecting_current_deny_has_zero_new_io_or_publication() {
    assert_p2_admitted_exact_outputs(false, true).await;
}
#[tokio::test]
async fn p2_mpu_admitted_intersecting_current_deny_has_zero_new_io_or_publication() {
    assert_p2_admitted_exact_outputs(true, true).await;
}
