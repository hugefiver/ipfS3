//! R25 primary-review #2/#4: real signed ingress and zero-side-effect rejection.
#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
    pub mod stage9_zip_controls_review;
    pub mod stage9_zip_controls_review_sigv4;
    pub mod stage9_zip_replay_source;
    pub mod stage9_zip_wire;
    pub mod stage9_zip_wire_golden;
    pub mod stage9_zip_wire_requests;
}

use futures_util::FutureExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use ipfs_s3_gateway::{
    pinning::tags::{ObjectTag, parse_tagging_header},
    store,
};
use std::panic::AssertUnwindSafe;
use support::{
    decompress::S3TestEndpoint,
    sigv4::{presign_sigv4_query, send_sigv4},
    stage9_zip_controls_review::{
        BUCKET, Entry, Fixture, KEY, rejected_unchanged, snapshot, tagging,
    },
};

macro_rules! scenario {
    (|$h:ident| $body:block) => {
        {
            let $h = Fixture::new().await;
            let result = AssertUnwindSafe(async $body).catch_unwind().await;
            $h.shutdown().await;
            if let Err(error) = result { std::panic::resume_unwind(error); }
        }
    };
}

#[tokio::test]
async fn signed_ordinary_put_zip_header_cannot_publish_or_overwrite() {
    scenario!(|h| {
        for overwrite in [false, true] {
            if overwrite {
                let (status, body) = h
                    .request(Entry::Ordinary, BUCKET, &[], tagging("team=existing"))
                    .await;
                assert_eq!(status, StatusCode::OK, "seed: {body}");
            }
            let mut headers = HeaderMap::new();
            headers.insert(
                "x-ipfs3-zip-publish-source",
                HeaderValue::from_static("maybe"),
            );
            rejected_unchanged(&h, Entry::Ordinary, BUCKET, &[], headers, "InvalidRequest").await;
        }
    });
}

async fn bad_root(entry: Entry) {
    scenario!(|h| {
        rejected_unchanged(
            &h,
            entry,
            BUCKET,
            &entry.query(),
            tagging("ipfs-s3%3Azip-root=TRUE"),
            "InvalidRequest",
        )
        .await;
    });
}

#[tokio::test]
async fn signed_zip_put_malformed_root_is_invalid_request_before_publication() {
    bad_root(Entry::ZipPut).await;
}

#[tokio::test]
async fn signed_zip_mpu_create_malformed_root_is_invalid_request_before_intake() {
    bad_root(Entry::Mpu).await;
}

#[tokio::test]
async fn signed_zip_import_malformed_root_is_invalid_request_before_source_or_intake() {
    bad_root(Entry::Import).await;
}

#[tokio::test]
async fn ordinary_put_rejects_all_zip_control_forms_in_current_bucket_states() {
    for versioned in [false, true] {
        scenario!(|h| {
            if versioned {
                let response = send_sigv4(
                    reqwest::Method::PUT,
                    h.endpoint(),
                    BUCKET,
                    "",
                    &[("versioning", "")],
                    b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
                        .to_vec(),
                    HeaderMap::new(),
                    "test",
                )
                .await;
                assert_eq!(response.status(), StatusCode::OK);
                response.bytes().await.unwrap();
            }
            for overwrite in [false, true] {
                if overwrite {
                    let mut headers =
                        tagging("team=existing&ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1h");
                    headers.insert(
                        "x-amz-meta-review",
                        HeaderValue::from_static("preserve-this"),
                    );
                    let (status, body) = h.request(Entry::Ordinary, BUCKET, &[], headers).await;
                    assert_eq!(status, StatusCode::OK, "seed: {body}");
                    let seeded = snapshot(h.state.store.db()).await;
                    for table in [
                        "objects",
                        "object_versions",
                        "pin_leases",
                        "pin_lease_targets",
                        "pin_provider_usage",
                        "pin_jobs",
                    ] {
                        assert!(
                            !seeded[table].is_empty(),
                            "nonempty overwrite witness: {table}"
                        );
                    }
                }
                for (name, value) in [
                    ("x-ipfs3-zip-unknown", "ignored"),
                    ("x-ipfs3-zip-publish-source", "maybe"),
                    ("x-ipfs3-zip-publish-source", "true"),
                    ("x-ipfs3-zip-contract", "v2"),
                    ("x-ipfs3-zip-expected-sha256", "a"),
                ] {
                    let mut headers = HeaderMap::new();
                    headers.insert(
                        http::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                        HeaderValue::from_str(value).unwrap(),
                    );
                    rejected_unchanged(&h, Entry::Ordinary, BUCKET, &[], headers, "InvalidRequest")
                        .await;
                }
                for query in [
                    vec![("x-ipfs3-zip-unknown", "true")],
                    vec![("ipfs3-zip-unknown", "true")],
                    vec![("decompress-zip-prefix", "out/")],
                    vec![("decompress-zip-result", "true")],
                    vec![("decompress-zip-result", "TRUE")],
                    vec![
                        ("decompress-zip-result", "false"),
                        ("decompress-zip-result", "false"),
                    ],
                ] {
                    rejected_unchanged(
                        &h,
                        Entry::Ordinary,
                        BUCKET,
                        &query,
                        HeaderMap::new(),
                        "InvalidRequest",
                    )
                    .await;
                }
                for tags in [
                    "ipfs-s3%3Azip-root=false",
                    "ipfs-s3%3Azip-root=TRUE",
                    "ipfs-s3%3Azip-unknown=true",
                    "ipfs-s3%3Azip-root=false&ipfs-s3:zip-root=false",
                    "ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed",
                ] {
                    rejected_unchanged(
                        &h,
                        Entry::Ordinary,
                        BUCKET,
                        &[],
                        tagging(tags),
                        "InvalidRequest",
                    )
                    .await;
                }
            }
            let mut headers = HeaderMap::new();
            headers.insert("x-ipfs3-zip-unknown", HeaderValue::from_static("true"));
            rejected_unchanged(
                &h,
                Entry::Ordinary,
                "absent-bucket",
                &[],
                headers,
                "InvalidRequest",
            )
            .await;
        });
    }
}

#[tokio::test]
async fn three_zip_entries_preserve_tag_multiplicity_and_reject_malformed_controls() {
    for entry in [Entry::ZipPut, Entry::Mpu, Entry::Import] {
        scenario!(|h| {
            for v2 in [false, true] {
                for value in [
                    "ipfs-s3%3Azip-root=TRUE",
                    "ipfs-s3%3Azip-root=true&ipfs-s3%3Azip-root=true",
                    "ipfs-s3%3Azip-unknown=true",
                    "ipfs-s3%3azip-root=true&%69pfs-s3:zip-root=false",
                    "%69pfs-s3%3azip-unknown=true",
                    "ipfs-s3%3Azip-root=%GG",
                    "ipfs-s3%3Azip-root",
                    "ipfs-s3%3Apin=TRUE",
                    "ipfs-s3%3Apin=true&ipfs-s3%3Aduration=bad",
                    "ipfs-s3%3Apin=true&ipfs-s3%3Acontent=bad",
                    "ipfs-s3%3Apin=true&ipfs-s3%3Apin=true",
                    "ipfs-s3%3Aunknown=true",
                ] {
                    let mut headers = tagging(value);
                    if v2 {
                        headers.insert("x-ipfs3-zip-contract", HeaderValue::from_static("v2"));
                        headers.insert("x-ipfs3-zip-token", HeaderValue::from_static("bad-tags"));
                    }
                    rejected_unchanged(
                        &h,
                        entry,
                        BUCKET,
                        &entry.query(),
                        headers,
                        "InvalidRequest",
                    )
                    .await;
                }
            }
        });
    }
}

#[tokio::test]
async fn structural_controls_do_not_reclassify_policy_ttl_or_generic_user_tag_errors() {
    let eleven = (0..10)
        .map(|i| format!("user{i}=value"))
        .chain(["ipfs-s3%3Azip-root=false".into()])
        .collect::<Vec<_>>()
        .join("&");
    for entry in [Entry::Ordinary, Entry::ZipPut, Entry::Mpu, Entry::Import] {
        scenario!(|h| {
            for tags in [
                "ipfs-s3%3Apin=true&ipfs-s3%3Aduration=25h",
                "user=first&user=second",
                "user=%GG",
                "=value",
                &format!("{}=value", "k".repeat(129)),
                &format!("user={}", "v".repeat(257)),
            ] {
                rejected_unchanged(
                    &h,
                    entry,
                    BUCKET,
                    &entry.query(),
                    tagging(tags),
                    "InvalidArgument",
                )
                .await;
            }
            if !matches!(entry, Entry::Ordinary) {
                rejected_unchanged(
                    &h,
                    entry,
                    BUCKET,
                    &entry.query(),
                    tagging(&eleven),
                    "InvalidArgument",
                )
                .await;
            } else {
                for value in [
                    "ipfs-s3%3Apin=TRUE",
                    "ipfs-s3%3Aunknown=true",
                    "ipfs-s3%3Apin=true&ipfs-s3%3Apin=true",
                ] {
                    rejected_unchanged(&h, entry, BUCKET, &[], tagging(value), "InvalidArgument")
                        .await;
                }
            }
        });
    }
}

#[tokio::test]
async fn ordinary_mpu_and_import_keep_legacy_pin_errors_but_reject_zip_only_content() {
    for (entry, query) in [
        (Entry::Mpu, vec![("uploads", "")]),
        (Entry::Import, vec![("ipfs3-import", "")]),
    ] {
        scenario!(|h| {
            for value in [
                "ipfs-s3%3Apin=TRUE",
                "ipfs-s3%3Aunknown=true",
                "ipfs-s3%3Apin=true&ipfs-s3%3Apin=true",
                "ipfs-s3%3Apin=true&ipfs-s3%3Aduration=25h",
            ] {
                rejected_unchanged(&h, entry, BUCKET, &query, tagging(value), "InvalidArgument")
                    .await;
            }
            rejected_unchanged(
                &h,
                entry,
                BUCKET,
                &query,
                tagging("ipfs-s3%3Apin=true&ipfs-s3%3Acontent=decompressed"),
                "InvalidRequest",
            )
            .await;
        });
    }
}

#[tokio::test]
async fn ordinary_untagged_user_tags_sse_and_manual_pin_still_publish() {
    scenario!(|h| {
        for (tag_value, encrypted) in [
            ("", false),
            ("team=R%26D&space=hello+world&literal=hello%2Bworld", false),
            ("ipfs-s3%253Azip-root=TRUE", false),
            ("team=sse", true),
            ("ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1h", false),
        ] {
            let mut headers = if tag_value.is_empty() {
                HeaderMap::new()
            } else {
                tagging(tag_value)
            };
            if encrypted {
                headers.insert(
                    "x-amz-server-side-encryption",
                    HeaderValue::from_static("AES256"),
                );
            }
            let before_rpc = h.kubo.received_requests().await.unwrap().len();
            let (status, body) = h.request(Entry::Ordinary, BUCKET, &[], headers).await;
            assert_eq!(status, StatusCode::OK, "{tag_value}: {body}");
            let object = store::object::get_latest(h.state.store.db(), BUCKET, KEY)
                .await
                .unwrap();
            assert_eq!(object.encrypted, encrypted);
            assert_eq!(object.size, 11);
            let mut expected = parse_tagging_header(tag_value).unwrap();
            expected.sort_by(|a, b| a.key.cmp(&b.key));
            assert_eq!(
                store::pinning::tags::list_object_tags(h.state.store.db(), &object.id)
                    .await
                    .unwrap(),
                expected
            );
            assert_eq!(
                h.kubo.received_requests().await.unwrap().len(),
                before_rpc + 2
            );
        }
    });
}

#[tokio::test]
async fn percent_encoded_authenticated_query_names_cannot_escape_ordinary_put() {
    scenario!(|h| {
        // Re-encoding a signed semantic query name keeps SigV4 canonicalization
        // equivalent; these are not signatures over double-encoded literals.
        for query in [
            vec![("x-ipfs3-zip-unknown", "true")],
            vec![("ipfs3-zip-unknown", "true")],
            vec![("decompress-zip-prefix", "out/")],
            vec![
                ("decompress-zip-result", "true"),
                ("decompress-zip-result", "true"),
            ],
        ] {
            let before = snapshot(h.state.store.db()).await;
            let rpc_before = h.kubo.received_requests().await.unwrap().len();
            let url = presign_sigv4_query(
                &reqwest::Method::PUT,
                h.endpoint(),
                BUCKET,
                KEY,
                &query,
                "test",
                "test",
                None,
                60,
                chrono::Utc::now(),
            );
            let name = query[0].0;
            let encoded = name.replace('-', "%2D");
            let url = url.replacen(&format!("{name}="), &format!("{encoded}="), 1);
            let response = reqwest::Client::new()
                .put(url)
                .body("new content")
                .send()
                .await
                .unwrap();
            let status = response.status();
            let body = response.text().await.unwrap();
            assert_eq!(status, StatusCode::BAD_REQUEST, "{query:?}: {body}");
            assert!(
                body.contains("<Code>InvalidRequest</Code>"),
                "{query:?}: {body}"
            );
            assert_eq!(snapshot(h.state.store.db()).await, before);
            assert_eq!(h.kubo.received_requests().await.unwrap().len(), rpc_before);
            assert_eq!(h.source.calls(), 0);
        }
    });
}

#[test]
fn ordinary_tag_codec_still_validates_all_limits_reserved_keys_and_exact_form_decoding() {
    for value in [
        "ipfs-s3%3Azip-root=TRUE",
        "ipfs-s3%3Azip-unknown=true",
        "ipfs-s3%3Azip-root=true&ipfs-s3:zip-root=true",
        "user=%GG",
    ] {
        assert!(
            parse_tagging_header(value).is_err(),
            "ordinary codec: {value}"
        );
    }
    assert_eq!(
        parse_tagging_header("ipfs-s3%253Azip-root=TRUE&user=+%2B%252B").unwrap(),
        vec![
            ObjectTag::new("ipfs-s3%3Azip-root", "TRUE"),
            ObjectTag::new("user", " +%2B")
        ]
    );
}

#[tokio::test]
async fn import_zip_query_repeated_same_prefix_is_common_invalid_request_before_intake() {
    scenario!(|h| {
        rejected_unchanged(
            &h,
            Entry::Import,
            BUCKET,
            &[
                ("ipfs3-import", ""),
                ("decompress-zip", "out/"),
                ("decompress-zip", "out/"),
            ],
            HeaderMap::new(),
            "InvalidRequest",
        )
        .await;
    });
}

#[tokio::test]
async fn import_zip_query_invalid_prefix_is_common_invalid_request_before_intake() {
    scenario!(|h| {
        rejected_unchanged(
            &h,
            Entry::Import,
            BUCKET,
            &[("ipfs3-import", ""), ("decompress-zip", "../out/")],
            HeaderMap::new(),
            "InvalidRequest",
        )
        .await;
    });
}

#[tokio::test]
async fn import_zip_query_duplicate_variants_match_direct_and_mpu_without_any_side_effects() {
    for entry in [Entry::ZipPut, Entry::Mpu, Entry::Import] {
        scenario!(|h| {
            for second in ["out/", "other/"] {
                let mut query = entry.query();
                query.push(("decompress-zip", second));
                rejected_unchanged(
                    &h,
                    entry,
                    BUCKET,
                    &query,
                    HeaderMap::new(),
                    "InvalidRequest",
                )
                .await;
                let before = snapshot(h.state.store.db()).await;
                let rpc = h.kubo.received_requests().await.unwrap().len();
                let response = support::stage9_zip_controls_review_sigv4::send_encoded_zip_query(
                    &h, entry, &query,
                )
                .await;
                let status = response.status();
                let body = response.text().await.unwrap();
                assert_eq!(
                    status,
                    StatusCode::BAD_REQUEST,
                    "{entry:?} {query:?}: {body}"
                );
                assert!(
                    body.contains("<Code>InvalidRequest</Code>"),
                    "{entry:?} {query:?}: {body}"
                );
                assert_eq!(snapshot(h.state.store.db()).await, before);
                assert_eq!(h.kubo.received_requests().await.unwrap().len(), rpc);
                assert_eq!(h.source.calls(), 0);
            }
            let mut query = entry.query();
            query.last_mut().unwrap().1 = "../out/";
            rejected_unchanged(
                &h,
                entry,
                BUCKET,
                &query,
                HeaderMap::new(),
                "InvalidRequest",
            )
            .await;
        });
    }
}

#[tokio::test]
async fn import_zip_query_ordinary_whitelist_path_and_xml_errors_remain_invalid_argument() {
    scenario!(|h| {
        for query in [
            vec![("ipfs3-import", ""), ("unknown", "value")],
            vec![("ipfs3-import", ""), ("decompress-zip-result", "true")],
            vec![("ipfs3-import", ""), ("x-ipfs3-zip-unknown", "true")],
            vec![("ipfs3-import", ""), ("ipfs3-import", "")],
        ] {
            rejected_unchanged(
                &h,
                Entry::Import,
                BUCKET,
                &query,
                HeaderMap::new(),
                "InvalidArgument",
            )
            .await;
        }
        for (key, body) in [
            (KEY, b"not XML".to_vec()),
            (
                "",
                b"<IPFS3ImportRequest><CID>not-a-cid</CID></IPFS3ImportRequest>".to_vec(),
            ),
        ] {
            let before = snapshot(h.state.store.db()).await;
            let rpc = h.kubo.received_requests().await.unwrap().len();
            let mut headers = HeaderMap::new();
            headers.insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/xml"),
            );
            let response = send_sigv4(
                reqwest::Method::POST,
                h.endpoint(),
                BUCKET,
                key,
                &[("ipfs3-import", "")],
                body,
                headers,
                "test",
            )
            .await;
            let status = response.status();
            let body = response.text().await.unwrap();
            assert_eq!(status, StatusCode::BAD_REQUEST, "key={key:?}: {body}");
            assert!(
                body.contains("<Code>InvalidArgument</Code>"),
                "key={key:?}: {body}"
            );
            assert_eq!(snapshot(h.state.store.db()).await, before);
            assert_eq!(h.kubo.received_requests().await.unwrap().len(), rpc);
            assert_eq!(h.source.calls(), 0);
        }
    });
}

#[tokio::test]
async fn import_zip_query_valid_encoded_name_and_once_decoded_prefix_keep_literal_accepted_wire() {
    use store::entities::import_job;
    use support::{
        stage9_zip_controls_review_sigv4::send_encoded_zip_query, stage9_zip_wire_golden::check,
        stage9_zip_wire_requests::Wire,
    };
    for (prefix, captured) in [
        ("out/nested/", "out/nested/"),
        ("out%2Fnested", "out%2Fnested/"),
    ] {
        scenario!(|h| {
            let wire = Wire::read(
                send_encoded_zip_query(
                    &h,
                    Entry::Import,
                    &[("ipfs3-import", ""), ("decompress-zip", prefix)],
                )
                .await,
            )
            .await;
            check("legacy-import-accepted", &wire);
            let id = wire.id("x-ipfs3-import-job-id");
            let job = import_job::Entity::find_by_id(&id)
                .one(h.state.store.db())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(job.decompress_prefix.as_deref(), Some(captured));
            assert_eq!(job.state, "queued");
            assert!(h.kubo.received_requests().await.unwrap().is_empty());
            assert_eq!(h.source.calls(), 0);
        });
    }
}
