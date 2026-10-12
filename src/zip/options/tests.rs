use http::{HeaderMap, HeaderValue};

use super::*;
use crate::pinning::tags::ObjectTag;

fn request(v2: bool) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if v2 {
        headers.insert(ZIP_CONTRACT, HeaderValue::from_static("v2"));
    }
    headers
}

#[test]
fn r25_common_parser_same_error_class_for_unknown_duplicate_and_malformed_controls() {
    for v2 in [false, true] {
        for is_zip in [false, true] {
            for (name, value) in [
                ("x-ipfs3-zip-unknown", "true"),
                (ZIP_EXPECTED_SHA256, "a"),
                (ZIP_TARGETS, "invalid"),
            ] {
                let mut headers = request(v2);
                headers.insert(
                    http::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    HeaderValue::from_str(value).unwrap(),
                );
                assert_eq!(
                    parse_authenticated_zip_controls(&headers, &[], &[], is_zip, true)
                        .unwrap_err()
                        .code()
                        .as_str(),
                    "InvalidRequest"
                );
            }
            for query in [
                vec![
                    ("decompress-zip".into(), "out".into()),
                    ("decompress-zip".into(), "out".into()),
                ],
                vec![("decompress-zip-result".into(), "maybe".into())],
                vec![
                    ("decompress-zip-result".into(), "true".into()),
                    ("decompress-zip-result".into(), "true".into()),
                ],
                vec![("x-ipfs3-zip-expected-sha256".into(), "a".into())],
            ] {
                assert_eq!(
                    parse_authenticated_zip_controls(&request(v2), &query, &[], is_zip, true)
                        .unwrap_err()
                        .code()
                        .as_str(),
                    "InvalidRequest"
                );
            }
            for tags in [
                vec![
                    ObjectTag::new("ipfs-s3:zip-root", "true"),
                    ObjectTag::new("ipfs-s3:zip-root", "true"),
                ],
                vec![ObjectTag::new("ipfs-s3:zip-root", "TRUE")],
                vec![ObjectTag::new("ipfs-s3:zip-expected-sha256", "a")],
            ] {
                assert_eq!(
                    parse_authenticated_zip_controls(&request(v2), &[], &tags, is_zip, true)
                        .unwrap_err()
                        .code()
                        .as_str(),
                    "InvalidRequest"
                );
            }
        }
    }
    for name in V2_HEADERS {
        let mut headers = request(true);
        let value = match name {
            ZIP_CONTRACT => "v2",
            PUBLISH_SOURCE | PUBLISH_EXTRACTED => "true",
            ZIP_TARGETS => "none",
            _ => "retry",
        };
        headers.insert(name, HeaderValue::from_static(value));
        headers.append(name, HeaderValue::from_static(value));
        assert_eq!(
            parse_authenticated_zip_controls(&headers, &[], &[], true, true)
                .unwrap_err()
                .code()
                .as_str(),
            "InvalidRequest"
        );
    }
}

#[test]
fn r25_non_zip_without_zip_controls_is_unchanged_but_valid_zip_controls_cannot_escape() {
    assert!(
        parse_authenticated_zip_controls(
            &HeaderMap::new(),
            &[("ordinary".into(), "value".into())],
            &[ObjectTag::new(
                "ipfs-s3:pin",
                "historically-classified-by-ordinary-path"
            )],
            false,
            true
        )
        .unwrap()
        .is_none()
    );
    assert!(parse_authenticated_zip_controls(&request(true), &[], &[], false, true).is_err());
    assert!(
        parse_authenticated_zip_controls(
            &HeaderMap::new(),
            &[("decompress-zip".into(), "out/".into())],
            &[],
            false,
            true
        )
        .is_err()
    );
    assert!(
        parse_authenticated_zip_controls(
            &HeaderMap::new(),
            &[],
            &[ObjectTag::new("ipfs-s3:zip-root", "false")],
            false,
            true
        )
        .is_err()
    );
}

#[test]
fn r25_explicit_root_manual_publish_and_target_contradictions_are_structural() {
    for (name, value, tags) in [
        (
            PUBLISH_SOURCE,
            "false",
            vec![ObjectTag::new("ipfs-s3:pin", "true")],
        ),
        (
            PUBLISH_SOURCE,
            "false",
            vec![ObjectTag::new("ipfs-s3:pin", "false")],
        ),
        (
            PUBLISH_SOURCE,
            "false",
            vec![
                ObjectTag::new("ipfs-s3:pin", "true"),
                ObjectTag::new("ipfs-s3:content", "decompressed"),
            ],
        ),
        (
            PUBLISH_EXTRACTED,
            "false",
            vec![ObjectTag::new("ipfs-s3:zip-root", "true")],
        ),
        (
            PUBLISH_EXTRACTED,
            "false",
            vec![
                ObjectTag::new("ipfs-s3:pin", "true"),
                ObjectTag::new("ipfs-s3:content", "decompressed"),
            ],
        ),
    ] {
        let mut headers = request(true);
        headers.insert(name, HeaderValue::from_static(value));
        headers.insert(ZIP_TOKEN, HeaderValue::from_static("retry"));
        assert_eq!(
            parse_authenticated_zip_controls(&headers, &[], &tags, true, true)
                .unwrap_err()
                .code()
                .as_str(),
            "InvalidRequest"
        );
    }
}

#[test]
fn r25_actual_old_options_envelope_and_digest_column_decode_without_defaults() {
    let mut headers = request(true);
    headers.insert(ZIP_TOKEN, HeaderValue::from_static("old-retry"));
    headers.insert(ZIP_TARGETS, HeaderValue::from_static("source"));
    let ZipOptions::V2(options) = parse_zip_options(&headers, true, None, false).unwrap() else {
        panic!("v2")
    };
    let envelope = serde_json::json!({"options": options, "target_prefix": "out/", "rule_revision": "historical", "source_metadata": {}});
    let digest = "b".repeat(64);
    let old = decode_historical_zip_options(&envelope.to_string(), Some(&digest)).unwrap();
    assert_eq!(old.options.targets, ZipTargets::Source);
    assert!(!old.options.root_enabled);
    assert_eq!(old.expected_sha256, Some(digest));
    assert!(
        decode_historical_zip_options("{}", None)
            .unwrap_err()
            .to_string()
            .contains("needs_attention")
    );
}
