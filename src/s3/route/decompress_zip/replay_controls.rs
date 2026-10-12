//! Complete may omit admission controls, but may never replace them on replay.
use http::HeaderMap;
use s3s::S3Result;

use crate::{
    pinning::tags::ObjectTag,
    zip::options::{self, ZipOptions, ZipTargets},
};

pub(super) fn saved_intent(
    value: &serde_json::Value,
) -> S3Result<Option<crate::pinning::zip_policy::CapturedZipIntent>> {
    if value.get("zip_intent").is_none()
        && value.get("schema_version").is_none()
        && value.get("legacy_zip_intent").is_none()
        && value.get("source_decision").is_none()
        && value.get("pin_plan").is_none()
    {
        // Old terminal receipts need no new authorization. Missing intent may
        // never authorize a pending publication; the direct adapter fences it.
        return Ok(None);
    }
    crate::store::pinning::publication::decode_zip_capture_json(&value.to_string())
        .map(Some)
        .map_err(|_| {
            s3s::s3_error!(
                OperationAborted,
                "ZIP capture needs_attention: invalid original intent evidence"
            )
        })
}

pub(super) fn verify_direct_contract(
    headers: &HeaderMap,
    canonical: &serde_json::Value,
    stored: &str,
) -> S3Result<Option<String>> {
    let mut stored: serde_json::Value =
        serde_json::from_str(stored).map_err(|_| super::mpu_zip_conflict())?;
    let Some(signed) = stored
        .as_object_mut()
        .and_then(|object| object.remove("signed_headers"))
    else {
        return if &stored == canonical {
            Ok(None)
        } else {
            Err(super::mpu_zip_conflict())
        };
    };
    // Read only the original header-value evidence. Authentication of this
    // request remains s3s's job; its SignedHeaders/Authorization shape is irrelevant.
    let mut normalized = canonical.clone();
    let object = normalized
        .as_object_mut()
        .ok_or_else(super::mpu_zip_conflict)?;
    for key in [
        "contract_version",
        "source",
        "content_type",
        "metadata",
        "tags",
    ] {
        object.remove(key);
    }
    if stored != normalized {
        return Err(super::mpu_zip_conflict());
    }
    let mut old_headers = HeaderMap::new();
    let mut expected = None;
    for (name, values) in signed.as_object().ok_or_else(super::mpu_zip_conflict)? {
        let values = values
            .as_array()
            .filter(|values| !values.is_empty())
            .ok_or_else(super::mpu_zip_conflict)?;
        let values = values
            .iter()
            .map(|value| value.as_str().ok_or_else(super::mpu_zip_conflict))
            .collect::<S3Result<Vec<_>>>()?;
        if name == options::ZIP_EXPECTED_SHA256 {
            if values.len() != 1
                || values[0].len() != 64
                || !values[0]
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(super::mpu_zip_conflict());
            }
            expected = Some(values[0].to_owned());
            continue;
        }
        if [
            options::ZIP_CONTRACT,
            options::PUBLISH_SOURCE,
            options::PUBLISH_EXTRACTED,
            options::ZIP_TARGETS,
            options::ZIP_TOKEN,
        ]
        .contains(&name.as_str())
        {
            continue; // Their normalized persisted values were compared above.
        }
        let header = http::header::HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| super::mpu_zip_conflict())?;
        for value in &values {
            old_headers.append(
                header.clone(),
                http::HeaderValue::from_str(value).map_err(|_| super::mpu_zip_conflict())?,
            );
        }
        if name != "content-type" && name != "x-amz-tagging" && !name.starts_with("x-amz-meta-") {
            let actual = headers
                .get_all(&header)
                .iter()
                .map(|value| value.to_str())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| super::mpu_zip_conflict())?;
            if actual != values {
                return Err(super::mpu_zip_conflict());
            }
        }
    }
    let mut tags = super::parse_publication_tags(&old_headers)?;
    tags.sort_by(|left, right| left.key.cmp(&right.key));
    let content_type = old_headers
        .get(http::header::CONTENT_TYPE)
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| super::mpu_zip_conflict())?;
    if canonical.get("tags") != Some(&serde_json::json!(tags))
        || canonical.get("content_type") != Some(&serde_json::json!(content_type))
        || canonical.get("metadata")
            != Some(&serde_json::json!(
                crate::s3::ops::object::extract_custom_metadata(&old_headers)
            ))
    {
        return Err(super::mpu_zip_conflict());
    }
    Ok(expected)
}

pub(crate) fn direct_options(
    parsed: &options::ParsedZipControls,
    saved: &options::ZipV2Options,
) -> S3Result<options::ZipV2Options> {
    let ZipOptions::V2(actual) = &parsed.options else {
        return Err(super::mpu_zip_conflict());
    };
    if actual.publish_source != saved.publish_source
        || actual.publish_extracted != saved.publish_extracted
        || (parsed.targets_explicit && actual.targets != saved.targets)
        || actual.token != saved.token
        || actual.root_override != saved.root_override
        || actual.result_version != saved.result_version
    {
        return Err(super::mpu_zip_conflict());
    }
    // Omitted server-derived defaults are frozen admission facts, never current cfg.
    Ok(saved.clone())
}

pub(crate) fn verify(
    headers: &HeaderMap,
    query: &[(String, String)],
    saved: &ZipOptions,
    prefix: &str,
    result_enabled: Option<bool>,
    root_enabled: bool,
) -> S3Result<()> {
    let mut headers = headers.clone();
    let mut tags = super::parse_publication_tags(&headers)?;
    if let ZipOptions::V2(options) = saved {
        for (name, value) in [
            (options::ZIP_CONTRACT, "v2"),
            (
                options::PUBLISH_SOURCE,
                if options.publish_source {
                    "true"
                } else {
                    "false"
                },
            ),
            (
                options::PUBLISH_EXTRACTED,
                if options.publish_extracted {
                    "true"
                } else {
                    "false"
                },
            ),
            (
                options::ZIP_TARGETS,
                match options.targets {
                    ZipTargets::None => "none",
                    ZipTargets::Source => "source",
                    ZipTargets::Extracted => "extracted",
                    ZipTargets::Both => "both",
                },
            ),
            (options::ZIP_TOKEN, options.token.as_str()),
        ] {
            if !headers.contains_key(name) {
                headers.insert(
                    http::header::HeaderName::from_static(name),
                    http::HeaderValue::from_str(value).map_err(|_| super::mpu_zip_conflict())?,
                );
            }
        }
        if !tags.iter().any(|tag| tag.key == "ipfs-s3:zip-root")
            && let Some(enabled) = options.root_override
        {
            tags.push(ObjectTag::new(
                "ipfs-s3:zip-root",
                if enabled { "true" } else { "false" },
            ));
        }
    }
    let controls =
        options::parse_authenticated_zip_controls(&headers, query, &tags, true, root_enabled)?
            .ok_or_else(super::mpu_zip_conflict)?;
    if controls
        .target_prefix
        .as_deref()
        .is_some_and(|value| value != prefix)
        || (query
            .iter()
            .any(|(name, _)| name == "decompress-zip-result")
            && result_enabled.is_some_and(|saved| controls.result_enabled != saved))
        || controls.root.enabled() != root_enabled
    {
        return Err(super::mpu_zip_conflict());
    }
    match (saved, &controls.options) {
        (ZipOptions::Legacy, ZipOptions::Legacy) => Ok(()),
        (ZipOptions::V2(saved), ZipOptions::V2(actual)) if saved == actual => Ok(()),
        _ => Err(super::mpu_zip_conflict()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zip::options::ZipV2Options;

    fn saved() -> ZipOptions {
        ZipOptions::V2(ZipV2Options {
            publish_source: false,
            publish_extracted: true,
            targets: ZipTargets::None,
            token: "original".into(),
            root_override: None,
            root_enabled: false,
            result_version: 2,
        })
    }

    #[test]
    fn r25_full_historical_empty_intent_preserves_original_decision_and_defaults() {
        use crate::pinning::{
            coordinator::PinningCoordinator,
            decision::DecisionOrigin,
            zip_policy::{ValidatedZipOutputRules, ZipCaptureContext, capture_zip_intent},
        };
        let coordinator = PinningCoordinator::disabled_for_test();
        let mut headers = HeaderMap::new();
        headers.insert(options::ZIP_CONTRACT, "v2".parse().unwrap());
        headers.insert(options::ZIP_TOKEN, "original".parse().unwrap());
        headers.insert(options::ZIP_TARGETS, "none".parse().unwrap());
        let controls = options::parse_authenticated_zip_controls(
            &headers,
            &[("decompress-zip".into(), "out/".into())],
            &[],
            true,
            false,
        )
        .unwrap()
        .unwrap();
        let zip = crate::config::DecompressZipConfig {
            auto_pin_outputs: Some(true),
            ..Default::default()
        };
        let current = capture_zip_intent(
            coordinator.effective_config(),
            &zip,
            coordinator.control_mode(),
            ZipCaptureContext {
                bucket: "bucket",
                source_key: "archive.zip",
                target_prefix: "out/",
                tags: &[],
            },
            &controls,
            DecisionOrigin::new("test", "batch"),
        )
        .unwrap();
        assert!(current.controls().auto_pin_outputs);
        let empty = ValidatedZipOutputRules::compile(&[], coordinator.effective_config()).unwrap();
        let legacy = serde_json::json!({
            "bucket": "bucket", "source_key": "archive.zip", "target_prefix": "out/",
            "options": current.v2_options().unwrap(),
            "source_decision": current.source_decision(), "providers": [],
            "pin_plan": {
                "rule_revision": empty.revision(),
                "targets": {"source": false, "extracted": false}, "outputs": [],
            },
        });
        for value in [
            legacy.clone(),
            serde_json::json!({"legacy_zip_intent": legacy}),
        ] {
            let decoded = saved_intent(&value).unwrap().unwrap();
            assert!(!decoded.controls().auto_pin_outputs);
            assert_eq!(decoded.source_decision(), current.source_decision());
            assert_eq!(decoded.v2_options(), current.v2_options());
            assert_eq!(decoded.policy_revision(), empty.revision());
        }
        let mut revision_only = legacy;
        revision_only.as_object_mut().unwrap().remove("pin_plan");
        revision_only["rule_revision"] = serde_json::json!(empty.revision());
        assert!(saved_intent(&revision_only).is_err());
    }

    fn old_direct() -> (HeaderMap, serde_json::Value, String) {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/zip".parse().unwrap());
        headers.insert("x-amz-meta-team", "blue".parse().unwrap());
        let canonical = serde_json::json!({
            "contract_version": 1, "source": "direct", "principal": "test",
            "bucket": "bucket", "source_key": "archive.zip", "target_prefix": "out/",
            "publish_source": true, "publish_extracted": true, "targets": "none",
            "token": "original", "root_override": null, "result_version": 2,
            "content_type": "application/zip", "metadata": {"team": "blue"}, "tags": [],
        });
        let mut stored = canonical.clone();
        let object = stored.as_object_mut().unwrap();
        for key in [
            "contract_version",
            "source",
            "content_type",
            "metadata",
            "tags",
        ] {
            object.remove(key);
        }
        object.insert(
            "signed_headers".into(),
            serde_json::json!({
                "content-type": ["application/zip"], "x-amz-meta-team": ["blue"],
            }),
        );
        (headers, canonical, stored.to_string())
    }

    #[test]
    fn r25_historical_direct_contract_reuses_evidence_without_authorization_reparse() {
        let (headers, canonical, stored) = old_direct();
        assert_eq!(
            verify_direct_contract(&headers, &canonical, &stored).unwrap(),
            None
        );
        let mut changed = canonical.clone();
        changed["metadata"]["team"] = serde_json::json!("red");
        assert!(verify_direct_contract(&headers, &changed, &stored).is_err());
        changed = canonical.clone();
        changed["target_prefix"] = serde_json::json!("other/");
        assert!(verify_direct_contract(&headers, &changed, &stored).is_err());
    }

    #[test]
    fn r25_historical_direct_expected_digest_stays_an_input_obligation() {
        let (headers, canonical, stored) = old_direct();
        let digest = "ab".repeat(32);
        let mut old: serde_json::Value = serde_json::from_str(&stored).unwrap();
        old["signed_headers"][options::ZIP_EXPECTED_SHA256] = serde_json::json!([digest]);
        assert_eq!(
            verify_direct_contract(&headers, &canonical, &old.to_string()).unwrap(),
            Some(digest)
        );
        old["signed_headers"][options::ZIP_EXPECTED_SHA256] = serde_json::json!(["malformed"]);
        assert!(verify_direct_contract(&headers, &canonical, &old.to_string()).is_err());
    }

    #[test]
    fn r25_replay_server_defaults_are_taken_only_from_the_capture() {
        let mut headers = HeaderMap::new();
        headers.insert(options::ZIP_CONTRACT, "v2".parse().unwrap());
        headers.insert(options::ZIP_TOKEN, "original".parse().unwrap());
        let query = vec![("decompress-zip".into(), "out/".into())];
        let controls = options::parse_authenticated_zip_controls(&headers, &query, &[], true, true)
            .unwrap()
            .unwrap();
        let persisted = options::ZipV2Options {
            publish_source: true,
            publish_extracted: true,
            targets: options::ZipTargets::Extracted,
            token: "original".into(),
            root_override: None,
            root_enabled: false,
            result_version: 2,
        };
        assert_eq!(direct_options(&controls, &persisted).unwrap(), persisted);
        headers.insert(options::ZIP_TARGETS, "none".parse().unwrap());
        let changed = options::parse_authenticated_zip_controls(&headers, &query, &[], true, true)
            .unwrap()
            .unwrap();
        assert!(direct_options(&changed, &persisted).is_err());
    }

    #[test]
    fn r25_complete_omits_controls_but_changed_control_conflicts_before_receipt() {
        verify(&HeaderMap::new(), &[], &saved(), "out/", Some(true), false).unwrap();
        for (name, value) in [
            (options::PUBLISH_SOURCE, "true"),
            (options::ZIP_TARGETS, "extracted"),
            (options::ZIP_TOKEN, "different"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(name, value.parse().unwrap());
            assert!(verify(&headers, &[], &saved(), "out/", Some(true), false).is_err());
        }
        assert!(
            verify(
                &HeaderMap::new(),
                &[("decompress-zip".into(), "other/".into())],
                &saved(),
                "out/",
                Some(true),
                false
            )
            .is_err()
        );
    }

    #[test]
    fn r25_complete_unknown_duplicate_and_malformed_controls_never_replay() {
        for (name, value) in [
            ("x-ipfs3-zip-unknown", "true"),
            (options::PUBLISH_SOURCE, "TRUE"),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(name, value.parse().unwrap());
            assert!(verify(&headers, &[], &saved(), "out/", Some(true), false).is_err());
        }
        let mut duplicate = HeaderMap::new();
        duplicate.append(options::ZIP_TOKEN, "original".parse().unwrap());
        duplicate.append(options::ZIP_TOKEN, "original".parse().unwrap());
        assert!(verify(&duplicate, &[], &saved(), "out/", Some(true), false).is_err());
        assert!(
            verify(
                &HeaderMap::new(),
                &[("decompress-zip-result".into(), "maybe".into())],
                &ZipOptions::Legacy,
                "out/",
                Some(true),
                false
            )
            .is_err()
        );
    }
}
