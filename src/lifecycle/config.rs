#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use s3s::dto::{BucketLifecycleConfiguration, LifecycleRule};
    use serde_json::{Value, json};

    use crate::lifecycle::model::CanonicalLifecycleConfiguration;

    fn rule(value: Value) -> LifecycleRule {
        serde_json::from_value(value).expect("test lifecycle rule JSON")
    }

    fn configuration(rules: Vec<LifecycleRule>) -> BucketLifecycleConfiguration {
        BucketLifecycleConfiguration { rules }
    }

    fn valid_rule() -> LifecycleRule {
        rule(json!({
            "prefix": "",
            "status": "Enabled",
            "expiration": { "days": 1 }
        }))
    }

    fn canonicalize(
        input: BucketLifecycleConfiguration,
    ) -> crate::error::AppResult<CanonicalLifecycleConfiguration> {
        super::validate_and_canonicalize(input)
    }

    fn abort_rule(days: i32, selector: Value) -> LifecycleRule {
        let mut value = json!({
            "status": "Enabled",
            "abort_incomplete_multipart_upload": { "days_after_initiation": days }
        });
        for (key, selected) in selector.as_object().unwrap() {
            value[key] = selected.clone();
        }
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn abort_days_are_positive_and_s3s_representable() {
        for days in [1, i32::MAX] {
            let canonical = canonicalize(configuration(vec![abort_rule(
                days,
                json!({ "filter": {} }),
            )]))
            .unwrap();
            assert_eq!(
                canonical.rules[0]
                    .abort_incomplete_multipart_upload
                    .as_ref()
                    .unwrap()
                    .days_after_initiation,
                u32::try_from(days).unwrap()
            );
            let projected = super::to_s3_rules(&canonical).unwrap();
            assert_eq!(
                projected[0]
                    .abort_incomplete_multipart_upload
                    .as_ref()
                    .unwrap()
                    .days_after_initiation,
                Some(days)
            );
        }
        for days in [0, -1, i32::MIN] {
            assert!(
                canonicalize(configuration(vec![abort_rule(
                    days,
                    json!({ "filter": {} })
                )]))
                .is_err(),
                "days={days}"
            );
        }
        assert!(
            canonicalize(configuration(vec![rule(json!({
                "status": "Enabled",
                "filter": {},
                "abort_incomplete_multipart_upload": {}
            }))]))
            .is_err()
        );

        let canonical = canonicalize(configuration(vec![valid_rule()])).unwrap();
        let mut stored = serde_json::to_value(canonical).unwrap();
        stored["rules"][0]["abort_incomplete_multipart_upload"] =
            json!({ "days_after_initiation": 2_147_483_648_u32 });
        let decoded: CanonicalLifecycleConfiguration =
            serde_json::from_value(stored.clone()).unwrap();
        assert!(super::canonical_json(&decoded).is_err());
        assert!(super::from_canonical_json(&stored.to_string()).is_err());
    }

    #[test]
    fn abort_is_a_supported_action_and_mixed_actions_round_trip() {
        let abort_only = abort_rule(7, json!({ "filter": {} }));
        assert!(canonicalize(configuration(vec![abort_only.clone()])).is_ok());
        assert!(
            canonicalize(configuration(vec![rule(json!({
                "status": "Enabled", "filter": {}
            }))]))
            .is_err()
        );

        for status in ["Enabled", "Disabled"] {
            let mut mixed = serde_json::to_value(&abort_only).unwrap();
            mixed["status"] = json!(status);
            mixed["expiration"] = json!({ "days": 30 });
            let canonical = canonicalize(configuration(vec![rule(mixed)])).unwrap();
            let encoded = super::canonical_json(&canonical).unwrap();
            assert_eq!(encoded, super::canonical_json(&canonical).unwrap());
            let stored: Value = serde_json::from_str(&encoded).unwrap();
            assert_eq!(stored["schema_version"], json!(1));
            assert_eq!(stored["rules"][0]["expiration"]["days"], json!(30));
            assert_eq!(
                stored["rules"][0]["abort_incomplete_multipart_upload"]["days_after_initiation"],
                json!(7)
            );
            let decoded = super::from_canonical_json(&encoded).unwrap();
            assert_eq!(decoded, canonical);
            let projected = super::to_s3_rules(&decoded).unwrap();
            assert_eq!(projected[0].status.as_str(), status);
            assert_eq!(projected[0].expiration.as_ref().unwrap().days, Some(30));
            assert_eq!(
                projected[0]
                    .abort_incomplete_multipart_upload
                    .as_ref()
                    .unwrap()
                    .days_after_initiation,
                Some(7)
            );
            assert_eq!(canonicalize(configuration(projected)).unwrap(), canonical);
        }
    }

    #[test]
    fn abort_accepts_only_all_or_prefix_selectors() {
        let cases = [
            (json!({ "prefix": "legacy/" }), true),
            (json!({ "filter": {} }), true),
            (json!({ "filter": { "prefix": "modern/" } }), true),
            (
                json!({ "filter": { "tag": { "key": "class", "value": "cold" } } }),
                false,
            ),
            (json!({ "filter": { "and": { "prefix": "logs/" } } }), false),
            (
                json!({ "filter": { "and": {
                "prefix": "logs/", "tags": [{ "key": "class", "value": "cold" }]
            } } }),
                false,
            ),
            (
                json!({ "filter": { "object_size_greater_than": 0 } }),
                false,
            ),
            (json!({ "filter": { "object_size_less_than": 10 } }), false),
        ];
        for (selector, accepted) in cases {
            let abort = abort_rule(1, selector.clone());
            let result = canonicalize(configuration(vec![abort.clone()]));
            assert_eq!(result.is_ok(), accepted, "{selector}");
            if let Ok(canonical) = result {
                let projected = super::to_s3_rules(&canonical).unwrap();
                assert_eq!(projected[0].prefix, abort.prefix);
                assert_eq!(projected[0].filter, abort.filter);
            }

            let mut expiration = serde_json::to_value(abort).unwrap();
            expiration
                .as_object_mut()
                .unwrap()
                .remove("abort_incomplete_multipart_upload");
            expiration["expiration"] = json!({ "days": 1 });
            let canonical = canonicalize(configuration(vec![rule(expiration.clone())])).unwrap();
            assert_eq!(
                canonicalize(configuration(super::to_s3_rules(&canonical).unwrap())).unwrap(),
                canonical
            );
            expiration["abort_incomplete_multipart_upload"] = json!({ "days_after_initiation": 1 });
            assert_eq!(
                canonicalize(configuration(vec![rule(expiration)])).is_ok(),
                accepted,
                "mixed actions: {selector}"
            );
        }
    }

    #[tokio::test]
    async fn abort_rejection_is_rule_local_but_document_atomic() {
        use crate::{
            state::AppState,
            store::{self, entities::bucket_lifecycle_config},
        };
        use sea_orm::EntityTrait;
        use std::{collections::HashMap, sync::Arc};

        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "bucket", None).await.unwrap();
        let state = Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new("http://127.0.0.1:5001".to_owned()),
            store: store::Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        });
        let request = |rules| s3s::S3Request {
            input: s3s::dto::PutBucketLifecycleConfigurationInput {
                bucket: "bucket".to_owned(),
                lifecycle_configuration: Some(configuration(rules)),
                ..Default::default()
            },
            method: http::Method::PUT,
            uri: "/bucket?lifecycle".parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        let tagged_expiration = rule(json!({
            "status": "Enabled", "expiration": { "days": 1 },
            "filter": { "tag": { "key": "class", "value": "cold" } }
        }));
        let safe_abort = abort_rule(1, json!({ "filter": {} }));
        crate::s3::ops::lifecycle::put_bucket_lifecycle_configuration(
            &state,
            request(vec![tagged_expiration.clone(), safe_abort.clone()]),
        )
        .await
        .unwrap();
        let before = bucket_lifecycle_config::Entity::find_by_id("bucket")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(before.revision, 1);

        for rejected in [
            abort_rule(
                1,
                json!({ "filter": { "tag": { "key": "class", "value": "cold" } } }),
            ),
            rule(
                json!({ "status": "Enabled", "prefix": "", "expiration": { "days": 1 }, "transitions": [{}] }),
            ),
            rule(
                json!({ "status": "Enabled", "prefix": "", "expiration": { "days": 1 }, "noncurrent_version_transitions": [{}] }),
            ),
        ] {
            let error = crate::s3::ops::lifecycle::put_bucket_lifecycle_configuration(
                &state,
                request(vec![
                    tagged_expiration.clone(),
                    safe_abort.clone(),
                    rejected,
                ]),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code().as_str(), "InvalidRequest");
            let after = bucket_lifecycle_config::Entity::find_by_id("bucket")
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(after.canonical_json, before.canonical_json);
            assert_eq!(after.revision, before.revision);
        }
    }

    #[test]
    fn phase_a_canonical_json_decodes_with_absent_abort() {
        let phase_a = json!({
            "schema_version": 1,
            "rules": [{
                "id": null,
                "status": "enabled",
                "selector": { "kind": "legacy_prefix", "prefix": "" },
                "expiration": { "kind": "days", "days": 1 },
                "noncurrent_version_expiration": null
            }]
        });
        let decoded = super::from_canonical_json(&phase_a.to_string()).unwrap();
        assert!(decoded.rules[0].abort_incomplete_multipart_upload.is_none());
        assert_eq!(
            decoded,
            canonicalize(configuration(vec![valid_rule()])).unwrap()
        );
        assert!(
            super::to_s3_rules(&decoded).unwrap()[0]
                .abort_incomplete_multipart_upload
                .is_none()
        );
    }

    #[test]
    fn stored_abort_json_is_semantically_revalidated() {
        let canonical =
            canonicalize(configuration(vec![abort_rule(1, json!({ "filter": {} }))])).unwrap();
        let stored: Value =
            serde_json::from_str(&super::canonical_json(&canonical).unwrap()).unwrap();
        for days in [0_u32, 2_147_483_648_u32] {
            let mut tampered = stored.clone();
            tampered["rules"][0]["abort_incomplete_multipart_upload"]["days_after_initiation"] =
                json!(days);
            let decoded: CanonicalLifecycleConfiguration =
                serde_json::from_value(tampered.clone()).unwrap();
            assert!(
                super::from_canonical_json(&tampered.to_string()).is_err(),
                "days={days}"
            );
            assert!(super::canonical_json(&decoded).is_err(), "days={days}");
            assert!(super::to_s3_rules(&decoded).is_err(), "days={days}");
        }
        for filter in [
            json!({ "kind": "tag", "tag": { "key": "class", "value": "cold" } }),
            json!({ "kind": "and", "prefix": "logs/", "tags": [], "object_size_greater_than": null, "object_size_less_than": null }),
            json!({ "kind": "object_size_greater_than", "bytes": 0 }),
            json!({ "kind": "object_size_less_than", "bytes": 10 }),
        ] {
            let mut tampered = stored.clone();
            tampered["rules"][0]["selector"]["filter"] = filter;
            let decoded: CanonicalLifecycleConfiguration =
                serde_json::from_value(tampered.clone()).unwrap();
            assert!(super::from_canonical_json(&tampered.to_string()).is_err());
            assert!(super::canonical_json(&decoded).is_err());
            assert!(super::to_s3_rules(&decoded).is_err());
        }
    }

    #[test]
    fn rule_count_bounds_are_enforced() {
        let cases = [
            (0, false, "zero rules"),
            (1, true, "one rule"),
            (1_000, true, "one thousand rules"),
            (1_001, false, "one thousand one rules"),
        ];

        for (count, accepted, name) in cases {
            let input = configuration((0..count).map(|_| valid_rule()).collect());
            assert_eq!(canonicalize(input).is_ok(), accepted, "{name}");
        }
    }

    #[test]
    fn rule_ids_must_be_short_and_unique_when_present() {
        let cases = [
            (Some("x".repeat(255)), true, "255-character ID"),
            (Some("x".repeat(256)), false, "256-character ID"),
            (Some(String::new()), true, "empty present ID"),
            (None, true, "absent ID"),
        ];

        for (id, accepted, name) in cases {
            let mut input = valid_rule();
            let mut value = serde_json::to_value(&input).unwrap();
            value["id"] = id.into();
            input = rule(value);
            assert_eq!(
                canonicalize(configuration(vec![input])).is_ok(),
                accepted,
                "{name}"
            );
        }

        let mut first = serde_json::to_value(valid_rule()).unwrap();
        first["id"] = json!("same");
        let mut second = serde_json::to_value(valid_rule()).unwrap();
        second["id"] = json!("same");
        assert!(canonicalize(configuration(vec![rule(first), rule(second)])).is_err());

        let mut first = serde_json::to_value(valid_rule()).unwrap();
        first["id"] = json!("");
        let mut second = serde_json::to_value(valid_rule()).unwrap();
        second["id"] = json!("");
        assert!(canonicalize(configuration(vec![rule(first), rule(second)])).is_err());
    }

    #[test]
    fn only_exact_enabled_and_disabled_statuses_are_accepted() {
        for (status, accepted) in [
            ("Enabled", true),
            ("Disabled", true),
            ("enabled", false),
            ("disabled", false),
            ("", false),
        ] {
            let mut value = serde_json::to_value(valid_rule()).unwrap();
            value["status"] = json!(status);
            assert_eq!(
                canonicalize(configuration(vec![rule(value)])).is_ok(),
                accepted,
                "{status:?}"
            );
        }
    }

    #[test]
    fn each_rule_requires_a_supported_action() {
        assert!(
            canonicalize(configuration(vec![rule(json!({
                "prefix": "",
                "status": "Enabled"
            }))]))
            .is_err()
        );
    }

    #[test]
    fn selector_form_is_unambiguous_and_preserves_empty_filter() {
        let cases = [
            (json!({ "prefix": "legacy/" }), true, "legacy prefix"),
            (json!({ "filter": {} }), true, "explicit empty filter"),
            (
                json!({ "prefix": "legacy/", "filter": {} }),
                false,
                "prefix plus filter",
            ),
            (json!({}), false, "missing prefix and filter"),
        ];

        for (selector, accepted, name) in cases {
            let mut value = serde_json::to_value(valid_rule()).unwrap();
            value.as_object_mut().unwrap().remove("prefix");
            for (key, selector_value) in selector.as_object().unwrap() {
                value[key] = selector_value.clone();
            }
            assert_eq!(
                canonicalize(configuration(vec![rule(value)])).is_ok(),
                accepted,
                "{name}"
            );
        }
    }

    #[test]
    fn modern_filter_forms_and_and_constraints_are_validated() {
        let cases = [
            (json!({}), true, "all"),
            (json!({ "prefix": "logs/" }), true, "prefix"),
            (
                json!({ "tag": { "key": "class", "value": "cold" } }),
                true,
                "tag",
            ),
            (
                json!({ "object_size_greater_than": 0 }),
                true,
                "greater-than",
            ),
            (json!({ "object_size_less_than": 10 }), true, "less-than"),
            (
                json!({
                    "and": {
                        "prefix": "logs/",
                        "tags": [
                            { "key": "class", "value": "cold" },
                            { "key": "region", "value": "us" }
                        ],
                        "object_size_greater_than": 1,
                        "object_size_less_than": 10
                    }
                }),
                true,
                "and",
            ),
            (
                json!({ "prefix": "a", "tag": { "key": "k", "value": "v" } }),
                false,
                "two top-level selectors",
            ),
            (
                json!({ "object_size_greater_than": -1 }),
                false,
                "negative size",
            ),
            (
                json!({
                    "and": {
                        "tags": [
                            { "key": "same", "value": "one" },
                            { "key": "same", "value": "two" }
                        ]
                    }
                }),
                false,
                "duplicate and tag key",
            ),
            (
                json!({
                    "and": {
                        "object_size_greater_than": 10,
                        "object_size_less_than": 10
                    }
                }),
                false,
                "equal exclusive bounds",
            ),
        ];

        for (filter, accepted, name) in cases {
            let mut value = serde_json::to_value(valid_rule()).unwrap();
            value.as_object_mut().unwrap().remove("prefix");
            value["filter"] = filter;
            assert_eq!(
                canonicalize(configuration(vec![rule(value)])).is_ok(),
                accepted,
                "{name}"
            );
        }
    }

    #[test]
    fn expiration_selects_exactly_one_effect() {
        let cases = [
            (
                json!({ "date": "2026-08-28T00:00:00Z" }),
                true,
                "midnight date",
            ),
            (json!({ "days": 1 }), true, "positive days"),
            (
                json!({ "expired_object_delete_marker": true }),
                true,
                "marker cleanup",
            ),
            (
                json!({ "date": "2026-08-28T01:00:00Z" }),
                false,
                "non-midnight date",
            ),
            (json!({ "days": 0 }), false, "zero days"),
            (
                json!({ "expired_object_delete_marker": false }),
                false,
                "false marker cleanup",
            ),
            (
                json!({ "date": "2026-08-28T00:00:00Z", "days": 1 }),
                false,
                "date plus days",
            ),
            (
                json!({ "days": 1, "expired_object_delete_marker": true }),
                false,
                "days plus marker",
            ),
            (json!({}), false, "empty expiration"),
        ];

        for (expiration, accepted, name) in cases {
            let mut value = serde_json::to_value(valid_rule()).unwrap();
            value["expiration"] = expiration;
            assert_eq!(
                canonicalize(configuration(vec![rule(value)])).is_ok(),
                accepted,
                "{name}"
            );
        }
    }

    #[test]
    fn noncurrent_expiration_requires_valid_thresholds_and_modern_filter_for_newer() {
        let cases = [
            (json!({ "noncurrent_days": 1 }), true, "positive days"),
            (
                json!({ "noncurrent_days": 1, "newer_noncurrent_versions": 1 }),
                true,
                "newer one",
            ),
            (
                json!({ "noncurrent_days": 1, "newer_noncurrent_versions": 100 }),
                true,
                "newer one hundred",
            ),
            (json!({}), false, "missing days"),
            (json!({ "noncurrent_days": 0 }), false, "zero days"),
            (
                json!({ "noncurrent_days": 1, "newer_noncurrent_versions": 0 }),
                false,
                "zero newer",
            ),
            (
                json!({ "noncurrent_days": 1, "newer_noncurrent_versions": 101 }),
                false,
                "101 newer",
            ),
        ];

        for (noncurrent, accepted, name) in cases {
            let mut value = serde_json::to_value(valid_rule()).unwrap();
            value.as_object_mut().unwrap().remove("expiration");
            if noncurrent.get("newer_noncurrent_versions").is_some() {
                value.as_object_mut().unwrap().remove("prefix");
                value["filter"] = json!({});
            }
            value["noncurrent_version_expiration"] = noncurrent;
            assert_eq!(
                canonicalize(configuration(vec![rule(value)])).is_ok(),
                accepted,
                "{name}"
            );
        }

        let mut value = serde_json::to_value(valid_rule()).unwrap();
        value.as_object_mut().unwrap().remove("expiration");
        value["noncurrent_version_expiration"] = json!({
            "noncurrent_days": 1,
            "newer_noncurrent_versions": 1
        });
        assert!(canonicalize(configuration(vec![rule(value)])).is_err());
    }

    #[test]
    fn forbidden_future_actions_are_rejected_before_serialization() {
        for (field, future_action) in [
            ("transitions", json!([{}])),
            ("noncurrent_version_transitions", json!([{}])),
        ] {
            let mut value = serde_json::to_value(valid_rule()).unwrap();
            value[field] = future_action;
            assert!(
                canonicalize(configuration(vec![rule(value)])).is_err(),
                "{field}"
            );
        }

        let mut transitions = valid_rule();
        transitions.transitions = Some(Vec::new());
        assert!(canonicalize(configuration(vec![transitions])).is_err());

        let mut noncurrent_transitions = valid_rule();
        noncurrent_transitions.noncurrent_version_transitions = Some(Vec::new());
        assert!(canonicalize(configuration(vec![noncurrent_transitions])).is_err());
    }

    #[test]
    fn stored_canonical_json_rejects_semantic_tampering() {
        let mut noncurrent = serde_json::to_value(valid_rule()).unwrap();
        noncurrent.as_object_mut().unwrap().remove("prefix");
        noncurrent.as_object_mut().unwrap().remove("expiration");
        noncurrent["filter"] = json!({});
        noncurrent["noncurrent_version_expiration"] = json!({ "noncurrent_days": 1 });
        let noncurrent = canonicalize(configuration(vec![rule(noncurrent)])).unwrap();
        let mut zero_days: Value =
            serde_json::from_str(&super::canonical_json(&noncurrent).unwrap()).unwrap();
        zero_days["rules"][0]["noncurrent_version_expiration"]["noncurrent_days"] = json!(0);
        assert!(super::from_canonical_json(&zero_days.to_string()).is_err());

        let mut first = serde_json::to_value(valid_rule()).unwrap();
        first["id"] = json!("first");
        let mut second = serde_json::to_value(valid_rule()).unwrap();
        second["id"] = json!("second");
        let duplicate_ids = canonicalize(configuration(vec![rule(first), rule(second)])).unwrap();
        let mut duplicate_ids: Value =
            serde_json::from_str(&super::canonical_json(&duplicate_ids).unwrap()).unwrap();
        duplicate_ids["rules"][0]["id"] = json!("same");
        duplicate_ids["rules"][1]["id"] = json!("same");
        assert!(super::from_canonical_json(&duplicate_ids.to_string()).is_err());

        let mut and_rule = serde_json::to_value(valid_rule()).unwrap();
        and_rule.as_object_mut().unwrap().remove("prefix");
        and_rule["filter"] = json!({
            "and": {
                "tags": [
                    { "key": "class", "value": "cold" },
                    { "key": "region", "value": "us" }
                ]
            }
        });
        let and_rule = canonicalize(configuration(vec![rule(and_rule)])).unwrap();
        let mut duplicate_tags: Value =
            serde_json::from_str(&super::canonical_json(&and_rule).unwrap()).unwrap();
        duplicate_tags["rules"][0]["selector"]["filter"]["tags"][1]["key"] = json!("class");
        assert!(super::from_canonical_json(&duplicate_tags.to_string()).is_err());
    }

    #[test]
    fn marker_cleanup_cannot_use_a_tag_based_filter() {
        let mut value = serde_json::to_value(valid_rule()).unwrap();
        value.as_object_mut().unwrap().remove("prefix");
        value["filter"] = json!({ "tag": { "key": "class", "value": "cold" } });
        value["expiration"] = json!({ "expired_object_delete_marker": true });
        assert!(canonicalize(configuration(vec![rule(value)])).is_err());
    }

    #[test]
    fn canonical_json_is_stable_and_dto_round_trips() {
        let mut first = serde_json::to_value(valid_rule()).unwrap();
        first.as_object_mut().unwrap().remove("prefix");
        first["filter"] = json!({
            "and": {
                "tags": [
                    { "key": "region", "value": "us" },
                    { "key": "class", "value": "cold" }
                ]
            }
        });
        let input = configuration(vec![rule(first)]);
        let canonical = canonicalize(input).unwrap();
        let json = super::canonical_json(&canonical).unwrap();
        assert_eq!(json, super::canonical_json(&canonical).unwrap());
        assert!(json.contains("\"id\":null"));
        assert!(json.contains("\"noncurrent_version_expiration\":null"));

        let reparsed = super::from_canonical_json(&json).unwrap();
        assert_eq!(reparsed, canonical);
        let projected = super::to_s3_rules(&reparsed).unwrap();
        let round_tripped = canonicalize(configuration(projected)).unwrap();
        assert_eq!(round_tripped, canonical);
    }

    #[test]
    fn canonical_json_sorts_and_tags_but_preserves_rule_order() {
        let and_rule = |tags: Value| {
            rule(json!({
                "id": "and-rule",
                "status": "Enabled",
                "filter": { "and": { "tags": tags } },
                "expiration": { "days": 1 }
            }))
        };
        let tags = json!([
            { "key": "class", "value": "cold" },
            { "key": "region", "value": "us" }
        ]);
        let reversed_tags = json!([
            { "key": "region", "value": "us" },
            { "key": "class", "value": "cold" }
        ]);
        let forward = canonicalize(configuration(vec![and_rule(tags)])).unwrap();
        let reverse = canonicalize(configuration(vec![and_rule(reversed_tags)])).unwrap();
        assert_eq!(
            super::canonical_json(&forward).unwrap(),
            super::canonical_json(&reverse).unwrap()
        );

        let mut first = serde_json::to_value(valid_rule()).unwrap();
        first["id"] = json!("first");
        let mut second = serde_json::to_value(valid_rule()).unwrap();
        second["id"] = json!("second");
        let ordered = canonicalize(configuration(vec![
            rule(first.clone()),
            rule(second.clone()),
        ]))
        .unwrap();
        let reordered = canonicalize(configuration(vec![rule(second), rule(first)])).unwrap();
        assert_ne!(
            super::canonical_json(&ordered).unwrap(),
            super::canonical_json(&reordered).unwrap()
        );
    }

    #[test]
    fn projection_preserves_selector_form_and_omits_unsupported_actions() {
        let legacy = canonicalize(configuration(vec![valid_rule()])).unwrap();
        let legacy_rule = super::to_s3_rules(&legacy).unwrap().pop().unwrap();
        assert_eq!(legacy_rule.prefix.as_deref(), Some(""));
        assert!(legacy_rule.filter.is_none());
        assert!(legacy_rule.id.is_none());
        assert!(legacy_rule.transitions.is_none());
        assert!(legacy_rule.noncurrent_version_transitions.is_none());
        assert!(legacy_rule.abort_incomplete_multipart_upload.is_none());

        let mut empty_filter = serde_json::to_value(valid_rule()).unwrap();
        empty_filter.as_object_mut().unwrap().remove("prefix");
        empty_filter["filter"] = json!({});
        let modern = canonicalize(configuration(vec![rule(empty_filter)])).unwrap();
        let modern_rule = super::to_s3_rules(&modern).unwrap().pop().unwrap();
        assert!(modern_rule.prefix.is_none());
        assert_eq!(modern_rule.filter, Some(Default::default()));
        assert_eq!(modern_rule.status.as_str(), "Enabled");
        assert_eq!(modern_rule.expiration.unwrap().days, Some(1));

        let mut noncurrent = serde_json::to_value(valid_rule()).unwrap();
        noncurrent.as_object_mut().unwrap().remove("prefix");
        noncurrent.as_object_mut().unwrap().remove("expiration");
        noncurrent["filter"] = json!({});
        noncurrent["noncurrent_version_expiration"] = json!({
            "noncurrent_days": 1,
            "newer_noncurrent_versions": 100
        });
        let noncurrent = canonicalize(configuration(vec![rule(noncurrent)])).unwrap();
        let noncurrent_rule = super::to_s3_rules(&noncurrent).unwrap().pop().unwrap();
        assert_eq!(
            noncurrent_rule
                .noncurrent_version_expiration
                .unwrap()
                .newer_noncurrent_versions,
            Some(100)
        );
    }
}
use std::collections::BTreeSet;
use std::time::SystemTime;

use chrono::{DateTime, Timelike, Utc};
use s3s::dto::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule,
    LifecycleRuleAndOperator, LifecycleRuleFilter, NoncurrentVersionExpiration, Tag, Timestamp,
    TimestampFormat,
};

use crate::error::{AppError, AppResult};
use crate::lifecycle::model::{
    AbortIncompleteMultipartUploadAction, CanonicalFilter, CanonicalLifecycleConfiguration,
    CanonicalLifecycleRule, CanonicalRuleSelector, CanonicalTag, CurrentExpiration,
    LifecycleRuleStatus, NoncurrentExpiration,
};

const SCHEMA_VERSION: u8 = 1;

fn invalid(message: &'static str) -> AppError {
    AppError::InvalidLifecycleConfiguration(message.to_owned())
}

pub fn validate_and_canonicalize(
    input: BucketLifecycleConfiguration,
) -> AppResult<CanonicalLifecycleConfiguration> {
    let mut rules = Vec::with_capacity(input.rules.len());
    for rule in input.rules {
        rules.push(canonicalize_rule(rule)?);
    }

    let config = CanonicalLifecycleConfiguration {
        schema_version: SCHEMA_VERSION,
        rules,
    };
    validate_canonical_configuration(&config)?;
    Ok(config)
}

pub fn canonical_json(config: &CanonicalLifecycleConfiguration) -> AppResult<String> {
    validate_canonical_configuration(config)?;
    serde_json::to_string(config)
        .map_err(|_| invalid("lifecycle configuration cannot be serialized"))
}

pub fn from_canonical_json(json: &str) -> AppResult<CanonicalLifecycleConfiguration> {
    let config: CanonicalLifecycleConfiguration = serde_json::from_str(json)
        .map_err(|_| invalid("stored lifecycle configuration is invalid"))?;
    validate_canonical_configuration(&config)?;
    Ok(config)
}

pub fn to_s3_rules(config: &CanonicalLifecycleConfiguration) -> AppResult<Vec<LifecycleRule>> {
    validate_canonical_configuration(config)?;
    config.rules.iter().map(project_rule).collect()
}

fn validate_canonical_configuration(config: &CanonicalLifecycleConfiguration) -> AppResult<()> {
    if config.schema_version != SCHEMA_VERSION {
        return Err(invalid(
            "unsupported lifecycle configuration schema version",
        ));
    }
    if !(1..=1_000).contains(&config.rules.len()) {
        return Err(invalid(
            "lifecycle configuration must contain 1 to 1000 rules",
        ));
    }

    let mut ids = BTreeSet::new();
    for rule in &config.rules {
        if let Some(id) = &rule.id {
            if id.chars().count() > 255 {
                return Err(invalid("lifecycle rule ID must be at most 255 characters"));
            }
            if !ids.insert(id) {
                return Err(invalid("lifecycle rule IDs must be unique"));
            }
        }
        validate_canonical_rule(rule)?;
    }
    Ok(())
}

fn validate_canonical_rule(rule: &CanonicalLifecycleRule) -> AppResult<()> {
    validate_canonical_selector(&rule.selector)?;

    match &rule.expiration {
        Some(CurrentExpiration::Date { utc_midnight })
            if utc_midnight.hour() != 0
                || utc_midnight.minute() != 0
                || utc_midnight.second() != 0
                || utc_midnight.nanosecond() != 0 =>
        {
            return Err(invalid("expiration date must be UTC midnight"));
        }
        Some(CurrentExpiration::Days { days }) if *days == 0 => {
            return Err(invalid("lifecycle expiration days must be positive"));
        }
        Some(CurrentExpiration::ExpiredObjectDeleteMarker)
            if selector_has_tag_filter(&rule.selector) =>
        {
            return Err(invalid(
                "expired object delete marker action cannot use a tag filter",
            ));
        }
        Some(_) | None => {}
    }

    if let Some(noncurrent) = &rule.noncurrent_version_expiration {
        if noncurrent.noncurrent_days == 0 {
            return Err(invalid("noncurrent expiration days must be positive"));
        }
        if let Some(newer) = noncurrent.newer_noncurrent_versions {
            if !(1..=100).contains(&newer) {
                return Err(invalid(
                    "newer noncurrent versions must be between 1 and 100",
                ));
            }
            if !matches!(&rule.selector, CanonicalRuleSelector::Modern { .. }) {
                return Err(invalid(
                    "newer noncurrent versions requires an explicit modern Filter",
                ));
            }
        }
    }

    if let Some(abort) = &rule.abort_incomplete_multipart_upload {
        if abort.days_after_initiation == 0 {
            return Err(invalid("abort days after initiation must be positive"));
        }
        if abort.days_after_initiation > i32::MAX as u32 {
            return Err(invalid("abort days after initiation are out of range"));
        }
        if !matches!(
            &rule.selector,
            CanonicalRuleSelector::LegacyPrefix { .. }
                | CanonicalRuleSelector::Modern {
                    filter: CanonicalFilter::All | CanonicalFilter::Prefix { .. }
                }
        ) {
            return Err(invalid(
                "abort incomplete multipart upload requires an all or prefix selector",
            ));
        }
    }

    if rule.expiration.is_none()
        && rule.noncurrent_version_expiration.is_none()
        && rule.abort_incomplete_multipart_upload.is_none()
    {
        return Err(invalid("lifecycle rule requires a supported action"));
    }
    Ok(())
}

fn validate_canonical_selector(selector: &CanonicalRuleSelector) -> AppResult<()> {
    let CanonicalRuleSelector::Modern { filter } = selector else {
        return Ok(());
    };
    match filter {
        CanonicalFilter::All | CanonicalFilter::Prefix { .. } | CanonicalFilter::Tag { .. } => {}
        CanonicalFilter::ObjectSizeGreaterThan { bytes }
        | CanonicalFilter::ObjectSizeLessThan { bytes } => {
            nonnegative_size(*bytes)?;
        }
        CanonicalFilter::And {
            tags,
            object_size_greater_than,
            object_size_less_than,
            ..
        } => {
            if let Some(bytes) = object_size_greater_than {
                nonnegative_size(*bytes)?;
            }
            if let Some(bytes) = object_size_less_than {
                nonnegative_size(*bytes)?;
            }
            if let (Some(lower), Some(upper)) = (object_size_greater_than, object_size_less_than)
                && lower >= upper
            {
                return Err(invalid("lifecycle And size bounds must be ordered"));
            }
            if tags.windows(2).any(|pair| pair[0] > pair[1]) {
                return Err(invalid("lifecycle And tags must be sorted"));
            }
            let mut keys = BTreeSet::new();
            if tags.iter().any(|tag| !keys.insert(&tag.key)) {
                return Err(invalid("lifecycle And tag keys must be distinct"));
            }
        }
    }
    Ok(())
}

fn canonicalize_rule(rule: LifecycleRule) -> AppResult<CanonicalLifecycleRule> {
    reject_future_actions(&rule)?;

    let status = match rule.status.as_str() {
        ExpirationStatus::ENABLED => LifecycleRuleStatus::Enabled,
        ExpirationStatus::DISABLED => LifecycleRuleStatus::Disabled,
        _ => return Err(invalid("lifecycle rule status must be Enabled or Disabled")),
    };
    let selector = canonicalize_selector(rule.prefix, rule.filter)?;
    let expiration = rule.expiration.map(canonicalize_expiration).transpose()?;
    let noncurrent_version_expiration = rule
        .noncurrent_version_expiration
        .map(|value| canonicalize_noncurrent_expiration(value, &selector))
        .transpose()?;

    let abort_incomplete_multipart_upload = rule
        .abort_incomplete_multipart_upload
        .map(|abort| -> AppResult<AbortIncompleteMultipartUploadAction> {
            let days = abort
                .days_after_initiation
                .ok_or_else(|| invalid("abort days after initiation are required"))?;
            if days <= 0 {
                return Err(invalid("abort days after initiation must be positive"));
            }
            Ok(AbortIncompleteMultipartUploadAction {
                days_after_initiation: u32::try_from(days)
                    .map_err(|_| invalid("abort days after initiation are out of range"))?,
            })
        })
        .transpose()?;
    if matches!(
        expiration,
        Some(CurrentExpiration::ExpiredObjectDeleteMarker)
    ) && selector_has_tag_filter(&selector)
    {
        return Err(invalid(
            "expired object delete marker action cannot use a tag filter",
        ));
    }

    Ok(CanonicalLifecycleRule {
        id: rule.id,
        status,
        selector,
        expiration,
        noncurrent_version_expiration,
        abort_incomplete_multipart_upload,
    })
}

fn reject_future_actions(rule: &LifecycleRule) -> AppResult<()> {
    if rule.transitions.is_some() {
        return Err(invalid("lifecycle transitions are not supported"));
    }
    if rule.noncurrent_version_transitions.is_some() {
        return Err(invalid(
            "noncurrent lifecycle transitions are not supported",
        ));
    }
    Ok(())
}

fn canonicalize_selector(
    prefix: Option<String>,
    filter: Option<LifecycleRuleFilter>,
) -> AppResult<CanonicalRuleSelector> {
    match (prefix, filter) {
        (Some(_), Some(_)) => Err(invalid(
            "lifecycle rule cannot contain both Prefix and Filter",
        )),
        (Some(prefix), None) => Ok(CanonicalRuleSelector::LegacyPrefix { prefix }),
        (None, Some(filter)) => Ok(CanonicalRuleSelector::Modern {
            filter: canonicalize_filter(filter)?,
        }),
        (None, None) => Err(invalid("lifecycle rule requires Prefix or Filter")),
    }
}

fn canonicalize_filter(filter: LifecycleRuleFilter) -> AppResult<CanonicalFilter> {
    let populated = usize::from(filter.and.is_some())
        + usize::from(filter.object_size_greater_than.is_some())
        + usize::from(filter.object_size_less_than.is_some())
        + usize::from(filter.prefix.is_some())
        + usize::from(filter.tag.is_some());
    if populated > 1 {
        return Err(invalid(
            "lifecycle Filter must contain at most one selector",
        ));
    }

    match (
        filter.and,
        filter.prefix,
        filter.tag,
        filter.object_size_greater_than,
        filter.object_size_less_than,
    ) {
        (None, None, None, None, None) => Ok(CanonicalFilter::All),
        (Some(and), None, None, None, None) => canonicalize_and(and),
        (None, Some(prefix), None, None, None) => Ok(CanonicalFilter::Prefix { prefix }),
        (None, None, Some(tag), None, None) => Ok(CanonicalFilter::Tag {
            tag: canonicalize_tag(tag)?,
        }),
        (None, None, None, Some(bytes), None) => Ok(CanonicalFilter::ObjectSizeGreaterThan {
            bytes: nonnegative_size(bytes)?,
        }),
        (None, None, None, None, Some(bytes)) => Ok(CanonicalFilter::ObjectSizeLessThan {
            bytes: nonnegative_size(bytes)?,
        }),
        _ => Err(invalid(
            "lifecycle Filter must contain at most one selector",
        )),
    }
}

fn canonicalize_and(and: LifecycleRuleAndOperator) -> AppResult<CanonicalFilter> {
    let object_size_greater_than = and
        .object_size_greater_than
        .map(nonnegative_size)
        .transpose()?;
    let object_size_less_than = and
        .object_size_less_than
        .map(nonnegative_size)
        .transpose()?;
    if let (Some(lower), Some(upper)) = (object_size_greater_than, object_size_less_than)
        && lower >= upper
    {
        return Err(invalid("lifecycle And size bounds must be ordered"));
    }

    let mut tags = and
        .tags
        .unwrap_or_default()
        .into_iter()
        .map(canonicalize_tag)
        .collect::<AppResult<Vec<_>>>()?;
    tags.sort();
    if tags.windows(2).any(|pair| pair[0].key == pair[1].key) {
        return Err(invalid("lifecycle And tag keys must be distinct"));
    }

    Ok(CanonicalFilter::And {
        prefix: and.prefix,
        tags,
        object_size_greater_than,
        object_size_less_than,
    })
}

fn canonicalize_tag(tag: Tag) -> AppResult<CanonicalTag> {
    let key = tag
        .key
        .ok_or_else(|| invalid("lifecycle tag key is required"))?;
    Ok(CanonicalTag {
        key,
        value: tag.value.unwrap_or_default(),
    })
}

fn nonnegative_size(bytes: i64) -> AppResult<i64> {
    if bytes < 0 {
        return Err(invalid("lifecycle object size must be nonnegative"));
    }
    Ok(bytes)
}

fn canonicalize_expiration(expiration: LifecycleExpiration) -> AppResult<CurrentExpiration> {
    let populated = usize::from(expiration.date.is_some())
        + usize::from(expiration.days.is_some())
        + usize::from(expiration.expired_object_delete_marker.is_some());
    if populated != 1 {
        return Err(invalid(
            "lifecycle Expiration must contain exactly one action",
        ));
    }

    match (
        expiration.date,
        expiration.days,
        expiration.expired_object_delete_marker,
    ) {
        (Some(date), None, None) => Ok(CurrentExpiration::Date {
            utc_midnight: timestamp_to_utc(&date)?,
        }),
        (None, Some(days), None) => Ok(CurrentExpiration::Days {
            days: positive_days(days, "lifecycle expiration days must be positive")?,
        }),
        (None, None, Some(true)) => Ok(CurrentExpiration::ExpiredObjectDeleteMarker),
        (None, None, Some(false)) => Err(invalid(
            "expired object delete marker must be true when present",
        )),
        _ => Err(invalid(
            "lifecycle Expiration must contain exactly one action",
        )),
    }
}

fn canonicalize_noncurrent_expiration(
    expiration: NoncurrentVersionExpiration,
    selector: &CanonicalRuleSelector,
) -> AppResult<NoncurrentExpiration> {
    let noncurrent_days = expiration
        .noncurrent_days
        .ok_or_else(|| invalid("noncurrent expiration days are required"))?;
    let newer_noncurrent_versions = match expiration.newer_noncurrent_versions {
        None => None,
        Some(value) if (1..=100).contains(&value) => {
            if !matches!(selector, CanonicalRuleSelector::Modern { .. }) {
                return Err(invalid(
                    "newer noncurrent versions requires an explicit modern Filter",
                ));
            }
            Some(value as u16)
        }
        Some(_) => {
            return Err(invalid(
                "newer noncurrent versions must be between 1 and 100",
            ));
        }
    };

    Ok(NoncurrentExpiration {
        noncurrent_days: positive_days(
            noncurrent_days,
            "noncurrent expiration days must be positive",
        )?,
        newer_noncurrent_versions,
    })
}

fn positive_days(value: i32, message: &'static str) -> AppResult<u32> {
    if value <= 0 {
        return Err(invalid(message));
    }
    Ok(value as u32)
}

fn selector_has_tag_filter(selector: &CanonicalRuleSelector) -> bool {
    match selector {
        CanonicalRuleSelector::Modern {
            filter: CanonicalFilter::Tag { .. },
        } => true,
        CanonicalRuleSelector::Modern {
            filter: CanonicalFilter::And { tags, .. },
        } => !tags.is_empty(),
        _ => false,
    }
}

fn timestamp_to_utc(value: &Timestamp) -> AppResult<DateTime<Utc>> {
    let mut bytes = Vec::new();
    value
        .format(TimestampFormat::DateTime, &mut bytes)
        .map_err(|_| invalid("expiration date is invalid"))?;
    let text = std::str::from_utf8(&bytes).map_err(|_| invalid("expiration date is invalid"))?;
    let parsed = DateTime::parse_from_rfc3339(text)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| invalid("expiration date is invalid"))?;
    if Timestamp::from(SystemTime::from(parsed)) != value.clone() {
        return Err(invalid("expiration date must use UTC"));
    }
    if parsed.hour() != 0
        || parsed.minute() != 0
        || parsed.second() != 0
        || parsed.nanosecond() != 0
    {
        return Err(invalid("expiration date must be UTC midnight"));
    }
    Ok(parsed)
}

fn utc_to_timestamp(value: DateTime<Utc>) -> Timestamp {
    Timestamp::from(SystemTime::from(value))
}

fn project_rule(rule: &CanonicalLifecycleRule) -> AppResult<LifecycleRule> {
    Ok(LifecycleRule {
        abort_incomplete_multipart_upload: rule
            .abort_incomplete_multipart_upload
            .as_ref()
            .map(
                |abort| -> AppResult<s3s::dto::AbortIncompleteMultipartUpload> {
                    Ok(s3s::dto::AbortIncompleteMultipartUpload {
                        days_after_initiation: Some(
                            i32::try_from(abort.days_after_initiation).map_err(|_| {
                                invalid("abort days after initiation are out of range")
                            })?,
                        ),
                    })
                },
            )
            .transpose()?,
        expiration: rule
            .expiration
            .as_ref()
            .map(project_expiration)
            .transpose()?,
        filter: match &rule.selector {
            CanonicalRuleSelector::LegacyPrefix { .. } => None,
            CanonicalRuleSelector::Modern { filter } => Some(project_filter(filter)?),
        },
        id: rule.id.clone(),
        noncurrent_version_expiration: rule
            .noncurrent_version_expiration
            .as_ref()
            .map(project_noncurrent_expiration),
        noncurrent_version_transitions: None,
        prefix: match &rule.selector {
            CanonicalRuleSelector::LegacyPrefix { prefix } => Some(prefix.clone()),
            CanonicalRuleSelector::Modern { .. } => None,
        },
        status: match rule.status {
            LifecycleRuleStatus::Enabled => {
                ExpirationStatus::from_static(ExpirationStatus::ENABLED)
            }
            LifecycleRuleStatus::Disabled => {
                ExpirationStatus::from_static(ExpirationStatus::DISABLED)
            }
        },
        transitions: None,
    })
}

fn project_filter(filter: &CanonicalFilter) -> AppResult<LifecycleRuleFilter> {
    let mut result = LifecycleRuleFilter::default();
    match filter {
        CanonicalFilter::All => {}
        CanonicalFilter::Prefix { prefix } => result.prefix = Some(prefix.clone()),
        CanonicalFilter::Tag { tag } => result.tag = Some(project_tag(tag)),
        CanonicalFilter::ObjectSizeGreaterThan { bytes } => {
            result.object_size_greater_than = Some(*bytes)
        }
        CanonicalFilter::ObjectSizeLessThan { bytes } => {
            result.object_size_less_than = Some(*bytes)
        }
        CanonicalFilter::And {
            prefix,
            tags,
            object_size_greater_than,
            object_size_less_than,
        } => {
            result.and = Some(LifecycleRuleAndOperator {
                object_size_greater_than: *object_size_greater_than,
                object_size_less_than: *object_size_less_than,
                prefix: prefix.clone(),
                tags: (!tags.is_empty()).then(|| tags.iter().map(project_tag).collect()),
            });
        }
    }
    Ok(result)
}

fn project_tag(tag: &CanonicalTag) -> Tag {
    Tag {
        key: Some(tag.key.clone()),
        value: Some(tag.value.clone()),
    }
}

fn project_expiration(expiration: &CurrentExpiration) -> AppResult<LifecycleExpiration> {
    let mut result = LifecycleExpiration::default();
    match expiration {
        CurrentExpiration::Date { utc_midnight } => {
            result.date = Some(utc_to_timestamp(*utc_midnight))
        }
        CurrentExpiration::Days { days } => {
            result.days = Some(
                i32::try_from(*days)
                    .map_err(|_| invalid("lifecycle expiration days are out of range"))?,
            );
        }
        CurrentExpiration::ExpiredObjectDeleteMarker => {
            result.expired_object_delete_marker = Some(true);
        }
    }
    Ok(result)
}

fn project_noncurrent_expiration(expiration: &NoncurrentExpiration) -> NoncurrentVersionExpiration {
    NoncurrentVersionExpiration {
        newer_noncurrent_versions: expiration.newer_noncurrent_versions.map(i32::from),
        noncurrent_days: Some(expiration.noncurrent_days as i32),
    }
}
