use s3s::dto::{BucketLifecycleConfiguration, LifecycleRule};
use serde_json::{Value, json};

use crate::lifecycle::model::{
    CanonicalLifecycleConfiguration, CurrentTransition, NoncurrentTransition,
};

fn rule(value: Value) -> LifecycleRule {
    serde_json::from_value(value).expect("test lifecycle rule JSON")
}

fn configuration(rule: LifecycleRule) -> BucketLifecycleConfiguration {
    BucketLifecycleConfiguration { rules: vec![rule] }
}

fn canonicalize(value: Value) -> crate::error::AppResult<CanonicalLifecycleConfiguration> {
    super::validate_and_canonicalize(configuration(rule(value)))
}

fn base_rule() -> Value {
    json!({
        "id": "transition",
        "status": "Enabled",
        "filter": {}
    })
}

fn with_action(field: &str, action: Value) -> Value {
    let mut value = base_rule();
    value[field] = action;
    value
}

#[test]
fn current_transition_date_and_days_round_trip_through_canonical_and_dto() {
    for (transition, expected) in [
        (
            json!({
                "date": "2026-09-13T00:00:00Z",
                "storage_class": "STANDARD_IA"
            }),
            CurrentTransition::Date {
                utc_midnight: "2026-09-13T00:00:00Z".parse().unwrap(),
            },
        ),
        (
            json!({ "days": 1, "storage_class": "STANDARD_IA" }),
            CurrentTransition::Days { days: 1 },
        ),
    ] {
        let canonical = canonicalize(with_action("transitions", json!([transition]))).unwrap();
        assert_eq!(canonical.rules[0].transition, Some(expected));

        let encoded = super::canonical_json(&canonical).unwrap();
        let decoded = super::from_canonical_json(&encoded).unwrap();
        assert_eq!(decoded, canonical);

        let projected = super::to_s3_rules(&decoded).unwrap();
        let projected_json = serde_json::to_value(&projected[0]).unwrap();
        assert_eq!(
            projected_json["transitions"][0]["storage_class"],
            json!("STANDARD_IA")
        );
        assert_eq!(
            super::validate_and_canonicalize(BucketLifecycleConfiguration { rules: projected })
                .unwrap(),
            canonical
        );
    }
}

#[test]
fn noncurrent_transition_round_trips_and_enforces_newer_version_filter_rule() {
    for newer in [None, Some(1), Some(100)] {
        let mut transition = json!({
            "noncurrent_days": 1,
            "storage_class": "STANDARD_IA"
        });
        if let Some(newer) = newer {
            transition["newer_noncurrent_versions"] = json!(newer);
        }
        let canonical = canonicalize(with_action(
            "noncurrent_version_transitions",
            json!([transition]),
        ))
        .unwrap();
        assert_eq!(
            canonical.rules[0].noncurrent_version_transition,
            Some(NoncurrentTransition {
                noncurrent_days: 1,
                newer_noncurrent_versions: newer,
            })
        );

        let projected = super::to_s3_rules(&canonical).unwrap();
        let projected_json = serde_json::to_value(&projected[0]).unwrap();
        assert_eq!(
            projected_json["noncurrent_version_transitions"][0]["storage_class"],
            json!("STANDARD_IA")
        );
        assert_eq!(
            super::validate_and_canonicalize(BucketLifecycleConfiguration { rules: projected })
                .unwrap(),
            canonical
        );
    }

    let legacy = json!({
        "status": "Enabled",
        "prefix": "logs/",
        "noncurrent_version_transitions": [{
            "noncurrent_days": 1,
            "newer_noncurrent_versions": 1,
            "storage_class": "STANDARD_IA"
        }]
    });
    assert!(canonicalize(legacy).is_err());
}

#[test]
fn transition_lists_are_nonempty_single_steps_to_standard_ia() {
    let valid_current = json!({ "days": 1, "storage_class": "STANDARD_IA" });
    let valid_noncurrent = json!({ "noncurrent_days": 1, "storage_class": "STANDARD_IA" });

    for transitions in [
        json!([]),
        json!([valid_current.clone(), valid_current.clone()]),
    ] {
        assert!(canonicalize(with_action("transitions", transitions)).is_err());
    }
    for transitions in [
        json!([]),
        json!([valid_noncurrent.clone(), valid_noncurrent.clone()]),
    ] {
        assert!(canonicalize(with_action("noncurrent_version_transitions", transitions)).is_err());
    }

    for transition in [
        json!({ "days": 1 }),
        json!({ "days": 1, "storage_class": "GLACIER" }),
        json!({ "days": 1, "storage_class": "standard_ia" }),
    ] {
        assert!(canonicalize(with_action("transitions", json!([transition]))).is_err());
    }
    for transition in [
        json!({ "noncurrent_days": 1 }),
        json!({ "noncurrent_days": 1, "storage_class": "GLACIER" }),
    ] {
        assert!(
            canonicalize(with_action(
                "noncurrent_version_transitions",
                json!([transition])
            ))
            .is_err()
        );
    }
}

#[test]
fn transition_timing_requires_exactly_one_valid_threshold() {
    for transition in [
        json!({ "storage_class": "STANDARD_IA" }),
        json!({ "days": 0, "storage_class": "STANDARD_IA" }),
        json!({ "days": -1, "storage_class": "STANDARD_IA" }),
        json!({
            "date": "2026-09-13T00:00:00Z",
            "days": 1,
            "storage_class": "STANDARD_IA"
        }),
        json!({
            "date": "2026-09-13T00:00:01Z",
            "storage_class": "STANDARD_IA"
        }),
    ] {
        assert!(canonicalize(with_action("transitions", json!([transition]))).is_err());
    }

    for transition in [
        json!({ "storage_class": "STANDARD_IA" }),
        json!({ "noncurrent_days": 0, "storage_class": "STANDARD_IA" }),
        json!({ "noncurrent_days": -1, "storage_class": "STANDARD_IA" }),
        json!({
            "noncurrent_days": 1,
            "newer_noncurrent_versions": 0,
            "storage_class": "STANDARD_IA"
        }),
        json!({
            "noncurrent_days": 1,
            "newer_noncurrent_versions": 101,
            "storage_class": "STANDARD_IA"
        }),
    ] {
        assert!(
            canonicalize(with_action(
                "noncurrent_version_transitions",
                json!([transition])
            ))
            .is_err()
        );
    }
}

#[test]
fn legacy_schema_one_json_defaults_transition_fields_to_absent() {
    let legacy = json!({
        "schema_version": 1,
        "rules": [{
            "id": null,
            "status": "enabled",
            "selector": { "kind": "legacy_prefix", "prefix": "" },
            "expiration": { "kind": "days", "days": 1 },
            "noncurrent_version_expiration": null,
            "abort_incomplete_multipart_upload": null
        }]
    });
    let decoded = super::from_canonical_json(&legacy.to_string()).unwrap();
    assert!(decoded.rules[0].transition.is_none());
    assert!(decoded.rules[0].noncurrent_version_transition.is_none());
}

#[test]
fn malformed_canonical_transition_values_are_rejected_on_every_boundary() {
    let valid = canonicalize(with_action(
        "transitions",
        json!([{ "days": 1, "storage_class": "STANDARD_IA" }]),
    ))
    .unwrap();
    let mut stored = serde_json::to_value(valid).unwrap();
    stored["rules"][0]["transition"] = json!({ "kind": "days", "days": 0 });
    let decoded: CanonicalLifecycleConfiguration = serde_json::from_value(stored.clone()).unwrap();
    assert!(super::canonical_json(&decoded).is_err());
    assert!(super::from_canonical_json(&stored.to_string()).is_err());
    assert!(super::to_s3_rules(&decoded).is_err());

    stored["rules"][0]["transition"] = json!({ "kind": "days", "days": 2_147_483_648_u32 });
    let decoded: CanonicalLifecycleConfiguration = serde_json::from_value(stored.clone()).unwrap();
    assert!(super::canonical_json(&decoded).is_err());
    assert!(super::from_canonical_json(&stored.to_string()).is_err());
    assert!(super::to_s3_rules(&decoded).is_err());

    stored["rules"][0]["transition"] = Value::Null;
    stored["rules"][0]["noncurrent_version_transition"] =
        json!({ "noncurrent_days": 0, "newer_noncurrent_versions": null });
    let decoded: CanonicalLifecycleConfiguration = serde_json::from_value(stored.clone()).unwrap();
    assert!(super::canonical_json(&decoded).is_err());
    assert!(super::from_canonical_json(&stored.to_string()).is_err());
    assert!(super::to_s3_rules(&decoded).is_err());
}

#[test]
fn canonical_transition_cannot_hide_extra_timing_or_storage_class() {
    let valid = canonicalize(with_action(
        "transitions",
        json!([{ "days": 1, "storage_class": "STANDARD_IA" }]),
    ))
    .unwrap();
    let mut stored = serde_json::to_value(valid).unwrap();
    for transition in [
        json!({ "kind": "days", "days": 1, "utc_midnight": "2026-09-01T00:00:00Z" }),
        json!({ "kind": "days", "days": 1, "storage_class": "GLACIER" }),
    ] {
        stored["rules"][0]["transition"] = transition;
        assert!(super::from_canonical_json(&stored.to_string()).is_err());
    }
    stored["rules"][0]["transition"] = Value::Null;
    stored["rules"][0]["noncurrent_version_transition"] = json!({
        "noncurrent_days": 1, "newer_noncurrent_versions": null, "storage_class": "STANDARD"
    });
    assert!(super::from_canonical_json(&stored.to_string()).is_err());
}
