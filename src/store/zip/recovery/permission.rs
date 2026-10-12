//! Original root permission only; no current policy or remote-plan upgrade.
use serde_json::Value;

use super::super::stale;
use crate::{
    error::AppResult,
    pinning::{tags::ZipRootCapture, zip_policy::CapturedZipIntent},
    store::{entities::zip_batch, pinning::publication::decode_zip_capture_json},
    zip::options::{ZipV2Options, decode_historical_zip_options},
};

const V2_FIELDS: [&str; 6] = [
    "publish_source",
    "publish_extracted",
    "targets",
    "token",
    "root_override",
    "result_version",
];

fn v2_options(batch: &zip_batch::Model, value: &Value) -> AppResult<ZipV2Options> {
    let captured = decode_historical_zip_options(&value.to_string(), None)
        .map_err(|_| stale())?
        .options;
    // Round-trip equality requires every original field (including nullable
    // root_override) and rejects unknown/mixed markers instead of defaulting.
    if captured.token != batch.token
        || captured.publish_source != batch.source_published
        || captured
            .root_override
            .is_some_and(|enabled| enabled != captured.root_enabled)
        || serde_json::to_value(&captured).map_err(|_| stale())? != *value
        || !captured.build_root()
    {
        return Err(stale());
    }
    Ok(captured)
}

fn prefix(value: &Value) -> AppResult<String> {
    let prefix = value
        .get("target_prefix")
        .and_then(Value::as_str)
        .ok_or_else(stale)?;
    if crate::zip::sanitize::normalize_target_prefix(prefix).map_err(|_| stale())? != prefix {
        return Err(stale());
    }
    Ok(prefix.to_owned())
}

fn verify_intent(batch: &zip_batch::Model, intent: &CapturedZipIntent) -> AppResult<()> {
    if intent.origin().principal_id != batch.owner
        || intent.origin().request_id != batch.id
        || intent.bucket() != batch.bucket
        || intent.source_key() != batch.archive_key
        || intent.controls().publish_source != batch.source_published
        || !intent.controls().publish_extracted
        || !intent.controls().root.enabled()
    {
        return Err(stale());
    }
    Ok(())
}

pub(super) fn root_prefix(batch: &zip_batch::Model, value: &Value) -> AppResult<Option<String>> {
    let object = value.as_object().ok_or_else(stale)?;
    if V2_FIELDS.iter().any(|field| object.contains_key(*field)) {
        // The original complete bare MPU contract remains sufficient for a
        // root-only retry of its already published, exactly bound manifest.
        if batch.source != "mpu" {
            return Err(stale());
        }
        v2_options(batch, value)?;
        return Ok(None);
    }
    if let Some(options) = object.get("options") {
        let captured = v2_options(batch, options)?;
        let prefix = prefix(value)?;
        if object.contains_key("root_enabled") || object.contains_key("root_capture") {
            return Err(stale());
        }
        if object.contains_key("zip_intent")
            && object.keys().any(|field| {
                ![
                    "schema_version",
                    "options",
                    "target_prefix",
                    "rule_revision",
                    "zip_intent",
                    "expected_sha256",
                    "source_content_type",
                    "source_metadata",
                    "source_tags",
                    "accepted_response",
                ]
                .contains(&field.as_str())
            })
        {
            return Err(stale());
        }
        let versioned = object.contains_key("schema_version");
        if versioned {
            if batch.source != "mpu"
                || value["schema_version"] != 1
                || object.len() != 5
                || ![
                    "schema_version",
                    "options",
                    "target_prefix",
                    "rule_revision",
                    "zip_intent",
                ]
                .iter()
                .all(|field| object.contains_key(*field))
            {
                return Err(stale());
            }
        } else if !matches!(batch.source.as_str(), "direct" | "import") {
            // MPU's new envelope has an explicit version; missing it cannot
            // silently select the historical direct envelope.
            return Err(stale());
        }
        let revision = value
            .get("rule_revision")
            .and_then(Value::as_str)
            .ok_or_else(stale)?;
        if revision.is_empty() {
            return Err(stale());
        }
        if object.contains_key("zip_intent")
            || versioned
            || object.contains_key("legacy_zip_intent")
            || object.contains_key("source_decision")
            || object.contains_key("pin_plan")
        {
            let intent = decode_zip_capture_json(&value.to_string()).map_err(|_| stale())?;
            verify_intent(batch, &intent)?;
            if intent.target_prefix() != prefix
                || intent.policy_revision() != revision
                || intent.v2_options().as_ref() != Some(&captured)
            {
                return Err(stale());
            }
        } else {
            // Known old direct/import evidence needs no new remote pin plan.
            if object.keys().any(|field| {
                ![
                    "options",
                    "target_prefix",
                    "rule_revision",
                    "expected_sha256",
                ]
                .contains(&field.as_str())
            }) {
                return Err(stale());
            }
            decode_historical_zip_options(&value.to_string(), None).map_err(|_| stale())?;
        }
        return Ok(Some(prefix));
    }
    if object.contains_key("schema_version") && !object.contains_key("zip_intent") {
        let intent = decode_zip_capture_json(&value.to_string()).map_err(|_| stale())?;
        verify_intent(batch, &intent)?;
        if let Some(options) = intent.v2_options() {
            v2_options(batch, &serde_json::to_value(options).map_err(|_| stale())?)?;
        }
        return Ok(Some(intent.target_prefix().to_owned()));
    }
    let prefix = prefix(value)?;
    if object.contains_key("schema_version") {
        return Err(stale());
    }
    let enabled = object
        .get("root_enabled")
        .map(|v| v.as_bool().ok_or_else(stale))
        .transpose()?;
    let root = object
        .get("root_capture")
        .map(|v| serde_json::from_value::<ZipRootCapture>(v.clone()).map_err(|_| stale()))
        .transpose()?;
    if enabled
        .zip(root)
        .is_some_and(|(enabled, root)| enabled != root.enabled())
        || enabled.or(root.map(ZipRootCapture::enabled)) != Some(true)
    {
        return Err(stale());
    }
    if [
        "zip_intent",
        "schema_version",
        "legacy_zip_intent",
        "source_decision",
        "pin_plan",
        "contract",
        "semantics",
        "controls",
        "policy",
        "historical_plan",
        "source_providers",
    ]
    .iter()
    .any(|field| object.contains_key(*field))
    {
        let intent = decode_zip_capture_json(&value.to_string()).map_err(|_| stale())?;
        verify_intent(batch, &intent)?;
        if intent.v2_options().is_some()
            || intent.target_prefix() != prefix
            || root.is_some_and(|root| root != intent.controls().root)
            || object
                .get("result_enabled")
                .is_some_and(|v| v.as_bool() != Some(intent.controls().result_enabled))
        {
            return Err(stale());
        }
    }
    if object
        .get("bucket")
        .is_some_and(|v| v.as_str() != Some(batch.bucket.as_str()))
        || object
            .get("source_key")
            .is_some_and(|v| v.as_str() != Some(batch.archive_key.as_str()))
    {
        return Err(stale());
    }
    Ok(Some(prefix))
}
