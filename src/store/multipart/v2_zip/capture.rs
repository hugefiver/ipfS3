use super::{Intake, Record, conflict};
use crate::{
    error::{AppError, AppResult},
    pinning::zip_policy::CapturedZipIntent,
    store::pinning::publication,
    zip::options::ZipV2Options,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    schema_version: u8,
    options: ZipV2Options,
    target_prefix: String,
    rule_revision: String,
    zip_intent: serde_json::Value,
}

pub(super) fn attention() -> AppError {
    AppError::InvalidPinningRequest(
        "ZIP v2 upload requires its original first-intake capture; needs_attention".into(),
    )
}

pub(super) fn envelope(request: &Intake, intent: &CapturedZipIntent) -> AppResult<String> {
    publication::serialize_zip_capture(intent)?;
    if intent.origin().principal_id != request.owner
        || intent.origin().request_id != request.upload_id
        || intent.bucket() != request.bucket
        || intent.source_key() != request.key
        || intent.target_prefix() != request.target_prefix
        || intent.controls().admission_token != request.token
        || intent.v2_options().as_ref() != Some(&request.options)
    {
        return Err(attention());
    }
    serde_json::to_string(&Envelope {
        schema_version: 1,
        options: request.options.clone(),
        target_prefix: request.target_prefix.clone(),
        rule_revision: request.rule_revision.clone(),
        zip_intent: serde_json::to_value(intent).map_err(|_| attention())?,
    })
    .map_err(|_| attention())
}

pub(super) fn read(record: &Record) -> AppResult<(ZipV2Options, Option<CapturedZipIntent>)> {
    let value: serde_json::Value =
        serde_json::from_str(&record.captured_options).map_err(|_| conflict())?;
    if value.get("options").is_some() {
        let envelope: Envelope = serde_json::from_value(value).map_err(|_| attention())?;
        if envelope.schema_version != 1
            || envelope.target_prefix != record.target_prefix
            || envelope.rule_revision != record.rule_revision
        {
            return Err(attention());
        }
        let intent = CapturedZipIntent::decode_json(&envelope.zip_intent.to_string())
            .map_err(|_| attention())?;
        verify(record, &envelope.options, &intent)?;
        Ok((envelope.options, Some(intent)))
    } else {
        // Old bare options can decode a durable completed receipt, not authorize
        // a fresh/NULL active upload. Old top-level capture segments are read
        // without changing their original bytes or policy.
        let intent = value
            .get("zip_intent")
            .map(|json| CapturedZipIntent::decode_json(&json.to_string()).map_err(|_| attention()))
            .transpose()?;
        let options = serde_json::from_value::<ZipV2Options>(value).map_err(|_| conflict())?;
        if let Some(intent) = &intent {
            verify(record, &options, intent)?;
        }
        Ok((options, intent))
    }
}

fn verify(record: &Record, options: &ZipV2Options, intent: &CapturedZipIntent) -> AppResult<()> {
    if intent.origin().principal_id != record.owner
        || intent.origin().request_id != record.execution_id
        || intent.bucket() != record.bucket
        || intent.source_key() != record.archive_key
        || intent.target_prefix() != record.target_prefix
        || intent.controls().admission_token != record.token
        || intent.v2_options().as_ref() != Some(options)
    {
        return Err(attention());
    }
    Ok(())
}

pub(super) fn same_contract(
    original: &ZipV2Options,
    requested: &ZipV2Options,
    explicit_targets: bool,
) -> bool {
    // Configured root and inferred omitted targets are not token identity.
    // The normalized signed contract and any explicit targets remain identity;
    // consume the original stored values, never those from the new config.
    original.publish_source == requested.publish_source
        && original.publish_extracted == requested.publish_extracted
        && (!explicit_targets || original.targets == requested.targets)
        && original.token == requested.token
        && original.root_override == requested.root_override
        && original.result_version == requested.result_version
}
