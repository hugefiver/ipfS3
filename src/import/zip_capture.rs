//! Persisted ZIP authority only. Historical decoding never consults runtime policy.
use crate::{
    import::{ImportExecutionError, ImportFailureCode, execution_error::terminal_failure},
    pinning::{
        tags::{ObjectTag, ZipRootCapture},
        zip_policy::CapturedZipIntent,
    },
    store::{
        entities::import_job, import::jobs::decision_from_job,
        pinning::publication::decode_zip_capture_json,
    },
};

pub(crate) fn legacy_capture(
    job: &import_job::Model,
) -> Result<CapturedZipIntent, ImportExecutionError> {
    let invalid = || {
        terminal_failure(
            ImportFailureCode::PublicationFailed,
            "ZIP admission capture needs_attention",
        )
    };
    let prefix = job.decompress_prefix.as_deref().ok_or_else(invalid)?;
    let tags: Vec<ObjectTag> = serde_json::from_str(&job.tags_json).map_err(|_| invalid())?;
    let decision = decision_from_job(job).map_err(|_| invalid())?;
    let capture = if let Some(json) = &job.zip_intent_json {
        decode_zip_capture_json(json).map_err(|_| invalid())?
    } else {
        // The known old empty decision has no provider authority to recover.
        // Nonempty/revision-only decisions lack frozen routes and fail closed.
        let decision = decision.ok_or_else(invalid)?;
        if !decision.effective_intents.is_empty() {
            return Err(invalid());
        }
        let root = job
            .root_capture_json
            .as_deref()
            .map(serde_json::from_str::<ZipRootCapture>)
            .transpose()
            .map_err(|_| invalid())?
            .unwrap_or(ZipRootCapture::Configured(false));
        let old = serde_json::json!({"bucket":job.bucket,"source_key":job.key,"target_prefix":prefix,
            "root_capture":root,"result_enabled":true,"decision":decision,"providers":[]});
        decode_zip_capture_json(&old.to_string()).map_err(|_| invalid())?
    };
    if capture.origin().request_id != job.id
        || capture.bucket() != job.bucket
        || capture.source_key() != job.key
        || capture.target_prefix() != prefix
        || capture.v2_options().is_some()
        || !capture.controls().publish_source
        || capture
            .source_decision()
            .replay_policy(tags.clone())
            .is_err()
    {
        return Err(invalid());
    }
    if let Some(json) = &job.root_capture_json {
        let root: ZipRootCapture = serde_json::from_str(json).map_err(|_| invalid())?;
        if root != capture.controls().root
            || crate::pinning::tags::resolve_zip_root_option(&tags, root.enabled()).ok()
                != Some(root)
        {
            return Err(invalid());
        }
    }
    Ok(capture)
}
