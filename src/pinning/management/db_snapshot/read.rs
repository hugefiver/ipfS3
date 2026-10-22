//! Typed diagnostics projected from the already bounded original SQL cells.
use super::*;
use crate::store::pinning::management::native::{NativeFrame, NativeRow, value};
use crate::store::{entities::*, pinning::jobs::history};

pub(super) trait Cell: Sized {
    fn parse(raw: &str) -> Result<Self>;
}
impl Cell for String {
    fn parse(raw: &str) -> Result<Self> {
        Ok(raw.to_owned())
    }
}
impl Cell for i64 {
    fn parse(raw: &str) -> Result<Self> {
        raw.parse().map_err(|_| SnapshotError::InvalidEvidence)
    }
}
impl Cell for i32 {
    fn parse(raw: &str) -> Result<Self> {
        raw.parse().map_err(|_| SnapshotError::InvalidEvidence)
    }
}
impl Cell for bool {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "1" | "true" => Ok(true),
            "0" | "false" => Ok(false),
            _ => Err(SnapshotError::InvalidEvidence),
        }
    }
}
impl Cell for chrono::DateTime<chrono::Utc> {
    fn parse(raw: &str) -> Result<Self> {
        crate::store::pinning::management::native::parse_time(raw)
            .map_err(|_| SnapshotError::InvalidEvidence)
    }
}
pub(super) trait Field: Sized {
    fn field(row: &NativeRow, name: &str) -> Result<Self>;
}
impl<T: Cell> Field for T {
    fn field(row: &NativeRow, name: &str) -> Result<Self> {
        Self::parse(value(row, name).ok_or(SnapshotError::InvalidEvidence)?)
    }
}
impl<T: Cell> Field for Option<T> {
    fn field(row: &NativeRow, name: &str) -> Result<Self> {
        row.get(name)
            .ok_or(SnapshotError::InvalidEvidence)?
            .as_deref()
            .map(T::parse)
            .transpose()
    }
}
pub(super) fn field<T: Field>(row: &NativeRow, name: &str) -> Result<T> {
    T::field(row, name)
}

pub(super) fn original_row(
    frame: &NativeFrame,
    name: &str,
    public: &NativeRow,
    work: &SharedDecodeBudget,
) -> Result<NativeRow> {
    let table = frame.table(name).ok_or(SnapshotError::InvalidEvidence)?;
    let key = table
        .primary_key
        .iter()
        .map(|column| value(public, column).ok_or(SnapshotError::InvalidEvidence))
        .collect::<Result<Vec<_>>>()?;
    let original = frame
        .original_row(name, &key)
        .ok_or(SnapshotError::InvalidEvidence)?;
    DecodeWork::shared(work)
        .copy(&original)
        .map_err(super::scoped::map_error)?;
    Ok(original)
}

pub(super) struct Rows {
    pub routes: Vec<pin_provider_route::Model>,
    pub leases: Vec<pin_lease::Model>,
    pub targets: Vec<pin_lease_target::Model>,
    pub remotes: Vec<remote_pin::Model>,
    pub ledgers: Vec<remote_pin_ledger::Model>,
    pub history: Vec<pin_resource_history::Model>,
    pub jobs: Vec<pin_job::Model>,
    pub invocations: Vec<pin_invocation_route::Model>,
    pub submissions: Vec<pin_submit_observation::Model>,
    pub submit_history: Vec<history::Model>,
    pub usage: Vec<pin_provider_usage::Model>,
    pub retryable_parks: std::collections::BTreeSet<String>,
}

impl Rows {
    pub fn project(
        frame: &NativeFrame,
        now: chrono::DateTime<chrono::Utc>,
        work: &SharedDecodeBudget,
    ) -> Result<Self> {
        macro_rules! rows {
            ($module:ident, $table:literal, $($name:ident),+ $(,)?) => {{
                frame.table($table).ok_or(SnapshotError::InvalidEvidence)?.rows.iter().map(|public| {
                    let original = original_row(frame, $table, public, work)?;
                    Ok($module::Model { $($name: field(&original, stringify!($name))?),+ })
                }).collect::<Result<Vec<_>>>()?
            }};
        }
        let mut result = Self {
            routes: rows!(
                pin_provider_route,
                "pin_provider_routes",
                provider,
                snapshot,
                display_name,
                retired
            ),
            leases: rows!(
                pin_lease,
                "pin_leases",
                id,
                owner_object_id,
                source,
                policy_id,
                provider_mode,
                content_mode,
                created_at,
                last_touched_at,
                expires_at,
                generation,
                state
            ),
            targets: rows!(
                pin_lease_target,
                "pin_lease_targets",
                id,
                lease_id,
                cid,
                logical_size,
                provider,
                state,
                created_at,
                last_touched_at
            ),
            remotes: rows!(
                remote_pin,
                "remote_pins",
                provider,
                cid,
                request_id,
                cid_size,
                status,
                epoch,
                failure_attempts,
                next_retry_at,
                last_failed_request_id,
                last_touched_at,
                last_error_class,
                last_error_text
            ),
            ledgers: rows!(
                remote_pin_ledger,
                "remote_pin_ledger",
                provider,
                cid,
                route,
                ownership,
                effect,
                first_observed_at,
                last_observed_at,
                remote_pinned_at,
                gateway_verified_at,
                content_verified_at,
                first_error,
                last_error
            ),
            history: rows!(
                pin_resource_history,
                "pin_resource_history",
                provider,
                cid,
                epoch,
                ledger
            ),
            jobs: rows!(
                pin_job,
                "pin_jobs",
                id,
                operation,
                provider,
                cid,
                lease_id,
                target_id,
                expected_generation,
                expected_remote_epoch,
                state,
                attempts,
                next_attempt_at,
                locked_until,
                submit_phase,
                last_error,
                created_at,
                updated_at
            ),
            invocations: rows!(
                pin_invocation_route,
                "pin_invocation_routes",
                job_id,
                route,
                remote_epoch
            ),
            submissions: rows!(
                pin_submit_observation,
                "pin_submit_observations",
                id,
                job_id,
                submit_call,
                provider,
                expected_cid,
                route,
                remote_epoch,
                claim_until,
                effect,
                resources,
                outcome,
                safe_error,
                needs_attention,
                started_at,
                observed_at
            ),
            submit_history: rows!(
                history,
                "pin_submit_history",
                job_id,
                correlation,
                api,
                strategy,
                effect,
                state,
                first_error,
                last_error,
                submit_calls,
                recovery_queries,
                started_at
            ),
            usage: rows!(
                pin_provider_usage,
                "pin_provider_usage",
                provider,
                reserved_bytes,
                reserved_pins,
                observed_bytes,
                observed_pins,
                observed_at
            ),
            retryable_parks: Default::default(),
        };
        for job in &result.jobs {
            if super::park::retryable(&result, job, now, work)? {
                result.retryable_parks.insert(job.id.clone());
            }
        }
        Ok(result)
    }
}

pub(super) fn objects(
    frame: &NativeFrame,
    request: &PlanRequest,
    work: &SharedDecodeBudget,
) -> Result<Vec<ObjectEvidence>> {
    use super::super::{Subject, TargetSelection};
    use crate::pinning::decision::{DecisionEffect, ExtensionDecision};
    let mut result: Vec<ObjectEvidence> = Vec::new();
    for item in &request.items {
        let Subject::Object { identity, target } = &item.subject else {
            continue;
        };
        let source = match target {
            TargetSelection::Existing { source, .. } => source.as_ref(),
            _ => None,
        };
        for identity in std::iter::once(identity).chain(source) {
            if result.iter().any(|object| object.identity == *identity) {
                continue;
            }
            let version = frame
                .row("object_versions", &[&identity.private_version_id])
                .ok_or(SnapshotError::MissingExactObject)?;
            let public = match &identity.public_version {
                PublicVersionId::Null => None,
                PublicVersionId::Version(id) => {
                    crate::store::object_version::PublicVersionId::parse_s3(id)
                        .map_err(|_| SnapshotError::Validation(SafeError::InvalidInput))?;
                    Some(id.as_str())
                }
            };
            let bindings = frame
                .table("object_versions")
                .ok_or(SnapshotError::InvalidEvidence)?
                .rows
                .iter()
                .filter(|row| {
                    value(row, "bucket") == Some(&identity.bucket)
                        && value(row, "key") == Some(&identity.key)
                        && value(row, "version_id") == public
                })
                .collect::<Vec<_>>();
            if bindings.len() != 1
                || value(bindings[0], "id") != Some(&identity.private_version_id)
                || value(version, "bucket") != Some(&identity.bucket)
                || value(version, "key") != Some(&identity.key)
                || value(version, "version_id") != public
                || value(version, "kind") != Some("object")
                || value(version, "object_id") != Some(&identity.object_id)
            {
                return Err(SnapshotError::MissingExactObject);
            }
            let object = frame
                .row("objects", &[&identity.object_id])
                .ok_or(SnapshotError::MissingExactObject)?;
            let original_cid: String = field(object, "cid")?;
            let cid = canonical(&original_cid)?;
            if value(object, "bucket") != Some(&identity.bucket)
                || value(object, "key") != Some(&identity.key)
                || cid != canonical(&identity.cid)?
                || identity.cid != cid
            {
                return Err(SnapshotError::MissingExactObject);
            }
            let logical_size: i64 = field(object, "size")?;
            let captured = if let Some(row) =
                frame.row("pin_extension_decisions", &[&identity.private_version_id])
            {
                let json = frame
                    .original_value(
                        "pin_extension_decisions",
                        &[&identity.private_version_id],
                        "snapshot",
                    )
                    .ok_or(SnapshotError::InvalidEvidence)?;
                let decision = decode::<ExtensionDecision>(json, work)?;
                decision
                    .validate_snapshot()
                    .map_err(|_| SnapshotError::InvalidEvidence)?;
                let expected = match decision.effect {
                    DecisionEffect::Accepted => "accepted",
                    DecisionEffect::Skipped => "skipped",
                    DecisionEffect::NoIntent => "no_intent",
                };
                if value(row, "effect") != Some(expected)
                    || value(row, "object_id") != Some(&identity.object_id)
                    || value(row, "control_revision") != Some(&decision.control_revision)
                    || value(row, "config_revision") != Some(&decision.config_revision)
                {
                    return Err(SnapshotError::InvalidEvidence);
                }
                Some(decision)
            } else {
                None
            };
            let legacy_unknown = captured
                .as_ref()
                .is_none_or(|decision| decision.legacy_unknown);
            let decision = captured.as_ref().map(|decision| match decision.effect {
                DecisionEffect::Accepted => DecisionState::Accepted,
                DecisionEffect::Skipped => DecisionState::Skipped,
                DecisionEffect::NoIntent => DecisionState::None,
            });
            let mut manifest_bindings = Vec::new();
            for entry in &frame
                .table("zip_manifest_entries")
                .ok_or(SnapshotError::InvalidEvidence)?
                .rows
            {
                if value(entry, "version_row_id") != Some(&identity.private_version_id) {
                    continue;
                }
                let batch_id: String = field(entry, "batch_id")?;
                let batch = frame
                    .row("zip_batches", &[&batch_id])
                    .ok_or(SnapshotError::InvalidEvidence)?;
                if value(batch, "state") != Some("published")
                    || !field::<bool>(batch, "manifest_prepared")?
                    || frame
                        .original_value("zip_batches", &[&batch_id], "terminal_result")
                        .is_none()
                {
                    continue;
                }
                if value(batch, "bucket") != Some(&identity.bucket)
                    || value(entry, "object_key") != Some(&identity.key)
                    || value(entry, "error_code").is_some()
                    || field::<Option<i64>>(entry, "size")? != Some(logical_size)
                    || value(entry, "cid").map(canonical).transpose()?.as_ref() != Some(&cid)
                {
                    return Err(SnapshotError::InvalidEvidence);
                }
                manifest_bindings.push(ManifestBinding {
                    batch_id,
                    entry_path: field(entry, "path")?,
                });
            }
            manifest_bindings
                .sort_by(|a, b| (&a.batch_id, &a.entry_path).cmp(&(&b.batch_id, &b.entry_path)));
            result.push(ObjectEvidence {
                identity: identity.clone(),
                original_cid,
                logical_bytes: number(logical_size)?,
                encrypted: field(object, "encrypted")?,
                decision,
                legacy_unknown,
                control_revision: captured
                    .as_ref()
                    .map(|decision| decision.control_revision.clone()),
                config_revision: captured
                    .as_ref()
                    .map(|decision| decision.config_revision.clone()),
                selection: if legacy_unknown
                    || decision == Some(DecisionState::Skipped)
                    || matches!(item.action, Action::Backfill(_))
                    || !manifest_bindings.is_empty()
                {
                    SelectionEvidence::LegacyZipCandidate
                } else {
                    SelectionEvidence::ExplicitObject
                },
                manifest_bindings,
            });
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nullable_native_projection_keeps_empty_distinct_from_missing_and_null() {
        let row = [("null".into(), None), ("empty".into(), Some(String::new()))].into();
        assert_eq!(field::<Option<String>>(&row, "null").unwrap(), None);
        assert_eq!(
            field::<Option<String>>(&row, "empty").unwrap(),
            Some(String::new())
        );
        assert_eq!(
            field::<Option<String>>(&row, "missing").unwrap_err(),
            SnapshotError::InvalidEvidence
        );
    }

    #[test]
    fn timestamp_projection_preserves_subsecond_comparisons() {
        let parsed =
            chrono::DateTime::<chrono::Utc>::parse("2026-10-17 12:34:56.123456+00:00").unwrap();
        assert_eq!(parsed.timestamp_subsec_nanos(), 123456000);
        assert!(bool::parse("1").unwrap());
        assert!(!bool::parse("false").unwrap());
        assert_eq!(
            bool::parse("unknown").unwrap_err(),
            SnapshotError::InvalidEvidence
        );
    }
}
