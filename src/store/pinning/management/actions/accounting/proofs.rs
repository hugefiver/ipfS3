use super::*;
use crate::pinning::management::{ExactObject, evidence};
use chrono::{DateTime, Utc};

struct EndpointShape;
impl<'de> serde::Deserialize<'de> for EndpointShape {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct StringShape;
        impl<'de> serde::de::Visitor<'de> for StringShape {
            type Value = EndpointShape;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("an endpoint string")
            }
            fn visit_str<E: serde::de::Error>(
                self,
                _: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(EndpointShape)
            }
        }
        deserializer.deserialize_str(StringShape)
    }
}

fn profile<T>(result: Result<T>) -> Result<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(ManagementError::Budget) => Err(ManagementError::Budget),
        Err(_) => Ok(None),
    }
}
fn originals(
    frame: records::FrozenRows<'_>,
    endpoint: Option<&str>,
    work: &mut records::DecodeBudget,
) -> Result<Vec<std::sync::Arc<ReadAttestation>>> {
    let table = frame
        .table(super::super::super::schema::RECORDS)
        .ok_or(ManagementError::Schema)?;
    let mut proofs = Vec::new();
    for row in table
        .rows
        .iter()
        .filter(|row| native::value(row, "kind") == Some("evidence_preparation"))
    {
        if native::value(row, "database_scope") != Some(frame.scope())
            || native::value(row, "native_identity") != Some(frame.identity())
            || native::value(row, "principal") != Some("local_configuration")
            || native::value(row, "state") != Some("prepared")
            || native::value(row, "completed_at").is_none()
        {
            continue;
        }
        work.nodes(1 + table.primary_key.len())?;
        let key = table
            .primary_key
            .iter()
            .map(|column| native::value(row, column).ok_or(ManagementError::InvalidEvidence))
            .collect::<Result<Vec<_>>>()?;
        let encoded = frame
            .original_value(table.table, &key, "result")
            .ok_or(ManagementError::InvalidEvidence)?;
        // Historical basis uses its original private endpoint, not current TOML.
        // Both paths still use the producer's one strict attestation decoder.
        // Historical outer protocol errors are fatal; invalid attestation
        // contents remain non-matching evidence. No endpoint copy is needed.
        if endpoint.is_none() {
            struct OuterShape;
            work.shared(encoded, |work| {
                work.charge_json_scan(encoded)?;
                let _: (serde::de::IgnoredAny, EndpointShape) =
                    serde_json::from_str(encoded).map_err(|_| ManagementError::InvalidEvidence)?;
                Ok(OuterShape)
            })?;
        }
        let parsed = evidence::from_persisted_shared_with_work(encoded, endpoint, work);
        let proof = match parsed {
            Ok(proof) => proof,
            Err(ManagementError::Budget) => return Err(ManagementError::Budget),
            Err(_) => continue,
        };
        if native::value(row, "item_id") != Some(&proof.identity.private_version_id)
            || native::parse_time(
                native::value(row, "expires_at").ok_or(ManagementError::InvalidEvidence)?,
            )? != proof.expires_at
        {
            continue;
        }
        let binding = frame
            .original_value(table.table, &key, "binding")
            .ok_or(ManagementError::InvalidEvidence)?;
        if !profile_matches(binding, &proof, work)? {
            continue;
        }
        let created = native::parse_time(
            native::value(row, "created_at").ok_or(ManagementError::InvalidEvidence)?,
        )?;
        let completed = native::parse_time(
            native::value(row, "completed_at").ok_or(ManagementError::InvalidEvidence)?,
        )?;
        if created < proof.observed_at
            || created >= proof.expires_at
            || completed < created
            || completed >= proof.expires_at
        {
            continue;
        }
        let before = frame
            .original_value(table.table, &key, "native_before")
            .ok_or(ManagementError::InvalidEvidence)?;
        let original = work.typed_shared::<(native::NativeRow, native::NativeRow)>(before)?;
        let (object, version) = original.as_ref();
        if !frame.original_row_matches("objects", &[&proof.identity.object_id], object)
            || !frame.original_row_matches(
                "object_versions",
                &[&proof.identity.private_version_id],
                version,
            )
            || !frame.projected_row_matches(
                "objects",
                &[&proof.identity.object_id],
                &proof.object_tuple,
            )
            || !frame.projected_row_matches(
                "object_versions",
                &[&proof.identity.private_version_id],
                &proof.version_tuple,
            )
        {
            continue;
        }
        work.nodes(1)?;
        proofs.push(proof);
    }
    Ok(proofs)
}

fn profile_matches(
    binding: &str,
    proof: &ReadAttestation,
    work: &mut records::DecodeBudget,
) -> Result<bool> {
    let ttl = if proof.proof == evidence::ReadProof::CompleteLocalDirectoryDagBlocksV1 {
        type Profile = (
            ExactObject,
            String,
            u64,
            bool,
            i64,
            evidence::ReadProof,
            crate::kubo::DirectoryProofBudgets,
            String,
            (u64, u32),
        );
        let Some(profile) = profile(work.typed_shared::<Profile>(binding))? else {
            return Ok(false);
        };
        let (identity, node, maximum, directory, ttl, kind, budgets, original_endpoint, idle) =
            profile.as_ref();
        let Some(receipt) = &proof.directory_receipt else {
            return Ok(false);
        };
        if identity != &proof.identity
            || node != &proof.source.node_identity
            || !*directory
            || *kind != proof.proof
            || *maximum != budgets.max_payload_bytes()
            || *budgets != receipt.budgets()
            || !proof.source.original_endpoint_matches(original_endpoint)
            || idle.1 >= 1_000_000_000
            || u64::try_from(std::time::Duration::new(idle.0, idle.1).as_millis()).ok()
                != Some(proof.source.idle_timeout_millis)
        {
            return Ok(false);
        }
        *ttl
    } else {
        let Some(profile) =
            profile(work.typed_shared::<(ExactObject, String, u64, bool, i64)>(binding))?
        else {
            return Ok(false);
        };
        let (identity, node, maximum, directory, ttl) = profile.as_ref();
        if identity != &proof.identity
            || node != &proof.source.node_identity
            || *directory != proof.proof.is_directory()
            || proof.stored_bytes.is_some_and(|bytes| bytes > *maximum)
        {
            return Ok(false);
        }
        *ttl
    };
    Ok(ttl > 0
        && ttl <= 3_600_000_000_000
        && proof
            .expires_at
            .signed_duration_since(proof.observed_at)
            .num_nanoseconds()
            .is_some_and(|duration| duration > 0 && duration <= ttl))
}

/// An archived counted directory summary is not its own receipt. The exact
/// persisted evidence row and all original private tuples must still match.
pub(crate) fn trusted_proof_with_work<'a>(
    frame: impl Into<records::FrozenRows<'a>>,
    summary: &ReadEvidence,
    work: &mut records::DecodeBudget,
) -> Result<std::sync::Arc<ReadAttestation>> {
    let mut found = None;
    for proof in originals(frame.into(), None, work)? {
        if !summary.matches_attestation(&proof) {
            continue;
        }
        if found
            .as_ref()
            .is_some_and(|old: &std::sync::Arc<ReadAttestation>| old != &proof)
        {
            return Err(ManagementError::InvalidEvidence);
        }
        found = Some(proof);
    }
    found.ok_or(ManagementError::InvalidEvidence)
}

pub(crate) fn fresh_original_proof_with_work<'a>(
    frame: impl Into<records::FrozenRows<'a>>,
    identity: &ExactObject,
    now: DateTime<Utc>,
    work: &mut records::DecodeBudget,
) -> Result<Option<std::sync::Arc<ReadAttestation>>> {
    let mut found: Option<std::sync::Arc<ReadAttestation>> = None;
    for proof in originals(frame.into(), None, work)? {
        if proof.identity != *identity
            || proof.observed_at > now
            || proof.expires_at <= now
            || !proof.proof.has_stored_byte_count()
        {
            continue;
        }
        if let Some(old) = &found
            && attestation_count(old)? != attestation_count(&proof)?
        {
            return Err(ManagementError::InvalidEvidence);
        }
        if found
            .as_ref()
            .is_none_or(|old| proof.observed_at > old.observed_at)
        {
            found = Some(proof);
        }
    }
    Ok(found)
}

/// Fresh source/config/DB-clock selection over the already complete native frame.
/// A new independently verified V1 RID may supersede a residency-only summary;
/// the original size-less RID and bytes remain untouched. Counted kind/byte
/// conflicts for the same root fail closed, including a different exact owner.
pub(crate) fn current_proof_with_work(
    frame: records::FrozenRows<'_>,
    config: &ManagementConfig,
    identity: &ExactObject,
    now: DateTime<Utc>,
    work: &mut records::ReadSession,
) -> Result<Option<std::sync::Arc<ReadAttestation>>> {
    current_proof_from_source(
        frame,
        &config.source_endpoint,
        config.source_node(),
        identity,
        now,
        work,
    )
}
pub(crate) fn current_proof_from_source(
    frame: records::FrozenRows<'_>,
    endpoint: &str,
    node: Option<&str>,
    identity: &ExactObject,
    now: DateTime<Utc>,
    work: &mut records::ReadSession,
) -> Result<Option<std::sync::Arc<ReadAttestation>>> {
    let mut found: Option<std::sync::Arc<ReadAttestation>> = None;
    let mut counted = None;
    for proof in originals(frame, Some(endpoint), work)? {
        if proof.identity.cid != identity.cid
            || node != Some(proof.source.node_identity.as_str())
            || proof.observed_at > now
            || now >= proof.expires_at
        {
            continue;
        }
        if proof.proof.has_stored_byte_count() {
            let value = count(&ReadEvidence::from(proof.as_ref()))?;
            if counted.is_some_and(|old| old != value) {
                return Err(ManagementError::InvalidEvidence);
            }
            counted = Some(value);
        }
        if proof.identity != *identity {
            continue;
        }
        if found.as_ref().is_none_or(|old| {
            (!old.proof.has_stored_byte_count() && proof.proof.has_stored_byte_count())
                || (old.proof == proof.proof && proof.observed_at > old.observed_at)
        }) {
            found = Some(proof);
        }
    }
    Ok(found)
}
