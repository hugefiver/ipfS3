use super::*;
use crate::pinning::decision::DecisionEffect;
use crate::store::{
    entities::*,
    pinning::{decision, jobs::history},
};
use sea_orm::{
    ColumnTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
};

#[derive(Default)]
pub(super) struct Budget {
    rows: u64,
    bytes: u64,
}

impl Budget {
    /// Counts all text BEFORE ORM/blob allocation; names are static, not user SQL.
    async fn table(
        &mut self,
        tx: &DatabaseTransaction,
        table: &str,
        fields: &[&str],
    ) -> Result<()> {
        let text = fields
            .iter()
            .map(|field| match tx.get_database_backend() {
                DatabaseBackend::Sqlite => format!("LENGTH(CAST(COALESCE({field}, '') AS BLOB))"),
                _ => format!("OCTET_LENGTH(COALESCE({field}, ''))"),
            })
            .collect::<Vec<_>>()
            .join(" + ");
        let row = tx
            .query_one(Statement::from_string(
                tx.get_database_backend(),
                format!(
                    "SELECT COUNT(*) AS records, COALESCE(SUM({text}), 0) AS bytes FROM {table}"
                ),
            ))
            .await
            .map_err(|_| SnapshotError::DatabaseUnavailable)?
            .ok_or(SnapshotError::InvalidEvidence)?;
        self.rows = self
            .rows
            .checked_add(number(
                row.try_get("", "records")
                    .map_err(|_| SnapshotError::InvalidEvidence)?,
            )?)
            .ok_or(SnapshotError::LimitExceeded)?;
        self.bytes = self
            .bytes
            .checked_add(number(
                row.try_get("", "bytes")
                    .map_err(|_| SnapshotError::InvalidEvidence)?,
            )?)
            .ok_or(SnapshotError::LimitExceeded)?;
        if self.rows > MAX_RECORDS as u64 || self.bytes > MAX_TEXT_BYTES as u64 {
            return Err(SnapshotError::LimitExceeded);
        }
        Ok(())
    }
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
}

impl Rows {
    pub async fn load(tx: &DatabaseTransaction, budget: &mut Budget) -> Result<Self> {
        macro_rules! load {
            ($entity:ident, $table:literal, [$($column:literal),+]) => {{
                budget.table(tx, $table, &[$($column),+]).await?;
                $entity::Entity::find().limit(MAX_RECORDS as u64 + 1).all(tx).await.map_err(|_| SnapshotError::DatabaseUnavailable)?
            }};
        }
        Ok(Self {
            routes: load!(
                pin_provider_route,
                "pin_provider_routes",
                ["provider", "snapshot", "display_name"]
            ),
            leases: load!(
                pin_lease,
                "pin_leases",
                [
                    "id",
                    "owner_object_id",
                    "source",
                    "policy_id",
                    "provider_mode",
                    "content_mode",
                    "state"
                ]
            ),
            targets: load!(
                pin_lease_target,
                "pin_lease_targets",
                ["id", "lease_id", "cid", "provider", "state"]
            ),
            remotes: load!(
                remote_pin,
                "remote_pins",
                [
                    "provider",
                    "cid",
                    "request_id",
                    "status",
                    "last_failed_request_id",
                    "last_error_class",
                    "last_error_text"
                ]
            ),
            ledgers: load!(
                remote_pin_ledger,
                "remote_pin_ledger",
                [
                    "provider",
                    "cid",
                    "route",
                    "ownership",
                    "effect",
                    "first_error",
                    "last_error"
                ]
            ),
            history: load!(
                pin_resource_history,
                "pin_resource_history",
                ["provider", "cid", "ledger"]
            ),
            jobs: load!(
                pin_job,
                "pin_jobs",
                [
                    "id",
                    "operation",
                    "provider",
                    "cid",
                    "lease_id",
                    "target_id",
                    "state",
                    "submit_phase",
                    "last_error"
                ]
            ),
            invocations: load!(
                pin_invocation_route,
                "pin_invocation_routes",
                ["job_id", "route"]
            ),
            submissions: load!(
                pin_submit_observation,
                "pin_submit_observations",
                [
                    "id",
                    "job_id",
                    "provider",
                    "expected_cid",
                    "route",
                    "effect",
                    "resources",
                    "outcome",
                    "safe_error"
                ]
            ),
            submit_history: load!(
                history,
                "pin_submit_history",
                [
                    "job_id",
                    "correlation",
                    "api",
                    "strategy",
                    "effect",
                    "state",
                    "first_error",
                    "last_error"
                ]
            ),
            usage: load!(pin_provider_usage, "pin_provider_usage", ["provider"]),
        })
    }
}

pub(super) async fn objects(
    tx: &DatabaseTransaction,
    request: &PlanRequest,
    budget: &mut Budget,
) -> Result<Vec<ObjectEvidence>> {
    // Bound decision/manifest blobs before reusing the authoritative decision API.
    budget
        .table(
            tx,
            "pin_extension_decisions",
            &[
                "version_row_id",
                "object_id",
                "control_revision",
                "config_revision",
                "effect",
                "snapshot",
            ],
        )
        .await?;
    budget
        .table(
            tx,
            "zip_manifest_entries",
            &[
                "batch_id",
                "path",
                "object_key",
                "cid",
                "version_row_id",
                "error_code",
            ],
        )
        .await?;
    // No ZIP input URL, captured options, arbitrary terminal result or root receipt
    // is exported. The bounds also prevent loading unbounded batch blobs below.
    budget
        .table(
            tx,
            "zip_batches",
            &[
                "id",
                "owner",
                "source",
                "token",
                "fingerprint",
                "bucket",
                "archive_key",
                "input_identity",
                "captured_options",
                "state",
                "terminal_result",
                "root_status",
                "root_error_code",
                "root_cid",
            ],
        )
        .await?;
    let mut result: Vec<ObjectEvidence> = Vec::new();
    let selected=request.items.iter().flat_map(|item|match &item.subject {
        super::super::Subject::Object {identity,target}=>{
            let mut selected=vec![(identity,item)];
            if let super::super::TargetSelection::Existing {source:Some(source),..}=target {selected.push((source,item));}
            selected
        },_=>vec![],
    });
    for (identity,item) in selected {
        if result.iter().any(|object| object.identity == *identity) {
            continue;
        }
        let row = object_version::Entity::find_by_id(identity.private_version_id.clone())
            .one(tx)
            .await
            .map_err(|_| SnapshotError::DatabaseUnavailable)?
            .ok_or(SnapshotError::MissingExactObject)?;
        let public = match &identity.public_version {
            PublicVersionId::Null => None,
            PublicVersionId::Version(id) => {
                crate::store::object_version::PublicVersionId::parse_s3(id)
                    .map_err(|_| SnapshotError::Validation(SafeError::InvalidInput))?;
                Some(id.clone())
            }
        };
        if row.bucket != identity.bucket
            || row.key != identity.key
            || row.version_id != public
            || row.kind != "object"
            || row.object_id.as_ref() != Some(&identity.object_id)
        {
            return Err(SnapshotError::MissingExactObject);
        }
        // Compare the public binding too, without querying current/latest keys.
        let bindings = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq(&identity.bucket))
            .filter(object_version::Column::Key.eq(&identity.key));
        let bindings = match &public {
            Some(id) => bindings.filter(object_version::Column::VersionId.eq(id)),
            None => bindings.filter(object_version::Column::VersionId.is_null()),
        };
        let bindings = bindings
            .limit(2)
            .all(tx)
            .await
            .map_err(|_| SnapshotError::DatabaseUnavailable)?;
        if bindings.len() != 1 || bindings[0].id != row.id {
            return Err(SnapshotError::MissingExactObject);
        }
        // Select only safe object fields: never load key_wrap/fingerprints/metadata.
        let raw = tx
            .query_one(Statement::from_sql_and_values(
                tx.get_database_backend(),
                if tx.get_database_backend() == DatabaseBackend::Postgres {
                    "SELECT bucket, key, cid, size, encrypted FROM objects WHERE id = $1"
                } else {
                    "SELECT bucket, key, cid, size, encrypted FROM objects WHERE id = ?"
                },
                [identity.object_id.clone().into()],
            ))
            .await
            .map_err(|_| SnapshotError::DatabaseUnavailable)?
            .ok_or(SnapshotError::MissingExactObject)?;
        let original_cid: String = raw
            .try_get("", "cid")
            .map_err(|_| SnapshotError::InvalidEvidence)?;
        let bucket: String = raw
            .try_get("", "bucket")
            .map_err(|_| SnapshotError::InvalidEvidence)?;
        let key: String = raw
            .try_get("", "key")
            .map_err(|_| SnapshotError::InvalidEvidence)?;
        let cid = canonical(&original_cid)?;
        if bucket != identity.bucket
            || key != identity.key
            || cid != canonical(&identity.cid)?
            || identity.cid != cid
        {
            return Err(SnapshotError::MissingExactObject);
        }
        let captured = decision::read_for_version(tx, &row.id)
            .await
            .map_err(|_| SnapshotError::InvalidEvidence)?;
        let legacy_unknown = captured
            .as_ref()
            .is_none_or(|decision| decision.legacy_unknown);
        let decision = captured.as_ref().map(|decision| match decision.effect {
            DecisionEffect::Accepted => DecisionState::Accepted,
            DecisionEffect::Skipped => DecisionState::Skipped,
            DecisionEffect::NoIntent => DecisionState::None,
        });
        let mut manifest_bindings = Vec::new();
        for entry in zip_manifest_entry::Entity::find()
            .filter(zip_manifest_entry::Column::VersionRowId.eq(&row.id))
            .order_by_asc(zip_manifest_entry::Column::BatchId)
            .all(tx)
            .await
            .map_err(|_| SnapshotError::DatabaseUnavailable)?
        {
            let batch = zip_batch::Entity::find_by_id(entry.batch_id.clone())
                .one(tx)
                .await
                .map_err(|_| SnapshotError::DatabaseUnavailable)?
                .ok_or(SnapshotError::InvalidEvidence)?;
            if batch.state != "published"
                || !batch.manifest_prepared
                || batch.terminal_result.is_none()
            {
                continue;
            }
            if batch.bucket != bucket
                || entry.object_key.as_deref() != Some(key.as_str())
                || entry.error_code.is_some()
                || entry.size
                    != Some(
                        raw.try_get("", "size")
                            .map_err(|_| SnapshotError::InvalidEvidence)?,
                    )
                || entry.cid.as_deref().map(canonical).transpose()?.as_ref() != Some(&cid)
            {
                return Err(SnapshotError::InvalidEvidence);
            }
            manifest_bindings.push(ManifestBinding {
                batch_id: batch.id,
                entry_path: entry.path,
            });
        }
        result.push(ObjectEvidence {
            identity: identity.clone(),
            original_cid,
            logical_bytes: number(
                raw.try_get("", "size")
                    .map_err(|_| SnapshotError::InvalidEvidence)?,
            )?,
            encrypted: raw
                .try_get("", "encrypted")
                .map_err(|_| SnapshotError::InvalidEvidence)?,
            decision,
            legacy_unknown,
            control_revision: captured
                .as_ref()
                .map(|decision| decision.control_revision.clone()),
            config_revision: captured.map(|decision| decision.config_revision),
            // A batch root revision is not proof of manifest revision. Backfill
            // remains candidate even if a committed binding can be diagnosed.
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
    Ok(result)
}
