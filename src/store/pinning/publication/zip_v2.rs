//! Atomic ZIP v2 publication of exact output versions and an optional real archive.

use std::collections::{BTreeMap, BTreeSet};

use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, DatabaseTransaction,
    EntityTrait, QueryFilter, QuerySelect, Statement, TransactionError, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    pinning::{
        config::{ProviderLimitMap, ValidatedPinningConfig},
        identity::ProviderRouteSnapshot,
        policy::{LeaseSource, PublicationPolicy},
        tags::ContentMode,
        zip_policy::{ValidatedZipOutputRules, ZipPublishedOutput, ZipTargets as PolicyTargets},
    },
    store::{
        entities::{zip_batch, zip_manifest_entry},
        import::ownership::StandardMutationGuard,
        object_version::{BucketVersioningState, PublicationResult},
        zip::{self, execution, import_intake},
    },
    zip::options::{ZipTargets, ZipV2Options},
};

use super::{
    PinTargetSpec, PublicationObject, PublicationRequest, invalid_publication,
    is_retryable_transaction_conflict, leases, ledger, lock_current_object_projection,
    lock_null_object_id, lock_object_by_id, ordered_enabled_providers, publication_retry_delay,
    quota, tags, transaction_error_into_app, write_object_version_and_update_lifecycle,
};

#[derive(Debug, Clone)]
pub struct ZipV2Success {
    pub object: PublicationObject,
    /// Trusted relative ZIP path, not a substring of the complete S3 key.
    pub path: String,
    pub object_key: String,
    pub cid: String,
    pub size: i64,
    /// Exact effective automatic policy for this output. Validated against the
    /// immutable rules after its version has been inserted in the transaction.
    pub policy: PublicationPolicy,
}

#[derive(Debug, Clone)]
pub struct ZipV2Publication {
    pub claim: execution::Claim,
    pub source: Option<PublicationObject>,
    pub source_policy: Option<PublicationPolicy>,
    pub source_guard: Option<StandardMutationGuard>,
    pub successes: Vec<ZipV2Success>,
    pub targets: ZipTargets,
    pub captured_rule_revision: Option<String>,
    pub root_outcome: zip::RootOutcome,
    pub terminal_result: String,
}

#[derive(Debug, Clone)]
pub enum ZipV2PublicationResult {
    Published(Vec<(String, PublicationResult)>),
    Fenced,
}

#[derive(Clone)]
enum PublicationKind {
    Direct,
    Mpu(String),
    Import,
}

fn invalid() -> AppError {
    invalid_publication("ZIP v2 publication identity, manifest, policy or ownership mismatch")
}

fn policy_targets(targets: ZipTargets) -> PolicyTargets {
    PolicyTargets {
        source: matches!(targets, ZipTargets::Source | ZipTargets::Both),
        extracted: matches!(targets, ZipTargets::Extracted | ZipTargets::Both),
    }
}

fn captured_options(
    snapshot: &execution::Snapshot,
    request: &ZipV2Publication,
) -> AppResult<ZipV2Options> {
    let capture: serde_json::Value =
        serde_json::from_str(&snapshot.captured_options).map_err(|_| invalid())?;
    // MPU Create freezes the plain options and rule revision in separate
    // immutable intake columns. Only the MPU transaction may use this shape.
    let (options, revision): (ZipV2Options, Option<&str>) = if snapshot.source == "mpu" {
        (
            serde_json::from_value(capture).map_err(|_| invalid())?,
            request.captured_rule_revision.as_deref(),
        )
    } else {
        (
            serde_json::from_value(capture.get("options").cloned().ok_or_else(invalid)?)
                .map_err(|_| invalid())?,
            capture
                .get("rule_revision")
                .and_then(serde_json::Value::as_str),
        )
    };
    if options.result_version != 2
        || options.token != snapshot.token
        || (!options.publish_extracted && !options.publish_source)
        || (matches!(options.targets, ZipTargets::Source | ZipTargets::Both)
            && !options.publish_source)
        || (matches!(options.targets, ZipTargets::Extracted | ZipTargets::Both)
            && !options.publish_extracted)
        || options.publish_source != request.source.is_some()
        || options.publish_source != request.source_guard.is_some()
        || options.publish_source != request.source_policy.is_some()
        || options.targets != request.targets
        || snapshot.input_sha256.is_none()
        || snapshot.state != "admitted"
        || revision != request.captured_rule_revision.as_deref()
        || (request.targets != ZipTargets::None && revision.is_none())
    {
        return Err(invalid());
    }
    if request.successes.is_empty()
        && ((options.build_root() && !matches!(&request.root_outcome, zip::RootOutcome::Empty))
            || (!options.build_root()
                && !matches!(&request.root_outcome, zip::RootOutcome::Disabled)))
    {
        return Err(invalid());
    }
    if !request.successes.is_empty()
        && ((options.build_root()
            && matches!(
                &request.root_outcome,
                zip::RootOutcome::Disabled | zip::RootOutcome::Empty
            ))
            || (!options.build_root()
                && !matches!(&request.root_outcome, zip::RootOutcome::Disabled)))
    {
        return Err(invalid());
    }
    Ok(options)
}

fn validate_shape(request: &ZipV2Publication, snapshot: &execution::Snapshot) -> AppResult<()> {
    // Source=true stays on the existing archive publication path. In particular,
    // an outputs-only execution must never mutate the original source key.
    if snapshot.id != request.claim.batch_id
        || snapshot.bucket.is_empty()
        || request.terminal_result.is_empty()
        || request.terminal_result.len() > 1_048_576
        || serde_json::from_str::<serde_json::Value>(&request.terminal_result).is_err()
        || request.successes.len() > 10_000
    {
        return Err(invalid());
    }
    if let Some(source) = request.source.as_ref()
        && (source.bucket != snapshot.bucket
            || source.key != snapshot.source_key
            || Some(source.cid.as_str()) != snapshot.input_art_cid.as_deref()
            || Some(source.logical_size) != snapshot.input_art_size
            || source.logical_size < 0
            || source.encrypted
            || source.multipart != (snapshot.source == "mpu")
            || request.successes.iter().any(|s| s.object.id == source.id))
    {
        return Err(invalid());
    }
    if let Some(policy) = request.source_policy.as_ref()
        && policy.leases.iter().any(|intent| {
            intent.source != LeaseSource::Automatic || intent.content_mode != ContentMode::Object
        })
    {
        return Err(invalid());
    }
    let mut keys = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for success in &request.successes {
        if success.object.bucket != snapshot.bucket
            || success.object.key != success.object_key
            || success.object.key == snapshot.source_key
            || success.object.cid != success.cid
            || success.object.logical_size != success.size
            || success.size < 0
            || !keys.insert(success.object_key.as_str())
            || !paths.insert(success.path.as_str())
            || !ids.insert(success.object.id.as_str())
            || !success.policy.tags.is_empty()
            || success.policy.leases.iter().any(|intent| {
                intent.source != LeaseSource::Automatic
                    || intent.content_mode != ContentMode::Object
            })
        {
            return Err(invalid());
        }
    }
    Ok(())
}

/// The caller has already bound complete input bytes and admitted the *entire*
/// manifest and exact guards. No fallback admission or reassignment is allowed.
/// Unknown commit outcomes must be reconciled via execution/batch snapshots.
pub async fn publish_zip_v2(
    db: &DatabaseConnection,
    request: ZipV2Publication,
    rules: &ValidatedZipOutputRules,
    config: &ValidatedPinningConfig,
    limits: &ProviderLimitMap,
) -> AppResult<ZipV2PublicationResult> {
    publish_with_mpu(db, request, rules, config, limits, PublicationKind::Direct).await
}

impl ZipV2Publication {
    /// Complete upload and freeze its exact replay response in the very same
    /// transaction that publishes outputs and clears the execution guards.
    pub async fn publish_mpu(
        self,
        db: &DatabaseConnection,
        contract: String,
        rules: &ValidatedZipOutputRules,
        config: &ValidatedPinningConfig,
        limits: &ProviderLimitMap,
    ) -> AppResult<ZipV2PublicationResult> {
        publish_with_mpu(
            db,
            self,
            rules,
            config,
            limits,
            PublicationKind::Mpu(contract),
        )
        .await
    }

    /// Import-only entry: bind the replay receipt after execution completion
    /// inside the very same bucket-serialized publication transaction.
    pub async fn publish_import(
        self,
        db: &DatabaseConnection,
        rules: &ValidatedZipOutputRules,
        config: &ValidatedPinningConfig,
        limits: &ProviderLimitMap,
    ) -> AppResult<ZipV2PublicationResult> {
        publish_with_mpu(db, self, rules, config, limits, PublicationKind::Import).await
    }
}

async fn publish_with_mpu(
    db: &DatabaseConnection,
    request: ZipV2Publication,
    rules: &ValidatedZipOutputRules,
    config: &ValidatedPinningConfig,
    limits: &ProviderLimitMap,
    kind: PublicationKind,
) -> AppResult<ZipV2PublicationResult> {
    let snapshot = execution::read(db, &request.claim.batch_id)
        .await?
        .ok_or_else(invalid)?;
    if (snapshot.source == "import") != matches!(kind, PublicationKind::Import)
        || (snapshot.source == "mpu") != matches!(kind, PublicationKind::Mpu(_))
    {
        return Err(invalid());
    }
    captured_options(&snapshot, &request)?;
    validate_shape(&request, &snapshot)?;
    if &config.provider_limits != limits
        || (request.targets != ZipTargets::None
            && request.captured_rule_revision.as_deref() != Some(rules.revision()))
    {
        return Err(invalid());
    }
    for retry in 0..=super::MAX_TRANSACTION_RETRIES {
        let rules = rules.clone();
        let config = config.clone();
        let limits = limits.clone();
        let kind = kind.clone();
        match db
            .transaction(|tx| {
                let request = request.clone();
                let snapshot = snapshot.clone();
                Box::pin(async move {
                    publish_in_transaction(tx, &request, &snapshot, &rules, &config, &limits, &kind)
                        .await
                })
            })
            .await
        {
            Ok(results) => return Ok(ZipV2PublicationResult::Published(results)),
            Err(TransactionError::Transaction(error))
                if is_retryable_transaction_conflict(&error)
                    && retry < super::MAX_TRANSACTION_RETRIES =>
            {
                publication_retry_delay(retry).await
            }
            Err(error) => {
                // A lost output guard is terminal, but a policy/manifest/DB error
                // must not consume an intact admission. Fence in its own bucket-
                // serialized transaction only when original guards are lost.
                if matches!(
                    error,
                    TransactionError::Transaction(AppError::StaleContentMutation)
                ) && let Err(fence_error) = if request.source.is_some() {
                    execution::fence(db, &request.claim).await
                } else {
                    execution::fence_if_lost(db, &request.claim).await
                } {
                    tracing::warn!(%fence_error, "ZIP v2 guard reconciliation deferred");
                }
                return Err(transaction_error_into_app(error));
            }
        }
    }
    unreachable!("ZIP v2 transaction retries exhausted")
}

fn verify_admission_snapshot(
    expected: &execution::Snapshot,
    actual: &execution::Snapshot,
) -> AppResult<()> {
    // Lease renewal may move lease_until, but the admitted signed contract,
    // clean-EOF input and owner may not move after the bucket-serialized hint.
    if expected.id != actual.id
        || expected.owner != actual.owner
        || expected.source != actual.source
        || expected.token != actual.token
        || expected.request_fingerprint != actual.request_fingerprint
        || expected.request_contract != actual.request_contract
        || expected.bucket != actual.bucket
        || expected.source_key != actual.source_key
        || expected.captured_options != actual.captured_options
        || expected.input_sha256 != actual.input_sha256
        || expected.input_art_cid != actual.input_art_cid
        || expected.input_art_size != actual.input_art_size
        || expected.epoch != actual.epoch
        || expected.worker != actual.worker
        || expected.state != actual.state
    {
        return Err(invalid());
    }
    Ok(())
}

async fn lock_root_batch(
    tx: &DatabaseTransaction,
    snapshot: &execution::Snapshot,
) -> AppResult<Option<StandardMutationGuard>> {
    let query = zip_batch::Entity::find_by_id(&snapshot.id);
    let row = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(tx).await?
    } else {
        query.one(tx).await?
    }
    .ok_or_else(invalid)?;
    if row.state != "open"
        || !row.manifest_prepared
        || row.source_published
        || row.owner != snapshot.owner
        || row.source != snapshot.source
        || row.token != snapshot.token
        // The mirror is admitted before complete input EOF. These fixed markers
        // are not the signed request fingerprint or the clean-input SHA-256;
        // the admitted execution row supplies those immutable attestations.
        || row.fingerprint != "pending"
        || row.bucket != snapshot.bucket
        || row.archive_key != snapshot.source_key
        || row.captured_options != snapshot.captured_options
        || (row.input_identity != "pending"
            && !row.input_identity.starts_with("zip-v2-source-gen:"))
        || snapshot.input_sha256.is_none()
    {
        return Err(invalid());
    }
    source_guard_from_batch(&row, snapshot)
}

fn source_guard_from_batch(
    row: &zip_batch::Model,
    snapshot: &execution::Snapshot,
) -> AppResult<Option<StandardMutationGuard>> {
    let Some(generation) = row.input_identity.strip_prefix("zip-v2-source-gen:") else {
        return if row.input_identity == "pending" {
            Ok(None)
        } else {
            Err(invalid())
        };
    };
    let result = StandardMutationGuard {
        bucket: snapshot.bucket.clone(),
        key: snapshot.source_key.clone(),
        mutation_id: format!("zip-v2-source:{}", snapshot.id),
        expected_generation: generation.parse().map_err(|_| invalid())?,
        mutation_prefix: None,
    };
    if result.mutation_id != format!("zip-v2-source:{}", snapshot.id)
        || result.expected_generation < 1
    {
        return Err(invalid());
    }
    Ok(Some(result))
}

async fn verify_both_manifests(
    tx: &DatabaseTransaction,
    request: &ZipV2Publication,
) -> AppResult<()> {
    let query = zip_manifest_entry::Entity::find()
        .filter(zip_manifest_entry::Column::BatchId.eq(&request.claim.batch_id));
    let legacy = if tx.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_shared().all(tx).await?
    } else {
        query.all(tx).await?
    };
    let placeholder = if tx.get_database_backend() == DatabaseBackend::Postgres {
        "$1"
    } else {
        "?"
    };
    let rows = tx.query_all(Statement::from_sql_and_values(tx.get_database_backend(),
        format!("SELECT path,object_key,cid,size,error_code FROM zip_v2_manifest WHERE batch_id={placeholder}"),
        [request.claim.batch_id.clone().into()])).await?;
    if legacy.len() != rows.len() {
        return Err(invalid());
    }
    let mut published = BTreeSet::new();
    let legacy = legacy
        .into_iter()
        .map(|entry| (entry.path.clone(), entry))
        .collect::<BTreeMap<_, _>>();
    for row in rows {
        let path: String = row.try_get("", "path")?;
        let key: Option<String> = row.try_get("", "object_key")?;
        let cid: Option<String> = row.try_get("", "cid")?;
        let size: Option<i64> = row.try_get("", "size")?;
        let error_code: Option<String> = row.try_get("", "error_code")?;
        let entry = legacy.get(&path).ok_or_else(invalid)?;
        if entry.object_key != key
            || entry.cid != cid
            || entry.size != size
            || entry.error_code != error_code
            || entry.version_row_id.is_some()
        {
            return Err(invalid());
        }
        if let Some(key) = key {
            published.insert((
                path,
                key,
                cid.ok_or_else(invalid)?,
                size.ok_or_else(invalid)?,
            ));
        }
    }
    let expected = request
        .successes
        .iter()
        .map(|s| (s.path.clone(), s.object_key.clone(), s.cid.clone(), s.size))
        .collect::<BTreeSet<_>>();
    if published != expected {
        return Err(invalid());
    }
    Ok(())
}

async fn lock_version_frontier(
    tx: &DatabaseTransaction,
    bucket: &str,
    keys: &BTreeSet<String>,
    state: BucketVersioningState,
) -> AppResult<Vec<String>> {
    let mut owners = Vec::new();
    for key in keys {
        crate::store::object_version::lock_current_row(tx, bucket, key).await?;
        crate::store::object_version::allocate_next_sequence(tx, bucket, key).await?;
        let current = lock_current_object_projection(tx, bucket, key).await?;
        match state {
            BucketVersioningState::Unversioned => {
                if let Some(current) = current {
                    owners.push(current.id);
                }
            }
            BucketVersioningState::Enabled => {}
            BucketVersioningState::Suspended => {
                if let Some(id) = lock_null_object_id(tx, bucket, key).await? {
                    lock_object_by_id(tx, &id).await?;
                    owners.push(id);
                }
            }
        }
    }
    owners.sort();
    owners.dedup();
    Ok(owners)
}

fn attachment_pairs(
    request: &ZipV2Publication,
    limits: &ProviderLimitMap,
) -> AppResult<Vec<(String, String)>> {
    let mut pairs = BTreeSet::new();
    for success in &request.successes {
        for intent in &success.policy.leases {
            for provider in ordered_enabled_providers(intent, limits)? {
                pairs.insert((provider, success.cid.clone()));
            }
        }
    }
    if let (Some(source), Some(policy)) = (&request.source, &request.source_policy) {
        for intent in &policy.leases {
            for provider in ordered_enabled_providers(intent, limits)? {
                pairs.insert((provider, source.cid.clone()));
            }
        }
    }
    Ok(pairs.into_iter().collect())
}

async fn plan_written_versions(
    tx: &DatabaseTransaction,
    request: &ZipV2Publication,
    rules: &ValidatedZipOutputRules,
    config: &ValidatedPinningConfig,
    limits: &ProviderLimitMap,
    results: &[(String, PublicationResult)],
) -> AppResult<BTreeMap<String, ProviderRouteSnapshot>> {
    if request.targets == ZipTargets::None {
        if request
            .source_policy
            .as_ref()
            .is_some_and(|p| !p.leases.is_empty())
            || request
                .successes
                .iter()
                .any(|output| !output.policy.leases.is_empty())
        {
            return Err(invalid());
        }
        return Ok(BTreeMap::new());
    }
    if request.captured_rule_revision.as_deref() != Some(rules.revision()) {
        return Err(invalid());
    }
    let output_versions = results
        .iter()
        .map(|(key, result)| (key.as_str(), result))
        .collect::<BTreeMap<_, _>>();
    let mut outputs = Vec::new();
    for success in &request.successes {
        let result = output_versions
            .get(success.object_key.as_str())
            .ok_or_else(invalid)?;
        let binding =
            zip::binding_for_published_object(tx, &success.path, &result.object_id).await?;
        outputs.push(ZipPublishedOutput {
            bucket: success.object.bucket.clone(),
            key: success.object_key.clone(),
            version_id: binding.version_row_id,
            cid: success.cid.clone(),
        });
    }
    // Plan only with private, newly inserted version-row IDs in the same txn.
    // It is never an executable/network authorization by itself.
    let source = if let Some(source) = &request.source {
        let result = output_versions
            .get(source.key.as_str())
            .ok_or_else(invalid)?;
        let binding = zip::binding_for_published_object(tx, "source", &result.object_id).await?;
        Some(ZipPublishedOutput {
            bucket: source.bucket.clone(),
            key: source.key.clone(),
            version_id: binding.version_row_id,
            cid: source.cid.clone(),
        })
    } else {
        None
    };
    let plan = rules
        .plan(policy_targets(request.targets), source, &outputs)
        .map_err(|_| invalid())?;
    if plan.rule_revision != rules.revision() {
        return Err(invalid());
    }
    let mut routes = BTreeMap::new();
    let actual = request
        .successes
        .iter()
        .map(|output| (output.object_key.as_str(), output))
        .collect::<BTreeMap<_, _>>();
    for decision in &plan.outputs {
        let policy = if decision.kind == crate::pinning::zip_policy::ZipOutputKind::Source {
            if request
                .source
                .as_ref()
                .is_none_or(|s| s.key != decision.output.key)
            {
                return Err(invalid());
            }
            request.source_policy.as_ref().ok_or_else(invalid)?
        } else {
            &actual
                .get(decision.output.key.as_str())
                .ok_or_else(invalid)?
                .policy
        };
        let expected = decision
            .intents
            .iter()
            .map(|planned| planned.intent.clone())
            .collect::<Vec<_>>();
        if policy.leases != expected {
            return Err(invalid());
        }
        for planned in &decision.intents {
            if !config.policies.iter().any(|policy| {
                policy.identity == planned.intent.policy_id
                    && policy.trigger == crate::pinning::config::PolicyTrigger::Always
            }) {
                return Err(invalid());
            }
            for provider in &planned.providers {
                let current = config
                    .providers
                    .iter()
                    .find(|entry| entry.name == provider.config_name)
                    .ok_or_else(invalid)?;
                let limit = limits.get(&provider.config_name).ok_or_else(invalid)?;
                if current.identity.route_snapshot() != provider.route
                    || current.limits != *limit
                    || current.limits.enabled != provider.enabled
                {
                    return Err(invalid());
                }
                if provider.enabled
                    && routes
                        .insert(provider.config_name.clone(), provider.route.clone())
                        .is_some_and(|old| old != provider.route)
                {
                    return Err(invalid());
                }
            }
        }
    }
    // Untargeted source/entries cannot silently acquire automatic leases.
    if !policy_targets(request.targets).source
        && request
            .source_policy
            .as_ref()
            .is_some_and(|p| !p.leases.is_empty())
    {
        return Err(invalid());
    }
    if !policy_targets(request.targets).extracted
        && request
            .successes
            .iter()
            .any(|s| !s.policy.leases.is_empty())
    {
        return Err(invalid());
    }
    Ok(routes)
}

async fn publish_in_transaction(
    tx: &DatabaseTransaction,
    request: &ZipV2Publication,
    expected: &execution::Snapshot,
    rules: &ValidatedZipOutputRules,
    config: &ValidatedPinningConfig,
    limits: &ProviderLimitMap,
    kind: &PublicationKind,
) -> AppResult<Vec<(String, PublicationResult)>> {
    // The outside read is a non-locking routing hint only. In SQLite, even a
    // SELECT inside the transaction before this write lock can pin a stale WAL
    // snapshot; in PostgreSQL it reverses the bucket-first publication order.
    crate::store::import::ownership::lock_bucket_for_ownership(tx, &expected.bucket).await?;
    let initial = execution::read(tx, &request.claim.batch_id)
        .await?
        .ok_or_else(invalid)?;
    verify_admission_snapshot(expected, &initial)?;
    if matches!(kind, PublicationKind::Import) {
        let outputs = request
            .successes
            .iter()
            .map(|s| (s.path.as_str(), s.object_key.as_str()))
            .collect::<Vec<_>>();
        import_intake::verify_publication_in_transaction(tx, &initial, &outputs).await?;
    }
    if let PublicationKind::Mpu(contract) = kind {
        let intake = crate::store::multipart::v2_zip::read_by_upload(tx, &initial.id)
            .await?
            .ok_or_else(invalid)?;
        if intake.active_upload_id.as_deref() != Some(initial.id.as_str())
            || intake.execution_id != initial.id
            || intake.owner != initial.owner
            || intake.token != initial.token
            || intake.bucket != initial.bucket
            || intake.archive_key != initial.source_key
            || intake.captured_options != initial.captured_options
            || intake.request_fingerprint != initial.request_fingerprint
            || request.captured_rule_revision.as_deref() != Some(&intake.rule_revision)
            || contract.is_empty()
        {
            return Err(invalid());
        }
    }
    captured_options(&initial, request)?;
    validate_shape(request, &initial)?;
    // Stable lock order: bucket, entire exact ownership set, version frontier,
    // root batch / execution / manifest, lifecycle / remote / quota, writes.
    let mut keys = request
        .successes
        .iter()
        .map(|success| success.object_key.clone())
        .collect::<BTreeSet<_>>();
    if request.source.is_some() {
        keys.insert(initial.source_key.clone());
    }
    let guards = execution::read_targets(tx, &request.claim).await?;
    if guards
        .iter()
        .map(|guard| guard.key.clone())
        .collect::<BTreeSet<_>>()
        != keys
            .iter()
            .filter(|key| *key != &initial.source_key)
            .cloned()
            .collect::<BTreeSet<_>>()
    {
        return Err(invalid());
    }
    let ordered_keys = guards
        .iter()
        .map(|guard| guard.key.clone())
        .collect::<Vec<_>>();
    crate::store::import::ownership::verify_zip_output_guards_in_transaction(
        tx,
        &initial.bucket,
        &initial.source_key,
        &ordered_keys,
        &guards,
    )
    .await?;
    let state = crate::store::bucket::lock_versioning_state(tx, &initial.bucket).await?;
    let owners = lock_version_frontier(tx, &initial.bucket, &keys, state).await?;
    let stored_source_guard = lock_root_batch(tx, &initial).await?;
    if stored_source_guard != request.source_guard {
        return Err(invalid());
    }
    if let Some(guard) = &stored_source_guard {
        crate::store::import::ownership::verify_standard_mutation_guard(
            tx,
            guard,
            &initial.bucket,
            &initial.source_key,
            &[],
        )
        .await?;
    }
    verify_both_manifests(tx, request).await?;
    execution::verify_in_transaction(tx, &request.claim).await?;
    let pairs = attachment_pairs(request, limits)?;
    leases::lock_publication_lifecycle_frontier(tx, &owners, &pairs).await?;
    let providers = pairs
        .iter()
        .map(|(provider, _)| provider.clone())
        .collect::<Vec<_>>();
    quota::lock_publication_usage_rows(tx, &providers).await?;
    // Include every installed version in the same sorted, deduplicated CID
    // frontier; installing the source later must not acquire an earlier CID.
    crate::store::residency::prepare_hot_publication_frontier(
        tx,
        request
            .successes
            .iter()
            .map(|s| s.cid.clone())
            .chain(request.source.iter().map(|s| s.cid.clone()))
            .collect(),
    )
    .await?;
    let now = crate::store::database_clock::database_now(tx).await?;
    let mut results = Vec::new();
    if let Some(source) = &request.source {
        let result = write_object_version_and_update_lifecycle(tx, source, state, now).await?;
        results.push((source.key.clone(), result));
    }
    for success in &request.successes {
        let result =
            write_object_version_and_update_lifecycle(tx, &success.object, state, now).await?;
        results.push((success.object_key.clone(), result));
    }
    let routes = plan_written_versions(tx, request, rules, config, limits, &results).await?;
    ledger::verify_selected_routes(tx, &routes, true).await?;
    if let (Some(source), Some(policy)) = (&request.source, &request.source_policy) {
        tags::replace_object_tags(tx, &source.id, &policy.tags).await?;
        super::create_publication_leases(
            tx,
            &PublicationRequest {
                object: source.clone(),
                tags: policy.tags.clone(),
                policy: policy.clone(),
                object_target: PinTargetSpec {
                    cid: source.cid.clone(),
                    logical_size: source.logical_size,
                },
            },
            &[],
            limits,
            now,
        )
        .await?;
    }
    let mut bindings = Vec::new();
    for success in &request.successes {
        tags::replace_object_tags(tx, &success.object.id, &success.policy.tags).await?;
        let publication = PublicationRequest {
            object: success.object.clone(),
            tags: success.policy.tags.clone(),
            policy: success.policy.clone(),
            object_target: PinTargetSpec {
                cid: success.cid.clone(),
                logical_size: success.size,
            },
        };
        super::create_publication_leases(tx, &publication, &[], limits, now).await?;
        bindings
            .push(zip::binding_for_published_object(tx, &success.path, &success.object.id).await?);
    }
    let mut terminal: serde_json::Value =
        serde_json::from_str(&request.terminal_result).map_err(|_| invalid())?;
    if let Some(source) = &request.source {
        let result = results
            .iter()
            .find(|(key, _)| key == &source.key)
            .ok_or_else(invalid)?;
        let map = terminal.as_object_mut().ok_or_else(invalid)?;
        map.insert("source_cid".into(), source.cid.clone().into());
        map.insert("source_size".into(), source.logical_size.into());
        let binding = zip::binding_for_published_object(tx, "source", &result.1.object_id).await?;
        map.insert(
            "source_version_row_id".into(),
            binding.version_row_id.into(),
        );
        map.insert(
            "source_version_id".into(),
            serde_json::to_value(&result.1.version_id).map_err(|_| invalid())?,
        );
        map.insert(
            "source_policy".into(),
            serde_json::json!({"tags": request.source_policy.as_ref().ok_or_else(invalid)?.tags,
            "leases": request.source_policy.as_ref().ok_or_else(invalid)?.leases}),
        );
    }
    let terminal = terminal.to_string();
    match &request.root_outcome {
        zip::RootOutcome::ClaimedFailed { claim, code } => {
            claim
                .publish_failed(tx, &bindings, request.source.is_some(), &terminal, code)
                .await?
        }
        outcome => {
            zip::publish(
                tx,
                &request.claim.batch_id,
                &bindings,
                request.source.is_some(),
                &terminal,
                outcome.clone(),
            )
            .await?
        }
    }
    // Must be the final authority write; failure rolls back versions, leases,
    // manifest bindings and root adoption together.
    if let Some(guard) = &stored_source_guard {
        crate::store::import::ownership::complete_standard_mutation_in_transaction(tx, guard, now)
            .await?;
    }
    execution::complete_in_transaction(tx, &request.claim, &terminal).await?;
    if matches!(kind, PublicationKind::Import) {
        import_intake::complete_publication_in_transaction(tx, &initial, &request.claim, &terminal)
            .await?;
    }
    if let PublicationKind::Mpu(contract) = kind {
        let source_result = request
            .source
            .as_ref()
            .map(|source| {
                results
                    .iter()
                    .find(|(key, _)| key == &source.key)
                    .map(|(_, result)| (source.cid.as_str(), result))
                    .ok_or_else(invalid)
            })
            .transpose()?;
        crate::store::multipart::v2_zip::finalize_complete_in_transaction(
            tx,
            &request.claim,
            contract,
            source_result,
        )
        .await?;
    }
    Ok(results)
}
