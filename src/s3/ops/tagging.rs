use std::sync::Arc;

use chrono::{DateTime, Utc};
use s3s::{S3Request, S3Response, S3Result, dto::*};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, TransactionError, TransactionTrait,
};

use crate::{
    error::{AppError, AppResult},
    pinning::{
        policy::{ExistingManualLease, ExistingManualLeaseState, ManualLeaseMutation},
        tags::{ContentMode, ObjectTag, PinControl, validate_tag_set},
    },
    state::AppState,
    store::{
        entities::{object, pin_lease},
        object_version::{
            BucketVersioningState, PublicVersionId, ResolvedVersion, VersionKind, VersionSelector,
        },
        pinning::{leases, tags},
    },
};

const INVALID_TAG_SET: &str = "invalid object tag set";
const INVALID_STORED_MANUAL_LEASE: &str = "stored manual pin lease is invalid";
const MANUAL_RENEWAL_REJECTED: &str = "manual pin lease cannot be renewed";
const MANUAL_LEASE_CHANGED: &str = "manual pin lease changed during tag replacement";

#[cfg(test)]
mod test_hooks {
    use std::sync::{Arc, LazyLock, Mutex};

    use tokio::sync::Notify;

    pub struct PolicyEvaluatedGate {
        pub lease_id: &'static str,
        pub arrived: Notify,
        pub resume: Notify,
    }

    pub static POLICY_EVALUATED: LazyLock<Mutex<Option<Arc<PolicyEvaluatedGate>>>> =
        LazyLock::new(|| Mutex::new(None));
    pub static POLICY_EVALUATED_TEST_LOCK: LazyLock<tokio::sync::Mutex<()>> =
        LazyLock::new(|| tokio::sync::Mutex::new(()));

    pub fn install_policy_evaluated_gate(
        gate: Arc<PolicyEvaluatedGate>,
    ) -> PolicyEvaluatedGateInstallation {
        let mut installed = POLICY_EVALUATED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(
            installed.is_none(),
            "policy-evaluated test gate is already installed"
        );
        *installed = Some(gate.clone());
        PolicyEvaluatedGateInstallation { gate }
    }

    pub struct PolicyEvaluatedGateInstallation {
        gate: Arc<PolicyEvaluatedGate>,
    }

    impl Drop for PolicyEvaluatedGateInstallation {
        fn drop(&mut self) {
            let mut installed = POLICY_EVALUATED
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if installed
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &self.gate))
            {
                *installed = None;
            }
        }
    }
}

#[cfg(test)]
async fn pause_after_policy_evaluation(lease_id: Option<&str>) {
    let gate = test_hooks::POLICY_EVALUATED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some(gate) = gate.filter(|gate| Some(gate.lease_id) == lease_id) {
        gate.arrived.notify_one();
        gate.resume.notified().await;
    }
}

pub async fn get_object_tagging(
    state: &Arc<AppState>,
    req: S3Request<GetObjectTaggingInput>,
) -> S3Result<S3Response<GetObjectTaggingOutput>> {
    let input = req.input;
    let selector = version_selector(input.version_id.as_deref())?;
    let resolved = crate::store::object_version::resolve_version(
        state.store.db(),
        &input.bucket,
        &input.key,
        &selector,
    )
    .await?;
    let owner_id = selected_content_owner(&resolved, matches!(selector, VersionSelector::Current))?
        .id
        .clone();
    let version_id = response_version_id(state.store.db(), &input.bucket, &resolved).await?;
    let stored = tags::list_object_tags(state.store.db(), &owner_id)
        .await
        .map_err(AppError::from)
        .map_err(s3s::S3Error::from)?;
    let tag_set = stored
        .into_iter()
        .map(|tag| Tag {
            key: Some(tag.key),
            value: Some(tag.value),
        })
        .collect();

    Ok(S3Response::new(GetObjectTaggingOutput {
        tag_set,
        version_id,
    }))
}

pub async fn put_object_tagging(
    state: &Arc<AppState>,
    req: S3Request<PutObjectTaggingInput>,
) -> S3Result<S3Response<PutObjectTaggingOutput>> {
    put_object_tagging_at(state, req, Utc::now()).await
}

pub(crate) async fn put_object_tagging_at(
    state: &Arc<AppState>,
    req: S3Request<PutObjectTaggingInput>,
    now: DateTime<Utc>,
) -> S3Result<S3Response<PutObjectTaggingOutput>> {
    let input = req.input;
    let selector = version_selector(input.version_id.as_deref())?;
    let replacement = dto_to_object_tags(input.tagging.tag_set).map_err(s3s::S3Error::from)?;
    validate_replacement(&replacement).map_err(s3s::S3Error::from)?;
    let version_id = replace_tag_set(state, input.bucket, input.key, selector, replacement, now)
        .await
        .map_err(s3s::S3Error::from)?;
    Ok(S3Response::new(PutObjectTaggingOutput { version_id }))
}

pub async fn delete_object_tagging(
    state: &Arc<AppState>,
    req: S3Request<DeleteObjectTaggingInput>,
) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
    delete_object_tagging_at(state, req, Utc::now()).await
}

pub(crate) async fn delete_object_tagging_at(
    state: &Arc<AppState>,
    req: S3Request<DeleteObjectTaggingInput>,
    now: DateTime<Utc>,
) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
    let input = req.input;
    let selector = version_selector(input.version_id.as_deref())?;
    let replacement = Vec::new();
    validate_replacement(&replacement).map_err(s3s::S3Error::from)?;
    let version_id = replace_tag_set(state, input.bucket, input.key, selector, replacement, now)
        .await
        .map_err(s3s::S3Error::from)?;
    Ok(S3Response::new(DeleteObjectTaggingOutput { version_id }))
}

fn dto_to_object_tags(tag_set: Vec<Tag>) -> AppResult<Vec<ObjectTag>> {
    tag_set
        .into_iter()
        .map(|tag| {
            let key = tag
                .key
                .ok_or_else(|| invalid_pinning_request(INVALID_TAG_SET))?;
            let value = tag
                .value
                .ok_or_else(|| invalid_pinning_request(INVALID_TAG_SET))?;
            Ok(ObjectTag::new(key, value))
        })
        .collect()
}

fn validate_replacement(replacement: &[ObjectTag]) -> AppResult<()> {
    validate_tag_set(replacement).map_err(|_| invalid_pinning_request(INVALID_TAG_SET))?;
    PinControl::from_tags(replacement).map_err(|_| invalid_pinning_request(INVALID_TAG_SET))?;
    Ok(())
}

fn version_selector(version_id: Option<&str>) -> AppResult<VersionSelector> {
    match version_id {
        Some(version_id) => Ok(VersionSelector::Exact(PublicVersionId::parse_s3(
            version_id,
        )?)),
        None => Ok(VersionSelector::Current),
    }
}

fn selected_content_owner(resolved: &ResolvedVersion, current: bool) -> AppResult<&object::Model> {
    match resolved.kind {
        VersionKind::DeleteMarker => Err(AppError::DeleteMarker {
            version_id: resolved.public_version_id.clone(),
            created_at: resolved.created_at,
            current,
        }),
        VersionKind::Object => resolved.object.as_ref().ok_or_else(|| {
            AppError::Internal("object version index is missing its object".to_owned())
        }),
    }
}

async fn response_version_id<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    resolved: &ResolvedVersion,
) -> AppResult<Option<String>> {
    Ok(
        (crate::store::bucket::get_versioning_state(db, bucket).await?
            != BucketVersioningState::Unversioned)
            .then_some(resolved.public_version_id.clone()),
    )
}

async fn replace_tag_set(
    state: &Arc<AppState>,
    bucket: String,
    key: String,
    selector: VersionSelector,
    replacement: Vec<ObjectTag>,
    now: DateTime<Utc>,
) -> AppResult<Option<String>> {
    let operation_state = Arc::clone(state);
    let selector_for_recheck = selector.clone();
    let bucket_for_recheck = bucket.clone();
    let key_for_recheck = key.clone();
    let result = state
        .store
        .db()
        .transaction(move |txn| {
            Box::pin(async move {
                let selected = crate::store::object_version::lock_resolved_version(
                    txn, &bucket, &key, &selector,
                )
                .await?;
                let owner_id = selected_content_owner(
                    &selected,
                    matches!(&selector, VersionSelector::Current),
                )?
                .id
                .clone();
                let version_id = response_version_id(txn, &bucket, &selected).await?;
                let manual = load_manual_lease(txn, &owner_id).await?;
                let existing = manual.as_ref().map(existing_manual_lease).transpose()?;
                let mutation = operation_state
                    .pinning
                    .policy()
                    .evaluate_tag_replacement(existing.as_ref(), &replacement, now)
                    .map_err(AppError::from)?;

                #[cfg(test)]
                pause_after_policy_evaluation(manual.as_ref().map(|lease| lease.id.as_str())).await;

                let revalidated = crate::store::object_version::lock_resolved_version(
                    txn, &bucket, &key, &selector,
                )
                .await?;
                let revalidated_owner_id = selected_content_owner(
                    &revalidated,
                    matches!(&selector, VersionSelector::Current),
                )?
                .id
                .clone();
                if revalidated_owner_id != owner_id {
                    return Err(AppError::StaleContentMutation);
                }

                match mutation {
                    ManualLeaseMutation::Keep => {
                        if let Some(manual) = &manual
                            && !leases::guard_manual_lease_snapshot(txn, manual).await?
                        {
                            return Err(AppError::Database(MANUAL_LEASE_CHANGED.to_owned()));
                        }
                    }
                    ManualLeaseMutation::Renew { retain_until } => {
                        let lease_id = manual
                            .as_ref()
                            .expect("renewal policy requires an existing manual lease")
                            .id
                            .as_str();
                        leases::renew_manual_lease(txn, &owner_id, lease_id, retain_until, now)
                            .await
                            .map_err(map_renewal_error)?;
                    }
                    ManualLeaseMutation::Cancel => {
                        let lease_id = manual
                            .as_ref()
                            .expect("cancellation policy requires an existing manual lease")
                            .id
                            .as_str();
                        leases::cancel_lease(txn, lease_id, now).await?;
                    }
                }
                tags::replace_object_tags(txn, &owner_id, &replacement).await?;
                Ok(version_id)
            })
        })
        .await;

    match result {
        Ok(version_id) => Ok(version_id),
        Err(error) => {
            let error = transaction_error_into_app(error);
            // SQLite retains the transaction's read snapshot across the test
            // seam. A concurrent exact delete can therefore surface as a
            // write-upgrade conflict even though the in-transaction
            // revalidation saw the old row. After rollback, resolve the same
            // exact selector once more so that this race is reported as
            // NoSuchVersion rather than an internal database error.
            if matches!(&selector_for_recheck, VersionSelector::Exact(_))
                && matches!(&error, AppError::Database(_))
                && let Err(AppError::NoSuchVersion {
                    bucket,
                    key,
                    version_id,
                }) = crate::store::object_version::resolve_version(
                    state.store.db(),
                    &bucket_for_recheck,
                    &key_for_recheck,
                    &selector_for_recheck,
                )
                .await
            {
                return Err(AppError::NoSuchVersion {
                    bucket,
                    key,
                    version_id,
                });
            }
            Err(error)
        }
    }
}

async fn load_manual_lease<C: ConnectionTrait>(
    db: &C,
    owner_object_id: &str,
) -> AppResult<Option<pin_lease::Model>> {
    Ok(manual_lease_snapshot_query(owner_object_id).one(db).await?)
}

fn manual_lease_snapshot_query(owner_object_id: &str) -> sea_orm::Select<pin_lease::Entity> {
    pin_lease::Entity::find()
        .filter(pin_lease::Column::OwnerObjectId.eq(owner_object_id))
        .filter(pin_lease::Column::Source.eq("manual"))
}

fn existing_manual_lease(lease: &pin_lease::Model) -> AppResult<ExistingManualLease> {
    let content_mode = match lease.content_mode.as_str() {
        "full" => ContentMode::Object,
        "decompressed" => ContentMode::Decompressed,
        _ => return Err(AppError::Database(INVALID_STORED_MANUAL_LEASE.to_owned())),
    };
    let state = match lease.state.as_str() {
        "active" => ExistingManualLeaseState::Active,
        "expired" => ExistingManualLeaseState::Expired,
        "cancelled" => ExistingManualLeaseState::Cancelled,
        "evicted" => ExistingManualLeaseState::Evicted,
        _ => return Err(AppError::Database(INVALID_STORED_MANUAL_LEASE.to_owned())),
    };
    Ok(ExistingManualLease {
        id: lease.id.clone(),
        policy_id: lease.policy_id.clone(),
        content_mode,
        expires_at: lease.expires_at,
        generation: lease.generation,
        state,
    })
}

fn map_renewal_error(error: leases::RenewManualLeaseError) -> AppError {
    match error {
        leases::RenewManualLeaseError::NotLatestOwner
        | leases::RenewManualLeaseError::InvalidState
        | leases::RenewManualLeaseError::NoRecoverableReservation => {
            invalid_pinning_request(MANUAL_RENEWAL_REJECTED)
        }
        leases::RenewManualLeaseError::Database(error) => AppError::from(error),
    }
}

fn transaction_error_into_app(error: TransactionError<AppError>) -> AppError {
    match error {
        TransactionError::Transaction(error) => error,
        TransactionError::Connection(error) => AppError::from(error),
    }
}

fn invalid_pinning_request(message: &str) -> AppError {
    AppError::InvalidPinningRequest(message.to_owned())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap},
        sync::Arc,
        time::Duration,
    };

    use chrono::{DateTime, Utc};
    use s3s::S3Request;
    use sea_orm::sea_query::Expr;
    use sea_orm::{
        ActiveValue::Set, ColumnTrait, ConnectOptions, ConnectionTrait, Database, DatabaseBackend,
        DatabaseConnection, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, QueryTrait,
    };

    use super::*;
    use crate::{
        config::{PinningConfig, PolicyConfig, ProviderConfig},
        pinning::{
            config::{LeaseDuration, ProviderMode, ValidatedPinningConfig},
            coordinator::PinningCoordinator,
            policy::{LeaseIntent, LeaseSource, PublicationPolicy},
            tags::{ContentMode, ObjectTag},
        },
        state::AppState,
        store::{
            Store,
            entities::{
                object_tag, object_version, pin_job, pin_lease, pin_lease_target,
                pin_provider_usage, remote_pin,
            },
            pinning::{
                jobs,
                publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
                tags as stored_tags,
            },
        },
    };

    const BUCKET: &str = "bucket";
    const KEY: &str = "key";
    const OBJECT_ID: &str = "object-1";

    struct Fixture {
        state: Arc<AppState>,
        policy_id: String,
    }

    struct VersionedFixture {
        fixture: Fixture,
        historical_object_id: String,
        historical_version_id: String,
        current_object_id: String,
        current_version_id: String,
    }

    #[derive(Clone, Copy)]
    struct TargetSeed<'a> {
        id: &'a str,
        cid: &'a str,
        provider: &'a str,
        target_state: &'a str,
        remote_status: &'a str,
        remote_epoch: i64,
        request_id: Option<&'a str>,
    }

    fn time(hour: u32) -> DateTime<Utc> {
        format!("2026-07-22T{hour:02}:00:00Z").parse().unwrap()
    }

    async fn wait_for_policy_evaluation_pause(
        gate: &test_hooks::PolicyEvaluatedGate,
        tagging: &mut tokio::task::JoinHandle<S3Result<S3Response<PutObjectTaggingOutput>>>,
    ) {
        enum PauseOutcome {
            Arrived,
            Finished(String),
            TimedOut,
        }

        let deadline = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(deadline);
        let outcome = tokio::select! {
            _ = gate.arrived.notified() => PauseOutcome::Arrived,
            result = &mut *tagging => PauseOutcome::Finished(match result {
                Ok(Ok(_)) => "success".to_owned(),
                Ok(Err(error)) => error.code().as_str().to_owned(),
                Err(_) => "JoinError".to_owned(),
            }),
            _ = &mut deadline => PauseOutcome::TimedOut,
        };

        match outcome {
            PauseOutcome::Arrived => {}
            PauseOutcome::Finished(code) => {
                panic!("tagging finished before policy-evaluated pause: {code}")
            }
            PauseOutcome::TimedOut => {
                tagging.abort();
                let _ = tagging.await;
                panic!(
                    "tagging neither reached policy-evaluated pause nor finished within 5 seconds"
                );
            }
        }
    }

    fn coordinator_fixture() -> (Arc<PinningCoordinator>, String) {
        coordinator_fixture_with_providers(&["alpha", "beta"])
    }

    fn coordinator_fixture_with_providers(
        provider_names: &[&str],
    ) -> (Arc<PinningCoordinator>, String) {
        let validated = ValidatedPinningConfig::from_raw(
            &PinningConfig {
                worker_interval: "5s".to_owned(),
                worker_concurrency: 2,
                providers: provider_names
                    .iter()
                    .enumerate()
                    .map(|(index, name)| ProviderConfig {
                        name: (*name).to_owned(),
                        kind: "noop".to_owned(),
                        token_env: None,
                        endpoint: None,
                        api: None,
                        strategy: None,
                        upload_endpoint: None,
                        enabled: true,
                        priority: (index + 1) as u32,
                        max_bytes: 10_000,
                        max_pins: 100,
                        requests_per_second: None,
                    })
                    .collect(),
                policies: vec![PolicyConfig {
                    bucket: BUCKET.to_owned(),
                    prefix: String::new(),
                    trigger: "request".to_owned(),
                    provider_mode: "all".to_owned(),
                    providers: provider_names
                        .iter()
                        .map(|name| (*name).to_owned())
                        .collect(),
                    default_duration: "1h".to_owned(),
                    max_duration: "24h".to_owned(),
                    allow_decompressed: true,
                }],
            },
            |_| None,
        )
        .unwrap();
        let policy_id = validated.policies[0].identity.clone();
        (PinningCoordinator::build(validated).unwrap(), policy_id)
    }

    async fn fixture() -> Fixture {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        fixture_from_db(db).await
    }

    async fn fixture_with_providers(provider_names: &[&str]) -> Fixture {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        fixture_from_db_and_coordinator(db, coordinator_fixture_with_providers(provider_names))
            .await
    }

    async fn file_backed_fixture() -> (tempfile::TempDir, Fixture) {
        let directory = tempfile::tempdir().unwrap();
        let database_path = directory.path().join("tagging-race.sqlite");
        let database_url = format!(
            "sqlite://{}?mode=rwc",
            database_path.display().to_string().replace('\\', "/")
        );
        let mut options = ConnectOptions::new(database_url);
        options.max_connections(4).min_connections(2);
        let db = Database::connect(options).await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        db.execute_unprepared("PRAGMA journal_mode = WAL")
            .await
            .unwrap();
        let fixture = fixture_from_db(db).await;
        (directory, fixture)
    }

    async fn versioned_fixture() -> VersionedFixture {
        versioned_fixture_from(fixture().await).await
    }

    async fn file_backed_versioned_fixture() -> (tempfile::TempDir, VersionedFixture) {
        let (directory, fixture) = file_backed_fixture().await;
        (directory, versioned_fixture_from(fixture).await)
    }

    async fn versioned_fixture_from(fixture: Fixture) -> VersionedFixture {
        let db = fixture.state.store.db();
        crate::store::bucket::set_versioning_state(
            db,
            BUCKET,
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();
        let historical = crate::store::object::get_latest(db, BUCKET, KEY)
            .await
            .unwrap();
        let historical_version_id = db
            .transaction({
                let historical = historical.clone();
                move |txn| {
                    Box::pin(async move {
                        crate::store::object_version::install_content_version(
                            txn,
                            crate::store::object_version::BucketVersioningState::Enabled,
                            &historical,
                            time(1),
                        )
                        .await
                    })
                }
            })
            .await
            .unwrap();
        crate::store::object::upsert(
            db,
            "object-2",
            BUCKET,
            KEY,
            "bafy-object-2",
            20,
            Some("application/octet-stream"),
            "bafy-object-2",
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let current = crate::store::object::get_latest(db, BUCKET, KEY)
            .await
            .unwrap();
        let current_version_id = db
            .transaction({
                let current = current.clone();
                move |txn| {
                    Box::pin(async move {
                        crate::store::object_version::install_content_version(
                            txn,
                            crate::store::object_version::BucketVersioningState::Enabled,
                            &current,
                            time(2),
                        )
                        .await
                    })
                }
            })
            .await
            .unwrap();

        VersionedFixture {
            fixture,
            historical_object_id: historical.id,
            historical_version_id,
            current_object_id: current.id,
            current_version_id,
        }
    }

    async fn install_current_marker(fixture: &Fixture) -> String {
        fixture
            .state
            .store
            .db()
            .transaction(move |txn| {
                Box::pin(async move {
                    crate::store::object_version::install_delete_marker(
                        txn,
                        crate::store::object_version::BucketVersioningState::Enabled,
                        BUCKET,
                        KEY,
                        time(3),
                    )
                    .await
                })
            })
            .await
            .unwrap()
    }

    async fn fixture_from_db(db: DatabaseConnection) -> Fixture {
        fixture_from_db_and_coordinator(db, coordinator_fixture()).await
    }

    async fn fixture_from_db_and_coordinator(
        db: DatabaseConnection,
        (pinning, policy_id): (Arc<PinningCoordinator>, String),
    ) -> Fixture {
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, BUCKET, None)
            .await
            .unwrap();
        crate::store::object::upsert(
            &db,
            OBJECT_ID,
            BUCKET,
            KEY,
            "bafy-object",
            10,
            Some("application/octet-stream"),
            "bafy-object",
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let object = crate::store::object::get_latest(&db, BUCKET, KEY)
            .await
            .unwrap();
        db.transaction(move |txn| {
            Box::pin(async move {
                crate::store::object_version::install_content_version(
                    txn,
                    BucketVersioningState::Unversioned,
                    &object,
                    time(1),
                )
                .await
            })
        })
        .await
        .unwrap();
        Fixture {
            state: Arc::new(AppState {
                kubo: crate::kubo::KuboClient::new("http://127.0.0.1:5001".to_owned()),
                store: Store::new(db),
                credentials: HashMap::new(),
                master_key: crate::crypto::key::MasterKey::from_hex(
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                )
                .unwrap(),
                pinning,
            }),
            policy_id,
        }
    }

    async fn publish_normal_manual_lease(fixture: &Fixture) -> pin_lease::Model {
        let tags = vec![
            ObjectTag::new("team", "publication"),
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("ipfs-s3:duration", "1h"),
        ];
        let object = PublicationObject::from_put(
            "published-normal".to_owned(),
            BUCKET,
            KEY,
            "bafy-published-normal".to_owned(),
            10,
            Some("application/octet-stream".to_owned()),
            None,
            false,
            None,
            None,
            time(1),
        );
        publication::publish_object(
            fixture.state.store.db(),
            PublicationRequest {
                object: object.clone(),
                tags: tags.clone(),
                policy: PublicationPolicy {
                    tags,
                    leases: vec![LeaseIntent {
                        source: LeaseSource::Manual,
                        policy_id: fixture.policy_id.clone(),
                        provider_mode: ProviderMode::All,
                        providers: vec!["alpha".to_owned()],
                        content_mode: ContentMode::Object,
                        duration: LeaseDuration::parse("1h").unwrap(),
                    }],
                },
                object_target: PinTargetSpec {
                    cid: object.cid,
                    logical_size: object.logical_size,
                },
            },
            fixture.state.pinning.provider_limits(),
        )
        .await
        .unwrap();

        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq("published-normal"))
            .filter(pin_lease::Column::Source.eq("manual"))
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap()
    }

    fn request<T>(input: T, method: http::Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: format!("/{BUCKET}/{KEY}?tagging").parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn dto_tags(pairs: &[(&str, &str)]) -> Tagging {
        Tagging {
            tag_set: pairs
                .iter()
                .map(|(key, value)| Tag {
                    key: Some((*key).to_owned()),
                    value: Some((*value).to_owned()),
                })
                .collect(),
        }
    }

    fn put_request(pairs: &[(&str, &str)]) -> S3Request<PutObjectTaggingInput> {
        put_version_request(pairs, None)
    }

    fn put_version_request(
        pairs: &[(&str, &str)],
        version_id: Option<&str>,
    ) -> S3Request<PutObjectTaggingInput> {
        request(
            PutObjectTaggingInput {
                bucket: BUCKET.to_owned(),
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                key: KEY.to_owned(),
                request_payer: None,
                tagging: dto_tags(pairs),
                version_id: version_id.map(str::to_owned),
            },
            http::Method::PUT,
        )
    }

    fn get_request(key: &str) -> S3Request<GetObjectTaggingInput> {
        get_version_request(key, None)
    }

    fn get_version_request(
        key: &str,
        version_id: Option<&str>,
    ) -> S3Request<GetObjectTaggingInput> {
        request(
            GetObjectTaggingInput {
                bucket: BUCKET.to_owned(),
                key: key.to_owned(),
                version_id: version_id.map(str::to_owned),
                ..Default::default()
            },
            http::Method::GET,
        )
    }

    fn delete_request(key: &str) -> S3Request<DeleteObjectTaggingInput> {
        delete_version_request(key, None)
    }

    fn delete_version_request(
        key: &str,
        version_id: Option<&str>,
    ) -> S3Request<DeleteObjectTaggingInput> {
        request(
            DeleteObjectTaggingInput {
                bucket: BUCKET.to_owned(),
                key: key.to_owned(),
                version_id: version_id.map(str::to_owned),
                ..Default::default()
            },
            http::Method::DELETE,
        )
    }

    async fn seed_tags(fixture: &Fixture, pairs: &[(&str, &str)]) {
        seed_tags_for_owner(fixture, OBJECT_ID, pairs).await;
    }

    async fn seed_tags_for_owner(fixture: &Fixture, object_id: &str, pairs: &[(&str, &str)]) {
        let tags = pairs
            .iter()
            .map(|(key, value)| crate::pinning::tags::ObjectTag::new(*key, *value))
            .collect::<Vec<_>>();
        stored_tags::replace_object_tags(fixture.state.store.db(), object_id, &tags)
            .await
            .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_lease(
        fixture: &Fixture,
        lease_id: &str,
        source: &str,
        state: &str,
        content_mode: &str,
        expires_at: DateTime<Utc>,
        generation: i64,
        targets: &[TargetSeed<'_>],
    ) {
        seed_lease_for_owner(
            fixture,
            OBJECT_ID,
            lease_id,
            source,
            state,
            content_mode,
            expires_at,
            generation,
            targets,
        )
        .await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn seed_lease_for_owner(
        fixture: &Fixture,
        owner_object_id: &str,
        lease_id: &str,
        source: &str,
        state: &str,
        content_mode: &str,
        expires_at: DateTime<Utc>,
        generation: i64,
        targets: &[TargetSeed<'_>],
    ) {
        let db = fixture.state.store.db();
        pin_lease::Entity::insert(pin_lease::ActiveModel {
            id: Set(lease_id.to_owned()),
            owner_object_id: Set(owner_object_id.to_owned()),
            source: Set(source.to_owned()),
            policy_id: Set(fixture.policy_id.clone()),
            provider_mode: Set("all".to_owned()),
            content_mode: Set(content_mode.to_owned()),
            created_at: Set(time(1)),
            last_touched_at: Set(time(1)),
            expires_at: Set(expires_at),
            generation: Set(generation),
            state: Set(state.to_owned()),
        })
        .exec(db)
        .await
        .unwrap();

        for target in targets {
            pin_lease_target::Entity::insert(pin_lease_target::ActiveModel {
                id: Set(target.id.to_owned()),
                lease_id: Set(lease_id.to_owned()),
                cid: Set(target.cid.to_owned()),
                logical_size: Set(10),
                provider: Set(target.provider.to_owned()),
                state: Set(target.target_state.to_owned()),
                created_at: Set(time(1)),
                last_touched_at: Set(time(1)),
            })
            .exec(db)
            .await
            .unwrap();
            if remote_pin::Entity::find_by_id((target.provider.to_owned(), target.cid.to_owned()))
                .one(db)
                .await
                .unwrap()
                .is_none()
            {
                remote_pin::Entity::insert(remote_pin::ActiveModel {
                    provider: Set(target.provider.to_owned()),
                    cid: Set(target.cid.to_owned()),
                    request_id: Set(target.request_id.map(str::to_owned)),
                    cid_size: Set(10),
                    status: Set(target.remote_status.to_owned()),
                    epoch: Set(target.remote_epoch),
                    failure_attempts: Set(0),
                    next_retry_at: Set(None),
                    last_failed_request_id: Set(None),
                    last_touched_at: Set(time(1)),
                    last_error_class: Set(None),
                    last_error_text: Set(None),
                })
                .exec(db)
                .await
                .unwrap();
            }
        }
    }

    async fn seed_usage(fixture: &Fixture, provider: &str, pins: i64) {
        pin_provider_usage::Entity::insert(pin_provider_usage::ActiveModel {
            provider: Set(provider.to_owned()),
            reserved_bytes: Set(pins * 10),
            reserved_pins: Set(pins),
            observed_bytes: Set(None),
            observed_pins: Set(None),
            observed_at: Set(None),
        })
        .exec(fixture.state.store.db())
        .await
        .unwrap();
    }

    async fn manual_lease(fixture: &Fixture) -> pin_lease::Model {
        pin_lease::Entity::find_by_id("manual")
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap()
    }

    async fn remote(fixture: &Fixture, provider: &str, cid: &str) -> remote_pin::Model {
        remote_pin::Entity::find_by_id((provider.to_owned(), cid.to_owned()))
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap()
    }

    async fn job_count(fixture: &Fixture) -> u64 {
        pin_job::Entity::find()
            .count(fixture.state.store.db())
            .await
            .unwrap()
    }

    type JobSignature = (
        String,
        String,
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<i64>,
    );

    async fn job_signatures(fixture: &Fixture) -> BTreeSet<JobSignature> {
        pin_job::Entity::find()
            .all(fixture.state.store.db())
            .await
            .unwrap()
            .into_iter()
            .map(|job| {
                (
                    job.operation,
                    job.provider,
                    job.cid,
                    job.expected_remote_epoch,
                    job.lease_id,
                    job.target_id,
                    job.expected_generation,
                )
            })
            .collect()
    }

    fn tag_pairs(tags: &[Tag]) -> Vec<(&str, &str)> {
        tags.iter()
            .map(|tag| {
                (
                    tag.key.as_deref().expect("stored tag key"),
                    tag.value.as_deref().expect("stored tag value"),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn round_trip_replaces_tags_and_get_sorts_by_key() {
        let fixture = fixture().await;

        put_object_tagging_at(
            &fixture.state,
            put_request(&[("z", "last"), ("a", "first")]),
            time(2),
        )
        .await
        .unwrap();
        let output = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(
            tag_pairs(&output.tag_set),
            vec![("a", "first"), ("z", "last")]
        );

        put_object_tagging_at(
            &fixture.state,
            put_request(&[("middle", "replacement")]),
            time(3),
        )
        .await
        .unwrap();
        let output = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(tag_pairs(&output.tag_set), vec![("middle", "replacement")]);
    }

    #[tokio::test]
    async fn publication_created_normal_manual_lease_supports_get_put_and_delete_tagging() {
        let fixture = fixture().await;
        let published_lease = publish_normal_manual_lease(&fixture).await;
        assert_eq!(published_lease.content_mode, "full");

        let initial = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(
            tag_pairs(&initial.tag_set),
            vec![
                ("ipfs-s3:duration", "1h"),
                ("ipfs-s3:pin", "true"),
                ("team", "publication"),
            ]
        );

        put_object_tagging_at(
            &fixture.state,
            put_request(&[("team", "replacement")]),
            published_lease.created_at,
        )
        .await
        .unwrap();
        let cancelled = pin_lease::Entity::find_by_id(&published_lease.id)
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cancelled.state, "cancelled");

        delete_object_tagging_at(
            &fixture.state,
            delete_request(KEY),
            published_lease.created_at + chrono::TimeDelta::seconds(1),
        )
        .await
        .unwrap();
        assert!(
            get_object_tagging(&fixture.state, get_request(KEY))
                .await
                .unwrap()
                .output
                .tag_set
                .is_empty()
        );
    }

    #[tokio::test]
    async fn malformed_version_ids_are_rejected_before_reads_or_mutations() {
        let fixture = fixture().await;
        seed_tags(&fixture, &[("before", "kept")]).await;
        seed_lease(
            &fixture,
            "manual",
            "manual",
            "active",
            "full",
            time(5),
            4,
            &[],
        )
        .await;
        let lease_before = manual_lease(&fixture).await;

        let mut get = get_request("missing");
        get.input.version_id = Some("unsupported-get".to_owned());
        let get_result = get_object_tagging(&fixture.state, get).await;

        let mut put = put_request(&[("after", "rejected")]);
        put.input.version_id = Some("unsupported-put".to_owned());
        let put_result = put_object_tagging_at(&fixture.state, put, time(2)).await;

        let mut delete = delete_request(KEY);
        delete.input.version_id = Some("unsupported-delete".to_owned());
        let delete_result = delete_object_tagging_at(&fixture.state, delete, time(2)).await;

        for result in [
            get_result.map(|_| ()),
            put_result.map(|_| ()),
            delete_result.map(|_| ()),
        ] {
            let error = result.unwrap_err();
            assert_eq!(error.code().as_str(), "InvalidArgument");
        }
        assert_eq!(manual_lease(&fixture).await, lease_before);
        let tags = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(tag_pairs(&tags.tag_set), vec![("before", "kept")]);
    }

    #[tokio::test]
    async fn renewal_enters_task6_without_prelocking_manual_lease_and_uses_canonical_frontier() {
        let _serial = leases::test_gates::LIFECYCLE_ORDER_TEST_LOCK.lock().await;
        let provider = "tagging-lock-order-provider";
        let fixture = fixture_with_providers(&[provider]).await;
        let owner_id = "tagging-lock-order-owner";
        let key = "tagging-lock-order-key";
        let automatic_lease_id = "tagging-lock-order-lease-a-automatic";
        let manual_lease_id = "tagging-lock-order-lease-z-manual";
        let automatic_target_id = "tagging-lock-order-target-a-automatic";
        let manual_target_id = "tagging-lock-order-target-z-manual";
        let shared_cid = "bafy-tagging-lock-order-shared";
        let snapshot_sql = manual_lease_snapshot_query(owner_id)
            .build(DatabaseBackend::Postgres)
            .to_string();
        assert!(!snapshot_sql.contains("FOR UPDATE"));
        crate::store::object::upsert(
            fixture.state.store.db(),
            owner_id,
            BUCKET,
            key,
            "bafy-tagging-lock-order-owner",
            10,
            Some("application/octet-stream"),
            "bafy-tagging-lock-order-owner",
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let object = crate::store::object::get_latest(fixture.state.store.db(), BUCKET, key)
            .await
            .unwrap();
        fixture
            .state
            .store
            .db()
            .transaction(move |txn| {
                Box::pin(async move {
                    crate::store::object_version::install_content_version(
                        txn,
                        BucketVersioningState::Unversioned,
                        &object,
                        time(1),
                    )
                    .await
                })
            })
            .await
            .unwrap();
        seed_lease_for_owner(
            &fixture,
            owner_id,
            automatic_lease_id,
            "automatic",
            "active",
            "full",
            time(5),
            1,
            &[TargetSeed {
                id: automatic_target_id,
                cid: shared_cid,
                provider,
                target_state: "pinned",
                remote_status: "pinned",
                remote_epoch: 3,
                request_id: Some("tagging-lock-order-request"),
            }],
        )
        .await;
        seed_lease_for_owner(
            &fixture,
            owner_id,
            manual_lease_id,
            "manual",
            "active",
            "full",
            time(5),
            2,
            &[TargetSeed {
                id: manual_target_id,
                cid: shared_cid,
                provider,
                target_state: "pinned",
                remote_status: "pinned",
                remote_epoch: 3,
                request_id: Some("tagging-lock-order-request"),
            }],
        )
        .await;
        seed_usage(&fixture, provider, 1).await;
        *leases::test_gates::LIFECYCLE_ORDER_RECORDER.lock().await =
            Some(leases::test_gates::LifecycleOrderRecorder {
                owner_ids: BTreeSet::from([owner_id.to_owned()]),
                lease_ids: BTreeSet::from([
                    automatic_lease_id.to_owned(),
                    manual_lease_id.to_owned(),
                ]),
                target_ids: BTreeSet::from([
                    automatic_target_id.to_owned(),
                    manual_target_id.to_owned(),
                ]),
                remote_pairs: BTreeSet::from([(provider.to_owned(), shared_cid.to_owned())]),
                record_desired_target_reads: true,
                events: Vec::new(),
            });

        let mut renewal = put_request(&[
            ("ipfs-s3:pin", "true"),
            ("ipfs-s3:retain-until", "2026-07-22T08:00:00Z"),
        ]);
        renewal.input.key = key.to_owned();
        put_object_tagging_at(&fixture.state, renewal, time(2))
            .await
            .unwrap();

        let events = leases::test_gates::LIFECYCLE_ORDER_RECORDER
            .lock()
            .await
            .take()
            .unwrap()
            .events;
        let lock_events = events
            .into_iter()
            .filter(|event| {
                matches!(
                    event,
                    leases::test_gates::LifecycleOrderEvent::OwnerLock(_)
                        | leases::test_gates::LifecycleOrderEvent::LeaseLock(_)
                        | leases::test_gates::LifecycleOrderEvent::TargetLock(_)
                        | leases::test_gates::LifecycleOrderEvent::RemoteLock(_, _)
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            lock_events,
            vec![
                leases::test_gates::LifecycleOrderEvent::OwnerLock(owner_id.to_owned()),
                leases::test_gates::LifecycleOrderEvent::LeaseLock(automatic_lease_id.to_owned()),
                leases::test_gates::LifecycleOrderEvent::LeaseLock(manual_lease_id.to_owned()),
                leases::test_gates::LifecycleOrderEvent::TargetLock(automatic_target_id.to_owned()),
                leases::test_gates::LifecycleOrderEvent::TargetLock(manual_target_id.to_owned()),
                leases::test_gates::LifecycleOrderEvent::RemoteLock(
                    provider.to_owned(),
                    shared_cid.to_owned(),
                ),
            ]
        );
    }

    #[tokio::test]
    async fn keep_snapshot_race_rolls_back_tags_when_expired_lease_is_reactivated() {
        let _serial = test_hooks::POLICY_EVALUATED_TEST_LOCK.lock().await;
        let (_directory, fixture) = file_backed_fixture().await;
        seed_tags(&fixture, &[("before", "kept")]).await;
        seed_lease(
            &fixture,
            "race-manual",
            "manual",
            "expired",
            "full",
            time(1),
            7,
            &[],
        )
        .await;
        let gate = Arc::new(test_hooks::PolicyEvaluatedGate {
            lease_id: "race-manual",
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let _gate_installation = test_hooks::install_policy_evaluated_gate(gate.clone());
        let state = fixture.state.clone();
        let mut tagging = tokio::spawn(async move {
            put_object_tagging_at(&state, put_request(&[("after", "rejected")]), time(2)).await
        });

        wait_for_policy_evaluation_pause(&gate, &mut tagging).await;
        let updated = pin_lease::Entity::update_many()
            .col_expr(pin_lease::Column::State, Expr::value("active"))
            .col_expr(pin_lease::Column::Generation, Expr::value(8_i64))
            .col_expr(pin_lease::Column::ExpiresAt, Expr::value(time(5)))
            .col_expr(pin_lease::Column::LastTouchedAt, Expr::value(time(2)))
            .filter(pin_lease::Column::Id.eq("race-manual"))
            .filter(pin_lease::Column::State.eq("expired"))
            .filter(pin_lease::Column::Generation.eq(7_i64))
            .exec(fixture.state.store.db())
            .await
            .unwrap();
        assert_eq!(updated.rows_affected, 1);
        gate.resume.notify_one();
        let result = tagging.await.unwrap();

        assert_eq!(result.unwrap_err().code().as_str(), "InternalError");
        let lease = pin_lease::Entity::find_by_id("race-manual")
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((lease.state.as_str(), lease.generation), ("active", 8));
        let tags = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(tag_pairs(&tags.tag_set), vec![("before", "kept")]);
    }

    #[tokio::test]
    async fn nonexistent_keys_return_no_such_key_for_all_three_operations() {
        let fixture = fixture().await;
        let get_error = get_object_tagging(&fixture.state, get_request("missing"))
            .await
            .unwrap_err();
        let put_error = put_object_tagging_at(
            &fixture.state,
            request(
                PutObjectTaggingInput {
                    bucket: BUCKET.to_owned(),
                    checksum_algorithm: None,
                    content_md5: None,
                    expected_bucket_owner: None,
                    key: "missing".to_owned(),
                    request_payer: None,
                    tagging: dto_tags(&[("team", "storage")]),
                    version_id: None,
                },
                http::Method::PUT,
            ),
            time(2),
        )
        .await
        .unwrap_err();
        let delete_error =
            delete_object_tagging_at(&fixture.state, delete_request("missing"), time(2))
                .await
                .unwrap_err();

        for error in [get_error, put_error, delete_error] {
            assert_eq!(error.code().as_str(), "NoSuchKey");
        }
    }

    #[tokio::test]
    async fn pin_true_without_manual_lease_is_rejected_without_replacing_tags() {
        let fixture = fixture().await;
        seed_tags(&fixture, &[("before", "kept")]).await;

        let error = put_object_tagging_at(
            &fixture.state,
            put_request(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "1h")]),
            time(2),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidArgument");
        let output = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(tag_pairs(&output.tag_set), vec![("before", "kept")]);
    }

    #[tokio::test]
    async fn content_mode_mutation_is_rejected_without_lease_or_tag_changes() {
        let fixture = fixture().await;
        let target = TargetSeed {
            id: "target-object",
            cid: "bafy-object",
            provider: "alpha",
            target_state: "pinned",
            remote_status: "pinned",
            remote_epoch: 6,
            request_id: Some("request-object"),
        };
        seed_tags(
            &fixture,
            &[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "1h")],
        )
        .await;
        seed_lease(
            &fixture,
            "manual",
            "manual",
            "active",
            "full",
            time(5),
            3,
            &[target],
        )
        .await;
        seed_usage(&fixture, "alpha", 1).await;
        let target_before = pin_lease_target::Entity::find_by_id("target-object")
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();

        let error = put_object_tagging_at(
            &fixture.state,
            put_request(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:duration", "2h"),
                ("ipfs-s3:content", "decompressed"),
            ]),
            time(2),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidArgument");
        let lease = manual_lease(&fixture).await;
        assert_eq!(lease.generation, 3);
        assert_eq!(lease.content_mode, "full");
        assert_eq!(lease.provider_mode, "all");
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-object")
                .one(fixture.state.store.db())
                .await
                .unwrap()
                .unwrap(),
            target_before
        );
        assert_eq!(remote(&fixture, "alpha", "bafy-object").await.epoch, 6);
        let output = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(
            tag_pairs(&output.tag_set),
            vec![("ipfs-s3:duration", "1h"), ("ipfs-s3:pin", "true")]
        );
    }

    #[tokio::test]
    async fn same_timestamp_renewal_is_idempotent_but_replaces_user_tags() {
        let fixture = fixture().await;
        let target = TargetSeed {
            id: "target-object",
            cid: "bafy-object",
            provider: "alpha",
            target_state: "waiting",
            remote_status: "reserved",
            remote_epoch: 7,
            request_id: None,
        };
        seed_lease(
            &fixture,
            "manual",
            "manual",
            "active",
            "full",
            time(5),
            4,
            &[target],
        )
        .await;
        seed_usage(&fixture, "alpha", 1).await;
        jobs::enqueue_job(
            fixture.state.store.db(),
            jobs::submit_job(
                "alpha",
                "bafy-object",
                "manual",
                "target-object",
                4,
                time(1),
            ),
        )
        .await
        .unwrap();
        let target_before = pin_lease_target::Entity::find_by_id("target-object")
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        let jobs_before = job_count(&fixture).await;

        put_object_tagging_at(
            &fixture.state,
            put_request(&[
                ("team", "new"),
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", "2026-07-22T05:00:00Z"),
            ]),
            time(2),
        )
        .await
        .unwrap();

        assert_eq!(manual_lease(&fixture).await.generation, 4);
        assert_eq!(remote(&fixture, "alpha", "bafy-object").await.epoch, 7);
        assert_eq!(job_count(&fixture).await, jobs_before);
        assert_eq!(
            pin_lease_target::Entity::find_by_id("target-object")
                .one(fixture.state.store.db())
                .await
                .unwrap()
                .unwrap(),
            target_before
        );
        let output = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(
            tag_pairs(&output.tag_set),
            vec![
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", "2026-07-22T05:00:00Z"),
                ("team", "new"),
            ]
        );
    }

    #[tokio::test]
    async fn extension_advances_generation_and_each_original_remote_epoch_once() {
        let fixture = fixture().await;
        let targets = [
            TargetSeed {
                id: "target-a",
                cid: "bafy-a",
                provider: "alpha",
                target_state: "pinned",
                remote_status: "pinned",
                remote_epoch: 4,
                request_id: Some("request-a"),
            },
            TargetSeed {
                id: "target-b",
                cid: "bafy-b",
                provider: "beta",
                target_state: "submitted",
                remote_status: "pinning",
                remote_epoch: 9,
                request_id: Some("request-b"),
            },
        ];
        seed_lease(
            &fixture,
            "manual",
            "manual",
            "active",
            "decompressed",
            time(5),
            2,
            &targets,
        )
        .await;
        seed_usage(&fixture, "alpha", 1).await;
        seed_usage(&fixture, "beta", 1).await;

        put_object_tagging_at(
            &fixture.state,
            put_request(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", "2026-07-22T08:00:00Z"),
            ]),
            time(2),
        )
        .await
        .unwrap();

        let lease = manual_lease(&fixture).await;
        assert_eq!(lease.generation, 3);
        assert_eq!(lease.content_mode, "decompressed");
        assert_eq!(lease.provider_mode, "all");
        assert_eq!(remote(&fixture, "alpha", "bafy-a").await.epoch, 5);
        assert_eq!(remote(&fixture, "beta", "bafy-b").await.epoch, 10);
        let targets_after = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("manual"))
            .order_by_asc(pin_lease_target::Column::Id)
            .all(fixture.state.store.db())
            .await
            .unwrap();
        assert_eq!(
            targets_after
                .iter()
                .map(|target| (
                    target.id.as_str(),
                    target.cid.as_str(),
                    target.provider.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("target-a", "bafy-a", "alpha"),
                ("target-b", "bafy-b", "beta"),
            ]
        );
        assert_eq!(
            job_signatures(&fixture).await,
            BTreeSet::from([
                (
                    "poll".to_owned(),
                    "beta".to_owned(),
                    "bafy-b".to_owned(),
                    None,
                    Some("manual".to_owned()),
                    Some("target-b".to_owned()),
                    Some(3),
                ),
                (
                    "reconcile".to_owned(),
                    "alpha".to_owned(),
                    "bafy-a".to_owned(),
                    Some(5),
                    None,
                    None,
                    None,
                ),
                (
                    "reconcile".to_owned(),
                    "beta".to_owned(),
                    "bafy-b".to_owned(),
                    Some(10),
                    None,
                    None,
                    None,
                ),
            ])
        );
    }

    #[tokio::test]
    async fn shortening_is_invalid_and_rolls_back_tag_replacement() {
        let fixture = fixture().await;
        seed_tags(&fixture, &[("before", "kept")]).await;
        seed_lease(
            &fixture,
            "manual",
            "manual",
            "active",
            "full",
            time(5),
            6,
            &[],
        )
        .await;

        let error = put_object_tagging_at(
            &fixture.state,
            put_request(&[
                ("after", "rejected"),
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", "2026-07-22T04:00:00Z"),
            ]),
            time(2),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidArgument");
        assert_eq!(manual_lease(&fixture).await.generation, 6);
        let output = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(tag_pairs(&output.tag_set), vec![("before", "kept")]);
    }

    #[tokio::test]
    async fn omission_cancels_only_active_manual_lease_and_creates_remote_scoped_work() {
        let fixture = fixture().await;
        let manual_target = TargetSeed {
            id: "manual-target",
            cid: "bafy-manual",
            provider: "alpha",
            target_state: "waiting",
            remote_status: "reserved",
            remote_epoch: 3,
            request_id: None,
        };
        let automatic_target = TargetSeed {
            id: "automatic-target",
            cid: "bafy-automatic",
            provider: "beta",
            target_state: "pinned",
            remote_status: "pinned",
            remote_epoch: 8,
            request_id: Some("automatic-request"),
        };
        seed_lease(
            &fixture,
            "manual",
            "manual",
            "active",
            "full",
            time(5),
            2,
            &[manual_target],
        )
        .await;
        seed_lease(
            &fixture,
            "automatic",
            "automatic",
            "active",
            "full",
            time(5),
            5,
            &[automatic_target],
        )
        .await;
        seed_usage(&fixture, "alpha", 1).await;
        seed_usage(&fixture, "beta", 1).await;

        put_object_tagging_at(&fixture.state, put_request(&[("team", "storage")]), time(2))
            .await
            .unwrap();

        let manual = manual_lease(&fixture).await;
        assert_eq!((manual.state.as_str(), manual.generation), ("cancelled", 3));
        let automatic = pin_lease::Entity::find_by_id("automatic")
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (automatic.state.as_str(), automatic.generation),
            ("active", 5)
        );
        assert_eq!(remote(&fixture, "alpha", "bafy-manual").await.epoch, 4);
        assert_eq!(remote(&fixture, "beta", "bafy-automatic").await.epoch, 8);
        assert_eq!(
            job_signatures(&fixture).await,
            BTreeSet::from([(
                "unpin".to_owned(),
                "alpha".to_owned(),
                "bafy-manual".to_owned(),
                Some(4),
                None,
                None,
                None,
            )])
        );
    }

    #[tokio::test]
    async fn delete_tagging_removes_tags_and_cancels_active_manual_lease() {
        let fixture = fixture().await;
        let target = TargetSeed {
            id: "delete-target",
            cid: "bafy-delete",
            provider: "alpha",
            target_state: "pinned",
            remote_status: "pinned",
            remote_epoch: 11,
            request_id: Some("delete-request"),
        };
        seed_tags(&fixture, &[("before", "removed")]).await;
        seed_lease(
            &fixture,
            "manual",
            "manual",
            "active",
            "full",
            time(5),
            1,
            &[target],
        )
        .await;
        seed_usage(&fixture, "alpha", 1).await;

        delete_object_tagging_at(&fixture.state, delete_request(KEY), time(2))
            .await
            .unwrap();

        let lease = manual_lease(&fixture).await;
        assert_eq!((lease.state.as_str(), lease.generation), ("cancelled", 2));
        assert_eq!(remote(&fixture, "alpha", "bafy-delete").await.epoch, 12);
        assert_eq!(
            job_signatures(&fixture).await,
            BTreeSet::from([(
                "unpin".to_owned(),
                "alpha".to_owned(),
                "bafy-delete".to_owned(),
                Some(12),
                None,
                None,
                None,
            )])
        );
        assert!(
            get_object_tagging(&fixture.state, get_request(KEY))
                .await
                .unwrap()
                .output
                .tag_set
                .is_empty()
        );
    }

    #[tokio::test]
    async fn omission_and_delete_never_reactivate_an_expired_manual_lease() {
        for delete in [false, true] {
            let fixture = fixture().await;
            seed_tags(&fixture, &[("before", "present")]).await;
            seed_lease(
                &fixture,
                "manual",
                "manual",
                "expired",
                "full",
                time(1),
                7,
                &[],
            )
            .await;

            if delete {
                delete_object_tagging_at(&fixture.state, delete_request(KEY), time(2))
                    .await
                    .unwrap();
            } else {
                put_object_tagging_at(
                    &fixture.state,
                    put_request(&[("after", "replacement")]),
                    time(2),
                )
                .await
                .unwrap();
            }

            let lease = manual_lease(&fixture).await;
            assert_eq!((lease.state.as_str(), lease.generation), ("expired", 7));
        }
    }

    #[tokio::test]
    async fn expired_reactivation_preserves_original_targets_quota_and_creates_typed_current_work()
    {
        let fixture = fixture().await;
        let targets = [
            TargetSeed {
                id: "target-reserved",
                cid: "bafy-reserved",
                provider: "alpha",
                target_state: "released",
                remote_status: "reserved",
                remote_epoch: 10,
                request_id: None,
            },
            TargetSeed {
                id: "target-pinning",
                cid: "bafy-pinning",
                provider: "alpha",
                target_state: "released",
                remote_status: "pinning",
                remote_epoch: 20,
                request_id: Some("request-pinning"),
            },
            TargetSeed {
                id: "target-pinned",
                cid: "bafy-pinned",
                provider: "alpha",
                target_state: "released",
                remote_status: "pinned",
                remote_epoch: 30,
                request_id: Some("request-pinned"),
            },
        ];
        seed_lease(
            &fixture,
            "manual",
            "manual",
            "expired",
            "decompressed",
            time(1),
            9,
            &targets,
        )
        .await;
        seed_usage(&fixture, "alpha", 3).await;
        for target in &targets {
            jobs::enqueue_job(
                fixture.state.store.db(),
                jobs::unpin_job(target.provider, target.cid, target.remote_epoch, time(1)),
            )
            .await
            .unwrap();
        }
        let target_ids_before = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("manual"))
            .order_by_asc(pin_lease_target::Column::Id)
            .all(fixture.state.store.db())
            .await
            .unwrap()
            .into_iter()
            .map(|target| (target.id, target.cid, target.provider))
            .collect::<Vec<_>>();

        put_object_tagging_at(
            &fixture.state,
            put_request(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", "2026-07-22T06:00:00Z"),
            ]),
            time(2),
        )
        .await
        .unwrap();

        let lease = manual_lease(&fixture).await;
        assert_eq!((lease.state.as_str(), lease.generation), ("active", 10));
        assert_eq!(lease.content_mode, "decompressed");
        assert_eq!(remote(&fixture, "alpha", "bafy-reserved").await.epoch, 11);
        assert_eq!(remote(&fixture, "alpha", "bafy-pinning").await.epoch, 21);
        assert_eq!(remote(&fixture, "alpha", "bafy-pinned").await.epoch, 31);
        let target_ids_after = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("manual"))
            .order_by_asc(pin_lease_target::Column::Id)
            .all(fixture.state.store.db())
            .await
            .unwrap()
            .into_iter()
            .map(|target| (target.id, target.cid, target.provider))
            .collect::<Vec<_>>();
        assert_eq!(target_ids_after, target_ids_before);
        let usage = pin_provider_usage::Entity::find_by_id("alpha")
            .one(fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((usage.reserved_pins, usage.reserved_bytes), (3, 30));

        let current_jobs = pin_job::Entity::find()
            .filter(pin_job::Column::State.eq("pending"))
            .all(fixture.state.store.db())
            .await
            .unwrap();
        assert!(current_jobs.iter().any(|job| {
            job.operation == "submit"
                && job.lease_id.as_deref() == Some("manual")
                && job.target_id.as_deref() == Some("target-reserved")
                && job.expected_generation == Some(10)
        }));
        assert!(current_jobs.iter().any(|job| {
            job.operation == "poll"
                && job.lease_id.as_deref() == Some("manual")
                && job.target_id.as_deref() == Some("target-pinning")
                && job.expected_generation == Some(10)
        }));
        for (cid, epoch) in [
            ("bafy-reserved", 11),
            ("bafy-pinning", 21),
            ("bafy-pinned", 31),
        ] {
            assert!(current_jobs.iter().any(|job| {
                job.operation == "reconcile"
                    && job.cid == cid
                    && job.lease_id.is_none()
                    && job.target_id.is_none()
                    && job.expected_remote_epoch == Some(epoch)
            }));
        }
    }

    #[tokio::test]
    async fn confirmed_release_rejects_renewal_without_tags_or_target_reconstruction() {
        let fixture = fixture().await;
        seed_tags(&fixture, &[("before", "kept")]).await;
        let targets = [
            TargetSeed {
                id: "released-a",
                cid: "bafy-released-a",
                provider: "alpha",
                target_state: "released",
                remote_status: "absent",
                remote_epoch: 12,
                request_id: None,
            },
            TargetSeed {
                id: "released-b",
                cid: "bafy-released-b",
                provider: "alpha",
                target_state: "released",
                remote_status: "absent",
                remote_epoch: 14,
                request_id: None,
            },
        ];
        seed_lease(
            &fixture,
            "manual",
            "manual",
            "expired",
            "decompressed",
            time(1),
            8,
            &targets,
        )
        .await;
        seed_usage(&fixture, "alpha", 0).await;
        let target_rows_before = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("manual"))
            .order_by_asc(pin_lease_target::Column::Id)
            .all(fixture.state.store.db())
            .await
            .unwrap();

        let error = put_object_tagging_at(
            &fixture.state,
            put_request(&[
                ("after", "rejected"),
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", "2026-07-22T06:00:00Z"),
            ]),
            time(2),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidArgument");
        let lease = manual_lease(&fixture).await;
        assert_eq!((lease.state.as_str(), lease.generation), ("expired", 8));
        let target_rows_after = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq("manual"))
            .order_by_asc(pin_lease_target::Column::Id)
            .all(fixture.state.store.db())
            .await
            .unwrap();
        assert_eq!(target_rows_after, target_rows_before);
        let output = get_object_tagging(&fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(tag_pairs(&output.tag_set), vec![("before", "kept")]);
    }

    #[tokio::test]
    async fn get_tagging_reads_exact_historical_owner() {
        let versions = versioned_fixture().await;
        seed_tags_for_owner(
            &versions.fixture,
            &versions.historical_object_id,
            &[("historical", "one")],
        )
        .await;
        seed_tags_for_owner(
            &versions.fixture,
            &versions.current_object_id,
            &[("current", "two")],
        )
        .await;

        let exact = get_object_tagging(
            &versions.fixture.state,
            get_version_request(KEY, Some(&versions.historical_version_id)),
        )
        .await
        .unwrap()
        .output;
        let current = get_object_tagging(&versions.fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;

        assert_eq!(tag_pairs(&exact.tag_set), vec![("historical", "one")]);
        assert_eq!(
            exact.version_id.as_deref(),
            Some(versions.historical_version_id.as_str())
        );
        assert_eq!(tag_pairs(&current.tag_set), vec![("current", "two")]);
        assert_eq!(
            current.version_id.as_deref(),
            Some(versions.current_version_id.as_str())
        );
    }

    #[tokio::test]
    async fn put_and_delete_tagging_mutate_only_exact_internal_owner() {
        let versions = versioned_fixture().await;
        seed_tags_for_owner(
            &versions.fixture,
            &versions.historical_object_id,
            &[("historical", "before")],
        )
        .await;
        seed_tags_for_owner(
            &versions.fixture,
            &versions.current_object_id,
            &[("current", "kept")],
        )
        .await;

        let put = put_object_tagging_at(
            &versions.fixture.state,
            put_version_request(
                &[("historical", "replacement")],
                Some(&versions.historical_version_id),
            ),
            time(4),
        )
        .await
        .unwrap()
        .output;
        assert_eq!(
            put.version_id.as_deref(),
            Some(versions.historical_version_id.as_str())
        );
        let historical = get_object_tagging(
            &versions.fixture.state,
            get_version_request(KEY, Some(&versions.historical_version_id)),
        )
        .await
        .unwrap()
        .output;
        let current = get_object_tagging(&versions.fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert_eq!(
            tag_pairs(&historical.tag_set),
            vec![("historical", "replacement")]
        );
        assert_eq!(tag_pairs(&current.tag_set), vec![("current", "kept")]);

        let deleted = delete_object_tagging_at(
            &versions.fixture.state,
            delete_version_request(KEY, Some(&versions.historical_version_id)),
            time(5),
        )
        .await
        .unwrap()
        .output;
        assert_eq!(
            deleted.version_id.as_deref(),
            Some(versions.historical_version_id.as_str())
        );
        let historical = get_object_tagging(
            &versions.fixture.state,
            get_version_request(KEY, Some(&versions.historical_version_id)),
        )
        .await
        .unwrap()
        .output;
        let current = get_object_tagging(&versions.fixture.state, get_request(KEY))
            .await
            .unwrap()
            .output;
        assert!(historical.tag_set.is_empty());
        assert_eq!(tag_pairs(&current.tag_set), vec![("current", "kept")]);
    }

    #[tokio::test]
    async fn current_tagging_marker_is_404() {
        let versions = versioned_fixture().await;
        let marker_version_id = install_current_marker(&versions.fixture).await;

        for error in [
            get_object_tagging(&versions.fixture.state, get_request(KEY))
                .await
                .expect_err("current marker GetObjectTagging must fail"),
            put_object_tagging_at(
                &versions.fixture.state,
                put_request(&[("unexpected", "mutation")]),
                time(4),
            )
            .await
            .expect_err("current marker PutObjectTagging must fail"),
            delete_object_tagging_at(&versions.fixture.state, delete_request(KEY), time(4))
                .await
                .expect_err("current marker DeleteObjectTagging must fail"),
        ] {
            assert_eq!(error.code().as_str(), "NoSuchKey");
            assert_eq!(error.status_code(), Some(http::StatusCode::NOT_FOUND));
            let headers = error.headers().expect("current marker headers");
            assert_eq!(headers["x-amz-delete-marker"], "true");
            assert_eq!(headers["x-amz-version-id"], marker_version_id);
        }
    }

    #[tokio::test]
    async fn exact_tagging_marker_is_method_not_allowed() {
        let versions = versioned_fixture().await;
        let marker_version_id = install_current_marker(&versions.fixture).await;

        for error in [
            get_object_tagging(
                &versions.fixture.state,
                get_version_request(KEY, Some(&marker_version_id)),
            )
            .await
            .expect_err("explicit marker GetObjectTagging must fail"),
            put_object_tagging_at(
                &versions.fixture.state,
                put_version_request(&[("unexpected", "mutation")], Some(&marker_version_id)),
                time(4),
            )
            .await
            .expect_err("explicit marker PutObjectTagging must fail"),
            delete_object_tagging_at(
                &versions.fixture.state,
                delete_version_request(KEY, Some(&marker_version_id)),
                time(4),
            )
            .await
            .expect_err("explicit marker DeleteObjectTagging must fail"),
        ] {
            assert_eq!(error.code().as_str(), "MethodNotAllowed");
            assert_eq!(
                error.status_code(),
                Some(http::StatusCode::METHOD_NOT_ALLOWED)
            );
            let headers = error.headers().expect("explicit marker headers");
            assert_eq!(headers["x-amz-delete-marker"], "true");
            assert_eq!(headers["x-amz-version-id"], marker_version_id);
            assert!(headers.get(http::header::LAST_MODIFIED).is_some());
        }
    }

    #[tokio::test]
    async fn unknown_tagging_version_is_no_such_version() {
        let versions = versioned_fixture().await;
        let unknown = "00000000-0000-0000-0000-000000000001";

        for error in [
            get_object_tagging(
                &versions.fixture.state,
                get_version_request(KEY, Some(unknown)),
            )
            .await
            .expect_err("unknown GetObjectTagging version must fail"),
            put_object_tagging_at(
                &versions.fixture.state,
                put_version_request(&[("unexpected", "mutation")], Some(unknown)),
                time(4),
            )
            .await
            .expect_err("unknown PutObjectTagging version must fail"),
            delete_object_tagging_at(
                &versions.fixture.state,
                delete_version_request(KEY, Some(unknown)),
                time(4),
            )
            .await
            .expect_err("unknown DeleteObjectTagging version must fail"),
        ] {
            assert_eq!(error.code().as_str(), "NoSuchVersion");
            assert_eq!(error.status_code(), Some(http::StatusCode::NOT_FOUND));
        }
    }

    #[tokio::test]
    async fn unversioned_tagging_version_is_invalid_argument() {
        let fixture = fixture().await;

        for error in [
            get_object_tagging(&fixture.state, get_version_request(KEY, Some("null")))
                .await
                .expect_err("unversioned GetObjectTagging version must fail"),
            put_object_tagging_at(
                &fixture.state,
                put_version_request(&[("unexpected", "mutation")], Some("null")),
                time(2),
            )
            .await
            .expect_err("unversioned PutObjectTagging version must fail"),
            delete_object_tagging_at(
                &fixture.state,
                delete_version_request(KEY, Some("null")),
                time(2),
            )
            .await
            .expect_err("unversioned DeleteObjectTagging version must fail"),
        ] {
            assert_eq!(error.code().as_str(), "InvalidArgument");
            assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
        }
    }

    #[tokio::test]
    async fn tagging_revalidates_version_and_lease_under_lock() {
        let _serial = test_hooks::POLICY_EVALUATED_TEST_LOCK.lock().await;
        let (_directory, versions) = file_backed_versioned_fixture().await;
        seed_tags_for_owner(
            &versions.fixture,
            &versions.historical_object_id,
            &[("before", "kept")],
        )
        .await;
        seed_tags_for_owner(
            &versions.fixture,
            &versions.current_object_id,
            &[("current", "kept")],
        )
        .await;
        seed_lease_for_owner(
            &versions.fixture,
            &versions.historical_object_id,
            "version-race-manual",
            "manual",
            "active",
            "full",
            time(5),
            7,
            &[],
        )
        .await;
        let lease_before = pin_lease::Entity::find_by_id("version-race-manual")
            .one(versions.fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        let gate = Arc::new(test_hooks::PolicyEvaluatedGate {
            lease_id: "version-race-manual",
            arrived: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
        });
        let _gate_installation = test_hooks::install_policy_evaluated_gate(gate.clone());
        let historical_version_id = versions.historical_version_id.clone();
        let state = versions.fixture.state.clone();
        let mut tagging = tokio::spawn(async move {
            put_object_tagging_at(
                &state,
                put_version_request(
                    &[("after", "must-not-replace")],
                    Some(&historical_version_id),
                ),
                time(4),
            )
            .await
        });

        wait_for_policy_evaluation_pause(&gate, &mut tagging).await;
        let removed = object_version::Entity::delete_many()
            .filter(object_version::Column::Bucket.eq(BUCKET))
            .filter(object_version::Column::Key.eq(KEY))
            .filter(object_version::Column::VersionId.eq(&versions.historical_version_id))
            .exec(versions.fixture.state.store.db())
            .await
            .unwrap();
        assert_eq!(removed.rows_affected, 1);
        gate.resume.notify_one();
        let result = tagging.await.unwrap();

        assert_eq!(result.unwrap_err().code().as_str(), "NoSuchVersion");
        let historical_tags = stored_tags::list_object_tags(
            versions.fixture.state.store.db(),
            &versions.historical_object_id,
        )
        .await
        .unwrap();
        let current_tags = stored_tags::list_object_tags(
            versions.fixture.state.store.db(),
            &versions.current_object_id,
        )
        .await
        .unwrap();
        assert_eq!(historical_tags, vec![ObjectTag::new("before", "kept")]);
        assert_eq!(current_tags, vec![ObjectTag::new("current", "kept")]);
        assert_eq!(
            pin_lease::Entity::find_by_id("version-race-manual")
                .one(versions.fixture.state.store.db())
                .await
                .unwrap()
                .unwrap(),
            lease_before
        );
    }

    #[tokio::test]
    async fn tagging_never_uses_public_id_as_owner_object_id() {
        let versions = versioned_fixture().await;
        assert_ne!(
            versions.historical_version_id, versions.historical_object_id,
            "public version IDs must not be used as immutable object owners"
        );
        seed_tags_for_owner(
            &versions.fixture,
            &versions.historical_object_id,
            &[("before", "replace")],
        )
        .await;
        seed_tags_for_owner(
            &versions.fixture,
            &versions.current_object_id,
            &[("current", "kept")],
        )
        .await;
        seed_lease_for_owner(
            &versions.fixture,
            &versions.historical_object_id,
            "historical-manual",
            "manual",
            "active",
            "full",
            time(5),
            1,
            &[],
        )
        .await;

        let output = put_object_tagging_at(
            &versions.fixture.state,
            put_version_request(
                &[("historical", "replacement")],
                Some(&versions.historical_version_id),
            ),
            time(4),
        )
        .await
        .unwrap()
        .output;

        assert_eq!(
            output.version_id.as_deref(),
            Some(versions.historical_version_id.as_str())
        );
        let historical_tags = object_tag::Entity::find()
            .filter(object_tag::Column::ObjectId.eq(&versions.historical_object_id))
            .order_by_asc(object_tag::Column::Key)
            .all(versions.fixture.state.store.db())
            .await
            .unwrap();
        assert_eq!(
            historical_tags
                .iter()
                .map(|tag| (tag.key.as_str(), tag.value.as_str()))
                .collect::<Vec<_>>(),
            vec![("historical", "replacement")]
        );
        assert_eq!(
            object_tag::Entity::find()
                .filter(object_tag::Column::ObjectId.eq(&versions.historical_version_id))
                .count(versions.fixture.state.store.db())
                .await
                .unwrap(),
            0
        );
        let historical_lease = pin_lease::Entity::find_by_id("historical-manual")
            .one(versions.fixture.state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            historical_lease.owner_object_id,
            versions.historical_object_id
        );
        assert_eq!(historical_lease.state, "cancelled");
        assert_eq!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq(&versions.historical_version_id))
                .count(versions.fixture.state.store.db())
                .await
                .unwrap(),
            0
        );
        let current_tags = stored_tags::list_object_tags(
            versions.fixture.state.store.db(),
            &versions.current_object_id,
        )
        .await
        .unwrap();
        assert_eq!(current_tags, vec![ObjectTag::new("current", "kept")]);
    }
}
