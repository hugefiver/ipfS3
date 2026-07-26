use std::sync::Arc;

use chrono::{DateTime, Utc};
use s3s::{S3Request, S3Response, S3Result, dto::*};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, QueryFilter, QuerySelect,
    TransactionError, TransactionTrait,
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
        pinning::{leases, tags},
    },
};

const INVALID_TAG_SET: &str = "invalid object tag set";
const INVALID_STORED_MANUAL_LEASE: &str = "stored manual pin lease is invalid";
const MANUAL_RENEWAL_REJECTED: &str = "manual pin lease cannot be renewed";
const MANUAL_LEASE_CHANGED: &str = "manual pin lease changed during tag replacement";

#[cfg(test)]
mod test_hooks {
    use std::sync::{Arc, LazyLock};

    use tokio::sync::{Mutex, Notify};

    pub struct PolicyEvaluatedGate {
        pub lease_id: &'static str,
        pub arrived: Notify,
        pub resume: Notify,
    }

    pub static POLICY_EVALUATED: LazyLock<Mutex<Option<Arc<PolicyEvaluatedGate>>>> =
        LazyLock::new(|| Mutex::new(None));
}

#[cfg(test)]
async fn pause_after_policy_evaluation(lease_id: Option<&str>) {
    let gate = test_hooks::POLICY_EVALUATED.lock().await.clone();
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
    require_unversioned(input.version_id.as_deref())?;
    let object = crate::store::object::get_latest(state.store.db(), &input.bucket, &input.key)
        .await
        .map_err(s3s::S3Error::from)?;
    let stored = tags::list_object_tags(state.store.db(), &object.id)
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
        version_id: None,
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
    require_unversioned(input.version_id.as_deref())?;
    let replacement = dto_to_object_tags(input.tagging.tag_set).map_err(s3s::S3Error::from)?;
    validate_replacement(&replacement).map_err(s3s::S3Error::from)?;
    replace_tag_set(state, input.bucket, input.key, replacement, now)
        .await
        .map_err(s3s::S3Error::from)?;
    Ok(S3Response::new(PutObjectTaggingOutput::default()))
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
    require_unversioned(input.version_id.as_deref())?;
    let replacement = Vec::new();
    validate_replacement(&replacement).map_err(s3s::S3Error::from)?;
    replace_tag_set(state, input.bucket, input.key, replacement, now)
        .await
        .map_err(s3s::S3Error::from)?;
    Ok(S3Response::new(DeleteObjectTaggingOutput::default()))
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

fn require_unversioned(version_id: Option<&str>) -> S3Result<()> {
    if version_id.is_some() {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "versionId is not supported"
        ));
    }
    Ok(())
}

async fn replace_tag_set(
    state: &Arc<AppState>,
    bucket: String,
    key: String,
    replacement: Vec<ObjectTag>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let operation_state = Arc::clone(state);
    state
        .store
        .db()
        .transaction(move |txn| {
            Box::pin(async move {
                let owner = lock_latest_object(txn, &bucket, &key).await?;
                let manual = load_manual_lease(txn, &owner.id).await?;
                let existing = manual.as_ref().map(existing_manual_lease).transpose()?;
                let mutation = operation_state
                    .pinning
                    .policy()
                    .evaluate_tag_replacement(existing.as_ref(), &replacement, now)
                    .map_err(AppError::from)?;

                #[cfg(test)]
                pause_after_policy_evaluation(manual.as_ref().map(|lease| lease.id.as_str())).await;

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
                        leases::renew_manual_lease(txn, &owner.id, lease_id, retain_until, now)
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
                tags::replace_object_tags(txn, &owner.id, &replacement).await?;
                Ok(())
            })
        })
        .await
        .map_err(transaction_error_into_app)
}

async fn lock_latest_object<C: ConnectionTrait>(
    db: &C,
    bucket: &str,
    key: &str,
) -> AppResult<object::Model> {
    let query = object::Entity::find()
        .filter(object::Column::Bucket.eq(bucket))
        .filter(object::Column::Key.eq(key))
        .filter(object::Column::IsLatest.eq(true));
    let object = if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    };
    object.ok_or_else(|| AppError::NoSuchKey(format!("{bucket}/{key}")))
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
    };

    use chrono::{DateTime, Utc};
    use s3s::{S3Request, dto::*};
    use sea_orm::sea_query::Expr;
    use sea_orm::{
        ActiveValue::Set, ColumnTrait, ConnectOptions, ConnectionTrait, Database,
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
            entities::{pin_job, pin_lease, pin_lease_target, pin_provider_usage, remote_pin},
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
        request(
            PutObjectTaggingInput {
                bucket: BUCKET.to_owned(),
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                key: KEY.to_owned(),
                request_payer: None,
                tagging: dto_tags(pairs),
                version_id: None,
            },
            http::Method::PUT,
        )
    }

    fn get_request(key: &str) -> S3Request<GetObjectTaggingInput> {
        request(
            GetObjectTaggingInput {
                bucket: BUCKET.to_owned(),
                key: key.to_owned(),
                ..Default::default()
            },
            http::Method::GET,
        )
    }

    fn delete_request(key: &str) -> S3Request<DeleteObjectTaggingInput> {
        request(
            DeleteObjectTaggingInput {
                bucket: BUCKET.to_owned(),
                key: key.to_owned(),
                ..Default::default()
            },
            http::Method::DELETE,
        )
    }

    async fn seed_tags(fixture: &Fixture, pairs: &[(&str, &str)]) {
        let tags = pairs
            .iter()
            .map(|(key, value)| crate::pinning::tags::ObjectTag::new(*key, *value))
            .collect::<Vec<_>>();
        stored_tags::replace_object_tags(fixture.state.store.db(), OBJECT_ID, &tags)
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
    async fn unsupported_version_ids_are_rejected_before_reads_or_mutations() {
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
            assert_eq!(error.message(), Some("versionId is not supported"));
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
        *test_hooks::POLICY_EVALUATED.lock().await = Some(gate.clone());
        let state = fixture.state.clone();
        let tagging = tokio::spawn(async move {
            put_object_tagging_at(&state, put_request(&[("after", "rejected")]), time(2)).await
        });

        gate.arrived.notified().await;
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
        *test_hooks::POLICY_EVALUATED.lock().await = None;

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
}
