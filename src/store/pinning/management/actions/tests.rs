use super::*;
use crate::{
    pinning::{management::*, policy::PublicationPolicy},
    store::{
        self,
        entities::{object_version, pin_job, pin_lease, pin_lease_target},
        pinning::publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
    },
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, Statement};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};
const CID: &str = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";

pub(crate) struct Fixture {
    _directory: tempfile::TempDir,
    _server: MockServer,
    pub(crate) db: DatabaseConnection,
    pub(crate) config: ManagementConfig,
    pub(crate) auth: records::LocalAuthorization,
    pub(crate) binding: records::ConfirmedBinding,
}
pub(crate) async fn fixture(plan: &str) -> Fixture {
    fixture_with_automatic(plan, false).await
}
pub(crate) async fn fixture_with_automatic(plan: &str, automatic: bool) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("actions.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    store::run_migrations(&db).await.unwrap();
    store::bucket::create(&db, "bucket", None).await.unwrap();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":CID})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
        .mount(&server)
        .await;
    let mut raw: crate::config::Config = toml::from_str(
        r#"
        [[pinning.providers]]
        name="fixture"
        kind="noop"
        priority=1
        max_bytes=1000000
        max_pins=1000
        [[pinning.policies]]
        bucket="bucket"
        prefix=""
        trigger="request"
        provider_mode="all"
        providers=["fixture"]
        default_duration="1h"
        max_duration="2h"
        allow_decompressed=true
        [pinning_identity]
        primary_storage_domain="hot"
        [[pinning_identity.providers]]
        config_name="fixture"
        provider_id="fixture-provider"
        display_name="Fixture"
        backend="noop"
        scope="isolated"
        storage_domain="remote"
        credential_revision=1
        endpoint_revision=1
        api_profile="noop"
        strategy="cid"
        cleanup="retain"
    "#,
    )
    .unwrap();
    raw.kubo.rpc_url = server.uri();
    let config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(CID.into())
        .unwrap();
    let provider = &config.pinning.providers[0];
    crate::store::pinning::ledger::register_route(&db, &provider.name, &provider.identity)
        .await
        .unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let leases = if automatic {
        vec![crate::pinning::policy::LeaseIntent {
            source: crate::pinning::policy::LeaseSource::Automatic,
            policy_id: config.pinning.policies[0].identity.clone(),
            provider_mode: crate::pinning::config::ProviderMode::All,
            providers: vec![provider.name.clone()],
            content_mode: crate::pinning::tags::ContentMode::Object,
            duration: crate::pinning::config::LeaseDuration::parse("1h").unwrap(),
        }]
    } else {
        vec![]
    };
    publication::publish_object(
        &db,
        PublicationRequest {
            object: PublicationObject::from_put(
                id.clone(),
                "bucket",
                "exact",
                CID.into(),
                10,
                None,
                None,
                false,
                None,
                None,
                chrono::Utc::now(),
            ),
            tags: vec![],
            policy: PublicationPolicy {
                tags: vec![],
                leases,
            },
            object_target: PinTargetSpec {
                cid: CID.into(),
                logical_size: 10,
            },
        },
        &config.pinning.provider_limits,
    )
    .await
    .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&id))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let identity = ExactObject {
        bucket: "bucket".into(),
        key: "exact".into(),
        object_id: id,
        private_version_id: version.id,
        public_version: version
            .version_id
            .map_or(PublicVersionId::Null, PublicVersionId::Version),
        cid: crate::pinning::provider::canonical_resource_cid(CID).unwrap(),
    };
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    evidence::prepare(
        &db,
        &config,
        &auth,
        &crate::kubo::KuboClient::new(server.uri()),
        &evidence::PrepareRequest {
            record_id: uuid::Uuid::new_v4().to_string(),
            identity: identity.clone(),
            expected_node: CID.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let now = store::database_clock::database_now(&db)
        .await
        .unwrap()
        .timestamp();
    let request = PlanRequest {
        plan_id: plan.into(),
        items: vec![ItemRequest {
            item_id: "object".into(),
            action: Action::Create(Intent {
                started_at: now - 1,
                expires_at: now + 3600,
                requirements: Requirements {
                    mode: ProviderMode::All,
                    provider_ids: vec![provider.identity.provider_id.clone()],
                    levels: vec![ConfirmationLevel::RemotePinned],
                    min_replicas: 1,
                    max_age_secs: None,
                    policy_revision: config.pinning.policies[0].identity.clone(),
                },
            }),
            subject: Subject::Object {
                identity,
                target: TargetSelection::New(RouteIdentity::from_original(
                    provider.route_snapshot(),
                )),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    assert!(
        plan.items()
            .iter()
            .all(|entry| !matches!(entry.status, PlanStatus::Blocked(_))),
        "fixture proposal blocked: {:?}",
        plan.items()[0].status
    );
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let binding = records::confirm(
        &db,
        &config,
        &auth,
        (&request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    Fixture {
        _directory: directory,
        _server: server,
        db,
        config,
        auth,
        binding,
    }
}

#[tokio::test]
async fn actual_receipt_update_trigger_fault_rolls_back_effects_and_trigger_ddl() {
    let fixture = fixture("actual-fault").await;
    records::fault_injection::install(
        &fixture.auth,
        &fixture.binding,
        records::fault_injection::Fault::ReceiptAbort,
    );
    let result = apply_detailed(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await;
    let failure = result.err().expect("the real receipt UPDATE must fail");
    assert!(matches!(failure.cause, ManagementError::Database));
    assert_eq!(failure.items.len(), 1);
    assert_eq!(failure.items[0].state, records::ActionState::Stopped);
    assert!(
        pin_lease::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        pin_lease_target::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        pin_job::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        records::lookup_committed(&fixture.db, &fixture.auth, &fixture.binding, "object")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding
        )
        .await
        .is_ok()
    );
}

#[tokio::test]
async fn final_real_database_clock_lease_expiry_fence_rolls_back_even_written_receipts() {
    let fixture = fixture("actual-expiry").await;
    records::fault_injection::install(
        &fixture.auth,
        &fixture.binding,
        records::fault_injection::Fault::ExpireLease,
    );
    let failure = apply_detailed(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .err()
    .expect("expired business lease cannot commit");
    assert!(matches!(failure.cause, ManagementError::Expired));
    assert_eq!(failure.items[0].state, records::ActionState::Stopped);
    assert!(
        pin_lease::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        pin_job::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        records::lookup_committed(&fixture.db, &fixture.auth, &fixture.binding, "object")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn crate_decoder_export_preserves_executed_receipts() {
    let fixture = fixture("decode-export").await;
    let applied = apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let action = &applied.items[0];
    // Read the actual stored encoding so the export exercises the record codec,
    // not a second independently implemented packing format.
    let row = fixture
        .db
        .query_one(sea_orm::Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "SELECT result FROM pin_management_records WHERE plan_id=$1 AND kind='action'",
            [fixture.binding.request.plan_id.clone().into()],
        ))
        .await
        .unwrap()
        .unwrap();
    let (receipt, business) =
        records::decode_action_result(&row.try_get::<String>("", "result").unwrap()).unwrap();
    assert!(receipt == action.receipt);
    assert!(business.as_ref() == Some(action));
    assert!(
        records::decode_action_result("{\"state\":\"accepted\",\"state\":\"needs_attention\"}")
            .is_err()
    );
}

#[tokio::test]
async fn lost_commit_response_and_unavailable_receipt_lookup_is_attention_then_exact_requery() {
    let mut fixture = fixture("commit-unknown").await;
    fault_injection::install(&fixture.auth, &fixture.binding);
    let failure = apply_detailed(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .err()
    .expect("commit response and receipt lookup are unavailable");
    assert_eq!(failure.items[0].state, records::ActionState::NeedsAttention);
    let path = fixture._directory.path().join("actions.sqlite");
    fixture.db = store::connect_database(&format!(
        "sqlite://{}?mode=rw",
        path.to_string_lossy().replace('\\', "/")
    ))
    .await
    .unwrap();
    let leases = pin_lease::Entity::find().all(&fixture.db).await.unwrap();
    let jobs = pin_job::Entity::find().all(&fixture.db).await.unwrap();
    assert_eq!(leases.len(), 1);
    let receipt = status::lookup(&fixture.db, &fixture.auth, &fixture.binding, "object")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.receipt.intent_ids, vec![leases[0].id.clone()]);
    let replay = apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    assert!(replay.replayed);
    assert_eq!(
        pin_lease::Entity::find().all(&fixture.db).await.unwrap(),
        leases
    );
    assert_eq!(
        pin_job::Entity::find().all(&fixture.db).await.unwrap(),
        jobs
    );
}

pub(crate) async fn seed_verified_root(fixture: &Fixture) -> native::NativeKey {
    use crate::store::zip::{self, BatchAdmission, ManifestItem, RootOutcome, VersionBinding};
    let Subject::Object { identity, .. } = &fixture.binding.request.items[0].subject else {
        unreachable!()
    };
    let controls = crate::zip::options::parse_authenticated_zip_controls(
        &http::HeaderMap::new(),
        &[("decompress-zip".into(), "".into())],
        &[],
        true,
        true,
    )
    .unwrap()
    .unwrap();
    let capture = crate::pinning::zip_policy::capture_zip_intent(
        &fixture.config.pinning,
        &Default::default(),
        crate::config::OptionalPinControlMode::Warn,
        crate::pinning::zip_policy::ZipCaptureContext {
            bucket: "bucket",
            source_key: "root.zip",
            target_prefix: "",
            tags: &[],
        },
        &controls,
        crate::pinning::decision::DecisionOrigin::new("root-owner", "root"),
    )
    .unwrap();
    zip::admit(
        &fixture.db,
        &BatchAdmission {
            id: "root".into(),
            owner: "root-owner".into(),
            source: "direct".into(),
            token: capture.controls().admission_token.clone(),
            fingerprint: "root".into(),
            bucket: "bucket".into(),
            archive_key: "root.zip".into(),
            input_identity: "input".into(),
            captured_options: serde_json::json!({"zip_intent":capture}).to_string(),
        },
    )
    .await
    .unwrap();
    zip::prepare_manifest(
        &fixture.db,
        "root",
        &[ManifestItem::Success {
            path: "exact".into(),
            object_key: identity.key.clone(),
            cid: CID.into(),
            size: 10,
        }],
    )
    .await
    .unwrap();
    let claim = zip::claim_root(&fixture.db, "root", "fixture", 3600)
        .await
        .unwrap();
    zip::mark_invoked(&fixture.db, &claim).await.unwrap();
    zip::retain_candidate(&fixture.db, &claim, CID, "hot", CID)
        .await
        .unwrap();
    zip::verify_root(&fixture.db, &claim, CID, "hot", CID, "verified-receipt")
        .await
        .unwrap();
    let tx = fixture.db.begin().await.unwrap();
    zip::publish(
        &tx,
        "root",
        &[VersionBinding {
            path: "exact".into(),
            version_row_id: identity.private_version_id.clone(),
        }],
        false,
        "{\"root_status\":\"complete\"}",
        RootOutcome::Verified {
            claim: claim.clone(),
            node_identity: CID.into(),
            tier: "hot".into(),
            cid: CID.into(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    native::NativeKey {
        table: "zip_root_references".into(),
        values: vec![
            "root".into(),
            claim.revision.to_string(),
            claim.epoch.to_string(),
            CID.into(),
            "hot".into(),
            CID.into(),
        ],
    }
}

#[tokio::test]
async fn trusted_rejected_park_restore_is_atomic_and_preserves_the_existing_reservation() {
    use crate::store::{
        entities::{pin_provider_usage, remote_pin},
        pinning::jobs,
    };
    let fixture = fixture_with_automatic("parked-source", true).await;
    let original = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let remote = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let usage = pin_provider_usage::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='rejected'").await.unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='not_created'")
        .await
        .unwrap();
    let now = crate::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','not_created','blocked',1,0,$2)",[original.id.clone().into(),now.into()])).await.unwrap();
    let parked = pin_job::Entity::find_by_id(original.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let history = jobs::submission_history(&fixture.db, &original.id)
        .await
        .unwrap()
        .unwrap();
    let route = fixture.config.pinning.providers[0].route_snapshot();
    assert!(
        jobs::management_retryable_park(&fixture.db, &parked, &route)
            .await
            .unwrap()
    );
    let tx = fixture.db.begin().await.unwrap();
    assert!(
        jobs::resume_management_submit_in_transaction(
            &tx,
            &parked,
            &route,
            now,
            records::DecodeWork::shared(&records::SharedDecodeBudget::default())
        )
        .await
        .unwrap()
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        pin_job::Entity::find_by_id(original.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        parked
    );
    assert_eq!(
        jobs::submission_history(&fixture.db, &original.id)
            .await
            .unwrap()
            .unwrap(),
        history
    );
    let tx = fixture.db.begin().await.unwrap();
    assert!(
        jobs::resume_management_submit_in_transaction(
            &tx,
            &parked,
            &route,
            now,
            records::DecodeWork::shared(&records::SharedDecodeBudget::default())
        )
        .await
        .unwrap()
    );
    tx.commit().await.unwrap();
    let recovered = pin_job::Entity::find_by_id(original.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.state, "pending");
    assert_eq!(recovered.submit_phase.as_deref(), Some("ready"));
    assert!(recovered.locked_until.is_none());
    assert_eq!(recovered.expected_generation, parked.expected_generation);
    assert_eq!(recovered.target_id, parked.target_id);
    assert_eq!(recovered.lease_id, parked.lease_id);
    let mut restored = jobs::submission_history(&fixture.db, &original.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.state, "active");
    restored.state = history.state.clone();
    assert_eq!(restored, history);
    assert_eq!(
        remote_pin::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        remote
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        usage
    );
    assert!(
        !jobs::management_retryable_park(&fixture.db, &recovered, &route)
            .await
            .unwrap()
    );
    assert_eq!(
        pin_job::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn root_release_real_receipt_fault_rolls_back_marker_and_all_retirement_writes() {
    use crate::store::zip;
    let fixture = fixture("root-source").await;
    let key = seed_verified_root(&fixture).await;
    let request = PlanRequest {
        plan_id: "root-receipt-fault".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::RetireWith {
                selection: RetirementSelection::ReleaseRootReferences {
                    references: vec![key],
                },
            },
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture.config.pinning.providers[0].route_snapshot(),
                ),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        vec![],
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    let before = zip::snapshot(&fixture.db, "root").await.unwrap().unwrap();
    records::fault_injection::install(
        &fixture.auth,
        &binding,
        records::fault_injection::Fault::ReceiptAbort,
    );
    let failure = apply_detailed(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .err()
        .expect("actual receipt write must abort");
    assert_eq!(failure.cause, ManagementError::Database);
    assert_eq!(failure.items[0].state, records::ActionState::Stopped);
    assert_eq!(
        zip::snapshot(&fixture.db, "root")
            .await
            .unwrap()
            .unwrap()
            .references,
        before.references
    );
    assert!(
        records::lookup_committed(&fixture.db, &fixture.auth, &binding, "provider")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        records::read_retirement(
            &fixture.db,
            &fixture.auth,
            &fixture.config.pinning.providers[0].name
        )
        .await
        .unwrap()
        .is_none()
    );
    apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
}

#[tokio::test]
async fn shared_rejected_source_allocation_preserves_history_and_replays_without_business() {
    use crate::store::{
        entities::{pin_invocation_route, pin_provider_usage, remote_pin},
        pinning::jobs,
    };
    let mut fixture = fixture_with_automatic("shared-rejected-continuity", true).await;
    let source = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let lease = pin_lease::Entity::find_by_id(source.lease_id.clone().unwrap())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let target = pin_lease_target::Entity::find_by_id(source.target_id.clone().unwrap())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let invocation = pin_invocation_route::Entity::find_by_id(source.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let usage = pin_provider_usage::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='rejected'").await.unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='not_created'")
        .await
        .unwrap();
    let now = store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','not_created','blocked',1,0,$2)",[source.id.clone().into(),now.into()])).await.unwrap();
    let history = jobs::submission_history(&fixture.db, &source.id)
        .await
        .unwrap()
        .unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "shared-rejected-confirmed".into();
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    fixture.binding = records::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    records::fault_injection::install(
        &fixture.auth,
        &fixture.binding,
        records::fault_injection::Fault::ReceiptAbort,
    );
    assert_eq!(
        apply_detailed(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding
        )
        .await
        .err()
        .unwrap()
        .cause,
        ManagementError::Database
    );
    assert_eq!(
        pin_lease::Entity::find().all(&fixture.db).await.unwrap(),
        vec![lease.clone()]
    );
    assert_eq!(
        jobs::submission_history(&fixture.db, &source.id)
            .await
            .unwrap()
            .unwrap(),
        history
    );
    assert_eq!(
        remote_pin::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .epoch,
        invocation.remote_epoch
    );
    let applied = apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let seed = applied.items[0]
        .jobs
        .iter()
        .find(|job| job.directive == records::JobDirective::Submit)
        .unwrap();
    assert_eq!(seed.job_id, source.id);
    assert_eq!(seed.lease_id, source.lease_id);
    assert_eq!(seed.target_id, source.target_id);
    assert_eq!(seed.lease_generation, source.expected_generation);
    assert_eq!(seed.remote_epoch, invocation.remote_epoch);
    assert_eq!(
        seed.owner.as_ref().unwrap().object_id,
        lease.owner_object_id
    );
    assert_eq!(seed.lease_source.as_deref(), Some(lease.source.as_str()));
    let mut recovered_history = jobs::submission_history(&fixture.db, &source.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered_history.state, "active");
    recovered_history.state = history.state.clone();
    assert_eq!(recovered_history, history);
    assert_eq!(
        pin_lease::Entity::find_by_id(lease.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        lease
    );
    assert_eq!(
        pin_lease_target::Entity::find_by_id(target.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        target
    );
    assert_eq!(
        pin_invocation_route::Entity::find_by_id(source.id.clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        invocation
    );
    assert_eq!(
        remote_pin::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .epoch,
        invocation.remote_epoch + 1
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        usage
    );
    assert_eq!(
        pin_job::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap()
            .len(),
        1
    );
    let before = native::capture(&fixture.db).await.unwrap();
    assert!(
        apply(
            &fixture.db,
            &fixture.config,
            &fixture.auth,
            &fixture.binding
        )
        .await
        .unwrap()
        .replayed
    );
    assert!(native::capture(&fixture.db).await.unwrap() == before);
}

#[tokio::test]
async fn shared_park_restore_rejects_changed_claim_route_effect_owner_and_unproven_epoch() {
    use crate::store::pinning::{
        jobs,
        publication::management::{AbsoluteIntent, allocate_existing_in_transaction},
    };
    let fixture = fixture_with_automatic("shared-park-fences", true).await;
    let original = pin_job::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    fixture.db.execute_unprepared("UPDATE pin_jobs SET state='running',submit_phase='recovering',locked_until=NULL,last_error='rejected'").await.unwrap();
    fixture
        .db
        .execute_unprepared("UPDATE remote_pin_ledger SET effect='not_created'")
        .await
        .unwrap();
    let now = store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"INSERT INTO pin_submit_history(job_id,api,strategy,effect,state,submit_calls,recovery_queries,started_at) VALUES($1,'noop','cid','not_created','blocked',1,0,$2)",[original.id.clone().into(),now.into()])).await.unwrap();
    let parked = pin_job::Entity::find_by_id(original.id.clone())
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    let route = fixture.config.pinning.providers[0].route_snapshot();
    let Action::Create(intent) = &fixture.binding.request.items[0].action else {
        unreachable!()
    };
    let Subject::Object { identity, .. } = &fixture.binding.request.items[0].subject else {
        unreachable!()
    };
    let lease_intent = crate::pinning::policy::LeaseIntent {
        source: crate::pinning::policy::LeaseSource::Manual,
        policy_id: fixture.config.pinning.policies[0].identity.clone(),
        provider_mode: crate::pinning::config::ProviderMode::All,
        providers: vec![parked.provider.clone()],
        content_mode: crate::pinning::tags::ContentMode::Object,
        duration: crate::pinning::config::LeaseDuration::parse("1h").unwrap(),
    };
    for mutation in [
        "UPDATE remote_pins SET epoch=epoch+1",
        "UPDATE pin_jobs SET locked_until='2099-01-01T00:00:00Z'",
        "UPDATE pin_submit_history SET effect='unknown'",
        "UPDATE pin_leases SET source='different' WHERE source NOT LIKE 'management:%'",
        "UPDATE pin_invocation_routes SET route='{}'",
        "UPDATE pin_provider_usage SET reserved_pins=reserved_pins+1",
        "UPDATE remote_pin_ledger SET effect='retained'",
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) SELECT provider,cid,epoch,'{\"confirmed_absent\":true}' FROM remote_pins",
    ] {
        let tx = fixture.db.begin().await.unwrap();
        let attached = allocate_existing_in_transaction(
            &tx,
            AbsoluteIntent {
                lease_id: "management:attached-proof",
                owner_object_id: &identity.object_id,
                source: "management:attached-proof",
                intent: &lease_intent,
                targets: &[PinTargetSpec {
                    cid: parked.cid.clone(),
                    logical_size: 10,
                }],
                started_at: chrono::DateTime::from_timestamp(intent.started_at, 0).unwrap(),
                expires_at: chrono::DateTime::from_timestamp(intent.expires_at, 0).unwrap(),
                requirements: &intent.requirements,
            },
            &fixture.config.pinning.provider_limits,
            now,
        )
        .await
        .unwrap();
        tx.execute_unprepared(mutation).await.unwrap();
        assert!(
            !jobs::resume_management_submit_after_allocation(
                &tx,
                &parked,
                &route,
                (&before).into(),
                &attached[0],
                now,
                records::DecodeWork::shared(&records::SharedDecodeBudget::default())
            )
            .await
            .unwrap(),
            "unsafe continuity admitted: {mutation}"
        );
        tx.rollback().await.unwrap();
    }
    assert!(native::capture(&fixture.db).await.unwrap() == before);
}
