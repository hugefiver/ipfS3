#[allow(dead_code)]
#[path = "support/stage7_management.rs"]
mod publication;
use ipfs_s3_gateway::{
    pinning::{self, management::*},
    store::{
        self,
        entities::{object_version, pin_extension_decision, pin_job, pin_lease_target, remote_pin},
        pinning::{
            management::{actions, records},
            publication::{PinTargetSpec, PublicationObject, PublicationRequest},
        },
    },
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn approved_reapply_preserves_skipped_decision_and_dispatches_bound_psa_submit() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":publication::NODE})),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
        .mount(&server)
        .await;
    Mock::given(method("POST")).and(path("/psa/pins")).respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({"requestid":"reapply-bound-request","status":"pinned","created":"2026-01-01T00:00:00Z","pin":{"cid":publication::CID,"name":"reapply"},"delegates":[],"info":{}}))).expect(1).mount(&server).await;
    let (_directory, db) = publication::database().await;
    let mut raw = publication::config(&server.uri());
    raw.pinning.providers[0].kind = "pinata".into();
    raw.pinning.providers[0].endpoint = Some(format!("{}/psa", server.uri()));
    raw.pinning.providers[0].token_env = Some("REVIEW_SOURCE_TOKEN".into());
    raw.pinning_identity.providers[0].backend = "pinata".into();
    raw.pinning_identity.providers[0].api_profile = "pinata-psa".into();
    raw.pinning_identity.providers[0].secret_ref = Some("env:REVIEW_SOURCE_TOKEN".into());
    let config = local_config::compile(&raw, |_| Some("local-fixture-only".into()))
        .unwrap()
        .with_source_node(publication::NODE.into())
        .unwrap();
    publication::setup(&db, &config).await;
    let mut old = raw.clone();
    old.pinning.policies.clear();
    old.pinning_control.unavailable = ipfs_s3_gateway::config::OptionalPinControlMode::Warn;
    let old = local_config::compile(&old, |_| Some("local-fixture-only".into())).unwrap();
    let tags = vec![pinning::tags::ObjectTag::new("ipfs-s3:pin", "true")];
    let (policy, decision) = old
        .policy
        .evaluate_publication_decision(
            pinning::policy::PublicationContext {
                bucket: "bucket",
                key: "skipped",
                tags: &tags,
                is_decompress_zip: false,
            },
            pinning::decision::DecisionOrigin::new("review", "skipped"),
        )
        .unwrap();
    assert_eq!(decision.effect, pinning::decision::DecisionEffect::Skipped);
    let object = uuid::Uuid::new_v4().to_string();
    store::pinning::publication::publish_object(
        &db,
        PublicationRequest {
            object: PublicationObject::from_put(
                object.clone(),
                "bucket",
                "skipped",
                publication::CID.into(),
                10,
                None,
                None,
                false,
                None,
                None,
                chrono::Utc::now(),
            ),
            tags,
            policy,
            object_target: PinTargetSpec {
                cid: publication::CID.into(),
                logical_size: 10,
            },
        },
        &config.pinning.provider_limits,
    )
    .await
    .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&object))
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let identity = ExactObject {
        bucket: "bucket".into(),
        key: "skipped".into(),
        public_version: PublicVersionId::Null,
        private_version_id: version.id.clone(),
        object_id: object,
        cid: pinning::provider::canonical_resource_cid(publication::CID).unwrap(),
    };
    let tx = db.begin().await.unwrap();
    store::pinning::decision::write_for_object_in_transaction(&tx, &identity.object_id, &decision)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let original = pin_extension_decision::Entity::find_by_id(version.id.clone())
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    evidence::prepare(
        &db,
        &config,
        &auth,
        &ipfs_s3_gateway::kubo::KuboClient::new(server.uri()),
        &evidence::PrepareRequest {
            record_id: "reapply-read".into(),
            identity: identity.clone(),
            expected_node: publication::NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut request = publication::request(identity, &config, false);
    request.plan_id = "review-reapply".into();
    let Action::Create(intent) = request.items[0].action.clone() else {
        unreachable!()
    };
    request.items[0].action = Action::Reapply(intent);
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request)
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
    let applied = actions::apply(&db, &config, &auth, &binding).await.unwrap();
    let seed = applied.items[0]
        .jobs
        .iter()
        .find(|seed| seed.directive == records::JobDirective::Submit)
        .unwrap();
    let mut runtime = config.pinning.clone();
    runtime.worker_interval = pinning::config::LeaseDuration::parse("1s").unwrap();
    let coordinator = pinning::coordinator::PinningCoordinator::build_with_kubo(
        runtime,
        Some(ipfs_s3_gateway::kubo::KuboClient::new(server.uri())),
    )
    .unwrap();
    let worker = coordinator.start(store::Store::new(db.clone()), CancellationToken::new());
    let receipt = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let remote = remote_pin::Entity::find_by_id((seed.provider.clone(), seed.cid.clone()))
                .one(&db)
                .await
                .unwrap()
                .unwrap();
            if remote.request_id.as_deref() == Some("reapply-bound-request")
                && remote.status == "pinned"
            {
                break remote;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    worker.shutdown(std::time::Duration::from_secs(2)).await;
    eprintln!(
        "REVIEW_SRC receipt_ok={} posts={} job={:?}",
        receipt.is_ok(),
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method == "POST" && request.url.path() == "/psa/pins")
            .count(),
        pin_job::Entity::find_by_id(seed.job_id.clone())
            .one(&db)
            .await
            .unwrap()
            .map(|job| (job.state, job.last_error))
    );
    let receipt = receipt.unwrap();
    assert_eq!(receipt.request_id.as_deref(), Some("reapply-bound-request"));
    assert_eq!(
        pin_lease_target::Entity::find_by_id(seed.target_id.clone().unwrap())
            .one(&db)
            .await
            .unwrap()
            .unwrap()
            .state,
        "pinned"
    );
    assert_eq!(
        pin_extension_decision::Entity::find_by_id(version.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap(),
        original
    );
    assert_eq!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.method == "POST" && request.url.path() == "/psa/pins")
            .count(),
        1
    );
    server.verify().await;
}
