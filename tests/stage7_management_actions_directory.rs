#[path = "support/stage7_management_actions_directory.rs"]
pub mod support;
use ipfs_s3_gateway::{
    pinning::management::*,
    store::{
        entities::{pin_lease, pin_lease_target, pin_provider_usage, remote_pin},
        pinning::management::{actions, native, records},
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, Statement};

#[tokio::test]
async fn real_directory_and_hamt_proofs_allocate_cid_and_car_with_original_bound_count_and_replay()
{
    for (strategy, mode, alternative, hamt) in [
        ("cid", "all", None, false),
        ("car", "all", None, false),
        ("cid", "all", None, true),
        ("cid", "all", Some("car"), false),
        ("cid", "one", Some("car"), false),
    ] {
        let mut fixture = support::Fixture::new(strategy, mode, alternative, hamt).await;
        let identity = fixture.publish("directory", false).await;
        let proof = fixture
            .prepare(identity.clone(), "actual-directory-proof")
            .await;
        assert_eq!(proof.directory_root_kind(), Some(fixture.kind));
        assert_eq!(proof.stored_bytes, Some(fixture.payload_bytes));
        assert!(fixture.car_bytes > fixture.payload_bytes);
        let request = fixture.request("directory-create", identity.clone());
        let (binding, imported) = fixture.confirm(&request).await;
        let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
            .await
            .unwrap();
        let target = pin_lease_target::Entity::find_by_id(applied.items[0].targets[0].clone())
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(target.logical_size, fixture.payload_bytes as i64);
        assert_eq!(target.cid, fixture.cid);
        let remote = remote_pin::Entity::find_by_id((target.provider, target.cid))
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(remote.cid_size, fixture.payload_bytes as i64);
        let usage = pin_provider_usage::Entity::find()
            .all(&fixture.db)
            .await
            .unwrap();
        let slots = if alternative.is_some() && mode == "all" {
            2
        } else {
            1
        };
        assert_eq!(
            usage.iter().map(|row| row.reserved_bytes).sum::<i64>(),
            fixture.payload_bytes as i64 * slots
        );
        assert_eq!(
            usage.iter().map(|row| row.reserved_pins).sum::<i64>(),
            slots
        );
        let birth = ipfs_s3_gateway::store::entities::pin_lease_requirement::contract::get(
            &fixture.db,
            &applied.items[0].receipt.intent_ids[0],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            birth.candidate_order.len(),
            if alternative.is_some() { 2 } else { 1 }
        );
        fixture.db.clone().close().await.unwrap();
        let path = fixture.directory.path().join("management.sqlite");
        fixture.db = ipfs_s3_gateway::store::connect_database(&format!(
            "sqlite://{}?mode=rw",
            path.to_string_lossy().replace('\\', "/")
        ))
        .await
        .unwrap();
        let restored = support::archive::restore(&fixture.db, &fixture.auth, &request, &imported)
            .await
            .unwrap();
        assert!(restored == binding);
        assert!(
            actions::apply(&fixture.db, &fixture.config, &fixture.auth, &restored)
                .await
                .unwrap()
                .replayed
        );
        // The basis remains readable from the original private applied binding
        // even after another exact owner of this root obtains an independent RID.
        let shared = fixture.publish("shared-directory", false).await;
        fixture
            .prepare(shared.clone(), "independent-directory-proof")
            .await;
        let (second, _) = fixture
            .confirm(&fixture.request("second-reference", shared))
            .await;
        actions::apply(&fixture.db, &fixture.config, &fixture.auth, &second)
            .await
            .unwrap();
        assert_eq!(
            pin_provider_usage::Entity::find()
                .all(&fixture.db)
                .await
                .unwrap()
                .iter()
                .map(|row| row.reserved_bytes)
                .sum::<i64>(),
            fixture.payload_bytes as i64 * slots
        );
        assert_eq!(
            pin_lease::Entity::find()
                .all(&fixture.db)
                .await
                .unwrap()
                .len(),
            2
        );
        assert!(
            fixture
                .server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| matches!(
                    request.url.path(),
                    "/api/v0/id" | "/api/v0/pin/ls" | "/api/v0/files/stat" | "/api/v0/dag/export"
                ))
        );
    }
}

#[tokio::test]
async fn directory_upload_and_complete_one_all_contracts_with_an_upload_slot_are_blocked() {
    for (strategy, mode, mixed) in [
        ("upload", "all", false),
        ("cid", "all", true),
        ("cid", "one", true),
    ] {
        let fixture = support::Fixture::new(strategy, mode, mixed.then_some("upload"), false).await;
        let identity = fixture.publish("unsupported-upload", false).await;
        fixture.prepare(identity.clone(), "actual-proof").await;
        let request = fixture.request("directory-upload", identity);
        let before = native::capture(&fixture.db).await.unwrap();
        let snapshot = fixture.snapshot(&request).await;
        let plan = dry_run(&request, &snapshot).unwrap();
        assert_eq!(
            plan.items()[0].status,
            PlanStatus::Blocked(SafeError::SafetyProhibition)
        );
        let Action::Create(intent) = &request.items[0].action else {
            unreachable!()
        };
        assert_eq!(
            intent.requirements.provider_ids.len(),
            if mixed { 2 } else { 1 }
        );
        assert!(
            records::stage_confirmation(
                &fixture.db,
                &fixture.config,
                &fixture.auth,
                &request,
                &snapshot,
                &plan,
                chrono::Duration::minutes(5)
            )
            .await
            .is_err()
        );
        assert!(native::capture(&fixture.db).await.unwrap() == before);
        assert!(
            pin_lease::Entity::find()
                .all(&fixture.db)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn equal_numeric_legacy_reservation_has_no_directory_basis_and_is_not_repriced() {
    let fixture = support::Fixture::new("cid", "all", None, false).await;
    let identity = fixture.publish("legacy-held-root", true).await;
    let original = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.cid_size, fixture.payload_bytes as i64);
    fixture
        .prepare(identity.clone(), "new-versioned-directory-proof")
        .await;
    let request = fixture.request("do-not-upgrade-old-hold", identity);
    let before = native::capture(&fixture.db).await.unwrap();
    let snapshot = fixture.snapshot(&request).await;
    assert_eq!(
        dry_run(&request, &snapshot).unwrap().items()[0].status,
        PlanStatus::Blocked(SafeError::SafetyProhibition)
    );
    assert_eq!(
        remote_pin::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        original
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        fixture.payload_bytes as i64
    );
    assert!(native::capture(&fixture.db).await.unwrap() == before);
}

#[tokio::test]
async fn legacy_sizeless_record_stays_blocked_until_a_separate_real_v1_rid_is_prepared() {
    let fixture = support::Fixture::new("cid", "all", None, false).await;
    let identity = fixture.publish("legacy-proof", false).await;
    let mut old = fixture.prepare(identity.clone(), "legacy-rid").await;
    old.proof = evidence::ReadProof::CompleteLocalDirectory;
    old.stored_bytes = None;
    old.directory_receipt = None;
    // Historical-format adversarial fixture only; no new production writer or
    // current configuration may manufacture its missing counted receipt.
    let result = serde_json::to_string(&(&old, fixture.server.uri())).unwrap();
    let legacy = serde_json::to_string(&(
        &old.identity,
        &old.source.node_identity,
        fixture.payload_bytes,
        true,
        chrono::Duration::minutes(15).num_nanoseconds().unwrap(),
    ))
    .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE pin_management_records SET result=$1,binding=$2 WHERE plan_id='legacy-rid' AND kind='evidence_preparation'",[result.clone().into(),legacy.into()])).await.unwrap();
    let request = fixture.request("legacy-still-no-count", identity.clone());
    let snapshot = fixture.snapshot(&request).await;
    assert_eq!(
        dry_run(&request, &snapshot).unwrap().items()[0].status,
        PlanStatus::Blocked(SafeError::StoredByteProofUnavailable)
    );
    fixture.prepare(identity, "new-independent-v1-rid").await;
    assert_eq!(
        dry_run(&request, &fixture.snapshot(&request).await)
            .unwrap()
            .items()[0]
            .status,
        PlanStatus::Ready
    );
    let row=fixture.db.query_one(Statement::from_string(fixture.db.get_database_backend(),"SELECT result FROM pin_management_records WHERE plan_id='legacy-rid' AND kind='evidence_preparation'")).await.unwrap().unwrap();
    assert_eq!(row.try_get::<String>("", "result").unwrap(), result);
}

#[tokio::test]
async fn public_directory_tag_or_mismatched_receipt_cannot_replace_the_trusted_original() {
    let fixture = support::Fixture::new("cid", "all", None, false).await;
    let identity = fixture.publish("exact-receipt", false).await;
    let proof = fixture.prepare(identity.clone(), "exact-rid").await;
    let request = fixture.request("reject-public-claim", identity);
    let mut snapshot = fixture.snapshot(&request).await;
    let public = snapshot.objects[0].read_evidence.as_mut().unwrap();
    public.stored_bytes = Some(fixture.payload_bytes + 1);
    assert!(validate_snapshot(&snapshot).is_err());
    let mut claimed = serde_json::to_value(&proof).unwrap();
    claimed["directory_receipt"]["cid"] =
        serde_json::json!("QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE");
    let claimed: evidence::ReadAttestation = serde_json::from_value(claimed).unwrap();
    assert_eq!(ReadEvidence::from(&claimed).stored_bytes, None);
    let mut claimed = serde_json::to_value(&proof).unwrap();
    claimed["directory_receipt"] = serde_json::Value::Null;
    let claimed: evidence::ReadAttestation = serde_json::from_value(claimed).unwrap();
    assert_eq!(ReadEvidence::from(&claimed).stored_bytes, None);
}

#[tokio::test]
async fn a_new_file_proof_with_equal_bytes_cannot_relabel_a_persisted_directory_hold() {
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path, query_param},
    };
    let fixture = support::Fixture::new("cid", "all", None, false).await;
    let identity = fixture.publish("held-kind", false).await;
    fixture.prepare(identity.clone(), "directory-basis").await;
    let (binding, _) = fixture
        .confirm(&fixture.request("original-directory-birth", identity.clone()))
        .await;
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let held = remote_pin::Entity::find()
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    // Adversarial local fixture drift, followed by the real legacy FileCleanEof
    // writer. Its numeric count equals the old hold but does not replace its kind.
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE objects SET size=$1 WHERE id=$2",
            [
                (fixture.payload_bytes as i64).into(),
                identity.object_id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .and(query_param("offline", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_bytes(vec![b'x'; fixture.payload_bytes as usize]),
        )
        .mount(&fixture.server)
        .await;
    evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &ipfs_s3_gateway::kubo::KuboClient::new(fixture.server.uri()),
        &evidence::PrepareRequest {
            record_id: "fresh-file-kind".into(),
            identity: identity.clone(),
            expected_node: "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE".into(),
            max_stored_bytes: fixture.payload_bytes,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        },
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    let request = fixture.request("not-the-same-basis", identity);
    assert_eq!(
        dry_run(&request, &fixture.snapshot(&request).await)
            .unwrap()
            .items()[0]
            .status,
        PlanStatus::Blocked(SafeError::SafetyProhibition)
    );
    assert_eq!(
        remote_pin::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap(),
        held
    );
    assert_eq!(
        pin_provider_usage::Entity::find()
            .one(&fixture.db)
            .await
            .unwrap()
            .unwrap()
            .reserved_bytes,
        fixture.payload_bytes as i64
    );
}

#[tokio::test]
async fn expired_directory_evidence_refuses_fresh_work_without_invalidating_exact_committed_replay()
{
    let fixture = support::Fixture::new("cid", "all", None, false).await;
    let identity = fixture.publish("proof-deadline", false).await;
    let mut proof = fixture
        .prepare(identity.clone(), "proof-deadline-rid")
        .await;
    let (binding, _) = fixture
        .confirm(&fixture.request("original-before-expiry", identity.clone()))
        .await;
    let committed = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    // Expired historical-row fixture; no producer retry, source RPC or deadline
    // extension occurs during either the readonly collection or exact replay.
    let now = ipfs_s3_gateway::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    proof.expires_at = now;
    let result = serde_json::to_string(&(&proof, fixture.server.uri())).unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE pin_management_records SET result=$1,expires_at=$2 WHERE plan_id='proof-deadline-rid' AND kind='evidence_preparation'",[result.into(),now.into()])).await.unwrap();
    let requests = fixture.server.received_requests().await.unwrap().len();
    let request = fixture.request("not-a-new-fresh-grant", identity);
    assert_eq!(
        dry_run(&request, &fixture.snapshot(&request).await)
            .unwrap()
            .items()[0]
            .status,
        PlanStatus::Blocked(SafeError::UnreadableObject)
    );
    let replay = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(replay.replayed);
    assert!(replay.items == committed.items);
    assert_eq!(
        fixture.server.received_requests().await.unwrap().len(),
        requests
    );
}
#[path = "support/stage7_management.rs"]
pub mod base;
