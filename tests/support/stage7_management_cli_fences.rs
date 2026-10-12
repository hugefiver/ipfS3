use super::support;
use ipfs_s3_gateway::{pinning::management::*, store::{self, entities::pin_job}};
use sea_orm::{ConnectionTrait, EntityTrait, Statement};
use serde_json::{Value, json};

#[tokio::test]
async fn full_document_scope_object_cid_ack_and_provider_field_tampering_never_writes() {
    let fixture = support::Fixture::new(true).await;
    fixture.prepare().await;
    let original = fixture.file("original-confirmed.json");
    support::report(&fixture.confirm(&original, "300", true).await);
    let document: Value = serde_json::from_slice(&std::fs::read(&original).unwrap()).unwrap();
    let pointers = [
        ("/request/plan_id", json!("another-plan")),
        ("/request/items/0/subject/object/identity/object_id", json!("replacement-object")),
        ("/request/items/0/subject/object/identity/cid", json!("bafkqaaa")),
        ("/request/items/0/subject/object/target/new/credential_revision", json!(99)),
        ("/request/items/0/subject/object/target/new/original/cleanup", json!("managed")),
        ("/confirmation/candidate_item_ids", json!([])),
        ("/confirmation/native_frame/database_scope", json!("foreign-scope")),
        ("/confirmation/native_frame/database_identity", json!("foreign-native-db")),
    ];
    let before = fixture.frame().await;
    let calls = fixture.remote_calls().await;
    for (index, (pointer, replacement)) in pointers.into_iter().enumerate() {
        let mut forged = document.clone();
        *forged.pointer_mut(pointer).expect("fixture wire field must actually exist") = replacement;
        let file = fixture.file(&format!("tampered-{index}.json"));
        support::write_json(&file, &forged);
        for mode in ["--apply", "--status"] {
            let result = fixture.import(mode, &file, false).await;
            assert!(!result.status.success(), "tampered binding was accepted: {pointer}");
            fixture.assert_redacted(&result);
            assert!(fixture.frame().await == before);
        }
    }
    let foreign = support::Fixture::new(false).await;
    let foreign_before = foreign.frame().await;
    assert!(!foreign.import("--apply", &original, false).await.status.success());
    assert!(foreign.frame().await == foreign_before);
    assert_eq!(fixture.remote_calls().await, calls);
}

#[tokio::test]
async fn untrusted_applied_claims_and_derived_points_do_not_authorize_business_or_jobs() {
    let fixture = support::Fixture::new(false).await;
    fixture.prepare().await;
    let original = fixture.file("confirmed.json");
    support::report(&fixture.confirm(&original, "300", false).await);
    let document: Value = serde_json::from_slice(&std::fs::read(&original).unwrap()).unwrap();
    let mut claims = document.clone();
    claims["snapshot"]["applied"] = json!([{"binding": document["plan"]["items"][0]}]);
    let mut points = document.clone();
    points["migration_points"] = json!([{"plan_id":fixture.request.plan_id,"item_id":"object","job_id":"untrusted-queue-id"}]);
    let before = fixture.frame().await;
    for (index, document) in [claims, points].into_iter().enumerate() {
        let file = fixture.file(&format!("untrusted-{index}.json"));
        support::write_json(&file, &document);
        assert!(!fixture.import("--apply", &file, true).await.status.success());
        assert!(fixture.frame().await == before);
        assert!(pin_job::Entity::find().all(&fixture.db).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_consistently_reencoded_public_plan_still_cannot_replace_the_trusted_private_proposal() {
    let fixture = support::Fixture::new(false).await;
    fixture.prepare().await;
    let original = fixture.file("confirmed.json");
    support::report(&fixture.confirm(&original, "300", false).await);
    let imported = wire::decode_plan(&std::fs::read(&original).unwrap()).unwrap();
    let mut snapshot = imported.snapshot_without_applied().clone();
    snapshot.objects[0].logical_bytes += 1;
    let plan = dry_run(imported.request(), &snapshot).unwrap();
    let reviewed = plan.confirmation(Vec::new());
    let bytes = wire::encode_plan(imported.request(), &snapshot, &plan, Some(&reviewed)).unwrap();
    assert!(wire::decode_plan(&bytes).is_ok(), "attack must pass the unauthenticated wire-consistency boundary");
    let forged = fixture.file("consistent-but-unauthorized.json");
    std::fs::write(&forged, bytes).unwrap();
    let before = fixture.frame().await;
    for mode in ["--apply", "--status"] {
        assert!(!fixture.import(mode, &forged, false).await.status.success());
        assert!(fixture.frame().await == before);
    }
}

#[tokio::test]
async fn local_authorization_failure_and_private_principal_drift_do_not_touch_existing_s3_objects() {
    let fixture = support::Fixture::new(false).await;
    fixture.prepare().await;
    let original = fixture.file("confirmed.json");
    support::report(&fixture.confirm(&original, "300", false).await);
    let config = std::fs::read_to_string(&fixture.config_path).unwrap();
    let unauthorized = fixture.file("unauthorized.toml");
    std::fs::write(&unauthorized, config.replace("credentials = [{ access_key = 'cli-admin', secret_key = 'CLI_SECRET_DO_NOT_PRINT' }]", "credentials = []")).unwrap();
    let unauthorized_config: ipfs_s3_gateway::config::Config = toml::from_str(&std::fs::read_to_string(&unauthorized).unwrap()).unwrap();
    assert!(unauthorized_config.auth.credentials.is_empty());
    let before = fixture.frame().await;
    let result = support::output(&mut support::command(&unauthorized, &["--pinning-manage", "--apply", original.to_str().unwrap(), "--run"])).await;
    assert!(!result.status.success());
    assert!(fixture.frame().await == before);
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "UPDATE pin_management_records SET principal=$1 WHERE plan_id=$2 AND kind='action'",
        ["foreign-principal".into(),fixture.request.plan_id.clone().into()])).await.unwrap();
    let before = fixture.frame().await;
    assert!(!fixture.import("--apply", &original, false).await.status.success());
    assert!(fixture.frame().await == before);
    let objects = store::entities::object::Entity::find().all(&fixture.db).await.unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].id, fixture.identity.object_id);
    assert_eq!(objects[0].cid, support::NODE);
}

#[tokio::test]
async fn replacement_private_native_and_real_multipart_part_drift_refuse_the_entire_apply() {
    for drift in ["replacement", "metadata", "part"] {
        let fixture = support::Fixture::new(false).await;
        if drift == "part" {
            store::multipart::create_upload(&fixture.db, "existing-upload", "pending-object", "bucket", "pending-key",
                "none", None, None, None, None, &[], None, false).await.unwrap();
            store::multipart::upsert_part(&fixture.db, "existing-upload", 1, support::NODE, 10, "old-part-etag").await.unwrap();
        }
        fixture.prepare().await;
        let original = fixture.file("confirmed.json");
        support::report(&fixture.confirm(&original, "300", false).await);
        match drift {
            "replacement" => fixture.replace_object().await,
            "metadata" => { fixture.db.execute_unprepared("UPDATE objects SET metadata='{\"private\":\"changed_after_confirmation\"}'").await.unwrap(); }
            "part" => { store::multipart::upsert_part(&fixture.db, "existing-upload", 1, support::NODE, 11, "replaced-part-etag").await.unwrap(); }
            _ => unreachable!(),
        }
        let before = fixture.frame().await;
        let calls = fixture.remote_calls().await;
        let result = fixture.import("--apply", &original, false).await;
        assert!(!result.status.success(), "native drift was accepted: {drift}");
        let stopped: Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(stopped["state_evidence"], "typed_apply_failure");
        assert_eq!(stopped["items"][0]["state"], "stopped");
        assert_eq!(stopped["items"][0]["receipt"], "not_reported");
        assert!(fixture.frame().await == before);
        assert!(pin_job::Entity::find().all(&fixture.db).await.unwrap().is_empty());
        assert_eq!(fixture.remote_calls().await, calls);
        fixture.assert_redacted(&result);
    }
}

#[tokio::test]
async fn damaged_formal_catalog_is_refused_read_only_and_never_repaired() {
    let fixture = support::Fixture::new(false).await;
    fixture.db.execute_unprepared("CREATE INDEX stage7_cli_unexpected_catalog_index ON pin_management_records(plan_id)").await.unwrap();
    fixture.db.clone().close().await.unwrap();
    let before = std::fs::read(&fixture.database).unwrap();
    let request = fixture.request_file();
    let result = support::output(&mut fixture.command(&["--pinning-manage", request.to_str().unwrap()])).await;
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("formal upgrade or repair"));
    assert_eq!(std::fs::read(&fixture.database).unwrap(), before);
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn typed_retirement_choices_are_request_bound_and_never_invent_migration_or_unpin() {
    for (index, selection) in [RetirementSelection::Retain, RetirementSelection::ControlledCleanup {resources: vec![]},
        RetirementSelection::Migrate {targets: vec![]}].into_iter().enumerate() {
        let mut fixture = support::Fixture::new(false).await;
        let route = RouteIdentity::from_original(fixture.config.pinning.providers[0].route_snapshot());
        fixture.request = PlanRequest {plan_id:format!("retire-{index}"), items:vec![ItemRequest {
            item_id:"retire".into(), action:Action::RetireWith {selection:selection.clone()}, subject:Subject::Provider {route},
        }]};
        let file = fixture.file("retirement.json");
        let before = fixture.frame().await;
        let result = fixture.confirm(&file, "300", false).await;
        if matches!(selection, RetirementSelection::Migrate {..}) {
            assert!(!result.status.success());
            assert!(fixture.frame().await == before);
        } else {
            support::report(&result);
            support::report(&fixture.import("--apply", &file, false).await);
            let row = fixture.db.query_one(Statement::from_string(fixture.db.get_database_backend(),
                "SELECT disposition FROM pin_management_retirements")).await.unwrap().unwrap();
            let disposition: String = row.try_get("", "disposition").unwrap();
            assert_eq!(disposition, if matches!(selection, RetirementSelection::Retain) {"retain"} else {"controlled_cleanup"});
            assert!(pin_job::Entity::find().all(&fixture.db).await.unwrap().is_empty());
        }
        assert_eq!(fixture.remote_calls().await, 0);
    }
}
