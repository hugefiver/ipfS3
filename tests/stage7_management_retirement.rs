#[path = "support/stage7_management.rs"]
pub mod support;
use ipfs_s3_gateway::{
    pinning::management::{db_snapshot, *},
    store::pinning::management::{actions, native, records},
};
use sea_orm::{ConnectionTrait, TransactionTrait};

#[tokio::test]
async fn retirement_record_is_typed_transactional_and_keeps_unknown_native_responsibilities() {
    let raw = support::config("http://127.0.0.1:1");
    let config = support::management_config(&raw);
    let (_directory, db) = support::database().await;
    support::setup(&db, &config).await;
    db.execute_unprepared("INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES('unknown-old-alias','QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE','unknown','unknown')").await.unwrap();
    let provider = config
        .pinning
        .providers
        .iter()
        .find(|provider| provider.identity.provider_id == "fixture-provider")
        .unwrap();
    let route = provider.route_snapshot();
    let request = PlanRequest {
        plan_id: "retire".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(route.clone()),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    assert!(snapshot.inventories[0].complete);
    assert!(!snapshot.inventories[0].attribution_complete);
    let plan = dry_run(&request, &snapshot).unwrap();
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    let binding = records::confirm(
        &db,
        &config,
        &auth,
        (&request, &snapshot, &plan),
        vec![],
        chrono::Duration::minutes(2),
    )
    .await
    .unwrap();
    let allocation = provider.identity.allocation_key();
    let tx = db.begin().await.unwrap();
    records::revalidate_confirmed_in_transaction(&tx, &config, &auth, &binding)
        .await
        .unwrap();
    for disposition in [
        records::Disposition::ControlledCleanup,
        records::Disposition::Migrate,
    ] {
        assert!(
            records::write_retirement_in_transaction(
                &tx,
                &auth,
                &binding,
                "provider",
                &allocation,
                disposition,
                vec![]
            )
            .await
            .is_err()
        );
        assert!(
            native::capture(&tx)
                .await
                .unwrap()
                .table("pin_management_retirements")
                .unwrap()
                .rows
                .is_empty()
        );
    }
    let record = records::write_retirement_in_transaction(
        &tx,
        &auth,
        &binding,
        "provider",
        &allocation,
        records::Disposition::Retain,
        vec![],
    )
    .await
    .unwrap();
    assert_eq!(record.state, records::RetirementState::NeedsAttention);
    assert!(
        record
            .inventory
            .original_value(
                "remote_pin_ledger",
                &["unknown-old-alias", support::CID],
                "provider"
            )
            .is_some()
    );
    tx.rollback().await.unwrap();
    assert!(
        records::read_retirement(&db, &auth, &allocation)
            .await
            .unwrap()
            .is_none()
    );
    actions::apply(&db, &config, &auth, &binding).await.unwrap();
    let stored = records::read_retirement(&db, &auth, &allocation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, record.state);
    assert!(stored.binding == record.binding);
    assert!(stored.inventory == record.inventory);
    assert!(stored.created_at >= record.created_at);
    let frame = native::capture(&db).await.unwrap();
    assert!(
        frame
            .row("remote_pin_ledger", &["unknown-old-alias", support::CID])
            .is_some()
    );
    let current = db_snapshot::collect_with_config(&db, &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    assert!(
        !current
            .routes
            .iter()
            .find(|snapshot_route| snapshot_route.identity.original.as_ref() == Some(&route))
            .is_some_and(|route| route.allocation_allowed)
    );
    db.execute_unprepared("UPDATE pin_management_retirements SET disposition='controlled_cleanup'")
        .await
        .unwrap();
    assert!(
        records::read_retirement(&db, &auth, &allocation)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn retained_history_with_unknown_counts_stays_locatable_and_needs_attention() {
    let raw = support::config("http://127.0.0.1:1");
    let config = support::management_config(&raw);
    let (_directory, db) = support::database().await;
    support::setup(&db, &config).await;
    let provider = &config.pinning.providers[0];
    let allocation = provider.identity.allocation_key();
    let ledger = ipfs_s3_gateway::store::entities::remote_pin_ledger::Model {
        provider: allocation.clone(),
        cid: support::CID.into(),
        route: Some(serde_json::to_string(&provider.route_snapshot()).unwrap()),
        ownership: "external_existing".into(),
        effect: "retained".into(),
        first_observed_at: None,
        last_observed_at: None,
        remote_pinned_at: None,
        gateway_verified_at: None,
        content_verified_at: None,
        first_error: None,
        last_error: None,
    };
    let history=serde_json::json!({"ledger":ledger,"request_id":"actual-historical-resource","confirmed_absent":false}).to_string();
    db.execute(sea_orm::Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Sqlite,
        "INSERT INTO pin_resource_history(provider,cid,epoch,ledger) VALUES($1,$2,7,$3)",
        [
            allocation.clone().into(),
            support::CID.into(),
            history.into(),
        ],
    ))
    .await
    .unwrap();
    let request = PlanRequest {
        plan_id: "history-retire".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(provider.route_snapshot()),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let old = &snapshot.historical_remotes[0];
    assert!(old.retained);
    assert!(old.accounted_bytes.is_none());
    assert!(old.inflight_operations.is_none());
    assert!(old.application_references.is_none());
    let plan = dry_run(&request, &snapshot).unwrap();
    let Some(PlannedStep::Retire(step)) = &plan.items()[0].step else {
        panic!("retirement plan")
    };
    assert!(step.needs_attention);
    assert_eq!(step.unknown, 1);
    assert_eq!(plan.summary().unknown_resources, 1);
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    let binding = records::confirm(
        &db,
        &config,
        &auth,
        (&request, &snapshot, &plan),
        vec![],
        chrono::Duration::minutes(2),
    )
    .await
    .unwrap();
    let before = native::capture(&db).await.unwrap();
    actions::apply(&db, &config, &auth, &binding).await.unwrap();
    let record = records::read_retirement(&db, &auth, &allocation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.state, records::RetirementState::NeedsAttention);
    assert!(
        record
            .unresolved
            .iter()
            .any(|locator| locator.contains("actual-historical-resource"))
    );
    let after = native::capture(&db).await.unwrap();
    assert!(before.table("pin_resource_history") == after.table("pin_resource_history"));
    assert_eq!(
        before.original_value(
            "pin_resource_history",
            &[&allocation, support::CID, "7"],
            "ledger"
        ),
        after.original_value(
            "pin_resource_history",
            &[&allocation, support::CID, "7"],
            "ledger"
        )
    );
    assert_eq!(
        records::read_retirement(&db, &auth, &allocation)
            .await
            .unwrap()
            .unwrap()
            .state,
        records::RetirementState::NeedsAttention
    );
    db.execute_unprepared("UPDATE pin_management_retirements SET state='retired'")
        .await
        .unwrap();
    assert!(
        records::read_retirement(&db, &auth, &allocation)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn retirement_private_material_survives_db_roundtrip_but_never_ordinary_wire() {
    use ipfs_s3_gateway::{
        kubo::KuboClient,
        pinning::management::{evidence, wire},
    };
    use tokio_util::sync::CancellationToken;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::path};
    let server = MockServer::start().await;
    Mock::given(path("/api/v0/id"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"ID":support::NODE})),
        )
        .mount(&server)
        .await;
    Mock::given(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"0123456789"))
        .mount(&server)
        .await;
    let raw = support::config(&server.uri());
    let config = support::management_config(&raw);
    let (_directory, db) = support::database().await;
    support::setup(&db, &config).await;
    let identity = support::publish(&db, &config, "private", false).await;
    db.execute_unprepared("UPDATE objects SET metadata='{\"private\":\"RETIREMENT_METADATA_MARKER\"}',key_wrap='RETIREMENT_KEYWRAP_MARKER'").await.unwrap();
    let auth = records::authorize_local(&raw, &db).await.unwrap();
    evidence::prepare(
        &db,
        &config,
        &auth,
        &KuboClient::new(server.uri()),
        &evidence::PrepareRequest {
            record_id: "private-proof".into(),
            identity,
            expected_node: support::NODE.into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(5),
            directory: false,
        },
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let provider = &config.pinning.providers[0];
    let allocation = provider.identity.allocation_key();
    let request = PlanRequest {
        plan_id: "private-retire".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            action: Action::Retire,
            subject: Subject::Provider {
                route: RouteIdentity::from_original(provider.route_snapshot()),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&db, &config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = dry_run(&request, &snapshot).unwrap();
    let binding = records::confirm(
        &db,
        &config,
        &auth,
        (&request, &snapshot, &plan),
        vec![],
        chrono::Duration::minutes(2),
    )
    .await
    .unwrap();
    let applied = actions::apply(&db, &config, &auth, &binding).await.unwrap();
    let stored = records::read_retirement(&db, &auth, &allocation)
        .await
        .unwrap()
        .unwrap();
    assert!(
        records::lookup_applied(&db, &auth, &binding, "provider")
            .await
            .unwrap()
            .unwrap()
            == applied.items[0].receipt
    );
    let frame = native::capture(&db).await.unwrap();
    for column in ["binding", "inventory"] {
        assert!(
            native::value(
                frame
                    .row("pin_management_retirements", &[&allocation])
                    .unwrap(),
                column
            )
            .is_none()
        );
        assert!(
            frame
                .original_value("pin_management_retirements", &[&allocation], column)
                .is_some()
        );
    }
    let mut originals = 0;
    stored
        .inventory
        .visit_original_cells(|table, _, column, value| {
            if table == "objects" {
                match column {
                    "metadata" => {
                        assert_eq!(value, Some("{\"private\":\"RETIREMENT_METADATA_MARKER\"}"));
                        originals += 1;
                    }
                    "key_wrap" => {
                        assert_eq!(value, Some("RETIREMENT_KEYWRAP_MARKER"));
                        originals += 1;
                    }
                    _ => {}
                }
            }
        });
    assert_eq!(originals, 2);
    let mut writer = wire::v3::Writer::new(
        Vec::new(),
        wire::v3::Projection::Review,
        wire::v3::files::stream::budget(),
    )
    .unwrap();
    wire::v3::planning::confirmed(&db, &mut writer, &binding, wire::v3::Projection::Review)
        .await
        .unwrap();
    let bytes = writer.finish().unwrap();
    let text = String::from_utf8_lossy(&bytes);
    assert!(!text.contains("RETIREMENT_METADATA_MARKER"));
    assert!(!text.contains("RETIREMENT_KEYWRAP_MARKER"));
    assert!(!text.contains(&server.uri()));
    wire::v3::document::inspect(
        std::io::Cursor::new(bytes),
        wire::v3::Projection::Review,
        wire::v3::files::stream::budget(),
    )
    .unwrap();
    db.execute_unprepared("UPDATE pin_management_retirements SET inventory=inventory||' '")
        .await
        .unwrap();
    let changed = native::capture(&db).await.unwrap();
    assert!(frame != changed);
    assert!(serde_json::to_vec(&frame).unwrap() == serde_json::to_vec(&changed).unwrap());
}
