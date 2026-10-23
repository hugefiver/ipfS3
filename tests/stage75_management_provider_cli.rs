#[path = "support/stage75_storage17.rs"]
mod storage17;
#[path = "support/stage7_management_cli.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::pinning::management::{
    Action, ItemRequest, PlanRequest, RetirementSelection, RouteIdentity, Subject,
};

#[tokio::test]
async fn provider_cli_confirms_complete_inventory_and_restores_immutable_replay() {
    let mut fixture = support::Fixture::new(false).await;
    fixture.request = PlanRequest {
        plan_id: "provider-cli".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture.config.pinning.providers[0].route_snapshot(),
                ),
            },
            action: Action::RetireWith {
                selection: RetirementSelection::Retain,
            },
        }],
    };
    let before = fixture.frame().await;
    let request = fixture.request_file();
    support::report(
        &support::output(&mut fixture.command(&[
            "--pinning-manage",
            request.to_str().unwrap(),
            "--source-node",
            support::NODE,
        ]))
        .await,
    );
    assert!(fixture.frame().await == before);
    let document = fixture.file("provider.v3");
    let confirmed = fixture.confirm(&document, "30", false).await;
    support::report(&confirmed);
    fixture.assert_redacted(&confirmed);
    let before = fixture.frame().await;
    let status = fixture.import("--status", &document, false).await;
    assert_eq!(support::report(&status)["items"][0]["state"], "not_applied");
    assert!(fixture.frame().await == before);
    let applied = support::report(&fixture.import("--apply", &document, false).await);
    assert_eq!(applied["replayed"], false);
    let status = fixture.import("--status", &document, false).await;
    assert_ne!(support::report(&status)["items"][0]["state"], "not_applied");
    fixture.assert_redacted(&status);
    fixture.wait_for_confirmation_expiry().await;
    let mut config: toml::Value =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let maximum = config["pinning"]["providers"][0]["max_pins"]
        .as_integer()
        .unwrap();
    config["pinning"]["providers"][0]["max_pins"] = (maximum + 1).into();
    std::fs::write(&fixture.config_path, toml::to_string(&config).unwrap()).unwrap();
    let before = fixture.frame().await;
    let replay = support::report(&fixture.import("--apply", &document, false).await);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["items"], applied["items"]);
    assert!(fixture.frame().await == before);
    assert_eq!(fixture.remote_calls().await, 0);

    use ipfs_s3_gateway::{
        config::Config,
        pinning::management::wire::v3::{self, Projection},
        store::{
            initialization,
            pinning::management::{native::meter::Budget, records, schema},
        },
    };
    use sea_orm::{ConnectionTrait, Statement};
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    storage17::restore(&fixture.db).await;
    let changes = fixture
        .db
        .query_one(Statement::from_string(
            fixture.db.get_database_backend(),
            "SELECT total_changes() AS n",
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "n")
        .unwrap();
    assert!(initialization::open(&fixture.db).await.is_err());
    assert_eq!(
        fixture
            .db
            .query_one(Statement::from_string(
                fixture.db.get_database_backend(),
                "SELECT total_changes() AS n"
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get::<i64>("", "n")
            .unwrap(),
        changes
    );
    initialization::upgrade_existing(
        &fixture.db,
        initialization::UpgradeRequest {
            database_scope: auth.database_scope(),
            native_identity: auth.database_identity(),
            source_version: "m20261017_000001_management_storage",
            target_version: schema::VERSION,
        },
    )
    .await
    .unwrap();
    assert!(fixture.frame().await == before);
    let plan =
        records::load_recorded_plan(&fixture.db, &auth, &fixture.request, &mut Budget::default())
            .await
            .unwrap()
            .unwrap();
    assert!(matches!(
        &plan.actions()[0],
        records::RecordedResult::Current(_)
    ));
    let recovered = fixture.file("provider-recorded.v3");
    let file = v3::files::reserve(recovered.to_str().unwrap())
        .await
        .unwrap()
        .into_std()
        .await;
    let mut writer = v3::Writer::new(file, Projection::Review, Budget::default()).unwrap();
    v3::planning::recorded(&fixture.db, &mut writer, &plan, Projection::Review)
        .await
        .unwrap();
    writer.finish().unwrap().sync_all().unwrap();
    assert_eq!(
        support::document(&recovered).expires_at,
        support::document(&document).expires_at
    );
    let replay = support::report(&fixture.import("--apply", &recovered, false).await);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["items"], applied["items"]);
    assert_eq!(replay["business_commit"], "exact_replay_no_new_business");
    let refused_run = fixture.import("--apply", &recovered, true).await;
    assert!(!refused_run.status.success());
    let refused: serde_json::Value = serde_json::from_slice(&refused_run.stdout).unwrap();
    assert_eq!(refused["run_started"], false);
    assert!(fixture.frame().await == before);
    assert_eq!(fixture.remote_calls().await, 0);
}

#[tokio::test]
async fn provider_cli_releases_owned_spools_when_export_or_confirmation_is_rejected() {
    let mut fixture = support::Fixture::new(false).await;
    fixture.request = PlanRequest {
        plan_id: "provider-rejected".into(),
        items: vec![ItemRequest {
            item_id: "provider".into(),
            subject: Subject::Provider {
                route: RouteIdentity::from_original(
                    fixture.config.pinning.providers[0].route_snapshot(),
                ),
            },
            action: Action::RetireWith {
                selection: RetirementSelection::Retain,
            },
        }],
    };
    let directories = || {
        std::fs::read_dir(fixture.directory.path())
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_type().unwrap().is_dir())
            .map(|entry| entry.path())
            .collect::<std::collections::BTreeSet<_>>()
    };
    let owned_before = directories();
    let before = fixture.frame().await;
    let request = fixture.request_file();
    let existing = fixture.file("existing.v3");
    std::fs::write(&existing, b"preserved output").unwrap();
    let rejected = support::output(&mut fixture.command(&[
        "--pinning-manage",
        request.to_str().unwrap(),
        "--output",
        existing.to_str().unwrap(),
    ]))
    .await;
    assert!(!rejected.status.success());
    assert_eq!(std::fs::read(&existing).unwrap(), b"preserved output");
    assert_eq!(
        directories(),
        owned_before,
        "failed export must close and remove only its owned spools"
    );
    let output = fixture.file("unconfirmed.v3");
    let rejected = support::output(&mut fixture.command(&[
        "--pinning-manage",
        request.to_str().unwrap(),
        "--confirm",
        output.to_str().unwrap(),
        "--ack-item",
        "provider",
    ]))
    .await;
    assert!(!rejected.status.success());
    assert_eq!(
        directories(),
        owned_before,
        "rejected confirmation must close its owned spools"
    );
    assert!(fixture.frame().await == before);
    assert_eq!(fixture.remote_calls().await, 0);
}
