#[path = "support/stage75_recorded_provider.rs"]
mod historical;
#[path = "support/stage7_management_cli.rs"]
#[allow(dead_code, unused_imports)]
mod support;

use ipfs_s3_gateway::{
    config::Config,
    pinning::management::wire::v3::{self, Projection},
    store::pinning::management::{native::meter::Budget, records},
};
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};

#[tokio::test]
async fn historical_provider_cli_exports_status_and_exact_replay_without_new_authority() {
    let fixture = support::Fixture::new(false).await;
    let request = historical::install(&fixture.db, &fixture.config).await;
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let auth = records::authorize_local(&raw, &fixture.db).await.unwrap();
    let before = fixture.frame().await;
    let plan = records::load_recorded_plan(&fixture.db, &auth, &request, &mut Budget::default())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        &plan.actions()[0],
        records::RecordedResult::Historical(_)
    ));
    let private = fixture.file("historical-private.v3");
    let file = v3::files::reserve(private.to_str().unwrap())
        .await
        .unwrap()
        .into_std()
        .await;
    let mut writer = v3::Writer::new(file, Projection::PrivateRecovery, Budget::default()).unwrap();
    v3::planning::recorded(&fixture.db, &mut writer, &plan, Projection::PrivateRecovery)
        .await
        .unwrap();
    writer.finish().unwrap().sync_all().unwrap();
    let mut reader = v3::Reader::new(
        std::fs::File::open(&private).unwrap(),
        Projection::PrivateRecovery,
        Budget::default(),
    )
    .unwrap();
    let mut source = false;
    let mut original = Vec::new();
    while let Some(event) = reader.next_frame().unwrap() {
        match event {
            v3::Event::Frame(v3::Frame::Row { section, key, .. }) => {
                source = section == v3::Section::Result && key.table == "historical_original"
            }
            v3::Event::Chunk(bytes) if source => original.extend_from_slice(&bytes),
            _ => {}
        }
    }
    assert!(reader.verified());
    let (_, original): (
        ipfs_s3_gateway::store::pinning::management::native::NativeKey,
        ipfs_s3_gateway::store::pinning::management::native::NativeRow,
    ) = serde_json::from_slice(&original).unwrap();
    let row = fixture.db.query_one(sea_orm::Statement::from_string(fixture.db.get_database_backend(), "SELECT binding,native_before,result FROM pin_management_records WHERE plan_id='recorded-provider' AND item_id='retire' AND kind='action'")).await.unwrap().unwrap();
    for column in ["binding", "native_before", "result"] {
        assert_eq!(
            original[column],
            row.try_get::<Option<String>>("", column).unwrap()
        );
    }
    assert!(
        String::from_utf8_lossy(&std::fs::read(&private).unwrap())
            .contains(support::PRIVATE_METADATA)
    );
    let exported = fixture.file("historical-public.v3");
    support::report(
        &support::output(&mut fixture.command(&[
            "--pinning-manage",
            "--export",
            private.to_str().unwrap(),
            "--private-input",
            "--output",
            exported.to_str().unwrap(),
        ]))
        .await,
    );
    assert!(
        !String::from_utf8_lossy(&std::fs::read(&exported).unwrap())
            .contains(support::PRIVATE_METADATA)
    );
    let head = support::document(&exported);
    assert!(
        chrono::DateTime::parse_from_rfc3339(head.expires_at.as_ref().unwrap()).unwrap()
            < chrono::Utc::now()
    );
    let request_file = fixture.file("history-request.json");
    support::write_json(&request_file, &request);
    let recovered = fixture.file("history-recovered.v3");
    let report = support::report(
        &support::output(&mut fixture.command(&[
            "--pinning-manage",
            request_file.to_str().unwrap(),
            "--output",
            recovered.to_str().unwrap(),
        ]))
        .await,
    );
    assert_eq!(report["database_writes"], false);
    assert_eq!(
        support::report(&fixture.import("--status", &recovered, false).await)["items"][0]["state"],
        "accepted"
    );
    let imported = fixture.file("historical-imported.v3");
    support::report(
        &support::output(&mut fixture.command(&[
            "--pinning-manage",
            "--import",
            exported.to_str().unwrap(),
            "--output",
            imported.to_str().unwrap(),
        ]))
        .await,
    );
    let mut configuration: toml::Value =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    configuration["pinning"]["providers"][0]["max_pins"] = 47.into();
    std::fs::write(
        &fixture.config_path,
        toml::to_string(&configuration).unwrap(),
    )
    .unwrap();
    assert!(fixture.frame().await == before);
    let route = fixture
        .db
        .query_one(Statement::from_string(
            fixture.db.get_database_backend(),
            "SELECT provider,snapshot FROM pin_provider_routes ORDER BY provider LIMIT 1",
        ))
        .await
        .unwrap()
        .unwrap();
    let allocation: String = route.try_get("", "provider").unwrap();
    let snapshot: String = route.try_get("", "snapshot").unwrap();
    let tx = fixture.db.begin().await.unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "INSERT INTO remote_pins(provider,cid,request_id,cid_size,status,epoch,failure_attempts,last_touched_at) VALUES($1,$2,NULL,10,'pinning',1,0,CURRENT_TIMESTAMP)",
        [allocation.clone().into(), fixture.identity.cid.clone().into()],
    ))
    .await
    .unwrap();
    tx.execute(Statement::from_sql_and_values(
        tx.get_database_backend(),
        "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES($1,$2,$3,'unknown','unknown')",
        [allocation.into(), fixture.identity.cid.clone().into(), snapshot.into()],
    ))
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let before_status = fixture.frame().await;
    let status = fixture.import("--status", &imported, false).await;
    let status = support::report(&status);
    assert_eq!(status["items"][0]["state"], "needs_attention");
    let replay = fixture.import("--apply", &imported, false).await;
    let replay = support::report(&replay);
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["items"][0]["state"], "accepted");
    assert_ne!(replay["items"], status["items"]);
    let run = fixture.import("--apply", &imported, true).await;
    assert!(!run.status.success());
    let report: serde_json::Value = serde_json::from_slice(&run.stdout).unwrap();
    assert_eq!(report["run_started"], false);
    assert_eq!(report["items"], replay["items"]);
    let altered = fixture.file("altered.v3");
    support::rewrite(
        &imported,
        &altered,
        |event, _| {
            if let v3::Event::Frame(v3::Frame::Header { expires_at, .. }) = event {
                *expires_at = Some((chrono::Utc::now() + chrono::Duration::days(1)).to_rfc3339());
            }
        },
        |_| {},
    );
    assert!(
        !fixture
            .import("--apply", &altered, false)
            .await
            .status
            .success()
    );
    assert!(fixture.frame().await == before_status);
    assert_eq!(fixture.remote_calls().await, 0);
}
