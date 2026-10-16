#[path = "support/stage7_management_cli.rs"]
pub mod support;

use ipfs_s3_gateway::{
    config::Config,
    kubo::KuboClient,
    pinning::management::{PlanStatus, db_snapshot, dry_run, evidence, local_config},
    store::pinning::management::{native, records},
};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn explicit_offline_prepare_is_replayable_and_available_to_native_planning() {
    let fixture = support::Fixture::new(false).await;
    let raw: Config =
        toml::from_str(&std::fs::read_to_string(&fixture.config_path).unwrap()).unwrap();
    let config = local_config::compile(&raw, |_| None)
        .unwrap()
        .with_source_node(support::NODE.into())
        .unwrap();
    let before = native::capture(&fixture.db).await.unwrap();
    before.validate().expect("initial native frame");
    let auth = records::authorize_local(&raw, &fixture.db)
        .await
        .expect("local authorization");
    auth.check(&fixture.db)
        .await
        .expect("same-database authorization");
    let request = evidence::PrepareRequest {
        record_id: "explicit-cli-proof".into(),
        identity: fixture.identity.clone(),
        expected_node: support::NODE.into(),
        max_stored_bytes: 10,
        ttl: chrono::Duration::hours(1),
        directory: false,
    };
    let proof = evidence::prepare(
        &fixture.db,
        &config,
        &auth,
        &KuboClient::new(raw.kubo.rpc_url.clone()),
        &request,
        CancellationToken::new(),
    )
    .await
    .expect("prepare API");
    assert_eq!(proof.proof, evidence::ReadProof::FileCleanEof);
    assert_eq!(proof.stored_bytes, Some(10));
    let prepared = native::capture(&fixture.db).await.unwrap();
    prepared.validate().expect("prepared native frame");
    assert!(before.evidence_inventory() == prepared.evidence_inventory());

    evidence::prepare(
        &fixture.db,
        &config,
        &auth,
        &KuboClient::new(raw.kubo.rpc_url),
        &request,
        CancellationToken::new(),
    )
    .await
    .expect("prepare API replay");
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &config, &fixture.request)
        .await
        .expect("native snapshot collection")
        .planning_snapshot()
        .expect("native planning projection");
    let plan = dry_run(&fixture.request, &snapshot).expect("native dry run");
    assert_eq!(plan.items()[0].status, PlanStatus::Ready);

    let calls_before_cli = fixture.remote_calls().await;
    let input = fixture.prepare_file(false);
    let args = ["--pinning-manage", "--prepare", input.to_str().unwrap()];
    let output = support::output(&mut fixture.command(&args)).await;
    fixture.assert_redacted(&output);
    assert_eq!(support::report(&output)["stored_bytes"], 10);
    assert_eq!(fixture.remote_calls().await, calls_before_cli + 1);
    assert!(prepared == native::capture(&fixture.db).await.unwrap());
}
