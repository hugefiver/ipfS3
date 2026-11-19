#[path = "support/pin_rows.rs"]
mod pin_rows;
mod stage9_zip_support;

use ipfs_s3_gateway::store;
use sea_orm::{ConnectionTrait, Statement};
use stage9_zip_support::{
    OUTPUT_CID, config, fixture_cid, object, prepared_with_outputs, publish, setup,
};

#[tokio::test]
async fn unknown_submission_keeps_new_refs_and_does_not_block_unrelated_output() {
    let db = setup().await;
    let config = config();
    let other = fixture_cid("other");
    let (request, batch, capture, guard) = prepared_with_outputs(
        &db,
        &config,
        vec![],
        vec![
            ("a".into(), object("a", "out/a", OUTPUT_CID)),
            ("b".into(), object("b", "out/b", &other)),
        ],
    )
    .await;
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        "INSERT INTO pin_resources(provider,cid,logical_size,status,action,claim_token,attempt) VALUES ('remote',$1,5,'unknown','submit','prior-attempt',1)",
        [OUTPUT_CID.into()],
    )).await.unwrap();
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();

    let refs = pin_rows::refs(&db).await;
    assert_eq!(refs.len(), 2);
    assert_eq!(
        (refs[0].owner_object_id.as_str(), refs[0].cid.as_str()),
        ("a", OUTPUT_CID)
    );
    assert_eq!(
        (refs[1].owner_object_id.as_str(), refs[1].cid.as_str()),
        ("b", other.as_str())
    );
    let resources = pin_rows::resources(&db).await;
    assert_eq!(resources.len(), 2);
    assert!(
        resources
            .iter()
            .any(|r| r.cid == OUTPUT_CID && r.status == "unknown")
    );
    assert!(
        resources.iter().any(|r| r.cid == other
            && r.status == "waiting"
            && r.action.as_deref() == Some("submit"))
    );
    let now = chrono::Utc::now();
    let claim = store::pinning::resources::claim(
        &db,
        "remote",
        &config.provider_limits["remote"],
        now,
        now + chrono::TimeDelta::minutes(1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(claim.cid, other);
    let snapshot = store::zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert!(
        snapshot
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_some())
    );
}
