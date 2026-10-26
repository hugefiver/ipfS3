use super::*;
use crate::pinning::identity::CleanupMode;
use sea_orm::{ColumnTrait, DatabaseConnection, QueryFilter};

mod postgres {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/postgres_pinning_stage3.rs"
    ));
}

fn route(profile: &str) -> ProviderRouteSnapshot {
    ProviderRouteSnapshot {
        provider_id: "native-test".into(),
        backend: "noop".into(),
        scope: "native-test".into(),
        storage_domain: "remote".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: profile.into(),
        strategy: "cid".into(),
        cleanup: CleanupMode::Retain,
        confirmation: None,
    }
}

fn content(index: u64) -> String {
    let mut digest = [0u8; 32];
    digest[..8].copy_from_slice(&index.to_le_bytes());
    cid::Cid::new_v1(
        0x55,
        cid::multihash::Multihash::<64>::wrap(0x12, &digest).unwrap(),
    )
    .to_string()
}

async fn execute(db: &DatabaseConnection, sql: &str, values: Vec<sea_orm::Value>) {
    db.execute(Statement::from_sql_and_values(
        db.get_database_backend(),
        sql,
        values,
    ))
    .await
    .unwrap();
}

async fn seed(db: &DatabaseConnection, birth: &BirthContract, now: DateTime<Utc>) {
    execute(
        db,
        "INSERT INTO buckets(name) VALUES ($1)",
        vec!["native-batch".into()],
    )
    .await;
    lease(db, "native-lease", now).await;
    lease(db, "unrelated-lease", now).await;
    insert_contract(
        db,
        "native-lease",
        serde_json::to_value(birth).unwrap(),
        now,
    )
    .await;
}

async fn lease(db: &DatabaseConnection, lease: &str, now: DateTime<Utc>) {
    execute(
        db,
        "INSERT INTO objects(id,bucket,key,cid,size,etag) VALUES ($1,'native-batch',$1,$2,1,$2)",
        vec![lease.into(), content(0).into()],
    )
    .await;
    execute(db, "INSERT INTO pin_leases(id,owner_object_id,source,policy_id,provider_mode,content_mode,generation,state,created_at,last_touched_at,expires_at) VALUES ($1,$1,'automatic','policy','all','decompressed',1,'active',$2,$2,$3)", vec![lease.into(), now.into(), (now+chrono::Duration::hours(1)).into()]).await;
}

async fn insert_contract(
    db: &DatabaseConnection,
    lease: &str,
    contract: serde_json::Value,
    now: DateTime<Utc>,
) {
    execute(db, "INSERT INTO pin_lease_requirements(lease_id,owner_object_id,contract,created_at) VALUES ($1,$1,$2,$3)", vec![lease.into(), contract.to_string().into(), now.into()]).await;
}

async fn target(
    db: &DatabaseConnection,
    lease: &str,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
) {
    execute(db, "INSERT INTO pin_lease_targets(id,lease_id,cid,logical_size,provider,state,created_at,last_touched_at) VALUES ($1,$2,$3,1,$4,'pinned',$5,$5)", vec![uuid::Uuid::new_v4().to_string().into(), lease.into(), cid.into(), provider.into(), now.into()]).await;
}

async fn remote(
    db: &DatabaseConnection,
    provider: &str,
    cid: &str,
    status: &str,
    route: &ProviderRouteSnapshot,
    now: DateTime<Utc>,
) {
    execute(db, "INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,last_touched_at) VALUES ($1,$2,1,$3,1,$4)", vec![provider.into(), cid.into(), status.into(), now.into()]).await;
    execute(db, "INSERT INTO remote_pin_ledger(provider,cid,route,ownership,effect) VALUES ($1,$2,$3,'unknown','confirmed')", vec![provider.into(), cid.into(), serde_json::to_string(route).unwrap().into()]).await;
}

async fn copy_contract(
    db: &DatabaseConnection,
    id: &str,
    contract: serde_json::Value,
    now: DateTime<Utc>,
) {
    lease(db, id, now).await;
    insert_contract(db, id, contract, now).await;
    for row in pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq("native-lease"))
        .filter(pin_lease_target::Column::State.eq("pinned"))
        .all(db)
        .await
        .unwrap()
    {
        target(db, id, &row.provider, &row.cid, now).await;
    }
}

fn birth(cids: impl Iterator<Item = String>, count: usize, profile: &str) -> BirthContract {
    let candidate_order = (0..count)
        .map(|index| format!("candidate-{index:03}"))
        .collect::<Vec<_>>();
    BirthContract {
        provider_mode: "all".into(),
        content_mode: "decompressed".into(),
        content_cids: cids.collect(),
        candidates: candidate_order
            .iter()
            .map(|slot| (slot.clone(), Some(route(profile))))
            .collect(),
        candidate_order,
    }
}

async fn mixed_pages(db: DatabaseConnection) {
    let now = crate::store::database_clock::database_now(&db)
        .await
        .unwrap();
    let mut birth = birth((0..90).map(content), 3, "noop");
    seed(&db, &birth, now).await;
    let slots = &birth.candidate_order;
    let route = route("noop");
    for cid in &birth.content_cids {
        remote(&db, &slots[0], cid, "pinned", &route, now).await;
        target(&db, "native-lease", &slots[0], cid, now).await;
        remote(&db, &slots[1], cid, "pinned", &route, now).await;
        target(&db, "unrelated-lease", &slots[1], cid, now).await;
        remote(&db, &slots[2], cid, "pinning", &route, now).await;
        target(&db, "native-lease", &slots[2], cid, now).await;
        remote(&db, "unrelated-provider", cid, "pinned", &route, now).await;
        target(&db, "native-lease", "unrelated-provider", cid, now).await;
    }
    let outside = content(1000);
    for slot in slots {
        remote(&db, slot, &outside, "pinned", &route, now).await;
        target(&db, "native-lease", slot, &outside, now).await;
    }
    assert_eq!(
        completion(&db, "native-lease", now).await.unwrap(),
        Some((true, false, 90))
    );
    birth.provider_mode = "one".into();
    copy_contract(&db, "one-lease", serde_json::to_value(&birth).unwrap(), now).await;
    assert_eq!(
        completion(&db, "one-lease", now).await.unwrap(),
        Some((true, true, 90))
    );

    // Combination 255 is this CID's sole valid candidate; its remaining
    // candidates are in the next page and cannot reset its availability.
    let boundary = birth.content_cids.iter().nth(85).unwrap();
    execute(&db, "UPDATE pin_lease_targets SET state='waiting' WHERE lease_id IN ('native-lease','one-lease') AND provider=$1 AND cid=$2", vec![birth.candidate_order[0].clone().into(), boundary.clone().into()]).await;
    assert_eq!(
        completion(&db, "one-lease", now).await.unwrap(),
        Some((false, false, 89))
    );
    execute(&db, "UPDATE pin_lease_targets SET state='pinned' WHERE lease_id IN ('native-lease','one-lease') AND provider=$1 AND cid=$2", vec![birth.candidate_order[0].clone().into(), boundary.clone().into()]).await;
    for cid in &birth.content_cids {
        target(&db, "native-lease", &birth.candidate_order[1], cid, now).await;
    }
    execute(
        &db,
        "UPDATE remote_pins SET status='pinned' WHERE provider=$1",
        vec![birth.candidate_order[2].clone().into()],
    )
    .await;
    assert_eq!(
        completion(&db, "native-lease", now).await.unwrap(),
        Some((true, true, 270))
    );
}

#[tokio::test]
async fn mixed_confirmations_preserve_cid_availability_across_pages() {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    mixed_pages(db).await;
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_mixed_confirmations_preserve_cid_availability_across_pages() {
    postgres::isolated(true, mixed_pages).await;
}

#[tokio::test]
async fn candidate_pages_do_not_skip_later_confirmation_errors() {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    let now = crate::store::database_clock::database_now(&db)
        .await
        .unwrap();
    let mut birth = birth(std::iter::once(content(0)), 257, "noop");
    birth.provider_mode = "one".into();
    seed(&db, &birth, now).await;
    let cid = birth.content_cids.first().unwrap();
    for slot in [&birth.candidate_order[0], &birth.candidate_order[256]] {
        remote(&db, slot, cid, "pinned", &route("noop"), now).await;
        target(&db, "native-lease", slot, cid, now).await;
    }
    assert_eq!(
        completion(&db, "native-lease", now).await.unwrap(),
        Some((true, true, 2))
    );
    // Leave only the last candidate eligible, beyond the first page of misses.
    execute(
        &db,
        "UPDATE pin_lease_targets SET state='waiting' WHERE provider=$1",
        vec![birth.candidate_order[0].clone().into()],
    )
    .await;
    assert_eq!(
        completion(&db, "native-lease", now).await.unwrap(),
        Some((true, true, 1))
    );
    let mut invalid = serde_json::to_value(&birth).unwrap();
    invalid["management_requirements"] = serde_json::json!({"levels":"invalid"});
    copy_contract(&db, "invalid-lease", invalid, now).await;
    assert!(completion(&db, "invalid-lease", now).await.is_err());
}

#[tokio::test]
async fn canonical_aliases_count_logical_combinations_but_non_rpc_keys_stay_raw() {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    let now = crate::store::database_clock::database_now(&db)
        .await
        .unwrap();
    let v0 = "QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE";
    let canonical = canonical_resource_cid(v0).unwrap();
    let mut birth = birth([v0.to_owned(), canonical.clone()].into_iter(), 2, "rpc");
    birth
        .candidates
        .insert(birth.candidate_order[1].clone(), Some(route("noop")));
    seed(&db, &birth, now).await;
    for (slot, profile) in birth.candidate_order.iter().zip(["rpc", "noop"]) {
        remote(&db, slot, &canonical, "pinned", &route(profile), now).await;
        target(&db, "native-lease", slot, &canonical, now).await;
    }
    assert_eq!(
        completion(&db, "native-lease", now).await.unwrap(),
        Some((true, false, 3))
    );
    birth.provider_mode = "one".into();
    copy_contract(&db, "one-lease", serde_json::to_value(&birth).unwrap(), now).await;
    assert_eq!(
        completion(&db, "one-lease", now).await.unwrap(),
        Some((true, true, 3))
    );
    let resolved = candidates(ReceiptCatalog::full(&db), "native-lease", &birth)
        .await
        .unwrap();
    let combinations = batch::combinations(&birth, &resolved).collect::<Vec<_>>();
    assert_eq!(combinations.len(), 4);
    let page = batch::Page::load(&db, "native-lease", &combinations)
        .await
        .unwrap();
    assert_eq!(
        combinations
            .iter()
            .filter(|combo| page.eligible(combo))
            .count(),
        3
    );
}

#[tokio::test]
async fn cluster_pinning_and_invalid_cid_fallback_keep_original_query_semantics() {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    crate::store::run_migrations(&db).await.unwrap();
    let now = crate::store::database_clock::database_now(&db)
        .await
        .unwrap();
    let mut birth = birth(std::iter::once("invalid-cid".into()), 3, "rpc");
    birth
        .candidates
        .insert(birth.candidate_order[1].clone(), Some(route("cluster")));
    birth
        .candidates
        .insert(birth.candidate_order[2].clone(), None);
    seed(&db, &birth, now).await;
    for slot in &birth.candidate_order {
        remote(&db, slot, "invalid-cid", "pinning", &route("rpc"), now).await;
        target(&db, "native-lease", slot, "invalid-cid", now).await;
    }
    let resolved = candidates(ReceiptCatalog::full(&db), "native-lease", &birth)
        .await
        .unwrap();
    let combinations = batch::combinations(&birth, &resolved).collect::<Vec<_>>();
    assert!(
        combinations
            .iter()
            .all(|combo| combo.query_cid == "invalid-cid")
    );
    let page = batch::Page::load(&db, "native-lease", &combinations)
        .await
        .unwrap();
    assert!(!page.eligible(&combinations[0]));
    assert!(page.eligible(&combinations[1]));
    assert!(!page.eligible(&combinations[2]));
    // Eligibility is not confirmation: a mismatched cluster birth route cannot
    // inherit the RPC ledger's confirmed effect.
    assert_eq!(
        completion(&db, "native-lease", now).await.unwrap(),
        Some((false, false, 0))
    );
}
