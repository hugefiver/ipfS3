use super::*;
use crate::store::{
    initialization,
    pinning::management::{native, schema},
};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, Statement};
use sea_orm_migration::{MigrationTrait, MigratorTrait};
use std::collections::BTreeMap;

struct Historical;
impl MigratorTrait for Historical {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        registry::historical()
    }
}

async fn columns(
    db: &DatabaseConnection,
) -> BTreeMap<(String, String), (String, i32, Option<String>, i32)> {
    db.query_all(Statement::from_string(db.get_database_backend(),
        "SELECT s.name AS tbl,p.name,p.type,p.\"notnull\",p.dflt_value,p.pk FROM sqlite_schema s JOIN pragma_table_info(s.name) p WHERE s.type='table' AND s.name NOT IN ('sqlite_sequence','seaql_migrations','_ipfs_s3_migration_lock') ORDER BY s.name,p.name"))
        .await.unwrap().into_iter().map(|row| {
            ((row.try_get("","tbl").unwrap(),row.try_get("","name").unwrap()),
             (row.try_get::<String>("","type").unwrap().to_uppercase(),row.try_get("","notnull").unwrap(),row.try_get("","dflt_value").unwrap(),row.try_get("","pk").unwrap()))
        }).collect()
}

#[tokio::test]
async fn fresh_baseline_matches_historical_final_columns_and_starts() {
    let old = Database::connect("sqlite::memory:").await.unwrap();
    Historical::up(&old, None).await.unwrap();
    let fresh = Database::connect("sqlite::memory:").await.unwrap();
    initialization::open(&fresh).await.unwrap();
    assert_eq!(columns(&fresh).await, columns(&old).await);
    for query in [
        "SELECT json_array(s.name,f.seq,f.\"table\",f.\"from\",f.\"to\",f.on_update,f.on_delete,f.match) AS descriptor FROM sqlite_schema s JOIN pragma_foreign_key_list(s.name) f WHERE s.type='table' ORDER BY descriptor",
        "SELECT json_array(s.name,i.\"unique\",i.origin,i.partial,(SELECT json_group_array(json_array(x.name,x.desc,x.coll)) FROM (SELECT * FROM pragma_index_xinfo(i.name) WHERE key=1 ORDER BY seqno) x)) AS descriptor FROM sqlite_schema s JOIN pragma_index_list(s.name) i WHERE s.type='table' AND s.name NOT IN ('seaql_migrations','_ipfs_s3_migration_lock') ORDER BY descriptor",
        "SELECT json_array(tbl_name,name) AS descriptor FROM sqlite_schema WHERE type='trigger' ORDER BY descriptor",
    ] {
        let mut layouts = Vec::new();
        for db in [&fresh, &old] {
            layouts.push(
                db.query_all(Statement::from_string(db.get_database_backend(), query))
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|row| row.try_get::<String>("", "descriptor").unwrap())
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(layouts[0], layouts[1], "{query}");
    }
    assert_eq!(schema::validate(&fresh).await.unwrap(), [baseline::VERSION]);
    crate::store::bucket::create(&fresh, "baseline", None)
        .await
        .unwrap();
    crate::store::object::upsert(
        &fresh, "object", "baseline", "key", "cid", 3, None, "cid", None, false, None, None, false,
    )
    .await
    .unwrap();
    assert_eq!(
        crate::store::object::get_by_id(&fresh, "object")
            .await
            .unwrap()
            .cid,
        "cid"
    );
}

#[tokio::test]
async fn historical_sources_require_explicit_bound_conversion() {
    for source in [history::M13, history::M17, history::M23] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.sqlite");
        let db = crate::store::connect_database(&format!(
            "sqlite://{}?mode=rwc",
            path.to_string_lossy().replace('\\', "/")
        ))
        .await
        .unwrap();
        Historical::up(&db, Some(history::prefix(source).unwrap().len() as u32))
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO buckets(name,owner) VALUES('preserved','owner')")
            .await
            .unwrap();
        db.execute_unprepared("CREATE INDEX idx_custom_bucket_owner ON buckets(owner)")
            .await
            .unwrap();
        let binding = native::scope::binding(&db).await.unwrap();
        let versions = history::prefix(source)
            .unwrap()
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        crate::store::pinning::management::catalog::validate(
            &db,
            native::archived_catalog(&versions).unwrap(),
        )
        .await
        .unwrap();
        assert!(initialization::open(&db).await.is_err());
        let request = || initialization::UpgradeRequest {
            database_scope: &binding.0,
            native_identity: &binding.1,
            source_version: source,
            target_version: baseline::VERSION,
        };
        initialization::upgrade_existing(&db, request())
            .await
            .unwrap_or_else(|e| panic!("{source}: {e}"));
        initialization::upgrade_existing(&db, request())
            .await
            .unwrap();
        assert_eq!(native::scope::binding(&db).await.unwrap(), binding);
        initialization::open(&db).await.unwrap();
        assert!(
            db.query_one(Statement::from_string(
                db.get_database_backend(),
                "SELECT owner FROM buckets INDEXED BY idx_custom_bucket_owner WHERE owner='owner'"
            ))
            .await
            .unwrap()
            .is_some()
        );
        assert_eq!(schema::validate(&db).await.unwrap(), [baseline::VERSION]);
    }
}
