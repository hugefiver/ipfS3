use std::sync::Mutex;

use sea_orm::{DatabaseBackend, DbErr, ExecResult, QueryResult, Statement};

use super::*;
use crate::{
    residency::{PhysicalVerification, StorageClass, VersionResidencyIdentity},
    store::{
        lifecycle_scan::version_evaluation_facts, object_version::BucketVersioningState,
        residency::attach_hot_in_transaction,
    },
};

struct CountReads<'a> {
    db: &'a sea_orm::DatabaseConnection,
    reads: Mutex<Vec<Statement>>,
}

impl<'a> CountReads<'a> {
    fn new(db: &'a sea_orm::DatabaseConnection) -> Self {
        Self {
            db,
            reads: Mutex::new(Vec::new()),
        }
    }

    fn count(&self, table: &str) -> usize {
        self.reads
            .lock()
            .unwrap()
            .iter()
            .filter(|statement| statement.sql.contains(&format!("FROM \"{table}\"")))
            .count()
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for CountReads<'_> {
    fn get_database_backend(&self) -> DatabaseBackend {
        self.db.get_database_backend()
    }

    async fn execute(&self, statement: Statement) -> Result<ExecResult, DbErr> {
        self.db.execute(statement).await
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        self.db.execute_unprepared(sql).await
    }

    async fn query_one(&self, statement: Statement) -> Result<Option<QueryResult>, DbErr> {
        self.reads.lock().unwrap().push(statement.clone());
        self.db.query_one(statement).await
    }

    async fn query_all(&self, statement: Statement) -> Result<Vec<QueryResult>, DbErr> {
        self.reads.lock().unwrap().push(statement.clone());
        self.db.query_all(statement).await
    }
}

async fn attach(db: &sea_orm::DatabaseConnection, id: &str, verified: bool) {
    let row = object_version::Entity::find_by_id(id)
        .one(db)
        .await
        .unwrap()
        .unwrap();
    let object = object::Entity::find_by_id(row.object_id.unwrap())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    attach_hot_in_transaction(
        db,
        &VersionResidencyIdentity::new(id, object.id, object.cid),
        &if verified {
            PhysicalVerification::verified("hot-node", "receipt")
        } else {
            PhysicalVerification::Pending
        },
    )
    .await
    .unwrap();
}

async fn assert_matches_fresh(db: &sea_orm::DatabaseConnection, limit: u64) {
    let page = scan_candidate_page(db, "bucket", None, limit)
        .await
        .unwrap();
    for candidate in page.candidates {
        let LifecycleCandidate::Version(candidate) = candidate else {
            panic!("expected a version candidate");
        };
        let row = object_version::Entity::find_by_id(&candidate.target.version_row_id)
            .find_also_related(object::Entity)
            .one(db)
            .await
            .unwrap()
            .unwrap();
        let fresh = version_evaluation_facts(db, &row.0, row.1.as_ref())
            .await
            .unwrap();
        assert_eq!(
            (
                candidate.bucket_versioning_state,
                candidate.primary_storage_class,
                candidate.hot_residency_verified,
            ),
            fresh
        );
    }
}

#[tokio::test]
async fn page_facts_match_fresh_for_marker_legacy_hot_and_cold() {
    let db = setup().await;
    for (id, latest) in [
        ("legacy", true),
        ("pending", true),
        ("verified", false),
        ("cold", false),
        ("failed", false),
    ] {
        insert_candidate(&db, id, id, 1, latest, Some(id), Some(opaque_version())).await;
    }
    insert_candidate(&db, "marker", "marker", 1, true, None, None).await;
    attach(&db, "pending", false).await;
    attach(&db, "verified", true).await;
    attach(&db, "failed", false).await;
    db.execute_unprepared(
        "UPDATE physical_residencies SET verification_state = 'failed' WHERE cid = 'cid-failed'",
    )
    .await
    .unwrap();
    attach(&db, "cold", true).await;
    db.execute_unprepared(
        "INSERT INTO physical_residencies SELECT 'cold', cid, node_identity, verification_state, verification_receipt, verified_at, created_at, updated_at FROM physical_residencies WHERE cid = 'cid-cold'",
    )
    .await
    .unwrap();
    db.execute_unprepared(
        "UPDATE version_residencies SET primary_tier = 'cold', storage_class = 'STANDARD_IA' WHERE version_row_id = 'cold'",
    )
    .await
    .unwrap();

    for state in [
        BucketVersioningState::Unversioned,
        BucketVersioningState::Enabled,
        BucketVersioningState::Suspended,
    ] {
        if state != BucketVersioningState::Unversioned {
            bucket::set_versioning_state(&db, "bucket", state)
                .await
                .unwrap();
        }
        assert_matches_fresh(&db, 10).await;
    }
    let page = scan_candidate_page(&db, "bucket", None, 10).await.unwrap();
    assert!(page.cycle_complete);
    let facts: Vec<_> = page
        .candidates
        .iter()
        .map(|candidate| {
            let LifecycleCandidate::Version(candidate) = candidate else {
                panic!()
            };
            (
                candidate.target.version_row_id.as_str(),
                candidate.primary_storage_class,
                candidate.hot_residency_verified,
            )
        })
        .collect();
    assert_eq!(
        facts,
        [
            ("legacy", Some(StorageClass::Standard), false),
            ("marker", None, false),
            ("pending", Some(StorageClass::Standard), false),
            ("cold", Some(StorageClass::StandardIa), false),
            ("failed", Some(StorageClass::Standard), false),
            ("verified", Some(StorageClass::Standard), true),
        ]
    );
}

#[tokio::test]
async fn fresh_evaluation_observes_changes_after_candidate_scan() {
    let db = setup().await;
    insert_candidate(&db, "version", "key", 1, true, Some("object"), None).await;
    attach(&db, "version", true).await;
    let page = scan_candidate_page(&db, "bucket", None, 1).await.unwrap();
    let LifecycleCandidate::Version(candidate) = &page.candidates[0] else {
        panic!()
    };
    assert!(candidate.hot_residency_verified);
    db.execute_unprepared(
        "UPDATE physical_residencies SET verification_state = 'pending', node_identity = NULL, verification_receipt = NULL, verified_at = NULL",
    ).await.unwrap();
    bucket::set_versioning_state(&db, "bucket", BucketVersioningState::Enabled)
        .await
        .unwrap();
    let (row, object) = object_version::Entity::find_by_id("version")
        .find_also_related(object::Entity)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    let fresh = version_evaluation_facts(&db, &row, object.as_ref())
        .await
        .unwrap();
    assert_eq!(
        fresh,
        (
            BucketVersioningState::Enabled,
            Some(StorageClass::Standard),
            false
        )
    );
    assert_matches_fresh(&db, 1).await;
}

#[tokio::test]
async fn page_reads_are_batched_and_shared_physical_locations_are_deduplicated() {
    let db = setup().await;
    for index in 0..401 {
        let id = format!("version-{index:04}");
        insert_candidate(&db, &id, &id, 1, true, Some(&id), None).await;
    }
    db.execute_unprepared("UPDATE objects SET cid = 'shared-cid'")
        .await
        .unwrap();
    for index in 0..401 {
        attach(&db, &format!("version-{index:04}"), true).await;
    }
    insert_candidate(
        &db,
        "history",
        "history",
        1,
        false,
        Some("history"),
        Some(opaque_version()),
    )
    .await;
    attach(&db, "history", false).await;
    for (limit, expected) in [(1, 1), (20, 20), (402, 402)] {
        let counted = CountReads::new(&db);
        let page = scan_candidate_page(&counted, "bucket", None, limit)
            .await
            .unwrap();
        assert_eq!(page.candidates.len(), expected);
        assert_eq!(counted.count("buckets"), 1);
        assert!(counted.count("version_residencies") <= 4);
        assert!(counted.count("physical_residencies") <= 2);
        assert_eq!(counted.count("objects"), 0, "reuse joined object owners");
        let reads = counted.reads.lock().unwrap();
        for statement in reads.iter() {
            assert!(statement.values.as_ref().map_or(0, |values| values.0.len()) <= 400);
        }
        let physical_bind_count: usize = reads
            .iter()
            .filter(|statement| statement.sql.contains("FROM \"physical_residencies\""))
            .map(|statement| statement.values.as_ref().unwrap().0.len())
            .sum();
        assert_eq!(physical_bind_count, if limit == 402 { 4 } else { 2 });
    }
}

#[tokio::test]
async fn empty_and_marker_only_pages_do_not_read_content_residency() {
    let db = setup().await;
    let counted = CountReads::new(&db);
    let empty = scan_candidate_page(&counted, "missing-bucket", None, 10)
        .await
        .unwrap();
    assert!(empty.candidates.is_empty());
    assert!(empty.cycle_complete);
    assert_eq!(counted.count("buckets"), 0);
    insert_candidate(&db, "marker", "key", 1, true, None, None).await;
    let counted = CountReads::new(&db);
    let page = scan_candidate_page(&counted, "bucket", None, 10)
        .await
        .unwrap();
    assert_eq!(page.candidates.len(), 1);
    assert_eq!(counted.count("buckets"), 1);
    assert_eq!(counted.count("version_residencies"), 0);
    assert_eq!(counted.count("physical_residencies"), 0);
}

#[tokio::test]
async fn distinct_physical_locations_are_read_in_bounded_batches() {
    let db = setup().await;
    for index in 0..201 {
        let id = format!("version-{index:04}");
        insert_candidate(&db, &id, &id, 1, true, Some(&id), None).await;
        attach(&db, &id, false).await;
    }
    let counted = CountReads::new(&db);
    let page = scan_candidate_page(&counted, "bucket", None, 201)
        .await
        .unwrap();
    assert_eq!(page.candidates.len(), 201);
    assert!(page.cycle_complete);
    assert_eq!(counted.count("buckets"), 1);
    assert_eq!(counted.count("version_residencies"), 2);
    assert_eq!(counted.count("physical_residencies"), 2);
    assert!(
        counted.reads.lock().unwrap().iter().all(|statement| {
            statement.values.as_ref().map_or(0, |values| values.0.len()) <= 400
        })
    );
    for candidate in page.candidates {
        let LifecycleCandidate::Version(candidate) = candidate else {
            panic!()
        };
        assert_eq!(
            candidate.primary_storage_class,
            Some(StorageClass::Standard)
        );
        assert!(!candidate.hot_residency_verified);
    }
}

#[tokio::test]
async fn candidate_page_rejects_missing_or_mismatched_content_owners() {
    let db = setup().await;
    insert_candidate(&db, "version", "key", 1, true, Some("object"), None).await;
    db.execute_unprepared("PRAGMA foreign_keys = OFF")
        .await
        .unwrap();
    db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
        .await
        .unwrap();
    for mutation in [
        "DELETE FROM objects",
        "UPDATE objects SET bucket = 'other'",
        "UPDATE objects SET key = 'other-key'",
        "UPDATE object_versions SET kind = 'delete_marker'",
        "UPDATE object_versions SET kind = 'unknown'",
        "UPDATE object_versions SET version_id = 'invalid-public-id'",
    ] {
        db.execute_unprepared("SAVEPOINT corrupt_owner")
            .await
            .unwrap();
        db.execute_unprepared(mutation).await.unwrap();
        assert!(matches!(
            scan_candidate_page(&db, "bucket", None, 10).await,
            Err(AppError::Internal(_))
        ));
        db.execute_unprepared("ROLLBACK TO corrupt_owner")
            .await
            .unwrap();
        db.execute_unprepared("RELEASE corrupt_owner")
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn corrupt_known_residency_fails_instead_of_falling_back_to_legacy_hot() {
    let db = setup().await;
    insert_candidate(&db, "version", "key", 1, true, Some("object"), None).await;
    attach(&db, "version", true).await;
    let (row, object) = object_version::Entity::find_by_id("version")
        .find_also_related(object::Entity)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    // Invalid states are injected without relying on the schema's preventive constraints.
    db.execute_unprepared("PRAGMA foreign_keys = OFF")
        .await
        .unwrap();
    db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
        .await
        .unwrap();
    for mutation in [
        "UPDATE version_residencies SET object_id = 'wrong-owner'",
        "UPDATE version_residencies SET cid = 'wrong-cid'",
        "UPDATE version_residencies SET primary_tier = 'cold'",
        "UPDATE version_residencies SET primary_tier = 'cold', storage_class = 'STANDARD_IA'",
        "UPDATE version_residencies SET primary_tier = 'unknown'",
        "UPDATE version_residencies SET storage_class = 'unknown'",
        "UPDATE version_residencies SET revision = 0",
        "UPDATE physical_residencies SET verification_state = 'unknown'",
        "UPDATE physical_residencies SET node_identity = NULL",
        "UPDATE physical_residencies SET verification_receipt = ''",
        "UPDATE physical_residencies SET verified_at = NULL",
        "UPDATE physical_residencies SET verification_state = 'pending'",
        "DELETE FROM physical_residencies",
    ] {
        db.execute_unprepared("SAVEPOINT corrupt_facts")
            .await
            .unwrap();
        db.execute_unprepared(mutation).await.unwrap();
        let fresh = version_evaluation_facts(&db, &row, object.as_ref()).await;
        let page = scan_candidate_page(&db, "bucket", None, 10).await;
        assert!(
            matches!(fresh, Err(AppError::Internal(_))),
            "{mutation}: {fresh:?}"
        );
        assert!(
            matches!(page, Err(AppError::Internal(_))),
            "{mutation}: {page:?}"
        );
        db.execute_unprepared("ROLLBACK TO corrupt_facts")
            .await
            .unwrap();
        db.execute_unprepared("RELEASE corrupt_facts")
            .await
            .unwrap();
    }
    assert_matches_fresh(&db, 1).await;
}

#[tokio::test]
async fn page_preserves_missing_bucket_and_invalid_bucket_state_errors() {
    let db = setup().await;
    insert_candidate(&db, "marker", "key", 1, true, None, None).await;
    db.execute_unprepared("PRAGMA foreign_keys = OFF")
        .await
        .unwrap();
    db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
        .await
        .unwrap();
    db.execute_unprepared("SAVEPOINT missing_bucket")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM buckets WHERE name = 'bucket'")
        .await
        .unwrap();
    assert!(matches!(
        scan_candidate_page(&db, "bucket", None, 10).await,
        Err(AppError::NoSuchBucket(_))
    ));
    db.execute_unprepared("ROLLBACK TO missing_bucket")
        .await
        .unwrap();
    db.execute_unprepared("RELEASE missing_bucket")
        .await
        .unwrap();
    db.execute_unprepared("UPDATE buckets SET versioning_status = 'unknown' WHERE name = 'bucket'")
        .await
        .unwrap();
    assert!(matches!(
        scan_candidate_page(&db, "bucket", None, 10).await,
        Err(AppError::Internal(_))
    ));
}

#[tokio::test]
async fn facts_errors_are_limited_to_the_selected_keyset_page() {
    let db = setup().await;
    insert_candidate(&db, "first", "a", 1, true, Some("first"), None).await;
    insert_candidate(&db, "second", "b", 1, true, Some("second"), None).await;
    attach(&db, "first", true).await;
    attach(&db, "second", true).await;
    db.execute_unprepared("PRAGMA foreign_keys = OFF")
        .await
        .unwrap();
    db.execute_unprepared("DELETE FROM physical_residencies WHERE cid = 'cid-second'")
        .await
        .unwrap();
    let first = scan_candidate_page(&db, "bucket", None, 1).await.unwrap();
    assert_eq!(first.candidates.len(), 1);
    assert!(!first.cycle_complete);
    let cursor = first.next_cursor.unwrap();
    assert_eq!(cursor.version_row_id.as_deref(), Some("first"));
    for cursor in [None, Some(&cursor)] {
        assert!(matches!(
            scan_candidate_page(&db, "bucket", cursor, 2).await,
            Err(AppError::Internal(_))
        ));
    }
}
