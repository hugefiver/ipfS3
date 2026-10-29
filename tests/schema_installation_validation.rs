use ipfs_s3_gateway::store::{
    self,
    initialization::{self, UpgradeRequest},
    migrations::{Historical, baseline, history},
    pinning::management::native,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use sea_orm_migration::MigratorTrait;
#[path = "support/baseline_postgres.rs"]
mod postgres;

struct Fault {
    table: &'static str,
    sqlite_from: &'static [&'static str],
    sqlite_to: &'static str,
    postgres: &'static str,
}
const FAULTS: &[Fault] = &[
    Fault {
        table: "bucket_lifecycle_configs",
        sqlite_from: &[
            "CONSTRAINT ck_bucket_lifecycle_configs_revision CHECK (revision > 0),",
            "CONSTRAINT ck_bucket_lifecycle_configs_revision CHECK(revision>0),",
        ],
        sqlite_to: "",
        postgres: "ALTER TABLE bucket_lifecycle_configs DROP CONSTRAINT ck_bucket_lifecycle_configs_revision",
    },
    Fault {
        table: "residency_backfill",
        sqlite_from: &["CHECK (claim_epoch >= 0)", "CHECK(claim_epoch>=0)"],
        sqlite_to: "CHECK (1)",
        postgres: "ALTER TABLE residency_backfill DROP CONSTRAINT ck_residency_backfill_epoch; ALTER TABLE residency_backfill ADD CONSTRAINT ck_residency_backfill_epoch CHECK (TRUE)",
    },
    Fault {
        table: "zip_mpu_replays",
        sqlite_from: &[
            "length(request_fingerprint) = 64",
            "length(request_fingerprint)=64",
        ],
        sqlite_to: "length(request_fingerprint)>0",
        postgres: "ALTER TABLE zip_mpu_replays DROP CONSTRAINT ck_zip_mpu_request; ALTER TABLE zip_mpu_replays ADD CONSTRAINT ck_zip_mpu_request CHECK(length(request_fingerprint)>0)",
    },
    Fault {
        table: "residency_backfill",
        sqlite_from: &["claim_epoch BIGINT NOT NULL DEFAULT 0"],
        sqlite_to: "claim_epoch BIGINT NOT NULL",
        postgres: "ALTER TABLE residency_backfill ALTER COLUMN claim_epoch DROP DEFAULT",
    },
    Fault {
        table: "residency_backfill",
        sqlite_from: &["claim_epoch BIGINT NOT NULL DEFAULT 0"],
        sqlite_to: "claim_epoch BIGINT NOT NULL DEFAULT 7",
        postgres: "ALTER TABLE residency_backfill ALTER COLUMN claim_epoch SET DEFAULT 7",
    },
    Fault {
        table: "residency_backfill",
        sqlite_from: &["completed BOOLEAN NOT NULL DEFAULT FALSE"],
        sqlite_to: "completed BOOLEAN NOT NULL DEFAULT TRUE",
        postgres: "ALTER TABLE residency_backfill ALTER COLUMN completed SET DEFAULT TRUE",
    },
    Fault {
        table: "bucket_lifecycle_configs",
        sqlite_from: &["scan_lease_epoch BIGINT NOT NULL DEFAULT 0"],
        sqlite_to: "scan_lease_epoch BIGINT NOT NULL",
        postgres: "ALTER TABLE bucket_lifecycle_configs ALTER COLUMN scan_lease_epoch DROP DEFAULT",
    },
];

async fn damage(db: &DatabaseConnection, fault: &Fault) {
    if db.get_database_backend() == DatabaseBackend::Postgres {
        db.execute_unprepared(fault.postgres).await.unwrap();
    } else {
        let raw: String = db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "SELECT sql FROM sqlite_schema WHERE type='table' AND name=$1",
                [fault.table.into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "sql")
            .unwrap();
        let from = fault
            .sqlite_from
            .iter()
            .find(|pattern| raw.contains(**pattern))
            .expect("fault must target an installed clause");
        let changed = raw.replacen(from, fault.sqlite_to, 1);
        assert_ne!(raw, changed);
        db.execute_unprepared(&format!(
            "ALTER TABLE {} RENAME TO damaged_source_backup",
            fault.table
        ))
        .await
        .unwrap();
        db.execute_unprepared(&changed).await.unwrap();
        db.execute_unprepared(&format!(
            "INSERT INTO {} SELECT * FROM damaged_source_backup; DROP TABLE damaged_source_backup",
            fault.table
        ))
        .await
        .unwrap();
    }
}

async fn ledger(db: &DatabaseConnection) -> Vec<(String, i64)> {
    db.query_all(Statement::from_string(
        db.get_database_backend(),
        "SELECT version,applied_at FROM seaql_migrations ORDER BY version",
    ))
    .await
    .unwrap()
    .into_iter()
    .map(|row| {
        (
            row.try_get("", "version").unwrap(),
            row.try_get("", "applied_at").unwrap(),
        )
    })
    .collect()
}

async fn stamp(db: &DatabaseConnection) -> (String, Vec<u8>) {
    let row = db
        .query_one(Statement::from_sql_and_values(
            db.get_database_backend(),
            "SELECT header,payload FROM pin_management_inline_archive WHERE archive_id=$1",
            [format!("history:upgrade:{}", baseline::VERSION).into()],
        ))
        .await
        .unwrap()
        .unwrap();
    (
        row.try_get("", "header").unwrap(),
        row.try_get("", "payload").unwrap(),
    )
}

async fn reject_damaged_source(db: &DatabaseConnection, fault: &Fault) {
    Historical::up(db, None).await.unwrap();
    let binding = native::scope::binding(db).await.unwrap();
    let before = ledger(db).await;
    damage(db, fault).await;
    assert!(
        initialization::upgrade_existing(
            db,
            UpgradeRequest {
                database_scope: &binding.0,
                native_identity: &binding.1,
                source_version: history::M23,
                target_version: baseline::VERSION,
            },
        )
        .await
        .is_err(),
        "{}: {}",
        fault.table,
        if db.get_database_backend() == DatabaseBackend::Sqlite {
            fault.sqlite_to
        } else {
            fault.postgres
        }
    );
    assert_eq!(ledger(db).await, before);
    assert_eq!(native::scope::binding(db).await.unwrap(), binding);
    let row = db.query_one(Statement::from_string(db.get_database_backend(), "SELECT (SELECT COUNT(*) FROM pin_management_archive WHERE archive_id LIKE 'history:upgrade:%')+(SELECT COUNT(*) FROM pin_management_inline_archive WHERE archive_id LIKE 'history:upgrade:%') AS n")).await.unwrap().unwrap();
    assert_eq!(row.try_get::<i64>("", "n").unwrap(), 0);
    let row = db
        .query_one(Statement::from_string(
            db.get_database_backend(),
            "SELECT claim_epoch,completed FROM residency_backfill WHERE id='hot_verification'",
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.try_get::<i64>("", "claim_epoch").unwrap(), 0);
    assert!(!row.try_get::<bool>("", "completed").unwrap());
}

async fn upgrade_healthy_then_reject_current_damage(db: &DatabaseConnection) {
    Historical::up(db, None).await.unwrap();
    let binding = native::scope::binding(db).await.unwrap();
    initialization::upgrade_existing(
        db,
        UpgradeRequest {
            database_scope: &binding.0,
            native_identity: &binding.1,
            source_version: history::M23,
            target_version: baseline::VERSION,
        },
    )
    .await
    .unwrap();
    initialization::open(db).await.unwrap();
    let before = ledger(db).await;
    let original_stamp = stamp(db).await;
    damage(db, &FAULTS[3]).await;
    assert!(initialization::open(db).await.is_err());
    assert!(initialization::validate_existing(db).await.is_err());
    assert_eq!(ledger(db).await, before);
    assert_eq!(stamp(db).await, original_stamp);
}

#[tokio::test]
async fn sqlite_rejects_missing_checks_and_missing_or_wrong_defaults_without_conversion() {
    for fault in FAULTS {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory
                .path()
                .join("source.sqlite")
                .to_string_lossy()
                .replace('\\', "/")
        );
        let db = store::connect_database(&url).await.unwrap();
        reject_damaged_source(&db, fault).await;
        db.close().await.unwrap();
    }
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("healthy.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    upgrade_healthy_then_reject_current_damage(&db).await;
    db.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires authorized isolated IPFS_S3_TEST_POSTGRES_URL"]
async fn postgres_rejects_missing_checks_and_missing_or_wrong_defaults_without_conversion() {
    for fault in FAULTS {
        postgres::with_schema(|db| async move { reject_damaged_source(&db, fault).await }).await;
    }
    postgres::with_schema(
        |db| async move { upgrade_healthy_then_reject_current_damage(&db).await },
    )
    .await;
}

#[tokio::test]
async fn sqlite_rejects_quoted_false_default_without_conversion() {
    let directory = tempfile::tempdir().unwrap();
    let url = format!(
        "sqlite://{}?mode=rwc",
        directory
            .path()
            .join("quoted-default.sqlite")
            .to_string_lossy()
            .replace('\\', "/")
    );
    let db = store::connect_database(&url).await.unwrap();
    let fault = Fault {
        sqlite_to: "completed BOOLEAN NOT NULL DEFAULT \"FALSE\"",
        ..FAULTS[5]
    };
    reject_damaged_source(&db, &fault).await;
    db.close().await.unwrap();
}
