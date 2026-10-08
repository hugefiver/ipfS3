//! Safe legacy seeds, exact fresh-DB bindings, and durable-state observations.

use std::{collections::BTreeMap, path::Path};

use ipfs_s3_gateway::{
    pinning::identity::ProviderIdentity,
    store::pinning::{
        ledger,
        legacy_migration::{self, Confirmation, Declaration},
        quota,
    },
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use serde_json::{Value, json};

use super::{Blocker, OLD_CID};

type Row = BTreeMap<String, Option<String>>;
pub type Snapshot = BTreeMap<String, Vec<Row>>;
const TABLES: &[&str] = &[
    "objects",
    "object_versions",
    "object_tags",
    "pin_extension_decisions",
    "pin_leases",
    "pin_lease_targets",
    "pin_jobs",
    "pin_provider_usage",
    "remote_pins",
    "remote_pin_ledger",
    "pin_provider_routes",
    "pin_resource_history",
    "pin_submit_history",
    "pin_invocation_routes",
    "pin_submit_observations",
    "pin_replica_observations",
    "pin_lease_requirements",
    "multipart_uploads",
    "import_jobs",
    "zip_batches",
    "zip_v2_executions",
    "zip_v2_mpu_intakes",
    "pin_legacy_migrations",
    "pin_legacy_migration_items",
];
const CAPTURES: &[(&str, &str)] = &[
    ("pin_extension_decisions", "version_row_id"),
    ("multipart_uploads", "upload_id"),
    ("import_jobs", "id"),
    ("zip_batches", "id"),
    ("zip_v2_executions", "id"),
    ("zip_v2_mpu_intakes", "original_upload_id"),
    ("pin_lease_requirements", "lease_id"),
];

pub async fn snapshot(db: &DatabaseConnection) -> Snapshot {
    let mut result = Snapshot::new();
    for table in TABLES {
        result.insert((*table).into(), rows(db, table).await);
    }
    result
}

async fn rows(db: &DatabaseConnection, table: &str) -> Vec<Row> {
    let table = table.replace('"', "\"\"");
    let columns = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!("PRAGMA table_info(\"{table}\")"),
        ))
        .await
        .unwrap();
    assert!(!columns.is_empty(), "known fixture table {table}");
    let columns: Vec<String> = columns
        .into_iter()
        .map(|r| r.try_get("", "name").unwrap())
        .collect();
    let selection = columns
        .iter()
        .map(|c| {
            let c = c.replace('"', "\"\"");
            format!("CAST(\"{c}\" AS TEXT) AS \"{c}\"")
        })
        .collect::<Vec<_>>()
        .join(",");
    let rows = db
        .query_all(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!("SELECT {selection} FROM \"{table}\""),
        ))
        .await
        .unwrap();
    let mut rows: Vec<Row> = rows
        .into_iter()
        .map(|r| {
            columns
                .iter()
                .map(|c| (c.clone(), r.try_get::<Option<String>>("", c).unwrap()))
                .collect()
        })
        .collect();
    rows.sort();
    rows
}

pub async fn assert_confirmation(
    db: &DatabaseConnection,
    confirmation: &Confirmation,
    after: bool,
) {
    for image in if after {
        &confirmation.after
    } else {
        &confirmation.before
    } {
        let mut expected = image.rows.clone();
        expected.sort();
        assert_eq!(
            rows(db, &image.table).await,
            expected,
            "formal CLI native image: {}",
            image.table
        );
    }
}

pub fn source(blocker: Blocker, identity: &ProviderIdentity) -> String {
    match blocker {
        Blocker::DomainNullRoute => identity.allocation_key(),
        _ => "old-surface-alias".into(),
    }
}

pub async fn seed(db: &DatabaseConnection, blocker: Blocker, identity: &ProviderIdentity) {
    let provider = source(blocker, identity);
    let (bytes, pins) = blocker.capacity();
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES (?,?,?)",
        [provider.clone().into(), bytes.into(), pins.into()],
    ))
    .await
    .unwrap();
    if matches!(blocker, Blocker::AccountedRemote) {
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO remote_pins(provider,cid,cid_size,status,epoch,request_id,last_touched_at) VALUES (?, ?,7,'pinned',42,'historical-request','2026-10-07T00:00:00Z')",
            [provider.clone().into(), OLD_CID.into()])).await.unwrap();
    }
    if !matches!(blocker, Blocker::UsageOnly) {
        db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO remote_pin_ledger(provider,cid,ownership,effect) VALUES (?,?,'unknown','unknown')",
            [provider.into(), OLD_CID.into()])).await.unwrap();
    }
}

pub fn assert_fixture_shape(state: &Snapshot, blocker: Blocker, identity: &ProviderIdentity) {
    assert_eq!(state["pin_provider_usage"].len(), 1);
    let usage = &state["pin_provider_usage"][0];
    let (bytes, pins) = blocker.capacity();
    assert_eq!(
        field(usage, "provider"),
        Some(source(blocker, identity).as_str())
    );
    assert_eq!(
        field(usage, "reserved_bytes"),
        Some(bytes.to_string().as_str())
    );
    assert_eq!(
        field(usage, "reserved_pins"),
        Some(pins.to_string().as_str())
    );
    assert_eq!(
        state["remote_pins"].len(),
        usize::from(matches!(blocker, Blocker::AccountedRemote))
    );
    assert_eq!(
        state["remote_pin_ledger"].len(),
        usize::from(!matches!(blocker, Blocker::UsageOnly))
    );
    for row in &state["remote_pin_ledger"] {
        assert_eq!(field(row, "route"), None);
        assert_eq!(field(row, "ownership"), Some("unknown"));
        assert_eq!(field(row, "effect"), Some("unknown"));
    }
}

fn field<'a>(row: &'a Row, name: &str) -> Option<&'a str> {
    row.get(name).and_then(Option::as_deref)
}

fn binding(table: &str, key: Value, provider: &str, route: Value, pointer: Option<&str>) -> Value {
    json!({"table":table,"key":key,"provider":provider,"historical_route":route,"pointer":pointer})
}

// The fresh fixture has only usage / optional remote+ledger provider rows;
// failed publications leave no jobs/targets/history. Domain binding-only also
// includes its registered route and exact captured MPU/ZIP provider pointers.
pub fn declaration(
    before: &Snapshot,
    blocker: Blocker,
    identity: &ProviderIdentity,
    confirmation: &Path,
) -> Declaration {
    let source = source(blocker, identity);
    let mut bindings = vec![binding(
        "pin_provider_usage",
        json!({"provider":source}),
        &source,
        Value::Null,
        None,
    )];
    if matches!(blocker, Blocker::AccountedRemote) {
        bindings.push(binding(
            "remote_pins",
            json!({"provider":source,"cid":OLD_CID}),
            &source,
            Value::Null,
            None,
        ));
    }
    if !matches!(blocker, Blocker::UsageOnly) {
        bindings.push(binding(
            "remote_pin_ledger",
            json!({"provider":source,"cid":OLD_CID}),
            &source,
            Value::Null,
            None,
        ));
    }
    if matches!(blocker, Blocker::DomainNullRoute) {
        bindings.push(binding(
            "pin_provider_routes",
            json!({"provider":source}),
            &source,
            serde_json::to_value(identity.route_snapshot()).unwrap(),
            None,
        ));
    }
    for table in [
        "pin_lease_targets",
        "pin_jobs",
        "pin_resource_history",
        "pin_submit_observations",
        "pin_replica_observations",
    ] {
        assert!(
            before[table].is_empty(),
            "fresh fixture has no other namespace evidence: {table}"
        );
    }
    // Matches the current leaf's capture columns, not prefix matching or
    // inferred production closure. Every pointer comes from an actual row.
    for (table, pk) in CAPTURES {
        for row in &before[*table] {
            for column in [
                "snapshot",
                "pin_decision_json",
                "captured_options",
                "captured_config",
                "contract",
            ] {
                if let Some(raw) = field(row, column) {
                    let value: Value = serde_json::from_str(raw).unwrap();
                    let mut paths = vec![];
                    provider_pointers(&value, &format!("/{column}"), &source, &mut paths);
                    for pointer in paths {
                        bindings.push(binding(
                            table,
                            json!({*pk:field(row, pk).expect("capture primary key")}),
                            &source,
                            Value::Null,
                            Some(&pointer),
                        ));
                    }
                }
            }
        }
    }
    if matches!(blocker, Blocker::UsageOnly) {
        assert_eq!(bindings.len(), 1, "usage-only binding is just usage");
    }
    if matches!(blocker, Blocker::AccountedRemote) {
        assert_eq!(bindings.len(), 3, "exactly usage + remote + ledger");
    }
    serde_json::from_value(json!({
        "version":1,"database_scope":"owned-surface-fixture","ttl_seconds":300,
        "confirmation_file":confirmation,
        "maintenance":{"all_writers_stopped":true,"all_workers_drained":true},
        "namespaces":[{"source":source,"target":identity.allocation_key(),
            "backend":identity.backend,"scope":identity.scope,"resource_type":identity.resource_type(),
            "binding_only":matches!(blocker, Blocker::DomainNullRoute),
            "target_route":identity.route_snapshot(),"bindings":bindings}]
    })).unwrap()
}

fn provider_pointers(value: &Value, path: &str, provider: &str, paths: &mut Vec<String>) {
    match value {
        Value::String(s) if s == provider => paths.push(path.into()),
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                provider_pointers(item, &format!("{path}/{i}"), provider, paths);
            }
        }
        Value::Object(items) => {
            for (key, item) in items {
                let key_path = key.replace('~', "~0").replace('/', "~1");
                if key == provider {
                    paths.push(format!("{path}/@key/{key_path}"));
                }
                provider_pointers(item, &format!("{path}/{key_path}"), provider, paths);
            }
        }
        _ => {}
    }
}

pub async fn assert_usage(
    db: &DatabaseConnection,
    identity: &ProviderIdentity,
    expected: (i64, i64),
) {
    let usage = quota::read_usage(db, &identity.allocation_key())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), expected);
}

pub async fn assert_unknown(
    db: &DatabaseConnection,
    blocker: Blocker,
    identity: &ProviderIdentity,
) {
    if matches!(blocker, Blocker::UsageOnly) {
        return;
    }
    let provider = identity.allocation_key();
    let row = ledger::get(db, &provider, OLD_CID).await.unwrap().unwrap();
    assert_eq!(
        (row.ownership.as_str(), row.effect.as_str()),
        ("unknown", "unknown")
    );
    assert!(row.route.is_some());
    assert!(
        row.remote_pinned_at.is_none()
            && row.gateway_verified_at.is_none()
            && row.content_verified_at.is_none()
    );
    assert!(
        !ledger::cleanup_allowed(db, &provider, OLD_CID)
            .await
            .unwrap()
    );
    let binding = legacy_migration::resource_binding(db, &provider, OLD_CID)
        .await
        .unwrap()
        .unwrap();
    assert!(binding.historical_route.is_none());
    assert_eq!(binding.declaration_route, identity.route_snapshot());
    assert_eq!(
        binding.provenance,
        "migration_declaration_not_remote_evidence"
    );
    let status = ledger::status(db, &provider, OLD_CID).await.unwrap();
    match blocker {
        Blocker::AccountedRemote => assert_eq!(
            status.unwrap().route_provenance,
            "migration_declaration_not_remote_evidence"
        ),
        // Ledger-only evidence does not invent a remote resource/status row.
        Blocker::DomainNullRoute => assert!(status.is_none()),
        Blocker::UsageOnly => unreachable!(),
    }
}

pub async fn assert_migrated(
    db: &DatabaseConnection,
    blocker: Blocker,
    identity: &ProviderIdentity,
) {
    assert_usage(db, identity, blocker.capacity()).await;
    assert_unknown(db, blocker, identity).await;
    let state = snapshot(db).await;
    assert_eq!(state["pin_legacy_migrations"].len(), 1);
    assert!(!state["pin_legacy_migration_items"].is_empty());
    if !matches!(blocker, Blocker::DomainNullRoute) {
        assert!(
            state["pin_provider_usage"]
                .iter()
                .all(|r| field(r, "provider") != Some("old-surface-alias"))
        );
    }
    if matches!(blocker, Blocker::AccountedRemote) {
        assert_eq!(state["remote_pins"].len(), 1);
        let row = &state["remote_pins"][0];
        for (key, expected) in [
            ("provider", identity.allocation_key().as_str()),
            ("cid", OLD_CID),
            ("cid_size", "7"),
            ("epoch", "42"),
            ("status", "pinned"),
            ("request_id", "historical-request"),
        ] {
            assert_eq!(field(row, key), Some(expected));
        }
    }
}
