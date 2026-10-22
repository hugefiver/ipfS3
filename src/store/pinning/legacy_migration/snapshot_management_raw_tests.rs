use super::{
    management_catalog_tests::{declaration, receipt_rows},
    migration, native_catalog,
};
use crate::store::pinning::{
    management::{native, records, schema},
    quota,
};
use sea_orm::{ConnectionTrait, Statement, TransactionTrait};

#[path = "snapshot_management_fixture.rs"]
pub(super) mod fixture;

fn image<'a>(tables: &'a [migration::TableImage], name: &str) -> &'a migration::TableImage {
    tables.iter().find(|table| table.table == name).unwrap()
}

#[tokio::test]
async fn full_management_originals_are_raw_fenced_receipted_and_not_rekeyed_or_scanned_for_authority()
 {
    let fixture::Fixture {
        directory,
        db,
        server,
        declaration,
    } = fixture::fixture().await;
    let native = native::capture(&db).await.unwrap();
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    let records = image(&plan.before, schema::RECORDS);
    assert_eq!(
        records.primary_key,
        ["database_scope", "plan_id", "item_id", "kind"]
    );
    assert_eq!(records.rows.len(), 3);
    assert_eq!(
        image(&plan.before, schema::RETIREMENTS).primary_key,
        ["allocation_domain"]
    );
    assert_eq!(image(&plan.before, schema::RETIREMENTS).rows.len(), 1);
    for name in [schema::RECORDS, schema::RETIREMENTS] {
        let table = image(&plan.before, name);
        let expected = native.table(name).unwrap();
        assert_eq!(table.columns.as_slice(), expected.columns.as_slice());
        assert_eq!(
            table.primary_key.as_slice(),
            expected.primary_key.as_slice()
        );
        for row in &table.rows {
            let key = table
                .primary_key
                .iter()
                .map(|column| row[column].as_deref().unwrap())
                .collect::<Vec<_>>();
            for column in &table.columns {
                assert_eq!(
                    row[column].as_deref(),
                    native.original_value(name, &key, column)
                );
            }
        }
        assert_eq!(
            table,
            image(&plan.after, name),
            "all raw columns remain immutable"
        );
    }
    let scope = records
        .rows
        .iter()
        .find(|row| row["kind"].as_deref() == Some("database_scope"))
        .unwrap();
    for column in ["result", "expires_at", "completed_at"] {
        assert_eq!(scope[column], None);
    }
    let evidence = records
        .rows
        .iter()
        .find(|row| row["kind"].as_deref() == Some("evidence_preparation"))
        .unwrap();
    assert!(
        evidence["native_before"]
            .as_deref()
            .unwrap()
            .contains("private")
    );
    let action = records
        .rows
        .iter()
        .find(|row| row["kind"].as_deref() == Some("action"))
        .unwrap();
    assert_eq!(action["state"].as_deref(), Some("applied"));
    let retirement = &image(&plan.before, schema::RETIREMENTS).rows[0];
    let review = records::inspect_retirement_review_with_work(
        &db,
        retirement,
        records::SharedDecodeBudget::default(),
    )
    .await
    .unwrap();
    let provider = retirement["allocation_domain"].as_deref().unwrap();
    let original_route = native.original_value("pin_provider_routes", &[provider], "snapshot");
    assert!(original_route.is_some());
    assert_eq!(
        review
            .rows()
            .original_value("pin_provider_routes", &[provider], "snapshot"),
        original_route
    );
    assert_eq!(
        plan.declaration.namespaces[0].bindings.len(),
        1,
        "opaque original frames grant no provider binding"
    );
    let calls = server.received_requests().await.unwrap().len();
    let applied = migration::apply(&db, &declaration, &plan).await.unwrap();
    assert_eq!(
        migration::apply(&db, &declaration, &plan).await.unwrap(),
        applied
    );
    let after = super::super::read(&db, &plan.schema).await.unwrap();
    assert_eq!(after, plan.after);
    let items = receipt_rows(&db).await;
    for name in [schema::RECORDS, schema::RETIREMENTS] {
        let captured = items
            .iter()
            .filter(|row| row["table_kind"].as_deref() == Some(name))
            .collect::<Vec<_>>();
        assert_eq!(captured.len(), image(&plan.before, name).rows.len());
        for item in captured {
            assert_eq!(item["before_image"], item["after_image"]);
            assert_eq!(item["bindings"].as_deref(), Some("[]"));
            let original: Option<migration::NativeRow> =
                serde_json::from_str(item["before_image"].as_deref().unwrap()).unwrap();
            assert!(image(&plan.before, name).rows.contains(&original.unwrap()));
        }
    }
    let reversed = migration::rollback(&db, &applied.id, &plan).await.unwrap();
    assert_eq!(
        migration::rollback(&db, &applied.id, &plan).await.unwrap(),
        reversed
    );
    assert_eq!(
        super::super::read(&db, &plan.schema).await.unwrap(),
        plan.before
    );
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        calls,
        "R26 performs no I/O or provider operations"
    );
    db.close().await.unwrap();
    directory.close().unwrap();
}

#[tokio::test]
async fn empty_retirement_has_original_receipt_sentinel_and_scope_nulls() {
    let db = native_catalog(schema::VERSION).await;
    let declaration = declaration(&db).await;
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    assert!(image(&plan.before, schema::RETIREMENTS).rows.is_empty());
    let applied = migration::apply(&db, &declaration, &plan).await.unwrap();
    let items = receipt_rows(&db).await;
    let sentinel = items
        .iter()
        .filter(|row| row["table_kind"].as_deref() == Some(schema::RETIREMENTS))
        .collect::<Vec<_>>();
    assert_eq!(sentinel.len(), 1);
    assert_eq!(sentinel[0]["original_key"].as_deref(), Some("{}"));
    assert_eq!(sentinel[0]["before_image"].as_deref(), Some("null"));
    assert_eq!(sentinel[0]["after_image"].as_deref(), Some("null"));
    let operation = super::super::super::receipt::lookup(&db, &applied.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        operation.item_count as usize,
        plan.before
            .iter()
            .map(|table| table.rows.len().max(1))
            .sum::<usize>()
    );
    assert_eq!(
        serde_json::from_str::<migration::Confirmation>(&operation.confirmation).unwrap(),
        plan
    );
}

#[tokio::test]
async fn management_private_cells_and_same_count_membership_invalidate_old_confirmation_with_zero_business_change()
 {
    let fixture::Fixture {
        directory,
        db,
        server: _server,
        declaration,
    } = fixture::fixture().await;
    let versions = super::super::schema(&db, false).await.unwrap();
    let immutable = db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT sql FROM main.sqlite_schema WHERE type='trigger' AND name='s75_applied_no_update'"))
        .await.unwrap().unwrap().try_get::<String>("", "sql").unwrap();
    for (table, selector, columns) in [
        (
            schema::RECORDS,
            "kind='action'",
            &["binding", "native_before", "result"][..],
        ),
        (
            schema::RETIREMENTS,
            "record_kind='action'",
            &["binding", "inventory"][..],
        ),
    ] {
        for column in columns {
            let plan = migration::inventory(&db, &declaration).await.unwrap();
            let fault = db.begin().await.unwrap();
            fault
                .execute_unprepared("DROP TRIGGER s75_applied_no_update")
                .await
                .unwrap();
            fault
                .execute_unprepared(&format!(
                    "UPDATE {table} SET {column}={column}||' ' WHERE {selector}"
                ))
                .await
                .unwrap();
            fault.execute_unprepared(&immutable).await.unwrap();
            let changed = super::super::read(&fault, &versions).await.unwrap();
            assert_ne!(changed, plan.before);
            assert!(
                migration::apply(&fault, &declaration, &plan).await.is_err(),
                "{table}.{column}"
            );
            assert_eq!(
                super::super::read(&fault, &versions).await.unwrap(),
                changed
            );
            assert_eq!(
                super::super::frontier(&fault, &versions).await.unwrap(),
                plan.receipt_frontier
            );
            fault.rollback().await.unwrap();
            assert_eq!(
                super::super::read(&db, &versions).await.unwrap(),
                plan.before
            );
        }
    }
    let plan = migration::inventory(&db, &declaration).await.unwrap();
    let fault = db.begin().await.unwrap();
    fault.execute_unprepared("UPDATE pin_management_records SET plan_id='same-count-different-member' WHERE kind='evidence_preparation'").await.unwrap();
    let changed = super::super::read(&fault, &versions).await.unwrap();
    assert_eq!(
        image(&changed, schema::RECORDS).rows.len(),
        image(&plan.before, schema::RECORDS).rows.len()
    );
    assert_ne!(
        image(&changed, schema::RECORDS),
        image(&plan.before, schema::RECORDS)
    );
    assert!(migration::apply(&fault, &declaration, &plan).await.is_err());
    assert_eq!(
        super::super::read(&fault, &versions).await.unwrap(),
        changed
    );
    assert_eq!(
        super::super::frontier(&fault, &versions).await.unwrap(),
        plan.receipt_frontier
    );
    fault.rollback().await.unwrap();
    assert_eq!(
        super::super::read(&db, &versions).await.unwrap(),
        plan.before
    );
    let usage = quota::read_usage(&db, "old").await.unwrap().unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (97, 3));
    db.close().await.unwrap();
    directory.close().unwrap();
}
