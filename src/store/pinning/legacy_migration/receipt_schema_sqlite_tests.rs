use super::*;
use sea_orm::{DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};

async fn fixture(special: bool) -> DatabaseConnection {
    let db = crate::store::connect_database("sqlite::memory:")
        .await
        .unwrap();
    for ddl in [
        expected::OPERATIONS_SQL,
        expected::ITEMS_SQL,
        expected::SQLITE_BOUND,
    ] {
        db.execute_unprepared(ddl).await.unwrap();
    }
    for table in [expected::OPERATIONS, expected::ITEMS] {
        for operation in ["UPDATE", "DELETE"] {
            db.execute_unprepared(&expected::sqlite_immutable(table, operation))
                .await
                .unwrap();
        }
    }
    if special {
        let (_, ddl) = crate::store::pinning::management::schema::storage::indexes()
            .into_iter()
            .find(|(name, _)| *name == "s75_legacy_reverses")
            .unwrap();
        db.execute_unprepared(ddl).await.unwrap();
    }
    db
}

// Project real SQLite rows, changing just one slot while retaining valid DDL
// and all unrelated metadata. No driver values are synthesized in Rust.
async fn project(
    db: &DatabaseConnection,
    mut statement: Statement,
    slots: &[&str],
    field: &str,
    expression: &str,
    suffix: &str,
) -> Vec<QueryResult> {
    let projection = slots
        .iter()
        .map(|slot| {
            if *slot == field {
                format!("{expression} AS {slot}")
            } else {
                (*slot).to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(",");
    statement.sql = format!("SELECT {projection} FROM ({}) {suffix}", statement.sql);
    db.query_all(statement).await.unwrap()
}

async fn metadata(
    db: &DatabaseConnection,
    field: &str,
    expression: &str,
    suffix: &str,
) -> Vec<QueryResult> {
    let fields = [
        ("table_sql", "table", "t0"),
        ("column_cid", "column", "i0"),
        ("column_name", "column", "t0"),
        ("column_type", "column", "t1"),
        ("column_notnull", "column", "i1"),
        ("column_default", "column", "t2"),
        ("column_pk", "column", "i2"),
        ("column_hidden", "column", "i3"),
        ("index_name", "index", "t0"),
        ("index_unique", "index", "i0"),
        ("index_origin", "index", "t1"),
        ("index_partial", "index", "i1"),
        ("fk_table", "fk", "t0"),
        ("fk_from", "fk", "t1"),
        ("fk_to", "fk", "t2"),
        ("fk_update", "fk", "t3"),
        ("fk_delete", "fk", "t4"),
        ("fk_match", "fk", "t5"),
        ("trigger_name", "trigger", "t0"),
        ("trigger_sql", "trigger", "t1"),
        ("special_sql", "special", "t0"),
    ];
    let rewrite = |text: &str| {
        fields
            .iter()
            .fold(text.to_owned(), |text, (logical, _, slot)| {
                text.replace(logical, slot)
            })
    };
    let (field, expression) =
        if let Some((_, kind, slot)) = fields.iter().find(|(logical, _, _)| *logical == field) {
            (
                *slot,
                format!(
                    "CASE WHEN kind='{kind}' THEN {} ELSE {slot} END",
                    rewrite(expression)
                ),
            )
        } else {
            (field, expression.to_owned())
        };
    let slots: Vec<_> = ["kind", "owner"]
        .into_iter()
        .chain(read::SLOTS.iter().copied())
        .collect();
    project(
        db,
        read::metadata_statement(DatabaseBackend::Sqlite),
        &slots,
        field,
        &expression,
        &rewrite(suffix),
    )
    .await
}

async fn keys(
    db: &DatabaseConnection,
    indexes: &[RegularIndex],
    field: &str,
    expression: &str,
    suffix: &str,
) -> Vec<QueryResult> {
    project(
        db,
        read::keys_statement(DatabaseBackend::Sqlite, indexes),
        &["kind", "owner", "index_name", "key_seqno", "key_name"],
        field,
        expression,
        suffix,
    )
    .await
}

#[tokio::test]
async fn compound_rows_decode_active_slots_without_coercion() {
    let db = fixture(true).await;
    validate(&db).await.unwrap();
    for (kind, slot) in [
        ("table", "table_sql"),
        ("column", "column_cid"),
        ("column", "column_name"),
        ("column", "column_type"),
        ("column", "column_notnull"),
        ("column", "column_pk"),
        ("column", "column_hidden"),
        ("index", "index_name"),
        ("index", "index_unique"),
        ("index", "index_origin"),
        ("index", "index_partial"),
        ("fk", "fk_table"),
        ("fk", "fk_from"),
        ("fk", "fk_to"),
        ("fk", "fk_update"),
        ("fk", "fk_delete"),
        ("fk", "fk_match"),
        ("trigger", "trigger_name"),
        ("trigger", "trigger_sql"),
        ("special", "special_sql"),
    ] {
        let numeric = matches!(
            slot,
            "column_cid"
                | "column_notnull"
                | "column_pk"
                | "column_hidden"
                | "index_unique"
                | "index_partial"
        );
        for bad in ["NULL", if numeric { "'not-an-integer'" } else { "17" }] {
            let rows = metadata(&db, slot, bad, &format!("WHERE kind='{kind}' LIMIT 1")).await;
            assert_eq!(rows.len(), 1);
            assert!(
                read::Table::default().push(&rows[0]).is_err(),
                "{slot}={bad}"
            );
        }
    }
    let rows = metadata(&db, "column_default", "17", "WHERE kind='column' LIMIT 1").await;
    assert!(read::Table::default().push(&rows[0]).is_err());
    // NULL is the valid Option<String> case, not a missing required value.
    let rows = metadata(&db, "", "", "WHERE kind='column'").await;
    for row in rows {
        let mut table = read::Table::default();
        table.push(&row).unwrap();
        assert!(table.columns[0].default.is_none());
    }
    for field in ["kind", "owner"] {
        for bad in ["NULL", "17", "'unknown'"] {
            assert!(validate_metadata(&metadata(&db, field, bad, "").await).is_err());
        }
    }
}

#[tokio::test]
async fn metadata_requires_each_group_and_rejects_duplicates_and_sentinels() {
    let db = fixture(true).await;
    for predicate in [
        "kind='table' AND owner='pin_legacy_migrations'",
        "kind='column' AND owner='pin_legacy_migration_items' AND column_cid=0",
        "kind='index' AND owner='pin_legacy_migrations' AND index_origin='pk'",
        "kind='fk' AND owner='pin_legacy_migration_items'",
        "kind='trigger' AND trigger_name='pin_legacy_migrations_no_update'",
        "kind='trigger' AND trigger_name='pin_legacy_item_bound'",
        "kind='special'",
    ] {
        let rows = metadata(&db, "", "", &format!("WHERE NOT ({predicate})")).await;
        assert!(validate_metadata(&rows).is_err(), "missing {predicate}");
        let suffix = format!(
            "CROSS JOIN (SELECT 0 AS copy UNION ALL SELECT 1) WHERE copy=0 OR ({predicate})"
        );
        let rows = metadata(&db, "", "", &suffix).await;
        assert!(validate_metadata(&rows).is_err(), "duplicate {predicate}");
    }
    let rows = metadata(&db, "trigger_name", "'unknown'", "").await;
    for row in &rows {
        let kind: String = row.try_get("", "kind").unwrap();
        if kind != "trigger" {
            let mut table = read::Table::default();
            table.push(row).unwrap();
            if kind == "table" {
                let owner: String = row.try_get("", "owner").unwrap();
                let ddl = if owner == expected::OPERATIONS {
                    expected::OPERATIONS_SQL
                } else {
                    expected::ITEMS_SQL
                };
                assert_eq!(normalize(&table.ddl[0]), normalize(ddl));
            }
        }
    }
    assert!(validate_metadata(&rows).is_err());
    let rows = metadata(&db, "owner", "'pin_legacy_migration_items'", "").await;
    assert!(validate_metadata(&rows).is_err());
    // A fourth index and a fourth key field are overflow sentinels, not rows
    // that may be filtered away as invalid flags or ignored after a match.
    let rows = metadata(
        &db,
        "index_name",
        "CASE WHEN copy=1 THEN 'extra' ELSE index_name END",
        "CROSS JOIN (SELECT 0 AS copy UNION ALL SELECT 1) WHERE copy=0 OR (kind='index' AND index_origin='pk' AND owner='pin_legacy_migrations')",
    ).await;
    assert!(validate_metadata(&rows).is_err());
    let rows = metadata(&db, "", "", "ORDER BY kind DESC,owner DESC,column_cid DESC").await;
    let indexes = validate_metadata(&rows).unwrap();
    let rows = keys(
        &db,
        &indexes,
        "",
        "",
        "ORDER BY owner DESC,index_name DESC,key_seqno DESC",
    )
    .await;
    validate_keys(&rows, &indexes).unwrap();
}

#[tokio::test]
async fn pragma_validation_is_independent_of_the_valid_table_ddl() {
    let db = fixture(true).await;
    for (slot, predicate, bad) in [
        ("column_name", "kind='column' AND column_cid=0", "'other'"),
        ("column_type", "kind='column' AND column_cid=0", "'BLOB'"),
        ("column_notnull", "kind='column' AND column_cid=0", "0"),
        ("column_default", "kind='column' AND column_cid=0", "'NULL'"),
        ("column_pk", "kind='column' AND column_cid=0", "0"),
        ("column_hidden", "kind='column' AND column_cid=0", "1"),
        ("fk_table", "kind='fk'", "'other'"),
        ("fk_from", "kind='fk'", "'other'"),
        ("fk_to", "kind='fk'", "'other'"),
        ("fk_update", "kind='fk'", "'CASCADE'"),
        ("fk_delete", "kind='fk'", "'CASCADE'"),
        ("fk_match", "kind='fk'", "'FULL'"),
        ("index_unique", "kind='index' AND index_origin='pk'", "0"),
        ("index_partial", "kind='index' AND index_origin='pk'", "1"),
        ("index_origin", "kind='index' AND index_origin='pk'", "'c'"),
        (
            "index_unique",
            "kind='index' AND index_name='s75_legacy_reverses'",
            "1",
        ),
        (
            "index_partial",
            "kind='index' AND index_name='s75_legacy_reverses'",
            "1",
        ),
        (
            "index_origin",
            "kind='index' AND index_name='s75_legacy_reverses'",
            "'u'",
        ),
    ] {
        let expression = format!("CASE WHEN {predicate} THEN {bad} ELSE {slot} END");
        let rows = metadata(&db, slot, &expression, "").await;
        assert!(validate_metadata(&rows).is_err(), "{slot}={bad}");
    }
}

#[tokio::test]
async fn key_vectors_are_ordered_multisets_independent_of_origins() {
    let db = fixture(false).await;
    let rows = metadata(
        &db,
        "index_origin",
        "CASE index_origin WHEN 'pk' THEN 'u' WHEN 'u' THEN 'pk' ELSE index_origin END",
        "",
    )
    .await;
    let indexes = validate_metadata(&rows).unwrap();
    validate_keys(&keys(&db, &indexes, "", "", "").await, &indexes).unwrap();
    let item_indexes: Vec<_> = indexes
        .iter()
        .filter(|index| index.table == expected::ITEMS)
        .collect();
    let a = &item_indexes[0].name;
    let b = &item_indexes[1].name;
    let expression = format!(
        "CASE WHEN owner='pin_legacy_migration_items' THEN CASE index_name WHEN '{a}' THEN '{b}' WHEN '{b}' THEN '{a}' END ELSE index_name END"
    );
    let rows = keys(&db, &indexes, "index_name", &expression, "").await;
    validate_keys(&rows, &indexes).unwrap();
    let expression = format!(
        "CASE WHEN owner='pin_legacy_migration_items' AND index_name='{a}' THEN CASE key_seqno WHEN 0 THEN 1 WHEN 1 THEN 0 ELSE key_seqno END ELSE key_seqno END"
    );
    let rows = keys(&db, &indexes, "key_seqno", &expression, "").await;
    assert!(validate_keys(&rows, &indexes).is_err());
    let rows = keys(
        &db,
        &indexes,
        "key_name",
        "CASE WHEN owner='pin_legacy_migrations' THEN 'id' ELSE key_name END",
        "",
    )
    .await;
    assert!(validate_keys(&rows, &indexes).is_err());
    // Swapping one origin is not a permutation of the required [pk,u] multiset.
    let rows = metadata(
        &db,
        "index_origin",
        "CASE WHEN index_origin='u' THEN 'pk' ELSE index_origin END",
        "",
    )
    .await;
    assert!(validate_metadata(&rows).is_err());
}

#[tokio::test]
async fn key_decoder_rejects_missing_extra_duplicate_and_malformed_rows() {
    let db = fixture(false).await;
    let indexes = validate_metadata(&metadata(&db, "", "", "").await).unwrap();
    let name = &indexes[0].name;
    for suffix in [
        format!("WHERE index_name!='{name}'"),
        format!(
            "CROSS JOIN (SELECT 0 AS copy UNION ALL SELECT 1) WHERE copy=0 OR index_name='{name}'"
        ),
    ] {
        let rows = keys(&db, &indexes, "", "", &suffix).await;
        assert!(validate_keys(&rows, &indexes).is_err());
    }
    let rows = keys(
        &db, &indexes, "key_seqno", "CASE WHEN copy=1 THEN 3 ELSE key_seqno END",
        "CROSS JOIN (SELECT 0 AS copy UNION ALL SELECT 1) WHERE copy=0 OR (owner='pin_legacy_migration_items' AND key_seqno=0)",
    ).await;
    assert!(validate_keys(&rows, &indexes).is_err());
    for (field, bad) in [
        ("kind", "'unknown'"),
        ("kind", "NULL"),
        ("kind", "17"),
        ("owner", "'unknown'"),
        ("owner", "NULL"),
        ("owner", "17"),
        ("index_name", "'unknown'"),
        ("index_name", "NULL"),
        ("index_name", "17"),
        ("key_seqno", "NULL"),
        ("key_seqno", "'not-an-integer'"),
        ("key_name", "NULL"),
        ("key_name", "17"),
    ] {
        let rows = keys(&db, &indexes, field, bad, "").await;
        assert!(validate_keys(&rows, &indexes).is_err(), "{field}={bad}");
    }
}

#[tokio::test]
async fn fresh_reads_reject_damage_and_accept_rollback_on_the_original_connection() {
    let db = fixture(true).await;
    validate(&db).await.unwrap();
    let transaction = db.begin().await.unwrap();
    validate(&transaction).await.unwrap();
    transaction
        .execute_unprepared("DROP TRIGGER pin_legacy_item_bound")
        .await
        .unwrap();
    assert!(validate(&transaction).await.is_err());
    transaction.rollback().await.unwrap();
    validate(&db).await.unwrap();
}
