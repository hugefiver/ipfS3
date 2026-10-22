use super::*;

async fn database() -> DatabaseConnection {
    sea_orm::Database::connect("sqlite::memory:").await.unwrap()
}

#[tokio::test]
async fn composite_key_pages_preserve_null_and_both_eofs() {
    let left = database().await;
    let right = database().await;
    for db in [&left, &right] {
        db.execute_unprepared("CREATE TABLE facts(kind TEXT NOT NULL,identity TEXT NOT NULL,payload TEXT,PRIMARY KEY(kind,identity)) WITHOUT ROWID").await.unwrap();
        for i in 0..300 {
            db.execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "INSERT INTO facts VALUES($1,$2,$3)",
                vec![
                    "current".into(),
                    format!("{i:04}").into(),
                    if i == 257 {
                        Value::from(None::<String>)
                    } else {
                        format!("private-{i}").into()
                    },
                ],
            ))
            .await
            .unwrap();
        }
    }
    let (next, rows, done) = compare(&left, &right, &Cursor::default(), false)
        .await
        .unwrap();
    assert_eq!(rows.len(), 256);
    assert!(!done);
    let (next, rows, _) = compare(&left, &right, &next, false).await.unwrap();
    assert_eq!(rows.len(), 44);
    assert_eq!(rows[1].cells[2], None);
    assert_eq!(next.table, 1);
    assert!(next.after.is_empty());
    let suffix = Cursor {
        table: 0,
        after: vec!["current".into(), "0299".into()],
    };
    let (end, rows, _) = compare(&left, &right, &suffix, false).await.unwrap();
    assert!(rows.is_empty());
    assert_eq!(end.table, 1);
    right
        .execute_unprepared("INSERT INTO facts VALUES('history','tail','private')")
        .await
        .unwrap();
    assert!(matches!(
        compare(&left, &right, &suffix, false).await,
        Err(ManagementError::Changed)
    ));
    right
        .execute_unprepared(
            "DELETE FROM facts WHERE kind='history'; DELETE FROM facts WHERE identity='0257'",
        )
        .await
        .unwrap();
    assert!(matches!(
        compare(
            &left,
            &right,
            &Cursor {
                table: 0,
                after: vec!["current".into(), "0255".into()]
            },
            false
        )
        .await,
        Err(ManagementError::Changed)
    ));
}

#[tokio::test]
async fn numeric_key_cursor_uses_numeric_order_and_empty_table_eof() {
    let db = database().await;
    db.execute_unprepared("CREATE TABLE totals_progress(id INTEGER PRIMARY KEY,stage INTEGER,after_key TEXT,payload TEXT); INSERT INTO totals_progress VALUES(2,0,'',NULL),(10,1,'a','private'),(100,2,'z','tail')").await.unwrap();
    let relation = DERIVED.last().unwrap();
    let result = page(&db, relation, &[]).await.unwrap();
    assert!(result.eof);
    let rows = result.rows;
    assert_eq!(
        rows.iter()
            .map(|row| row.key[0].as_str())
            .collect::<Vec<_>>(),
        vec!["2", "10", "100"]
    );
    assert_eq!(rows[0].cells[3], None);
    let result = page(&db, relation, &["2".into()]).await.unwrap();
    assert!(result.eof);
    let rows = result.rows;
    assert_eq!(
        rows.iter()
            .map(|row| row.key[0].as_str())
            .collect::<Vec<_>>(),
        vec!["10", "100"]
    );
    let result = page(&db, relation, &["100".into()]).await.unwrap();
    assert!(result.rows.is_empty());
    assert!(result.eof);
    db.execute_unprepared("DELETE FROM totals_progress")
        .await
        .unwrap();
    let result = page(&db, relation, &[]).await.unwrap();
    assert!(result.rows.is_empty());
    assert!(result.eof);
}

async fn facts_database(count: usize, payload: &str) -> DatabaseConnection {
    let db = database().await;
    db.execute_unprepared("CREATE TABLE facts(kind TEXT NOT NULL,identity TEXT NOT NULL,payload TEXT,PRIMARY KEY(kind,identity)) WITHOUT ROWID").await.unwrap();
    for i in 0..count {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO facts VALUES('current',$1,$2)",
            vec![format!("{i:04}").into(), payload.into()],
        ))
        .await
        .unwrap();
    }
    db
}

fn raw_bytes(rows: &[Row]) -> usize {
    rows.iter()
        .map(|row| {
            serde_json::to_vec(&row.key).unwrap().len()
                + serde_json::to_vec(&row.cells).unwrap().len()
        })
        .sum()
}

#[tokio::test]
async fn short_nonempty_page_advances_one_table_with_all_rows() {
    let db = facts_database(1, "private").await;
    let (next, rows, done) = compare(&db, &db, &Cursor::default(), false).await.unwrap();
    assert_eq!(next.table, 1);
    assert!(next.after.is_empty());
    assert!(!done);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cells[2].as_deref(), Some("private"));
}

#[tokio::test]
async fn full_window_requires_another_read_and_preserves_the_257th_row() {
    for count in [256, 257] {
        let db = facts_database(count, "private").await;
        let (next, rows, done) = compare(&db, &db, &Cursor::default(), false).await.unwrap();
        assert_eq!(rows.len(), 256);
        assert!(raw_bytes(&rows) < 1048576);
        assert_eq!(next.table, 0);
        assert_eq!(next.after, vec!["current", "0255"]);
        assert!(!done);
        let (next, tail, done) = compare(&db, &db, &next, false).await.unwrap();
        assert_eq!(tail.len(), count - 256);
        let keys = rows
            .iter()
            .chain(&tail)
            .map(|row| row.key[1].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            keys,
            (0..count).map(|i| format!("{i:04}")).collect::<Vec<_>>()
        );
        assert_eq!(next.table, 1);
        assert!(next.after.is_empty());
        assert!(!done);
    }
}

#[tokio::test]
async fn byte_filtered_short_window_keeps_hidden_tail_and_detects_its_difference() {
    let payload = "界".repeat(150000);
    let left = facts_database(3, &payload).await;
    let right = facts_database(3, &payload).await;
    let (next, rows, done) = compare(&left, &right, &Cursor::default(), false)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(raw_bytes(&rows) < 1048576);
    assert_eq!(rows[0].cells[2].as_deref(), Some(payload.as_str()));
    assert_eq!(rows[1].cells[2].as_deref(), Some(payload.as_str()));
    assert_eq!(next.table, 0);
    assert_eq!(next.after, vec!["current", "0001"]);
    assert!(!done);
    let (end, tail, done) = compare(&left, &right, &next, false).await.unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].key[1], "0002");
    assert_eq!(tail[0].cells[2].as_deref(), Some(payload.as_str()));
    assert_eq!(end.table, 1);
    assert!(end.after.is_empty());
    assert!(!done);
    right
        .execute_unprepared("UPDATE facts SET payload='changed' WHERE identity='0002'")
        .await
        .unwrap();
    assert!(matches!(
        compare(&left, &right, &next, false).await,
        Err(ManagementError::Changed)
    ));
}

#[tokio::test]
async fn exact_byte_cap_requires_an_explicit_tail_read() {
    let db = facts_database(1, "").await;
    let baseline = page(&db, &DERIVED[0], &[]).await.unwrap();
    let payload = "x".repeat(1048576 - raw_bytes(&baseline.rows));
    db.execute(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "UPDATE facts SET payload=$1",
        vec![payload.clone().into()],
    ))
    .await
    .unwrap();
    let (next, rows, done) = compare(&db, &db, &Cursor::default(), false).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(raw_bytes(&rows), 1048576);
    assert_eq!(rows[0].cells[2].as_deref(), Some(payload.as_str()));
    assert_eq!(next.table, 0);
    assert_eq!(next.after, vec!["current", "0000"]);
    assert!(!done);
    let (end, rows, done) = compare(&db, &db, &next, false).await.unwrap();
    assert!(rows.is_empty());
    assert_eq!(end.table, 1);
    assert!(end.after.is_empty());
    assert!(!done);
}

#[tokio::test]
async fn oversized_first_row_is_rejected() {
    let db = facts_database(1, &"x".repeat(1048576)).await;
    assert!(matches!(
        compare(&db, &db, &Cursor::default(), false).await,
        Err(ManagementError::InvalidEvidence)
    ));
}

#[tokio::test]
async fn unicode_composite_keyset_preserves_null_and_complete_order() {
    let db = facts_database(0, "").await;
    let expected = [("a", "é"), ("a", "😀"), ("aa", "a"), ("中", "")];
    for (kind, identity) in expected.into_iter().rev() {
        db.execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO facts VALUES($1,$2,NULL)",
            vec![kind.into(), identity.into()],
        ))
        .await
        .unwrap();
    }
    let mut after = Vec::new();
    for (index, (kind, identity)) in expected.into_iter().enumerate() {
        let result = page(&db, &DERIVED[0], &after).await.unwrap();
        assert!(result.eof);
        assert_eq!(result.rows.len(), expected.len() - index);
        assert_eq!(result.rows[0].key, vec![kind, identity]);
        for (row, (kind, identity)) in result.rows.iter().zip(&expected[index..]) {
            assert_eq!(row.key, vec![*kind, *identity]);
            assert_eq!(row.cells[2], None);
        }
        after = result.rows[0].key.clone();
    }
    let result = page(&db, &DERIVED[0], &after).await.unwrap();
    assert!(result.rows.is_empty());
    assert!(result.eof);
}

#[tokio::test]
async fn final_nonempty_table_returns_done_with_serializable_full_payload() {
    for union in [false, true] {
        let db = database().await;
        let (table, sql, cells): (_, _, Vec<Option<String>>) = if union {
            (
                UNION.len() - 1,
                "CREATE TABLE output(id INTEGER PRIMARY KEY,base TEXT,summary TEXT,stage INTEGER,after_key TEXT); INSERT INTO output VALUES(10,'private',NULL,2,'尾')",
                vec![
                    Some("10".into()),
                    Some("private".into()),
                    None,
                    Some("2".into()),
                    Some("尾".into()),
                ],
            )
        } else {
            (
                DERIVED.len() - 1,
                "CREATE TABLE totals_progress(id INTEGER PRIMARY KEY,stage INTEGER,after_key TEXT,payload TEXT); INSERT INTO totals_progress VALUES(10,2,'尾','private')",
                vec![
                    Some("10".into()),
                    Some("2".into()),
                    Some("尾".into()),
                    Some("private".into()),
                ],
            )
        };
        db.execute_unprepared(sql).await.unwrap();
        let (next, rows, done) = compare(
            &db,
            &db,
            &Cursor {
                table,
                after: vec![],
            },
            union,
        )
        .await
        .unwrap();
        assert!(done);
        assert_eq!(next.table, table + 1);
        assert!(next.after.is_empty());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key, vec!["10"]);
        assert_eq!(rows[0].cells, cells);
        let payload: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&rows).unwrap()).unwrap();
        assert_eq!(
            payload,
            serde_json::json!([{ "key": ["10"], "cells": cells }])
        );
    }
}
