//! Restartable per-original conversion; readiness publication is revision fenced.
use super::*;
use crate::store::pinning::management::{
    native::{
        self, NativeKey,
        meter::{Budget, Limits},
    },
    records, schema,
};
use sea_orm::Value;
use std::collections::BTreeMap;

fn failure(_: impl std::fmt::Display) -> DbErr {
    required()
}

fn budget() -> Budget {
    // Work is bounded per historical original, not by total database history.
    Budget::new(Limits {
        rows: 65536,
        bytes: 128 * 1024 * 1024,
        sql: 65536,
        resident: 32 * 1024 * 1024,
        ..Limits::default()
    })
}

async fn check_binding<C: ConnectionTrait>(
    db: &C,
    request: &UpgradeRequest<'_>,
) -> Result<(), DbErr> {
    let (scope, identity) = native::scope::binding(db).await.map_err(failure)?;
    if scope != request.database_scope
        || identity != request.native_identity
        || identity.starts_with("diagnostic:")
    {
        return Err(required());
    }
    if db.query_one(Statement::from_string(db.get_database_backend(),
        "SELECT 1 FROM pin_management_readiness WHERE id=1 AND format='S75A1' AND state='backfill'"))
        .await?.is_none() { return Err(required()); }
    Ok(())
}

/// Only bounded primary-key metadata is selected. Cell bytes are read separately.
async fn next_key<C: ConnectionTrait>(
    db: &C,
    table: &native::NativeTable,
    after: Option<&NativeKey>,
) -> Result<Option<NativeKey>, DbErr> {
    let keys = table
        .primary_key
        .iter()
        .map(|c| format!("\"{c}\""))
        .collect::<Vec<_>>()
        .join(",");
    let projection = table
        .primary_key
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let octets = if db.get_database_backend() == DatabaseBackend::Sqlite {
                format!("CAST(\"{c}\" AS BLOB)")
            } else {
                format!("convert_to(CAST(\"{c}\" AS TEXT),'UTF8')")
            };
            format!("CASE WHEN length({octets})<=1024 THEN CAST(\"{c}\" AS TEXT) END AS k{i}")
        })
        .collect::<Vec<_>>()
        .join(",");
    let mut values: Vec<Value> = Vec::new();
    let predicate = if let Some(after) = after {
        for (c, v) in table.primary_key.iter().zip(&after.values) {
            values.push(native::scope::bound_value(table, c, v).map_err(failure)?);
        }
        let slots = (1..=values.len())
            .map(|n| format!("${n}"))
            .collect::<Vec<_>>()
            .join(",");
        format!("WHERE ({keys}) > ({slots})")
    } else {
        String::new()
    };
    db.query_one(Statement::from_sql_and_values(
        db.get_database_backend(),
        format!(
            "SELECT {projection} FROM \"{}\" {predicate} ORDER BY {keys} LIMIT 1",
            table.table
        ),
        values,
    ))
    .await?
    .map(|row| {
        Ok(NativeKey {
            table: table.table.clone(),
            values: (0..table.primary_key.len())
                .map(|i| row.try_get("", &format!("k{i}")))
                .collect::<Result<_, _>>()?,
        })
    })
    .transpose()
}

async fn revisions<C: ConnectionTrait>(db: &C) -> Result<BTreeMap<String, i64>, DbErr> {
    db.query_all(Statement::from_string(
        db.get_database_backend(),
        "SELECT source_table,revision FROM pin_management_revision ORDER BY source_table",
    ))
    .await?
    .iter()
    .map(|row| {
        Ok((
            row.try_get("", "source_table")?,
            row.try_get("", "revision")?,
        ))
    })
    .collect()
}

async fn fence(tx: &impl TransactionConnection) -> Result<(), DbErr> {
    native::scope::writer_boundary_in(tx)
        .await
        .map_err(failure)?;
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        let tables = native::registry()
            .iter()
            .map(|t| format!("\"{}\"", t.table))
            .chain(
                schema::storage::EXPORT_TABLES
                    .iter()
                    .map(|table| format!("\"{table}\"")),
            )
            .collect::<Vec<_>>()
            .join(",");
        tx.execute_unprepared(&format!("LOCK TABLE {tables},pin_management_source_control,pin_management_locator,pin_management_pending_scope,pin_management_readiness IN SHARE ROW EXCLUSIVE MODE NOWAIT")).await?;
    }
    Ok(())
}

pub(super) async fn backfill(
    db: &DatabaseConnection,
    request: &UpgradeRequest<'_>,
) -> Result<(), DbErr> {
    let request = maintenance::Request::from(request);
    for table in native::registry()
        .iter()
        .filter(|t| schema::storage::SOURCE_TABLES.contains(&t.table.as_str()))
    {
        let mut after = None;
        loop {
            let table = table.clone();
            let cursor = after.clone();
            let request = request.clone();
            let key = maintenance::run(db, move |tx| {
                Box::pin(async move {
                    let request = request.borrowed();
                    fence(tx).await?;
                    check_binding(tx, &request).await?;
                    let Some(key) = next_key(tx, &table, cursor.as_ref()).await? else {
                        return Ok(None);
                    };
                    let source = records::locator::source_key(&key).map_err(failure)?;
                    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
                        "INSERT INTO pin_management_source_control(source_key) VALUES ($1) ON CONFLICT(source_key) DO NOTHING",[source.into()])).await?;
                    let mut work = budget();
                    super::read_context::run(
                        tx,
                        request.database_scope,
                        request.native_identity,
                        async {
                            history::convert_source(
                                tx,
                                &key,
                                request.database_scope,
                                request.native_identity,
                                &mut work,
                            )
                            .await?;
                            records::locator::rebuild_source_for_upgrade(tx, &key, &mut work).await
                        },
                    )
                    .await
                    .map_err(|error| {
                        DbErr::Migration(format!("management history conversion failed: {error}"))
                    })?;
                    Ok(Some(key))
                })
            }).await?;
            let Some(key) = key else { break };
            after = Some(key);
        }
    }
    // Coverage is checked without a long transaction. The small final transaction
    // proves no business row changed while this bounded metadata scan ran.
    let vector = maintenance::run(db, |tx| Box::pin(revisions(tx))).await?;
    let expected = native::registry()
        .into_iter()
        .map(|table| table.table)
        .chain(
            schema::storage::EXPORT_TABLES
                .iter()
                .map(|name| (*name).to_owned()),
        )
        .collect::<std::collections::BTreeSet<_>>();
    if vector
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        != expected
    {
        return Err(required());
    }
    for table in native::registry()
        .iter()
        .filter(|t| schema::storage::SOURCE_TABLES.contains(&t.table.as_str()))
    {
        let mut after = None;
        loop {
            let table = table.clone();
            let cursor = after.clone();
            let request = request.clone();
            let key = maintenance::run(db, move |tx| {
                Box::pin(async move {
                    let request = request.borrowed();
                    let Some(key) = next_key(tx, &table, cursor.as_ref()).await? else {
                        return Ok(None);
                    };
                    let generation = history::generation(tx, &key).await.map_err(failure)?;
                    if history::recover_at(
                        tx,
                        request.database_scope,
                        request.native_identity,
                        &key,
                        generation,
                        &mut budget(),
                    )
                    .await
                    .map_err(failure)?
                    .is_none()
                    {
                        return Err(required());
                    }
                    Ok(Some(key))
                })
            })
            .await?;
            let Some(key) = key else { break };
            after = Some(key);
        }
    }
    maintenance::run(db, move |tx| {
        Box::pin(async move {
            let request = request.borrowed();
            fence(tx).await?;
            check_binding(tx, &request).await?;
            if revisions(tx).await? != vector {
                return Err(required());
            }
            let dirty = tx.query_one(Statement::from_string(tx.get_database_backend(),
                "SELECT 1 WHERE EXISTS (SELECT 1 FROM pin_management_source_control WHERE sealed_source IS NULL OR sealed_locator IS NULL OR source_generation<>sealed_source OR locator_generation<>sealed_locator) OR EXISTS (SELECT 1 FROM pin_management_pending_scope) OR EXISTS (SELECT 1 FROM pin_management_archive WHERE state<>'sealed')")).await?;
            if dirty.is_some() {
                return Err(required());
            }
            let updated = tx.execute_unprepared("UPDATE pin_management_readiness SET state='ready' WHERE id=1 AND format='S75A1' AND state='backfill'").await?;
            if updated.rows_affected() != 1 {
                return Err(required());
            }
            validate_existing(tx).await?;
            Ok(())
        })
    }).await
}
