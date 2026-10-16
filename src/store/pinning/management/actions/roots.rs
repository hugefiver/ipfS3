//! Caller-owned root release transaction. No root deletion, provider IO or GC.
use super::*;
use sea_orm::{DatabaseTransaction, Statement};

pub(super) async fn release(
    tx: &DatabaseTransaction,
    binding: records::BindingView<'_>,
    result: &mut records::CommittedAction,
) -> Result<()> {
    let before = binding.frozen_rows()?;
    let crate::pinning::management::Action::RetireWith {
        selection:
            crate::pinning::management::RetirementSelection::ReleaseRootReferences { references },
    } = &result.receipt.binding.request.action
    else {
        return Err(ManagementError::Confirmation);
    };
    binding.copy(references)?;
    result.root_releases = references.clone();
    result.root_releases.sort();
    records::roots::validate_rows(result, before, binding.decode_work())?;
    binding.copy(&result.root_releases)?;
    let mut batches = result
        .root_releases
        .iter()
        .map(|key| key.values[0].clone())
        .collect::<Vec<_>>();
    batches.sort();
    batches.dedup();
    if tx.get_database_backend() == DatabaseBackend::Postgres {
        for batch in &batches {
            if tx
                .query_one(Statement::from_sql_and_values(
                    DatabaseBackend::Postgres,
                    "SELECT id FROM zip_batches WHERE id=$1 FOR UPDATE NOWAIT",
                    [batch.clone().into()],
                ))
                .await
                .map_err(|_| ManagementError::Changed)?
                .is_none()
            {
                return Err(ManagementError::Changed);
            }
        }
    }
    let now = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    for key in &result.root_releases {
        let proof = before
            .row_key(&key.table, &key.values)
            .ok_or(ManagementError::InvalidEvidence)?;
        for (table, values) in [
            ("zip_batches", &key.values[..1]),
            ("zip_root_builds", &key.values[..3]),
            ("zip_root_references", key.values.as_slice()),
        ] {
            binding.with_decode_work(|work| work.nodes(1 + values.len()))?;
            let refs = values.iter().map(String::as_str).collect::<Vec<_>>();
            let actual = records::current_row(tx, table, &refs, binding.decode_work()).await?;
            if !before.original_row_matches(table, &refs, &actual) {
                return Err(ManagementError::Changed);
            }
        }
        let table = before
            .table(&key.table)
            .ok_or(ManagementError::InvalidEvidence)?;
        binding.with_decode_work(|work| work.nodes(2 + table.columns.len() * 2))?;
        binding.copy(&proof)?;
        let mut predicates = vec!["logical_released_at IS NULL".to_owned()];
        let mut parameters = vec![sea_orm::Value::from(now)];
        for column in table
            .columns
            .iter()
            .filter(|column| column.as_str() != "logical_released_at")
        {
            match proof.get(column).ok_or(ManagementError::InvalidEvidence)? {
                Some(value) => {
                    parameters.push(value.clone().into());
                    predicates.push(format!("CAST(\"{column}\" AS TEXT)=${}", parameters.len()));
                }
                None => predicates.push(format!("\"{column}\" IS NULL")),
            }
        }
        let changed = tx
            .execute(Statement::from_sql_and_values(
                tx.get_database_backend(),
                format!(
                    "UPDATE zip_root_references SET logical_released_at=$1 WHERE {}",
                    predicates.join(" AND ")
                ),
                parameters,
            ))
            .await
            .map_err(|_| ManagementError::Database)?;
        if changed.rows_affected() != 1 {
            return Err(ManagementError::Changed);
        }
    }
    Ok(())
}
