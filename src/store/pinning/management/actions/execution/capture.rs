//! Transaction-local raw rows. Metadata reserves driver and owned copies first.
use super::*;
use facts::{Cursor, Key, Output, Read};
use rows::RowSet;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};
use std::collections::BTreeMap;

pub(super) type Rows = BTreeMap<Key, native::NativeRow>;

/// Exact raw cells for one natively indexed key. PlanningScope separately
/// re-executes complete memberships, including empty sets and allocator births.
pub(super) async fn matches<'a>(
    tx: &DatabaseTransaction,
    binding: records::BindingView<'_>,
    table: &native::NativeTable,
    cells: impl Iterator<Item = (&'a str, Option<&'a str>)>,
    completed: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<()> {
    use std::fmt::Write;
    binding.with_decode_work(|work| work.nodes(2 + table.columns.len()))?;
    let cells = cells.collect::<Vec<_>>();
    let key = table
        .primary_key
        .iter()
        .map(|column| {
            cells
                .iter()
                .find(|(name, _)| *name == column.as_str())
                .and_then(|(_, value)| *value)
                .ok_or(ManagementError::InvalidEvidence)
        })
        .collect::<Result<Vec<_>>>()?;
    binding.copy(&key)?;
    let (predicate, mut values) = super::super::sql::exact_key(table, &key)?;
    let mut sql = format!(
        "SELECT COUNT(*) AS n FROM \"{}\" WHERE {predicate}",
        table.table
    );
    for (column, value) in cells {
        binding.with_decode_work(|work| work.bytes(value.map_or(0, str::len)))?;
        sql.push_str(" AND ");
        values.push(value.map(str::to_owned).into());
        if tx.get_database_backend() == DatabaseBackend::Sqlite {
            write!(
                sql,
                "CAST(CAST(\"{column}\" AS TEXT) AS BLOB) IS CAST(${} AS BLOB)",
                values.len()
            )
        } else {
            write!(
                sql,
                "CAST(\"{column}\" AS TEXT) COLLATE \"C\" IS NOT DISTINCT FROM ${}",
                values.len()
            )
        }
        .map_err(|_| ManagementError::Database)?;
    }
    if let Some(completed) = completed {
        binding.copy(&[completed])?;
        values.push(completed.into());
        write!(sql, " AND completed_at=${}", values.len())
            .map_err(|_| ManagementError::Database)?;
    }
    let matched = tx
        .query_one(Statement::from_sql_and_values(
            tx.get_database_backend(),
            sql,
            values,
        ))
        .await
        .map_err(|_| ManagementError::Database)?
        .ok_or(ManagementError::Database)?
        .try_get::<i64>("", "n")
        .map_err(|_| ManagementError::Database)?;
    if matched != 1 {
        return Err(ManagementError::Changed);
    }
    Ok(())
}

pub(super) async fn row(
    tx: &DatabaseTransaction,
    binding: records::BindingView<'_>,
    key: &Key,
) -> Result<Option<native::NativeRow>> {
    let frame = binding.frozen_rows()?;
    let table = frame.table(&key.0).ok_or(ManagementError::Schema)?;
    if key.1.len() != table.primary_key.len() {
        return Err(ManagementError::Confirmation);
    }
    let layout = native::current_catalog()
        .iter()
        .find(|layout| layout.table == key.0)
        .ok_or(ManagementError::Schema)?;
    let refs = key.1.iter().map(String::as_str).collect::<Vec<_>>();
    let (predicate, params) = super::super::sql::exact_key(layout, &refs)?;
    let terms = table
        .columns
        .iter()
        .map(|c| {
            if tx.get_database_backend() == DatabaseBackend::Postgres {
                format!("CAST(COALESCE(octet_length(CAST(\"{c}\" AS TEXT)),0) AS BIGINT)")
            } else {
                format!("COALESCE(length(CAST(CAST(\"{c}\" AS TEXT) AS BLOB)),0)")
            }
        })
        .collect::<Vec<_>>()
        .join("+");
    binding.copy(&key.1)?;
    let metadata = tx
        .query_one(Statement::from_sql_and_values(
            tx.get_database_backend(),
            format!("SELECT {terms} AS b FROM \"{}\" WHERE {predicate}", key.0),
            params,
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let bytes = usize::try_from(
        metadata
            .try_get::<i64>("", "b")
            .map_err(|_| ManagementError::Database)?,
    )
    .map_err(|_| ManagementError::Budget)?;
    binding.with_decode_work(|work| work.nodes(1))?;
    let row = records::current_row(tx, &key.0, &refs, binding.decode_work()).await?;
    if row.values().flatten().map(String::len).sum::<usize>() != bytes {
        return Err(ManagementError::Changed);
    }
    Ok(Some(row))
}

pub(super) async fn rows(
    tx: &DatabaseTransaction,
    binding: records::BindingView<'_>,
    action: &records::CommittedAction,
    before: &impl RowSet,
) -> Result<Rows> {
    let frame = binding.frozen_rows()?;
    let mut result = BTreeMap::new();
    for id in &action.targets {
        let key = key("pin_lease_targets", id);
        if let Some(value) = row(tx, binding, &key).await? {
            binding.copy(&key)?;
            result.insert(key, value);
        }
    }
    let mut keys = BTreeSet::new();
    closure::keys(binding, action, before, &result, &mut keys)?;
    // Complete resource-local memberships discover allocator births, including
    // jobs that are deliberately not eligible for this action's authority.
    let pairs = keys
        .iter()
        .filter(|key| key.0 == "remote_pins")
        .collect::<Vec<_>>();
    let mut discovered = BTreeSet::new();
    for pair in pairs {
        for name in ["pin_lease_targets", "pin_jobs", "pin_resource_history"] {
            let table = frame.table(name).ok_or(ManagementError::Schema)?;
            binding.with_decode_work(|work| work.nodes(1 + table.primary_key.len()))?;
            let columns = table
                .primary_key
                .iter()
                .map(|c| (c.as_str(), c.as_str()))
                .collect::<Vec<_>>();
            binding.copy(&pair.1)?;
            let (members, bytes) = records::bounded_projection(
                tx,
                &format!("FROM \"{name}\" WHERE provider=$1 AND cid=$2"),
                &columns,
                &table.primary_key.join(","),
                pair.1.iter().cloned().map(Into::into).collect(),
                (native::MAX_ROWS, native::MAX_BYTES),
                binding.decode_work(),
            )
            .await?;
            binding
                .decode_work()
                .index_cells(members.len() * columns.len(), bytes)?;
            for member in members {
                binding.copy(&name)?;
                binding.with_decode_work(|work| work.nodes(2 + columns.len()))?;
                let values = columns
                    .iter()
                    .map(|(_, c)| {
                        member
                            .try_get::<String>("", c)
                            .map_err(|_| ManagementError::Database)
                    })
                    .collect::<Result<_>>()?;
                let key = Key(name.into(), values);
                discovered.insert(key);
            }
        }
    }
    keys.extend(discovered);
    for key in keys {
        if !result.contains_key(&key) && mutable(action, &key, before) {
            if let Some(value) = row(tx, binding, &key).await? {
                result.insert(key, value);
            } else if before.contains_key(&key) {
                return Err(ManagementError::Changed);
            }
        }
    }
    let mut keys = BTreeSet::new();
    closure::keys(
        binding,
        action,
        before,
        &rows::CapturedRows {
            before,
            captured: &result,
        },
        &mut keys,
    )?;
    for key in keys {
        if !result.contains_key(&key) && mutable(action, &key, before) {
            if let Some(value) = row(tx, binding, &key).await? {
                result.insert(key, value);
            } else if before.contains_key(&key) {
                return Err(ManagementError::Changed);
            }
        }
    }
    Ok(result)
}

fn mutable(action: &records::CommittedAction, key: &Key, before: &impl RowSet) -> bool {
    let action_kind = &action.receipt.binding.request.action;
    let allocation = matches!(
        action_kind,
        Action::Create(_) | Action::Backfill(_) | Action::Reapply(_)
    );
    match key.0.as_str() {
        "pin_leases" => {
            action.receipt.intent_ids.contains(&key.1[0])
                && (allocation || matches!(action_kind, Action::Renew { .. } | Action::Cancel))
        }
        "pin_lease_requirements" => allocation && action.receipt.intent_ids.contains(&key.1[0]),
        "pin_lease_targets" => {
            action.targets.contains(&key.1[0]) || matches!(action_kind, Action::Retry)
        }
        "pin_jobs" => {
            matches!(action_kind, Action::Retry)
                || before.get(key).is_none_or(|row| {
                    allocation
                        && native::value(row, "state") == Some("running")
                        && native::value(row, "locked_until").is_none()
                })
        }
        "pin_invocation_routes" => {
            !before.contains_key(key) || matches!(action_kind, Action::Retry)
        }
        "pin_submit_history" => allocation || matches!(action_kind, Action::Retry),
        "pin_resource_history" => !before.contains_key(key) && matches!(action_kind, Action::Retry),
        "pin_provider_usage" => allocation || matches!(action_kind, Action::Migrate { .. }),
        "remote_pins" | "remote_pin_ledger" => !matches!(
            action_kind,
            Action::Reconcile | Action::Retire | Action::RetireWith { .. }
        ),
        "pin_management_retirements" | "pin_provider_routes" | "zip_root_references" => {
            matches!(action_kind, Action::Retire | Action::RetireWith { .. })
        }
        _ => false,
    }
}

pub(super) fn produce(
    binding: records::BindingView<'_>,
    before: &impl RowSet,
    after: &impl RowSet,
    cursor: &Cursor,
    keys: &std::collections::BTreeSet<Key>,
) -> Result<(Vec<Read>, Vec<Output>)> {
    let frame = binding.frozen_rows()?;
    let mut reads = Vec::new();
    let mut outputs = Vec::new();
    for key in keys {
        let actual = before.get(key);
        binding.copy(key)?;
        binding.with_decode_work(|work| work.nodes(1))?;
        let input = cursor.input(key, frame, binding)?;
        let slot = reads.len();
        reads.push(Read(key.clone(), input));
        if let Some(new) = after.get(key) {
            if actual != Some(new) {
                struct Cells<'a>(records::RowView<'a>);
                impl Serialize for Cells<'_> {
                    fn serialize<S: serde::Serializer>(
                        &self,
                        s: S,
                    ) -> std::result::Result<S::Ok, S::Error> {
                        use serde::ser::SerializeSeq;
                        let mut seq = s.serialize_seq(Some(self.0.iter().count()))?;
                        for (_, value) in self.0.iter() {
                            seq.serialize_element(value)?;
                        }
                        seq.end()
                    }
                }
                binding.copy(&(key, slot, Some(Cells(new))))?;
                outputs.push(Output(
                    key.clone(),
                    slot,
                    Some(new.iter().map(|(_, value)| value.clone()).collect()),
                ));
            }
        } else if actual.is_some() {
            binding.copy(key)?;
            outputs.push(Output(key.clone(), slot, None));
        }
    }
    Ok((reads, outputs))
}

pub(super) fn key(table: &str, id: &str) -> Key {
    Key(table.into(), vec![id.into()])
}
