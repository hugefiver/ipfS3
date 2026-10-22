//! Page-local writes for lease and target reduction; no state grows with inventory size.
use super::*;
use sea_orm::{DatabaseBackend, Statement, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

#[derive(Default)]
pub(super) struct PageWrites {
    facts: BTreeMap<(String, String), String>,
    edges: Vec<Vec<Value>>,
    cids: BTreeSet<String>,
    counts: BTreeMap<String, [i64; 3]>,
    bytes: usize,
}
impl PageWrites {
    pub fn fact(&mut self, kind: &str, id: &str, value: &impl Serialize) -> Result<()> {
        let payload = encode(value)?;
        self.bytes = self
            .bytes
            .checked_add(payload.len())
            .ok_or(ManagementError::Budget)?;
        match self.facts.entry((kind.into(), id.into())) {
            std::collections::btree_map::Entry::Occupied(old) if old.get() != &payload => {
                Err(ManagementError::Changed)
            }
            std::collections::btree_map::Entry::Occupied(_) => Ok(()),
            std::collections::btree_map::Entry::Vacant(entry) => {
                self.bytes = self
                    .bytes
                    .checked_add(kind.len() + id.len())
                    .ok_or(ManagementError::Budget)?;
                entry.insert(payload);
                Ok(())
            }
        }
    }
    pub fn edge(&mut self, values: Vec<Value>) {
        self.bytes += values
            .iter()
            .filter_map(|value| {
                if let Value::String(Some(text)) = value {
                    Some(text.len())
                } else {
                    None
                }
            })
            .sum::<usize>();
        self.edges.push(values);
    }
    pub fn cid(&mut self, cid: &str) {
        if self.cids.insert(cid.into()) {
            self.bytes += cid.len();
        }
    }
    pub fn count(&mut self, resource: String, flags: [bool; 3]) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(resource.len())
            .ok_or(ManagementError::Budget)?;
        let counts = self.counts.entry(resource).or_default();
        for (n, flag) in counts.iter_mut().zip(flags) {
            *n = n
                .checked_add(i64::from(flag))
                .ok_or(ManagementError::Budget)?;
        }
        Ok(())
    }
    pub fn full(&self) -> bool {
        self.bytes >= 1024 * 1024
    }
    pub async fn flush(self, tx: &impl ConnectionTrait, run: &Run<'_>) -> Result<()> {
        super::super::spool::budget::copy_work(tx, run, self.bytes).await?;
        let mut facts = self.facts.into_iter().peekable();
        while facts.peek().is_some() {
            run.check()?;
            let mut batch = Vec::new();
            let mut bytes = 0usize;
            while let Some((_, payload)) = facts.peek() {
                if !batch.is_empty() && (batch.len() == 128 || bytes + payload.len() > 1024 * 1024)
                {
                    break;
                }
                bytes += payload.len();
                batch.push(facts.next().ok_or(ManagementError::InvalidEvidence)?);
            }
            let values = batch
                .iter()
                .flat_map(|((kind, id), payload)| {
                    [
                        Value::from(kind.clone()),
                        Value::from(id.clone()),
                        Value::from(payload.clone()),
                    ]
                })
                .collect::<Vec<_>>();
            let inserted = tx
                .execute(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    format!(
                        "INSERT INTO facts VALUES {} ON CONFLICT(kind,identity) DO NOTHING",
                        tuples(batch.len(), 3)
                    ),
                    values,
                ))
                .await
                .map_err(|_| ManagementError::Database)?;
            if inserted.rows_affected() != batch.len() as u64 {
                for ((kind, id), payload) in batch {
                    insert_fact(tx, &kind, &id, &payload).await?;
                }
            }
        }
        if !self.edges.is_empty() {
            let tuples = tuples(self.edges.len(), 7);
            exec(
                tx,
                &format!("INSERT INTO target_edges VALUES {tuples}"),
                self.edges.into_iter().flatten().collect(),
            )
            .await?;
        }
        if !self.cids.is_empty() {
            let tuples = tuples(self.cids.len(), 1);
            exec(
                tx,
                &format!("INSERT INTO cids VALUES {tuples} ON CONFLICT(cid) DO NOTHING"),
                self.cids.into_iter().map(Value::from).collect(),
            )
            .await?;
        }
        for (resource, counts) in self.counts {
            let changed=tx.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "INSERT INTO resource_counts VALUES($1,$2,$3,$4) ON CONFLICT(resource_key) DO UPDATE SET refs=refs+excluded.refs,inflight=inflight+excluded.inflight,cleanup=cleanup+excluded.cleanup WHERE typeof(refs)='integer' AND refs BETWEEN 0 AND 9223372036854775807-excluded.refs AND typeof(inflight)='integer' AND inflight BETWEEN 0 AND 9223372036854775807-excluded.inflight AND typeof(cleanup)='integer' AND cleanup BETWEEN 0 AND 9223372036854775807-excluded.cleanup",
                vec![resource.into(),counts[0].into(),counts[1].into(),counts[2].into()],
            )).await.map_err(|_|ManagementError::Database)?;
            if changed.rows_affected() != 1 {
                return Err(ManagementError::Budget);
            }
        }
        run.check()
    }
}
fn tuples(rows: usize, columns: usize) -> String {
    (0..rows)
        .map(|row| {
            format!(
                "({})",
                (1..=columns)
                    .map(|column| format!("${}", row * columns + column))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}
async fn insert_fact(tx: &impl ConnectionTrait, kind: &str, id: &str, payload: &str) -> Result<()> {
    let inserted = tx
        .execute(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "INSERT INTO facts VALUES($1,$2,$3) ON CONFLICT(kind,identity) DO NOTHING",
            vec![kind.into(), id.into(), payload.into()],
        ))
        .await
        .map_err(|_| ManagementError::Database)?;
    if inserted.rows_affected() == 1 {
        return Ok(());
    }
    let row=one(tx,"SELECT CASE WHEN length(CAST(payload AS BLOB))<=$3 THEN payload END AS payload FROM facts WHERE kind=$1 AND identity=$2",vec![kind.into(),id.into(),(payload.len() as i64).into()]).await?.ok_or(ManagementError::InvalidEvidence)?;
    if get::<String>(&row, "payload")? != payload {
        return Err(ManagementError::Changed);
    }
    Ok(())
}

impl ProjectionContext<'_, '_> {
    pub(super) async fn fact(
        &self,
        tx: &impl ConnectionTrait,
        kind: &str,
        id: &str,
        value: &impl Serialize,
    ) -> Result<()> {
        if let Some(writes) = self.writes {
            writes
                .lock()
                .map_err(|_| ManagementError::InvalidEvidence)?
                .fact(kind, id, value)
        } else {
            fact(tx, kind, id, value).await
        }
    }
}

pub(super) type Writes = Mutex<PageWrites>;
