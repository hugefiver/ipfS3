//! Keyset passes with a fixed revision vector. A page never grants authority.
use super::{
    ManagementError, NativeKey, NativeRow, Result,
    meter::{Budget, ResidentGuard},
    scope::{self, ScopeSpec},
};
use sea_orm::{ConnectionTrait, DatabaseTransaction, Statement};
use serde::Serialize;
use std::collections::BTreeMap;

/// Dedicated full-history source, never used by local authority readers.
pub mod history;

#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct Checkpoint {
    spec: ScopeSpec,
    database_scope: String,
    database_identity: String,
    vector: BTreeMap<String, i64>,
    query: usize,
    after: Option<NativeKey>,
    pages: u64,
    rows: u64,
}
pub struct ScanPage {
    pub rows: Vec<(NativeKey, NativeRow)>,
    pub checkpoint: Checkpoint,
    pub complete: bool,
    _resident: Vec<ResidentGuard>,
}
pub struct SealedInventory {
    checkpoint: Checkpoint,
}
impl SealedInventory {
    pub fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }
}

async fn vector(tx: &DatabaseTransaction, spec: &ScopeSpec) -> Result<BTreeMap<String, i64>> {
    let mut result = BTreeMap::new();
    for selector in spec.selectors() {
        for q in scope::queries(selector)? {
            let name = &q.table.table;
            if result.contains_key(name) {
                continue;
            }
            let row = tx
                .query_one(Statement::from_sql_and_values(
                    tx.get_database_backend(),
                    "SELECT revision FROM pin_management_revision WHERE source_table=$1",
                    [name.clone().into()],
                ))
                .await
                .map_err(|_| ManagementError::Database)?
                .ok_or(ManagementError::Schema)?;
            result.insert(
                name.clone(),
                row.try_get("", "revision")
                    .map_err(|_| ManagementError::Schema)?,
            );
        }
    }
    Ok(result)
}
pub async fn start_scan(
    tx: &DatabaseTransaction,
    spec: ScopeSpec,
    budget: &mut Budget,
) -> Result<Checkpoint> {
    budget.check()?;
    super::super::schema::validate(tx).await?;
    let (database_scope, database_identity) = scope::binding(tx).await?;
    let vector = vector(tx, &spec).await?;
    Ok(Checkpoint {
        spec,
        database_scope,
        database_identity,
        vector,
        query: 0,
        after: None,
        pages: 0,
        rows: 0,
    })
}
pub async fn scan_page(
    tx: &DatabaseTransaction,
    checkpoint: &Checkpoint,
    budget: &mut Budget,
) -> Result<ScanPage> {
    budget.check()?;
    if scope::binding(tx).await?
        != (
            checkpoint.database_scope.clone(),
            checkpoint.database_identity.clone(),
        )
        || vector(tx, &checkpoint.spec).await? != checkpoint.vector
    {
        return Err(ManagementError::Changed);
    }
    let queries = checkpoint
        .spec
        .selectors()
        .iter()
        .map(scope::queries)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let mut next = checkpoint.clone();
    let mut rows = Vec::new();
    let mut resident = Vec::new();
    if let Some(query) = queries.get(next.query) {
        let page = scope::read_page(tx, query, next.after.as_ref(), budget).await?;
        next.pages += 1;
        next.rows += page.rows.len() as u64;
        if page.eof {
            next.query += 1;
            next.after = None;
        } else {
            next.after = page.last;
        }
        rows = page.rows;
        resident = page.resident;
    }
    if vector(tx, &checkpoint.spec).await? != checkpoint.vector {
        return Err(ManagementError::Changed);
    }
    let complete = next.query == queries.len();
    Ok(ScanPage {
        rows,
        checkpoint: next,
        complete,
        _resident: resident,
    })
}
pub async fn seal_scan(
    tx: &DatabaseTransaction,
    checkpoint: &Checkpoint,
    budget: &mut Budget,
) -> Result<SealedInventory> {
    scope::fence_memberships(tx, &checkpoint.spec).await?;
    let page = scan_page(tx, checkpoint, budget).await?;
    if !page.complete || !page.rows.is_empty() || page.checkpoint.query != checkpoint.query {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(SealedInventory {
        checkpoint: checkpoint.clone(),
    })
}
