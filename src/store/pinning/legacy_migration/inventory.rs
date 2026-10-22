use super::{Confirmation, Declaration, MAX_NAMESPACES, projection, refused, snapshot};
use crate::error::AppResult;
use sea_orm::{
    AccessMode, ConnectionTrait, DatabaseBackend, IsolationLevel, Statement, TransactionTrait,
};

pub async fn inventory<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    declaration: &Declaration,
) -> AppResult<Confirmation> {
    validate(declaration)?;
    if let Some(receipt) = declaration.rollback_receipt.as_deref() {
        return rollback_inventory(db, receipt, declaration).await;
    }
    let pg = db.get_database_backend() == DatabaseBackend::Postgres;
    let txn = db
        .begin_with_config(
            pg.then_some(IsolationLevel::RepeatableRead),
            pg.then_some(AccessMode::ReadOnly),
        )
        .await?;
    let schema = snapshot::schema(&txn, false).await?;
    for namespace in &declaration.namespaces {
        if super::receipt::source_has_receipt(&txn, &namespace.source, &schema).await? {
            return Err(refused(
                "source already has an active receipt; replay original apply or explicitly rollback",
            ));
        }
    }
    let before = snapshot::read(&txn, &schema).await?;
    let management =
        super::management_closure::collect(&txn, &before, declaration, &schema).await?;
    let projected = projection::project(&before, declaration, &management)?;
    if super::fence::current(&schema) {
        crate::store::pinning::management::admission::check_r26_open(&txn, &projected.admission)
            .await?;
    }
    let after = projected.after;
    let sql = if pg {
        "WITH clock AS MATERIALIZED (SELECT clock_timestamp() AS now) SELECT now::text AS now, (now+($1::bigint * interval '1 second'))::text AS expires FROM clock"
    } else {
        "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now') AS now, strftime('%Y-%m-%dT%H:%M:%fZ','now','+' || $1 || ' seconds') AS expires"
    };
    let clock = txn
        .query_one(Statement::from_sql_and_values(
            txn.get_database_backend(),
            sql,
            [i64::from(declaration.ttl_seconds).into()],
        ))
        .await?
        .ok_or_else(|| refused("database clock unavailable"))?;
    let result = Confirmation {
        version: 1,
        operation_id: uuid::Uuid::new_v4().to_string(),
        rollback_of: None,
        database_scope: declaration.database_scope.clone(),
        database_identity: snapshot::identity(&txn).await?,
        schema: schema.clone(),
        db_clock: clock.try_get("", "now")?,
        expires_at: clock.try_get("", "expires")?,
        declaration: declaration.clone(),
        receipt_frontier: snapshot::frontier(&txn, &schema).await?,
        before,
        after,
    };
    if serde_json::to_vec(&result)
        .map_err(|_| refused("confirmation serialization failed"))?
        .len()
        > super::MAX_BYTES
    {
        return Err(refused("complete confirmation exceeds byte budget"));
    }
    txn.commit().await?;
    Ok(result)
}

pub async fn rollback_inventory<C: ConnectionTrait + TransactionTrait>(
    db: &C,
    id: &str,
    declaration: &Declaration,
) -> AppResult<Confirmation> {
    validate(declaration)?;
    let pg = db.get_database_backend() == DatabaseBackend::Postgres;
    let txn = db
        .begin_with_config(
            pg.then_some(IsolationLevel::RepeatableRead),
            pg.then_some(AccessMode::ReadOnly),
        )
        .await?;
    let schema = snapshot::schema(&txn, true).await?;
    let original = super::receipt::lookup(&txn, id)
        .await?
        .filter(|r| r.direction == "apply")
        .ok_or_else(|| refused("committed apply receipt unavailable"))?;
    if super::receipt::reverse(&txn, id).await?.is_some() {
        return Err(refused(
            "already rolled back; use original parameters for DB receipt replay",
        ));
    }
    let mut plan: Confirmation = serde_json::from_str(&original.confirmation)
        .map_err(|_| refused("invalid DB receipt confirmation"))?;
    if declaration.namespaces != plan.declaration.namespaces
        || declaration.database_scope != plan.database_scope
    {
        return Err(refused(
            "rollback declaration scope differs from DB receipt",
        ));
    }
    let mut frontier = plan.receipt_frontier.clone();
    frontier.push(id.into());
    frontier.sort();
    if plan.schema != schema
        || snapshot::identity(&txn).await? != plan.database_identity
        || snapshot::read(&txn, &schema).await? != plan.after
        || snapshot::frontier(&txn, &schema).await? != frontier
    {
        return Err(refused("post-state has new work; rollback is unsafe"));
    }
    let sql = if pg {
        "WITH clock AS MATERIALIZED (SELECT clock_timestamp() AS now) SELECT now::text AS now, (now+($1::bigint * interval '1 second'))::text AS expires FROM clock"
    } else {
        "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now') AS now,strftime('%Y-%m-%dT%H:%M:%fZ','now','+' || $1 || ' seconds') AS expires"
    };
    let clock = txn
        .query_one(Statement::from_sql_and_values(
            txn.get_database_backend(),
            sql,
            [i64::from(declaration.ttl_seconds).into()],
        ))
        .await?
        .ok_or_else(|| refused("database clock unavailable"))?;
    plan.db_clock = clock.try_get("", "now")?;
    plan.expires_at = clock.try_get("", "expires")?;
    plan.rollback_of = Some(id.into());
    plan.declaration = declaration.clone();
    txn.commit().await?;
    Ok(plan)
}

pub fn validate(decl: &Declaration) -> AppResult<()> {
    if decl.version != 1
        || decl.database_scope.is_empty()
        || decl.database_scope.len() > 256
        || !decl
            .database_scope
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        || decl.namespaces.is_empty()
        || decl.namespaces.len() > MAX_NAMESPACES
        || !(1..=900).contains(&decl.ttl_seconds)
    {
        return Err(refused("invalid declaration bounds"));
    }
    let mut sources = std::collections::BTreeSet::new();
    let mut binding_count = 0;
    for ns in &decl.namespaces {
        binding_count += ns.bindings.len();
        if binding_count > super::MAX_ROWS {
            return Err(refused("binding budget exceeded"));
        }
        if ns.source.is_empty()
            || ns.source.len() > 1024
            || !sources.insert(&ns.source)
            || ns.bindings.is_empty()
            || ns.binding_only != (ns.source == ns.target)
            || (ns.source.starts_with("domain:") && !ns.binding_only)
        {
            return Err(refused("empty, conflicting or self mapping"));
        }
        let expected = format!(
            "domain:{}",
            serde_json::to_string(&(&ns.backend, &ns.scope, ns.resource_type))
                .map_err(|_| refused("invalid domain"))?
        );
        if ns.target != expected
            || ns.target_route.backend != ns.backend
            || ns.target_route.scope != ns.scope
            || ns.target_route.resource_type() != ns.resource_type
            || ns.target_route.provider_id.starts_with("legacy:")
        {
            return Err(refused(
                "target declaration is not an exact explicit domain",
            ));
        }
    }
    if decl
        .namespaces
        .iter()
        .any(|n| n.source != n.target && sources.contains(&n.target))
    {
        return Err(refused("chained or cyclic mapping"));
    }
    Ok(())
}
