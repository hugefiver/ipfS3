//! Reference epochs are not allocation lifetimes. Only a closed boundary plus
//! a captured current allocation can retire the migration's resource mask.
use super::receipt::work::Work;
use super::{LegacyBinding, MAX_ROWS, refused};
use crate::{error::AppResult, pinning::identity::ProviderRouteSnapshot};
use sea_orm::{ConnectionTrait, Statement};

pub(super) struct FreshLifetime {
    pub boundary: i64,
    pub epoch: i64,
    pub route: String,
}

pub(super) async fn fresh<C: ConnectionTrait>(
    db: &C,
    binding: &LegacyBinding,
    cid: &str,
) -> AppResult<Option<FreshLifetime>> {
    fresh_with_work(db, binding, cid, &()).await
}
pub(super) async fn fresh_with_work<C: ConnectionTrait, W: Work>(
    db: &C,
    binding: &LegacyBinding,
    cid: &str,
    work: &W,
) -> std::result::Result<Option<FreshLifetime>, W::Error> {
    let Some(original) = binding.historical_epoch else {
        return Ok(None);
    };
    // Current state and releases come from one statement snapshot. No recursive
    // call to resource_binding/release_history, and no client receipt evidence.
    let backend = db.get_database_backend();
    let source = "SELECT r.epoch,r.status,l.effect,l.route,p.snapshot,p.retired,h.epoch AS release_epoch,h.ledger AS release_image FROM remote_pins r JOIN remote_pin_ledger l ON l.provider=r.provider AND l.cid=r.cid JOIN pin_provider_routes p ON p.provider=r.provider LEFT JOIN pin_resource_history h ON h.provider=r.provider AND h.cid=r.cid AND h.epoch >= $3 AND h.epoch < r.epoch WHERE r.provider=$1 AND r.cid=$2 ORDER BY h.epoch LIMIT 10001";
    let terms = [
        "epoch",
        "status",
        "effect",
        "route",
        "snapshot",
        "retired",
        "release_epoch",
        "release_image",
    ]
    .map(|column| {
        if backend == sea_orm::DatabaseBackend::Sqlite {
            format!("LENGTH(CAST(COALESCE(CAST({column} AS TEXT),'') AS BLOB))")
        } else {
            format!("OCTET_LENGTH(COALESCE(CAST({column} AS TEXT),''))::bigint")
        }
    })
    .join("+");
    let sum = if backend == sea_orm::DatabaseBackend::Postgres {
        format!("CAST(COALESCE(SUM({terms}),0) AS BIGINT)")
    } else {
        format!("COALESCE(SUM({terms}),0)")
    };
    for _ in 0..2 {
        work.copy(&(&binding.operational_key, cid))?;
    }
    let values: Vec<sea_orm::Value> = vec![
        binding.operational_key.clone().into(),
        cid.into(),
        original.into(),
    ];
    let measured = db
        .query_one(Statement::from_sql_and_values(
            backend,
            format!(
                "WITH lifetime AS ({source}) SELECT COUNT(*) AS count,{sum} AS bytes FROM lifetime"
            ),
            values.clone(),
        ))
        .await
        .map_err(|error| W::application(error.into()))?
        .ok_or_else(|| W::application(refused("resource lifetime proof missing")))?;
    let count = usize::try_from(
        measured
            .try_get::<i64>("", "count")
            .map_err(|error| W::application(error.into()))?,
    )
    .map_err(|_| W::application(refused("resource lifetime proof exceeds row budget")))?;
    let bytes = usize::try_from(
        measured
            .try_get::<i64>("", "bytes")
            .map_err(|error| W::application(error.into()))?,
    )
    .map_err(|_| W::application(refused("resource lifetime proof exceeds byte budget")))?;
    if count > MAX_ROWS {
        return Err(W::application(refused(
            "resource lifetime proof exceeds row budget",
        )));
    }
    for _ in 0..2 {
        work.cells(count * 8, bytes)?;
    }
    let rows=db.query_all(Statement::from_sql_and_values(backend,format!("WITH lifetime AS ({source}) SELECT * FROM lifetime WHERE (SELECT COUNT(*) FROM lifetime)<={count} AND (SELECT {sum} FROM lifetime)<={bytes} ORDER BY release_epoch"),values)).await.map_err(|error|W::application(error.into()))?;
    if rows.len() != count {
        return Err(W::application(refused("resource lifetime proof changed")));
    }
    if rows.len() > MAX_ROWS {
        return Err(W::application(refused(
            "resource lifetime proof exceeds row budget",
        )));
    }
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    let epoch: i64 = row
        .try_get("", "epoch")
        .map_err(|error| W::application(error.into()))?;
    let status: String = row
        .try_get("", "status")
        .map_err(|error| W::application(error.into()))?;
    let effect: String = row
        .try_get("", "effect")
        .map_err(|error| W::application(error.into()))?;
    let route: Option<String> = row
        .try_get("", "route")
        .map_err(|error| W::application(error.into()))?;
    let registered: String = row
        .try_get("", "snapshot")
        .map_err(|error| W::application(error.into()))?;
    if epoch <= original
        || !matches!(
            status.as_str(),
            "reserved" | "queued" | "pinning" | "pinned" | "failed" | "evicting"
        )
        || effect == "absent"
        || row
            .try_get::<bool>("", "retired")
            .map_err(|error| W::application(error.into()))?
        || route.as_deref() != Some(registered.as_str())
    {
        return Ok(None);
    }
    let current: ProviderRouteSnapshot =
        work.parse(&registered, "current allocation route invalid")?;
    for _ in 0..2 {
        work.copy(&(&current.backend, &current.scope, current.resource_type()))?;
    }
    let domain = format!(
        "domain:{}",
        serde_json::to_string(&(&current.backend, &current.scope, current.resource_type()))
            .map_err(|_| W::application(refused("current allocation domain invalid")))?
    );
    if domain != binding.operational_key {
        return Ok(None);
    }
    let mut boundary = binding.closed_at_migration.then_some(original);
    for row in rows {
        let release: Option<i64> = row
            .try_get("", "release_epoch")
            .map_err(|error| W::application(error.into()))?;
        let image: Option<String> = row
            .try_get("", "release_image")
            .map_err(|error| W::application(error.into()))?;
        if let (Some(release), Some(image)) = (release, image)
            && !binding.release_epochs_at_migration.contains(&release)
        {
            let image: serde_json::Value = work.parse(&image, "release lifetime image invalid")?;
            if image["confirmed_absent"] == true
                && image["ledger"]["provider"].as_str() == Some(&binding.operational_key)
                && image["ledger"]["cid"].as_str() == Some(cid)
            {
                boundary = Some(boundary.map_or(release, |old| old.max(release)));
            }
        }
    }
    Ok(boundary.map(|boundary| FreshLifetime {
        boundary,
        epoch,
        route: registered,
    }))
}

pub(super) async fn capture_is_fresh<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    fresh: &FreshLifetime,
) -> AppResult<bool> {
    use sea_orm::EntityTrait;
    let capture =
        crate::store::entities::pin_invocation_route::Entity::find_by_id(job_id.to_owned())
            .one(db)
            .await?;
    Ok(capture.is_some_and(|c| {
        c.remote_epoch > fresh.boundary && c.remote_epoch <= fresh.epoch && c.route == fresh.route
    }))
}
