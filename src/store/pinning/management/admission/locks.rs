use crate::{error::AppResult, pinning::identity::ProviderRouteSnapshot};
#[cfg(test)]
use sea_orm::DatabaseTransaction;
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

/// Transaction-local catalog comparison, not a cached absence authorization.
pub(super) struct AdmissionSchema(Vec<String>);

impl AdmissionSchema {
    pub(super) async fn read<C: ConnectionTrait>(db: &C) -> AppResult<Self> {
        crate::store::pinning::legacy_migration::admission_schema(db)
            .await
            .map(Self)
            .map_err(|_| super::needs_attention())
    }

    pub(super) fn management_present(&self) -> bool {
        self.0
            .iter()
            .any(|version| version == super::super::schema::VERSION)
    }

    pub(super) async fn revalidate<C: ConnectionTrait>(&self, db: &C) -> AppResult<()> {
        if Self::read(db).await?.0 != self.0 {
            return Err(super::needs_attention());
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub(super) enum RetirementAccess {
    Admission,
    #[cfg(test)]
    RetirementWrite,
}

/// Must precede business reads in caller-owned SQLite write transactions. The
/// known native route table exists in every supported prefix; main qualification
/// avoids executing against a TEMP shadow before the strict catalog rejects it.
/// Bare ConnectionTrait callers do not gain a retained transaction barrier.
pub(crate) async fn lock_sqlite_writer_in_caller_transaction<C: ConnectionTrait>(
    db: &C,
) -> AppResult<()> {
    if db.get_database_backend() == DatabaseBackend::Sqlite {
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "UPDATE main.pin_provider_routes SET provider=provider WHERE 1=0",
        ))
        .await
        .map_err(|_| super::needs_attention())?;
    }
    Ok(())
}

/// Exercises the retirement writer boundary after the complete usage frontier,
/// before route rows and native recheck/insertion.
/// NOWAIT prevents a late management lock from waiting behind a birth which
/// already owns an earlier lifecycle/usage row. Retry only from the first lock.
#[cfg(test)]
pub(crate) async fn lock_retirement_write_in_transaction(
    tx: &DatabaseTransaction,
) -> AppResult<()> {
    let schema = serialize(tx, RetirementAccess::RetirementWrite).await?;
    serialize_routes(tx, true).await?;
    schema.revalidate(tx).await
}

/// Retirement's table fence does not itself serialize missing route rows.
/// SHARE coexists with other births and ordinary history SELECTs. Registration
/// and retirement writes take EXCLUSIVE NOWAIT before touching route rows;
/// R26's full-scope EXCLUSIVE NOWAIT conflicts without an upgrade wait cycle.
pub(super) async fn serialize_routes<C: ConnectionTrait>(db: &C, writer: bool) -> AppResult<()> {
    if db.get_database_backend() == DatabaseBackend::Postgres {
        let sql = if writer {
            "LOCK TABLE pin_provider_routes IN EXCLUSIVE MODE NOWAIT"
        } else {
            "LOCK TABLE pin_provider_routes IN SHARE MODE NOWAIT"
        };
        db.execute(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await
            .map_err(|_| super::needs_attention())?;
    }
    // SQLite already holds its database writer lock from serialize().
    Ok(())
}

pub(super) async fn lock_existing_usage<C: ConnectionTrait>(
    db: &C,
    providers: &[String],
) -> AppResult<()> {
    lock_sqlite_writer_in_caller_transaction(db).await?;
    if db.get_database_backend() != DatabaseBackend::Postgres {
        return Ok(());
    }
    let mut providers = providers.to_vec();
    providers.sort();
    providers.dedup();
    for provider in providers {
        db.query_one(Statement::from_sql_and_values(
            DatabaseBackend::Postgres,
            "SELECT provider FROM pin_provider_usage WHERE provider=$1 FOR UPDATE NOWAIT",
            [provider.into()],
        ))
        .await
        .map_err(|_| super::needs_attention())?;
    }
    Ok(())
}

pub(super) async fn serialize<C: ConnectionTrait>(
    db: &C,
    access: RetirementAccess,
) -> AppResult<AdmissionSchema> {
    lock_sqlite_writer_in_caller_transaction(db).await?;
    let schema = AdmissionSchema::read(db).await?;
    #[cfg(test)]
    if matches!(access, RetirementAccess::RetirementWrite) && !schema.management_present() {
        // Registration may write the native registry in an old catalog. No old
        // catalog can authorize a new durable retirement or its missing fence.
        return Err(super::needs_attention());
    }
    if db.get_database_backend() == DatabaseBackend::Postgres {
        // Concurrent admissions share this metadata fence. A normal migration
        // cannot publish a new version/prefix after our final comparison but
        // before the caller commits. This is neither an allocation sentinel nor
        // an exclusive whole-site birth lock; it precedes retirement/routes.
        db.execute(Statement::from_string(
            DatabaseBackend::Postgres,
            "LOCK TABLE seaql_migrations IN SHARE MODE NOWAIT",
        ))
        .await
        .map_err(|_| super::needs_attention())?;
    }
    if db.get_database_backend() == DatabaseBackend::Postgres && schema.management_present() {
        let sql = match access {
            #[cfg(test)]
            RetirementAccess::RetirementWrite => {
                "LOCK TABLE pin_management_retirements IN EXCLUSIVE MODE NOWAIT"
            }
            RetirementAccess::Admission => {
                "LOCK TABLE pin_management_retirements IN SHARE MODE NOWAIT"
            }
        };
        db.execute(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await
            .map_err(|_| super::needs_attention())?;
    }
    Ok(schema)
}

pub(super) async fn lock_admission_tables<C: ConnectionTrait>(
    db: &C,
) -> AppResult<AdmissionSchema> {
    let schema = serialize(db, RetirementAccess::Admission).await?;
    serialize_routes(db, false).await?;
    // An upgrade between classification and the route fence aborts this whole
    // caller transaction. Never acquire a newly discovered retirement lock late.
    schema.revalidate(db).await?;
    Ok(schema)
}

pub(crate) async fn lock_in_caller_transaction<C: ConnectionTrait>(
    db: &C,
    routes: &[ProviderRouteSnapshot],
) -> AppResult<()> {
    let work = super::SharedDecodeBudget::default();
    lock_with_work(db, routes, super::DecodeWork::shared(&work)).await
}
pub(super) async fn lock_with_work<C: ConnectionTrait>(
    db: &C,
    routes: &[ProviderRouteSnapshot],
    work: super::DecodeWork<'_>,
) -> AppResult<()> {
    if routes.is_empty() {
        return Ok(());
    }
    let schema = lock_admission_tables(db).await?;
    lock_route_rows_with_work(db, routes, work).await?;
    let closed = super::retirement::closed_in_schema(db, routes, &schema, false, work).await?;
    schema.revalidate(db).await?;
    if closed {
        return Err(super::retired());
    }
    Ok(())
}

pub(super) async fn lock_route_rows_with_work<C: ConnectionTrait>(
    db: &C,
    routes: &[ProviderRouteSnapshot],
    work: super::DecodeWork<'_>,
) -> AppResult<()> {
    // Do not wait for a route while holding later locks. The missing-row case
    // remains covered by the table barrier, rather than an invented row lock.
    work.with(|work| work.nodes(1 + routes.len()))
        .map_err(super::work_error)?;
    let mut keys = routes
        .iter()
        .map(|route| super::domains::registration_key_with_work(route, work))
        .collect::<AppResult<Vec<_>>>()?;
    keys.sort();
    keys.dedup();
    if db.get_database_backend() == DatabaseBackend::Postgres {
        for key in keys {
            work.with(|work| work.nodes(1)).map_err(super::work_error)?;
            db.query_one(Statement::from_sql_and_values(
                DatabaseBackend::Postgres,
                "SELECT 1 AS present FROM pin_provider_routes WHERE provider=$1 FOR UPDATE NOWAIT",
                [key.into()],
            ))
            .await
            .map_err(|_| super::needs_attention())?;
        }
    }
    Ok(())
}
