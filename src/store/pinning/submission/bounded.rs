//! Bounded facts shared by RPC receipt selection and non-RPC continuity checks.
use super::*;
use crate::store::entities::pin_replica_observation as replica;
use crate::store::pinning::management::{
    ManagementError,
    records::{DecodeWork, SharedDecodeBudget},
};
use sea_orm::{DatabaseBackend, QueryResult, Value};
type Result<T> = std::result::Result<T, ManagementError>;

fn invalid() -> ManagementError {
    ManagementError::InvalidEvidence
}
fn database(_: sea_orm::DbErr) -> ManagementError {
    ManagementError::Database
}

struct Candidates {
    backend: DatabaseBackend,
    sql: String,
    values: Vec<Value>,
    limit: i32,
}

impl Candidates {
    fn new<E: EntityTrait + Default>(
        backend: DatabaseBackend,
        columns: &str,
        scope: &str,
        values: Vec<Value>,
        order: &str,
        limit: i32,
    ) -> Self {
        let terms = E::Column::iter()
            .map(|column| {
                let name = Iden::to_string(&column);
                if backend == DatabaseBackend::Sqlite {
                    format!("LENGTH(CAST(COALESCE(CAST(\"{name}\" AS TEXT),'') AS BLOB))")
                } else {
                    format!("OCTET_LENGTH(COALESCE(CAST(\"{name}\" AS TEXT),''))::bigint")
                }
            })
            .collect::<Vec<_>>()
            .join("+");
        Self {
            backend,
            sql: format!(
                "SELECT {columns},({terms}) AS row_bytes FROM {} WHERE {scope} ORDER BY {order} LIMIT {}",
                E::default().table_name(),
                limit + 1
            ),
            values,
            limit,
        }
    }

    fn budget(&self) -> String {
        let byte_sum = if self.backend == DatabaseBackend::Postgres {
            "CAST(COALESCE(SUM(row_bytes),0) AS BIGINT)"
        } else {
            "COALESCE(SUM(row_bytes),0)"
        };
        format!("SELECT COUNT(*) AS count,{byte_sum} AS bytes FROM candidates")
    }

    fn guarded(&self, select: &str, values: Vec<Value>) -> Statement {
        let max_bytes = super::super::super::management::native::MAX_BYTES;
        Statement::from_sql_and_values(
            self.backend,
            format!(
                "WITH candidates AS ({}), budget AS ({}) {select} AND (SELECT count FROM budget)<={} AND (SELECT bytes FROM budget)<={max_bytes}",
                self.sql,
                self.budget(),
                self.limit,
            ),
            values,
        )
    }

    async fn metadata<C: ConnectionTrait>(
        &self,
        db: &C,
        columns: &str,
        order: &str,
        work: DecodeWork<'_>,
    ) -> Result<Vec<QueryResult>> {
        let projected = columns
            .split(',')
            .map(|column| {
                if self.backend == DatabaseBackend::Sqlite {
                    format!("LENGTH(CAST(COALESCE(CAST({column} AS TEXT),'') AS BLOB))")
                } else {
                    format!("OCTET_LENGTH(COALESCE(CAST({column} AS TEXT),''))::bigint")
                }
            })
            .collect::<Vec<_>>()
            .join("+");
        let transfer = if self.backend == DatabaseBackend::Postgres {
            format!("CAST(COALESCE(SUM({projected}),0) AS BIGINT)")
        } else {
            format!("COALESCE(SUM({projected}),0)")
        };
        let budget = db
            .query_one(Statement::from_sql_and_values(
                self.backend,
                format!("WITH candidates AS ({}) SELECT budget.*, (SELECT {transfer} FROM candidates) AS transfer_bytes FROM ({}) AS budget", self.sql, self.budget()),
                self.values.clone(),
            ))
            .await.map_err(database)?
            .ok_or_else(invalid)?;
        let count: i64 = budget.try_get("", "count").map_err(database)?;
        let bytes: i64 = budget.try_get("", "bytes").map_err(database)?;
        let transfer_bytes: usize = usize::try_from(
            budget
                .try_get::<i64>("", "transfer_bytes")
                .map_err(database)?,
        )
        .map_err(|_| ManagementError::Budget)?;
        if count < 0
            || count > i64::from(self.limit)
            || bytes < 0
            || bytes > super::super::super::management::native::MAX_BYTES as i64
        {
            return Err(invalid());
        }
        if count == 0 {
            return Ok(Vec::new());
        }
        for _ in 0..2 {
            work.index_cells(
                count as usize * (columns.split(',').count() + 1),
                transfer_bytes,
            )?;
        }
        let mut statement = self.guarded(
            &format!("SELECT {columns},row_bytes FROM candidates WHERE 1=1"),
            self.values.clone(),
        );
        statement.sql.push_str(&format!(
            " AND (SELECT {transfer} FROM candidates)<={transfer_bytes}"
        ));
        statement.sql.push_str(&format!(" ORDER BY {order}"));
        let rows = db.query_all(statement).await.map_err(database)?;
        if rows.len() != count as usize {
            return Err(invalid());
        }
        Ok(rows)
    }
}

#[derive(FromQueryResult)]
struct ProtocolRow {
    id: String,
    provider: String,
    expected_cid: String,
    route: String,
    submit_call: i32,
    row_bytes: i64,
}

struct Observations<'a> {
    candidates: Candidates,
    rows: Vec<(ProtocolRow, ObservationProtocol)>,
    work: DecodeWork<'a>,
}

impl<'a> Observations<'a> {
    async fn read<C: ConnectionTrait>(db: &C, job_id: &str, work: DecodeWork<'a>) -> Result<Self> {
        work.copy(&(job_id, job_id))?;
        let backend = db.get_database_backend();
        let columns = "id,provider,expected_cid,route,submit_call";
        let candidates = Candidates::new::<evidence::Entity>(
            backend,
            columns,
            "job_id=$1",
            vec![job_id.into()],
            "submit_call DESC",
            jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS,
        );
        let metadata = candidates
            .metadata(db, columns, "submit_call DESC", work)
            .await?;
        work.with(|work| work.nodes(2 + metadata.len() * 3))?;
        let mut rows = Vec::new();
        if !metadata.is_empty() {
            let max_bytes = super::super::super::management::native::MAX_BYTES;
            let (context,context_bytes)=crate::store::pinning::management::records::bounded_projection(db,"FROM pin_jobs AS j JOIN pin_invocation_routes AS i ON i.job_id=j.id WHERE j.id=$1",&[("j.provider","provider"),("j.cid","cid"),("i.route","route")],"j.id",vec![job_id.into()],(1,max_bytes),work).await?;
            work.index_cells(context.len() * 3, context_bytes)?;
            let context = context.first().ok_or_else(invalid)?;
            let provider: String = context.try_get("", "provider").map_err(database)?;
            let cid: String = context.try_get("", "cid").map_err(database)?;
            let raw: String = context.try_get("", "route").map_err(database)?;
            let route: ProviderRouteSnapshot = work.with(|work| work.typed(&raw))?;
            observation_protocol(&route).map_err(|_| invalid())?;
            let mut calls = BTreeSet::new();
            for row in metadata {
                let row = ProtocolRow::from_query_result(&row, "").map_err(database)?;
                let frozen: ProviderRouteSnapshot = work.with(|work| work.typed(&row.route))?;
                let protocol = observation_protocol(&frozen).map_err(|_| invalid())?;
                if row.provider != provider
                    || row.expected_cid != cid
                    || frozen != route
                    || row.submit_call <= 0
                    || row.submit_call > jobs::MAX_SUBMIT_RECOVERY_ATTEMPTS
                    || !calls.insert(row.submit_call)
                {
                    return Err(invalid());
                }
                rows.push((row, protocol));
            }
        }
        Ok(Self {
            candidates,
            rows,
            work,
        })
    }

    async fn payload<C: ConnectionTrait>(
        &self,
        db: &C,
        selected: &ProtocolRow,
    ) -> Result<evidence::Model> {
        self.work.copy(&selected.id)?;
        let bytes = usize::try_from(selected.row_bytes).map_err(|_| ManagementError::Budget)?;
        for _ in 0..2 {
            self.work
                .index_cells(evidence::Column::iter().count(), bytes)?;
        }
        let mut values = self.candidates.values.clone();
        values.push(selected.id.clone().into());
        let row = db.query_one(self.candidates.guarded(
            &format!("SELECT fact.* FROM pin_submit_observations AS fact JOIN candidates ON fact.id=candidates.id WHERE fact.id=$2 AND fact.job_id=$1 AND candidates.row_bytes<={bytes}"), values,
        )).await.map_err(database)?.ok_or_else(invalid)?;
        let row = evidence::Model::from_query_result(&row, "").map_err(database)?;
        if row.provider != selected.provider
            || row.expected_cid != selected.expected_cid
            || row.route != selected.route
            || row.submit_call != selected.submit_call
        {
            return Err(invalid());
        }
        Ok(row)
    }
}

/// Classify every bounded candidate before selecting; newer observations cannot
/// hide RPC debt. Receipt begin/record/replay retain their own protocol.
pub(crate) async fn latest_rpc<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
) -> AppResult<Option<evidence::Model>> {
    let work = SharedDecodeBudget::default();
    latest_rpc_with_work(db, job_id, DecodeWork::shared(&work))
        .await
        .map_err(crate::store::pinning::management::actions::job_authority::safe)
}
pub(crate) async fn latest_rpc_with_work<C: ConnectionTrait>(
    db: &C,
    job_id: &str,
    work: DecodeWork<'_>,
) -> Result<Option<evidence::Model>> {
    let observations = Observations::read(db, job_id, work).await?;
    let Some((selected, _)) = observations
        .rows
        .iter()
        .find(|(_, protocol)| *protocol == ObservationProtocol::RpcReceipt)
    else {
        return Ok(None);
    };
    observations.payload(db, selected).await.map(Some)
}

/// Facts only: this cannot create a grant or replace the caller's live fences.
pub(crate) async fn rejected_non_rpc_call_with_work<C: ConnectionTrait>(
    db: &C,
    job: &pin_job::Model,
    invocation: &pin_invocation_route::Model,
    call: i32,
    work: DecodeWork<'_>,
) -> Result<bool> {
    let observations = Observations::read(db, &job.id, work).await?;
    let Some((selected, protocol)) = observations.rows.first() else {
        return Ok(false);
    };
    if *protocol != ObservationProtocol::NonRpcObservation || selected.submit_call != call {
        return Ok(false);
    }
    let fact = observations.payload(db, selected).await?;
    let Some(observed_at) = fact.observed_at else {
        return Ok(false);
    };
    let resources: Vec<ResourceEvidence> = match work.with(|work| work.typed(&fact.resources)) {
        Ok(resources) => resources,
        Err(ManagementError::Budget) => return Err(ManagementError::Budget),
        Err(_) => return Ok(false),
    };
    if fact.id != format!("{}:call:{call}", job.id)
        || fact.job_id != job.id
        || fact.provider != job.provider
        || fact.expected_cid != job.cid
        || fact.route != invocation.route
        || fact.remote_epoch != invocation.remote_epoch
        || fact.needs_attention
        || fact.effect != "not_created"
        || !matches!(fact.outcome.as_str(), "NotCreated" | "Rejected")
        || !resources.is_empty()
    {
        return Ok(false);
    }
    // An append has no replica call number. Either captured locator identifies
    // a candidate, including conflicting claims/times, not only exact matches.
    work.copy(&job.id)?;
    let candidates = Candidates::new::<replica::Entity>(
        db.get_database_backend(),
        "sequence",
        "job_id=$1 AND (observed_at=$2 OR claim_until=$3)",
        vec![
            job.id.clone().into(),
            observed_at.into(),
            fact.claim_until.into(),
        ],
        "sequence DESC",
        1,
    );
    let metadata = candidates
        .metadata(db, "sequence", "sequence DESC", work)
        .await?;
    let Some(row) = metadata.first() else {
        return Ok(false);
    };
    let sequence: i64 = row.try_get("", "sequence").map_err(database)?;
    let bytes = usize::try_from(row.try_get::<i64>("", "row_bytes").map_err(database)?)
        .map_err(|_| ManagementError::Budget)?;
    for _ in 0..2 {
        work.index_cells(replica::Column::iter().count(), bytes)?;
    }
    let mut values = candidates.values.clone();
    values.push(sequence.into());
    let row = db.query_one(candidates.guarded(
        &format!("SELECT fact.* FROM pin_replica_observations AS fact JOIN candidates ON fact.sequence=candidates.sequence WHERE fact.sequence=$4 AND candidates.row_bytes<={bytes}"), values,
    )).await.map_err(database)?.ok_or_else(invalid)?;
    let counterpart = replica::Model::from_query_result(&row, "").map_err(database)?;
    let counterpart_resources: Vec<ObservedResource> =
        match work.with(|work| work.typed(&counterpart.resources)) {
            Ok(resources) => resources,
            Err(ManagementError::Budget) => return Err(ManagementError::Budget),
            Err(_) => return Ok(false),
        };
    Ok(counterpart.job_id == fact.job_id
        && counterpart.phase == "submit"
        && counterpart.provider == fact.provider
        && counterpart.expected_cid == fact.expected_cid
        && counterpart.route == fact.route
        && counterpart.remote_epoch == fact.remote_epoch
        && counterpart.claim_until == fact.claim_until
        && counterpart.observed_at == observed_at
        && counterpart.lease_id == job.lease_id
        && counterpart.target_id == job.target_id
        && counterpart.expected_generation == job.expected_generation
        && counterpart.outcome == fact.outcome
        && counterpart.safe_error == fact.safe_error
        && !counterpart.projectable
        && counterpart_resources.is_empty())
}
