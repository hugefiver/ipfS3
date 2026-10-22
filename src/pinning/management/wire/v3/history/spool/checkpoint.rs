use super::*;
use crate::store::pinning::management::native::meter::{Checkpoint as BudgetCheckpoint, Limits};
use std::path::Path;

pub(in crate::pinning::management::wire::v3::history) struct Resumed {
    pub spool: Spool,
    pub source: source::Checkpoint,
    pub principal: String,
    pub created_at: String,
    pub budget: Budget,
    pub sealed: bool,
}
async fn connect(path: &Path, readonly: bool) -> Result<DatabaseConnection> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(path)
                .create_if_missing(false)
                .read_only(readonly),
        )
        .await
        .map_err(invalid)?;
    let db = sea_orm::SqlxSqliteConnector::from_sqlx_sqlite_pool(pool);
    db.execute_unprepared("PRAGMA trusted_schema=OFF; PRAGMA cache_size=-2048; PRAGMA temp_store=FILE; PRAGMA busy_timeout=1000;").await.map_err(invalid)?;
    Ok(db)
}
fn normalized(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<String>()
        .to_ascii_lowercase()
}
async fn schema(db: &DatabaseConnection) -> Result<()> {
    let rows=db.query_all(Statement::from_string(DatabaseBackend::Sqlite,
        "SELECT CASE WHEN length(CAST(sql AS BLOB))<=16384 THEN sql END AS sql FROM sqlite_schema WHERE substr(name,1,7)<>'sqlite_' LIMIT 32",
    )).await.map_err(invalid)?;
    let mut expected = SCHEMA.iter().map(|sql| normalized(sql)).collect::<Vec<_>>();
    if rows.len() != expected.len() {
        return Err(SafeError::InvalidInput);
    }
    for row in rows {
        let sql = get::<String>(&row, "sql")?;
        let index = expected
            .iter()
            .position(|value| *value == normalized(&sql))
            .ok_or(SafeError::InvalidInput)?;
        expected.swap_remove(index);
    }
    Ok(())
}
impl Spool {
    /// A caller-selected LOCAL task, not an imported management archive. Existing
    /// artifacts are retained on close; only newly created workspaces are cleaned.
    pub async fn resume(path: &Path, ceiling: Limits) -> Result<Resumed> {
        if ceiling.resident < 3 * 1024 * 1024 {
            return Err(SafeError::LimitExceeded);
        }
        let guard = files::stream::reopen_task(path).map_err(invalid)?;
        let db = connect(path, true).await?;
        let loaded=async {
            schema(&db).await?;
            let row=db.query_one(Statement::from_string(DatabaseBackend::Sqlite,
                "SELECT CASE WHEN length(CAST(source AS BLOB))<=65536 THEN source END AS source,CASE WHEN length(CAST(budget AS BLOB))<=4096 THEN budget END AS budget,sealed,CASE WHEN length(CAST(principal AS BLOB))<=4096 THEN principal END AS principal,CASE WHEN length(CAST(created_at AS BLOB))<=1024 THEN created_at END AS created_at FROM checkpoint WHERE id=1",
            )).await.map_err(invalid)?.ok_or(SafeError::InvalidInput)?;
            let state:i64=get(&row,"sealed")?;
            if !matches!(state,0|1) {return Err(SafeError::InvalidInput)}
            let source:source::Checkpoint=serde_json::from_str(&get::<String>(&row,"source")?).map_err(invalid)?;
            let checkpoint:BudgetCheckpoint=serde_json::from_str(&get::<String>(&row,"budget")?).map_err(invalid)?;
            let mut budget=Budget::resume(checkpoint,ceiling).map_err(storage)?;
            budget.charge(6,0,0,0).map_err(storage)?;
            let principal=get::<String>(&row,"principal")?;
            let created_at=get::<String>(&row,"created_at")?;
            if principal.is_empty() || created_at.is_empty() || state==1 && !source.complete() {return Err(SafeError::InvalidInput)}
            Ok((source,principal,created_at,budget,state==1))
        }.await;
        db.close().await.map_err(invalid)?;
        let (source, principal, created_at, mut budget, sealed) = loaded?;
        let memory = budget.reserve(3 * 1024 * 1024).map_err(storage)?;
        let db = connect(path, false).await?;
        Ok(Resumed {
            spool: Self {
                db,
                workspace: None,
                path: path.to_owned(),
                _guard: Some(guard),
                _memory: memory,
            },
            source,
            principal,
            created_at,
            budget,
            sealed,
        })
    }
    pub async fn suspend(self) -> Result<PathBuf> {
        self.db.close().await.map_err(invalid)?;
        // Dropping Workspace releases its parent guard without deleting the task.
        Ok(self.path)
    }
    pub async fn fail(&self) -> Result<()> {
        self.db
            .execute_unprepared("UPDATE checkpoint SET sealed=2 WHERE id=1")
            .await
            .map_err(invalid)?;
        Ok(())
    }
    pub async fn active(&self, budget: &mut Budget) -> Result<()> {
        exec(
            &self.db,
            "UPDATE checkpoint SET sealed=3 WHERE id=1",
            vec![],
            budget,
        )
        .await
    }
    pub async fn begin_verification(&self, budget: &mut Budget) -> Result<()> {
        exec(&self.db,"CREATE TEMP TABLE checked(table_name TEXT NOT NULL,sort_key BLOB NOT NULL,column_name TEXT NOT NULL,offset BIGINT NOT NULL,PRIMARY KEY(table_name,sort_key,column_name,offset)) WITHOUT ROWID",vec![],budget).await
    }
    pub async fn verify_part(&self, part: &source::Part, budget: &mut Budget) -> Result<()> {
        let key = sort_key(&part.key);
        let row = one(
            &self.db,
            "SELECT row_key FROM rows WHERE table_name=$1 AND sort_key=$2",
            vec![part.key.table.clone().into(), key.clone().into()],
            budget,
        )
        .await?
        .ok_or(SafeError::InvalidInput)?;
        if serde_json::from_str::<NativeKey>(&get::<String>(&row, "row_key")?).map_err(invalid)?
            != part.key
        {
            return Err(SafeError::InvalidInput);
        }
        exec(
            &self.db,
            "INSERT INTO checked VALUES($1,$2,'',-2) ON CONFLICT DO NOTHING",
            vec![part.key.table.clone().into(), key.clone().into()],
            budget,
        )
        .await?;
        let row = one(
            &self.db,
            "SELECT length FROM cells WHERE table_name=$1 AND sort_key=$2 AND column_name=$3",
            vec![
                part.key.table.clone().into(),
                key.clone().into(),
                part.column.clone().into(),
            ],
            budget,
        )
        .await?
        .ok_or(SafeError::InvalidInput)?;
        if get::<Option<i64>>(&row, "length")?
            .map(u64::try_from)
            .transpose()
            .map_err(invalid)?
            != part.length
        {
            return Err(SafeError::InvalidInput);
        }
        exec(
            &self.db,
            "INSERT INTO checked VALUES($1,$2,$3,-1) ON CONFLICT DO NOTHING",
            vec![
                part.key.table.clone().into(),
                key.clone().into(),
                part.column.clone().into(),
            ],
            budget,
        )
        .await?;
        if !part.bytes.is_empty() {
            let row=one(&self.db,"SELECT CASE WHEN length(payload)<=65536 THEN payload END AS payload FROM parts WHERE table_name=$1 AND sort_key=$2 AND column_name=$3 AND offset=$4",vec![part.key.table.clone().into(),key.clone().into(),part.column.clone().into(),i64::try_from(part.offset).map_err(invalid)?.into()],budget).await?.ok_or(SafeError::InvalidInput)?;
            let bytes: Vec<u8> = get(&row, "payload")?;
            budget
                .charge(0, 0, 0, bytes.len() as u64)
                .map_err(storage)?;
            if bytes != part.bytes {
                return Err(SafeError::InvalidInput);
            }
            exec(
                &self.db,
                "INSERT INTO checked VALUES($1,$2,$3,$4)",
                vec![
                    part.key.table.clone().into(),
                    key.into(),
                    part.column.clone().into(),
                    i64::try_from(part.offset).map_err(invalid)?.into(),
                ],
                budget,
            )
            .await?;
        }
        Ok(())
    }
    pub async fn finish_verification(&self, budget: &mut Budget) -> Result<()> {
        for sql in [
            "SELECT 1 FROM rows r WHERE NOT EXISTS(SELECT 1 FROM checked c WHERE c.table_name=r.table_name AND c.sort_key=r.sort_key AND c.column_name='' AND c.offset=-2) LIMIT 1",
            "SELECT 1 FROM cells r WHERE NOT EXISTS(SELECT 1 FROM checked c WHERE c.table_name=r.table_name AND c.sort_key=r.sort_key AND c.column_name=r.column_name AND c.offset=-1) LIMIT 1",
            "SELECT 1 FROM parts r WHERE NOT EXISTS(SELECT 1 FROM checked c WHERE c.table_name=r.table_name AND c.sort_key=r.sort_key AND c.column_name=r.column_name AND c.offset=r.offset) LIMIT 1",
        ] {
            if one(&self.db, sql, vec![], budget).await?.is_some() {
                return Err(SafeError::InvalidInput);
            }
        }
        exec(&self.db, "DROP TABLE checked", vec![], budget).await
    }
}
