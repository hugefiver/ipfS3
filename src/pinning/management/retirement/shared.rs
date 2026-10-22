//! Provider domains have separate reducers over one protected original database.
use super::{
    spool::{exec, get, one},
    *,
};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement, TransactionTrait};

const RAW: &[&str] = &["parts", "members", "eof", "wire_keys"];

impl InventorySpool {
    pub(super) async fn share_original(
        &mut self,
        primary: &InventorySpool,
        route: RouteIdentity,
        fresh: bool,
    ) -> Result<()> {
        if self.path == primary.path || primary.shared_route.is_some() {
            return Err(ManagementError::InvalidEvidence);
        }
        if fresh {
            let tx = self
                .db
                .begin()
                .await
                .map_err(|_| ManagementError::Database)?;
            for table in RAW {
                exec(&tx, &format!("DROP TABLE {table}"), vec![]).await?;
            }
            exec(&tx, "UPDATE state SET checkpoint='null' WHERE id=1", vec![]).await?;
            tx.commit().await.map_err(|_| ManagementError::Database)?;
        }
        // The path is a caller-owned reservation, never a path read from JSON.
        let path = primary
            .path
            .to_str()
            .ok_or(ManagementError::InvalidEvidence)?;
        self.db
            .execute(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "ATTACH DATABASE $1 AS original",
                [path.into()],
            ))
            .await
            .map_err(|_| ManagementError::Database)?;
        for table in RAW {
            self.db
                .execute_unprepared(&format!(
                    "CREATE TEMP VIEW {table} AS SELECT * FROM original.{table}"
                ))
                .await
                .map_err(|_| ManagementError::Database)?;
        }
        self.shared_route = Some(route);
        self.shared_source = Some(primary.reference.clone());
        Ok(())
    }

    pub(super) async fn adopt_shared_scan(&self) -> Result<Progress> {
        let checkpoint = self.checkpoint().await?;
        if !checkpoint.complete() {
            return Ok(Progress::Pending);
        }
        let row = one(&self.db, "SELECT phase FROM state WHERE id=1", vec![])
            .await?
            .ok_or(ManagementError::InvalidEvidence)?;
        if get::<i64>(&row, "phase")? == 0 {
            exec(
                &self.db,
                "UPDATE state SET phase=1 WHERE id=1 AND phase=0",
                vec![],
            )
            .await?;
        }
        Ok(Progress::Archived)
    }
}
