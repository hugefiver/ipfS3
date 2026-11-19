use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, DatabaseTransaction};

use super::stmt;
use crate::error::{AppError, AppResult};

pub(super) async fn advance(
    tx: &DatabaseTransaction,
    provider: &str,
    cid: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let groups = tx
        .query_all(stmt(
            tx,
            "SELECT DISTINCT remaining_providers FROM pin_refs WHERE provider=$1 AND cid=$2
            AND expires_at>$3 AND remaining_providers!='[]'",
            vec![provider.into(), cid.into(), now.timestamp_micros().into()],
        ))
        .await?;
    for row in groups {
        let previous: String = row.try_get("", "remaining_providers")?;
        let candidates: Vec<String> = serde_json::from_str(&previous)
            .map_err(|_| AppError::Database("invalid remaining pin providers".into()))?;
        let mut destination = provider;
        let mut remaining = "[]".to_owned();
        for (index, next) in candidates.iter().enumerate() {
            tx.execute(stmt(tx,&format!("INSERT INTO pin_resources(provider,cid,source_cid,logical_size,status,action)
                 SELECT $3,cid,COALESCE(source_cid,cid),logical_size,'waiting','submit' FROM pin_resources WHERE provider=$1 AND cid=$2 {}",super::batch::ATTACH_RESOURCE),
                 vec![provider.into(),cid.into(),next.clone().into()])).await?;
            let resource = tx
                .query_one(stmt(
                    tx,
                    "SELECT status FROM pin_resources WHERE provider=$1 AND cid=$2",
                    vec![next.clone().into(), cid.into()],
                ))
                .await?
                .expect("candidate resource locked during settlement");
            if resource.try_get::<String>("", "status")? != "failed" {
                destination = next;
                remaining = serde_json::to_string(&candidates[index + 1..])
                    .expect("provider names serialize as strings");
                break;
            }
        }
        tx.execute(stmt(tx,
            "UPDATE pin_refs SET provider=$5,remaining_providers=$6 WHERE provider=$1 AND cid=$2 AND expires_at>$3 AND remaining_providers=$4",
            vec![provider.into(),cid.into(),now.timestamp_micros().into(),previous.into(),destination.into(),remaining.into()],
        )).await?;
    }
    Ok(())
}
