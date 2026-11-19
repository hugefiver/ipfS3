#![allow(dead_code)]
use sea_orm::{ConnectionTrait, FromQueryResult, Statement};

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
pub struct Ref {
    pub owner_object_id: String,
    pub provider: String,
    pub cid: String,
    pub source: String,
    pub policy_id: String,
    pub content_mode: String,
    pub expires_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, FromQueryResult)]
pub struct Resource {
    pub provider: String,
    pub cid: String,
    pub logical_size: i64,
    pub status: String,
    pub action: Option<String>,
    pub handle: Option<String>,
}

pub async fn refs(db: &impl ConnectionTrait) -> Vec<Ref> {
    Ref::find_by_statement(Statement::from_string(db.get_database_backend(),
        "SELECT owner_object_id,provider,cid,source,policy_id,content_mode,expires_at FROM pin_refs ORDER BY owner_object_id,provider,cid,source,policy_id,content_mode"))
        .all(db).await.unwrap()
}

pub async fn resources(db: &impl ConnectionTrait) -> Vec<Resource> {
    Resource::find_by_statement(Statement::from_string(db.get_database_backend(),
        "SELECT provider,cid,logical_size,status,action,handle FROM pin_resources ORDER BY provider,cid"))
        .all(db).await.unwrap()
}

pub async fn count(db: &impl ConnectionTrait, table: &str) -> u64 {
    db.query_one(Statement::from_string(
        db.get_database_backend(),
        format!("SELECT COUNT(*) AS n FROM {table}"),
    ))
    .await
    .unwrap()
    .unwrap()
    .try_get::<i64>("", "n")
    .unwrap() as u64
}
