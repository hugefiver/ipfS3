use std::collections::BTreeSet;

use sea_orm::{ConnectionTrait, EntityTrait, Set, entity::prelude::*, sea_query::OnConflict};

use crate::error::AppResult;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_cid_comparisons")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub cid: String,
    pub canonical_cid: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

pub(crate) async fn ensure_cid_comparisons<C, I, S>(db: &C, cids: I) -> AppResult<()>
where
    C: ConnectionTrait,
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let rows = cids
        .into_iter()
        .map(|cid| cid.as_ref().to_owned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|cid| ActiveModel {
            canonical_cid: Set(crate::pinning::provider::canonical_resource_cid(&cid).ok()),
            cid: Set(cid),
        })
        .collect::<Vec<_>>();
    for chunk in rows.chunks(400) {
        Entity::insert_many(chunk.iter().cloned())
            .on_conflict(OnConflict::column(Column::Cid).do_nothing().to_owned())
            .exec_without_returning(db)
            .await?;
    }
    Ok(())
}
