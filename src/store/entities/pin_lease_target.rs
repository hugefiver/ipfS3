use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_lease_targets")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub lease_id: String,
    pub cid: String,
    pub logical_size: i64,
    pub provider: String,
    pub state: String,
    pub created_at: DateTimeUtc,
    pub last_touched_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::pin_lease::Entity",
        from = "Column::LeaseId",
        to = "super::pin_lease::Column::Id"
    )]
    Lease,
}

impl Related<super::pin_lease::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Lease.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
