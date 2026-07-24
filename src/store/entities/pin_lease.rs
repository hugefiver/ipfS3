use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_leases")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub owner_object_id: String,
    pub source: String,
    pub policy_id: String,
    pub provider_mode: String,
    pub content_mode: String,
    pub created_at: DateTimeUtc,
    pub last_touched_at: DateTimeUtc,
    pub expires_at: DateTimeUtc,
    pub generation: i64,
    pub state: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::OwnerObjectId",
        to = "super::object::Column::Id"
    )]
    OwnerObject,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::OwnerObject.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
