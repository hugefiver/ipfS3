use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "pin_extension_decisions")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub version_row_id: String,
    pub object_id: String,
    pub control_revision: String,
    pub config_revision: String,
    pub effect: String,
    pub snapshot: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object_version::Entity",
        from = "Column::VersionRowId",
        to = "super::object_version::Column::Id"
    )]
    Version,
}

impl Related<super::object_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Version.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
