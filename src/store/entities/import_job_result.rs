use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "import_job_results")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub job_id: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub sequence: i64,
    pub key: String,
    pub cid: Option<String>,
    pub size: Option<i64>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::import_job::Entity",
        from = "Column::JobId",
        to = "super::import_job::Column::Id"
    )]
    Job,
}

impl Related<super::import_job::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Job.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
