use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "import_destinations")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub bucket: String,
    #[sea_orm(primary_key, auto_increment = false)]
    pub key: String,
    pub generation: i64,
    pub owner_job_id: Option<String>,
    pub mutation_id: Option<String>,
    pub mutation_prefix: Option<String>,
    pub updated_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::bucket::Entity",
        from = "Column::Bucket",
        to = "super::bucket::Column::Name"
    )]
    Bucket,
    #[sea_orm(
        belongs_to = "super::import_job::Entity",
        from = "Column::OwnerJobId",
        to = "super::import_job::Column::Id"
    )]
    OwnerJob,
}

impl Related<super::bucket::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Bucket.def()
    }
}

impl Related<super::import_job::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::OwnerJob.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
