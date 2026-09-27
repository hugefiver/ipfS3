use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "import_jobs")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub source_type: String,
    pub source_value: String,
    pub request_fingerprint: String,
    pub client_token: Option<String>,
    pub object_content_type: Option<String>,
    pub metadata_json: String,
    pub tags_json: String,
    /// Captured at authenticated submission; NULL only for pre-migration jobs.
    pub pin_decision_json: Option<String>,
    pub decompress_prefix: Option<String>,
    pub state: String,
    pub phase: String,
    pub attempts: i32,
    pub next_attempt_at: DateTimeUtc,
    pub locked_by: Option<String>,
    pub locked_until: Option<DateTimeUtc>,
    pub claim_epoch: i64,
    pub providers_observed: i64,
    pub pin_nodes_processed: i64,
    pub pin_bytes_processed: i64,
    pub downloaded_bytes: i64,
    pub download_total: Option<i64>,
    pub ipfs_add_bytes: i64,
    pub logical_size: Option<i64>,
    pub entries_processed: i64,
    pub entries_succeeded: i64,
    pub entries_failed: i64,
    pub decompressed_bytes: i64,
    pub final_cid: Option<String>,
    pub failure_code: Option<String>,
    pub failure_message: Option<String>,
    pub created_at: DateTimeUtc,
    pub updated_at: DateTimeUtc,
    pub completed_at: Option<DateTimeUtc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::bucket::Entity",
        from = "Column::Bucket",
        to = "super::bucket::Column::Name"
    )]
    Bucket,
}

impl Related<super::bucket::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Bucket.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
