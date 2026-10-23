use sea_orm::entity::prelude::*;
use sea_orm::{Iterable, JoinType, QueryFilter, QuerySelect, sea_query::Expr};

/// Joined execution projection. Request columns belong to import_job_requests;
/// intake writes are coordinated by the import store, not full-model updates.
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
    /// Standalone decision; ZIP captures own their source decision.
    pub pin_decision_json: Option<String>,
    pub zip_intent_json: Option<String>,
    pub decompress_prefix: Option<String>,
    /// Legacy standalone root option. New ZIP captures own this option.
    pub root_capture_json: Option<String>,
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
    #[sea_orm(has_one = "super::import_job_request::Entity")]
    Request,
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

impl Related<super::import_job_request::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Request.def()
    }
}

impl Column {
    pub fn is_request(&self) -> bool {
        matches!(
            self,
            Self::SourceValue
                | Self::ObjectContentType
                | Self::MetadataJson
                | Self::TagsJson
                | Self::PinDecisionJson
                | Self::ZipIntentJson
                | Self::DecompressPrefix
                | Self::RootCaptureJson
        )
    }
}

impl Entity {
    /// Full execution/response projection. Hot-only callers select their own
    /// columns from find_state and never join or read the frozen request.
    pub fn find() -> sea_orm::Select<Self> {
        let mut query = Self::find_state().select_only();
        for column in Column::iter() {
            query = if column.is_request() {
                query.column_as(
                    Expr::col((super::import_job_request::Entity, column)),
                    column,
                )
            } else {
                query.column(column)
            };
        }
        query.join(JoinType::InnerJoin, Relation::Request.def())
    }

    pub fn find_state() -> sea_orm::Select<Self> {
        <Self as EntityTrait>::find()
    }

    pub fn find_by_id(id: impl Into<String>) -> sea_orm::Select<Self> {
        Self::find().filter(Column::Id.eq(id.into()))
    }
}
