use chrono::{DateTime, Utc};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseBackend, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, Set, sea_query::Expr,
};

use crate::error::{AppError, AppResult};

use super::{
    bucket,
    entities::{object, object_version},
};

pub const NULL_VERSION_ID: &str = "null";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BucketVersioningState {
    Unversioned,
    Enabled,
    Suspended,
}

impl BucketVersioningState {
    pub fn from_db_value(value: Option<&str>) -> AppResult<Self> {
        match value {
            None => Ok(Self::Unversioned),
            Some("Enabled") => Ok(Self::Enabled),
            Some("Suspended") => Ok(Self::Suspended),
            Some(_) => Err(AppError::Internal(
                "invalid bucket versioning state in database".to_owned(),
            )),
        }
    }

    pub fn as_db_value(self) -> Option<&'static str> {
        match self {
            Self::Unversioned => None,
            Self::Enabled => Some("Enabled"),
            Self::Suspended => Some("Suspended"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicVersionId {
    Null,
    Opaque(String),
}

impl PublicVersionId {
    pub fn parse_s3(value: &str) -> AppResult<Self> {
        if value == NULL_VERSION_ID {
            return Ok(Self::Null);
        }

        let uuid = uuid::Uuid::parse_str(value).map_err(|_| {
            AppError::InvalidArgument("version ID must be `null` or a canonical UUID".to_owned())
        })?;
        if uuid.to_string() != value {
            return Err(AppError::InvalidArgument(
                "version ID must be `null` or a canonical UUID".to_owned(),
            ));
        }
        Ok(Self::Opaque(value.to_owned()))
    }

    pub fn as_s3_str(&self) -> &str {
        match self {
            Self::Null => NULL_VERSION_ID,
            Self::Opaque(value) => value,
        }
    }

    pub fn as_db_value(&self) -> Option<String> {
        match self {
            Self::Null => None,
            Self::Opaque(value) => Some(value.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionKind {
    Object,
    DeleteMarker,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VersionSelector {
    Current,
    Exact(PublicVersionId),
}

#[derive(Clone, Debug)]
pub struct ResolvedVersion {
    pub key: String,
    pub public_version_id: String,
    pub kind: VersionKind,
    pub object: Option<object::Model>,
    pub is_latest: bool,
    pub sequence: i64,
    pub created_at: DateTime<Utc>,
}

impl ResolvedVersion {
    pub fn new(
        key: String,
        public_version_id: String,
        kind: VersionKind,
        object: Option<object::Model>,
        is_latest: bool,
        sequence: i64,
        created_at: DateTime<Utc>,
    ) -> AppResult<Self> {
        if matches!(kind, VersionKind::Object) != object.is_some() {
            return Err(invalid_version_index());
        }
        Ok(Self {
            key,
            public_version_id,
            kind,
            object,
            is_latest,
            sequence,
            created_at,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VersionCursor {
    pub key: String,
    pub sequence: i64,
    pub public_version_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationResult {
    pub object_id: String,
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteVersionResult {
    pub version_id: Option<String>,
    pub deleted_delete_marker: bool,
    pub created_delete_marker: bool,
}

fn invalid_version_index() -> AppError {
    AppError::Internal("invalid object version index row".to_owned())
}

fn no_such_key(bucket_name: &str, key: &str) -> AppError {
    AppError::NoSuchKey(format!("{bucket_name}/{key}"))
}

fn no_such_version(bucket_name: &str, key: &str, version_id: &PublicVersionId) -> AppError {
    AppError::NoSuchVersion {
        bucket: bucket_name.to_owned(),
        key: key.to_owned(),
        version_id: version_id.as_s3_str().to_owned(),
    }
}

fn version_kind(row: &object_version::Model) -> AppResult<VersionKind> {
    match row.kind.as_str() {
        "object" if row.object_id.is_some() => Ok(VersionKind::Object),
        "delete_marker" if row.object_id.is_none() => Ok(VersionKind::DeleteMarker),
        _ => Err(invalid_version_index()),
    }
}

fn public_version_id(row: &object_version::Model) -> AppResult<PublicVersionId> {
    match row.version_id.as_deref() {
        None => Ok(PublicVersionId::Null),
        Some(value) => PublicVersionId::parse_s3(value).map_err(|_| invalid_version_index()),
    }
}

fn selector_db_value(selector: &PublicVersionId) -> AppResult<Option<String>> {
    match selector {
        PublicVersionId::Null => Ok(None),
        PublicVersionId::Opaque(value) => Ok(Some(
            PublicVersionId::parse_s3(value)?.as_s3_str().to_owned(),
        )),
    }
}

async fn one_version<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    selector: &VersionSelector,
    locked: bool,
) -> AppResult<Option<object_version::Model>> {
    let mut query = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(bucket_name))
        .filter(object_version::Column::Key.eq(key));
    match selector {
        VersionSelector::Current => {
            query = query.filter(object_version::Column::IsLatest.eq(true));
        }
        VersionSelector::Exact(public_version_id) => match selector_db_value(public_version_id)? {
            Some(version_id) => {
                query = query.filter(object_version::Column::VersionId.eq(version_id));
            }
            None => {
                query = query.filter(object_version::Column::VersionId.is_null());
            }
        },
    }

    let row = if locked && db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    };
    Ok(row)
}

async fn resolved_from_row<C: ConnectionTrait>(
    db: &C,
    row: object_version::Model,
    validate_current_projection: bool,
) -> AppResult<ResolvedVersion> {
    let kind = version_kind(&row)?;
    let version_id = public_version_id(&row)?.as_s3_str().to_owned();
    match kind {
        VersionKind::Object => {
            let object_id = row.object_id.as_deref().ok_or_else(invalid_version_index)?;
            let object = super::object::get_by_id(db, object_id)
                .await
                .map_err(|_| invalid_version_index())?;
            if object.bucket != row.bucket || object.key != row.key {
                return Err(invalid_version_index());
            }
            if validate_current_projection && (!row.is_latest || !object.is_latest) {
                return Err(invalid_version_index());
            }
            ResolvedVersion::new(
                row.key,
                version_id,
                kind,
                Some(object),
                row.is_latest,
                row.sequence,
                row.created_at,
            )
        }
        VersionKind::DeleteMarker => {
            if validate_current_projection {
                if !row.is_latest {
                    return Err(invalid_version_index());
                }
                let current_count = object::Entity::find()
                    .filter(object::Column::Bucket.eq(&row.bucket))
                    .filter(object::Column::Key.eq(&row.key))
                    .filter(object::Column::IsLatest.eq(true))
                    .count(db)
                    .await?;
                if current_count != 0 {
                    return Err(invalid_version_index());
                }
            }
            ResolvedVersion::new(
                row.key,
                version_id,
                kind,
                None,
                row.is_latest,
                row.sequence,
                row.created_at,
            )
        }
    }
}

async fn resolve_with_lock<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    selector: &VersionSelector,
    locked: bool,
    lock_versioning_state: bool,
) -> AppResult<ResolvedVersion> {
    let state = if locked && lock_versioning_state {
        bucket::lock_versioning_state(db, bucket_name).await?
    } else {
        bucket::get_versioning_state(db, bucket_name).await?
    };

    if state == BucketVersioningState::Unversioned {
        return match selector {
            VersionSelector::Current => {
                let object = super::object::get_latest(db, bucket_name, key).await?;
                ResolvedVersion::new(
                    object.key.clone(),
                    NULL_VERSION_ID.to_owned(),
                    VersionKind::Object,
                    Some(object.clone()),
                    true,
                    0,
                    object.created_at,
                )
            }
            VersionSelector::Exact(_) => Err(AppError::InvalidArgument(
                "version IDs are unavailable for an unversioned bucket".to_owned(),
            )),
        };
    }

    let row = one_version(db, bucket_name, key, selector, locked).await?;
    let row = match (selector, row) {
        (VersionSelector::Current, Some(row)) => row,
        (VersionSelector::Current, None) => return Err(no_such_key(bucket_name, key)),
        (VersionSelector::Exact(_), Some(row)) => row,
        (VersionSelector::Exact(version_id), None) => {
            return Err(no_such_version(bucket_name, key, version_id));
        }
    };
    resolved_from_row(db, row, matches!(selector, VersionSelector::Current)).await
}

pub async fn resolve_version<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    selector: &VersionSelector,
) -> AppResult<ResolvedVersion> {
    resolve_with_lock(db, bucket_name, key, selector, false, false).await
}

/// Locks the selected version row without taking SQLite's bucket-row write
/// intent. Callers that mutate metadata can re-resolve before their final
/// write; this keeps the lock scope on the immutable selected object and lets
/// the revalidation observe an intervening publication or exact deletion.
pub async fn lock_resolved_version<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    selector: &VersionSelector,
) -> AppResult<ResolvedVersion> {
    resolve_with_lock(db, bucket_name, key, selector, true, false).await
}

pub(crate) async fn lock_current_row<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
) -> AppResult<Option<object_version::Model>> {
    one_version(db, bucket_name, key, &VersionSelector::Current, true).await
}

/// Locks one exact public version row after the caller has locked the bucket
/// versioning state. The optional result lets simple suspended/unversioned
/// transitions probe the database-null slot without turning absence into an
/// exact-delete error.
pub(crate) async fn lock_indexed_version<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    version_id: &PublicVersionId,
) -> AppResult<Option<(object_version::Model, ResolvedVersion)>> {
    let row = one_version(
        db,
        bucket_name,
        key,
        &VersionSelector::Exact(version_id.clone()),
        true,
    )
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let resolved = resolved_from_row(db, row.clone(), false).await?;
    Ok(Some((row, resolved)))
}

pub(crate) async fn allocate_next_sequence<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
) -> AppResult<i64> {
    let query = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(bucket_name))
        .filter(object_version::Column::Key.eq(key));
    let rows = if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().all(db).await?
    } else {
        query.all(db).await?
    };
    let current_max = rows.into_iter().map(|row| row.sequence).max().unwrap_or(0);
    current_max
        .checked_add(1)
        .ok_or_else(|| AppError::Internal("object version sequence overflow".to_owned()))
}

async fn demote_current_version<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
) -> AppResult<()> {
    let result = object_version::Entity::update_many()
        .col_expr(object_version::Column::IsLatest, Expr::value(false))
        .filter(object_version::Column::Bucket.eq(bucket_name))
        .filter(object_version::Column::Key.eq(key))
        .filter(object_version::Column::IsLatest.eq(true))
        .exec(db)
        .await?;
    if result.rows_affected > 1 {
        return Err(invalid_version_index());
    }
    Ok(())
}

pub(crate) async fn remove_null_slot<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
) -> AppResult<()> {
    let result = object_version::Entity::delete_many()
        .filter(object_version::Column::Bucket.eq(bucket_name))
        .filter(object_version::Column::Key.eq(key))
        .filter(object_version::Column::VersionId.is_null())
        .exec(db)
        .await?;
    if result.rows_affected > 1 {
        return Err(invalid_version_index());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn insert_version_row<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    key: &str,
    version_id: PublicVersionId,
    kind: VersionKind,
    object_id: Option<String>,
    sequence: i64,
    now: DateTime<Utc>,
) -> AppResult<()> {
    if matches!(kind, VersionKind::Object) != object_id.is_some() {
        return Err(invalid_version_index());
    }
    let kind = match kind {
        VersionKind::Object => "object",
        VersionKind::DeleteMarker => "delete_marker",
    };
    object_version::Entity::insert(object_version::ActiveModel {
        id: Set(uuid::Uuid::new_v4().to_string()),
        bucket: Set(bucket_name.to_owned()),
        key: Set(key.to_owned()),
        version_id: Set(version_id.as_db_value()),
        kind: Set(kind.to_owned()),
        object_id: Set(object_id),
        sequence: Set(sequence),
        is_latest: Set(true),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .exec(db)
    .await?;
    Ok(())
}

async fn prepare_install<C: ConnectionTrait>(
    db: &C,
    state: BucketVersioningState,
    bucket_name: &str,
    key: &str,
) -> AppResult<(i64, PublicVersionId)> {
    let _ = lock_current_row(db, bucket_name, key).await?;
    let sequence = allocate_next_sequence(db, bucket_name, key).await?;
    let version_id = match state {
        BucketVersioningState::Enabled => {
            demote_current_version(db, bucket_name, key).await?;
            PublicVersionId::Opaque(uuid::Uuid::new_v4().to_string())
        }
        BucketVersioningState::Suspended | BucketVersioningState::Unversioned => {
            demote_current_version(db, bucket_name, key).await?;
            remove_null_slot(db, bucket_name, key).await?;
            PublicVersionId::Null
        }
    };
    Ok((sequence, version_id))
}

pub(crate) async fn install_content_version<C: ConnectionTrait>(
    db: &C,
    state: BucketVersioningState,
    object: &object::Model,
    now: DateTime<Utc>,
) -> AppResult<String> {
    let (sequence, version_id) = prepare_install(db, state, &object.bucket, &object.key).await?;
    let public_version_id = version_id.as_s3_str().to_owned();
    insert_version_row(
        db,
        &object.bucket,
        &object.key,
        version_id,
        VersionKind::Object,
        Some(object.id.clone()),
        sequence,
        now,
    )
    .await?;
    super::object::set_only_latest(db, &object.bucket, &object.key, Some(&object.id)).await?;
    Ok(public_version_id)
}

pub(crate) async fn install_delete_marker<C: ConnectionTrait>(
    db: &C,
    state: BucketVersioningState,
    bucket_name: &str,
    key: &str,
    now: DateTime<Utc>,
) -> AppResult<String> {
    let (sequence, version_id) = prepare_install(db, state, bucket_name, key).await?;
    let public_version_id = version_id.as_s3_str().to_owned();
    insert_version_row(
        db,
        bucket_name,
        key,
        version_id,
        VersionKind::DeleteMarker,
        None,
        sequence,
        now,
    )
    .await?;
    super::object::set_only_latest(db, bucket_name, key, None).await?;
    Ok(public_version_id)
}

async fn lock_version_by_id<C: ConnectionTrait>(
    db: &C,
    id: &str,
) -> AppResult<Option<object_version::Model>> {
    let query = object_version::Entity::find_by_id(id.to_owned());
    let row = if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    };
    Ok(row)
}

pub(crate) async fn remove_and_promote<C: ConnectionTrait>(
    db: &C,
    selected: &object_version::Model,
) -> AppResult<Option<ResolvedVersion>> {
    let selected =
        lock_version_by_id(db, &selected.id)
            .await?
            .ok_or_else(|| AppError::NoSuchVersion {
                bucket: selected.bucket.clone(),
                key: selected.key.clone(),
                version_id: selected
                    .version_id
                    .clone()
                    .unwrap_or_else(|| NULL_VERSION_ID.to_owned()),
            })?;
    let _ = version_kind(&selected)?;

    let deleted = object_version::Entity::delete_by_id(selected.id.clone())
        .exec(db)
        .await?;
    if deleted.rows_affected != 1 {
        return Err(AppError::NoSuchVersion {
            bucket: selected.bucket.clone(),
            key: selected.key.clone(),
            version_id: selected
                .version_id
                .clone()
                .unwrap_or_else(|| NULL_VERSION_ID.to_owned()),
        });
    }
    if !selected.is_latest {
        return Ok(None);
    }

    let query = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(&selected.bucket))
        .filter(object_version::Column::Key.eq(&selected.key))
        .order_by_desc(object_version::Column::Sequence)
        .order_by_asc(object_version::Column::VersionId);
    let promoted = if db.get_database_backend() == DatabaseBackend::Postgres {
        query.lock_exclusive().one(db).await?
    } else {
        query.one(db).await?
    };

    let Some(mut promoted) = promoted else {
        super::object::set_only_latest(db, &selected.bucket, &selected.key, None).await?;
        return Ok(None);
    };

    let cleared = object_version::Entity::update_many()
        .col_expr(object_version::Column::IsLatest, Expr::value(false))
        .filter(object_version::Column::Bucket.eq(&selected.bucket))
        .filter(object_version::Column::Key.eq(&selected.key))
        .filter(object_version::Column::IsLatest.eq(true))
        .exec(db)
        .await?;
    if cleared.rows_affected > 1 {
        return Err(invalid_version_index());
    }
    let made_latest = object_version::Entity::update_many()
        .col_expr(object_version::Column::IsLatest, Expr::value(true))
        .filter(object_version::Column::Id.eq(&promoted.id))
        .filter(object_version::Column::IsLatest.eq(false))
        .exec(db)
        .await?;
    if made_latest.rows_affected != 1 {
        return Err(invalid_version_index());
    }
    promoted.is_latest = true;

    match version_kind(&promoted)? {
        VersionKind::Object => {
            let object_id = promoted
                .object_id
                .as_deref()
                .ok_or_else(invalid_version_index)?;
            super::object::set_only_latest(db, &selected.bucket, &selected.key, Some(object_id))
                .await?;
        }
        VersionKind::DeleteMarker => {
            super::object::set_only_latest(db, &selected.bucket, &selected.key, None).await?;
        }
    }
    resolved_from_row(db, promoted, true).await.map(Some)
}

const KEY_BOUNDARY_SEQUENCE: i64 = i64::MIN;

fn key_boundary_cursor(key: &str) -> VersionCursor {
    VersionCursor {
        key: key.to_owned(),
        sequence: KEY_BOUNDARY_SEQUENCE,
        public_version_id: String::new(),
    }
}

fn is_key_boundary(cursor: &VersionCursor) -> bool {
    cursor.sequence == KEY_BOUNDARY_SEQUENCE && cursor.public_version_id.is_empty()
}

pub async fn validate_version_cursor<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    prefix: &str,
    key_marker: Option<&str>,
    version_id_marker: Option<&str>,
) -> AppResult<Option<VersionCursor>> {
    let state = bucket::get_versioning_state(db, bucket_name).await?;
    match (key_marker, version_id_marker) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(AppError::InvalidArgument(
            "version-id-marker requires key-marker".to_owned(),
        )),
        (Some(key), None) => Ok(Some(key_boundary_cursor(key))),
        (Some(key), Some(version_id_marker)) => {
            if state == BucketVersioningState::Unversioned {
                return Err(AppError::InvalidArgument(
                    "version cursor cannot address hidden unversioned rows".to_owned(),
                ));
            }
            if !key.starts_with(prefix) {
                return Err(AppError::InvalidArgument(
                    "version cursor is outside the requested prefix".to_owned(),
                ));
            }
            let selector = VersionSelector::Exact(PublicVersionId::parse_s3(version_id_marker)?);
            let Some(row) = one_version(db, bucket_name, key, &selector, false).await? else {
                return Err(AppError::InvalidArgument(
                    "version cursor does not resolve to a visible version".to_owned(),
                ));
            };
            let parsed = public_version_id(&row).map_err(|_| {
                AppError::InvalidArgument("version cursor is not a visible version".to_owned())
            })?;
            Ok(Some(VersionCursor {
                key: row.key,
                sequence: row.sequence,
                public_version_id: parsed.as_s3_str().to_owned(),
            }))
        }
    }
}

pub async fn scan_versions<C: ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    prefix: &str,
    cursor: Option<&VersionCursor>,
    limit: u64,
) -> AppResult<Vec<ResolvedVersion>> {
    let state = bucket::get_versioning_state(db, bucket_name).await?;
    if state == BucketVersioningState::Unversioned || limit == 0 {
        return Ok(Vec::new());
    }

    let mut query = object_version::Entity::find()
        .filter(object_version::Column::Bucket.eq(bucket_name))
        .filter(object_version::Column::Key.starts_with(prefix));
    if let Some(cursor) = cursor {
        if is_key_boundary(cursor) {
            query = query.filter(object_version::Column::Key.gt(&cursor.key));
        } else {
            let same_key_after_cursor = if cursor.public_version_id == NULL_VERSION_ID {
                Condition::any()
                    .add(object_version::Column::Sequence.lt(cursor.sequence))
                    .add(
                        Condition::all()
                            .add(object_version::Column::Sequence.eq(cursor.sequence))
                            .add(object_version::Column::VersionId.is_null()),
                    )
            } else {
                let version_id = PublicVersionId::parse_s3(&cursor.public_version_id)?;
                Condition::any()
                    .add(object_version::Column::Sequence.lt(cursor.sequence))
                    .add(
                        Condition::all()
                            .add(object_version::Column::Sequence.eq(cursor.sequence))
                            .add(object_version::Column::VersionId.gte(version_id.as_s3_str())),
                    )
            };
            query = query.filter(
                Condition::any()
                    .add(object_version::Column::Key.gt(&cursor.key))
                    .add(
                        Condition::all()
                            .add(object_version::Column::Key.eq(&cursor.key))
                            .add(same_key_after_cursor),
                    ),
            );
        }
    }

    let rows = query
        .order_by_asc(object_version::Column::Key)
        .order_by_desc(object_version::Column::Sequence)
        .order_by_asc(object_version::Column::VersionId)
        .limit(limit)
        .all(db)
        .await?;
    let mut versions = Vec::with_capacity(rows.len());
    for row in rows {
        versions.push(resolved_from_row(db, row, false).await?);
    }
    Ok(versions)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use sea_orm::{
        ColumnTrait, ConnectionTrait, Database, EntityTrait, QueryFilter, QueryOrder,
        TransactionTrait,
    };
    use uuid::Uuid;

    use super::{
        BucketVersioningState, NULL_VERSION_ID, PublicVersionId, VersionSelector,
        allocate_next_sequence, install_content_version, install_delete_marker, lock_current_row,
        remove_and_promote, resolve_version, scan_versions, validate_version_cursor,
    };
    use crate::{
        error::AppError,
        store::{
            bucket,
            entities::{object, object_version},
            object as object_store, run_migrations,
        },
    };

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        run_migrations(&db).await.unwrap();
        bucket::create(&db, "versioned", None).await.unwrap();
        db
    }

    async fn insert_object(db: &sea_orm::DatabaseConnection, id: &str, key: &str) -> object::Model {
        object_store::upsert(
            db,
            id,
            "versioned",
            key,
            &format!("Qm{id}"),
            7,
            None,
            &format!("Qm{id}"),
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        object_store::get_by_id(db, id).await.unwrap()
    }

    async fn install(
        db: &sea_orm::DatabaseConnection,
        state: BucketVersioningState,
        object: object::Model,
    ) -> String {
        db.transaction(move |txn| {
            Box::pin(async move { install_content_version(txn, state, &object, Utc::now()).await })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn bucket_state_null_enabled_suspended_round_trip() {
        let db = setup().await;
        assert_eq!(
            bucket::get_versioning_state(&db, "versioned")
                .await
                .unwrap(),
            BucketVersioningState::Unversioned
        );

        bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Enabled)
            .await
            .unwrap();
        assert_eq!(
            bucket::get_versioning_state(&db, "versioned")
                .await
                .unwrap(),
            BucketVersioningState::Enabled
        );

        bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Suspended)
            .await
            .unwrap();
        assert_eq!(
            bucket::get_versioning_state(&db, "versioned")
                .await
                .unwrap(),
            BucketVersioningState::Suspended
        );
        assert!(matches!(
            bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Unversioned)
                .await,
            Err(AppError::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn bucket_state_rejects_corrupt_database_value() {
        let db = setup().await;
        db.execute_unprepared("PRAGMA ignore_check_constraints = ON")
            .await
            .unwrap();
        db.execute_unprepared(
            "UPDATE buckets SET versioning_status = 'broken' WHERE name = 'versioned'",
        )
        .await
        .unwrap();
        db.execute_unprepared("PRAGMA ignore_check_constraints = OFF")
            .await
            .unwrap();

        assert!(matches!(
            bucket::get_versioning_state(&db, "versioned").await,
            Err(AppError::Internal(_))
        ));
    }

    #[test]
    fn public_version_id_accepts_null_or_canonical_uuid_only() {
        let canonical = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        assert_eq!(
            PublicVersionId::parse_s3(NULL_VERSION_ID).unwrap(),
            PublicVersionId::Null
        );
        assert_eq!(
            PublicVersionId::parse_s3(canonical).unwrap().as_s3_str(),
            canonical
        );
        for invalid in [
            "",
            "NULL",
            "f47ac10b58cc4372a5670e02b2c3d479",
            "F47AC10B-58CC-4372-A567-0E02B2C3D479",
            "not-a-version",
        ] {
            assert!(matches!(
                PublicVersionId::parse_s3(invalid),
                Err(AppError::InvalidArgument(_))
            ));
        }
    }

    #[tokio::test]
    async fn current_and_exact_selectors_never_expose_legacy_nonlatest() {
        let db = setup().await;
        let legacy_id = Uuid::new_v4().to_string();
        let current_id = Uuid::new_v4().to_string();
        insert_object(&db, &legacy_id, "key").await;
        let current = insert_object(&db, &current_id, "key").await;
        bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Enabled)
            .await
            .unwrap();
        let current_version_id = install(&db, BucketVersioningState::Enabled, current).await;

        let resolved = resolve_version(&db, "versioned", "key", &VersionSelector::Current)
            .await
            .unwrap();
        assert_eq!(resolved.public_version_id, current_version_id);
        assert_eq!(resolved.object.unwrap().id, current_id);
        assert!(matches!(
            resolve_version(
                &db,
                "versioned",
                "key",
                &VersionSelector::Exact(PublicVersionId::Opaque(legacy_id)),
            )
            .await,
            Err(AppError::NoSuchVersion { .. })
        ));
    }

    #[tokio::test]
    async fn locked_next_sequence_and_null_slot_obey_constraints() {
        let db = setup().await;
        bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Suspended)
            .await
            .unwrap();
        let first = insert_object(&db, &Uuid::new_v4().to_string(), "key").await;
        assert_eq!(
            install(&db, BucketVersioningState::Suspended, first).await,
            NULL_VERSION_ID
        );

        let next = db
            .transaction(|txn| {
                Box::pin(async move {
                    lock_current_row(txn, "versioned", "key").await?;
                    allocate_next_sequence(txn, "versioned", "key").await
                })
            })
            .await
            .unwrap();
        assert_eq!(next, 2);

        let second = insert_object(&db, &Uuid::new_v4().to_string(), "key").await;
        assert_eq!(
            install(&db, BucketVersioningState::Suspended, second).await,
            NULL_VERSION_ID
        );
        let rows = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("versioned"))
            .filter(object_version::Column::Key.eq("key"))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].version_id.is_none());
        assert_eq!(rows[0].sequence, 2);
        assert!(rows[0].is_latest);
    }

    #[tokio::test]
    async fn suspended_install_demotes_opaque_current_and_replaces_only_null_slot() {
        let db = setup().await;
        bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Enabled)
            .await
            .unwrap();
        let first = insert_object(&db, &Uuid::new_v4().to_string(), "key").await;
        let first_version_id = install(&db, BucketVersioningState::Enabled, first.clone()).await;

        bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Suspended)
            .await
            .unwrap();
        let second = insert_object(&db, &Uuid::new_v4().to_string(), "key").await;
        assert_eq!(
            install(&db, BucketVersioningState::Suspended, second.clone()).await,
            NULL_VERSION_ID
        );

        let first_resolved = resolve_version(
            &db,
            "versioned",
            "key",
            &VersionSelector::Exact(PublicVersionId::parse_s3(&first_version_id).unwrap()),
        )
        .await
        .unwrap();
        assert_eq!(first_resolved.object.unwrap().id, first.id);

        let after_second = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("versioned"))
            .filter(object_version::Column::Key.eq("key"))
            .order_by_asc(object_version::Column::Sequence)
            .all(&db)
            .await
            .unwrap();
        assert_eq!(after_second.len(), 2);
        assert_eq!(
            after_second[0].object_id.as_deref(),
            Some(first.id.as_str())
        );
        assert!(after_second[0].version_id.is_some());
        assert!(!after_second[0].is_latest);
        assert_eq!(
            after_second[1].object_id.as_deref(),
            Some(second.id.as_str())
        );
        assert!(after_second[1].version_id.is_none());
        assert!(after_second[1].is_latest);

        let third = insert_object(&db, &Uuid::new_v4().to_string(), "key").await;
        assert_eq!(
            install(&db, BucketVersioningState::Suspended, third.clone()).await,
            NULL_VERSION_ID
        );
        let after_third = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("versioned"))
            .filter(object_version::Column::Key.eq("key"))
            .order_by_asc(object_version::Column::Sequence)
            .all(&db)
            .await
            .unwrap();
        assert_eq!(after_third.len(), 2);
        assert_eq!(after_third[0].object_id.as_deref(), Some(first.id.as_str()));
        assert!(after_third[0].version_id.is_some());
        assert!(!after_third[0].is_latest);
        assert_eq!(after_third[1].object_id.as_deref(), Some(third.id.as_str()));
        assert!(after_third[1].version_id.is_none());
        assert!(after_third[1].is_latest);
        assert_eq!(after_third.iter().filter(|row| row.is_latest).count(), 1);
        assert_eq!(
            after_third
                .iter()
                .filter(|row| row.version_id.is_none())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn delete_marker_replaces_the_current_object_projection() {
        let db = setup().await;
        bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Enabled)
            .await
            .unwrap();
        let object = insert_object(&db, &Uuid::new_v4().to_string(), "key").await;
        install(&db, BucketVersioningState::Enabled, object).await;
        let marker_version_id = db
            .transaction(|txn| {
                Box::pin(async move {
                    install_delete_marker(
                        txn,
                        BucketVersioningState::Enabled,
                        "versioned",
                        "key",
                        Utc::now(),
                    )
                    .await
                })
            })
            .await
            .unwrap();

        assert!(matches!(
            PublicVersionId::parse_s3(&marker_version_id),
            Ok(PublicVersionId::Opaque(_))
        ));
        let current = resolve_version(&db, "versioned", "key", &VersionSelector::Current)
            .await
            .unwrap();
        assert_eq!(current.public_version_id, marker_version_id);
        assert!(current.object.is_none());
    }

    #[tokio::test]
    async fn scan_orders_versions_and_preserves_inclusive_cursor_rows() {
        let db = setup().await;
        bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Enabled)
            .await
            .unwrap();
        let first = insert_object(&db, &Uuid::new_v4().to_string(), "alpha").await;
        let first_version_id = install(&db, BucketVersioningState::Enabled, first).await;
        let second = insert_object(&db, &Uuid::new_v4().to_string(), "alpha").await;
        let second_version_id = install(&db, BucketVersioningState::Enabled, second).await;
        let marker_version_id = db
            .transaction(|txn| {
                Box::pin(async move {
                    install_delete_marker(
                        txn,
                        BucketVersioningState::Enabled,
                        "versioned",
                        "alpha",
                        Utc::now(),
                    )
                    .await
                })
            })
            .await
            .unwrap();
        let beta = insert_object(&db, &Uuid::new_v4().to_string(), "beta").await;
        let beta_version_id = install(&db, BucketVersioningState::Enabled, beta).await;

        let all = scan_versions(&db, "versioned", "", None, 10).await.unwrap();
        assert_eq!(
            all.iter()
                .map(|version| version.public_version_id.as_str())
                .collect::<Vec<_>>(),
            [
                marker_version_id.as_str(),
                second_version_id.as_str(),
                first_version_id.as_str(),
                beta_version_id.as_str(),
            ]
        );

        let cursor = validate_version_cursor(
            &db,
            "versioned",
            "",
            Some("alpha"),
            Some(&second_version_id),
        )
        .await
        .unwrap()
        .unwrap();
        let from_cursor = scan_versions(&db, "versioned", "", Some(&cursor), 10)
            .await
            .unwrap();
        assert_eq!(from_cursor[0].public_version_id, second_version_id);

        let key_boundary = validate_version_cursor(&db, "versioned", "", Some("alpha"), None)
            .await
            .unwrap()
            .unwrap();
        let after_key = scan_versions(&db, "versioned", "", Some(&key_boundary), 10)
            .await
            .unwrap();
        assert_eq!(after_key.len(), 1);
        assert_eq!(after_key[0].public_version_id, beta_version_id);
        assert!(matches!(
            validate_version_cursor(&db, "versioned", "", None, Some("not-a-version")).await,
            Err(AppError::InvalidArgument(_))
        ));
    }

    #[tokio::test]
    async fn promotion_rebuilds_only_the_selected_object_projection() {
        let db = setup().await;
        bucket::set_versioning_state(&db, "versioned", BucketVersioningState::Enabled)
            .await
            .unwrap();
        let first = insert_object(&db, &Uuid::new_v4().to_string(), "key").await;
        install(&db, BucketVersioningState::Enabled, first.clone()).await;
        let second = insert_object(&db, &Uuid::new_v4().to_string(), "key").await;
        install(&db, BucketVersioningState::Enabled, second.clone()).await;
        let third = insert_object(&db, &Uuid::new_v4().to_string(), "key").await;
        install(&db, BucketVersioningState::Enabled, third.clone()).await;
        let other = insert_object(&db, &Uuid::new_v4().to_string(), "other").await;

        let selected = object_version::Entity::find()
            .filter(object_version::Column::Bucket.eq("versioned"))
            .filter(object_version::Column::Key.eq("key"))
            .filter(object_version::Column::IsLatest.eq(true))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        let promoted = db
            .transaction(move |txn| {
                Box::pin(async move { remove_and_promote(txn, &selected).await })
            })
            .await
            .unwrap()
            .unwrap();

        assert_eq!(promoted.object.unwrap().id, second.id);
        assert_eq!(
            object_store::get_latest(&db, "versioned", "key")
                .await
                .unwrap()
                .id,
            second.id
        );
        assert_eq!(
            object_store::get_latest(&db, "versioned", "other")
                .await
                .unwrap()
                .id,
            other.id
        );
        assert_ne!(first.id, second.id);
        assert_ne!(third.id, second.id);
    }
}
