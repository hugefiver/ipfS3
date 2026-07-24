use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};

use crate::{
    pinning::tags::{ObjectTag, validate_tag_set},
    store::entities::{object, object_tag},
};

/// Replaces an object's tags through the caller-provided connection.
///
/// Callers that require atomic replacement and PostgreSQL row-lock serialization must pass a
/// caller-owned transaction. This function never opens an independent transaction.
pub async fn replace_object_tags<C: ConnectionTrait>(
    db: &C,
    object_id: &str,
    tags: &[ObjectTag],
) -> Result<(), sea_orm::DbErr> {
    validate_tag_set(tags)
        .map_err(|_| sea_orm::DbErr::Custom("invalid object tag set".to_owned()))?;
    lock_parent_object(db, object_id).await?;

    object_tag::Entity::delete_many()
        .filter(object_tag::Column::ObjectId.eq(object_id))
        .exec(db)
        .await?;

    for tag in tags {
        object_tag::Entity::insert(object_tag::ActiveModel {
            object_id: Set(object_id.to_owned()),
            key: Set(tag.key.clone()),
            value: Set(tag.value.clone()),
        })
        .exec(db)
        .await?;
    }
    Ok(())
}

pub async fn list_object_tags<C: ConnectionTrait>(
    db: &C,
    object_id: &str,
) -> Result<Vec<ObjectTag>, sea_orm::DbErr> {
    object_tag::Entity::find()
        .filter(object_tag::Column::ObjectId.eq(object_id))
        .order_by_asc(object_tag::Column::Key)
        .all(db)
        .await
        .map(|tags| {
            tags.into_iter()
                .map(|tag| ObjectTag::new(tag.key, tag.value))
                .collect()
        })
}

pub fn tags_to_json(tags: &[ObjectTag]) -> Result<serde_json::Value, serde_json::Error> {
    validate_tag_set(tags).map_err(|_| invalid_tag_set_json_error())?;
    serde_json::to_value(tags)
}

pub fn tags_from_json(value: &serde_json::Value) -> Result<Vec<ObjectTag>, serde_json::Error> {
    let tags: Vec<ObjectTag> = serde_json::from_value(value.clone())?;
    validate_tag_set(&tags).map_err(|_| invalid_tag_set_json_error())?;
    Ok(tags)
}

fn locked_parent_query(object_id: &str) -> sea_orm::Select<object::Entity> {
    object::Entity::find_by_id(object_id.to_owned()).lock_exclusive()
}

async fn lock_parent_object<C: ConnectionTrait>(
    db: &C,
    object_id: &str,
) -> Result<(), sea_orm::DbErr> {
    let parent = if db.get_database_backend() == DatabaseBackend::Postgres {
        locked_parent_query(object_id).one(db).await?
    } else {
        object::Entity::find_by_id(object_id.to_owned())
            .one(db)
            .await?
    };
    parent
        .map(|_| ())
        .ok_or_else(|| sea_orm::DbErr::RecordNotFound("object not found".to_owned()))
}

fn invalid_tag_set_json_error() -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom("invalid object tag set")
}

#[cfg(test)]
mod tests {
    use sea_orm::{
        ConnectionTrait, Database, DatabaseBackend, QueryTrait, Statement, TransactionTrait,
    };

    use super::{
        list_object_tags, locked_parent_query, replace_object_tags, tags_from_json, tags_to_json,
    };
    use crate::pinning::tags::ObjectTag;

    fn tags(pairs: &[(&str, &str)]) -> Vec<ObjectTag> {
        pairs
            .iter()
            .map(|(key, value)| ObjectTag::new(*key, *value))
            .collect()
    }

    async fn setup() -> sea_orm::DatabaseConnection {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute_unprepared("PRAGMA foreign_keys = ON")
            .await
            .unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        crate::store::object::upsert(
            &db, "object-1", "bucket", "key", "QmObject", 7, None, "QmObject", None, false, None,
            None, false,
        )
        .await
        .unwrap();
        db
    }

    #[test]
    fn json_round_trips_tag_values_without_rewriting_them() {
        let tags = tags(&[("team", "R&D"), ("space", "hello world"), ("empty", "")]);

        let json = tags_to_json(&tags).unwrap();

        assert_eq!(tags_from_json(&json).unwrap(), tags);
    }

    #[test]
    fn json_codec_rejects_invalid_tag_sets_without_leaking_values() {
        let eleven_tags = (0..11)
            .map(|index| ObjectTag::new(format!("key-{index}"), "sensitive-value"))
            .collect::<Vec<_>>();
        let invalid_tag_sets = vec![
            eleven_tags,
            tags(&[("", "sensitive-value")]),
            tags(&[
                ("duplicate", "first-sensitive-value"),
                ("duplicate", "second-sensitive-value"),
            ]),
            vec![ObjectTag::new("界".repeat(129), "sensitive-value")],
            vec![ObjectTag::new("key", "é".repeat(257))],
            tags(&[("ipfs-s3:unknown", "sensitive-value")]),
        ];

        for tag_set in invalid_tag_sets {
            let encoded_error = tags_to_json(&tag_set).unwrap_err();
            assert_eq!(encoded_error.to_string(), "invalid object tag set");
            assert!(!encoded_error.to_string().contains("sensitive-value"));

            let json = serde_json::to_value(&tag_set).unwrap();
            let decoded_error = tags_from_json(&json).unwrap_err();
            assert_eq!(decoded_error.to_string(), "invalid object tag set");
            assert!(!decoded_error.to_string().contains("sensitive-value"));
        }
    }

    #[tokio::test]
    async fn replacement_requires_a_parent_object_before_mutation() {
        let db = setup().await;

        let error = replace_object_tags(&db, "missing-object", &[])
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            sea_orm::DbErr::RecordNotFound(ref message) if message == "object not found"
        ));
    }

    #[test]
    fn postgres_parent_lock_query_renders_for_update() {
        assert_eq!(
            locked_parent_query("object-1")
                .build(DatabaseBackend::Postgres)
                .to_string(),
            "SELECT \"objects\".\"id\", \"objects\".\"bucket\", \"objects\".\"key\", \
             \"objects\".\"cid\", \"objects\".\"size\", \"objects\".\"content_type\", \
             \"objects\".\"etag\", \"objects\".\"metadata\", \"objects\".\"encrypted\", \
             \"objects\".\"key_wrap\", \"objects\".\"sse_c_key_fingerprint\", \
             \"objects\".\"multipart\", \"objects\".\"is_latest\", \"objects\".\"created_at\" \
             FROM \"objects\" WHERE \"objects\".\"id\" = 'object-1' FOR UPDATE"
        );
    }

    #[tokio::test]
    async fn replacement_persists_values_sorts_reads_and_is_atomic_in_the_callers_transaction() {
        let db = setup().await;
        let original = tags(&[("zebra", "last"), ("apple", "first")]);
        replace_object_tags(&db, "object-1", &original)
            .await
            .unwrap();
        assert_eq!(
            list_object_tags(&db, "object-1").await.unwrap(),
            tags(&[("apple", "first"), ("zebra", "last")])
        );

        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            "CREATE TRIGGER fail_tag_insert BEFORE INSERT ON object_tags \
             WHEN NEW.value = 'fail' BEGIN SELECT RAISE(FAIL, 'forced tag failure'); END;",
        ))
        .await
        .unwrap();
        let replacement = tags(&[("replacement", "fail")]);
        let error = db
            .transaction(|txn| {
                Box::pin(async move { replace_object_tags(txn, "object-1", &replacement).await })
            })
            .await;

        assert!(error.is_err());
        assert_eq!(
            list_object_tags(&db, "object-1").await.unwrap(),
            tags(&[("apple", "first"), ("zebra", "last")])
        );
    }

    #[tokio::test]
    async fn invalid_replacement_is_rejected_before_existing_tags_are_deleted() {
        let db = setup().await;
        let original = tags(&[("zebra", "last"), ("apple", "first")]);
        replace_object_tags(&db, "object-1", &original)
            .await
            .unwrap();
        let expected = tags(&[("apple", "first"), ("zebra", "last")]);
        let eleven_tags = (0..11)
            .map(|index| ObjectTag::new(format!("key-{index}"), "value"))
            .collect::<Vec<_>>();
        let invalid_replacements = vec![
            eleven_tags,
            tags(&[("", "value")]),
            tags(&[("duplicate", "first"), ("duplicate", "second")]),
            tags(&[("ipfs-s3:unknown", "value")]),
        ];

        for replacement in invalid_replacements {
            let error = replace_object_tags(&db, "object-1", &replacement).await;
            assert!(
                matches!(error, Err(sea_orm::DbErr::Custom(ref message)) if message == "invalid object tag set"),
                "unexpected replacement result: {error:?}"
            );
            assert_eq!(list_object_tags(&db, "object-1").await.unwrap(), expected);
        }
    }
}
