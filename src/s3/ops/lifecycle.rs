use std::sync::Arc;

use s3s::{S3Request, S3Response, S3Result, dto::*};

use crate::{
    error::{AppError, AppResult},
    lifecycle::config::{
        canonical_json, from_canonical_json, to_s3_rules, validate_and_canonicalize,
    },
    state::AppState,
    store::entities::bucket,
};

pub async fn delete_bucket_lifecycle(
    state: &Arc<AppState>,
    req: S3Request<DeleteBucketLifecycleInput>,
) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
    let input = req.input;
    let bucket = load_bucket_and_verify_owner(
        state.store.db(),
        &input.bucket,
        input.expected_bucket_owner.as_deref(),
    )
    .await?;
    crate::store::lifecycle_config::delete_configuration(state.store.db(), &bucket.name).await?;
    Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
}

pub async fn get_bucket_lifecycle_configuration(
    state: &Arc<AppState>,
    req: S3Request<GetBucketLifecycleConfigurationInput>,
) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
    let input = req.input;
    let bucket = load_bucket_and_verify_owner(
        state.store.db(),
        &input.bucket,
        input.expected_bucket_owner.as_deref(),
    )
    .await?;
    let json =
        crate::store::lifecycle_config::get_configuration(state.store.db(), &bucket.name).await?;
    let configuration = from_canonical_json(&json)?;
    let rules = to_s3_rules(&configuration)?;
    Ok(S3Response::new(GetBucketLifecycleConfigurationOutput {
        rules: Some(rules),
        transition_default_minimum_object_size: None,
    }))
}

pub async fn put_bucket_lifecycle_configuration(
    state: &Arc<AppState>,
    req: S3Request<PutBucketLifecycleConfigurationInput>,
) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
    let input = req.input;
    let bucket = load_bucket_and_verify_owner(
        state.store.db(),
        &input.bucket,
        input.expected_bucket_owner.as_deref(),
    )
    .await?;
    if input.transition_default_minimum_object_size.is_some() {
        return Err(AppError::InvalidLifecycleConfiguration(
            "transition default minimum object size is not supported".to_owned(),
        )
        .into());
    }
    let configuration = input.lifecycle_configuration.ok_or_else(|| {
        AppError::InvalidLifecycleConfiguration("lifecycle configuration is required".to_owned())
    })?;
    let canonical = validate_and_canonicalize(configuration)?;
    let json = canonical_json(&canonical)?;
    crate::store::lifecycle_config::put_configuration(state.store.db(), &bucket.name, &json)
        .await?;
    Ok(S3Response::new(PutBucketLifecycleConfigurationOutput {
        transition_default_minimum_object_size: None,
    }))
}

async fn load_bucket_and_verify_owner<C: sea_orm::ConnectionTrait>(
    db: &C,
    bucket_name: &str,
    expected_owner: Option<&str>,
) -> AppResult<bucket::Model> {
    let bucket = crate::store::bucket::get(db, bucket_name).await?;
    verify_expected_owner(bucket.owner.as_deref(), expected_owner)?;
    Ok(bucket)
}

fn verify_expected_owner(
    bucket_owner: Option<&str>,
    expected_owner: Option<&str>,
) -> AppResult<()> {
    match expected_owner {
        None => Ok(()),
        Some(value) if bucket_owner == Some(value) => Ok(()),
        Some(_) => Err(AppError::AccessDenied(
            "expected bucket owner mismatch".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use s3s::dto::{
        BucketLifecycleConfiguration, DeleteBucketLifecycleInput,
        GetBucketLifecycleConfigurationInput, PutBucketLifecycleConfigurationInput,
    };
    use sea_orm::EntityTrait;

    use super::*;
    use crate::{
        state::AppState,
        store::{self, entities::bucket_lifecycle_config},
    };

    async fn state_with_bucket(owner: Option<&str>) -> Arc<AppState> {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "bucket", owner).await.unwrap();
        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new("http://127.0.0.1:5001".to_owned()),
            store: store::Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        })
    }

    fn request<T>(input: T) -> S3Request<T> {
        S3Request {
            input,
            method: http::Method::PUT,
            uri: "/bucket?lifecycle".parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn configuration(rule_id: &str, days: i32) -> BucketLifecycleConfiguration {
        serde_json::from_value(serde_json::json!({
            "rules": [{
                "id": rule_id,
                "prefix": "logs/",
                "status": "Enabled",
                "expiration": { "days": days }
            }]
        }))
        .unwrap()
    }

    fn put_request(
        bucket: &str,
        configuration: Option<BucketLifecycleConfiguration>,
        expected_bucket_owner: Option<&str>,
    ) -> S3Request<PutBucketLifecycleConfigurationInput> {
        request(PutBucketLifecycleConfigurationInput {
            bucket: bucket.to_owned(),
            checksum_algorithm: None,
            expected_bucket_owner: expected_bucket_owner.map(str::to_owned),
            lifecycle_configuration: configuration,
            transition_default_minimum_object_size: None,
        })
    }

    fn get_request(
        bucket: &str,
        expected_bucket_owner: Option<&str>,
    ) -> S3Request<GetBucketLifecycleConfigurationInput> {
        request(GetBucketLifecycleConfigurationInput {
            bucket: bucket.to_owned(),
            expected_bucket_owner: expected_bucket_owner.map(str::to_owned),
        })
    }

    fn delete_request(
        bucket: &str,
        expected_bucket_owner: Option<&str>,
    ) -> S3Request<DeleteBucketLifecycleInput> {
        request(DeleteBucketLifecycleInput {
            bucket: bucket.to_owned(),
            expected_bucket_owner: expected_bucket_owner.map(str::to_owned),
        })
    }

    async fn stored_row(state: &Arc<AppState>) -> bucket_lifecycle_config::Model {
        bucket_lifecycle_config::Entity::find_by_id("bucket")
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn put_get_delete_and_replacement_use_canonical_configuration_and_monotonic_tombstones() {
        let state = state_with_bucket(Some("owner")).await;

        let absent = get_bucket_lifecycle_configuration(&state, get_request("bucket", None))
            .await
            .unwrap_err();
        assert_eq!(absent.code().as_str(), "NoSuchLifecycleConfiguration");
        assert_eq!(absent.status_code(), Some(http::StatusCode::NOT_FOUND));

        let put = put_bucket_lifecycle_configuration(
            &state,
            put_request("bucket", Some(configuration("first", 1)), None),
        )
        .await
        .unwrap();
        assert_eq!(put.output.transition_default_minimum_object_size, None);
        let first = stored_row(&state).await;
        assert_eq!(first.revision, 1);

        let get = get_bucket_lifecycle_configuration(&state, get_request("bucket", None))
            .await
            .unwrap()
            .output;
        assert_eq!(get.transition_default_minimum_object_size, None);
        let rules = get.rules.expect("GET must return canonical rules");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].id.as_deref(), Some("first"));
        assert_eq!(rules[0].transitions, None);
        assert_eq!(rules[0].noncurrent_version_transitions, None);
        assert_eq!(rules[0].abort_incomplete_multipart_upload, None);

        put_bucket_lifecycle_configuration(
            &state,
            put_request("bucket", Some(configuration("replacement", 2)), None),
        )
        .await
        .unwrap();
        assert_eq!(stored_row(&state).await.revision, 2);

        delete_bucket_lifecycle(&state, delete_request("bucket", None))
            .await
            .unwrap();
        let tombstone = stored_row(&state).await;
        assert_eq!(tombstone.revision, 3);
        assert_eq!(tombstone.canonical_json, None);
        let error = get_bucket_lifecycle_configuration(&state, get_request("bucket", None))
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "NoSuchLifecycleConfiguration");
        assert_eq!(error.status_code(), Some(http::StatusCode::NOT_FOUND));

        delete_bucket_lifecycle(&state, delete_request("bucket", None))
            .await
            .unwrap();
        assert_eq!(stored_row(&state).await.revision, 4);
        put_bucket_lifecycle_configuration(
            &state,
            put_request("bucket", Some(configuration("later", 3)), None),
        )
        .await
        .unwrap();
        assert_eq!(stored_row(&state).await.revision, 5);
    }

    #[tokio::test]
    async fn put_rejects_missing_or_unsupported_configuration_without_replacing_prior_json() {
        let state = state_with_bucket(Some("owner")).await;
        put_bucket_lifecycle_configuration(
            &state,
            put_request("bucket", Some(configuration("saved", 1)), None),
        )
        .await
        .unwrap();
        let before = stored_row(&state).await;

        let missing = put_bucket_lifecycle_configuration(&state, put_request("bucket", None, None))
            .await
            .unwrap_err();
        assert_eq!(missing.code().as_str(), "InvalidRequest");

        let mut transition_minimum =
            put_request("bucket", Some(configuration("transition-minimum", 2)), None);
        transition_minimum
            .input
            .transition_default_minimum_object_size = Some(
            TransitionDefaultMinimumObjectSize::from_static("all_storage_classes_128K"),
        );
        let transition_minimum = put_bucket_lifecycle_configuration(&state, transition_minimum)
            .await
            .unwrap_err();
        assert_eq!(transition_minimum.code().as_str(), "InvalidRequest");

        for unsupported in [
            serde_json::json!({
                "rules": [{
                    "prefix": "logs/", "status": "Enabled", "expiration": { "days": 1 },
                    "transitions": []
                }]
            }),
            serde_json::json!({
                "rules": [{
                    "prefix": "logs/", "status": "Enabled", "expiration": { "days": 1 },
                    "noncurrent_version_transitions": []
                }]
            }),
        ] {
            let configuration = serde_json::from_value(unsupported).unwrap();
            let error = put_bucket_lifecycle_configuration(
                &state,
                put_request("bucket", Some(configuration), None),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code().as_str(), "InvalidRequest");
        }

        let after = stored_row(&state).await;
        assert_eq!(after.canonical_json, before.canonical_json);
        assert_eq!(after.revision, before.revision);
    }

    #[tokio::test]
    async fn lifecycle_operations_require_existing_bucket_and_exact_expected_owner_when_present() {
        let state = state_with_bucket(Some("owner")).await;
        put_bucket_lifecycle_configuration(
            &state,
            put_request("bucket", Some(configuration("omitted-owner", 1)), None),
        )
        .await
        .unwrap();
        get_bucket_lifecycle_configuration(&state, get_request("bucket", Some("owner")))
            .await
            .unwrap();
        let mismatch =
            get_bucket_lifecycle_configuration(&state, get_request("bucket", Some("other")))
                .await
                .unwrap_err();
        assert_eq!(mismatch.code().as_str(), "AccessDenied");
        assert_eq!(mismatch.status_code(), Some(http::StatusCode::FORBIDDEN));

        let missing = put_bucket_lifecycle_configuration(
            &state,
            put_request("missing", Some(configuration("missing", 1)), None),
        )
        .await
        .unwrap_err();
        assert_eq!(missing.code().as_str(), "NoSuchBucket");
    }
}
