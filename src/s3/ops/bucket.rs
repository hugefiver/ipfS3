use std::sync::Arc;
use std::time::SystemTime;

use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::dto::*;

use crate::state::AppState;

pub async fn create_bucket(
    state: &Arc<AppState>,
    req: S3Request<CreateBucketInput>,
) -> S3Result<S3Response<CreateBucketOutput>> {
    let bucket = &req.input.bucket;
    let db = state.store.db();

    crate::store::bucket::create(db, bucket, None).await?;

    Ok(S3Response::new(CreateBucketOutput::default()))
}

pub async fn delete_bucket(
    state: &Arc<AppState>,
    req: S3Request<DeleteBucketInput>,
) -> S3Result<S3Response<DeleteBucketOutput>> {
    let bucket = &req.input.bucket;
    let db = state.store.db();

    crate::store::bucket::delete(db, bucket).await?;

    Ok(S3Response::new(DeleteBucketOutput::default()))
}

pub async fn head_bucket(
    state: &Arc<AppState>,
    req: S3Request<HeadBucketInput>,
) -> S3Result<S3Response<HeadBucketOutput>> {
    let bucket = &req.input.bucket;
    let db = state.store.db();

    let exists = crate::store::bucket::exists(db, bucket).await?;
    if !exists {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }

    Ok(S3Response::new(HeadBucketOutput::default()))
}

pub async fn get_bucket_location(
    state: &Arc<AppState>,
    req: S3Request<GetBucketLocationInput>,
) -> S3Result<S3Response<GetBucketLocationOutput>> {
    let bucket = &req.input.bucket;

    if !crate::store::bucket::exists(state.store.db(), bucket).await? {
        return Err(s3s::s3_error!(NoSuchBucket, "bucket not found: {}", bucket));
    }

    Ok(S3Response::new(GetBucketLocationOutput {
        location_constraint: None,
    }))
}

pub async fn list_buckets(
    state: &Arc<AppState>,
    _req: S3Request<ListBucketsInput>,
) -> S3Result<S3Response<ListBucketsOutput>> {
    let db = state.store.db();

    let models = crate::store::bucket::list(db).await?;

    let buckets: Vec<Bucket> = models
        .into_iter()
        .map(|m| {
            let creation_date = Timestamp::from(SystemTime::from(m.created_at));
            Bucket {
                name: Some(m.name),
                creation_date: Some(creation_date),
                bucket_region: None,
            }
        })
        .collect();

    Ok(S3Response::new(ListBucketsOutput {
        buckets: Some(buckets),
        owner: None,
        continuation_token: None,
        prefix: None,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use sea_orm::Database;

    use super::*;

    async fn state_with_bucket() -> Arc<AppState> {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        crate::store::bucket::create(&db, "bucket", None)
            .await
            .unwrap();
        Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new("http://127.0.0.1:5001".to_owned()),
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .unwrap(),
        })
    }

    fn location_request(bucket: &str) -> S3Request<GetBucketLocationInput> {
        S3Request {
            input: GetBucketLocationInput {
                bucket: bucket.to_owned(),
                expected_bucket_owner: None,
            },
            method: http::Method::GET,
            uri: format!("/{bucket}?location").parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn get_bucket_location_returns_empty_constraint_for_us_east_1() {
        let output = get_bucket_location(&state_with_bucket().await, location_request("bucket"))
            .await
            .unwrap()
            .output;
        assert_eq!(output.location_constraint, None);
    }

    #[tokio::test]
    async fn get_bucket_location_rejects_missing_bucket() {
        let error = get_bucket_location(&state_with_bucket().await, location_request("missing"))
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "NoSuchBucket");
        assert_eq!(error.message(), Some("bucket not found: missing"));
    }
}
