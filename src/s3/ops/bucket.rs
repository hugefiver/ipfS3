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
