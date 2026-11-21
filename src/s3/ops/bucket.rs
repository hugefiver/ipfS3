use std::sync::Arc;
use std::time::SystemTime;

use s3s::S3Request;
use s3s::S3Response;
use s3s::S3Result;
use s3s::dto::*;

use crate::{
    access::authorize,
    iam::{Action, Resource},
    state::AppState,
};

mod acl;
pub use acl::{get_bucket_acl, put_bucket_acl};

fn signed_subject<'a, T>(state: &AppState, req: &'a S3Request<T>) -> S3Result<&'a str> {
    crate::access::subject(state, req)?.ok_or_else(|| s3s::s3_error!(AccessDenied))
}

pub async fn create_bucket(
    state: &Arc<AppState>,
    req: S3Request<CreateBucketInput>,
) -> S3Result<S3Response<CreateBucketOutput>> {
    let bucket = &req.input.bucket;
    let db = state.store.db();

    let owner = signed_subject(state, &req)?;
    let input = &req.input;
    if input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_write.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write_acp.is_some()
    {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "explicit grant headers are not supported"
        ));
    }
    let acl = input
        .acl
        .as_ref()
        .map(acl::canned)
        .transpose()?
        .unwrap_or("private");
    crate::store::bucket::create_with_acl(db, bucket, Some(owner), Some(acl)).await?;

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
    authorize(
        state,
        &req,
        Action::HeadBucket,
        Some(&req.input.bucket),
        Resource::Bucket,
    )
    .await?;

    Ok(S3Response::new(HeadBucketOutput::default()))
}

pub async fn get_bucket_location(
    state: &Arc<AppState>,
    req: S3Request<GetBucketLocationInput>,
) -> S3Result<S3Response<GetBucketLocationOutput>> {
    authorize(
        state,
        &req,
        Action::GetBucketLocation,
        Some(&req.input.bucket),
        Resource::Bucket,
    )
    .await?;

    Ok(S3Response::new(GetBucketLocationOutput {
        location_constraint: None,
    }))
}

pub async fn list_buckets(
    state: &Arc<AppState>,
    req: S3Request<ListBucketsInput>,
) -> S3Result<S3Response<ListBucketsOutput>> {
    let db = state.store.db();
    let owner = signed_subject(state, &req)?;
    let models = crate::store::bucket::list_for_owner(db, owner).await?;

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
        owner: Some(Owner {
            id: Some(owner.to_owned()),
            display_name: None,
        }),
        continuation_token: None,
        prefix: None,
    }))
}

#[cfg(test)]
mod tests {

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
            cold_kubo: None,
            store: crate::store::Store::new(db),
            credentials: Default::default(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .unwrap(),
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
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

    #[tokio::test]
    async fn create_bucket_requires_signature_and_persists_default_acl() {
        let state = state_with_bucket().await;
        let input = CreateBucketInput {
            bucket: "owned-bucket".to_owned(),
            ..Default::default()
        };
        let mut anonymous = signed(input.clone(), "alice");
        anonymous.credentials = None;
        assert_eq!(
            create_bucket(&state, anonymous)
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "AccessDenied"
        );
        create_bucket(&state, signed(input, "alice")).await.unwrap();
        let bucket = crate::store::bucket::get(state.store.db(), "owned-bucket")
            .await
            .unwrap();
        assert_eq!(bucket.owner.as_deref(), Some("alice"));
        assert_eq!(bucket.canned_acl.as_deref(), Some("private"));

        let input = CreateBucketInput {
            bucket: "other".to_owned(),
            acl: Some(BucketCannedACL::from_static("public-read")),
            ..Default::default()
        };
        create_bucket(&state, signed(input, "bob")).await.unwrap();
        let listed = list_buckets(&state, signed(ListBucketsInput::default(), "alice"))
            .await
            .unwrap()
            .output;
        let names: Vec<_> = listed
            .buckets
            .unwrap()
            .into_iter()
            .map(|bucket| bucket.name.unwrap())
            .collect();
        assert_eq!(names, ["owned-bucket"]);
    }

    fn signed<T>(input: T, user: &str) -> S3Request<T> {
        S3Request {
            input,
            method: http::Method::PUT,
            uri: "/owned-bucket".parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: Some(s3s::auth::Credentials {
                access_key: user.to_owned(),
                secret_key: s3s::auth::SecretKey::from("secret"),
            }),
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    #[tokio::test]
    async fn bucket_acl_requires_owner_and_validates_before_replacing() {
        let state = state_with_bucket().await;
        crate::store::bucket::create(state.store.db(), "owned-bucket", Some("alice"))
            .await
            .unwrap();
        let get = GetBucketAclInput {
            bucket: "owned-bucket".to_owned(),
            expected_bucket_owner: None,
        };
        assert_eq!(
            get_bucket_acl(&state, signed(get.clone(), "alice"))
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "InvalidBucketState"
        );
        let put = PutBucketAclInput {
            bucket: get.bucket.clone(),
            acl: Some(BucketCannedACL::from_static("private")),
            ..Default::default()
        };
        for mode in [
            "private",
            "authenticated-read",
            "public-read",
            "public-read-write",
        ] {
            let input = PutBucketAclInput {
                acl: Some(BucketCannedACL::from_static(mode)),
                ..put.clone()
            };
            put_bucket_acl(&state, signed(input, "alice"))
                .await
                .unwrap();
            let result = get_bucket_acl(&state, signed(get.clone(), "alice"))
                .await
                .unwrap()
                .output;
            let mut grants = result.grants.unwrap();
            grants.reverse();
            let xml = AccessControlPolicy {
                owner: result.owner,
                grants: Some(grants),
            };
            let input = PutBucketAclInput {
                acl: None,
                access_control_policy: Some(xml),
                ..put.clone()
            };
            put_bucket_acl(&state, signed(input, "alice"))
                .await
                .unwrap();
            assert_eq!(
                crate::store::bucket::get(state.store.db(), &get.bucket)
                    .await
                    .unwrap()
                    .canned_acl
                    .as_deref(),
                Some(mode)
            );
        }
        assert_eq!(
            put_bucket_acl(&state, signed(put.clone(), "bob"))
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "AccessDenied"
        );
        let bad_owner = PutBucketAclInput {
            expected_bucket_owner: Some("bob".to_owned()),
            ..put.clone()
        };
        assert_eq!(
            put_bucket_acl(&state, signed(bad_owner, "alice"))
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "AccessDenied"
        );
        let mismatch = PutBucketAclInput {
            access_control_policy: Some(Default::default()),
            ..put.clone()
        };
        let unsupported = PutBucketAclInput {
            acl: Some(BucketCannedACL::from_static("log-delivery-write")),
            ..put.clone()
        };
        let bad_xml = PutBucketAclInput {
            acl: None,
            access_control_policy: Some(AccessControlPolicy::default()),
            ..put
        };
        for input in [mismatch, unsupported, bad_xml] {
            assert_eq!(
                put_bucket_acl(&state, signed(input, "alice"))
                    .await
                    .unwrap_err()
                    .code()
                    .as_str(),
                "InvalidArgument"
            );
        }
        assert_eq!(
            get_bucket_acl(&state, signed(get.clone(), "bob"))
                .await
                .unwrap_err()
                .code()
                .as_str(),
            "AccessDenied"
        );
        assert_eq!(
            crate::store::bucket::get(state.store.db(), &get.bucket)
                .await
                .unwrap()
                .canned_acl
                .as_deref(),
            Some("public-read-write")
        );
    }
}
