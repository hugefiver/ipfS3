use std::{sync::Arc, time::SystemTime};

use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result};

use crate::{
    error::AppError,
    state::AppState,
    store::object_version::{BucketVersioningState, ResolvedVersion, VersionCursor, VersionKind},
};

use super::object::{
    normalized_max_keys, project_listing_field, project_optional_listing_field,
    url_encoding_requested,
};

const VERSION_SCAN_BATCH_SIZE: u64 = 1001;

fn common_prefix_for_key(key: &str, prefix: &str, delimiter: Option<&str>) -> Option<String> {
    let delimiter = delimiter.filter(|value| !value.is_empty())?;
    let remainder = key.strip_prefix(prefix)?;
    let delimiter_at = remainder.find(delimiter)?;
    Some(format!(
        "{}{}",
        prefix,
        &remainder[..delimiter_at + delimiter.len()]
    ))
}

fn version_cursor(version: &ResolvedVersion) -> VersionCursor {
    VersionCursor {
        key: version.key.clone(),
        sequence: version.sequence,
        public_version_id: version.public_version_id.clone(),
    }
}

fn cursor_matches(version: &ResolvedVersion, cursor: &VersionCursor) -> bool {
    version.key == cursor.key
        && version.sequence == cursor.sequence
        && version.public_version_id == cursor.public_version_id
}

fn content_dto(version: ResolvedVersion, url_encode: bool) -> Result<ObjectVersion, AppError> {
    let object = version.object.ok_or_else(|| {
        AppError::Internal("content version is missing its immutable object".to_owned())
    })?;
    Ok(ObjectVersion {
        checksum_algorithm: None,
        checksum_type: None,
        e_tag: Some(ETag::Strong(object.cid)),
        is_latest: Some(version.is_latest),
        key: Some(project_listing_field(&version.key, url_encode)),
        last_modified: Some(Timestamp::from(SystemTime::from(version.created_at))),
        owner: None,
        restore_status: None,
        size: Some(object.size),
        storage_class: Some(ObjectVersionStorageClass::from_static(
            ObjectVersionStorageClass::STANDARD,
        )),
        version_id: Some(version.public_version_id),
    })
}

fn delete_marker_dto(version: ResolvedVersion, url_encode: bool) -> DeleteMarkerEntry {
    DeleteMarkerEntry {
        is_latest: Some(version.is_latest),
        key: Some(project_listing_field(&version.key, url_encode)),
        last_modified: Some(Timestamp::from(SystemTime::from(version.created_at))),
        owner: None,
        version_id: Some(version.public_version_id),
    }
}

struct VersionListingPage {
    versions: Vec<ObjectVersion>,
    delete_markers: Vec<DeleteMarkerEntry>,
    common_prefixes: Vec<CommonPrefix>,
    next_cursor: Option<VersionCursor>,
}

async fn build_version_listing_page(
    state: &Arc<AppState>,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    initial_cursor: Option<VersionCursor>,
    max_keys: usize,
    url_encode: bool,
) -> Result<VersionListingPage, AppError> {
    let mut versions = Vec::new();
    let mut delete_markers = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut output_count = 0usize;
    let mut active_common_prefix: Option<String> = None;
    let mut scan_cursor = initial_cursor;
    let mut skip_internal_cursor = false;

    loop {
        let rows = crate::store::object_version::scan_versions(
            state.store.db(),
            bucket,
            prefix,
            scan_cursor.as_ref(),
            VERSION_SCAN_BATCH_SIZE,
        )
        .await?;
        let exhausted = rows.len() < VERSION_SCAN_BATCH_SIZE as usize;
        let mut last_scanned = None;

        for version in rows {
            if skip_internal_cursor
                && scan_cursor
                    .as_ref()
                    .is_some_and(|cursor| cursor_matches(&version, cursor))
            {
                skip_internal_cursor = false;
                last_scanned = Some(version_cursor(&version));
                continue;
            }
            skip_internal_cursor = false;
            last_scanned = Some(version_cursor(&version));

            if let Some(common_prefix) = common_prefix_for_key(&version.key, prefix, delimiter) {
                if active_common_prefix.as_deref() == Some(common_prefix.as_str()) {
                    continue;
                }
                if output_count == max_keys {
                    return Ok(VersionListingPage {
                        versions,
                        delete_markers,
                        common_prefixes,
                        next_cursor: Some(version_cursor(&version)),
                    });
                }
                output_count += 1;
                common_prefixes.push(CommonPrefix {
                    prefix: Some(project_listing_field(&common_prefix, url_encode)),
                });
                active_common_prefix = Some(common_prefix);
                continue;
            }

            active_common_prefix = None;
            if output_count == max_keys {
                return Ok(VersionListingPage {
                    versions,
                    delete_markers,
                    common_prefixes,
                    next_cursor: Some(version_cursor(&version)),
                });
            }
            output_count += 1;
            match version.kind {
                VersionKind::Object => versions.push(content_dto(version, url_encode)?),
                VersionKind::DeleteMarker => {
                    delete_markers.push(delete_marker_dto(version, url_encode));
                }
            }
        }

        if exhausted {
            return Ok(VersionListingPage {
                versions,
                delete_markers,
                common_prefixes,
                next_cursor: None,
            });
        }

        let next_scan_cursor = last_scanned
            .ok_or_else(|| AppError::Internal("version listing scan did not advance".to_owned()))?;
        if scan_cursor.as_ref() == Some(&next_scan_cursor) {
            return Err(AppError::Internal(
                "version listing scan cursor did not advance".to_owned(),
            ));
        }
        scan_cursor = Some(next_scan_cursor);
        skip_internal_cursor = true;
    }
}

pub async fn list_object_versions(
    state: &Arc<AppState>,
    req: S3Request<ListObjectVersionsInput>,
) -> S3Result<S3Response<ListObjectVersionsOutput>> {
    let input = req.input;
    let bucket = input.bucket;
    let prefix = input.prefix.unwrap_or_default();
    let delimiter = input.delimiter;
    let key_marker = input.key_marker;
    let version_id_marker = input.version_id_marker;
    let encoding_type = input.encoding_type;
    let max_keys = normalized_max_keys(input.max_keys);
    let url_encode = url_encoding_requested(encoding_type.as_ref());

    let versioning_state =
        crate::store::bucket::get_versioning_state(state.store.db(), &bucket).await?;
    let cursor = crate::store::object_version::validate_version_cursor(
        state.store.db(),
        &bucket,
        &prefix,
        key_marker.as_deref(),
        version_id_marker.as_deref(),
    )
    .await?;

    let page = if versioning_state == BucketVersioningState::Unversioned {
        VersionListingPage {
            versions: Vec::new(),
            delete_markers: Vec::new(),
            common_prefixes: Vec::new(),
            next_cursor: None,
        }
    } else {
        build_version_listing_page(
            state,
            &bucket,
            &prefix,
            delimiter.as_deref(),
            cursor,
            max_keys,
            url_encode,
        )
        .await?
    };
    let is_truncated = page.next_cursor.is_some();
    let next_key_marker = page
        .next_cursor
        .as_ref()
        .map(|cursor| project_listing_field(&cursor.key, url_encode));
    let next_version_id_marker = page
        .next_cursor
        .as_ref()
        .map(|cursor| cursor.public_version_id.clone());

    Ok(S3Response::new(ListObjectVersionsOutput {
        common_prefixes: (!page.common_prefixes.is_empty()).then_some(page.common_prefixes),
        delete_markers: (!page.delete_markers.is_empty()).then_some(page.delete_markers),
        delimiter: project_optional_listing_field(delimiter, url_encode),
        encoding_type,
        is_truncated: Some(is_truncated),
        key_marker: project_optional_listing_field(key_marker, url_encode),
        max_keys: Some(max_keys as i32),
        name: Some(bucket),
        next_key_marker,
        next_version_id_marker,
        prefix: Some(project_listing_field(&prefix, url_encode)),
        request_charged: None,
        version_id_marker,
        versions: (!page.versions.is_empty()).then_some(page.versions),
    }))
}

pub async fn get_bucket_versioning(
    state: &Arc<AppState>,
    req: S3Request<GetBucketVersioningInput>,
) -> S3Result<S3Response<GetBucketVersioningOutput>> {
    let versioning_state =
        crate::store::bucket::get_versioning_state(state.store.db(), &req.input.bucket).await?;
    let status = match versioning_state {
        BucketVersioningState::Unversioned => None,
        BucketVersioningState::Enabled => Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::ENABLED,
        )),
        BucketVersioningState::Suspended => Some(BucketVersioningStatus::from_static(
            BucketVersioningStatus::SUSPENDED,
        )),
    };

    Ok(S3Response::new(GetBucketVersioningOutput {
        mfa_delete: None,
        status,
    }))
}

pub async fn put_bucket_versioning(
    state: &Arc<AppState>,
    req: S3Request<PutBucketVersioningInput>,
) -> S3Result<S3Response<PutBucketVersioningOutput>> {
    let input = req.input;
    crate::store::bucket::get(state.store.db(), &input.bucket).await?;

    if input.mfa.is_some() {
        return Err(AppError::InvalidArgument("MFA delete is not supported".to_owned()).into());
    }
    if input.versioning_configuration.mfa_delete.is_some() {
        return Err(AppError::InvalidArgument("MFA delete is not supported".to_owned()).into());
    }

    let status = input
        .versioning_configuration
        .status
        .ok_or_else(|| AppError::InvalidArgument("versioning status is required".to_owned()))?;
    let versioning_state = match status.as_str() {
        BucketVersioningStatus::ENABLED => BucketVersioningState::Enabled,
        BucketVersioningStatus::SUSPENDED => BucketVersioningState::Suspended,
        _ => {
            return Err(AppError::InvalidArgument(
                "versioning status must be Enabled or Suspended".to_owned(),
            )
            .into());
        }
    };

    crate::store::bucket::set_versioning_state(state.store.db(), &input.bucket, versioning_state)
        .await?;
    Ok(S3Response::new(PutBucketVersioningOutput::default()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use s3s::S3Request;
    use s3s::dto::*;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, TransactionTrait};

    use super::*;
    use crate::state::AppState;
    use crate::store::{self, object_version::BucketVersioningState};

    async fn state_with_bucket() -> Arc<AppState> {
        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, "bucket", None).await.unwrap();
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

    fn request<T>(input: T, method: http::Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: "/bucket?versioning".parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn get_request(bucket: &str) -> S3Request<GetBucketVersioningInput> {
        request(
            GetBucketVersioningInput {
                bucket: bucket.to_owned(),
                expected_bucket_owner: None,
            },
            http::Method::GET,
        )
    }

    fn put_request(
        bucket: &str,
        status: Option<BucketVersioningStatus>,
        mfa: Option<String>,
        mfa_delete: Option<MFADelete>,
    ) -> S3Request<PutBucketVersioningInput> {
        request(
            PutBucketVersioningInput {
                bucket: bucket.to_owned(),
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                mfa,
                versioning_configuration: VersioningConfiguration { status, mfa_delete },
            },
            http::Method::PUT,
        )
    }

    fn list_request(input: ListObjectVersionsInput) -> S3Request<ListObjectVersionsInput> {
        request(input, http::Method::GET)
    }

    fn list_input() -> ListObjectVersionsInput {
        ListObjectVersionsInput {
            bucket: "bucket".to_owned(),
            ..Default::default()
        }
    }

    async fn set_state(state: &Arc<AppState>, versioning_state: BucketVersioningState) {
        store::bucket::set_versioning_state(state.store.db(), "bucket", versioning_state)
            .await
            .unwrap();
    }

    async fn install_content(
        state: &Arc<AppState>,
        versioning_state: BucketVersioningState,
        key: &str,
        cid: &str,
        size: i64,
        second: u32,
    ) -> String {
        let object_id = uuid::Uuid::new_v4().to_string();
        store::object::upsert(
            state.store.db(),
            &object_id,
            "bucket",
            key,
            cid,
            size,
            None,
            cid,
            None,
            false,
            None,
            None,
            false,
        )
        .await
        .unwrap();
        let object = store::object::get_by_id(state.store.db(), &object_id)
            .await
            .unwrap();
        let now = Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, second).unwrap();
        state
            .store
            .db()
            .transaction(move |txn| {
                Box::pin(async move {
                    store::object_version::install_content_version(
                        txn,
                        versioning_state,
                        &object,
                        now,
                    )
                    .await
                })
            })
            .await
            .unwrap()
    }

    async fn install_marker(state: &Arc<AppState>, key: &str, second: u32) -> String {
        let key = key.to_owned();
        let now = Utc.with_ymd_and_hms(2026, 8, 25, 12, 0, second).unwrap();
        state
            .store
            .db()
            .transaction(move |txn| {
                Box::pin(async move {
                    store::object_version::install_delete_marker(
                        txn,
                        BucketVersioningState::Enabled,
                        "bucket",
                        &key,
                        now,
                    )
                    .await
                })
            })
            .await
            .unwrap()
    }

    fn enabled() -> BucketVersioningStatus {
        BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)
    }

    fn suspended() -> BucketVersioningStatus {
        BucketVersioningStatus::from_static(BucketVersioningStatus::SUSPENDED)
    }

    #[tokio::test]
    async fn get_bucket_versioning_omits_unversioned_status() {
        let state = state_with_bucket().await;

        let output = get_bucket_versioning(&state, get_request("bucket"))
            .await
            .unwrap()
            .output;
        assert_eq!(output.status, None);
        assert_eq!(output.mfa_delete, None);

        put_bucket_versioning(&state, put_request("bucket", Some(enabled()), None, None))
            .await
            .unwrap();
        let enabled_output = get_bucket_versioning(&state, get_request("bucket"))
            .await
            .unwrap()
            .output;
        assert_eq!(
            enabled_output
                .status
                .as_ref()
                .map(BucketVersioningStatus::as_str),
            Some(BucketVersioningStatus::ENABLED)
        );

        put_bucket_versioning(&state, put_request("bucket", Some(suspended()), None, None))
            .await
            .unwrap();
        let suspended_output = get_bucket_versioning(&state, get_request("bucket"))
            .await
            .unwrap()
            .output;
        assert_eq!(
            suspended_output
                .status
                .as_ref()
                .map(BucketVersioningStatus::as_str),
            Some(BucketVersioningStatus::SUSPENDED)
        );
    }

    #[tokio::test]
    async fn put_bucket_versioning_accepts_only_enabled_or_suspended() {
        let state = state_with_bucket().await;

        for (requested, expected) in [
            (enabled(), BucketVersioningState::Enabled),
            (suspended(), BucketVersioningState::Suspended),
        ] {
            put_bucket_versioning(&state, put_request("bucket", Some(requested), None, None))
                .await
                .unwrap();
            assert_eq!(
                store::bucket::get_versioning_state(state.store.db(), "bucket")
                    .await
                    .unwrap(),
                expected
            );
        }

        for status in [
            None,
            Some(BucketVersioningStatus::from_static("Disabled")),
            Some(BucketVersioningStatus::from_static("enabled")),
        ] {
            let error = put_bucket_versioning(&state, put_request("bucket", status, None, None))
                .await
                .unwrap_err();
            assert_eq!(error.code().as_str(), "InvalidArgument");
            assert_eq!(
                store::bucket::get_versioning_state(state.store.db(), "bucket")
                    .await
                    .unwrap(),
                BucketVersioningState::Suspended
            );
        }

        let missing_bucket = put_bucket_versioning(
            &state,
            put_request(
                "missing",
                Some(BucketVersioningStatus::from_static("Disabled")),
                None,
                None,
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(missing_bucket.code().as_str(), "NoSuchBucket");
    }

    #[tokio::test]
    async fn put_bucket_versioning_rejects_mfa_headers_and_configuration() {
        let state = state_with_bucket().await;

        for (mfa, mfa_delete) in [
            (Some("serial token".to_owned()), None),
            (None, Some(MFADelete::from("Enabled".to_owned()))),
        ] {
            let error = put_bucket_versioning(
                &state,
                put_request("bucket", Some(enabled()), mfa, mfa_delete),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code().as_str(), "InvalidArgument");
            assert_eq!(
                store::bucket::get_versioning_state(state.store.db(), "bucket")
                    .await
                    .unwrap(),
                BucketVersioningState::Unversioned
            );
        }
    }

    #[tokio::test]
    async fn put_bucket_versioning_is_idempotent_without_rewriting_versions() {
        let state = state_with_bucket().await;
        put_bucket_versioning(&state, put_request("bucket", Some(enabled()), None, None))
            .await
            .unwrap();

        store::object_version::install_delete_marker(
            state.store.db(),
            BucketVersioningState::Enabled,
            "bucket",
            "deleted",
            Utc.with_ymd_and_hms(2015, 10, 21, 7, 28, 0).unwrap(),
        )
        .await
        .unwrap();
        let before = store::entities::object_version::Entity::find()
            .filter(store::entities::object_version::Column::Bucket.eq("bucket"))
            .all(state.store.db())
            .await
            .unwrap();

        put_bucket_versioning(&state, put_request("bucket", Some(enabled()), None, None))
            .await
            .unwrap();
        let after = store::entities::object_version::Entity::find()
            .filter(store::entities::object_version::Column::Bucket.eq("bucket"))
            .all(state.store.db())
            .await
            .unwrap();

        assert_eq!(after.len(), before.len());
        assert_eq!(after, before);
    }

    #[tokio::test]
    async fn list_versions_orders_key_asc_sequence_desc() {
        let state = state_with_bucket().await;
        set_state(&state, BucketVersioningState::Enabled).await;
        let alpha_old = install_content(
            &state,
            BucketVersioningState::Enabled,
            "alpha",
            "QmAlphaOld",
            11,
            1,
        )
        .await;
        let alpha_new = install_content(
            &state,
            BucketVersioningState::Enabled,
            "alpha",
            "QmAlphaNew",
            12,
            2,
        )
        .await;
        let beta = install_content(
            &state,
            BucketVersioningState::Enabled,
            "beta",
            "QmBeta",
            13,
            3,
        )
        .await;

        let output = list_object_versions(&state, list_request(list_input()))
            .await
            .unwrap()
            .output;
        let versions = output.versions.unwrap();
        assert_eq!(
            versions
                .iter()
                .map(|version| (version.key.as_deref(), version.version_id.as_deref()))
                .collect::<Vec<_>>(),
            vec![
                (Some("alpha"), Some(alpha_new.as_str())),
                (Some("alpha"), Some(alpha_old.as_str())),
                (Some("beta"), Some(beta.as_str())),
            ]
        );
        assert_eq!(output.is_truncated, Some(false));
    }

    #[tokio::test]
    async fn list_versions_projects_content_and_markers() {
        let state = state_with_bucket().await;
        set_state(&state, BucketVersioningState::Enabled).await;
        let content_id = install_content(
            &state,
            BucketVersioningState::Enabled,
            "key",
            "QmContent",
            37,
            1,
        )
        .await;
        let marker_id = install_marker(&state, "key", 2).await;

        let output = list_object_versions(&state, list_request(list_input()))
            .await
            .unwrap()
            .output;
        let version = &output.versions.as_ref().unwrap()[0];
        assert_eq!(version.key.as_deref(), Some("key"));
        assert_eq!(version.version_id.as_deref(), Some(content_id.as_str()));
        assert_eq!(version.e_tag, Some(ETag::Strong("QmContent".to_owned())));
        assert_eq!(version.size, Some(37));
        assert_eq!(version.is_latest, Some(false));
        assert_eq!(version.owner, None);
        assert_eq!(version.checksum_algorithm, None);
        assert_eq!(version.checksum_type, None);
        assert_eq!(version.restore_status, None);
        assert_eq!(
            version
                .storage_class
                .as_ref()
                .map(ObjectVersionStorageClass::as_str),
            Some(ObjectVersionStorageClass::STANDARD)
        );
        assert!(version.last_modified.is_some());

        let marker = &output.delete_markers.as_ref().unwrap()[0];
        assert_eq!(marker.key.as_deref(), Some("key"));
        assert_eq!(marker.version_id.as_deref(), Some(marker_id.as_str()));
        assert_eq!(marker.is_latest, Some(true));
        assert_eq!(marker.owner, None);
        assert!(marker.last_modified.is_some());
    }

    #[tokio::test]
    async fn list_versions_next_pair_is_first_unreturned_and_resume_is_inclusive() {
        let state = state_with_bucket().await;
        set_state(&state, BucketVersioningState::Enabled).await;
        let oldest = install_content(
            &state,
            BucketVersioningState::Enabled,
            "same-key",
            "QmOldest",
            1,
            1,
        )
        .await;
        let middle = install_content(
            &state,
            BucketVersioningState::Enabled,
            "same-key",
            "QmMiddle",
            2,
            2,
        )
        .await;
        let newest = install_content(
            &state,
            BucketVersioningState::Enabled,
            "same-key",
            "QmNewest",
            3,
            3,
        )
        .await;

        let mut first_input = list_input();
        first_input.max_keys = Some(1);
        let first = list_object_versions(&state, list_request(first_input))
            .await
            .unwrap()
            .output;
        assert_eq!(
            first.versions.as_ref().unwrap()[0].version_id.as_deref(),
            Some(newest.as_str())
        );
        assert_eq!(first.next_key_marker.as_deref(), Some("same-key"));
        assert_eq!(
            first.next_version_id_marker.as_deref(),
            Some(middle.as_str())
        );
        assert_eq!(first.is_truncated, Some(true));

        let mut second_input = list_input();
        second_input.max_keys = Some(1);
        second_input.key_marker = first.next_key_marker;
        second_input.version_id_marker = first.next_version_id_marker;
        let second = list_object_versions(&state, list_request(second_input))
            .await
            .unwrap()
            .output;
        assert_eq!(
            second.versions.as_ref().unwrap()[0].version_id.as_deref(),
            Some(middle.as_str())
        );
        assert_eq!(second.next_key_marker.as_deref(), Some("same-key"));
        assert_eq!(
            second.next_version_id_marker.as_deref(),
            Some(oldest.as_str())
        );

        let mut third_input = list_input();
        third_input.max_keys = Some(1);
        third_input.key_marker = second.next_key_marker;
        third_input.version_id_marker = second.next_version_id_marker;
        let third = list_object_versions(&state, list_request(third_input))
            .await
            .unwrap()
            .output;
        assert_eq!(
            third.versions.as_ref().unwrap()[0].version_id.as_deref(),
            Some(oldest.as_str())
        );
        assert_eq!(third.is_truncated, Some(false));
        assert_eq!(third.next_key_marker, None);
        assert_eq!(third.next_version_id_marker, None);
    }

    #[tokio::test]
    async fn list_versions_rejects_version_marker_without_key_or_mismatched_pair() {
        let state = state_with_bucket().await;
        set_state(&state, BucketVersioningState::Enabled).await;
        let version_id = install_content(
            &state,
            BucketVersioningState::Enabled,
            "alpha",
            "QmAlpha",
            1,
            1,
        )
        .await;

        let mut version_only = list_input();
        version_only.version_id_marker = Some(version_id.clone());
        let error = list_object_versions(&state, list_request(version_only))
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidArgument");

        let mut mismatch = list_input();
        mismatch.key_marker = Some("beta".to_owned());
        mismatch.version_id_marker = Some(version_id);
        let error = list_object_versions(&state, list_request(mismatch))
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidArgument");
    }

    #[tokio::test]
    async fn list_versions_key_marker_alone_is_exclusive_key_boundary() {
        let state = state_with_bucket().await;
        set_state(&state, BucketVersioningState::Enabled).await;
        install_content(
            &state,
            BucketVersioningState::Enabled,
            "alpha",
            "QmAlphaOld",
            1,
            1,
        )
        .await;
        install_content(
            &state,
            BucketVersioningState::Enabled,
            "alpha",
            "QmAlphaNew",
            2,
            2,
        )
        .await;
        install_content(
            &state,
            BucketVersioningState::Enabled,
            "beta",
            "QmBeta",
            3,
            3,
        )
        .await;

        let mut input = list_input();
        input.key_marker = Some("alpha".to_owned());
        let output = list_object_versions(&state, list_request(input))
            .await
            .unwrap()
            .output;
        assert_eq!(
            output
                .versions
                .unwrap()
                .iter()
                .filter_map(|version| version.key.as_deref())
                .collect::<Vec<_>>(),
            vec!["beta"]
        );
    }

    #[tokio::test]
    async fn list_versions_delimiter_prefix_consumes_one_budget_and_skips_group() {
        let state = state_with_bucket().await;
        set_state(&state, BucketVersioningState::Enabled).await;
        let photos_first = install_content(
            &state,
            BucketVersioningState::Enabled,
            "photos/a.jpg",
            "QmPhotoA",
            1,
            1,
        )
        .await;
        install_content(
            &state,
            BucketVersioningState::Enabled,
            "photos/a.jpg",
            "QmPhotoANew",
            2,
            2,
        )
        .await;
        install_content(
            &state,
            BucketVersioningState::Enabled,
            "photos/b.jpg",
            "QmPhotoB",
            3,
            3,
        )
        .await;
        let video = install_content(
            &state,
            BucketVersioningState::Enabled,
            "videos/clip.mp4",
            "QmVideo",
            4,
            4,
        )
        .await;
        let zed = install_content(
            &state,
            BucketVersioningState::Enabled,
            "z.txt",
            "QmZed",
            5,
            5,
        )
        .await;

        let mut input = list_input();
        input.delimiter = Some("/".to_owned());
        input.max_keys = Some(1);
        let first = list_object_versions(&state, list_request(input))
            .await
            .unwrap()
            .output;
        assert_eq!(
            first.common_prefixes.as_ref().unwrap()[0].prefix.as_deref(),
            Some("photos/")
        );
        assert_eq!(first.versions, None);
        assert_eq!(first.next_key_marker.as_deref(), Some("videos/clip.mp4"));
        assert_eq!(
            first.next_version_id_marker.as_deref(),
            Some(video.as_str())
        );
        assert_ne!(
            first.next_version_id_marker.as_deref(),
            Some(photos_first.as_str())
        );

        let mut second_input = list_input();
        second_input.delimiter = Some("/".to_owned());
        second_input.max_keys = Some(1);
        second_input.key_marker = first.next_key_marker;
        second_input.version_id_marker = first.next_version_id_marker;
        let second = list_object_versions(&state, list_request(second_input))
            .await
            .unwrap()
            .output;
        assert_eq!(
            second.common_prefixes.as_ref().unwrap()[0]
                .prefix
                .as_deref(),
            Some("videos/")
        );
        assert_eq!(second.next_key_marker.as_deref(), Some("z.txt"));
        assert_eq!(second.next_version_id_marker.as_deref(), Some(zed.as_str()));

        let mut third_input = list_input();
        third_input.delimiter = Some("/".to_owned());
        third_input.max_keys = Some(1);
        third_input.key_marker = second.next_key_marker;
        third_input.version_id_marker = second.next_version_id_marker;
        let third = list_object_versions(&state, list_request(third_input))
            .await
            .unwrap()
            .output;
        assert_eq!(
            third.versions.as_ref().unwrap()[0].key.as_deref(),
            Some("z.txt")
        );
        assert_eq!(third.is_truncated, Some(false));
    }

    #[tokio::test]
    async fn list_versions_url_encodes_keys_prefixes_and_markers() {
        let state = state_with_bucket().await;
        set_state(&state, BucketVersioningState::Enabled).await;
        install_content(
            &state,
            BucketVersioningState::Enabled,
            "pre/dir%/one",
            "QmDir",
            1,
            1,
        )
        .await;
        let plain_id = install_content(
            &state,
            BucketVersioningState::Enabled,
            "pre/plain (é)",
            "QmPlain",
            2,
            2,
        )
        .await;

        let mut input = list_input();
        input.prefix = Some("pre/".to_owned());
        input.delimiter = Some("/".to_owned());
        input.key_marker = Some("pre/".to_owned());
        input.max_keys = Some(1);
        input.encoding_type = Some(EncodingType::from_static(EncodingType::URL));
        let first = list_object_versions(&state, list_request(input))
            .await
            .unwrap()
            .output;
        assert_eq!(first.prefix.as_deref(), Some("pre%2F"));
        assert_eq!(first.delimiter.as_deref(), Some("%2F"));
        assert_eq!(first.key_marker.as_deref(), Some("pre%2F"));
        assert_eq!(
            first.common_prefixes.as_ref().unwrap()[0].prefix.as_deref(),
            Some("pre%2Fdir%25%2F")
        );
        assert_eq!(
            first.next_key_marker.as_deref(),
            Some("pre%2Fplain%20%28%C3%A9%29")
        );
        assert_eq!(
            first.next_version_id_marker.as_deref(),
            Some(plain_id.as_str())
        );

        let mut second_input = list_input();
        second_input.prefix = Some("pre/".to_owned());
        second_input.delimiter = Some("/".to_owned());
        second_input.max_keys = Some(1);
        second_input.key_marker = Some("pre/plain (é)".to_owned());
        second_input.version_id_marker = Some(plain_id.clone());
        second_input.encoding_type = Some(EncodingType::from_static(EncodingType::URL));
        let second = list_object_versions(&state, list_request(second_input))
            .await
            .unwrap()
            .output;
        assert_eq!(
            second.versions.as_ref().unwrap()[0].key.as_deref(),
            Some("pre%2Fplain%20%28%C3%A9%29")
        );
        assert_eq!(
            second.versions.as_ref().unwrap()[0].version_id.as_deref(),
            Some(plain_id.as_str())
        );
        assert_eq!(second.version_id_marker.as_deref(), Some(plain_id.as_str()));
    }

    #[tokio::test]
    async fn unversioned_list_is_empty_and_hides_backfill() {
        let state = state_with_bucket().await;
        install_content(
            &state,
            BucketVersioningState::Unversioned,
            "legacy",
            "QmLegacy",
            9,
            1,
        )
        .await;

        let mut input = list_input();
        input.prefix = Some("leg".to_owned());
        input.delimiter = Some("/".to_owned());
        input.max_keys = Some(7);
        let output = list_object_versions(&state, list_request(input))
            .await
            .unwrap()
            .output;
        assert_eq!(output.name.as_deref(), Some("bucket"));
        assert_eq!(output.prefix.as_deref(), Some("leg"));
        assert_eq!(output.delimiter.as_deref(), Some("/"));
        assert_eq!(output.max_keys, Some(7));
        assert_eq!(output.is_truncated, Some(false));
        assert_eq!(output.versions, None);
        assert_eq!(output.delete_markers, None);
        assert_eq!(output.common_prefixes, None);
    }

    #[tokio::test]
    async fn list_versions_write_delete_race_has_no_duplicate_or_loop() {
        let state = state_with_bucket().await;
        set_state(&state, BucketVersioningState::Enabled).await;
        for (key, cid, second) in [("a", "QmA", 1), ("b", "QmB", 2), ("c", "QmC", 3)] {
            install_content(&state, BucketVersioningState::Enabled, key, cid, 1, second).await;
        }

        let mut input = list_input();
        input.max_keys = Some(1);
        let first = list_object_versions(&state, list_request(input))
            .await
            .unwrap()
            .output;
        let mut receipts = vec![first.versions.as_ref().unwrap()[0].key.clone().unwrap()];
        let mut next = (
            first.next_key_marker.unwrap(),
            first.next_version_id_marker.unwrap(),
        );

        install_content(
            &state,
            BucketVersioningState::Enabled,
            "a",
            "QmANewer",
            2,
            4,
        )
        .await;
        let selected = store::entities::object_version::Entity::find()
            .filter(store::entities::object_version::Column::Bucket.eq("bucket"))
            .filter(store::entities::object_version::Column::Key.eq("c"))
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        state
            .store
            .db()
            .transaction(move |txn| {
                Box::pin(
                    async move { store::object_version::remove_and_promote(txn, &selected).await },
                )
            })
            .await
            .unwrap();
        install_content(&state, BucketVersioningState::Enabled, "d", "QmD", 1, 5).await;

        for _ in 0..8 {
            let mut page_input = list_input();
            page_input.max_keys = Some(1);
            page_input.key_marker = Some(next.0.clone());
            page_input.version_id_marker = Some(next.1.clone());
            let page = list_object_versions(&state, list_request(page_input))
                .await
                .unwrap()
                .output;
            let key = page.versions.as_ref().unwrap()[0].key.clone().unwrap();
            assert!(!receipts.contains(&key), "duplicate page receipt for {key}");
            receipts.push(key);
            if page.is_truncated == Some(false) {
                assert_eq!(receipts, vec!["a", "b", "d"]);
                return;
            }
            let candidate = (
                page.next_key_marker.unwrap(),
                page.next_version_id_marker.unwrap(),
            );
            assert_ne!(candidate, next, "pagination cursor must advance");
            next = candidate;
        }
        panic!("version pagination did not terminate");
    }
}
