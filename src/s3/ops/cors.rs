use std::sync::Arc;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use s3s::{S3Request, S3Response, S3Result, dto::*};
use subtle::ConstantTimeEq;

use crate::{
    cors::{
        CorsPutBodyMetadata, Crc64NvmeHeader, MAX_CORS_CONFIGURATION_BYTES,
        SdkChecksumAlgorithmHeader,
    },
    error::{AppError, AppResult},
    state::AppState,
    store::entities::bucket,
};

pub async fn get_bucket_cors(
    state: &Arc<AppState>,
    req: S3Request<GetBucketCorsInput>,
) -> S3Result<S3Response<GetBucketCorsOutput>> {
    let input = req.input;
    let bucket = load_bucket_and_verify_owner(
        state.store.db(),
        &input.bucket,
        input.expected_bucket_owner.as_deref(),
    )
    .await?;
    let json =
        crate::store::cors_config::get_optional_configuration(state.store.db(), &bucket.name)
            .await?
            .ok_or(AppError::NoSuchCorsConfiguration)?;
    let configuration = crate::cors::config::from_canonical_json(&json)?;
    let configuration = crate::cors::config::to_s3_configuration(&configuration);

    Ok(S3Response::new(GetBucketCorsOutput {
        cors_rules: Some(configuration.cors_rules),
    }))
}

pub async fn put_bucket_cors(
    state: &Arc<AppState>,
    req: S3Request<PutBucketCorsInput>,
) -> S3Result<S3Response<PutBucketCorsOutput>> {
    let input = req.input;
    let bucket = load_bucket_and_verify_owner(
        state.store.db(),
        &input.bucket,
        input.expected_bucket_owner.as_deref(),
    )
    .await?;
    let metadata = req
        .extensions
        .get::<CorsPutBodyMetadata>()
        .copied()
        .ok_or(AppError::InvalidCorsConfiguration)?;
    if metadata.len > MAX_CORS_CONFIGURATION_BYTES {
        return Err(AppError::InvalidCorsConfiguration.into());
    }
    if matches!(
        metadata.sdk_checksum_algorithm,
        SdkChecksumAlgorithmHeader::Invalid
    ) {
        return Err(AppError::InvalidCorsConfiguration.into());
    }

    if input.content_md5.is_none() && input.checksum_algorithm.is_none() {
        return Err(AppError::InvalidCorsConfiguration.into());
    }
    if input.checksum_algorithm.is_none()
        && !matches!(metadata.supplied_crc64nvme, Crc64NvmeHeader::Absent)
    {
        return Err(AppError::InvalidCorsConfiguration.into());
    }

    if let Some(algorithm) = input.checksum_algorithm.as_ref() {
        if algorithm != &ChecksumAlgorithm::from_static("CRC64NVME") {
            return Err(AppError::InvalidCorsConfiguration.into());
        }
        match metadata.supplied_crc64nvme {
            Crc64NvmeHeader::Absent => return Err(AppError::InvalidCorsConfiguration.into()),
            Crc64NvmeHeader::Invalid => return Err(AppError::InvalidCorsDigest.into()),
            Crc64NvmeHeader::Value(supplied)
                if supplied.ct_eq(&metadata.computed_crc64nvme).unwrap_u8() != 1 =>
            {
                return Err(AppError::BadCorsDigest.into());
            }
            Crc64NvmeHeader::Value(_) => {}
        }
    }

    if let Some(encoded) = input.content_md5.as_deref() {
        let decoded = STANDARD
            .decode(encoded.as_bytes())
            .map_err(|_| AppError::InvalidCorsDigest)?;
        let supplied: [u8; 16] = decoded
            .try_into()
            .map_err(|_| AppError::InvalidCorsDigest)?;
        if supplied.ct_eq(&metadata.computed_md5).unwrap_u8() != 1 {
            return Err(AppError::BadCorsDigest.into());
        }
    }

    let configuration = crate::cors::config::validate_and_canonicalize(input.cors_configuration)?;
    let json = crate::cors::config::canonical_json(&configuration)?;
    crate::store::cors_config::put_configuration(state.store.db(), &bucket.name, &json).await?;

    Ok(S3Response::new(PutBucketCorsOutput::default()))
}

pub async fn delete_bucket_cors(
    state: &Arc<AppState>,
    req: S3Request<DeleteBucketCorsInput>,
) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
    let input = req.input;
    let bucket = load_bucket_and_verify_owner(
        state.store.db(),
        &input.bucket,
        input.expected_bucket_owner.as_deref(),
    )
    .await?;
    crate::store::cors_config::delete_configuration(state.store.db(), &bucket.name).await?;

    Ok(S3Response::new(DeleteBucketCorsOutput::default()))
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
    use std::{collections::HashMap, sync::Arc};

    use s3s::{
        S3Request,
        dto::{
            CORSConfiguration, CORSRule, ChecksumAlgorithm, DeleteBucketCorsInput,
            GetBucketCorsInput, PutBucketCorsInput,
        },
    };

    use super::*;
    use crate::{
        cors::{CorsPutBodyMetadata, Crc64NvmeHeader, MAX_CORS_CONFIGURATION_BYTES},
        state::AppState,
        store,
    };

    const TEST_MD5: [u8; 16] = [
        0x19, 0x28, 0x37, 0x46, 0x55, 0x64, 0x73, 0x82, 0x91, 0xa0, 0xbf, 0xce, 0xdd, 0xec, 0xfb,
        0x0a,
    ];
    const TEST_CRC64NVME: [u8; 8] = [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef];
    const PRIVATE_ORIGIN: &str = "https://private-origin.example";
    const PRIVATE_BODY: &str = "private CORS body text";
    const PRIVATE_DIGEST: &str = "paWlpaWlpaWlpaWlpaWlpQ==";
    const PRIVATE_DATABASE_DETAIL: &str = "private database detail";

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

    fn request<T>(input: T, method: http::Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: "/bucket?cors".parse().unwrap(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn configuration(id: &str, origin: &str) -> CORSConfiguration {
        CORSConfiguration {
            cors_rules: vec![CORSRule {
                allowed_headers: None,
                allowed_methods: vec!["GET".to_owned()],
                allowed_origins: vec![origin.to_owned()],
                expose_headers: None,
                id: Some(id.to_owned()),
                max_age_seconds: None,
            }],
        }
    }

    fn body_metadata() -> CorsPutBodyMetadata {
        CorsPutBodyMetadata {
            len: 128,
            computed_md5: TEST_MD5,
            computed_crc64nvme: TEST_CRC64NVME,
            sdk_checksum_algorithm: SdkChecksumAlgorithmHeader::Absent,
            supplied_crc64nvme: Crc64NvmeHeader::Absent,
        }
    }

    fn encoded_md5(digest: [u8; 16]) -> String {
        base64::engine::general_purpose::STANDARD.encode(digest)
    }

    fn put_request(
        bucket: &str,
        configuration: CORSConfiguration,
        expected_bucket_owner: Option<&str>,
        content_md5: Option<String>,
        metadata: Option<CorsPutBodyMetadata>,
    ) -> S3Request<PutBucketCorsInput> {
        let mut req = request(
            PutBucketCorsInput {
                bucket: bucket.to_owned(),
                cors_configuration: configuration,
                checksum_algorithm: None,
                content_md5,
                expected_bucket_owner: expected_bucket_owner.map(str::to_owned),
            },
            http::Method::PUT,
        );
        if let Some(metadata) = metadata {
            req.extensions.insert(metadata);
        }
        req
    }

    fn get_request(
        bucket: &str,
        expected_bucket_owner: Option<&str>,
    ) -> S3Request<GetBucketCorsInput> {
        request(
            GetBucketCorsInput {
                bucket: bucket.to_owned(),
                expected_bucket_owner: expected_bucket_owner.map(str::to_owned),
            },
            http::Method::GET,
        )
    }

    fn delete_request(
        bucket: &str,
        expected_bucket_owner: Option<&str>,
    ) -> S3Request<DeleteBucketCorsInput> {
        request(
            DeleteBucketCorsInput {
                bucket: bucket.to_owned(),
                expected_bucket_owner: expected_bucket_owner.map(str::to_owned),
            },
            http::Method::DELETE,
        )
    }

    fn assert_error(error: s3s::S3Error, code: &str, status: http::StatusCode, message: &str) {
        assert_eq!(error.code().as_str(), code);
        assert_eq!(error.status_code(), Some(status));
        assert_eq!(error.message(), Some(message));
        let rendered = error.to_string();
        for private in [
            PRIVATE_ORIGIN,
            PRIVATE_BODY,
            PRIVATE_DIGEST,
            PRIVATE_DATABASE_DETAIL,
        ] {
            assert!(!rendered.contains(private));
        }
    }

    #[tokio::test]
    async fn put_get_replaces_configuration_and_delete_is_idempotent() {
        let state = state_with_bucket(Some("owner")).await;
        let absent = get_bucket_cors(&state, get_request("bucket", None))
            .await
            .unwrap_err();
        assert_error(
            absent,
            "NoSuchCORSConfiguration",
            http::StatusCode::NOT_FOUND,
            "CORS configuration not found",
        );

        put_bucket_cors(
            &state,
            put_request(
                "bucket",
                configuration("first", "https://safe-first.example"),
                None,
                Some(encoded_md5(TEST_MD5)),
                Some(body_metadata()),
            ),
        )
        .await
        .unwrap();
        let first = get_bucket_cors(&state, get_request("bucket", None))
            .await
            .unwrap()
            .output
            .cors_rules
            .unwrap();
        assert_eq!(first[0].id.as_deref(), Some("first"));

        put_bucket_cors(
            &state,
            put_request(
                "bucket",
                configuration("replacement", "https://safe-replacement.example"),
                None,
                Some(encoded_md5(TEST_MD5)),
                Some(body_metadata()),
            ),
        )
        .await
        .unwrap();
        let replacement = get_bucket_cors(&state, get_request("bucket", None))
            .await
            .unwrap()
            .output
            .cors_rules
            .unwrap();
        assert_eq!(replacement[0].id.as_deref(), Some("replacement"));
        assert_eq!(
            replacement[0].allowed_origins,
            vec!["https://safe-replacement.example"]
        );

        delete_bucket_cors(&state, delete_request("bucket", None))
            .await
            .unwrap();
        let absent = get_bucket_cors(&state, get_request("bucket", None))
            .await
            .unwrap_err();
        assert_error(
            absent,
            "NoSuchCORSConfiguration",
            http::StatusCode::NOT_FOUND,
            "CORS configuration not found",
        );
        delete_bucket_cors(&state, delete_request("bucket", None))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn operations_require_existing_bucket_and_exact_expected_owner_when_present() {
        let state = state_with_bucket(Some("owner")).await;
        put_bucket_cors(
            &state,
            put_request(
                "bucket",
                configuration("omitted-owner", "https://safe.example"),
                None,
                Some(encoded_md5(TEST_MD5)),
                Some(body_metadata()),
            ),
        )
        .await
        .unwrap();
        get_bucket_cors(&state, get_request("bucket", Some("owner")))
            .await
            .unwrap();

        let mismatch = get_bucket_cors(&state, get_request("bucket", Some("other")))
            .await
            .unwrap_err();
        assert_error(
            mismatch,
            "AccessDenied",
            http::StatusCode::FORBIDDEN,
            "access denied: expected bucket owner mismatch",
        );

        let missing = put_bucket_cors(
            &state,
            put_request(
                "missing",
                configuration("missing", "https://safe.example"),
                None,
                Some(encoded_md5(TEST_MD5)),
                Some(body_metadata()),
            ),
        )
        .await
        .unwrap_err();
        assert_error(
            missing,
            "NoSuchBucket",
            http::StatusCode::NOT_FOUND,
            "bucket not found: missing",
        );
    }

    #[tokio::test]
    async fn put_requires_exact_content_md5_and_rejects_invalid_integrity_inputs() {
        let state = state_with_bucket(Some("owner")).await;
        let config = configuration("digest", "https://safe.example");

        let missing = put_bucket_cors(
            &state,
            put_request("bucket", config.clone(), None, None, Some(body_metadata())),
        )
        .await
        .unwrap_err();
        assert_error(
            missing,
            "InvalidRequest",
            http::StatusCode::BAD_REQUEST,
            "invalid CORS configuration",
        );

        let malformed = put_bucket_cors(
            &state,
            put_request(
                "bucket",
                config.clone(),
                None,
                Some("not valid base64!".to_owned()),
                Some(body_metadata()),
            ),
        )
        .await
        .unwrap_err();
        assert_error(
            malformed,
            "InvalidDigest",
            http::StatusCode::BAD_REQUEST,
            "invalid digest",
        );

        let wrong_size = put_bucket_cors(
            &state,
            put_request(
                "bucket",
                config.clone(),
                None,
                Some("AQ==".to_owned()),
                Some(body_metadata()),
            ),
        )
        .await
        .unwrap_err();
        assert_error(
            wrong_size,
            "InvalidDigest",
            http::StatusCode::BAD_REQUEST,
            "invalid digest",
        );

        let mismatch = put_bucket_cors(
            &state,
            put_request(
                "bucket",
                config.clone(),
                None,
                Some(PRIVATE_DIGEST.to_owned()),
                Some(body_metadata()),
            ),
        )
        .await
        .unwrap_err();
        assert_error(
            mismatch,
            "BadDigest",
            http::StatusCode::BAD_REQUEST,
            "digest mismatch",
        );

        put_bucket_cors(
            &state,
            put_request(
                "bucket",
                config.clone(),
                None,
                Some(encoded_md5(TEST_MD5)),
                Some(body_metadata()),
            ),
        )
        .await
        .unwrap();

        let extension_missing = put_bucket_cors(
            &state,
            put_request(
                "bucket",
                config.clone(),
                None,
                Some(encoded_md5(TEST_MD5)),
                None,
            ),
        )
        .await
        .unwrap_err();
        assert_error(
            extension_missing,
            "InvalidRequest",
            http::StatusCode::BAD_REQUEST,
            "invalid CORS configuration",
        );

        let oversized = put_bucket_cors(
            &state,
            put_request(
                "bucket",
                config.clone(),
                None,
                Some(encoded_md5(TEST_MD5)),
                Some(CorsPutBodyMetadata {
                    len: MAX_CORS_CONFIGURATION_BYTES + 1,
                    ..body_metadata()
                }),
            ),
        )
        .await
        .unwrap_err();
        assert_error(
            oversized,
            "InvalidRequest",
            http::StatusCode::BAD_REQUEST,
            "invalid CORS configuration",
        );

        let unpaired_checksum = put_bucket_cors(
            &state,
            put_request(
                "bucket",
                config.clone(),
                None,
                Some(encoded_md5(TEST_MD5)),
                Some(CorsPutBodyMetadata {
                    supplied_crc64nvme: Crc64NvmeHeader::Value(TEST_CRC64NVME),
                    ..body_metadata()
                }),
            ),
        )
        .await
        .unwrap_err();
        assert_error(
            unpaired_checksum,
            "InvalidRequest",
            http::StatusCode::BAD_REQUEST,
            "invalid CORS configuration",
        );

        let duplicate_sdk_checksum_algorithm = put_bucket_cors(
            &state,
            put_request(
                "bucket",
                config.clone(),
                None,
                Some(encoded_md5(TEST_MD5)),
                Some(CorsPutBodyMetadata {
                    sdk_checksum_algorithm: SdkChecksumAlgorithmHeader::Invalid,
                    ..body_metadata()
                }),
            ),
        )
        .await
        .unwrap_err();
        assert_error(
            duplicate_sdk_checksum_algorithm,
            "InvalidRequest",
            http::StatusCode::BAD_REQUEST,
            "invalid CORS configuration",
        );

        let mut checksum_algorithm = put_request(
            "bucket",
            config.clone(),
            None,
            Some(encoded_md5(TEST_MD5)),
            Some(body_metadata()),
        );
        checksum_algorithm.input.checksum_algorithm = Some(ChecksumAlgorithm::from_static("CRC32"));
        let checksum_algorithm = put_bucket_cors(&state, checksum_algorithm)
            .await
            .unwrap_err();
        assert_error(
            checksum_algorithm,
            "InvalidRequest",
            http::StatusCode::BAD_REQUEST,
            "invalid CORS configuration",
        );

        let mut private_body_configuration = config;
        private_body_configuration.cors_rules[0].allowed_origins =
            vec![format!("{PRIVATE_BODY}\n")];
        let invalid_configuration = put_bucket_cors(
            &state,
            put_request(
                "bucket",
                private_body_configuration,
                None,
                Some(encoded_md5(TEST_MD5)),
                Some(body_metadata()),
            ),
        )
        .await
        .unwrap_err();
        assert_error(
            invalid_configuration,
            "InvalidRequest",
            http::StatusCode::BAD_REQUEST,
            "invalid CORS configuration",
        );
    }

    #[tokio::test]
    async fn put_accepts_crc64nvme_and_rejects_crc_integrity_matrix() {
        let state = state_with_bucket(Some("owner")).await;
        let config = configuration("digest", "https://safe.example");

        let mut crc_only = put_request(
            "bucket",
            config.clone(),
            None,
            None,
            Some(CorsPutBodyMetadata {
                supplied_crc64nvme: Crc64NvmeHeader::Value(TEST_CRC64NVME),
                ..body_metadata()
            }),
        );
        crc_only.input.checksum_algorithm = Some(ChecksumAlgorithm::from_static("CRC64NVME"));
        put_bucket_cors(&state, crc_only).await.unwrap();

        let cases = [
            (
                Crc64NvmeHeader::Absent,
                Some(ChecksumAlgorithm::from_static("CRC64NVME")),
                Some(encoded_md5(TEST_MD5)),
                "InvalidRequest",
            ),
            (
                Crc64NvmeHeader::Invalid,
                Some(ChecksumAlgorithm::from_static("CRC64NVME")),
                Some(encoded_md5(TEST_MD5)),
                "InvalidDigest",
            ),
            (
                Crc64NvmeHeader::Value([0; 8]),
                Some(ChecksumAlgorithm::from_static("CRC64NVME")),
                Some(encoded_md5(TEST_MD5)),
                "BadDigest",
            ),
            (
                Crc64NvmeHeader::Value(TEST_CRC64NVME),
                None,
                Some(encoded_md5(TEST_MD5)),
                "InvalidRequest",
            ),
            (
                Crc64NvmeHeader::Invalid,
                None,
                Some(encoded_md5(TEST_MD5)),
                "InvalidRequest",
            ),
            (
                Crc64NvmeHeader::Value(TEST_CRC64NVME),
                Some(ChecksumAlgorithm::from_static("CRC32")),
                Some(encoded_md5(TEST_MD5)),
                "InvalidRequest",
            ),
            (
                Crc64NvmeHeader::Value(TEST_CRC64NVME),
                Some(ChecksumAlgorithm::from_static("CRC64NVME")),
                Some(PRIVATE_DIGEST.to_owned()),
                "BadDigest",
            ),
        ];

        for (supplied_crc64nvme, checksum_algorithm, content_md5, code) in cases {
            let mut request = put_request(
                "bucket",
                config.clone(),
                None,
                content_md5,
                Some(CorsPutBodyMetadata {
                    supplied_crc64nvme,
                    ..body_metadata()
                }),
            );
            request.input.checksum_algorithm = checksum_algorithm;
            let error = put_bucket_cors(&state, request).await.unwrap_err();
            assert_error(
                error,
                code,
                http::StatusCode::BAD_REQUEST,
                match code {
                    "InvalidDigest" => "invalid digest",
                    "BadDigest" => "digest mismatch",
                    "InvalidRequest" => "invalid CORS configuration",
                    _ => unreachable!(),
                },
            );
        }

        let mut both_match = put_request(
            "bucket",
            config,
            None,
            Some(encoded_md5(TEST_MD5)),
            Some(CorsPutBodyMetadata {
                supplied_crc64nvme: Crc64NvmeHeader::Value(TEST_CRC64NVME),
                ..body_metadata()
            }),
        );
        both_match.input.checksum_algorithm = Some(ChecksumAlgorithm::from_static("CRC64NVME"));
        put_bucket_cors(&state, both_match).await.unwrap();
    }

    #[tokio::test]
    async fn get_rejects_semantically_corrupt_stored_configuration() {
        let state = state_with_bucket(Some("owner")).await;
        let corrupt = format!(
            r#"{{"rules":[{{"allowed_origins":["{PRIVATE_ORIGIN}"],"allowed_methods":["get"],"allowed_headers":[],"expose_headers":[],"id":"{PRIVATE_DATABASE_DETAIL}","max_age_seconds":null}}]}}"#
        );
        store::cors_config::put_configuration(state.store.db(), "bucket", &corrupt)
            .await
            .unwrap();

        let error = get_bucket_cors(&state, get_request("bucket", None))
            .await
            .unwrap_err();
        assert_error(
            error,
            "InternalError",
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "stored CORS configuration is invalid",
        );
    }
}
