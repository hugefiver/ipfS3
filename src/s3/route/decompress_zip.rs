use std::sync::Arc;

use bytes::Buf as _;
use http::{HeaderMap, Method, Uri};
use http_body_util::BodyExt as _;
use s3s::dto::{ETag, ParseETagError};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result};

use crate::{
    pinning::{policy::PublicationContext, tags::ObjectTag},
    state::AppState,
    store::pinning::publication::{
        PinTargetSpec, PublicationObject, PublicationRequest, ZipPublicationRequest,
    },
};

pub struct DecompressZipRoute {
    state: Arc<AppState>,
}

const MAX_COMPLETE_MULTIPART_XML_BYTES: usize = 4 * 1024 * 1024;

fn invalid_pinning_argument(message: &str) -> s3s::S3Error {
    crate::error::AppError::InvalidPinningRequest(message.to_owned()).into()
}

fn parse_publication_tags(headers: &HeaderMap) -> S3Result<Vec<ObjectTag>> {
    let mut values = headers.get_all("x-amz-tagging").iter();
    let Some(header) = values.next() else {
        return Ok(Vec::new());
    };
    if values.next().is_some() {
        return Err(invalid_pinning_argument("duplicate x-amz-tagging header"));
    }
    let value = header
        .to_str()
        .map_err(|_| invalid_pinning_argument("invalid x-amz-tagging header"))?;
    crate::pinning::tags::parse_tagging_header(value)
        .map_err(|_| invalid_pinning_argument("invalid x-amz-tagging header"))
}

fn evaluate_publication_policy(
    state: &Arc<AppState>,
    bucket: &str,
    key: &str,
    tags: &[ObjectTag],
) -> S3Result<crate::pinning::policy::PublicationPolicy> {
    state
        .pinning
        .policy()
        .evaluate_publication(PublicationContext {
            bucket,
            key,
            tags,
            is_decompress_zip: true,
        })
        .map_err(crate::error::AppError::from)
        .map_err(s3s::S3Error::from)
}

fn insert_version_id_header(headers: &mut HeaderMap, version_id: Option<&str>) -> S3Result<()> {
    if let Some(version_id) = version_id {
        headers.insert(
            "x-amz-version-id",
            http::HeaderValue::from_str(version_id)
                .map_err(|_| s3s::s3_error!(InternalError, "invalid object version ID"))?,
        );
    }
    Ok(())
}

fn publication_entries(
    bucket: &str,
    entries: &[crate::zip::response::ExtractedEntry],
) -> Vec<PublicationObject> {
    entries
        .iter()
        .map(|entry| {
            PublicationObject::from_put(
                uuid::Uuid::new_v4().to_string(),
                bucket,
                &entry.key,
                entry.cid.clone(),
                entry.size,
                None,
                None,
                false,
                None,
                None,
                chrono::Utc::now(),
            )
        })
        .collect()
}

fn complete_xml_too_large() -> s3s::S3Error {
    s3s::s3_error!(InvalidRequest, "CompleteMultipartUpload XML exceeds 4 MiB")
}

async fn collect_complete_xml(body: &mut Body) -> S3Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| {
            s3s::s3_error!(
                IncompleteBody,
                "failed to read CompleteMultipartUpload XML: {error}"
            )
        })?;
        let Ok(mut data) = frame.into_data() else {
            continue;
        };
        let data_len = data.remaining();
        let next_len = bytes
            .len()
            .checked_add(data_len)
            .ok_or_else(complete_xml_too_large)?;
        if next_len > MAX_COMPLETE_MULTIPART_XML_BYTES {
            return Err(complete_xml_too_large());
        }

        bytes.reserve(data_len);
        while data.has_remaining() {
            let chunk = data.chunk();
            let chunk_len = chunk.len();
            bytes.extend_from_slice(chunk);
            data.advance(chunk_len);
        }
    }
    Ok(bytes)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CompleteField {
    PartNumber,
    ETag,
    ChecksumCRC32,
    ChecksumCRC32C,
    ChecksumCRC64NVME,
    ChecksumSHA1,
    ChecksumSHA256,
}

fn append_general_ref(
    value: &mut String,
    reference: quick_xml::events::BytesRef<'_>,
) -> S3Result<()> {
    if reference.is_char_ref() {
        let character = reference
            .resolve_char_ref()
            .map_err(|error| {
                s3s::s3_error!(MalformedXML, "invalid numeric XML reference: {error}")
            })?
            .ok_or_else(|| s3s::s3_error!(MalformedXML, "invalid numeric XML reference"))?;
        value.push(character);
        return Ok(());
    }

    let name = reference
        .decode()
        .map_err(|error| s3s::s3_error!(MalformedXML, "invalid XML entity encoding: {error}"))?;
    let replacement = match name.as_ref() {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "apos" => '\'',
        "quot" => '"',
        _ => return Err(s3s::s3_error!(MalformedXML, "unknown XML entity: {name}")),
    };
    value.push(replacement);
    Ok(())
}

fn malformed_complete_xml(message: impl std::fmt::Display) -> s3s::S3Error {
    s3s::s3_error!(MalformedXML, "{message}")
}

fn parse_complete_etag(value: String) -> S3Result<ETag> {
    match ETag::parse_http_header(value.as_bytes()) {
        Ok(etag) => Ok(etag),
        Err(ParseETagError::InvalidFormat) => Ok(ETag::Strong(value)),
        Err(ParseETagError::InvalidChar) => Err(malformed_complete_xml("invalid ETag character")),
    }
}

fn validate_root_attributes(event: &quick_xml::events::BytesStart<'_>) -> S3Result<()> {
    for attribute in event.attributes() {
        let attribute = attribute.map_err(|error| {
            malformed_complete_xml(format!(
                "invalid CompleteMultipartUpload attribute: {error}"
            ))
        })?;
        let name = attribute.key.as_ref();
        if name != b"xmlns" && !name.starts_with(b"xmlns:") {
            return Err(malformed_complete_xml(
                "CompleteMultipartUpload only permits xmlns attributes",
            ));
        }
        if attribute.value.as_ref().contains(&b'&') {
            return Err(malformed_complete_xml(
                "CompleteMultipartUpload namespace contains an entity reference",
            ));
        }
    }
    Ok(())
}

fn reject_element_attributes(event: &quick_xml::events::BytesStart<'_>) -> S3Result<()> {
    if let Some(attribute) = event.attributes().next() {
        attribute.map_err(|error| {
            malformed_complete_xml(format!(
                "invalid CompleteMultipartUpload attribute: {error}"
            ))
        })?;
        return Err(malformed_complete_xml(
            "Part and CompleteMultipartUpload fields must not have attributes",
        ));
    }
    Ok(())
}

fn parse_complete_multipart_xml(bytes: &[u8]) -> S3Result<Vec<s3s::dto::CompletedPart>> {
    let mut reader = quick_xml::Reader::from_reader(bytes);
    reader.config_mut().trim_text(false);
    let mut buf = Vec::new();
    let mut parts = Vec::new();
    let mut saw_declaration = false;
    let mut saw_root = false;
    let mut closed_root = false;
    let mut in_part = false;
    let mut current_part_number = None;
    let mut current_etag: Option<ETag> = None;
    let mut current_checksum_crc32 = None;
    let mut current_checksum_crc32c = None;
    let mut current_checksum_crc64nvme = None;
    let mut current_checksum_sha1 = None;
    let mut current_checksum_sha256 = None;
    let mut current_field = None;
    let mut field_value = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(event)) => match event.name().as_ref() {
                b"CompleteMultipartUpload" if !saw_root && !closed_root => {
                    validate_root_attributes(&event)?;
                    saw_root = true;
                }
                b"Part" if saw_root && !closed_root && !in_part && current_field.is_none() => {
                    reject_element_attributes(&event)?;
                    in_part = true;
                    current_part_number = None;
                    current_etag = None;
                    current_checksum_crc32 = None;
                    current_checksum_crc32c = None;
                    current_checksum_crc64nvme = None;
                    current_checksum_sha1 = None;
                    current_checksum_sha256 = None;
                }
                b"PartNumber"
                    if in_part && current_field.is_none() && current_part_number.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::PartNumber);
                    field_value.clear();
                }
                b"ETag" if in_part && current_field.is_none() && current_etag.is_none() => {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ETag);
                    field_value.clear();
                }
                b"ChecksumCRC32"
                    if in_part && current_field.is_none() && current_checksum_crc32.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumCRC32);
                    field_value.clear();
                }
                b"ChecksumCRC32C"
                    if in_part && current_field.is_none() && current_checksum_crc32c.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumCRC32C);
                    field_value.clear();
                }
                b"ChecksumCRC64NVME"
                    if in_part
                        && current_field.is_none()
                        && current_checksum_crc64nvme.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumCRC64NVME);
                    field_value.clear();
                }
                b"ChecksumSHA1"
                    if in_part && current_field.is_none() && current_checksum_sha1.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumSHA1);
                    field_value.clear();
                }
                b"ChecksumSHA256"
                    if in_part && current_field.is_none() && current_checksum_sha256.is_none() =>
                {
                    reject_element_attributes(&event)?;
                    current_field = Some(CompleteField::ChecksumSHA256);
                    field_value.clear();
                }
                _ => {
                    return Err(malformed_complete_xml(
                        "unexpected CompleteMultipartUpload element or nesting",
                    ));
                }
            },
            Ok(quick_xml::events::Event::Text(text)) => {
                let decoded = text.decode().map_err(|error| {
                    s3s::s3_error!(
                        MalformedXML,
                        "invalid CompleteMultipartUpload text encoding: {error}"
                    )
                })?;
                if current_field.is_some() {
                    field_value.push_str(decoded.as_ref());
                } else if !decoded.trim().is_empty() {
                    return Err(malformed_complete_xml(
                        "non-whitespace text outside CompleteMultipartUpload fields",
                    ));
                }
            }
            Ok(quick_xml::events::Event::GeneralRef(reference)) => match current_field {
                Some(_) => append_general_ref(&mut field_value, reference)?,
                None => {
                    return Err(malformed_complete_xml(
                        "entity reference outside CompleteMultipartUpload fields",
                    ));
                }
            },
            Ok(quick_xml::events::Event::Empty(event)) => {
                reject_element_attributes(&event)?;
                match event.name().as_ref() {
                    b"ChecksumCRC32"
                        if in_part
                            && current_field.is_none()
                            && current_checksum_crc32.is_none() =>
                    {
                        current_checksum_crc32 = Some(String::new());
                    }
                    b"ChecksumCRC32C"
                        if in_part
                            && current_field.is_none()
                            && current_checksum_crc32c.is_none() =>
                    {
                        current_checksum_crc32c = Some(String::new());
                    }
                    b"ChecksumCRC64NVME"
                        if in_part
                            && current_field.is_none()
                            && current_checksum_crc64nvme.is_none() =>
                    {
                        current_checksum_crc64nvme = Some(String::new());
                    }
                    b"ChecksumSHA1"
                        if in_part
                            && current_field.is_none()
                            && current_checksum_sha1.is_none() =>
                    {
                        current_checksum_sha1 = Some(String::new());
                    }
                    b"ChecksumSHA256"
                        if in_part
                            && current_field.is_none()
                            && current_checksum_sha256.is_none() =>
                    {
                        current_checksum_sha256 = Some(String::new());
                    }
                    b"ETag" if in_part && current_field.is_none() && current_etag.is_none() => {
                        current_etag = Some(parse_complete_etag(String::new())?);
                    }
                    _ => {
                        return Err(malformed_complete_xml(
                            "unexpected self-closing CompleteMultipartUpload element or nesting",
                        ));
                    }
                }
            }
            Ok(quick_xml::events::Event::End(event)) => match event.name().as_ref() {
                b"PartNumber" if in_part && current_field == Some(CompleteField::PartNumber) => {
                    let value = std::mem::take(&mut field_value);
                    current_part_number = Some(
                        value
                            .trim()
                            .parse::<i32>()
                            .map_err(|_| s3s::s3_error!(MalformedXML, "invalid PartNumber"))?,
                    );
                    current_field = None;
                }
                b"ETag" if in_part && current_field == Some(CompleteField::ETag) => {
                    let value = std::mem::take(&mut field_value);
                    current_etag = Some(parse_complete_etag(value)?);
                    current_field = None;
                }
                b"ChecksumCRC32"
                    if in_part && current_field == Some(CompleteField::ChecksumCRC32) =>
                {
                    current_checksum_crc32 = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"ChecksumCRC32C"
                    if in_part && current_field == Some(CompleteField::ChecksumCRC32C) =>
                {
                    current_checksum_crc32c = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"ChecksumCRC64NVME"
                    if in_part && current_field == Some(CompleteField::ChecksumCRC64NVME) =>
                {
                    current_checksum_crc64nvme = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"ChecksumSHA1"
                    if in_part && current_field == Some(CompleteField::ChecksumSHA1) =>
                {
                    current_checksum_sha1 = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"ChecksumSHA256"
                    if in_part && current_field == Some(CompleteField::ChecksumSHA256) =>
                {
                    current_checksum_sha256 = Some(std::mem::take(&mut field_value));
                    current_field = None;
                }
                b"Part" if in_part && current_field.is_none() => {
                    let part_number = current_part_number
                        .ok_or_else(|| malformed_complete_xml("PartNumber is required"))?;
                    let e_tag = current_etag
                        .take()
                        .ok_or_else(|| malformed_complete_xml("ETag is required"))?;
                    parts.push(s3s::dto::CompletedPart {
                        part_number: Some(part_number),
                        e_tag: Some(e_tag),
                        checksum_crc32: current_checksum_crc32.take(),
                        checksum_crc32c: current_checksum_crc32c.take(),
                        checksum_crc64nvme: current_checksum_crc64nvme.take(),
                        checksum_sha1: current_checksum_sha1.take(),
                        checksum_sha256: current_checksum_sha256.take(),
                    });
                    in_part = false;
                }
                b"CompleteMultipartUpload"
                    if saw_root && !closed_root && !in_part && current_field.is_none() =>
                {
                    closed_root = true;
                }
                _ => {
                    return Err(malformed_complete_xml(
                        "mismatched or unexpected CompleteMultipartUpload closing element",
                    ));
                }
            },
            Ok(quick_xml::events::Event::Decl(_)) if !saw_root && !saw_declaration => {
                saw_declaration = true;
            }
            Ok(quick_xml::events::Event::Comment(_)) if current_field.is_none() => {}
            Ok(quick_xml::events::Event::Eof) => {
                if saw_root && closed_root && !in_part && current_field.is_none() {
                    break;
                }
                return Err(malformed_complete_xml(
                    "incomplete CompleteMultipartUpload document",
                ));
            }
            Err(error) => {
                return Err(s3s::s3_error!(
                    MalformedXML,
                    "invalid CompleteMultipartUpload XML: {error}"
                ));
            }
            Ok(_) => {
                return Err(malformed_complete_xml(
                    "unsupported CompleteMultipartUpload XML content",
                ));
            }
        }
        buf.clear();
    }

    Ok(parts)
}

impl DecompressZipRoute {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    pub(super) async fn call_authenticated(
        &self,
        req: S3Request<Body>,
    ) -> S3Result<S3Response<Body>> {
        if req.method == Method::PUT {
            return self.call_put(req).await;
        }
        if req.method == Method::POST {
            return self.call_complete(req).await;
        }
        Err(s3s::s3_error!(
            MethodNotAllowed,
            "unsupported decompress route method"
        ))
    }

    async fn call_put(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        self.call_put_with_decompressed_limit(
            req,
            crate::zip::extract::MAX_DECOMPRESSED_ARCHIVE_BYTES,
        )
        .await
    }

    async fn call_put_with_decompressed_limit(
        &self,
        req: S3Request<Body>,
        max_decompressed_bytes: u64,
    ) -> S3Result<S3Response<Body>> {
        let parsed = parse_decompress_put_uri(&req.uri)?;
        crate::s3::http::reject_write_conditions(&req.headers, false)?;
        crate::s3::ops::storage_class::require_standard_write_headers(&req.headers)?;
        if has_sse_header(&req.headers) {
            return Err(s3s::s3_error!(
                InvalidArgument,
                "decompress-zip does not support server-side encryption in MVP"
            ));
        }
        if !crate::store::bucket::exists(self.state.store.db(), &parsed.bucket).await? {
            return Err(s3s::s3_error!(
                NoSuchBucket,
                "bucket not found: {}",
                parsed.bucket
            ));
        }

        let tags = parse_publication_tags(&req.headers)?;
        let policy = evaluate_publication_policy(&self.state, &parsed.bucket, &parsed.key, &tags)?;

        let content_type = req
            .headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let metadata = crate::s3::ops::object::extract_custom_metadata(&req.headers);

        let mutation_guard = crate::store::import::ownership::admit_content_and_prefix_mutation(
            self.state.store.db(),
            &parsed.bucket,
            &parsed.key,
            &parsed.target_prefix,
            crate::import::SupersedeReason::DecompressZip,
            chrono::Utc::now(),
        )
        .await?;

        crate::store::import::ownership::run_mutation(
            self.state.store.db(),
            &mutation_guard.clone(),
            |lease| async move {
                let archive =
                    crate::s3::ops::object::add_plain_object_stream(&self.state, req.input).await?;
                let archive_stream =
                    crate::kubo::cat::stream_cat(&self.state.kubo, &archive.cid, None).await?;
                let outcome = crate::zip::extract::extract_zip_stream_with_limit(
                    &self.state,
                    &parsed.target_prefix,
                    archive_stream,
                    max_decompressed_bytes,
                )
                .await?;

                reject_archive_key_collision(&parsed.key, &outcome.entries)?;

                let published = outcome.entries;
                let failures = outcome.failures;
                let archive_object = PublicationObject::from_put(
                    uuid::Uuid::new_v4().to_string(),
                    &parsed.bucket,
                    &parsed.key,
                    archive.cid.clone(),
                    archive.size,
                    content_type,
                    metadata,
                    false,
                    None,
                    None,
                    chrono::Utc::now(),
                );
                let request = ZipPublicationRequest {
                    archive: PublicationRequest {
                        object: archive_object,
                        tags: tags.clone(),
                        policy,
                        object_target: PinTargetSpec {
                            cid: archive.cid.clone(),
                            logical_size: archive.size,
                        },
                    },
                    entries: publication_entries(&parsed.bucket, &published),
                };
                let publication_result = lease
                    .commit(crate::store::pinning::publication::publish_standard_zip(
                        self.state.store.db(),
                        request,
                        mutation_guard,
                        self.state.pinning.provider_limits(),
                    ))
                    .await?;

                let mut headers = HeaderMap::new();
                headers.insert(
                    http::header::ETAG,
                    http::HeaderValue::from_str(&format!("\"{}\"", archive.cid)).unwrap(),
                );
                insert_version_id_header(&mut headers, publication_result.version_id.as_deref())?;
                if parsed.return_result_xml {
                    let result = crate::zip::response::DecompressZipResult {
                        archive_key: parsed.key,
                        archive_cid: archive.cid,
                        archive_size: archive.size,
                        entries: published,
                        failures,
                    };
                    headers.insert(
                        http::header::CONTENT_TYPE,
                        http::HeaderValue::from_static("application/xml"),
                    );
                    Ok(S3Response::with_headers(
                        Body::from(crate::zip::response::decompress_result_xml(&result)),
                        headers,
                    ))
                } else {
                    Ok(S3Response::with_headers(Body::empty(), headers))
                }
            },
        )
        .await
    }

    async fn call_complete(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        crate::s3::http::reject_write_conditions(&req.headers, false)?;
        crate::s3::ops::storage_class::require_standard_write_headers(&req.headers)?;
        let (bucket, key) = parse_path_bucket_key(&req.uri)?;
        let upload_id = crate::s3::query::decoded_query_pairs(&req.uri)?
            .into_iter()
            .filter_map(|(name, value)| (name == "uploadId").then_some(value))
            .next_back()
            .ok_or_else(|| s3s::s3_error!(InvalidArgument, "uploadId is required"))?;

        let body_bytes = collect_complete_xml(&mut req.input).await?;
        let parts = parse_complete_multipart_xml(&body_bytes)?;
        let input = s3s::dto::CompleteMultipartUploadInput {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id,
            multipart_upload: Some(s3s::dto::CompletedMultipartUpload { parts: Some(parts) }),
            ..Default::default()
        };
        let inner_req = S3Request {
            input,
            method: req.method,
            uri: req.uri,
            headers: req.headers,
            extensions: req.extensions,
            credentials: req.credentials,
            region: req.region,
            service: req.service,
            trailing_headers: req.trailing_headers,
        };
        let completed =
            crate::s3::ops::multipart::complete_multipart_upload_inner(&self.state, inner_req)
                .await?;

        let lease = completed
            .mutation_lease
            .clone()
            .ok_or_else(|| s3s::s3_error!(InternalError, "missing multipart mutation lease"))?;
        let result = lease
            .run(async {
                let mut headers = HeaderMap::new();
                headers.insert(
                    http::header::CONTENT_TYPE,
                    http::HeaderValue::from_static("application/xml"),
                );
                headers.insert(
                    http::header::ETAG,
                    http::HeaderValue::from_str(&format!("\"{}\"", completed.root_cid)).unwrap(),
                );
                if let Some(sse) = &completed.server_side_encryption {
                    headers.insert(
                        "x-amz-server-side-encryption",
                        http::HeaderValue::from_str(sse.as_str()).unwrap(),
                    );
                }

                if let Some(target_prefix) = completed.decompress_zip_target.clone() {
                    let archive_stream =
                        crate::kubo::cat::stream_cat(&self.state.kubo, &completed.root_cid, None)
                            .await?;
                    let outcome = crate::zip::extract::extract_zip_stream(
                        &self.state,
                        &target_prefix,
                        archive_stream,
                    )
                    .await?;

                    reject_archive_key_collision(&completed.key, &outcome.entries)?;
                    let published = outcome.entries;
                    let failures = outcome.failures;
                    let request = ZipPublicationRequest {
                        archive: crate::s3::ops::multipart::completed_publication_request(
                            &completed,
                        ),
                        entries: publication_entries(&completed.bucket, &published),
                    };
                    let publication_result =
                        crate::s3::ops::multipart::finalize_completed_multipart_zip(
                            &self.state,
                            &completed,
                            request,
                        )
                        .await?;
                    insert_version_id_header(
                        &mut headers,
                        publication_result.version_id.as_deref(),
                    )?;

                    let xml = if completed.decompress_zip_result {
                        crate::zip::response::decompress_result_xml(
                            &crate::zip::response::DecompressZipResult {
                                archive_key: completed.key.clone(),
                                archive_cid: completed.root_cid.clone(),
                                archive_size: completed.total_size,
                                entries: published,
                                failures,
                            },
                        )
                    } else {
                        crate::zip::response::complete_multipart_result_xml(
                            &completed.bucket,
                            &completed.key,
                            &completed.root_cid,
                        )
                    };
                    return Ok(S3Response::with_headers(Body::from(xml), headers));
                }

                let publication_result =
                    crate::s3::ops::multipart::finalize_completed_multipart_archive(
                        &self.state,
                        &completed,
                    )
                    .await?;
                insert_version_id_header(&mut headers, publication_result.version_id.as_deref())?;
                let xml = crate::zip::response::complete_multipart_result_xml(
                    &completed.bucket,
                    &completed.key,
                    &completed.root_cid,
                );
                Ok(S3Response::with_headers(Body::from(xml), headers))
            })
            .await;
        lease.finish().await;
        result
    }
}

#[derive(Debug)]
struct ParsedDecompressPut {
    bucket: String,
    key: String,
    target_prefix: String,
    return_result_xml: bool,
}

fn parse_path_bucket_key(uri: &Uri) -> S3Result<(String, String)> {
    let path = uri.path().trim_start_matches('/');
    let (bucket, key) = path
        .split_once('/')
        .ok_or_else(|| s3s::s3_error!(InvalidArgument, "path-style bucket and key are required"))?;
    let bucket = percent_encoding::percent_decode_str(bucket)
        .decode_utf8()
        .map_err(|_| s3s::s3_error!(InvalidArgument, "bucket is not valid UTF-8"))?
        .into_owned();
    let key = percent_encoding::percent_decode_str(key)
        .decode_utf8()
        .map_err(|_| s3s::s3_error!(InvalidArgument, "key is not valid UTF-8"))?
        .into_owned();
    if bucket.is_empty() || key.is_empty() {
        return Err(s3s::s3_error!(
            InvalidArgument,
            "bucket and key are required"
        ));
    }
    Ok((bucket, key))
}

fn parse_decompress_put_uri(uri: &Uri) -> S3Result<ParsedDecompressPut> {
    let (bucket, key) = parse_path_bucket_key(uri)?;
    let mut target = None;
    let mut return_result_xml = true;
    for (name, value) in crate::s3::query::decoded_query_pairs(uri)? {
        if name == "decompress-zip" {
            target = Some(value);
        } else if name == "decompress-zip-result" {
            return_result_xml = value != "false";
        }
    }
    let target_prefix =
        crate::zip::sanitize::normalize_target_prefix(target.as_deref().unwrap_or(""))
            .map_err(s3s::S3Error::from)?;

    Ok(ParsedDecompressPut {
        bucket,
        key,
        target_prefix,
        return_result_xml,
    })
}

fn has_sse_header(headers: &HeaderMap) -> bool {
    headers.contains_key("x-amz-server-side-encryption")
        || headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
        || headers.contains_key("x-amz-server-side-encryption-customer-key")
        || headers.contains_key("x-amz-server-side-encryption-customer-key-MD5")
}

pub(crate) fn reject_archive_key_collision(
    archive_key: &str,
    entries: &[crate::zip::response::ExtractedEntry],
) -> S3Result<()> {
    if entries.iter().any(|entry| entry.key == archive_key) {
        let mut error = s3s::S3Error::with_message(
            s3s::S3ErrorCode::Custom("InvalidParameterValue".into()),
            format!("zip entry collides with archive key: {archive_key}"),
        );
        error.set_status_code(http::StatusCode::BAD_REQUEST);
        return Err(error);
    }
    Ok(())
}

#[async_trait::async_trait]
impl S3Route for DecompressZipRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        _headers: &HeaderMap,
        _extensions: &mut http::Extensions,
    ) -> bool {
        (*method == Method::PUT && crate::s3::query::query_key_is_present(uri, "decompress-zip"))
            || (*method == Method::POST
                && crate::s3::query::query_key_is_present(uri, "uploadId")
                && !crate::s3::query::query_key_is_present(uri, "uploads"))
    }

    async fn call(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        self.check_access(&mut req).await?;
        self.call_authenticated(req).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use http_body_util::BodyExt;
    use sea_orm::{
        ColumnTrait, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    };
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    const HELLO: &[u8] = b"hello";

    #[derive(Clone, Copy)]
    struct ZipEntryFixture<'a> {
        name: &'a [u8],
        data: &'a [u8],
    }

    fn push_u16(out: &mut Vec<u8>, value: u16) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn push_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_le_bytes());
    }

    fn crc32(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &byte in bytes {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0xedb8_8320 & (0u32.wrapping_sub(crc & 1)));
            }
        }
        !crc
    }

    fn zip(entries: &[ZipEntryFixture<'_>]) -> Vec<u8> {
        let mut output = Vec::new();
        let mut offsets = Vec::with_capacity(entries.len());

        for entry in entries {
            let crc = crc32(entry.data);
            offsets.push(output.len() as u32);
            push_u32(&mut output, 0x0403_4b50);
            push_u16(&mut output, 20);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u32(&mut output, crc);
            push_u32(&mut output, entry.data.len() as u32);
            push_u32(&mut output, entry.data.len() as u32);
            push_u16(&mut output, entry.name.len() as u16);
            push_u16(&mut output, 0);
            output.extend_from_slice(entry.name);
            output.extend_from_slice(entry.data);
        }

        let central_offset = output.len() as u32;
        for (entry, offset) in entries.iter().zip(offsets) {
            let crc = crc32(entry.data);
            push_u32(&mut output, 0x0201_4b50);
            push_u16(&mut output, 20);
            push_u16(&mut output, 20);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u32(&mut output, crc);
            push_u32(&mut output, entry.data.len() as u32);
            push_u32(&mut output, entry.data.len() as u32);
            push_u16(&mut output, entry.name.len() as u16);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u16(&mut output, 0);
            push_u32(&mut output, 0);
            push_u32(&mut output, offset);
            output.extend_from_slice(entry.name);
        }

        let central_size = output.len() as u32 - central_offset;
        push_u32(&mut output, 0x0605_4b50);
        push_u16(&mut output, 0);
        push_u16(&mut output, 0);
        push_u16(&mut output, entries.len() as u16);
        push_u16(&mut output, entries.len() as u16);
        push_u32(&mut output, central_size);
        push_u32(&mut output, central_offset);
        push_u16(&mut output, 0);
        output
    }

    async fn route_with_mock_kubo_responses_and_coordinator(
        add_responses: Vec<ResponseTemplate>,
        archive_body: Vec<u8>,
        pinning: Arc<crate::pinning::coordinator::PinningCoordinator>,
    ) -> (DecompressZipRoute, Arc<AppState>, MockServer) {
        let kubo = MockServer::start().await;
        let response_index = Arc::new(AtomicUsize::new(0));
        let add_responses = Arc::new(add_responses);
        if !add_responses.is_empty() {
            Mock::given(method("POST"))
                .and(path("/api/v0/add"))
                .respond_with({
                    let response_index = response_index.clone();
                    let add_responses = add_responses.clone();
                    move |_: &wiremock::Request| {
                        let index = response_index.fetch_add(1, Ordering::SeqCst);
                        add_responses[index].clone()
                    }
                })
                .up_to_n_times(add_responses.len() as u64)
                .mount(&kubo)
                .await;
        }
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"))
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(archive_body))
            .mount(&kubo)
            .await;

        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        let state = Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo.uri()),
            cold_kubo: None,
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
            pinning,
        });
        (DecompressZipRoute::new(state.clone()), state, kubo)
    }

    async fn route_with_mock_kubo_and_coordinator(
        add_bodies: Vec<&'static str>,
        archive_body: Vec<u8>,
        pinning: Arc<crate::pinning::coordinator::PinningCoordinator>,
    ) -> (DecompressZipRoute, Arc<AppState>, MockServer) {
        route_with_mock_kubo_responses_and_coordinator(
            add_bodies
                .into_iter()
                .map(|body| ResponseTemplate::new(200).set_body_string(body))
                .collect(),
            archive_body,
            pinning,
        )
        .await
    }

    async fn route_with_mock_kubo(
        add_bodies: Vec<&'static str>,
        archive_body: Vec<u8>,
    ) -> (DecompressZipRoute, Arc<AppState>, MockServer) {
        route_with_mock_kubo_and_coordinator(
            add_bodies,
            archive_body,
            crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        )
        .await
    }

    fn pinning_coordinator(
        trigger: &str,
        provider_mode: &str,
    ) -> Arc<crate::pinning::coordinator::PinningCoordinator> {
        use crate::config::{PinningConfig, PolicyConfig, ProviderConfig};
        use crate::pinning::config::ValidatedPinningConfig;

        let provider = |name: &str, priority: u32| ProviderConfig {
            name: name.to_owned(),
            kind: "noop".to_owned(),
            token_env: None,
            endpoint: None,
            api: None,
            strategy: None,
            upload_endpoint: None,
            enabled: true,
            priority,
            max_bytes: 10_000,
            max_pins: 100,
            requests_per_second: None,
        };
        let config = ValidatedPinningConfig::from_raw(
            &PinningConfig {
                worker_interval: "5s".to_owned(),
                worker_concurrency: 2,
                providers: vec![provider("alpha", 1), provider("beta", 2)],
                policies: vec![PolicyConfig {
                    bucket: "bucket".to_owned(),
                    prefix: String::new(),
                    trigger: trigger.to_owned(),
                    provider_mode: provider_mode.to_owned(),
                    providers: vec!["alpha".to_owned(), "beta".to_owned()],
                    default_duration: "1h".to_owned(),
                    max_duration: "24h".to_owned(),
                    allow_decompressed: true,
                }],
            },
            |_| None,
        )
        .unwrap();
        crate::pinning::coordinator::PinningCoordinator::build(config).unwrap()
    }

    async fn pinning_route_with_mock_kubo(
        add_bodies: Vec<&'static str>,
        archive_body: Vec<u8>,
        trigger: &str,
        provider_mode: &str,
    ) -> (DecompressZipRoute, Arc<AppState>, MockServer) {
        route_with_mock_kubo_and_coordinator(
            add_bodies,
            archive_body,
            pinning_coordinator(trigger, provider_mode),
        )
        .await
    }

    fn decompressed_tagging_request(method: Method, uri: &str, body: Body) -> S3Request<Body> {
        let mut request = signed_route_request(method, uri, body);
        request.headers.insert(
            "x-amz-tagging",
            http::HeaderValue::from_static(
                "team=storage&ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1h&ipfs-s3%3Acontent=decompressed",
            ),
        );
        request
    }

    async fn archive_leases(
        state: &Arc<AppState>,
    ) -> Vec<crate::store::entities::pin_lease::Model> {
        use crate::store::entities::pin_lease;

        let archive = crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip")
            .await
            .unwrap();
        pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq(archive.id))
            .order_by_asc(pin_lease::Column::Source)
            .all(state.store.db())
            .await
            .unwrap()
    }

    async fn lease_target_cids(state: &Arc<AppState>, lease_id: &str) -> Vec<String> {
        use crate::store::entities::pin_lease_target;

        let mut cids = pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
            .all(state.store.db())
            .await
            .unwrap()
            .into_iter()
            .map(|target| target.cid)
            .collect::<Vec<_>>();
        cids.sort();
        cids
    }

    async fn lease_targets(
        state: &Arc<AppState>,
        lease_id: &str,
    ) -> Vec<crate::store::entities::pin_lease_target::Model> {
        use crate::store::entities::pin_lease_target;

        pin_lease_target::Entity::find()
            .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
            .order_by_asc(pin_lease_target::Column::Provider)
            .order_by_asc(pin_lease_target::Column::Cid)
            .all(state.store.db())
            .await
            .unwrap()
    }

    async fn assert_no_zip_publication_rows(state: &Arc<AppState>) {
        use crate::store::entities::{
            object, object_tag, pin_job, pin_lease, pin_lease_target, pin_provider_usage,
            remote_pin,
        };

        assert_eq!(
            object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            object_tag::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            pin_lease::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            remote_pin::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            pin_provider_usage::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            pin_job::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
    }

    fn put_tagging_request(
        key: &str,
        pairs: &[(&str, &str)],
    ) -> S3Request<s3s::dto::PutObjectTaggingInput> {
        S3Request {
            input: s3s::dto::PutObjectTaggingInput {
                bucket: "bucket".to_owned(),
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                key: key.to_owned(),
                request_payer: None,
                tagging: s3s::dto::Tagging {
                    tag_set: pairs
                        .iter()
                        .map(|(key, value)| s3s::dto::Tag {
                            key: Some((*key).to_owned()),
                            value: Some((*value).to_owned()),
                        })
                        .collect(),
                },
                version_id: None,
            },
            method: Method::PUT,
            uri: format!("/bucket/{key}?tagging").parse().unwrap(),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    async fn published_pinning_zip(provider_mode: &str) -> (Arc<AppState>, MockServer) {
        let archive_body = zip(&[
            ZipEntryFixture {
                name: b"a.txt",
                data: HELLO,
            },
            ZipEntryFixture {
                name: b"b.txt",
                data: HELLO,
            },
        ]);
        let (route, state, kubo) = pinning_route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmEntryA\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmEntryB\",\"Size\":\"5\"}\n",
            ],
            archive_body,
            "always",
            provider_mode,
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        route
            .call(decompressed_tagging_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_owned()),
            ))
            .await
            .unwrap();
        (state, kubo)
    }

    fn signed_route_request(method: Method, uri: &str, body: Body) -> S3Request<Body> {
        S3Request {
            input: body,
            method,
            uri: uri.parse().unwrap(),
            headers: HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: Some(s3s::auth::Credentials {
                access_key: "test".to_string(),
                secret_key: s3s::auth::SecretKey::from("test"),
            }),
            region: Some("us-east-1".parse().unwrap()),
            service: Some("s3".to_string()),
            trailing_headers: None,
        }
    }

    async fn assert_no_pin_removes(kubo: &MockServer, cids: &[&str]) {
        let requests = kubo.received_requests().await.unwrap();
        for cid in cids {
            assert!(
                !requests.iter().any(|request| {
                    request.url.path() == "/api/v0/pin/rm"
                        && request.url.query() == Some(&format!("arg={cid}"))
                }),
                "must not remove pin for {cid}"
            );
        }
    }

    async fn seed_plain_multipart(
        state: &Arc<AppState>,
        target: Option<&str>,
        return_result_xml: bool,
        part_size: i64,
    ) {
        crate::store::multipart::create_upload(
            state.store.db(),
            "upload-1",
            "encryption-object-1",
            "bucket",
            "archive.zip",
            "none",
            None,
            None,
            Some("application/zip"),
            None,
            &[],
            target,
            return_result_xml,
        )
        .await
        .unwrap();
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmPart",
            part_size,
            "QmPart",
        )
        .await
        .unwrap();
    }

    async fn route_with_seeded_plain_multipart(
        target: Option<&str>,
        return_result_xml: bool,
        archive_body: Vec<u8>,
        add_bodies: Vec<&'static str>,
    ) -> (DecompressZipRoute, Arc<AppState>, MockServer) {
        let part_size = archive_body.len() as i64;
        let (route, state, kubo) = route_with_mock_kubo(add_bodies, archive_body).await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        seed_plain_multipart(&state, target, return_result_xml, part_size).await;
        (route, state, kubo)
    }

    async fn pinning_route_with_seeded_plain_multipart(
        provider_mode: &str,
        archive_body: Vec<u8>,
        add_bodies: Vec<&'static str>,
    ) -> (DecompressZipRoute, Arc<AppState>, MockServer) {
        let part_size = archive_body.len() as i64;
        let (route, state, kubo) =
            pinning_route_with_mock_kubo(add_bodies, archive_body, "always", provider_mode).await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        let tags = vec![
            ObjectTag::new("team", "storage"),
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("ipfs-s3:duration", "1h"),
            ObjectTag::new("ipfs-s3:content", "decompressed"),
        ];
        crate::store::multipart::create_upload(
            state.store.db(),
            "upload-1",
            "encryption-object-1",
            "bucket",
            "archive.zip",
            "none",
            None,
            None,
            Some("application/zip"),
            None,
            &tags,
            Some("prefix/"),
            true,
        )
        .await
        .unwrap();
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmPart",
            part_size,
            "QmPart",
        )
        .await
        .unwrap();
        (route, state, kubo)
    }

    async fn route_with_seeded_sse_multipart() -> (DecompressZipRoute, Arc<AppState>, MockServer) {
        let kubo = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v0/add"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmRoot\",\"Size\":\"10\"}\n"),
            )
            .mount(&kubo)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/v0/pin/add"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"Pins\":[]}"))
            .mount(&kubo)
            .await;

        let master_key = crate::crypto::key::MasterKey::from_hex(
            "0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let object_key = crate::crypto::key::ObjectKey { bytes: [42; 32] };
        let wrapped_key = master_key.wrap(&object_key).unwrap();
        let nonce = [0x31; 12];
        let encrypted_part =
            crate::crypto::aes_gcm::encrypt_chunk(&object_key, &nonce, b"plain part").unwrap();
        Mock::given(method("POST"))
            .and(path("/api/v0/cat"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(encrypted_part.to_vec()))
            .mount(&kubo)
            .await;

        let db = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        crate::store::run_migrations(&db).await.unwrap();
        let state = Arc::new(AppState {
            kubo: crate::kubo::KuboClient::new(kubo.uri()),
            cold_kubo: None,
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key,
            pinning: crate::pinning::coordinator::PinningCoordinator::disabled_for_test(),
        });
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        crate::store::multipart::create_upload(
            state.store.db(),
            "upload-1",
            "encryption-object-1",
            "bucket",
            "archive.zip",
            "sse_s3",
            Some(&wrapped_key),
            None,
            Some("application/zip"),
            None,
            &[],
            None,
            true,
        )
        .await
        .unwrap();
        crate::store::multipart::upsert_part(
            state.store.db(),
            "upload-1",
            1,
            "QmPart",
            10,
            "QmPart",
        )
        .await
        .unwrap();

        (DecompressZipRoute::new(state.clone()), state, kubo)
    }

    fn framed_complete_body(
        frames: Vec<Result<hyper::body::Frame<bytes::Bytes>, std::io::Error>>,
    ) -> Body {
        Body::http_body_unsync(http_body_util::StreamBody::new(futures_util::stream::iter(
            frames,
        )))
    }

    #[test]
    fn parse_complete_multipart_xml_extracts_parts_in_order() {
        let xml = r#"<?xml version="1.0"?>
        <!-- optional document comment -->
        <CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
            <Part><PartNumber>2</PartNumber><ETag>etag-2</ETag></Part>
        </CompleteMultipartUpload><!-- optional trailing comment -->"#;

        let parts = parse_complete_multipart_xml(xml.as_bytes()).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, Some(1));
        assert_eq!(
            parts[0].e_tag.as_ref().map(|etag| etag.value()),
            Some("etag-1")
        );
        assert_eq!(parts[1].part_number, Some(2));
        assert_eq!(
            parts[1].e_tag.as_ref().map(|etag| etag.value()),
            Some("etag-2")
        );
    }

    #[test]
    fn parse_complete_multipart_xml_preserves_standard_checksum_fields() {
        let xml = r#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag-1</ETag><ChecksumCRC32>crc32-value</ChecksumCRC32><ChecksumCRC32C>crc32c-value</ChecksumCRC32C><ChecksumCRC64NVME>crc64nvme-value</ChecksumCRC64NVME><ChecksumSHA1>sha1-value</ChecksumSHA1><ChecksumSHA256>sha256-value</ChecksumSHA256></Part></CompleteMultipartUpload>"#;

        let parts = parse_complete_multipart_xml(xml.as_bytes()).unwrap();

        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number, Some(1));
        assert_eq!(
            parts[0].e_tag.as_ref().map(|etag| etag.value()),
            Some("etag-1")
        );
        assert_eq!(parts[0].checksum_crc32.as_deref(), Some("crc32-value"));
        assert_eq!(parts[0].checksum_crc32c.as_deref(), Some("crc32c-value"));
        assert_eq!(
            parts[0].checksum_crc64nvme.as_deref(),
            Some("crc64nvme-value")
        );
        assert_eq!(parts[0].checksum_sha1.as_deref(), Some("sha1-value"));
        assert_eq!(parts[0].checksum_sha256.as_deref(), Some("sha256-value"));
    }

    #[test]
    fn parse_complete_multipart_xml_preserves_checksum_text_boundaries() {
        let xml = br#"<CompleteMultipartUpload>
            <Part>
                <PartNumber> 1 </PartNumber>
                <ETag>etag-1</ETag>
                <ChecksumCRC32>  crc&amp;32  </ChecksumCRC32>
            </Part>
        </CompleteMultipartUpload>"#;

        let parts = parse_complete_multipart_xml(xml).unwrap();

        assert_eq!(parts[0].checksum_crc32.as_deref(), Some("  crc&32  "));
    }

    #[test]
    fn parse_complete_multipart_xml_accepts_self_closing_checksum_as_empty_string() {
        let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag-1</ETag><ChecksumCRC32/><ChecksumCRC32C/><ChecksumCRC64NVME/><ChecksumSHA1/><ChecksumSHA256/></Part></CompleteMultipartUpload>"#;

        let parts = parse_complete_multipart_xml(xml).unwrap();

        assert_eq!(parts[0].checksum_crc32.as_deref(), Some(""));
        assert_eq!(parts[0].checksum_crc32c.as_deref(), Some(""));
        assert_eq!(parts[0].checksum_crc64nvme.as_deref(), Some(""));
        assert_eq!(parts[0].checksum_sha1.as_deref(), Some(""));
        assert_eq!(parts[0].checksum_sha256.as_deref(), Some(""));
    }

    #[test]
    fn parse_complete_multipart_xml_rejects_duplicate_checksum_field() {
        assert_malformed_complete_xml(
            br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>etag</ETag><ChecksumSHA256/><ChecksumSHA256>second</ChecksumSHA256></Part></CompleteMultipartUpload>"#,
        );
    }

    #[test]
    fn parse_complete_multipart_xml_accumulates_quoted_ampersand_general_refs() {
        let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&quot;etag&amp;1&quot;</ETag></Part></CompleteMultipartUpload>"#;

        let parts = parse_complete_multipart_xml(xml).unwrap();
        assert_eq!(
            parts[0].e_tag.as_ref().map(|etag| etag.value()),
            Some("etag&1")
        );
    }

    #[test]
    fn parse_complete_multipart_xml_preserves_weak_etag() {
        let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>W/&quot;QmPart&quot;</ETag></Part></CompleteMultipartUpload>"#;

        let parts = parse_complete_multipart_xml(xml).unwrap();

        assert!(matches!(
            parts[0].e_tag.as_ref(),
            Some(s3s::dto::ETag::Weak(value)) if value == "QmPart"
        ));
        assert_eq!(
            parts[0].e_tag.as_ref().map(|etag| etag.value()),
            Some("QmPart")
        );
    }

    #[test]
    fn parse_complete_multipart_xml_falls_back_to_raw_strong_etag_on_invalid_format() {
        for (xml_value, expected) in [(r#"&quot;QmPart"#, r#""QmPart"#), ("W/QmPart", "W/QmPart")] {
            let xml = format!(
                "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{xml_value}</ETag></Part></CompleteMultipartUpload>"
            );

            let parts = parse_complete_multipart_xml(xml.as_bytes()).unwrap();

            assert!(matches!(
                parts[0].e_tag.as_ref(),
                Some(s3s::dto::ETag::Strong(actual)) if actual == expected
            ));
        }
    }

    #[test]
    fn parse_complete_multipart_xml_rejects_invalid_character_in_etag() {
        assert_malformed_complete_xml(
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"µ\"</ETag></Part></CompleteMultipartUpload>"
                .as_bytes(),
        );
    }

    #[test]
    fn parse_complete_multipart_xml_accepts_self_closing_etag_as_empty_strong() {
        let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag/></Part></CompleteMultipartUpload>"#;

        let parts = parse_complete_multipart_xml(xml).unwrap();

        assert!(matches!(
            parts[0].e_tag.as_ref(),
            Some(s3s::dto::ETag::Strong(value)) if value.is_empty()
        ));
    }

    #[test]
    fn parse_complete_multipart_xml_rejects_duplicate_etag_including_self_closing_form() {
        assert_malformed_complete_xml(
            br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag/><ETag>QmPart</ETag></Part></CompleteMultipartUpload>"#,
        );
    }

    #[test]
    fn parse_complete_multipart_xml_resolves_decimal_and_hex_numeric_refs() {
        let xml = br#"<CompleteMultipartUpload><Part><PartNumber>&#49;</PartNumber><ETag>&#34;etag&#38;&#x31;&#x22;</ETag></Part></CompleteMultipartUpload>"#;

        let parts = parse_complete_multipart_xml(xml).unwrap();
        assert_eq!(parts[0].part_number, Some(1));
        assert_eq!(
            parts[0].e_tag.as_ref().map(|etag| etag.value()),
            Some("etag&1")
        );
    }

    #[test]
    fn parse_complete_multipart_xml_rejects_unknown_entity() {
        let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&bogus;</ETag></Part></CompleteMultipartUpload>"#;

        assert_eq!(
            parse_complete_multipart_xml(xml)
                .unwrap_err()
                .code()
                .as_str(),
            "MalformedXML"
        );
    }

    #[test]
    fn parse_complete_multipart_xml_rejects_invalid_numeric_ref() {
        let xml = br#"<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>&#xZZ;</ETag></Part></CompleteMultipartUpload>"#;

        assert_eq!(
            parse_complete_multipart_xml(xml)
                .unwrap_err()
                .code()
                .as_str(),
            "MalformedXML"
        );
    }

    fn assert_malformed_complete_xml(xml: &[u8]) {
        assert_eq!(
            parse_complete_multipart_xml(xml)
                .unwrap_err()
                .code()
                .as_str(),
            "MalformedXML"
        );
    }

    #[test]
    fn parse_complete_multipart_xml_rejects_wrong_root() {
        assert_malformed_complete_xml(
            br#"<WrongRoot><Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></WrongRoot>"#,
        );
    }

    #[test]
    fn parse_complete_multipart_xml_rejects_general_ref_outside_a_field() {
        assert_malformed_complete_xml(
            br#"<CompleteMultipartUpload>&bogus;<Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>"#,
        );
    }

    #[test]
    fn parse_complete_multipart_xml_rejects_nested_element_inside_a_field() {
        assert_malformed_complete_xml(
            br#"<CompleteMultipartUpload><Part><PartNumber>1<Unexpected/></PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>"#,
        );
    }

    #[test]
    fn parse_complete_multipart_xml_rejects_non_whitespace_text_outside_a_field() {
        assert_malformed_complete_xml(
            br#"<CompleteMultipartUpload>unexpected<Part><PartNumber>1</PartNumber><ETag>etag</ETag></Part></CompleteMultipartUpload>"#,
        );
    }

    #[tokio::test]
    async fn complete_xml_collector_accepts_exactly_four_mib_across_frames() {
        let first = bytes::Bytes::from(vec![b' '; 2 * 1024 * 1024]);
        let second = bytes::Bytes::from(vec![b' '; 2 * 1024 * 1024]);
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-test-trailer", http::HeaderValue::from_static("ignored"));
        let mut body = framed_complete_body(vec![
            Ok(hyper::body::Frame::data(first)),
            Ok(hyper::body::Frame::data(second)),
            Ok(hyper::body::Frame::trailers(trailers)),
        ]);

        let bytes = collect_complete_xml(&mut body).await.unwrap();
        assert_eq!(bytes.len(), MAX_COMPLETE_MULTIPART_XML_BYTES);
    }

    #[tokio::test]
    async fn complete_xml_collector_rejects_one_byte_over_limit() {
        let mut body = framed_complete_body(vec![
            Ok(hyper::body::Frame::data(bytes::Bytes::from(vec![
                b'x'; MAX_COMPLETE_MULTIPART_XML_BYTES
            ]))),
            Ok(hyper::body::Frame::data(bytes::Bytes::from_static(b"x"))),
        ]);

        let error = collect_complete_xml(&mut body).await.unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidRequest");
        assert_eq!(
            error.message(),
            Some("CompleteMultipartUpload XML exceeds 4 MiB")
        );
    }

    #[tokio::test]
    async fn complete_xml_collector_maps_frame_error_to_incomplete_body() {
        let mut body = framed_complete_body(vec![Err(std::io::Error::other("broken body"))]);

        let error = collect_complete_xml(&mut body).await.unwrap_err();
        assert_eq!(error.code().as_str(), "IncompleteBody");
        assert!(
            error
                .to_string()
                .contains("failed to read CompleteMultipartUpload XML: broken body")
        );
    }

    #[tokio::test]
    async fn route_matches_decompress_put_and_complete_but_not_create() {
        let (route, _, _) = route_with_mock_kubo(Vec::new(), Vec::new()).await;
        let mut extensions = http::Extensions::new();

        assert!(
            route.is_match(
                &Method::PUT,
                &"/bucket/archive.zip?decompress-zip=prefix/"
                    .parse::<Uri>()
                    .unwrap(),
                &HeaderMap::new(),
                &mut extensions,
            )
        );
        assert!(
            route.is_match(
                &Method::PUT,
                &"/bucket/archive.zip?decompress%2Dzip=%FF"
                    .parse::<Uri>()
                    .unwrap(),
                &HeaderMap::new(),
                &mut extensions,
            )
        );
        assert!(
            !route.is_match(
                &Method::GET,
                &"/bucket/archive.zip?decompress-zip=prefix/"
                    .parse::<Uri>()
                    .unwrap(),
                &HeaderMap::new(),
                &mut extensions,
            )
        );
        assert!(!route.is_match(
            &Method::PUT,
            &"/bucket/archive.zip".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            &mut extensions,
        ));
        assert!(
            !route.is_match(
                &Method::POST,
                &"/bucket/archive.zip?uploads&decompress-zip=prefix/"
                    .parse::<Uri>()
                    .unwrap(),
                &HeaderMap::new(),
                &mut extensions,
            )
        );
        assert!(route.is_match(
            &Method::POST,
            &"/bucket/archive.zip?uploadId=abc".parse::<Uri>().unwrap(),
            &HeaderMap::new(),
            &mut extensions,
        ));
        assert!(
            route.is_match(
                &Method::POST,
                &"/bucket/archive.zip?uploadId=abc&decompress-zip=prefix/"
                    .parse::<Uri>()
                    .unwrap(),
                &HeaderMap::new(),
                &mut extensions,
            )
        );
        assert!(
            !route.is_match(
                &Method::POST,
                &"/bucket/archive.zip?uploads=&uploadId=abc"
                    .parse::<Uri>()
                    .unwrap(),
                &HeaderMap::new(),
                &mut extensions,
            )
        );
    }

    #[test]
    fn parse_path_and_query_decodes_once_and_last_values_win() {
        let parsed = parse_decompress_put_uri(
            &"/bucket/folder%20a%252F/archive.zip?decompress-zip=ignored&decompress-zip=prefix%2Fnested%2F&decompress-zip-result=true&decompress-zip-result=false"
                .parse::<Uri>()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(parsed.bucket, "bucket");
        assert_eq!(parsed.key, "folder a%2F/archive.zip");
        assert_eq!(parsed.target_prefix, "prefix/nested/");
        assert!(!parsed.return_result_xml);
    }

    #[test]
    fn parse_decompress_put_accepts_empty_prefix_and_rejects_invalid_utf8() {
        let empty = parse_decompress_put_uri(
            &"/bucket/archive.zip?decompress-zip="
                .parse::<Uri>()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(empty.target_prefix, "");

        let invalid = parse_decompress_put_uri(
            &"/bucket/archive.zip?decompress-zip=%FF"
                .parse::<Uri>()
                .unwrap(),
        )
        .unwrap_err();
        assert_eq!(invalid.code().as_str(), "InvalidArgument");
    }

    #[test]
    fn all_sse_headers_are_rejected() {
        for name in [
            "x-amz-server-side-encryption",
            "x-amz-server-side-encryption-customer-algorithm",
            "x-amz-server-side-encryption-customer-key",
            "x-amz-server-side-encryption-customer-key-MD5",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(name, http::HeaderValue::from_static("AES256"));
            assert!(has_sse_header(&headers), "{name} must reject");
        }
    }

    #[test]
    fn archive_key_collision_returns_fixed_invalid_parameter_value() {
        let entries = vec![crate::zip::response::ExtractedEntry {
            key: "archive.zip".to_string(),
            cid: "QmEntry".to_string(),
            size: 5,
        }];

        let error = reject_archive_key_collision("archive.zip", &entries).unwrap_err();
        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_eq!(
            error.message(),
            Some("zip entry collides with archive key: archive.zip")
        );
    }

    #[test]
    fn archive_key_collision_allows_non_matching_successful_entries() {
        let entries = vec![crate::zip::response::ExtractedEntry {
            key: "prefix/file.txt".to_string(),
            cid: "QmEntry".to_string(),
            size: 5,
        }];

        reject_archive_key_collision("archive.zip", &entries).unwrap();
    }

    #[tokio::test]
    async fn put_decompress_zip_returns_xml_and_publishes_archive_and_entries() {
        let archive_body = zip(&[ZipEntryFixture {
            name: b"foo/bar.txt",
            data: HELLO,
        }]);
        let (route, state, _kubo) = route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n",
            ],
            archive_body,
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        crate::store::bucket::set_versioning_state(
            state.store.db(),
            "bucket",
            crate::store::object_version::BucketVersioningState::Enabled,
        )
        .await
        .unwrap();

        let response = route
            .call(signed_route_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_string()),
            ))
            .await
            .unwrap();

        assert_eq!(
            response.headers.get(http::header::ETAG).unwrap(),
            "\"QmArchive\""
        );
        uuid::Uuid::parse_str(
            response
                .headers
                .get("x-amz-version-id")
                .unwrap()
                .to_str()
                .unwrap(),
        )
        .unwrap();
        let xml = response.output.collect().await.unwrap().to_bytes();
        let xml = std::str::from_utf8(&xml).unwrap();
        assert!(xml.contains("<DecompressZipResult>"));
        assert!(xml.contains("<Key>prefix/foo/bar.txt</Key>"));

        let archive = crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip")
            .await
            .unwrap();
        assert_eq!(archive.cid, "QmArchive");
        let entry =
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/foo/bar.txt")
                .await
                .unwrap();
        assert_eq!(entry.cid, "QmEntry");
    }

    #[tokio::test]
    async fn pinning_decompressed_targets_successful_entries_not_archive_and_generated_has_no_lease()
     {
        use crate::store::entities::{object_tag, pin_lease};

        let archive_body = zip(&[ZipEntryFixture {
            name: b"file.txt",
            data: HELLO,
        }]);
        let (route, state, _kubo) = pinning_route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n",
            ],
            archive_body,
            "always",
            "one",
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();

        route
            .call(decompressed_tagging_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_owned()),
            ))
            .await
            .unwrap();

        let leases = archive_leases(&state).await;
        assert_eq!(leases.len(), 2);
        let automatic = leases
            .iter()
            .find(|lease| lease.source == "automatic")
            .unwrap();
        let manual = leases
            .iter()
            .find(|lease| lease.source == "manual")
            .unwrap();
        assert_eq!(
            lease_target_cids(&state, &automatic.id).await,
            vec!["QmArchive"]
        );
        assert_eq!(lease_target_cids(&state, &manual.id).await, vec!["QmEntry"]);

        let entry = crate::store::object::get_latest(state.store.db(), "bucket", "prefix/file.txt")
            .await
            .unwrap();
        assert_eq!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.eq(&entry.id))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            object_tag::Entity::find()
                .filter(object_tag::Column::ObjectId.eq(entry.id))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn pinning_partial_extraction_targets_only_successfully_staged_entry() {
        let archive_body = zip(&[
            ZipEntryFixture {
                name: b"failed.txt",
                data: HELLO,
            },
            ZipEntryFixture {
                name: b"published.txt",
                data: HELLO,
            },
        ]);
        let (route, state, kubo) = route_with_mock_kubo_responses_and_coordinator(
            vec![
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n"),
                ResponseTemplate::new(500).set_body_string("forced entry add failure"),
                ResponseTemplate::new(200)
                    .set_body_string("{\"Hash\":\"QmPublished\",\"Size\":\"5\"}\n"),
            ],
            archive_body,
            pinning_coordinator("always", "one"),
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();

        let response = route
            .call(decompressed_tagging_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_owned()),
            ))
            .await
            .unwrap();
        let xml = response.output.collect().await.unwrap().to_bytes();
        let xml = std::str::from_utf8(&xml).unwrap();
        assert!(xml.contains("<EntryName>failed.txt</EntryName>"));
        assert!(xml.contains("<Code>EntryUploadFailed</Code>"));
        assert!(xml.contains("<Key>prefix/published.txt</Key>"));

        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/failed.txt",)
                .await
                .is_err()
        );
        let published =
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/published.txt")
                .await
                .unwrap();
        assert_eq!(published.cid, "QmPublished");
        let leases = archive_leases(&state).await;
        let automatic = leases
            .iter()
            .find(|lease| lease.source == "automatic")
            .unwrap();
        let manual = leases
            .iter()
            .find(|lease| lease.source == "manual")
            .unwrap();
        assert_eq!(
            lease_target_cids(&state, &automatic.id).await,
            vec!["QmArchive"]
        );
        assert_eq!(
            lease_target_cids(&state, &manual.id).await,
            vec!["QmPublished"]
        );
        assert_no_pin_removes(&kubo, &["QmArchive", "QmPublished"]).await;
    }

    #[tokio::test]
    async fn pinning_decompressed_provider_all_mode_targets_each_entry_on_each_provider() {
        let archive_body = zip(&[ZipEntryFixture {
            name: b"file.txt",
            data: HELLO,
        }]);
        let (route, state, _kubo) = pinning_route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n",
            ],
            archive_body,
            "always",
            "all",
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();

        route
            .call(decompressed_tagging_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_owned()),
            ))
            .await
            .unwrap();

        let leases = archive_leases(&state).await;
        let automatic = leases
            .iter()
            .find(|lease| lease.source == "automatic")
            .unwrap();
        let manual = leases
            .iter()
            .find(|lease| lease.source == "manual")
            .unwrap();
        let automatic_pairs = lease_targets(&state, &automatic.id)
            .await
            .into_iter()
            .map(|target| (target.provider, target.cid))
            .collect::<Vec<_>>();
        let manual_pairs = lease_targets(&state, &manual.id)
            .await
            .into_iter()
            .map(|target| (target.provider, target.cid))
            .collect::<Vec<_>>();
        assert_eq!(
            automatic_pairs,
            vec![
                ("alpha".to_owned(), "QmArchive".to_owned()),
                ("beta".to_owned(), "QmArchive".to_owned()),
            ]
        );
        assert_eq!(
            manual_pairs,
            vec![
                ("alpha".to_owned(), "QmEntry".to_owned()),
                ("beta".to_owned(), "QmEntry".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn pinning_archive_renewal_touches_all_targets_entry_delete_does_not_cancel_owner_and_later_put_is_independent()
     {
        use crate::store::entities::pin_lease;

        let (state, _) = published_pinning_zip("one").await;
        let leases = archive_leases(&state).await;
        let manual = leases
            .iter()
            .find(|lease| lease.source == "manual")
            .unwrap()
            .clone();
        let original_targets = lease_targets(&state, &manual.id).await;
        assert_eq!(
            original_targets
                .iter()
                .map(|target| target.cid.as_str())
                .collect::<Vec<_>>(),
            vec!["QmEntryA", "QmEntryB"]
        );

        let delete_time = chrono::Utc::now();
        assert!(
            crate::store::pinning::publication::delete_latest_with_leases(
                state.store.db(),
                "bucket",
                "prefix/a.txt",
                delete_time,
            )
            .await
            .unwrap()
        );
        assert_eq!(
            pin_lease::Entity::find_by_id(&manual.id)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "active"
        );

        let renew_time = delete_time + chrono::TimeDelta::minutes(1);
        let retain_until = renew_time + chrono::TimeDelta::hours(2);
        let retain_until_text = retain_until.to_rfc3339();
        crate::s3::ops::tagging::put_object_tagging_at(
            &state,
            put_tagging_request(
                "archive.zip",
                &[
                    ("team", "storage"),
                    ("ipfs-s3:pin", "true"),
                    ("ipfs-s3:retain-until", retain_until_text.as_str()),
                ],
            ),
            renew_time,
        )
        .await
        .unwrap();

        let renewed = pin_lease::Entity::find_by_id(&manual.id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(renewed.generation, manual.generation + 1);
        assert_eq!(renewed.expires_at, retain_until);
        let renewed_targets = lease_targets(&state, &manual.id).await;
        assert_eq!(
            renewed_targets
                .iter()
                .map(|target| target.id.as_str())
                .collect::<Vec<_>>(),
            original_targets
                .iter()
                .map(|target| target.id.as_str())
                .collect::<Vec<_>>()
        );
        assert!(
            renewed_targets
                .iter()
                .all(|target| target.last_touched_at == renew_time)
        );

        let entry_tags = vec![
            ObjectTag::new("ipfs-s3:pin", "true"),
            ObjectTag::new("ipfs-s3:duration", "1h"),
            ObjectTag::new("ipfs-s3:content", "object"),
        ];
        let entry_policy = state
            .pinning
            .policy()
            .evaluate_publication(PublicationContext {
                bucket: "bucket",
                key: "prefix/a.txt",
                tags: &entry_tags,
                is_decompress_zip: false,
            })
            .unwrap();
        let entry_object = PublicationObject::from_put(
            "independent-entry".to_owned(),
            "bucket",
            "prefix/a.txt",
            "QmIndependent".to_owned(),
            8,
            None,
            None,
            false,
            None,
            None,
            chrono::Utc::now(),
        );
        crate::store::pinning::publication::publish_object(
            state.store.db(),
            PublicationRequest {
                object: entry_object,
                tags: entry_tags,
                policy: entry_policy,
                object_target: PinTargetSpec {
                    cid: "QmIndependent".to_owned(),
                    logical_size: 8,
                },
            },
            state.pinning.provider_limits(),
        )
        .await
        .unwrap();

        let independent_leases = pin_lease::Entity::find()
            .filter(pin_lease::Column::OwnerObjectId.eq("independent-entry"))
            .all(state.store.db())
            .await
            .unwrap();
        assert_eq!(independent_leases.len(), 2);
        assert!(independent_leases.iter().all(|lease| {
            lease.id != manual.id && lease.owner_object_id == "independent-entry"
        }));
        assert_eq!(
            pin_lease::Entity::find_by_id(&manual.id)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "active"
        );
        assert_eq!(
            lease_target_cids(&state, &manual.id).await,
            vec!["QmEntryA", "QmEntryB"]
        );
    }

    #[tokio::test]
    async fn pinning_archive_cancel_delete_and_overwrite_end_all_owned_entry_targets() {
        use crate::store::entities::pin_lease;

        let (cancel_state, _) = published_pinning_zip("one").await;
        let cancel_manual = archive_leases(&cancel_state)
            .await
            .into_iter()
            .find(|lease| lease.source == "manual")
            .unwrap();
        crate::s3::ops::tagging::put_object_tagging_at(
            &cancel_state,
            put_tagging_request(
                "archive.zip",
                &[("team", "storage"), ("ipfs-s3:pin", "false")],
            ),
            chrono::Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(
            pin_lease::Entity::find_by_id(&cancel_manual.id)
                .one(cancel_state.store.db())
                .await
                .unwrap()
                .unwrap()
                .state,
            "cancelled"
        );
        assert!(
            lease_targets(&cancel_state, &cancel_manual.id)
                .await
                .iter()
                .all(|target| target.state == "released")
        );

        let (delete_state, _) = published_pinning_zip("one").await;
        let delete_archive =
            crate::store::object::get_latest(delete_state.store.db(), "bucket", "archive.zip")
                .await
                .unwrap();
        let delete_leases = archive_leases(&delete_state).await;
        assert!(
            crate::store::pinning::publication::delete_latest_with_leases(
                delete_state.store.db(),
                "bucket",
                "archive.zip",
                chrono::Utc::now(),
            )
            .await
            .unwrap()
        );
        assert!(
            crate::store::object::get_latest(delete_state.store.db(), "bucket", "archive.zip",)
                .await
                .is_err()
        );
        for lease in delete_leases {
            assert_eq!(
                pin_lease::Entity::find_by_id(&lease.id)
                    .one(delete_state.store.db())
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "cancelled"
            );
            assert!(
                lease_targets(&delete_state, &lease.id)
                    .await
                    .iter()
                    .all(|target| target.state == "released")
            );
        }
        assert!(
            !crate::store::entities::object::Entity::find_by_id(delete_archive.id)
                .one(delete_state.store.db())
                .await
                .unwrap()
                .unwrap()
                .is_latest
        );

        let (overwrite_state, _) = published_pinning_zip("one").await;
        let old_leases = archive_leases(&overwrite_state).await;
        let replacement_policy = overwrite_state
            .pinning
            .policy()
            .evaluate_publication(PublicationContext {
                bucket: "bucket",
                key: "archive.zip",
                tags: &[],
                is_decompress_zip: false,
            })
            .unwrap();
        let replacement = PublicationObject::from_put(
            "replacement-archive".to_owned(),
            "bucket",
            "archive.zip",
            "QmReplacement".to_owned(),
            9,
            Some("application/octet-stream".to_owned()),
            None,
            false,
            None,
            None,
            chrono::Utc::now(),
        );
        crate::store::pinning::publication::publish_object(
            overwrite_state.store.db(),
            PublicationRequest {
                object: replacement,
                tags: Vec::new(),
                policy: replacement_policy,
                object_target: PinTargetSpec {
                    cid: "QmReplacement".to_owned(),
                    logical_size: 9,
                },
            },
            overwrite_state.pinning.provider_limits(),
        )
        .await
        .unwrap();
        for lease in old_leases {
            assert_eq!(
                pin_lease::Entity::find_by_id(&lease.id)
                    .one(overwrite_state.store.db())
                    .await
                    .unwrap()
                    .unwrap()
                    .state,
                "cancelled"
            );
            assert!(
                lease_targets(&overwrite_state, &lease.id)
                    .await
                    .iter()
                    .all(|target| target.state == "released")
            );
        }
    }

    #[tokio::test]
    async fn pinning_confirmed_release_expired_renew_fails_without_reconstructing_targets() {
        use crate::store::entities::{pin_lease, pin_lease_target, remote_pin};

        let (state, kubo) = published_pinning_zip("one").await;
        let manual = archive_leases(&state)
            .await
            .into_iter()
            .find(|lease| lease.source == "manual")
            .unwrap();
        let original_targets = lease_targets(&state, &manual.id).await;
        let expire_time = manual.expires_at + chrono::TimeDelta::seconds(1);
        crate::store::pinning::leases::expire_due_leases(state.store.db(), expire_time)
            .await
            .unwrap();

        for target in &original_targets {
            let remote =
                remote_pin::Entity::find_by_id((target.provider.clone(), target.cid.clone()))
                    .one(state.store.db())
                    .await
                    .unwrap()
                    .unwrap();
            let outcome = crate::store::pinning::quota::confirmed_release(
                state.store.db(),
                &target.provider,
                &target.cid,
                remote.epoch,
                expire_time + chrono::TimeDelta::seconds(1),
            )
            .await
            .unwrap();
            assert!(matches!(
                outcome,
                crate::store::pinning::quota::ConfirmedReleaseOutcome::Released
                    | crate::store::pinning::quota::ConfirmedReleaseOutcome::AlreadyAbsent
            ));
        }

        let expired = pin_lease::Entity::find_by_id(&manual.id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expired.state, "expired");
        let preserved_targets = lease_targets(&state, &manual.id).await;
        assert_eq!(
            preserved_targets
                .iter()
                .map(|target| target.id.as_str())
                .collect::<Vec<_>>(),
            original_targets
                .iter()
                .map(|target| target.id.as_str())
                .collect::<Vec<_>>()
        );
        assert!(
            preserved_targets
                .iter()
                .all(|target| target.state == "released")
        );
        let target_count = pin_lease_target::Entity::find()
            .count(state.store.db())
            .await
            .unwrap();
        let kubo_request_count = kubo.received_requests().await.unwrap().len();

        let renew_time = expire_time + chrono::TimeDelta::minutes(1);
        let retain_until_text = (renew_time + chrono::TimeDelta::hours(1)).to_rfc3339();
        let error = crate::s3::ops::tagging::put_object_tagging_at(
            &state,
            put_tagging_request(
                "archive.zip",
                &[
                    ("team", "storage"),
                    ("ipfs-s3:pin", "true"),
                    ("ipfs-s3:retain-until", retain_until_text.as_str()),
                ],
            ),
            renew_time,
        )
        .await
        .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidArgument");
        let still_expired = pin_lease::Entity::find_by_id(&manual.id)
            .one(state.store.db())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(still_expired.state, "expired");
        assert_eq!(still_expired.generation, expired.generation);
        assert_eq!(
            lease_targets(&state, &manual.id)
                .await
                .into_iter()
                .map(|target| target.id)
                .collect::<Vec<_>>(),
            preserved_targets
                .into_iter()
                .map(|target| target.id)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            pin_lease_target::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            target_count
        );
        assert_eq!(
            kubo.received_requests().await.unwrap().len(),
            kubo_request_count,
            "tagging renewal must not reopen or re-extract the ZIP"
        );
    }

    #[tokio::test]
    async fn put_decompress_zip_result_false_returns_an_empty_body() {
        let archive_body = zip(&[ZipEntryFixture {
            name: b"file.txt",
            data: HELLO,
        }]);
        let (route, state, _kubo) = route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n",
            ],
            archive_body,
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();

        let response = route
            .call(signed_route_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/&decompress-zip-result=false",
                Body::from("archive bytes".to_string()),
            ))
            .await
            .unwrap();

        assert_eq!(
            response.headers.get(http::header::ETAG).unwrap(),
            "\"QmArchive\""
        );
        assert!(
            response
                .output
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn put_decompress_zip_checks_access_before_any_kubo_mutation() {
        let (route, state, kubo) = route_with_mock_kubo(Vec::new(), Vec::new()).await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        let mut request = signed_route_request(
            Method::PUT,
            "/bucket/archive.zip?decompress-zip=prefix/",
            Body::from("archive bytes".to_string()),
        );
        request.credentials = None;

        assert!(route.call(request).await.is_err());
        assert!(
            kubo.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/add")
        );
    }

    #[tokio::test]
    async fn put_decompress_zip_rejects_sse_headers() {
        let (route, state, kubo) = route_with_mock_kubo(Vec::new(), Vec::new()).await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        let mut request = signed_route_request(
            Method::PUT,
            "/bucket/archive.zip?decompress-zip=prefix/",
            Body::from("archive bytes".to_string()),
        );
        request.headers.insert(
            "x-amz-server-side-encryption",
            http::HeaderValue::from_static("AES256"),
        );

        let error = route.call(request).await.unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidArgument");
        assert!(
            kubo.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/add")
        );
    }

    #[tokio::test]
    async fn pinning_global_rejection_creates_no_publication_state_and_keeps_local_pins() {
        let archive_body = zip(&[
            ZipEntryFixture {
                name: b"safe.txt",
                data: HELLO,
            },
            ZipEntryFixture {
                name: b"../escape.txt",
                data: HELLO,
            },
        ]);
        let (route, state, kubo) = pinning_route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmSharedEntry\",\"Size\":\"5\"}\n",
            ],
            archive_body,
            "always",
            "all",
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();

        let error = route
            .call(decompressed_tagging_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_string()),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_no_zip_publication_rows(&state).await;
        assert_no_pin_removes(&kubo, &["QmArchive", "QmSharedEntry"]).await;
    }

    #[tokio::test]
    async fn put_budget_rejection_is_a_global_400_without_publication() {
        let archive_body = zip(&[
            ZipEntryFixture {
                name: b"first.txt",
                data: HELLO,
            },
            ZipEntryFixture {
                name: b"second.txt",
                data: HELLO,
            },
        ]);
        let (route, state, kubo) = route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmFirst\",\"Size\":\"5\"}\n",
            ],
            archive_body,
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();

        // This private helper is the production PUT implementation with only
        // the test-injected limit varied; it exposes the S3Error before HTTP
        // serialization, so status/code are asserted here.
        let error = route
            .call_put_with_decompressed_limit(
                signed_route_request(
                    Method::PUT,
                    "/bucket/archive.zip?decompress-zip=prefix/",
                    Body::from("archive bytes".to_owned()),
                ),
                7,
            )
            .await
            .expect_err("two five-byte entries must exceed the seven-byte global budget");

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_eq!(error.status_code(), Some(http::StatusCode::BAD_REQUEST));
        assert_no_zip_publication_rows(&state).await;
        let requests = kubo.received_requests().await.unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/add")
                .count(),
            2
        );
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/api/v0/pin/add")
                .count(),
            2
        );
        assert!(
            requests
                .iter()
                .all(|request| request.url.path() != "/api/v0/pin/rm")
        );
    }

    #[tokio::test]
    async fn pinning_archive_key_collision_creates_no_publication_state_and_keeps_local_pins() {
        let archive_body = zip(&[ZipEntryFixture {
            name: b"archive.zip",
            data: HELLO,
        }]);
        let (route, state, kubo) = pinning_route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmCollision\",\"Size\":\"5\"}\n",
            ],
            archive_body,
            "always",
            "all",
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();

        let error = route
            .call(decompressed_tagging_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=",
                Body::from("archive bytes".to_owned()),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_no_zip_publication_rows(&state).await;
        assert_no_pin_removes(&kubo, &["QmArchive", "QmCollision"]).await;
    }

    #[tokio::test]
    async fn pinning_invalid_tags_are_rejected_before_archive_kubo_add() {
        let (route, state, kubo) =
            pinning_route_with_mock_kubo(Vec::new(), Vec::new(), "always", "one").await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        let mut request = signed_route_request(
            Method::PUT,
            "/bucket/archive.zip?decompress-zip=prefix/",
            Body::from("archive bytes".to_owned()),
        );
        request.headers.insert(
            "x-amz-tagging",
            http::HeaderValue::from_static("ipfs-s3%3Aunknown=value"),
        );

        let error = route.call(request).await.unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidArgument");
        assert!(
            kubo.received_requests()
                .await
                .unwrap()
                .iter()
                .all(|request| request.url.path() != "/api/v0/add")
        );
        assert_no_zip_publication_rows(&state).await;
    }

    #[tokio::test]
    async fn pinning_put_publication_failure_rolls_back_all_db_state_and_keeps_pins() {
        let archive_body = zip(&[
            ZipEntryFixture {
                name: b"first.txt",
                data: HELLO,
            },
            ZipEntryFixture {
                name: b"second.txt",
                data: HELLO,
            },
        ]);
        let (route, state, kubo) = pinning_route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmFailedEntry\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmPublishedEntry\",\"Size\":\"5\"}\n",
            ],
            archive_body,
            "always",
            "one",
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();
        state
            .store
            .db()
            .execute_unprepared(
                "CREATE TRIGGER reject_first_entry BEFORE INSERT ON objects \
                 WHEN NEW.key = 'prefix/first.txt' \
                 BEGIN SELECT RAISE(FAIL, 'forced entry publish failure'); END",
            )
            .await
            .unwrap();

        let error = route
            .call(decompressed_tagging_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_string()),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InternalError");
        assert_no_zip_publication_rows(&state).await;
        assert_no_pin_removes(&kubo, &["QmArchive", "QmFailedEntry", "QmPublishedEntry"]).await;
    }

    #[tokio::test]
    async fn complete_route_returns_standard_xml_for_non_decompress_sse_upload() {
        let (route, _state, _kubo) = route_with_seeded_sse_multipart().await;

        let response = route
            .call(signed_route_request(
                Method::POST,
                "/bucket/archive.zip?uploadId=upload-1",
                Body::from(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>"
                        .to_string(),
                ),
            ))
            .await
            .unwrap();

        assert_eq!(
            response.headers.get(http::header::ETAG).unwrap(),
            "\"QmRoot\""
        );
        assert_eq!(
            response
                .headers
                .get("x-amz-server-side-encryption")
                .unwrap(),
            "AES256"
        );
        let body = response.output.collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("<CompleteMultipartUploadResult>"));
        assert!(body.contains("<ETag>\"QmRoot\"</ETag>"));
    }

    #[tokio::test]
    async fn complete_route_extracts_when_upload_has_decompress_target() {
        let archive_body = zip(&[ZipEntryFixture {
            name: b"foo/bar.txt",
            data: HELLO,
        }]);
        let (route, state, _kubo) = route_with_seeded_plain_multipart(
            Some("prefix/"),
            true,
            archive_body,
            vec![
                "{\"Hash\":\"QmRoot\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n",
            ],
        )
        .await;

        let response = route
            .call(signed_route_request(
                Method::POST,
                "/bucket/archive.zip?uploadId=upload-1",
                Body::from(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>"
                        .to_string(),
                ),
            ))
            .await
            .unwrap();

        assert_eq!(
            response.headers.get(http::header::ETAG).unwrap(),
            "\"QmRoot\""
        );
        let body = response.output.collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("<DecompressZipResult>"));
        assert!(body.contains("<Key>prefix/foo/bar.txt</Key>"));
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip")
                .await
                .is_ok()
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/foo/bar.txt")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn pinning_multipart_zip_success_commits_publication_and_deletes_upload_atomically() {
        use crate::store::entities::{
            object, object_tag, pin_job, pin_lease, pin_provider_usage, remote_pin,
        };

        let archive_body = zip(&[
            ZipEntryFixture {
                name: b"a.txt",
                data: HELLO,
            },
            ZipEntryFixture {
                name: b"b.txt",
                data: HELLO,
            },
        ]);
        let archive_size = archive_body.len() as i64;
        let (route, state, _kubo) = pinning_route_with_seeded_plain_multipart(
            "all",
            archive_body,
            vec![
                "{\"Hash\":\"QmRoot\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmEntryA\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmEntryB\",\"Size\":\"5\"}\n",
            ],
        )
        .await;

        let response = route
            .call(signed_route_request(
                Method::POST,
                "/bucket/archive.zip?uploadId=upload-1",
                Body::from(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>"
                        .to_owned(),
                ),
            ))
            .await
            .unwrap();
        let body = response.output.collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("<Key>prefix/a.txt</Key>"));
        assert!(body.contains("<Key>prefix/b.txt</Key>"));

        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_err()
        );
        assert!(
            crate::store::multipart::list_parts(state.store.db(), "upload-1")
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            object::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            3
        );
        let archive = crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip")
            .await
            .unwrap();
        assert_eq!(archive.cid, "QmRoot");
        assert!(archive.multipart);
        assert_eq!(
            object_tag::Entity::find()
                .filter(object_tag::Column::ObjectId.eq(&archive.id))
                .count(state.store.db())
                .await
                .unwrap(),
            4
        );
        let entry_ids = object::Entity::find()
            .filter(object::Column::Key.is_in(["prefix/a.txt", "prefix/b.txt"]))
            .all(state.store.db())
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>();
        assert_eq!(entry_ids.len(), 2);
        assert_eq!(
            object_tag::Entity::find()
                .filter(object_tag::Column::ObjectId.is_in(entry_ids.clone()))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            pin_lease::Entity::find()
                .filter(pin_lease::Column::OwnerObjectId.is_in(entry_ids))
                .count(state.store.db())
                .await
                .unwrap(),
            0
        );

        let leases = archive_leases(&state).await;
        assert_eq!(leases.len(), 2);
        let automatic = leases
            .iter()
            .find(|lease| lease.source == "automatic")
            .unwrap();
        let manual = leases
            .iter()
            .find(|lease| lease.source == "manual")
            .unwrap();
        assert_eq!(
            lease_target_cids(&state, &automatic.id).await,
            vec!["QmRoot", "QmRoot"]
        );
        assert_eq!(
            lease_target_cids(&state, &manual.id).await,
            vec!["QmEntryA", "QmEntryA", "QmEntryB", "QmEntryB"]
        );
        assert_eq!(
            remote_pin::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            6
        );
        assert_eq!(
            pin_job::Entity::find()
                .count(state.store.db())
                .await
                .unwrap(),
            6
        );
        for provider in ["alpha", "beta"] {
            let usage = pin_provider_usage::Entity::find_by_id(provider)
                .one(state.store.db())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(usage.reserved_pins, 3);
            assert_eq!(usage.reserved_bytes, archive_size + 10);
        }
    }

    #[tokio::test]
    async fn pinning_multipart_zip_outbox_failure_rolls_back_and_keeps_upload_parts_and_local_pins()
    {
        let archive_body = zip(&[
            ZipEntryFixture {
                name: b"a.txt",
                data: HELLO,
            },
            ZipEntryFixture {
                name: b"b.txt",
                data: HELLO,
            },
        ]);
        let (route, state, kubo) = pinning_route_with_seeded_plain_multipart(
            "all",
            archive_body,
            vec![
                "{\"Hash\":\"QmRoot\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmEntryA\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmEntryB\",\"Size\":\"5\"}\n",
            ],
        )
        .await;
        state
            .store
            .db()
            .execute_unprepared(
                "CREATE TRIGGER fail_zip_outbox BEFORE INSERT ON pin_jobs \
                 BEGIN SELECT RAISE(FAIL, 'forced zip outbox failure'); END",
            )
            .await
            .unwrap();

        let error = route
            .call(signed_route_request(
                Method::POST,
                "/bucket/archive.zip?uploadId=upload-1",
                Body::from(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>"
                        .to_owned(),
                ),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InternalError");
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_ok()
        );
        assert_eq!(
            crate::store::multipart::list_parts(state.store.db(), "upload-1")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_no_zip_publication_rows(&state).await;
        assert_no_pin_removes(&kubo, &["QmPart", "QmRoot", "QmEntryA", "QmEntryB"]).await;
    }

    #[tokio::test]
    async fn complete_route_global_traversal_reject_preserves_retry_state_and_pins() {
        let archive_body = zip(&[
            ZipEntryFixture {
                name: b"safe.txt",
                data: HELLO,
            },
            ZipEntryFixture {
                name: b"../escape.txt",
                data: HELLO,
            },
        ]);
        let (route, state, kubo) = route_with_seeded_plain_multipart(
            Some("prefix/"),
            true,
            archive_body,
            vec![
                "{\"Hash\":\"QmRoot\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmSharedEntry\",\"Size\":\"5\"}\n",
            ],
        )
        .await;

        let error = route
            .call(signed_route_request(
                Method::POST,
                "/bucket/archive.zip?uploadId=upload-1",
                Body::from(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>"
                        .to_string(),
                ),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip")
                .await
                .is_err()
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/safe.txt")
                .await
                .is_err()
        );
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_ok()
        );
        assert_eq!(
            crate::store::multipart::get_part(state.store.db(), "upload-1", 1)
                .await
                .unwrap()
                .etag,
            "QmPart"
        );
        assert_no_pin_removes(&kubo, &["QmRoot", "QmSharedEntry", "QmPart"]).await;
    }

    #[tokio::test]
    async fn complete_route_archive_key_collision_preserves_retry_state_and_pins() {
        let archive_body = zip(&[ZipEntryFixture {
            name: b"archive.zip",
            data: HELLO,
        }]);
        let (route, state, kubo) = route_with_seeded_plain_multipart(
            Some(""),
            true,
            archive_body,
            vec![
                "{\"Hash\":\"QmRoot\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmCollisionEntry\",\"Size\":\"5\"}\n",
            ],
        )
        .await;

        let error = route
            .call(signed_route_request(
                Method::POST,
                "/bucket/archive.zip?uploadId=upload-1",
                Body::from(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>"
                        .to_string(),
                ),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InvalidParameterValue");
        assert_eq!(
            error.message(),
            Some("zip entry collides with archive key: archive.zip")
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip")
                .await
                .is_err()
        );
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_ok()
        );
        assert_eq!(
            crate::store::multipart::get_part(state.store.db(), "upload-1", 1)
                .await
                .unwrap()
                .etag,
            "QmPart"
        );
        assert_no_pin_removes(&kubo, &["QmRoot", "QmPart", "QmCollisionEntry"]).await;
    }

    #[tokio::test]
    async fn pinning_multipart_zip_publication_failure_rolls_back_and_preserves_upload_parts() {
        let archive_body = zip(&[
            ZipEntryFixture {
                name: b"first.txt",
                data: HELLO,
            },
            ZipEntryFixture {
                name: b"second.txt",
                data: HELLO,
            },
        ]);
        let (route, state, kubo) = route_with_seeded_plain_multipart(
            Some("prefix/"),
            true,
            archive_body,
            vec![
                "{\"Hash\":\"QmRoot\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmFailedEntry\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmPublishedEntry\",\"Size\":\"5\"}\n",
            ],
        )
        .await;
        state
            .store
            .db()
            .execute_unprepared(
                "CREATE TRIGGER reject_first_complete_entry BEFORE INSERT ON objects \
                 WHEN NEW.key = 'prefix/first.txt' \
                 BEGIN SELECT RAISE(FAIL, 'forced entry publish failure'); END",
            )
            .await
            .unwrap();

        let error = route
            .call(signed_route_request(
                Method::POST,
                "/bucket/archive.zip?uploadId=upload-1",
                Body::from(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>"
                        .to_string(),
                ),
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code().as_str(), "InternalError");
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip")
                .await
                .is_err()
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/second.txt")
                .await
                .is_err()
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/first.txt")
                .await
                .is_err()
        );
        assert!(
            crate::store::multipart::get_upload(state.store.db(), "upload-1")
                .await
                .is_ok()
        );
        assert_eq!(
            crate::store::multipart::list_parts(state.store.db(), "upload-1")
                .await
                .unwrap()
                .len(),
            1
        );
        assert_no_pin_removes(
            &kubo,
            &["QmPart", "QmRoot", "QmFailedEntry", "QmPublishedEntry"],
        )
        .await;
    }

    #[tokio::test]
    async fn complete_route_decompress_result_false_returns_standard_complete_xml() {
        let archive_body = zip(&[ZipEntryFixture {
            name: b"file.txt",
            data: HELLO,
        }]);
        let (route, _state, _kubo) = route_with_seeded_plain_multipart(
            Some("prefix/"),
            false,
            archive_body,
            vec![
                "{\"Hash\":\"QmRoot\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmEntry\",\"Size\":\"5\"}\n",
            ],
        )
        .await;

        let response = route
            .call(signed_route_request(
                Method::POST,
                "/bucket/archive.zip?uploadId=upload-1",
                Body::from(
                    "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"QmPart\"</ETag></Part></CompleteMultipartUpload>"
                        .to_string(),
                ),
            ))
            .await
            .unwrap();

        let body = response.output.collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("<CompleteMultipartUploadResult>"));
        assert!(body.contains("<ETag>\"QmRoot\"</ETag>"));
        assert!(!body.contains("<DecompressZipResult>"));
    }
}
