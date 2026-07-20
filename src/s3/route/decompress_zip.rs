use std::sync::Arc;

use bytes::Buf as _;
use http::{HeaderMap, Method, Uri};
use http_body_util::BodyExt as _;
use s3s::dto::{ETag, ParseETagError};
use s3s::route::S3Route;
use s3s::{Body, S3Request, S3Response, S3Result};

use crate::state::AppState;

pub struct DecompressZipRoute {
    state: Arc<AppState>,
}

const MAX_COMPLETE_MULTIPART_XML_BYTES: usize = 4 * 1024 * 1024;

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

    async fn call_put(&self, req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let parsed = parse_decompress_put_uri(&req.uri)?;
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

        let content_type = req
            .headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let metadata = crate::s3::ops::object::extract_custom_metadata(&req.headers);

        let archive =
            crate::s3::ops::object::add_plain_object_stream(&self.state, req.input).await?;
        let archive_stream = crate::kubo::cat::stream_cat(&self.state.kubo, &archive.cid, None)
            .await
            .map_err(|err| s3s::s3_error!(InternalError, "cat archive: {err}"))?;
        let outcome = crate::zip::extract::extract_zip_stream(
            &self.state,
            &parsed.target_prefix,
            archive_stream,
        )
        .await?;

        reject_archive_key_collision(&parsed.key, &outcome.entries)?;

        crate::s3::ops::object::publish_plain_object(
            &self.state,
            &parsed.bucket,
            &parsed.key,
            content_type.as_deref(),
            metadata,
            &archive,
            false,
        )
        .await?;

        let mut published = Vec::new();
        let mut failures = outcome.failures;
        for entry in outcome.entries {
            let stored = crate::s3::ops::object::StoredObject {
                cid: entry.cid.clone(),
                size: entry.size,
            };
            match crate::s3::ops::object::publish_plain_object(
                &self.state,
                &parsed.bucket,
                &entry.key,
                None,
                None,
                &stored,
                false,
            )
            .await
            {
                Ok(()) => published.push(entry),
                Err(error) => failures.push(crate::zip::response::ExtractFailure {
                    entry_name: entry.key,
                    code: "EntryPublishFailed".to_string(),
                    message: error.to_string(),
                }),
            }
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::ETAG,
            http::HeaderValue::from_str(&format!("\"{}\"", archive.cid)).unwrap(),
        );
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
    }

    async fn call_complete(&self, mut req: S3Request<Body>) -> S3Result<S3Response<Body>> {
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
                    .await
                    .map_err(|error| {
                        s3s::s3_error!(InternalError, "cat completed archive: {error}")
                    })?;
            let outcome = crate::zip::extract::extract_zip_stream(
                &self.state,
                &target_prefix,
                archive_stream,
            )
            .await?;

            reject_archive_key_collision(&completed.key, &outcome.entries)?;
            crate::s3::ops::multipart::finalize_completed_multipart_archive(
                &self.state,
                &completed,
            )
            .await?;

            let mut published = Vec::new();
            let mut failures = outcome.failures;
            for entry in outcome.entries {
                let stored = crate::s3::ops::object::StoredObject {
                    cid: entry.cid.clone(),
                    size: entry.size,
                };
                match crate::s3::ops::object::publish_plain_object(
                    &self.state,
                    &completed.bucket,
                    &entry.key,
                    None,
                    None,
                    &stored,
                    false,
                )
                .await
                {
                    Ok(()) => published.push(entry),
                    Err(error) => failures.push(crate::zip::response::ExtractFailure {
                        entry_name: entry.key,
                        code: "EntryPublishFailed".to_string(),
                        message: error.to_string(),
                    }),
                }
            }

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

        crate::s3::ops::multipart::finalize_completed_multipart_archive(&self.state, &completed)
            .await?;
        let xml = crate::zip::response::complete_multipart_result_xml(
            &completed.bucket,
            &completed.key,
            &completed.root_cid,
        );
        Ok(S3Response::with_headers(Body::from(xml), headers))
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use http_body_util::BodyExt;
    use sea_orm::ConnectionTrait;
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

    async fn route_with_mock_kubo(
        add_bodies: Vec<&'static str>,
        archive_body: Vec<u8>,
    ) -> (DecompressZipRoute, Arc<AppState>, MockServer) {
        let kubo = MockServer::start().await;
        let response_index = Arc::new(AtomicUsize::new(0));
        let add_bodies = Arc::new(add_bodies);
        if !add_bodies.is_empty() {
            Mock::given(method("POST"))
                .and(path("/api/v0/add"))
                .respond_with({
                    let response_index = response_index.clone();
                    let add_bodies = add_bodies.clone();
                    move |_: &wiremock::Request| {
                        let index = response_index.fetch_add(1, Ordering::SeqCst);
                        ResponseTemplate::new(200).set_body_string(add_bodies[index])
                    }
                })
                .up_to_n_times(add_bodies.len() as u64)
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
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key: crate::crypto::key::MasterKey::from_hex(
                "0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        });
        (DecompressZipRoute::new(state.clone()), state, kubo)
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
            store: crate::store::Store::new(db),
            credentials: HashMap::new(),
            master_key,
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
        let (route, state, _) = route_with_mock_kubo(
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
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_string()),
            ))
            .await
            .unwrap();

        assert_eq!(
            response.headers.get(http::header::ETAG).unwrap(),
            "\"QmArchive\""
        );
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
    async fn put_decompress_zip_result_false_returns_an_empty_body() {
        let archive_body = zip(&[ZipEntryFixture {
            name: b"file.txt",
            data: HELLO,
        }]);
        let (route, state, _) = route_with_mock_kubo(
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
    async fn put_decompress_zip_global_reject_keeps_archive_and_entry_pins() {
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
        let (route, state, kubo) = route_with_mock_kubo(
            vec![
                "{\"Hash\":\"QmArchive\",\"Size\":\"13\"}\n",
                "{\"Hash\":\"QmSharedEntry\",\"Size\":\"5\"}\n",
            ],
            archive_body,
        )
        .await;
        crate::store::bucket::create(state.store.db(), "bucket", None)
            .await
            .unwrap();

        let error = route
            .call(signed_route_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_string()),
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
        assert_no_pin_removes(&kubo, &["QmArchive", "QmSharedEntry"]).await;
    }

    #[tokio::test]
    async fn put_decompress_zip_entry_publish_failure_reports_failure_and_keeps_pins() {
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
                "{\"Hash\":\"QmFailedEntry\",\"Size\":\"5\"}\n",
                "{\"Hash\":\"QmPublishedEntry\",\"Size\":\"5\"}\n",
            ],
            archive_body,
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

        let response = route
            .call(signed_route_request(
                Method::PUT,
                "/bucket/archive.zip?decompress-zip=prefix/",
                Body::from("archive bytes".to_string()),
            ))
            .await
            .unwrap();

        let xml = response.output.collect().await.unwrap().to_bytes();
        let xml = std::str::from_utf8(&xml).unwrap();
        assert!(xml.contains("<Code>EntryPublishFailed</Code>"));
        assert!(xml.contains("<EntryName>prefix/first.txt</EntryName>"));
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip")
                .await
                .is_ok()
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/second.txt")
                .await
                .is_ok()
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/first.txt")
                .await
                .is_err()
        );
        assert_no_pin_removes(&kubo, &["QmArchive", "QmFailedEntry", "QmPublishedEntry"]).await;
    }

    #[tokio::test]
    async fn complete_route_returns_standard_xml_for_non_decompress_sse_upload() {
        let (route, _state, _) = route_with_seeded_sse_multipart().await;

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
        let (route, state, _) = route_with_seeded_plain_multipart(
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
    async fn complete_route_entry_publish_failure_is_partial_and_keeps_pins() {
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
        assert!(body.contains("<DecompressZipResult>"));
        assert!(body.contains("<Code>EntryPublishFailed</Code>"));
        assert!(body.contains("<EntryName>prefix/first.txt</EntryName>"));
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "archive.zip")
                .await
                .is_ok()
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/second.txt")
                .await
                .is_ok()
        );
        assert!(
            crate::store::object::get_latest(state.store.db(), "bucket", "prefix/first.txt")
                .await
                .is_err()
        );
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
