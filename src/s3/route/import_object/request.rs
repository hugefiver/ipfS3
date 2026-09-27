use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Buf as _;
use http::{HeaderMap, Uri};
use http_body_util::BodyExt as _;
use s3s::{Body, S3Result};
use sha2::{Digest as _, Sha256};

use crate::{error::AppError, import::ImportSource, pinning::tags::ObjectTag};

pub(super) const MAX_IMPORT_XML_BYTES: usize = 64 * 1024;
pub(super) const DEFAULT_MAX_RESULTS: u64 = 100;
pub(super) const MAX_RESULTS: u64 = 1_000;

#[derive(Debug)]
pub(super) struct ParsedSubmitQuery {
    pub bucket: String,
    pub key: String,
    pub decompress_prefix: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct ParsedStatusQuery {
    pub bucket: String,
    pub key: String,
    pub job_id: String,
    pub max_results: u64,
    pub continuation_sequence: Option<i64>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum ParsedImportSource {
    Cid(String),
    Url(url::Url),
}

fn invalid_import() -> s3s::S3Error {
    AppError::InvalidImportRequest.into()
}

fn parse_path_bucket_key(uri: &Uri) -> S3Result<(String, String)> {
    let path = uri.path().trim_start_matches('/');
    let (bucket, key) = path.split_once('/').ok_or_else(invalid_import)?;
    let bucket = percent_encoding::percent_decode_str(bucket)
        .decode_utf8()
        .map_err(|_| invalid_import())?
        .into_owned();
    let key = percent_encoding::percent_decode_str(key)
        .decode_utf8()
        .map_err(|_| invalid_import())?
        .into_owned();
    if bucket.is_empty() || key.is_empty() {
        return Err(invalid_import());
    }
    Ok((bucket, key))
}

pub(super) fn parse_submit_query(uri: &Uri) -> S3Result<ParsedSubmitQuery> {
    let (bucket, key) = parse_path_bucket_key(uri)?;
    let mut import_value = None;
    let mut decompress_value = None;
    for (name, value) in crate::s3::query::decoded_query_pairs(uri)? {
        match name.as_str() {
            "ipfs3-import" if import_value.is_none() => import_value = Some(value),
            "decompress-zip" if decompress_value.is_none() => decompress_value = Some(value),
            _ => return Err(invalid_import()),
        }
    }
    if import_value.as_deref() != Some("") {
        return Err(invalid_import());
    }
    let decompress_prefix = decompress_value
        .as_deref()
        .map(crate::zip::sanitize::normalize_target_prefix)
        .transpose()
        .map_err(|_| invalid_import())?;
    Ok(ParsedSubmitQuery {
        bucket,
        key,
        decompress_prefix,
    })
}

pub(super) fn parse_status_query(uri: &Uri) -> S3Result<ParsedStatusQuery> {
    let (bucket, key) = parse_path_bucket_key(uri)?;
    let mut job_id = None;
    let mut max_results = None;
    let mut continuation_token = None;
    for (name, value) in crate::s3::query::decoded_query_pairs(uri)? {
        match name.as_str() {
            "ipfs3-import" if job_id.is_none() => job_id = Some(value),
            "max-results" if max_results.is_none() => max_results = Some(value),
            "continuation-token" if continuation_token.is_none() => {
                continuation_token = Some(value)
            }
            _ => return Err(invalid_import()),
        }
    }
    let job_id = job_id
        .filter(|value| !value.is_empty())
        .ok_or_else(invalid_import)?;
    let parsed_job_id = uuid::Uuid::parse_str(&job_id).map_err(|_| invalid_import())?;
    if parsed_job_id.to_string() != job_id.to_ascii_lowercase() {
        return Err(invalid_import());
    }
    let max_results = max_results
        .map(|value| value.parse::<u64>().map_err(|_| invalid_import()))
        .transpose()?
        .unwrap_or(DEFAULT_MAX_RESULTS)
        .clamp(1, MAX_RESULTS);
    let continuation_sequence = continuation_token
        .map(|token| decode_continuation_token(&job_id, &token))
        .transpose()?;
    Ok(ParsedStatusQuery {
        bucket,
        key,
        job_id,
        max_results,
        continuation_sequence,
    })
}

pub(super) fn encode_continuation_token(job_id: &str, sequence: i64) -> String {
    URL_SAFE_NO_PAD.encode(format!("{job_id}:{sequence}"))
}

pub(super) fn decode_continuation_token(job_id: &str, token: &str) -> S3Result<i64> {
    let decoded = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| invalid_import())?;
    let decoded = std::str::from_utf8(&decoded).map_err(|_| invalid_import())?;
    let (token_job_id, sequence) = decoded.rsplit_once(':').ok_or_else(invalid_import)?;
    if token_job_id != job_id {
        return Err(invalid_import());
    }
    let sequence = sequence.parse::<i64>().map_err(|_| invalid_import())?;
    if sequence < 0 {
        return Err(invalid_import());
    }
    Ok(sequence)
}

pub(super) fn reject_sse_headers(headers: &HeaderMap) -> S3Result<()> {
    if headers
        .keys()
        .any(|name| name.as_str().starts_with("x-amz-server-side-encryption"))
    {
        return Err(invalid_import());
    }
    Ok(())
}

pub(super) fn validate_submission_content_type(headers: &HeaderMap) -> S3Result<()> {
    let value = required_single_header(headers, http::header::CONTENT_TYPE.as_str())?;
    let media_type = value
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if media_type == "application/xml" || media_type == "text/xml" || media_type.ends_with("+xml") {
        Ok(())
    } else {
        Err(invalid_import())
    }
}

pub(super) fn parse_client_token(headers: &HeaderMap) -> S3Result<Option<String>> {
    let token = optional_single_header(headers, "x-ipfs3-client-token")?;
    if token.as_ref().is_some_and(|token| {
        token.len() > 128
            || token.is_empty()
            || !token.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    }) {
        return Err(invalid_import());
    }
    Ok(token)
}

fn required_single_header(headers: &HeaderMap, name: &str) -> S3Result<String> {
    optional_single_header(headers, name)?.ok_or_else(invalid_import)
}

pub(super) fn optional_single_header(headers: &HeaderMap, name: &str) -> S3Result<Option<String>> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(invalid_import());
    }
    value
        .to_str()
        .map(str::to_owned)
        .map(Some)
        .map_err(|_| invalid_import())
}

pub(super) fn parse_tags(headers: &HeaderMap) -> S3Result<Vec<ObjectTag>> {
    let Some(value) = optional_single_header(headers, "x-amz-tagging")? else {
        return Ok(Vec::new());
    };
    crate::pinning::tags::parse_tagging_header(&value).map_err(|_| invalid_import())
}

pub(super) fn parse_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    let Some(serde_json::Value::Object(metadata)) =
        crate::s3::ops::object::extract_custom_metadata(headers)
    else {
        return HashMap::new();
    };
    metadata
        .into_iter()
        .filter_map(|(name, value)| value.as_str().map(|value| (name, value.to_owned())))
        .collect()
}

pub(super) fn request_fingerprint(
    source: &ImportSource,
    principal: &str,
    object_content_type: Option<&str>,
    metadata: &HashMap<String, String>,
    tags: &[ObjectTag],
    decompress_prefix: Option<&str>,
) -> S3Result<String> {
    fingerprint(
        source,
        Some(principal),
        object_content_type,
        metadata,
        tags,
        decompress_prefix,
    )
}

pub(super) fn legacy_request_fingerprint(
    source: &ImportSource,
    object_content_type: Option<&str>,
    metadata: &HashMap<String, String>,
    tags: &[ObjectTag],
    decompress_prefix: Option<&str>,
) -> S3Result<String> {
    fingerprint(
        source,
        None,
        object_content_type,
        metadata,
        tags,
        decompress_prefix,
    )
}

fn fingerprint(
    source: &ImportSource,
    principal: Option<&str>,
    object_content_type: Option<&str>,
    metadata: &HashMap<String, String>,
    tags: &[ObjectTag],
    decompress_prefix: Option<&str>,
) -> S3Result<String> {
    let (source_type, source_value) = match source {
        ImportSource::Cid(cid) => ("cid", cid.as_str()),
        ImportSource::Url(url) => ("url", url.as_str()),
    };
    let metadata = metadata
        .iter()
        .collect::<std::collections::BTreeMap<_, _>>();
    let tags = tags
        .iter()
        .map(|tag| (&tag.key, &tag.value))
        .collect::<std::collections::BTreeMap<_, _>>();
    let canonical = if let Some(principal) = principal {
        serde_json::to_vec(&(
            "import-pin-decision-v1",
            principal,
            source_type,
            source_value,
            object_content_type,
            &metadata,
            &tags,
            decompress_prefix,
        ))
    } else {
        serde_json::to_vec(&(
            source_type,
            source_value,
            object_content_type,
            &metadata,
            &tags,
            decompress_prefix,
        ))
    }
    .map_err(|_| invalid_import())?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(canonical))))
}

pub(super) async fn collect_import_xml(body: &mut Body) -> S3Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| invalid_import())?;
        let Ok(mut data) = frame.into_data() else {
            continue;
        };
        let data_len = data.remaining();
        let next_len = bytes
            .len()
            .checked_add(data_len)
            .ok_or_else(invalid_import)?;
        if next_len > MAX_IMPORT_XML_BYTES {
            return Err(invalid_import());
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ImportXmlField {
    Cid,
    Url,
}

pub(super) fn parse_import_xml(bytes: &[u8]) -> S3Result<ParsedImportSource> {
    let mut reader = quick_xml::Reader::from_reader(bytes);
    reader.config_mut().trim_text(false);
    let mut buffer = Vec::new();
    let mut saw_declaration = false;
    let mut saw_root = false;
    let mut closed_root = false;
    let mut current_field = None;
    let mut field_value = String::new();
    let mut cid = None;
    let mut url = None;

    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(quick_xml::events::Event::Decl(_))
                if !saw_declaration && !saw_root && !closed_root =>
            {
                saw_declaration = true;
            }
            Ok(quick_xml::events::Event::Start(event)) if !saw_root && !closed_root => {
                if event.name().as_ref() != b"IPFS3ImportRequest" {
                    return Err(invalid_import());
                }
                validate_root_attributes(&event)?;
                saw_root = true;
            }
            Ok(quick_xml::events::Event::Start(event))
                if saw_root && !closed_root && current_field.is_none() =>
            {
                reject_attributes(&event)?;
                current_field = match event.name().as_ref() {
                    b"CID" if cid.is_none() => Some(ImportXmlField::Cid),
                    b"URL" if url.is_none() => Some(ImportXmlField::Url),
                    _ => return Err(invalid_import()),
                };
                field_value.clear();
            }
            Ok(quick_xml::events::Event::Text(text)) => {
                let decoded = text.decode().map_err(|_| invalid_import())?;
                if current_field.is_some() {
                    field_value.push_str(decoded.as_ref());
                } else if !decoded.trim().is_empty() {
                    return Err(invalid_import());
                }
            }
            Ok(quick_xml::events::Event::End(event)) => match current_field {
                Some(ImportXmlField::Cid) if event.name().as_ref() == b"CID" => {
                    cid = Some(parse_canonical_cid(std::mem::take(&mut field_value))?);
                    current_field = None;
                }
                Some(ImportXmlField::Url) if event.name().as_ref() == b"URL" => {
                    url = Some(parse_source_url(std::mem::take(&mut field_value))?);
                    current_field = None;
                }
                None if event.name().as_ref() == b"IPFS3ImportRequest"
                    && saw_root
                    && !closed_root =>
                {
                    closed_root = true;
                }
                _ => return Err(invalid_import()),
            },
            Ok(quick_xml::events::Event::Eof) => break,
            Err(_) | Ok(_) => return Err(invalid_import()),
        }
        buffer.clear();
    }

    if !saw_root || !closed_root || current_field.is_some() {
        return Err(invalid_import());
    }
    match (cid, url) {
        (Some(cid), None) => Ok(ParsedImportSource::Cid(cid)),
        (None, Some(url)) => Ok(ParsedImportSource::Url(url)),
        _ => Err(invalid_import()),
    }
}

fn validate_root_attributes(event: &quick_xml::events::BytesStart<'_>) -> S3Result<()> {
    for attribute in event.attributes() {
        let attribute = attribute.map_err(|_| invalid_import())?;
        let name = attribute.key.as_ref();
        if name != b"xmlns" && !name.starts_with(b"xmlns:") {
            return Err(invalid_import());
        }
        if attribute.value.as_ref().contains(&b'&') {
            return Err(invalid_import());
        }
    }
    Ok(())
}

fn reject_attributes(event: &quick_xml::events::BytesStart<'_>) -> S3Result<()> {
    if event.attributes().next().is_some() {
        return Err(invalid_import());
    }
    Ok(())
}

fn parse_canonical_cid(value: String) -> S3Result<String> {
    if value.is_empty() {
        return Err(invalid_import());
    }
    value
        .parse::<cid::Cid>()
        .map(|cid| cid.to_string())
        .map_err(|_| invalid_import())
}

fn parse_source_url(value: String) -> S3Result<url::Url> {
    if value.is_empty() || raw_authority_has_userinfo(&value) {
        return Err(invalid_import());
    }
    let source = url::Url::parse(&value).map_err(|_| invalid_import())?;
    if !source.username().is_empty() || source.password().is_some() {
        return Err(invalid_import());
    }
    Ok(source)
}

fn raw_authority_has_userinfo(value: &str) -> bool {
    let Some((_, remainder)) = value.split_once(':') else {
        return false;
    };
    let bytes = remainder.as_bytes();
    if bytes.len() < 2 || !matches!(bytes[0], b'/' | b'\\') || !matches!(bytes[1], b'/' | b'\\') {
        return false;
    }
    let authority = &remainder[2..];
    let authority_end = authority
        .find(['/', '\\', '?', '#'])
        .unwrap_or(authority.len());
    authority[..authority_end].contains('@')
}
