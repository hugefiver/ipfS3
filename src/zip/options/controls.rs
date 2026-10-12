use std::collections::BTreeSet;

use http::HeaderMap;

use super::{ZIP_TARGETS, ZIP_TOKEN, ZipOptions, invalid, parse_zip_options_impl};
use crate::pinning::tags::{
    ContentMode, ObjectTag, PinControl, TagErrorKind, ZipRootCapture, decode_tagging_header,
    resolve_zip_root_option, validate_tag_set_classified,
};

/// Preserve ordinary user-tag errors while exposing malformed ZIP controls to
/// the shared authenticated boundary, before any intake or publication writes.
pub(crate) fn parse_authenticated_zip_tagging(
    value: &str,
    is_zip: bool,
) -> s3s::S3Result<Vec<ObjectTag>> {
    let classify = |error: crate::pinning::tags::TagError| match error.kind {
        TagErrorKind::ZipControl => invalid("invalid ZIP control tags"),
        TagErrorKind::ReservedControl if is_zip => invalid("invalid ZIP control tags"),
        _ => crate::error::AppError::InvalidPinningRequest("invalid x-amz-tagging header".into())
            .into(),
    };
    let tags = decode_tagging_header(value).map_err(classify)?;
    validate_tag_set_classified(&tags).map_err(classify)?;
    Ok(tags)
}

/// Common structural result, not pin authorization. Persist effective controls
/// with CapturedZipIntent after server policy capture. Query pairs and tags are
/// decoded, multiplicity-preserving values from the authenticated request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedZipControls {
    pub options: ZipOptions,
    pub target_prefix: Option<String>,
    pub result_enabled: bool,
    pub root: ZipRootCapture,
    pub pin: PinControl,
    pub targets_explicit: bool,
    pub token_supplied: bool,
}

/// Validate decoded ZIP query controls without reading headers or generating a
/// token. Returns prefix, result selection, and whether a ZIP control is present.
pub(crate) fn parse_authenticated_zip_query(
    query: &[(String, String)],
) -> s3s::S3Result<(Option<String>, bool, bool)> {
    let mut seen = BTreeSet::new();
    let mut prefix = None;
    let mut result_enabled = true;
    let mut has_query_control = false;
    for (name, value) in query {
        if name.starts_with("decompress-zip")
            || name.starts_with("x-ipfs3-zip-")
            || name.starts_with("ipfs3-zip-")
        {
            has_query_control = true;
            if !seen.insert(name.as_str()) {
                return Err(invalid("duplicate ZIP control query"));
            }
            match name.as_str() {
                "decompress-zip" => {
                    prefix = Some(
                        crate::zip::sanitize::normalize_target_prefix(value)
                            .map_err(|_| invalid("invalid ZIP target prefix"))?,
                    );
                }
                "decompress-zip-result" => {
                    result_enabled = match value.as_str() {
                        "true" => true,
                        "false" => false,
                        _ => return Err(invalid("ZIP result must be exactly true or false")),
                    }
                }
                _ => return Err(invalid("unknown ZIP control query")),
            }
        }
    }
    Ok((prefix, result_enabled, has_query_control))
}

pub fn parse_authenticated_zip_controls(
    headers: &HeaderMap,
    query: &[(String, String)],
    tags: &[ObjectTag],
    is_zip: bool,
    root_default: bool,
) -> s3s::S3Result<Option<ParsedZipControls>> {
    let (prefix, result_enabled, has_query_control) = parse_authenticated_zip_query(query)?;
    let has_zip_tag = tags.iter().any(|tag| {
        tag.key.starts_with("ipfs-s3:zip-")
            || (tag.key == "ipfs-s3:content" && tag.value == "decompressed")
    });
    if !is_zip
        && !has_query_control
        && !has_zip_tag
        && !headers
            .keys()
            .any(|name| name.as_str().starts_with("x-ipfs3-zip-"))
    {
        // Let the unchanged ordinary-object path classify its own pin/tags.
        return Ok(None);
    }
    let root = resolve_zip_root_option(tags, root_default)
        .map_err(|_| invalid("invalid ZIP control tags"))?;
    let pin = PinControl::from_tags(tags).map_err(|_| invalid("invalid pin control tags"))?;
    let root_override = match root {
        ZipRootCapture::Configured(_) => None,
        ZipRootCapture::Tagged(value) => Some(value),
    };
    let options = parse_zip_options_impl(headers, result_enabled, root_override, root_default)?;
    let has_zip_control = has_query_control
        || root_override.is_some()
        || !matches!(options, ZipOptions::Legacy)
        || matches!(
            pin,
            PinControl::Request {
                content: ContentMode::Decompressed,
                ..
            }
        );
    if !is_zip && has_zip_control {
        return Err(invalid("ZIP controls require a decompress-zip request"));
    }
    if is_zip && matches!(pin, PinControl::Renew { .. }) {
        return Err(invalid("ZIP admission cannot renew a manual lease"));
    }
    if let ZipOptions::V2(v2) = &options {
        if !matches!(pin, PinControl::Absent) && !v2.publish_source {
            return Err(invalid(
                "archive-owned manual pinning requires a published source",
            ));
        }
        if matches!(
            pin,
            PinControl::Request {
                content: ContentMode::Decompressed,
                ..
            }
        ) && !v2.publish_extracted
        {
            return Err(invalid(
                "decompressed manual pinning requires extracted outputs",
            ));
        }
    }
    Ok(Some(ParsedZipControls {
        options,
        target_prefix: prefix,
        result_enabled,
        root,
        pin,
        targets_explicit: headers.contains_key(ZIP_TARGETS),
        token_supplied: headers.contains_key(ZIP_TOKEN),
    }))
}
