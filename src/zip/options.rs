//! Versioned ZIP request contract. Pass only headers from an authenticated
//! `S3Request`; this parser does not authenticate the request itself.
//!
//! v2 requires header-SigV4 Authorization signing each of these ASCII headers:
//! `x-ipfs3-zip-contract: v2`, `x-ipfs3-zip-publish-source: true|false`,
//! `x-ipfs3-zip-publish-extracted: true|false`,
//! `x-ipfs3-zip-targets: none|source|extracted|both`, and
//! `x-ipfs3-zip-token: [A-Za-z0-9._~-]{1,128}`. `x-amz-tagging` must also be
//! signed if present. The v2 result cannot be disabled. Legacy requests with
//! none of these headers retain the old contract.

use http::{HeaderMap, header::AUTHORIZATION};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ZIP_CONTRACT: &str = "x-ipfs3-zip-contract";
pub const PUBLISH_SOURCE: &str = "x-ipfs3-zip-publish-source";
pub const PUBLISH_EXTRACTED: &str = "x-ipfs3-zip-publish-extracted";
pub const ZIP_TARGETS: &str = "x-ipfs3-zip-targets";
pub const ZIP_TOKEN: &str = "x-ipfs3-zip-token";
pub const ZIP_EXPECTED_SHA256: &str = "x-ipfs3-zip-expected-sha256";
const V2_HEADERS: [&str; 5] = [
    ZIP_CONTRACT,
    PUBLISH_SOURCE,
    PUBLISH_EXTRACTED,
    ZIP_TARGETS,
    ZIP_TOKEN,
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ZipOptions {
    Legacy,
    V2(ZipV2Options),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ZipTargets {
    None,
    Source,
    Extracted,
    Both,
}

/// Persist this entire snapshot on admission; never recompute missing values
/// from the current process defaults when replaying a token.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZipV2Options {
    pub publish_source: bool,
    pub publish_extracted: bool,
    pub targets: ZipTargets,
    pub token: String,
    pub root_override: Option<bool>,
    /// Effective value captured on first admission, never re-evaluated on replay.
    pub root_enabled: bool,
    pub result_version: u8,
}

impl ZipV2Options {
    /// A source-only ZIP cannot construct a fictitious empty directory root.
    pub fn build_root(&self) -> bool {
        self.publish_extracted && self.root_enabled
    }

    /// Entry-level remote policy is an additional restriction, never a way to
    /// add a source or child excluded by the batch or private-prefix policy.
    /// Callers must separately reject conflicting server-enforced requirements;
    /// returning false here is not permission to silently downgrade those.
    pub fn target_eligible(&self, kind: ZipOutputKind, output_policy_permits: bool) -> bool {
        output_policy_permits
            && match kind {
                ZipOutputKind::Source => {
                    self.publish_source
                        && matches!(self.targets, ZipTargets::Source | ZipTargets::Both)
                }
                ZipOutputKind::Extracted => {
                    self.publish_extracted
                        && matches!(self.targets, ZipTargets::Extracted | ZipTargets::Both)
                }
            }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZipOutputKind {
    Source,
    Extracted,
}

fn invalid(message: &'static str) -> s3s::S3Error {
    s3s::s3_error!(InvalidRequest, "{message}")
}

fn single<'a>(headers: &'a HeaderMap, name: &str) -> s3s::S3Result<&'a str> {
    let mut values = headers.get_all(name).iter();
    let first = values
        .next()
        .ok_or_else(|| invalid("missing ZIP v2 control header"))?;
    if values.next().is_some() {
        return Err(invalid("duplicate ZIP v2 control header"));
    }
    first
        .to_str()
        .map_err(|_| invalid("invalid ZIP v2 control header"))
}

fn bool_header(headers: &HeaderMap, name: &str) -> s3s::S3Result<bool> {
    match single(headers, name)? {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(invalid("ZIP v2 booleans must be exactly true or false")),
    }
}

/// Extracts the names that s3s used to validate a header-Authorization SigV4
/// request. The caller MUST pass the unchanged headers from an authenticated
/// S3Request, not a raw HTTP request or an unverified Credentials test stub.
/// Presigned and SigV2 requests have no acceptable Authorization evidence here.
fn signed_header_names(headers: &HeaderMap) -> s3s::S3Result<Vec<&str>> {
    let authorization = single(headers, AUTHORIZATION.as_str())?
        .strip_prefix("AWS4-HMAC-SHA256 Credential=")
        .ok_or_else(|| invalid("ZIP v2 requires header SigV4 Authorization"))?;
    let mut fields = authorization.split(", ");
    let credential = fields.next().unwrap_or_default();
    let names = fields
        .next()
        .unwrap_or_default()
        .strip_prefix("SignedHeaders=")
        .ok_or_else(|| invalid("ZIP v2 requires SigV4 SignedHeaders"))?;
    let signature = fields
        .next()
        .unwrap_or_default()
        .strip_prefix("Signature=")
        .ok_or_else(|| invalid("invalid ZIP v2 SigV4 Authorization"))?;
    if fields.next().is_some()
        || credential.is_empty()
        || signature.len() != 64
        || !signature.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(invalid("invalid ZIP v2 SigV4 Authorization"));
    }
    let names: Vec<_> = names.split(';').collect();
    if names.iter().any(|name| {
        name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    }) || names.windows(2).any(|pair| pair[0] >= pair[1])
        || !names.contains(&"host")
    {
        return Err(invalid("invalid ZIP v2 SigV4 SignedHeaders"));
    }
    Ok(names)
}

/// For direct PUT, pass `result_enabled` from the already parsed legacy query
/// (false only for `decompress-zip-result=false`). `root_override` is the
/// validated `ipfs-s3:zip-root` tag, if supplied; the configured default is
/// deliberately NOT a fingerprint field; its effective value is captured in
/// the returned snapshot. Run this before any body or DB work.
pub fn parse_zip_options(
    headers: &HeaderMap,
    result_enabled: bool,
    root_override: Option<bool>,
    root_default: bool,
) -> s3s::S3Result<ZipOptions> {
    parse_zip_options_impl(headers, result_enabled, root_override, root_default, false)
}

/// Import may commit to an immutable URL input before its worker downloads it.
/// The signed expected digest is a promise, not proof of the actual ZIP bytes.
pub fn parse_zip_options_for_import(
    headers: &HeaderMap,
    result_enabled: bool,
    root_override: Option<bool>,
    root_default: bool,
) -> s3s::S3Result<(ZipOptions, Option<String>)> {
    let options =
        parse_zip_options_impl(headers, result_enabled, root_override, root_default, true)?;
    let expected = if headers.contains_key(ZIP_EXPECTED_SHA256) {
        let digest = single(headers, ZIP_EXPECTED_SHA256)?;
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(invalid("invalid ZIP v2 expected SHA-256"));
        }
        Some(digest.to_owned())
    } else {
        None
    };
    Ok((options, expected))
}

fn parse_zip_options_impl(
    headers: &HeaderMap,
    result_enabled: bool,
    root_override: Option<bool>,
    root_default: bool,
    allow_expected_sha256: bool,
) -> s3s::S3Result<ZipOptions> {
    let has_v2_header = headers
        .keys()
        .any(|name| name.as_str().starts_with("x-ipfs3-zip-"));
    if !has_v2_header {
        return Ok(ZipOptions::Legacy);
    }
    if headers.keys().any(|name| {
        name.as_str().starts_with("x-ipfs3-zip-")
            && !V2_HEADERS.contains(&name.as_str())
            && !(allow_expected_sha256 && name.as_str() == ZIP_EXPECTED_SHA256)
    }) {
        return Err(invalid("unknown ZIP v2 control header"));
    }
    if single(headers, ZIP_CONTRACT)? != "v2" {
        return Err(invalid("unsupported ZIP contract version"));
    }
    let signed = signed_header_names(headers)?;
    if V2_HEADERS.iter().any(|name| !signed.contains(name)) {
        return Err(invalid("ZIP v2 control header must be SigV4-signed"));
    }
    if headers.contains_key(ZIP_EXPECTED_SHA256) && !signed.contains(&ZIP_EXPECTED_SHA256) {
        return Err(invalid("ZIP v2 expected SHA-256 must be SigV4-signed"));
    }
    if (root_override.is_some() && !headers.contains_key("x-amz-tagging"))
        || (headers.contains_key("x-amz-tagging") && !signed.contains(&"x-amz-tagging"))
    {
        return Err(invalid("ZIP root override must be SigV4-signed"));
    }
    if !result_enabled {
        return Err(invalid("ZIP v2 requires a result"));
    }
    let publish_source = bool_header(headers, PUBLISH_SOURCE)?;
    let publish_extracted = bool_header(headers, PUBLISH_EXTRACTED)?;
    if !publish_source && !publish_extracted {
        return Err(invalid("ZIP v2 must publish at least one output"));
    }
    let targets = match single(headers, ZIP_TARGETS)? {
        "none" => ZipTargets::None,
        "source" => ZipTargets::Source,
        "extracted" => ZipTargets::Extracted,
        "both" => ZipTargets::Both,
        _ => return Err(invalid("invalid ZIP v2 targets")),
    };
    if (matches!(targets, ZipTargets::Source | ZipTargets::Both) && !publish_source)
        || (matches!(targets, ZipTargets::Extracted | ZipTargets::Both) && !publish_extracted)
    {
        return Err(invalid("ZIP targets must be published outputs"));
    }
    let token = single(headers, ZIP_TOKEN)?;
    if token.is_empty()
        || token.len() > 128
        || !token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._~-".contains(&b))
    {
        return Err(invalid("invalid ZIP v2 idempotency token"));
    }
    Ok(ZipOptions::V2(ZipV2Options {
        publish_source,
        publish_extracted,
        targets,
        token: token.to_owned(),
        root_override,
        root_enabled: root_override.unwrap_or(root_default),
        result_version: 2,
    }))
}

/// The caller provides the exact authenticated destination, source identity
/// (e.g. SHA-256 of upload bytes), and other semantic controls (tags, metadata,
/// etc.) so a replay cannot quietly alter the published result. Sort order of
/// semantic_controls does not affect identity; multiplicity does.
pub struct ZipFingerprintContext<'a> {
    pub principal: &'a str,
    pub bucket: &'a str,
    pub archive_key: &'a str,
    pub target_prefix: &'a str,
    pub input_identity: &'a str,
    pub semantic_controls: &'a [(&'a str, &'a str)],
}

pub fn zip_request_fingerprint(
    options: &ZipV2Options,
    context: &ZipFingerprintContext<'_>,
) -> String {
    let mut sha = Sha256::new();
    let mut add = |field: &str| {
        sha.update((field.len() as u64).to_be_bytes());
        sha.update(field.as_bytes());
    };
    add("ipfs3-zip-contract-v2");
    for field in [
        context.principal,
        context.bucket,
        context.archive_key,
        context.target_prefix,
        context.input_identity,
        &options.token,
        if options.publish_source {
            "true"
        } else {
            "false"
        },
        if options.publish_extracted {
            "true"
        } else {
            "false"
        },
        match options.targets {
            ZipTargets::None => "none",
            ZipTargets::Source => "source",
            ZipTargets::Extracted => "extracted",
            ZipTargets::Both => "both",
        },
        match options.root_override {
            Some(true) => "true",
            Some(false) => "false",
            None => "absent",
        },
        "result-v2",
    ] {
        add(field);
    }
    add(&options.result_version.to_string());
    let mut controls = context.semantic_controls.to_vec();
    controls.sort_unstable();
    add(&controls.len().to_string());
    for (key, value) in controls {
        add(key);
        add(value);
    }
    format!("sha256:{}", hex::encode(sha.finalize()))
}

#[cfg(test)]
mod tests {
    use http::{HeaderMap, HeaderValue};

    use super::*;

    fn headers(source: &str, extracted: &str, targets: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in [
            (ZIP_CONTRACT, "v2"),
            (PUBLISH_SOURCE, source),
            (PUBLISH_EXTRACTED, extracted),
            (ZIP_TARGETS, targets),
            (ZIP_TOKEN, "request-123"),
        ] {
            headers.insert(name, HeaderValue::from_str(value).unwrap());
        }
        signed(&mut headers, &V2_HEADERS);
        headers
    }

    fn signed(headers: &mut HeaderMap, names: &[&str]) {
        let mut names = names.to_vec();
        names.push("host");
        names.sort_unstable();
        let auth = format!(
            "AWS4-HMAC-SHA256 Credential=test/20260927/us-east-1/s3/aws4_request, SignedHeaders={}, Signature={}",
            names.join(";"),
            "0".repeat(64)
        );
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&auth).unwrap(),
        );
    }

    #[test]
    fn legacy_unchanged_and_unsigned_v2_field_cannot_fall_back() {
        assert_eq!(
            parse_zip_options(&HeaderMap::new(), false, None, true).unwrap(),
            ZipOptions::Legacy
        );
        let mut partial = HeaderMap::new();
        partial.insert(PUBLISH_SOURCE, HeaderValue::from_static("false"));
        assert!(parse_zip_options(&partial, true, None, true).is_err());
        let mut unknown = HeaderMap::new();
        unknown.insert("x-ipfs3-zip-unknown", HeaderValue::from_static("ignored"));
        assert!(parse_zip_options(&unknown, true, None, true).is_err());
        let mut v2 = headers("false", "true", "extracted");
        v2.insert("x-ipfs3-zip-unknown", HeaderValue::from_static("ignored"));
        assert!(parse_zip_options(&v2, true, None, true).is_err());
    }

    #[test]
    fn all_v2_controls_must_be_signed_and_authorization_must_be_parseable() {
        let mut headers = headers("false", "true", "extracted");
        assert!(matches!(
            parse_zip_options(&headers, true, None, true),
            Ok(ZipOptions::V2(_))
        ));
        headers.remove(http::header::AUTHORIZATION);
        assert!(parse_zip_options(&headers, true, None, true).is_err());
        signed(&mut headers, &V2_HEADERS[..4]);
        assert!(parse_zip_options(&headers, true, None, true).is_err());
        signed(&mut headers, &V2_HEADERS);
        assert!(parse_zip_options(&headers, true, Some(false), true).is_err());
        headers.insert(
            "x-amz-tagging",
            HeaderValue::from_static("ipfs-s3%3Azip-root=false"),
        );
        assert!(parse_zip_options(&headers, true, None, true).is_err());
        let mut signed_names = V2_HEADERS.to_vec();
        signed_names.push("x-amz-tagging");
        signed(&mut headers, &signed_names);
        assert!(parse_zip_options(&headers, true, Some(false), true).is_ok());
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 Credential=test, SignedHeaders=host, Signature=bad",
            ),
        );
        assert!(parse_zip_options(&headers, true, Some(false), true).is_err());
    }

    #[test]
    fn import_expected_digest_is_signed_canonical_and_not_a_direct_put_control() {
        let mut request = headers("false", "true", "none");
        let digest = "a".repeat(64);
        request.insert(ZIP_EXPECTED_SHA256, HeaderValue::from_str(&digest).unwrap());
        assert!(parse_zip_options_for_import(&request, true, None, true).is_err());
        let mut signed_names = V2_HEADERS.to_vec();
        signed_names.push(ZIP_EXPECTED_SHA256);
        signed(&mut request, &signed_names);
        assert_eq!(
            parse_zip_options_for_import(&request, true, None, true)
                .unwrap()
                .1,
            Some(digest.clone())
        );
        assert!(parse_zip_options(&request, true, None, true).is_err());
        request.append(ZIP_EXPECTED_SHA256, HeaderValue::from_str(&digest).unwrap());
        assert!(parse_zip_options_for_import(&request, true, None, true).is_err());
        request.remove(ZIP_EXPECTED_SHA256);
        request.insert(
            ZIP_EXPECTED_SHA256,
            HeaderValue::from_static(
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            ),
        );
        assert!(parse_zip_options_for_import(&request, true, None, true).is_err());
    }

    #[test]
    fn publication_combinations_are_explicit_and_targets_are_intersection_bounded() {
        for (source, extracted, targets) in [
            ("false", "false", "none"),
            ("false", "true", "source"),
            ("true", "false", "extracted"),
            ("true", "true", "all"),
        ] {
            assert!(
                parse_zip_options(&headers(source, extracted, targets), true, None, true).is_err()
            );
        }
        assert!(parse_zip_options(&headers("false", "true", "none"), false, None, true).is_err());
        assert!(parse_zip_options(&headers("true", "true", "none"), false, None, true).is_err());
        let ZipOptions::V2(snapshot) =
            parse_zip_options(&headers("true", "false", "source"), true, None, true).unwrap()
        else {
            panic!("expected v2")
        };
        assert!(!snapshot.build_root());
        assert!(!snapshot.publish_extracted);
        assert!(snapshot.target_eligible(ZipOutputKind::Source, true));
        assert!(!snapshot.target_eligible(ZipOutputKind::Source, false));
        assert!(!snapshot.target_eligible(ZipOutputKind::Extracted, true));
    }

    #[test]
    fn reject_invalid_bool_duplicate_and_oversize_or_unsafe_token() {
        let mut request = headers("False", "true", "none");
        assert!(parse_zip_options(&request, true, None, true).is_err());
        request = headers("true", "true", "none");
        request.append(ZIP_TOKEN, HeaderValue::from_static("second"));
        assert!(parse_zip_options(&request, true, None, true).is_err());
        for token in ["", "a b", "a/b", "é", &"x".repeat(129)] {
            let mut request = headers("true", "true", "none");
            request.insert(ZIP_TOKEN, HeaderValue::from_str(token).unwrap());
            assert!(
                parse_zip_options(&request, true, None, true).is_err(),
                "token: {token}"
            );
        }
    }

    #[test]
    fn fingerprint_is_stable_across_config_defaults_and_changes_with_semantics() {
        let request = headers("false", "true", "extracted");
        let ZipOptions::V2(snapshot) = parse_zip_options(&request, true, None, true).unwrap()
        else {
            panic!("expected v2")
        };
        let ctx = ZipFingerprintContext {
            principal: "tenant",
            bucket: "bucket",
            archive_key: "archive.zip",
            target_prefix: "out/",
            input_identity: "sha256:123",
            semantic_controls: &[("tag", "value")],
        };
        let first = zip_request_fingerprint(&snapshot, &ctx);
        assert_eq!(first, zip_request_fingerprint(&snapshot, &ctx));
        let ZipOptions::V2(new_default) = parse_zip_options(&request, true, None, false).unwrap()
        else {
            panic!("expected v2")
        };
        assert!(!new_default.root_enabled);
        assert_eq!(first, zip_request_fingerprint(&new_default, &ctx));
        let persisted: ZipV2Options =
            serde_json::from_str(&serde_json::to_string(&snapshot).unwrap()).unwrap();
        assert!(persisted.root_enabled);
        assert_eq!(snapshot, persisted);
        let other = ZipFingerprintContext {
            target_prefix: "other/",
            ..ctx
        };
        assert_ne!(first, zip_request_fingerprint(&snapshot, &other));
        let mut changed = snapshot.clone();
        changed.root_override = Some(true);
        assert_ne!(first, zip_request_fingerprint(&changed, &ctx));
        changed = snapshot.clone();
        changed.targets = ZipTargets::None;
        assert_ne!(first, zip_request_fingerprint(&changed, &ctx));
    }
}
