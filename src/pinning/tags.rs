use std::collections::BTreeSet;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::{Deserialize, Serialize};

use crate::pinning::config::LeaseDuration;

const TAGGING_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

const RESERVED_KEYS: [&str; 5] = [
    "ipfs-s3:pin",
    "ipfs-s3:duration",
    "ipfs-s3:content",
    "ipfs-s3:retain-until",
    "ipfs-s3:zip-root",
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectTag {
    pub key: String,
    pub value: String,
}

impl ObjectTag {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PinControl {
    Absent,
    Cancel,
    Request {
        duration: Option<LeaseDuration>,
        content: ContentMode,
    },
    Renew {
        retain_until: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentMode {
    Object,
    Decompressed,
}

/// Captured ZIP root decision and its origin; persist the capture on ZIP admission
/// so later config changes do not alter an accepted request's behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ZipRootCapture {
    Configured(bool),
    Tagged(bool),
}

impl ZipRootCapture {
    pub fn enabled(self) -> bool {
        match self {
            Self::Configured(enabled) | Self::Tagged(enabled) => enabled,
        }
    }
}

/// Resolve only at ZIP admission. Callers must retain original tags (including
/// zip-root) for fingerprinting; resolving a default must not erase an override.
pub fn resolve_zip_root_option(tags: &[ObjectTag], default: bool) -> Result<ZipRootCapture> {
    validate_tag_set(tags)?;
    Ok(match reserved_value(tags, "ipfs-s3:zip-root") {
        Some(value) => ZipRootCapture::Tagged(parse_zip_root_value(value)?),
        None => ZipRootCapture::Configured(default),
    })
}

fn parse_zip_root_value(value: &str) -> Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("ipfs-s3:zip-root must be exactly true or false"),
    }
}

impl PinControl {
    pub fn from_tags(tags: &[ObjectTag]) -> Result<Self> {
        validate_tag_set(tags)?;

        let pin = reserved_value(tags, "ipfs-s3:pin");
        let duration = reserved_value(tags, "ipfs-s3:duration");
        let content = reserved_value(tags, "ipfs-s3:content");
        let retain_until = reserved_value(tags, "ipfs-s3:retain-until");

        let Some(pin) = pin else {
            if duration.is_some() || content.is_some() || retain_until.is_some() {
                bail!("pin control parameters require ipfs-s3:pin");
            }
            return Ok(Self::Absent);
        };

        match pin {
            "false" => {
                if duration.is_some() || content.is_some() || retain_until.is_some() {
                    bail!("ipfs-s3:pin=false cannot be combined with pin parameters");
                }
                Ok(Self::Cancel)
            }
            "true" => {
                if let Some(retain_until) = retain_until {
                    if duration.is_some() || content.is_some() {
                        bail!("ipfs-s3:retain-until cannot be combined with request parameters");
                    }
                    let retain_until = DateTime::parse_from_rfc3339(retain_until)
                        .map_err(|_| anyhow::anyhow!("invalid ipfs-s3:retain-until"))?
                        .with_timezone(&Utc);
                    return Ok(Self::Renew { retain_until });
                }

                let duration = duration.map(LeaseDuration::parse).transpose()?;
                let content = match content {
                    None | Some("object") => ContentMode::Object,
                    Some("decompressed") => ContentMode::Decompressed,
                    Some(_) => bail!("invalid ipfs-s3:content"),
                };
                Ok(Self::Request { duration, content })
            }
            _ => bail!("ipfs-s3:pin must be exactly true or false"),
        }
    }
}

pub fn parse_tagging_header(header: &str) -> Result<Vec<ObjectTag>> {
    if header.is_empty() {
        return Ok(Vec::new());
    }

    let mut tags = Vec::new();
    for pair in header.split('&') {
        let Some((raw_key, raw_value)) = pair.split_once('=') else {
            bail!("invalid tagging header pair");
        };
        tags.push(ObjectTag::new(
            decode_form_component(raw_key)?,
            decode_form_component(raw_value)?,
        ));
    }
    validate_tag_set(&tags)?;
    Ok(tags)
}

pub fn validate_tag_set(tags: &[ObjectTag]) -> Result<()> {
    if tags.len() > 10 {
        bail!("tag set exceeds the maximum of 10 tags");
    }

    let mut keys = BTreeSet::new();
    for tag in tags {
        if tag.key.is_empty() {
            bail!("tag key must not be empty");
        }
        if tag.key.chars().count() > 128 {
            bail!("tag key exceeds the maximum of 128 characters");
        }
        if tag.value.chars().count() > 256 {
            bail!("tag value exceeds the maximum of 256 characters");
        }
        if !keys.insert(tag.key.as_str()) {
            bail!("duplicate tag key");
        }
        if tag.key.starts_with("ipfs-s3:") && !RESERVED_KEYS.contains(&tag.key.as_str()) {
            bail!("unknown ipfs-s3 reserved tag key");
        }
        if tag.key == "ipfs-s3:zip-root" {
            parse_zip_root_value(&tag.value)?;
        }
    }
    Ok(())
}

pub fn encode_tagging_header(tags: &[ObjectTag]) -> String {
    tags.iter()
        .map(|tag| {
            format!(
                "{}={}",
                utf8_percent_encode(&tag.key, TAGGING_ENCODE_SET),
                utf8_percent_encode(&tag.value, TAGGING_ENCODE_SET)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn reserved_value<'a>(tags: &'a [ObjectTag], key: &str) -> Option<&'a str> {
    tags.iter()
        .find(|tag| tag.key == key)
        .map(|tag| tag.value.as_str())
}

fn decode_form_component(component: &str) -> Result<String> {
    for (index, byte) in component.bytes().enumerate() {
        if byte == b'%' {
            let bytes = component.as_bytes();
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                bail!("invalid percent encoding in tagging header");
            }
        }
    }

    let form_decoded = component.replace('+', " ");
    percent_encoding::percent_decode_str(&form_decoded)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| anyhow::anyhow!("invalid UTF-8 in tagging header"))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{
        ContentMode, ObjectTag, PinControl, ZipRootCapture, encode_tagging_header,
        parse_tagging_header, resolve_zip_root_option, validate_tag_set,
    };
    use crate::pinning::config::LeaseDuration;

    fn tags(pairs: &[(&str, &str)]) -> Vec<ObjectTag> {
        pairs
            .iter()
            .map(|(key, value)| ObjectTag::new(*key, *value))
            .collect()
    }

    #[test]
    fn header_codec_round_trips_form_encoding_and_preserves_user_values() {
        let tags = parse_tagging_header("team=R%26D&space=hello+world&empty=").unwrap();
        assert_eq!(
            tags,
            vec![
                ObjectTag::new("team", "R&D"),
                ObjectTag::new("space", "hello world"),
                ObjectTag::new("empty", ""),
            ]
        );
        assert_eq!(
            parse_tagging_header(&encode_tagging_header(&tags)).unwrap(),
            tags
        );
        assert_eq!(
            parse_tagging_header("plus=%2B+%2B").unwrap(),
            vec![ObjectTag::new("plus", "+ +")]
        );
        assert_eq!(
            encode_tagging_header(&tags),
            "team=R%26D&space=hello%20world&empty="
        );
    }

    #[test]
    fn header_codec_projects_rfc3986_unreserved_and_reserved_characters() {
        let tags = vec![ObjectTag::new("AZaz09-._~/%()&=+界", "AZaz09-._~/%()&=+é")];

        assert_eq!(
            encode_tagging_header(&tags),
            "AZaz09-._~%2F%25%28%29%26%3D%2B%E7%95%8C=AZaz09-._~%2F%25%28%29%26%3D%2B%C3%A9"
        );
        assert_eq!(
            parse_tagging_header(&encode_tagging_header(&tags)).unwrap(),
            tags
        );
    }

    #[test]
    fn header_rejects_malformed_form_pairs_and_percent_encoding() {
        for header in [
            "missing-equals",
            "=missing-key",
            "key=value&",
            "key=value&&other=value",
            "key=%",
            "key=%2",
            "key=%GG",
            "key=%FF",
        ] {
            assert!(
                parse_tagging_header(header).is_err(),
                "{header:?} must be invalid"
            );
        }
    }

    #[test]
    fn tag_set_enforces_s3_limits_duplicates_and_reserved_namespace() {
        let eleven = (0..11)
            .map(|index| ObjectTag::new(format!("key-{index}"), "value"))
            .collect::<Vec<_>>();
        let cases = [
            (eleven, "more than ten tags"),
            (
                tags(&[("duplicate", "first"), ("duplicate", "second")]),
                "duplicate keys",
            ),
            (tags(&[("", "value")]), "empty key"),
            (
                tags(&[("k".repeat(129).as_str(), "value")]),
                "key longer than 128 characters",
            ),
            (
                tags(&[("key", "v".repeat(257).as_str())]),
                "value longer than 256 characters",
            ),
            (
                tags(&[("ipfs-s3:unknown", "value")]),
                "unknown reserved key",
            ),
            (tags(&[("ipfs-s3:pin ", "true")]), "unknown reserved key"),
        ];

        for (tag_set, reason) in cases {
            assert!(validate_tag_set(&tag_set).is_err(), "{reason}");
        }

        assert!(validate_tag_set(&tags(&[("IPFS-S3:pin", "true")])).is_ok());
        assert!(
            validate_tag_set(&tags(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:duration", "1d"),
                ("ipfs-s3:content", "object"),
                ("ipfs-s3:retain-until", "2026-07-21T00:00:00Z"),
            ]))
            .is_ok()
        );
    }

    #[test]
    fn tag_set_accepts_exact_unicode_scalar_boundaries() {
        let ten_tags = (0..10)
            .map(|index| ObjectTag::new(format!("key-{index}"), "value"))
            .collect::<Vec<_>>();
        assert!(validate_tag_set(&ten_tags).is_ok());

        assert!(validate_tag_set(&[ObjectTag::new("界".repeat(128), "é".repeat(256),)]).is_ok());
        assert!(validate_tag_set(&[ObjectTag::new("界".repeat(129), "value")]).is_err());
        assert!(validate_tag_set(&[ObjectTag::new("key", "é".repeat(257))]).is_err());
    }

    #[test]
    fn reserved_controls_normalize_every_valid_form() {
        assert_eq!(
            PinControl::from_tags(&tags(&[("team", "R&D")])).unwrap(),
            PinControl::Absent
        );
        assert_eq!(
            PinControl::from_tags(&tags(&[("ipfs-s3:pin", "false")])).unwrap(),
            PinControl::Cancel
        );
        assert_eq!(
            PinControl::from_tags(&tags(&[("ipfs-s3:pin", "true")])).unwrap(),
            PinControl::Request {
                duration: None,
                content: ContentMode::Object,
            }
        );
        assert_eq!(
            PinControl::from_tags(&tags(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:duration", "30d"),
                ("ipfs-s3:content", "decompressed"),
            ]))
            .unwrap(),
            PinControl::Request {
                duration: Some(LeaseDuration::parse("30d").unwrap()),
                content: ContentMode::Decompressed,
            }
        );
        assert_eq!(
            PinControl::from_tags(&tags(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", "2026-07-21T01:30:00+01:30"),
            ]))
            .unwrap(),
            PinControl::Renew {
                retain_until: Utc.with_ymd_and_hms(2026, 7, 21, 0, 0, 0).unwrap(),
            }
        );
    }

    #[test]
    fn reserved_controls_reject_every_invalid_combination() {
        let invalid = [
            tags(&[("ipfs-s3:pin", "yes")]),
            tags(&[("ipfs-s3:duration", "30d")]),
            tags(&[("ipfs-s3:content", "object")]),
            tags(&[("ipfs-s3:retain-until", "2026-07-21T00:00:00Z")]),
            tags(&[("ipfs-s3:pin", "false"), ("ipfs-s3:duration", "30d")]),
            tags(&[("ipfs-s3:pin", "false"), ("ipfs-s3:content", "object")]),
            tags(&[
                ("ipfs-s3:pin", "false"),
                ("ipfs-s3:retain-until", "2026-07-21T00:00:00Z"),
            ]),
            tags(&[("ipfs-s3:pin", "true"), ("ipfs-s3:duration", "0d")]),
            tags(&[("ipfs-s3:pin", "true"), ("ipfs-s3:content", "recursive")]),
            tags(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:retain-until", "not-a-time"),
            ]),
            tags(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:duration", "30d"),
                ("ipfs-s3:retain-until", "2026-07-21T00:00:00Z"),
            ]),
            tags(&[
                ("ipfs-s3:pin", "true"),
                ("ipfs-s3:content", "object"),
                ("ipfs-s3:retain-until", "2026-07-21T00:00:00Z"),
            ]),
        ];

        for tag_set in invalid {
            assert!(PinControl::from_tags(&tag_set).is_err(), "{tag_set:?}");
        }
    }

    #[test]
    fn zip_root_capture_preserves_configured_default_and_exact_tag_override() {
        assert_eq!(
            resolve_zip_root_option(&[], true).unwrap(),
            ZipRootCapture::Configured(true)
        );
        assert_eq!(
            resolve_zip_root_option(&[], false).unwrap(),
            ZipRootCapture::Configured(false)
        );
        for (value, expected) in [("true", true), ("false", false)] {
            let tag_set = tags(&[("ipfs-s3:zip-root", value)]);
            for default in [true, false] {
                let capture = resolve_zip_root_option(&tag_set, default).unwrap();
                assert_eq!(capture, ZipRootCapture::Tagged(expected));
                assert_eq!(capture.enabled(), expected);
                assert_eq!(PinControl::from_tags(&tag_set).unwrap(), PinControl::Absent);
            }
        }
    }

    #[test]
    fn zip_root_tag_validates_exact_values_duplicates_and_existing_tag_rules() {
        for value in ["False", "TRUE", "1", "yes", "", "true "] {
            let tag_set = tags(&[("ipfs-s3:zip-root", value)]);
            assert!(validate_tag_set(&tag_set).is_err(), "{value:?}");
            assert!(PinControl::from_tags(&tag_set).is_err(), "{value:?}");
            assert!(
                resolve_zip_root_option(&tag_set, true).is_err(),
                "{value:?}"
            );
        }
        for tag_set in [
            tags(&[("ipfs-s3:zip-root", "true"), ("ipfs-s3:zip-root", "false")]),
            tags(&[("ipfs-s3:zip-root", "true"), ("ipfs-s3:unknown", "value")]),
            tags(&[("ipfs-s3:zip-root", "true"), ("", "value")]),
        ] {
            assert!(validate_tag_set(&tag_set).is_err());
            assert!(resolve_zip_root_option(&tag_set, false).is_err());
        }
    }

    #[test]
    fn zip_root_encoding_round_trip_and_pinning_controls_are_orthogonal() {
        let encoded =
            "ipfs-s3%3Azip-root=false&ipfs-s3%3Apin=true&ipfs-s3%3Aduration=1d&team=R%26D";
        let tag_set = parse_tagging_header(encoded).unwrap();
        assert_eq!(encode_tagging_header(&tag_set), encoded);
        assert_eq!(
            parse_tagging_header(&encode_tagging_header(&tag_set)).unwrap(),
            tag_set
        );
        assert_eq!(
            resolve_zip_root_option(&tag_set, true).unwrap(),
            ZipRootCapture::Tagged(false)
        );
        assert_eq!(
            PinControl::from_tags(&tag_set).unwrap(),
            PinControl::Request {
                duration: Some(LeaseDuration::parse("1d").unwrap()),
                content: ContentMode::Object,
            }
        );

        let without_pin = parse_tagging_header("ipfs-s3%3Azip-root=true&team=R%26D").unwrap();
        assert_eq!(
            PinControl::from_tags(&without_pin).unwrap(),
            PinControl::Absent
        );
        let unrelated_case = tags(&[("IPFS-S3:zip-root", "false")]);
        assert_eq!(
            resolve_zip_root_option(&unrelated_case, true).unwrap(),
            ZipRootCapture::Configured(true)
        );
        assert!(
            PinControl::from_tags(&tags(&[
                ("ipfs-s3:zip-root", "true"),
                ("ipfs-s3:duration", "1d"),
            ]))
            .is_err()
        );
        assert!(parse_tagging_header("ipfs-s3%3Azip-root=true&ipfs-s3:zip-root=false").is_err());
        assert!(parse_tagging_header("ipfs-s3%3Azip-root=FALSE").is_err());
        assert!(parse_tagging_header("ipfs-s3%3Azip-root=%GG").is_err());
    }
}
