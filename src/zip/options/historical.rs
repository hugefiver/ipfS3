use anyhow::{Context, ensure};
use serde::{Deserialize, Serialize};

use super::{ZipTargets, ZipV2Options};

/// Only for trusted persisted pre-R25 captures. Do not route new HTTP controls
/// through this decoder. The old digest remains an input-integrity obligation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoricalZipOptions {
    pub options: ZipV2Options,
    pub expected_sha256: Option<String>,
}

/// Accepts the original bare options or the persisted direct/MPU/import
/// captured-options envelope. Import's separate digest column is passed intact.
pub fn decode_historical_zip_options(
    json: &str,
    expected_sha256: Option<&str>,
) -> anyhow::Result<HistoricalZipOptions> {
    let value: serde_json::Value = serde_json::from_str(json)
        .context("historical ZIP capture needs_attention: invalid stored contract")?;
    let options = serde_json::from_value(value.get("options").unwrap_or(&value).clone())
        .context("historical ZIP capture needs_attention: missing contract evidence")?;
    let embedded = value.get("expected_sha256").filter(|v| !v.is_null());
    ensure!(
        embedded.is_none_or(|value| value.is_string()),
        "historical ZIP capture needs_attention: invalid expected digest"
    );
    let embedded = embedded.and_then(|value| value.as_str());
    ensure!(
        embedded.is_none() || expected_sha256.is_none() || embedded == expected_sha256,
        "historical ZIP capture needs_attention: conflicting expected digest evidence"
    );
    let decoded = HistoricalZipOptions {
        options,
        expected_sha256: expected_sha256.or(embedded).map(str::to_owned),
    };
    let options = &decoded.options;
    ensure!(
        options.result_version == 2
            && (options.publish_source || options.publish_extracted)
            && (!matches!(options.targets, ZipTargets::Source | ZipTargets::Both)
                || options.publish_source)
            && (!matches!(options.targets, ZipTargets::Extracted | ZipTargets::Both)
                || options.publish_extracted)
            && !options.token.is_empty()
            && options.token.len() <= 128
            && options
                .token
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._~-".contains(&b)),
        "historical ZIP capture needs_attention: invalid contract evidence"
    );
    ensure!(
        decoded
            .expected_sha256
            .as_ref()
            .is_none_or(|digest| digest.len() == 64
                && digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))),
        "historical ZIP capture needs_attention: invalid expected digest"
    );
    Ok(decoded)
}
