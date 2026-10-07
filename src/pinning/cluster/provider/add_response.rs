//! Fixed v1.1.6 wire types: Proxy transforms AddedOutput; REST does not.
use super::{ClusterFailureCode as Code, ClusterProtocol, ClusterWriteFailure as Failure};
use serde::{Deserialize, Deserializer};

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "PascalCase")]
struct ProxyAddedOutput {
    name: String,
    #[serde(default)]
    hash: String,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    size: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestAddedOutput {
    name: String,
    // CidUndef is JSON null; still require the official field to be present.
    #[serde(deserialize_with = "optional_cid")]
    cid: Option<String>,
    #[serde(default)]
    bytes: u64,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    allocations: Vec<String>,
}

fn optional_cid<'de, D: Deserializer<'de>>(de: D) -> Result<Option<String>, D::Error> {
    Option::deserialize(de)
}

#[derive(Deserialize)]
struct ReportedRestCid {
    cid: Option<String>,
}
#[derive(Deserialize)]
struct ReportedProxyHash {
    #[serde(rename = "Hash")]
    hash: Option<String>,
}

pub(super) fn reported(protocol: ClusterProtocol, record: &[u8]) -> Option<String> {
    match protocol {
        ClusterProtocol::Proxy => {
            serde_json::from_slice::<ReportedProxyHash>(record)
                .ok()?
                .hash
        }
        ClusterProtocol::Rest => serde_json::from_slice::<ReportedRestCid>(record).ok()?.cid,
    }
}

pub(super) fn parse(
    protocol: ClusterProtocol,
    record: &[u8],
) -> Result<(Option<String>, Option<Vec<String>>), Failure> {
    let invalid = || Failure::new(Code::Protocol);
    match protocol {
        ClusterProtocol::Proxy => {
            let output: ProxyAddedOutput = serde_json::from_slice(record).map_err(|_| invalid())?;
            let _ = (output.name, output.bytes);
            if !output.size.is_empty() {
                output.size.parse::<u64>().map_err(|_| invalid())?;
            }
            Ok(((!output.hash.is_empty()).then_some(output.hash), None))
        }
        ClusterProtocol::Rest => {
            let output: RestAddedOutput = serde_json::from_slice(record).map_err(|_| invalid())?;
            let _ = (output.name, output.bytes, output.size);
            Ok((
                output.cid.filter(|cid| !cid.is_empty()),
                Some(output.allocations),
            ))
        }
    }
}
