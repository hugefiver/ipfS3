//! Fixed v1.1.6 wire types: Proxy transforms AddedOutput; REST does not.
use super::{ClusterFailureCode as Code, ClusterProtocol, ClusterWriteFailure as Failure};
use cid::Cid;
use serde::{Deserialize, Deserializer, de::DeserializeOwned};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProxyAddedOutput {
    #[serde(rename = "Name")]
    _name: String,
    #[serde(default, rename = "Hash")]
    hash: String,
    #[serde(default, rename = "Bytes")]
    _bytes: u64,
    #[serde(default, rename = "Size")]
    size: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestAddedOutput {
    #[serde(rename = "name")]
    _name: String,
    // CidUndef is JSON null; still require the official field to be present.
    #[serde(deserialize_with = "optional_cid")]
    cid: Option<String>,
    #[serde(default, rename = "bytes")]
    _bytes: u64,
    #[serde(default, rename = "size")]
    _size: u64,
    #[serde(default, rename = "allocations")]
    _allocations: Vec<String>,
}

fn optional_cid<'de, D: Deserializer<'de>>(de: D) -> Result<Option<String>, D::Error> {
    Option::deserialize(de)
}

fn records<T: DeserializeOwned>(body: &[u8], streamed: bool) -> Result<Vec<T>, Failure> {
    if streamed {
        body.split(|b| *b == b'\n')
            .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
            .map(serde_json::from_slice)
            .collect()
    } else {
        serde_json::from_slice(body)
    }
    .map_err(|_| Failure::new(Code::Protocol))
}

pub(super) fn parse(
    protocol: ClusterProtocol,
    body: &[u8],
    streamed: bool,
) -> Result<Cid, Failure> {
    let invalid = || Failure::new(Code::Protocol);
    let mut root = None;
    let mut accept = |value: &str| {
        if root.is_some() {
            return Err(invalid());
        }
        root = Some(value.parse().map_err(|_| invalid())?);
        Ok(())
    };
    match protocol {
        ClusterProtocol::Proxy => {
            for output in records::<ProxyAddedOutput>(body, streamed)? {
                if !output.size.is_empty() {
                    output.size.parse::<u64>().map_err(|_| invalid())?;
                }
                if !output.hash.is_empty() {
                    accept(&output.hash)?;
                }
            }
        }
        ClusterProtocol::Rest => {
            for output in records::<RestAddedOutput>(body, streamed)? {
                if let Some(cid) = output.cid.filter(|cid| !cid.is_empty()) {
                    accept(&cid)?;
                }
            }
        }
    }
    root.ok_or_else(invalid)
}
