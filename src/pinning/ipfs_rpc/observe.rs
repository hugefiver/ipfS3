use super::{IpfsRpcProvider, RpcProfile, cid, error::protocol};
use crate::pinning::provider::{ProviderError, RemotePin};
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcPinKind {
    Recursive,
    Direct,
    Indirect,
}

/// This is pin/ls evidence only. Recursive is NOT a complete Kubo residency
/// receipt; `observe` also checks a local/offline DAG and stable node identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcPinObservation {
    Absent,
    Present(RpcPinKind),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PinList {
    #[serde(rename = "Keys")]
    keys: BTreeMap<String, PinEntry>,
}

#[derive(Deserialize)]
struct PinEntry {
    #[serde(rename = "Type")]
    kind: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcFailure {
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "Code")]
    code: u64,
    #[serde(rename = "Type")]
    kind: String,
}

#[derive(Deserialize)]
struct LocalStat {
    #[serde(rename = "Hash")]
    hash: String,
    #[serde(rename = "WithLocality")]
    with_locality: bool,
    #[serde(rename = "Local")]
    local: bool,
}

#[derive(Deserialize)]
struct NodeIdentity {
    #[serde(rename = "ID")]
    id: String,
}

impl IpfsRpcProvider {
    pub async fn observe_pin(&self, requested: &str) -> Result<RpcPinObservation, ProviderError> {
        let requested = cid::canonical(requested)?;
        // Filebase's official surface lists arg/stream/names, NOT type. Require
        // a typed recursive response rather than assuming default pin kind.
        let filebase = [
            ("arg", requested.as_str()),
            ("stream", "false"),
            ("names", "false"),
        ];
        let kubo = [
            ("arg", requested.as_str()),
            ("stream", "false"),
            ("names", "false"),
            ("type", "all"),
            ("offline", "true"),
        ];
        let query = if self.profile == RpcProfile::Filebase {
            &filebase[..]
        } else {
            &kubo[..]
        };
        let response = self.target.control("pin/ls", query, false).await?;
        // Kubo 0.43's arg-not-pinned result is a JSON RPC error, not a 404.
        // Only this exact, fully read, CID-bound error proves absence.
        if self.profile == RpcProfile::Kubo
            && response.status.as_u16() == 500
            && let Ok(failure) = response.json::<RpcFailure>()
        {
            let absent_cid = failure
                .message
                .strip_prefix("path '")
                .and_then(|v| v.strip_suffix("' is not pinned"));
            if failure.code == 0
                && failure.kind == "error"
                && absent_cid
                    .is_some_and(|cid| super::cid::equivalent(cid, &requested).unwrap_or(false))
            {
                return Ok(RpcPinObservation::Absent);
            }
        }
        response.check(false)?;
        let list: PinList = response.json()?;
        if list.keys.is_empty() {
            return Ok(RpcPinObservation::Absent);
        }
        if list.keys.len() != 1 {
            return Err(protocol("RPC pin query did not return one exact CID"));
        }
        let (returned_cid, entry) = list.keys.into_iter().next().expect("length checked");
        if !cid::equivalent(&requested, &returned_cid).unwrap_or(false) {
            return Err(protocol("RPC pin query returned a different CID"));
        }
        let kind = match entry.kind.as_str() {
            "recursive" => RpcPinKind::Recursive,
            "direct" => RpcPinKind::Direct,
            "indirect" => RpcPinKind::Indirect,
            value
                if value
                    .strip_prefix("indirect through ")
                    .is_some_and(|cid| super::cid::parse(cid).is_ok()) =>
            {
                RpcPinKind::Indirect
            }
            _ => return Err(protocol("RPC pin query returned an unknown pin type")),
        };
        Ok(RpcPinObservation::Present(kind))
    }

    pub(super) async fn find_verified(
        &self,
        requested: &str,
    ) -> Result<Vec<RemotePin>, ProviderError> {
        match self.observe_pin(requested).await? {
            RpcPinObservation::Absent => return Ok(Vec::new()),
            RpcPinObservation::Present(RpcPinKind::Direct | RpcPinKind::Indirect) => {
                return Err(protocol(
                    "RPC pin exists but is not recursive; complete observation unavailable",
                ));
            }
            RpcPinObservation::Present(RpcPinKind::Recursive) => {}
        }
        if self.profile == RpcProfile::Kubo {
            let identity = self.node_identity().await?;
            self.verify_local(requested).await?;
            if self.observe_pin(requested).await?
                != RpcPinObservation::Present(RpcPinKind::Recursive)
                || self.node_identity().await? != identity
            {
                return Err(protocol(
                    "RPC recursive pin or node identity changed during verification",
                ));
            }
        }
        Ok(vec![self.remote_pin(requested)?])
    }

    async fn node_identity(&self) -> Result<String, ProviderError> {
        let response = self
            .target
            .control("id", &[("peerid-base", "b58mh")], false)
            .await?;
        response.check(false)?;
        let node: NodeIdentity = response.json()?;
        if node.id.is_empty() || node.id.len() > 512 {
            return Err(protocol("RPC node identity unavailable"));
        }
        Ok(node.id)
    }

    async fn verify_local(&self, requested: &str) -> Result<(), ProviderError> {
        let requested = cid::canonical(requested)?;
        let path = format!("/ipfs/{requested}");
        let response = self
            .target
            .control(
                "files/stat",
                &[("arg", &path), ("with-local", "true"), ("offline", "true")],
                false,
            )
            .await?;
        response.check(false)?;
        let stat: LocalStat = response.json()?;
        if !stat.with_locality
            || !stat.local
            || !cid::equivalent(&requested, &stat.hash).unwrap_or(false)
        {
            return Err(protocol(
                "RPC recursive pin lacks a complete offline local DAG",
            ));
        }
        Ok(())
    }
}
