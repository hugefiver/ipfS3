//! Minimal official v1.1.6 api/types.go read DTOs. Never export provider free errors.
use chrono::{DateTime, Utc};
use serde::{
    Deserialize, Deserializer,
    de::{Error, MapAccess, Visitor},
};
use std::{collections::BTreeMap, fmt};

pub(super) fn null_vec<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    Ok(Option::<Vec<String>>::deserialize(d)?.unwrap_or_default())
}

#[derive(Deserialize)]
pub(super) struct Version {
    pub version: String,
}

#[derive(Deserialize)]
pub(super) struct PinAllocation {
    pub cid: String,
    #[serde(deserialize_with = "null_vec")]
    pub allocations: Vec<String>,
    pub replication_factor_min: i32,
    pub replication_factor_max: i32,
    pub timestamp: DateTime<Utc>,
}

#[derive(Deserialize)]
pub(super) struct GlobalPinInfo {
    pub cid: String,
    #[serde(deserialize_with = "null_vec")]
    pub allocations: Vec<String>,
    pub created: DateTime<Utc>,
    #[serde(deserialize_with = "unique_peer_map")]
    pub peer_map: BTreeMap<String, PeerPinInfo>,
}

#[derive(Deserialize)]
pub(super) struct PeerPinInfo {
    pub status: String,
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "error", deserialize_with = "has_error")]
    pub has_error: bool,
}

fn has_error<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
    String::deserialize(d).map(|v| !v.is_empty())
}

fn unique_peer_map<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<String, PeerPinInfo>, D::Error> {
    struct Unique;
    impl<'de> Visitor<'de> for Unique {
        type Value = BTreeMap<String, PeerPinInfo>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("unique peer map")
        }
        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
            let mut peers = BTreeMap::new();
            while let Some((id, info)) = map.next_entry::<String, PeerPinInfo>()? {
                if peers.insert(id, info).is_some() {
                    return Err(M::Error::custom("duplicate peer"));
                }
            }
            Ok(peers)
        }
    }
    d.deserialize_map(Unique)
}
