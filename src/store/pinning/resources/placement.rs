use std::collections::{BTreeMap, BTreeSet};

use sea_orm::{ConnectionTrait, DatabaseTransaction, Value};

use crate::pinning::config::ProviderLimitMap;

use super::{AppResult, OCCUPIED, stmt};

// Resource reads bind two chunks at once and must fit SQLite's 999-bind limit.
const IN_CHUNK: usize = 450;

#[derive(Default)]
pub(super) struct Placement {
    providers: BTreeMap<String, ProviderPlacement>,
}

struct ProviderPlacement {
    pins: i64,
    bytes: i128,
    max_bytes: i64,
    resources: BTreeMap<String, Resource>,
}

struct Resource {
    status: String,
    size: i64,
    has_handle: bool,
    reusable: bool,
    refs: BTreeMap<String, BTreeSet<[String; 3]>>,
}

impl Resource {
    fn counts_towards_usage(&self, max_bytes: i64) -> bool {
        self.has_handle
            || matches!(self.status.as_str(), "accepted" | "pending" | "unknown")
            || (self.status == "waiting" && self.size <= max_bytes)
    }
}

impl Placement {
    pub(super) async fn load(
        tx: &DatabaseTransaction,
        cids: &BTreeSet<String>,
        owners: &BTreeSet<String>,
        providers: &BTreeSet<String>,
        limits: &ProviderLimitMap,
    ) -> AppResult<Self> {
        let mut placement = Self::default();
        if cids.is_empty() || providers.is_empty() {
            return Ok(placement);
        }

        // Waiting demand influences placement; only claims actually reserve capacity.
        for provider in providers {
            let max_bytes = limits[provider].max_bytes;
            let row = tx
                .query_one(stmt(
                    tx,
                    &format!("SELECT COUNT(*) AS pins, CAST(COALESCE(SUM(logical_size),0) AS BIGINT) AS bytes
                     FROM pin_resources WHERE provider=$1 AND status!='cleaned' AND (
                         {OCCUPIED} OR (status='waiting' AND logical_size<=$2))"),
                    vec![provider.as_str().into(), max_bytes.into()],
                ))
                .await?
                .unwrap();
            placement.providers.insert(
                provider.clone(),
                ProviderPlacement {
                    pins: row.try_get("", "pins")?,
                    bytes: i128::from(row.try_get::<i64>("", "bytes")?),
                    max_bytes,
                    resources: BTreeMap::new(),
                },
            );
        }

        let providers = providers.iter().map(String::as_str).collect::<Vec<_>>();
        let cids = cids.iter().map(String::as_str).collect::<Vec<_>>();
        for provider_chunk in providers.chunks(IN_CHUNK) {
            for cid_chunk in cids.chunks(IN_CHUNK) {
                let mut values = Vec::new();
                let provider_slots = bind_in(&mut values, provider_chunk);
                let cid_slots = bind_in(&mut values, cid_chunk);
                let sql = format!(
                    "SELECT provider,cid,status,logical_size,handle IS NOT NULL AS has_handle,
                         (status='accepted' OR (status='pending' AND (action='submit' OR (action='delete' AND attempt=0)))) AS reusable
                     FROM pin_resources WHERE provider IN ({provider_slots}) AND cid IN ({cid_slots})"
                );
                for row in tx.query_all(stmt(tx, &sql, values)).await? {
                    let provider: String = row.try_get("", "provider")?;
                    placement
                        .providers
                        .get_mut(&provider)
                        .unwrap()
                        .resources
                        .insert(
                            row.try_get("", "cid")?,
                            Resource {
                                status: row.try_get("", "status")?,
                                size: row.try_get("", "logical_size")?,
                                has_handle: row.try_get("", "has_handle")?,
                                reusable: row.try_get("", "reusable")?,
                                refs: BTreeMap::new(),
                            },
                        );
                }
            }
        }

        let owners = owners.iter().map(String::as_str).collect::<Vec<_>>();
        for chunk in owners.chunks(IN_CHUNK) {
            let mut values = Vec::new();
            let slots = bind_in(&mut values, chunk);
            let sql = format!(
                "SELECT provider,cid,owner_object_id,source,policy_id,content_mode
                 FROM pin_refs WHERE owner_object_id IN ({slots})"
            );
            for row in tx.query_all(stmt(tx, &sql, values)).await? {
                let provider: String = row.try_get("", "provider")?;
                let cid: String = row.try_get("", "cid")?;
                if let Some(resource) = placement
                    .providers
                    .get_mut(&provider)
                    .and_then(|state| state.resources.get_mut(&cid))
                {
                    resource
                        .refs
                        .entry(row.try_get("", "owner_object_id")?)
                        .or_default()
                        .insert([
                            row.try_get("", "source")?,
                            row.try_get("", "policy_id")?,
                            row.try_get("", "content_mode")?,
                        ]);
                }
            }
        }
        Ok(placement)
    }

    pub(super) fn can_serve(
        &self,
        provider: &str,
        cid: &str,
        size: i64,
        limit: &crate::pinning::config::ProviderLimits,
    ) -> bool {
        (limit.max_pins > 0 && size <= limit.max_bytes)
            || self.providers[provider]
                .resources
                .get(cid)
                .is_some_and(|r| r.reusable)
    }

    pub(super) fn select<'a>(
        &self,
        owner: &str,
        cid: &str,
        source: &str,
        policy: &str,
        content: &str,
        size: i64,
        candidates: &[&'a str],
        limits: &ProviderLimitMap,
    ) -> &'a str {
        candidates
            .iter()
            .copied()
            .min_by_key(|&provider| {
                let state = &self.providers[provider];
                let limit = &limits[provider];
                let resource = state.resources.get(cid);
                let assigned = resource
                    .and_then(|resource| resource.refs.get(owner))
                    .is_some_and(|refs| {
                        refs.iter()
                            .any(|[s, p, c]| s == source && p == policy && c == content)
                    });
                let rank = if assigned {
                    0
                } else if !self.can_serve(provider, cid, size, limit) {
                    6
                } else {
                    // Reuse the aggregate for every CID without charging the selected CID twice.
                    let counted = resource.filter(|r| r.counts_towards_usage(state.max_bytes));
                    let pins = state.pins - i64::from(counted.is_some());
                    let bytes = state.bytes - i128::from(counted.map_or(0, |r| r.size));
                    let fit = pins < limit.max_pins
                        && bytes + i128::from(size) <= i128::from(limit.max_bytes);
                    match resource.map(|r| r.status.as_str()) {
                        Some("accepted" | "pending") => 1,
                        Some("waiting") if fit => 2,
                        None | Some("cleaned") if fit => 3,
                        Some("unknown" | "failed") => 5,
                        _ => 4,
                    }
                };
                (rank, limit.priority, provider)
            })
            .unwrap()
    }

    pub(super) fn record(
        &mut self,
        provider: &str,
        cid: &str,
        size: i64,
        owner: &str,
        source: &str,
        policy: &str,
        content: &str,
    ) {
        let Some(state) = self.providers.get_mut(provider) else {
            return;
        };
        let resource = state
            .resources
            .entry(cid.to_owned())
            .or_insert_with(|| Resource {
                status: "cleaned".into(),
                size,
                has_handle: false,
                reusable: false,
                refs: BTreeMap::new(),
            });
        if resource.status == "cleaned" {
            resource.status = "waiting".into();
            resource.size = size;
            if resource.counts_towards_usage(state.max_bytes) {
                state.pins += 1;
                state.bytes += i128::from(size);
            }
        }
        resource.refs.entry(owner.to_owned()).or_default().insert([
            source.to_owned(),
            policy.to_owned(),
            content.to_owned(),
        ]);
    }
}

fn bind_in(values: &mut Vec<Value>, keys: &[&str]) -> String {
    let first = values.len() + 1;
    values.extend(keys.iter().map(|&key| Value::from(key)));
    (first..=values.len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(",")
}
