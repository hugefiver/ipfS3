use super::*;
use sea_orm::{ColumnTrait, QueryFilter, QuerySelect};

pub(super) const PAGE_SIZE: usize = 256;

pub(super) struct Combination<'a> {
    pub object_cid: &'a str,
    pub slot: &'a str,
    pub allocation: &'a str,
    pub query_cid: String,
    pub cluster: bool,
}

pub(super) fn combinations<'a>(
    birth: &'a BirthContract,
    resolved: &'a [(String, String)],
) -> impl Iterator<Item = Combination<'a>> {
    birth.content_cids.iter().flat_map(move |object_cid| {
        resolved.iter().map(move |(slot, allocation)| {
            use crate::pinning::identity::RemoteResourceType::{ClusterPin, RpcPin};
            let resource = birth
                .candidates
                .get(slot)
                .and_then(Option::as_ref)
                .map(ProviderRouteSnapshot::resource_type);
            let query_cid = if matches!(resource, Some(ClusterPin | RpcPin)) {
                canonical_resource_cid(object_cid).unwrap_or_else(|_| object_cid.clone())
            } else {
                object_cid.clone()
            };
            Combination {
                object_cid,
                slot,
                allocation,
                query_cid,
                cluster: resource == Some(ClusterPin),
            }
        })
    })
}

#[derive(Default)]
struct Allocation {
    statuses: BTreeMap<String, String>,
    attached: BTreeSet<String>,
}

pub(super) struct Page(BTreeMap<String, Allocation>);

impl Page {
    pub(super) async fn load<C: ConnectionTrait>(
        db: &C,
        lease_id: &str,
        combinations: &[Combination<'_>],
    ) -> AppResult<Self> {
        let mut groups: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for combination in combinations {
            groups
                .entry(combination.allocation)
                .or_default()
                .insert(&combination.query_cid);
        }
        let mut allocations = BTreeMap::new();
        for (allocation, cids) in groups {
            // The remote PK and target unique key bound each projection by the
            // page's distinct query CIDs, including when raw CIDs are aliases.
            let statuses = remote_pin::Entity::find()
                .select_only()
                .column(remote_pin::Column::Cid)
                .column(remote_pin::Column::Status)
                .filter(remote_pin::Column::Provider.eq(allocation))
                .filter(remote_pin::Column::Cid.is_in(cids.iter().copied()))
                .into_tuple::<(String, String)>()
                .all(db)
                .await?
                .into_iter()
                .collect();
            let attached = pin_lease_target::Entity::find()
                .select_only()
                .column(pin_lease_target::Column::Cid)
                .filter(pin_lease_target::Column::LeaseId.eq(lease_id))
                .filter(pin_lease_target::Column::Provider.eq(allocation))
                .filter(pin_lease_target::Column::Cid.is_in(cids))
                .filter(pin_lease_target::Column::State.eq("pinned"))
                .into_tuple::<String>()
                .all(db)
                .await?
                .into_iter()
                .collect();
            allocations.insert(allocation.to_owned(), Allocation { statuses, attached });
        }
        Ok(Self(allocations))
    }

    pub(super) fn eligible(&self, combination: &Combination<'_>) -> bool {
        self.0
            .get(combination.allocation)
            .is_some_and(|allocation| {
                allocation.attached.contains(&combination.query_cid)
                    && allocation
                        .statuses
                        .get(&combination.query_cid)
                        .is_some_and(|status| {
                            status == "pinned" || combination.cluster && status == "pinning"
                        })
            })
    }
}
