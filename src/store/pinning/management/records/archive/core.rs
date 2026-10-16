use super::*;
use crate::pinning::management::*;

/// A stable immutable section slot, shared by the review and its ACK handle.
pub struct SharedValue<T>(std::sync::Arc<T>);
impl<T> Clone for SharedValue<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T> std::ops::Deref for SharedValue<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
impl<T: PartialEq> PartialEq for SharedValue<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl<T: Eq> Eq for SharedValue<T> {}
impl<T: Serialize> Serialize for SharedValue<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        self.0.as_ref().serialize(s)
    }
}
impl<'de, T: Deserialize<'de>> Deserialize<'de> for SharedValue<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Ok(Self(std::sync::Arc::new(T::deserialize(d)?)))
    }
}
impl<'a, T> IntoIterator for &'a SharedValue<Vec<T>> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Serialize, Deserialize)]
pub(in crate::store::pinning::management::records) struct Core(
    pub SharedValue<PlanRequest>,
    pub SharedValue<Vec<PlanEntry>>,
    pub Summary,
    pub SharedValue<SnapshotRest>,
    pub DateTime<Utc>,
    pub DateTime<Utc>,
    pub Vec<MigrationPoint>,
    pub String,
);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRest {
    pub observed_at: i64,
    pub database_now: Option<DateTime<Utc>>,
    pub configuration: Option<ConfigurationBinding>,
    pub objects: Vec<ObjectSnapshot>,
    pub routes: Vec<RouteSnapshot>,
    pub targets: Vec<TargetSnapshot>,
    pub remotes: Vec<RemoteSnapshot>,
    pub historical_remotes: Vec<RemoteSnapshot>,
    pub unknown_resources: Vec<NativeKey>,
    pub ledger_domains: Vec<LedgerDomainSnapshot>,
    pub inventories: Vec<ProviderInventory>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub root_references: Vec<RootReferenceSnapshot>,
    pub applied: Vec<AppliedItem>,
}

pub(super) struct SnapshotRef<'a>(pub &'a Snapshot);
impl Serialize for SnapshotRef<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let snapshot = self.0;
        let mut state = serializer.serialize_struct(
            "SnapshotRest",
            12 + usize::from(!snapshot.root_references.is_empty()),
        )?;
        state.serialize_field("observed_at", &snapshot.observed_at)?;
        state.serialize_field("database_now", &snapshot.database_now)?;
        state.serialize_field("configuration", &snapshot.configuration)?;
        state.serialize_field("objects", &snapshot.objects)?;
        state.serialize_field("routes", &snapshot.routes)?;
        state.serialize_field("targets", &snapshot.targets)?;
        state.serialize_field("remotes", &snapshot.remotes)?;
        state.serialize_field("historical_remotes", &snapshot.historical_remotes)?;
        state.serialize_field("unknown_resources", &snapshot.unknown_resources)?;
        state.serialize_field("ledger_domains", &snapshot.ledger_domains)?;
        state.serialize_field("inventories", &snapshot.inventories)?;
        if !snapshot.root_references.is_empty() {
            state.serialize_field("root_references", &snapshot.root_references)?;
        }
        state.serialize_field("applied", &snapshot.applied)?;
        state.end()
    }
}

impl SnapshotRest {
    pub(super) fn view<'a>(&'a self, rows: FrozenRows<'a>) -> SnapshotView<'a> {
        SnapshotView {
            work: None,
            observed_at: self.observed_at,
            database_now: self.database_now,
            configuration: &self.configuration,
            native_frame: None,
            rows: Some(rows),
            objects: &self.objects,
            routes: &self.routes,
            targets: &self.targets,
            remotes: &self.remotes,
            historical_remotes: &self.historical_remotes,
            unknown_resources: &self.unknown_resources,
            ledger_domains: &self.ledger_domains,
            inventories: &self.inventories,
            root_references: &self.root_references,
            applied: &self.applied,
        }
    }
}
