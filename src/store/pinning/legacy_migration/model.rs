use crate::pinning::identity::{ProviderRouteSnapshot, RemoteResourceType};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub type NativeRow = BTreeMap<String, Option<String>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Maintenance {
    pub all_writers_stopped: bool,
    pub all_workers_drained: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Declaration {
    pub version: u32,
    pub database_scope: String,
    pub maintenance: Maintenance,
    pub ttl_seconds: u32,
    #[serde(default)]
    pub confirmation_file: Option<String>,
    #[serde(default)]
    pub rollback_receipt: Option<String>,
    pub namespaces: Vec<Namespace>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Namespace {
    pub source: String,
    pub target: String,
    pub backend: String,
    pub scope: String,
    pub resource_type: RemoteResourceType,
    pub binding_only: bool,
    pub target_route: ProviderRouteSnapshot,
    pub bindings: Vec<Binding>,
}

/// A primary key and optional /column/JSON-pointer identify one original field.
/// No wildcard, prefix, latest-epoch, or current-configuration inference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub table: String,
    pub key: BTreeMap<String, String>,
    pub provider: String,
    #[serde(default)]
    pub pointer: Option<String>,
    pub historical_route: Option<ProviderRouteSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableImage {
    pub table: String,
    pub columns: Vec<String>,
    pub primary_key: Vec<String>,
    pub rows: Vec<NativeRow>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Confirmation {
    pub version: u32,
    pub operation_id: String,
    #[serde(default)]
    pub rollback_of: Option<String>,
    pub database_scope: String,
    pub database_identity: String,
    pub schema: Vec<String>,
    pub db_clock: String,
    pub expires_at: String,
    pub declaration: Declaration,
    pub receipt_frontier: Vec<String>,
    pub before: Vec<TableImage>,
    pub after: Vec<TableImage>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub id: String,
    pub direction: String,
    pub reverses: Option<String>,
    pub db_clock: String,
    pub database_scope: String,
    pub evidence: String,
}

pub(super) fn field<'a>(row: &'a NativeRow, column: &str) -> Option<&'a str> {
    row.get(column).and_then(Option::as_deref)
}

pub(super) fn key(image: &TableImage, row: &NativeRow) -> BTreeMap<String, String> {
    image
        .primary_key
        .iter()
        .filter_map(|column| field(row, column).map(|value| (column.clone(), value.to_owned())))
        .collect()
}
