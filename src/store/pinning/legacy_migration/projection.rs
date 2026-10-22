//! Dedicated migration projection; never rewrites captured JSON or history.
use super::{
    Binding, Declaration, NativeRow, TableImage,
    model::{field, key},
    refused, snapshot,
};
use crate::{error::AppResult, pinning::identity::ProviderRouteSnapshot};
use std::collections::BTreeSet;

const MOVABLE: [&str; 5] = [
    "remote_pins",
    "remote_pin_ledger",
    "pin_provider_usage",
    "pin_lease_targets",
    "pin_jobs",
];

/// Operation-local proof of the complete R26 projection, never wire authority.
pub(in crate::store::pinning) struct R26Admission<'a> {
    management: &'a super::management_closure::Closure,
    declaration: &'a Declaration,
}

impl R26Admission<'_> {
    pub(in crate::store::pinning) fn routes(&self) -> &[ProviderRouteSnapshot] {
        self.management.routes()
    }

    pub(in crate::store::pinning) fn sources(&self) -> impl Iterator<Item = &str> {
        self.declaration
            .namespaces
            .iter()
            .map(|ns| ns.source.as_str())
    }

    pub(in crate::store::pinning) fn preserves(&self, route: &ProviderRouteSnapshot) -> bool {
        route.provider_id.starts_with("legacy:")
            && route.scope.starts_with("legacy:")
            && self.routes().contains(route)
            && !self
                .declaration
                .namespaces
                .iter()
                .any(|ns| ns.target_route == *route)
    }

    pub(in crate::store::pinning) fn references(&self, alias: &str) -> bool {
        self.sources().any(|source| source == alias)
            || self.routes().iter().any(|route| {
                route.provider_id == alias
                    || route.provider_id.strip_prefix("legacy:") == Some(alias)
            })
    }
}

pub(super) struct Projection<'a> {
    pub(super) after: Vec<TableImage>,
    pub(super) admission: R26Admission<'a>,
}
pub(super) const CAPTURES: [&str; 7] = [
    "pin_extension_decisions",
    "multipart_uploads",
    "import_jobs",
    "zip_batches",
    "zip_v2_executions",
    "zip_v2_mpu_intakes",
    "pin_lease_requirements",
];
pub(super) const CAPTURE_COLUMNS: [&str; 6] = [
    "snapshot",
    "pin_decision_json",
    "zip_intent_json",
    "captured_options",
    "captured_config",
    "contract",
];

pub(super) fn bindings(
    images: &[TableImage],
    ns: &super::Namespace,
    management: &super::management_closure::Closure,
) -> AppResult<Vec<Binding>> {
    let source = ns.source.as_str();
    let mut result = vec![];
    for image in images {
        for row in &image.rows {
            if field(row, "provider") == Some(source) {
                let raw = field(row, "route").or_else(|| {
                    (image.table == "pin_provider_routes")
                        .then(|| field(row, "snapshot"))
                        .flatten()
                });
                let historical_route = raw
                    .map(|v| {
                        serde_json::from_str::<ProviderRouteSnapshot>(v)
                            .map_err(|_| refused("historical route cannot be parsed"))
                    })
                    .transpose()?;
                result.push(Binding {
                    table: image.table.clone(),
                    key: key(image, row),
                    provider: source.into(),
                    pointer: None,
                    historical_route,
                });
            }
            if CAPTURES.contains(&image.table.as_str()) {
                for column in CAPTURE_COLUMNS {
                    if let Some(raw) = field(row, column) {
                        let value: serde_json::Value = serde_json::from_str(raw)
                            .map_err(|_| refused("captured provider keys cannot be parsed"))?;
                        let mut paths = vec![];
                        occurrences(&value, &format!("/{column}"), source, 0, &mut paths)?;
                        for path in paths {
                            result.push(Binding {
                                table: image.table.clone(),
                                key: key(image, row),
                                provider: source.into(),
                                pointer: Some(path),
                                historical_route: None,
                            });
                        }
                    }
                }
            }
        }
    }
    management.augment(images, ns, &mut result)?;
    Ok(result)
}

fn occurrences(
    value: &serde_json::Value,
    path: &str,
    source: &str,
    depth: usize,
    found: &mut Vec<String>,
) -> AppResult<()> {
    if depth > 32 {
        return Err(refused("captured JSON exceeds depth budget"));
    }
    match value {
        serde_json::Value::String(s) => {
            validate_provider_key(s)?;
            if s == source {
                found.push(path.into());
            }
        }
        serde_json::Value::Array(values) => {
            for (i, v) in values.iter().enumerate() {
                occurrences(v, &format!("{path}/{i}"), source, depth + 1, found)?;
            }
        }
        serde_json::Value::Object(values) => {
            for (k, v) in values {
                validate_provider_key(k)?;
                let escaped = k.replace('~', "~0").replace('/', "~1");
                if k == source {
                    found.push(format!("{path}/@key/{escaped}"));
                }
                occurrences(v, &format!("{path}/{escaped}"), source, depth + 1, found)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_provider_key(value: &str) -> AppResult<()> {
    if let Some(raw) = value.strip_prefix("domain:") {
        let tuple: (String, String, crate::pinning::identity::RemoteResourceType) =
            serde_json::from_str(raw)
                .map_err(|_| refused("captured domain key cannot be parsed"))?;
        if serde_json::to_string(&tuple).map_err(|_| refused("captured domain key invalid"))? != raw
        {
            return Err(refused("captured domain key is not canonical"));
        }
    }
    Ok(())
}

pub(super) fn project<'a>(
    before: &[TableImage],
    declaration: &'a Declaration,
    management: &'a super::management_closure::Closure,
) -> AppResult<Projection<'a>> {
    let mut after = before.to_vec();
    for ns in &declaration.namespaces {
        let expected = bindings(before, ns, management)?;
        if expected.is_empty()
            || expected.len() != ns.bindings.len()
            || expected
                .iter()
                .any(|b| ns.bindings.iter().filter(|c| *c == b).count() != 1)
        {
            return Err(refused(
                "missing, duplicate or conflicting per-class row binding",
            ));
        }
        let registered = before
            .iter()
            .find(|i| i.table == "pin_provider_routes")
            .and_then(|i| {
                i.rows
                    .iter()
                    .find(|r| field(r, "provider") == Some(&ns.target))
            })
            .ok_or_else(|| refused("target route must already be registered"))?;
        let route: ProviderRouteSnapshot = serde_json::from_str(
            field(registered, "snapshot").ok_or_else(|| refused("target route missing"))?,
        )
        .map_err(|_| refused("invalid target route"))?;
        if route != ns.target_route {
            return Err(refused("target route/revision differs from declaration"));
        }
        super::closure_identity::validate(before, ns)?;
        management.validate_identity(ns)?;
        if ns.binding_only
            && !before
                .iter()
                .filter(|i| i.table == "remote_pin_ledger")
                .flat_map(|i| &i.rows)
                .any(|r| {
                    field(r, "provider") == Some(&ns.source)
                        && field(r, "route").is_none()
                        && field(r, "effect") != Some("absent")
                })
        {
            return Err(refused(
                "binding-only declaration has no missing-route blocker",
            ));
        }
        validate_accounting(before, &ns.source)?;
        validate_accounting(before, &ns.target)?;
        for image in &mut after {
            if ns.source != ns.target
                && matches!(image.table.as_str(), "remote_pins" | "remote_pin_ledger")
            {
                let target_cids: BTreeSet<_> = image
                    .rows
                    .iter()
                    .filter(|r| field(r, "provider") == Some(&ns.target))
                    .filter_map(|r| field(r, "cid").map(str::to_owned))
                    .collect();
                if image.rows.iter().any(|r| {
                    field(r, "provider") == Some(&ns.source)
                        && field(r, "cid").is_some_and(|c| target_cids.contains(c))
                }) {
                    return Err(refused(
                        "shared CID lifetime collision cannot be proved equivalent",
                    ));
                }
            }
            if image.table == "pin_provider_usage" && ns.source != ns.target {
                merge_usage(image, &ns.source, &ns.target)?;
            } else if MOVABLE.contains(&image.table.as_str()) {
                for row in &mut image.rows {
                    if field(row, "provider") == Some(&ns.source) {
                        row.insert("provider".into(), Some(ns.target.clone()));
                        if image.table == "remote_pin_ledger" && field(row, "route").is_none() {
                            row.insert(
                                "route".into(),
                                Some(
                                    serde_json::to_string(&ns.target_route)
                                        .map_err(|_| refused("invalid declared route"))?,
                                ),
                            );
                        }
                    }
                }
            } else if image.table == "pin_provider_routes" && ns.source != ns.target {
                for row in &mut image.rows {
                    if field(row, "provider") == Some(&ns.source) {
                        let pg =
                            field(row, "retired").is_some_and(|v| matches!(v, "true" | "false"));
                        row.insert("retired".into(), Some(if pg { "true" } else { "1" }.into()));
                    }
                }
            }
            if !snapshot::distinct_keys(image) {
                return Err(refused("projected primary key conflict"));
            }
        }
    }
    snapshot::normalize(&mut after);
    if before == after {
        return Err(refused("no operational change"));
    }
    Ok(Projection {
        after,
        admission: R26Admission {
            management,
            declaration,
        },
    })
}

fn number(row: &NativeRow, column: &str) -> AppResult<i64> {
    field(row, column)
        .and_then(|v| v.parse().ok())
        .filter(|v| *v >= 0)
        .ok_or_else(|| refused("invalid native accounting"))
}

fn validate_accounting(images: &[TableImage], provider: &str) -> AppResult<()> {
    let remotes = images
        .iter()
        .filter(|i| i.table == "remote_pins")
        .flat_map(|i| &i.rows)
        .filter(|r| field(r, "provider") == Some(provider) && field(r, "status") != Some("absent"));
    let mut bytes: i64 = 0;
    let mut pins: i64 = 0;
    for row in remotes {
        bytes = bytes
            .checked_add(number(row, "cid_size")?)
            .ok_or_else(|| refused("capacity overflow"))?;
        pins = pins
            .checked_add(1)
            .ok_or_else(|| refused("capacity overflow"))?;
    }
    let usage = images
        .iter()
        .filter(|i| i.table == "pin_provider_usage")
        .flat_map(|i| &i.rows)
        .find(|r| field(r, "provider") == Some(provider));
    match usage {
        Some(row)
            if number(row, "reserved_bytes")? >= bytes && number(row, "reserved_pins")? >= pins =>
        {
            Ok(())
        }
        None if pins == 0 => Ok(()),
        _ => Err(refused(
            "capacity is under-accounted; migration is not repair",
        )),
    }
}

fn merge_usage(image: &mut TableImage, source: &str, target: &str) -> AppResult<()> {
    let Some(source_index) = image
        .rows
        .iter()
        .position(|r| field(r, "provider") == Some(source))
    else {
        return Ok(());
    };
    let mut moved = image.rows.remove(source_index);
    moved.insert("provider".into(), Some(target.into()));
    if let Some(current) = image
        .rows
        .iter_mut()
        .find(|r| field(r, "provider") == Some(target))
    {
        for column in ["reserved_bytes", "reserved_pins"] {
            let total = number(current, column)?
                .checked_add(number(&moved, column)?)
                .ok_or_else(|| refused("merged capacity overflow"))?;
            current.insert(column.into(), Some(total.to_string()));
        }
        // Account-level observations cannot be summed into invented physical facts.
        for column in ["observed_bytes", "observed_pins", "observed_at"] {
            match (field(current, column), field(&moved, column)) {
                (Some(a), Some(b)) if a != b => {
                    return Err(refused("conflicting account observations"));
                }
                (None, Some(_)) => {
                    current.insert(column.into(), moved.get(column).cloned().flatten());
                }
                _ => {}
            }
        }
    } else {
        image.rows.push(moved);
    }
    Ok(())
}
