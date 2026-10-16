//! Async authenticated record collection followed by a private pure adapter.
use super::{Binding, Declaration, Namespace, TableImage, model::field, refused};
use crate::{
    error::AppResult,
    pinning::identity::ProviderRouteSnapshot,
    store::pinning::management::{
        actions::provider_points::{self, Domain, NamespaceChange, VerifiedRecord},
        native, records, schema,
    },
};
use sea_orm::ConnectionTrait;

struct NamespaceClosure {
    source: String,
    records: Vec<VerifiedRecord>,
    bindings: Vec<Binding>,
    originals: Vec<TableImage>,
}

/// No wire decoder or public constructor can supply authenticated record views.
pub(super) struct Closure {
    namespaces: Vec<NamespaceClosure>,
    routes: Vec<ProviderRouteSnapshot>,
}

pub(super) fn enabled(versions: &[String]) -> bool {
    versions.iter().any(|version| version == schema::VERSION)
}

pub(super) async fn collect<C: ConnectionTrait>(
    db: &C,
    images: &[TableImage],
    declaration: &Declaration,
    versions: &[String],
) -> AppResult<Closure> {
    let mut closure = Closure {
        namespaces: vec![],
        routes: vec![],
    };
    if !enabled(versions) {
        return Ok(closure);
    }
    let work = records::SharedDecodeBudget::default();
    let context = records::DecodeWork::shared(&work);
    let registry = native::registry();
    for ns in &declaration.namespaces {
        let change = NamespaceChange {
            source: Domain {
                allocation: ns.source.clone(),
                backend: ns.backend.clone(),
                scope: ns.scope.clone(),
                resource_type: ns.resource_type,
            },
            destination: Domain {
                allocation: ns.target.clone(),
                backend: ns.backend.clone(),
                scope: ns.scope.clone(),
                resource_type: ns.resource_type,
            },
            destination_route: ns.target_route.clone(),
        };
        let mut captured = NamespaceClosure {
            source: ns.source.clone(),
            records: vec![],
            bindings: vec![],
            originals: vec![],
        };
        add_route(&mut closure.routes, &ns.target_route);
        for table in images {
            for row in &table.rows {
                if field(row, "provider")
                    .is_some_and(|provider| provider == ns.source || provider == ns.target)
                {
                    let original = field(row, "route").or_else(|| {
                        (table.table == "pin_provider_routes")
                            .then(|| field(row, "snapshot"))
                            .flatten()
                    });
                    if let Some(original) = original {
                        let route = serde_json::from_str(original)
                            .map_err(|_| refused("original admission route cannot be parsed"))?;
                        add_route(&mut closure.routes, &route);
                    }
                }
            }
            if !matches!(table.table.as_str(), schema::RECORDS | schema::RETIREMENTS) {
                continue;
            }
            for row in &table.rows {
                // The schema identity has no provider points and creates no
                // VerifiedRecord or authority, including diagnostic memory DBs.
                if table.table == schema::RECORDS && field(row, "kind") == Some("database_scope") {
                    if !matches!(
                        records::inspect_original_row(&table.table, row)
                            .map_err(|_| refused("invalid raw management identity"))?,
                        records::RecordPayload::Identity
                    ) {
                        return Err(refused("unexpected management identity payload"));
                    }
                    continue;
                }
                context
                    .copy(&(&table.table, &table.primary_key))
                    .map_err(|_| refused("management work budget exceeded"))?;
                for column in &table.primary_key {
                    context
                        .copy(&field(row, column))
                        .map_err(|_| refused("management work budget exceeded"))?;
                }
                let record_key = native::NativeKey {
                    table: table.table.clone(),
                    values: table
                        .primary_key
                        .iter()
                        .map(|column| {
                            field(row, column)
                                .map(str::to_owned)
                                .ok_or_else(|| refused("management record key missing"))
                        })
                        .collect::<AppResult<_>>()?,
                };
                let view =
                    provider_points::inspect_with_work(db, &record_key, row, &change, work.clone())
                        .await
                        .map_err(|_| {
                            refused("management original record cannot be authenticated")
                        })?;
                captured
                    .bindings
                    .extend(super::management_points::bindings(&view, table, row, ns)?);
                for point in view.points() {
                    if let Some(route) = &point.original_route {
                        add_route(&mut closure.routes, route);
                    }
                }
                for (key, original) in view.native_origins() {
                    let shape = registry
                        .iter()
                        .find(|shape| shape.table == key.table)
                        .ok_or_else(|| refused("unknown typed management origin table"))?;
                    if original.keys().ne(shape.columns.iter())
                        || shape
                            .primary_key
                            .iter()
                            .map(|column| field(original, column))
                            .ne(key.values.iter().map(|value| Some(value.as_str())))
                    {
                        return Err(refused("typed management origin shape differs"));
                    }
                    context
                        .copy(&(&shape.table, &shape.columns, &shape.primary_key, original))
                        .map_err(|_| refused("management work budget exceeded"))?;
                    captured.originals.push(TableImage {
                        table: shape.table.clone(),
                        columns: shape.columns.to_vec(),
                        primary_key: shape.primary_key.to_vec(),
                        rows: vec![original.clone()],
                    });
                }
                captured.records.push(view);
            }
        }
        if captured.bindings.len() > super::MAX_ROWS {
            return Err(refused("typed management bindings exceed R26 budget"));
        }
        closure.namespaces.push(captured);
    }
    Ok(closure)
}

fn add_route(routes: &mut Vec<ProviderRouteSnapshot>, route: &ProviderRouteSnapshot) {
    // Admission is domain-only. Prefer the already collected complete current
    // registry/declaration route; old credential revisions remain in bindings.
    if !routes.iter().any(|existing| {
        existing.backend == route.backend
            && existing.scope == route.scope
            && existing.resource_type() == route.resource_type()
    }) {
        routes.push(route.clone());
    }
}

impl Closure {
    pub(super) fn routes(&self) -> &[ProviderRouteSnapshot] {
        &self.routes
    }

    pub(super) fn augment(
        &self,
        images: &[TableImage],
        ns: &Namespace,
        bindings: &mut Vec<Binding>,
    ) -> AppResult<()> {
        if let Some(captured) = self
            .namespaces
            .iter()
            .find(|captured| captured.source == ns.source)
        {
            super::management_points::enrich(bindings, images, &captured.records, ns)?;
            bindings.extend(captured.bindings.iter().cloned());
        }
        Ok(())
    }

    pub(super) fn validate_identity(&self, ns: &Namespace) -> AppResult<()> {
        if let Some(captured) = self
            .namespaces
            .iter()
            .find(|captured| captured.source == ns.source)
        {
            super::closure_identity::validate(&captured.originals, ns)?;
            for view in &captured.records {
                for point in view.points() {
                    if let Some(route) = &point.original_route {
                        super::closure_identity::typed_route(ns, route)?;
                    }
                }
            }
        }
        Ok(())
    }
}
