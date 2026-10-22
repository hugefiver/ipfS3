//! R26 read-only typed service. Original record PK/full row and declared domains
//! are mandatory. No opaque string search or namespace-wide execution grant.
use super::*;
use crate::pinning::{
    identity::{ProviderRouteSnapshot, RemoteResourceType},
    management::{Capture, RouteSnapshot},
};
use sea_orm::ConnectionTrait;

#[derive(Clone, PartialEq, Eq)]
pub struct Domain {
    pub allocation: String,
    pub backend: String,
    pub scope: String,
    pub resource_type: RemoteResourceType,
}
pub struct NamespaceChange {
    pub source: Domain,
    pub destination: Domain,
    pub destination_route: ProviderRouteSnapshot,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RecordClass {
    Identity,
    Evidence,
    Staged,
    Applied,
    Retirement,
}
#[derive(Clone, PartialEq, Eq)]
pub enum Locator {
    SnapshotRoute {
        container: &'static str,
        item: String,
        field: String,
    },
    NativeColumn {
        container: &'static str,
        key: native::NativeKey,
        column: &'static str,
    },
    BirthCandidate {
        container: &'static str,
        key: native::NativeKey,
        slot: String,
    },
    Job {
        job_id: String,
    },
    Migration {
        contract: native::NativeKey,
        slot: String,
        destination: bool,
    },
}
#[derive(Clone, PartialEq, Eq)]
pub enum Evidence {
    Historical,
    Staged,
    ExecutableJob(records::JobDirective),
    ExecutableSlot,
    Unresolved,
}
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderPoint {
    pub locator: Locator,
    pub allocation: String,
    pub original_route: Option<ProviderRouteSnapshot>,
    pub evidence: Evidence,
    pub lease_id: Option<String>,
    pub lease_generation: Option<i64>,
    pub cid: Option<String>,
    pub resource_key: Option<native::NativeKey>,
    pub epoch: Option<i64>,
    pub resource_id: Option<String>,
    pub job_authority: Option<records::JobAuthority>,
    pub migration_point: Option<records::MigrationPoint>,
}
/// No Deserialize/public constructor: caller data cannot create a verified view.
pub struct VerifiedRecord {
    key: native::NativeKey,
    scope: String,
    identity: String,
    class: RecordClass,
    points: Vec<ProviderPoint>,
    origins: Vec<(native::NativeKey, native::NativeRow)>,
}
impl VerifiedRecord {
    pub fn record_key(&self) -> &native::NativeKey {
        &self.key
    }
    pub fn database_scope(&self) -> &str {
        &self.scope
    }
    pub fn database_identity(&self) -> &str {
        &self.identity
    }
    pub fn class(&self) -> RecordClass {
        self.class
    }
    pub fn points(&self) -> &[ProviderPoint] {
        &self.points
    }
    /// Exact structured original images for the R26-owned typed adapter. These
    /// remain private DB data; they are not CLI serialization or executable work.
    pub fn native_origins(&self) -> &[(native::NativeKey, native::NativeRow)] {
        &self.origins
    }
}

pub async fn inspect<C: ConnectionTrait>(
    db: &C,
    key: &native::NativeKey,
    original: &native::NativeRow,
    namespace: &NamespaceChange,
) -> Result<VerifiedRecord> {
    inspect_with_work(
        db,
        key,
        original,
        namespace,
        records::SharedDecodeBudget::default(),
    )
    .await
}
pub(crate) async fn inspect_with_work<C: ConnectionTrait>(
    db: &C,
    key: &native::NativeKey,
    original: &native::NativeRow,
    namespace: &NamespaceChange,
    work: records::SharedDecodeBudget,
) -> Result<VerifiedRecord> {
    let context = records::DecodeWork::shared(&work);
    validate_namespace(namespace)?;
    let expected = original_record_with_work(db, key, context)
        .await?
        .ok_or(ManagementError::InvalidEvidence)?;
    if &expected != original {
        return Err(ManagementError::Changed);
    }
    let identity = native::database_identity(db).await?;
    context.copy(&identity)?;
    if identity.starts_with("diagnostic:") {
        return Err(ManagementError::Unauthorized);
    }
    let (scopes, scope_bytes) = records::bounded_projection(
        db,
        "FROM pin_management_records WHERE kind='database_scope'",
        &[("database_scope", "database_scope")],
        "database_scope",
        vec![],
        (1, native::MAX_BYTES),
        context,
    )
    .await?;
    context.index_cells(scopes.len(), scope_bytes)?;
    let scope = scopes
        .first()
        .ok_or(ManagementError::Schema)?
        .try_get::<String>("", "database_scope")
        .map_err(|_| ManagementError::Schema)?;
    if native::value(original, "database_scope") != Some(&scope) {
        return Err(ManagementError::Unauthorized);
    }
    if native::value(original, "kind") != Some("database_scope")
        && (native::value(original, "native_identity") != Some(&identity)
            || native::value(original, "principal") != Some("local_configuration"))
    {
        return Err(ManagementError::Unauthorized);
    }
    context.copy(key)?;
    context.with(|work| work.nodes(3))?;
    let mut view = VerifiedRecord {
        key: key.clone(),
        scope,
        identity,
        class: RecordClass::Identity,
        points: vec![],
        origins: vec![],
    };
    if key.table == super::super::schema::RECORDS
        && native::value(original, "kind") == Some("action")
    {
        let review = records::inspect_review_with_work(db, original, work.clone()).await?;
        if review.scope() != view.scope
            || review.identity() != view.identity
            || review.principal() != "local_configuration"
        {
            return Err(ManagementError::Unauthorized);
        }
        let applied = native::value(original, "state") == Some("applied");
        view.class = if applied {
            RecordClass::Applied
        } else {
            RecordClass::Staged
        };
        native_points(&mut view, review.rows(), "binding", namespace, context)?;
        native_points(
            &mut view,
            review.rows(),
            "native_before",
            namespace,
            context,
        )?;
        snapshot_points(&mut view, &review, namespace, context)?;
        if applied {
            let trusted = records::action_at_key_with_work(db, key, work.clone())
                .await?
                .ok_or(ManagementError::Confirmation)?;
            let action = trusted.action_view();
            for job in &action.jobs {
                trusted.validate_job(job)?;
                if job.provider != namespace.source.allocation {
                    continue;
                }
                route_facts(&job.route, namespace)?;
                let current = records::current_model::<crate::store::entities::pin_job::Entity, _>(
                    db,
                    &[&job.job_id],
                    context,
                )
                .await?;
                let executable = if let Some(current) = current
                    && matches!(current.state.as_str(), "pending" | "running" | "retry")
                {
                    matches!(
                        job_authority::check_before_call_with_work(db, &current, work.clone())
                            .await?,
                        Some(
                            job_authority::JobPermit::Submit(_)
                                | job_authority::JobPermit::ReadOnly(_)
                                | job_authority::JobPermit::ExistingCleanup(_)
                        )
                    )
                } else {
                    false
                };
                context.copy(&(
                    job,
                    &job.job_id,
                    &job.provider,
                    &job.route,
                    &job.lease_id,
                    &job.lease_generation,
                    &job.cid,
                    &job.resource_key,
                    &job.resource_id,
                ))?;
                view.points.push(ProviderPoint {
                    locator: Locator::Job {
                        job_id: job.job_id.clone(),
                    },
                    allocation: job.provider.clone(),
                    original_route: Some(job.route.clone()),
                    evidence: if executable {
                        Evidence::ExecutableJob(job.directive)
                    } else {
                        Evidence::Historical
                    },
                    lease_id: job.lease_id.clone(),
                    lease_generation: job.lease_generation,
                    cid: Some(
                        crate::pinning::provider::canonical_resource_cid(&job.cid)
                            .map_err(|_| ManagementError::InvalidEvidence)?,
                    ),
                    resource_key: Some(job.resource_key.clone()),
                    epoch: Some(job.remote_epoch),
                    resource_id: job.resource_id.clone(),
                    job_authority: Some(job.clone()),
                    migration_point: None,
                });
            }
            for point in &action.migration_points {
                trusted.validate_point(point)?;
                for destination in [false, true] {
                    let (allocation, route, resource_key, epoch, resource_id) = if destination {
                        (
                            &point.destination_allocation,
                            &point.destination_route,
                            &point.destination_resource_key,
                            point.destination_epoch,
                            &point.destination_resource_id,
                        )
                    } else {
                        (
                            &point.source_allocation,
                            &point.source_resource_route,
                            &point.source_resource_key,
                            point.source_epoch,
                            &point.source_resource_id,
                        )
                    };
                    if allocation != &namespace.source.allocation {
                        continue;
                    }
                    route_facts(route, namespace)?;
                    let live = points::resolved_slot_with_work(
                        db,
                        &point.lease_id,
                        &point.cid,
                        &point.slot,
                        work.clone(),
                    )
                    .await?
                    .is_some_and(|resolved| resolved.record_key == *key);
                    let lease = records::current_model::<
                        crate::store::entities::pin_lease::Entity,
                        _,
                    >(db, &[&point.lease_id], context)
                    .await?;
                    let now = crate::store::database_clock::database_now(db)
                        .await
                        .map_err(|_| ManagementError::Database)?;
                    let live = live
                        && lease
                            .is_some_and(|lease| lease.state == "active" && lease.expires_at > now);
                    context.copy(&(
                        point,
                        &point.primary_key,
                        &point.slot,
                        allocation,
                        route,
                        resource_key,
                        resource_id,
                        &point.lease_id,
                        &point.cid,
                    ))?;
                    view.points.push(ProviderPoint {
                        locator: Locator::Migration {
                            contract: point.primary_key.clone(),
                            slot: point.slot.clone(),
                            destination,
                        },
                        allocation: allocation.clone(),
                        original_route: Some(route.clone()),
                        evidence: if live {
                            Evidence::ExecutableSlot
                        } else {
                            Evidence::Historical
                        },
                        lease_id: Some(point.lease_id.clone()),
                        lease_generation: Some(point.source_generation),
                        cid: Some(point.cid.clone()),
                        resource_key: Some(resource_key.clone()),
                        epoch: Some(epoch),
                        resource_id: resource_id.clone(),
                        job_authority: None,
                        migration_point: Some(point.clone()),
                    });
                }
            }
        }
    } else if key.table == super::super::schema::RETIREMENTS {
        let review =
            records::inspect_retirement_review_with_work(db, original, work.clone()).await?;
        if review.scope() != view.scope
            || review.identity() != view.identity
            || review.principal() != "local_configuration"
        {
            return Err(ManagementError::Unauthorized);
        }
        view.class = RecordClass::Retirement;
        native_points(&mut view, review.rows(), "inventory", namespace, context)?;
        snapshot_points(&mut view, &review, namespace, context)?;
    } else {
        match records::inspect_original_row_with_work(&key.table, original, context)? {
            records::RecordPayload::Identity => {}
            records::RecordPayload::Evidence {
                attestation,
                original_object,
                original_version,
            } => {
                let attestation = *attestation;
                view.class = RecordClass::Evidence;
                view.origins.push((
                    native::NativeKey {
                        table: "objects".into(),
                        values: vec![attestation.identity.object_id],
                    },
                    original_object,
                ));
                view.origins.push((
                    native::NativeKey {
                        table: "object_versions".into(),
                        values: vec![attestation.identity.private_version_id],
                    },
                    original_version,
                ));
            }
        }
    }
    let mut origins = Vec::new();
    for origin in view.origins {
        if !origins.contains(&origin) {
            origins.push(origin);
        }
    }
    view.origins = origins;
    if view.points.len() > native::MAX_ROWS || view.origins.len() > native::MAX_ROWS {
        return Err(ManagementError::Budget);
    }
    Ok(view)
}

pub async fn original_record<C: ConnectionTrait>(
    db: &C,
    key: &native::NativeKey,
) -> Result<Option<native::NativeRow>> {
    original_record_with_work(
        db,
        key,
        records::DecodeWork::shared(&records::SharedDecodeBudget::default()),
    )
    .await
}
async fn original_record_with_work<C: ConnectionTrait>(
    db: &C,
    key: &native::NativeKey,
    work: records::DecodeWork<'_>,
) -> Result<Option<native::NativeRow>> {
    if !matches!(
        key.table.as_str(),
        super::super::schema::RECORDS | super::super::schema::RETIREMENTS
    ) {
        return Err(ManagementError::InvalidEvidence);
    }
    work.with(|work| work.nodes(1 + key.values.len()))?;
    let refs = key.values.iter().map(String::as_str).collect::<Vec<_>>();
    match records::current_row(db, &key.table, &refs, work).await {
        Ok(row) => Ok(Some(row)),
        Err(ManagementError::Changed) => Ok(None),
        Err(error) => Err(error),
    }
}

fn validate_namespace(namespace: &NamespaceChange) -> Result<()> {
    let destination = &namespace.destination;
    let route = &namespace.destination_route;
    // R26 v1's source is an exact historical SQL coordinate, not a newly
    // configured provider identifier. Preserve its nonempty/1024-byte bound.
    let source = &namespace.source.allocation;
    if source.is_empty()
        || source.len() > 1024
        || [
            &namespace.source.backend,
            &namespace.source.scope,
            &destination.backend,
            &destination.scope,
        ]
        .iter()
        .any(|value| value.is_empty() || value.len() > 255 || value.chars().any(char::is_control))
        || destination.backend != route.backend
        || destination.scope != route.scope
        || destination.resource_type != route.resource_type()
        || namespace.source.backend != destination.backend
        || namespace.source.resource_type != destination.resource_type
        || namespace.source.scope != destination.scope
            && !namespace.source.scope.starts_with("legacy:")
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let destination_valid = if destination.allocation.starts_with("domain:") {
        destination.allocation
            == format!(
                "domain:{}",
                serde_json::to_string(&(&route.backend, &route.scope, route.resource_type()))
                    .map_err(|_| ManagementError::InvalidEvidence)?
            )
    } else {
        !destination.allocation.is_empty()
            && destination.allocation.len() <= 255
            && !destination.allocation.chars().any(char::is_control)
    };
    if !destination_valid {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}
fn route_facts(route: &ProviderRouteSnapshot, namespace: &NamespaceChange) -> Result<()> {
    if route.backend != namespace.source.backend
        || route.resource_type() != namespace.source.resource_type
        || route.scope != namespace.source.scope && !route.scope.starts_with("legacy:")
    {
        return Err(ManagementError::InvalidEvidence);
    }
    Ok(())
}
fn add_route(
    view: &mut VerifiedRecord,
    locator: Locator,
    allocation: String,
    route: Option<ProviderRouteSnapshot>,
    namespace: &NamespaceChange,
) -> Result<()> {
    if allocation != namespace.source.allocation {
        return Ok(());
    }
    if let Some(route) = &route {
        route_facts(route, namespace)?;
    }
    let evidence = if route.is_none() {
        Evidence::Unresolved
    } else if view.class == RecordClass::Staged {
        Evidence::Staged
    } else {
        Evidence::Historical
    };
    view.points.push(ProviderPoint {
        locator,
        allocation,
        original_route: route,
        evidence,
        lease_id: None,
        lease_generation: None,
        cid: None,
        resource_key: None,
        epoch: None,
        resource_id: None,
        job_authority: None,
        migration_point: None,
    });
    Ok(())
}

fn snapshot_points(
    view: &mut VerifiedRecord,
    binding: &records::ReviewHandle,
    namespace: &NamespaceChange,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let frame = binding.rows();
    for entry in binding.entries() {
        work.copy(&entry.request.item_id)?;
        work.with(|work| work.nodes(1))?;
        let item = entry.request.item_id.clone();
        let mut routes = Vec::<(String, &RouteSnapshot)>::new();
        match &entry.capture {
            Some(Capture::Object(capture)) => {
                routes.push(("source".into(), &capture.route));
                if let Some(route) = &capture.remote_route {
                    routes.push(("remote_source".into(), route));
                }
                if let Some(route) = &capture.destination_route {
                    routes.push(("destination".into(), route));
                }
                if let Some(route) = &capture.destination_remote_route {
                    routes.push(("remote_destination".into(), route));
                }
                for (index, route) in capture.requirement_routes.iter().enumerate() {
                    routes.push((format!("requirements/{index}"), route));
                }
                for (index, route) in capture.historical_routes.iter().enumerate() {
                    routes.push((format!("history/{index}"), route));
                }
            }
            Some(Capture::Provider(capture)) => {
                for (index, route) in capture.mutation_scope.routes.iter().enumerate() {
                    routes.push((format!("provider/{index}"), route));
                }
            }
            None => return Err(ManagementError::InvalidEvidence),
        }
        for (field, route) in routes {
            for row in frame
                .table("pin_provider_routes")
                .ok_or(ManagementError::Schema)?
                .rows
                .iter()
            {
                let Some(raw) = row.cell("snapshot") else {
                    continue;
                };
                let parsed: ProviderRouteSnapshot = work.with(|work| work.typed(raw))?;
                if Some(&parsed) != route.original.as_ref() {
                    continue;
                }
                let allocation = row
                    .cell("provider")
                    .ok_or(ManagementError::InvalidEvidence)?;
                work.copy(&(&item, &field, allocation, &route.original))?;
                add_route(
                    view,
                    Locator::SnapshotRoute {
                        container: "binding",
                        item: item.clone(),
                        field: field.clone(),
                    },
                    allocation.into(),
                    route.original.clone(),
                    namespace,
                )?;
            }
        }
    }
    Ok(())
}

fn native_points(
    view: &mut VerifiedRecord,
    frame: records::FrozenRows<'_>,
    container: &'static str,
    namespace: &NamespaceChange,
    work: records::DecodeWork<'_>,
) -> Result<()> {
    let decode_route =
        |encoded: &str| work.with(|work| work.typed::<ProviderRouteSnapshot>(encoded));
    for (table, row) in frame.all_rows() {
        if matches!(
            table.table.as_str(),
            "pin_management_records" | "pin_management_retirements"
        ) {
            continue;
        }
        let invocation = table.table == "pin_invocation_routes"
            && row
                .cell("job_id")
                .and_then(|id| frame.row("pin_jobs", &[id]))
                .is_some_and(|job| job.cell("provider") == Some(&namespace.source.allocation));
        if row.cell("provider") != Some(&namespace.source.allocation)
            && table.table != "pin_lease_requirements"
            && !invocation
        {
            continue;
        }
        work.copy(&table.table)?;
        work.with(|work| work.nodes(2 + table.primary_key.len()))?;
        for column in &table.primary_key {
            work.copy(&row.cell(column))?;
        }
        let values = table
            .primary_key
            .iter()
            .map(|column| {
                native::value(row, column)
                    .map(str::to_owned)
                    .ok_or(ManagementError::InvalidEvidence)
            })
            .collect::<Result<Vec<_>>>()?;
        let key = native::NativeKey {
            table: table.table.clone(),
            values,
        };
        if matches!(
            table.table.as_str(),
            "pin_management_records" | "pin_management_retirements"
        ) {
            continue;
        }
        // Only known provider columns/captured route types, never substring
        // searching opaque framework material or issuing permits from it.
        if native::value(row, "provider") == Some(&namespace.source.allocation) {
            let route = match table.table.as_str() {
                "pin_provider_routes" => native::value(row, "snapshot")
                    .map(decode_route)
                    .transpose()?,
                "remote_pin_ledger" | "pin_submit_observations" | "pin_replica_observations" => {
                    native::value(row, "route").map(decode_route).transpose()?
                }
                "pin_jobs" => native::value(row, "id")
                    .and_then(|id| frame.row("pin_invocation_routes", &[id]))
                    .and_then(|row| native::value(row, "route"))
                    .map(decode_route)
                    .transpose()?,
                "remote_pins" => native::value(row, "cid")
                    .and_then(|cid| {
                        frame.row("remote_pin_ledger", &[&namespace.source.allocation, cid])
                    })
                    .and_then(|row| native::value(row, "route"))
                    .map(decode_route)
                    .transpose()?,
                "pin_resource_history" => {
                    let envelope: ResourceHistory = work.with(|work| {
                        work.typed(
                            native::value(row, "ledger").ok_or(ManagementError::InvalidEvidence)?,
                        )
                    })?;
                    envelope
                        .ledger
                        .route
                        .as_deref()
                        .map(decode_route)
                        .transpose()?
                }
                _ => None,
            };
            work.copy(&(&key, &namespace.source.allocation))?;
            add_route(
                view,
                Locator::NativeColumn {
                    container,
                    key: key.clone(),
                    column: "provider",
                },
                namespace.source.allocation.clone(),
                route,
                namespace,
            )?;
            let point = view
                .points
                .last_mut()
                .ok_or(ManagementError::InvalidEvidence)?;
            point.lease_id = native::value(row, "lease_id").map(str::to_owned);
            point.lease_generation =
                native::value(row, "expected_generation").and_then(|value| value.parse().ok());
            point.cid = native::value(row, "cid")
                .map(crate::pinning::provider::canonical_resource_cid)
                .transpose()
                .map_err(|_| ManagementError::InvalidEvidence)?;
            if matches!(
                table.table.as_str(),
                "remote_pins" | "remote_pin_ledger" | "pin_resource_history"
            ) {
                point.resource_key = Some(key.clone());
            }
            point.epoch = native::value(row, "epoch")
                .or_else(|| native::value(row, "remote_epoch"))
                .and_then(|value| value.parse().ok());
            point.resource_id = native::value(row, "request_id").map(str::to_owned);
            work.copy(&(&key, row))?;
            view.origins.push((
                key.clone(),
                row.iter()
                    .map(|(column, value)| (column.clone(), value.clone()))
                    .collect(),
            ));
        }
        if table.table == "pin_lease_requirements" {
            let contract: crate::store::entities::pin_lease_requirement::contract::BirthContract =
                work.with(|work| {
                    work.typed(
                        native::value(row, "contract").ok_or(ManagementError::InvalidEvidence)?,
                    )
                })?;
            for (slot, route) in contract.candidates {
                if slot != namespace.source.allocation {
                    continue;
                }
                work.copy(&(&key, &slot, &key, row))?;
                add_route(
                    view,
                    Locator::BirthCandidate {
                        container,
                        key: key.clone(),
                        slot: slot.clone(),
                    },
                    slot,
                    route,
                    namespace,
                )?;
                view.origins.push((
                    key.clone(),
                    row.iter()
                        .map(|(column, value)| (column.clone(), value.clone()))
                        .collect(),
                ));
            }
        }
        if table.table == "pin_invocation_routes"
            && let Some(id) = native::value(row, "job_id")
            && frame.row("pin_jobs", &[id]).is_some_and(|job| {
                native::value(job, "provider") == Some(&namespace.source.allocation)
            })
        {
            work.copy(&(&key, &namespace.source.allocation, &key, row))?;
            add_route(
                view,
                Locator::NativeColumn {
                    container,
                    key: key.clone(),
                    column: "route",
                },
                namespace.source.allocation.clone(),
                native::value(row, "route").map(decode_route).transpose()?,
                namespace,
            )?;
            view.origins.push((
                key,
                row.iter()
                    .map(|(column, value)| (column.clone(), value.clone()))
                    .collect(),
            ));
        }
    }
    Ok(())
}
#[derive(serde::Deserialize)]
struct ResourceHistory {
    ledger: HistoricalLedger,
}
#[derive(serde::Deserialize)]
struct HistoricalLedger {
    route: Option<String>,
}
