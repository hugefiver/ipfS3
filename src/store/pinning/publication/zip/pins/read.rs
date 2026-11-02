use super::*;
use crate::store::pinning::{ledger, legacy_migration};
use sea_orm::{EntityTrait, Statement};

pub(super) async fn by_ids_where<E: EntityTrait + Default, C: ConnectionTrait>(
    db: &C,
    column: &str,
    ids: &[String],
    extra: &str,
    work: DecodeWork<'_>,
    resident: &mut Vec<ResidentGuard>,
    mut maintenance: Option<&mut super::super::PublicationLease<'_>>,
) -> AppResult<Vec<E::Model>> {
    let mut rows = Vec::new();
    for chunk in ids.chunks(sql::PARAMS) {
        rows.extend(
            sql::read::<E, _>(
                db,
                &format!("\"{column}\" IN ({}) {extra}", sql::slots(chunk.len(), 0)),
                chunk.iter().cloned().map(Into::into).collect(),
                column,
                work,
                resident,
                maintenance.as_deref_mut(),
            )
            .await?,
        );
    }
    Ok(rows)
}

pub(super) async fn by_ids<E: EntityTrait + Default, C: ConnectionTrait>(
    db: &C,
    column: &str,
    ids: &[String],
    work: DecodeWork<'_>,
    resident: &mut Vec<ResidentGuard>,
    maintenance: Option<&mut super::super::PublicationLease<'_>>,
) -> AppResult<Vec<E::Model>> {
    by_ids_where::<E, _>(db, column, ids, "", work, resident, maintenance).await
}

async fn lock_ids<C: ConnectionTrait>(
    db: &C,
    table: &str,
    column: &str,
    ids: &[String],
    order: &str,
    work: DecodeWork<'_>,
    mut maintenance: Option<&mut super::super::PublicationLease<'_>>,
) -> AppResult<()> {
    for chunk in ids.chunks(sql::PARAMS) {
        sql::lock(
            db,
            table,
            &format!("\"{column}\" IN ({})", sql::slots(chunk.len(), 0)),
            chunk.iter().cloned().map(Into::into).collect(),
            order,
            work,
            maintenance.as_deref_mut(),
        )
        .await?;
    }
    Ok(())
}

pub(super) async fn by_pairs<E: EntityTrait + Default, C: ConnectionTrait>(
    db: &C,
    pairs: &BTreeSet<Pair>,
    extra: &str,
    work: DecodeWork<'_>,
    resident: &mut Vec<ResidentGuard>,
    mut maintenance: Option<&mut super::super::PublicationLease<'_>>,
) -> AppResult<Vec<E::Model>> {
    let mut providers: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (p, c) in pairs {
        providers.entry(p).or_default().push(c);
    }
    let mut rows = Vec::new();
    for (provider, cids) in providers {
        for chunk in cids.chunks(sql::PARAMS - 1) {
            let mut values = vec![provider.into()];
            values.extend(chunk.iter().copied().map(Into::into));
            rows.extend(
                sql::read::<E, _>(
                    db,
                    &format!(
                        "provider=$1 AND cid IN ({}) {extra}",
                        sql::slots(chunk.len(), 1)
                    ),
                    values,
                    "provider,cid",
                    work,
                    resident,
                    maintenance.as_deref_mut(),
                )
                .await?,
            );
        }
    }
    Ok(rows)
}

async fn lock_pairs<C: ConnectionTrait>(
    db: &C,
    table: &str,
    pairs: &BTreeSet<Pair>,
    work: DecodeWork<'_>,
    mut maintenance: Option<&mut super::super::PublicationLease<'_>>,
) -> AppResult<()> {
    let mut providers: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for (provider, cid) in pairs {
        providers.entry(provider).or_default().push(cid);
    }
    for (provider, cids) in providers {
        for chunk in cids.chunks(sql::PARAMS - 1) {
            let mut values = vec![provider.into()];
            values.extend(chunk.iter().copied().map(Into::into));
            sql::lock(
                db,
                table,
                &format!("provider=$1 AND cid IN ({})", sql::slots(chunk.len(), 1)),
                values,
                "provider,cid",
                work,
                maintenance.as_deref_mut(),
            )
            .await?;
        }
    }
    Ok(())
}

pub(super) async fn new_allocation_safe<C: ConnectionTrait>(
    db: &C,
    work: DecodeWork<'_>,
) -> AppResult<bool> {
    sql::charge(
        work,
        crate::store::pinning::management::native::meter::WorkMeter {
            sql: 1,
            rows: 1,
            cells: 1,
            raw_bytes: 1,
            ..Default::default()
        },
    )?;
    let unsafe_capacity = db.query_one(Statement::from_string(db.get_database_backend(), "SELECT (EXISTS(SELECT 1 FROM pin_provider_usage WHERE provider NOT LIKE 'domain:%' AND (reserved_bytes>0 OR reserved_pins>0)) OR EXISTS(SELECT 1 FROM remote_pins WHERE provider NOT LIKE 'domain:%' AND status IN ('reserved','queued','pinning','pinned','failed')) OR EXISTS(SELECT 1 FROM remote_pin_ledger WHERE route IS NULL AND effect<>'absent')) AS unsafe_capacity")).await?.ok_or_else(invalid)?.try_get::<bool>("", "unsafe_capacity")?;
    Ok(!unsafe_capacity)
}

impl PinEvidence {
    pub(super) async fn load<R: publication::reservations::Reservations>(
        reservations: &R,
        providers: &BTreeSet<String>,
        pairs: &BTreeSet<Pair>,
        work: DecodeWork<'_>,
        resident: &mut Vec<ResidentGuard>,
        mut maintenance: Option<&mut super::super::PublicationLease<'_>>,
    ) -> AppResult<Self> {
        let db = reservations.db();
        let catalog = reservations.catalog(work);
        let mut scope = providers.clone();
        let mut replica_pairs = pairs.clone();
        for provider in providers {
            scope.extend(
                legacy_migration::admission_historical_keys_with_catalog(provider, &work, catalog)
                    .await
                    .map_err(work_error)?,
            );
            scope.extend(
                legacy_migration::historical_keys_with_catalog(
                    "pin_jobs", provider, None, work, catalog,
                )
                .await
                .map_err(work_error)?,
            );
            let mut aliases = legacy_migration::historical_keys_with_catalog(
                "pin_replica_observations",
                provider,
                None,
                work,
                catalog,
            )
            .await
            .map_err(work_error)?;
            aliases.push(provider.clone());
            for alias in aliases {
                replica_pairs.extend(
                    pairs
                        .iter()
                        .filter(|(p, _)| p == provider)
                        .map(|(_, cid)| (alias.clone(), cid.clone())),
                );
            }
        }
        let keys = scope.iter().cloned().collect::<Vec<_>>();
        let submissions = by_ids::<pin_submit_observation::Entity, _>(
            db,
            "provider",
            &keys,
            work,
            resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect();
        let releases = by_ids::<pin_resource_history::Entity, _>(
            db,
            "provider",
            &keys,
            work,
            resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|row| ((row.provider.clone(), row.cid.clone(), row.epoch), row))
        .collect();
        let mut histories = BTreeMap::new();
        for chunk in keys.chunks(sql::PARAMS) {
            let slots = sql::slots(chunk.len(), 0);
            let predicate = format!(
                "job_id IN (SELECT id FROM pin_jobs WHERE provider IN ({slots}) UNION SELECT job_id FROM pin_submit_observations WHERE provider IN ({slots}))"
            );
            for row in sql::read::<jobs::history::Entity, _>(
                db,
                &predicate,
                chunk.iter().cloned().map(Into::into).collect(),
                "job_id",
                work,
                resident,
                maintenance.as_deref_mut(),
            )
            .await?
            {
                histories.insert(row.job_id.clone(), row);
            }
        }
        let mut replicas = BTreeMap::new();
        let mut by_provider: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (provider, cid) in &replica_pairs {
            by_provider.entry(provider).or_default().push(cid);
        }
        for (provider, cids) in by_provider {
            for chunk in cids.chunks(sql::PARAMS - 1) {
                let mut values = vec![provider.into()];
                values.extend(chunk.iter().copied().map(Into::into));
                for row in sql::read::<pin_replica_observation::Entity, _>(
                    db,
                    &format!(
                        "provider=$1 AND expected_cid IN ({})",
                        sql::slots(chunk.len(), 1)
                    ),
                    values,
                    "sequence",
                    work,
                    resident,
                    maintenance.as_deref_mut(),
                )
                .await?
                {
                    replicas.insert(row.sequence, row);
                }
            }
        }
        Ok(Self {
            providers: scope,
            submissions,
            histories,
            releases,
            replicas,
        })
    }
}

impl LockedPins {
    pub(in crate::store::pinning::publication::zip) async fn load<
        R: publication::reservations::Reservations,
    >(
        reservations: &R,
        keys: &AllocationKeys,
        selected: &ZipIntentSelection,
        owners: &[String],
        now: DateTime<Utc>,
        work: DecodeWork<'_>,
        mut maintenance: Option<&mut super::super::PublicationLease<'_>>,
    ) -> AppResult<Self> {
        let db = reservations.db();
        let catalog = reservations.catalog(work);
        let mut resident = Vec::new();
        let mut leases = by_ids::<pin_lease::Entity, _>(
            db,
            "owner_object_id",
            owners,
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|l| (l.id.clone(), l))
        .collect::<BTreeMap<_, _>>();
        let mut pairs = keys.pairs.iter().cloned().collect::<BTreeSet<_>>();
        let mut targets = BTreeMap::new();
        let mut read_pairs = BTreeSet::new();
        let mut read_leases = BTreeSet::new();
        loop {
            let ids = leases
                .keys()
                .filter(|id| !read_leases.contains(*id))
                .cloned()
                .collect::<Vec<_>>();
            let pending = pairs
                .difference(&read_pairs)
                .cloned()
                .collect::<BTreeSet<_>>();
            if ids.is_empty() && pending.is_empty() {
                break;
            }
            let siblings = by_ids::<pin_lease_target::Entity, _>(
                db,
                "lease_id",
                &ids,
                work,
                &mut resident,
                maintenance.as_deref_mut(),
            )
            .await?;
            read_leases.extend(ids);
            let related = by_pairs::<pin_lease_target::Entity, _>(db, &pending, "AND state IN ('waiting','submitted','pinned','degraded') AND EXISTS (SELECT 1 FROM pin_leases l WHERE l.id=lease_id AND l.state='active')", work, &mut resident, maintenance.as_deref_mut()).await?;
            read_pairs.extend(pending);
            for target in siblings.into_iter().chain(related) {
                if desired(&target.state) {
                    pairs.insert((target.provider.clone(), target.cid.clone()));
                }
                targets.insert(target.id.clone(), target);
            }
            let missing = targets
                .values()
                .filter(|t| !leases.contains_key(&t.lease_id))
                .map(|t| t.lease_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            for lease in by_ids::<pin_lease::Entity, _>(
                db,
                "id",
                &missing,
                work,
                &mut resident,
                maintenance.as_deref_mut(),
            )
            .await?
            {
                leases.insert(lease.id.clone(), lease);
            }
        }
        let lease_ids = leases.keys().cloned().collect::<Vec<_>>();
        let requirements = by_ids::<pin_lease_requirement::Entity, _>(
            db,
            "lease_id",
            &lease_ids,
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|row| (row.lease_id.clone(), row))
        .collect();
        let mut target_ids = targets
            .values()
            .map(|target| (target.created_at, target.id.clone()))
            .collect::<Vec<_>>();
        target_ids.sort();
        let target_ids = target_ids.into_iter().map(|(_, id)| id).collect::<Vec<_>>();
        lock_ids(
            db,
            "pin_leases",
            "id",
            &lease_ids,
            "id",
            work,
            maintenance.as_deref_mut(),
        )
        .await?;
        lock_ids(
            db,
            "pin_lease_targets",
            "id",
            &target_ids,
            "created_at,id",
            work,
            maintenance.as_deref_mut(),
        )
        .await?;
        lock_pairs(db, "remote_pins", &pairs, work, maintenance.as_deref_mut()).await?;
        crate::store::pinning::quota::lock_publication_usage_rows(
            db,
            &keys.enabled.iter().cloned().collect::<Vec<_>>(),
        )
        .await?;
        let remotes = by_pairs::<remote_pin::Entity, _>(
            db,
            &pairs,
            "",
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|r| ((r.provider.clone(), r.cid.clone()), r))
        .collect::<BTreeMap<_, _>>();
        let mut ledgers = by_pairs::<remote_pin_ledger::Entity, _>(
            db,
            &pairs,
            "",
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|r| ((r.provider.clone(), r.cid.clone()), r))
        .collect::<BTreeMap<_, _>>();
        let mut jobs = by_pairs::<pin_job::Entity, _>(
            db,
            &pairs,
            "",
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|j| (j.id.clone(), j))
        .collect::<BTreeMap<_, _>>();
        for job in by_ids::<pin_job::Entity, _>(
            db,
            "lease_id",
            &lease_ids,
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        {
            jobs.insert(job.id.clone(), job);
        }
        let job_ids = jobs.keys().cloned().collect::<Vec<_>>();
        let invocations = by_ids::<pin_invocation_route::Entity, _>(
            db,
            "job_id",
            &job_ids,
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|row| (row.job_id.clone(), row))
        .collect();
        let providers = keys.enabled.iter().cloned().collect::<Vec<_>>();
        let usages = by_ids::<pin_provider_usage::Entity, _>(
            db,
            "provider",
            &providers,
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|u| (u.provider.clone(), u))
        .collect();
        let mut review_resident = Vec::new();
        let current_owners = by_ids::<pin_lease::Entity, _>(
            db,
            "owner_object_id",
            owners,
            work,
            &mut review_resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect::<BTreeMap<_, _>>();
        let expected_owners = leases
            .iter()
            .filter(|(_, row)| owners.contains(&row.owner_object_id))
            .map(|(id, row)| (id.clone(), row.clone()))
            .collect::<BTreeMap<_, _>>();
        let current_targets = by_ids::<pin_lease_target::Entity, _>(
            db,
            "lease_id",
            &lease_ids,
            work,
            &mut review_resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect::<BTreeMap<_, _>>();
        let current_related = by_pairs::<pin_lease_target::Entity, _>(db, &pairs, "AND state IN ('waiting','submitted','pinned','degraded') AND EXISTS (SELECT 1 FROM pin_leases l WHERE l.id=lease_id AND l.state='active')", work, &mut review_resident, maintenance.as_deref_mut()).await?;
        if current_owners != expected_owners
            || current_targets != targets
            || current_related
                .iter()
                .any(|row| targets.get(&row.id) != Some(row))
        {
            return Err(invalid());
        }
        let mut barriers = BTreeMap::new();
        let mut retired_sources = BTreeSet::new();
        let mut unconfirmed_sources = BTreeSet::new();
        for provider in &providers {
            barriers.insert(
                provider.clone(),
                ledger::submission::admission_barrier_with_catalog(provider, catalog, work).await?,
            );
            use crate::store::pinning::management::records::locator::AuthoritySelector;
            let candidates = catalog
                .load(
                    &[
                        AuthoritySelector::LegacyResource(
                            "remote_pins".into(),
                            provider.clone(),
                            None,
                        ),
                        AuthoritySelector::LegacyResource(
                            "remote_pin_ledger".into(),
                            provider.clone(),
                            None,
                        ),
                    ],
                    work,
                )
                .await
                .map_err(work_error)?;
            for pair in remotes.keys().filter(|(p, _)| p == provider) {
                if let Some(binding) = legacy_migration::resource_binding_from_catalog(
                    &pair.0,
                    &pair.1,
                    &candidates,
                    work,
                    catalog,
                )
                .await
                .map_err(work_error)?
                {
                    if binding.source_retired {
                        retired_sources.insert(pair.clone());
                    }
                    if binding.historical_route.is_none() {
                        unconfirmed_sources.insert(pair.clone());
                    }
                    if let Some(ledger) =
                        ledger::get_from_catalog(&pair.0, &pair.1, &candidates, work, catalog)
                            .await?
                    {
                        ledgers.insert(pair.clone(), ledger);
                    }
                }
            }
        }
        let new_allocation_safe = new_allocation_safe(db, work).await?;
        let evidence_providers = pairs
            .iter()
            .map(|(provider, _)| provider.clone())
            .chain(providers)
            .chain(jobs.values().map(|job| job.provider.clone()))
            .collect();
        let evidence = PinEvidence::load(
            reservations,
            &evidence_providers,
            &pairs,
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?;
        let mut result = Self {
            owners: owners.to_vec(),
            pairs,
            evidence,
            requirements,
            leases,
            targets,
            remotes,
            ledgers,
            jobs,
            invocations,
            usages,
            barriers,
            retired_sources,
            unconfirmed_sources,
            new_allocation_safe,
            cluster_confirmed: BTreeSet::new(),
            resident,
        };
        result
            .cluster_evidence(reservations, keys, selected, now, work, maintenance)
            .await?;
        Ok(result)
    }

    pub(in crate::store::pinning::publication::zip) async fn lock_tail<C: ConnectionTrait>(
        &self,
        db: &C,
        work: DecodeWork<'_>,
        mut maintenance: Option<&mut super::super::PublicationLease<'_>>,
    ) -> AppResult<()> {
        let job_ids = self.jobs.keys().cloned().collect::<Vec<_>>();
        lock_ids(
            db,
            "pin_jobs",
            "id",
            &job_ids,
            "id",
            work,
            maintenance.as_deref_mut(),
        )
        .await?;
        lock_ids(
            db,
            "pin_invocation_routes",
            "job_id",
            &job_ids,
            "job_id",
            work,
            maintenance.as_deref_mut(),
        )
        .await?;
        let mut resident = Vec::new();
        let jobs = by_ids::<pin_job::Entity, _>(
            db,
            "id",
            &job_ids,
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect::<BTreeMap<_, _>>();
        let invocations = by_ids::<pin_invocation_route::Entity, _>(
            db,
            "job_id",
            &job_ids,
            work,
            &mut resident,
            maintenance,
        )
        .await?
        .into_iter()
        .map(|row| (row.job_id.clone(), row))
        .collect::<BTreeMap<_, _>>();
        if jobs != self.jobs || invocations != self.invocations {
            return Err(invalid());
        }
        Ok(())
    }

    async fn cluster_evidence<R: publication::reservations::Reservations>(
        &mut self,
        reservations: &R,
        keys: &AllocationKeys,
        selected: &ZipIntentSelection,
        now: DateTime<Utc>,
        work: DecodeWork<'_>,
        mut maintenance: Option<&mut super::super::PublicationLease<'_>>,
    ) -> AppResult<()> {
        let catalog = reservations.catalog(work);
        for (index, target) in self.targets.values().enumerate() {
            if index.is_multiple_of(sql::PARAMS)
                && let Some(lease) = maintenance.as_deref_mut()
            {
                lease.checkpoint(reservations.db()).await?;
            }
            let pair = (target.provider.clone(), target.cid.clone());
            if keys.routes.get(&target.provider).is_some_and(|r| {
                r.resource_type() == crate::pinning::identity::RemoteResourceType::ClusterPin
            }) && pin_lease_requirement::contract::target_confirmed_catalog(
                catalog,
                &target.lease_id,
                &pair.0,
                &pair.1,
                now,
                work.budget().clone(),
            )
            .await
            .map_err(work_error)?
            {
                self.cluster_confirmed
                    .insert((target.lease_id.clone(), pair));
            }
        }
        let mut fresh = BTreeMap::new();
        for lease in &selected.leases {
            for provider in publication::ordered_enabled_providers(&lease.intent, &keys.limits)? {
                let route = &keys.routes[&provider];
                if route.resource_type() != crate::pinning::identity::RemoteResourceType::ClusterPin
                {
                    continue;
                }
                for (index, target) in lease.targets.iter().enumerate() {
                    if index.is_multiple_of(sql::PARAMS)
                        && let Some(lease) = maintenance.as_deref_mut()
                    {
                        lease.checkpoint(reservations.db()).await?;
                    }
                    let pair = (
                        provider.clone(),
                        keys.canonical[&(provider.clone(), target.cid.clone())].clone(),
                    );
                    let confirmed = if let Some(confirmed) = fresh.get(&pair) {
                        *confirmed
                    } else {
                        let confirmed = crate::store::entities::pin_replica_observation::receipt::fresh_complete_catalog(catalog, &pair.0, &pair.1, route, now, work).await.map_err(work_error)?;
                        fresh.insert(pair.clone(), confirmed);
                        confirmed
                    };
                    if confirmed {
                        self.cluster_confirmed.insert((
                            publication::lease_id(&lease.owner.object_id, lease.intent.source),
                            pair,
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}
