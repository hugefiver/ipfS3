use super::*;
use sea_orm::{EntityTrait, ModelTrait};

fn changes<K: Ord, M: Clone + PartialEq>(
    old: &BTreeMap<K, M>,
    new: &BTreeMap<K, M>,
) -> (Vec<M>, Vec<(M, M)>) {
    let mut inserted = Vec::new();
    let mut updated = Vec::new();
    for (key, row) in new {
        match old.get(key) {
            None => inserted.push(row.clone()),
            Some(before) if before != row => updated.push((before.clone(), row.clone())),
            Some(_) => {}
        }
    }
    (inserted, updated)
}

async fn verify<E: EntityTrait + Default, C: ConnectionTrait>(
    db: &C,
    expected: &[E::Model],
    column: E::Column,
    work: DecodeWork<'_>,
) -> AppResult<()>
where
    E::Model: PartialEq,
{
    use sea_orm::Iden;
    let mut resident = Vec::new();
    for chunk in expected.chunks(sql::PARAMS) {
        let values = chunk.iter().map(|r| r.get(column)).collect::<Vec<_>>();
        let name = Iden::to_string(&column);
        let actual = sql::read::<E, _>(
            db,
            &format!("\"{name}\" IN ({})", sql::slots(chunk.len(), 0)),
            values,
            &name,
            work,
            &mut resident,
            None,
        )
        .await?;
        if actual.len() != chunk.len() || chunk.iter().any(|row| !actual.contains(row)) {
            return Err(invalid());
        }
        resident.clear();
    }
    Ok(())
}

impl PinWriteSet {
    pub(in crate::store::pinning::publication::zip) async fn persist<C: ConnectionTrait>(
        &self,
        db: &C,
        work: DecodeWork<'_>,
        maintenance: &mut PublicationLease<'_>,
    ) -> AppResult<()> {
        let (inserted, updated) = changes(&self.before.leases, &self.leases);
        sql::update::<pin_lease::Entity, _>(db, &updated, work, Some(maintenance)).await?;
        sql::insert::<pin_lease::Entity, _>(db, &inserted, work, Some(maintenance)).await?;
        sql::insert::<pin_lease_requirement::Entity, _>(
            db,
            &self.requirements,
            work,
            Some(maintenance),
        )
        .await?;
        let (inserted, updated) = changes(&self.before.usages, &self.usages);
        sql::update::<pin_provider_usage::Entity, _>(db, &updated, work, Some(maintenance)).await?;
        sql::insert::<pin_provider_usage::Entity, _>(db, &inserted, work, Some(maintenance))
            .await?;
        let (inserted, updated) = changes(&self.before.remotes, &self.remotes);
        sql::update::<remote_pin::Entity, _>(db, &updated, work, Some(maintenance)).await?;
        sql::insert::<remote_pin::Entity, _>(db, &inserted, work, Some(maintenance)).await?;
        let (inserted, updated) = changes(&self.before.ledgers, &self.ledgers);
        sql::update::<remote_pin_ledger::Entity, _>(db, &updated, work, Some(maintenance)).await?;
        sql::insert::<remote_pin_ledger::Entity, _>(db, &inserted, work, Some(maintenance)).await?;
        let (inserted, updated) = changes(&self.before.targets, &self.targets);
        sql::update::<pin_lease_target::Entity, _>(db, &updated, work, Some(maintenance)).await?;
        sql::insert::<pin_lease_target::Entity, _>(db, &inserted, work, Some(maintenance)).await?;
        let (inserted, updated) = changes(&self.before.jobs, &self.jobs);
        sql::update::<pin_job::Entity, _>(db, &updated, work, Some(maintenance)).await?;
        sql::insert::<pin_job::Entity, _>(db, &inserted, work, Some(maintenance)).await?;
        let (inserted, updated) = changes(&self.before.invocations, &self.invocations);
        if !updated.is_empty() {
            return Err(invalid());
        }
        sql::insert::<pin_invocation_route::Entity, _>(db, &inserted, work, Some(maintenance))
            .await?;
        Ok(())
    }

    async fn verify_membership<C: ConnectionTrait>(
        &self,
        db: &C,
        work: DecodeWork<'_>,
    ) -> AppResult<()> {
        let mut resident = Vec::new();
        let owners = self
            .before
            .owners
            .iter()
            .cloned()
            .chain(self.publication_owners.iter().cloned())
            .collect::<BTreeSet<_>>();
        let owner_ids = owners.iter().cloned().collect::<Vec<_>>();
        let leases = super::read::by_ids::<pin_lease::Entity, _>(
            db,
            "owner_object_id",
            &owner_ids,
            work,
            &mut resident,
            None,
        )
        .await?
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect::<BTreeMap<_, _>>();
        let expected_count = self
            .leases
            .values()
            .filter(|row| owners.contains(&row.owner_object_id))
            .count();
        if leases.len() != expected_count
            || leases
                .iter()
                .any(|(id, row)| self.leases.get(id) != Some(row))
        {
            return Err(invalid());
        }
        drop(leases);
        resident.clear();
        let lease_ids = self.leases.keys().cloned().collect::<Vec<_>>();
        let targets = super::read::by_ids::<pin_lease_target::Entity, _>(
            db,
            "lease_id",
            &lease_ids,
            work,
            &mut resident,
            None,
        )
        .await?
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect::<BTreeMap<_, _>>();
        if targets != self.targets {
            return Err(invalid());
        }
        drop(targets);
        resident.clear();
        let related = super::read::by_pairs::<pin_lease_target::Entity, _>(db, &self.before.pairs, "AND state IN ('waiting','submitted','pinned','degraded') AND EXISTS (SELECT 1 FROM pin_leases l WHERE l.id=lease_id AND l.state='active')", work, &mut resident, None).await?.into_iter().map(|row| (row.id.clone(), row)).collect::<BTreeMap<_, _>>();
        let expected_count = self
            .targets
            .values()
            .filter(|row| desired(&row.state) && self.leases[&row.lease_id].state == "active")
            .count();
        if related.len() != expected_count
            || related
                .iter()
                .any(|(id, row)| self.targets.get(id) != Some(row))
        {
            return Err(invalid());
        }
        drop(related);
        resident.clear();
        let mut jobs = super::read::by_pairs::<pin_job::Entity, _>(
            db,
            &self.before.pairs,
            "",
            work,
            &mut resident,
            None,
        )
        .await?
        .into_iter()
        .map(|row| (row.id.clone(), row))
        .collect::<BTreeMap<_, _>>();
        for job in super::read::by_ids::<pin_job::Entity, _>(
            db,
            "lease_id",
            &lease_ids,
            work,
            &mut resident,
            None,
        )
        .await?
        {
            jobs.insert(job.id.clone(), job);
        }
        if jobs != self.jobs {
            return Err(invalid());
        }
        drop(jobs);
        resident.clear();
        let job_ids = self.jobs.keys().cloned().collect::<Vec<_>>();
        let invocations = super::read::by_ids::<pin_invocation_route::Entity, _>(
            db,
            "job_id",
            &job_ids,
            work,
            &mut resident,
            None,
        )
        .await?
        .into_iter()
        .map(|row| (row.job_id.clone(), row))
        .collect::<BTreeMap<_, _>>();
        if invocations != self.invocations {
            return Err(invalid());
        }
        drop(invocations);
        resident.clear();
        let requirements = super::read::by_ids::<pin_lease_requirement::Entity, _>(
            db,
            "lease_id",
            &lease_ids,
            work,
            &mut resident,
            None,
        )
        .await?
        .into_iter()
        .map(|row| (row.lease_id.clone(), row))
        .collect::<BTreeMap<_, _>>();
        if requirements.len() != self.before.requirements.len() + self.requirements.len()
            || self
                .before
                .requirements
                .values()
                .chain(self.requirements.iter())
                .any(|row| requirements.get(&row.lease_id) != Some(row))
        {
            return Err(invalid());
        }
        Ok(())
    }

    async fn verify_evidence<R: publication::reservations::Reservations>(
        &self,
        reservations: &R,
        work: DecodeWork<'_>,
    ) -> AppResult<()> {
        let providers = self
            .before
            .pairs
            .iter()
            .map(|(provider, _)| provider.clone())
            .chain(self.before.barriers.keys().cloned())
            .chain(self.before.jobs.values().map(|job| job.provider.clone()))
            .collect();
        let mut resident = Vec::new();
        let evidence = PinEvidence::load(
            reservations,
            &providers,
            &self.before.pairs,
            work,
            &mut resident,
            None,
        )
        .await?;
        if evidence != self.before.evidence {
            return Err(invalid());
        }
        let new_domain_allocation = self.affected.iter().any(|pair| {
            pair.0.starts_with("domain:")
                && self
                    .before
                    .remotes
                    .get(pair)
                    .is_none_or(|remote| remote.status == "absent")
        });
        if new_domain_allocation
            && !super::read::new_allocation_safe(reservations.db(), work).await?
        {
            return Err(invalid());
        }
        // Only admitted desired targets require a clear provider barrier. Existing
        // blocked targets and owner cancellation may legitimately retain debt.
        let admitted = self
            .affected
            .iter()
            .map(|(provider, _)| provider)
            .collect::<BTreeSet<_>>();
        for provider in admitted {
            if crate::store::pinning::ledger::submission::admission_barrier_with_catalog(
                provider,
                reservations.catalog(work),
                work,
            )
            .await?
                != AdmissionBarrier::Clear
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    pub(in crate::store::pinning::publication::zip) async fn verify<
        R: publication::reservations::Reservations,
    >(
        &self,
        reservations: &R,
        work: DecodeWork<'_>,
    ) -> AppResult<()> {
        let db = reservations.db();
        self.verify_membership(db, work).await?;
        self.verify_evidence(reservations, work).await?;
        verify::<pin_lease::Entity, _>(
            db,
            &self.leases.values().cloned().collect::<Vec<_>>(),
            pin_lease::Column::Id,
            work,
        )
        .await?;
        verify::<pin_provider_usage::Entity, _>(
            db,
            &self.usages.values().cloned().collect::<Vec<_>>(),
            pin_provider_usage::Column::Provider,
            work,
        )
        .await?;
        let mut resident = Vec::new();
        let remotes = super::read::by_pairs::<remote_pin::Entity, _>(
            db,
            &self.before.pairs,
            "",
            work,
            &mut resident,
            None,
        )
        .await?
        .into_iter()
        .map(|row| ((row.provider.clone(), row.cid.clone()), row))
        .collect::<BTreeMap<_, _>>();
        if remotes != self.remotes {
            return Err(invalid());
        }
        drop(remotes);
        resident.clear();
        let ledgers = super::read::by_pairs::<remote_pin_ledger::Entity, _>(
            db,
            &self.before.pairs,
            "",
            work,
            &mut resident,
            None,
        )
        .await?
        .into_iter()
        .map(|row| ((row.provider.clone(), row.cid.clone()), row))
        .collect::<BTreeMap<_, _>>();
        if ledgers != self.ledgers {
            return Err(invalid());
        }
        Ok(())
    }
}
