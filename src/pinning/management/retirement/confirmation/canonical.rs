use super::*;

const DERIVED: &[&str] = &[
    "state",
    "facts",
    "quotes",
    "target_edges",
    "jobs",
    "cids",
    "resource_counts",
    "allocation_attribution",
    "totals_progress",
];
const UNION_TABLES: &[&str] = &[
    "quotes",
    "cids",
    "transfers",
    "unknowns",
    "binding",
    "merged",
    "output",
];

pub(super) fn name(reference: &RetirementInventoryRef) -> Result<String> {
    uuid::Uuid::parse_str(&reference.inventory_id).map_err(|_| ManagementError::InvalidEvidence)?;
    Ok(format!("canonical-{}.sqlite", reference.inventory_id))
}
pub(super) fn union_name(reference: &RetirementInventoryRef) -> Result<String> {
    uuid::Uuid::parse_str(&reference.inventory_id).map_err(|_| ManagementError::InvalidEvidence)?;
    Ok(format!("canonical-union-{}.sqlite", reference.inventory_id))
}

pub(super) struct Canonical {
    workspace: Workspace,
    spools: Vec<InventorySpool>,
    union: PlanUnion,
}
impl Canonical {
    pub(super) async fn open(plan: &DryRunPlan, limits: &RunLimits) -> Result<Self> {
        let primary = plan
            .retirement_sources
            .first()
            .ok_or(ManagementError::InvalidEvidence)?;
        let task = primary
            .task
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?;
        let retained_limits = task.envelope_limits(limits);
        let mut workspace = Workspace::open(
            primary
                .path
                .parent()
                .ok_or(ManagementError::InvalidEvidence)?,
        )?;
        let mut spools: Vec<InventorySpool> = Vec::new();
        let mut union = None;
        let opened=async {
            // A final local tail read is outside every target transaction.
            let source=primary.checkpoint().await?;
            cache_guard::check(&primary.db).await?;
            for original in &plan.retirement_sources {
                cache_guard::install(&original.db,DERIVED).await?;
                let name=name(original.reference())?;
                let path=workspace.named(&name)?;
                let fresh=!path.try_exists().map_err(|_| ManagementError::Database)?;
                let mut spool=if fresh {
                    let (path,file)=workspace.reserve_named(&name).await?;
                    let mut spool=InventorySpool::create(&path,file,&source.rewind()).await?;
                    spool.reference=original.reference.clone();
                    exec(&spool.db,"UPDATE state SET reference=$1 WHERE id=1",vec![encode(spool.reference())?.into()]).await?;
                    spool
                } else {
                    drop(workspace.open_named(&name)?);
                    let spool=InventorySpool::resume_task(&path).await?;
                    if one(&spool.db,"SELECT 1 AS present FROM sqlite_master WHERE name='canonical_cache_guard' AND type='table'",vec![]).await?.is_none() {
                        spool.close().await?;return Err(ManagementError::InvalidEvidence);
                    }
                    spool
                };
                if spool.reference()!=original.reference() {spool.close().await?;return Err(ManagementError::Changed);}
                spool.share_original(primary,original.checkpoint().await?.route,fresh).await?;
                if fresh {spool.adopt_shared_scan().await?;}
                cache_guard::install(&spool.db,DERIVED).await?;
                spool.source_scanned=true;
                spool.task=primary.task.clone();
                spool::task_run(&spool.db,&retained_limits).await?;
                task_budget::TaskBudget::forward_cache(&spool.db,false).await?;
                spools.push(spool);
            }
            let name=union_name(primary.reference())?;
            let path=workspace.named(&name)?;
            let fresh=!path.try_exists().map_err(|_| ManagementError::Database)?;
            let merged=if fresh {
                let (path,file)=workspace.reserve_named(&name).await?;
                PlanUnion::create(&path,file).await?
            } else {
                drop(workspace.open_named(&name)?);
                PlanUnion::resume_task(&path).await?
            };
            if !fresh && one(&merged.db,"SELECT 1 AS present FROM sqlite_master WHERE name='canonical_cache_guard' AND type='table'",vec![]).await?.is_none() {
                merged.close().await?;return Err(ManagementError::InvalidEvidence);
            }
            cache_guard::install(&merged.db,UNION_TABLES).await?;
            merged.db.execute(Statement::from_sql_and_values(DatabaseBackend::Sqlite,"ATTACH DATABASE $1 AS original",
                [primary.path.to_str().ok_or(ManagementError::InvalidEvidence)?.into()])).await.map_err(|_| ManagementError::Database)?;
            merged.start_task(&retained_limits).await?;
            task_budget::TaskBudget::forward_cache(&merged.db,true).await?;
            union=Some(merged);
            Ok(())
        }.await;
        if let Err(error) = opened {
            planning::close_resources(None, &spools, union).await?;
            return Err(error);
        }
        Ok(Self {
            workspace,
            spools,
            union: union.ok_or(ManagementError::InvalidEvidence)?,
        })
    }

    pub(super) async fn advance(
        &mut self,
        engine: &mut ConfirmationEngine<'_>,
        archive: &ProgressArchive<'_>,
        auth: &LocalAuthorization,
        review: (ReviewedPlan<'_>, &ConfigurationBinding),
        limits: &RunLimits,
        update: ConfirmationUpdate<'_>,
    ) -> Result<ConfirmationProgress> {
        let ProgressArchive { db, id, header } = *archive;
        let (
            ReviewedPlan {
                request,
                snapshot,
                plan: original,
            },
            configuration,
        ) = review;
        let ConfirmationUpdate {
            position,
            state,
            effects,
            phase,
        } = update;
        let primary = original
            .retirement_sources
            .first()
            .ok_or(ManagementError::InvalidEvidence)?;
        let task = primary
            .task
            .as_ref()
            .ok_or(ManagementError::InvalidEvidence)?;
        loop {
            cache_guard::check(&primary.db).await?;
            state.metered_before = Some(coordinates(primary).await?);
            match state.stage {
                Stage::Source | Stage::Wire => return Err(ManagementError::InvalidEvidence),
                Stage::Reduce => {
                    *phase = ConfirmationPhase::Canonical;
                    if let Some(spool) = self.spools.get(state.provider) {
                        task_budget::TaskBudget::forward_cache(&spool.db, false).await?;
                        let caps = task.caps(spool, limits).await?;
                        let reduced = spool.reduce(configuration, &caps).await;
                        task_budget::TaskBudget::forward_cache(&spool.db, false).await?;
                        if reduced? == Progress::Pending {
                            return Ok(ConfirmationProgress::Incomplete);
                        }
                        state.provider += 1;
                    } else {
                        state.stage = Stage::Compare;
                        state.provider = 0;
                    }
                    save(
                        engine,
                        archive,
                        ConfirmationUpdate {
                            position,
                            state,
                            effects,
                            phase,
                        },
                        &[],
                        primary,
                        limits,
                    )
                    .await?;
                }
                Stage::Compare | Stage::UnionCompare => {
                    *phase = ConfirmationPhase::Comparing;
                    let union = state.stage == Stage::UnionCompare;
                    let caps = task.caps(primary, limits).await?;
                    let mut run = spool::task_run(&primary.db, &caps).await?;
                    if !run.page(0, 0)? {
                        return Ok(ConfirmationProgress::Incomplete);
                    }
                    // Both bounded reads, including a rejected page, consume the
                    // existing task envelope before their target-DB CAS attempt.
                    spool::budget::charge(&primary.db, &run, 512, 2 * 1024 * 1024).await?;
                    let (left, right) = if union {
                        (
                            &original
                                .retirement_union
                                .as_ref()
                                .ok_or(ManagementError::InvalidEvidence)?
                                .db,
                            &self.union.db,
                        )
                    } else if let (Some(left), Some(right)) = (
                        original.retirement_sources.get(state.provider),
                        self.spools.get(state.provider),
                    ) {
                        (&left.db, &right.db)
                    } else {
                        state.stage = Stage::Union;
                        state.provider = 0;
                        save(
                            engine,
                            archive,
                            ConfirmationUpdate {
                                position,
                                state,
                                effects,
                                phase,
                            },
                            &[],
                            primary,
                            limits,
                        )
                        .await?;
                        continue;
                    };
                    let (cursor, rows, done) =
                        comparison::compare(left, right, &state.comparison, union).await?;
                    let payload =
                        serde_json::to_vec(&rows).map_err(|_| ManagementError::InvalidEvidence)?;
                    spool::budget::copy_work(&primary.db, &run, payload.len()).await?;
                    state.comparison = cursor;
                    if done {
                        state.comparison = Default::default();
                        if union {
                            state.stage = Stage::Archive;
                            state.provider = 0;
                        } else {
                            state.provider += 1;
                        }
                    }
                    save(
                        engine,
                        archive,
                        ConfirmationUpdate {
                            position,
                            state,
                            effects,
                            phase,
                        },
                        &payload,
                        primary,
                        limits,
                    )
                    .await?;
                }
                Stage::Union => {
                    *phase = ConfirmationPhase::Canonical;
                    let mut reviews = Vec::new();
                    for spool in &self.spools {
                        cache_guard::check(&spool.db).await?;
                        reviews.push(spool.frozen_review().await?);
                    }
                    task_budget::TaskBudget::forward_cache(&self.union.db, true).await?;
                    let caps = task.union_caps(&self.union, limits).await?;
                    let plan = super::super::super::dry_run_retirement(
                        request,
                        snapshot,
                        &reviews,
                        &planning::mutation_scopes(request, snapshot),
                        &self.union,
                        &caps,
                    )
                    .await
                    .map_err(planning::safe);
                    for spool in &self.spools {
                        task_budget::TaskBudget::forward_cache(&spool.db, false).await?;
                    }
                    task_budget::TaskBudget::forward_cache(&self.union.db, true).await?;
                    let Some(plan) = plan? else {
                        return Ok(ConfirmationProgress::Incomplete);
                    };
                    if &plan != original {
                        return Err(ManagementError::Changed);
                    }
                    state.canonical = Some(CanonicalPlan {
                        items: plan.items().to_vec(),
                        summary: *plan.summary(),
                    });
                    state.stage = Stage::UnionCompare;
                    save(
                        engine,
                        archive,
                        ConfirmationUpdate {
                            position,
                            state,
                            effects,
                            phase,
                        },
                        &[],
                        primary,
                        limits,
                    )
                    .await?;
                }
                Stage::Archive => {
                    *phase = ConfirmationPhase::Archiving;
                    let canonical = state
                        .canonical
                        .as_ref()
                        .ok_or(ManagementError::InvalidEvidence)?;
                    let work = records::SharedDecodeBudget::default();
                    for spool in &self.spools {
                        cache_guard::check(&spool.db).await?;
                    }
                    let published = records::stage_canonical_inventory(
                        db,
                        auth,
                        request,
                        &canonical.items,
                        (&self.spools, limits),
                        &work,
                        effects,
                    )
                    .await?;
                    if !published {
                        if limits.pages_remaining() > 0 {
                            continue;
                        }
                        return Ok(ConfirmationProgress::Incomplete);
                    }
                    if !records::stage_canonical_output(
                        db,
                        auth,
                        request,
                        &canonical.items,
                        (&self.spools, limits, &work, effects),
                        snapshot,
                        canonical.summary,
                    )
                    .await?
                    {
                        if limits.pages_remaining() > 0 {
                            continue;
                        }
                        return Ok(ConfirmationProgress::Incomplete);
                    }
                    state.stage = Stage::Ready;
                    save(
                        engine,
                        archive,
                        ConfirmationUpdate {
                            position,
                            state,
                            effects,
                            phase,
                        },
                        &[],
                        primary,
                        limits,
                    )
                    .await?;
                }
                Stage::Ready => {
                    *phase = ConfirmationPhase::Ready;
                    let canonical = state
                        .canonical
                        .as_ref()
                        .ok_or(ManagementError::InvalidEvidence)?;
                    return Ok(ConfirmationProgress::Ready(Box::new(ReadyConfirmation {
                        task: task.clone(),
                        plan: DryRunPlan::from_canonical(
                            request.plan_id.clone(),
                            canonical.items.clone(),
                            canonical.summary,
                        ),
                        archive_id: id.into(),
                        header: header.into(),
                        position: position.clone(),
                        origin: state.source.source.clone(),
                        request: request.clone(),
                        snapshot: snapshot.clone(),
                        principal: auth.principal().into(),
                    })));
                }
            }
        }
    }
    pub(super) async fn close(self) -> Result<()> {
        let mut result = Ok(());
        for spool in &self.spools {
            if let Err(error) = task_budget::TaskBudget::forward_cache(&spool.db, false).await {
                result = Err(error);
            }
        }
        if let Err(error) = task_budget::TaskBudget::forward_cache(&self.union.db, true).await {
            result = Err(error);
        }
        if let Err(error) = planning::close_resources(None, &self.spools, Some(self.union)).await {
            result = Err(error);
        }
        drop(self.workspace);
        result
    }
}

pub(super) async fn save(
    engine: &mut ConfirmationEngine<'_>,
    archive: &ProgressArchive<'_>,
    update: ConfirmationUpdate<'_>,
    payload: &[u8],
    primary: &InventorySpool,
    limits: &RunLimits,
) -> Result<()> {
    let ProgressArchive { id, header, .. } = *archive;
    let ConfirmationUpdate {
        position,
        state,
        effects,
        ..
    } = update;
    state.metered_after = Some(coordinates(primary).await?);
    let mut tx = engine
        .begin(Run::new(limits).remaining(), limits.cancellation.clone())
        .await
        .map_err(TransactionFailure::management_error)?;
    let id_copy = id.to_owned();
    let header_copy = header.to_owned();
    let expected = position.clone();
    let state = state.clone();
    let data = payload.to_vec();
    let next = tx
        .run(move |tx, _| {
            Box::pin(async move {
                native::scope::writer_boundary(tx).await?;
                if progress::load(tx, &id_copy, &header_copy, true)
                    .await?
                    .as_ref()
                    != Some(&expected)
                {
                    return Err(ManagementError::Changed);
                }
                state.source.source.check(tx).await?;
                let next = progress::append(
                    tx,
                    &id_copy,
                    &header_copy,
                    &expected,
                    &encode(&state)?,
                    &data,
                    state.stage == Stage::Ready,
                )
                .await?;
                state.source.source.check(tx).await?;
                Ok(next)
            })
        })
        .await
        .map_err(|error| staging_failure(error, effects))?;
    commit(archive, tx, Some(position), &next, payload, effects).await?;
    *position = next;
    Ok(())
}
