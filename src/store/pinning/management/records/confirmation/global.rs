use super::*;
use crate::pinning::management::retirement::{
    ConfirmationAdvance, ConfirmationEffects, ConfirmationPhase, ConfirmationProgress,
    ReadyConfirmation, RunLimits, SourceTransition,
};
use crate::store::pinning::management::transaction::{
    FINAL_WRITE_LIMIT, ManagementTransaction, TransactionFailure,
};

pub struct ReadyPublication<'a> {
    pub ready: &'a ReadyConfirmation,
    pub acknowledgements: &'a [String],
    pub ttl: chrono::Duration,
}

pub(in crate::store::pinning::management::records) async fn confirm(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    (request, snapshot, plan): (&PlanRequest, &Snapshot, &DryRunPlan),
    acks: &[String],
    ttl: chrono::Duration,
    work: SharedDecodeBudget,
) -> Result<ConfirmedPlan> {
    let deadline = std::time::Instant::now() + RunLimits::default().elapsed;
    let ready = loop {
        let mut limits = RunLimits::default();
        limits.elapsed = deadline.saturating_duration_since(std::time::Instant::now());
        if limits.elapsed.is_zero() {
            return Err(ManagementError::Budget);
        }
        let advanced = management::retirement::confirmation::advance(
            db, config, auth, request, snapshot, plan, &limits,
        )
        .await;
        match advanced.progress? {
            ConfirmationProgress::Ready(ready) => break ready,
            ConfirmationProgress::Incomplete => {}
            ConfirmationProgress::Confirmed(confirmed) => return Ok(*confirmed),
        }
    };
    let mut limits = RunLimits::default();
    limits.elapsed = deadline.saturating_duration_since(std::time::Instant::now());
    final_inner(
        db,
        config,
        auth,
        ReadyPublication {
            ready: &ready,
            acknowledgements: acks,
            ttl,
        },
        work,
        &limits,
        &mut ConfirmationEffects::default(),
    )
    .await
}

pub async fn confirm_ready(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    publication: ReadyPublication<'_>,
    work: SharedDecodeBudget,
    limits: &RunLimits,
) -> ConfirmationAdvance {
    let mut effects = ConfirmationEffects::default();
    let result = final_inner(db, config, auth, publication, work, limits, &mut effects).await;
    ConfirmationAdvance {
        phase: if result.is_ok() {
            ConfirmationPhase::Confirmed
        } else {
            ConfirmationPhase::Ready
        },
        effects,
        progress: result.map(|confirmed| ConfirmationProgress::Confirmed(Box::new(confirmed))),
    }
}

async fn final_inner(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    publication: ReadyPublication<'_>,
    work: SharedDecodeBudget,
    limits: &RunLimits,
    effects: &mut ConfirmationEffects,
) -> Result<ConfirmedPlan> {
    let ready = publication.ready;
    let (mut accounting, mut budget) = ready.task.begin_output(limits).await?;
    let result = async {
        struct Count(u64);
        impl std::io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self
                    .0
                    .checked_add(bytes.len() as u64)
                    .ok_or(std::io::ErrorKind::InvalidData)?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut count = Count(0);
        serde_json::to_writer(&mut count, &(&ready.snapshot, ready.plan.items()))
            .map_err(|_| ManagementError::InvalidEvidence)?;
        let items = ready.request.items.len() as u64;
        budget.charge(
            items * 8 + 8,
            items * 6 + native::current_catalog().len() as u64,
            0,
            count.0.checked_mul(4).ok_or(ManagementError::Budget)?,
        )?;
        accounting.persist(budget.work).await?;
        final_transaction(db, config, auth, publication, work, limits, effects).await
    }
    .await;
    let accounted = accounting.finish(budget.work).await;
    match (result, accounted) {
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

async fn final_transaction(
    db: &DatabaseConnection,
    config: &ManagementConfig,
    auth: &LocalAuthorization,
    publication: ReadyPublication<'_>,
    work: SharedDecodeBudget,
    limits: &RunLimits,
    effects: &mut ConfirmationEffects,
) -> Result<ConfirmedPlan> {
    let ReadyPublication {
        ready,
        acknowledgements: acks,
        ttl,
    } = publication;
    let request = &ready.request;
    let snapshot = &ready.snapshot;
    let plan = &ready.plan;
    auth.check(db).await?;
    management::validate_request(request).map_err(|_| ManagementError::Confirmation)?;
    reject_converted_unexecuted(db, auth, request).await?;
    if ttl <= chrono::Duration::zero() || ttl > chrono::Duration::minutes(15) {
        return Err(ManagementError::Confirmation);
    }
    let configuration = management::ConfigurationBinding {
        routes: config
            .pinning
            .providers
            .iter()
            .map(|p| p.route_snapshot())
            .collect(),
        policies: config.pinning.policies.clone(),
        limits: config
            .pinning
            .provider_limits
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        zip_rules: Some(config.zip_rules.clone()),
        source_node: config.source_node().map(str::to_owned),
    };
    if ready.principal != auth.principal()
        || snapshot.configuration.as_ref() != Some(&configuration)
    {
        return Err(ManagementError::Changed);
    }
    let acknowledged =
        management::plan::validate_acks(plan, acks).map_err(|_| ManagementError::Confirmation)?;
    if plan.items().iter().any(|entry| {
        matches!(entry.status, PlanStatus::Blocked(_))
            || entry.status == PlanStatus::Candidate
                && !acknowledged.contains(&entry.request.item_id)
    }) {
        return Err(ManagementError::Confirmation);
    }
    let checkpoint = ready.origin.clone();
    let mut transaction = ManagementTransaction::begin(
        db,
        FINAL_WRITE_LIMIT.min(limits.remaining()),
        limits.cancellation.clone(),
    )
    .await
    .map_err(TransactionFailure::management_error)?;
    let executed=transaction.run_with((ready,auth,request,snapshot,plan,acks,work.clone()),move |tx,deadline,(ready,auth,request,snapshot,plan,acks,work)|Box::pin(async move {
        lock(tx).await?;
        ready.check(tx).await?;
        inventory_review::bind_canonical_archive(tx,auth,request,plan.items(),&checkpoint,work).await?;
        let now=crate::store::database_clock::database_now(tx).await.map_err(|_|ManagementError::Database)?;
        let expires_at=now.checked_add_signed(ttl).ok_or(ManagementError::Expired)?;
        let before=archive::encode_proposal(auth,request,snapshot,plan,now,expires_at,work)?;
        let (reference,binding)={let mut budget=work.lock().map_err(|_|ManagementError::InvalidEvidence)?;
            (archive::encode_locator(&before,&mut budget)?,archive::encode_confirmed_proposal(&before,now,acks,&mut budget)?)};
        let anchor=&plan.items().first().ok_or(ManagementError::Confirmation)?.request.item_id;
        let mut changes=Vec::new();
        for item in &request.items {
            let key=native::NativeKey {table:super::super::super::schema::RECORDS.into(),values:vec![auth.database_scope.clone(),request.plan_id.clone(),item.item_id.clone(),"action".into()]};
            let empty=native::scope::collect_scope(tx,&native::scope::ScopeSpec::new(vec![native::scope::Selector::Exact(key.clone())])?,&mut native::meter::Budget::default()).await?;
            if !empty.rows().is_empty() {return Err(ManagementError::Confirmation)}
            let original=if &item.item_id==anchor {&before}else{&reference};
            tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),
                "INSERT INTO pin_management_records(database_scope,plan_id,item_id,kind,native_identity,principal,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) VALUES($1,$2,$3,'action',$4,$5,$6,$7,NULL,'[]','[]','prepared',$8,$9,NULL)",
                [auth.database_scope.clone().into(),request.plan_id.clone().into(),item.item_id.clone().into(),auth.database_identity.clone().into(),auth.principal.clone().into(),binding.clone().into(),original.clone().into(),now.into(),expires_at.into()])).await.map_err(|_|ManagementError::Database)?;
            let values=key.values.iter().map(String::as_str).collect::<Vec<_>>();
            let row=current_row(tx,&key.table,&values,DecodeWork::shared(work)).await?;
            for (column,expected) in [("database_scope",auth.database_scope.as_str()),("plan_id",request.plan_id.as_str()),("item_id",item.item_id.as_str()),("kind","action"),("native_identity",auth.database_identity.as_str()),("principal",auth.principal.as_str()),("binding",binding.as_str()),("native_before",original.as_str()),("intent_ids","[]"),("job_ids","[]"),("state","prepared")] {
                if native::value(&row,column)!=Some(expected) {return Err(ManagementError::Changed)}
            }
            if ["result","completed_at"].iter().any(|column|row.get(*column)!=Some(&None))||native::parse_time(native::value(&row,"created_at").ok_or(ManagementError::InvalidEvidence)?)?!=now||native::parse_time(native::value(&row,"expires_at").ok_or(ManagementError::InvalidEvidence)?)?!=expires_at {return Err(ManagementError::Changed)}
            changes.push(SourceTransition {key,before:None,after:Some(row),writes:1});
        }
        let keys=changes.iter().map(|change|change.key.clone()).collect::<Vec<_>>();
        seal_originals(tx,&keys,DecodeWork::shared(work)).await?;
        inventory_review::publish_transition(tx,auth,request,&checkpoint,changes).await?;
        deadline.business_expiry(tx,expires_at).await?;
        Ok(())
    })).await;
    if let Err(failure) = executed {
        effects.commit_unknown = !failure.rollback_confirmed;
        return Err(failure.management_error());
    }
    if let Err(failure) = transaction.commit().await {
        match super::super::authority::load_confirmed_authority(db, auth, request, work.clone())
            .await
        {
            Ok(confirmed)
                if confirmed.confirmation.bindings.as_slice() == plan.items()
                    && confirmed.confirmation.candidate_item_ids.as_slice() == acks =>
            {
                effects.database_commits += 1;
                return Ok(confirmed);
            }
            _ => {
                effects.commit_unknown = true;
                return Err(failure.management_error());
            }
        }
    }
    effects.database_commits += 1;
    super::super::authority::load_confirmed_authority(db, auth, request, work).await
}
