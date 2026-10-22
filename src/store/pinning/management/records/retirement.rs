use super::*;
use crate::pinning::management::{Capture, PlannedStep, Subject};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetirementState {
    Retired,
    NeedsAttention,
}
impl RetirementState {
    fn sql(self) -> &'static str {
        match self {
            Self::Retired => "retired",
            Self::NeedsAttention => "needs_attention",
        }
    }
}

/// Classification supplies only a route locator. It never yields executable authority.
pub(crate) fn classify_retirement_root(
    source: &evidence::Evidence,
    row: &native::NativeRow,
    action: &native::NativeRow,
) -> Result<ActionReceipt> {
    let binding = archive::classify(
        source,
        action,
        native::value(row, "inventory").ok_or(ManagementError::InvalidEvidence)?,
        &mut *source.budget()?,
    )?;
    let (receipt, _) = results::result_from_row(source, action)?;
    classify_retirement_binding(row, action, binding.view(), receipt)
}
fn classify_retirement_binding(
    row: &native::NativeRow,
    action: &native::NativeRow,
    binding: BindingView<'_>,
    receipt: ActionReceipt,
) -> Result<ActionReceipt> {
    let text = |column| native::value(row, column).ok_or(ManagementError::InvalidEvidence);
    let action_text =
        |column| native::value(action, column).ok_or(ManagementError::InvalidEvidence);
    if text("record_kind")? != "action"
        || !matches!(text("state")?, "retired" | "needs_attention")
        || action_text("state")? != "applied"
        || native::value(action, "completed_at").is_none()
        || text("binding")? != action_text("binding")?
    {
        return Err(ManagementError::Confirmation);
    }
    for column in [
        "database_scope",
        "plan_id",
        "item_id",
        "principal",
        "native_identity",
    ] {
        if text(column)? != action_text(column)? {
            return Err(ManagementError::Confirmation);
        }
    }
    if binding.database_scope != text("database_scope")?
        || binding.database_identity != text("native_identity")?
        || binding.principal != text("principal")?
        || binding.principal != "local_configuration"
        || binding.request.plan_id != text("plan_id")?
        || binding.confirmation.plan_id != &binding.request.plan_id
    {
        return Err(ManagementError::Confirmation);
    }
    let entries = binding
        .confirmation
        .bindings
        .iter()
        .filter(|entry| entry.request.item_id == text("item_id").unwrap_or(""))
        .collect::<Vec<_>>();
    if entries.len() != 1
        || *entries[0] != receipt.binding
        || serde_json::from_str::<Vec<String>>(action_text("intent_ids")?)
            .map_err(|_| ManagementError::InvalidEvidence)?
            != receipt.intent_ids
        || serde_json::from_str::<Vec<String>>(action_text("job_ids")?)
            .map_err(|_| ManagementError::InvalidEvidence)?
            != receipt.job_ids
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(receipt)
}

pub(crate) fn retirement_review_from_source(
    source: &evidence::Evidence,
    row: &native::NativeRow,
    action: &native::NativeRow,
) -> Result<archive::ReviewHandle> {
    classify_retirement_root(source, row, action)?;
    let trusted = results::trusted_from_record(
        source,
        native::value(row, "database_scope").ok_or(ManagementError::InvalidEvidence)?,
        native::value(row, "native_identity").ok_or(ManagementError::InvalidEvidence)?,
        action,
    )?
    .ok_or(ManagementError::Confirmation)?;
    let review = archive::load_review(source, action, &mut *source.budget()?)?;
    let entry = &trusted.action_view().receipt.binding;
    let disposition = match confirmed_disposition(&entry.request.action)? {
        Disposition::Retain => "retain",
        Disposition::Migrate => "migrate",
        Disposition::ControlledCleanup => "controlled_cleanup",
    };
    if native::value(row, "disposition") != Some(disposition) {
        return Err(ManagementError::Confirmation);
    }
    let (Some(Capture::Provider(capture)), Some(PlannedStep::Retire(step))) =
        (&entry.capture, &entry.step)
    else {
        return Err(ManagementError::Confirmation);
    };
    let unresolved: Vec<String> = source
        .budget()?
        .typed(native::value(row, "unresolved").ok_or(ManagementError::InvalidEvidence)?)?;
    inventory_review::require(
        DecodeWork::shared(&source.work),
        native::value(row, "database_scope").ok_or(ManagementError::InvalidEvidence)?,
        native::value(row, "native_identity").ok_or(ManagementError::InvalidEvidence)?,
        native::value(row, "principal").ok_or(ManagementError::InvalidEvidence)?,
        native::value(row, "plan_id").ok_or(ManagementError::InvalidEvidence)?,
        entry,
    )?;
    if !step.stop_new_allocations
        || unresolved.len() > native::MAX_ROWS
        || native::value(row, "state") == Some("retired")
            && (capture
                .mutation_scope
                .remotes
                .iter()
                .chain(&capture.mutation_scope.historical_remotes)
                .any(|remote| remote.has_unknown_responsibility())
                || !capture.totals.attribution_complete
                || step.needs_attention
                || step.unknown > 0
                || step.inflight > 0
                || step.cleanup_pending > 0
                || !unresolved.is_empty())
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(review)
}

/// Records a disposition beside the already-written exact action receipt in the
/// same transaction. This is not admission enforcement or a remote cleanup call.
pub async fn write_retirement_in_transaction(
    tx: &DatabaseTransaction,
    auth: &LocalAuthorization,
    confirmation: &ConfirmedBinding,
    item_id: &str,
    allocation_domain: &str,
    disposition: Disposition,
    unresolved: Vec<String>,
) -> Result<RetirementRecord> {
    let work = SharedDecodeBudget::default();
    let review = read_confirmed_plan(tx, confirmation, work).await?;
    let written = write_retirement_marker(
        tx,
        auth,
        review.view(),
        item_id,
        allocation_domain,
        disposition,
        unresolved,
    )
    .await?;
    seal_originals(
        tx,
        &[native::NativeKey {
            table: "pin_management_retirements".into(),
            values: vec![allocation_domain.into()],
        }],
        review.view().decode_work(),
    )
    .await?;
    let inventory = review.review();
    review.view().copy(&(
        allocation_domain,
        auth.database_scope(),
        &confirmation.request.plan_id,
        item_id,
        auth.principal(),
        auth.database_identity(),
    ))?;
    let mut storage_work = native::meter::Budget::default();
    let published = locator::load_authority(
        tx,
        &[locator::AuthoritySelector::RetirementDomain(
            allocation_domain.into(),
        )],
        &mut storage_work,
    )
    .await;
    charge_storage_work(&storage_work, review.view().decode_work())?;
    if !published?.originals.contains_key(&native::NativeKey {
        table: super::super::schema::RETIREMENTS.into(),
        values: vec![allocation_domain.into()],
    }) {
        return Err(ManagementError::Changed);
    }
    Ok(RetirementRecord {
        allocation_domain: allocation_domain.into(),
        database_scope: auth.database_scope.clone(),
        plan_id: confirmation.request.plan_id.clone(),
        item_id: item_id.into(),
        principal: auth.principal.clone(),
        native_identity: auth.database_identity.clone(),
        binding: confirmation.clone(),
        inventory,
        disposition,
        unresolved: written.unresolved,
        state: written.state,
        created_at: written.created_at,
    })
}
pub(crate) struct RetirementWrite {
    pub(crate) state: RetirementState,
    unresolved: Vec<String>,
    created_at: chrono::DateTime<chrono::Utc>,
}
pub(crate) async fn write_retirement_marker(
    tx: &DatabaseTransaction,
    auth: &LocalAuthorization,
    confirmation: BindingView<'_>,
    item_id: &str,
    allocation_domain: &str,
    disposition: Disposition,
    unresolved: Vec<String>,
) -> Result<RetirementWrite> {
    auth.check(tx).await?;
    if confirmation.principal != auth.principal()
        || confirmation.database_scope != auth.database_scope()
        || confirmation.database_identity != auth.database_identity()
        || confirmation.confirmation.plan_id != &confirmation.request.plan_id
    {
        return Err(ManagementError::Unauthorized);
    }
    let created_at = crate::store::database_clock::database_now(tx)
        .await
        .map_err(|_| ManagementError::Database)?;
    if created_at < confirmation.confirmed_at
        || created_at >= confirmation.expires_at
        || confirmation.expires_at <= confirmation.confirmed_at
    {
        return Err(ManagementError::Expired);
    }
    let entry = confirmation
        .confirmation
        .bindings
        .iter()
        .find(|entry| entry.request.item_id == item_id)
        .ok_or(ManagementError::Confirmation)?;
    let (
        Some(Capture::Provider(capture)),
        Some(PlannedStep::Retire(step)),
        Subject::Provider { route },
    ) = (&entry.capture, &entry.step, &entry.request.subject)
    else {
        return Err(ManagementError::Confirmation);
    };
    if confirmed_disposition(&entry.request.action)? != disposition {
        return Err(ManagementError::Confirmation);
    }
    let inventory = confirmation.frozen_rows()?;
    let original = route
        .original
        .as_ref()
        .ok_or(ManagementError::InvalidEvidence)?;
    let registered = inventory
        .row("pin_provider_routes", &[allocation_domain])
        .and_then(|row| row.cell("snapshot"))
        .map(|raw| confirmation.decode_route(raw))
        .transpose()?
        .as_deref()
        == Some(original);
    inventory_review::require(
        confirmation.decode_work(),
        auth.database_scope(),
        auth.database_identity(),
        auth.principal(),
        &confirmation.request.plan_id,
        entry,
    )?;
    if !registered
        || !step.stop_new_allocations
        || unresolved.len() > native::MAX_ROWS
        || unresolved
            .iter()
            .any(|id| id.is_empty() || id.len() > 256 || id.chars().any(char::is_control))
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut unresolved = unresolved;
    for remote in capture
        .mutation_scope
        .remotes
        .iter()
        .chain(&capture.mutation_scope.historical_remotes)
        .filter(|remote| remote.has_unknown_responsibility())
    {
        // Exact captured coordinates, not an invented UUID/hash/count proof.
        confirmation.copy(&(&remote.key, remote.epoch, &remote.resource_id))?;
        let locator = serde_json::to_string(&(&remote.key, remote.epoch, &remote.resource_id))
            .map_err(|_| ManagementError::InvalidEvidence)?;
        unresolved.push(locator);
    }
    unresolved.sort();
    unresolved.dedup();
    if unresolved.len() > native::MAX_ROWS {
        return Err(ManagementError::Budget);
    }
    let state = if step.needs_attention
        || step.unknown > 0
        || step.inflight > 0
        || step.cleanup_pending > 0
        || !unresolved.is_empty()
    {
        RetirementState::NeedsAttention
    } else {
        RetirementState::Retired
    };
    confirmation.copy(&confirmation.request.plan_id)?;
    let source = evidence::Evidence::load_with_work(
        tx,
        &auth.database_scope,
        &auth.database_identity,
        std::slice::from_ref(&confirmation.request.plan_id),
        confirmation.decode_work().budget().clone(),
    )
    .await?;
    let root = source
        .archive_members(auth.database_scope(), &confirmation.request.plan_id)
        .next()
        .ok_or(ManagementError::Confirmation)?;
    let stored = source.read_binding(root)?;
    if stored.view() != confirmation {
        return Err(ManagementError::Confirmation);
    }
    let binding = native::value(root, "binding").ok_or(ManagementError::InvalidEvidence)?;
    confirmation.copy(binding)?;
    let binding = binding.to_owned();
    let anchor = native::value(
        source.row(&evidence::view_anchor_key(confirmation)?)?,
        "native_before",
    )
    .ok_or(ManagementError::InvalidEvidence)?;
    let inventory = archive::encode_inventory(anchor, &mut *source.budget()?)?;
    confirmation.copy(&unresolved)?;
    let encoded_unresolved =
        serde_json::to_string(&unresolved).map_err(|_| ManagementError::InvalidEvidence)?;
    let disposition = match disposition {
        Disposition::Retain => "retain",
        Disposition::Migrate => "migrate",
        Disposition::ControlledCleanup => "controlled_cleanup",
    };
    confirmation.copy(&(
        allocation_domain,
        auth.database_scope(),
        &confirmation.request.plan_id,
        item_id,
        auth.principal(),
        auth.database_identity(),
    ))?;
    tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"INSERT INTO pin_management_retirements (allocation_domain,database_scope,plan_id,item_id,principal,native_identity,binding,inventory,disposition,unresolved,state,created_at,record_kind) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'action')",[allocation_domain.into(),auth.database_scope().into(),confirmation.request.plan_id.clone().into(),item_id.into(),auth.principal().into(),auth.database_identity().into(),binding.into(),inventory.into(),disposition.into(),encoded_unresolved.into(),state.sql().into(),created_at.into()])).await.map_err(|_|ManagementError::Database)?;
    Ok(RetirementWrite {
        state,
        unresolved,
        created_at,
    })
}

pub async fn read_retirement<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    allocation_domain: &str,
) -> Result<Option<RetirementRecord>> {
    read_retirement_with_session(db, auth, allocation_domain, SharedDecodeBudget::default()).await
}
pub async fn read_retirement_with_session<C: ConnectionTrait>(
    db: &C,
    auth: &LocalAuthorization,
    allocation_domain: &str,
    work: SharedDecodeBudget,
) -> Result<Option<RetirementRecord>> {
    auth.check(db).await?;
    super::super::schema::validate(db).await?;
    super::super::catalog::validate(db, native::current_catalog()).await?;
    let context = DecodeWork::shared(&work);
    context.copy(allocation_domain)?;
    let mut storage_work = native::meter::Budget::default();
    let selected = locator::load_authority(
        db,
        &[locator::AuthoritySelector::RetirementDomain(
            allocation_domain.into(),
        )],
        &mut storage_work,
    )
    .await;
    charge_storage_work(&storage_work, context)?;
    let selected = selected?;
    let key = native::NativeKey {
        table: super::super::schema::RETIREMENTS.into(),
        values: vec![allocation_domain.into()],
    };
    let Some(native_row) = selected.originals.get(&key) else {
        return Ok(None);
    };
    let text =
        |column: &str| native::value(native_row, column).ok_or(ManagementError::InvalidEvidence);
    if text("database_scope")? != auth.database_scope
        || text("principal")? != auth.principal
        || text("native_identity")? != auth.database_identity
        || text("record_kind")? != "action"
    {
        return Err(ManagementError::Unauthorized);
    }
    context.copy(&(text("plan_id")?, text("item_id")?))?;
    let plan_id = text("plan_id")?.to_owned();
    let item_id = text("item_id")?.to_owned();
    context.copy(&plan_id)?;
    let source = evidence::Evidence::load_with_work(
        db,
        &auth.database_scope,
        &auth.database_identity,
        std::slice::from_ref(&plan_id),
        work.clone(),
    )
    .await?;
    let action = source.row(&native::NativeKey {
        table: super::super::schema::RECORDS.into(),
        values: vec![
            auth.database_scope.clone(),
            plan_id.clone(),
            item_id.clone(),
            "action".into(),
        ],
    })?;
    if native::value(action, "binding") != Some(text("binding")?) {
        return Err(ManagementError::Confirmation);
    }
    let binding = decode_binding_from_row(&source, action)?;
    if binding.request.plan_id != plan_id {
        return Err(ManagementError::InvalidEvidence);
    }
    let inventory = retirement_review_from_source(&source, native_row, action)?;
    let disposition = match text("disposition")? {
        "retain" => Disposition::Retain,
        "migrate" => Disposition::Migrate,
        "controlled_cleanup" => Disposition::ControlledCleanup,
        _ => return Err(ManagementError::InvalidEvidence),
    };
    let state = match text("state")? {
        "retired" => RetirementState::Retired,
        "needs_attention" => RetirementState::NeedsAttention,
        _ => return Err(ManagementError::InvalidEvidence),
    };
    let unresolved: Vec<String> = source.budget()?.typed(text("unresolved")?)?;
    let entry = binding
        .confirmation
        .bindings
        .iter()
        .find(|entry| entry.request.item_id == item_id)
        .ok_or(ManagementError::InvalidEvidence)?;
    if confirmed_disposition(&entry.request.action)? != disposition {
        return Err(ManagementError::InvalidEvidence);
    }
    let (Some(Capture::Provider(capture)), Some(PlannedStep::Retire(step))) =
        (&entry.capture, &entry.step)
    else {
        return Err(ManagementError::InvalidEvidence);
    };
    let unknown = capture
        .mutation_scope
        .remotes
        .iter()
        .chain(&capture.mutation_scope.historical_remotes)
        .any(|remote| remote.has_unknown_responsibility());
    if state == RetirementState::Retired
        && (unknown
            || !capture.totals.attribution_complete
            || step.needs_attention
            || step.unknown > 0
            || step.inflight > 0
            || step.cleanup_pending > 0
            || !unresolved.is_empty())
    {
        return Err(ManagementError::InvalidEvidence);
    }
    let mut storage_work = native::meter::Budget::default();
    let checked = locator::check_authority(db, &selected, &mut storage_work).await;
    charge_storage_work(&storage_work, context)?;
    checked?;
    context.copy(&(
        allocation_domain,
        &auth.database_scope,
        &auth.principal,
        &auth.database_identity,
    ))?;
    Ok(Some(RetirementRecord {
        allocation_domain: allocation_domain.into(),
        database_scope: auth.database_scope.clone(),
        plan_id,
        item_id,
        principal: auth.principal.clone(),
        native_identity: auth.database_identity.clone(),
        binding,
        inventory,
        disposition,
        unresolved,
        state,
        created_at: native::parse_time(text("created_at")?)?,
    }))
}

pub(super) fn confirmed_disposition(
    action: &crate::pinning::management::Action,
) -> Result<Disposition> {
    use crate::pinning::management::{Action, RetirementSelection};
    match action {
        Action::Retire
        | Action::RetireWith {
            selection:
                RetirementSelection::Retain | RetirementSelection::ReleaseRootReferences { .. },
        } => Ok(Disposition::Retain),
        Action::RetireWith {
            selection: RetirementSelection::Migrate { .. },
        } => Ok(Disposition::Migrate),
        Action::RetireWith {
            selection: RetirementSelection::ControlledCleanup { .. },
        } => Ok(Disposition::ControlledCleanup),
        _ => Err(ManagementError::Confirmation),
    }
}
