use super::*;
use crate::pinning::management::{self as management, PlanStatus};
use batch::{Binding, StoredSource};
use std::sync::Arc;

pub(in crate::store::pinning::management::records) fn encode_locator(
    raw: &str,
    work: &mut DecodeBudget,
) -> Result<String> {
    let sections = Sections::parse(raw, work)?;
    let head: Head = work.typed(&raw[sections.head])?;
    let body = encoded(&(&head.3, &head.5, &head.6), work)?;
    work.bytes(LOCATOR.len() + body.len())?;
    Ok(format!("{LOCATOR}{body}"))
}
pub(in crate::store::pinning::management::records) fn encode_draft(
    raw: &str,
    work: &mut DecodeBudget,
) -> Result<String> {
    let sections = Sections::parse(raw, work)?;
    let head: Head = work.typed(&raw[sections.head])?;
    let body = encoded(&(0, &head.3, &head.5), work)?;
    work.bytes(BINDING.len() + body.len())?;
    Ok(format!("{BINDING}{body}"))
}
pub(in crate::store::pinning::management::records) fn encode_inventory(
    raw: &str,
    work: &mut DecodeBudget,
) -> Result<String> {
    let sections = Sections::parse(raw, work)?;
    let head: Head = work.typed(&raw[sections.head])?;
    let body = encoded(&(&head.3, &head.6), work)?;
    work.bytes(INVENTORY.len() + body.len())?;
    Ok(format!("{INVENTORY}{body}"))
}

pub(super) fn owner(
    source: &evidence::Evidence,
    root: &native::NativeRow,
    work: &mut DecodeBudget,
) -> Result<Arc<StoredSource>> {
    let raw = native::value(root, "native_before").ok_or(ManagementError::InvalidEvidence)?;
    let scope = native::value(root, "database_scope").ok_or(ManagementError::InvalidEvidence)?;
    let identity =
        native::value(root, "native_identity").ok_or(ManagementError::InvalidEvidence)?;
    let principal = native::value(root, "principal").ok_or(ManagementError::InvalidEvidence)?;
    source.archive_scope(scope, identity)?;
    let key = work.key(root)?;
    let key = Key(key.table, key.values);
    let original = source.archive_row(&key)?;
    if original.as_ref() != root {
        return Err(ManagementError::Changed);
    }
    if raw.starts_with(LOCATOR) {
        let locator = batch::locator(original, work)?;
        if locator.0.1.get(1) != key.1.get(1) {
            return Err(ManagementError::Confirmation);
        }
        let owner = batch::source(
            source.archive_row(&locator.0)?,
            &locator.0,
            scope,
            identity,
            principal,
            work,
        )?;
        if locator.1 != owner.head.5 || locator.2 != owner.head.6 {
            return Err(ManagementError::InvalidEvidence);
        }
        Ok(owner)
    } else {
        batch::source(original, &key, scope, identity, principal, work)
    }
}

pub(super) fn manifest(
    owner: &Arc<StoredSource>,
    work: &mut DecodeBudget,
) -> Result<(Arc<Manifest>, &'static [native::NativeTable])> {
    let manifest = batch::manifest(owner, work)?;
    let catalog = native::archived_catalog(&manifest.0)?;
    manifest.validate(&owner.head, catalog, work)?;
    Ok((manifest, catalog))
}

pub(super) fn core(owner: &Arc<StoredSource>, work: &mut DecodeBudget) -> Result<Arc<Core>> {
    let core = batch::core(owner, work)?;
    management::validate_request(&core.0).map_err(|_| ManagementError::Confirmation)?;
    if core.7 != owner.head.6
        || core.4 >= core.5
        || core.1.len() != core.0.items.len()
        || core
            .1
            .windows(2)
            .any(|pair| pair[0].request.item_id >= pair[1].request.item_id)
        || core.1.iter().any(|entry| {
            matches!(entry.status, PlanStatus::Blocked(_))
                || !core.0.items.contains(&entry.request)
                || entry
                    .id
                    .as_str()
                    .strip_prefix("management-v1:")
                    .and_then(|id| id.strip_prefix(&core.0.plan_id))
                    .and_then(|id| id.strip_prefix(':'))
                    != Some(entry.request.item_id.as_str())
        })
        || core.1.first().is_none_or(|entry| {
            owner.head.3.1[1] != core.0.plan_id || owner.head.3.1[2] != entry.request.item_id
        })
    {
        return Err(ManagementError::Confirmation);
    }
    Ok(core)
}

pub(super) fn members(
    source: &evidence::Evidence,
    root: &native::NativeRow,
    owner: &Arc<StoredSource>,
    core: &Core,
    work: &mut DecodeBudget,
) -> Result<Arc<Binding>> {
    let row = source.archive_row(&owner.head.3)?;
    let binding = batch::binding(row, work)?;
    let (key, hash, time, acks) = match binding.as_ref() {
        Binding::Draft(key, hash) => (key, hash, None, None),
        Binding::Confirmed(key, hash, time, acks) => (key, hash, Some(*time), Some(acks)),
    };
    if key != &owner.head.3 || hash != &owner.head.5 {
        return Err(ManagementError::Confirmation);
    }
    if let Some(time) = time {
        if time < core.4 || time >= core.5 {
            return Err(ManagementError::Confirmation);
        }
        let acks = acks.ok_or(ManagementError::Confirmation)?;
        if acks
            .iter()
            .enumerate()
            .any(|(index, id)| acks[..index].contains(id))
            || core
                .1
                .iter()
                .filter(|entry| entry.status == PlanStatus::Candidate)
                .count()
                != acks.len()
            || core
                .1
                .iter()
                .filter(|entry| entry.status == PlanStatus::Candidate)
                .any(|entry| !acks.contains(&entry.request.item_id))
        {
            return Err(ManagementError::Confirmation);
        }
    }
    let expected_raw = native::value(source.archive_row_ref(key)?, "binding")
        .ok_or(ManagementError::Confirmation)?;
    if native::value(root, "plan_id") != Some(&core.0.plan_id)
        || !native::value(root, "binding").is_some_and(|raw| binding_matches(raw, expected_raw))
    {
        return Err(ManagementError::Confirmation);
    }
    let mut count = 0;
    for (key, row) in source.archive_entries(&owner.head.0, &core.0.plan_id) {
        count += 1;
        batch::check_coordinates(
            row,
            &key.table,
            &key.values,
            &owner.head.0,
            &owner.head.1,
            &owner.head.2,
        )?;
        if !core
            .0
            .items
            .iter()
            .any(|item| item.item_id == key.values[2])
            || !native::value(row, "binding").is_some_and(|raw| binding_matches(raw, expected_raw))
            || native::parse_time(
                native::value(row, "expires_at").ok_or(ManagementError::Confirmation)?,
            )? != core.5
        {
            return Err(ManagementError::Confirmation);
        }
        let created = native::parse_time(
            native::value(row, "created_at").ok_or(ManagementError::Confirmation)?,
        )?;
        if created < core.4 || created >= core.5 || time.is_some_and(|time| created > time) {
            return Err(ManagementError::Confirmation);
        }
        match native::value(row, "state") {
            Some("prepared")
                if native::value(row, "result").is_none()
                    && native::value(row, "completed_at").is_none()
                    && native::value(row, "intent_ids") == Some("[]")
                    && native::value(row, "job_ids") == Some("[]") => {}
            Some("applied") if time.is_some() && native::value(row, "result").is_some() => {
                let completed = native::parse_time(
                    native::value(row, "completed_at").ok_or(ManagementError::Confirmation)?,
                )?;
                if completed < time.ok_or(ManagementError::Confirmation)?
                    || completed < created
                    || completed >= core.5
                {
                    return Err(ManagementError::Confirmation);
                }
            }
            _ => return Err(ManagementError::Confirmation),
        }
        if key.table == owner.head.3.0 && key.values == owner.head.3.1 {
            if native::value(row, "native_before") != Some(owner.raw()) {
                return Err(ManagementError::InvalidEvidence);
            }
        } else {
            let locator = batch::locator(row.clone(), work)?;
            if locator.0 != owner.head.3 || locator.1 != owner.head.5 || locator.2 != owner.head.6 {
                return Err(ManagementError::Confirmation);
            }
        }
    }
    if count != core.0.items.len() {
        return Err(ManagementError::Confirmation);
    }
    Ok(binding)
}

pub(in crate::store::pinning::management::records) fn read_binding(
    source: &evidence::Evidence,
    root: &native::NativeRow,
    work: &mut DecodeBudget,
) -> Result<ConfirmedBinding> {
    super::review::load_confirmed(source, root, work)
}

pub(in crate::store::pinning::management::records) fn validate_draft(
    source: &evidence::Evidence,
    root: &native::NativeRow,
    work: &mut DecodeBudget,
) -> Result<()> {
    let owner = owner(source, root, work)?;
    let core = core(&owner, work)?;
    if !matches!(
        members(source, root, &owner, &core, work)?.as_ref(),
        Binding::Draft(..)
    ) {
        return Err(ManagementError::Confirmation);
    }
    Ok(())
}

pub(in crate::store::pinning::management::records) fn dependencies(
    row: Arc<native::NativeRow>,
    work: &mut DecodeBudget,
) -> Result<Vec<native::NativeKey>> {
    let key = work.key(&row)?;
    let key = Key(key.table, key.values);
    let scope = native::value(&row, "database_scope").ok_or(ManagementError::InvalidEvidence)?;
    let id = native::value(&row, "native_identity").ok_or(ManagementError::InvalidEvidence)?;
    let principal = native::value(&row, "principal").ok_or(ManagementError::InvalidEvidence)?;
    let owner = batch::source(row.clone(), &key, scope, id, principal, work)?;
    let (manifest, _) = manifest(&owner, work)?;
    let mut keys = BTreeSet::new();
    work.nodes(1)?;
    for (table, rows) in &manifest.1 {
        for row in rows {
            if !keys
                .iter()
                .any(|key: &native::NativeKey| key.table == row.1.0 && key.values == row.1.1)
            {
                work.nodes(1)?;
                keys.insert(row.1.native(work)?);
            }
            if table == super::super::super::schema::RECORDS
                && row.0.len() == 4
                && row.0[0] == owner.head.0
                && row.0[3] == "action"
                && !keys
                    .iter()
                    .any(|key| key.table == *table && key.values == row.0)
            {
                work.nodes(1)?;
                keys.insert(native_key(table, &row.0, work)?);
            }
        }
    }
    work.nodes(1 + keys.len())?;
    Ok(keys.into_iter().collect())
}
