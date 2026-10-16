use super::*;
use crate::pinning::management::DryRunPlan;

struct Candidate {
    owner: Key,
    slot: usize,
    length: usize,
    hash: String,
    leaf: std::sync::Arc<Leaf>,
}

pub(in crate::store::pinning::management::records) fn encode_proposal(
    auth: &LocalAuthorization,
    request: &PlanRequest,
    snapshot: &crate::pinning::management::Snapshot,
    plan: &DryRunPlan,
    staged_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    shared: &SharedDecodeBudget,
) -> Result<String> {
    let frame = snapshot
        .native_frame
        .as_ref()
        .ok_or(ManagementError::Confirmation)?;
    if frame.database_scope != auth.database_scope
        || frame.database_identity != auth.database_identity
    {
        return Err(ManagementError::Unauthorized);
    }
    let points = crate::store::pinning::management::actions::points::planning_points_with_work(
        frame,
        shared.clone(),
    )?;
    let mut guard = shared
        .lock()
        .map_err(|_| ManagementError::InvalidEvidence)?;
    let work = &mut *guard;
    frame.validate()?;
    let item = plan.items().first().ok_or(ManagementError::Confirmation)?;
    work.copy(&(
        super::super::super::schema::RECORDS,
        &auth.database_scope,
        &request.plan_id,
        &item.request.item_id,
        "action",
    ))?;
    let owner = Key(
        super::super::super::schema::RECORDS.into(),
        vec![
            auth.database_scope.clone(),
            request.plan_id.clone(),
            item.request.item_id.clone(),
            "action".into(),
        ],
    );
    let candidates = candidates(frame, auth, work)?;
    let mut leaves = Vec::new();
    let mut hashes = Vec::new();
    let mut tables = Vec::new();
    work.nodes(3)?;
    for (ordinal, table) in frame.tables.iter().enumerate() {
        work.copy(&table.table)?;
        work.nodes(2)?;
        let mut rows = Vec::new();
        for public in &table.rows {
            work.nodes(2)?;
            work.copy(&table.table)?;
            work.nodes(2)?;
            for column in &table.primary_key {
                work.copy(&native::value(public, column).ok_or(ManagementError::InvalidEvidence)?)?;
            }
            let key = Key(
                table.table.clone(),
                table
                    .primary_key
                    .iter()
                    .map(|column| {
                        native::value(public, column)
                            .map(str::to_owned)
                            .ok_or(ManagementError::InvalidEvidence)
                    })
                    .collect::<Result<_>>()?,
            );
            let mut matching = None;
            for candidate in &candidates {
                if candidate.leaf.0 == ordinal
                    && candidate.leaf.1 == key.1
                    && equal(&candidate.leaf, frame, table, &key, work)?
                {
                    matching = Some(candidate);
                    break;
                }
            }
            let version = if let Some(candidate) = matching {
                work.copy(&(&key.1, &candidate.owner, &candidate.hash))?;
                RowVersion(
                    key.1.clone(),
                    Key(candidate.owner.0.clone(), candidate.owner.1.clone()),
                    candidate.slot,
                    candidate.length,
                    candidate.hash.clone(),
                )
            } else {
                let leaf = owned_leaf(frame, table, ordinal, &key, &auth.principal, work)?;
                let raw = encoded(&leaf, work)?;
                work.bytes(64)?;
                work.nodes(1)?;
                let hash = identity(b"stage7/leaf/1\0", raw.as_bytes());
                work.copy(&(&key.1, &owner, &hash))?;
                let version = RowVersion(
                    key.1.clone(),
                    Key(owner.0.clone(), owner.1.clone()),
                    leaves.len(),
                    raw.len(),
                    hash.clone(),
                );
                leaves.push(raw);
                hashes.push(hash);
                version
            };
            rows.push(version);
        }
        tables.push((table.table.clone(), rows));
    }
    work.copy(&frame.schema_versions)?;
    let manifest = Manifest(frame.schema_versions.clone(), tables);
    let manifest_raw = encoded(&manifest, work)?;
    work.bytes(64)?;
    work.nodes(1)?;
    let manifest_hash = identity(b"stage7/manifest/1\0", manifest_raw.as_bytes());
    let core = (
        request,
        plan.items(),
        plan.summary(),
        core::SnapshotRef(snapshot),
        staged_at,
        expires_at,
        &points,
        &manifest_hash,
    );
    let core_raw = encoded(&core, work)?;
    work.bytes(64)?;
    work.nodes(1)?;
    let core_hash = identity(b"stage7/core/1\0", core_raw.as_bytes());
    let head = (
        &auth.database_scope,
        &auth.database_identity,
        &auth.principal,
        &owner,
        &frame.schema_versions,
        &core_hash,
        &manifest_hash,
        &hashes,
    );
    let head_raw = encoded(&head, work)?;
    assemble(&head_raw, &core_raw, &manifest_raw, &leaves, work)
}

fn candidates(
    frame: &NativeFrame,
    binding: &LocalAuthorization,
    work: &mut DecodeBudget,
) -> Result<Vec<Candidate>> {
    let mut result = Vec::new();
    work.nodes(1)?;
    // The capture itself defines the allowed sources. No current DB lookup extends it.
    let mut owners = Vec::new();
    work.nodes(1)?;
    for key in &frame.frontier {
        if key.table != super::super::super::schema::RECORDS
            || key.values.len() != 4
            || key.values[0] != binding.database_scope
            || key.values[3] != "action"
        {
            continue;
        }
        work.nodes(1)?;
        owners.push(key);
    }
    owners.sort();
    for key in owners {
        work.nodes(1 + key.values.len())?;
        let refs = key.values.iter().map(String::as_str).collect::<Vec<_>>();
        let raw = frame
            .original_value(&key.table, &refs, "native_before")
            .ok_or(ManagementError::InvalidEvidence)?;
        if !raw.starts_with(FRAME) {
            continue;
        }
        if frame.original_value(&key.table, &refs, "native_identity")
            != Some(&binding.database_identity)
            || frame.original_value(&key.table, &refs, "principal") != Some(&binding.principal)
        {
            continue;
        }
        work.copy(&(key.table.as_str(), &key.values))?;
        let owner = Key(key.table.clone(), key.values.clone());
        let row = batch::frame_row(frame, key, work)?;
        let source = batch::source(
            row,
            &owner,
            &binding.database_scope,
            &binding.database_identity,
            &binding.principal,
            work,
        )?;
        if source.head.4 != frame.schema_versions {
            continue;
        }
        for (slot, range) in source.sections.leaves.iter().enumerate() {
            let raw = &source.raw()[range.clone()];
            if !identity_matches(b"stage7/leaf/1\0", raw.as_bytes(), &source.head.7[slot]) {
                return Err(ManagementError::InvalidEvidence);
            }
            let leaf = batch::owned_leaf(&source, slot, work)?;
            let table = frame
                .tables
                .get(leaf.0)
                .ok_or(ManagementError::InvalidEvidence)?;
            if leaf.1.len() != table.primary_key.len() || leaf.2.len() != table.columns.len() {
                return Err(ManagementError::InvalidEvidence);
            }
            work.copy(&(&owner, &source.head.7[slot]))?;
            work.nodes(1)?;
            result.push(Candidate {
                owner: Key(owner.0.clone(), owner.1.clone()),
                slot,
                length: raw.len(),
                hash: source.head.7[slot].clone(),
                leaf,
            });
        }
    }
    Ok(result)
}

fn equal(
    leaf: &Leaf,
    frame: &NativeFrame,
    table: &native::NativeTable,
    key: &Key,
    work: &mut DecodeBudget,
) -> Result<bool> {
    if leaf.2.len() != table.columns.len() {
        return Err(ManagementError::InvalidEvidence);
    }
    work.nodes(1 + key.1.len())?;
    let refs = key.1.iter().map(String::as_str).collect::<Vec<_>>();
    for (column, cell) in table.columns.iter().zip(&leaf.2) {
        let actual = frame.original_value(&key.0, &refs, column);
        match cell {
            Cell::Text(expected) if expected.as_deref() == actual => {}
            Cell::Before(source, length, hash)
                if column == "native_before"
                    && source == key
                    && key.action(&frame.database_scope) =>
            {
                let Some(actual) = actual else {
                    return Ok(false);
                };
                if actual.len() != *length || !digest_matches(before_digest(source, actual)?, hash)
                {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
    }
    Ok(true)
}

fn owned_leaf(
    frame: &NativeFrame,
    table: &native::NativeTable,
    ordinal: usize,
    key: &Key,
    principal: &str,
    work: &mut DecodeBudget,
) -> Result<Leaf> {
    work.copy(&key.1)?;
    work.nodes(2)?;
    work.nodes(1 + key.1.len())?;
    let refs = key.1.iter().map(String::as_str).collect::<Vec<_>>();
    let mut cells = Vec::new();
    for column in &table.columns {
        let raw = frame.original_value(&key.0, &refs, column);
        if column == "native_before"
            && key.action(&frame.database_scope)
            && frame.original_value(&key.0, &refs, "native_identity")
                == Some(&frame.database_identity)
            && frame.original_value(&key.0, &refs, "principal") == Some(principal)
            && let Some(raw) = raw
        {
            work.copy(key)?;
            work.nodes(3)?;
            work.bytes(64)?;
            cells.push(Cell::Before(
                Key(key.0.clone(), key.1.clone()),
                raw.len(),
                before_identity(key, raw)?,
            ));
        } else {
            work.copy(&raw)?;
            cells.push(Cell::Text(raw.map(str::to_owned)));
        }
    }
    Ok(Leaf(ordinal, key.1.clone(), cells))
}
