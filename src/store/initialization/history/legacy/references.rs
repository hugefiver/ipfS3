use super::*;

#[derive(Default)]
pub(super) struct Cache(BTreeMap<(Key, String, String, String), ArchiveRef>);

pub(in crate::store::initialization::history) fn validate_frozen_binding(
    reference: &FrozenReviewReference,
    staged: DateTime<Utc>,
    expires: DateTime<Utc>,
    entries: &[PlanEntry],
) -> Result<()> {
    match (&reference.confirmed_at, &reference.acknowledgements) {
        (None, None) => Ok(()),
        (Some(at), Some(acks)) if *at >= staged && *at < expires => {
            let expected = entries
                .iter()
                .filter(|entry| entry.status == model::PlanStatus::Candidate)
                .map(|entry| entry.request.item_id.as_str())
                .collect::<BTreeSet<_>>();
            let actual = acks.iter().map(String::as_str).collect::<BTreeSet<_>>();
            if expected != actual || actual.len() != acks.len() {
                return Err(ManagementError::Confirmation);
            }
            Ok(())
        }
        _ => Err(ManagementError::Confirmation),
    }
}

pub(super) async fn store(
    tx: &DatabaseTransaction,
    id: &str,
    header: &ArchiveHeader,
    (key, row): (&NativeKey, &NativeRow),
    context: &Review,
    cache: &mut Cache,
    budget: &mut Budget,
) -> Result<RowReference> {
    let original = store_json(tx, id, header, row, budget).await?;
    let mut output = RowReference {
        key: key.clone(),
        original,
        review: None,
        recorded: None,
    };
    if key.table != management::schema::RECORDS
        || key.values.get(3).map(String::as_str) != Some("action")
    {
        return Ok(output);
    }
    let Some(before) = native::value(row, "native_before") else {
        return Ok(output);
    };
    if !before.starts_with("S7F1\n") && !before.starts_with("S7L1\n") {
        return Ok(output);
    }
    // Text cells are frozen final values. Only actual Before dependencies bind
    // the current SQL original; foreign text must not acquire that dependency.
    let current = if context.before_dependencies.contains(key) {
        let current = context
            .original(key)
            .ok_or(ManagementError::InvalidEvidence)?;
        for column in ["native_before", "native_identity", "principal"] {
            if native::value(row, column) != native::value(current, column) {
                return Err(ManagementError::Changed);
            }
        }
        current
    } else {
        row
    };
    let frozen = read_from(tx, key, Some(&context.rows), budget).await?;
    let mut work = records::ReadSession::default();
    if let Some(recorded) = result::frozen(row, current, key, &frozen.entries, &mut work)? {
        if frozen.recorded_result(key)?.as_ref() != Some(&recorded) {
            return Err(ManagementError::Confirmation);
        }
        output.recorded =
            Some(store_json(tx, &format!("{id}:recorded"), header, &recorded, budget).await?);
    }
    let identity = (
        frozen.archive_owner.clone(),
        frozen.core_identity.clone(),
        frozen.database_identity.clone(),
        frozen.principal.clone(),
    );
    let version = if let Some(reference) = cache.0.get(&identity) {
        reference.clone()
    } else {
        let view = Box::pin(archive::store_review(
            tx,
            &format!("{id}:review"),
            header,
            &frozen,
            cache,
            budget,
        ))
        .await?;
        let reference = store_json(tx, &format!("{id}:review"), header, &view, budget).await?;
        cache.0.insert(identity, reference.clone());
        reference
    };
    let reference = FrozenReviewReference {
        version,
        anchor: frozen.archive_owner.native(),
        core_identity: frozen.core_identity,
        database_identity: frozen.database_identity,
        principal: frozen.principal,
        confirmed_at: frozen.confirmed_at,
        acknowledgements: frozen.acknowledgements,
    };
    validate_frozen_binding(
        &reference,
        frozen.staged_at,
        frozen.expires_at,
        &frozen.entries,
    )?;
    output.review = Some(reference);
    Ok(output)
}
