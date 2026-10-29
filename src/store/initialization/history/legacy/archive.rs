use super::*;

pub(in crate::store::initialization::history) async fn convert(
    tx: &impl TransactionConnection,
    (key, row, generation): (&NativeKey, &NativeRow, i64),
    (scope, identity): (&str, &str),
    review: Review,
    historical: bool,
    budget: &mut Budget,
) -> Result<()> {
    let id = archive_id(scope, identity, key, generation)?;
    let header = ArchiveHeader {
        database_scope: scope.into(),
        database_identity: identity.into(),
        principal: review.principal.clone(),
        catalog: review.catalog.clone(),
        request: serde_json::to_string(&review.request)
            .map_err(|_| ManagementError::InvalidEvidence)?,
        configuration: serde_json::to_string(&review.snapshot.configuration)
            .map_err(|_| ManagementError::InvalidEvidence)?,
        acknowledgements: review.acknowledgements.clone().unwrap_or_default(),
        created_at: protocol::cell(row, "created_at")?.into(),
        expires_at: native::value(row, "expires_at").map(str::to_owned),
    };
    let local = review.database_scope == scope
        && review.database_identity == identity
        && review.principal == "local_configuration";
    let applied =
        key.table == management::schema::RECORDS && native::value(row, "state") == Some("applied");
    let recorded = if applied && (historical || !local) {
        let result = review
            .recorded_result(key)?
            .ok_or(ManagementError::Confirmation)?;
        Some(store_json(tx, &format!("{id}:recorded"), &header, &result, budget).await?)
    } else {
        None
    };
    let committed = if !historical && local && applied {
        match records::action_at_key_with_work(tx, key, records::SharedDecodeBudget::default())
            .await?
        {
            Some(trusted) => Some(
                store_json(
                    tx,
                    &format!("{id}:committed"),
                    &header,
                    trusted.action_view(),
                    budget,
                )
                .await?,
            ),
            None => return Err(ManagementError::InvalidEvidence),
        }
    } else {
        None
    };
    let mut references = references::Cache::default();
    let view = store_review(tx, &id, &header, &review, &mut references, budget).await?;
    let original = store_json(tx, &format!("{id}:original"), &header, row, budget).await?;
    let index = ConvertedSource {
        source: key.clone(),
        source_generation: generation,
        original,
        review: Some(view),
        committed,
        historical,
        recorded,
    };
    store_json(tx, &id, &header, &index, budget).await?;
    Ok(())
}

pub(super) async fn store_review(
    tx: &impl TransactionConnection,
    id: &str,
    header: &ArchiveHeader,
    review: &Review,
    references: &mut references::Cache,
    budget: &mut Budget,
) -> Result<ReviewIndex> {
    let mut rows = Vec::new();
    for (key, row) in &review.rows {
        rows.push(
            references::store(
                tx,
                &format!("{id}:row:{}", rows.len()),
                header,
                (key, row),
                review,
                references,
                budget,
            )
            .await?,
        );
    }
    let view = ReviewIndex {
        anchor: Some(review.archive_owner.native()),
        core_identity: Some(review.core_identity.clone()),
        catalog: review.catalog.clone(),
        database_scope: review.database_scope.clone(),
        database_identity: review.database_identity.clone(),
        principal: review.principal.clone(),
        request: store_json(
            tx,
            &format!("{id}:request"),
            header,
            &review.request,
            budget,
        )
        .await?,
        entries: store_json(
            tx,
            &format!("{id}:entries"),
            header,
            &review.entries,
            budget,
        )
        .await?,
        summary: store_json(
            tx,
            &format!("{id}:summary"),
            header,
            &review.summary,
            budget,
        )
        .await?,
        snapshot: store_json(
            tx,
            &format!("{id}:snapshot"),
            header,
            &review.snapshot,
            budget,
        )
        .await?,
        migration_points: store_json(
            tx,
            &format!("{id}:points"),
            header,
            &review.migration_points,
            budget,
        )
        .await?,
        rows,
        staged_at: review.staged_at,
        expires_at: review.expires_at,
        confirmed_at: review.confirmed_at,
        acknowledgements: review.acknowledgements.clone(),
    };
    Ok(view)
}
