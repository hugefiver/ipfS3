use super::*;
use crate::pinning::zip_policy::ZipIntentSelection;
use crate::store::entities::object_tag;

impl ObjectWriteSet {
    pub(in crate::store::pinning::publication::zip) fn decisions(
        &mut self,
        request: &CapturedZipPublication,
        selected: &ZipIntentSelection,
        work: DecodeWork<'_>,
    ) -> AppResult<()> {
        if let Some(source) = &request.source {
            let bytes = request.source_tags.iter().try_fold(0usize, |total, tag| {
                total
                    .checked_add(source.id.len())
                    .and_then(|n| n.checked_add(tag.key.len()))
                    .and_then(|n| n.checked_add(tag.value.len()))
                    .and_then(|n| n.checked_add(128))
                    .ok_or(AppError::ManagementBudget)
            })?;
            self.resident.push(sql::retain_copy(work, bytes)?);
            self.new_tags = request
                .source_tags
                .iter()
                .map(|tag| object_tag::Model {
                    object_id: source.id.clone(),
                    key: tag.key.clone(),
                    value: tag.value.clone(),
                })
                .collect();
        }
        let mut decisions = selected
            .output_decisions
            .iter()
            .map(|(o, d)| {
                let mut decision = d.clone();
                decision.control_revision = uuid::Uuid::new_v4().to_string();
                (o, decision, Vec::new())
            })
            .collect::<Vec<_>>();
        if let (Some(source), Some(decision)) = (&self.source, &selected.source_decision) {
            decisions.push((source, decision.clone(), request.source_tags.clone()));
        }
        for (bound, decision, tags) in decisions {
            decision.replay_policy(tags).map_err(|_| invalid())?;
            let snapshot = serde_json::to_string(&decision).map_err(|_| invalid())?;
            let bytes = snapshot
                .len()
                .checked_add(bound.version_id.len())
                .and_then(|n| n.checked_add(bound.object_id.len()))
                .and_then(|n| n.checked_add(decision.control_revision.len()))
                .and_then(|n| n.checked_add(256))
                .ok_or(AppError::ManagementBudget)?;
            self.resident.push(sql::retain_copy(work, bytes)?);
            self.decisions.push(pin_extension_decision::Model {
                version_row_id: bound.version_id.clone(),
                object_id: bound.object_id.clone(),
                control_revision: decision.control_revision.clone(),
                effect: match decision.effect {
                    crate::pinning::decision::DecisionEffect::Accepted => "accepted",
                    crate::pinning::decision::DecisionEffect::Skipped => "skipped",
                    crate::pinning::decision::DecisionEffect::NoIntent => "no_intent",
                }
                .into(),
                snapshot,
            });
        }
        Ok(())
    }

    pub(in crate::store::pinning::publication::zip) async fn persist<C: ConnectionTrait>(
        &self,
        db: &C,
        work: DecodeWork<'_>,
        maintenance: &mut PublicationLease<'_>,
    ) -> AppResult<()> {
        let removed = self
            .removed_versions
            .iter()
            .filter(|v| v.kind == "object")
            .map(|v| v.id.clone())
            .collect::<Vec<_>>();
        for chunk in removed.chunks(sql::PARAMS) {
            let predicate = format!("version_row_id IN ({})", sql::slots(chunk.len(), 0));
            let mut resident = Vec::new();
            let rows = sql::read::<version_residency::Entity, _>(
                db,
                &predicate,
                chunk.iter().cloned().map(Into::into).collect(),
                "version_row_id",
                work,
                &mut resident,
                Some(maintenance),
            )
            .await?;
            if rows.len() != chunk.len() {
                return Err(invalid());
            }
            let references = sql::read::<residency_reference::Entity, _>(
                db,
                &format!("{predicate} AND owner_kind='version' AND reason='retained_version'"),
                chunk.iter().cloned().map(Into::into).collect(),
                "owner_id,tier,cid",
                work,
                &mut resident,
                Some(maintenance),
            )
            .await?;
            let exact = references
                .into_iter()
                .filter(|r| {
                    rows.iter().any(|v| {
                        v.version_row_id == r.owner_id
                            && v.version_row_id == r.version_row_id
                            && v.object_id == r.object_id
                            && v.primary_tier == r.tier
                            && v.cid == r.cid
                    })
                })
                .collect::<Vec<_>>();
            sql::delete::<residency_reference::Entity, _>(db, &exact, work, Some(maintenance))
                .await?;
        }
        sql::delete::<object_version::Entity, _>(
            db,
            &self.removed_versions,
            work,
            Some(maintenance),
        )
        .await?;
        sql::update::<object::Entity, _>(db, &self.old_objects, work, Some(maintenance)).await?;
        sql::update::<object_version::Entity, _>(db, &self.old_versions, work, Some(maintenance))
            .await?;
        sql::insert::<object::Entity, _>(db, &self.new_objects, work, Some(maintenance)).await?;
        sql::insert::<object_version::Entity, _>(db, &self.new_versions, work, Some(maintenance))
            .await?;
        sql::insert::<version_residency::Entity, _>(db, &self.residencies, work, Some(maintenance))
            .await?;
        sql::insert::<residency_reference::Entity, _>(
            db,
            &self.references,
            work,
            Some(maintenance),
        )
        .await?;
        sql::delete::<object_tag::Entity, _>(db, &self.old_tags, work, Some(maintenance)).await?;
        sql::insert::<object_tag::Entity, _>(db, &self.new_tags, work, Some(maintenance)).await?;
        sql::insert::<pin_extension_decision::Entity, _>(
            db,
            &self.decisions,
            work,
            Some(maintenance),
        )
        .await?;
        Ok(())
    }

    pub(in crate::store::pinning::publication::zip) async fn verify<C: ConnectionTrait>(
        &self,
        db: &C,
        work: DecodeWork<'_>,
    ) -> AppResult<()> {
        for chunk in self.old_objects.chunks(sql::PARAMS) {
            let mut resident = Vec::new();
            let expected = chunk.iter().map(|(_, row)| row).collect::<Vec<_>>();
            let actual = sql::read::<object::Entity, _>(
                db,
                &format!("id IN ({})", sql::slots(chunk.len(), 0)),
                chunk.iter().map(|(_, row)| row.id.clone().into()).collect(),
                "id",
                work,
                &mut resident,
                None,
            )
            .await?;
            if actual.len() != expected.len() || expected.iter().any(|row| !actual.contains(row)) {
                return Err(invalid());
            }
        }
        for chunk in self.old_versions.chunks(sql::PARAMS) {
            let mut resident = Vec::new();
            let expected = chunk.iter().map(|(_, row)| row).collect::<Vec<_>>();
            let actual = sql::read::<object_version::Entity, _>(
                db,
                &format!("id IN ({})", sql::slots(chunk.len(), 0)),
                chunk.iter().map(|(_, row)| row.id.clone().into()).collect(),
                "id",
                work,
                &mut resident,
                None,
            )
            .await?;
            if actual.len() != expected.len() || expected.iter().any(|row| !actual.contains(row)) {
                return Err(invalid());
            }
        }
        for chunk in self.removed_versions.chunks(sql::PARAMS) {
            let mut resident = Vec::new();
            let values = chunk
                .iter()
                .map(|row| row.id.clone().into())
                .collect::<Vec<_>>();
            let predicate = format!("id IN ({})", sql::slots(chunk.len(), 0));
            if !sql::read::<object_version::Entity, _>(
                db,
                &predicate,
                values.clone(),
                "id",
                work,
                &mut resident,
                None,
            )
            .await?
            .is_empty()
            {
                return Err(invalid());
            }
            let predicate = format!("version_row_id IN ({})", sql::slots(chunk.len(), 0));
            if !sql::read::<version_residency::Entity, _>(
                db,
                &predicate,
                values.clone(),
                "version_row_id",
                work,
                &mut resident,
                None,
            )
            .await?
            .is_empty()
                || !sql::read::<residency_reference::Entity, _>(
                    db,
                    &predicate,
                    values,
                    "owner_id,tier,cid",
                    work,
                    &mut resident,
                    None,
                )
                .await?
                .is_empty()
            {
                return Err(invalid());
            }
        }
        for chunk in self.new_versions.chunks(sql::PARAMS) {
            let mut resident = Vec::new();
            let values = chunk
                .iter()
                .map(|v| v.id.clone().into())
                .collect::<Vec<_>>();
            let predicate = format!("id IN ({})", sql::slots(chunk.len(), 0));
            let versions = sql::read::<object_version::Entity, _>(
                db,
                &predicate,
                values.clone(),
                "id",
                work,
                &mut resident,
                None,
            )
            .await?;
            if versions.len() != chunk.len() || chunk.iter().any(|v| !versions.contains(v)) {
                return Err(invalid());
            }
            let predicate = format!("version_row_id IN ({})", sql::slots(chunk.len(), 0));
            let residencies = sql::read::<version_residency::Entity, _>(
                db,
                &predicate,
                values.clone(),
                "version_row_id",
                work,
                &mut resident,
                None,
            )
            .await?;
            if residencies.len() != chunk.len()
                || residencies.iter().any(|r| !self.residencies.contains(r))
            {
                return Err(invalid());
            }
            let decisions = sql::read::<pin_extension_decision::Entity, _>(
                db,
                &predicate,
                values.clone(),
                "version_row_id",
                work,
                &mut resident,
                None,
            )
            .await?;
            if decisions.len()
                != self
                    .decisions
                    .iter()
                    .filter(|d| chunk.iter().any(|v| v.id == d.version_row_id))
                    .count()
                || decisions.iter().any(|d| !self.decisions.contains(d))
            {
                return Err(invalid());
            }
            let refs = sql::read::<residency_reference::Entity, _>(
                db,
                &predicate,
                values,
                "owner_id,tier,cid",
                work,
                &mut resident,
                None,
            )
            .await?;
            if refs.len() != chunk.len() || refs.iter().any(|r| !self.references.contains(r)) {
                return Err(invalid());
            }
        }
        for chunk in self.new_objects.chunks(sql::PARAMS) {
            let mut resident = Vec::new();
            let objects = sql::read::<object::Entity, _>(
                db,
                &format!("id IN ({})", sql::slots(chunk.len(), 0)),
                chunk.iter().map(|o| o.id.clone().into()).collect(),
                "id",
                work,
                &mut resident,
                None,
            )
            .await?;
            if objects.len() != chunk.len() || chunk.iter().any(|o| !objects.contains(o)) {
                return Err(invalid());
            }
        }
        let tag_owners = self
            .owners
            .iter()
            .chain(self.new_tags.iter().map(|tag| &tag.object_id))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let mut actual_tags = Vec::new();
        let mut resident = Vec::new();
        for chunk in tag_owners.chunks(sql::PARAMS) {
            actual_tags.extend(
                sql::read::<object_tag::Entity, _>(
                    db,
                    &format!("object_id IN ({})", sql::slots(chunk.len(), 0)),
                    chunk.iter().cloned().map(Into::into).collect(),
                    "object_id,key",
                    work,
                    &mut resident,
                    None,
                )
                .await?,
            );
        }
        if actual_tags.len() != self.new_tags.len()
            || self.new_tags.iter().any(|tag| !actual_tags.contains(tag))
        {
            return Err(invalid());
        }
        Ok(())
    }
}
