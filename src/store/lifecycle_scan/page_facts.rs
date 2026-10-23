use std::collections::{BTreeMap, BTreeSet};

use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter};

use crate::{
    error::{AppError, AppResult},
    residency::model::{KuboTier, StorageClass, VerificationState, corrupt},
    store::{
        entities::{object, object_version, physical_residency, version_residency},
        object_version::{VersionKind, version_kind},
    },
};

// Two binds per physical location; keep both statement size and predicate depth
// bounded even at the maximum scan page size.
const FACTS_BATCH_SIZE: usize = 200;

/// Candidate-only observations, never authority to execute lifecycle actions.
pub(super) struct PageFacts {
    residencies: BTreeMap<String, version_residency::Model>,
    physical: BTreeMap<(String, String), physical_residency::Model>,
}

impl PageFacts {
    pub(super) async fn load<C: ConnectionTrait>(
        db: &C,
        rows: &[(object_version::Model, Option<object::Model>)],
    ) -> AppResult<Self> {
        let ids: Vec<_> = rows
            .iter()
            .filter(|(row, _)| row.kind == "object")
            .map(|(row, _)| row.id.clone())
            .collect();
        let mut residencies = BTreeMap::new();
        for batch in ids.chunks(FACTS_BATCH_SIZE) {
            for row in version_residency::Entity::find()
                .filter(version_residency::Column::VersionRowId.is_in(batch.iter().cloned()))
                .all(db)
                .await?
            {
                residencies.insert(row.version_row_id.clone(), row);
            }
        }
        let locations: Vec<_> = residencies
            .values()
            .map(|row| (row.primary_tier.clone(), row.cid.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut physical = BTreeMap::new();
        for batch in locations.chunks(FACTS_BATCH_SIZE) {
            let condition = batch
                .iter()
                .fold(Condition::any(), |condition, (tier, cid)| {
                    condition.add(
                        Condition::all()
                            .add(physical_residency::Column::Tier.eq(tier.clone()))
                            .add(physical_residency::Column::Cid.eq(cid.clone())),
                    )
                });
            for row in physical_residency::Entity::find()
                .filter(condition)
                .all(db)
                .await?
            {
                physical.insert((row.tier.clone(), row.cid.clone()), row);
            }
        }
        Ok(Self {
            residencies,
            physical,
        })
    }

    pub(super) fn evaluate(
        &self,
        row: &object_version::Model,
        joined_object: Option<&object::Model>,
    ) -> AppResult<(Option<StorageClass>, bool)> {
        if version_kind(row)? == VersionKind::DeleteMarker {
            return Ok((None, false));
        }
        let object = joined_object.ok_or_else(|| {
            AppError::Internal("lifecycle content version has no object".to_owned())
        })?;
        if row.object_id.as_deref() != Some(&object.id)
            || row.bucket != object.bucket
            || row.key != object.key
        {
            return Err(AppError::Internal(
                "lifecycle content owner mismatch".to_owned(),
            ));
        }
        let Some(residency) = self.residencies.get(&row.id) else {
            return Ok((Some(StorageClass::Standard), false));
        };
        if row.id.is_empty() {
            return Err(corrupt("empty version identity"));
        }
        if row.object_id.as_deref() != Some(&residency.object_id) {
            return Err(corrupt("residency owner is not the exact content version"));
        }
        if object.cid != residency.cid {
            return Err(corrupt(
                "residency identity does not match immutable object metadata",
            ));
        }
        let tier = KuboTier::from_db_str(&residency.primary_tier)?;
        let storage_class = StorageClass::from_db_str(&residency.storage_class)?;
        if !matches!(
            (tier, storage_class),
            (KuboTier::Hot, StorageClass::Standard) | (KuboTier::Cold, StorageClass::StandardIa)
        ) || residency.revision <= 0
        {
            return Err(corrupt("invalid primary residency"));
        }
        let physical = self
            .physical
            .get(&(residency.primary_tier.clone(), residency.cid.clone()))
            .ok_or_else(|| corrupt("primary physical residency is missing"))?;
        KuboTier::from_db_str(&physical.tier)?;
        let verification = VerificationState::from_db_str(&physical.verification_state)?;
        let valid_shape = match verification {
            VerificationState::Verified => {
                physical
                    .node_identity
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
                    && physical
                        .verification_receipt
                        .as_deref()
                        .is_some_and(|value| !value.is_empty())
                    && physical.verified_at.is_some()
            }
            VerificationState::Pending | VerificationState::Failed => {
                physical.node_identity.is_none()
                    && physical.verification_receipt.is_none()
                    && physical.verified_at.is_none()
            }
        };
        if !valid_shape {
            return Err(corrupt("invalid physical verification shape"));
        }
        Ok((
            Some(storage_class),
            tier == KuboTier::Hot && verification == VerificationState::Verified,
        ))
    }
}
