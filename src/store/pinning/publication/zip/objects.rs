use super::*;
use crate::store::{
    entities::{
        object, object_tag, object_version, physical_residency, pin_extension_decision,
        residency_reference, version_residency,
    },
    pinning::management::{native::meter::ResidentGuard, records::DecodeWork},
};
use chrono::{DateTime, Utc};

mod write;

pub(super) struct LockedObjects {
    objects: Vec<object::Model>,
    versions: Vec<object_version::Model>,
    tags: Vec<object_tag::Model>,
    resident: Vec<ResidentGuard>,
}

pub(super) struct ObjectWriteSet {
    pub owners: Vec<String>,
    pub results: Vec<(String, PublicationResult)>,
    pub outputs: Vec<ZipBoundOutput>,
    pub source: Option<ZipBoundOutput>,
    pub bindings: Vec<VersionBinding>,
    new_objects: Vec<object::Model>,
    old_objects: Vec<(object::Model, object::Model)>,
    new_versions: Vec<object_version::Model>,
    old_versions: Vec<(object_version::Model, object_version::Model)>,
    removed_versions: Vec<object_version::Model>,
    residencies: Vec<version_residency::Model>,
    references: Vec<residency_reference::Model>,
    decisions: Vec<pin_extension_decision::Model>,
    old_tags: Vec<object_tag::Model>,
    new_tags: Vec<object_tag::Model>,
    resident: Vec<ResidentGuard>,
}

pub(super) struct PhysicalFrontier {
    rows: BTreeMap<String, physical_residency::Model>,
    _resident: Vec<ResidentGuard>,
}

pub(super) async fn prepare_physical<C: ConnectionTrait>(
    db: &C,
    cids: Vec<String>,
    now: DateTime<Utc>,
    work: DecodeWork<'_>,
    mut maintenance: Option<&mut super::PublicationLease<'_>>,
) -> AppResult<PhysicalFrontier> {
    let cids = cids
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut existing = BTreeMap::new();
    let mut resident = Vec::new();
    for chunk in cids.chunks(sql::PARAMS) {
        let values = chunk.iter().cloned().map(Into::into).collect::<Vec<_>>();
        let predicate = format!("tier='hot' AND cid IN ({})", sql::slots(chunk.len(), 0));
        sql::lock(
            db,
            "physical_residencies",
            &predicate,
            values.clone(),
            "tier,cid",
            work,
            maintenance.as_deref_mut(),
        )
        .await?;
        for row in sql::read::<physical_residency::Entity, _>(
            db,
            &predicate,
            values,
            "tier,cid",
            work,
            &mut resident,
            maintenance.as_deref_mut(),
        )
        .await?
        {
            existing.insert(row.cid.clone(), row);
        }
    }
    let inserted = cids
        .iter()
        .filter(|cid| !existing.contains_key(*cid))
        .map(|cid| physical_residency::Model {
            tier: "hot".into(),
            cid: cid.clone(),
            node_identity: None,
            verification_state: "pending".into(),
            verification_receipt: None,
            verified_at: None,
            created_at: now,
            updated_at: now,
        })
        .collect::<Vec<_>>();
    let inserted_bytes = inserted.iter().try_fold(0usize, |total, row| {
        total
            .checked_add(row.cid.len())
            .and_then(|n| n.checked_add(256))
            .ok_or(AppError::ManagementBudget)
    })?;
    resident.push(sql::retain_copy(work, inserted_bytes)?);
    sql::insert::<physical_residency::Entity, _>(db, &inserted, work, maintenance.as_deref_mut())
        .await?;
    existing.extend(inserted.into_iter().map(|row| (row.cid.clone(), row)));
    let frontier = PhysicalFrontier {
        rows: existing,
        _resident: resident,
    };
    frontier.verify(db, work, maintenance).await?;
    Ok(frontier)
}

impl PhysicalFrontier {
    pub(super) async fn verify<C: ConnectionTrait>(
        &self,
        db: &C,
        work: DecodeWork<'_>,
        mut maintenance: Option<&mut super::PublicationLease<'_>>,
    ) -> AppResult<()> {
        let cids = self.rows.keys().collect::<Vec<_>>();
        for chunk in cids.chunks(sql::PARAMS) {
            let mut checked = Vec::new();
            let rows = sql::read::<physical_residency::Entity, _>(
                db,
                &format!("tier='hot' AND cid IN ({})", sql::slots(chunk.len(), 0)),
                chunk.iter().map(|cid| cid.as_str().into()).collect(),
                "tier,cid",
                work,
                &mut checked,
                maintenance.as_deref_mut(),
            )
            .await?;
            if rows.len() != chunk.len()
                || rows.iter().any(|row| {
                    self.rows.get(&row.cid) != Some(row)
                        || row.tier != "hot"
                        || row.cid.is_empty()
                        || match row.verification_state.as_str() {
                            "verified" => {
                                row.node_identity.as_ref().is_none_or(String::is_empty)
                                    || row
                                        .verification_receipt
                                        .as_ref()
                                        .is_none_or(String::is_empty)
                                    || row.verified_at.is_none()
                            }
                            "pending" | "failed" => {
                                row.node_identity.is_some()
                                    || row.verification_receipt.is_some()
                                    || row.verified_at.is_some()
                            }
                            _ => true,
                        }
                })
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
}

impl LockedObjects {
    pub async fn load<C: ConnectionTrait>(
        db: &C,
        request: &CapturedZipPublication,
        work: DecodeWork<'_>,
        mut maintenance: Option<&mut super::PublicationLease<'_>>,
    ) -> AppResult<Self> {
        let mut result = Self {
            objects: Vec::new(),
            versions: Vec::new(),
            tags: Vec::new(),
            resident: Vec::new(),
        };
        let ordered = objects(request);
        for chunk in ordered.chunks(sql::PARAMS - 1) {
            let mut values = vec![request.capture.bucket().into()];
            values.extend(chunk.iter().map(|o| o.key.clone().into()));
            let predicate = format!("bucket=$1 AND key IN ({})", sql::slots(chunk.len(), 1));
            let relevant = format!(
                "{predicate} AND (is_latest=TRUE OR version_id IS NULL OR sequence=(SELECT MAX(v.sequence) FROM object_versions v WHERE v.bucket=object_versions.bucket AND v.key=object_versions.key))"
            );
            sql::lock(
                db,
                "object_versions",
                &relevant,
                values.clone(),
                "bucket,key,sequence",
                work,
                maintenance.as_deref_mut(),
            )
            .await?;
            result.versions.extend(
                sql::read::<object_version::Entity, _>(
                    db,
                    &relevant,
                    values.clone(),
                    "bucket,key,sequence",
                    work,
                    &mut result.resident,
                    maintenance.as_deref_mut(),
                )
                .await?,
            );
            let latest = format!("{predicate} AND is_latest=TRUE");
            sql::lock(
                db,
                "objects",
                &latest,
                values.clone(),
                "bucket,key,id",
                work,
                maintenance.as_deref_mut(),
            )
            .await?;
            result.objects.extend(
                sql::read::<object::Entity, _>(
                    db,
                    &latest,
                    values,
                    "bucket,key,id",
                    work,
                    &mut result.resident,
                    maintenance.as_deref_mut(),
                )
                .await?,
            );
        }
        let owner_ids = result
            .objects
            .iter()
            .map(|row| row.id.clone())
            .chain(
                result
                    .versions
                    .iter()
                    .filter_map(|row| row.object_id.clone()),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        for chunk in owner_ids.chunks(sql::PARAMS) {
            let values = chunk.iter().cloned().map(Into::into).collect::<Vec<_>>();
            let predicate = format!("object_id IN ({})", sql::slots(chunk.len(), 0));
            sql::lock(
                db,
                "object_tags",
                &predicate,
                values.clone(),
                "object_id,key",
                work,
                maintenance.as_deref_mut(),
            )
            .await?;
            result.tags.extend(
                sql::read::<object_tag::Entity, _>(
                    db,
                    &predicate,
                    values,
                    "object_id,key",
                    work,
                    &mut result.resident,
                    maintenance.as_deref_mut(),
                )
                .await?,
            );
        }
        Ok(result)
    }

    pub fn owners(&self, state: BucketVersioningState) -> Vec<String> {
        match state {
            BucketVersioningState::Unversioned => {
                self.objects.iter().map(|o| o.id.clone()).collect()
            }
            BucketVersioningState::Enabled => Vec::new(),
            BucketVersioningState::Suspended => self
                .versions
                .iter()
                .filter(|v| v.version_id.is_none())
                .filter_map(|v| v.object_id.clone())
                .collect(),
        }
    }

    pub fn plan(
        self,
        request: &CapturedZipPublication,
        state: BucketVersioningState,
        now: DateTime<Utc>,
        work: DecodeWork<'_>,
    ) -> AppResult<ObjectWriteSet> {
        let current_versions = self
            .versions
            .iter()
            .filter(|row| row.is_latest)
            .map(|row| (&row.key, row))
            .collect::<BTreeMap<_, _>>();
        let current_objects = self
            .objects
            .iter()
            .map(|row| (&row.key, row))
            .collect::<BTreeMap<_, _>>();
        for item in objects(request) {
            match (
                current_versions.get(&item.key),
                current_objects.get(&item.key),
            ) {
                (None, None) => {}
                (None, Some(_)) if state == BucketVersioningState::Unversioned => {}
                (Some(version), Some(object))
                    if version.kind == "object"
                        && version.object_id.as_deref() == Some(object.id.as_str()) => {}
                (Some(version), None)
                    if version.kind == "delete_marker" && version.object_id.is_none() => {}
                _ => return Err(invalid()),
            }
        }
        let cloned = self.resident.iter().try_fold(0usize, |total, guard| {
            total
                .checked_add(guard.bytes())
                .ok_or(AppError::ManagementBudget)
        })?;
        let planned = objects(request)
            .into_iter()
            .try_fold(0usize, |total, item| {
                let metadata = item
                    .metadata
                    .as_ref()
                    .map_or(0, |value| value.to_string().len());
                let strings = item
                    .id
                    .len()
                    .checked_add(item.bucket.len())
                    .and_then(|n| n.checked_add(item.key.len()))
                    .and_then(|n| n.checked_add(item.cid.len()))
                    .and_then(|n| n.checked_add(item.etag.len()))
                    .and_then(|n| n.checked_add(item.content_type.as_ref().map_or(0, String::len)))
                    .and_then(|n| n.checked_add(item.key_wrap.as_ref().map_or(0, String::len)))
                    .and_then(|n| {
                        n.checked_add(item.sse_c_key_fingerprint.as_ref().map_or(0, String::len))
                    })
                    .and_then(|n| n.checked_add(metadata))
                    .ok_or(AppError::ManagementBudget)?;
                total
                    .checked_add(
                        strings
                            .checked_mul(4)
                            .and_then(|n| n.checked_add(1536))
                            .ok_or(AppError::ManagementBudget)?,
                    )
                    .ok_or(AppError::ManagementBudget)
            })?;
        let owners = self.owners(state);
        let owner_ids = owners.iter().collect::<BTreeSet<_>>();
        let old_tags = self
            .tags
            .into_iter()
            .filter(|tag| owner_ids.contains(&tag.object_id))
            .collect();
        let mut resident = self.resident;
        resident.push(sql::retain_copy(
            work,
            cloned
                .checked_add(planned)
                .ok_or(AppError::ManagementBudget)?,
        )?);
        let mut writes = ObjectWriteSet {
            owners,
            results: Vec::new(),
            outputs: Vec::new(),
            source: None,
            bindings: Vec::new(),
            new_objects: Vec::new(),
            old_objects: Vec::new(),
            new_versions: Vec::new(),
            old_versions: Vec::new(),
            removed_versions: Vec::new(),
            residencies: Vec::new(),
            references: Vec::new(),
            decisions: Vec::new(),
            old_tags,
            new_tags: Vec::new(),
            resident,
        };
        for old in self.objects {
            let mut new = old.clone();
            new.is_latest = false;
            writes.old_objects.push((old, new));
        }
        let mut sequences = BTreeMap::new();
        for old in self.versions {
            let next = old.sequence.checked_add(1).ok_or_else(invalid)?;
            sequences
                .entry(old.key.clone())
                .and_modify(|n: &mut i64| *n = (*n).max(next))
                .or_insert(next);
            if state != BucketVersioningState::Enabled && old.version_id.is_none() {
                writes.removed_versions.push(old);
            } else if old.is_latest {
                let mut new = old.clone();
                new.is_latest = false;
                new.became_noncurrent_at = Some(now);
                new.updated_at = now;
                writes.old_versions.push((old, new));
            }
        }
        let paths = request
            .manifest
            .successful
            .iter()
            .map(|f| (&f.object_key, &f.relative_path))
            .collect::<BTreeMap<_, _>>();
        for item in objects(request) {
            let row_id = uuid::Uuid::new_v4().to_string();
            let public_id = if state == BucketVersioningState::Enabled {
                Some(uuid::Uuid::new_v4().to_string())
            } else {
                None
            };
            writes.new_objects.push(object::Model {
                id: item.id.clone(),
                bucket: item.bucket.clone(),
                key: item.key.clone(),
                cid: item.cid.clone(),
                size: item.logical_size,
                content_type: item.content_type.clone(),
                etag: item.etag.clone(),
                metadata: item.metadata.clone(),
                encrypted: item.encrypted,
                key_wrap: item.key_wrap.clone(),
                sse_c_key_fingerprint: item.sse_c_key_fingerprint.clone(),
                multipart: item.multipart,
                is_latest: true,
                created_at: now,
            });
            writes.new_versions.push(object_version::Model {
                id: row_id.clone(),
                bucket: item.bucket.clone(),
                key: item.key.clone(),
                version_id: public_id.clone(),
                kind: "object".into(),
                object_id: Some(item.id.clone()),
                sequence: sequences.get(&item.key).copied().unwrap_or(1),
                is_latest: true,
                lifecycle_age_started_at: now,
                became_noncurrent_at: None,
                created_at: now,
                updated_at: now,
            });
            writes.residencies.push(version_residency::Model {
                version_row_id: row_id.clone(),
                object_id: item.id.clone(),
                primary_tier: "hot".into(),
                storage_class: "STANDARD".into(),
                cid: item.cid.clone(),
                revision: 1,
                created_at: now,
                updated_at: now,
            });
            writes.references.push(residency_reference::Model {
                owner_kind: "version".into(),
                owner_id: row_id.clone(),
                reason: "retained_version".into(),
                version_row_id: row_id.clone(),
                object_id: item.id.clone(),
                tier: "hot".into(),
                cid: item.cid.clone(),
                created_at: now,
            });
            let mut bound = candidate_binding(item);
            bound.version_id = row_id.clone();
            if request.source.as_ref().is_some_and(|s| s.id == item.id) {
                writes.source = Some(bound);
            } else {
                writes.outputs.push(bound);
                writes.bindings.push(VersionBinding {
                    path: paths.get(&item.key).ok_or_else(invalid)?.to_string(),
                    version_row_id: row_id,
                });
            }
            writes.results.push((
                item.key.clone(),
                PublicationResult {
                    object_id: item.id.clone(),
                    version_id: match state {
                        BucketVersioningState::Unversioned => None,
                        _ => Some(public_id.unwrap_or_else(|| "null".into())),
                    },
                },
            ));
        }
        let mut outputs = std::mem::take(&mut writes.outputs)
            .into_iter()
            .map(|output| (output.key.clone(), output))
            .collect::<BTreeMap<_, _>>();
        writes.outputs = request
            .successes
            .iter()
            .map(|object| outputs.remove(&object.key).ok_or_else(invalid))
            .collect::<AppResult<Vec<_>>>()?;
        if !outputs.is_empty() {
            return Err(invalid());
        }
        Ok(writes)
    }
}
