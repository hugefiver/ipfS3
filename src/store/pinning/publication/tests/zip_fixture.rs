//! Native admission for pre-R25 store fixtures, with their original frozen v1
//! source intents. This does not enable new archive Automatic pinning.
use super::*;
use crate::pinning::{
    decision::{DecisionEffect, DecisionOrigin, ExtensionDecision},
    identity::ProviderIdentity,
    tags::{PinControl, ZipRootCapture},
    zip_policy::{CapturedZipIntent, decode_legacy_v1_intent},
};
use crate::store::zip::{self as zip_store, BatchAdmission, RootOutcome};
use crate::zip::{batch::final_zip_manifest, response::ExtractedEntry};

pub(in crate::store::pinning::publication) struct AdmittedZip {
    pub capture: CapturedZipIntent,
    pub guard: Option<StandardMutationGuard>,
    pub id: String,
}

fn admission(capture: &CapturedZipIntent, source: &str) -> BatchAdmission {
    let id = capture.origin().request_id.clone();
    BatchAdmission {
        id: id.clone(),
        owner: capture.origin().principal_id.clone(),
        source: source.into(),
        token: id.clone(),
        fingerprint: format!(
            "{source}:{id}:{}:{}:{}:false",
            capture.bucket(),
            capture.source_key(),
            capture.target_prefix()
        ),
        bucket: capture.bucket().into(),
        archive_key: capture.source_key().into(),
        input_identity: id,
        captured_options: serde_json::json!({"zip_intent": capture}).to_string(),
    }
}

pub(in crate::store::pinning::publication) async fn original_capture<
    C: ConnectionTrait + sea_orm::TransactionTrait,
>(
    db: &C,
    archive: &PublicationRequest,
    prefix: &str,
    origin: &str,
) -> CapturedZipIntent {
    let mut providers = std::collections::BTreeMap::new();
    for name in archive
        .policy
        .leases
        .iter()
        .flat_map(|intent| &intent.providers)
    {
        let identity = ProviderIdentity::legacy(name, "noop", None, "psa", "psa");
        super::super::super::ledger::register_route(db, name, &identity)
            .await
            .unwrap();
        providers.insert(name.clone(), serde_json::json!({
            "selected": {"config_name": name, "route": identity.route_snapshot(), "enabled": true},
            "limits": limits()[name], "requests_per_second": null,
        }));
    }
    let decision = ExtensionDecision::capture(
        DecisionOrigin::new("principal", origin),
        "a".repeat(64), // Original fixture revision, not a current configuration.
        if archive
            .policy
            .leases
            .iter()
            .any(|i| i.source == LeaseSource::Manual)
        {
            DecisionEffect::Accepted
        } else {
            DecisionEffect::NoIntent
        },
        None,
        PinControl::from_tags(&archive.tags).unwrap(),
        &archive.policy,
    )
    .unwrap();
    decode_legacy_v1_intent(
        &serde_json::json!({
            "bucket": archive.object.bucket, "source_key": archive.object.key,
            "target_prefix": prefix, "root_capture": ZipRootCapture::Configured(false),
            "result_enabled": true, "decision": decision,
            "providers": providers.into_values().collect::<Vec<_>>(),
        })
        .to_string(),
    )
    .unwrap()
}

pub(super) async fn direct(
    db: &DatabaseConnection,
    archive: &PublicationRequest,
    prefix: &str,
) -> AdmittedZip {
    let tx = db.begin().await.unwrap();
    lock_bucket_for_ownership(&tx, &archive.object.bucket)
        .await
        .unwrap();
    let guard = crate::store::import::ownership::admit_content_and_prefix_mutation(
        &tx,
        &archive.object.bucket,
        &archive.object.key,
        prefix,
        SupersedeReason::DecompressZip,
        Utc::now(),
    )
    .await
    .unwrap();
    let capture = original_capture(&tx, archive, prefix, &guard.mutation_id).await;
    BatchAdmission::admit_legacy_in_transaction(&tx, &admission(&capture, "direct"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    AdmittedZip {
        id: guard.mutation_id.clone(),
        guard: Some(guard),
        capture,
    }
}

pub(super) async fn with_guard(
    db: &DatabaseConnection,
    archive: &PublicationRequest,
    prefix: &str,
    guard: &StandardMutationGuard,
) -> AdmittedZip {
    let tx = db.begin().await.unwrap();
    lock_bucket_for_ownership(&tx, &archive.object.bucket)
        .await
        .unwrap();
    let capture = original_capture(&tx, archive, prefix, &guard.mutation_id).await;
    BatchAdmission::admit_legacy_in_transaction(&tx, &admission(&capture, "direct"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    AdmittedZip {
        id: guard.mutation_id.clone(),
        guard: Some(guard.clone()),
        capture,
    }
}

impl AdmittedZip {
    pub(super) async fn prepare(
        &self,
        db: &DatabaseConnection,
        request: &mut ZipPublicationRequest,
    ) -> ZipBatchPublication {
        let entries = request
            .entries
            .iter()
            .map(|e| ExtractedEntry {
                key: e.key.clone(),
                cid: e.cid.clone(),
                size: e.logical_size,
            })
            .collect::<Vec<_>>();
        let manifest = final_zip_manifest(&entries, &[], self.capture.target_prefix());
        request.entries.retain(|entry| {
            manifest.successful.iter().any(|f| {
                f.object_key == entry.key && f.cid == entry.cid && f.size == entry.logical_size
            })
        });
        let batch = crate::store::entities::zip_batch::Entity::find_by_id(&self.id)
            .one(db)
            .await
            .unwrap()
            .unwrap();
        if !batch.manifest_prepared {
            zip_store::prepare_manifest(db, &self.id, &manifest.manifest_items())
                .await
                .unwrap();
        }
        ZipBatchPublication {
            batch_id: self.id.clone(),
            source_published: true,
            root_outcome: RootOutcome::Disabled,
            terminal_result: serde_json::json!({"archive_cid": request.archive.object.cid})
                .to_string(),
            entries: manifest
                .successful
                .iter()
                .map(|file| ZipBatchEntry {
                    path: file.relative_path.clone(),
                    object_id: request
                        .entries
                        .iter()
                        .find(|e| e.key == file.object_key)
                        .unwrap()
                        .id
                        .clone(),
                })
                .collect(),
        }
    }

    pub(super) async fn publish(
        &self,
        db: &DatabaseConnection,
        mut request: ZipPublicationRequest,
        limits: &ProviderLimitMap,
    ) -> AppResult<PublicationResult> {
        let batch = self.prepare(db, &mut request).await;
        let config = crate::pinning::config::ValidatedPinningConfig::from_raw(
            &crate::config::PinningConfig::default(),
            |_| None,
        )
        .unwrap();
        publish_decided_zip_batch(
            db,
            request,
            self.guard.clone(),
            batch,
            DecidedPublish {
                decision: self.capture.source_decision(),
                config: &config,
                mode: crate::config::OptionalPinControlMode::Warn,
                limits,
            },
        )
        .await
    }

    pub(super) async fn publish_import(
        &self,
        db: &DatabaseConnection,
        mut request: ZipPublicationRequest,
        guard: ImportPublicationGuard,
        result_rows: Vec<import_job_result::ActiveModel>,
        now: DateTime<Utc>,
        limits: &ProviderLimitMap,
    ) -> AppResult<PublicationResult> {
        let batch = self.prepare(db, &mut request).await;
        let config = crate::pinning::config::ValidatedPinningConfig::from_raw(
            &crate::config::PinningConfig::default(),
            |_| None,
        )
        .unwrap();
        publish_decided_import_zip_batch(
            db,
            request,
            guard,
            result_rows,
            now,
            batch,
            DecidedPublish {
                decision: self.capture.source_decision(),
                config: &config,
                mode: crate::config::OptionalPinControlMode::Warn,
                limits,
            },
        )
        .await
    }

    pub(super) async fn publish_completed(
        &self,
        db: &DatabaseConnection,
        target: &MultipartUploadTargetIdentity,
        mut request: ZipPublicationRequest,
        limits: &ProviderLimitMap,
    ) -> Result<PublicationResult, CommitCompletedUploadError> {
        let guard = self.completion_guard(db).await;
        request.archive.object.multipart = true;
        let batch = self.prepare(db, &mut request).await;
        let config = crate::pinning::config::ValidatedPinningConfig::from_raw(
            &crate::config::PinningConfig::default(),
            |_| None,
        )
        .unwrap();
        publish_decided_completed_zip_batch(
            db,
            target,
            request,
            Some(guard),
            batch,
            replay(),
            DecidedPublish {
                decision: self.capture.source_decision(),
                config: &config,
                mode: crate::config::OptionalPinControlMode::Warn,
                limits,
            },
        )
        .await
    }

    pub(super) async fn completion_guard(&self, db: &DatabaseConnection) -> StandardMutationGuard {
        let tx = db.begin().await.unwrap();
        lock_bucket_for_ownership(&tx, self.capture.bucket())
            .await
            .unwrap();
        let guard = crate::store::import::ownership::admit_content_and_prefix_mutation(
            &tx,
            self.capture.bucket(),
            self.capture.source_key(),
            self.capture.target_prefix(),
            SupersedeReason::DecompressZip,
            Utc::now(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        guard
    }
}

pub(super) async fn publish(
    db: &DatabaseConnection,
    request: ZipPublicationRequest,
    limits: &ProviderLimitMap,
) -> AppResult<PublicationResult> {
    let fixture = direct(db, &request.archive, "").await;
    fixture.publish(db, request, limits).await
}

pub(super) fn replay() -> ZipCompletedReplay {
    ZipCompletedReplay {
        owner: "principal".into(),
        parts: vec![s3s::dto::CompletedPart {
            part_number: Some(1),
            e_tag: Some(s3s::dto::ETag::Strong("bafy-part".into())),
            ..Default::default()
        }],
        response_xml: "<CompleteMultipartUploadResult/>".into(),
        server_side_encryption: None,
        pin_warning: None,
    }
}

pub(super) async fn upload(
    db: &DatabaseConnection,
    id: &str,
    archive: &PublicationRequest,
    prefix: &str,
) -> (AdmittedZip, MultipartUploadTargetIdentity) {
    let tx = db.begin().await.unwrap();
    lock_bucket_for_ownership(&tx, &archive.object.bucket)
        .await
        .unwrap();
    let capture = original_capture(&tx, archive, prefix, id).await;
    multipart::create_upload_with_zip_intent(
        &tx,
        id,
        &format!("encryption-{id}"),
        &archive.object.bucket,
        &archive.object.key,
        "none",
        None,
        None,
        Some("application/octet-stream"),
        None,
        &archive.tags,
        Some(prefix),
        true,
        Some(capture.source_decision()),
        Some(&capture),
    )
    .await
    .unwrap();
    let guard = crate::store::import::ownership::admit_content_and_prefix_mutation(
        &tx,
        &archive.object.bucket,
        &archive.object.key,
        prefix,
        SupersedeReason::DecompressZip,
        Utc::now(),
    )
    .await
    .unwrap();
    BatchAdmission::admit_legacy_in_transaction(&tx, &admission(&capture, "mpu"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    multipart::upsert_part(db, id, 1, "bafy-part", 3, "bafy-part")
        .await
        .unwrap();
    let replay = replay();
    BatchAdmission::capture_mpu_complete(
        db,
        &replay.owner,
        &archive.object.bucket,
        &archive.object.key,
        id,
        &replay.parts,
    )
    .await
    .unwrap();
    BatchAdmission::capture_prepared_archive(
        db,
        &replay.owner,
        &archive.object.bucket,
        &archive.object.key,
        id,
        &replay.parts,
        &archive.object.cid,
    )
    .await
    .unwrap();
    let upload = multipart::get_upload(db, id).await.unwrap();
    (
        AdmittedZip {
            id: id.into(),
            capture,
            guard: Some(guard),
        },
        MultipartUploadTargetIdentity {
            bucket: upload.bucket,
            key: upload.key,
            upload_id: upload.upload_id,
            initiated_at: upload.created_at,
        },
    )
}

pub(super) async fn claimed_import(
    db: &DatabaseConnection,
    id: &str,
    archive: &PublicationRequest,
    prefix: &str,
    now: DateTime<Utc>,
) -> (AdmittedZip, crate::import::ImportClaim) {
    let capture = original_capture(db, archive, prefix, id).await;
    let mut job = import_request(id, &archive.object.key, Some(prefix));
    job.tags = archive.tags.clone();
    crate::store::import::ownership::submit_captured_zip(db, job, capture.clone(), now)
        .await
        .unwrap();
    let tx = db.begin().await.unwrap();
    lock_bucket_for_ownership(&tx, &archive.object.bucket)
        .await
        .unwrap();
    BatchAdmission::admit_legacy_in_transaction(&tx, &admission(&capture, "import"))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let claim = claim_due(
        db,
        "import-worker",
        now,
        now + chrono::Duration::seconds(30),
        1,
    )
    .await
    .unwrap()
    .pop()
    .unwrap()
    .claim;
    // Store-only complete-tuple fixture, not a source pipeline/clean-EOF claim.
    bind_legacy_import_input(
        db,
        &claim,
        &"b".repeat(64),
        &archive.object.cid,
        archive.object.logical_size,
    )
    .await
    .unwrap();
    (
        AdmittedZip {
            id: id.into(),
            capture,
            guard: None,
        },
        claim,
    )
}
