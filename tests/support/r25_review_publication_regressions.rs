use super::*;

#[tokio::test]
async fn fresh_first_intake_capture_and_exact_native_guards_are_ready_before_publication() {
    let (_dir, first, second) = setup().await;
    let claim = prepared(&first, &[("one", "out/one", "cid-1")]).await;
    let execution = execution::read(&second, &claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    let capture = publication::decode_zip_capture_json(&execution.captured_options).expect(
        "fresh first intake must persist complete capture, not revision-only authorization",
    );
    assert_eq!(capture.origin().principal_id, "principal");
    assert_eq!(capture.origin().request_id, claim.batch_id);
    assert!(!capture.controls().publish_source);
    assert!(!capture.controls().auto_pin_outputs);
    assert_eq!(capture.v2_options().unwrap().targets, ZipTargets::None);
    assert_eq!(
        execution.input_sha256.as_deref(),
        Some("a".repeat(64).as_str())
    );
    assert_eq!(execution.input_art_cid.as_deref(), Some("input-cid"));
    assert_eq!(execution.input_art_size, Some(99));
    assert_eq!(execution.epoch, claim.epoch);
    assert_eq!(execution.worker.as_deref(), Some(claim.worker.as_str()));
    let batch = zip::read(&second, &claim.batch_id).await.unwrap().unwrap();
    assert_eq!(batch.captured_options, execution.captured_options);
    assert_eq!(batch.fingerprint, "pending");
    assert_eq!(batch.input_identity, "pending");
    assert_eq!(batch.state, "open");
    let tx = first.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    let guards = execution::verify_in_transaction(&tx, &claim).await.unwrap();
    assert_eq!(guards.len(), 1);
    assert_eq!(guards[0].key, "out/one");
    assert_eq!(guards[0].mutation_id, "guard-out/one");
    tx.commit().await.unwrap();
    assert_eq!(object::Entity::find().count(&second).await.unwrap(), 0);
    assert_eq!(
        object_version::Entity::find().count(&second).await.unwrap(),
        0
    );
    assert_eq!(pin_lease::Entity::find().count(&second).await.unwrap(), 0);
}

#[tokio::test]
async fn historical_null_or_current_revision_without_original_evidence_never_backfills_permission()
{
    let config = pin_config();
    let rules = output_rules(&config);
    for revision in [None, Some(rules.revision())] {
        let (_dir, first, second) = setup().await;
        let claim = r25_review_publication::historical_revision_only(
            &first,
            &[("one", "out/a/one", "cid-1")],
            ZipTargets::Extracted,
            revision,
        )
        .await;
        let original = execution::read(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap();
        let mirror = zip::snapshot(&first, &claim.batch_id)
            .await
            .unwrap()
            .unwrap();
        let mut request = request(
            claim.clone(),
            vec![success("one", "out/a/one", "cid-1", "one")],
        );
        request.targets = ZipTargets::Extracted;
        request.captured_rule_revision = revision.map(str::to_owned);
        let error =
            publication::publish_zip_v2(&second, request, &rules, &config, &config.provider_limits)
                .await
                .unwrap_err();
        assert!(
            matches!(error, ipfs_s3_gateway::error::AppError::InvalidPinningRequest(ref message) if message.contains("needs_attention"))
        );
        assert_eq!(
            execution::read(&second, &claim.batch_id)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        let after = zip::snapshot(&second, &claim.batch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.batch, mirror.batch);
        assert_eq!(after.entries, mirror.entries);
        assert_eq!(object::Entity::find().count(&second).await.unwrap(), 0);
        assert_eq!(
            object_version::Entity::find().count(&second).await.unwrap(),
            0
        );
        assert_eq!(pin_lease::Entity::find().count(&second).await.unwrap(), 0);
        assert_eq!(
            pin_lease_target::Entity::find()
                .count(&second)
                .await
                .unwrap(),
            0
        );
        assert_eq!(remote_pin::Entity::find().count(&second).await.unwrap(), 0);
        let tx = first.begin().await.unwrap();
        store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
            .await
            .unwrap();
        assert_eq!(
            execution::verify_in_transaction(&tx, &claim)
                .await
                .unwrap()
                .len(),
            1
        );
        tx.commit().await.unwrap();
    }
}

#[tokio::test]
async fn original_capture_survives_config_drift_and_native_terminal_tail_rolls_back_exact_versions()
{
    let (_dir, first, second) = setup().await;
    let original_config = pin_config();
    for provider in &original_config.providers {
        ledger::register_route(&first, &provider.name, &provider.identity)
            .await
            .unwrap();
    }
    let claim = prepared_with_controls(
        &first,
        &[("one", "out/a/one", "cid-1")],
        false,
        ZipTargets::Extracted,
    )
    .await;
    let original = execution::read(&first, &claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    let capture = publication::decode_zip_capture_json(&original.captured_options).unwrap();
    assert!(capture.controls().auto_pin_outputs);
    let later_config = ValidatedPinningConfig::from_raw(&Default::default(), |_| None).unwrap();
    let later_rules = ValidatedZipOutputRules::compile(&[], &later_config).unwrap();
    assert_ne!(capture.policy_revision(), later_rules.revision());
    let mut request = request(
        claim.clone(),
        vec![success("one", "out/a/one", "cid-1", "one")],
    );
    request.targets = ZipTargets::Extracted;
    request.captured_rule_revision = Some(capture.policy_revision().into());
    first.execute_unprepared("CREATE TRIGGER fail_review_publication_tail BEFORE UPDATE OF state ON zip_v2_executions WHEN NEW.state='completed' BEGIN SELECT RAISE(FAIL, 'review publication terminal tail'); END;").await.unwrap();
    let error = publication::publish_zip_v2(
        &second,
        request.clone(),
        &later_rules,
        &later_config,
        &later_config.provider_limits,
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, ipfs_s3_gateway::error::AppError::Database(_)),
        "expected native terminal-tail fault after publication writes: {error:?}"
    );
    assert_eq!(
        execution::read(&second, &claim.batch_id)
            .await
            .unwrap()
            .unwrap(),
        original
    );
    let mirror = zip::snapshot(&second, &claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mirror.batch.state, "open");
    assert_eq!(mirror.batch.captured_options, original.captured_options);
    assert!(
        mirror
            .entries
            .iter()
            .all(|entry| entry.version_row_id.is_none())
    );
    assert_eq!(object::Entity::find().count(&second).await.unwrap(), 0);
    assert_eq!(
        object_version::Entity::find().count(&second).await.unwrap(),
        0
    );
    assert_eq!(pin_lease::Entity::find().count(&second).await.unwrap(), 0);
    assert_eq!(
        pin_lease_target::Entity::find()
            .count(&second)
            .await
            .unwrap(),
        0
    );
    assert_eq!(remote_pin::Entity::find().count(&second).await.unwrap(), 0);
    assert!(
        pin_provider_usage::Entity::find()
            .all(&second)
            .await
            .unwrap()
            .iter()
            .all(|usage| usage.reserved_bytes == 0 && usage.reserved_pins == 0)
    );
    let tx = first.begin().await.unwrap();
    store::import::ownership::lock_bucket_for_ownership(&tx, "bucket")
        .await
        .unwrap();
    assert_eq!(
        execution::verify_in_transaction(&tx, &claim)
            .await
            .unwrap()
            .len(),
        1
    );
    tx.commit().await.unwrap();
    first
        .execute_unprepared("DROP TRIGGER fail_review_publication_tail")
        .await
        .unwrap();
    publication::publish_zip_v2(
        &second,
        request,
        &later_rules,
        &later_config,
        &later_config.provider_limits,
    )
    .await
    .unwrap();
    let execution = execution::read(&first, &claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(execution.state, "completed");
    assert_eq!(execution.captured_options, original.captured_options);
    let mirror = zip::snapshot(&first, &claim.batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mirror.batch.state, "published");
    assert_eq!(mirror.batch.captured_options, original.captured_options);
    assert_eq!(mirror.entries.len(), 1);
    let version =
        object_version::Entity::find_by_id(mirror.entries[0].version_row_id.clone().unwrap())
            .one(&first)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(version.object_id.as_deref(), Some("one"));
    assert_eq!(version.key, "out/a/one");
    let leases = pin_lease::Entity::find().all(&first).await.unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].owner_object_id, version.object_id.unwrap());
    assert_eq!(leases[0].source, "automatic");
    assert_eq!(
        (leases[0].expires_at - leases[0].created_at).num_seconds(),
        3600
    );
    let targets = pin_lease_target::Entity::find().all(&first).await.unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].provider, "a");
    assert_eq!(remote_pin::Entity::find().count(&first).await.unwrap(), 1);
}
