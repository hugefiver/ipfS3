use super::*;

#[tokio::test]
async fn confirmed_archive_rejects_extra_same_plan_member() {
    let fixture = super::super::super::actions::tests::fixture("archive-members").await;
    let binding = &fixture.binding;
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "INSERT INTO pin_management_records (database_scope,plan_id,item_id,kind,native_identity,principal,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at) SELECT database_scope,plan_id,'extra',kind,native_identity,principal,binding,native_before,result,intent_ids,job_ids,state,created_at,expires_at,completed_at FROM pin_management_records WHERE plan_id=$1 AND kind='action'",
        [binding.request.plan_id.clone().into()])).await.unwrap();
    let source =
        evidence::Evidence::from_frame(&native::capture(&fixture.db).await.unwrap()).unwrap();
    let key = evidence::anchor_key(binding).unwrap();
    assert!(
        read_binding(
            &source,
            source.row(&key).unwrap(),
            &mut source.budget().unwrap()
        )
        .is_err()
    );
}

fn stored_leaf() -> (std::sync::Arc<native::NativeRow>, Key, RowVersion) {
    let key = Key(
        "pin_management_records".into(),
        vec![
            "scope".into(),
            "plan".into(),
            "item".into(),
            "action".into(),
        ],
    );
    let leaf = "[0,[\"key\"],[null,\"原文\"]]";
    let hash = identity(b"stage7/leaf/1\0", leaf.as_bytes());
    let head = Head(
        "scope".into(),
        "identity".into(),
        "local_configuration".into(),
        key.clone(),
        vec![],
        "a".repeat(64),
        "b".repeat(64),
        vec![hash.clone()],
    );
    let raw = assemble(
        &serde_json::to_string(&head).unwrap(),
        "opaque source core",
        "opaque source manifest",
        &[leaf.into()],
        &mut DecodeBudget::default(),
    )
    .unwrap();
    let row = [
        ("database_scope", "scope"),
        ("native_identity", "identity"),
        ("principal", "local_configuration"),
        ("plan_id", "plan"),
        ("item_id", "item"),
        ("kind", "action"),
    ]
    .into_iter()
    .map(|(column, value)| (column.into(), Some(value.into())))
    .chain(std::iter::once(("native_before".into(), Some(raw))))
    .collect();
    (
        std::sync::Arc::new(row),
        key.clone(),
        RowVersion(vec!["key".into()], key, 0, leaf.len(), hash),
    )
}

#[test]
fn batch_reuses_exact_source_and_leaf_data_without_caching_validation() {
    let (row, key, version) = stored_leaf();
    let mut work = DecodeBudget::default();
    let source = batch::source(
        row.clone(),
        &key,
        "scope",
        "identity",
        "local_configuration",
        &mut work,
    )
    .unwrap();
    let leaf = batch::leaf(&source, &version, &mut work).unwrap();
    let paid = work.usage();
    let again = batch::source(
        row.clone(),
        &key,
        "scope",
        "identity",
        "local_configuration",
        &mut work,
    )
    .unwrap();
    assert!(std::sync::Arc::ptr_eq(&source, &again));
    assert!(std::sync::Arc::ptr_eq(
        &leaf,
        &batch::leaf(&again, &version, &mut work).unwrap()
    ));
    assert_eq!(work.usage(), paid);
    // Cached bytes cannot carry another consumer's source coordinates or locator.
    for defect in 0..5 {
        let mut changed = RowVersion(
            version.0.clone(),
            key.clone(),
            0,
            version.3,
            version.4.clone(),
        );
        match defect {
            0 => changed.0[0].push('x'),
            1 => changed.1.1[2].push('x'),
            2 => changed.2 = 1,
            3 => changed.3 += 1,
            4 => changed.4 = "c".repeat(64),
            _ => unreachable!(),
        }
        assert!(batch::leaf(&source, &changed, &mut work).is_err());
    }
    assert!(
        batch::source(
            row.clone(),
            &key,
            "scope",
            "foreign",
            "local_configuration",
            &mut work
        )
        .is_err()
    );
    let mut foreign = row.as_ref().clone();
    foreign.insert("item_id".into(), Some("foreign".into()));
    assert!(
        batch::source(
            std::sync::Arc::new(foreign),
            &key,
            "scope",
            "identity",
            "local_configuration",
            &mut work
        )
        .is_err()
    );
    let mut other_manifest = Manifest(vec![], vec![(key.0.clone(), vec![])]);
    let head = &source.head;
    let catalog = [native::NativeTable {
        table: key.0.clone(),
        columns: vec![].into(),
        primary_key: vec![
            "database_scope".into(),
            "plan_id".into(),
            "item_id".into(),
            "kind".into(),
        ]
        .into(),
        rows: vec![],
        definitions: vec![].into(),
    }];
    other_manifest.1[0].1.push(RowVersion(
        vec![
            "scope".into(),
            "other".into(),
            "item".into(),
            "action".into(),
        ],
        key.clone(),
        0,
        version.3,
        version.4.clone(),
    ));
    // A source permitted by one manifest does not become permitted globally.
    let consumer = Head(
        head.0.clone(),
        head.1.clone(),
        head.2.clone(),
        Key(
            key.0.clone(),
            vec![
                "scope".into(),
                "consumer".into(),
                "item".into(),
                "action".into(),
            ],
        ),
        vec![],
        head.5.clone(),
        head.6.clone(),
        vec![],
    );
    assert!(
        other_manifest
            .validate(&consumer, &catalog, &mut work)
            .is_err()
    );
}

#[test]
fn leaf_materialization_cannot_reset_exhausted_batch_budget() {
    let (row, key, version) = stored_leaf();
    let mut work = DecodeBudget::default();
    let source = batch::source(
        row,
        &key,
        "scope",
        "identity",
        "local_configuration",
        &mut work,
    )
    .unwrap();
    let remaining = crate::pinning::management::wire::MAX_WIRE_BYTES - work.usage().1;
    work.consume(0, remaining).unwrap();
    assert!(matches!(
        batch::leaf(&source, &version, &mut work),
        Err(ManagementError::Budget)
    ));
    assert!(matches!(
        batch::leaf(&source, &version, &mut work),
        Err(ManagementError::Budget)
    ));
}

#[tokio::test]
async fn draft_and_empty_ack_confirmation_are_distinct_and_public_copies_are_prepaid() {
    let fixture = super::super::super::actions::tests::fixture("archive-draft").await;
    let binding = &fixture.binding;
    let key = evidence::anchor_key(binding).unwrap();
    let source = evidence::Evidence::load(
        &fixture.db,
        &binding.database_scope,
        &binding.database_identity,
        std::slice::from_ref(&binding.request.plan_id),
    )
    .await
    .unwrap();
    let row = source.row(&key).unwrap();
    assert!(read_binding(&source, row, &mut source.budget().unwrap()).unwrap() == *binding);
    assert!(validate_draft(&source, row, &mut source.budget().unwrap()).is_err());
    let review = binding.review();
    let mut output = Vec::new();
    let mut budget = DecodeBudget::default();
    crate::pinning::management::wire::encode_review(
        &mut budget,
        &review,
        review.candidate_item_ids(),
        &mut output,
    )
    .unwrap();
    assert!(budget.usage().1 >= output.len());
    let raw = native::value(row, "native_before").unwrap();
    let draft = encode_draft(raw, &mut DecodeBudget::default()).unwrap();
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_management_records SET binding=$1 WHERE plan_id=$2 AND kind='action'",
            [draft.into(), binding.request.plan_id.clone().into()],
        ))
        .await
        .unwrap();
    let draft_source = evidence::Evidence::load(
        &fixture.db,
        &binding.database_scope,
        &binding.database_identity,
        std::slice::from_ref(&binding.request.plan_id),
    )
    .await
    .unwrap();
    let row = draft_source.row(&key).unwrap();
    validate_draft(&draft_source, row, &mut draft_source.budget().unwrap()).unwrap();
    assert!(read_binding(&draft_source, row, &mut draft_source.budget().unwrap()).is_err());
    let empty_confirmed = binding
        .review()
        .confirmation_encoding(binding.confirmed_at, &[])
        .unwrap();
    fixture
        .db
        .execute(Statement::from_sql_and_values(
            fixture.db.get_database_backend(),
            "UPDATE pin_management_records SET binding=$1 WHERE plan_id=$2 AND kind='action'",
            [
                empty_confirmed.into(),
                binding.request.plan_id.clone().into(),
            ],
        ))
        .await
        .unwrap();
    let missing_ack_source = evidence::Evidence::load(
        &fixture.db,
        &binding.database_scope,
        &binding.database_identity,
        std::slice::from_ref(&binding.request.plan_id),
    )
    .await
    .unwrap();
    let row = missing_ack_source.row(&key).unwrap();
    assert!(
        validate_draft(
            &missing_ack_source,
            row,
            &mut missing_ack_source.budget().unwrap()
        )
        .is_err()
    );
    assert!(
        read_binding(
            &missing_ack_source,
            row,
            &mut missing_ack_source.budget().unwrap()
        )
        .is_err()
    );
    // The borrowed serializer cannot reset an exhausted operation budget.
    {
        let mut limited = source.budget().unwrap();
        let remaining = crate::pinning::management::wire::MAX_WIRE_BYTES - limited.usage().1;
        limited.consume(0, remaining).unwrap();
        assert!(
            crate::pinning::management::wire::encode_review(
                &mut limited,
                &review,
                review.candidate_item_ids(),
                &mut Vec::new()
            )
            .is_err()
        );
    }
    assert!(matches!(
        evidence::Evidence::load_with_work(
            &fixture.db,
            &binding.database_scope,
            &binding.database_identity,
            std::slice::from_ref(&binding.request.plan_id),
            source.work.clone()
        )
        .await,
        Err(ManagementError::Budget)
    ));
}

#[tokio::test]
async fn foreign_history_is_preserved_without_borrowing_foreign_authority() {
    use crate::pinning::management::db_snapshot;
    let fixture = super::super::super::actions::tests::fixture("archive-foreign-source").await;
    let foreign = evidence::anchor_key(&fixture.binding).unwrap();
    let refs = foreign
        .values
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let work = SharedDecodeBudget::default();
    let mut expected = super::super::current_row(
        &fixture.db,
        &foreign.table,
        &refs,
        DecodeWork::shared(&work),
    )
    .await
    .unwrap();
    expected.insert("principal".into(), Some("foreign".into()));
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),
        "UPDATE pin_management_records SET principal='foreign' WHERE database_scope=$1 AND plan_id=$2 AND item_id=$3 AND kind=$4",
        foreign.values.iter().cloned().map(Into::into).collect::<Vec<sea_orm::Value>>(),
    )).await.unwrap();
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "archive-foreign-consumer".into();
    // A late principal change is unsealed data, never replacement authority.
    assert!(matches!(
        db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request).await,
        Err(db_snapshot::SnapshotError::Changed)
    ));
    assert!(
        super::super::load_confirmed_authority(
            &fixture.db,
            &fixture.auth,
            &fixture.binding.request,
            work.clone()
        )
        .await
        .is_err()
    );
    assert_eq!(
        super::super::current_row(
            &fixture.db,
            &foreign.table,
            &refs,
            DecodeWork::shared(&work)
        )
        .await
        .unwrap(),
        expected
    );
}

#[tokio::test]
async fn same_plan_uses_approved_owner_order_and_rechecks_every_members_binding_and_metadata() {
    use crate::pinning::management::{self as management, PublicVersionId, Subject, db_snapshot};
    use crate::store::{
        entities::object_version,
        pinning::publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
    };
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    let fixture = super::super::super::actions::tests::fixture("archive-multi-base").await;
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "archive-multi".into();
    request.items[0].item_id = "z".into();
    let mut second = request.items[0].clone();
    second.item_id = "a".into();
    let Subject::Object { identity, .. } = &mut second.subject else {
        unreachable!()
    };
    let id = uuid::Uuid::new_v4().to_string();
    publication::publish_object(
        &fixture.db,
        PublicationRequest {
            object: PublicationObject::from_put(
                id.clone(),
                "bucket",
                "second",
                identity.cid.clone(),
                10,
                None,
                None,
                false,
                None,
                None,
                Utc::now(),
            ),
            tags: vec![],
            policy: crate::pinning::policy::PublicationPolicy {
                tags: vec![],
                leases: vec![],
            },
            object_target: PinTargetSpec {
                cid: identity.cid.clone(),
                logical_size: 10,
            },
        },
        &fixture.config.pinning.provider_limits,
    )
    .await
    .unwrap();
    let version = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&id))
        .one(&fixture.db)
        .await
        .unwrap()
        .unwrap();
    identity.object_id = id;
    identity.key = "second".into();
    identity.private_version_id = version.id;
    identity.public_version = version
        .version_id
        .map_or(PublicVersionId::Null, PublicVersionId::Version);
    management::evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &crate::kubo::KuboClient::new(fixture.config.source_endpoint.clone()),
        &management::evidence::PrepareRequest {
            record_id: "archive-multi-read".into(),
            identity: identity.clone(),
            expected_node: fixture.config.source_node().unwrap().into(),
            max_stored_bytes: 10,
            ttl: chrono::Duration::minutes(15),
            directory: false,
        },
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    request.items.push(second);
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = management::dry_run(&request, &snapshot).unwrap();
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == management::PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let confirmed = super::super::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    assert_eq!(confirmed.request.items[0].item_id, "z");
    assert_eq!(confirmed.confirmation.bindings[0].request.item_id, "a");
    let source = evidence::Evidence::load(
        &fixture.db,
        &confirmed.database_scope,
        &confirmed.database_identity,
        &[request.plan_id.clone()],
    )
    .await
    .unwrap();
    let mut key = evidence::anchor_key(&confirmed).unwrap();
    assert_eq!(key.values[2], "a");
    key.values[2] = "z".into();
    assert!(
        read_binding(
            &source,
            source.row(&key).unwrap(),
            &mut source.budget().unwrap()
        )
        .unwrap()
            == confirmed
    );
    // One member switching back to draft cannot borrow its owner's confirmation.
    let owner_key = evidence::anchor_key(&confirmed).unwrap();
    let draft = encode_draft(
        native::value(source.row(&owner_key).unwrap(), "native_before").unwrap(),
        &mut DecodeBudget::default(),
    )
    .unwrap();
    fixture.db.execute(Statement::from_sql_and_values(fixture.db.get_database_backend(),"UPDATE pin_management_records SET binding=$1 WHERE plan_id=$2 AND item_id='z' AND kind='action'",[draft.into(),request.plan_id.clone().into()])).await.unwrap();
    let changed = evidence::Evidence::from_frame_with_work(
        &native::capture(&fixture.db).await.unwrap(),
        source.work.clone(),
    )
    .unwrap();
    assert!(matches!(
        read_binding(
            &changed,
            changed.row(&owner_key).unwrap(),
            &mut changed.budget().unwrap()
        ),
        Err(ManagementError::Confirmation)
    ));
}

#[test]
fn sections_borrow_exact_utf8_and_reject_overflow_or_trailing_data() {
    let mut work = DecodeBudget::default();
    let leaves = vec!["[0,[\"k\"],[null,\"\",\"原文\\n\",\"2026-10-15 00:00:00.123456\"]]".into()];
    let raw = assemble("[]", "{}", "[]", &leaves, &mut work).unwrap();
    let sections = Sections::parse(&raw, &mut work).unwrap();
    assert_eq!(&raw[sections.leaves[0].clone()], leaves[0]);
    assert!(Sections::parse(&(raw.clone() + "x"), &mut work).is_err());
    let mut invalid = raw.into_bytes();
    invalid[29..37].copy_from_slice(b"ffffffff");
    assert!(Sections::parse(std::str::from_utf8(&invalid).unwrap(), &mut work).is_err());
}

#[test]
fn terminal_before_preserves_raw_bytes_and_requires_exact_member_key() {
    let key = Key(
        "pin_management_records".into(),
        vec![
            "scope".into(),
            "plan".into(),
            "item".into(),
            "action".into(),
        ],
    );
    let raw = "S7L1\n[\"opaque terminal, not recursively decoded\"]";
    let manifest = Manifest(
        vec![],
        vec![(
            key.0.clone(),
            vec![RowVersion(key.1.clone(), key.clone(), 0, 0, "a".repeat(64))],
        )],
    );
    let cell = Cell::Before(
        Key(key.0.clone(), key.1.clone()),
        raw.len(),
        before_identity(&key, raw).unwrap(),
    );
    assert_eq!(
        raw_cell(&cell, "native_before", &key, &manifest, |_| Ok(raw)).unwrap(),
        Some(raw)
    );
    assert!(raw_cell(&cell, "binding", &key, &manifest, |_| Ok(raw)).is_err());
    assert!(
        raw_cell(
            &cell,
            "native_before",
            &key,
            &Manifest(vec![], vec![]),
            |_| Ok(raw)
        )
        .is_err()
    );
    assert!(raw_cell(&cell, "native_before", &key, &manifest, |_| Ok("changed")).is_err());
    let mut work = DecodeBudget::default();
    for raw in ["null", "\"\"", "\"原文\""] {
        let parsed: Cell = work.typed(raw).unwrap();
        assert_eq!(serde_json::to_string(&parsed).unwrap(), raw);
    }
    assert!(
        work.typed::<Cell>("[[\"pin_management_records\",[]],0,\"hash\",0]")
            .is_err()
    );
}
