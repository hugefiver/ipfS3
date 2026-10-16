use super::*;
use crate::pinning::management::{self as management, db_snapshot, wire};
use crate::store::pinning::management::actions;

#[tokio::test]
async fn shared_records_restore_exact_frames_and_reject_missing_or_tampered_sources() {
    let fixture = actions::tests::fixture("shared-private").await;
    let binding = &fixture.binding;
    let source = Evidence::load(
        &fixture.db,
        &binding.database_scope,
        &binding.database_identity,
        std::slice::from_ref(&binding.request.plan_id),
    )
    .await
    .unwrap();
    let key = anchor_key(binding).unwrap();
    let row = source.row(&key).unwrap();
    assert!(decode_binding_from_row(&source, row).unwrap() == *binding);
    assert!(
        binding
            .view()
            .frozen_rows()
            .unwrap()
            .row_key(&key.table, &key.values)
            .is_none()
    );
    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, binding)
        .await
        .unwrap();
    let frame = native::capture(&fixture.db).await.unwrap();
    assert!(
        results::private_action(&frame, &key)
            .unwrap()
            .unwrap()
            .action_view()
            == &applied.items[0]
    );
    let public: NativeFrame =
        serde_json::from_str(&serde_json::to_string(&frame).unwrap()).unwrap();
    assert!(results::private_action(&public, &key).is_err());
    let mut source = Evidence::from_frame(&frame).unwrap();
    let row = source.row(&key).unwrap().clone();
    let original = native::value(&row, "native_before").unwrap().to_owned();
    Arc::make_mut(Arc::make_mut(&mut source.rows).get_mut(&key).unwrap())
        .insert("native_before".into(), Some(format!("{original} ")));
    assert!(decode_binding_from_row(&source, &row).is_err());
    Arc::make_mut(&mut source.rows).remove(&key);
    assert!(decode_binding_from_row(&source, &row).is_err());
}

#[tokio::test]
async fn immutable_original_cells_reuse_parsing_without_reusing_authority() {
    let fixture = actions::tests::fixture("immutable-decoding").await;
    let binding = &fixture.binding;
    let mut source = Evidence::load(
        &fixture.db,
        &binding.database_scope,
        &binding.database_identity,
        std::slice::from_ref(&binding.request.plan_id),
    )
    .await
    .unwrap();
    let key = anchor_key(binding).unwrap();
    let row = source.row(&key).unwrap().clone();
    source.read_binding(&row).unwrap();
    assert!(decode_binding_from_row(&source, &row).unwrap() == *binding);
    let mut changed = row.clone();
    changed.insert(
        "native_before".into(),
        Some(format!(
            "{} ",
            native::value(&row, "native_before").unwrap()
        )),
    );
    Arc::make_mut(&mut source.rows).insert(key, Arc::new(changed.clone()));
    assert!(source.read_binding(&changed).is_err());
}

#[tokio::test]
async fn multiple_items_share_one_immutable_proposal_and_frozen_history_retains_raw_cells() {
    let fixture = actions::tests::fixture("private-history").await;
    let mut request = (*fixture.binding.request).clone();
    request.plan_id = "private-multi".into();
    request.items[0].item_id = "a".into();
    let mut second = request.items[0].clone();
    second.item_id = "z".into();
    let management::Subject::Object { identity, .. } = &mut second.subject else {
        unreachable!()
    };
    let id = uuid::Uuid::new_v4().to_string();
    use crate::store::{
        entities::object_version,
        pinning::publication::{self, PinTargetSpec, PublicationObject, PublicationRequest},
    };
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
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
    identity.public_version = version.version_id.map_or(
        management::PublicVersionId::Null,
        management::PublicVersionId::Version,
    );
    management::evidence::prepare(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &crate::kubo::KuboClient::new(fixture.config.source_endpoint.clone()),
        &management::evidence::PrepareRequest {
            record_id: "private-second-read".into(),
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
    let imported = wire::decode_plan(
        &wire::encode_plan(&request, &snapshot, &plan, Some(&plan.confirmation(ack))).unwrap(),
    )
    .unwrap();
    let before_stage = native::capture(&fixture.db).await.unwrap();
    for body in [
        "SELECT RAISE(ABORT,'last item fault');",
        "UPDATE pin_management_records SET result=printf('%.*c',4194305,'x') WHERE plan_id=NEW.plan_id AND item_id=NEW.item_id;",
    ] {
        fixture.db.execute_unprepared(&format!("CREATE TRIGGER private_staging_failure AFTER INSERT ON pin_management_records WHEN NEW.plan_id='private-multi' AND NEW.item_id='z' BEGIN {body} END")).await.unwrap();
        assert!(
            stage_confirmation(
                &fixture.db,
                &fixture.config,
                &fixture.auth,
                &request,
                &snapshot,
                &plan,
                chrono::Duration::minutes(5)
            )
            .await
            .is_err()
        );
        fixture
            .db
            .execute_unprepared("DROP TRIGGER private_staging_failure")
            .await
            .unwrap();
        assert!(native::capture(&fixture.db).await.unwrap() == before_stage);
    }
    stage_confirmation(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &request,
        &snapshot,
        &plan,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    assert!(
        load_confirmed_from_imported(&fixture.db, &fixture.auth, &imported)
            .await
            .is_err()
    );
    let staged = native::capture(&fixture.db).await.unwrap();
    fixture.db.execute_unprepared("CREATE TRIGGER private_confirmation_failure BEFORE UPDATE OF binding ON pin_management_records WHEN NEW.plan_id='private-multi' AND NEW.item_id='z' BEGIN SELECT RAISE(ABORT,'confirmation fault'); END").await.unwrap();
    assert!(
        confirm_imported(&fixture.db, &fixture.config, &fixture.auth, &imported)
            .await
            .is_err()
    );
    fixture
        .db
        .execute_unprepared("DROP TRIGGER private_confirmation_failure")
        .await
        .unwrap();
    assert!(native::capture(&fixture.db).await.unwrap() == staged);
    let binding = confirm_imported(&fixture.db, &fixture.config, &fixture.auth, &imported)
        .await
        .unwrap();
    let confirmed = native::capture(&fixture.db).await.unwrap();
    for item in ["a", "z"] {
        let key = [
            binding.database_scope.as_str(),
            request.plan_id.as_str(),
            item,
            "action",
        ];
        assert_eq!(
            staged.original_value("pin_management_records", &key, "native_before"),
            confirmed.original_value("pin_management_records", &key, "native_before")
        );
    }
    assert!(
        confirm_imported(&fixture.db, &fixture.config, &fixture.auth, &imported)
            .await
            .unwrap()
            == binding
    );
    let source = Evidence::from_frame(&confirmed).unwrap();
    for row in source.plan_rows(&request.plan_id) {
        let restored = archive::load_review(&source, &row, &mut source.budget().unwrap()).unwrap();
        assert!(
            restored
                .rows()
                .all_rows()
                .map(|(table, row)| (&table.table, row))
                .eq(FrozenRows::Frame(snapshot.native_frame.as_ref().unwrap())
                    .all_rows()
                    .map(|(table, row)| (&table.table, row)))
        );
        assert!(decode_binding_from_row(&source, &row).unwrap() == binding);
    }
    let root_key = anchor_key(&binding).unwrap();
    let mut nonanchor = root_key.clone();
    nonanchor.values[2] = "z".into();
    let row = source.row(&nonanchor).unwrap().clone();
    let missing_anchor = confirmed
        .clone()
        .without_records(std::slice::from_ref(&root_key));
    let missing = Evidence::from_frame(&missing_anchor).unwrap();
    assert!(decode_binding_from_row(&missing, &row).is_err());
    let historical_key = anchor_key(&fixture.binding).unwrap();
    let old_raw = native::value(source.row(&historical_key).unwrap(), "native_before").unwrap();
    let review = source.read_binding(source.row(&root_key).unwrap()).unwrap();
    assert_eq!(
        review
            .view()
            .rows
            .unwrap()
            .original_cell(
                &historical_key.table,
                &historical_key.values,
                "native_before"
            )
            .unwrap()
            .as_deref(),
        Some(old_raw)
    );
    let mut next = request.clone();
    next.plan_id = "private-next-generation".into();
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &next)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = management::dry_run(&next, &snapshot).unwrap();
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == management::PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let next = super::super::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&next, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    let source = Evidence::load(
        &fixture.db,
        &next.database_scope,
        &next.database_identity,
        std::slice::from_ref(&next.request.plan_id),
    )
    .await
    .unwrap();
    let restored = archive::load_review(
        &source,
        source.row(&anchor_key(&next).unwrap()).unwrap(),
        &mut source.budget().unwrap(),
    )
    .unwrap();
    let refs = nonanchor
        .values
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        restored.original_value(&nonanchor.table, &refs, "native_before"),
        confirmed.original_value(&nonanchor.table, &refs, "native_before")
    );
    assert!(
        restored
            .rows()
            .all_rows()
            .map(|(table, row)| (&table.table, row))
            .eq(FrozenRows::Frame(snapshot.native_frame.as_ref().unwrap())
                .all_rows()
                .map(|(table, row)| (&table.table, row)))
    );
}

#[tokio::test]
async fn retirement_roots_classify_before_authority_and_fail_closed() {
    use crate::store::pinning::management::admission;
    let fixture = actions::tests::fixture("retirement-shared-history").await;
    let route = fixture.config.pinning.providers[0].route_snapshot();
    let request = management::PlanRequest {
        plan_id: "retirement-whole-packed".into(),
        items: vec![management::ItemRequest {
            item_id: "provider".into(),
            action: management::Action::Retire,
            subject: management::Subject::Provider {
                route: management::RouteIdentity::from_original(route.clone()),
            },
        }],
    };
    let snapshot = db_snapshot::collect_with_config(&fixture.db, &fixture.config, &request)
        .await
        .unwrap()
        .planning_snapshot()
        .unwrap();
    let plan = management::dry_run(&request, &snapshot).unwrap();
    let binding = super::super::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        vec![],
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    assert!(
        admission::registration_closed(&fixture.db, &route)
            .await
            .unwrap()
    );
    let mut other = route.clone();
    other.scope = "different-private-domain".into();
    assert!(
        !admission::registration_closed(&fixture.db, &other)
            .await
            .unwrap()
    );
    let frame = native::capture(&fixture.db).await.unwrap();
    let source = Evidence::from_frame(&frame).unwrap();
    let key = anchor_key(&binding).unwrap();
    let action = source.row(&key).unwrap();
    let retirement = frame
        .table("pin_management_retirements")
        .unwrap()
        .rows
        .first()
        .unwrap();
    let domain = native::value(retirement, "allocation_domain").unwrap();
    let retirement = frame
        .original_row("pin_management_retirements", &[domain])
        .unwrap();
    // A different domain may skip private authority only after root integrity.
    for defect in 0..6 {
        use sea_orm::TransactionTrait;
        let tx = fixture.db.begin().await.unwrap();
        if defect == 0 {
            tx.execute(Statement::from_sql_and_values(tx.get_database_backend(),"UPDATE pin_management_records SET native_before=$1 WHERE plan_id=$2 AND kind='action'",["{malformed".into(),request.plan_id.clone().into()])).await.unwrap();
        } else if defect == 1 {
            tx.execute_unprepared("PRAGMA defer_foreign_keys=ON")
                .await
                .unwrap();
            tx.execute(Statement::from_sql_and_values(
                tx.get_database_backend(),
                "DELETE FROM pin_management_records WHERE plan_id=$1 AND kind='action'",
                [request.plan_id.clone().into()],
            ))
            .await
            .unwrap();
        } else if defect < 4 {
            let mut changed: serde_json::Value = serde_json::from_str(
                native::value(action, "binding")
                    .unwrap()
                    .strip_prefix("S7B1\n")
                    .unwrap(),
            )
            .unwrap();
            if defect == 2 {
                changed[1][1][2] = serde_json::json!("wrong-owner");
            } else {
                changed[2] = serde_json::json!("0".repeat(64));
            }
            let changed = format!("S7B1\n{changed}");
            tx.execute(Statement::from_sql_and_values(
                tx.get_database_backend(),
                "UPDATE pin_management_records SET binding=$1 WHERE plan_id=$2 AND kind='action'",
                [changed.clone().into(), request.plan_id.clone().into()],
            ))
            .await
            .unwrap();
            tx.execute(Statement::from_sql_and_values(
                tx.get_database_backend(),
                "UPDATE pin_management_retirements SET binding=$1 WHERE plan_id=$2",
                [changed.into(), request.plan_id.clone().into()],
            ))
            .await
            .unwrap();
        } else {
            let mut changed: serde_json::Value = serde_json::from_str(
                native::value(&retirement, "inventory")
                    .unwrap()
                    .strip_prefix("S7I1\n")
                    .unwrap(),
            )
            .unwrap();
            if defect == 4 {
                changed[1] = serde_json::json!("0".repeat(64));
            } else {
                changed[0][1][2] = serde_json::json!("wrong-owner");
            }
            tx.execute(Statement::from_sql_and_values(
                tx.get_database_backend(),
                "UPDATE pin_management_retirements SET inventory=$1 WHERE plan_id=$2",
                [
                    format!("S7I1\n{changed}").into(),
                    request.plan_id.clone().into(),
                ],
            ))
            .await
            .unwrap();
        }
        assert!(
            admission::registration_closed(&tx, &other).await.is_err(),
            "root defect {defect}"
        );
        assert!(
            admission::lock_open_in_transaction(&tx, &[other.clone()])
                .await
                .is_err(),
            "root defect {defect} must block an open birth"
        );
        assert!(admission::registration_closed(&tx, &route).await.is_err());
        tx.rollback().await.unwrap();
    }
    assert!(
        admission::registration_closed(&fixture.db, &route)
            .await
            .unwrap()
    );
    assert!(
        !admission::registration_closed(&fixture.db, &other)
            .await
            .unwrap()
    );
    for corrupt in ["stage7/lz1:8:AQAAAQA=", "{malformed"] {
        use sea_orm::TransactionTrait;
        let tx = fixture.db.begin().await.unwrap();
        tx.execute(Statement::from_sql_and_values(
            tx.get_database_backend(),
            "UPDATE pin_management_records SET result=$1 WHERE plan_id=$2 AND kind='action'",
            [corrupt.into(), request.plan_id.clone().into()],
        ))
        .await
        .unwrap();
        assert!(admission::registration_closed(&tx, &route).await.is_err());
        assert!(admission::registration_closed(&tx, &other).await.is_err());
        tx.rollback().await.unwrap();
    }
}

#[test]
fn decoding_accumulates_nodes_and_checks_owned_cells_before_allocation() {
    let first = serde_json::to_string(&vec![0; wire::MAX_WIRE_NODES / 2]).unwrap();
    let mut budget = Budget::default();
    budget.typed::<serde_json::Value>(&first).unwrap();
    assert!(matches!(
        budget.typed::<serde_json::Value>(&first),
        Err(ManagementError::Budget)
    ));
    let plain = serde_json::to_string(&"x".repeat(8192)).unwrap();
    let mut budget = Budget::default();
    budget
        .consume(0, wire::MAX_WIRE_BYTES - plain.len() - 8192 + 1)
        .unwrap();
    assert!(matches!(
        budget.typed::<serde_json::Value>(&plain),
        Err(ManagementError::Budget)
    ));
    assert!(Budget::default().typed::<serde_json::Value>(&plain).is_ok());
}

#[tokio::test]
async fn trusted_result_uses_the_remaining_session_budget() {
    let fixture = actions::tests::fixture("result-cumulative-budget").await;
    actions::apply(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        &fixture.binding,
    )
    .await
    .unwrap();
    let frame = native::capture(&fixture.db).await.unwrap();
    let source = Evidence::from_frame(&frame).unwrap();
    let key = anchor_key(&fixture.binding).unwrap();
    let original = source.row(&key).unwrap().clone();
    source.read_binding(&original).unwrap();
    let remaining = wire::MAX_WIRE_BYTES - source.budget().unwrap().bytes;
    source.budget().unwrap().bytes(remaining).unwrap();
    assert!(matches!(
        results::trusted_from_record(
            &source,
            &frame.database_scope,
            &frame.database_identity,
            &original
        ),
        Err(ManagementError::Budget)
    ));
    let source = Evidence::from_frame(&frame).unwrap();
    assert!(
        results::trusted_from_record(
            &source,
            &frame.database_scope,
            &frame.database_identity,
            &original
        )
        .unwrap()
        .is_some()
    );
}
