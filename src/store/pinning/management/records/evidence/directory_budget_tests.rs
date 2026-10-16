use super::*;
use crate::pinning::management::{self as management, *};
use crate::stage7_management_test_support::base;
#[allow(dead_code, unused_imports)]
#[path = "../../../../../../tests/support/stage7_management_actions_directory.rs"]
mod support;

#[tokio::test]
async fn directory_receipts_use_snapshot_and_worker_source_remaining_work() {
    use crate::store::pinning::management::actions;
    let fixture = support::Fixture::new("cid", "all", None, false).await;
    let identity = fixture.publish("directory-budget", false).await;
    fixture
        .prepare(identity.clone(), "older-directory-proof")
        .await;
    let proof = fixture
        .prepare(identity.clone(), "current-directory-proof")
        .await;
    assert!(proof.directory_receipt.is_some());
    let request = fixture.request("directory-budget", identity);
    let snapshot = fixture.snapshot(&request).await;
    let before = native::capture(&fixture.db).await.unwrap();
    let mut measured = Budget::default();
    management::validate_snapshot_view_with_work((&snapshot).into(), &mut measured).unwrap();
    assert!(measured.nodes > 0 && measured.bytes > 0);
    for nodes in [true, false] {
        let mut limited = Budget::default();
        if nodes {
            limited
                .nodes(management::wire::MAX_WIRE_NODES - measured.nodes + 1)
                .unwrap();
        } else {
            limited
                .bytes(management::wire::MAX_WIRE_BYTES - measured.bytes + 1)
                .unwrap();
        }
        assert_eq!(
            management::validate_snapshot_view_with_work((&snapshot).into(), &mut limited),
            Err(SafeError::LimitExceeded)
        );
    }
    assert!(native::capture(&fixture.db).await.unwrap() == before);
    let plan = dry_run(&request, &snapshot).unwrap();
    let ack = plan
        .items()
        .iter()
        .filter(|entry| entry.status == PlanStatus::Candidate)
        .map(|entry| entry.request.item_id.clone())
        .collect();
    let binding = super::super::confirm(
        &fixture.db,
        &fixture.config,
        &fixture.auth,
        (&request, &snapshot, &plan),
        ack,
        chrono::Duration::minutes(5),
    )
    .await
    .unwrap();
    let source = Evidence::load(
        &fixture.db,
        &binding.database_scope,
        &binding.database_identity,
        std::slice::from_ref(&binding.request.plan_id),
    )
    .await
    .unwrap();
    let key = anchor_key(&binding).unwrap();
    let root = source.row(&key).unwrap();
    assert!(source.read_binding(root).unwrap().view() == &binding);
    let start = source.budget().unwrap().nodes;
    assert!(source.read_binding(root).unwrap().view() == &binding);
    assert!(source.budget().unwrap().nodes > start);

    let applied = actions::apply(&fixture.db, &fixture.config, &fixture.auth, &binding)
        .await
        .unwrap();
    let action = &applied.items[0];
    let authority = action
        .jobs
        .iter()
        .find(|job| job.directive == JobDirective::Submit)
        .unwrap();
    let frame = binding.view().frozen_rows().unwrap();
    let now = crate::store::database_clock::database_now(&fixture.db)
        .await
        .unwrap();
    let work = Arc::new(Mutex::new(Budget::default()));
    let view = BindingView::from(&binding).with_work(&work);
    assert!(
        actions::job_authority::source::birth_from_rows(
            &fixture.db,
            view,
            action,
            authority,
            frame,
            now
        )
        .await
        .unwrap()
    );
    let (nodes, bytes) = {
        let work = work.lock().unwrap();
        (work.nodes, work.bytes)
    };
    assert!(nodes >= measured.nodes && bytes >= measured.bytes);
    eprintln!(
        "directory proof work: snapshot nodes={} bytes={}; worker source nodes={nodes} bytes={bytes}",
        measured.nodes, measured.bytes
    );
    for node_limit in [true, false] {
        let mut limited = Budget::default();
        if node_limit {
            limited
                .nodes(management::wire::MAX_WIRE_NODES - nodes + 1)
                .unwrap();
        } else {
            limited
                .bytes(management::wire::MAX_WIRE_BYTES - bytes + 1)
                .unwrap();
        }
        let limited = Arc::new(Mutex::new(limited));
        let view = BindingView::from(&binding).with_work(&limited);
        assert_eq!(
            actions::job_authority::source::birth_from_rows(
                &fixture.db,
                view,
                action,
                authority,
                frame,
                now
            )
            .await,
            Err(ManagementError::Budget)
        );
    }
    let mut fresh_work = Budget::default();
    assert!(
        actions::accounting::fresh_original_proof_with_work(
            frame,
            &proof.identity,
            now,
            &mut fresh_work
        )
        .unwrap()
        .is_some()
    );
    let mut limited = Budget::default();
    limited
        .nodes(management::wire::MAX_WIRE_NODES - fresh_work.nodes + 1)
        .unwrap();
    assert!(matches!(
        actions::accounting::fresh_original_proof_with_work(
            frame,
            &proof.identity,
            now,
            &mut limited
        ),
        Err(ManagementError::Budget)
    ));

    // Historical invalid inner proofs are skipped; their outer protocol remains fatal.
    for (rid, fault, expected) in [
        (
            "current-directory-proof",
            0,
            Err(SafeError::InconsistentSnapshot),
        ),
        ("older-directory-proof", 0, Ok(())),
        (
            "older-directory-proof",
            1,
            Err(SafeError::InconsistentSnapshot),
        ),
        ("older-directory-proof", 2, Ok(())),
        (
            "current-directory-proof",
            2,
            Err(SafeError::InconsistentSnapshot),
        ),
    ] {
        let mut corrupted = snapshot.clone();
        let frame = corrupted.native_frame.as_mut().unwrap();
        let mut private = frame.private_material();
        let (_, row) = private
            .iter_mut()
            .find(|(key, _)| key.values.get(1).is_some_and(|plan| plan == rid))
            .unwrap();
        let result = row.get_mut("result").unwrap().as_mut().unwrap();
        let mut value: serde_json::Value = serde_json::from_str(result).unwrap();
        if fault == 1 {
            value[1] = serde_json::json!(42);
        } else if fault == 0 {
            value[0]["directory_receipt"]["cid"] =
                serde_json::json!("QmYwAPJzv5CZsnAzt8auVTL7VYhESWDFoCPTqCkiP6fKGE");
        }
        *result = serde_json::to_string(&value).unwrap();
        if fault == 2 {
            let observed = serde_json::to_string(&value[0]["observed_at"]).unwrap();
            *result = result.replacen(
                "\"observed_at\":",
                &format!("\"observed_at\":{observed},\"observed_at\":"),
                1,
            );
        }
        frame.restore_private(private).unwrap();
        assert_eq!(management::validate_snapshot(&corrupted), expected);
        let mut probe_work = Budget::default();
        assert_eq!(
            management::validate_snapshot_view_with_work((&corrupted).into(), &mut probe_work),
            expected
        );
        if rid == "older-directory-proof" && fault == 2 {
            for node_limit in [true, false] {
                let mut limited = Budget::default();
                if node_limit {
                    limited
                        .nodes(management::wire::MAX_WIRE_NODES - probe_work.nodes + 1)
                        .unwrap();
                } else {
                    limited
                        .bytes(management::wire::MAX_WIRE_BYTES - probe_work.bytes + 1)
                        .unwrap();
                }
                assert_eq!(
                    management::validate_snapshot_view_with_work((&corrupted).into(), &mut limited),
                    Err(SafeError::LimitExceeded)
                );
            }
            let frame = corrupted.native_frame.as_ref().unwrap();
            assert!(
                actions::job_authority::source::birth_from_rows(
                    &fixture.db,
                    (&binding).into(),
                    action,
                    authority,
                    frame.into(),
                    now
                )
                .await
                .unwrap()
            );
            let work = Arc::new(Mutex::new(Budget::default()));
            assert!(
                actions::job_authority::source::birth_from_rows(
                    &fixture.db,
                    BindingView::from(&binding).with_work(&work),
                    action,
                    authority,
                    frame.into(),
                    now
                )
                .await
                .unwrap()
            );
            let (nodes, bytes) = {
                let work = work.lock().unwrap();
                (work.nodes, work.bytes)
            };
            for node_limit in [true, false] {
                let mut limited = Budget::default();
                if node_limit {
                    limited
                        .nodes(management::wire::MAX_WIRE_NODES - nodes + 1)
                        .unwrap();
                } else {
                    limited
                        .bytes(management::wire::MAX_WIRE_BYTES - bytes + 1)
                        .unwrap();
                }
                let limited = Arc::new(Mutex::new(limited));
                assert_eq!(
                    actions::job_authority::source::birth_from_rows(
                        &fixture.db,
                        BindingView::from(&binding).with_work(&limited),
                        action,
                        authority,
                        frame.into(),
                        now
                    )
                    .await,
                    Err(ManagementError::Budget)
                );
            }
        }
    }
}
