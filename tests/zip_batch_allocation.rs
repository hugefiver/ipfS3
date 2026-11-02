mod stage9_zip_support;

use std::collections::{BTreeMap, BTreeSet};

use chrono::Utc;
use ipfs_s3_gateway::{
    config::{PinningConfig, PolicyConfig, ProviderConfig},
    pinning::{config::ValidatedPinningConfig, coordinator::normalize_validated_config},
    store::{
        self,
        entities::{
            object_version, pin_invocation_route, pin_job, pin_lease, pin_lease_target,
            pin_provider_usage, remote_pin, remote_pin_ledger,
        },
    },
};
use sea_orm::{ActiveModelTrait, EntityTrait, Set};
use stage9_zip_support::{object, prepared_with_outputs, publish, setup};

fn four_providers(mode: &str) -> ValidatedPinningConfig {
    normalize_validated_config(
        ValidatedPinningConfig::from_raw(
            &PinningConfig {
                providers: (1..=4)
                    .map(|priority| ProviderConfig {
                        name: format!("p{priority}"),
                        kind: "noop".into(),
                        enabled: true,
                        priority,
                        max_bytes: 5,
                        max_pins: 1,
                        cluster: None,
                        token_env: None,
                        endpoint: None,
                        api: None,
                        strategy: None,
                        upload_endpoint: None,
                        requests_per_second: None,
                    })
                    .collect(),
                policies: vec![PolicyConfig {
                    bucket: "bucket".into(),
                    prefix: "out/".into(),
                    trigger: "always".into(),
                    provider_mode: mode.into(),
                    providers: (1..=4).rev().map(|n| format!("p{n}")).collect(),
                    default_duration: "1h".into(),
                    max_duration: "2h".into(),
                    allow_decompressed: true,
                }],
                ..Default::default()
            },
            |_| None,
        )
        .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn one_falls_back_in_priority_order_using_batch_usage_and_reuses_shared_cid() {
    let db = setup().await;
    let config = four_providers("one");
    let outputs = [
        ("a", "cid-a"),
        ("b", "shared-cid"),
        ("c", "cid-c"),
        ("d", "cid-d"),
        ("e", "shared-cid"),
    ]
    .into_iter()
    .map(|(id, cid)| (id.into(), object(id, &format!("out/{id}"), cid)))
    .collect();
    let (request, batch, capture, guard) =
        prepared_with_outputs(&db, &config, vec![], outputs).await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();

    let leases = pin_lease::Entity::find().all(&db).await.unwrap();
    assert_eq!(leases.len(), 5);
    assert!(
        leases
            .iter()
            .all(|l| l.state == "active" && l.provider_mode == "one")
    );
    let owners = leases
        .iter()
        .map(|l| (l.id.clone(), l.owner_object_id.clone()))
        .collect::<BTreeMap<_, _>>();
    let names = config
        .providers
        .iter()
        .map(|p| (p.identity.allocation_key(), p.name.clone()))
        .collect::<BTreeMap<_, _>>();
    let targets = pin_lease_target::Entity::find().all(&db).await.unwrap();
    let assignments = targets
        .iter()
        .map(|t| {
            (
                owners[&t.lease_id].as_str(),
                names[&t.provider].as_str(),
                t.cid.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(targets.len(), 5);
    assert_eq!(
        assignments,
        BTreeSet::from([
            ("a", "p1", "cid-a"),
            ("b", "p2", "shared-cid"),
            ("c", "p3", "cid-c"),
            ("d", "p4", "cid-d"),
            ("e", "p2", "shared-cid"),
        ])
    );
    assert!(targets.iter().all(|t| t.state == "waiting"));
    let usages = pin_provider_usage::Entity::find().all(&db).await.unwrap();
    assert_eq!(usages.len(), 4);
    assert!(
        usages
            .iter()
            .all(|u| (u.reserved_bytes, u.reserved_pins) == (5, 1))
    );
    let remotes = remote_pin::Entity::find().all(&db).await.unwrap();
    assert_eq!(remotes.len(), 4);
    assert!(
        remotes
            .iter()
            .all(|r| r.status == "reserved" && r.request_id.is_none())
    );
    let jobs = pin_job::Entity::find().all(&db).await.unwrap();
    assert_eq!(jobs.len(), 4);
    assert!(
        jobs.iter()
            .all(|j| j.operation == "submit" && j.state == "pending")
    );
    assert_eq!(
        jobs.iter()
            .map(|j| (&j.provider, &j.cid))
            .collect::<BTreeSet<_>>()
            .len(),
        4
    );
}

#[tokio::test]
async fn existing_pinned_failed_and_confirmed_absent_remotes_keep_distinct_allocation_semantics() {
    let db = setup().await;
    let config = stage9_zip_support::config();
    let outputs = [
        ("pinned-a", "pinned-cid"),
        ("pinned-b", "pinned-cid"),
        ("failed-a", "failed-cid"),
        ("failed-b", "failed-cid"),
        ("absent-a", "absent-cid"),
        ("absent-b", "absent-cid"),
    ]
    .into_iter()
    .map(|(id, cid)| (id.into(), object(id, &format!("out/{id}"), cid)))
    .collect();
    let (request, batch, capture, guard) =
        prepared_with_outputs(&db, &config, vec![], outputs).await;
    let provider = &config.providers[0];
    let key = provider.identity.allocation_key();
    let old_time = Utc::now() - chrono::TimeDelta::hours(1);
    for (cid, status, effect) in [
        ("pinned-cid", "pinned", "confirmed"),
        ("failed-cid", "failed", "unknown"),
        ("absent-cid", "absent", "absent"),
    ] {
        remote_pin::ActiveModel {
            provider: Set(key.clone()),
            cid: Set(cid.into()),
            request_id: Set((status == "pinned").then(|| "retained-request".into())),
            cid_size: Set(5),
            status: Set(status.into()),
            epoch: Set(7),
            failure_attempts: Set(if status == "failed" { 3 } else { 0 }),
            next_retry_at: Set((status == "failed").then_some(old_time)),
            last_failed_request_id: Set((status == "failed").then(|| "failed-request".into())),
            last_touched_at: Set(old_time),
            last_error_class: Set((status == "failed").then(|| "provider".into())),
            last_error_text: Set((status == "failed").then(|| "retained failure".into())),
        }
        .insert(&db)
        .await
        .unwrap();
        remote_pin_ledger::ActiveModel {
            provider: Set(key.clone()),
            cid: Set(cid.into()),
            route: Set(Some(
                serde_json::to_string(&provider.route_snapshot()).unwrap(),
            )),
            ownership: Set("external_existing".into()),
            effect: Set(effect.into()),
            first_observed_at: Set(Some(old_time)),
            last_observed_at: Set(Some(old_time)),
            remote_pinned_at: Set((status == "pinned").then_some(old_time)),
            gateway_verified_at: Set(None),
            content_verified_at: Set(None),
            first_error: Set(None),
            last_error: Set(None),
        }
        .insert(&db)
        .await
        .unwrap();
    }
    pin_provider_usage::ActiveModel {
        provider: Set(key.clone()),
        reserved_bytes: Set(10),
        reserved_pins: Set(2),
        observed_bytes: Set(None),
        observed_pins: Set(None),
        observed_at: Set(None),
    }
    .insert(&db)
    .await
    .unwrap();
    let ledgers_before = remote_pin_ledger::Entity::find().all(&db).await.unwrap();
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();

    let leases = pin_lease::Entity::find().all(&db).await.unwrap();
    assert_eq!(leases.len(), 6);
    assert!(leases.iter().all(|l| l.state == "active"));
    let owners = leases
        .iter()
        .map(|l| (l.id.clone(), l.owner_object_id.clone()))
        .collect::<BTreeMap<_, _>>();
    let targets = pin_lease_target::Entity::find().all(&db).await.unwrap();
    assert_eq!(targets.len(), 6);
    for (cid, state, owner_ids) in [
        ("pinned-cid", "pinned", ["pinned-a", "pinned-b"]),
        ("failed-cid", "degraded", ["failed-a", "failed-b"]),
        ("absent-cid", "waiting", ["absent-a", "absent-b"]),
    ] {
        let related = targets.iter().filter(|t| t.cid == cid).collect::<Vec<_>>();
        assert_eq!(related.len(), 2);
        assert!(
            related
                .iter()
                .all(|t| t.state == state && t.provider == key)
        );
        assert_eq!(
            related
                .iter()
                .map(|t| owners[&t.lease_id].as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(owner_ids)
        );
    }
    let usage = pin_provider_usage::Entity::find_by_id(&key)
        .one(&db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((usage.reserved_bytes, usage.reserved_pins), (15, 3));
    let remotes = remote_pin::Entity::find().all(&db).await.unwrap();
    assert_eq!(remotes.len(), 3);
    assert!(remotes.iter().all(|r| r.epoch == 9));
    let pinned = remotes.iter().find(|r| r.cid == "pinned-cid").unwrap();
    assert_eq!(
        (pinned.status.as_str(), pinned.request_id.as_deref()),
        ("pinned", Some("retained-request"))
    );
    let failed = remotes.iter().find(|r| r.cid == "failed-cid").unwrap();
    assert_eq!(
        (failed.status.as_str(), failed.failure_attempts),
        ("failed", 0)
    );
    assert_eq!(
        failed.last_failed_request_id.as_deref(),
        Some("failed-request")
    );
    assert_eq!(failed.last_error_class.as_deref(), Some("provider"));
    assert_eq!(failed.last_error_text.as_deref(), Some("retained failure"));
    let retry = failed.next_retry_at.unwrap();
    assert!(failed.last_touched_at > old_time);
    assert_eq!(
        retry,
        failed.last_touched_at + chrono::TimeDelta::seconds(30)
    );
    let absent = remotes.iter().find(|r| r.cid == "absent-cid").unwrap();
    assert_eq!(
        (absent.status.as_str(), absent.request_id.as_deref()),
        ("reserved", None)
    );
    let ledgers = remote_pin_ledger::Entity::find().all(&db).await.unwrap();
    for cid in ["pinned-cid", "failed-cid"] {
        assert_eq!(
            ledgers.iter().find(|l| l.cid == cid),
            ledgers_before.iter().find(|l| l.cid == cid)
        );
    }
    let reallocated = ledgers.iter().find(|l| l.cid == "absent-cid").unwrap();
    assert_eq!(
        (reallocated.effect.as_str(), reallocated.ownership.as_str()),
        ("reserved", "unknown")
    );
    assert!(reallocated.first_observed_at.is_none() && reallocated.last_observed_at.is_none());
    let jobs = pin_job::Entity::find().all(&db).await.unwrap();
    assert_eq!(jobs.len(), 2);
    assert!(
        jobs.iter()
            .all(|j| j.state == "pending" && j.provider == key)
    );
    let retry_job = jobs.iter().find(|j| j.cid == "failed-cid").unwrap();
    assert_eq!(retry_job.operation, "reconcile");
    assert_eq!(retry_job.expected_remote_epoch, Some(9));
    assert_eq!(retry_job.next_attempt_at, retry);
    let submit = jobs.iter().find(|j| j.cid == "absent-cid").unwrap();
    assert_eq!(submit.operation, "submit");
    let canonical = targets
        .iter()
        .filter(|t| t.cid == "absent-cid")
        .min_by_key(|t| (t.created_at, &t.id))
        .unwrap();
    assert_eq!(submit.target_id.as_deref(), Some(canonical.id.as_str()));
    assert_eq!(
        submit.lease_id.as_deref(),
        Some(canonical.lease_id.as_str())
    );
    let invocations = pin_invocation_route::Entity::find().all(&db).await.unwrap();
    assert_eq!(invocations.len(), 2);
    assert!(invocations.iter().all(|i| i.remote_epoch == 9));
}

#[tokio::test]
async fn all_gives_each_owner_four_targets_but_reserves_and_submits_each_resource_once() {
    let db = setup().await;
    let config = four_providers("all");
    let (request, batch, capture, guard) = stage9_zip_support::prepared(&db, &config, vec![]).await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();

    let leases = pin_lease::Entity::find().all(&db).await.unwrap();
    assert_eq!(leases.len(), 2);
    assert_eq!(
        leases
            .iter()
            .map(|l| l.owner_object_id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["a", "b"])
    );
    let providers = config
        .providers
        .iter()
        .map(|p| p.identity.allocation_key())
        .collect::<BTreeSet<_>>();
    let targets = pin_lease_target::Entity::find().all(&db).await.unwrap();
    assert_eq!(targets.len(), 8);
    for lease in &leases {
        assert_eq!(
            (lease.state.as_str(), lease.provider_mode.as_str()),
            ("active", "all")
        );
        assert_eq!(
            targets
                .iter()
                .filter(|t| t.lease_id == lease.id)
                .map(|t| t.provider.clone())
                .collect::<BTreeSet<_>>(),
            providers
        );
    }
    assert!(
        targets
            .iter()
            .all(|t| t.cid == "shared-cid" && t.state == "waiting")
    );
    let usages = pin_provider_usage::Entity::find().all(&db).await.unwrap();
    assert_eq!(usages.len(), 4);
    assert!(
        usages
            .iter()
            .all(|u| (u.reserved_bytes, u.reserved_pins) == (5, 1))
    );
    let remotes = remote_pin::Entity::find().all(&db).await.unwrap();
    assert_eq!(remotes.len(), 4);
    assert!(
        remotes
            .iter()
            .all(|r| r.status == "reserved" && r.epoch == 2)
    );
    let jobs = pin_job::Entity::find().all(&db).await.unwrap();
    assert_eq!(jobs.len(), 4);
    let invocations = pin_invocation_route::Entity::find().all(&db).await.unwrap();
    assert_eq!(invocations.len(), 4);
    for job in &jobs {
        assert_eq!(
            (job.operation.as_str(), job.state.as_str()),
            ("submit", "pending")
        );
        let target = targets
            .iter()
            .find(|t| Some(t.id.as_str()) == job.target_id.as_deref())
            .unwrap();
        assert_eq!(job.lease_id.as_deref(), Some(target.lease_id.as_str()));
        assert_eq!((&job.provider, &job.cid), (&target.provider, &target.cid));
        assert_eq!(job.expected_generation, Some(1));
        assert_eq!(
            invocations
                .iter()
                .find(|i| i.job_id == job.id)
                .unwrap()
                .remote_epoch,
            2
        );
    }
    let snapshot = store::zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(snapshot.entries.len(), 2);
    let versions = object_version::Entity::find().all(&db).await.unwrap();
    for entry in &snapshot.entries {
        let version = versions
            .iter()
            .find(|v| Some(v.id.as_str()) == entry.version_row_id.as_deref())
            .unwrap();
        assert_eq!(version.object_id.as_deref(), Some(entry.path.as_str()));
        let published = store::object::get_latest(&db, "bucket", &version.key)
            .await
            .unwrap();
        assert_eq!(published.cid, "shared-cid");
        assert_eq!(Some(published.id.as_str()), version.object_id.as_deref());
    }
}
