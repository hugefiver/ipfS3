#[path = "support/pin_rows.rs"]
mod pin_rows;
mod stage9_zip_support;

use std::collections::BTreeSet;

use ipfs_s3_gateway::{
    config::{PinningConfig, PolicyConfig, ProviderConfig},
    pinning::{config::ValidatedPinningConfig, coordinator::normalize_validated_config},
    store::{self, entities::object_version},
};
use sea_orm::{ConnectionTrait, EntityTrait, Statement};
use stage9_zip_support::{OUTPUT_CID, fixture_cid, object, prepared_with_outputs, publish, setup};

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
async fn one_places_files_in_priority_order_and_reuses_shared_cid() {
    let db = setup().await;
    let config = four_providers("one");
    let [a, c, d, overflow] = ["a", "c", "d", "f"].map(fixture_cid);
    let expected = [
        ("a", "p1", a.as_str()),
        ("b", "p2", OUTPUT_CID),
        ("c", "p3", c.as_str()),
        ("d", "p4", d.as_str()),
        ("e", "p2", OUTPUT_CID),
        ("f", "p1", overflow.as_str()),
    ];
    let outputs = expected
        .iter()
        .map(|&(id, _, cid)| (id.into(), object(id, &format!("out/{id}"), cid)))
        .collect();
    let (request, batch, capture, guard) =
        prepared_with_outputs(&db, &config, vec![], outputs).await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();

    let refs = pin_rows::refs(&db).await;
    assert_eq!(refs.len(), expected.len());
    assert!(refs.iter().all(|r| r.source == "automatic"));
    assert_eq!(
        refs.iter()
            .map(|r| (
                r.owner_object_id.as_str(),
                r.provider.as_str(),
                r.cid.as_str()
            ))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(expected)
    );
    let resources = pin_rows::resources(&db).await;
    assert_eq!(resources.len(), 5);
    assert!(resources.iter().all(|r| (
        r.status.as_str(),
        r.action.as_deref(),
        r.handle.as_deref(),
        r.logical_size
    ) == ("waiting", Some("submit"), None, 5)));
    let limits = &config.provider_limits["p1"];
    let now = chrono::Utc::now();
    let deadline = now + chrono::TimeDelta::minutes(1);
    let first = store::pinning::resources::claim(&db, "p1", limits, now, deadline)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.action, store::pinning::resources::Action::Submit);
    assert!([a, overflow].contains(&first.cid));
    assert!(
        store::pinning::resources::claim(&db, "p1", limits, now, deadline)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn new_owners_preserve_accepted_and_failed_resources_and_requeue_cleaned_cids() {
    let db = setup().await;
    let config = stage9_zip_support::config();
    let accepted = fixture_cid("accepted");
    let failed = fixture_cid("failed");
    let absent = fixture_cid("absent");
    let expected = [
        (
            accepted.as_str(),
            "accepted",
            Some("retained-handle"),
            None,
            ["accepted-a", "accepted-b"],
        ),
        (
            failed.as_str(),
            "failed",
            None,
            None,
            ["failed-a", "failed-b"],
        ),
        (
            absent.as_str(),
            "waiting",
            None,
            Some("submit"),
            ["absent-a", "absent-b"],
        ),
    ];
    for (cid, status, handle) in [
        (accepted.as_str(), "accepted", Some("retained-handle")),
        (failed.as_str(), "failed", None),
        (absent.as_str(), "cleaned", None),
    ] {
        db.execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            "INSERT INTO pin_resources(provider,cid,logical_size,status,handle) VALUES ('remote',$1,5,$2,$3)",
            [cid.into(), status.into(), handle.map(str::to_owned).into()],
        )).await.unwrap();
    }
    let outputs = expected
        .iter()
        .flat_map(|(cid, _, _, _, owners)| {
            owners
                .iter()
                .map(|id| ((*id).into(), object(id, &format!("out/{id}"), cid)))
        })
        .collect();
    let (request, batch, capture, guard) =
        prepared_with_outputs(&db, &config, vec![], outputs).await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();
    let refs = pin_rows::refs(&db).await;
    let resources = pin_rows::resources(&db).await;
    assert_eq!(refs.len(), 6);
    assert_eq!(resources.len(), 3);
    for (cid, status, handle, action, owners) in expected {
        let resource = resources.iter().find(|r| r.cid == cid).unwrap();
        assert_eq!(
            (
                resource.status.as_str(),
                resource.handle.as_deref(),
                resource.action.as_deref()
            ),
            (status, handle, action)
        );
        assert_eq!(
            refs.iter()
                .filter(|r| r.cid == cid)
                .map(|r| r.owner_object_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(owners)
        );
    }
}

#[tokio::test]
async fn all_records_each_owner_at_each_provider_and_preserves_manifest_bindings() {
    let db = setup().await;
    let config = four_providers("all");
    let (request, batch, capture, guard) = stage9_zip_support::prepared(&db, &config, vec![]).await;
    publish(&db, &config, request, batch, &capture, guard)
        .await
        .unwrap();
    let refs = pin_rows::refs(&db).await;
    assert_eq!(refs.len(), 8);
    for owner in ["a", "b"] {
        assert_eq!(
            refs.iter()
                .filter(|r| r.owner_object_id == owner && r.cid == OUTPUT_CID)
                .map(|r| r.provider.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["p1", "p2", "p3", "p4"])
        );
    }
    let resources = pin_rows::resources(&db).await;
    assert_eq!(resources.len(), 4);
    assert!(resources.iter().all(|r| r.cid == OUTPUT_CID
        && r.status == "waiting"
        && r.action.as_deref() == Some("submit")));
    let snapshot = store::zip::snapshot(&db, "batch").await.unwrap().unwrap();
    assert_eq!(snapshot.batch.state, "published");
    assert_eq!(snapshot.entries.len(), 2);
    for entry in &snapshot.entries {
        let version = object_version::Entity::find_by_id(entry.version_row_id.clone().unwrap())
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(version.object_id.as_deref(), Some(entry.path.as_str()));
        let published = store::object::get_latest(&db, "bucket", &version.key)
            .await
            .unwrap();
        assert_eq!(published.cid, OUTPUT_CID);
        assert_eq!(Some(published.id.as_str()), version.object_id.as_deref());
    }
}
