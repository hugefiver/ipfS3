//! R25-D prerequisite: real SigV4 legacy ZIP output-pin acceptance (baseline may be RED).

#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
}

#[path = "support/stage9_zip_http.rs"]
mod fixture;
#[path = "support/stage9_zip_http_zip.rs"]
mod zip_fixture;

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::store::{
    self,
    entities::{
        object, object_version, pin_lease, pin_lease_target, pin_provider_usage, remote_pin,
    },
    zip,
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

use fixture::{
    ARCHIVE_CID, ARCHIVE_KEY, BUCKET, ENTRY_POINTS, EntryPoint, Fixture, OUTPUT_BODY, OUTPUT_CID,
    ROOT_CID,
};
use support::{
    decompress::{AddReply, S3TestEndpoint},
    sigv4::send_sigv4,
};

#[derive(Clone, Copy, Debug)]
enum Scenario {
    SharedOutputs,
    Manual,
    Partial,
    Empty,
    NonZip,
}

async fn run_case(entry: EntryPoint, auto: bool, scenario: Scenario) {
    let (archive, replies) = match scenario {
        Scenario::SharedOutputs | Scenario::Manual => (
            zip_fixture::shared_outputs_zip(),
            vec![AddReply::Ok(OUTPUT_CID), AddReply::Ok(OUTPUT_CID)],
        ),
        Scenario::Partial => (
            zip_fixture::partial_zip(),
            vec![
                AddReply::Ok(OUTPUT_CID),
                AddReply::Error(StatusCode::SERVICE_UNAVAILABLE, "scripted failed output"),
            ],
        ),
        Scenario::Empty => (zip_fixture::empty_zip(), Vec::new()),
        Scenario::NonZip => (b"ordinary non-ZIP bytes".to_vec(), Vec::new()),
    };
    let fixture = Fixture::new(entry, auto, &archive, replies).await;
    // Drain server/worker and close the owned DB even when a RED assertion panics.
    let outcome = AssertUnwindSafe(async {
        fixture.enable_versioning().await;
        let batch_id = fixture
            .publish(
                entry,
                archive.clone(),
                !matches!(scenario, Scenario::NonZip),
                matches!(scenario, Scenario::Manual),
            )
            .await;
        assert_get(&fixture, ARCHIVE_KEY, None, &archive).await;
        if matches!(scenario, Scenario::NonZip) {
            assert_non_zip_automatic(&fixture).await;
            return;
        }
        let snapshot = zip::snapshot(fixture.state.store.db(), &batch_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.batch.state, "published", "{entry:?} {scenario:?}");
        let expected_paths: &[&str] = match scenario {
            Scenario::SharedOutputs | Scenario::Manual => &["first.txt", "second.txt"],
            Scenario::Partial => &["success.txt"],
            Scenario::Empty => &[],
            Scenario::NonZip => unreachable!(),
        };
        let successful = snapshot
            .entries
            .iter()
            .filter(|entry| entry.error_code.is_none())
            .collect::<Vec<_>>();
        assert_eq!(
            successful.len(),
            expected_paths.len(),
            "successful outputs: {:?}",
            snapshot.entries
        );
        if expected_paths.is_empty() {
            assert_eq!(snapshot.batch.root_status, "empty");
            assert!(snapshot.batch.root_cid.is_none());
        } else {
            let expected_root_status = if matches!(scenario, Scenario::Partial) {
                "partial"
            } else {
                "complete"
            };
            assert_eq!(
                snapshot.batch.root_status, expected_root_status,
                "verified local root fixture: {:?}",
                snapshot.batch
            );
            assert_eq!(snapshot.batch.root_cid.as_deref(), Some(ROOT_CID));
        }
        if matches!(scenario, Scenario::Partial) {
            let failed = snapshot
                .entries
                .iter()
                .filter(|entry| entry.error_code.is_some())
                .collect::<Vec<_>>();
            // Legacy failure manifest paths are not output paths. Bind the
            // exclusion to the actual failed key/version and GET instead.
            assert_eq!(failed.len(), 1, "{:?}", snapshot.entries);
            assert_eq!(failed[0].error_code.as_deref(), Some("entry_upload_failed"));
            assert!(failed[0].cid.is_none());
            assert!(failed[0].version_row_id.is_none());
            assert!(
                object_version::Entity::find()
                    .filter(object_version::Column::Bucket.eq(BUCKET))
                    .filter(object_version::Column::Key.eq("expanded/failed.txt"))
                    .all(fixture.state.store.db())
                    .await
                    .unwrap()
                    .is_empty()
            );
            let response = send_sigv4(
                reqwest::Method::GET,
                fixture.endpoint(),
                BUCKET,
                "expanded/failed.txt",
                &[],
                Vec::new(),
                HeaderMap::new(),
                "test",
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "failed output must not be published"
            );
            response.bytes().await.unwrap();
        }
        let mut owners = BTreeSet::new();
        for path in expected_paths {
            let manifest = successful.iter().find(|entry| entry.path == *path).unwrap();
            let key = format!("expanded/{path}");
            assert_eq!(manifest.object_key.as_deref(), Some(key.as_str()));
            assert_eq!(manifest.cid.as_deref(), Some(OUTPUT_CID));
            let version =
                object_version::Entity::find_by_id(manifest.version_row_id.as_ref().unwrap())
                    .one(fixture.state.store.db())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(
                (
                    &version.bucket,
                    &version.key,
                    version.kind.as_str(),
                    version.is_latest
                ),
                (&BUCKET.to_owned(), &key, "object", true)
            );
            let object_id = version.object_id.as_ref().unwrap();
            let output = object::Entity::find_by_id(object_id)
                .one(fixture.state.store.db())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (&output.key, output.cid.as_str(), output.size),
                (&key, OUTPUT_CID, OUTPUT_BODY.len() as i64)
            );
            assert!(
                owners.insert(object_id.clone()),
                "each output owns its exact object/version"
            );
            assert_get(
                &fixture,
                &key,
                Some(version.version_id.as_deref().expect("enabled version ID")),
                OUTPUT_BODY,
            )
            .await;
        }
        assert_zip_pins(&fixture, auto, scenario, &owners).await;
    })
    .catch_unwind()
    .await;
    fixture.shutdown().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

async fn assert_get(fixture: &Fixture, key: &str, version: Option<&str>, expected: &[u8]) {
    let query = version
        .map(|version| vec![("versionId", version)])
        .unwrap_or_default();
    let response = send_sigv4(
        reqwest::Method::GET,
        fixture.endpoint(),
        BUCKET,
        key,
        &query,
        Vec::new(),
        HeaderMap::new(),
        "test",
    )
    .await;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "signed GET {key} {version:?}"
    );
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        expected,
        "signed GET {key}"
    );
}

async fn assert_zip_pins(
    fixture: &Fixture,
    auto: bool,
    scenario: Scenario,
    owners: &BTreeSet<String>,
) {
    let db = fixture.state.store.db();
    let leases = pin_lease::Entity::find().all(db).await.unwrap();
    let targets = pin_lease_target::Entity::find().all(db).await.unwrap();
    let archive = store::object::get_latest(db, BUCKET, ARCHIVE_KEY)
        .await
        .unwrap();
    let automatic = leases
        .iter()
        .filter(|lease| lease.source == "automatic")
        .collect::<Vec<_>>();
    let expected_auto = if auto { owners.len() } else { 0 };
    assert_eq!(
        automatic.len(),
        expected_auto,
        "{scenario:?}: auto_pin_outputs={auto} requires output-owned Automatic leases only; archive_id={} leases={leases:?} targets={targets:?}",
        archive.id
    );
    let actual_owners = automatic
        .iter()
        .map(|lease| lease.owner_object_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_owners,
        if auto {
            owners.clone()
        } else {
            BTreeSet::new()
        },
        "automatic owner must bind the exact output versions, never archive/root"
    );
    assert!(automatic.iter().all(|lease| lease.state == "active"
        && lease.content_mode == "full"
        && lease.owner_object_id != archive.id));
    if matches!(scenario, Scenario::Manual) {
        assert_eq!(
            leases.len(),
            1,
            "auto-off preserves explicit legacy manual decompressed pin"
        );
        assert_eq!(
            (
                leases[0].source.as_str(),
                leases[0].owner_object_id.as_str(),
                leases[0].content_mode.as_str(),
                leases[0].state.as_str()
            ),
            ("manual", archive.id.as_str(), "decompressed", "active")
        );
    } else {
        assert_eq!(
            leases.len(),
            expected_auto,
            "no implicit manual or archive/root lease"
        );
    }
    assert_eq!(
        targets.len(),
        leases.len(),
        "each lease targets the one shared output CID"
    );
    for lease in &leases {
        let target = targets
            .iter()
            .filter(|target| target.lease_id == lease.id)
            .collect::<Vec<_>>();
        assert_eq!(target.len(), 1, "exact lease target binding");
        assert_eq!(
            (
                target[0].cid.as_str(),
                target[0].logical_size,
                target[0].provider.as_str(),
                target[0].state.as_str()
            ),
            (
                OUTPUT_CID,
                OUTPUT_BODY.len() as i64,
                fixture.provider.as_str(),
                "waiting"
            )
        );
    }
    assert!(
        targets
            .iter()
            .all(|target| target.cid != ARCHIVE_CID && target.cid != ROOT_CID)
    );
    assert_quota(
        fixture,
        !leases.is_empty(),
        OUTPUT_CID,
        OUTPUT_BODY.len() as i64,
    )
    .await;
}

async fn assert_quota(fixture: &Fixture, occupied: bool, cid: &str, size: i64) {
    let db = fixture.state.store.db();
    let remotes = remote_pin::Entity::find().all(db).await.unwrap();
    assert_eq!(
        remotes.len(),
        usize::from(occupied),
        "shared CID is one reservation, not one per output/version"
    );
    if occupied {
        assert_eq!(
            (
                remotes[0].provider.as_str(),
                remotes[0].cid.as_str(),
                remotes[0].cid_size
            ),
            (fixture.provider.as_str(), cid, size)
        );
        assert_ne!(
            remotes[0].status, "pinned",
            "fixture must not pretend Noop/remote work completed"
        );
    }
    let usages = pin_provider_usage::Entity::find().all(db).await.unwrap();
    assert!(
        usages
            .iter()
            .all(|usage| usage.provider == fixture.provider)
    );
    assert_eq!(
        usages.iter().map(|usage| usage.reserved_pins).sum::<i64>(),
        i64::from(occupied)
    );
    assert_eq!(
        usages.iter().map(|usage| usage.reserved_bytes).sum::<i64>(),
        if occupied { size } else { 0 }
    );
}

async fn assert_non_zip_automatic(fixture: &Fixture) {
    let db = fixture.state.store.db();
    let object = store::object::get_latest(db, BUCKET, ARCHIVE_KEY)
        .await
        .unwrap();
    let leases = pin_lease::Entity::find().all(db).await.unwrap();
    assert_eq!(
        leases.len(),
        1,
        "non-ZIP Always unchanged for either auto-output flag"
    );
    assert_eq!(
        (
            leases[0].source.as_str(),
            leases[0].owner_object_id.as_str(),
            leases[0].state.as_str()
        ),
        ("automatic", object.id.as_str(), "active")
    );
    let versions = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&object.id))
        .all(db)
        .await
        .unwrap();
    assert_eq!(versions.len(), 1);
    assert!(versions[0].version_id.is_some());
    let targets = pin_lease_target::Entity::find().all(db).await.unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(
        (
            targets[0].lease_id.as_str(),
            targets[0].cid.as_str(),
            targets[0].provider.as_str()
        ),
        (
            leases[0].id.as_str(),
            ARCHIVE_CID,
            fixture.provider.as_str()
        )
    );
    assert_quota(fixture, true, ARCHIVE_CID, object.size).await;
}

#[tokio::test]
async fn legacy_put_auto_on_pins_exact_outputs_not_archive_or_root() {
    run_case(EntryPoint::Put, true, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_put_auto_off_creates_no_automatic_zip_lease() {
    run_case(EntryPoint::Put, false, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_complete_mpu_auto_on_pins_exact_outputs_not_archive_or_root() {
    run_case(EntryPoint::CompleteMpu, true, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_complete_mpu_auto_off_creates_no_automatic_zip_lease() {
    run_case(EntryPoint::CompleteMpu, false, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_import_auto_on_pins_exact_outputs_not_archive_or_root() {
    run_case(EntryPoint::Import, true, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_import_auto_off_creates_no_automatic_zip_lease() {
    run_case(EntryPoint::Import, false, Scenario::SharedOutputs).await;
}

#[tokio::test]
async fn legacy_put_auto_off_preserves_explicit_manual_decompressed_pin() {
    run_case(EntryPoint::Put, false, Scenario::Manual).await;
}
#[tokio::test]
async fn legacy_complete_mpu_auto_off_preserves_explicit_manual_decompressed_pin() {
    run_case(EntryPoint::CompleteMpu, false, Scenario::Manual).await;
}
#[tokio::test]
async fn legacy_import_auto_off_preserves_explicit_manual_decompressed_pin() {
    run_case(EntryPoint::Import, false, Scenario::Manual).await;
}

#[tokio::test]
async fn legacy_put_auto_on_excludes_failed_output() {
    run_case(EntryPoint::Put, true, Scenario::Partial).await;
}
#[tokio::test]
async fn legacy_complete_mpu_auto_on_excludes_failed_output() {
    run_case(EntryPoint::CompleteMpu, true, Scenario::Partial).await;
}
#[tokio::test]
async fn legacy_import_auto_on_excludes_failed_output() {
    run_case(EntryPoint::Import, true, Scenario::Partial).await;
}
#[tokio::test]
async fn legacy_put_auto_on_empty_has_no_automatic_lease() {
    run_case(EntryPoint::Put, true, Scenario::Empty).await;
}
#[tokio::test]
async fn legacy_complete_mpu_auto_on_empty_has_no_automatic_lease() {
    run_case(EntryPoint::CompleteMpu, true, Scenario::Empty).await;
}
#[tokio::test]
async fn legacy_import_auto_on_empty_has_no_automatic_lease() {
    run_case(EntryPoint::Import, true, Scenario::Empty).await;
}

#[tokio::test]
async fn non_zip_always_is_unchanged_for_all_legacy_entries_and_both_flags() {
    for entry in ENTRY_POINTS {
        for auto in [true, false] {
            run_case(entry, auto, Scenario::NonZip).await;
        }
    }
}
