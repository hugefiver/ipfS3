//! Signed legacy ZIP publication and output pinning.
#[path = "support/pin_rows.rs"]
mod pin_rows;

#[allow(dead_code)]
mod support {
    pub mod cors;
    pub mod decompress;
    pub mod sigv4;
}

#[path = "support/stage9_zip_http.rs"]
mod fixture;
#[path = "support/stage9_zip_http_pin_lifecycle.rs"]
mod pin_lifecycle;
#[path = "support/stage9_zip_http_zip.rs"]
mod zip_fixture;

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::store::{
    self,
    entities::{object, object_version},
    zip,
};
use sea_orm::{ColumnTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder};

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
    let mut fixture = Fixture::new(entry, auto, &archive, replies).await;
    // Close the owned server, workers and database even when an assertion panics.
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
        let mut output_versions = Vec::new();
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
            let version_id = version.version_id.expect("enabled version ID");
            assert_get(&fixture, &key, Some(&version_id), OUTPUT_BODY).await;
            output_versions.push((key, version_id));
        }
        assert_zip_pins(&fixture, auto, scenario, &owners).await;
        if matches!(scenario, Scenario::Manual) {
            pin_lifecycle::assert_file_lifecycle(&mut fixture, &output_versions).await;
        }
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
    let refs = pin_rows::refs(db).await;
    let resources = pin_rows::resources(db).await;
    let archive = store::object::get_latest(db, BUCKET, ARCHIVE_KEY)
        .await
        .unwrap();
    let automatic = refs
        .iter()
        .filter(|row| row.source == "automatic")
        .collect::<Vec<_>>();
    let expected_auto = if auto { owners.len() } else { 0 };
    assert_eq!(automatic.len(), expected_auto, "{scenario:?}: {refs:?}");
    let actual_owners = automatic
        .iter()
        .map(|row| row.owner_object_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual_owners,
        if auto {
            owners.clone()
        } else {
            BTreeSet::new()
        }
    );
    assert!(
        automatic
            .iter()
            .all(|row| row.content_mode == "object" && row.owner_object_id != archive.id)
    );
    if matches!(scenario, Scenario::Manual) {
        assert_eq!(refs.len(), owners.len());
        assert_eq!(
            refs.iter()
                .map(|row| row.owner_object_id.clone())
                .collect::<BTreeSet<_>>(),
            *owners
        );
        assert!(refs.iter().all(|row| row.source == "manual"
            && row.content_mode == "decompressed"
            && row.owner_object_id != archive.id));
    } else {
        assert_eq!(
            refs.len(),
            expected_auto,
            "no implicit manual or archive/root pin"
        );
    }
    assert!(
        refs.iter()
            .all(|row| row.provider == fixture.provider && row.cid == OUTPUT_CID)
    );
    assert_eq!(
        resources.len(),
        usize::from(!refs.is_empty()),
        "shared output CID uses one resource"
    );
    if let Some(resource) = resources.first() {
        assert_eq!(
            (
                resource.provider.as_str(),
                resource.cid.as_str(),
                resource.logical_size
            ),
            (
                fixture.provider.as_str(),
                OUTPUT_CID,
                OUTPUT_BODY.len() as i64
            )
        );
        assert_eq!(
            (
                resource.status.as_str(),
                resource.action.as_deref(),
                resource.handle.as_deref()
            ),
            ("waiting", Some("submit"), None)
        );
    }
}

async fn assert_non_zip_automatic(fixture: &Fixture) {
    let db = fixture.state.store.db();
    let object = store::object::get_latest(db, BUCKET, ARCHIVE_KEY)
        .await
        .unwrap();
    let versions = object_version::Entity::find()
        .filter(object_version::Column::ObjectId.eq(&object.id))
        .all(db)
        .await
        .unwrap();
    assert_eq!(versions.len(), 1);
    assert!(versions[0].version_id.is_some());
    let refs = pin_rows::refs(db).await;
    assert_eq!(
        refs.len(),
        1,
        "non-ZIP Always unchanged for either auto-output flag"
    );
    assert_eq!(
        (
            refs[0].source.as_str(),
            refs[0].owner_object_id.as_str(),
            refs[0].provider.as_str(),
            refs[0].cid.as_str()
        ),
        (
            "automatic",
            object.id.as_str(),
            fixture.provider.as_str(),
            ARCHIVE_CID
        )
    );
    let resources = pin_rows::resources(db).await;
    assert_eq!(resources.len(), 1);
    assert_eq!(
        (
            resources[0].provider.as_str(),
            resources[0].cid.as_str(),
            resources[0].logical_size
        ),
        (fixture.provider.as_str(), ARCHIVE_CID, object.size)
    );
}

async fn run_distinct_output_batch(outputs: usize) {
    let archive = zip_fixture::distinct_outputs_zip(outputs);
    let output_cids = zip_fixture::distinct_output_cids(outputs);
    let replies = output_cids.iter().map(|cid| AddReply::Ok(cid)).collect();
    let fixture = Fixture::new(EntryPoint::CompleteMpu, true, &archive, replies).await;
    let outcome = AssertUnwindSafe(async {
        fixture.enable_versioning().await;
        let upload_id = fixture
            .publish(EntryPoint::CompleteMpu, archive, true, false)
            .await;
        let db = fixture.state.store.db();
        let snapshot = zip::snapshot(db, &upload_id).await.unwrap().unwrap();
        assert_eq!(snapshot.batch.state, "published");
        assert_eq!(
            snapshot.batch.root_status, "complete",
            "root publication: {:?}",
            snapshot.batch
        );
        assert_eq!(snapshot.batch.root_cid.as_deref(), Some(ROOT_CID));
        assert!(snapshot.batch.terminal_result.is_some());
        assert_eq!(snapshot.entries.len(), outputs);
        assert!(snapshot.entries.iter().all(|entry| {
            entry.error_code.is_none()
                && entry.version_row_id.is_some()
                && entry.object_key.is_some()
        }));
        let actual_cids = snapshot
            .entries
            .iter()
            .map(|entry| entry.cid.as_deref().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(actual_cids, output_cids.iter().copied().collect());

        let source = store::object::get_latest(db, BUCKET, ARCHIVE_KEY)
            .await
            .unwrap();
        assert_eq!(source.cid, ARCHIVE_CID);
        assert_eq!(
            object::Entity::find().count(db).await.unwrap(),
            (outputs + 1) as u64
        );
        let refs = pin_rows::refs(db).await;
        let resources = pin_rows::resources(db).await;
        assert_eq!(refs.len(), outputs);
        assert_eq!(resources.len(), outputs);
        assert_eq!(
            resources
                .iter()
                .map(|row| row.cid.as_str())
                .collect::<BTreeSet<_>>(),
            actual_cids
        );
        assert!(resources.iter().all(|row| row.provider == fixture.provider
            && row.logical_size == 8
            && row.status == "waiting"));
        let versions = object_version::Entity::find()
            .order_by_asc(object_version::Column::Id)
            .all(db)
            .await
            .unwrap();
        assert_eq!(versions.len(), outputs + 1);
        let owners = versions
            .iter()
            .map(|row| (row.id.as_str(), row.object_id.as_deref().unwrap()))
            .collect::<std::collections::BTreeMap<_, _>>();
        let expected_refs = snapshot
            .entries
            .iter()
            .map(|entry| {
                (
                    owners[entry.version_row_id.as_deref().unwrap()],
                    entry.cid.as_deref().unwrap(),
                )
            })
            .collect::<BTreeSet<_>>();
        let actual_refs = refs
            .iter()
            .map(|row| (row.owner_object_id.as_str(), row.cid.as_str()))
            .collect::<BTreeSet<_>>();
        assert_eq!(actual_refs, expected_refs);
        assert!(refs.iter().all(|row| row.source == "automatic"
            && row.content_mode == "object"
            && row.provider == fixture.provider));
        for index in [0, outputs / 2, outputs - 1] {
            let key = format!("expanded/entry-{index:02}.txt");
            let response = send_sigv4(
                reqwest::Method::GET,
                fixture.endpoint(),
                BUCKET,
                &key,
                &[],
                Vec::new(),
                HeaderMap::new(),
                "test",
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK, "GET {key}");
            assert_eq!(
                response.bytes().await.unwrap().as_ref(),
                &(index as u64).to_le_bytes()
            );
        }

        let replay = support::decompress::complete_multipart(
            &fixture,
            ARCHIVE_KEY,
            &upload_id,
            &[(1, fixture::PART_CID.to_owned())],
        )
        .await;
        let status = replay.status();
        let body = replay.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "Complete replay: {body}");
        assert!(body.contains(ARCHIVE_CID), "{body}");
        assert_eq!(
            object_version::Entity::find()
                .order_by_asc(object_version::Column::Id)
                .all(db)
                .await
                .unwrap(),
            versions
        );
        assert_eq!(pin_rows::refs(db).await, refs);
        assert_eq!(pin_rows::resources(db).await, resources);
    })
    .catch_unwind()
    .await;
    fixture.shutdown().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn legacy_complete_mpu_four_psa_one_publishes_21_distinct_outputs_and_replays() {
    run_distinct_output_batch(21).await;
}

#[tokio::test]
async fn legacy_complete_mpu_four_psa_one_publishes_256_distinct_outputs_and_replays() {
    run_distinct_output_batch(256).await;
}

#[tokio::test]
async fn legacy_complete_mpu_four_psa_one_publishes_1649_distinct_outputs_and_replays() {
    run_distinct_output_batch(1649).await;
}

#[tokio::test]
async fn legacy_put_auto_on_pins_exact_outputs_not_archive_or_root() {
    run_case(EntryPoint::Put, true, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_put_auto_off_creates_no_automatic_zip_reference() {
    run_case(EntryPoint::Put, false, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_complete_mpu_auto_on_pins_exact_outputs_not_archive_or_root() {
    run_case(EntryPoint::CompleteMpu, true, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_complete_mpu_auto_off_creates_no_automatic_zip_reference() {
    run_case(EntryPoint::CompleteMpu, false, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_import_auto_on_pins_exact_outputs_not_archive_or_root() {
    run_case(EntryPoint::Import, true, Scenario::SharedOutputs).await;
}
#[tokio::test]
async fn legacy_import_auto_off_creates_no_automatic_zip_reference() {
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
async fn legacy_put_auto_on_empty_has_no_automatic_reference() {
    run_case(EntryPoint::Put, true, Scenario::Empty).await;
}
#[tokio::test]
async fn legacy_complete_mpu_auto_on_empty_has_no_automatic_reference() {
    run_case(EntryPoint::CompleteMpu, true, Scenario::Empty).await;
}
#[tokio::test]
async fn legacy_import_auto_on_empty_has_no_automatic_reference() {
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
