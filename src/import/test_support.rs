//! Native first-intake/source preconditions for ZIP unit fixtures.
use crate::{
    import::{
        ImportClaim, ImportConfig, pipeline::ImportCoordinator, v2_source::fetch_v2_zip_source,
    },
    pinning::{
        decision::DecisionOrigin,
        zip_policy::{CapturedZipIntent, ZipCaptureContext, capture_zip_intent},
    },
    state::AppState,
    store::{
        Store,
        entities::import_job,
        import::jobs::NewImportJob,
        zip::{self, BatchAdmission},
    },
};
use tokio_util::sync::CancellationToken;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

pub(crate) fn capture_legacy(
    state: &AppState,
    request: &NewImportJob,
    root_enabled: bool,
) -> CapturedZipIntent {
    let mut zip = state.pinning.decompress_zip_config().clone();
    zip.unixfs_directory_root = root_enabled;
    let prefix = request.decompress_prefix.as_deref().unwrap();
    let controls = crate::zip::options::parse_authenticated_zip_controls(
        &http::HeaderMap::new(),
        &[("decompress-zip".into(), prefix.into())],
        &request.tags,
        true,
        root_enabled,
    )
    .unwrap()
    .unwrap();
    capture_zip_intent(
        state.pinning.effective_config(),
        &zip,
        state.pinning.control_mode(),
        ZipCaptureContext {
            bucket: &request.bucket,
            source_key: &request.key,
            target_prefix: prefix,
            tags: &request.tags,
        },
        &controls,
        DecisionOrigin::new("test", &request.id),
    )
    .unwrap()
}

/// The primer serves the original CID bytes through the real pin/inspect/full-CAT
/// source helper. It shares the existing DB/pinning state, not another SQL pool.
/// The extraction mock remains independent so its original exact call counts
/// continue to measure only extraction/root side effects.
pub(crate) async fn prove_legacy_input(
    state: &AppState,
    job: &import_job::Model,
    claim: &ImportClaim,
    archive: &[u8],
) {
    use crate::store::pinning::publication::{bind_legacy_import_input, read_legacy_import_input};
    use sha2::{Digest, Sha256};
    if let Some(proof) = read_legacy_import_input(state.store.db(), &job.id)
        .await
        .unwrap()
    {
        assert_eq!(proof.sha256(), hex::encode(Sha256::digest(archive)));
        assert_eq!(proof.size(), archive.len() as i64);
        return;
    }
    let capture = crate::import::zip_capture::legacy_capture(job).unwrap();
    if zip::read(state.store.db(), &job.id)
        .await
        .unwrap()
        .is_none()
    {
        zip::admit(state.store.db(), &BatchAdmission {
            id: job.id.clone(), owner: capture.origin().principal_id.clone(),
            source: "import".into(), token: job.id.clone(), fingerprint: job.request_fingerprint.clone(),
            bucket: job.bucket.clone(), archive_key: job.key.clone(), input_identity: job.id.clone(),
            captured_options: serde_json::json!({"zip_intent":capture,"target_prefix":capture.target_prefix(),"root_capture":capture.controls().root}).to_string(),
        }).await.unwrap();
    }
    assert_eq!(job.source_type, "cid");
    let source = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v0/pin/add"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{{\"Pins\":[\"{}\"]}}", job.source_value)),
        )
        .expect(1)
        .mount(&source)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v0/cat"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(archive.to_vec()))
        .expect(2)
        .mount(&source)
        .await;
    let source_state = AppState {
        kubo: crate::kubo::KuboClient::new(source.uri()),
        cold_kubo: None,
        store: Store::new(state.store.db().clone()),
        credentials: Default::default(),
        master_key: crate::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
        pinning: state.pinning.clone(),
    };
    let config = ImportConfig::default().validate().unwrap();
    let coordinator = ImportCoordinator::new(
        config.clone(),
        crate::import::downloader::SourceDownloader::production(std::sync::Arc::new(config)),
    );
    let verified = fetch_v2_zip_source(
        &coordinator,
        &source_state,
        &serde_json::to_string(&("cid", &job.source_value)).unwrap(),
        None,
        CancellationToken::new(),
        &state.pinning.zip_extraction_limits(),
    )
    .await
    .unwrap();
    assert_eq!(verified.size, archive.len() as u64);
    assert_eq!(verified.sha256, hex::encode(Sha256::digest(archive)));
    crate::store::zip::import_intake::stage_legacy_input_metadata(state.store.db(),claim,&job.bucket,
        &serde_json::json!({"schema_version":1,"kind":"verified_input","cid":verified.cid,"size":verified.size,
            "sha256":verified.sha256,"content_type":job.object_content_type}).to_string()).await.unwrap();
    let proof = bind_legacy_import_input(
        state.store.db(),
        claim,
        &verified.sha256,
        &verified.cid,
        verified.size as i64,
    )
    .await
    .unwrap();
    assert_eq!(proof.cid(), verified.cid);
    source.verify().await;
}
