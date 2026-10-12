//! R25-only setup: retain the original HTTPS/Kubo/worker fixtures, but construct
//! admission authority from the full production configuration, not a bare hash.
pub use crate::support::{decompress, residency, sigv4};

mod original {
    include!("import.rs");

    pub async fn migrated_harness(config: ImportHarnessConfig) -> ImportHarness {
        let source = TestHttpsSource::start().await;
        let kubo_harness =
            start_import_kubo_harness(config.kubo_script, config.streaming_kubo_add).await;
        let mut cfg: ipfs_s3_gateway::config::Config = toml::from_str(
            r#"
[storage]
database_url = "sqlite::memory:"
[[auth.credentials]]
access_key = "test"
secret_key = "test"
[decompress_zip]
auto_pin_outputs = false
[pinning_identity]
primary_storage_domain = "r25-oldwire-import-kubo"
[[pinning_identity.providers]]
config_name = "source-manual"
provider_id = "r25-oldwire-import-source-noop"
display_name = "R25 oldwire source manual Noop"
backend = "noop"
scope = "r25-oldwire-import-source-only"
storage_domain = "r25-oldwire-import-noop-domain"
credential_revision = 1
endpoint_revision = 1
api_profile = "noop"
strategy = "cid"
cleanup = "retain"
[[pinning.providers]]
name = "source-manual"
kind = "noop"
priority = 1
max_bytes = 1000000
max_pins = 100
[[pinning.policies]]
bucket = "test-bkt"
prefix = "archive.zip"
trigger = "request"
provider_mode = "one"
providers = ["source-manual"]
default_duration = "1h"
max_duration = "2h"
allow_decompressed = false
"#,
        )
        .unwrap();
        cfg.kubo.rpc_url = kubo_harness.endpoint.clone();
        cfg.storage.database_url = "sqlite::memory:".into();
        cfg.decompress_zip.auto_pin_outputs = Some(false);
        let state = AppState::new(&cfg).await.unwrap();
        assert_eq!(state.pinning.effective_config().policies.len(), 1);
        assert_eq!(
            state.pinning.effective_config().policies[0].trigger,
            ipfs_s3_gateway::pinning::config::PolicyTrigger::Request
        );
        let bucket = "test-bkt".to_owned();
        store::bucket::create(state.store.db(), &bucket, None)
            .await
            .unwrap();
        let raw_config = ImportConfig {
            allowed_https_origins: vec![source.origin()],
            worker_concurrency: config.worker_concurrency,
            poll_interval_ms: config.poll_interval_ms,
            lease_duration_secs: config.lease_duration_secs,
            progress_flush_interval_ms: 10,
            connect_timeout_secs: 2,
            idle_timeout_secs: 15,
            job_timeout_secs: 30,
            max_download_bytes: config.max_download_bytes,
            max_attempts: config.max_attempts,
            ..ImportConfig::default()
        };
        let validated = raw_config.validate().unwrap();
        let downloader = SourceDownloader::with_components(
            Arc::new(validated.clone()),
            Arc::new(FixedResolver(source.address)),
            Arc::new(PermitLoopbackForTest),
            Arc::new(ReqwestImportHttpTransport::new(
                download_limits(&validated),
                vec![source.root.clone()],
            )),
        );
        let coordinator = if let Some(observer) = config.execution_observer {
            ImportCoordinator::new_with_observer(validated, downloader, observer)
        } else {
            ImportCoordinator::new(validated, downloader)
        };
        let server =
            start_s3_server_with_imports(state.clone(), Arc::default(), coordinator.clone()).await;
        let worker = TestImportWorkerGuard::start(&coordinator, state.clone());
        ImportHarness {
            endpoint: server.endpoint.clone(),
            bucket,
            state,
            coordinator,
            kubo: kubo_harness.server,
            source,
            worker,
            cat_bodies: kubo_harness.cat_bodies,
            add_file_bytes: kubo_harness.add_file_bytes,
            kubo_calls: kubo_harness.calls,
            pin_gate: kubo_harness.pin_gate,
            kubo_file_ingress: kubo_harness.file_ingress,
            streaming_kubo: kubo_harness.streaming_server,
            server,
        }
    }
}

pub use original::migrated_harness as start_import_harness;
pub use original::{
    ImportHarness, ImportHarnessConfig, ImportPublicationBlockControl, TestHttpsReply,
    get_import_status,
};

pub async fn assert_manual_source(db: &sea_orm::DatabaseConnection, id: &str) {
    use ipfs_s3_gateway::store::{
        entities::{object, object_tag, object_version, pin_lease, pin_lease_target},
        zip::execution,
    };
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    let persisted = execution::read(db, id).await.unwrap().unwrap();
    let receipt: serde_json::Value =
        serde_json::from_str(persisted.terminal_result.as_deref().unwrap()).unwrap();
    let version =
        object_version::Entity::find_by_id(receipt["source_version_row_id"].as_str().unwrap())
            .one(db)
            .await
            .unwrap()
            .unwrap();
    let source = object::Entity::find_by_id(version.object_id.unwrap())
        .one(db)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.key, "archive.zip");
    assert_eq!(source.cid, persisted.input_art_cid.unwrap());
    let leases = pin_lease::Entity::find().all(db).await.unwrap();
    assert_eq!(
        leases.len(),
        1,
        "auto=false/targets=none cannot suppress the source manual request or create Automatic leases"
    );
    let lease = &leases[0];
    assert_eq!(lease.source, "manual");
    assert_eq!(lease.owner_object_id, source.id);
    assert_eq!(lease.content_mode, "full");
    assert_eq!((lease.expires_at - lease.created_at).num_seconds(), 3600);
    let targets = pin_lease_target::Entity::find()
        .filter(pin_lease_target::Column::LeaseId.eq(&lease.id))
        .all(db)
        .await
        .unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0].cid, source.cid);
    assert_eq!(targets[0].logical_size, source.size);
    let tags = object_tag::Entity::find()
        .filter(object_tag::Column::ObjectId.eq(&source.id))
        .all(db)
        .await
        .unwrap();
    assert!(
        tags.iter()
            .any(|tag| tag.key == "label" && tag.value == "source")
    );
    assert!(
        tags.iter()
            .any(|tag| tag.key == "ipfs-s3:pin" && tag.value == "true")
    );
}
