//! Captured authorization versus current explicit safety-veto HTTP fixtures.
use std::sync::Arc;

use http::{HeaderMap, StatusCode};
use ipfs_s3_gateway::{
    config::Config,
    pinning::zip_policy::ZipRuleEffect,
    state::AppState,
    store::{self, zip::execution},
};

use crate::{
    review::{self, ARCHIVE, KEY, OUTPUT, Receipt},
    support::decompress::{
        AddReply, KuboScript, S3ServerHandle, TestHarness, start_harness_with_root_default,
        start_s3_server,
    },
};

pub struct SafetyFixture {
    pub h: TestHarness,
    pub archive: Vec<u8>,
    config: Config,
    servers: Vec<S3ServerHandle>,
}

#[derive(Debug, Clone, Copy)]
pub enum LocalSourceAuthority {
    EmptyRules,
    NoMatchingRule,
    NoAvailableProvider,
}

impl SafetyFixture {
    pub async fn new(mpu: bool) -> Self {
        let archive = crate::support::decompress::legal_single_entry_zip();
        Self::with_archive(mpu, archive).await
    }

    pub async fn with_archive(mpu: bool, archive: Vec<u8>) -> Self {
        let mut add_replies = vec![AddReply::Ok(ARCHIVE)];
        if mpu {
            add_replies.push(AddReply::Ok(ARCHIVE));
        }
        add_replies.extend([AddReply::Ok(OUTPUT), AddReply::Ok(OUTPUT)]);
        let mut h = start_harness_with_root_default(
            KuboScript {
                add_replies,
                cat_bodies: [(ARCHIVE.to_owned(), archive.clone())].into(),
            },
            false,
        )
        .await;
        let config: Config = toml::from_str(&format!(
            r#"
[kubo]
rpc_url = {:?}
[storage]
database_url = 'sqlite::memory:'
[[auth.credentials]]
access_key = 'test'
secret_key = 'test'
[decompress_zip]
unixfs_directory_root = false
auto_pin_outputs = true
[[decompress_zip.pin_output_rules]]
name = 'original-allow'
priority = 1
bucket = 'test-bkt'
prefix = ''
effect = 'allow'
policy_index = 0
[[pinning.providers]]
name = 'alpha'
kind = 'noop'
priority = 1
max_bytes = 1000000
max_pins = 100
[[pinning.policies]]
bucket = 'test-bkt'
prefix = ''
trigger = 'always'
provider_mode = 'one'
providers = ['alpha']
default_duration = '1h'
max_duration = '2h'
allow_decompressed = true
"#,
            h.kubo.uri()
        ))
        .unwrap();
        h.state = AppState::new(&config).await.unwrap();
        store::bucket::create(h.state.store.db(), &h.bucket, Some("test"))
            .await
            .unwrap();
        let server = start_s3_server(h.state.clone(), Arc::default()).await;
        h.endpoint = server.endpoint.clone();
        Self {
            h,
            archive,
            config,
            servers: vec![server],
        }
    }

    pub async fn change_current_rule(&mut self, effect: ZipRuleEffect, bucket: &str, prefix: &str) {
        let mut config = self.config.clone();
        let rule = &mut config.decompress_zip.pin_output_rules[0];
        rule.effect = effect;
        rule.bucket = bucket.into();
        rule.prefix = prefix.into();
        if effect == ZipRuleEffect::Deny {
            rule.policy_id = None;
        } else {
            // A changed Allow/TTL is not a safety veto or replacement authority.
            config.pinning.policies[0].default_duration = "2h".into();
        }
        self.use_pinning_config(&config).await;
    }

    pub async fn original_local_source(&mut self, authority: LocalSourceAuthority) {
        let mut config = self.config.clone();
        match authority {
            LocalSourceAuthority::EmptyRules => config.decompress_zip.pin_output_rules.clear(),
            LocalSourceAuthority::NoMatchingRule => {
                config.decompress_zip.pin_output_rules[0].prefix = "out/".into();
            }
            LocalSourceAuthority::NoAvailableProvider => {
                config.decompress_zip.pin_output_rules.clear();
                config.decompress_zip.pin_output_rules_provided = false;
                let mut disabled = config.pinning.providers[0].clone();
                disabled.name = "beta".into();
                disabled.priority = 2;
                disabled.enabled = false;
                config.pinning.providers.push(disabled);
                config.pinning.policies[0].provider_mode = "all".into();
                config.pinning.policies[0].providers.push("beta".into());
            }
        }
        self.use_pinning_config(&config).await;
    }

    async fn use_pinning_config(&mut self, config: &Config) {
        let changed = AppState::new(config).await.unwrap();
        let state = Arc::new(AppState {
            kubo: self.h.state.kubo.clone(),
            cold_kubo: None,
            store: self.h.state.store.clone(),
            credentials: self.h.state.credentials.clone(),
            master_key: ipfs_s3_gateway::crypto::key::MasterKey::from_hex(&"0".repeat(64)).unwrap(),
            pinning: changed.pinning.clone(),
        });
        let server = start_s3_server(state.clone(), Arc::default()).await;
        self.h.state = state;
        self.h.endpoint = server.endpoint.clone();
        self.servers.push(server);
    }

    pub async fn direct(&self, token: &str, targets: &str, archive: Vec<u8>) -> Receipt {
        review::request(
            &self.h,
            reqwest::Method::PUT,
            KEY,
            &[("decompress-zip", "out/")],
            archive,
            remote_controls(token, targets, false),
        )
        .await
    }

    pub async fn pending_direct(&self, token: &str, targets: &str) -> execution::Snapshot {
        review::sql(&self.h, "CREATE TRIGGER safety_manifest_rollback BEFORE INSERT ON zip_batches BEGIN SELECT RAISE(ABORT, 'fixture pending manifest rollback'); END", vec![]).await;
        let first = self.direct(token, targets, self.archive.clone()).await;
        assert!(!first.status.is_success(), "{first:?}");
        review::sql(&self.h, "DROP TRIGGER safety_manifest_rollback", vec![]).await;
        let saved = execution::read_by_identity(self.h.state.store.db(), "test", "direct", token)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.state, "pending");
        assert!(saved.input_sha256.is_some());
        assert_eq!(saved.input_art_cid.as_deref(), Some(ARCHIVE));
        review::expire(&self.h, &saved.id).await;
        saved
    }

    pub async fn direct_source_only(&self, token: &str) -> Receipt {
        review::request(
            &self.h,
            reqwest::Method::PUT,
            KEY,
            &[("decompress-zip", "out/")],
            self.archive.clone(),
            source_only_controls(token),
        )
        .await
    }

    pub async fn pending_direct_source_only(&self, token: &str) -> execution::Snapshot {
        review::sql(&self.h, "CREATE TRIGGER safety_manifest_rollback BEFORE INSERT ON zip_batches BEGIN SELECT RAISE(ABORT, 'fixture pending manifest rollback'); END", vec![]).await;
        let first = self.direct_source_only(token).await;
        assert_eq!(first.status, StatusCode::INTERNAL_SERVER_ERROR, "{first:?}");
        review::sql(&self.h, "DROP TRIGGER safety_manifest_rollback", vec![]).await;
        let saved = execution::read_by_identity(self.h.state.store.db(), "test", "direct", token)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(saved.state, "pending");
        assert!(saved.input_sha256.is_some());
        assert_eq!(saved.input_art_cid.as_deref(), Some(ARCHIVE));
        review::expire(&self.h, &saved.id).await;
        saved
    }

    pub async fn create_and_upload(&self, token: &str) -> (String, String) {
        self.create_and_upload_controls(remote_controls(token, "both", true))
            .await
    }

    pub async fn create_and_upload_source_only(&self, token: &str) -> (String, String) {
        self.create_and_upload_controls(source_only_controls(token))
            .await
    }

    async fn create_and_upload_controls(&self, controls: HeaderMap) -> (String, String) {
        let create = review::request(
            &self.h,
            reqwest::Method::POST,
            KEY,
            &[("uploads", ""), ("decompress-zip", "out/")],
            vec![],
            controls,
        )
        .await;
        assert_eq!(create.status, StatusCode::OK, "{create:?}");
        let id = create
            .body
            .split("<UploadId>")
            .nth(1)
            .unwrap()
            .split("</UploadId>")
            .next()
            .unwrap()
            .to_owned();
        let upload = review::request(
            &self.h,
            reqwest::Method::PUT,
            KEY,
            &[("uploadId", &id), ("partNumber", "1")],
            self.archive.clone(),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(upload.status, StatusCode::OK, "{upload:?}");
        (
            id,
            upload.headers[http::header::ETAG]
                .to_str()
                .unwrap()
                .to_owned(),
        )
    }
}

fn source_only_controls(token: &str) -> HeaderMap {
    let mut controls = remote_controls(token, "source", true);
    controls.insert("x-ipfs3-zip-publish-extracted", "false".parse().unwrap());
    controls
}

/// Keep the existing valid stored ZIP but give its sole entry a nested path.
pub fn public_entry_zip() -> Vec<u8> {
    let mut archive = crate::support::decompress::legal_single_entry_zip();
    archive[26..28].copy_from_slice(&15u16.to_le_bytes());
    archive.splice(30..30, b"public/".iter().copied());
    let central = archive.windows(4).position(|s| s == b"PK\x01\x02").unwrap();
    archive[central + 28..central + 30].copy_from_slice(&15u16.to_le_bytes());
    archive.splice(central + 46..central + 46, b"public/".iter().copied());
    let end = archive.len() - 22;
    let old_size = u32::from_le_bytes(archive[end + 12..end + 16].try_into().unwrap());
    archive[end + 12..end + 16].copy_from_slice(&(old_size + 7).to_le_bytes());
    archive[end + 16..end + 20].copy_from_slice(&(central as u32).to_le_bytes());
    archive
}

pub fn remote_controls(token: &str, targets: &str, source: bool) -> HeaderMap {
    let mut controls = review::controls(token);
    controls.insert("x-ipfs3-zip-targets", targets.parse().unwrap());
    controls.insert(
        "x-ipfs3-zip-publish-source",
        if source { "true" } else { "false" }.parse().unwrap(),
    );
    controls
}
