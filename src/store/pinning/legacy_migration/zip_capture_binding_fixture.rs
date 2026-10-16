use crate::{
    config::Config,
    pinning::{
        config::ValidatedPinningConfig, identity::ProviderIdentity, zip_policy::CapturedZipIntent,
    },
    store::{
        self,
        pinning::{ledger, legacy_migration as migration},
    },
};
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use std::collections::BTreeMap;

#[path = "zip_capture_binding_intake.rs"]
mod intake;
use intake::persist;

pub const BUCKET: &str = "zip-inventory";
pub const CID: &str = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku";
pub const POINTERS: [&str; 2] = [
    "/zip_intent_json/policy/policies/0/policy/providers/0",
    "/zip_intent_json/policy/policies/0/providers/0/selected/config_name",
];

#[derive(Clone, Copy, Debug)]
pub enum Intake {
    Mpu,
    Import,
}

fn config(identity: Option<(&str, &str)>) -> Config {
    let mut text = format!(
        r#"
[decompress_zip]
auto_pin_outputs = true
[[pinning.providers]]
name = 'old'
kind = 'noop'
priority = 1
max_bytes = 1000
max_pins = 100
[[pinning.policies]]
bucket = '{BUCKET}'
prefix = 'out/'
trigger = 'always'
provider_mode = 'all'
providers = ['old']
default_duration = '1h'
max_duration = '2h'
allow_decompressed = true
"#
    );
    if let Some((id, scope)) = identity {
        text.push_str(&format!(
            r#"
[pinning_identity]
primary_storage_domain = 'local'
[[pinning_identity.providers]]
config_name = 'old'
provider_id = '{id}'
display_name = 'Original Noop'
backend = 'noop'
scope = '{scope}'
storage_domain = 'backup'
credential_revision = 1
endpoint_revision = 1
api_profile = 'noop'
strategy = 'cid'
cleanup = 'retain'
"#
        ));
    }
    toml::from_str(&text).unwrap()
}

pub struct Fixture {
    pub db: DatabaseConnection,
    pub capture: CapturedZipIntent,
    pub table: &'static str,
    pub key: BTreeMap<String, String>,
    pub raw: String,
    pub declaration: migration::Declaration,
}

impl Fixture {
    pub async fn new(kind: Intake, known_account: bool) -> Self {
        let db = store::connect_database("sqlite::memory:").await.unwrap();
        store::run_migrations(&db).await.unwrap();
        store::bucket::create(&db, BUCKET, None).await.unwrap();
        let source_config = config(known_account.then_some(("legacy:old", "account")));
        let validated = ValidatedPinningConfig::from_config(&source_config, |_| None).unwrap();
        let source = &validated.providers[0].identity;
        assert_eq!(source.allocation_key(), "old");
        assert_eq!(
            source.scope,
            if known_account {
                "account"
            } else {
                "legacy:old"
            }
        );
        ledger::register_route(&db, "old", source).await.unwrap();
        let target_config = config(Some(("configured-account", "account")));
        let target: ProviderIdentity =
            ValidatedPinningConfig::from_config(&target_config, |_| None)
                .unwrap()
                .providers
                .remove(0)
                .identity;
        ledger::register_route(&db, &target.allocation_key(), &target)
            .await
            .unwrap();
        db.execute_unprepared("INSERT INTO pin_provider_usage(provider,reserved_bytes,reserved_pins) VALUES ('old',97,3)").await.unwrap();
        let capture = persist(&db, &source_config, kind, "original", "archive.zip").await;
        let table = match kind {
            Intake::Mpu => "multipart_uploads",
            Intake::Import => "import_jobs",
        };
        let pk = match kind {
            Intake::Mpu => "upload_id",
            Intake::Import => "id",
        };
        let key: BTreeMap<String, String> = [(pk.into(), "original".into())].into();
        let mut declaration: migration::Declaration = serde_json::from_value(serde_json::json!({
            "version":1,"database_scope":"zip-capture-fixture","ttl_seconds":300,
            "maintenance":{"all_writers_stopped":true,"all_workers_drained":true},
            "namespaces":[{"source":"old","target":target.allocation_key(),
                "backend":target.backend,"scope":target.scope,"resource_type":target.resource_type(),
                "binding_only":false,"target_route":target.route_snapshot(),"bindings":[
                    {"table":"pin_provider_usage","key":{"provider":"old"},"provider":"old","historical_route":null},
                    {"table":"pin_provider_routes","key":{"provider":"old"},"provider":"old","historical_route":source.route_snapshot()}
                ]}]
        })).unwrap();
        declaration.namespaces[0]
            .bindings
            .extend(POINTERS.map(|pointer| migration::Binding {
                table: table.into(),
                key: key.clone(),
                provider: "old".into(),
                pointer: Some(pointer.into()),
                historical_route: None,
            }));
        let mut fixture = Self {
            db,
            capture,
            table,
            key,
            raw: String::new(),
            declaration,
        };
        fixture.raw = fixture.read_raw().await;
        assert_eq!(
            fixture.raw,
            store::pinning::publication::serialize_zip_capture(&fixture.capture).unwrap()
        );
        let json: serde_json::Value = serde_json::from_str(&fixture.raw).unwrap();
        for pointer in POINTERS {
            assert_eq!(
                json.pointer(pointer.trim_start_matches("/zip_intent_json")),
                Some(&serde_json::json!("old"))
            );
        }
        fixture
    }

    pub fn omitted(&self) -> migration::Declaration {
        let mut declaration = self.declaration.clone();
        declaration.namespaces[0]
            .bindings
            .retain(|b| b.table != self.table);
        declaration
    }

    pub async fn images(&self) -> Vec<migration::TableImage> {
        let versions = migration::snapshot::schema(&self.db, false).await.unwrap();
        migration::snapshot::read(&self.db, &versions)
            .await
            .unwrap()
    }

    pub async fn read_raw(&self) -> String {
        let (pk, id) = self.key.first_key_value().unwrap();
        self.db
            .query_one(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                format!("SELECT zip_intent_json FROM {} WHERE {pk}=$1", self.table),
                [id.clone().into()],
            ))
            .await
            .unwrap()
            .unwrap()
            .try_get("", "zip_intent_json")
            .unwrap()
    }

    pub async fn later_capture(&self) {
        let current_config = config(Some(("post-receipt-account", "post-receipt-account")));
        let current = ValidatedPinningConfig::from_config(&current_config, |_| None).unwrap();
        let identity = &current.providers[0].identity;
        ledger::register_route(&self.db, &identity.allocation_key(), identity)
            .await
            .unwrap();
        persist(
            &self.db,
            &current_config,
            Intake::Mpu,
            "post-receipt",
            "later.zip",
        )
        .await;
    }
}
