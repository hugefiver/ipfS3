use super::*;
use sea_orm_migration::MigrationTrait;

macro_rules! migrations {
    ($($module:ident),+ $(,)?) => {
        #[cfg(any(test, feature = "historical-fixtures"))]
        pub(crate) fn historical() -> Vec<Box<dyn MigrationTrait>> {
            vec![$(Box::new($module::Migration)),+]
        }
    }
}
pub(crate) fn native() -> Vec<Box<dyn MigrationTrait>> {
    vec![Box::new(m20261027_000001_schema_baseline::Migration)]
}
pub(super) fn shared() -> Vec<Box<dyn bridge::SharedMigration>> {
    vec![Box::new(m20261027_000001_schema_baseline::Migration)]
}
migrations!(
    m20250701_000001_init,
    m20260707_000001_decompress_zip,
    m20260720_000001_sse_c_key_fingerprint,
    m20260721_000001_multi_provider_pinning,
    m20260729_000001_ipfs3_import,
    m20260729_000002_postgres_utc_timestamps,
    m20260730_000001_standard_mutation_fence,
    m20260813_000001_postgres_json_columns,
    m20260825_000001_object_versioning,
    m20260826_000001_lifecycle_expiration,
    m20260831_000001_bucket_cors,
    m20260901_000001_lifecycle_abort_multipart,
    m20260912_000001_residency_references,
    m20260912_000002_lifecycle_transition,
    m20260919_000001_standard_mutation_lease,
    m20260920_000001_pin_submit_history,
    m20260920_000002_pin_identity_ledger,
    m20260920_000003_pin_extension_decision,
    m20260920_000004_multipart_pin_decision,
    m20260920_000005_import_pin_decision,
    m20260920_000006_pin_submit_correlation,
    m20260927_000001_zip_batches,
    m20260927_000002_import_zip_root_capture,
    m20260927_000003_zip_mpu_replay,
    m20260927_000004_zip_v2_execution,
    m20260927_000005_zip_v2_import,
    m20260927_000006_zip_v2_mpu_intake,
    m20260927_000007_zip_v2_mpu_completion,
    m20261004_000001_rpc_submission_ledger,
    m20261004_000002_cluster_replica_contract,
    m20261007_000001_legacy_pinning_migration,
    m20261008_000001_zip_intent_capture,
    m20261012_000001_pinning_management,
    m20261013_000001_zip_root_logical_release,
    m20261017_000001_management_storage,
    m20261023_000001_inline_documents,
);
