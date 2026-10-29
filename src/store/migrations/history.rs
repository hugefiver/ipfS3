//! Historical identities describe layouts, never current execution authority.
pub const M13: &str = "m20261013_000001_zip_root_logical_release";
pub const M17: &str = "m20261017_000001_management_storage";
pub const M23: &str = "m20261023_000001_inline_documents";
pub const VERSIONS: &[&str] = &[
    "m20250701_000001_init",
    "m20260707_000001_decompress_zip",
    "m20260720_000001_sse_c_key_fingerprint",
    "m20260721_000001_multi_provider_pinning",
    "m20260729_000001_ipfs3_import",
    "m20260729_000002_postgres_utc_timestamps",
    "m20260730_000001_standard_mutation_fence",
    "m20260813_000001_postgres_json_columns",
    "m20260825_000001_object_versioning",
    "m20260826_000001_lifecycle_expiration",
    "m20260831_000001_bucket_cors",
    "m20260901_000001_lifecycle_abort_multipart",
    "m20260912_000001_residency_references",
    "m20260912_000002_lifecycle_transition",
    "m20260919_000001_standard_mutation_lease",
    "m20260920_000001_pin_submit_history",
    "m20260920_000002_pin_identity_ledger",
    "m20260920_000003_pin_extension_decision",
    "m20260920_000004_multipart_pin_decision",
    "m20260920_000005_import_pin_decision",
    "m20260920_000006_pin_submit_correlation",
    "m20260927_000001_zip_batches",
    "m20260927_000002_import_zip_root_capture",
    "m20260927_000003_zip_mpu_replay",
    "m20260927_000004_zip_v2_execution",
    "m20260927_000005_zip_v2_import",
    "m20260927_000006_zip_v2_mpu_intake",
    "m20260927_000007_zip_v2_mpu_completion",
    "m20261004_000001_rpc_submission_ledger",
    "m20261004_000002_cluster_replica_contract",
    "m20261007_000001_legacy_pinning_migration",
    "m20261008_000001_zip_intent_capture",
    "m20261012_000001_pinning_management",
    M13,
    M17,
    M23,
];

pub fn prefix(endpoint: &str) -> Option<&'static [&'static str]> {
    VERSIONS
        .iter()
        .position(|name| *name == endpoint)
        .map(|end| &VERSIONS[..=end])
}

pub fn is_prefix(versions: &[String]) -> bool {
    !versions.is_empty()
        && versions.len() <= VERSIONS.len()
        && versions
            .iter()
            .map(String::as_str)
            .eq(VERSIONS[..versions.len()].iter().copied())
}

pub fn is_m23(versions: &[String]) -> bool {
    versions.len() == VERSIONS.len() && is_prefix(versions)
}

/// Layout-only predicate for immutable historical documents.
pub fn has_final_layout(versions: &[String]) -> bool {
    versions == [super::baseline::VERSION] || is_m23(versions)
}

pub fn has_stage(versions: &[String], stage: &str) -> bool {
    versions == [super::baseline::VERSION] || versions.iter().any(|v| v == stage)
}

pub fn final_checkpoint(revision: &str, catalog: &[String]) -> bool {
    catalog.last().map(String::as_str) == Some(revision) && has_final_layout(catalog)
}
