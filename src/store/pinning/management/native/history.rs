//! Exact committed catalog compatibility, not an execution projection.
use super::*;
use sea_orm_migration::MigratorTrait;

pub(super) fn current_versions() -> Vec<String> {
    crate::store::migrator::Migrator::migrations()
        .iter()
        .map(|migration| migration.name().to_owned())
        .collect()
}

/// Compiled catalog metadata contains no SQL cells or authorization state.
/// Archive callers borrow it and prepay every owned catalog they materialize.
pub(crate) fn archived_catalog(versions: &[String]) -> Result<&'static [NativeTable]> {
    use std::sync::LazyLock;
    static VERSIONS: LazyLock<Vec<String>> = LazyLock::new(current_versions);
    static PRE_DOCUMENTS: LazyLock<Vec<NativeTable>> = LazyLock::new(|| {
        let mut tables = registry();
        tables.retain(|table| table.table != "import_job_requests");
        *tables
            .iter_mut()
            .find(|table| table.table == "import_jobs")
            .expect("import jobs catalog") =
            super::entity::<crate::store::entities::import_job::Entity>();
        tables
    });
    static HISTORICAL: LazyLock<Vec<NativeTable>> = LazyLock::new(|| {
        let mut tables = PRE_DOCUMENTS.clone();
        if let Some(root) = tables
            .iter_mut()
            .find(|table| table.table == "zip_root_references")
        {
            *root = historical_root_table();
        }
        tables
    });
    if versions == *VERSIONS {
        return Ok(current_catalog());
    }
    let known = &*VERSIONS;
    if versions.len() >= known.len() || versions != &known[..versions.len()] {
        return Err(ManagementError::InvalidEvidence);
    }
    match versions.last().map(String::as_str) {
        Some("m20261013_000001_zip_root_logical_release") => Ok(&PRE_DOCUMENTS),
        Some("m20261017_000001_management_storage") => Ok(&PRE_DOCUMENTS),
        Some("m20261012_000001_pinning_management") => Ok(&HISTORICAL),
        _ => Err(ManagementError::InvalidEvidence),
    }
}

pub(crate) fn current_catalog() -> &'static [NativeTable] {
    use std::sync::LazyLock;
    static CATALOG: LazyLock<Vec<NativeTable>> = LazyLock::new(registry);
    &CATALOG
}

fn historical_root_table() -> NativeTable {
    let columns = [
        "batch_id",
        "cid",
        "created_at",
        "epoch",
        "node_identity",
        "revision",
        "state",
        "tier",
        "updated_at",
        "verification_receipt",
    ];
    let definitions = columns
        .iter()
        .map(|name| {
            let (sqlite_type, postgres_type) = match *name {
                "revision" | "epoch" => ("BIGINT", "bigint"),
                "created_at" | "updated_at" => ("TIMESTAMP", "timestamp with time zone"),
                _ => ("TEXT", "text"),
            };
            super::super::catalog::NativeColumn {
                name: (*name).into(),
                sqlite_type: sqlite_type.into(),
                postgres_type: postgres_type.into(),
                nullable: *name == "verification_receipt",
            }
        })
        .collect::<Vec<_>>();
    NativeTable {
        table: "zip_root_references".into(),
        columns: columns
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>()
            .into(),
        primary_key: [
            "batch_id",
            "revision",
            "epoch",
            "node_identity",
            "tier",
            "cid",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>()
        .into(),
        rows: vec![],
        definitions: definitions.into(),
    }
}
