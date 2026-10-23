//! Native capture layout for the isolated pre-document migration fixtures.
use ipfs_s3_gateway::store::pinning::management::native::{self, NativeTable};

pub fn tables(versions: &[String]) -> Vec<NativeTable> {
    let version = versions.last().unwrap().as_str();
    assert!(matches!(
        version,
        "m20261012_000001_pinning_management" | "m20261013_000001_zip_root_logical_release"
    ));
    let mut tables = native::registry();
    let index = tables
        .iter()
        .position(|table| table.table == "import_job_requests")
        .unwrap();
    let requests = tables.remove(index);
    let jobs = tables
        .iter_mut()
        .find(|table| table.table == "import_jobs")
        .unwrap();
    let mut definitions = jobs.definitions.to_vec();
    definitions.extend(
        requests
            .definitions
            .iter()
            .filter(|column| column.name != "job_id")
            .cloned(),
    );
    definitions.sort_by(|a, b| a.name.cmp(&b.name));
    jobs.columns = definitions
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>()
        .into();
    jobs.definitions = definitions.into();
    if version == "m20261012_000001_pinning_management" {
        let root = tables
            .iter_mut()
            .find(|table| table.table == "zip_root_references")
            .unwrap();
        root.columns = root
            .columns
            .iter()
            .filter(|column| *column != "logical_released_at")
            .cloned()
            .collect::<Vec<_>>()
            .into();
        root.definitions = root
            .definitions
            .iter()
            .filter(|column| column.name != "logical_released_at")
            .cloned()
            .collect::<Vec<_>>()
            .into();
    }
    tables
}
