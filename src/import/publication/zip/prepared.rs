use serde::{Deserialize, Serialize};

use super::{ZipPublicationData, ZipPublicationEntry};
use crate::{
    import::{
        ImportClaim, ImportExecutionError, ImportFailureCode, execution_error::terminal_failure,
        pipeline::ImportArtifact,
    },
    store::{
        entities::import_job,
        import::{
            ownership::{ExpectedImportTarget, ImportPublicationGuard},
            results::ZipResultRecord,
        },
    },
    zip::{
        batch::{FinalZipManifest, ManifestFailure, ManifestFile},
        response::ExtractedEntry,
    },
};

type PreparedResultRecord = (
    String,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedImport {
    schema_version: u8,
    cid: String,
    size: u64,
    content_type: Option<String>,
    targets: Vec<(String, String, i64)>,
    successes: Vec<(String, String, String, i64)>,
    failures: Vec<(String, String)>,
    root_error: Option<String>,
    // Original observation order and safe client-facing messages, not path sort.
    records: Vec<PreparedResultRecord>,
}

fn invalid() -> ImportExecutionError {
    terminal_failure(
        ImportFailureCode::PublicationFailed,
        "prepared ZIP response needs_attention",
    )
}

fn static_code(code: &str) -> Result<&'static str, ImportExecutionError> {
    match code {
        "entry_read_failed" => Ok("entry_read_failed"),
        "entry_upload_failed" => Ok("entry_upload_failed"),
        "entry_failed" => Ok("entry_failed"),
        "invalid_manifest" => Ok("invalid_manifest"),
        "path_conflict" => Ok("path_conflict"),
        "too_many_entries" => Ok("too_many_entries"),
        _ => Err(invalid()),
    }
}

impl PreparedImport {
    pub(super) fn capture(
        artifact: &ImportArtifact,
        guard: &ImportPublicationGuard,
        data: &ZipPublicationData<'_>,
    ) -> Result<Self, ImportExecutionError> {
        let manifest = data.manifest.ok_or_else(invalid)?;
        Ok(Self {
            schema_version: 1,
            cid: artifact.cid.clone(),
            size: artifact.logical_size,
            content_type: artifact.object_content_type.clone(),
            targets: guard
                .targets
                .iter()
                .map(|t| (t.bucket.clone(), t.key.clone(), t.generation))
                .collect(),
            successes: manifest
                .successful
                .iter()
                .map(|f| {
                    (
                        f.relative_path.clone(),
                        f.object_key.clone(),
                        f.cid.clone(),
                        f.size,
                    )
                })
                .collect(),
            failures: manifest
                .failed
                .iter()
                .map(|f| (f.path.clone(), f.code.to_owned()))
                .collect(),
            root_error: manifest.root_error.map(str::to_owned),
            records: data
                .records
                .iter()
                .map(|r| {
                    (
                        r.key.clone(),
                        r.cid.clone(),
                        r.size,
                        r.error_code.clone(),
                        r.error_message.clone(),
                    )
                })
                .collect(),
        })
    }

    pub(super) fn json(&self) -> Result<String, ImportExecutionError> {
        serde_json::to_string(self).map_err(|_| invalid())
    }

    pub(super) fn decode(json: &str) -> Result<Self, ImportExecutionError> {
        let result: Self = serde_json::from_str(json).map_err(|_| invalid())?;
        if result.schema_version != 1
            || result.cid.is_empty()
            || result.size > i64::MAX as u64
            || result.successes.len().saturating_add(result.failures.len())
                > crate::zip::extract::MAX_ARCHIVE_ENTRIES as usize
            || result.records.len() > crate::zip::extract::MAX_ARCHIVE_ENTRIES as usize
            || result.targets.len() > crate::zip::extract::MAX_ARCHIVE_ENTRIES as usize + 1
        {
            return Err(invalid());
        }
        Ok(result)
    }

    pub(super) fn restore(
        self,
        job: &import_job::Model,
        claim: &ImportClaim,
    ) -> Result<RestoredImport, ImportExecutionError> {
        let guard = ImportPublicationGuard {
            job_id: claim.job_id.clone(),
            worker_id: claim.worker_id.clone(),
            claim_epoch: claim.claim_epoch,
            targets: self
                .targets
                .into_iter()
                .map(|(bucket, key, generation)| ExpectedImportTarget {
                    bucket,
                    key,
                    generation,
                })
                .collect(),
        };
        if guard.targets.iter().any(|t| t.bucket != job.bucket)
            || !guard.targets.iter().any(|t| t.key == job.key)
        {
            return Err(invalid());
        }
        let mut entries = Vec::with_capacity(self.successes.len());
        let mut manifest = FinalZipManifest::default();
        for (relative_path, object_key, cid, size) in self.successes {
            let target = guard
                .targets
                .iter()
                .find(|t| t.key == object_key)
                .ok_or_else(invalid)?
                .clone();
            entries.push(ZipPublicationEntry {
                entry: ExtractedEntry {
                    key: object_key.clone(),
                    cid: cid.clone(),
                    size,
                },
                target,
            });
            manifest.successful.push(ManifestFile {
                relative_path,
                object_key,
                cid,
                size,
            });
        }
        for (path, code) in self.failures {
            manifest.failed.push(ManifestFailure {
                path,
                code: static_code(&code)?,
            });
        }
        manifest.root_error = self.root_error.as_deref().map(static_code).transpose()?;
        Ok(RestoredImport {
            artifact: ImportArtifact {
                cid: self.cid,
                logical_size: self.size,
                object_content_type: self.content_type,
            },
            entries,
            manifest,
            guard,
            records: self
                .records
                .into_iter()
                .map(
                    |(key, cid, size, error_code, error_message)| ZipResultRecord {
                        key,
                        cid,
                        size,
                        error_code,
                        error_message,
                    },
                )
                .collect(),
        })
    }
}

pub(super) struct RestoredImport {
    pub artifact: ImportArtifact,
    pub entries: Vec<ZipPublicationEntry>,
    pub records: Vec<ZipResultRecord>,
    pub manifest: FinalZipManifest,
    pub guard: ImportPublicationGuard,
}
