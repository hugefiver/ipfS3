use std::collections::BTreeSet;

use chrono::Utc;

use crate::{
    error::AppError,
    import::{
        ImportClaim, ImportExecutionError, ImportFailureCode,
        execution_error::{ensure_active, map_publication_error, terminal_failure},
        pipeline::JobCancellation,
        progress::{ExtractionProgress, ProgressReporter},
        publication::zip::ZipPublicationEntry,
    },
    state::AppState,
    store::import::{
        ownership::{ExpectedImportTarget, claim_extracted_target, release_extracted_target},
        results::ZipResultRecord,
    },
    zip::{
        extract::ExtractionObserver,
        response::{ExtractFailure, ExtractedEntry},
    },
};

pub(super) struct ImportExtractionObserver<'a> {
    state: &'a AppState,
    job_bucket: &'a str,
    archive_key: &'a str,
    claim: &'a ImportClaim,
    cancellation: &'a JobCancellation,
    reporter: &'a ProgressReporter,
    seen: BTreeSet<String>,
    pending: Option<(String, i64)>,
    progress: ExtractionProgress,
    pub successful: Vec<ZipPublicationEntry>,
    pub records: Vec<ZipResultRecord>,
}

impl<'a> ImportExtractionObserver<'a> {
    pub fn new(
        state: &'a AppState,
        job_bucket: &'a str,
        archive_key: &'a str,
        claim: &'a ImportClaim,
        cancellation: &'a JobCancellation,
        reporter: &'a ProgressReporter,
    ) -> Self {
        Self {
            state,
            job_bucket,
            archive_key,
            claim,
            cancellation,
            reporter,
            seen: BTreeSet::new(),
            pending: None,
            progress: ExtractionProgress::default(),
            successful: Vec::new(),
            records: Vec::new(),
        }
    }

    async fn report(&self) -> Result<(), ImportExecutionError> {
        self.reporter
            .extraction(self.progress, self.cancellation)
            .await
    }

    fn next_counter(value: &mut u64, label: &str) -> Result<(), ImportExecutionError> {
        *value = value.checked_add(1).ok_or_else(|| {
            terminal_failure(
                ImportFailureCode::DecompressionLimitExceeded,
                &format!("ZIP import {label} counter exceeded the supported range"),
            )
        })?;
        Ok(())
    }

    fn map_ownership(error: AppError, cancellation: &JobCancellation) -> ImportExecutionError {
        map_publication_error(error, cancellation)
    }
}

#[async_trait::async_trait]
impl ExtractionObserver for ImportExtractionObserver<'_> {
    type Error = ImportExecutionError;

    async fn entry_started(&mut self, key: &str) -> Result<(), Self::Error> {
        ensure_active(self.cancellation)?;
        if key == self.archive_key || !self.seen.insert(key.to_owned()) {
            return Err(terminal_failure(
                ImportFailureCode::InvalidArchive,
                "ZIP output key collides with another import destination",
            ));
        }
        if self.pending.is_some() {
            return Err(terminal_failure(
                ImportFailureCode::InvalidArchive,
                "ZIP extractor started an entry before finishing the prior entry",
            ));
        }
        let generation = claim_extracted_target(
            self.state.store.db(),
            self.claim,
            self.job_bucket,
            key,
            Utc::now(),
        )
        .await
        .map_err(|error| Self::map_ownership(error, self.cancellation))?;
        self.pending = Some((key.to_owned(), generation));
        Self::next_counter(&mut self.progress.entries_processed, "entry")?;
        self.report().await
    }

    async fn entry_finished(&mut self, entry: &ExtractedEntry) -> Result<(), Self::Error> {
        ensure_active(self.cancellation)?;
        let Some((key, generation)) = self.pending.take() else {
            return Err(terminal_failure(
                ImportFailureCode::InvalidArchive,
                "ZIP extractor finished an entry that was not started",
            ));
        };
        if key != entry.key {
            return Err(terminal_failure(
                ImportFailureCode::InvalidArchive,
                "ZIP extractor changed an entry key during extraction",
            ));
        }
        self.successful.push(ZipPublicationEntry {
            entry: entry.clone(),
            target: ExpectedImportTarget {
                bucket: self.job_bucket.to_owned(),
                key: key.clone(),
                generation,
            },
        });
        self.records.push(ZipResultRecord {
            key,
            cid: Some(entry.cid.clone()),
            size: Some(entry.size),
            error_code: None,
            error_message: None,
        });
        Self::next_counter(&mut self.progress.entries_succeeded, "success")?;
        self.report().await
    }

    async fn entry_failed(&mut self, key: &str, error: &ExtractFailure) -> Result<(), Self::Error> {
        ensure_active(self.cancellation)?;
        if let Some((pending_key, generation)) = self.pending.take() {
            if pending_key != key {
                return Err(terminal_failure(
                    ImportFailureCode::InvalidArchive,
                    "ZIP extractor failed a different entry than it started",
                ));
            }
            release_extracted_target(
                self.state.store.db(),
                self.claim,
                self.job_bucket,
                key,
                generation,
                Utc::now(),
            )
            .await
            .map_err(|error| Self::map_ownership(error, self.cancellation))?;
            self.records.push(ZipResultRecord {
                key: key.to_owned(),
                cid: None,
                size: None,
                error_code: Some(error.code.clone()),
                error_message: Some(error.message.clone()),
            });
            Self::next_counter(&mut self.progress.entries_failed, "failure")?;
            self.report().await
        } else if let Some(record) = self
            .records
            .last_mut()
            .filter(|record| record.key == key && record.error_code.is_some())
        {
            record.error_code = Some(error.code.clone());
            record.error_message = Some(error.message.clone());
            Ok(())
        } else {
            self.seen.insert(key.to_owned());
            Self::next_counter(&mut self.progress.entries_processed, "entry")?;
            Self::next_counter(&mut self.progress.entries_failed, "failure")?;
            self.records.push(ZipResultRecord {
                key: key.to_owned(),
                cid: None,
                size: None,
                error_code: Some(error.code.clone()),
                error_message: Some(error.message.clone()),
            });
            self.report().await
        }
    }

    async fn bytes_processed(&mut self, bytes: u64) -> Result<(), Self::Error> {
        ensure_active(self.cancellation)?;
        self.progress.decompressed_bytes = self
            .progress
            .decompressed_bytes
            .checked_add(bytes)
            .ok_or_else(|| {
                terminal_failure(
                    ImportFailureCode::DecompressionLimitExceeded,
                    "ZIP decompressed byte counter exceeded the supported range",
                )
            })?;
        self.report().await
    }
}
