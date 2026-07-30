use std::fmt;

use chrono::{DateTime, Utc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImportState {
    Queued,
    Running,
    Completed,
    Failed,
    Superseded,
}

impl ImportState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Superseded => "superseded",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Superseded)
    }

    pub const fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Queued => matches!(next, Self::Queued | Self::Running | Self::Superseded),
            Self::Running => matches!(
                next,
                Self::Queued | Self::Running | Self::Completed | Self::Failed | Self::Superseded
            ),
            Self::Completed | Self::Failed | Self::Superseded => false,
        }
    }

    pub fn validate_transition(self, next: Self) -> anyhow::Result<()> {
        if self.can_transition_to(next) {
            Ok(())
        } else {
            anyhow::bail!(
                "invalid import state transition from {} to {}",
                self.as_str(),
                next.as_str()
            )
        }
    }
}

impl fmt::Display for ImportState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImportPhase {
    Queued,
    DiscoveringProviders,
    PinningLocal,
    Downloading,
    AddingToIpfs,
    Inspecting,
    Decompressing,
    Publishing,
}

impl ImportPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::DiscoveringProviders => "discovering_providers",
            Self::PinningLocal => "pinning_local",
            Self::Downloading => "downloading",
            Self::AddingToIpfs => "adding_to_ipfs",
            Self::Inspecting => "inspecting",
            Self::Decompressing => "decompressing",
            Self::Publishing => "publishing",
        }
    }

    pub const fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Queued => matches!(
                next,
                Self::Queued | Self::DiscoveringProviders | Self::Downloading
            ),
            Self::DiscoveringProviders => matches!(next, Self::PinningLocal),
            Self::PinningLocal => matches!(
                next,
                Self::PinningLocal | Self::Inspecting | Self::Decompressing | Self::Publishing
            ),
            Self::Downloading => matches!(next, Self::Downloading | Self::AddingToIpfs),
            Self::AddingToIpfs => matches!(next, Self::AddingToIpfs | Self::PinningLocal),
            Self::Inspecting => matches!(
                next,
                Self::Inspecting | Self::Decompressing | Self::Publishing
            ),
            Self::Decompressing => matches!(next, Self::Decompressing | Self::Publishing),
            Self::Publishing => matches!(next, Self::Publishing),
        }
    }

    pub fn validate_transition(self, next: Self) -> anyhow::Result<()> {
        if self.can_transition_to(next) {
            Ok(())
        } else {
            anyhow::bail!(
                "invalid import phase transition from {} to {}",
                self.as_str(),
                next.as_str()
            )
        }
    }
}

impl fmt::Display for ImportPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ImportSource {
    Cid(String),
    Url(url::Url),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupersedeReason {
    NewImport,
    PutObject,
    CopyObject,
    DeleteObject,
    CompleteMultipartUpload,
    DecompressZip,
    DeleteBucket,
}

impl SupersedeReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NewImport => "new_import",
            Self::PutObject => "put_object",
            Self::CopyObject => "copy_object",
            Self::DeleteObject => "delete_object",
            Self::CompleteMultipartUpload => "complete_multipart_upload",
            Self::DecompressZip => "decompress_zip",
            Self::DeleteBucket => "delete_bucket",
        }
    }
}

impl fmt::Display for SupersedeReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImportFailureCode {
    SourceUnreachable,
    SourceHttpError,
    SourceRedirected,
    SourceTooLarge,
    SourceStalled,
    CidNotFound,
    CidNotFile,
    KuboAddFailed,
    KuboPinFailed,
    InvalidArchive,
    DecompressionLimitExceeded,
    PublicationFailed,
    JobDeadlineExceeded,
}

impl ImportFailureCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SourceUnreachable => "source_unreachable",
            Self::SourceHttpError => "source_http_error",
            Self::SourceRedirected => "source_redirected",
            Self::SourceTooLarge => "source_too_large",
            Self::SourceStalled => "source_stalled",
            Self::CidNotFound => "cid_not_found",
            Self::CidNotFile => "cid_not_file",
            Self::KuboAddFailed => "kubo_add_failed",
            Self::KuboPinFailed => "kubo_pin_failed",
            Self::InvalidArchive => "invalid_archive",
            Self::DecompressionLimitExceeded => "decompression_limit_exceeded",
            Self::PublicationFailed => "publication_failed",
            Self::JobDeadlineExceeded => "job_deadline_exceeded",
        }
    }
}

impl fmt::Display for ImportFailureCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportFailure {
    pub code: ImportFailureCode,
    pub message: String,
    pub retryable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportClaim {
    pub job_id: String,
    pub worker_id: String,
    pub attempt: u32,
    pub claim_epoch: i64,
    pub locked_until: DateTime<Utc>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ImportProgress {
    pub providers_observed: u32,
    pub pin_nodes_processed: u64,
    pub pin_bytes_processed: u64,
    pub downloaded_bytes: u64,
    pub download_total: Option<u64>,
    pub ipfs_add_bytes: u64,
    pub logical_size: Option<u64>,
    pub entries_processed: u64,
    pub entries_succeeded: u64,
    pub entries_failed: u64,
    pub decompressed_bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ImportExecutionError {
    #[error("retryable import failure")]
    Retryable(ImportFailure),
    #[error("terminal import failure")]
    Terminal(ImportFailure),
    #[error("import ownership was superseded")]
    Superseded,
    #[error("import execution was interrupted by process shutdown")]
    Interrupted,
}

#[cfg(test)]
mod tests {
    use super::{ImportPhase, ImportState};

    #[test]
    fn import_model_uses_stable_state_and_phase_strings() {
        assert_eq!(ImportState::Queued.as_str(), "queued");
        assert_eq!(ImportState::Running.as_str(), "running");
        assert_eq!(ImportState::Completed.as_str(), "completed");
        assert_eq!(ImportState::Failed.as_str(), "failed");
        assert_eq!(ImportState::Superseded.as_str(), "superseded");

        let phases = [
            (ImportPhase::Queued, "queued"),
            (ImportPhase::DiscoveringProviders, "discovering_providers"),
            (ImportPhase::PinningLocal, "pinning_local"),
            (ImportPhase::Downloading, "downloading"),
            (ImportPhase::AddingToIpfs, "adding_to_ipfs"),
            (ImportPhase::Inspecting, "inspecting"),
            (ImportPhase::Decompressing, "decompressing"),
            (ImportPhase::Publishing, "publishing"),
        ];
        for (phase, expected) in phases {
            assert_eq!(phase.as_str(), expected);
            assert_eq!(phase.to_string(), expected);
        }
    }

    #[test]
    fn import_model_rejects_terminal_to_running_transitions() {
        for terminal in [
            ImportState::Completed,
            ImportState::Failed,
            ImportState::Superseded,
        ] {
            assert!(terminal.validate_transition(ImportState::Running).is_err());
        }
    }
}
