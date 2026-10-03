//! Final ZIP root manifest from successfully extracted objects.

use std::collections::{HashMap, HashSet};

use crate::kubo::directory::DirectoryFile;
use crate::store::zip::ManifestItem;
use crate::zip::response::{ExtractFailure, ExtractedEntry};

const MAX_METADATA_RECORDS: usize = crate::zip::extract::MAX_ARCHIVE_ENTRIES as usize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestFile {
    /// Durable binding path. For unrepresentable ZIP paths this is a synthetic
    /// ordinal, not a UnixFS path; `object_key` always remains the exact S3 key.
    pub relative_path: String,
    pub object_key: String,
    pub cid: String,
    pub size: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestFailure {
    pub path: String,
    pub code: &'static str,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FinalZipManifest {
    pub successful: Vec<ManifestFile>,
    pub failed: Vec<ManifestFailure>,
    pub root_error: Option<&'static str>,
}

impl FinalZipManifest {
    /// Never expose synthetic paths as UnixFS files. Object successes remain
    /// available for S3 publication even when the root cannot be built.
    pub fn directory_files(&self) -> Vec<DirectoryFile> {
        if self.root_error.is_some() {
            return Vec::new();
        }
        self.successful
            .iter()
            .map(|file| DirectoryFile {
                path: file.relative_path.clone(),
                cid: file.cid.clone(),
            })
            .collect()
    }

    pub fn manifest_items(&self) -> Vec<ManifestItem> {
        let mut items = Vec::with_capacity(self.successful.len() + self.failed.len());
        items.extend(self.successful.iter().map(|file| ManifestItem::Success {
            path: file.relative_path.clone(),
            object_key: file.object_key.clone(),
            cid: file.cid.clone(),
            size: file.size,
        }));
        items.extend(self.failed.iter().map(|failure| ManifestItem::Failure {
            path: failure.path.clone(),
            code: failure.code.to_owned(),
        }));
        items
    }
}

fn valid_relative_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

fn safe_failure_code(code: &str) -> &'static str {
    match code {
        "EntryReadFailed" => "entry_read_failed",
        "EntryUploadFailed" => "entry_upload_failed",
        _ => "entry_failed",
    }
}

fn has_file_directory_conflict(files: &[ManifestFile]) -> bool {
    let paths: HashSet<&str> = files
        .iter()
        .map(|file| file.relative_path.as_str())
        .collect();
    files.iter().any(|file| {
        file.relative_path
            .match_indices('/')
            .any(|(index, _)| paths.contains(&file.relative_path[..index]))
    })
}

/// `target_prefix` must be normalized with `normalize_target_prefix` first.
/// Only successful entries compete for duplicate keys; failures are metadata, not objects.
/// `root_error` is a directory-root outcome and does not cancel object publication.
pub fn final_zip_manifest(
    entries: &[ExtractedEntry],
    failures: &[ExtractFailure],
    target_prefix: &str,
) -> FinalZipManifest {
    // Even if called with an outcome from outside the configured extractor,
    // never transform real results into a valid-looking empty archive.
    let over_limit = entries.len().saturating_add(failures.len()) > MAX_METADATA_RECORDS;

    let mut winners = HashMap::<&str, usize>::with_capacity(entries.len());
    let mut invalid_root_path = false;
    for (index, entry) in entries.iter().enumerate() {
        invalid_root_path |= entry
            .key
            .strip_prefix(target_prefix)
            .is_none_or(|path| !valid_relative_path(path));
        if entry.key.len() > 1024
            || entry.cid.is_empty()
            || entry.cid.len() > 4096
            || entry.size < 0
        {
            return FinalZipManifest {
                root_error: Some("invalid_manifest"),
                ..FinalZipManifest::default()
            };
        }
        winners.insert(&entry.key, index);
    }

    // Stable order by each key's final successful occurrence, matching publication's
    // stable key sort (which keeps later successes last for equal keys).
    let mut final_winners: Vec<_> = winners.into_iter().collect();
    final_winners.sort_unstable_by_key(|&(_, index)| index);
    // Reserve a whole first segment for invalid-path bindings so their durable
    // IDs cannot alias real paths (or turn into file/directory conflicts).
    let first_segments: HashSet<_> = final_winners
        .iter()
        .filter_map(|(key, _)| {
            key.strip_prefix(target_prefix)
                .filter(|path| valid_relative_path(path))
                .and_then(|path| path.split('/').next())
        })
        .collect();
    let namespace_candidates = 0..=first_segments.len().saturating_add(1);
    let invalid_namespace = namespace_candidates
        .clone()
        .map(|n| {
            if n == 0 {
                "invalid".to_owned()
            } else {
                format!("invalid-{n}")
            }
        })
        .find(|candidate| !first_segments.contains(candidate.as_str()))
        .expect("enough candidate names for all successful first segments");
    let successful: Vec<_> = final_winners
        .into_iter()
        .map(|(key, index)| {
            let entry = &entries[index];
            ManifestFile {
                relative_path: key
                    .strip_prefix(target_prefix)
                    .filter(|path| valid_relative_path(path))
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("{invalid_namespace}/{index}")),
                object_key: entry.key.clone(),
                cid: entry.cid.clone(),
                size: entry.size,
            }
        })
        .collect();

    // Failures also use a distinct reserved first segment.
    let failure_namespace = namespace_candidates
        .map(|n| {
            if n == 0 {
                "failed".to_owned()
            } else {
                format!("failed-{n}")
            }
        })
        .find(|candidate| {
            candidate != &invalid_namespace && !first_segments.contains(candidate.as_str())
        })
        .expect("enough candidate names for both reserved namespaces");
    let failed = failures
        .iter()
        .enumerate()
        .map(|(index, failure)| ManifestFailure {
            path: format!("{failure_namespace}/{index}"),
            code: safe_failure_code(&failure.code),
        })
        .collect();

    let root_error = if invalid_root_path || over_limit {
        Some("invalid_manifest")
    } else {
        has_file_directory_conflict(&successful).then_some("path_conflict")
    };
    FinalZipManifest {
        successful,
        failed,
        root_error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(key: &str, cid: &str, size: i64) -> ExtractedEntry {
        ExtractedEntry {
            key: key.into(),
            cid: cid.into(),
            size,
        }
    }

    fn failure(name: &str, code: &str) -> ExtractFailure {
        ExtractFailure {
            entry_name: name.into(),
            code: code.into(),
            message: "untrusted message".into(),
        }
    }

    #[test]
    fn nested_unicode_paths_keep_exact_identity_in_both_adapters() {
        let manifest = final_zip_manifest(
            &[
                entry("a/é/🐈.txt", "cid-cat", 9),
                entry("a/é/child/x", "cid-x", 0),
            ],
            &[],
            "a/",
        );
        assert_eq!(manifest.root_error, None);
        assert_eq!(
            manifest.successful,
            vec![
                ManifestFile {
                    relative_path: "é/🐈.txt".into(),
                    object_key: "a/é/🐈.txt".into(),
                    cid: "cid-cat".into(),
                    size: 9
                },
                ManifestFile {
                    relative_path: "é/child/x".into(),
                    object_key: "a/é/child/x".into(),
                    cid: "cid-x".into(),
                    size: 0
                },
            ]
        );
        let directory = manifest.directory_files();
        assert_eq!(
            (directory[0].path.as_str(), directory[0].cid.as_str()),
            ("é/🐈.txt", "cid-cat")
        );
        let items = manifest.manifest_items();
        assert!(
            matches!(&items[0], ManifestItem::Success { path, object_key, cid, size }
            if path == "é/🐈.txt" && object_key == "a/é/🐈.txt" && cid == "cid-cat" && *size == 9)
        );
    }

    #[test]
    fn duplicate_last_success_wins_and_failed_later_cannot_replace_it() {
        let manifest = final_zip_manifest(
            &[
                entry("p/dup", "cid-first", 1),
                entry("p/other", "cid-other", 2),
                entry("p/dup", "cid-last", 3),
            ],
            &[failure("dup", "EntryReadFailed")],
            "p/",
        );
        assert_eq!(manifest.root_error, None);
        assert_eq!(manifest.successful.len(), 2);
        assert_eq!(manifest.successful[0].relative_path, "other");
        assert_eq!(manifest.successful[1].cid, "cid-last");
        assert_eq!(manifest.successful[1].size, 3);
        assert_eq!(manifest.failed[0].code, "entry_read_failed");
        assert_ne!(manifest.failed[0].path, "dup");
    }

    #[test]
    fn file_directory_conflict_is_only_a_root_error() {
        let manifest = final_zip_manifest(
            &[entry("p/x/y", "one", 1), entry("p/x", "two", 2)],
            &[],
            "p/",
        );
        assert_eq!(manifest.root_error, Some("path_conflict"));
        assert_eq!(manifest.successful.len(), 2);
        assert_eq!(manifest.manifest_items().len(), 2);
    }

    #[test]
    fn empty_or_directory_only_archive_has_no_root_files() {
        let empty = final_zip_manifest(&[], &[], "");
        assert_eq!(empty, FinalZipManifest::default());
        assert!(empty.directory_files().is_empty());
        let no_files = final_zip_manifest(&[], &[failure("dir/", "EntryReadFailed")], "p/");
        assert!(no_files.successful.is_empty());
        assert_eq!(no_files.failed.len(), 1);
        assert_eq!(no_files.root_error, None);
    }

    #[test]
    fn untrusted_failure_names_and_codes_never_enter_persisted_paths_or_codes() {
        let manifest = final_zip_manifest(
            &[entry("p/failed/0", "a", 1), entry("p/failed", "b", 1)],
            &[
                failure("../../secrets", "DROP TABLE"),
                failure("C:\\bad", "EntryUploadFailed"),
            ],
            "p/",
        );
        assert_eq!(manifest.root_error, Some("path_conflict"));
        assert_eq!(manifest.failed.len(), 2);
        assert_eq!(manifest.failed[0].code, "entry_failed");
        assert_eq!(manifest.failed[1].code, "entry_upload_failed");
        assert!(
            manifest
                .failed
                .iter()
                .all(|f| f.path.starts_with("failed-1/")
                    && !f.path.contains("..")
                    && !f.path.contains('\\'))
        );
        assert!(manifest.manifest_items().iter().all(|item| match item {
            ManifestItem::Failure { path, code } =>
                path.starts_with("failed-1/") && !code.contains(' '),
            _ => true,
        }));
    }

    #[test]
    fn prefix_is_stripped_once_at_boundary_not_replaced_inside_names() {
        let manifest = final_zip_manifest(
            &[entry("ab/a/file", "one", 1), entry("ab/ab/x", "two", 2)],
            &[],
            "ab/",
        );
        assert_eq!(manifest.successful[0].relative_path, "a/file");
        assert_eq!(manifest.successful[1].relative_path, "ab/x");
        let mismatched = final_zip_manifest(&[entry("ab/file", "one", 1)], &[], "a/");
        assert_eq!(mismatched.root_error, Some("invalid_manifest"));
        assert_eq!(mismatched.successful.len(), 1);
        assert_eq!(mismatched.successful[0].relative_path, "invalid/0");
        assert_eq!(mismatched.successful[0].object_key, "ab/file");
    }

    #[test]
    fn invalid_root_path_keeps_exact_final_s3_winners_under_distinct_durable_paths() {
        let entries = [
            entry("p/invalid/0", "reserved", 1),
            entry("p/a//b", "old", 2),
            entry("p/a/b", "valid", 3),
            entry("p/a//b", "new", 4),
            entry("p/invalid-1/0", "also-reserved", 5),
        ];
        let failures = [failure("a//b", "EntryReadFailed")];
        let manifest = final_zip_manifest(&entries, &failures, "p/");
        assert_eq!(manifest, final_zip_manifest(&entries, &failures, "p/"));
        assert_eq!(manifest.root_error, Some("invalid_manifest"));
        assert!(manifest.directory_files().is_empty());
        assert_eq!(manifest.successful.len(), 4);
        assert_eq!(manifest.successful[0].object_key, "p/invalid/0");
        assert_eq!(manifest.successful[1].object_key, "p/a/b");
        assert_eq!(manifest.successful[2].object_key, "p/a//b");
        assert_eq!(manifest.successful[2].cid, "new");
        assert_eq!(manifest.successful[2].relative_path, "invalid-2/3");
        assert_eq!(manifest.successful[3].object_key, "p/invalid-1/0");
        assert_eq!(manifest.failed.len(), 1);
        assert_eq!(manifest.manifest_items().len(), 5);
        assert_eq!(
            manifest
                .successful
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<HashSet<_>>()
                .len(),
            4
        );
        assert!(manifest.failed.iter().all(|failure| {
            !manifest
                .successful
                .iter()
                .any(|file| file.relative_path == failure.path)
        }));
    }

    #[test]
    fn invalid_path_cannot_hide_valid_winner_or_turn_conflict_into_root_success() {
        let invalid = final_zip_manifest(
            &[
                entry("p/a", "one", 1),
                entry("p/a/b", "two", 2),
                entry("p/a//b", "three", 3),
            ],
            &[],
            "p/",
        );
        assert_eq!(invalid.root_error, Some("invalid_manifest"));
        assert_eq!(invalid.successful.len(), 3);
        assert!(invalid.directory_files().is_empty());
        let conflict = final_zip_manifest(
            &[entry("p/a", "one", 1), entry("p/a/b", "two", 2)],
            &[],
            "p/",
        );
        assert_eq!(conflict.root_error, Some("path_conflict"));
        assert_eq!(conflict.successful.len(), 2);
        assert!(conflict.directory_files().is_empty());
    }

    #[test]
    fn invalid_later_success_does_not_replace_last_valid_winner() {
        let manifest = final_zip_manifest(
            &[
                entry("p/a/b", "old", 1),
                entry("p/a//b", "invalid", 2),
                entry("p/a/b", "new", 3),
            ],
            &[failure("a/b", "EntryUploadFailed")],
            "p/",
        );
        assert_eq!(manifest.root_error, Some("invalid_manifest"));
        assert_eq!(manifest.successful.len(), 2);
        assert_eq!(manifest.successful[0].object_key, "p/a//b");
        assert_eq!(manifest.successful[0].relative_path, "invalid/1");
        assert_eq!(manifest.successful[1].relative_path, "a/b");
        assert_eq!(manifest.successful[1].cid, "new");
        assert_eq!(manifest.failed[0].code, "entry_upload_failed");
    }

    #[test]
    fn keeps_invalid_root_paths_and_over_limit_results_without_faking_an_empty_archive() {
        let invalid = final_zip_manifest(&[entry("p/a/../b", "cid", 2)], &[], "p/");
        assert_eq!(invalid.root_error, Some("invalid_manifest"));
        assert_eq!(invalid.successful[0].object_key, "p/a/../b");
        assert_eq!(invalid.successful[0].relative_path, "invalid/0");
        assert!(invalid.directory_files().is_empty());
        let entries: Vec<_> = (0..10_000)
            .map(|n| entry(&format!("p/{n}"), "cid", 0))
            .collect();
        assert_eq!(
            final_zip_manifest(&entries, &[], "p/").successful.len(),
            10_000
        );
        let too_many = final_zip_manifest(&entries, &[failure("bad", "EntryReadFailed")], "p/");
        assert_eq!(too_many.root_error, Some("invalid_manifest"));
        assert_eq!(too_many.successful.len(), 10_000);
        assert_eq!(too_many.failed.len(), 1);
        assert_eq!(too_many.manifest_items().len(), 10_001);
        assert!(too_many.directory_files().is_empty());
    }

    #[test]
    fn ten_thousand_and_one_distinct_successes_keep_their_real_bindings() {
        let entries: Vec<_> = (0..10_001)
            .map(|n| {
                let key = if n == 0 {
                    "p/invalid".to_owned()
                } else {
                    format!("p/invalid-{n}")
                };
                entry(&key, "cid", 0)
            })
            .collect();
        let manifest = final_zip_manifest(&entries, &[], "p/");
        assert_eq!(manifest.root_error, Some("invalid_manifest"));
        assert_eq!(manifest.successful.len(), 10_001);
        assert_eq!(manifest.manifest_items().len(), 10_001);
        assert!(manifest.directory_files().is_empty());
    }

    #[test]
    fn over_limit_failed_entries_are_not_lost() {
        let failures: Vec<_> = (0..10_001)
            .map(|n| failure(&format!("bad-{n}"), "EntryReadFailed"))
            .collect();
        let manifest = final_zip_manifest(&[], &failures, "p/");
        assert_eq!(manifest.root_error, Some("invalid_manifest"));
        assert_eq!(manifest.failed.len(), 10_001);
        assert_eq!(manifest.manifest_items().len(), 10_001);
    }
}
