#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedEntry {
    pub key: String,
    pub cid: String,
    pub size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractFailure {
    pub entry_name: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecompressZipResult {
    pub archive_key: String,
    pub archive_cid: String,
    pub archive_size: i64,
    pub entries: Vec<ExtractedEntry>,
    pub failures: Vec<ExtractFailure>,
}

fn esc(value: &str) -> String {
    quick_xml::escape::escape(value).into_owned()
}

pub fn decompress_result_xml(result: &DecompressZipResult) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.push_str("<DecompressZipResult>");
    xml.push_str(&format!(
        "<ArchiveKey>{}</ArchiveKey>",
        esc(&result.archive_key)
    ));
    xml.push_str(&format!(
        "<ArchiveETag>{}</ArchiveETag>",
        esc(&result.archive_cid)
    ));
    xml.push_str(&format!(
        "<ArchiveSize>{}</ArchiveSize>",
        result.archive_size
    ));
    xml.push_str(&format!(
        "<ExtractedCount>{}</ExtractedCount>",
        result.entries.len()
    ));
    xml.push_str(&format!(
        "<FailedCount>{}</FailedCount>",
        result.failures.len()
    ));
    xml.push_str("<Entries>");
    for entry in &result.entries {
        xml.push_str("<Entry>");
        xml.push_str(&format!("<Key>{}</Key>", esc(&entry.key)));
        xml.push_str(&format!("<ETag>{}</ETag>", esc(&entry.cid)));
        xml.push_str(&format!("<Size>{}</Size>", entry.size));
        xml.push_str("</Entry>");
    }
    xml.push_str("</Entries><Failures>");
    for failure in &result.failures {
        xml.push_str("<Failure>");
        xml.push_str(&format!(
            "<EntryName>{}</EntryName>",
            esc(&failure.entry_name)
        ));
        xml.push_str(&format!("<Code>{}</Code>", esc(&failure.code)));
        xml.push_str(&format!("<Message>{}</Message>", esc(&failure.message)));
        xml.push_str("</Failure>");
    }
    xml.push_str("</Failures></DecompressZipResult>");
    xml
}

pub fn complete_multipart_result_xml(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key><ETag>\"{}\"</ETag></CompleteMultipartUploadResult>",
        esc(bucket),
        esc(key),
        esc(etag)
    )
}

/// Distinct extension result for negotiated ZIP v2. Do not substitute this for
/// a standard S3 CompleteMultipartUpload response or a legacy ZIP result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZipV2Result {
    pub batch_id: String,
    pub source_published: bool,
    /// SHA-256 of input bytes, never an S3 object ETag.
    pub input_sha256: String,
    pub manifest_citation: String,
    /// Successfully published extracted files; source publication is separate.
    pub published_count: u64,
    pub failed_count: u64,
    pub root: ZipRootV2,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZipRootStatus {
    Disabled,
    Empty,
    Complete,
    Partial,
    Failed,
    Retryable,
}

impl ZipRootStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Empty => "empty",
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Failed => "failed",
            Self::Retryable => "retryable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZipRootV2 {
    pub status: ZipRootStatus,
    /// Must be verified and batch-published before this is populated.
    pub cid: Option<String>,
    pub successful_file_count: u64,
    pub revision: Option<String>,
    pub safe_error: Option<String>,
}

fn zip_v2_invalid() -> s3s::S3Error {
    s3s::s3_error!(InternalError, "invalid ZIP v2 result")
}

fn safe_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"_-".contains(&b))
}

fn v2_xml_text(value: &str) -> String {
    // XML 1.0 cannot represent NUL and most control chars even after escaping.
    let safe: String = value
        .chars()
        .map(|ch| match ch {
            '\t' | '\n' | '\r' => ch,
            '\u{0}'..='\u{1f}' | '\u{fffe}' | '\u{ffff}' => '\u{fffd}',
            _ => ch,
        })
        .collect();
    esc(&safe)
}

fn push_v2_field(xml: &mut String, name: &str, value: &str) {
    xml.push('<');
    xml.push_str(name);
    xml.push('>');
    xml.push_str(&v2_xml_text(value));
    xml.push_str("</");
    xml.push_str(name);
    xml.push('>');
}

/// Validate before serializing: an unverified, failed or empty root must never
/// gain a CID just because a caller supplied one.
pub fn zip_v2_result_xml(result: &ZipV2Result) -> s3s::S3Result<String> {
    let has_root = matches!(
        result.root.status,
        ZipRootStatus::Complete | ZipRootStatus::Partial
    );
    if result.batch_id.is_empty()
        || result.manifest_citation.is_empty()
        || result.input_sha256.len() != 64
        || !result
            .input_sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || has_root != result.root.cid.as_ref().is_some_and(|cid| !cid.is_empty())
        || (!has_root && result.root.cid.is_some())
        || (has_root && result.root.successful_file_count == 0)
        || (result.root.status == ZipRootStatus::Empty && result.root.successful_file_count != 0)
        || result
            .root
            .safe_error
            .as_deref()
            .is_some_and(|code| !safe_code(code))
    {
        return Err(zip_v2_invalid());
    }
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ZipBatchResult>");
    push_v2_field(&mut xml, "BatchId", &result.batch_id);
    push_v2_field(
        &mut xml,
        "SourcePublished",
        if result.source_published {
            "true"
        } else {
            "false"
        },
    );
    push_v2_field(&mut xml, "InputSHA256", &result.input_sha256);
    push_v2_field(&mut xml, "ManifestCitation", &result.manifest_citation);
    push_v2_field(
        &mut xml,
        "PublishedCount",
        &result.published_count.to_string(),
    );
    push_v2_field(&mut xml, "FailedCount", &result.failed_count.to_string());
    xml.push_str("<Root>");
    push_v2_field(&mut xml, "Status", result.root.status.as_str());
    push_v2_field(
        &mut xml,
        "SuccessfulFileCount",
        &result.root.successful_file_count.to_string(),
    );
    if let Some(cid) = &result.root.cid {
        push_v2_field(&mut xml, "CID", cid);
    }
    if let Some(revision) = &result.root.revision {
        push_v2_field(&mut xml, "Revision", revision);
    }
    if let Some(error) = &result.root.safe_error {
        push_v2_field(&mut xml, "SafeError", error);
    }
    xml.push_str("</Root><Warnings>");
    for warning in &result.warnings {
        push_v2_field(&mut xml, "Warning", warning);
    }
    xml.push_str("</Warnings></ZipBatchResult>");
    Ok(xml)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xml_escapes_result_fields_and_counts_entries() {
        let xml = decompress_result_xml(&DecompressZipResult {
            archive_key: "archive&.zip".to_string(),
            archive_cid: "QmArchive".to_string(),
            archive_size: 12,
            entries: vec![ExtractedEntry {
                key: "prefix/a<&>.txt".to_string(),
                cid: "QmEntry".to_string(),
                size: 5,
            }],
            failures: vec![ExtractFailure {
                entry_name: "bad&name".to_string(),
                code: "KuboAddFailed".to_string(),
                message: "pin <failed>".to_string(),
            }],
        });

        assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(xml.contains("<DecompressZipResult>"));
        assert!(xml.contains("<ArchiveKey>archive&amp;.zip</ArchiveKey>"));
        assert!(xml.contains("<ExtractedCount>1</ExtractedCount>"));
        assert!(xml.contains("<FailedCount>1</FailedCount>"));
        assert!(xml.contains("prefix/a&lt;&amp;&gt;.txt"));
        assert!(xml.contains("pin &lt;failed&gt;"));
    }

    #[test]
    fn complete_xml_matches_s3_shape() {
        let xml = complete_multipart_result_xml("bucket", "archive.zip", "QmRoot");

        assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
        assert!(xml.contains("<CompleteMultipartUploadResult>"));
        assert!(xml.contains("<Bucket>bucket</Bucket>"));
        assert!(xml.contains("<Key>archive.zip</Key>"));
        assert!(xml.contains("<ETag>\"QmRoot\"</ETag>"));
    }

    #[test]
    fn v2_batch_xml_is_distinct_and_has_no_fictitious_object_identity() {
        let result = ZipV2Result {
            batch_id: "batch<&".into(),
            source_published: false,
            input_sha256: "a".repeat(64),
            manifest_citation: "manifest?x=1&y=2".into(),
            published_count: 1,
            failed_count: 0,
            root: ZipRootV2 {
                status: ZipRootStatus::Complete,
                cid: Some("bafyroot".into()),
                successful_file_count: 1,
                revision: Some("v<&".into()),
                safe_error: None,
            },
            warnings: vec!["warning <unsafe>\u{0}text".into()],
        };
        let xml = zip_v2_result_xml(&result).unwrap();
        assert!(xml.contains("<ZipBatchResult>"));
        assert!(xml.contains("<BatchId>batch&lt;&amp;</BatchId>"));
        assert!(xml.contains("<ManifestCitation>manifest?x=1&amp;y=2</ManifestCitation>"));
        assert!(xml.contains("<SourcePublished>false</SourcePublished>"));
        assert!(xml.contains("<InputSHA256>"));
        assert!(xml.contains("<CID>bafyroot</CID>"));
        assert!(xml.contains("warning &lt;unsafe&gt;�text"));
        assert!(!xml.contains("<ArchiveETag>"));
        assert!(!xml.contains("<VersionId>"));
        assert!(!xml.contains("<CompleteMultipartUploadResult>"));
    }

    #[test]
    fn v2_source_only_is_disabled_and_failure_cannot_leak_unverified_root_cid() {
        let mut result = ZipV2Result {
            batch_id: "batch".into(),
            source_published: true,
            input_sha256: "0".repeat(64),
            manifest_citation: "manifest".into(),
            published_count: 0,
            failed_count: 0,
            root: ZipRootV2 {
                status: ZipRootStatus::Disabled,
                cid: None,
                successful_file_count: 0,
                revision: None,
                safe_error: None,
            },
            warnings: vec![],
        };
        let xml = zip_v2_result_xml(&result).unwrap();
        assert!(xml.contains("<Status>disabled</Status>"));
        assert!(!xml.contains("<CID>"));
        result.root.status = ZipRootStatus::Failed;
        result.root.cid = Some("unverified".into());
        assert!(zip_v2_result_xml(&result).is_err());
        result.root.status = ZipRootStatus::Complete;
        result.root.cid = None;
        assert!(zip_v2_result_xml(&result).is_err());
        result.root.status = ZipRootStatus::Failed;
        result.root.cid = Some(String::new());
        assert!(zip_v2_result_xml(&result).is_err());
        result.root.cid = None;
        result.root.safe_error = Some("provider secret: do not leak".into());
        assert!(zip_v2_result_xml(&result).is_err());
        result.root.safe_error = Some("path_conflict".into());
        assert!(
            zip_v2_result_xml(&result)
                .unwrap()
                .contains("<SafeError>path_conflict</SafeError>")
        );
    }
}
