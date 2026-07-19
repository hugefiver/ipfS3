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
}
