use crate::store::entities::{import_job, import_job_result};

const XML_DECLARATION: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

pub struct StatusResultPage<'a> {
    pub rows: &'a [import_job_result::Model],
    pub next_continuation_token: Option<&'a str>,
}

fn esc(value: &str) -> String {
    quick_xml::escape::escape(value).into_owned()
}

fn element(xml: &mut String, name: &str, value: &str) {
    xml.push('<');
    xml.push_str(name);
    xml.push('>');
    xml.push_str(&esc(value));
    xml.push_str("</");
    xml.push_str(name);
    xml.push('>');
}

fn numeric_element(xml: &mut String, name: &str, value: i64) {
    element(xml, name, &value.to_string());
}

pub fn accepted_xml(job_id: &str, state: &str, phase: &str) -> String {
    let mut xml = String::from(XML_DECLARATION);
    xml.push_str("<IPFS3ImportAccepted>");
    element(&mut xml, "JobId", job_id);
    element(&mut xml, "State", state);
    element(&mut xml, "Phase", phase);
    xml.push_str("</IPFS3ImportAccepted>");
    xml
}

pub fn status_xml(job: &import_job::Model, page: Option<StatusResultPage<'_>>) -> String {
    let mut xml = String::from(XML_DECLARATION);
    xml.push_str("<IPFS3ImportStatus>");
    element(&mut xml, "JobId", &job.id);
    element(&mut xml, "State", &job.state);
    element(&mut xml, "Phase", &job.phase);
    numeric_element(&mut xml, "Attempt", i64::from(job.attempts));

    xml.push_str("<Progress>");
    match job.source_type.as_str() {
        "cid" => {
            numeric_element(&mut xml, "ProvidersObserved", job.providers_observed);
            numeric_element(&mut xml, "PinNodesProcessed", job.pin_nodes_processed);
            numeric_element(&mut xml, "PinBytesProcessed", job.pin_bytes_processed);
        }
        "url" => {
            numeric_element(&mut xml, "DownloadedBytes", job.downloaded_bytes);
            if let Some(total) = job.download_total {
                numeric_element(&mut xml, "DownloadTotal", total);
            }
            numeric_element(&mut xml, "IPFSAddBytes", job.ipfs_add_bytes);
            numeric_element(&mut xml, "PinNodesProcessed", job.pin_nodes_processed);
            numeric_element(&mut xml, "PinBytesProcessed", job.pin_bytes_processed);
        }
        _ => {}
    }
    if let Some(size) = job.logical_size {
        numeric_element(&mut xml, "LogicalSize", size);
    }
    if job.decompress_prefix.is_some() {
        numeric_element(&mut xml, "EntriesProcessed", job.entries_processed);
        numeric_element(&mut xml, "EntriesSucceeded", job.entries_succeeded);
        numeric_element(&mut xml, "EntriesFailed", job.entries_failed);
        numeric_element(&mut xml, "DecompressedBytes", job.decompressed_bytes);
    }
    xml.push_str("</Progress>");

    if let Some(cid) = job.final_cid.as_deref() {
        xml.push_str("<Artifact>");
        element(&mut xml, "CID", cid);
        if let Some(size) = job.logical_size {
            numeric_element(&mut xml, "Size", size);
        }
        xml.push_str("</Artifact>");
    }
    if let Some(code) = job.failure_code.as_deref() {
        xml.push_str("<Failure>");
        element(&mut xml, "Code", code);
        if let Some(message) = job.failure_message.as_deref() {
            element(&mut xml, "Message", message);
        }
        xml.push_str("</Failure>");
    }

    element(
        &mut xml,
        "CreatedAt",
        &job.created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    );
    element(
        &mut xml,
        "UpdatedAt",
        &job.updated_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    );

    if let Some(page) = page {
        xml.push_str("<Results>");
        for row in page.rows {
            result_xml(&mut xml, row);
        }
        xml.push_str("</Results>");
        element(
            &mut xml,
            "IsTruncated",
            if page.next_continuation_token.is_some() {
                "true"
            } else {
                "false"
            },
        );
        if let Some(token) = page.next_continuation_token {
            element(&mut xml, "NextContinuationToken", token);
        }
    }

    xml.push_str("</IPFS3ImportStatus>");
    xml
}

fn result_xml(xml: &mut String, row: &import_job_result::Model) {
    xml.push_str("<Result>");
    element(xml, "Key", &row.key);
    if let Some(code) = row.error_code.as_deref() {
        element(xml, "Status", "failure");
        element(xml, "ErrorCode", code);
        if let Some(message) = row.error_message.as_deref() {
            element(xml, "ErrorMessage", message);
        }
    } else {
        element(xml, "Status", "success");
        if let Some(cid) = row.cid.as_deref() {
            element(xml, "ETag", cid);
        }
        if let Some(size) = row.size {
            numeric_element(xml, "Size", size);
        }
    }
    xml.push_str("</Result>");
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;

    fn job(source_type: &str) -> import_job::Model {
        let now = Utc.with_ymd_and_hms(2026, 7, 29, 0, 0, 0).unwrap();
        import_job::Model {
            id: "job<&".to_owned(),
            bucket: "bucket".to_owned(),
            key: "key".to_owned(),
            source_type: source_type.to_owned(),
            source_value: "https://secret.example/token?credential=never-render".to_owned(),
            request_fingerprint: "fingerprint".to_owned(),
            client_token: None,
            object_content_type: None,
            metadata_json: "{}".to_owned(),
            tags_json: "[]".to_owned(),
            decompress_prefix: None,
            state: "running".to_owned(),
            phase: "pinning_local".to_owned(),
            attempts: 2,
            next_attempt_at: now,
            locked_by: None,
            locked_until: None,
            claim_epoch: 1,
            providers_observed: 3,
            pin_nodes_processed: 4,
            pin_bytes_processed: 5,
            downloaded_bytes: 6,
            download_total: Some(7),
            ipfs_add_bytes: 8,
            logical_size: Some(9),
            entries_processed: 10,
            entries_succeeded: 8,
            entries_failed: 2,
            decompressed_bytes: 11,
            final_cid: Some("bafy<&".to_owned()),
            failure_code: None,
            failure_message: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
        }
    }

    #[test]
    fn accepted_xml_escapes_fields_and_reports_persisted_state() {
        let xml = accepted_xml("job<&", "queued", "queued");
        assert!(xml.contains("<JobId>job&lt;&amp;</JobId>"));
        assert!(xml.contains("<State>queued</State>"));
        assert!(xml.contains("<Phase>queued</Phase>"));
    }

    #[test]
    fn cid_status_reports_truthful_counters_without_url_replica_or_percentage_claims() {
        let xml = status_xml(&job("cid"), None);

        assert!(xml.contains("<ProvidersObserved>3</ProvidersObserved>"));
        assert!(xml.contains("<PinNodesProcessed>4</PinNodesProcessed>"));
        assert!(xml.contains("<PinBytesProcessed>5</PinBytesProcessed>"));
        assert!(!xml.contains("DownloadedBytes"));
        assert!(!xml.contains("secret.example"));
        assert!(!xml.to_ascii_lowercase().contains("replica"));
        assert!(!xml.to_ascii_lowercase().contains("percent"));
        assert!(xml.contains("<CID>bafy&lt;&amp;</CID>"));
    }

    #[test]
    fn url_and_decompression_status_reports_known_totals_and_escaped_result_outcomes() {
        let mut job = job("url");
        job.decompress_prefix = Some("prefix/".to_owned());
        let rows = vec![
            import_job_result::Model {
                job_id: job.id.clone(),
                sequence: 0,
                key: "prefix/good<&.txt".to_owned(),
                cid: Some("bafy-good".to_owned()),
                size: Some(12),
                error_code: None,
                error_message: None,
            },
            import_job_result::Model {
                job_id: job.id.clone(),
                sequence: 1,
                key: "prefix/bad.txt".to_owned(),
                cid: None,
                size: None,
                error_code: Some("entry_failed<&".to_owned()),
                error_message: Some("safe <message>".to_owned()),
            },
        ];
        let xml = status_xml(
            &job,
            Some(StatusResultPage {
                rows: &rows,
                next_continuation_token: Some("token<&"),
            }),
        );

        assert!(xml.contains("<DownloadedBytes>6</DownloadedBytes>"));
        assert!(xml.contains("<DownloadTotal>7</DownloadTotal>"));
        assert!(xml.contains("<IPFSAddBytes>8</IPFSAddBytes>"));
        assert!(xml.contains("<EntriesSucceeded>8</EntriesSucceeded>"));
        assert!(xml.contains("<Status>success</Status>"));
        assert!(xml.contains("<Status>failure</Status>"));
        assert!(xml.contains("prefix/good&lt;&amp;.txt"));
        assert!(xml.contains("entry_failed&lt;&amp;"));
        assert!(xml.contains("safe &lt;message&gt;"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextContinuationToken>token&lt;&amp;</NextContinuationToken>"));
        assert!(!xml.contains("secret.example"));
    }
}
