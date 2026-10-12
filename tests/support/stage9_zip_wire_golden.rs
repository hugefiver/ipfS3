//! Literal bodies and header sets observed on committed dc51927, not main.
//!
//! Provenance: r25d-wire-samples-2.log (non-import) and
//! r25d-wire-import-sample-final.log in the parent-owned R25 QA root.
//! UUID fields alone use fixed UUIDs of the same width. Caller separately
//! authorized CreatedAt/UpdatedAt/CompletedAt value-only UTC normalization.
//! No XML parser, whitespace trim, sorting, serialization or update mode.
use http::{HeaderMap, HeaderValue};

use crate::support::stage9_zip_wire_requests::Wire;

const BATCH: &str = "00000000-0000-4000-8000-000000000001";
const VERSION: &str = "00000000-0000-4000-8000-000000000002";
const JOB: &str = "00000000-0000-4000-8000-000000000003";
const TIME: &str = "2000-01-01T00:00:00Z";
const ETAG: &str = "\"bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku\"";
const ROOT: &str = "bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u";

// Raw byte literals deliberately have no implicit trailing newline.
const MPU_PARTIAL: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><DecompressZipResult><ArchiveKey>archive.zip</ArchiveKey><ArchiveETag>bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku</ArchiveETag><ArchiveSize>247</ArchiveSize><ExtractedCount>1</ExtractedCount><FailedCount>1</FailedCount><Entries><Entry><Key>out/first.txt</Key><ETag>bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyk4</ETag><Size>17</Size></Entry></Entries><Failures><Failure><EntryName>second.txt</EntryName><Code>EntryUploadFailed</Code><Message>S3Error { code: InternalError, message: &quot;internal storage backend error&quot;, .. }</Message></Failure></Failures></DecompressZipResult>"#;
const IMPORT_ACCEPTED: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><IPFS3ImportAccepted><JobId>00000000-0000-4000-8000-000000000003</JobId><State>queued</State><Phase>queued</Phase></IPFS3ImportAccepted>"#;
const IMPORT_QUEUED: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><IPFS3ImportStatus><JobId>00000000-0000-4000-8000-000000000003</JobId><State>queued</State><Phase>queued</Phase><Attempt>0</Attempt><Progress><DownloadedBytes>0</DownloadedBytes><IPFSAddBytes>0</IPFSAddBytes><PinNodesProcessed>0</PinNodesProcessed><PinBytesProcessed>0</PinBytesProcessed><EntriesProcessed>0</EntriesProcessed><EntriesSucceeded>0</EntriesSucceeded><EntriesFailed>0</EntriesFailed><DecompressedBytes>0</DecompressedBytes></Progress><CreatedAt>2000-01-01T00:00:00Z</CreatedAt><UpdatedAt>2000-01-01T00:00:00Z</UpdatedAt></IPFS3ImportStatus>"#;
const IMPORT_COMPLETED: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><IPFS3ImportStatus><JobId>00000000-0000-4000-8000-000000000003</JobId><State>completed</State><Phase>publishing</Phase><Attempt>1</Attempt><Progress><DownloadedBytes>132</DownloadedBytes><DownloadTotal>132</DownloadTotal><IPFSAddBytes>132</IPFSAddBytes><PinNodesProcessed>3</PinNodesProcessed><PinBytesProcessed>132</PinBytesProcessed><LogicalSize>132</LogicalSize><EntriesProcessed>1</EntriesProcessed><EntriesSucceeded>1</EntriesSucceeded><EntriesFailed>0</EntriesFailed><DecompressedBytes>18</DecompressedBytes></Progress><Artifact><CID>bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku</CID><Size>132</Size></Artifact><ZipRoot><Status>complete</Status><CID>bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u</CID></ZipRoot><CreatedAt>2000-01-01T00:00:00Z</CreatedAt><UpdatedAt>2000-01-01T00:00:00Z</UpdatedAt><Results><Result><Key>archive.zip</Key><Status>success</Status><ETag>bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku</ETag><Size>132</Size></Result><Result><Key>out/file.txt</Key><Status>success</Status><ETag>bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyk4</ETag><Size>18</Size></Result></Results><IsTruncated>false</IsTruncated></IPFS3ImportStatus>"#;
const V2_SOURCE_FALSE: &[u8] = br#"<ZipBatchResult><BatchId>00000000-0000-4000-8000-000000000001</BatchId><SourcePublished>false</SourcePublished><InputSHA256>acf956bb25d6dca1e884dbea656e60c759e84a57fcad0c59a0c5921453cf85ad</InputSHA256><PublishedCount>1</PublishedCount><FailedCount>0</FailedCount><RootStatus>complete</RootStatus></ZipBatchResult>"#;
const V2_DIRECT_FAILED: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>InvalidRequest</Code><Message>ZIP v2 did not publish any outputs</Message></Error>"#;
const V2_MPU_FAILED: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>InvalidRequest</Code><Message>ZIP v2 multipart batch published no files; query its status with the original UploadId</Message></Error>"#;

struct Golden {
    status: u16,
    body: &'static [u8],
    headers: &'static [(&'static str, &'static str)],
}

fn golden(name: &str) -> Golden {
    match name {
        "legacy-put" => Golden {
            status: 200,
            body: b"",
            headers: &[
                ("etag", ETAG),
                ("x-amz-version-id", VERSION),
                ("x-ipfs-s3-zip-batch-id", BATCH),
                ("x-ipfs-s3-zip-root-status", "complete"),
                ("x-ipfs-s3-zip-root-cid", ROOT),
                ("content-length", "0"),
            ],
        },
        "legacy-put-warning" => Golden {
            status: 200,
            body: b"",
            headers: &[
                ("etag", ETAG),
                ("x-amz-version-id", VERSION),
                ("x-ipfs-s3-zip-batch-id", BATCH),
                ("x-ipfs-s3-zip-root-status", "failed"),
                ("x-ipfs-s3-zip-root-warning", "directory_build_failed"),
                ("content-length", "0"),
            ],
        },
        "legacy-mpu-partial" => Golden {
            status: 200,
            body: MPU_PARTIAL,
            headers: &[
                ("content-type", "application/xml"),
                ("etag", ETAG),
                ("x-amz-version-id", VERSION),
                ("x-ipfs-s3-zip-batch-id", BATCH),
                ("x-ipfs-s3-zip-root-cid", ROOT),
                ("x-ipfs-s3-zip-root-status", "partial"),
                ("content-length", "650"),
            ],
        },
        "legacy-import-accepted" => Golden {
            status: 202,
            body: IMPORT_ACCEPTED,
            headers: &[
                ("content-type", "application/xml"),
                (
                    "location",
                    "/test-bkt/archive.zip?ipfs3-import=00000000-0000-4000-8000-000000000003",
                ),
                ("x-ipfs3-import-job-id", JOB),
                ("content-length", "174"),
            ],
        },
        "legacy-import-queued" => Golden {
            status: 200,
            body: IMPORT_QUEUED,
            headers: &[
                ("x-ipfs3-pin-decision", "no_intent"),
                ("content-type", "application/xml"),
                ("content-length", "591"),
            ],
        },
        "legacy-import-completed" => Golden {
            status: 200,
            body: IMPORT_COMPLETED,
            headers: &[
                ("x-ipfs3-pin-decision", "no_intent"),
                ("content-type", "application/xml"),
                ("content-length", "1243"),
            ],
        },
        "v2-source-false" => Golden {
            status: 200,
            body: V2_SOURCE_FALSE,
            headers: &[
                ("content-type", "application/xml"),
                ("x-ipfs-s3-zip-batch-id", BATCH),
                ("x-ipfs-s3-zip-root-status", "complete"),
                ("x-ipfs-s3-zip-root-cid", ROOT),
                ("content-length", "314"),
            ],
        },
        "v2-direct-failed" => Golden {
            status: 400,
            body: V2_DIRECT_FAILED,
            headers: &[
                ("content-type", "application/xml"),
                ("content-length", "133"),
            ],
        },
        "v2-mpu-failed" => Golden {
            status: 400,
            body: V2_MPU_FAILED,
            headers: &[
                ("content-type", "application/xml"),
                ("x-ipfs-s3-zip-batch-id", BATCH),
                ("x-ipfs-s3-zip-batch-status", "failed"),
                ("x-ipfs-s3-zip-root-status", "empty"),
                ("content-length", "185"),
            ],
        },
        _ => panic!("unknown committed-baseline golden: {name}"),
    }
}

pub fn check(name: &str, wire: &Wire) {
    let expected = golden(name);
    assert_eq!(wire.status.as_u16(), expected.status, "{name}: status");
    let date = wire.headers[http::header::DATE].as_bytes();
    assert_eq!(date.len(), 29, "transport Date format, not byte equality");
    assert!(date.ends_with(b" GMT"));
    let mut protocol = wire.protocol();
    for (name, fixed) in [
        ("x-amz-version-id", VERSION),
        ("x-ipfs-s3-zip-batch-id", BATCH),
        ("x-ipfs3-import-job-id", JOB),
    ] {
        if let Some(value) = protocol.get(name) {
            assert_eq!(protocol.get_all(name).iter().count(), 1);
            uuid_value(value.to_str().unwrap(), fixed);
            protocol.insert(name, HeaderValue::from_static(fixed));
        }
    }
    if let Some(job) = wire.headers.get("x-ipfs3-import-job-id") {
        let job = job.to_str().unwrap();
        assert_eq!(wire.field("JobId"), job);
        assert_eq!(
            wire.headers[http::header::LOCATION],
            format!("/test-bkt/archive.zip?ipfs3-import={job}")
        );
        protocol.insert(
            http::header::LOCATION,
            HeaderValue::from_static(
                "/test-bkt/archive.zip?ipfs3-import=00000000-0000-4000-8000-000000000003",
            ),
        );
    }
    let mut body = std::str::from_utf8(&wire.body).unwrap().to_owned();
    for (field, fixed) in [("BatchId", BATCH), ("JobId", JOB)] {
        if body.contains(&format!("<{field}>")) {
            let original = replace_field(&mut body, field, fixed);
            uuid_value(&original, fixed);
            if field == "BatchId" {
                assert_eq!(wire.headers["x-ipfs-s3-zip-batch-id"], original);
            }
        }
    }
    if name.starts_with("legacy-import-") {
        for field in ["CreatedAt", "UpdatedAt", "CompletedAt"] {
            if body.contains(&format!("<{field}>")) {
                let original = replace_field(&mut body, field, TIME);
                let parsed = chrono::DateTime::parse_from_rfc3339(&original).unwrap();
                assert_eq!(
                    parsed.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    original
                );
            }
        }
    }
    assert_eq!(
        body.as_bytes(),
        expected.body,
        "{name}: complete body bytes"
    );
    let mut headers = HeaderMap::new();
    for &(name, value) in expected.headers {
        headers.insert(name, HeaderValue::from_static(value));
    }
    // Whole protocol map equality also proves absence of warning, ETag,
    // VersionId, batch, content-type and any unlisted response headers.
    assert_eq!(protocol, headers, "{name}: protocol values/absence");
}

fn uuid_value(value: &str, fixed: &str) {
    assert_eq!(
        value.len(),
        fixed.len(),
        "UUID width remains wire-compatible"
    );
    assert_eq!(uuid::Uuid::parse_str(value).unwrap().to_string(), value);
}

fn replace_field(body: &mut String, field: &str, fixed: &str) -> String {
    let opening = format!("<{field}>");
    let closing = format!("</{field}>");
    assert_eq!(body.matches(&opening).count(), 1, "unique named field");
    assert_eq!(body.matches(&closing).count(), 1, "unique named field");
    let start = body.find(&opening).unwrap() + opening.len();
    let end = start + body[start..].find(&closing).unwrap();
    let original = body[start..end].to_owned();
    assert_eq!(
        original.len(),
        fixed.len(),
        "only equal-width field value substitution"
    );
    body.replace_range(start..end, fixed);
    original
}
