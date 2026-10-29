#[path = "support/optimistic_integrity.rs"]
mod optimistic_integrity;
mod support;

// optimistic_integrity imports its shared fixtures from the integration root.
use chrono::Duration as ChronoDuration;
use common::{
    FIRST_ENTRY_BYTES, PIN_CID_ENTRY1, PIN_CID_ENTRY2, PIN_CID_PART, PIN_CID_ROOT,
    SECOND_ENTRY_BYTES, assert_put_cid_headers, latest_pinning_object, owner_leases,
    signed_complete_multipart, signed_copy_with_tagging,
    signed_create_multipart_zip_upload_with_tagging, signed_delete_object,
    signed_delete_object_tagging, signed_get, signed_put_object_tagging, signed_put_with_tagging,
    signed_upload_part, xml_text,
};
use http::StatusCode;
use ipfs_s3_gateway::store;
use support::decompress::{AddReply, legal_two_entry_zip};
use support::pinning::{PinningHarnessConfig, PsaReply, TEST_CID};

#[path = "integration/basics.rs"]
mod basics;

#[path = "integration/common.rs"]
mod common;

#[path = "integration/encryption.rs"]
mod encryption;

#[path = "integration/imports.rs"]
mod imports;

#[path = "integration/lifecycle_expiration.rs"]
mod lifecycle_expiration;

#[path = "integration/lifecycle_multipart.rs"]
mod lifecycle_multipart;

#[path = "integration/pinning.rs"]
mod pinning;

#[path = "integration/sigv4.rs"]
mod sigv4;

#[path = "integration/tier_reads.rs"]
mod tier_reads;

#[path = "integration/versioning.rs"]
mod versioning;

#[path = "integration/zip.rs"]
mod zip;
