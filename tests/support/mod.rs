#[allow(dead_code)]
pub mod cors;
pub mod decompress;
pub mod import;
pub mod lifecycle;
pub mod pinning;
pub mod residency;
pub mod sigv4;
mod storage_class;
pub mod tier_reads;

#[cfg(test)]
mod tests {
    #[test]
    fn presign_sigv4_query_includes_custom_query_before_signature() {
        super::sigv4::assert_presign_sigv4_query_includes_custom_query_before_signature();
    }

    #[test]
    fn canonical_uri_omits_trailing_slash_for_bucket_operations() {
        super::sigv4::assert_canonical_uri_omits_trailing_slash_for_bucket_operations();
    }
}
