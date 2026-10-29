use crate::support;

#[tokio::test]
async fn tier_reads_signed_real_service_routes_by_immutable_residency() {
    support::tier_reads::assert_signed_tier_reads().await;
}

#[tokio::test]
async fn signed_fixed_length_gets_reject_late_kubo_error_trailers() {
    support::tier_reads::assert_signed_fixed_length_gets_wait_for_kubo_eof().await;
}
