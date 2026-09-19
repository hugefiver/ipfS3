#[path = "support/lifecycle_transition_real.rs"]
mod lifecycle_transition_real;
#[path = "support/lifecycle_transition_real_http.rs"]
mod lifecycle_transition_real_http;
#[path = "support/lifecycle_transition_real_sigv4.rs"]
mod lifecycle_transition_real_sigv4;

#[tokio::test]
#[ignore = "NOT RUN by default: requires PG17, two independent Kubo nodes, and two real gateway OS processes"]
async fn real_lifecycle_transition_current_plain_encrypted_copy_and_reporting_matrix() {
    lifecycle_transition_real::current_transition_matrix().await;
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires PG17, two independent Kubo nodes, and two real gateway OS processes"]
async fn real_lifecycle_transition_noncurrent_public_and_null_version_matrix() {
    lifecycle_transition_real::noncurrent_transition_matrix().await;
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires real signed import route plus PG17 and independent hot/cold Kubo nodes"]
async fn real_lifecycle_transition_integrity_nondefault_raw_leaf_import_to_ia() {
    lifecycle_transition_real::nondefault_raw_leaf_import_integrity().await;
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires PG17 and independent real hot/cold Kubo nodes"]
async fn real_lifecycle_transition_integrity_empty_and_multiblock_cross_range() {
    lifecycle_transition_real::empty_and_multiblock_integrity().await;
}

#[tokio::test]
#[ignore = "NOT RUN by default: requires PG17 and independent real hot/cold Kubo nodes"]
async fn real_lifecycle_transition_integrity_multipart_root_to_ia() {
    lifecycle_transition_real::multipart_root_integrity().await;
}

#[tokio::test]
#[ignore = "phase 1: prepare persistent IA and STANDARD fixtures before parent stops a backend"]
async fn real_lifecycle_transition_backend_stop_1_prepare_fixture() {
    lifecycle_transition_real::prepare_backend_stop_fixture().await;
}

#[tokio::test]
#[ignore = "phase 2: parent must stop only hot Kubo after the prepare phase"]
async fn real_lifecycle_transition_backend_stop_2_hot_down_cold_read_succeeds() {
    lifecycle_transition_real::hot_stopped_fixture_read().await;
}

#[tokio::test]
#[ignore = "phase 3: parent must restart hot Kubo and stop only cold Kubo"]
async fn real_lifecycle_transition_backend_stop_3_cold_down_fails_without_hot_fallback() {
    lifecycle_transition_real::cold_stopped_fixture_fails_closed().await;
}

#[tokio::test]
#[ignore = "phase 4: parent must restart both Kubo nodes before cleanup"]
async fn real_lifecycle_transition_backend_stop_4_cleanup_fixture() {
    lifecycle_transition_real::cleanup_backend_stop_fixture().await;
}
