//! Opt-in, bounded multi-process measurements using isolated SQLite and loopback services.
//! Measures test-executable workers, not production deployment or external accounts.
//!
//! Run from the repository root with cached dependencies:
//! $env:IPFS_S3_PINNING_BENCHMARK = 'offline-sqlite'
//! $env:IPFS_S3_PINNING_BENCHMARK_SECONDS = '60'
//! cargo test --locked --offline --test pinning_multi_process_benchmark pinning_multi_process_offline_benchmark -- --ignored --exact --nocapture --test-threads=1
//!
//! The second ignored test is an internal OS-child entrypoint. It fails closed
//! without a parent-created run directory and is never selected by the command above.
//!
//! Optional narrow diagnosis, separate from the full 65-output scenario:
//! $env:IPFS_S3_PINNING_BENCHMARK_SECONDS = '25'
//! $env:IPFS_S3_PINNING_BENCHMARK_DIAGNOSTIC = 'duplicate-cids'
//! cargo test --locked --offline --test pinning_multi_process_benchmark pinning_multi_process_offline_diagnostic -- --ignored --exact --nocapture --test-threads=1
//! Keeps two OS workers, one ledger/account, duplicate ZIP contents, actual 429,
//! ordinary PUTs during dispatch, and historical PSA Find recovery. Safe bounded
//! worker traces and changing job/lock/epoch snapshots are retained in its report.
//! The separate unique-cids diagnostic removes duplicate contents as a control;
//! neither diagnostic changes the original benchmark's completion/ownership gates.
//!
//! Additional entrypoint: pinning_multi_process_offline_comparison. Set MODE
//! (IPFS_S3_PINNING_BENCHMARK_MODE) to single-process or dual-provider-65 with
//! SECONDS=60, or dual-provider-10k with SECONDS=120. The baseline keeps the
//! formal 65 outputs/33 CIDs/39 allocations/71 targets, W=3, RPS=4 and provider
//! acceptance cap. It does not require two-process oversubscription or a 429.
//! Mixed cases use two loopback PSA simulated accounts, all-mode replicas,
//! ordinary signed PUTs, uncertain-call Find recovery, and logical cancellation
//! whose unknown ownership must retain remote content without provider DELETE.
//! 10k uses 5k distinct contents and a fixed 120s publication+observation budget;
//! it reports backlog and throughput, not a full-completion or 60s claim.
//! Diagnostic values equal-duplicate-cids and equal-unique-cids both publish
//! 65 entries at SECONDS=25, with 33 vs 65 distinct CIDs; these are NOT formal runs.
//! This harness is not production main.rs, remote-account validation, HA, or a
//! cross-process global-budget implementation.

#[path = "support/pinning_multi_process_benchmark.rs"]
mod benchmark;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit offline-sqlite opt-in and bounded observation seconds"]
async fn pinning_multi_process_offline_benchmark() {
    let outcome = benchmark::parent()
        .await
        .expect("offline benchmark setup failed; raw details suppressed");
    println!("STAGE8_PINNING_BENCHMARK {}", outcome.report);
    if let Some(reason) = outcome.failure {
        panic!("offline benchmark incomplete: {reason}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "internal test-executable child; requires the parent's private fixture files"]
async fn pinning_multi_process_benchmark_child() {
    benchmark::child()
        .await
        .expect("offline child failed; raw details suppressed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "bounded 25s runtime diagnosis; requires a separate explicit diagnostic opt-in"]
async fn pinning_multi_process_offline_diagnostic() {
    let outcome = benchmark::diagnostic()
        .await
        .expect("offline diagnostic setup failed; raw details suppressed");
    println!(
        "STAGE8_PINNING_DIAGNOSTIC {}",
        serde_json::to_string_pretty(&outcome.report).unwrap()
    );
    if let Some(reason) = outcome.failure {
        panic!("offline diagnostic incomplete: {reason}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "fixed-budget offline comparison; requires explicit mode and observation budget"]
async fn pinning_multi_process_offline_comparison() {
    let outcome = benchmark::comparison()
        .await
        .expect("offline comparison setup failed; raw details suppressed");
    println!(
        "STAGE8_PINNING_COMPARISON {}",
        serde_json::to_string_pretty(&outcome.report).unwrap()
    );
    if let Some(reason) = outcome.failure {
        panic!("offline comparison incomplete: {reason}");
    }
}

#[test]
fn pinning_benchmark_baseline_preserves_workload_and_provider_limits() {
    let formal = benchmark::Case::formal();
    let baseline = benchmark::Case::named("single-process").unwrap();
    assert_eq!(baseline.processes, 1);
    assert_eq!(
        (
            baseline.outputs,
            baseline.distinct,
            baseline.providers,
            baseline.seconds
        ),
        (
            formal.outputs,
            formal.distinct,
            formal.providers,
            formal.seconds
        )
    );
    assert_eq!(baseline.expected_allocations(), 39);
}

#[test]
fn pinning_benchmark_equal_scale_diagnostics_keep_all_65_inputs() {
    let duplicate = benchmark::Case::named("equal-duplicate-cids").unwrap();
    let unique = benchmark::Case::named("equal-unique-cids").unwrap();
    assert_eq!((duplicate.outputs, unique.outputs), (65, 65));
    assert_eq!((duplicate.distinct, unique.distinct), (33, 65));
    assert_eq!((duplicate.seconds, unique.seconds), (25, 25));
    assert!(duplicate.diagnostic && unique.diagnostic);
}

#[test]
fn pinning_benchmark_large_case_is_fixed_and_bounded() {
    let case = benchmark::Case::named("dual-provider-10k").unwrap();
    assert_eq!(
        (
            case.outputs,
            case.distinct,
            case.providers,
            case.processes,
            case.seconds
        ),
        (10_000, 5_000, 2, 2, 120)
    );
    assert_eq!(case.expected_allocations(), 10_014);
    assert!(benchmark::Case::named("unbounded").is_err());
}

#[test]
fn pinning_benchmark_safe_correlations_accept_generated_ids_not_raw_errors() {
    assert!(benchmark::metrics::safe_identity(
        "submit:offline-account:bafk-test:target:g1"
    ));
    for value in [
        "",
        "Bearer secret token",
        "https://account.invalid",
        "SELECT * FROM pin_jobs",
        "line\nerror",
    ] {
        assert!(!benchmark::metrics::safe_identity(value));
    }
}

#[test]
fn pinning_benchmark_archives_preserve_exact_entry_and_distinct_content_counts() {
    for (outputs, distinct) in [(65, 33), (65, 65), (10_000, 5_000)] {
        let bytes = benchmark::http::archive_with_counts(outputs, distinct);
        assert!(bytes.len() <= 2 * 1024 * 1024);
        let word = |offset: usize| {
            u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize
        };
        let dword = |offset: usize| {
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize
        };
        let mut offset = 0;
        let mut count = 0;
        let mut cids = std::collections::BTreeSet::new();
        while dword(offset) == 0x04034b50 {
            let size = dword(offset + 18);
            let data = offset + 30 + word(offset + 26) + word(offset + 28);
            cids.insert(benchmark::http::cid(&bytes[data..data + size]));
            count += 1;
            offset = data + size;
        }
        assert_eq!(count, outputs);
        assert_eq!(cids.len(), distinct);
    }
}

fn psa_ledger_contract_fixture() -> (
    ipfs_s3_gateway::store::entities::remote_pin_ledger::Model,
    ipfs_s3_gateway::pinning::identity::ProviderRouteSnapshot,
) {
    use ipfs_s3_gateway::{
        pinning::identity::{CleanupMode, ProviderRouteSnapshot},
        store::entities::remote_pin_ledger,
    };
    let registered = ProviderRouteSnapshot {
        confirmation: None,
        provider_id: "offline-account".into(),
        backend: "filebase".into(),
        scope: "offline-contract".into(),
        storage_domain: "offline-shared-remote".into(),
        credential_revision: 1,
        endpoint_revision: 1,
        secret_ref: None,
        api_profile: "filebase-psa".into(),
        strategy: "cid".into(),
        cleanup: CleanupMode::Managed,
    };
    let row = remote_pin_ledger::Model {
        provider: "offline-account".into(),
        cid: "offline-contract-content".into(),
        route: Some(serde_json::to_string(&registered).unwrap()),
        ownership: "unknown".into(),
        effect: "confirmed".into(),
        first_observed_at: None,
        last_observed_at: None,
        remote_pinned_at: None,
        gateway_verified_at: None,
        content_verified_at: None,
        first_error: None,
        last_error: None,
    };
    (row, registered)
}

#[test]
fn pinning_psa_ledger_contract_accepts_unknown_with_exact_route_and_confirmed_effect() {
    let (row, registered) = psa_ledger_contract_fixture();
    assert!(benchmark::metrics::psa_ledger_row_matches_contract(
        &row,
        &registered
    ));
}

#[test]
fn pinning_psa_ledger_contract_rejects_application_created_and_other_ownership() {
    let (mut row, registered) = psa_ledger_contract_fixture();
    for ownership in ["application_created", "external_existing", "Unknown", ""] {
        row.ownership = ownership.into();
        assert!(
            !benchmark::metrics::psa_ledger_row_matches_contract(&row, &registered),
            "unexpected ownership accepted: {ownership}"
        );
    }
}

#[test]
fn pinning_psa_ledger_contract_rejects_route_and_effect_tampering() {
    let (row, registered) = psa_ledger_contract_fixture();
    assert!(benchmark::metrics::psa_ledger_row_matches_contract(
        &row,
        &registered
    ));
    let mut changed = row.clone();
    let mut changed_route = registered.clone();
    changed_route.credential_revision += 1;
    changed.route = Some(serde_json::to_string(&changed_route).unwrap());
    assert!(!benchmark::metrics::psa_ledger_row_matches_contract(
        &changed,
        &registered
    ));
    for route in [None, Some("invalid-json".into())] {
        changed.route = route;
        assert!(!benchmark::metrics::psa_ledger_row_matches_contract(
            &changed,
            &registered
        ));
    }
    changed = row;
    for effect in ["unknown", "reserved", "retained", "cleanup_pending", ""] {
        changed.effect = effect.into();
        assert!(
            !benchmark::metrics::psa_ledger_row_matches_contract(&changed, &registered),
            "unexpected effect accepted: {effect}"
        );
    }
}
