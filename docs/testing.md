# Testing

## Local checks

Run the library and signed S3 integration tests without external services:

```powershell
cargo test --locked --lib --test integration
```

The `integration` target runs the gateway against SQLite and loopback Kubo fixtures. For focused changes, select the relevant target or filter. The HTTP admission, read snapshot, bucket ownership, and schema checks also run as separate targets:

```powershell
cargo test --locked --test http_admission --test object_read_snapshot --test bucket_config_owner_atomicity
cargo test --locked --test file_pin_schema --test schema_baseline_semantics
```

`cargo test --locked --all-targets` compiles every test target and runs non-ignored tests. Set `IPFS_S3_TEST_POSTGRES_URL` to a dedicated disposable PostgreSQL test database before running it; some non-ignored `postgres_import` cases require a live endpoint. Ignored cases for external topologies or child processes need their own entrypoints.

Run the PowerShell runner checks without starting service topologies:

```powershell
Get-ChildItem tests/*.Tests.ps1 | ForEach-Object {
    pwsh -NoProfile -File $_.FullName
    if ($LASTEXITCODE -ne 0) { throw "Failed: $($_.Name)" }
}
```

These checks cover native argument handling, process-tree termination, isolated temporary roots, owned-only cleanup, redaction, and private-swarm entrypoints. CI runs `tests/native-runner.Tests.ps1` separately on Ubuntu with PowerShell.

## CI test harness distribution

The [CI workflow](../.github/workflows/ci.yml) builds the library and binary test harnesses, `integration`, `http_admission`, `object_read_snapshot`, `bucket_config_owner_atomicity`, `file_pin_schema`, and `schema_baseline_semantics`. It packages those executables with the gateway binary that Cargo exposes through `CARGO_BIN_EXE_ipfs-s3-gateway`. This CI build omits debug symbols; assertions and panic source locations remain enabled.

The consuming jobs extract the archive at the same absolute checkout path used for compilation and run it from the repository root. This preserves executable modes and the embedded gateway and manifest paths. The runner refuses a different checkout path.

The runner lists the library tests and partitions them by module. `store::pinning::` goes to `store-pinning`, `pinning::` goes to `worker-providers`, and everything else goes to `core-remainder`. Each job runs its shard in one libtest process with exact filters, preserving process-local test locks. An empty shard fails; ignored tests keep their normal libtest behavior.

```powershell
python3 tests/ci_test_runner.Tests.py
python3 scripts/ci_test_runner.py build
python3 scripts/ci_test_runner.py run-lib-shard worker-providers
python3 scripts/ci_test_runner.py run-harnesses integration http_admission
```

The final `test` job requires formatting, Clippy, the harness build, all library shards, the non-library harnesses, and the native runner check. External PostgreSQL and deployment coverage runs separately in [Release validation](../.github/workflows/release-validation.yml).

## File pin publication and restart

The signed integration checks exercise publication, shared references, renewal, and final release. `stage3_restart` restarts an actual gateway process and completes an admitted multipart upload using the current policy.

```powershell
cargo test --locked --test integration pinning::
cargo test --locked --test integration optimistic_integrity::sqlite
cargo test --locked --test stage3_restart --test multipart_pin_decision --test cors
```

For the PostgreSQL variants, set `IPFS_S3_TEST_POSTGRES_URL` to an authorized disposable PostgreSQL 17 or 18 database. Use a fresh database for `postgres_import`, which includes tests against the default schema. The `file_pin_schema` upgrade fixture verifies that the new pin schema preserves unrelated data from a supported prior layout; `postgres_pinning_stage3` exercises transactional object, multipart, ZIP, and import publication.

```powershell
cargo test --locked --test file_pin_schema --test schema_baseline_semantics --test postgres_pinning_stage3 -- --ignored --nocapture --test-threads=1
cargo test --locked --test postgres_import -- --nocapture --test-threads=1
cargo test --locked --test integration optimistic_integrity::postgres -- --ignored --nocapture --test-threads=1
```

The warm-path tests check that S3 and worker operations use the initialized schema without repeating structural qualification. They report SQL counts without treating a fixed count as a performance requirement.

## ZIP publication and recovery

The ZIP targets cover the signed HTTP contract, archive and entry identity, versioned outputs, capture and restart recovery, and ZIP v2 request controls. Run the focused targets for the affected path:

```powershell
cargo test --locked --test 'stage9_zip_*'
cargo test --locked --test zip_stage4_http --test zip_stage4_restart --test zip_v2_http --test zip_v2_mpu_complete
cargo test --locked --test zip_v2_pinning_load signed_zip_batches_share_resources_and_provider_acks_leave_s3_unblocked -- --ignored --exact --nocapture --test-threads=1
```

For real Kubo directory checks, set `IPFS3_DIRECTORY_TEST_KUBO_URL` and `IPFS3_DIRECTORY_LARGE_TEST_KUBO_URL` to authorized disposable nodes. The large HAMT case builds 10,000 paths.

```powershell
cargo test --locked --test zip_directory real_kubo_ -- --ignored --nocapture --test-threads=1
```

### Stage 9 R25 ZIP verification

The PostgreSQL ZIP cases use `IPFS_S3_TEST_POSTGRES_URL` on an authorized disposable PostgreSQL 17 or 18 database. The isolated tail-rollback case verifies that a failed publication leaves no partial state and that retry publishes the shared CID. Use the other targets for batch storage, v2 execution, publication, lock ordering, multipart completion, and import intake:

```powershell
cargo test --locked --test stage9_zip_postgres r25_pg_tail_rollback_then_exact_shared_cid_publication -- --ignored --exact --nocapture --test-threads=1
cargo test --locked --test postgres_zip_batch_store --test zip_v2_execution --test postgres_zip_v2_publication --test postgres_zip_v2_residency_lock_order --test zip_v2_mpu_complete --test import_zip_v2_acceptance -- --ignored --test-threads=1
```

## Kubo RPC acceptance

```powershell
cargo test --locked --test rpc_provider_config --test rpc_diagnostics --test pinning_identity
cargo test --locked --test rpc_publication_cids --test rpc_stage5_http
pwsh -NoProfile -File tests/run-ipfs-rpc-real.ps1
```

The config and diagnostic tests check effective capabilities, credentials redaction, and identity and endpoint binding. `rpc_stage5_http` exercises signed S3 publication and the worker against loopback RPC fixtures, including multipart and ZIP output handles. `cargo run -- --config-explain` prints the local effective configuration without contacting providers.

The real runner selects `rpc_provider_real_transports_and_local_dags` in `ipfs_rpc_real`. It checks Kubo transports, exact DAGs, and stored bytes on a target after stopping the source. CID mode uses explicitly preseeded content. Anonymous fixture nodes can show header transport, not server authentication enforcement or paid Filebase account access. `-PreseedProbe` selects the CAR import diagnostic instead of the full acceptance case.

## Cluster acceptance

```powershell
cargo test --locked --test cluster_provider_config --test cluster_diagnostics --test cluster_provider_contract
cargo test --locked --test cluster_network_policy --test cluster_documented_config --test stage6_cluster_auth --test stage6_cluster_worker
pwsh -NoProfile -File tests/run-cluster-pinning-real.ps1
```

The real runner selects `cluster_provider_real_independent_source_car` in `cluster_provider_real`. It exercises Proxy and native REST against two Cluster peers and their allocated Kubo nodes. The source is independent and offline. The test verifies local DAGs and stored bytes on both targets after the source stops. The worker records the provider's operation acknowledgement; physical DAG and byte checks are separate fixture observations. The provider runner doesn't exercise gateway workers, signed S3 requests, account permissions, or continuous multi-host availability.

The signed worker lifecycle also has a PostgreSQL case. Set `IPFS_S3_STAGE6_TEST_DATABASE_URL` to a fresh, isolated PostgreSQL test database before selecting it:

```powershell
cargo test --locked --test stage6_cluster_worker stage6_cluster_postgres_signed_lifecycle -- --ignored --exact --nocapture --test-threads=1
```

Both real runners need PowerShell 7, Docker, cached `ipfs/kubo:v0.43.0`, and offline Cargo dependencies. The Cluster runner also needs its pinned `ipfs/ipfs-cluster:v1.1.6` image. Run from the repository root and pass `-OutputParent` pointing to an existing directory if the default `opencode` directory under the OS temp directory doesn't exist. Each runner creates fresh fixture resources on an internal Docker network, exposes only loopback endpoints, and retains its logs and summary while cleaning up only owned resources. They don't pull images or use external accounts.

## Deployment and lifecycle checks

Use a disposable authorized PostgreSQL database and the matching topology for the external tests:

```powershell
pwsh -NoProfile -File tests/run-postgres-lifecycle-validation.ps1 -PostgresUrl $env:IPFS_S3_TEST_POSTGRES_URL
pwsh -NoProfile -File tests/run-lifecycle-transition-validation.ps1
cargo test --locked --test postgres_versioning -- --nocapture --test-threads=1
cargo test --locked --test e2e -- --include-ignored --nocapture --test-threads=1
cargo test --locked --test multi_gateway -- --include-ignored --nocapture --test-threads=1
cargo test --locked --test cluster -- --include-ignored --nocapture --test-threads=1
```

To select one external case, use `--ignored --exact`. For example:

```powershell
cargo test --locked --test multi_gateway multi_gateway_cross_replica_contract -- --ignored --exact --nocapture --test-threads=1
cargo test --locked --test cluster cluster_topology_converges -- --ignored --exact --nocapture --test-threads=1
```

Check the executed test count and the runner's cleanup result. An ignored case, an empty selection, or a compile-only run doesn't establish acceptance.
