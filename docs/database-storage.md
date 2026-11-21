# Database storage and upgrades

The gateway keeps S3 metadata and durable work in SQLite or PostgreSQL. Object bytes live in Kubo. Use a persistent SQLite URL for data you want to keep (for example, `sqlite:///data/ipfs-s3.db?mode=rwc` in Docker), or provision a PostgreSQL database and schema before starting the gateway.

## Initialize an empty database

Normal startup installs the final schema directly and records `m20261122_000001_bucket_quota` in `seaql_migrations`. It doesn't run the historical migration chain. On a current database startup reads the latest ledger marker; it doesn't rerun DDL or audit the catalog. It refuses nonempty databases with an older or missing marker. PostgreSQL serializes concurrent empty-schema initialization using a transaction-scoped advisory lock.

## Upgrade an existing database

`--upgrade-database` adds [bucket quotas](bucket-quota.md) and initializes logical usage from all addressable object versions. Existing buckets start without a limit. From `m20261120_000001_file_pins`, the upgrade also adds the nullable bucket ACL column. It preserves pin resources and references, object data and in-flight uploads; no remote pins need to be released. Existing buckets keep their signed-access behavior until the recorded owner sets a [bucket default](local-policies.md#bucket-defaults).

For databases **older than file-pins**, the old ledger must be an exact ordered prefix of the list below, with at least the first two entries. A matching last entry alone doesn't make an old ledger eligible.

- `m20261027_000001_schema_baseline`
- `m20261108_000001_current_pinning_config`
- `m20261109_000001_pin_cid_comparisons`
- `m20261110_000001_requeue_retained`

For earlier layouts, first use the previous gateway's documented upgrade path to reach a supported layout. Before **any** upgrade, drain and stop every gateway and worker writing to this database and take a backup. The quota migration counts retained object versions once; older writers do not maintain that count. For a pre-file-pins upgrade, also release the old remote pins on their original backup providers. Keep the backup until you've verified you can restore it. If the old Cluster configuration uses `min_replicas` or `verifier_limits`, remove those options before starting the new gateway; the current adapter records service acknowledgments.

Run the new binary with the deployment's configuration:

```powershell
$env:IPFS_S3_CONFIG = "C:\gateway\config.toml"
ipfs-s3-gateway --upgrade-database
```

Check `storage.database_url` and any `IPFS_S3_DATABASE_URL` override before running the command. It opens that database and runs the schema change in a transaction without starting HTTP, workers, or Kubo or provider operations. If the connection is lost during commit, establish the database outcome before proceeding. Start only upgraded gateway and worker binaries after the upgrade succeeds. There's no down migration.

The upgrade from a pre-file-pins schema removes the old pin tables and management triggers and creates empty `pin_refs` and `pin_resources` tables. It retains buckets, object versions, ordinary tags, multipart uploads, ZIP and import records, lifecycle configuration, and residency. It doesn't recover remote handles, replay old pin requests, or rebuild references from S3 tags. For any file that still needs remote pinning, [submit a fresh signed request under the current policy](../README.md#standard-s3-tag-control).

## Durable import requests

`import_jobs` holds scheduling, claim epochs, progress, destination identity, idempotency keys, and terminal state. Its one-to-one `import_job_requests` row holds the frozen source value, content type, metadata, tags, and pin or ZIP inputs. Admission commits both together, and deleting a job cascades to its request. The request keeps SQL text and nullable cells. ZIP capture carries the frozen options used at execution instead of applying live defaults to an already admitted job.

The worker first selects bounded candidate pages and loads complete requests only for jobs it claims. It reads large request values in bounded byte slices and decodes UTF-8 after reassembly. Renewal, progress, and ownership checks don't load or rewrite full request bodies. Status queries project only the state and capture fields needed for the response. See the [import request and status contract](../README.md#durable-ipfs3-import).
