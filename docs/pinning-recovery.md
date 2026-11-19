# Remote pin operations

## Renew or release a file

Use signed S3 requests against the bucket, key and, for a versioned file, its
`versionId`. `GetObjectTagging` returns the stored tags. `PutObjectTagging`
replaces them: preserve application tags when renewing a manual pin with
`ipfs-s3:pin=true` and an RFC 3339 `ipfs-s3:retain-until` timestamp. To cancel
manual demand, use `ipfs-s3:pin=false` without duration or retention controls.
`DeleteObjectTagging` clears **all** tags and cancels manual demand. See the
[CLI examples](../README.md#standard-s3-tag-control).

A successful tagging request commits the local change. The worker releases a
remote pin only after the last reference to its provider and CID ends. Other
files or versions may therefore keep it alive. A removal acknowledgment ends
the gateway's cleanup work; the provider may still retain readable blocks. The
gateway does not run remote garbage collection or delete the primary object.

## A provider did not acknowledge the request

Send a signed `GET /BUCKET/KEY?ipfs3-pin-status` (add `&versionId=VERSION` for
a particular version). A reference's `status` is `waiting`, `pending`, `accepted`,
`failed` or `unknown`; `accepted` records a provider acknowledgment. Only active
references are listed. After cancellation or expiry the response cannot tell
you whether blocks are still on a remote node.

Check the configured backup account and its credentials. `--config-explain`
can diagnose local configuration without reaching the account or changing
the database. When an upload response is lost, the provider may already have
created the pin. Inspect that provider before issuing a new upload. Use its
exact request or file handle where available; on a CID-based service a removal
affects that CID on the entire backup node.

## Upgrade from the old pin database layout

Stop accepting writes, drain every gateway and worker using the database, and
take a database backup. Before upgrading, release the old remote pins on their
original providers. The new schema starts with no pin references and cannot
use old records to recover a remote handle. It does not call providers during
upgrade, import old intent, or rebuild it by scanning S3 tags.

The upgrade accepts previous layouts ending at `m20261108_000001_current_pinning_config`,
`m20261109_000001_pin_cid_comparisons` or `m20261110_000001_requeue_retained`.
For an older layout, use the previous gateway's upgrade procedure first to
update the shared ZIP and import schema. Normal startup refuses an old layout.

If the old Cluster provider configuration sets `min_replicas` or
`verifier_limits`, remove those options: provider completion now uses the
service acknowledgment. With the new binary, select the deployment's
configuration and run:

```powershell
$env:IPFS_S3_CONFIG = "C:\gateway\config.toml"
ipfs-s3-gateway --upgrade-database
```

The command uses `storage.database_url` from this config, overridden by
`IPFS_S3_DATABASE_URL` if set. Check that it points to the backed-up database.
The command exits after the database transaction; it does not start the HTTP
server, workers, Kubo or provider operations.

The schema change replaces the old pin tables and management triggers. Bucket
and object versions, ordinary tags, multipart uploads, ZIP and import records,
lifecycle configuration and residency are retained. Start only upgraded
binaries against the new schema. For a file that still needs remote pinning,
submit a fresh signed request under the current policy.
