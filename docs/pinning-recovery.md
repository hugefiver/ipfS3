# Remote pin operations

## Renew or release a file

Use signed S3 requests against the bucket, key and, for a versioned file, its
`versionId`. `GetObjectTagging` returns the stored tags. `PutObjectTagging`
replaces them: preserve application tags when renewing a manual pin with
`ipfs-s3:pin=true` and an RFC 3339 `ipfs-s3:retain-until` timestamp. To cancel
manual demand, use `ipfs-s3:pin=false` without duration or retention controls.
`DeleteObjectTagging` clears **all** tags and cancels manual demand. See the
[CLI examples](../README.md#standard-s3-tag-control).

For ZIPs published with `content=decompressed`, use the extracted file's key
and version for pin status, renewal or cancellation. Renewal is subject to the
current policy for that key. Existing references created under the archive key
by an older release keep that owner and expiry; the gateway does not move or
resubmit them on startup.

A successful tagging request commits the local change. The worker releases a
remote pin only after the last reference to its provider and CID ends. Other
files or versions may therefore keep it alive. A removal acknowledgment ends
the gateway's cleanup work; the provider may still retain readable blocks. The
gateway does not run remote garbage collection or delete the primary object.

## A new PUT is slow to receive a pin acknowledgment

For a new CID, enable `RUST_LOG=info,ipfs_s3_gateway::pinning=debug` on the gateway handling the PUT and on any worker that can claim its provider. Match the provider and canonical CID in the logs. The interval from the PUT commit event to dispatch shows the wait before the provider call; the provider completion and database writeback events report their own `elapsed_us`. A CID already pinned for another file may need no new call. A commit event names a candidate provider, not proof that it was selected or that a POST was sent.

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

Release old pins on their original backup providers before running the
[database upgrade](database-storage.md#upgrade-an-existing-database). The new
schema starts without pin references and can't recover legacy handles or
recreate demand from tags. Submit a fresh signed request under the current
policy for any file that still needs remote pinning.
