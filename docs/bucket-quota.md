# Bucket storage quota

A bucket quota limits the logical bytes in published S3 object versions. New and upgraded buckets have no limit by default. To set a limit, run a controlled SQL update against the gateway's metadata database (SQLite or PostgreSQL):

```sql
-- Allow 1 GiB in the paste bucket.
UPDATE buckets SET quota_bytes = 1073741824 WHERE name = 'paste';
SELECT name, used_bytes, quota_bytes FROM buckets WHERE name = 'paste';
```

Use a nonnegative integer for `quota_bytes`. Set it to `0` to stop net growth or `NULL` to remove the limit. The change takes effect when the SQL transaction commits. Lowering a limit leaves existing objects readable and allows deletions, smaller replacements and writes with no net growth. `used_bytes` is maintained by the gateway; read it for reporting and change only `quota_bytes`.

## What counts

Each addressable object version counts at its plaintext size, including noncurrent versions and copies of the same CID. With versioning enabled, overwriting a key adds a charged version. With versioning suspended, a write replaces the null version and retains named versions. Delete markers use zero bytes. Creating one frees only the null version it actually replaces; permanently deleting a version frees that version's bytes. Lifecycle expiry uses the same rules.

A completed multipart upload counts only the selected parts' combined plaintext size. Uncompleted parts, directory DAGs, backup pins and replicas do not count. A ZIP publication charges the source when published and the successful output objects, applying replacements across the entire batch. Skipped or failed entries add no bytes.

## When the limit is enforced

PUT, CopyObject, import, multipart completion and ZIP publication settle the net change in their metadata transaction. Concurrent gateways sharing the database use the same bucket lock. A publication that reaches the exact limit succeeds. A positive net change above the limit returns S3 `QuotaExceeded` (HTTP 403); no new S3 versions become visible, and the previous versions and usage remain intact. Asynchronous imports record terminal failure `quota_exceeded`.

The quota protects published metadata, not upload bandwidth or physical Kubo space: content may already have reached Kubo before publication is rejected. Keep request-size and ZIP extraction limits for those resources. The [database upgrade](database-storage.md#upgrade-an-existing-database) initializes usage from retained versions once; requests do not scan the bucket to measure capacity.
