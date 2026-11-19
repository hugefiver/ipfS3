# S3 Lifecycle Design

The lifecycle worker applies bucket rules to a specific published object version, delete marker or incomplete multipart upload. It records actions durably and mutates only that target. A bulk scan can fall behind while ordinary S3 requests and unrelated actions continue.

## Bucket rules

`PutBucketLifecycleConfiguration` validates and replaces the whole rule set atomically. `GetBucketLifecycleConfiguration` returns the stored configuration. `DeleteBucketLifecycle` removes it and preserves the revision boundary so a later PUT cannot reuse a revision held by an old action. All three honor `ExpectedBucketOwner` when supplied.

A configuration has up to 1,000 rules. Eligibility uses database UTC. For age-based rules the action becomes due at the first UTC midnight after the specified number of full days; a date rule uses its specified UTC midnight. An existing eligible object can be picked up by the next scan.

| Action | Target and effect |
| --- | --- |
| `Expiration` | Acts on the current version using the bucket's versioning rules. A current delete marker may be removed only if it is the sole retained version and meets the age condition. |
| `NoncurrentVersionExpiration` | Permanently removes the exact eligible noncurrent version or marker from S3 metadata. |
| `ExpiredObjectDeleteMarker` | Removes an eligible sole current delete marker without waiting for a Date/Days condition. |
| `AbortIncompleteMultipartUpload` | Aborts the exact upload by age since initiation; another part upload doesn't reset its age. The selector must cover all objects or use a prefix. An already-absent upload satisfies the lifecycle action. |
| `Transition` and `NoncurrentVersionTransition` | Publish `STANDARD_IA` for eligible current and noncurrent content on an independent cold Kubo. Delete markers and incomplete uploads aren't transition targets. |

Rules support prefix, tag and exclusive size bounds where the action permits them. For overlapping eligible actions on a version, permanent deletion takes precedence over transition, followed by creation of a current delete marker. Within a category the earliest due time wins.

## Task execution and write boundaries

A bounded scan records each action with the bucket's configuration revision, rule identity, action kind and exact target. Workers claim due actions with a lease and claim epoch. Before changing S3 metadata, the relevant write checks the current claim, policy and target; a new rule revision, tag change or competing write can make the action stale. One action's stale or terminal result leaves other targets available. Expired claims may be reclaimed; retries are bounded and a permanent failure leaves a terminal result.

Scanning and storage I/O stay outside publication transactions. For expiration and abort, the final write changes only the selected version or upload and its related metadata. No action calls primary Kubo `pin/rm`, deletes blocks or runs GC.

## Cold residency

`STANDARD` reads use hot Kubo. The gateway accepts transition rules only when cold Kubo is configured and supports the single direction `STANDARD` to `STANDARD_IA`. The worker streams the original DAG to the independent cold node, verifies the original CID and complete pinned local content, then publishes the new class and residency for that exact version in a short transaction. Network copy and residency verification run outside write transactions. A copy checkpoint can survive restart; the worker confirms cold content before publishing the class.

CID, ETag, version ID, metadata, tags, encryption and object bytes stay unchanged. Versions with the same CID may have different classes. A published IA version reads from cold with no hot fallback; copying it produces a new `STANDARD` destination. Disabling a rule cancels unpublished work and leaves published IA versions on cold. Cleanup releases logical references only. Removing cold storage can make published IA versions unreadable.

Cold residency is a readable storage tier, not a Pinata, Filebase or Cluster backup acknowledgment. Direct IA writes, archive and restore classes, and reverse transitions are unsupported. See [README](../../../README.md#transition-to-independent-cold-storage) for configuration and deployment boundaries.

Detailed action contracts: [expiration](2026-08-26-lifecycle-expiration-design.md) and [multipart abort](2026-09-01-lifecycle-abort-multipart-design.md).

AWS references: [PUT lifecycle configuration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html), [GET lifecycle configuration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html), [DELETE lifecycle configuration](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html), [filters](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html), [expiration and conflicts](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html).
