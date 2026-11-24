# Local S3 policies

Configure gateway authorization with `[[auth.policy]]` tables in `config.toml`. A rule applies to an exact configured user (the static access key ID), an exact bucket name, or both. Temporary credentials use the policies of their bound static user.

```toml
[[auth.policy]]
effect = "allow"
user = "media-reader"
bucket = "media"
actions = ["GetObject", "HeadObject", "ListObjects", "ListObjectsV2"]
prefix = "public/"

[[auth.policy]]
effect = "deny"
bucket = "media"
actions = ["GetObject", "HeadObject", "ListObjects", "ListObjectsV2"]
prefix = "public/private/"

[[auth.policy]]
effect = "allow"
user = "media-editor"
bucket = "media"
actions = ["PutObjectTagging"]
prefix = "review/"
version = "null"
request_tags = { team = "media", state = "approved" }
existing_tags = { team = "media" }
```

## Bucket defaults

A signed `CreateBucket` assigns the bucket to the caller's stable user. Omit `x-amz-acl` for `private`, or choose one of the four bucket categories. The owner can read or change it through `GetBucketAcl` and `PutBucketAcl`. Changes apply to subsequent requests for existing objects, versions and ordinary multipart uploads. There are no per-object ACLs: setting an ACL on an upload does not make an individual object private inside a public bucket. Use a private bucket or a directory deny for that boundary.

| Canned ACL | Additional access beyond the bucket owner |
|---|---|
| `private` | None. A directory policy can still share a specific prefix. |
| `authenticated-read` | Authenticated gateway users can list the bucket's contents and read objects. |
| `public-read` | The same ordinary reads, including unsigned requests. |
| `public-read-write` | Public read plus ordinary uploads, including writes to an existing key and multipart upload. This does not grant deletion, bucket administration, manual pin requests or archive extraction. |

```console
aws --endpoint-url http://localhost:9000 s3api create-bucket --bucket media
aws --endpoint-url http://localhost:9000 s3api put-bucket-acl --bucket media --acl authenticated-read
aws --endpoint-url http://localhost:9000 s3api get-bucket-acl --bucket media
```

`ListBuckets` returns only buckets owned by the signed user. To use a shared or public bucket, address it by name. These permissions govern the S3 gateway; plaintext accessible through an IPFS node by CID is outside the bucket ACL.

## Scope and precedence

Bucket defaults and directory rules are separate. An explicit matching `deny` wins, even for the owner. Otherwise either the bucket default or a matching policy `allow` can grant the operation. A rule with both `user` and `bucket` applies only to that combination. Bucket-only allow rules share with authenticated gateway users, not anonymous callers. Bucket-only denies restrict both signed and anonymous requests on public buckets unless `anonymous = true` (matches unsigned requests only, valid on bucket-scoped `deny` rules without `user`) or `anonymous = false` (matches signed requests only) is set. Only the owner can manage the bucket ACL.

Buckets upgraded without an ACL retain their earlier signed-access policy until the recorded owner sets a category. They do not acquire anonymous access. `GetBucketAcl` returns `InvalidBucketState` to the owner until the first `PutBucketAcl`. A legacy bucket with no recorded owner is not automatically assigned to a user.

`ipfs-s3/anonymous` is reserved for recording anonymous upload provenance. It cannot be configured as a user and grants no permissions. The static access key ID `ipfs-s3:anonymous` remains a regular user ID.

A rule needs `effect`, a non-empty `actions` list, and at least one of `user` and `bucket`. Use the exact, case-sensitive names in [the supported action list](../src/iam/action.rs). An omitted `prefix` covers all keys in the rule's scope. A configured prefix is a literal key prefix; `docs/` and `docs` differ. For a listing or an operation that writes a subtree, one allow must cover the whole requested prefix. A deny on any overlapping prefix also denies the whole operation; the gateway does not filter out denied descendants. To list the example bucket, request a narrower prefix such as `public/releases/`.

Service-wide `ListBuckets` uses user rules without a `bucket` or `prefix`. A user named by any policy needs a matching service-level allow to list their own buckets. Bucket administration actions such as `HeadBucket` and `PutBucketVersioning` do not take a key prefix.

## Conditions

All conditions on a rule must match. Tag condition tables must contain at least one key and use case-sensitive, exact key and value equality. Missing keys never match. Request tags are tags carried by the actual operation; existing tags are the tags of the object version that the operation reads or acts on. Protect tag-writing permissions if those tags are used as authorization labels.

| Field | Actions that can use it |
| --- | --- |
| `version` | `GetObject`, `HeadObject`, `DeleteObject`, `GetObjectTagging`, `PutObjectTagging`, `DeleteObjectTagging`, `GetPinningStatus` |
| `existing_tags` | `GetObject`, `HeadObject`, `DeleteObject`, `GetObjectTagging`, `PutObjectTagging`, `DeleteObjectTagging`, `GetPinningStatus` |
| `request_tags` | `PutObject`, `PutObjectTagging`, `CopyObject`, `CreateMultipartUpload`, `CompleteMultipartUpload`, `ImportObject`, `DecompressZip` |

`version` matches the explicit `versionId` supplied for an object operation, either `"null"` or a canonical UUID. A request that omits `versionId` does not match this condition. A rule's conditions must be supported by every action it lists; unsupported conditions, actions, and fields cause a configuration error at startup.

## Operations spanning multiple resources

- Copy checks `GetObject` on the source and `CopyObject` on the destination. Only replacement tags explicitly supplied with `REPLACE` count as `CopyObject` request tags; tags inherited from the source do not.
- Multi-object delete evaluates `DeleteObject` independently for each key and version. An authorized mutation can supersede a pending import that owns the same destination. This finishes that import as a whole; it does not delete published objects at its other destinations.
- Multipart completion uses the tags captured at upload creation. A completion request cannot supply a replacement tag set.
- Import and ZIP decompression check the archive or destination key and any extraction prefix the operation writes. Status actions `GetImportStatus` and `GetDecompressZipStatus` also cover the extraction prefix when their response exposes that subtree.
- `PutObjectTagging` and `DeleteObjectTagging` include any resulting manual pin changes. Reading pin status uses `GetPinningStatus`.
