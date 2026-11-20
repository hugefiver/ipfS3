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

## Scope and precedence

Any rule with a `user` puts that user under policy in **every** bucket and on service-wide operations. A `bucket` on a user rule limits where that rule can match. A bucket-only rule puts every user under policy for that bucket. The gateway considers all matching user and bucket rules: an explicit `deny` wins, otherwise at least one `allow` must match. A request outside both scopes retains the gateway's default access for authenticated users.

A rule needs `effect`, a non-empty `actions` list, and at least one of `user` and `bucket`. Use the exact, case-sensitive names in [the supported action list](../src/iam/action.rs). An omitted `prefix` covers all keys in the rule's scope. A configured prefix is a literal key prefix; `docs/` and `docs` differ. For a listing or an operation that writes a subtree, one allow must cover the whole requested prefix. A deny on any overlapping prefix also denies the whole operation; the gateway does not filter out denied descendants. To list the example bucket, request a narrower prefix such as `public/releases/`.

Service-wide `ListBuckets` uses user rules without a `bucket` or `prefix`. Bucket administration actions such as `HeadBucket` and `PutBucketVersioning` do not take a key prefix.

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
