# Bucket CORS Design

**Status:** Approved design
**Date:** 2026-08-31
**Audience:** Implementers and operators

**Runtime revision (2026-08-31):** The pinned real-client lane proved that
current AWS CLI v2 sends its default `CRC64NVME` request checksum on
`PutBucketCors` and omits `Content-MD5`. The original MD5-only design therefore
could not satisfy its own AWS CLI acceptance criterion. This revision adds a
strict, dependency-free `CRC64NVME` validation path while retaining the existing
MD5 path and rejecting every other SDK checksum algorithm.

## Summary

This design adds one atomic Bucket CORS feature: authenticated configuration
management through native s3s 0.14 S3 operations, plus browser CORS execution
at the outer Axum boundary. A saved configuration controls both standard S3
object requests and the custom import and decompress object routes. The feature
is path-style only. The current `S3ServiceBuilder` has no host parser, so it
does not support or claim virtual-hosted-style routing.

`/health` and `/ready` remain outside CORS handling. A CORS configuration does
not grant data access, bypass SigV4, alter bucket ownership, or affect Kubo.

## Scope and Integration Boundary

`S3Impl` will implement the native s3s 0.14 methods:

```text
get_bucket_cors
put_bucket_cors
delete_bucket_cors
```

Those methods retain s3s routing, SigV4 verification, XML decoding, and XML
serialization. A separate outer Axum middleware will handle unsigned browser
preflight requests and apply CORS headers to actual responses. It runs around
the S3 fallback service and custom object routes so that import and decompress
routes receive the same policy. It explicitly skips `/health` and `/ready`.

The middleware recognizes buckets only from path-style request paths. It must
not infer a bucket from `Host`, introduce virtual-hosted handling, or apply a
bucket policy to an unparseable route.

## Configuration Model

The implementation will create `src/cors/` with focused modules:

| Path | Responsibility |
|---|---|
| `src/cors/mod.rs` | Public CORS module boundary and shared exports. |
| `src/cors/model.rs` | Canonical in-memory configuration and rule types. |
| `src/cors/config.rs` | S3 DTO conversion, validation, canonical serialization, and stored-value revalidation. |
| `src/cors/matcher.rs` | Origin, method, and request-header matching. |
| `src/cors/http.rs` | Axum request classification, body preservation, and response-header application. |

The canonical representation preserves rule order and repeated fields. Rules
are never merged, deduplicated, or reordered because matching selects the first
matching rule. Canonical JSON is the only persisted format and must be
semantically revalidated whenever it is read. A syntactically valid but
semantically corrupt stored value therefore fails closed instead of becoming a
browser policy.

PUT validation constructs the complete canonical model before a database write.
Malformed XML remains s3s framework `MalformedXML`; a typed but semantically
invalid configuration is a fixed `InvalidRequest` response. The validation
contract is:

- There are 1 to 100 rules.
- Canonical serialized JSON is at most 64 KiB.
- Every rule has at least one `AllowedOrigin` and at least one `AllowedMethod`.
- Allowed methods are exactly `GET`, `PUT`, `HEAD`, `POST`, and `DELETE`.
  `AllowedMethod=*` is rejected as undocumented ambiguity.
- Rule `ID` is optional and, when present, has at most 255 characters.
- `MaxAgeSeconds` is optional and nonnegative.
- Origin and allowed-header patterns are nonempty visible strings, contain no
  control characters or whitespace, and contain at most one `*`.
- Allowed-header wildcard matching is ASCII-case-insensitive.
- Exposed headers must parse as HTTP header names.

These constraints apply on initial PUT and on stored JSON revalidation. The
implementation must not reinterpret an invalid stored value as an empty,
permissive, or partially valid configuration.

## Persistence and Atomicity

An additive migration placed after lifecycle expiration creates
`bucket_cors_configs`:

| Column | Meaning |
|---|---|
| `bucket` | Primary key and foreign key to `buckets(name)` with `ON DELETE CASCADE`. |
| `canonical_json` | Non-null canonical, validated complete configuration JSON. |
| `created_at` | Database-clock creation time. |
| `updated_at` | Database-clock replacement time. |

There is at most one row per bucket. PUT and DELETE lock the bucket row, use the
database clock, and execute in one transaction. PUT atomically replaces the
entire configuration. DELETE physically removes the row and returns 204 even
when no row exists. GET with no row returns `NoSuchCORSConfiguration` with HTTP
404.

The configuration is deliberately not modeled as a lifecycle tombstone or
lifecycle revision. It has no tombstone, revision, scanner state, or worker
state. Concurrent PUT and DELETE operations expose only a whole old snapshot,
a whole new snapshot, or absence. They never expose a partial JSON document or
mixed rule set.

## Management Request Integrity

For a path-style `PUT ?cors`, the outer middleware reads no more than
64 KiB plus one byte, computes the MD5 and CRC64NVME digests, stores only parsed
fixed-size checksum metadata in a private request extension, and reconstructs
the identical request body before forwarding to s3s and SigV4. This is a
byte-for-byte preservation requirement, including the body consumed to compute
the digests. A body larger than the limit or a body-read failure returns a fixed
safe S3-style HTTP 400 response without reaching s3s.

The request must provide at least one supported integrity proof:

1. `Content-MD5`, base64-decoded to exactly 16 bytes; or
2. `x-amz-sdk-checksum-algorithm: CRC64NVME` paired with exactly one
   `x-amz-checksum-crc64nvme` value, base64-decoded to exactly 8 bytes.

When both proofs are present, both must validate. Digest comparisons use
constant-time equality. A missing integrity proof or an algorithm/header pairing
error is `InvalidRequest`; malformed base64 or a decoded length mismatch is
`InvalidDigest`; a well-formed checksum that differs from the computed body
checksum is `BadDigest`. SDK algorithms other than exact `CRC64NVME` remain
unsupported and are rejected as `InvalidRequest` rather than ignored.

CRC64NVME uses the standard reflected NVME parameters and is locked by the
`123456789` check value `ae8b14860a799888` (base64 `rosUhgp5mIg=`). The body
limit makes a small table-free implementation sufficient. No dependency is added;
the implementation continues to use existing `md5`, `base64`, `subtle`, and
`http` dependencies. The canonical JSON 64 KiB limit remains a second
persistence boundary after request integrity succeeds.

## Management API and Ownership Semantics

Bucket lookup and `x-amz-expected-bucket-owner` follow the lifecycle operation
pattern. An omitted expected owner is accepted. A supplied owner must equal the
stored owner or the request returns `AccessDenied` with HTTP 403. A nonexistent
bucket returns `NoSuchBucket`.

| Operation | Success | Required result |
|---|---:|---|
| `put_bucket_cors` | 200 | Validate XML and one or both supported integrity proofs, then atomically replace the complete configuration. |
| `get_bucket_cors` | 200 | Revalidate canonical JSON, then serialize the complete stored configuration as XML. |
| `get_bucket_cors`, absent | 404 | Return custom `NoSuchCORSConfiguration` S3 XML. |
| `delete_bucket_cors` | 204 | Physically remove the row; repeated deletion remains successful. |

Store errors and logs must not expose configured origin values, header values,
the MD5 value, or request-body content. Errors are fixed, safe classifications,
not echoes of client configuration.

## Browser Matching and Middleware Flow

The middleware reads exactly one configuration snapshot before it invokes the
inner service. A database failure at this lookup produces a fixed HTTP 500.
Stored JSON that cannot be semantically revalidated is treated as corruption and
fails closed with no CORS headers; it must not reach matching or header output.

An actual request matches only its valid `Origin` and actual HTTP method. If it
has no valid Origin, no configuration, or no matching rule, the middleware
forwards it unchanged. If it matches, the middleware appends its actual-response
CORS headers after the inner service completes, including when the inner S3
service returns an error. That lets a browser inspect an allowed error response
without making a failed policy request permissive.

A valid preflight has all of the following:

1. Method `OPTIONS`.
2. A valid `Origin`.
3. `Access-Control-Request-Method`.

The requested method and origin must match one rule. If
`Access-Control-Request-Headers` is supplied, each comma-separated token must
be valid and must match an `AllowedHeader` pattern case-insensitively. An
invalid token, missing configuration, or no matching rule produces HTTP 403
with zero `Access-Control-*` headers. An `OPTIONS` request that contains either
`Origin` or `Access-Control-Request-Method`, but not both, is an invalid
preflight and receives the same 403 response. The inner S3 service is not
invoked for either a valid or invalid preflight attempt. A plain `OPTIONS`
request containing neither preflight header continues through the normal inner
service behavior.

The first matching rule wins. Later matching rules are not combined with it.

## Response Header Policy

The following are explicit gateway implementation decisions. They do not claim
undocumented AWS response quirks.

Every successful CORS match appends these `Vary` tokens without removing or
overwriting existing `Vary` values:

```text
Origin
Access-Control-Request-Method
Access-Control-Request-Headers
```

For an `AllowedOrigin` exactly equal to `*`, the response emits
`Access-Control-Allow-Origin: *` and omits
`Access-Control-Allow-Credentials`. For an exact origin or a partial wildcard
origin pattern, the middleware echoes the validated request Origin and emits
`Access-Control-Allow-Credentials: true`.

For a matching preflight, the response emits the requested method only, not the
rule's full method list. When requested headers were supplied, it emits only
those requested header names, preserving safe input spelling and order. It emits
`Access-Control-Max-Age` only when configured and emits no expose-header value.

For a matching actual request, the response emits the optional configured
`Access-Control-Expose-Headers` list. It emits no
`Access-Control-Allow-Methods`, `Access-Control-Allow-Headers`, or
`Access-Control-Max-Age`. Header construction must use valid HTTP header names
and values. An impossible stored corruption or failed safe header construction
fails closed to no CORS headers.

## Failure and Security Contract

The implementation must preserve this boundary:

| Condition | HTTP / S3 result |
|---|---|
| Malformed management XML | Framework `MalformedXML`. |
| Semantic configuration failure | Fixed `InvalidRequest`. |
| Missing both supported integrity proofs | `InvalidRequest`. |
| Malformed MD5 or CRC64NVME checksum | `InvalidDigest`. |
| MD5 or CRC64NVME mismatch | `BadDigest`. |
| Unsupported or unpaired SDK checksum input | `InvalidRequest`. |
| Missing configuration on management GET | `NoSuchCORSConfiguration`, 404. |
| Owner mismatch | `AccessDenied`, 403. |
| Missing bucket | `NoSuchBucket`. |
| Missing, invalid, or disallowed preflight | 403 with no CORS headers. |
| Middleware configuration lookup failure | Fixed 500. |

The feature must not log or return origin patterns, requested headers, configured
headers, configuration JSON, body content, or MD5 material. It must not make
health or readiness endpoints browser-public.

## Test and Evidence Plan

Implementation is test-driven. The test suite must cover migration schema,
upgrade, rollback/down behavior, and cascade delete. It must cover canonical
JSON tampering, rule order, repeated fields, wildcard behavior, and
ASCII-case-insensitive header matching. It must prove whole-configuration
concurrency on SQLite and a fresh PostgreSQL 17 instance.

Signed management tests cover XML, MD5, CRC64NVME, the fixed CRC64NVME known
vector, exact body reconstruction through SigV4, expected-owner mismatch,
missing bucket, error mappings, replacement, absent GET, and idempotent delete.
HTTP tests cover unsigned OPTIONS, signed successful and
failed actual requests, first-match selection, no configuration, disallowed
origin, method, and header, wildcard credential policy, response errors that
retain allowed CORS headers, custom import and decompress preflight, and health
and readiness exclusion.

The implementation must add these evidence surfaces:

```text
tests/cors.rs
tests/postgres_cors.rs
tests/compose.cors-validation.yml
scripts/bucket-cors-smoke.ps1
docs/bucket-cors-evidence-2026-08-31.log
```

The Compose validation and smoke script must have unique ownership, require no
image pull or software installation, and leave zero residual resources after
cleanup. Live evidence uses real AWS CLI Bucket CORS PUT, GET, and DELETE plus
browser-style HTTP requests. The evidence log records the commands, client and
gateway versions, redacted assertions, and a truthful result. README promotion
and only the Bucket CORS item in `ROADMAP.md` may occur after evidence is PASS.
The existing Lifecycle roadmap item remains unchecked.

## Likely Changed Paths

The future implementation plan should keep edits to paths justified by this
design, expected to include:

```text
src/store/migrations/m20260831_000001_bucket_cors.rs
src/store/entities/bucket_cors_config.rs
src/store/entities/mod.rs
src/store/cors_config.rs
src/store/mod.rs
src/cors/mod.rs
src/cors/model.rs
src/cors/config.rs
src/cors/matcher.rs
src/cors/checksum.rs
src/cors/http.rs
src/s3/ops/cors.rs
src/s3/ops/mod.rs
src/s3/handler.rs
src/s3/http.rs
src/main.rs
src/lib.rs
tests/support/cors.rs
tests/fixtures/cors/
tests/cors.rs
tests/postgres_cors.rs
tests/compose.cors-validation.yml
scripts/bucket-cors-smoke.ps1
docs/bucket-cors-evidence-2026-08-31.log
README.md
ROADMAP.md
```

The exact migration timestamp and generated-entity registration follow the
repository convention. Cargo dependency versions remain unchanged. The existing
default, single-PostgreSQL, multi-gateway, Cluster, Lifecycle Compose, and
release workflow remain unchanged unless a concrete implementation necessity is
found. That discovery requires an orchestrator-approved design revision before
the implementation changes those boundaries.

## Non-goals

- Virtual-hosted-style Bucket CORS.
- Bucket policy, IAM, anonymous data access, TLS, or edge-proxy behavior.
- Credential-mode behavior beyond this document's wildcard credential decision.
- `AllowedMethod=*`.
- JSON management payloads or directory buckets.
- Lifecycle, transition, or CORS interaction.
- Global or Kubo CORS.
- Public CORS for health or readiness.
- Exact undocumented AWS header quirks.

## Acceptance Criteria

1. The gateway exposes native s3s 0.14 Bucket CORS PUT, GET, and DELETE XML
   operations with the specified owner, MD5/CRC64NVME integrity, success, and
   error behavior.
2. One durable row per bucket stores only semantically valid canonical JSON;
   every replacement and deletion is atomic, uses database time, and preserves
   whole old or new snapshots under concurrency.
3. The outer middleware recognizes only path-style S3 and custom object routes,
   serves unsigned valid preflight requests, and excludes `/health` and `/ready`.
4. Rule validation, canonical revalidation, first-match behavior, origin
   wildcard handling, method handling, and header matching meet every stated
   bound and fail closed on corruption.
5. Actual allowed requests receive the stated CORS headers even when the inner
   S3 response is an error. Disallowed or malformed preflights receive 403 with
   no CORS headers.
6. `Vary` tokens are appended without destroying existing values, and preflight
   and actual headers follow the separate response policies in this design.
7. SQLite, fresh PostgreSQL 17, signed API, unsigned browser HTTP, migration,
   concurrency, import/decompress, and live-client evidence all pass before
   README and the Bucket CORS roadmap item are promoted.
8. Implementation receives identity-bound Oracle and Reviewer final acceptance.
   Existing user authorization permits a commit only after that approval. It
   must never push or tag as part of this feature.

## Risks

The main compatibility risk is the locked s3s 0.14 DTO and trait shape,
especially the exact `get_bucket_cors`, `put_bucket_cors`, and
`delete_bucket_cors` signatures and XML fields. The implementation must verify
them before coding and stop for an orchestrator design revision if they conflict
with this contract.

The integrity middleware can accidentally change signed bytes if it rebuilds a
body incorrectly. Exact-body reconstruction and SigV4 tests are mandatory.
Concurrent SQLite behavior also needs explicit coverage because its row-lock
mechanics differ from PostgreSQL. Finally, invalid stored configuration must
remain a closed failure path: returning permissive CORS headers after corruption
would expose browser clients beyond the saved policy.

## Git and Review Boundary

This approved design authorizes no implementation, test, README, ROADMAP, plan,
configuration, staging, commit, push, tag, or other Git-history change. Future
implementation requires an approved implementation plan and the identity-bound
Oracle and Reviewer final acceptance named above. It must stay inside the
approved changed-path boundary and stop for an orchestrator design revision when
the locked API or live evidence contradicts this design.

*Author's note: Written for the engineer and operator who need one auditable Bucket CORS feature, with browser behavior that stays bounded by the saved bucket policy.*
