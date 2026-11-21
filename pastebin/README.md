# Static Pastebin for ipfS3

`pastebin/` is a deployable static directory. It uses native HTML, CSS,
JavaScript modules, WebCrypto and `fetch`; there is no build step, package
manager, external script or runtime dependency.

## Use

Serve this directory over HTTPS, or over HTTP on loopback for development. For
example, from the repository root:

```powershell
uv run --no-project python -m http.server 8000 --bind 127.0.0.1 --directory pastebin
```

Open `http://127.0.0.1:8000/`, configure the S3 endpoint, bucket and region, then
enter publishing credentials. Text is uploaded as a `text/plain;charset=utf-8`
Blob. File mode stores the original bytes; ZIP directory mode uses the signed
`decompress-zip` extension and displays complete or partial extraction results.

Credentials exist only in the current page's fields. Only the endpoint, bucket,
region, effective byte limit and public IPFS gateway are saved in local storage.
Changing the S3 endpoint clears all credential fields. **Reset defaults** also
clears them. A share fragment never updates instance settings.

The deployment's `maxUploadBytes` is an upper bound. A user can lower the limit
for the current browser, but cannot raise it above the deployment value. The
publisher checks the Blob size before reading bytes for signing.

## Sharing and previews

Choose **Share** beside the exact preview or raw link to send. IPFS,
inbrowser.link and public S3 file-preview shares are direct object URLs. S3 raw
and directory-listing shares use a fixed-size fragment containing the item name,
kind, selected backend, preview/raw mode and target URL. They do not contain
credentials, the complete ZIP entry list or failure names.

- **inbrowser.link** opens a top-level preview on `https://inbrowser.link/ipfs/`;
  raw uses the file or original archive CID with `download=true`.
- **Public IPFS gateway** uses the configured gateway. Directory preview uses
  the UnixFS root CID; raw directory download uses the original archive CID.
- **Public S3** appears only after an explicit anonymous check. Object access is
  proved with unsigned `HEAD` using `credentials: omit` and `redirect: error`.
  Directory preview probes one `ListObjectsV2` page with `max-keys=1`; recipients
  can explicitly list the paginated extraction prefix. Individual entries are
  checked only when their own **Check** action is activated.

Opening a shared S3 directory requires the recipient to activate **List public
directory**. Parsing a share link never performs a request. Preview navigation
is blocked if uploaded active content would use the static site's own origin.
Use a separate origin for the site, S3 endpoint and IPFS gateway. IPFS raw links
use top-level gateway navigation with `download=true`. S3 raw is a button, not a
navigable object URL: activation performs an anonymous `fetch`, creates a Blob
and downloads an object URL. S3 raw GET sends
`X-IPFS3-Download-Gateway: proxy` to keep bucket download redirects at the
gateway. Failures are shown beside the action; no same-origin active content is
opened automatically.

A partial ZIP result remains explicitly marked. Failed entries are shown only on
the publisher's current page. A response without a directory root is not a
directory share.

## Deployment

Edit [`config.js`](config.js) before deploying the directory. The checked-in
values describe a loopback gateway at `http://127.0.0.1:9000`, bucket `pastebin`,
region `us-east-1`, a 16 MiB ceiling and these gateways:

```text
https://ipfs.io/ipfs/
https://inbrowser.link/ipfs/
```

The S3 endpoint is an HTTP(S) origin only: no path, query, fragment or userinfo.
HTTPS is required except for literal `localhost`, `127.0.0.1` and `[::1]` during
development. Explicit non-default ports are preserved. The gateway URL may have
a path and must identify an IPFS path gateway ending in `/ipfs/` or an equivalent
prefix. Do not place publisher credentials in deployment files.

Create a private bucket for private publishing, or a public-read bucket when S3
links should work anonymously. Never grant anonymous write. With the bucket ACL
feature, an operator can deliberately select public read:

```powershell
$env:AWS_ACCESS_KEY_ID = "operator-access-key"
$env:AWS_SECRET_ACCESS_KEY = "operator-secret"
aws --endpoint-url https://s3.example.com s3api put-bucket-acl --bucket pastebin --acl public-read
```

This does not make writes anonymous. Signed publisher credentials still need
object write permission. Private buckets can use IPFS links when the referenced
content is available from the selected gateway; S3 checks remain unavailable.

### Bucket CORS

Replace the example origin with the exact static site origin, including a
non-default port. Do not use `*` for a publishing origin.

```powershell
aws --endpoint-url https://s3.example.com s3api put-bucket-cors `
  --bucket pastebin --cors-configuration file://pastebin/deploy/cors.example.json
```

[`deploy/cors.example.json`](deploy/cors.example.json) permits signed `PUT`,
anonymous `HEAD`/`GET`, `authorization`, `content-type`, `x-amz-*` (including
`x-amz-tagging` and session tokens) and `x-ipfs3-download-gateway`. It exposes `ETag`,
`x-amz-meta-ipfs-cid`, `x-ipfs-s3-zip-root-cid` and
`x-ipfs-s3-zip-root-status`. CORS permits browser access; it does not change
bucket ACL or SigV4 authorization.

### Upload and ZIP limits

Keep all three boundaries aligned:

1. `config.js`: `maxUploadBytes = 16 * 1024 * 1024`.
2. [`deploy/nginx.conf.example`](deploy/nginx.conf.example):
   `client_max_body_size 16m` on a dedicated path-style S3 virtual host, whose
   `location /` covers both bucket-root operations and object keys.
3. [`deploy/ipfs-s3.zip.toml.example`](deploy/ipfs-s3.zip.toml.example):
   `max_archive_bytes = 16777216`, plus actual decompressed, entry, metadata,
   staging and processing-deadline budgets.

The example permits 64 MiB total and per-entry decompressed bytes, 1,000 entries,
16 MiB metadata accounting and a 120-second processing deadline. These are real
server extraction limits, not a claim that the browser limit limits decompressed
output. Tune them for the deployment and keep `max_archive_bytes` no larger than
the browser and reverse-proxy compressed upload limits. Proxy request buffering
allows the ingress body limit to run before forwarding the upload.

To cap the total storage used by a bucket, set its [ipfS3 bucket quota](../docs/bucket-quota.md). The server rejects an over-quota publication, including ZIP output, regardless of the settings in this page.

### Expiration

The site never reads or replaces lifecycle rules. An operator should first get
and review existing configuration, then merge the intended rule:
`PutBucketLifecycleConfiguration` replaces the complete configuration.

```powershell
aws --endpoint-url https://s3.example.com s3api get-bucket-lifecycle-configuration --bucket pastebin
aws --endpoint-url https://s3.example.com s3api put-bucket-lifecycle-configuration `
  --bucket pastebin --lifecycle-configuration file://pastebin/deploy/lifecycle.example.json
```

The example's empty `Filter` applies to the whole bucket. `Expiration.Days = 30`
expires current object versions; `NoncurrentVersionExpiration.NoncurrentDays = 7`
expires eligible noncurrent versions. On a versioning-enabled bucket, current
expiration can create a delete marker and leave the content noncurrent until the
noncurrent rule applies. Review existing objects and versioning before applying
an empty whole-bucket filter.

Expiration ends ipfS3's S3 metadata and storage commitment. It does not promise
that a CID disappears from IPFS, other pins, caches or remote providers. The
lifecycle action does not start Kubo unpin or garbage collection.

## Tests

No package installation is needed. Run deterministic module tests with:

```powershell
node --test "pastebin/test/*.test.mjs"
```

Open `test/publish.browser.html` on the static site to exercise native browser XML parsing. To include the live signed PUT and anonymous read tests, set `PASTEBIN_TEST_ENDPOINT`, `PASTEBIN_TEST_BUCKET`, `PASTEBIN_TEST_ACCESS_KEY` and `PASTEBIN_TEST_SECRET_KEY` for a dedicated public-read test bucket. The test publishes new objects.
