# Directory CID for an S3 prefix

A signed path-style `ListObjectsV2` can return a UnixFS directory for the **whole current prefix**. Include `ipfs3-directory-cid=true` once in the SigV4 canonical query. For example, to list one entry under `photos/` and obtain the CID for every current object below it:

```http
GET /media?list-type=2&prefix=photos%2F&delimiter=%2F&max-keys=1&ipfs3-directory-cid=true
```

Read `x-ipfs3-directory-cid` from the successful response headers. The body is the ordinary list XML, including truncation and common prefixes. `max-keys`, `delimiter`, and `encoding-type=url` affect the XML only. A key such as `photos/trip/猫.jpg` is accessible as `trip/猫.jpg` under the returned CID. To consume the next XML page, send the continuation token **without** the extension. Omit the extension for ordinary lists: they perform no Kubo work.

The prefix must end in `/` and have at least one current object. The extension rejects `start-after` and `continuation-token`, even when supplied with a valid prefix. The caller must be signed, have `ListObjectsV2` on the prefix, and have `GetObject` on every current key, including keys outside the XML page. Current object tags participate in that decision. Anonymous access to a public bucket does not permit directory generation.

The XML, object CIDs, access checks and storage classes use one database snapshot. Noncurrent versions and deleted keys are excluded. A cold or encrypted current object, a file/directory name conflict (for example `photos/a` and `photos/a/b`), or an unrepresentable relative path fails the entire request. Empty path components (including S3 folder-marker objects), `.` and `..` components, backslashes and NUL are rejected. Builds are limited to 10,000 files, 64 relative path components and a 64 MiB builder manifest budget. No partial CID is returned.

After the snapshot closes, the gateway assembles UnixFS blocks on hot Kubo with `pin=false`. The CID names that immutable snapshot; later S3 writes or deletes do not update it. Requesting a CID does not pin or unpin any DAG, reserve storage, or extend object retention. Unpinned blocks may be garbage-collected, and a CID is not a guarantee of future availability. Pin the root separately on a node you control if it must be retained. Sharing a reachable CID gives its recipients access through IPFS independently of subsequent S3 authorization changes.
