import assert from "node:assert/strict";
import test from "node:test";
import {
  decodeShare,
  directoryListUrl,
  encodeShare,
  formatBytes,
  ipfsUrl,
  normalizeSelection,
  objectUrlFromList,
  probePublicUrl,
  safeHttpUrl,
} from "../share.js";

const file = {
  kind: "file",
  name: "说明 & notes.txt",
  backend: "ipfs",
  mode: "raw",
  url: "https://ipfs.example.com/ipfs/bafyfile?filename=%E8%AF%B4%E6%98%8E&download=true",
};

test("selected links round-trip Unicode without sharing ZIP result lists", () => {
  const directory = {
    kind: "directory",
    name: "week (1)!.zip",
    backend: "s3",
    mode: "preview",
    url: "https://s3.example.com/pastebin/?list-type=2&prefix=pastebin%2Fid%2Ffiles%2F",
    rootStatus: "partial",
    entries: [{ key: "private-name", cid: "bafyentry", size: 1 }],
    failures: [{ name: "unrelated-failure", code: "Limit", message: "details" }],
  };
  const encoded = encodeShare(directory);
  assert.equal(encoded.includes("private-name"), false);
  assert.equal(encoded.includes("unrelated-failure"), false);
  assert.deepEqual(decodeShare(encoded), normalizeSelection(directory));
  assert.deepEqual(decodeShare(encodeShare(file)), file);
});

test("share validation rejects unsafe targets and rootless directories", () => {
  for (const url of ["javascript:alert(1)", "file:///etc/passwd", "https://user:pass@example.com/a", "https://example.com/a#x"]) {
    assert.equal(safeHttpUrl(url), null);
    assert.throws(() => normalizeSelection({ ...file, url }));
  }
  assert.throws(() => normalizeSelection({ ...file, kind: "directory" }), /root status/);
  assert.throws(() => normalizeSelection({ ...file, backend: "unknown" }));
});

test("IPFS URLs distinguish preview, raw and directory navigation", () => {
  assert.equal(
    ipfsUrl("https://ipfs.example.com/ipfs/", "bafyfile", { name: "a b.txt" }),
    "https://ipfs.example.com/ipfs/bafyfile?filename=a+b.txt",
  );
  const raw = new URL(ipfsUrl("https://inbrowser.link/ipfs/", "bafyarchive", { name: "a.zip", download: true }));
  assert.equal(raw.searchParams.get("download"), "true");
  assert.equal(raw.searchParams.get("filename"), "a.zip");
  assert.equal(
    ipfsUrl("https://ipfs.example.com/ipfs/", "bafyroot", { directory: true }),
    "https://ipfs.example.com/ipfs/bafyroot/",
  );
  assert.throws(() => ipfsUrl("https://ipfs.example.com/ipfs/", "../bad"));
});

test("path-style directory listing accepts RFC3986-encoded source names", () => {
  const result = {
    key: "pastebin/id/week (1)!.zip",
    url: "https://s3.example.com:9443/pastebin/pastebin/id/week%20%281%29%21.zip",
  };
  const listing = new URL(directoryListUrl(result));
  assert.equal(listing.origin, "https://s3.example.com:9443");
  assert.equal(listing.pathname, "/pastebin/");
  assert.equal(listing.searchParams.get("list-type"), "2");
  assert.equal(listing.searchParams.get("prefix"), "pastebin/id/files/");
  assert.equal(
    objectUrlFromList(listing.href, "pastebin/id/files/说明 #1.txt"),
    "https://s3.example.com:9443/pastebin/pastebin/id/files/%E8%AF%B4%E6%98%8E%20%231.txt",
  );
});

test("anonymous HEAD uses no credentials and never follows redirects", async () => {
  let invocation;
  const publicResult = await probePublicUrl("https://s3.example.com/bucket/key", async (url, options) => {
    invocation = { url, options };
    return { ok: true, status: 200 };
  });
  assert.equal(publicResult.state, "public");
  assert.equal(invocation.options.method, "HEAD");
  assert.equal(invocation.options.credentials, "omit");
  assert.equal(invocation.options.redirect, "error");
  assert.equal(invocation.options.referrerPolicy, "no-referrer");
  assert.equal((await probePublicUrl(file.url, async () => ({ ok: false, status: 403 }))).state, "unavailable");
  assert.equal((await probePublicUrl(file.url, async () => { throw new TypeError("CORS"); })).state, "unknown");
});

test("byte formatting covers exact and fractional binary units", () => {
  assert.equal(formatBytes(0), "0 B");
  assert.equal(formatBytes(1024), "1.00 KiB");
  assert.equal(formatBytes(16 * 1024 * 1024), "16.0 MiB");
});
