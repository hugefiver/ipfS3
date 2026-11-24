import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeBucket,
  normalizeEndpoint,
  normalizeGateway,
  persistSettings,
  readSettings,
  SETTINGS_KEY,
  validateSettings,
} from "../settings.js";

const defaults = {
  endpoint: "http://127.0.0.1:9000",
  bucket: "pastebin",
  region: "us-east-1",
  maxUploadBytes: 1024,
  ipfsGateway: "https://ipfs.io/ipfs/",
  inbrowserGateway: "https://inbrowser.link/ipfs/",
};

function storage() {
  const values = new Map();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    values,
  };
}

test("endpoint normalization preserves explicit ports and loopback HTTP", () => {
  assert.equal(normalizeEndpoint("https://s3.example.com:9443/"), "https://s3.example.com:9443");
  assert.equal(normalizeEndpoint("http://localhost:9000"), "http://localhost:9000");
  assert.equal(normalizeEndpoint("http://[::1]:9000"), "http://[::1]:9000");
});

test("endpoint validation rejects signing ambiguity and insecure remote HTTP", () => {
  for (const value of [
    "http://s3.example.com",
    "https://s3.example.com/base",
    "https://s3.example.com/?x=1",
    "https://s3.example.com/#fragment",
    "https://user:secret@s3.example.com",
    "https://s3.example.com\\@localhost",
    "file:///tmp/s3",
  ]) {
    assert.throws(() => normalizeEndpoint(value), undefined, value);
  }
});

test("gateway normalization retains a path prefix and adds a trailing slash", () => {
  assert.equal(normalizeGateway("https://ipfs.example.com/ipfs"), "https://ipfs.example.com/ipfs/");
  assert.throws(() => normalizeGateway("https://ipfs.example.com/ipfs/?url=https://evil.test"));
});

test("upload limit allows any positive integer without a hard deployment ceiling", () => {
  assert.equal(validateSettings(defaults).maxUploadBytes, 1024);
  assert.equal(validateSettings({ ...defaults, maxUploadBytes: 512 }).maxUploadBytes, 512);
  assert.equal(validateSettings({ ...defaults, maxUploadBytes: 256 * 1024 * 1024 }).maxUploadBytes, 256 * 1024 * 1024);
  assert.throws(() => validateSettings({ ...defaults, maxUploadBytes: 0 }));
  assert.throws(() => validateSettings({ ...defaults, maxUploadBytes: -1 }));
  assert.throws(() => validateSettings({ ...defaults, maxUploadBytes: 1.5 }));
});

test("only validated non-secret settings are persisted", () => {
  const store = storage();
  persistSettings(store, { ...defaults, secretAccessKey: "do-not-store" });
  const serialized = store.values.get(SETTINGS_KEY);
  assert.equal(serialized.includes("do-not-store"), false);
  assert.equal(serialized.includes("secretAccessKey"), false);
  assert.deepEqual(readSettings(store, defaults), defaults);
});

test("saved settings higher than deployment default remain valid", () => {
  const store = storage();
  persistSettings(store, { ...defaults, maxUploadBytes: 2048 });
  assert.equal(readSettings(store, { ...defaults, maxUploadBytes: 512 }).maxUploadBytes, 2048);
});

test("bucket validation uses the publisher's S3 name boundary", () => {
  assert.equal(normalizeBucket("my-pastebin.01"), "my-pastebin.01");
  for (const value of ["ab", "Uppercase", "-leading", "trailing-", "two..dots"]) {
    assert.throws(() => normalizeBucket(value));
  }
});
