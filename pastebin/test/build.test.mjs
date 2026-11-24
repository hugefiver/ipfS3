import assert from "node:assert/strict";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import { buildSite, resolveDeploymentDefaults, RUNTIME_FILES } from "../build.mjs";
import { deploymentDefaults } from "../config.js";

test("resolveDeploymentDefaults keeps base defaults when env vars are unset or blank", () => {
  const resolved = resolveDeploymentDefaults({
    PASTEBIN_ENDPOINT: "",
    PASTEBIN_BUCKET: "   ",
    PASTEBIN_MAX_UPLOAD_MIB: "",
  });
  assert.deepEqual(resolved, deploymentDefaults);
});

test("resolveDeploymentDefaults applies PASTEBIN_* env overrides and MiB conversion", () => {
  const resolved = resolveDeploymentDefaults({
    PASTEBIN_ENDPOINT: "https://s3.example.com/",
    PASTEBIN_BUCKET: "public-bin",
    PASTEBIN_REGION: "auto",
    PASTEBIN_MAX_UPLOAD_MIB: "96",
    PASTEBIN_IPFS_GATEWAY: "https://gw.example.com/ipfs",
    PASTEBIN_INBROWSER_GATEWAY: "https://viewer.example.com/ipfs",
  });
  assert.deepEqual(resolved, {
    endpoint: "https://s3.example.com",
    bucket: "public-bin",
    region: "auto",
    maxUploadBytes: 96 * 1024 * 1024,
    ipfsGateway: "https://gw.example.com/ipfs/",
    inbrowserGateway: "https://viewer.example.com/ipfs/",
  });
});

test("resolveDeploymentDefaults rejects invalid env overrides", () => {
  assert.throws(() => resolveDeploymentDefaults({ PASTEBIN_ENDPOINT: "http://remote.example.com" }));
  assert.throws(() => resolveDeploymentDefaults({ PASTEBIN_MAX_UPLOAD_MIB: "0" }));
  assert.throws(() => resolveDeploymentDefaults({ PASTEBIN_MAX_UPLOAD_BYTES: "not-a-number" }));
});

test("buildSite stages only runtime assets and generates an importable config.js", async () => {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "pastebin-build-test-"));
  try {
    const outDir = path.join(tmpDir, "dist");
    await buildSite(outDir, {
      PASTEBIN_ENDPOINT: "https://s3.example.com",
      PASTEBIN_MAX_UPLOAD_MIB: "100",
      PASTEBIN_IPFS_GATEWAY: "https://gw.example.com/ipfs/",
    });
    const entries = (await readdir(outDir)).sort();
    assert.deepEqual(entries, [...RUNTIME_FILES, "config.js"].sort());
    const mod = await import(`${pathToFileURL(path.join(outDir, "config.js")).href}?t=${Date.now()}`);
    assert.equal(mod.deploymentDefaults.endpoint, "https://s3.example.com");
    assert.equal(mod.deploymentDefaults.maxUploadBytes, 100 * 1024 * 1024);
    assert.equal(mod.deploymentDefaults.ipfsGateway, "https://gw.example.com/ipfs/");
  } finally {
    await rm(tmpDir, { recursive: true, force: true });
  }
});
