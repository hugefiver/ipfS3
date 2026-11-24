// Deployment defaults. Override via PASTEBIN_* env vars with build.mjs, or edit for a custom static host.
export const deploymentDefaults = Object.freeze({
  endpoint: "http://127.0.0.1:9000",
  bucket: "pastebin",
  region: "us-east-1",
  maxUploadBytes: 64 * 1024 * 1024,
  ipfsGateway: "https://ipfs.io/ipfs/",
  inbrowserGateway: "https://inbrowser.link/ipfs/",
});
