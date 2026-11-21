// Deployment defaults. Copy this directory first, then edit these non-secret values.
export const deploymentDefaults = Object.freeze({
  endpoint: "http://127.0.0.1:9000",
  bucket: "pastebin",
  region: "us-east-1",
  maxUploadBytes: 16 * 1024 * 1024,
  ipfsGateway: "https://ipfs.io/ipfs/",
  inbrowserGateway: "https://inbrowser.link/ipfs/",
});
