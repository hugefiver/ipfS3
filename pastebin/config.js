// Deployment defaults. Copy this directory first, then edit these non-secret values.
export const deploymentDefaults = Object.freeze({
  endpoint: "https://ipfs3.moyuteam.me",
  bucket: "pastebin",
  region: "us-east-1",
  maxUploadBytes: 16 * 1024 * 1024,
  ipfsGateway: "https://apac.orbitor.dev/ipfs/",
  inbrowserGateway: "https://inbrowser.link/ipfs/",
});
