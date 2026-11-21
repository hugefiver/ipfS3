export const SETTINGS_KEY = "ipfs3-pastebin.settings.v1";

function isLoopback(hostname) {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]";
}

function parseHttpUrl(value, originOnly) {
  if (typeof value !== "string" || value.includes("\\")) {
    throw new Error("Enter a valid HTTP(S) URL.");
  }
  let url;
  try {
    url = new URL(value.trim());
  } catch {
    throw new Error("Enter a valid HTTP(S) URL.");
  }
  if (url.protocol !== "https:" && !(url.protocol === "http:" && isLoopback(url.hostname))) {
    throw new Error("Use HTTPS, or HTTP only for a loopback development endpoint.");
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new Error("URLs cannot contain credentials, query parameters, or fragments.");
  }
  if (originOnly && url.pathname !== "/") {
    throw new Error("The S3 endpoint must be an origin without a path.");
  }
  return url;
}

export function normalizeEndpoint(value) {
  return parseHttpUrl(value, true).origin;
}

export function normalizeGateway(value) {
  const url = parseHttpUrl(value, false);
  if (!url.pathname.endsWith("/")) url.pathname += "/";
  return url.href;
}

export function normalizeBucket(value) {
  if (
    typeof value !== "string" ||
    !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(value) ||
    value.includes("..")
  ) {
    throw new Error("Bucket names use 3–63 lowercase letters, digits, dots, or hyphens.");
  }
  return value;
}

export function validateSettings(settings, deploymentMaxBytes) {
  const maxUploadBytes = settings.maxUploadBytes;
  if (!Number.isSafeInteger(deploymentMaxBytes) || deploymentMaxBytes < 1) {
    throw new Error("The deployment upload limit is invalid.");
  }
  if (!Number.isSafeInteger(maxUploadBytes) || maxUploadBytes < 1 || maxUploadBytes > deploymentMaxBytes) {
    throw new Error(`Upload limit must be between 1 and ${deploymentMaxBytes} bytes.`);
  }
  if (typeof settings.region !== "string" || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(settings.region)) {
    throw new Error("Enter a valid S3 region.");
  }
  return {
    endpoint: normalizeEndpoint(settings.endpoint),
    bucket: normalizeBucket(settings.bucket),
    region: settings.region,
    maxUploadBytes,
    ipfsGateway: normalizeGateway(settings.ipfsGateway),
    inbrowserGateway: normalizeGateway(settings.inbrowserGateway),
  };
}

export function readSettings(storage, defaults) {
  const saved = storage.getItem(SETTINGS_KEY);
  return saved === null
    ? validateSettings(defaults, defaults.maxUploadBytes)
    : validateSettings(JSON.parse(saved), defaults.maxUploadBytes);
}

export function persistSettings(storage, settings, defaults) {
  const valid = validateSettings(settings, defaults.maxUploadBytes);
  storage.setItem(SETTINGS_KEY, JSON.stringify(valid));
  return valid;
}
