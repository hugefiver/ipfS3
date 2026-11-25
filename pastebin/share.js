const SHARE_PREFIX = "#share?";
const BACKENDS = new Set(["inbrowser", "ipfs", "s3"]);
const ENCRYPTED_BACKENDS = new Set(["ipfs", "s3"]);
const KDFS = new Set(["raw", "passphrase"]);

function text(value, label) {
  if (typeof value !== "string" || value.length === 0 || value.length > 4096) {
    throw new Error(`Invalid shared ${label}.`);
  }
  return value;
}

export function safeHttpUrl(value) {
  if (typeof value !== "string" || value.includes("\\")) return null;
  let url;
  try {
    url = new URL(value);
  } catch {
    return null;
  }
  if (!["https:", "http:"].includes(url.protocol) || url.username || url.password || url.hash) {
    return null;
  }
  return url.href;
}

export function normalizeSelection(selection) {
  if (!selection || typeof selection !== "object") throw new Error("Invalid shared selection.");
  const kind = selection.kind;
  const backend = selection.backend;
  const url = safeHttpUrl(selection.url);
  if (!url) throw new Error("Invalid shared target URL.");

  if (kind === "encrypted") {
    if (!ENCRYPTED_BACKENDS.has(backend)) {
      throw new Error("Invalid encrypted share backend.");
    }
    const kdf = selection.kdf;
    if (!KDFS.has(kdf)) {
      throw new Error("Invalid encrypted share key mode.");
    }
    const normalized = { kind, backend, url, kdf };
    if (selection.secret !== undefined && selection.secret !== null && selection.secret !== "") {
      normalized.secret = text(selection.secret, "decryption secret");
    }
    return normalized;
  }

  const mode = selection.mode;
  if (kind !== "file" && kind !== "directory") throw new Error("Invalid shared item kind.");
  if (!BACKENDS.has(backend) || (mode !== "preview" && mode !== "raw")) {
    throw new Error("Invalid shared link selection.");
  }
  const normalized = {
    kind,
    name: text(selection.name, "name"),
    backend,
    mode,
    url,
  };
  if (kind === "directory") {
    if (selection.rootStatus !== "complete" && selection.rootStatus !== "partial") {
      throw new Error("Invalid shared directory root status.");
    }
    normalized.rootStatus = selection.rootStatus;
  }
  return normalized;
}

export function encodeShare(selection) {
  const normalized = normalizeSelection(selection);
  if (normalized.kind === "encrypted") {
    const params = new URLSearchParams({
      kind: normalized.kind,
      backend: normalized.backend,
      url: normalized.url,
      kdf: normalized.kdf,
    });
    if (normalized.secret) {
      params.set(normalized.kdf === "passphrase" ? "p" : "k", normalized.secret);
    }
    return SHARE_PREFIX + params;
  }
  return SHARE_PREFIX + new URLSearchParams(normalized);
}

export function decodeShare(hash) {
  if (!hash.startsWith(SHARE_PREFIX)) throw new Error("This link does not contain a shared item.");
  const parameters = new URLSearchParams(hash.slice(SHARE_PREFIX.length));
  const kind = parameters.get("kind");
  if (kind === "encrypted") {
    const kdf = parameters.get("kdf");
    const k = parameters.get("k");
    const p = parameters.get("p");
    if (k !== null && p !== null) {
      throw new Error("Encrypted share link cannot contain both key and passphrase.");
    }
    if ((k !== null && kdf !== "raw") || (p !== null && kdf !== "passphrase")) {
      throw new Error("Encrypted share link key does not match its key mode.");
    }
    return normalizeSelection({
      kind,
      backend: parameters.get("backend"),
      url: parameters.get("url"),
      kdf,
      secret: kdf === "passphrase" ? p : k,
    });
  }
  return normalizeSelection({
    kind,
    name: parameters.get("name"),
    backend: parameters.get("backend"),
    mode: parameters.get("mode"),
    url: parameters.get("url"),
    rootStatus: parameters.get("rootStatus") ?? undefined,
  });
}

function validCid(cid) {
  if (typeof cid !== "string" || !/^[A-Za-z0-9]{1,128}$/.test(cid)) {
    throw new Error("Invalid CID for an IPFS link.");
  }
  return cid;
}

export function ipfsUrl(gateway, cid, { name, download = false, directory = false } = {}) {
  const base = new URL(gateway);
  if (!["https:", "http:"].includes(base.protocol) || base.username || base.password) {
    throw new Error("Invalid IPFS gateway.");
  }
  const url = new URL(validCid(cid) + (directory ? "/" : ""), base.href.endsWith("/") ? base : base.href + "/");
  if (name) url.searchParams.set("filename", name);
  if (download) url.searchParams.set("download", "true");
  return url.href;
}

export function directoryPrefix(key) {
  const parent = key.slice(0, key.lastIndexOf("/") + 1);
  return `${parent}files/`;
}

export function directoryListUrl(result) {
  const safe = safeHttpUrl(result.url);
  if (!safe) throw new Error("The S3 object URL is invalid.");
  const source = new URL(safe);
  const bucket = source.pathname.match(/^\/[^/]+\//)?.[0];
  if (!bucket) throw new Error("The S3 object URL is not path-style.");
  const url = new URL(source.origin + bucket);
  url.searchParams.set("list-type", "2");
  url.searchParams.set("prefix", directoryPrefix(result.key));
  return url.href;
}

export function objectUrlFromList(listUrl, key) {
  const url = new URL(listUrl);
  url.pathname = url.pathname.replace(/\/$/, "") + "/" + key.split("/").map(encodeURIComponent).join("/");
  url.search = "";
  return url.href;
}

export async function probePublicUrl(value, fetchImpl = fetch) {
  const url = safeHttpUrl(value);
  if (!url) return { state: "unknown", message: "The object URL is not a safe HTTP(S) URL." };
  try {
    const response = await fetchImpl(url, {
      method: "HEAD",
      credentials: "omit",
      redirect: "follow",
      cache: "no-store",
      referrerPolicy: "no-referrer",
    });
    if (response.ok) return { state: "public", message: "Anonymous HEAD succeeded." };
    return { state: "unavailable", message: `Anonymous HEAD returned HTTP ${response.status}.` };
  } catch {
    return { state: "unknown", message: "The check failed; the object may be private or CORS may be blocked." };
  }
}

export function formatBytes(value) {
  if (!Number.isSafeInteger(value) || value < 0) return "Unknown size";
  if (value < 1024) return `${value} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let amount = value;
  let unit = "B";
  for (const next of units) {
    if (amount < 1024) break;
    amount /= 1024;
    unit = next;
  }
  return `${amount.toFixed(amount >= 10 ? 1 : 2)} ${unit}`;
}
