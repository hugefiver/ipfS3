const MAGIC = new Uint8Array([0x49, 0x50, 0x46, 0x53, 0x33, 0x45, 0x31, 0x00]); // "IPFS3E1\0"
const KDF_RAW = 0;
const KDF_PBKDF2 = 1;
const FIXED_HEADER_BYTES = 14; // magic(8) + kdf(1) + saltLen(1) + iterations(4)
const SALT_BYTES = 16;
const IV_BYTES = 12;
const KEY_BYTES = 32;
const GCM_TAG_BYTES = 16;

export const DEFAULT_PBKDF2_ITERATIONS = 600_000;
const MAX_PBKDF2_ITERATIONS = 5_000_000;

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder("utf-8", { fatal: true });

function requireWebCrypto() {
  if (globalThis.isSecureContext === false || !globalThis.crypto?.subtle) {
    throw new Error("Client-side encryption requires a secure browser context with WebCrypto.");
  }
  return globalThis.crypto;
}

export function encodeBase64Url(bytes) {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 1) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function decodeBase64Url(text) {
  if (typeof text !== "string") throw new Error("Invalid decryption key.");
  const trimmed = text.trim();
  if (!trimmed || !/^[A-Za-z0-9+/_-]+={0,2}$/.test(trimmed)) {
    throw new Error("Invalid decryption key format.");
  }
  const normalized = trimmed.replace(/-/g, "+").replace(/_/g, "/");
  const remainder = normalized.length % 4;
  if (remainder === 1) throw new Error("Invalid decryption key format.");
  const padded = remainder === 0 ? normalized : normalized + "=".repeat(4 - remainder);
  let binary;
  try {
    binary = atob(padded);
  } catch {
    throw new Error("Invalid decryption key format.");
  }
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

export function generateRandomKey() {
  const cryptoApi = requireWebCrypto();
  const key = new Uint8Array(KEY_BYTES);
  cryptoApi.getRandomValues(key);
  return encodeBase64Url(key);
}

function encodeMetadata({ name, type = "application/octet-stream", kind = "file" }) {
  return utf8Encoder.encode(
    JSON.stringify({
      name: typeof name === "string" && name ? name : "decrypted.bin",
      type: typeof type === "string" && type ? type : "application/octet-stream",
      kind: kind === "text" ? "text" : "file",
    }),
  );
}

export function estimateEncryptedSize(plainByteLength, { name, type, kind, kdf = "raw" } = {}) {
  const saltLen = kdf === "passphrase" ? SALT_BYTES : 0;
  const metaLen = encodeMetadata({ name, type, kind }).byteLength;
  return FIXED_HEADER_BYTES + saltLen + IV_BYTES + 4 + metaLen + plainByteLength + GCM_TAG_BYTES;
}

function toUint8Array(input) {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) {
    return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  }
  throw new Error("Expected binary input.");
}

async function importRawAesKey(cryptoApi, rawKeyBytes, usages) {
  return cryptoApi.subtle.importKey("raw", rawKeyBytes, { name: "AES-GCM" }, false, usages);
}

async function derivePassphraseAesKey(cryptoApi, passphrase, salt, iterations, usages) {
  const baseKey = await cryptoApi.subtle.importKey(
    "raw",
    utf8Encoder.encode(passphrase),
    "PBKDF2",
    false,
    ["deriveKey"],
  );
  return cryptoApi.subtle.deriveKey(
    { name: "PBKDF2", hash: "SHA-256", salt, iterations },
    baseKey,
    { name: "AES-GCM", length: 256 },
    false,
    usages,
  );
}

export async function encryptEnvelope(
  plaintext,
  {
    name = "paste.txt",
    type = "application/octet-stream",
    kind = "file",
    kdf = "raw",
    passphrase = "",
    iterations = DEFAULT_PBKDF2_ITERATIONS,
  } = {},
) {
  const cryptoApi = requireWebCrypto();
  if (kdf !== "raw" && kdf !== "passphrase") {
    throw new Error("Unsupported encryption key mode.");
  }

  const contentBytes = toUint8Array(plaintext);
  const metaBytes = encodeMetadata({ name, type, kind });
  const iv = new Uint8Array(IV_BYTES);
  cryptoApi.getRandomValues(iv);

  let kdfId;
  let salt;
  let usedIterations;
  let aesKey;
  let secret;

  if (kdf === "raw") {
    kdfId = KDF_RAW;
    salt = new Uint8Array(0);
    usedIterations = 0;
    const rawKey = new Uint8Array(KEY_BYTES);
    cryptoApi.getRandomValues(rawKey);
    aesKey = await importRawAesKey(cryptoApi, rawKey, ["encrypt"]);
    secret = encodeBase64Url(rawKey);
  } else {
    if (typeof passphrase !== "string" || passphrase.length === 0) {
      throw new Error("Enter a passphrase to encrypt this item.");
    }
    if (!Number.isSafeInteger(iterations) || iterations < 1 || iterations > MAX_PBKDF2_ITERATIONS) {
      throw new Error("Invalid PBKDF2 iteration count.");
    }
    kdfId = KDF_PBKDF2;
    salt = new Uint8Array(SALT_BYTES);
    cryptoApi.getRandomValues(salt);
    usedIterations = iterations;
    aesKey = await derivePassphraseAesKey(cryptoApi, passphrase, salt, usedIterations, ["encrypt"]);
    secret = passphrase;
  }

  const header = new Uint8Array(FIXED_HEADER_BYTES + salt.byteLength + IV_BYTES);
  const headerView = new DataView(header.buffer);
  header.set(MAGIC, 0);
  header[8] = kdfId;
  header[9] = salt.byteLength;
  headerView.setUint32(10, usedIterations, false);
  header.set(salt, FIXED_HEADER_BYTES);
  header.set(iv, FIXED_HEADER_BYTES + salt.byteLength);

  const inner = new Uint8Array(4 + metaBytes.byteLength + contentBytes.byteLength);
  new DataView(inner.buffer).setUint32(0, metaBytes.byteLength, false);
  inner.set(metaBytes, 4);
  inner.set(contentBytes, 4 + metaBytes.byteLength);

  const ciphertext = new Uint8Array(
    await cryptoApi.subtle.encrypt(
      { name: "AES-GCM", iv, additionalData: header },
      aesKey,
      inner,
    ),
  );

  const payload = new Uint8Array(header.byteLength + ciphertext.byteLength);
  payload.set(header, 0);
  payload.set(ciphertext, header.byteLength);

  return { payload, kdf, secret };
}

export function inspectEnvelope(input) {
  const bytes = toUint8Array(input);
  if (bytes.byteLength < FIXED_HEADER_BYTES + IV_BYTES + GCM_TAG_BYTES + 4) {
    throw new Error("The encrypted payload is truncated or invalid.");
  }
  for (let i = 0; i < MAGIC.length; i += 1) {
    if (bytes[i] !== MAGIC[i]) {
      throw new Error("Unsupported or unrecognized encrypted payload format.");
    }
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const kdfId = bytes[8];
  const saltLen = bytes[9];
  const iterations = view.getUint32(10, false);

  let kdf;
  if (kdfId === KDF_RAW) {
    if (saltLen !== 0 || iterations !== 0) {
      throw new Error("Invalid encryption header for raw key mode.");
    }
    kdf = "raw";
  } else if (kdfId === KDF_PBKDF2) {
    if (saltLen < 8 || saltLen > 64 || iterations < 1 || iterations > MAX_PBKDF2_ITERATIONS) {
      throw new Error("Invalid encryption header for passphrase mode.");
    }
    kdf = "passphrase";
  } else {
    throw new Error("Unsupported encryption key derivation mode.");
  }

  const headerLen = FIXED_HEADER_BYTES + saltLen + IV_BYTES;
  if (bytes.byteLength < headerLen + GCM_TAG_BYTES + 4) {
    throw new Error("The encrypted payload is truncated or invalid.");
  }

  const salt = bytes.subarray(FIXED_HEADER_BYTES, FIXED_HEADER_BYTES + saltLen);
  const iv = bytes.subarray(FIXED_HEADER_BYTES + saltLen, headerLen);
  const header = bytes.subarray(0, headerLen);
  const ciphertext = bytes.subarray(headerLen);

  return { kdf, salt, iterations, iv, header, ciphertext };
}

function sanitizeDecryptedName(name) {
  if (typeof name !== "string") return "decrypted.bin";
  const cleaned = name.replace(/[\/\\\x00-\x1f\x7f]/g, "_").trim();
  if (!cleaned || cleaned === "." || cleaned === "..") return "decrypted.bin";
  return cleaned.slice(0, 255);
}

export async function decryptEnvelope(input, { secret } = {}) {
  const cryptoApi = requireWebCrypto();
  const parsed = inspectEnvelope(input);
  if (typeof secret !== "string" || secret.length === 0) {
    throw new Error(
      parsed.kdf === "passphrase"
        ? "Enter the passphrase to decrypt this item."
        : "Enter the decryption key to decrypt this item.",
    );
  }

  let aesKey;
  if (parsed.kdf === "raw") {
    const rawKey = decodeBase64Url(secret);
    if (rawKey.byteLength !== KEY_BYTES) {
      throw new Error("Invalid decryption key length; expected a 32-byte key.");
    }
    aesKey = await importRawAesKey(cryptoApi, rawKey, ["decrypt"]);
  } else {
    aesKey = await derivePassphraseAesKey(
      cryptoApi,
      secret,
      parsed.salt,
      parsed.iterations,
      ["decrypt"],
    );
  }

  let innerBuffer;
  try {
    innerBuffer = await cryptoApi.subtle.decrypt(
      { name: "AES-GCM", iv: parsed.iv, additionalData: parsed.header },
      aesKey,
      parsed.ciphertext,
    );
  } catch {
    throw new Error(
      parsed.kdf === "passphrase"
        ? "Decryption failed. Check the passphrase or verify the payload is intact."
        : "Decryption failed. Check the decryption key or verify the payload is intact.",
    );
  }

  const inner = new Uint8Array(innerBuffer);
  if (inner.byteLength < 4) {
    throw new Error("Decrypted payload metadata is invalid.");
  }
  const metaLen = new DataView(inner.buffer, inner.byteOffset, inner.byteLength).getUint32(0, false);
  if (metaLen === 0 || 4 + metaLen > inner.byteLength) {
    throw new Error("Decrypted payload metadata is invalid.");
  }

  let meta;
  try {
    meta = JSON.parse(utf8Decoder.decode(inner.subarray(4, 4 + metaLen)));
  } catch {
    throw new Error("Decrypted payload metadata could not be parsed.");
  }

  return {
    bytes: inner.slice(4 + metaLen),
    name: sanitizeDecryptedName(meta?.name),
    type: typeof meta?.type === "string" && meta.type ? meta.type : "application/octet-stream",
    kind: meta?.kind === "text" ? "text" : "file",
    kdf: parsed.kdf,
  };
}
