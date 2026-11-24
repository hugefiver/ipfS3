import assert from "node:assert/strict";
import test from "node:test";
import {
  decodeBase64Url,
  decryptEnvelope,
  encodeBase64Url,
  encryptEnvelope,
  estimateEncryptedSize,
  generateRandomKey,
  inspectEnvelope,
} from "../crypto.js";

test("random key encrypt/decrypt round-trips UTF-8 text and exact size estimate", async () => {
  const plaintext = new TextEncoder().encode("你好，零知识加密！\n<script>alert(1)</script>");
  const meta = {
    name: "机密 & notes.txt",
    type: "text/plain;charset=utf-8",
    kind: "text",
    kdf: "raw",
  };
  const encrypted = await encryptEnvelope(plaintext, meta);
  assert.equal(encrypted.kdf, "raw");
  assert.equal(decodeBase64Url(encrypted.secret).byteLength, 32);
  assert.equal(encrypted.payload.byteLength, estimateEncryptedSize(plaintext.byteLength, meta));

  const inspected = inspectEnvelope(encrypted.payload);
  assert.equal(inspected.kdf, "raw");
  assert.equal(inspected.iterations, 0);

  const decrypted = await decryptEnvelope(encrypted.payload, { secret: encrypted.secret });
  assert.deepEqual(decrypted.bytes, plaintext);
  assert.equal(decrypted.name, meta.name);
  assert.equal(decrypted.type, meta.type);
  assert.equal(decrypted.kind, "text");
});

test("passphrase PBKDF2 encrypt/decrypt round-trips binary data and empty payload", async () => {
  const binary = new Uint8Array([0, 1, 2, 254, 255, 42, 99]);
  const meta = {
    name: "photo.png",
    type: "image/png",
    kind: "file",
    kdf: "passphrase",
    passphrase: "correct horse battery staple 🚀",
    iterations: 20_000,
  };
  const encrypted = await encryptEnvelope(binary, meta);
  assert.equal(encrypted.kdf, "passphrase");
  assert.equal(encrypted.secret, meta.passphrase);
  assert.equal(encrypted.payload.byteLength, estimateEncryptedSize(binary.byteLength, meta));

  const decrypted = await decryptEnvelope(encrypted.payload, { secret: meta.passphrase });
  assert.deepEqual(decrypted.bytes, binary);
  assert.equal(decrypted.name, "photo.png");
  assert.equal(decrypted.type, "image/png");
  assert.equal(decrypted.kind, "file");

  const empty = await encryptEnvelope(new Uint8Array(0), {
    name: "empty.txt",
    type: "text/plain;charset=utf-8",
    kind: "text",
    kdf: "raw",
  });
  const emptyDecrypted = await decryptEnvelope(empty.payload, { secret: empty.secret });
  assert.equal(emptyDecrypted.bytes.byteLength, 0);
  assert.equal(emptyDecrypted.name, "empty.txt");
});

test("wrong key, wrong passphrase, header tampering, and ciphertext tampering fail authentication", async () => {
  const raw = await encryptEnvelope(new TextEncoder().encode("secret"), {
    name: "a.txt",
    type: "text/plain",
    kind: "text",
    kdf: "raw",
  });
  await assert.rejects(
    decryptEnvelope(raw.payload, { secret: generateRandomKey() }),
    /Decryption failed/,
  );

  // Flip a byte in the ciphertext.
  const tamperedCipher = new Uint8Array(raw.payload);
  tamperedCipher[tamperedCipher.length - 1] ^= 0x01;
  await assert.rejects(
    decryptEnvelope(tamperedCipher, { secret: raw.secret }),
    /Decryption failed/,
  );

  // Flip a byte in the IV (covered by AAD and GCM).
  const tamperedIv = new Uint8Array(raw.payload);
  tamperedIv[14] ^= 0x80;
  await assert.rejects(
    decryptEnvelope(tamperedIv, { secret: raw.secret }),
    /Decryption failed/,
  );

  const byPassphrase = await encryptEnvelope(new TextEncoder().encode("secret"), {
    name: "b.txt",
    type: "text/plain",
    kind: "text",
    kdf: "passphrase",
    passphrase: "right-passphrase",
    iterations: 10_000,
  });
  await assert.rejects(
    decryptEnvelope(byPassphrase.payload, { secret: "wrong-passphrase" }),
    /Decryption failed/,
  );
});

test("rejects malformed headers, truncated envelopes, excessive iterations, and bad keys", async () => {
  await assert.rejects(decryptEnvelope(new Uint8Array(10), { secret: generateRandomKey() }), /truncated/);

  const badMagic = new Uint8Array(64);
  await assert.rejects(decryptEnvelope(badMagic, { secret: generateRandomKey() }), /unrecognized/);

  const hugeIter = await encryptEnvelope(new Uint8Array([1, 2, 3]), {
    name: "x.bin",
    kdf: "passphrase",
    passphrase: "abc",
    iterations: 10_000,
  });
  const forged = new Uint8Array(hugeIter.payload);
  new DataView(forged.buffer).setUint32(10, 99_999_999, false);
  assert.throws(() => inspectEnvelope(forged), /Invalid encryption header/);

  const raw = await encryptEnvelope(new Uint8Array([7]), { kdf: "raw" });
  await assert.rejects(decryptEnvelope(raw.payload, { secret: encodeBase64Url(new Uint8Array(16)) }), /32-byte key/);
  await assert.rejects(decryptEnvelope(raw.payload, { secret: "" }), /Enter the decryption key/);
  await assert.rejects(encryptEnvelope(new Uint8Array([7]), { kdf: "passphrase", passphrase: "" }), /Enter a passphrase/);
});

test("sanitizes path traversal characters in decrypted filenames", async () => {
  const encrypted = await encryptEnvelope(new Uint8Array([65]), {
    name: "../../etc/passwd",
    type: "text/plain",
    kind: "file",
    kdf: "raw",
  });
  const decrypted = await decryptEnvelope(encrypted.payload, { secret: encrypted.secret });
  assert.equal(decrypted.name.includes("/"), false);
  assert.equal(decrypted.name.includes("\\"), false);
  assert.notEqual(decrypted.name, "..");
});
