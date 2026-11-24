import { encodeS3Component, signPut } from './sigv4.mjs';
import { instanceTarget } from './settings.js';

function readCid(value) {
  const cid = value?.trim().replace(/^"([^"]+)"$/, '$1').trim();
  return /^(?:Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,})$/.test(cid) ? cid : undefined;
}

const s3ErrorMessages = new Map([
  ['AccessDenied', 'The bucket denied this upload; publishing credentials may be required'],
  ['QuotaExceeded', 'The S3 bucket is full'],
  ['EntityTooLarge', 'The upload exceeds the gateway size limit'],
  ['NoSuchBucket', 'The configured S3 bucket does not exist'],
  ['InvalidAccessKeyId', 'The gateway did not recognize the access key'],
  ['SignatureDoesNotMatch', 'The gateway could not verify the upload signature'],
  ['ExpiredToken', 'The S3 session has expired'],
]);

function normalizeCredentials(credentials, mode) {
  const accessKeyId = credentials?.accessKeyId ?? '';
  const secretAccessKey = credentials?.secretAccessKey ?? '';
  const sessionToken = credentials?.sessionToken ?? '';
  const hasAny = Boolean(accessKeyId || secretAccessKey || sessionToken);
  if (!hasAny) {
    if (mode === 'directory') {
      throw new Error('ZIP directory publishing requires S3 credentials.');
    }
    return null;
  }
  if (!accessKeyId || !secretAccessKey) {
    throw new Error('Enter both Access key ID and Secret access key, or leave all credential fields empty for anonymous publishing.');
  }
  if (/[^!-~]|[/,]/.test(accessKeyId) || (sessionToken && /[^!-~]/.test(sessionToken))) {
    throw new Error('Enter valid S3 credentials.');
  }
  return sessionToken
    ? { accessKeyId, secretAccessKey, sessionToken }
    : { accessKeyId, secretAccessKey };
}

async function uploadError(response) {
  const xml = await response.text().catch(() => '');
  const code = /^(?:<\?xml[^>]*\?>\s*)?<Error>\s*<Code>([A-Za-z]+)<\/Code>/.exec(xml.trim())?.[1];
  const reason = s3ErrorMessages.get(code);
  if (reason) return new Error(`${reason} (HTTP ${response.status}).`);
  if (response.status >= 500) {
    return new Error(`The gateway returned HTTP ${response.status}. The object may already be stored.`);
  }
  return new Error(`The gateway rejected the upload (HTTP ${response.status}).`);
}

export async function publish({ mode, body, name, credentials }, config) {
  const { maxUploadBytes } = config;
  if (mode !== 'file' && mode !== 'directory') throw new Error('Choose file or directory publishing.');
  const signedCredentials = normalizeCredentials(credentials, mode);
  if (signedCredentials && (globalThis.isSecureContext === false || !globalThis.crypto?.subtle)) {
    throw new Error('Publishing with credentials requires a secure browser context with WebCrypto.');
  }
  if (!Number.isSafeInteger(maxUploadBytes) || maxUploadBytes < 1) throw new Error('Set a valid upload size limit.');
  if (body.size > maxUploadBytes) throw new Error('This upload exceeds the site size limit.');
  if (!name || /^\.{1,2}$/.test(name) || /[\/\\\x00-\x1f\x7f\uD800-\uDFFF]/u.test(name)) {
    throw new Error('Use a file name without path separators or control characters.');
  }
  const { endpoint: origin, bucket, region } = instanceTarget(config);

  const baseKey = `pastebin/${crypto.randomUUID()}/`;
  const key = `${baseKey}${name}`;
  const url = new URL(`/${[bucket, ...key.split('/')].map(encodeS3Component).join('/')}`, origin);
  const uploadUrl = new URL(url);
  const prefix = `${baseKey}files/`;
  const headers = new Headers({ 'content-type': body.type || 'application/octet-stream' });
  if (mode === 'directory') {
    uploadUrl.search = `decompress-zip=${encodeS3Component(prefix)}`;
    headers.set('x-amz-tagging', 'ipfs-s3%3Azip-root=true');
  }

  const bytes = await body.arrayBuffer();
  if (signedCredentials) {
    await signPut(uploadUrl, bytes, headers, region, signedCredentials);
  }
  let response;
  try {
    response = await fetch(uploadUrl, {
      method: 'PUT',
      headers,
      body: bytes,
      redirect: 'error',
      credentials: 'omit',
      referrerPolicy: 'no-referrer',
    });
  } catch {
    throw new Error('Upload outcome could not be confirmed. The object may already be stored. Check the endpoint and CORS settings before retrying.');
  }
  if (!response.ok) throw await uploadError(response);

  const result = { key, url: url.href, name, kind: mode };
  if (mode === 'file') {
    const cid = readCid(response.headers.get('x-amz-meta-ipfs-cid')) ?? readCid(response.headers.get('etag'));
    if (!cid) {
      throw new Error('The upload was stored, but its CID could not be read. Check that CORS exposes ETag or x-amz-meta-ipfs-cid.');
    }
    return { ...result, cid };
  }

  const cid = readCid(response.headers.get('x-ipfs-s3-zip-root-cid'));
  const rootStatus = response.headers.get('x-ipfs-s3-zip-root-status');
  if (!cid || (rootStatus !== 'complete' && rootStatus !== 'partial')) {
    throw new Error('The ZIP was stored, but no shareable directory root could be read. Check the gateway’s ZIP root and CORS settings.');
  }

  let xml;
  try {
    xml = await response.text();
  } catch {
    throw new Error('The ZIP was stored, but the extraction result could not be read.');
  }
  return { ...result, cid, rootStatus, ...readZipResult(xml, key, prefix, rootStatus) };
}

function readZipResult(source, key, prefix, rootStatus) {
  const doc = new DOMParser().parseFromString(source, 'application/xml');
  const root = doc.documentElement;
  const invalid = new Error('The ZIP was stored, but its extraction result was incomplete or invalid.');
  if (root.tagName !== 'DecompressZipResult' || doc.querySelector('parsererror')) throw invalid;

  function child(parent, name) {
    const matches = parent.querySelectorAll(`:scope > ${name}`);
    if (matches.length !== 1) throw invalid;
    return matches[0];
  }

  function text(parent, name) {
    const element = child(parent, name);
    if (element.childElementCount) throw invalid;
    return element.textContent;
  }

  function size(parent, name) {
    const value = text(parent, name).trim();
    const number = Number(value);
    if (!/^\d+$/.test(value) || !Number.isSafeInteger(number)) throw invalid;
    return number;
  }

  function list(name, item, parse) {
    return Array.from(child(root, name).children, element => {
      if (element.tagName !== item) throw invalid;
      return parse(element);
    });
  }

  if (text(root, 'ArchiveKey') !== key) throw invalid;
  const archiveCid = readCid(text(root, 'ArchiveETag'));
  size(root, 'ArchiveSize');
  const extractedCount = size(root, 'ExtractedCount');
  const failedCount = size(root, 'FailedCount');
  const entries = list('Entries', 'Entry', entry => {
    const key = text(entry, 'Key');
    const cid = readCid(text(entry, 'ETag'));
    if (!key.startsWith(prefix) || !cid) throw invalid;
    return { key, cid, size: size(entry, 'Size') };
  });
  const failures = list('Failures', 'Failure', failure => ({
    name: text(failure, 'EntryName'), code: text(failure, 'Code'), message: text(failure, 'Message'),
  }));
  if (!archiveCid || !entries.length || entries.length !== extractedCount ||
      failures.length !== failedCount || (rootStatus === 'partial') !== (failedCount > 0)) throw invalid;
  return { archiveCid, entries, failures };
}
