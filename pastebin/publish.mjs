import { encodeS3Component, signPut } from './sigv4.mjs';

function endpointOrigin(endpoint) {
  const invalid = 'Use an HTTPS endpoint origin, or HTTP on localhost, 127.0.0.1 or [::1].';
  const match = /^https?:\/\/([^/\s?#@\\%]+)\/?$/i.exec(endpoint);
  if (!match || match[0] !== endpoint) throw new Error(invalid);

  let url;
  try {
    url = new URL(endpoint);
  } catch {
    throw new Error(invalid);
  }
  const local = /^(?:localhost|127\.0\.0\.1|\[::1\])(?::\d+)?$/i.test(match[1]);
  if (url.protocol === 'http:' && !local) throw new Error(invalid);
  return url.origin;
}

function readCid(value) {
  const cid = value?.trim().replace(/^"([^"]+)"$/, '$1').trim();
  return /^(?:Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,})$/.test(cid) ? cid : undefined;
}

const s3ErrorMessages = new Map([
  ['QuotaExceeded', 'The S3 bucket is full'],
  ['EntityTooLarge', 'The upload exceeds the gateway size limit'],
  ['NoSuchBucket', 'The configured S3 bucket does not exist'],
  ['InvalidAccessKeyId', 'The gateway did not recognize the access key'],
  ['SignatureDoesNotMatch', 'The gateway could not verify the upload signature'],
  ['ExpiredToken', 'The S3 session has expired'],
]);

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
  const { endpoint, bucket, region, maxUploadBytes } = config;
  if (globalThis.isSecureContext === false || !globalThis.crypto?.subtle) {
    throw new Error('Publishing requires a secure browser context with WebCrypto.');
  }
  if (mode !== 'file' && mode !== 'directory') throw new Error('Choose file or directory publishing.');
  if (!Number.isSafeInteger(maxUploadBytes) || maxUploadBytes < 1) throw new Error('Set a valid upload size limit.');
  if (body.size > maxUploadBytes) throw new Error('This upload exceeds the site size limit.');
  if (!name || /^\.{1,2}$/.test(name) || /[\/\\\x00-\x1f\x7f\uD800-\uDFFF]/u.test(name)) {
    throw new Error('Use a file name without path separators or control characters.');
  }
  const origin = endpointOrigin(endpoint);
  if (bucket?.trim() !== bucket || !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(bucket ?? '')) {
    throw new Error('Use a valid S3 bucket name.');
  }
  if (region?.trim() !== region || !/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(region ?? '')) {
    throw new Error('Use a valid S3 region.');
  }
  if (!credentials?.accessKeyId || /[^!-~]|[/,]/.test(credentials.accessKeyId) ||
      !credentials.secretAccessKey || (credentials.sessionToken && /[^!-~]/.test(credentials.sessionToken))) {
    throw new Error('Enter valid S3 credentials.');
  }

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
  await signPut(uploadUrl, bytes, headers, region, credentials);
  let response;
  try {
    response = await fetch(uploadUrl, { method: 'PUT', headers, body: bytes, redirect: 'error', credentials: 'omit' });
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

const xmlEscapes = { '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'" };

// Decode only the gateway's v1 result format, without interpreting arbitrary XML markup.
function readZipResult(source, key, prefix, rootStatus) {
  const xml = source.trim();
  const invalid = new Error('The ZIP was stored, but its extraction result was incomplete or invalid.');
  let offset = 0;

  function tag(name) {
    const expected = `<${name}>`;
    if (!xml.startsWith(expected, offset)) throw invalid;
    offset += expected.length;
  }

  function text(name) {
    tag(name);
    const end = xml.indexOf('<', offset);
    if (end < 0) throw invalid;
    const value = xml.slice(offset, end).replace(/&(?:amp;|lt;|gt;|quot;|apos;)?/g, entity => {
      if (entity === '&') throw invalid;
      return xmlEscapes[entity];
    });
    offset = end;
    tag(`/${name}`);
    return value;
  }

  function size(name) {
    const value = text(name);
    const number = Number(value);
    if (!/^\d+$/.test(value) || !Number.isSafeInteger(number)) throw invalid;
    return number;
  }

  function list(name, item, parse) {
    tag(name);
    const entries = [];
    while (xml.startsWith(`<${item}>`, offset)) {
      tag(item);
      entries.push(parse());
      tag(`/${item}`);
    }
    tag(`/${name}`);
    return entries;
  }

  const declaration = '<?xml version="1.0" encoding="UTF-8"?>';
  if (xml.startsWith(declaration)) offset = declaration.length;
  tag('DecompressZipResult');
  if (text('ArchiveKey') !== key) throw invalid;
  const archiveCid = readCid(text('ArchiveETag'));
  size('ArchiveSize');
  const extractedCount = size('ExtractedCount');
  const failedCount = size('FailedCount');
  const entries = list('Entries', 'Entry', () => {
    const key = text('Key');
    const cid = readCid(text('ETag'));
    if (!key.startsWith(prefix) || !cid) throw invalid;
    return { key, cid, size: size('Size') };
  });
  const failures = list('Failures', 'Failure', () => ({
    name: text('EntryName'), code: text('Code'), message: text('Message'),
  }));
  tag('/DecompressZipResult');
  if (offset !== xml.length || !archiveCid || !entries.length || entries.length !== extractedCount ||
      failures.length !== failedCount || (rootStatus === 'partial') !== (failedCount > 0)) throw invalid;
  return { archiveCid, entries, failures };
}
