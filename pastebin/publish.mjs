import { encodeS3Component, signRequest } from './sigv4.mjs';
import { instanceTarget } from './settings.js';

const MIB = 1024 * 1024;
export const MULTIPART_THRESHOLD_BYTES = 6 * MIB;
export const MIN_PART_SIZE_BYTES = 5 * MIB;
export const MAX_PART_SIZE_BYTES = 64 * MIB;
export const MAX_MULTIPART_PARTS = 10_000;

const utf8Encoder = new TextEncoder();

const s3ErrorMessages = new Map([
  ['AccessDenied', 'The bucket denied this upload; publishing credentials may be required'],
  ['QuotaExceeded', 'The S3 bucket is full'],
  ['EntityTooLarge', 'The upload exceeds the gateway size limit'],
  ['EntityTooSmall', 'A multipart upload part was smaller than the gateway minimum'],
  ['NoSuchBucket', 'The configured S3 bucket does not exist'],
  ['InvalidAccessKeyId', 'The gateway did not recognize the access key'],
  ['SignatureDoesNotMatch', 'The gateway could not verify the upload signature'],
  ['ExpiredToken', 'The S3 session has expired'],
  ['InvalidToken', 'The S3 session token is invalid'],
  ['InvalidRequest', 'The gateway rejected the upload parameters'],
  ['SlowDown', 'The gateway rate limit was reached'],
]);

function readCid(value) {
  const cid = value?.trim().replace(/^"([^"]+)"$/, '$1').trim();
  return /^(?:Qm[1-9A-HJ-NP-Za-km-z]{44}|b[a-z2-7]{20,})$/.test(cid) ? cid : undefined;
}

function escapeXmlText(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;');
}

function decodeXmlEntities(value) {
  return value
    .replaceAll('&quot;', '"')
    .replaceAll('&apos;', "'")
    .replaceAll('&lt;', '<')
    .replaceAll('&gt;', '>')
    .replaceAll('&amp;', '&');
}

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

function isRetryableStatus(status) {
  return status === 408 || status === 429 || status >= 500;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function planMultipartUpload(totalBytes, options = {}) {
  const thresholdBytes = options.thresholdBytes ?? MULTIPART_THRESHOLD_BYTES;
  const minPartSize = options.minPartSize ?? MIN_PART_SIZE_BYTES;
  const maxPartSize = options.maxPartSize ?? MAX_PART_SIZE_BYTES;
  if (!Number.isSafeInteger(totalBytes) || totalBytes <= thresholdBytes) {
    return null;
  }

  let partSize;
  let initialConcurrency;
  let maxConcurrency;
  if (totalBytes <= 32 * MIB) {
    partSize = 5 * MIB;
    initialConcurrency = 2;
    maxConcurrency = 4;
  } else if (totalBytes <= 128 * MIB) {
    partSize = 8 * MIB;
    initialConcurrency = 3;
    maxConcurrency = 5;
  } else if (totalBytes <= 512 * MIB) {
    partSize = 16 * MIB;
    initialConcurrency = 4;
    maxConcurrency = 6;
  } else {
    partSize = 32 * MIB;
    initialConcurrency = 4;
    maxConcurrency = 6;
  }

  if (options.partSize) {
    partSize = options.partSize;
  } else {
    partSize = Math.max(minPartSize, Math.min(maxPartSize, partSize));
  }

  const requiredMinForMaxParts = Math.ceil(totalBytes / MAX_MULTIPART_PARTS);
  if (partSize < requiredMinForMaxParts) {
    partSize = requiredMinForMaxParts;
  }

  const partCount = Math.ceil(totalBytes / partSize);
  const hwCap = typeof globalThis.navigator?.hardwareConcurrency === 'number'
    ? Math.max(2, Math.min(6, globalThis.navigator.hardwareConcurrency))
    : 6;
  const effectiveMax = Math.max(1, Math.min(options.maxConcurrency ?? maxConcurrency, hwCap, partCount));
  const effectiveInitial = Math.max(1, Math.min(options.initialConcurrency ?? initialConcurrency, effectiveMax));

  return {
    partSize,
    partCount,
    initialConcurrency: effectiveInitial,
    maxConcurrency: effectiveMax,
  };
}

function buildCompleteMultipartXml(parts) {
  const items = parts
    .map(({ partNumber, etag }) => `<Part><PartNumber>${partNumber}</PartNumber><ETag>${escapeXmlText(etag)}</ETag></Part>`)
    .join('');
  return `<CompleteMultipartUpload>${items}</CompleteMultipartUpload>`;
}

function parseUploadId(xml) {
  const match = /<UploadId>([^<]+)<\/UploadId>/.exec(xml);
  return match ? decodeXmlEntities(match[1]).trim() : null;
}

function parseCompleteEtag(xml) {
  const match = /<ETag>([^<]+)<\/ETag>/.exec(xml);
  return match ? readCid(decodeXmlEntities(match[1])) : null;
}

async function sendS3Request(method, url, bytes, headers, region, signedCredentials) {
  if (signedCredentials) {
    await signRequest(method, url, bytes, headers, region, signedCredentials);
  }
  return fetch(url, {
    method,
    headers,
    body: bytes && bytes.byteLength > 0 ? bytes : undefined,
    redirect: 'error',
    credentials: 'omit',
    referrerPolicy: 'no-referrer',
  });
}

async function finalizePublishResponse(response, { mode, key, url, name, prefix, isMultipart }) {
  const result = { key, url: url.href, name, kind: mode };
  if (mode === 'file') {
    let cid = readCid(response.headers.get('x-amz-meta-ipfs-cid')) ?? readCid(response.headers.get('etag'));
    if (!cid && isMultipart) {
      const xml = await response.text().catch(() => '');
      cid = parseCompleteEtag(xml);
    }
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

async function publishMultipart({
  mode,
  body,
  name,
  key,
  url,
  prefix,
  region,
  signedCredentials,
  plan,
  onProgress,
  retryDelaysMs = [400, 1200],
}) {
  const totalBytes = body.size;
  const initUrl = new URL(url);
  initUrl.search = mode === 'directory'
    ? `decompress-zip=${encodeS3Component(prefix)}&uploads=`
    : 'uploads=';
  const initHeaders = new Headers({ 'content-type': body.type || 'application/octet-stream' });
  if (mode === 'directory') {
    initHeaders.set('x-amz-tagging', 'ipfs-s3%3Azip-root=true');
  }

  onProgress?.({
    phase: 'init',
    uploadedBytes: 0,
    totalBytes,
    completedParts: 0,
    totalParts: plan.partCount,
    concurrency: plan.initialConcurrency,
  });

  let initResponse;
  try {
    initResponse = await sendS3Request('POST', initUrl, new Uint8Array(0), initHeaders, region, signedCredentials);
  } catch {
    throw new Error('Could not start multipart upload. Check the endpoint and CORS settings (POST and DELETE must be allowed).');
  }
  if (!initResponse.ok) throw await uploadError(initResponse);
  const initXml = await initResponse.text();
  const uploadId = parseUploadId(initXml);
  if (!uploadId) {
    throw new Error('The gateway did not return a valid multipart upload ID.');
  }

  const completed = new Array(plan.partCount);
  let nextPartIndex = 0;
  let completedParts = 0;
  let uploadedBytes = 0;
  let concurrency = plan.initialConcurrency;
  let consecutiveFast = 0;
  let inFlight = 0;
  let fatalError = null;

  async function uploadSinglePart(partIndex) {
    const partNumber = partIndex + 1;
    const start = partIndex * plan.partSize;
    const end = Math.min(totalBytes, start + plan.partSize);
    const partBytes = await body.slice(start, end).arrayBuffer();

    for (let attempt = 0; ; attempt += 1) {
      if (fatalError) return;
      const partUrl = new URL(url);
      partUrl.search = `partNumber=${partNumber}&uploadId=${encodeS3Component(uploadId)}`;
      const partHeaders = new Headers();
      const startedAt = Date.now();
      try {
        const response = await sendS3Request('PUT', partUrl, partBytes, partHeaders, region, signedCredentials);
        if (!response.ok) {
          if (attempt < retryDelaysMs.length && isRetryableStatus(response.status)) {
            concurrency = Math.max(1, Math.floor(concurrency / 2));
            consecutiveFast = 0;
            await sleep(retryDelaysMs[attempt]);
            continue;
          }
          throw await uploadError(response);
        }
        const rawEtag = response.headers.get('etag')?.trim();
        if (!rawEtag) {
          throw new Error(`Part ${partNumber} uploaded without an ETag header. Check bucket CORS ExposeHeaders.`);
        }
        const durationMs = Date.now() - startedAt;
        completed[partIndex] = { partNumber, etag: rawEtag };
        completedParts += 1;
        uploadedBytes += partBytes.byteLength;

        if (durationMs > 15_000 && concurrency > 2) {
          concurrency -= 1;
          consecutiveFast = 0;
        } else if (durationMs < 8_000) {
          consecutiveFast += 1;
          if (consecutiveFast >= 2 && concurrency < plan.maxConcurrency) {
            concurrency += 1;
            consecutiveFast = 0;
          }
        }

        onProgress?.({
          phase: 'uploading',
          uploadedBytes,
          totalBytes,
          completedParts,
          totalParts: plan.partCount,
          concurrency,
        });
        return;
      } catch (error) {
        const isNetworkError = error instanceof TypeError;
        if (isNetworkError && attempt < retryDelaysMs.length) {
          concurrency = Math.max(1, Math.floor(concurrency / 2));
          consecutiveFast = 0;
          await sleep(retryDelaysMs[attempt]);
          continue;
        }
        throw error;
      }
    }
  }

  try {
    await new Promise((resolve, reject) => {
      const pump = () => {
        if (fatalError) {
          if (inFlight === 0) reject(fatalError);
          return;
        }
        if (nextPartIndex >= plan.partCount) {
          if (inFlight === 0) resolve();
          return;
        }
        while (!fatalError && nextPartIndex < plan.partCount && inFlight < concurrency) {
          const partIndex = nextPartIndex;
          nextPartIndex += 1;
          inFlight += 1;
          uploadSinglePart(partIndex)
            .catch(error => {
              if (!fatalError) fatalError = error;
            })
            .finally(() => {
              inFlight -= 1;
              pump();
            });
        }
      };
      pump();
    });

    onProgress?.({
      phase: 'completing',
      uploadedBytes: totalBytes,
      totalBytes,
      completedParts: plan.partCount,
      totalParts: plan.partCount,
      concurrency,
    });

    const completeUrl = new URL(url);
    completeUrl.search = `uploadId=${encodeS3Component(uploadId)}`;
    const completeXml = utf8Encoder.encode(buildCompleteMultipartXml(completed));
    const completeHeaders = new Headers({ 'content-type': 'application/xml' });
    let completeResponse;
    try {
      completeResponse = await sendS3Request('POST', completeUrl, completeXml, completeHeaders, region, signedCredentials);
    } catch {
      throw new Error('Multipart completion outcome could not be confirmed. The object may already be stored. Check the endpoint and CORS settings before retrying.');
    }
    if (!completeResponse.ok) throw await uploadError(completeResponse);
    return await finalizePublishResponse(completeResponse, { mode, key, url, name, prefix, isMultipart: true });
  } catch (error) {
    const abortUrl = new URL(url);
    abortUrl.search = `uploadId=${encodeS3Component(uploadId)}`;
    await sendS3Request('DELETE', abortUrl, new Uint8Array(0), new Headers(), region, signedCredentials).catch(() => {});
    throw error;
  }
}

export async function publish({ mode, body, name, credentials, onProgress }, config) {
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
  const prefix = `${baseKey}files/`;

  const plan = planMultipartUpload(body.size, config.multipart);
  if (plan) {
    return publishMultipart({
      mode,
      body,
      name,
      key,
      url,
      prefix,
      region,
      signedCredentials,
      plan,
      onProgress,
      retryDelaysMs: config.multipart?.retryDelaysMs,
    });
  }

  const uploadUrl = new URL(url);
  const headers = new Headers({ 'content-type': body.type || 'application/octet-stream' });
  if (mode === 'directory') {
    uploadUrl.search = `decompress-zip=${encodeS3Component(prefix)}`;
    headers.set('x-amz-tagging', 'ipfs-s3%3Azip-root=true');
  }

  const bytes = await body.arrayBuffer();
  let response;
  try {
    response = await sendS3Request('PUT', uploadUrl, bytes, headers, region, signedCredentials);
  } catch {
    throw new Error('Upload outcome could not be confirmed. The object may already be stored. Check the endpoint and CORS settings before retrying.');
  }
  if (!response.ok) throw await uploadError(response);

  return finalizePublishResponse(response, { mode, key, url, name, prefix, isMultipart: false });
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

  const archiveCid = readCid(text(root, 'ArchiveCid'));
  if (!archiveCid) throw invalid;
  const sourceKey = text(root, 'SourceKey');
  const extractPrefix = text(root, 'Prefix');
  if (sourceKey !== key || extractPrefix !== prefix) throw invalid;
  size(root, 'ArchiveSize');
  const extractedCount = size(root, 'ExtractedCount');
  const failedCount = size(root, 'FailedCount');
  const entries = list('Entries', 'Entry', entry => {
    const entryKey = text(entry, 'Key');
    const cid = readCid(text(entry, 'Cid'));
    if (!entryKey.startsWith(prefix) || !cid) throw invalid;
    return { key: entryKey, cid, size: size(entry, 'Size') };
  });
  const failures = list('Failures', 'Failure', failure => ({
    name: text(failure, 'Name'),
    code: text(failure, 'Code'),
    message: text(failure, 'Message'),
  }));
  if (entries.length !== extractedCount || failures.length !== failedCount) throw invalid;
  if (rootStatus === 'partial' ? failures.length === 0 : failures.length !== 0) throw invalid;
  return { archiveCid, entries, failures };
}
