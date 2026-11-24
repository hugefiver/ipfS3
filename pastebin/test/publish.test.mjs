import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { buffer } from 'node:stream/consumers';
import { test } from 'node:test';
import { MULTIPART_THRESHOLD_BYTES, planMultipartUpload, publish } from '../publish.mjs';
import { canonicalQuery, encodeS3Component, signPut } from '../sigv4.mjs';

const fileCid = 'bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku';
const rootCid = 'bafybeib4r26s6xrk5uqyy6k5lkwxmrsfecmfxluvlz44b2qnt3rottiw3u';
const credentials = {
  accessKeyId: 'paste-test', secretAccessKey: 'fixture-secret', sessionToken: 'fixture-token+/=',
};
const commonConfig = { bucket: 'test-bucket', region: 'us-east-1', maxUploadBytes: 4096 };

async function serve(t, reply) {
  const requests = [];
  const server = createServer(async (req, res) => {
    const request = { method: req.method, url: req.url, headers: req.headers, bytes: await buffer(req) };
    requests.push(request);
    const { status = 200, headers = {}, body } = reply(request);
    res.writeHead(status, headers).end(body);
  });
  server.listen(0, '127.0.0.1');
  t.after(() => new Promise(resolve => server.close(resolve)));
  await once(server, 'listening');
  const endpoint = `http://127.0.0.1:${server.address().port}`;
  return { endpoint, requests, config: { ...commonConfig, endpoint } };
}

function assertSignature(request, zip = false) {
  const { bytes, headers, url } = request;
  const { pathname, search } = new URL(url, `http://${headers.host}`);
  const hash = createHash('sha256').update(bytes).digest('hex');
  assert.equal(headers['x-amz-content-sha256'], hash);
  assert.equal(headers['x-amz-security-token'], credentials.sessionToken);
  assert.equal(headers['x-amz-tagging'], zip ? 'ipfs-s3%3Azip-root=true' : undefined);
  const fields = 'content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token' + (zip ? ';x-amz-tagging' : '');
  const timestamp = headers['x-amz-date'];
  assert.match(timestamp, /^\d{8}T\d{6}Z$/);
  const day = timestamp.slice(0, 8);
  const scope = `${day}/${commonConfig.region}/s3/aws4_request`;
  const canonical = fields.split(';').map(field => `${field}:${headers[field]}\n`).join('');
  const canonicalHash = createHash('sha256')
    .update(`PUT\n${pathname}\n${search.slice(1)}\n${canonical}\n${fields}\n${hash}`).digest('hex');
  const key = scope.split('/').reduce((secret, part) => createHmac('sha256', secret).update(part).digest(),
    `AWS4${credentials.secretAccessKey}`);
  const signature = createHmac('sha256', key)
    .update(`AWS4-HMAC-SHA256\n${timestamp}\n${scope}\n${canonicalHash}`).digest('hex');
  assert.equal(headers.authorization,
    `AWS4-HMAC-SHA256 Credential=${credentials.accessKeyId}/${scope}, SignedHeaders=${fields}, Signature=${signature}`);
}

const file = {
  mode: 'file', name: '笔记 &%2F!🌍.txt',
  body: new Blob(['你好\n<hello>'], { type: 'text/plain;charset=utf-8' }), credentials,
};

const directory = { ...file, mode: 'directory', name: "归档 &'.zip" };

test('signs the exact UTF-8 bytes and path-style file URL with a port and session token', async t => {
  const { config, endpoint, requests } = await serve(t, () => ({ headers: { etag: `"${fileCid}"` } }));
  const limit = { ...config, maxUploadBytes: file.body.size };
  const result = await publish(file, limit);
  const second = await publish(file, limit);
  const [request] = requests;
  assert.equal(request.method, 'PUT');
  assert.equal(request.headers['content-type'], 'text/plain;charset=utf-8');
  assert.equal(request.bytes.toString(), '你好\n<hello>');
  assert.equal(request.url, `/test-bucket/${result.key.slice(0, -file.name.length)}%E7%AC%94%E8%AE%B0%20%26%252F%21%F0%9F%8C%8D.txt`);
  assertSignature(request);
  assert.deepEqual(result, { key: result.key, url: endpoint + request.url, cid: fileCid, name: file.name, kind: 'file' });
  assert.notEqual(result.key, second.key);
});

test('signs the ZIP v1 query and explicit directory-root request', async t => {
  const { config, endpoint, requests } = await serve(t, () => ({ headers: { etag: `"${fileCid}"` } }));
  // A reply without a shareable root ends before browser XML parsing.
  await assert.rejects(publish(directory, config), /no shareable directory root/);
  const [request] = requests;
  const raw = new URL(request.url, endpoint);
  const prefix = raw.pathname.split('/').slice(2, -1).join('/') + '/files/';
  assert.match(prefix, /^pastebin\/[a-f0-9-]+\/files\/$/);
  assert.equal(raw.pathname.split('/').at(-1), '%E5%BD%92%E6%A1%A3%20%26%27.zip');
  assert.equal(raw.search, `?decompress-zip=${prefix.replaceAll('/', '%2F')}`);
  assertSignature(request, true);
});

test('never reports a successful object or directory link without the required CID and status', async t => {
  let headers = {};
  const { config } = await serve(t, () => ({ headers }));
  await assert.rejects(publish(file, config), /stored.*CID could not be read/);
  headers = { etag: '"d41d8cd98f00b204e9800998ecf8427e"' };
  await assert.rejects(publish(file, config), /stored.*CID could not be read/);
  headers = { 'x-amz-meta-ipfs-cid': fileCid };
  assert.equal((await publish(file, config)).cid, fileCid);
  for (const rootHeaders of [
    { 'x-ipfs-s3-zip-root-status': 'complete' },
    { 'x-ipfs-s3-zip-root-status': 'failed', 'x-ipfs-s3-zip-root-cid': rootCid },
    { 'x-ipfs-s3-zip-root-cid': rootCid },
  ]) {
    headers = { etag: `"${fileCid}"`, ...rootHeaders };
    await assert.rejects(publish(directory, config), /stored.*no shareable directory root/);
  }
});

test('blocks redirects without forwarding credentials and ignores gateway error body details', async t => {
  const destination = await serve(t, () => ({}));
  let status = 307;
  let s3Code;
  const { config, requests } = await serve(t, request => {
    const message = request.headers.authorization;
    const body = s3Code ? `<Error><Code>${s3Code}</Code><Message>${message}</Message></Error>` : message;
    return { status, headers: { location: destination.endpoint }, body };
  });
  await assert.rejects(publish(file, config), /could not be confirmed.*may already be stored/);
  assert.equal(destination.requests.length, 0);
  assert.equal(requests.length, 1);
  status = 403;
  await assert.rejects(publish(file, config), error => {
    assert.match(error.message, /HTTP 403/);
    for (const credential of Object.values(credentials)) assert.ok(!error.message.includes(credential));
    assert.ok(!error.message.includes('AWS4-HMAC-SHA256'));
    return true;
  });
  s3Code = 'QuotaExceeded';
  await assert.rejects(publish(file, config), /bucket is full.*HTTP 403/);
  s3Code = undefined;
  status = 500;
  await assert.rejects(publish(file, config), /HTTP 500.*may already be stored/);
});

test('rejects oversize bodies and ambiguous endpoint or name inputs before reading bytes', async () => {
  class UnreadableBlob extends Blob {
    arrayBuffer() { assert.fail('upload body loaded before validation'); }
  }
  const body = new UnreadableBlob(['1234']);
  const config = { ...commonConfig, endpoint: 'https://gateway.example' };
  await assert.rejects(publish({ ...file, body }, { ...config, maxUploadBytes: 3 }), /site size limit/);
  for (const endpoint of [
    'http://gateway.example', 'http://127.1:8080', 'https://localhost@evil.example', 'https://@example.com',
    'https://gateway.example/.', 'https://gateway.example/subpath', 'https://gateway.example?',
    'https://gateway.example#', 'https://gateway.example\\@evil.example', 'https://%65xample.com',
  ]) {
    await assert.rejects(publish({ ...file, body }, { ...config, endpoint }), /HTTPS endpoint origin/);
  }
  for (const name of ['..', '../file', 'alt\\file', 'line\nbreak', '\uD800']) {
    await assert.rejects(publish({ ...file, body, name }, config), /file name/);
  }
  for (const partial of [
    { accessKeyId: 'only-key', secretAccessKey: '' },
    { accessKeyId: '', secretAccessKey: 'only-secret' },
    { accessKeyId: '', secretAccessKey: '', sessionToken: 'only-token' },
  ]) {
    await assert.rejects(publish({ ...file, body, credentials: partial }, config), /Access key ID and Secret access key/);
  }
  await assert.rejects(publish({ ...directory, body, credentials: undefined }, config), /ZIP directory publishing requires S3 credentials/);
});

test('supports anonymous unsigned file PUT and maps AccessDenied clearly', async t => {
  let status = 200;
  let body;
  const { config, endpoint, requests } = await serve(t, () => ({
    status,
    headers: status === 200 ? { etag: `"${fileCid}"` } : { 'content-type': 'application/xml' },
    body,
  }));
  const result = await publish({ ...file, credentials: undefined }, config);
  const [request] = requests;
  assert.equal(request.method, 'PUT');
  assert.equal(request.headers.authorization, undefined);
  assert.equal(request.headers['x-amz-date'], undefined);
  assert.equal(request.headers['x-amz-content-sha256'], undefined);
  assert.equal(request.bytes.toString(), '你好\n<hello>');
  assert.deepEqual(result, { key: result.key, url: endpoint + request.url, cid: fileCid, name: file.name, kind: 'file' });

  status = 403;
  body = '<?xml version="1.0"?><Error><Code>AccessDenied</Code></Error>';
  await assert.rejects(publish({ ...file, credentials: undefined }, config), /credentials may be required.*HTTP 403/);
});

test('planMultipartUpload uses single PUT up to 6 MiB and scales part size and concurrency above 6 MiB', () => {
  const mib = 1024 * 1024;
  assert.equal(planMultipartUpload(0), null);
  assert.equal(planMultipartUpload(MULTIPART_THRESHOLD_BYTES), null);

  const small = planMultipartUpload(7 * mib);
  assert.equal(small.partSize, 5 * mib);
  assert.equal(small.partCount, 2);
  assert.ok(small.initialConcurrency >= 2 && small.maxConcurrency >= small.initialConcurrency);

  const medium = planMultipartUpload(64 * mib);
  assert.equal(medium.partSize, 8 * mib);
  assert.equal(medium.partCount, 8);
  assert.ok(medium.initialConcurrency >= 2);

  const large = planMultipartUpload(256 * mib);
  assert.equal(large.partSize, 16 * mib);
  assert.equal(large.partCount, 16);

  const huge = planMultipartUpload(1024 * mib);
  assert.equal(huge.partSize, 32 * mib);
  assert.equal(huge.partCount, 32);
});

test('uploads files over threshold concurrently via multipart upload with adaptive retry and complete XML', async t => {
  const payload = new Uint8Array(640);
  for (let i = 0; i < payload.length; i += 1) payload[i] = i & 0xff;

  let activeParts = 0;
  let peakActiveParts = 0;
  let failedOnce = false;
  const uploadedParts = new Map();
  const requests = [];

  const server = createServer(async (req, res) => {
    const bytes = await buffer(req);
    const parsed = new URL(req.url, `http://${req.headers.host}`);
    requests.push({ method: req.method, url: req.url, headers: req.headers, bytes });

    if (req.method === 'POST' && parsed.searchParams.has('uploads')) {
      res.writeHead(200, { 'content-type': 'application/xml' }).end(
        '<InitiateMultipartUploadResult><UploadId>mpu-test-1</UploadId></InitiateMultipartUploadResult>',
      );
      return;
    }
    if (req.method === 'PUT' && parsed.searchParams.has('partNumber')) {
      const partNumber = Number(parsed.searchParams.get('partNumber'));
      assert.equal(parsed.searchParams.get('uploadId'), 'mpu-test-1');
      activeParts += 1;
      peakActiveParts = Math.max(peakActiveParts, activeParts);
      await new Promise(resolve => setTimeout(resolve, 25));
      activeParts -= 1;
      if (partNumber === 2 && !failedOnce) {
        failedOnce = true;
        res.writeHead(503, { 'content-type': 'application/xml' }).end('<Error><Code>SlowDown</Code></Error>');
        return;
      }
      uploadedParts.set(partNumber, bytes);
      res.writeHead(200, { etag: `"bafypart${partNumber}"` }).end();
      return;
    }
    if (req.method === 'POST' && parsed.searchParams.get('uploadId') === 'mpu-test-1') {
      const bodyText = bytes.toString('utf8');
      assert.ok(bodyText.includes('<PartNumber>1</PartNumber><ETag>"bafypart1"</ETag>'));
      assert.ok(bodyText.includes('<PartNumber>5</PartNumber><ETag>"bafypart5"</ETag>'));
      res.writeHead(200, { 'content-type': 'application/xml' }).end(
        `<CompleteMultipartUploadResult><ETag>"${fileCid}"</ETag></CompleteMultipartUploadResult>`,
      );
      return;
    }
    res.writeHead(400).end('unexpected request');
  });
  server.listen(0, '127.0.0.1');
  t.after(() => new Promise(resolve => server.close(resolve)));
  await once(server, 'listening');
  const endpoint = `http://127.0.0.1:${server.address().port}`;

  const progressEvents = [];
  const result = await publish(
    {
      mode: 'file',
      name: 'chunked.bin',
      body: new Blob([payload], { type: 'application/octet-stream' }),
      credentials: undefined,
      onProgress: (p) => progressEvents.push(p),
    },
    {
      ...commonConfig,
      endpoint,
      maxUploadBytes: 4096,
      multipart: { thresholdBytes: 200, partSize: 128, initialConcurrency: 2, maxConcurrency: 4, retryDelaysMs: [10, 20] },
    },
  );

  assert.equal(result.cid, fileCid);
  assert.equal(uploadedParts.size, 5);
  assert.ok(peakActiveParts >= 2, `expected concurrent part uploads, got peak=${peakActiveParts}`);
  assert.equal(requests.every(r => r.headers.authorization === undefined), true);
  assert.equal(progressEvents[0].phase, 'init');
  assert.equal(progressEvents.at(-1).phase, 'completing');

  const reassembled = Buffer.concat([1, 2, 3, 4, 5].map(n => uploadedParts.get(n)));
  assert.deepEqual(new Uint8Array(reassembled), payload);
});

test('signs multipart upload requests with canonical query strings and aborts on fatal part error', async t => {
  const payload = new Uint8Array(300);
  let aborted = false;

  const { config, requests } = await serve(t, (request) => {
    const parsed = new URL(request.url, `http://${request.headers.host}`);
    if (request.method === 'POST' && parsed.searchParams.has('uploads')) {
      assert.equal(canonicalQuery(parsed), 'uploads=');
      assert.ok(request.headers.authorization?.startsWith('AWS4-HMAC-SHA256 '));
      return {
        headers: { 'content-type': 'application/xml' },
        body: '<InitiateMultipartUploadResult><UploadId>mpu-abort-1</UploadId></InitiateMultipartUploadResult>',
      };
    }
    if (request.method === 'PUT' && parsed.searchParams.has('partNumber')) {
      assert.match(canonicalQuery(parsed), /^partNumber=\d+&uploadId=mpu-abort-1$/);
      assert.ok(request.headers.authorization?.startsWith('AWS4-HMAC-SHA256 '));
      return {
        status: 403,
        headers: { 'content-type': 'application/xml' },
        body: '<Error><Code>AccessDenied</Code></Error>',
      };
    }
    if (request.method === 'DELETE' && parsed.searchParams.get('uploadId') === 'mpu-abort-1') {
      assert.ok(request.headers.authorization?.startsWith('AWS4-HMAC-SHA256 '));
      aborted = true;
      return { status: 204 };
    }
    return { status: 400 };
  });

  await assert.rejects(
    publish(
      {
        mode: 'file',
        name: 'denied.bin',
        body: new Blob([payload], { type: 'application/octet-stream' }),
        credentials,
      },
      {
        ...config,
        multipart: { thresholdBytes: 100, partSize: 128, initialConcurrency: 2, maxConcurrency: 2, retryDelaysMs: [5] },
      },
    ),
    /denied this upload/,
  );

  assert.equal(aborted, true);
  assert.ok(requests.some(r => r.method === 'DELETE'));
});

const liveEndpoint = process.env.PASTEBIN_TEST_ENDPOINT;
test('live gateway accepts signed file and ZIP PUTs, rejects unsigned PUT and serves public objects',
  { skip: !liveEndpoint }, async () => {
    const config = { ...commonConfig, endpoint: liveEndpoint, bucket: process.env.PASTEBIN_TEST_BUCKET };
    const liveCredentials = {
      accessKeyId: process.env.PASTEBIN_TEST_ACCESS_KEY,
      secretAccessKey: process.env.PASTEBIN_TEST_SECRET_KEY,
    };
    const receipt = await publish({ ...file, credentials: liveCredentials }, config);
    const downloaded = await fetch(receipt.url);
    assert.equal(downloaded.status, 200);
    assert.equal(await downloaded.text(), await file.body.text());

    // A ZIP containing nested/entry.txt with the UTF-8 content "zip entry".
    const archive = Buffer.from('UEsDBBQAAAAIAGUfdl0FWedKEQAAAAkAAAAQAAAAbmVzdGVkL2VudHJ5LnR4dKrKLFBIzSspqgQAAAD//wMAUEsBAhQAFAAAAAgAZR92XQVZ50oRAAAACQAAABAAAAAAAAAAAAAAAAAAAAAAAG5lc3RlZC9lbnRyeS50eHRQSwUGAAAAAAEAAQA+AAAAPwAAAAAA', 'base64');
    const baseKey = receipt.key.slice(0, -receipt.name.length);
    const prefix = `${baseKey}files/`;
    const archiveUrl = new URL(`/${config.bucket}/${baseKey}${encodeS3Component(directory.name)}`, liveEndpoint);
    const uploadUrl = new URL(archiveUrl);
    uploadUrl.search = `decompress-zip=${encodeS3Component(prefix)}`;
    const headers = new Headers({ 'content-type': 'application/zip', 'x-amz-tagging': 'ipfs-s3%3Azip-root=true' });
    await signPut(uploadUrl, archive, headers, config.region, liveCredentials);
    const zip = await fetch(uploadUrl, { method: 'PUT', body: archive, headers, redirect: 'error' });
    assert.equal(zip.status, 200);
    assert.equal(zip.headers.get('x-ipfs-s3-zip-root-status'), 'complete');
    const cid = zip.headers.get('x-ipfs-s3-zip-root-cid');
    const archiveCid = zip.headers.get('etag')?.slice(1, -1);
    assert.ok(cid && archiveCid && cid !== archiveCid);
    const entryKey = `${prefix}nested/entry.txt`;
    const xml = await zip.text();
    assert.ok(xml.includes(`<ArchiveETag>${archiveCid}</ArchiveETag>`));
    assert.ok(xml.includes('<ExtractedCount>1</ExtractedCount><FailedCount>0</FailedCount>'));
    assert.ok(xml.includes(`<Key>${entryKey}</Key>`));
    const original = await fetch(archiveUrl);
    assert.equal(original.status, 200);
    assert.deepEqual(Buffer.from(await original.arrayBuffer()), archive);
    const extracted = await fetch(new URL(`/${config.bucket}/${entryKey}`, config.endpoint));
    assert.equal(extracted.status, 200);
    assert.equal(await extracted.text(), 'zip entry');

    const unsigned = await fetch(new URL('unsigned.txt', receipt.url), { method: 'PUT', body: 'not allowed' });
    assert.equal(unsigned.status, 403);
  });
