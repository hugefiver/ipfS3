import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import { once } from 'node:events';
import { createServer } from 'node:http';
import { buffer } from 'node:stream/consumers';
import { test } from 'node:test';
import { publish } from '../publish.mjs';

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

function zipReply(request, partial) {
  const url = new URL(request.url, 'http://example.invalid');
  const archiveKey = decodeURIComponent(url.pathname.slice('/test-bucket/'.length))
    .replaceAll('&', '&amp;').replaceAll("'", '&apos;');
  const prefix = url.searchParams.get('decompress-zip');
  const failures = partial ? '<Failure><EntryName>&lt;lost&gt;.txt</EntryName><Code>EntryUploadFailed</Code><Message>Can&apos;t upload &amp; retry</Message></Failure>' : '';
  return {
    headers: { 'x-ipfs-s3-zip-root-cid': rootCid, 'x-ipfs-s3-zip-root-status': partial ? 'partial' : 'complete' },
    body: `<?xml version="1.0" encoding="UTF-8"?><DecompressZipResult><ArchiveKey>${archiveKey}</ArchiveKey>` +
      `<ArchiveETag>${fileCid}</ArchiveETag><ArchiveSize>${request.bytes.length}</ArchiveSize>` +
      `<ExtractedCount>1</ExtractedCount><FailedCount>${partial ? 1 : 0}</FailedCount><Entries>` +
      `<Entry><Key>${prefix}nested/好 &amp;lt;.txt</Key><ETag>${fileCid}</ETag><Size>0</Size></Entry>` +
      `</Entries><Failures>${failures}</Failures></DecompressZipResult>`,
  };
}

test('signs the ZIP v1 query and separates complete/partial roots from the archive', async t => {
  let partial = false;
  const { config, requests } = await serve(t, request => zipReply(request, partial));
  for (const rootStatus of ['complete', 'partial']) {
    partial = rootStatus === 'partial';
    const result = await publish(directory, config);
    const request = requests.at(-1);
    const raw = new URL(result.url);
    const prefix = result.key.slice(0, -directory.name.length) + 'files/';
    assert.equal(raw.search, '');
    assert.equal(decodeURIComponent(raw.pathname), `/test-bucket/${result.key}`);
    assert.equal(request.url, `${raw.pathname}?decompress-zip=${prefix.replaceAll('/', '%2F')}`);
    assertSignature(request, true);
    assert.deepEqual(result, {
      key: result.key, url: result.url, name: directory.name, kind: 'directory', cid: rootCid, archiveCid: fileCid,
      rootStatus, entries: [{ key: `${prefix}nested/好 &lt;.txt`, cid: fileCid, size: 0 }],
      failures: partial ? [{ name: '<lost>.txt', code: 'EntryUploadFailed', message: "Can't upload & retry" }] : [],
    });
  }
});

test('never reports a successful object or directory link without the required CID and status', async t => {
  let headers = {};
  const { config } = await serve(t, request => ({
    body: request.url.includes('?') ? zipReply(request, false).body : undefined, headers,
  }));
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

test('rejects unreadable ZIP extraction results even with a shareable root header', async t => {
  const { config } = await serve(t, request => {
    const reply = zipReply(request, true);
    return { ...reply, body: reply.body.replace('</Failure>', '') };
  });
  await assert.rejects(publish(directory, config), /stored.*extraction result/);
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
    const zip = await publish({ ...directory, credentials: liveCredentials, body: new Blob([archive]) }, config);
    assert.equal(zip.rootStatus, 'complete');
    assert.notEqual(zip.cid, zip.archiveCid);
    assert.deepEqual(zip.failures, []);
    assert.equal(zip.entries.length, 1);
    const [entry] = zip.entries;
    assert.match(entry.key, /\/files\/nested\/entry\.txt$/);
    assert.equal(entry.size, 9);
    const original = await fetch(zip.url);
    assert.equal(original.status, 200);
    assert.deepEqual(Buffer.from(await original.arrayBuffer()), archive);
    const extracted = await fetch(new URL(`/${config.bucket}/${entry.key}`, config.endpoint));
    assert.equal(extracted.status, 200);
    assert.equal(await extracted.text(), 'zip entry');

    const unsigned = await fetch(new URL('unsigned.txt', receipt.url), { method: 'PUT', body: 'not allowed' });
    assert.equal(unsigned.status, 403);
  });
