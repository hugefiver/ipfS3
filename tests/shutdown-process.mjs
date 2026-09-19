// Build the Linux binary first, then point at its task-owned Docker volume:
// IPFS3_TEST_TARGET_VOLUME=<volume> node --test tests/shutdown-process.mjs
// Uses only cached rust:latest; no pull/install, unique containers, automatic cleanup.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash, createHmac, randomUUID } from 'node:crypto';
import http from 'node:http';
import { setTimeout as delay } from 'node:timers/promises';
import test from 'node:test';

const docker = (...args) => execFileSync('docker', args, { encoding: 'utf8', timeout: 10000 }).trim();
const volume = process.env.IPFS3_TEST_TARGET_VOLUME;
assert.ok(volume, 'IPFS3_TEST_TARGET_VOLUME must identify a prebuilt Linux target volume');
const hash = text => createHash('sha256').update(text).digest('hex');
const hmac = (key, text) => createHmac('sha256', key).update(text).digest();

function request(endpoint, method, uri, body = '') {
  const host = new URL(endpoint).host;
  const date = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');
  const day = date.slice(0, 8);
  const headers = { host, 'x-amz-content-sha256': hash(body), 'x-amz-date': date };
  const signed = Object.keys(headers).join(';');
  const canonical = `${method}\n${uri}\n\n${Object.entries(headers).map(([k, v]) => `${k}:${v}\n`).join('')}\n${signed}\n${hash(body)}`;
  const scope = `${day}/us-east-1/s3/aws4_request`;
  const key = hmac(hmac(hmac(hmac('AWS4test', day), 'us-east-1'), 's3'), 'aws4_request');
  headers.authorization = `AWS4-HMAC-SHA256 Credential=test/${scope}, SignedHeaders=${signed}, Signature=${hmac(key, `AWS4-HMAC-SHA256\n${date}\n${scope}\n${hash(canonical)}`).toString('hex')}`;
  headers['content-length'] = Buffer.byteLength(body);
  return new Promise((resolve, reject) => {
    const req = http.request(`${endpoint}${uri}`, { method, headers }, res => {
      let text = '';
      res.on('data', chunk => { text += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, body: text }));
    });
    req.on('error', reject);
    req.setTimeout(40000, () => req.destroy(new Error('HTTP deadline')));
    req.end(body);
  });
}

async function waitFor(check, timeoutMs, description) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return;
    await delay(50);
  }
  throw new Error(`deadline waiting for ${description}`);
}

async function fixture(run) {
  const name = `ipfs3-shutdown-${randomUUID()}`;
  let accepted;
  const arrival = new Promise(resolve => { accepted = resolve; });
  let release;
  const gate = new Promise(resolve => { release = resolve; });
  const kubo = http.createServer(async (req, res) => {
    for await (const _chunk of req) { /* Accept full upload before exposing the gate. */ }
    if (req.url.startsWith('/api/v0/add')) {
      accepted();
      await gate;
      res.end(JSON.stringify({ Hash: 'bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq', Size: '7' }) + '\n');
    } else {
      res.end(JSON.stringify({ Pins: [] }));
    }
  });
  let started = false;
  try {
    await new Promise(resolve => kubo.listen(0, '0.0.0.0', resolve));
    docker('run', '--pull=never', '--detach', '--name', name,
      '--mount', `type=volume,source=${volume},target=/target,readonly`,
      '--add-host', 'host.docker.internal:host-gateway', '--publish', '127.0.0.1::9000',
      '--env', 'IPFS_S3_DATABASE_URL=sqlite:///tmp/shutdown.db?mode=rwc',
      '--env', `IPFS_S3_KUBO_RPC_URL=http://host.docker.internal:${kubo.address().port}`,
      '--env', 'IPFS_S3_ACCESS_KEY_ID=test', '--env', 'IPFS_S3_SECRET_ACCESS_KEY=test',
      '--env', `IPFS_S3_MASTER_KEY=${'12'.repeat(32)}`, '--env', 'RUST_LOG=warn,ipfs_s3_gateway=info',
      '--entrypoint', '/target/debug/ipfs-s3-gateway', 'rust:latest');
    started = true;
    let endpoint;
    const ready = async () => {
      endpoint = `http://${docker('port', name, '9000/tcp')}`;
      await waitFor(async () => {
        try { return (await request(endpoint, 'GET', '/health')).status === 200; }
        catch { return false; }
      }, 15000, 'HTTP readiness');
    };
    await ready();
    await run({
      name, release,
      waitForUpload: put => Promise.race([
        arrival,
        put.then(result => assert.fail(`PUT completed before Kubo accepted it: ${JSON.stringify(result)}`)),
      ]),
      request: (...args) => request(endpoint, ...args),
      signal: signal => docker('kill', '--signal', signal, name),
      stopped: async maxMs => {
        const start = Date.now();
        await waitFor(() => docker('inspect', '--format', '{{.State.Running}}', name) === 'false', maxMs, 'process exit');
        const code = Number(docker('inspect', '--format', '{{.State.ExitCode}}', name));
        const logs = docker('logs', name);
        console.log(JSON.stringify({ name, elapsedMs: Date.now() - start, exitCode: code, logs }));
        return code;
      },
      restart: async () => { docker('start', name); await ready(); },
    });
  } finally {
    release();
    if (started) docker('rm', '--force', name);
    kubo.closeAllConnections();
    await new Promise(resolve => kubo.close(resolve));
    console.log(`cleaned ${name} and Kubo fixture sockets; build volume retained by caller`);
  }
}

for (const signal of ['TERM', 'INT']) {
  test(`idle ${signal} exits cleanly`, async () => fixture(async f => {
    f.signal(signal);
    assert.equal(await f.stopped(8000), 0);
  }));
}

test('TERM drains active PUT before exit; restart sees committed metadata', async () => fixture(async f => {
  assert.equal((await f.request('PUT', '/shutdown-bucket')).status, 200);
  const put = f.request('PUT', '/shutdown-bucket/drained', 'payload').catch(error => ({ error }));
  await f.waitForUpload(put);
  f.signal('TERM');
  await delay(1000);
  await assert.rejects(f.request('GET', '/health'), 'shutdown must stop HTTP admission');
  f.release();
  assert.equal((await put).status, 200);
  assert.equal(await f.stopped(8000), 0);
  await f.restart();
  assert.equal((await f.request('HEAD', '/shutdown-bucket/drained')).status, 200);
  f.signal('TERM');
  assert.equal(await f.stopped(8000), 0);
}));

test('INT bounds stalled PUT by one 30s budget; restart cannot publish abandoned data', async () => fixture(async f => {
  assert.equal((await f.request('PUT', '/shutdown-bucket')).status, 200);
  const put = f.request('PUT', '/shutdown-bucket/abandoned', 'payload').catch(error => ({ error }));
  await f.waitForUpload(put);
  f.signal('INT');
  assert.equal(await f.stopped(34000), 1);
  assert.ok((await put).error, 'stalled HTTP request must be terminated');
  f.release();
  await f.restart();
  assert.equal((await f.request('HEAD', '/shutdown-bucket/abandoned')).status, 404);
  assert.equal((await f.request('PUT', '/shutdown-bucket/new', 'payload')).status, 200);
  f.signal('TERM');
  assert.equal(await f.stopped(8000), 0);
}));
