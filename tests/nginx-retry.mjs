// Real nginx regression, no downloads: node --test tests/nginx-retry.mjs
// Requires the already-cached image used by docker-compose.multi-gateway.yml.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import http from 'node:http';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test from 'node:test';

const docker = (...args) => execFileSync('docker', args, { encoding: 'utf8', timeout: 30000 }).trim();
const image = 'nginx:1.28.0-alpine';
const configPath = process.env.NGINX_CONFIG ?? new URL('../deploy/nginx/multi-gateway.conf', import.meta.url);

async function listen(server) {
  await new Promise(resolve => server.listen(0, '0.0.0.0', resolve));
  return server.address().port;
}

async function scenario(method, uri, expectedRetry) {
  const seen = [[], []];
  const servers = [0, 1].map(index => http.createServer(async (req, res) => {
    for await (const _chunk of req) { /* Drain before committing the simulated write. */ }
    seen[index].push({ method: req.method, uri: req.url, auth: req.headers.authorization });
    res.writeHead(index === 0 ? 502 : 200, { 'Content-Length': '0' });
    res.end();
  }));
  const name = `ipfs3-retry-${randomUUID()}`;
  const directory = await mkdtemp(path.join(tmpdir(), 'ipfs3-retry-'));
  let started = false;
  try {
    const [a, b] = await Promise.all(servers.map(listen));
    const config = (await readFile(configPath, 'utf8'))
      .replace('gateway-a:9000 max_fails=1 fail_timeout=10s', `host.docker.internal:${a} max_fails=1 fail_timeout=10s`)
      .replace('gateway-b:9000 max_fails=1 fail_timeout=10s', `host.docker.internal:${b} backup`);
    await writeFile(path.join(directory, 'nginx.conf'), config);
    docker('run', '--pull=never', '--detach', '--name', name,
      '--add-host', 'host.docker.internal:host-gateway',
      '--publish', '127.0.0.1::9000', '--mount', `type=bind,source=${directory},target=/fixture,readonly`,
      image, 'nginx', '-c', '/fixture/nginx.conf', '-g', 'daemon off;');
    started = true;
    docker('exec', name, 'nginx', '-t', '-c', '/fixture/nginx.conf');
    const binding = docker('port', name, '9000/tcp');
    const response = await new Promise((resolve, reject) => {
      const request = http.request(`http://${binding}${uri}`, {
        method, headers: { Authorization: 'test-signature', 'Content-Length': '0' },
      }, res => { res.resume(); res.on('end', () => resolve(res.statusCode)); });
      request.setTimeout(5000, () => request.destroy(new Error('request timeout')));
      request.on('error', reject);
      request.end();
    });
    console.log(JSON.stringify({ method, uri, response, upstreamA: seen[0], upstreamB: seen[1] }));
    assert.equal(seen[0].length, 1, 'A processed request before failing response');
    assert.equal(seen[1].length, expectedRetry ? 1 : 0, `${method} replay count at B`);
    assert.equal(response, expectedRetry ? 200 : 502);
    if (expectedRetry) {
      assert.equal(seen[1][0].method, method);
      assert.equal(seen[1][0].auth, 'test-signature');
      assert.equal(seen[1][0].uri, uri.startsWith('/health') ? uri.replace('/health', '/ready') : uri);
    }
  } finally {
    if (started) docker('rm', '--force', name);
    await Promise.all(servers.map(server => new Promise(resolve => server.close(resolve))));
    await rm(directory, { recursive: true, force: true });
    console.log(`cleaned ${name}, upstream sockets, ${directory}`);
  }
}

test('mutations are not replayed after the first upstream processes them', async () => {
  for (const method of ['PUT', 'DELETE', 'POST', 'PATCH']) {
    await scenario(method, '/bucket/key?uploads', false);
  }
});

test('GET/HEAD fail over without changing signed URI, method or authorization', async () => {
  for (const [method, uri] of [
    ['GET', '/bucket/a%2Fb%20c?versionId=a%2Bb'],
    ['HEAD', '/bucket/key?versionId=old'],
    ['GET', '/health?probe=1'],
    ['GET', '/ready'],
  ]) await scenario(method, uri, true);
});
