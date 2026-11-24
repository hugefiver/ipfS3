const encoder = new TextEncoder();

export function encodeS3Component(value) {
  return encodeURIComponent(value).replace(/[!'()*]/g, char =>
    `%${char.charCodeAt(0).toString(16).toUpperCase()}`);
}

export function canonicalQuery(url) {
  if (!url.search || url.search === '?') return '';
  return url.search
    .slice(1)
    .split('&')
    .filter(Boolean)
    .map(pair => {
      const idx = pair.indexOf('=');
      const rawKey = idx === -1 ? pair : pair.slice(0, idx);
      const rawValue = idx === -1 ? '' : pair.slice(idx + 1);
      return [
        encodeS3Component(decodeURIComponent(rawKey)),
        encodeS3Component(decodeURIComponent(rawValue)),
      ];
    })
    .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0))
    .map(([k, v]) => `${k}=${v}`)
    .join('&');
}

function hex(bytes) {
  return Array.from(new Uint8Array(bytes), byte => byte.toString(16).padStart(2, '0')).join('');
}

function sha256(bytes) {
  return crypto.subtle.digest('SHA-256', bytes);
}

async function hmac(keyBytes, text) {
  const key = await crypto.subtle.importKey(
    'raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
  );
  return crypto.subtle.sign('HMAC', key, encoder.encode(text));
}

export async function signRequest(method, url, bytes, headers, region, credentials) {
  const { accessKeyId, secretAccessKey, sessionToken } = credentials;
  const timestamp = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');
  const day = timestamp.slice(0, 8);
  const scope = `${day}/${region}/s3/aws4_request`;
  const payload = bytes instanceof ArrayBuffer || ArrayBuffer.isView(bytes) ? bytes : new Uint8Array(0);
  const payloadHash = hex(await sha256(payload));
  headers.set('x-amz-content-sha256', payloadHash);
  headers.set('x-amz-date', timestamp);
  if (sessionToken) headers.set('x-amz-security-token', sessionToken);

  // Fetch sets Host itself; the signature still covers the host and any port.
  const signed = new Map(headers);
  signed.set('host', url.host);
  const fields = [...signed.keys()].sort();
  const signedHeaders = fields.join(';');
  const canonicalHeaders = fields.map(name => `${name}:${signed.get(name).replace(/[\t ]+/g, ' ')}\n`).join('');
  const request = [method.toUpperCase(), url.pathname, canonicalQuery(url), canonicalHeaders, signedHeaders, payloadHash].join('\n');
  const toSign = ['AWS4-HMAC-SHA256', timestamp, scope, hex(await sha256(encoder.encode(request)))].join('\n');
  const dateKey = await hmac(encoder.encode(`AWS4${secretAccessKey}`), day);
  const regionKey = await hmac(dateKey, region);
  const serviceKey = await hmac(regionKey, 's3');
  const signingKey = await hmac(serviceKey, 'aws4_request');
  const signature = hex(await hmac(signingKey, toSign));
  headers.set('authorization',
    `AWS4-HMAC-SHA256 Credential=${accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`);
}

export async function signPut(url, bytes, headers, region, credentials) {
  return signRequest('PUT', url, bytes, headers, region, credentials);
}
