const encoder = new TextEncoder();

export function encodeS3Component(value) {
  return encodeURIComponent(value).replace(/[!'()*]/g, char =>
    `%${char.charCodeAt(0).toString(16).toUpperCase()}`);
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

export async function signPut(url, bytes, headers, region, credentials) {
  const { accessKeyId, secretAccessKey, sessionToken } = credentials;
  const timestamp = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');
  const day = timestamp.slice(0, 8);
  const scope = `${day}/${region}/s3/aws4_request`;
  const payloadHash = hex(await sha256(bytes));
  headers.set('x-amz-content-sha256', payloadHash);
  headers.set('x-amz-date', timestamp);
  if (sessionToken) headers.set('x-amz-security-token', sessionToken);

  // Fetch sets Host itself; the signature still covers the host and any port.
  const signed = new Map(headers);
  signed.set('host', url.host);
  const fields = [...signed.keys()].sort();
  const signedHeaders = fields.join(';');
  const canonicalHeaders = fields.map(name => `${name}:${signed.get(name).replace(/[\t ]+/g, ' ')}\n`).join('');
  const request = ['PUT', url.pathname, url.search.slice(1), canonicalHeaders, signedHeaders, payloadHash].join('\n');
  const toSign = ['AWS4-HMAC-SHA256', timestamp, scope, hex(await sha256(encoder.encode(request)))].join('\n');
  const dateKey = await hmac(encoder.encode(`AWS4${secretAccessKey}`), day);
  const regionKey = await hmac(dateKey, region);
  const serviceKey = await hmac(regionKey, 's3');
  const signingKey = await hmac(serviceKey, 'aws4_request');
  const signature = hex(await hmac(signingKey, toSign));
  headers.set('authorization',
    `AWS4-HMAC-SHA256 Credential=${accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`);
}
