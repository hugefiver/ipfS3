# Local temporary S3 credentials

A trusted operator can issue a time-limited session for any configured `auth.credentials.access_key`. Requests made with that session have the same owner identity and policy as the static key. Issuing and checking sessions work offline, without a database, Kubo, or an STS endpoint.

## Configure the issuer and gateways

Set `IPFS_S3_CONFIG` to the gateway TOML file. Add the user under `[[auth.credentials]]` and provide the same dedicated session signing key to the issuer and every gateway serving the bucket. Generate 32 random bytes (for example with `openssl rand -hex 32`) and store their 64-character hex encoding in `IPFS_S3_SESSION_SIGNING_KEY`, or in `session_signing_key` under `[auth]`. The environment variable overrides the file. Keep it separate from S3 secrets and the SSE master key.

```toml
[[auth.credentials]]
access_key = "reports-reader"
secret_key = "<private static secret>"
```

Protect both the configuration and the issuer environment: possession of the signing key allows issuing sessions for **any** configured user. Give untrusted clients only their issued credentials. With no signing key, static keys work as usual and session issuance is disabled. When sessions are enabled, static IDs cannot use the temporary ID format (`IPS3T1` followed by 32 hexadecimal digits).

## Issue credentials

```console
ipfs-s3-gateway --issue-session reports-reader 3600
```

The second argument is the configured access key, the third is the lifetime in seconds (1–43,200). Success writes one AWS `credential_process` Version 1 JSON object to stdout: `AccessKeyId`, `SecretAccessKey`, `SessionToken`, and `Expiration` (UTC), plus `Version`. Treat this output as a secret. Each invocation issues a new key ID. Errors return a nonzero exit code.

On a trusted issuer host, an AWS profile can refresh credentials by running the command directly:

```ini
[profile gateway-reports]
region = us-east-1
credential_process = /opt/bin/ipfs-s3-gateway --issue-session reports-reader 3600
```

Set `IPFS_S3_CONFIG` in the AWS process environment as well, and replace the executable path with the installed absolute path. Use the profile with your gateway endpoint:

```console
aws --profile gateway-reports --endpoint-url https://s3.example.org s3api list-objects-v2 --bucket reports
```

Clients using exported credentials must set all three AWS variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN`. For SigV4 header authentication the token goes in the signed `x-amz-security-token` header; for a presigned URL it goes in the signed `X-Amz-Security-Token` query parameter. The URL stops working at the earlier of session expiry and URL expiry. Treat the whole presigned URL as sensitive.

When embedding the gateway in Rust, register both `GatewayAuth` (`set_auth`) and `GatewayAccess` (`set_access`) on the service builder, and use `GatewayRoute` for custom operations. `S3Impl` rejects temporary keys without a session-validated principal.

Session verification uses the receiving gateway's clock and effective configuration. Synchronize clocks between hosts. To remove a user, remove its static credential and any policy rules that name it. This invalidates that subject's sessions once the new configuration is loaded; changing only its static secret does not. Rotating or removing the session signing key invalidates all sessions issued with the old key. Apply these changes across gateways together.
