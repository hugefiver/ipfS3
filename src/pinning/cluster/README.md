# IPFS Cluster pinning provider

The gateway completes a Cluster submission or removal when the configured API acknowledges it. The gateway stores the returned CID handle and uses it on that provider for removal. Requests do not check peer replica counts or initiate or wait for garbage collection.

Configure a `kind = "cluster"` provider with an explicit `strategy` (`cid`, `upload`, or `car`) and `[pinning.providers.cluster]`. The `protocol` is `rest` or `proxy`; `rest_endpoint` is required and Proxy also requires `proxy_endpoint`. The selected protocol uses only its own `rest_auth` or `proxy_auth` credentials. Upload and CAR read stored data from the main Kubo endpoint with independent `source` credentials; an alternate source URL is not accepted. Cluster targets must be independent of main Kubo. Credentials can be absent, Bearer, or Basic, and credential values come from environment references.

Private endpoints and cleartext HTTP require `allow_private_network = true`. Clients disable redirects, automatic HTTP retries, and ambient proxies. TLS hostname and certificate verification remain enabled. Configure transfer deadlines and body and CAR limits with `transfer_limits`; `stream_channels` controls the add response framing.

See the [configuration example](../../../config.example.toml) for a complete provider entry and [Cluster write transport](provider/README.md) for the wire contract. For file status, cancellation and ambiguous writes, see [remote pin operations](../../../docs/pinning-recovery.md).
