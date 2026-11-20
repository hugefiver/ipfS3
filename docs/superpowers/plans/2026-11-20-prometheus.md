# Gateway and pin metrics

An optional dedicated HTTP listener exposes Prometheus text at `/metrics`. With no listener configured the existing service, including an S3 bucket named `metrics`, is unchanged. Measure S3 requests through response headers, including errors, independently from admin probes and scrapes. Record counts and elapsed time with bounded labels, never keys, CIDs or request IDs.

Observe pin work at existing dispatch, provider response and database settlement points. Report eligible-to-dispatch waiting time when the eligible timestamp is known; persist it as part of the existing scheduling write, not a separate ledger. Report provider outcomes independently of writeback. Scraping reads local counters only; no external calls, SQL scans, new retries, or changes to quota and concurrency. Exercise the listener and request/worker observations through HTTP and the existing worker fixtures.
