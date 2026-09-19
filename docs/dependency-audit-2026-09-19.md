# Dependency security audit — 2026-09-19

## Scope and evidence

At `2026-09-19T15:09:22Z`, the official [OSV batch API](https://google.github.io/osv.dev/post-v1-querybatch/) was queried with every name/version pair from registry-sourced packages in the current `Cargo.lock`: **507 queries, 507 results, no continuation tokens**. The query therefore covers normal, dev, build, target-specific, and currently inactive optional lock entries; it is not limited to packages reachable by the production feature selection.

Individual advisory ranges were rechecked from the official OSV records at `2026-09-19T15:09:44Z` and `2026-09-19T15:10:49Z`. Crates.io release and dependency metadata was consulted on the same date for the versions discussed below.

## Remediated lock entries

| Advisory | Before | Current lock state | Verification |
| --- | --- | --- | --- |
| [RUSTSEC-2026-0221](https://rustsec.org/advisories/RUSTSEC-2026-0221.html) | `event-listener 5.4.1` | `5.4.2` | OSV affected range is `>=5.1.0, <5.4.2`. |
| [RUSTSEC-2026-0258](https://rustsec.org/advisories/RUSTSEC-2026-0258.html) | `h2 0.4.15` | `0.4.19` | OSV fixes at `0.4.16`; the selected version exceeds the minimum. |
| [RUSTSEC-2026-0285](https://rustsec.org/advisories/RUSTSEC-2026-0285.html) | `rustls 0.23.41` | `0.23.45` | OSV affected range is `>=0.23.13, <0.23.45`. Resolution also added `aws-lc-rs 1.18.0`, `aws-lc-sys 0.44.0`, and `rustls-webpki 0.103.15`; these are resolution changes, not findings from this audit. |
| [RUSTSEC-2026-0194](https://rustsec.org/advisories/RUSTSEC-2026-0194.html), [RUSTSEC-2026-0195](https://rustsec.org/advisories/RUSTSEC-2026-0195.html) | `s3s 0.14.0` with `quick-xml 0.40.1` | `s3s 0.14.1` with `quick-xml 0.41.0` | Both OSV records fix at `0.41.0`; the vulnerable `0.40.1` entry is absent. |

`s3s 0.14.1` is an upstream published patch release whose dependency constraint is `quick-xml ^0.41.0`; it is compatible with the gateway's direct `quick-xml ^0.41` requirement. No vendoring or manifest constraint override was needed for this production path.

## Remaining OSV matches and feature boundary

| Package | OSV result | Reachability classification | Current remediation position |
| --- | --- | --- | --- |
| `quick-xml 0.38.4` | [RUSTSEC-2026-0194](https://rustsec.org/advisories/RUSTSEC-2026-0194.html), [RUSTSEC-2026-0195](https://rustsec.org/advisories/RUSTSEC-2026-0195.html) | Dev/test only through `[dev-dependencies] rust-s3 0.37.2` (also through its `aws-creds` dependency). It is compiled when tests using that client are built, not for the normal gateway package. | `rust-s3 0.37.2` and `aws-creds 0.39.1` both require `quick-xml ^0.38`; crates.io has no newer stable release of either with a compatible `^0.41` constraint. A future upstream release, replacing the test client, or a tested fork/vendor change is required. |
| `proc-macro-error2 2.0.1` | [RUSTSEC-2026-0173](https://rustsec.org/advisories/RUSTSEC-2026-0173.html) | Compile-time proc-macro path from `sea-orm-macros`; not a runtime dependency. | OSV marks this as **unmaintained**, not a security vulnerability; no fixed version exists. |
| `rkyv 0.7.46` | [RUSTSEC-2026-0235](https://rustsec.org/advisories/RUSTSEC-2026-0235.html) | Present only through the locked `sqlx-mysql` optional branch (`sqlx-mysql -> rust_decimal -> rkyv`). The gateway enables `sqlx-sqlite` and `sqlx-postgres`, not MySQL. | No 0.7-series fix; OSV fixes at `0.8.17`. Reassess if MySQL support is enabled. |
| `rsa 0.9.10` | [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071.html) | Present only through the locked `sqlx-mysql` optional branch; not selected by the gateway's declared production features. | OSV reports no patched release. Reassess if MySQL support is enabled. |

For the declared production feature selection, `s3s` now resolves to fixed `quick-xml 0.41.0`; `rust-s3` is explicitly a dev dependency, and MySQL is not among the enabled SeaORM features. The complete lock scan still includes all of those non-production entries so that changing features, targets, or test commands does not hide them. A concurrent worker owns fetch/build validation, so this audit deliberately did not run Cargo tree/build commands and did not contend for Cargo's cache lock.

## Limits

An OSV name/version match is an advisory-database check, not a proof of zero vulnerabilities, exploitability, or absence of advisories from sources not represented in OSV. The lack of a currently published compatible `rust-s3`/`aws-creds` update is not a guarantee that no future upstream fix will appear. No source, manifest, lockfile, dependency source, or Git state was changed by this audit.
