# Pinning recovery runbook

Use the local management CLI with the gateway's effective configuration and the
same database as its workers. Protect database, configuration and private
management artifacts. These procedures do not authorize production database
edits, remote deletion, local garbage collection or an account change.

## Identify the responsibility

```powershell
ipfs-s3-gateway --pinning-doctor
ipfs-s3-gateway --pinning-status BUCKET KEY --version-id VERSION
```

`--pinning-doctor` reports configuration, not provider reachability.
`--pinning-status` reads stored evidence without contacting providers. Inspect
all relevant pages and identify the exact object version, lease, target, CID,
provider identity, route revisions, remote epoch and unresolved observations.
A configuration alias alone does not identify the account responsible for a pin.

For decompressed ZIP entries, distinguish the readable member object from the
lease-owner archive. Tag renewal addresses the lease owner; a management target
selection must retain its exact content and version binding. Do not infer either
from a key prefix.

## Interpret a stalled target

| State or evidence | Meaning and next step |
| --- | --- |
| `quota_waiting` | Capacity is unavailable. Check the current matching policy and provider capacity; One mode can select an eligible successor. Do not increase recorded capacity by deleting rows. |
| `quota_blocked` or an `Operator` admission barrier | Inspect unresolved provider responsibilities and route fences. More free quota alone does not remove an unknown mutation. |
| `running`, `recovering`, no `locked_until` | The job is parked. This database state does not mean a live task holds worker concurrency. |
| `needs_attention`, unknown effect | The gateway cannot prove the complete remote outcome. Use evidence-based reconciliation; never blindly resubmit. |
| `error` | The task or target has a terminal configuration, identity or state error. Inspect its reason; ordinary scans and restoring an old configuration do not restart it. |
| Historical remote identity unavailable | The current configuration cannot safely operate that existing resource. Keep its responsibility recorded; a same-name provider is not proof of the original account. |

A transport timeout, retry-budget exhaustion or empty `pin/ls` result cannot
prove that an earlier mutation never completed. Keep unknown responsibility and
its quota reservation until the required evidence supports settlement. A verified
pin's existence does not grant ownership or permission to delete it.

## Reconcile before retrying

Use the existing `reconcile` management action for the exact selected target or
historical resource. Build its `PlanRequest` using the
[management request schema](../src/pinning/management/model.rs) and review the
captured route and source binding. Reconcile grants observation, not replacement
Submit or Unpin authority.

The normal reviewed workflow is:

```powershell
ipfs-s3-gateway --pinning-manage request.json --output private/plan.v3
ipfs-s3-gateway --pinning-manage --review private/plan.v3
ipfs-s3-gateway --pinning-manage request.json --confirm private/confirmed.v3 --ack-item ITEM_ID
ipfs-s3-gateway --pinning-manage --apply private/confirmed.v3 --run
ipfs-s3-gateway --pinning-manage --status private/confirmed.v3
```

Create and protect the artifact directory first. Each output path must be new;
exports do not overwrite files. A failed command can leave a reserved file or a
committed database action, so inspect status before repeating it. `--run` performs
one due-job pass for the verified receipt, not a global worker drain. See the
[management CLI reference](../README.md#local-pinning-management-cli-stage-75)
for source preparation, confirmation expiry and exact-version selectors.

Read the resulting observations and current responsibility, not only the CLI
exit status. If the historical account is unavailable, a response is incomplete,
additional resources remain unexplained or another invocation in the same
allocation domain is unresolved, retain the barrier. Resolving one target does
not settle other provider debt.

## Renew or reapply only eligible intent

After evidence has settled, use `retry` or `reapply` only when the reviewed plan
permits it. For S3 tag-based retention, preserve application tags and address the
lease-owner object's exact version:

- `ipfs-s3:pin=true` and `ipfs-s3:retain-until` use an RFC 3339 timestamp.
- A timestamp equal to the existing expiry is Keep and does not regenerate work.
- Renewal requires a later timestamp within the applicable maximum duration.
- An expired lease also requires a future timestamp; cancellation is not renewal.
- Renewal does not itself resolve unknown submission evidence or grant a new
  provider identity.

Verify the resulting lease generation, target state and remote evidence.
Successful S3 tagging confirms the logical update, not remote pin completion.

## Configuration and terminal errors

Current configuration controls policy matching, provider order, capacity and
retention limits. Configuration digests do not lock pending work. Captured user
choices still apply: a skipped or cancelled request does not become a new pin
request when configuration changes.

An actual owner, resource identity or persisted-state mismatch terminates the
affected task or target with an error. Ordinary scans do not requeue that work,
even if the old configuration is restored. Independent tasks continue. A transient
database failure is reported as a database failure, not committed as a successful
skip.

Termination does not prove that a remote mutation failed. Unknown effects remain
in the ledger and retain their quota responsibility; the affected provider's
unsafe new mutations remain blocked until evidence resolves them. Read-only
reconciliation does not restart the terminal task or grant a replacement Submit.

Drain and upgrade all gateway writers and workers together, with a database backup.
Use the explicit database upgrade procedure before starting the new binary on an
older schema; normal startup does not silently upgrade it. Do not mix binaries
that disagree on terminal states or the current schema. SQLite upgrades refuse
extra inline constraints or foreign-key references from external tables rather
than discard those protections. Historical receipt and archive integrity remains
intact; old configuration digests in sealed originals are not execution authority.

## Preserve quota and cleanup accounting

Do not delete `remote_pins`, submission observations, history or ledger rows to
unblock admission. Supported confirmed-release operations fence the exact epoch
and request and update usage with the remote state in one transaction. Direct
SQL deletion bypasses that accounting; row counts alone cannot reconstruct
historical or unknown responsibility.

An active lease with an error target still retains its remote resource and quota.
Cancelling another lease that shares the CID does not authorize deleting that
resource. The error lease must end before its hold is removed; cleanup still
requires the original route, ownership and release evidence. This does not
restart the failed task.

If an earlier manual edit has damaged accounting, stop automated corrective
writes and retain a database backup. Reconcile complete provider and database
evidence under a separately authorized repair procedure. Never set usage to zero
merely because a local table or a single provider query is empty.
