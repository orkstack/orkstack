# Retention

Set how long a finished run stays readable, then turn on the cleanup that removes its stored data. Retention counts from when a run finished, not when it was submitted.

## Example

```bash
orkctl admin retention set 72h                              # the most any namespace may keep a run
orkctl namespace update payments --run-retention 24h        # payments keeps its runs for less
```

Cleanup does not run until you turn it on. Set it in the engine's configuration:

```yaml
engine:
  cleanup:
    enabled: true
```

The same setting is `ORK_CLEANUP_ENABLED=true` in the environment.

## Two values

- **The deployment maximum** is set with `orkctl admin retention set`. No namespace may exceed it, and a namespace that sets nothing inherits it. A new deployment starts at the cluster's current retention, 72 hours on a fresh cluster.
- **A namespace's run retention** is set with `--run-retention`. It must not be above the maximum.

Lowering the maximum below a namespace's own value is refused, and the error names that namespace. Lower the namespace first.

## What cleanup does

With cleanup on, each pass retires finished runs that are older than their namespace's retention, or the maximum where none is set. Cleanup removes:

- the run's logs;
- its attempt records and snapshots;
- its completion commits.

The archived run record stays, so a retired run is still listed and readable. A pass that cannot read the retention settings deletes nothing.

Without cleanup, every run is kept for the deployment maximum plus a margin (24 hours by default). `orkctl admin retention get` says `not enforced` in that case.

## Lower the maximum safely

`orkctl admin retention set` states what the next cleanup pass will remove, and asks first. Read it before you confirm. A script passes `--yes`:

```bash
orkctl admin retention set 24h --yes
```

## Bucket lifecycle rules

Cleanup does not touch every prefix. Use bucket lifecycle rules for these, and only for the prefixes listed below:

| Prefix | What a rule shortens |
|---|---|
| `metadata/logs/` | Task output of finished runs |
| `metadata/attempts/` | Per-attempt records |
| `orkflows/` (on S3) or `objects/` (on the local backend) | Artifact bytes. References stay, and `bytes_present` turns false. The Orkflow definitions under `metadata/orkflows/` are a different prefix, and must never expire. |
| `metadata/audit/` | The audit trail. No OrkStack setting shortens it. |
| `metadata/deliveries/` | Webhook delivery records |
| `metadata/memo/` | Cached-task memos. Keep the rule at least as long as `ORK_CACHE_MAX_TTL`. |

Never expire the definition, registry, settings, trigger or webhook prefixes, or the snapshot, commit and run archive prefixes. Live reads depend on them.

A rule expires by age, whatever a run's retention says. Keep every expiry longer than the longest run, or a running run's inputs can disappear.

Add one bucket-wide rule to abort incomplete multipart uploads. A killed Orker leaves its parts in the bucket, and they stay billed. SeaweedFS removes them itself. On AWS S3, add this rule in the same document as any expiry rule, because the call replaces the whole rule set:

```json
{"Rules":[{"ID":"ork-abort-incomplete-multipart","Status":"Enabled",
  "Filter":{"Prefix":""},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}
```

## Limits

- Cleanup deletes durable data. It is off by default for that reason.
- Archived artifact bytes are not reclaimed by cleanup. A lifecycle rule is the answer.
- Retention is enforced only with cleanup on. Enabling it later moves each run's retirement to the moment it crosses its own retention, which can be sooner.

## See also

- [DEPLOY.md, "Run retention: a deployment maximum, a value per namespace"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#run-retention-a-deployment-maximum-a-value-per-namespace): the two values, and the first boot
- [DEPLOY.md, "Retention cleanup and bucket lifecycle rules"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules): the cleanup worker, its fail-closed rules and the lifecycle recipes
- [STORAGE.md, "Reclaiming space"](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md#reclaiming-space): what cleanup deletes, and what it keeps
- [LIMITATIONS.md, "Engine, cluster and gateway"](https://github.com/orkstack/orkstack/blob/main/docs/LIMITATIONS.md#engine-cluster-and-gateway): the retention limits
- [CLI.md, "orkctl admin retention set"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-admin-retention-set): flags and confirmation
- Settings: [Engine settings](/docs/configuration/engine/)
