# Back up and restore

Back up the two places a deployment keeps its state, in the right order, and restore them without losing runs.

## What to back up

| Store | How |
|---|---|
| Temporal | By its own procedure for the database it runs on. |
| The object-store bucket | Versioning with point-in-time restore (recommended), replication, or a snapshot consistent across the whole bucket. |

Back up Temporal first, then the bucket. OrkStack writes every object before the record that points at it, so a bucket at least as new as Temporal holds everything Temporal names.

Add an `AbortIncompleteMultipartUpload` lifecycle rule to the bucket, and with versioning a `NoncurrentVersionExpiration` rule.

## Restore

1. Stop every engine and Orker.
2. Turn retention cleanup off: `ORK_CLEANUP_ENABLED=false`.
3. Restore Temporal, then the bucket to the same moment or later. Write both moments down.
4. Start one engine and no Orkers. Run `orkctl admin restore reconcile --temporal-restored-at T --dry-run`, then again without `--dry-run`. It deletes the Temporal records of runs cleanup retired after T that the restore brought back, where the archive answers for them, and lists the runs that will run their last steps again.
5. Re-apply your automation documents: `orkctl automation apply --dry-run`, then `orkctl automation apply`.
6. Check that `orkctl run list` answers and `orkctl schedule list` shows your schedules.
7. Start the Orkers and turn cleanup back on. If the artifact GC deletes, run it in dry-run until `orkctl admin gc preview` shows no hold.

## What a restore loses

Everything Temporal recorded after its backup: approvals and resumes, idempotency claims, and the end of runs that finished later, which run their last steps again. Tasks that had not committed run again with the same idempotency key, so their outside effects can repeat.

## Limits

- A bucket older than Temporal makes tasks fail when they read objects written after the bucket's backup. Avoid it.
- A snapshot of separate disks taken at different moments is not a backup of the bucket.

## See also

- [RUNBOOKS.md, "Backup and disaster recovery"](https://github.com/orkstack/orkstack/blob/main/docs/RUNBOOKS.md#backup-and-disaster-recovery): the full procedure and what each step protects
- [CLI.md, "orkctl admin restore reconcile"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-admin-restore-reconcile): flags and confirmation
- [DEPLOY.md, "Retention cleanup and bucket lifecycle rules"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules): the lifecycle rules
