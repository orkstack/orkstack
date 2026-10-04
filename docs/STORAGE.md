# Storage — what OrkStack persists, and where

OrkStack has two durable stores and no third:

- **Temporal** holds every live run: workflow state, task state, the event
  stream, idempotency claims, and Worker Deployment versions. It is the read
  model for a run until its run retention expires (72 h by default, set at
  boot by `internal/task.EnsureNamespace`, and read or changed as the
  deployment maximum with `orkctl admin retention`; a namespace may keep runs
  for less with `orkctl namespace update --run-retention`), and every Orkflow
  schedule — its coordinates, its spec and its fire history
  (`docs/ORKFLOW_SCHEDULES.md`).
- **Object storage** holds everything that must outlive a process or a
  retention window: Orkflow definitions, the namespace registry, artifacts,
  task logs, run snapshots, activity completion records, and the archive of
  finished runs.

Nothing durable lives in an `ork` or `orker` process. There is no SQL
database, no embedded store, and no OrkStack-owned counter; AGENTS.md §13
lists them as non-goals. An earlier decision record chose SQLite for run
persistence; that design was never built, and the layout below is what shipped
instead.

## Backends

| Variable | Default | Meaning |
|---|---|---|
| `ORK_ARTIFACT_BACKEND` | `local` | `local` (a directory) or `s3` (any S3-compatible store with conditional `PutObject`) |
| `ORK_ARTIFACT_DIR` | `./.data/artifacts` | root directory of the `local` backend |
| `ORK_S3_ENDPOINT` | `http://localhost:9000` | S3 endpoint |
| `ORK_S3_REGION` | `us-east-1` | S3 region |
| `ORK_S3_BUCKET` | `orkstack-artifacts` | the one bucket a deployment uses |
| `ORK_S3_ACCESS_KEY`, `ORK_S3_SECRET_KEY`, `ORK_S3_SESSION_TOKEN` | dev-stack credentials | static credentials; unset both keys to use the SDK's default chain |
| `ORK_S3_PATH_STYLE` | `true` | path-style addressing (MinIO) |

The defaults live in `pkg/artifacts/config.go`. A replicated deployment needs
`s3`: on `local` each replica archives runs to its own disk and each Orker
writes artifacts nobody else can read. `docs/DEPLOY.md` §1 covers that and the
lifecycle rules operators may add.

The local backend mirrors the S3 key layout as directories under
`ORK_ARTIFACT_DIR`, with two private additions: `.metadata/` sidecars carrying
an object's attributes, and `.tmp/` for in-flight writes.

## Key layout

One bucket, one key family per kind of record. The builders are the single
place each shape is spelled.

| Key | Holds | Written | Removed by |
|---|---|---|---|
| `metadata/orkflows/{ns}/{name}/{version}.json` | one immutable Orkflow version, source YAML included (`OrkflowKey`, `pkg/artifacts/orkflow_store.go`) | conditional create; a divergent rewrite is `ErrVersionExists` | `orkctl orkflow delete`, which removes every version and both pointers after checking for recent runs |
| `metadata/orkflows/{ns}/{name}/@current.json` | the version new runs execute (`OrkflowCurrentKey`, `orkflow_current.go`) | compare-and-swap | the same delete |
| `metadata/orkflows/{ns}/{name}/@head.json` | the newest applied version; moving it is the apply commit point (`OrkflowHeadKey`, `orkflow_head.go`) | compare-and-swap | the same delete |
| `metadata/namespaces/{name}.json` | the namespace registry entry (`NamespaceKey`, `namespace_store.go`) | conditional PUT with an etag | `orkctl namespace delete`, refused while the namespace holds anything |
| `metadata/triggers/{id}.json` | one inbound run starter: namespace, orkflow, pinned version, description, optional `name`, and the salted digest of its token — never the token (`triggerKey`, `pkg/artifacts/trigger_store.go`); flat, because the invoke URL carries only the id | conditional create; rewritten in place only by `UpdateTrigger`, through a conditional put on the ETag it read (name, pin, description change; the id and the digest never do) | `orkctl trigger delete` |
| `metadata/webhooks/{id}.json` | one subscription: the protojson of `RegisterWebhookResponse` — URL, events, scope, optional `webhook.name`, and the signing secret, which no read but delivery's returns (`webhookKey`, `webhook_store.go`) | conditional create; rewritten in place only by `UpdateWebhook`, through a conditional put on the ETag it read (name, URL, events and scope change; the id and the secret never do) | `orkctl webhook delete` |
| `metadata/settings/image-policy.json` | the deployment image supply-chain floor (`ImagePolicyKey`, `image_policy_store.go`) | plain put by `UpdateImagePolicy`; absent until an operator writes one | nothing |
| `metadata/runs/{run-id}.jsonl` | an archived run: the run record then every event, one JSON line each (`RunArchiveKey`, `archive.go`) | once, at the run's terminal event; read verbatim as `run.jsonl` by `RunService.ExportRunEvidence` (docs/RUN_EVIDENCE.md) | nothing; archive bodies are never deleted |
| `metadata/index/runs/{ns}/{orkflow}/{status}/{stamp}/{run-id}` | zero bytes; the key is the index row (`RunIndexKey`) | with the archive body | nothing |
| `metadata/index/origins/{ns}/{kind}/{origin-id}/{orkflow}/{status}/{stamp}/{run-id}` | zero bytes; the key is the origin index row (`RunOriginIndexKey`): `kind` is the origin kind's slug (`submit`, `trigger`, `schedule`, `parent-run`, `run-again`), `origin-id` the originating object's id or `none`; read keys-only by an origin-filtered `ListRuns` | with the archive body, only for a run with a recorded origin, before its run-index row | nothing |
| `metadata/index/objects/{sha256(object-id)}/{run-id}` | zero bytes; one marker per artifact object the run references (`RunObjectRefKey`); read keys-only by `RunService.GetArtifactProvenance` (`ListObjectReferences`) as well as by cleanup | with the archive body | the cleanup worker, when it releases the run's claims |
| `metadata/index/run-objects/{run-id}.json` | the list of object ids the run references (`RunObjectListKey`) | with the archive body | the cleanup worker |
| `metadata/cleanup/runs/{run-id}.json` | a sidecar recording that the run's artifacts were cleaned (`RunCleanupKey`) | once, by the cleanup worker | nothing |
| `metadata/snapshots/{ns}/{run-id}/{sequence}.pb` | the run snapshot behind each projected event, kept out of Temporal history (`RunSnapshotKey`, `snapshots.go`) | by the workflow's snapshot local activity | the cleanup worker |
| `metadata/logs/{run-id}/{task-id}/{attempt}/{seq}` (and `.trunc`) | live task stdout/stderr chunks; order is the zero-padded key (`LogChunkKey`, `logs.go`) | by the Orker while the task runs | the cleanup worker, or a bucket lifecycle rule |
| `metadata/attempts/{run-id}/{task-id}/{attempt}.json` | one `TaskAttempt`: the outcome, error, exit code and §7.1 phase of ONE attempt, the phases it passed through, and the cpu and memory the Orker reserved for it; order is the zero-padded key (`AttemptKey`, `attempts.go`) | by the Orker at the end of every attempt, best-effort, plain put; read verbatim as `attempts/{n}.json` by `ExportRunEvidence` | the cleanup worker, or a bucket lifecycle rule |
| `metadata/audit/{yyyy}/{mm}/{dd}/{id}.json` | one control-plane audit record: who asserted which write, on what, when, and whether it was refused (`AuditKey`, `pkg/artifacts/audit.go`); the day is the record's own `at` in UTC | create-only, by the engine after every audited write RPC | nothing; a bucket lifecycle rule is the operator's |
| `metadata/deliveries/{yyyy}/{mm}/{dd}/{id}/{ns}/{run-id}/{webhook-id}/{event}/{outcome}.json` | one settled webhook delivery: every attempt, the signature sent, the body's SHA-256 — never the body (`DeliveryKey`, `pkg/artifacts/deliveries.go`); the day is the one the delivery id's stamp carries (`created_at`), and the segments after the id are the record's own coordinates so every listing filter is decided from the key | create-only, by the engine after the dispatcher returns | nothing; a bucket lifecycle rule is the operator's |
| `metadata/commits/{ns}/{run-id}/{task-id}/commit.json` | the immutable activity completion record (`CommitKey`, `commit_store.go`) | conditional create-if-absent; read verbatim as `commit.json` by `ExportRunEvidence`, only at the `CommitRef.uri` the run snapshot recorded | the cleanup worker |
| `metadata/probes/commit-store/…/commit.json` | the boot-time probe proving conditional create works on this store | at boot | nothing |
| `orkflows/{orkflow}/{run-id}/{task-id}/{artifact}/{sha256}` | workload artifact bytes on the `s3` backend (`s3ObjectKey`, `s3_manifest.go`) | by the Orker, content-addressed | a bucket lifecycle rule, or `DeleteUnreferencedObjects` |
| `manifests/{object-key}` | the artifact's exact logical name, beside its object | with the object | with the object |
| `objects/n-{name}[/c-{…}]/{sha256}` | workload artifact bytes on the `local` backend (`objectKey`, `artifacts.go`) | by the Orker, content-addressed | nothing automatic |

Run ids, namespaces, and task ids pass through `encodeArchiveSegment`
(`pkg/artifacts/archive.go`): a clean slug is used verbatim, anything else is
encoded and prefixed with `~`, and every segment is bounded at 200 bytes. A
run id is encoded, never parsed (AGENTS.md §7).

## Invariants the layout carries

- **Bytes never travel through Temporal.** Events and activity inputs carry
  references into this layout; the 256 KiB payload ceiling (AGENTS.md §2.2)
  is what forces snapshots, logs, and artifacts out here.
- **A durably committed result cannot become pending again.** `commit.json`
  is written with create-if-absent; an Orker retry that finds one returns it
  and never runs the container again. The boot probe refuses a store that
  cannot arbitrate that write.
- **Any replica answers for any run.** Every read of a finished run, a log, a
  definition, or a namespace goes to this layout or to Temporal, never to the
  memory of the process that started it.
- **Every attempt is recorded, not just the winner.** Temporal discards a
  failed attempt's error when it schedules the next one, so the Orker writes
  one `TaskAttempt` per attempt beside the logs that attempt produced. The
  write is best-effort: a record that cannot be stored is logged and never
  changes what the attempt reported.
- **A schedule is the Temporal Schedule.** Nothing about one is written to
  this layout, so nothing here can disagree with the cluster about when it
  fires. The one trace a schedule leaves here is its runs' own origin-index
  rows (`metadata/index/origins/{ns}/schedule/{sch-…}/…`), which every run
  has whatever started it.
- **An awaited task's `values.json` is written by the engine** when a
  resolution arrives, before the resolution is delivered; it is the only
  object an await produces. It lands in the same content-addressed family, with
  the same provenance metadata, as a container's `values.json`.
- **No automation document is stored anywhere.** A document lives in Git;
  `PlanAutomation` reads it from the request and the namespace's live objects
  and writes nothing, and an apply writes only through the ordinary trigger,
  webhook, schedule and namespace RPCs. A trigger or webhook record carries
  an optional `name`, nothing else: no "managed-by" marker, no last-applied
  copy. A record written before names existed reads as unnamed; nothing is
  migrated.
- **An update never recreates a delete.** The trigger and webhook records are
  rewritable by their own update only, with a conditional put naming the
  ETag the update read; a record deleted between the read and the write stays
  deleted, and the update reports NotFound.
- **The archive is append-only.** Bodies and index rows — the run index and
  the origin index alike — are never deleted;
  what the cleanup worker reclaims is the run's logs, attempt records,
  snapshots, commit record, and object markers, and it records that in the
  cleanup sidecar.

## Artifact provenance

`RunService.GetArtifactProvenance` (`orkctl run artifact provenance`, and the
**Provenance** disclosure on each artifact row of the run page) is the one
user-facing reader of the object index. It writes nothing. For one artifact,
addressed by its run, task, kind and recorded name and resolved against the
namespace-checked run — never by a caller-supplied URI or key — it answers:

- **`bytes_present`** — a `Stat` of the recorded object, no bytes
  transferred. `false` means a lifecycle rule, an operator, or
  `DeleteUnreferencedObjects` removed the bytes while the references stand. A
  store that cannot answer fails the call; it never reports `false`.
- **`own_reference`** — the addressed run's marker: `HELD`; `RELEASED`, with
  `released_at` from the cleanup sidecar's `temporal_deleted_at`; or `ABSENT`,
  meaning no marker and no sidecar (not archived yet, or archived before the
  index existed).
- **`references`** — every marker under the object's prefix whose run is in
  the calling namespace, newest `created_at` first. Markers of other
  namespaces are dropped and not counted; a marker whose run body is missing
  is counted in `references_unreadable` and listed nowhere. More than
  `MaxObjectReferences` (1000) markers is `ResourceExhausted`, never a
  truncated list.

What the list holds follows from the object key, not from a flag. An `s3`
object key embeds its run id, so its prefix holds the producing run alone —
OrkStack never dedupes across runs there. A `local` object key is
name-and-digest, so every run that produced identical bytes under one name is
listed. Markers record a run's own task outputs only (artifacts, values, and
stdout/stderr); nothing records consumption, and a bare content digest cannot
be looked up, because the marker's object segment is one-way.

## Reclaiming space

`internal/archive/cleanup.go` runs inside `ork` when `ORK_CLEANUP_ENABLED=true`
(`ORK_CLEANUP_INTERVAL` `1h`, `ORK_CLEANUP_RETENTION_BUFFER` `24h`,
`ORK_CLEANUP_BATCH_SIZE` `100`). It walks the run index in key order, and for
each archived run past its OWN OrkStack namespace's run retention (falling
back to the deployment maximum) it explicitly retires the execution — never
waiting for anything to expire passively — then deletes the run's logs,
attempt records, snapshots, and commit record, releases its object markers, and
writes the cleanup sidecar.

The cleanup worker never touches `metadata/audit/`: the control-plane audit
trail is kept until a bucket lifecycle rule on that prefix says otherwise, and
no setting in OrkStack shortens it.

Nor does it touch `metadata/deliveries/`: a webhook delivery record is kept
until a bucket lifecycle rule on that prefix expires it. It is a log, never a
queue — nothing in OrkStack reads a record to decide what to send — so
expiring one changes no behaviour; it only shortens how far back
`orkctl webhook deliveries` can look.

Artifact objects themselves are not reclaimed automatically.
`DeleteUnreferencedObjects` exists and is proven against live object storage,
but the cleanup worker deliberately does not call it yet. Until automatic
reclamation is wired into the cleanup pass, a bucket lifecycle rule on
`orkflows/` (and on `metadata/logs/`) is the operational answer. `docs/DEPLOY.md`
"Object reference accounting" has the details and the rule recipes.
