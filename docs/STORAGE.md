# Storage — what OrkStack persists, and where

OrkStack has two durable stores and no third:

- **Temporal** holds every live run — workflow state, task state, the event
  stream — plus idempotency claims, Worker Deployment versions and routing,
  and every Orkflow schedule (its coordinates, spec and fire history,
  `docs/ORKFLOW_SCHEDULES.md`). It is the read model for a run until the run
  is retired at its run retention (`docs/DEPLOY.md` §5, "Run retention").
- **Object storage** holds everything that must outlive a process or a
  retention window: Orkflow definitions, the namespace registry, deployment
  settings, artifacts, task logs, run snapshots, activity completion and
  attempt records, the audit trail, webhook delivery records, cache memos,
  and the archive of finished runs.

Nothing durable lives in an `ork` or `orker` process. There is no SQL
database, no embedded store, and no OrkStack-owned counter (AGENTS.md §13).

## Backends

`storage.backend` / `ORK_ARTIFACT_BACKEND` selects `local` (a directory,
`ORK_ARTIFACT_DIR`) or `s3` (any S3-compatible store with conditional
`PutObject`); the keys are in `docs/DEPLOY.md` §1. A replicated deployment
needs `s3`: on `local` each replica archives runs to its own disk and each
Orker writes artifacts nobody else can read. The engine reads Orkflow
definitions from S3 whatever the backend says.

The local backend mirrors the S3 key layout as directories under
`ORK_ARTIFACT_DIR`, with two private additions: `.metadata/` sidecars carrying
an object's attributes, and `.tmp/` for in-flight writes.

## Key layout

One bucket, one key family per kind of record. The builders in
`pkg/artifacts` are the single place each shape is spelled.

| Key | Holds | Written | Removed by |
|---|---|---|---|
| `metadata/orkflows/{ns}/{name}/{version}.json` | one immutable Orkflow version, source YAML included (`OrkflowKey`) | conditional create; a divergent rewrite is `ErrVersionExists` | `orkctl orkflow delete`, which removes every version and both pointers after checking for recent runs |
| `metadata/orkflows/{ns}/{name}/@current.json` | `{version, updated_at}`: the version new runs execute; absent means the head (`OrkflowCurrentKey`) | compare-and-swap; a promotion swaps it only while it still names the version the promotion compared the target's declared interface against (`CurrentExpectation`), so a promotion that raced another is refused, never landed over it | the same delete |
| `metadata/orkflows/{ns}/{name}/@head.json` | `{version, applied_at}`: the newest applied version; moving it is the apply's commit point (`OrkflowHeadKey`) | compare-and-swap | the same delete |
| `metadata/namespaces/{name}.json` | the namespace registry entry, every field of `Namespace` (`NamespaceKey`); `default` is synthesized when absent | conditional PUT with an etag | `orkctl namespace delete`, refused while the namespace holds anything |
| `metadata/settings/retention-policy.json` | the deployment's maximum run retention (`RetentionPolicyKey`) | create-if-absent at first boot, then plain put by `UpdateRetentionPolicy` | nothing |
| `metadata/settings/image-policy.json` | the deployment image supply-chain floor, signing keys as public PEM (`ImagePolicyKey`) | plain put by `UpdateImagePolicy`; absent until an operator writes one | nothing |
| `metadata/triggers/{id}.json` | one inbound run starter: namespace, orkflow, pinned version, description, optional `name`, and the salted digest of its token — never the token (`triggerKey`); flat, because the invoke URL carries only the id | conditional create; rewritten only by `UpdateTrigger`, through a conditional put on the ETag it read (name, pin, description change; the id and the digest never do) | `orkctl trigger delete` |
| `metadata/webhooks/{id}.json` | one subscription: URL, events, scope, optional `name`, and the signing secret, which no read but delivery returns (`webhookKey`) | conditional create; rewritten only by `UpdateWebhook`, through a conditional put on the ETag it read (name, URL, events and scope change; the id and the secret never do) | `orkctl webhook delete` |
| `metadata/runs/{run-id}.jsonl` | an archived run: the run record then every event, one JSON line each (`RunArchiveKey`) | once, at the run's terminal event; read verbatim as `run.jsonl` by `ExportRunEvidence` (`docs/RUN_EVIDENCE.md`) | nothing |
| `metadata/index/runs/{ns}/{orkflow}/{status}/{stamp}/{run-id}` | zero bytes; the key is the index row, stamped by `created_at` (`RunIndexKey`) | with the archive body | nothing |
| `metadata/index/origins/{ns}/{kind}/{origin-id}/{orkflow}/{status}/{stamp}/{run-id}` | zero bytes; the origin index row (`RunOriginIndexKey`): `kind` is `submit`, `trigger`, `schedule`, `parent-run` or `run-again`, `origin-id` the originating object's id or `none`; read keys-only by an origin-filtered `ListRuns` | with the archive body, only for a run with a recorded origin, before its run-index row | nothing |
| `metadata/index/objects/{sha256(object-id)}/{run-id}` | zero bytes; one marker per artifact object the run references (`RunObjectRefKey`) | with the archive body | cleanup, when it releases the run's claims |
| `metadata/index/run-objects/{run-id}.json` | the object ids the run references (`RunObjectListKey`) | with the archive body | cleanup |
| `metadata/cleanup/runs/{run-id}.json` | the sidecar recording that the run was retired and cleaned (`RunCleanupKey`) | once, by cleanup | nothing |
| `metadata/snapshots/{ns}/{run-id}/{sequence}.pb` | the run snapshot behind each projected event, kept out of Temporal history (`RunSnapshotKey`) | by the workflow's snapshot local activity | cleanup |
| `metadata/logs/{run-id}/{task-id}/[{item:05}/]{attempt}/{seq}` (and `.trunc`) | live task stdout/stderr chunks; order is the zero-padded key (`LogChunkKey`, `LogScopePrefix`) | by the Orker while the attempt runs | cleanup, or a bucket lifecycle rule |
| `metadata/attempts/{run-id}/{task-id}/[{item:05}/]{attempt}.json` | one `TaskAttempt`: outcome, error, exit code, the phase it ended in and every phase it passed through (`docs/OCI_ACTIVITY_CONTRACT.md` §7.1), the image it ran, and the cpu and memory the Orker reserved (`AttemptKey`, `AttemptScopePrefix`); each item numbers its attempts from 1 | by the Orker at the end of every attempt, best-effort, plain put; read verbatim as `attempts/{n}.json` by `ExportRunEvidence` | cleanup, or a bucket lifecycle rule |
| `metadata/commits/{ns}/{run-id}/{task-id}/[{item:05}/]commit.json` | the immutable activity completion record of a task, or of one fan-out item (`CommitKey`, `ItemCommitKey`); a dependent of a fan-out reads every item's to stage its files | conditional create-if-absent; read verbatim as `commit.json` by `ExportRunEvidence`, only at the `CommitRef.uri` the run snapshot recorded | cleanup |
| `metadata/memo/{ns}/orkflow/{orkflow}/{task-id}/{key}.json` | one orkflow-scoped `CacheMemo`: a pointer from a cached task's 64-hex memo key to a `SUCCEEDED` commit (`MemoKey`; `docs/DSL.md` "Caching") | by the Orker after a fresh commit of a cached task, best-effort, plain put, last writer wins | `orkctl cache purge`, or a bucket lifecycle rule |
| `metadata/memo/{ns}/namespace/{task-id}/{key}.json` | one namespace-scoped `CacheMemo` | same | same |
| `metadata/audit/{yyyy}/{mm}/{dd}/{id}.json` | one control-plane audit record: who asserted which write, on what, when, and whether it was refused (`AuditKey`); the day is the record's own `at` in UTC | create-only, by the engine after every audited write RPC | a bucket lifecycle rule only |
| `metadata/deliveries/{yyyy}/{mm}/{dd}/{id}/{ns}/{run-id}/{webhook-id}/{event}/{delivered\|failed}.json` | one settled webhook delivery: every attempt, the signature sent, the body's SHA-256 — never the body (`DeliveryKey`); the day is the delivery id's stamp, and the segments after the id are the record's own coordinates, so every listing filter is decided from the key | create-only, by the engine after the dispatcher returns | a bucket lifecycle rule only |
| `metadata/probes/commit-store/…/commit.json` | the probe proving conditional create works on this store | at every Orker boot | nothing |
| `orkflows/{orkflow}/{run-id}/{task-id}/{artifact}/{sha256}` | workload artifact bytes on the `s3` backend (`s3ObjectKey`) | by the Orker, content-addressed | a bucket lifecycle rule, or `DeleteUnreferencedObjects` |
| `manifests/{object-key}` | the artifact's exact logical name, beside its object | with the object | with the object |
| `objects/n-{name}[/c-{…}]/{sha256}` | workload artifact bytes on the `local` backend (`objectKey`) | by the Orker, content-addressed | nothing automatic |

Run ids, namespaces and task ids pass through `encodeArchiveSegment`: a clean
slug is used verbatim, anything else is encoded and prefixed with `~`, and
every segment is bounded at 200 bytes. A run id is encoded, never parsed
(AGENTS.md §7). A basename beginning `@` is reserved for per-orkflow
pointers, so a pointer can never be read as a version.

## Invariants the layout carries

- **Bytes never travel through Temporal.** Events and activity inputs carry
  references into this layout; the 256 KiB payload ceiling (AGENTS.md §2.2)
  is what forces snapshots, logs and artifacts out here.
- **A durably committed result cannot become pending again.** `commit.json`
  is written with create-if-absent; an Orker retry that finds one returns it
  and never runs the container again. The boot probe refuses a store that
  cannot arbitrate that write.
- **Any replica answers for any run.** Every read of a finished run, a log, a
  definition, a namespace, a setting, an audit or delivery record goes to
  this layout or to Temporal, never to the memory of the process that wrote
  it.
- **Pointers, not copies.** `@current` and `@head` name a version and hold no
  copy of the document: a second copy is a second place to be wrong. Moving
  them is a compare-and-swap arbitrated by the store's conditional PUT, so
  two replicas promoting or applying at once need no lock, lease or Temporal
  state — the loser is told and swaps again against the winner's etag. The
  head is a pointer rather than "the greatest `created_at`" because each
  replica stamps from its own clock, and because an edit's
  `expected_head_version` must be checked by the write itself. `applied_at`
  never goes backwards, so `orkctl orkflow versions` lists versions in apply
  order even when clocks disagree.
- **A fan-out item has its own commit, attempt and log keys**, one
  zero-padded segment below its task's; artifact objects stay
  content-addressed and are shared between items that wrote the same bytes.
  Every item key stays under its run's prefix, so cleanup reclaims items with
  no rule of their own.
- **Every attempt is recorded, not just the winner.** Temporal discards a
  failed attempt's error when it schedules the next one, so the Orker writes
  one `TaskAttempt` per attempt beside that attempt's logs. The write is
  best-effort: a record that cannot be stored is logged and never changes
  what the attempt reported, and every reader of attempt records reports a
  missing one rather than assuming it clean.
- **Settings are documents, not live reads.** The retention maximum and the
  image policy are single documents under `metadata/settings/`, so any
  replica answers identically. A run pins the image policy (and the signing
  key) it was submitted under in its plan; a later change never reaches it.
- **A schedule is the Temporal Schedule.** Nothing about one is written here,
  so nothing here can disagree with the cluster about when it fires. The one
  trace a schedule leaves is its runs' origin-index rows
  (`metadata/index/origins/{ns}/schedule/{sch-…}/…`).
- **An awaited or composed task's `values.json` is written by the engine** —
  an await's when its resolution arrives, before the resolution is
  delivered; a composed task's when its child run succeeds (the child's
  declared outputs, stored for the PARENT run under the composed task's id).
  Both land in the same content-addressed family, with the same provenance
  metadata, as a container's. A child run needs no key family of its own: it
  is an ordinary run with its own snapshots, events, archive record and
  origin-index row (`…/parent-run/{parent}/…`).
- **No automation document is stored.** A document lives in Git;
  `PlanAutomation` reads it from the request and the namespace's live objects
  and writes nothing, and an apply writes only through the ordinary trigger,
  webhook, schedule and namespace RPCs. A trigger or webhook record carries
  an optional `name` and nothing else: no "managed-by" marker, no
  last-applied copy. A record written without a name reads as unnamed.
- **An update never recreates a delete.** Trigger and webhook records are
  rewritable by their own update only, with a conditional put naming the
  ETag the update read; a record deleted in between stays deleted, and the
  update reports NotFound.
- **A memo changes no result.** It is a pointer to a commit that already
  exists, written only after a fresh `commit.json` was created and read only
  to decide whether a container is needed; every doubt resolves to *run the
  task*. A hit writes its run's own `commit.json` whose references are the
  source's content-addressed objects — no byte is copied. A memo belongs to
  no run, so cleanup never touches it; one whose source run was retired
  simply misses (`SOURCE_COMMIT_GONE`). Deleting any memo, or the whole
  prefix, only makes the next run of a cached task run its container.
- **Logs and records, never queues.** Nothing in OrkStack reads an audit or
  delivery record to decide what to do or send; expiring one only shortens
  how far back `orkctl admin audit` or `orkctl webhook deliveries` can look.
- **The archive is append-only.** Bodies and index rows — the run index and
  the origin index alike — are never deleted; what cleanup reclaims is the
  run's logs, attempt records, snapshots, commit records and object markers,
  and it records that in the cleanup sidecar.

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
(`docs/DEPLOY.md` §1, "Retention cleanup and bucket lifecycle rules", has its
settings, its fail-closed rules and the lifecycle recipes). It walks the run
index in key order, and for each archived run past its OWN namespace's run
retention (falling back to the deployment maximum) it explicitly retires the
execution, deletes the run's `metadata/logs/{run-id}/`,
`metadata/attempts/{run-id}/`, `metadata/commits/{ns}/{run-id}/` and
`metadata/snapshots/{ns}/{run-id}/`, releases its object markers, and writes
the cleanup sidecar.

Paging is in key order, not newest first, because the stamp is the fourth
segment of a run-index key: a newest-first page cannot be produced without
reading and sorting every key under the prefix, which would re-read the whole
index once per page.

Cleanup never touches `metadata/audit/`, `metadata/deliveries/` or
`metadata/memo/`; a bucket lifecycle rule is the only retention for each.

**Object reference accounting.** Archiving a run records which physical
objects its completion references: one zero-byte marker per object under
`metadata/index/objects/{sha256(object-id)}/`, and the run's own list under
`metadata/index/run-objects/`. The marker key is the reference — an object
with no keys under its prefix is referenced by no archived run — so counting
references is a keys-only prefix scan, and releasing a run's claims costs one
read plus one delete per object. Two details are load-bearing:

- **The object identity is the whole content-addressed key, not the bare
  digest.** Equal bytes under two logical names are two stored objects;
  refcounting on the digest alone would let one run's cleanup delete the
  other's.
- **The marker segment is a SHA-256 *of* the object id**, because an object
  key embeds a base64 logical name that easily exceeds the 200-byte segment
  bound; hashing it means no artifact name can make a run unarchivable.

Marker writes are issued 8 at a time because archiving happens inside a
projection activity with a 10-second budget whose failure is only logged, and
a run that exhausted it would have no archive body. The cost is still linear
in a run's artifact count, so a very wide run can still approach that budget.

`DeleteRunArtifacts` releases only the calling run's markers and list; it
never deletes artifact bytes. `DeleteUnreferencedObjects` deletes an object
only when its marker prefix is empty, and treats an object already gone as a
no-op. Cleanup does not call it — that is a scan of the whole reference
index, not a delete under one run's prefixes — and on `local` deleting by
producing run would destroy another run's committed result, because objects
there carry no run id. Artifact bytes are therefore reclaimed by a bucket
lifecycle rule on `orkflows/` (or `objects/`).
