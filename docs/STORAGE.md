# Storage Architecture — OrkStack v0.0.2

**Status:** Accepted · **Decision:** GO — SQLite · **Confidence:** High
**Date:** 2026-08-14 · **Baseline commit:** `d51cfd0` (`main`, tag `v0.0.1`)
**Scope:** persistence for orkflow definitions, runs, and run events, to be built in M7.5.
**Supersedes:** nothing. This is the first storage decision record.

> **M9 note.** This record was written before the entity was renamed. Its
> identifiers were re-spelled at M9 — the proposed run columns are now
> `orkflow_name`/`orkflow_version` and the in-memory store is
> `MemoryOrkflowStore` — so the schema it proposes and the tree it cites read
> as one vocabulary. The measurements, the decision and the dates are
> untouched.

> **M11.9 note.** This record predates the object store's reference
> accounting, which is summarised here because it changes what the object
> store can safely reclaim. An archived run now records which physical
> content-addressed objects its completion references. Archiving writes two
> pieces of bookkeeping beside the run body: a zero-byte marker
> `metadata/index/objects/{sha256(object-id)}/{encoded-run-id}` for every
> object the run references, and
> `metadata/index/run-objects/{encoded-run-id}.json` listing that run's object
> ids. The marker key *is* the reference, so asking whether an object is still
> claimed is a prefix scan that reads no bodies, in the same spirit as the run
> index this record proposes; the per-run list is what makes releasing one
> run's claims cost a read and a delete per referenced object rather than a
> scan of every marker in the bucket.
>
> Two choices in those key shapes are load-bearing. The object identity is the
> whole content-addressed key rather than its terminal digest, because equal
> bytes stored under two different logical names are two distinct stored
> objects; refcounting on the digest alone would let one run's cleanup destroy
> another run's committed result. And the marker's object segment is a SHA-256
> *of* the object id rather than the id itself, because an object key embeds a
> base64 logical name and so easily exceeds the 200-byte limit on a single
> archive key segment — hashing it to fixed-width hex is what stops a long
> artifact name from making a run unarchivable.
>
> `DeleteUnreferencedObjects` therefore deletes a physical object only when
> that object's marker prefix is empty, and treats an object that is already
> gone as a no-op rather than an error, so an operator, a bucket lifecycle
> rule, or a second cleaner racing the same object cannot fail the pass.
>
> Reclaiming `objects/` automatically is nevertheless still deferred: the
> mechanism ships and is proven against live object storage, but the cleanup
> worker does not call it. Reference accounting exists only for runs archived
> by a build that has it, so a run archived earlier carries no per-run list and
> contributes no markers, and an object that only such a run referenced would
> look unreferenced and be deleted while it is still needed. Until an
> archive-wide backfill exists, bucket lifecycle policy remains the operational
> answer for `objects/` growth.
> [DEPLOY.md](DEPLOY.md#object-reference-accounting) carries the operational
> detail and the `ORK_CLEANUP_*` tuning. The decision, measurements, and dates
> below are untouched.

---

## 1. Decision

OrkStack persists orkflows, runs, and run events in **SQLite** (`modernc.org/sqlite`, WAL,
`synchronous=NORMAL`), written exclusively from the existing `EventActivities.PublishEvent`
activity, selected by `ORK_STORE_BACKEND` behind store interfaces that keep today's in-memory
maps as the `memory` implementation.

The two headline questions were tested, not assumed:

- **Temporal event history cannot replace a run DB — No.** Listing is fast; *reading* is not.
  Reconstructing run detail costs 8.2–8.5 ms per run serially and 2.5–2.6 ms per run at 8-way
  concurrency, so a 1000-run list costs **~8.2 s serial / ~2.5 s concurrent** on localhost against
  an in-memory database. `ORDER BY` is rejected outright, namespace retention is 24 h, and the
  shipped dev-stack Temporal is itself ephemeral.
- **S3/MinIO metadata is not viable for v0.0.2 — No.** S3 `LIST` returns keys only, so filtering by
  status requires reading every object: **3.09 s serial / 344 ms at 32-way** for 1000 objects on
  localhost. Standard S3 has no append, and the existing `pkg/artifacts.Store` is content-addressed
  and immutable, so it physically cannot hold a mutable `runs/{id}.json`.

SQLite clears the sub-second budget by three orders of magnitude at 100× the stated scale.

---

## 2. Context — everything is process memory today

| Fact | Evidence |
|---|---|
| `RunStore` = `map[string]*Run` + `order []string` + `sync.RWMutex` | `internal/engine/store.go:13-18` |
| `ListFilter` supports only `OrkflowName`, `Status` — **no time bounds** | `internal/engine/store.go:71-75` |
| `MemoryOrkflowStore` = nested maps; `List()` returns only the *latest* version per name | `internal/engine/store.go:99-162` |
| Stores and bus constructed fresh at process start | `cmd/orkengine/main.go:106-107,135` |
| `EventBus` holds the event log and assigns `sequence` from `len(b.log[runID])+1` | `internal/engine/bus.go:35-68` |
| Idempotency map and run-id counter are in-memory | `internal/api/service.go:59-63,181-190` |
| WS bridge and `StreamEvents` 404 unless the run is in `RunStore` | `internal/api/ws.go:23-26`, `internal/api/service.go:318-319` |

An engine restart today loses every orkflow definition, every run, every event, every idempotency
key, and the UI's ability to attach to a run Temporal is still executing.

### 2.1 The write seam already exists and is already idempotent

`internal/engine/workflow.go:796-847` — `emit()` first writes the run snapshot
to object storage through a **local activity** (`ActivityWriteSnapshot`,
workflow.go:825 — local because Temporal does not persist local-activity
input, which is what keeps the snapshot bytes out of history), then calls
`workflow.ExecuteActivity(activityCtx, ActivityPublishEvent, event, snapshotURI, dedupeKey)`
with `dedupeKey = fmt.Sprintf("%s#%d", s.runID, s.eventSeq)`.
`internal/engine/activities.go:108-125` — `PublishEvent` resolves the URI back
into the run (`readSnapshot`), then does `store.Apply(run)` and
`bus.Publish(event, dedupeKey)`.

> **M11.10 note.** This section originally quoted
> `PublishEvent(event, s.snapshot(), dedupeKey)`: the complete run snapshot
> rode along as a regular activity argument, so Temporal history carried every
> snapshot and grew quadratically with task count. M11.10 externalized the
> snapshot to object storage (`metadata/snapshots/{namespace}/{run-id}/{sequence}.pb`,
> `pkg/artifacts/snapshots.go`) and made the activity argument a URI. The seam
> itself is unchanged — one idempotent projection call per lifecycle event,
> same dedupe key — but a durable store slotting in behind it now receives a
> URI to resolve rather than an inline run. See `proof/M11.10/`.

A durable store slots in behind this seam with **zero Temporal workflow-code change**, so the
determinism boundary (AGENTS.md §1.3) is untouched. The existing dedupe key is exactly the natural
key for `INSERT … ON CONFLICT(run_id, dedupe_key) DO NOTHING`.

One caveat in the same function: publish failures are logged and swallowed — *"Projection is
observability, not orchestration"* (`internal/engine/workflow.go:840-845`). Once the store **is**
the system of record, a swallowed write is silent data loss. The snapshot write itself is the
exception: it fails the emit (workflow.go:826-833), so object-storage failure cannot silently
produce a successful run. See open question OQ-1.

### 2.2 Contract gaps found while reading (independent of backend)

- `proto/orkstack/v1/service.proto:60-65` — `ListRunsRequest` is
  `{orkflow_name, status, page_size, page_token}`. **No date-range filter**, which the stated API
  constraint explicitly requires. `cmd/orkctl/run.go` has no matching flag.
- `internal/api/service.go:271-283` — `ListRuns` **ignores `page_token`** and never sets
  `next_page_token`; it materializes everything and truncates with `runs[:size]`. Verified: the
  identifiers `PageToken`/`NextPageToken` do not appear anywhere in `internal/api/service.go`.
  `ListOrkflows` has the same gap.
- `ListOrkflowsRequest` (`service.proto:31-34`) has no name filter, and `MemoryOrkflowStore.List`
  returns only the latest version per name — there is no way to enumerate versions of one orkflow.
- Useful: `Orkflow.version` is either a DSL-pinned value or a content hash over the semantically
  normalized YAML (`internal/dsl/hash.go:19-35`) → a natural immutable primary key `(name, version)`.
  `Orkflow.source_yaml` is inline, so an orkflow row carries its full DSL.

---

## 3. How the numbers were produced

Every figure marked *measured* was executed against the live dev stack (`orkstack-temporal-1`,
`orkstack-minio-1`, both healthy) on **darwin/amd64, Go 1.25.6**. Probe programs lived in `/tmp`;
no repository source, proto, or config was changed to produce them.

Two independent measurement passes were made. Where they differ, both are shown. The second pass
(`re-measured`) used a slim four-column list projection — no `snapshot` blob — which is the shape
the run strategy in §7 actually recommends, and is why its numbers are lower.

`pgrep -fl 'bin/orkengine|bin/orker'` returned nothing before and after, so no stray worker stole
activities (AGENTS.md §6).

---

## 4. Decision matrix

### Option 1 — SQLite ★ chosen

**Architecture sketch.**

```
orkctl / UI ──Connect──> internal/api ──reads──> Store iface ─┐
                                                              ├─> SQLite (WAL, one file)
Temporal ──ExecuteTask──> Orker                               │   .data/orkstack.db
Temporal ──PublishEvent activity──> engine ──single writer──> ┘
```

Tables: `orkflows(name, version, …)` PK `(name, version)`; `orkflow_latest(name, version)`;
`runs(id PK, orkflow_name, orkflow_version, status, created_at, started_at, finished_at,
snapshot BLOB)`; `run_events(run_id, sequence, dedupe_key, type, …)` with
`UNIQUE(run_id, dedupe_key)` and `UNIQUE(run_id, sequence)`. Backend chosen by
`ORK_STORE_BACKEND=memory|sqlite`, mirroring the registry precedent in
`pkg/artifacts/registry.go:76-89`. File sits at `./.data/orkstack.db`, beside `./.data/artifacts`.

**Query performance** — *measured*, 100 000 rows, best-of-20 after warmup:

| Query | research pass | re-measured pass |
|---|---|---|
| newest 50, no filter | 403 µs | **111 µs** |
| status filter, newest 50 | 414 µs | **137 µs** |
| **status + date range, newest 50** | 404 µs | **166 µs** |
| newest 1000, no filter | 8.8 ms | 1.34 ms |
| name + status + date range, newest 1000 | 46.2 ms | **19.5 ms** |
| point get by id | 63 µs | 23 µs |
| single-row UPDATE commit | 164 µs | 106 µs |

The target CLI query (`status` + date range, newest page) answers in **166 µs at 100 000 rows** —
roughly 6000× inside the sub-second budget at 100× the stated 1000-run scale. Even the widest
query measured (1000 full rows, three predicates) is 19.5 ms, still 50× inside budget.

**Durability.** *Engine restart:* full — WAL plus `synchronous=NORMAL` is crash-safe against
process kill; only OS or power loss can drop the last commits. *Temporal loss:* **survives** — this
is the only option that outlives a namespace wipe or 24 h retention expiry. *MinIO loss:*
unaffected; artifact bytes stay a separate failure domain.

**Operational complexity.** Lowest of the durable options: one file, zero containers, zero
credentials. `modernc.org/sqlite` is CGO-free, so `make build` and cross-compilation are unchanged.
Costs added: goose + `embed.FS` migrations, and a single-writer pool discipline.

**MVP fit.** Excellent. **Production fit.** Good to single-node scale; Litestream can replicate the
file continuously into the MinIO already in the stack (~1 s RPO). The ceiling is horizontal
scale-out of the engine: one writer, one node.

**Migration path to v1.0.** Clean. Behind the store interface, a PostgreSQL implementation is the
same SQL with different placeholders; export/import via `INSERT … SELECT` or a JSONL dump.

**Risks.** Single writer caps engine HA. A 4 KB snapshot per row measured **452 MB at 100 000 runs**
(main DB file, WAL checkpointed), so a retention/GC policy becomes mandatory. SQLite `ALTER TABLE`
is add/drop/rename-column only, so schemas must be designed additively.

---

### Option 2 — Temporal event history

**Architecture sketch.**

```
orkctl ──ListRuns──> internal/api ──ListWorkflowExecutions──────> Temporal visibility
                                 └─GetWorkflowExecutionHistory──> Temporal history
                                        └─ replay events into a Run on every read
```

No separate store. Orkflow YAML would live in memo or search attributes.

**Query performance** — *measured*, Go SDK v1.47.0, `default` namespace, 152 executions:

| Probe | Best | Avg |
|---|---|---|
| `ListWorkflowExecutions` pageSize=100, no query | 29.5 ms | 45.1 ms |
| `ExecutionStatus='Completed'` | 45.0 ms | 52.8 ms |
| `ExecutionStatus + WorkflowType + StartTime >` | 35.6 ms | 51.8 ms |
| `GetWorkflowExecutionHistory`, 1 run (89 events) | 7.0 ms | 10.0 ms |
| history × 20 runs, serial | **8.2–8.5 ms/run** | — |
| history × 20 runs, 8-way concurrent | **2.5–2.6 ms/run** | — |
| `ORDER BY StartTime DESC` | **rejected** | — |

The list call alone is fine. But list rows carry only search-attribute fields; any per-run
`TaskRun` detail forces a history fetch, so **1000 runs cost ~8.2 s serial or ~2.5 s at 8-way** —
8×–25× over budget, on localhost against an in-memory database, which is the best case that will
ever exist. Pushing detail into custom search attributes hits the SQL visibility quota of **10
Keyword + 3 each** of Bool/Datetime/Double/Int/KeywordList/Text per namespace.

Independently re-verified this session against the running dev server:

```
$ temporal workflow list --query "ExecutionStatus='Completed' ORDER BY StartTime DESC" --limit 1
ERROR  failed listing workflows: invalid query: operation is not supported: 'ORDER BY' clause
```

So "newest first" — which the CLI and UI already rely on — is **inexpressible**; you get the
server's fixed `ClosedTime DESC, StartTime DESC`.

**Durability.** *Engine restart:* survives, state is server-side. *Temporal loss:* **total loss**,
and not hypothetically. Re-verified this session:

```
$ temporal operator namespace describe --namespace default
Config.WorkflowExecutionRetentionTtl  24h0m0s
```

24 h is what this dev cluster registered for Temporal's own built-in `default`
namespace — not this deployment's namespace (`ork-main`, selected by
`ORK_TEMPORAL_NAMESPACE`), and not a hard floor Temporal enforces here: the
retention-admin proof below moves `ork-main`'s retention to 6 h via the CLI
and 2 h via the UI on this same cluster, and Temporal accepts both. Whatever
the configured period, expiry deletes the history **and** the visibility
record. Worse, `deploy/docker-compose.yml` runs `server start-dev` with **no
`--db-filename` and no volume** (only `minio-data` is a named volume), and
`start-dev` defaults to an in-memory SQLite database — a container restart
wipes everything. This directly violates "no loss of orkflow or run data".
*MinIO loss:* unaffected.

**Reading and writing it is now a first-class deployment operation**, not
only a manual `temporal operator namespace describe`: `orkctl admin retention
get`/`orkctl admin retention set <duration>` and the Admin → Retention UI
page read and write the deployment's own namespace (`ork-main`, provisioned
at 72 h by `internal/task.EnsureNamespace`; see `docs/DEPLOY.md`'s "Execution
retention: one deployment-wide Temporal cluster setting") — never the
built-in `default` namespace measured above, and never a per-OrkStack-namespace
value that `orkctl namespace` touches. The real-cluster proof in
`proof/retention-admin/` is what shows 24 h above was this cluster's default
for an unconfigured namespace rather than an enforced floor: it moves
`ork-main` to 6 h through the CLI and to 2 h through the UI, and Temporal
accepts both.

**Operational complexity.** Deceptively high: register custom search attributes per namespace,
raise retention, add a persistent Temporal database and volume (a `deploy/` change, coordinated
under AGENTS.md §8), and then version the history→`Run` reconstruction against workflow-code
changes forever.

**MVP fit.** Poor. **Production fit.** Poor as a system of record; good as a secondary
reconciliation source.

**Migration path to v1.0.** Dead end — migrating later means backfilling from expiring history.

**Worth keeping.** Startup reconciliation. `ListWorkflowExecutions` at 35–53 ms is cheap enough to
sweep `Running` executions on engine boot and heal any run whose terminal `PublishEvent` was lost.

---

### Option 3 — PostgreSQL

**Architecture sketch.** Same tables as Option 1 with JSONB projections; `pgx/v5` + `pgxpool`;
goose or golang-migrate; one extra container in `deploy/`.

**Query performance.** Comparable to or better than SQLite at this scale; it wins only well past
10⁶ rows or with genuinely concurrent writers. **Not measured — no PostgreSQL exists in this
stack.** Treat any figure quoted for it as an estimate.

**Durability.** Best in class: WAL, point-in-time recovery, replication. Survives engine restart,
Temporal loss, and MinIO loss.

**Operational complexity.** Highest: a container, credentials, pool tuning, backups, migrations,
monitoring. Softer than the constraint implies — Temporal's own docker-compose repository already
ships PostgreSQL 16, so at production this is a `CREATE DATABASE`, not a new component. But the
OrkStack dev stack does not run those files; it runs `start-dev` with no PostgreSQL.

**MVP fit.** Over-built, and violates "no external databases in dev stack" as the stack exists
today. **Production fit.** Best.

**Migration path to v1.0.** It *is* v1.0. Choosing SQLite behind an interface costs almost nothing
and defers this cleanly.

---

### Option 4 — S3-backed metadata

**Architecture sketch.** `metadata/namespaces/{name}.json`,
`metadata/orkflows/{namespace}/{name}/{version}.json`,
`metadata/runs/{run-id}.jsonl`; `ListObjectsV2` prefix scans for queries.

**Query performance** — *measured*, aws-sdk-go-v2 v1.107.1, path-style, localhost MinIO:

| Probe | Result |
|---|---|
| `ListObjectsV2`, whole 1000-key prefix, 1 request | 53.8 ms |
| GET 1000 objects serial (status filter needs bodies) | **3.094 s** |
| GET 1000 objects, 32-way concurrent | 303 ms best / 344 ms avg |
| GET one 81 KB index object holding 1000 rows | 2.8 ms best / 4.5 ms avg |
| `PUT If-None-Match: *` on existing key | `PreconditionFailed` (412) |
| `PUT If-Match: <stale ETag>` | `PreconditionFailed` (412) |

LIST returns keys only — S3 exposes prefix, delimiter, and start-after, and nothing else — so any
filter on status or date must read bodies. That is N+1 by construction and **fails the budget**:
3.09 s serial on localhost, and roughly 150 s on real S3 at AWS's published 100–200 ms
small-object latency. The index-object variant reads 1000 rows in 2.8 ms, but then the index is a
single hot object requiring read-modify-write with `If-Match` on every run update — write
contention and lost-update retries at exactly the point of highest concurrency.

**Two repo-specific blockers make this worse than the generic case:**

1. `pkg/artifacts.Store` is content-addressed and immutable — keys are
   `objects/{sha256(name)}/{sha256(content)}` (`pkg/artifacts/s3_manifest.go:21-27`), and the
   interface offers `Put/Get/Stat/List/Presign/Delete` with **no in-place overwrite**
   (`pkg/artifacts/artifacts.go:40-61`). A mutable `metadata/runs/{id}.json` cannot be expressed
   through this API at all.
2. `S3Store.List` (`pkg/artifacts/s3.go:183-223`) lists *every* object under `objects/`, then per
   object does a manifest GET **plus** a HEAD before filtering by logical-name prefix in Go —
   two extra sequential round trips each, so 1000 metadata objects ≈ 2000 round trips.

Standard S3 also has **no append** (only S3 Express One Zone directory buckets; community MinIO
lacks it), so a `runs/{id}.jsonl` design is GET-whole → concat → PUT-whole per event: **O(N²)
bytes** for an N-event run.

**Durability.** *Engine restart:* survives. *Temporal loss:* survives. *MinIO loss:* **total loss
of both metadata and artifacts** — the two failure domains collapse into one.

**Operational complexity.** Deceptive. No append, hand-rolled `If-Match` retry loops, and a
parallel raw-S3 client that would duplicate the provider layer and cross the AGENTS.md §8
ownership boundary.

**MVP fit.** Poor. **Production fit.** Only as an archive/export tier.

**Migration path to v1.0.** Rewrite.

**Salvage.** Conditional writes are fully working on MinIO (verified 412 on both a stale `If-Match`
and `If-None-Match: *` against an existing key), which makes S3 a sound **backup/export target** —
Litestream replicating the SQLite file, or periodic JSONL exports under `metadata/`.

---

### Option 5 — Hybrid (in-memory + SQLite checkpoint on shutdown)

**Architecture sketch.** Keep today's maps as the live store; flush to SQLite on `SIGTERM`; restore
into memory on boot.

**Query performance.** Best possible — map plus slice, in-process, microseconds.

**Durability.** **Broken by design.** `SIGKILL`, OOM, or a panic loses every run since the last
checkpoint, which directly violates "no loss of orkflow or run data". Periodic checkpointing
narrows the window but never closes it, and the memory copy remains the source of truth for a
bounded-loss interval.

**Operational complexity.** Higher than plain SQLite: two representations, a flush path, a restore
path, and the divergence bugs between them — for zero measured gain.

**MVP fit.** Poor. **Production fit.** Poor.

**Migration path to v1.0.** Converges to Option 1 anyway.

**Verdict.** The premise is that synchronous writes are expensive. Measured, a commit is
**106–164 µs** — invisible beside the ~10 ms `PublishEvent` activity round trip. There is nothing
to optimize. Plain SQLite can still keep a warm read path (a small LRU over `GetRun`) without
adopting checkpoint semantics.

---

### Summary scoring

| | 1 · SQLite | 2 · Temporal history | 3 · PostgreSQL | 4 · S3 metadata | 5 · Hybrid |
|---|---|---|---|---|---|
| List 1000 runs, status + date | **166 µs** ✅ | 2.5–8.5 s ❌ | ~ms (est.) ✅ | 0.34–3.1 s ❌ | µs ✅ |
| Ordering (newest first) | ✅ | ❌ `ORDER BY` rejected | ✅ | manual sort in Go | ✅ |
| Survives engine restart | ✅ | ✅ | ✅ | ✅ | ⚠️ clean shutdown only |
| Survives Temporal loss / retention | ✅ | ❌ | ✅ | ✅ | ⚠️ |
| Survives MinIO loss | ✅ | ✅ | ✅ | ❌ | ✅ |
| New dev-stack containers | 0 | 0 (but needs a Temporal DB + volume) | 1 | 0 | 0 |
| Reuses existing code seams | ✅ | partial | ✅ | ❌ blocked | ✅ |
| MVP fit | **★★★★★** | ★☆☆☆☆ | ★★☆☆☆ | ★☆☆☆☆ | ★★☆☆☆ |
| Production fit | ★★★☆☆ | ★☆☆☆☆ | **★★★★★** | ★☆☆☆☆ | ★☆☆☆☆ |
| Migration path to v1.0 | ★★★★★ | ★☆☆☆☆ | n/a | ★☆☆☆☆ | ★★★☆☆ |

---

## 5. Direct answers to the two explicit questions

**Can Temporal's event history replace a separate run DB?** **No.** Three independent
disqualifiers, each sufficient on its own:

1. Reconstruct-on-read blows the latency budget — ~8.2 s serial / ~2.5 s at 8-way for 1000 runs,
   measured on localhost against an in-memory database.
2. `ORDER BY` is rejected by the SQL visibility store (re-verified this session), so "newest first"
   cannot be expressed.
3. Retention is a data-loss clock: 24 h on Temporal's built-in `default` namespace
   (re-verified this session) is this cluster's default for an unconfigured namespace, not
   a hard floor — retention-admin's proof (§4 above) moves this deployment's own namespace
   (`ork-main`) to 6 h and 2 h on the same cluster — and expiry deletes history *and* the
   visibility record regardless of the configured period, while the shipped dev stack's
   Temporal has no database file and no volume at all.

Storing orkflow YAML in Temporal is additionally hopeless: memo is not queryable and is capped at
2 MiB, and custom search attributes are quota-limited to 10 Keyword + 3 each of the other types on
the SQL visibility store. **Yes, a separate orkflow store is required.** Custom search attributes
*do* work on the dev server and list queries are fast (35–53 ms), so Temporal stays an excellent
**secondary reconciliation source** — just not the system of record.

**Is S3-backed metadata viable for v0.0.2?** **No.** Filtering by status requires reading object
bodies, because S3 LIST offers only prefix/delimiter/start-after: 3.094 s serial and 344 ms at
32-way for 1000 objects on localhost, roughly 150 s serial on real S3. There is no append on
standard S3 or community MinIO, so `runs/{id}.jsonl` is O(N²) bytes. In this repository
specifically, `pkg/artifacts.Store` is content-addressed and immutable, so it cannot express a
mutable run object at all, and `S3Store.List` already performs two extra sequential round trips per
object. It also fuses the metadata and artifact failure domains. MinIO's conditional writes *are*
fully working, which makes S3 a good **backup/export target** — not a system of record.

---

## 6. Implementation guidance for M7.5

Sequencing follows AGENTS.md §4:

1. **Proto first, in its own commit** (`chore(proto):`, then `make proto`): add date-range filtering
   to `ListRunsRequest` (`created_after` / `created_before` as `google.protobuf.Timestamp`) and make
   pagination real. Regenerate `gen/go` **and** `ui/src/lib/gen`.
2. **Promote `RunStore`/`MemoryOrkflowStore` to interfaces** in `internal/engine`, keeping the current
   maps as the `memory` implementation so every existing test keeps passing untouched.
3. **Add the SQLite implementation** with goose + `embed.FS` migrations. Two pools: a write pool
   with `SetMaxOpenConns(1)` and `_txlock=immediate`, and a read pool. Pragmas
   `journal_mode=WAL, synchronous=NORMAL, busy_timeout=5000, foreign_keys=ON`. Note the non-obvious
   gotcha: `busy_timeout` does **not** retry `SQLITE_BUSY_SNAPSHOT`, so read-then-write transactions
   must use `BEGIN IMMEDIATE`.
4. **Wire backend selection by env**, following `pkg/artifacts/registry.go:76-89`:
   `ORK_STORE_BACKEND=memory|sqlite`, `ORK_STORE_DSN`, default `./.data/orkstack.db`.
5. **Persist the event log, not just run snapshots.** `internal/engine/bus.go:54` derives `sequence`
   from in-memory log length, so a partial implementation leaves `StreamEvents` and
   `/ws/runs/{id}` resume broken after restart, with sequences restarting at 1.
6. **Decide the failure policy** at `internal/engine/workflow.go:462-467` before shipping (OQ-1).
7. **Add startup reconciliation** via `ListWorkflowExecutions` (35–53 ms measured) to heal runs
   whose terminal event was missed while the engine was down.
8. **Ownership.** This touches `internal/engine` and `internal/api` (engine agents), `cmd/orkctl`
   (CLI agent), `ui/` (UI agent), and `proto/` (coordinated) — AGENTS.md §8. Land the proto commit
   first so the rest can proceed in parallel.

**Schema notes.** `(name, version)` is a natural immutable primary key for orkflows
(`internal/dsl/hash.go:19-35`), and `Orkflow.source_yaml` means the row carries its full DSL.
Store the `Run` proto as a `BLOB` via `proto.Marshal` with hot predicates promoted to typed
columns. The measured 452 MB at 100 000 runs argues for a slim list projection rather than
returning the blob from `ListRuns` — and the re-measured latency table above is exactly that slim
projection, which is why it is 2–3× faster than the blob-carrying pass.

**Effort:** ~5–6 engineer-days.

---

## 7. Open questions

| ID | Question | Status |
|---|---|---|
| OQ-1 | `PublishEvent` swallows write failures (`internal/engine/workflow.go:840-845`). Once SQLite is authoritative this is silent data loss. | **Blocking** for "no data loss". Must be decided before M7.5 ships. |
| OQ-2 | Event-log persistence and sequence continuity across restart. | **Blocking** for WS / `StreamEvents` resume. |
| OQ-3 | How much of `internal/api/service.go`'s in-memory state (idempotency map, run-id counter) is persisted. | Open; drives the effort estimate's main variance. |
| OQ-4 | Retention / GC policy for `runs` and `run_events`. No retention policy exists anywhere in the repo today. | Open; newly required by the 452 MB / 100k-run measurement. |
| OQ-5 | Quadratic Temporal history growth: `s.snapshot()` was an activity input on every event, so history was ≈(3N+2) events × an O(N) snapshot. A 3-task run already measured 89 events / 41 KB. | **Resolved by M11.10**: the snapshot moved to object storage and the activity input became a URI; measured history growth is now linear (`proof/M11.10/README.md`). |

**Where evidence is missing.** PostgreSQL latency was not measured — no PostgreSQL in this stack.
All S3 numbers are localhost MinIO floors; real AWS was not measured. Temporal visibility lag was
measured only on an idle dev server (17–22 ms) and the docs give no SLA, warning it can be "several
seconds or longer" — do not design read-your-writes against 20 ms. The SQLite benchmarks ran on
darwin/amd64 with a synthetic 4 KB blob, not on CI hardware with real `Run` protos; re-run on CI
before quoting them in `proof/M7.5/`. A "255 characters per search-attribute value" limit appears
in Temporal docs but was not found in server source; plan for 255 on Keyword values.

---

## 8. Validation plan for M7.5

1. **Restart durability** (`-tags=integration`, `-p 1`): apply an orkflow, submit a run, wait for
   `SUCCEEDED`, `SIGKILL` *only* the engine (not `SIGTERM` — that is the point), restart, and assert
   `GetWorkflow`, `GetRun`, and `ListRuns` all return the pre-restart data. Name it
   `TestSQLiteStore_RunSurvivesEngineSIGKILL`.
2. **Latency gate:** seed 1000 and 10 000 runs; assert `ListRuns` with status + date range +
   orkflow name completes within a documented budget end-to-end through the Connect client (suggest
   100 ms, ~600× the measured store time). Commit the numbers to `proof/M7.5/`.
3. **Idempotency:** replay the same `dedupeKey` through `PublishEvent`; assert exactly one event row
   and one run row.
4. **Sequence continuity:** restart with an in-flight run; assert `StreamEvents(from_sequence=N)`
   resumes with no gaps or repeats and `/ws/runs/{id}` no longer 404s for a pre-restart run.
5. **Concurrent writers** under `-race`: several parallel runs, asserting no `SQLITE_BUSY` escapes.
6. **Migrations:** goose up on an empty DB and on a seeded DB; assert identical version, no data loss.
7. **Startup reconciliation:** kill the engine mid-run, let Temporal finish it, restart, assert the
   run converges to its terminal status.
8. **Cross-compile gate:** `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...` — proves the
   `modernc.org/sqlite` choice holds.
9. **Proof artifact** `proof/M7.5/` per AGENTS.md §6: `test.log`, coverage, the latency table, a
   restart transcript, and a single-command `reproduce.sh` modelled on `proof/M6/e2e.sh`, with
   stray-worker checks before and after.

---

## 9. Decision record (JSON)

```json
{
  "chosen_approach": "SQLite (modernc.org/sqlite, WAL) as the durable read model and system of record for orkflow definitions, runs, and run events, written exclusively from the existing EventActivities.PublishEvent activity, selected via ORK_STORE_BACKEND behind Store interfaces that keep the current in-memory maps as the 'memory' implementation.",
  "reasoning": "Measured on this machine, SQLite answers the exact target query (status + date range, newest 50) in 166 microseconds at 100,000 rows, with 23 microsecond point gets and 106 microsecond write commits - roughly 6000x inside the sub-second budget at 100x the stated scale, with zero new containers and no CGO. An earlier independent pass carrying the snapshot blob measured 404 microseconds for the same query, so the conclusion holds across both. Temporal event history is disqualified on three independent grounds: reconstructing run detail costs 8.2-8.5 ms/run serial and 2.5-2.6 ms/run at 8-way concurrency (about 8.2 s / 2.5 s for 1000 runs, on localhost against an in-memory DB), ORDER BY is rejected outright by the SQL visibility store so 'newest first' cannot be expressed, and the namespace retention is 24h while deploy/docker-compose.yml runs 'temporal server start-dev' with no --db-filename and no volume, making Temporal itself ephemeral. Both the ORDER BY rejection and the 24h retention were re-verified live against the running dev server. S3/MinIO metadata is disqualified because LIST exposes only prefix/delimiter/start-after, so filtering by status requires reading every object (3.094 s serial / 344 ms at 32-way for 1000 objects on localhost, roughly 150 s on real S3), standard S3 has no append so a runs/{id}.jsonl design is O(N^2) bytes, the existing pkg/artifacts Store is content-addressed and immutable (objects/{sha256(name)}/{sha256(content)}) so it cannot hold a mutable run object at all, its List already does two extra sequential round trips per object, and it would fuse the metadata and artifact failure domains. PostgreSQL is the correct v1.0 destination but is over-built for the MVP and adds a container the dev stack does not have. The hybrid checkpoint option optimizes a cost that does not exist (about 106-164 microseconds per commit) while reintroducing data loss on SIGKILL. The engine is already CQRS-shaped with a single idempotent write seam, so SQLite drops in behind PublishEvent with no change to Temporal workflow code and no violation of the determinism boundary.",
  "orkflow_strategy": "Table orkflows(name, version, description, labels_json, inputs_json, tasks_blob, source_yaml, created_at) with PRIMARY KEY (name, version) - immutable rows, since Orkflow.version is either the DSL-pinned value or the semantic content hash from internal/dsl/hash.go, and Orkflow.source_yaml already carries the full DSL for round-trip and audit. A separate orkflow_latest(name, version) pointer preserves the current 'most recently applied wins' semantics of MemoryOrkflowStore.Put. Apply() writes the row and updates the pointer in one transaction. This also unblocks listing all versions of a name, which MemoryOrkflowStore.List cannot do today.",
  "run_strategy": "Table runs(id PK, orkflow_name, orkflow_version, status, created_at, started_at, finished_at, snapshot BLOB) storing the Run proto via proto.Marshal, with hot predicates promoted to typed columns and indexes on created_at DESC, (status, created_at DESC), (orkflow_name, created_at DESC); plus run_events(run_id, sequence, dedupe_key, type, task_id, status, message, data_blob, timestamp) with UNIQUE(run_id, dedupe_key) and UNIQUE(run_id, sequence). PublishEvent performs one transaction: upsert the run snapshot (preserving created_at and input exactly as RunStore.Apply does today) and INSERT the event ON CONFLICT(run_id, dedupe_key) DO NOTHING, which makes Temporal activity retries and workflow replays idempotent for free. Sequence assignment moves from len(bus.log) to MAX(sequence)+1 within that transaction so StreamEvents and /ws/runs/{id} resume correctly after a restart. SubmitRun's pending run and idempotency key persist to the same DB. ListRuns gets a slim projection that omits the snapshot blob - the measured 166 microsecond figure is that projection - plus real keyset pagination on (created_at, id). On boot the engine restores nothing into memory and reconciles non-terminal runs against Temporal via ListWorkflowExecutions (35-53 ms measured).",
  "pros": [
    "Measured 166 us for the exact target query at 100k rows, 137 us status-filtered, 23 us point get - about 6000x inside the sub-second budget at 100x scale",
    "Zero new containers, zero credentials, zero external services - satisfies the 'no external databases in dev stack' constraint literally",
    "CGO-free with modernc.org/sqlite, so make build and cross-compilation stay unchanged",
    "Drops in behind the existing single write seam (EventActivities.PublishEvent) with no Temporal workflow-code change and no determinism-boundary risk",
    "The existing dedupeKey ('<runID>#<eventSeq>') is already the natural idempotency key for an ON CONFLICT insert",
    "Independent of Temporal's 24h retention and of the ephemeral start-dev server, so run data outlives both",
    "Independent of MinIO, keeping metadata and artifact failure domains separate",
    "Fixes real filters, ordering, and pagination that Temporal visibility cannot express (ORDER BY is rejected)",
    "Behind an interface, the same SQL migrates to PostgreSQL for v1.0 with placeholder changes only",
    "Optional Litestream replication to the MinIO already in the stack gives roughly 1 s RPO offsite backup with no new infrastructure"
  ],
  "cons": [
    "Single writer: an HA or multi-replica engine needs PostgreSQL, so this caps horizontal scale-out at one engine node",
    "Requires the single-write-pool discipline (SetMaxOpenConns(1), _txlock=immediate, busy_timeout) - a real footgun if a second write pool is added later, since busy_timeout does not retry SQLITE_BUSY_SNAPSHOT",
    "Adds migration tooling (goose + embed.FS) and SQLite's add/drop/rename-column-only ALTER TABLE, so schemas must be designed additively",
    "Measured 452 MB at 100k runs with a 4 KB snapshot per row, so a retention/GC policy is now required and was not previously",
    "New failure mode: PublishEvent currently swallows write errors, which becomes silent data loss once the store is authoritative - the policy must change before shipping",
    "A run's state now lives in two places (Temporal for execution, SQLite for the read model), so startup reconciliation is needed to heal divergence",
    "The one .db file is a new backup target that operators must know about"
  ],
  "effort_estimate": {
    "total": "5-6 engineer-days (about 1 engineer-week)",
    "breakdown": {
      "proto_date_range_and_pagination_plus_regen": "0.5d",
      "store_interfaces_keeping_memory_impl": "0.5d",
      "sqlite_implementation_schema_and_goose_migrations": "2d",
      "wiring_env_config_and_registry_selection": "0.5d",
      "restart_recovery_event_sequence_continuity_and_temporal_reconciliation": "0.5-1d",
      "unit_integration_tests_and_proof_artifact": "1.5d"
    },
    "confidence_in_estimate": "medium-high; the write seam and CQRS shape already exist, and the main unknown is how much of internal/api/service.go's in-memory state (idempotency map, run-id counter) the team decides to persist"
  },
  "unknown_risks": [
    "PublishEvent failure policy is undecided (internal/engine/workflow.go:462-467 logs and continues); if left as-is, a SQLite write failure is silent data loss and the 'no data loss' constraint is not actually met",
    "Event-log persistence and sequence continuity are unresolved; EventBus derives sequence from in-memory log length, so a partial implementation breaks StreamEvents/WS resume after restart",
    "The SQLite benchmarks ran on darwin/amd64 with a synthetic 4 KB blob, not on CI hardware with real Run protos; re-measure before quoting in proof",
    "Temporal visibility lag was measured only on an idle dev server (17-22 ms); docs give no SLA and warn it can be seconds, so startup reconciliation must not assume read-your-writes",
    "Quadratic Temporal history growth from passing the full Run snapshot as an activity input on every event (89 events / 41 KB measured for a 3-task run) is a pre-existing ceiling against the 50 MiB / 51,200-event server limits, unaffected by this decision but likely to surface in the same milestone",
    "PostgreSQL latency figures are estimates, not measurements - no Postgres exists in this stack",
    "All S3 numbers are localhost MinIO floors; real-AWS behaviour was not measured",
    "Concurrent write contention under a busier engine was not load-tested; the 106-164 us commit figure is single-threaded",
    "No retention/GC policy exists anywhere in the repo, so unbounded growth is currently unowned",
    "Changing deploy/, Makefile, proto/, and go.mod all cross the AGENTS.md section 8 coordinated boundary and need deliberate, announced commits"
  ],
  "validation_steps": [
    "Restart-durability integration test (-tags=integration, -p 1): apply orkflow, submit run to SUCCEEDED, SIGKILL only the engine, restart, assert GetOrkflow/GetRun/ListRuns all return pre-restart data",
    "Latency gate: seed 1,000 and 10,000 runs, assert ListRuns with status + date range + orkflow_name completes within a documented budget end-to-end through the Connect client; commit the numbers to proof/M7.5/",
    "Idempotency test: replay the same dedupeKey through PublishEvent and assert exactly one event row and one run row",
    "Sequence-continuity test: restart with an in-flight run, assert StreamEvents(from_sequence=N) resumes with no gaps or repeats and /ws/runs/{id} no longer 404s for a pre-restart run",
    "Concurrent-writer test under -race with several parallel runs, asserting no SQLITE_BUSY escapes",
    "Migration test: goose up on an empty DB and on a seeded DB, asserting identical version and no data loss",
    "Startup reconciliation test: kill the engine mid-run, let Temporal complete it, restart, assert the run converges to its terminal status",
    "Cross-compile gate: CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build ./...",
    "make lint test green, plus the tag-gated integration suites against make infra-up",
    "Proof artifact proof/M7.5/ with test.log, coverage, latency table, restart transcript, and a single-command reproduce.sh, with stray-worker checks before and after per AGENTS.md section 6"
  ],
  "decision": "GO",
  "confidence": "high"
}
```
