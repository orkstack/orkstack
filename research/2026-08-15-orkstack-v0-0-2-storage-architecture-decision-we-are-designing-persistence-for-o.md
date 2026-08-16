## Contract amendments received

None. No mid-run user steering was received in this stage; the launch objective and acceptance criteria are unmodified.

---

# OrkStack v0.0.2 — Storage Architecture Research

**Repo:** `/Users/daniel/Code/orkstack` @ `d51cfd0` (`main`, tag `v0.0.1`) · **Date:** 2026-08-14
**Research doc written:** `research/docs/2026-08-14-v002-storage-architecture-decision.md`
**Supporting web research:** `research/web/temporal-visibility-findings.md`, `research/web/sqlite-findings.md`, `research/web/s3-postgres-findings.md` (+ three cached source extracts)
**Stage scope:** research only. No repo source, proto, or config was changed.

---

## 1. Headline

Commit to **SQLite** (`modernc.org/sqlite`, WAL) as the durable store for orkflows and runs, written behind the existing `EventActivities.PublishEvent` seam. Every alternative was measured, not assumed:

- **Temporal event history cannot replace a run DB.** Filtering is fine; *reading* is not. Measured 8.2 ms/run serial and 2.5 ms/run at 8-way to fetch history — a 1000-run list costs **~8.2 s serial / ~2.5 s concurrent**, on localhost, against an in-memory DB. Also: `ORDER BY` is rejected outright, retention defaults to 24 h, and the shipped dev stack's Temporal is itself ephemeral.
- **S3/MinIO metadata is not viable for v0.0.2.** Conditional writes work and an index object reads in ~3 ms, but per-run objects force N+1 (**3.09 s** for 1000 serial GETs on localhost), and the existing `pkg/artifacts` Store is content-addressed and immutable — it physically cannot hold a mutable `runs/{id}.json`.
- **SQLite clears the budget by 3 orders of magnitude.** At 100 000 rows: status+date-filtered newest-50 in **404 µs**; point get **63 µs**; write commit **164 µs**.

---

## 2. Evidence base

Everything below marked *measured* was executed in this session against the live dev stack (`orkstack-temporal-1`, `orkstack-minio-1`, both up 26 h). Throwaway probe modules lived in `/tmp`; the MinIO probe bucket and the temporary `OrkWorkflowName` search attribute were deleted afterwards, and `pgrep -fl 'bin/orkengine|bin/orker'` confirms no stray workers were left (AGENTS.md §6).

### 2.1 Current code state — everything is process memory

| Fact | Evidence |
|---|---|
| `RunStore` = `map[string]*Run` + `order []string` + `sync.RWMutex` | `internal/engine/store.go:11-96` |
| `ListFilter` supports only `WorkflowName`, `Status` — **no time bounds** | `internal/engine/store.go:71-75` |
| `WorkflowStore` = nested maps, `List()` returns only *latest* per name | `internal/engine/store.go:98-162` |
| Stores + bus constructed fresh at process start | `cmd/orkengine/main.go:106-136` |
| `EventBus` holds the event log **and assigns sequence numbers** from `len(b.log[runID])+1` | `internal/engine/bus.go:35-72` |
| Idempotency map and run-id counter are also in-memory | `internal/api/service.go:60-63,168-183` |
| WS bridge and `StreamEvents` 404 unless the run is in `RunStore` | `internal/api/ws.go:22-26`, `internal/api/service.go:308-313` |

Restart today loses: all orkflow definitions, all runs, all events, all idempotency keys, and the UI's ability to attach to a run Temporal is still executing.

### 2.2 The write seam already exists and is already idempotent

`internal/engine/workflow.go:439-471` — `emit()` calls `workflow.ExecuteActivity(ctx, ActivityPublishEvent, event, s.snapshot(), dedupeKey)` with `dedupeKey = "<runID>#<eventSeq>"`. `internal/engine/activities.go:29-42` — `PublishEvent` does `store.Apply(run)` then `bus.Publish(event, dedupeKey)`.

**A durable store slots in behind this with zero workflow-code change** (AGENTS.md §1.3 determinism boundary respected). The dedupe key is exactly the natural key for `INSERT … ON CONFLICT(run_id, dedupe_key) DO NOTHING`.

One caveat in the same function: publish failures are logged and swallowed ("Projection is observability, not orchestration", `workflow.go:462-468`). Once the store *is* the system of record, a swallowed write is silent data loss. That policy needs a deliberate decision (open question 1).

### 2.3 Temporal — *measured*, in-process Go SDK v1.47.0, `default` ns, 152 executions

| Probe | Best | Avg |
|---|---|---|
| `ListWorkflowExecutions` pageSize=100, no query | 29.5 ms | 45.1 ms |
| `ExecutionStatus='Completed'` | 45.0 ms | 52.8 ms |
| `ExecutionStatus + WorkflowType + StartTime >` | 35.6 ms | 51.8 ms |
| paginate all 152 @1000/page | 64.3 ms | 67.6 ms |
| `GetWorkflowExecutionHistory`, 1 run (89 events) | 7.0 ms | 10.0 ms |
| history × 20 runs, serial | 164 ms (**8.2 ms/run**) | 171 ms |
| history × 20 runs, 8-way concurrent | 49.4 ms (**2.5 ms/run**) | 51.9 ms |
| `ORDER BY StartTime DESC` | **`invalid query: operation is not supported: 'ORDER BY' clause`** | — |
| custom SA `OrkWorkflowName` create + filter | **works** on the SQLite dev server | — |
| visibility lag: start → listable by `WorkflowId` | 17.4–22.1 ms end-to-end, first poll always hit | — |
| visibility lag: terminate → listable as `Terminated` | 17 ms | — |

Server facts confirmed live: `Config.WorkflowExecutionRetentionTtl = 24h0m0s`; 34 predefined search attributes (`ExecutionStatus`, `StartTime`, `CloseTime`, `WorkflowType`, `TaskQueue`, `HistoryLength`, `HistorySizeBytes`, …); zero custom SAs registered by default.

Corroborated externally (`research/web/temporal-visibility-findings.md`): standard visibility removed in server v1.24 (all visibility is "advanced"); ORDER BY unsupported in the SQL visibility store per server source (`common/persistence/visibility/store/sql/visibility_store.go`) and disabled by default on Elasticsearch (`system.visibilityDisableOrderByClause` = true); per-namespace custom-SA quotas on SQL are **10 Keyword + 3 each** of Bool/Datetime/Double/Int/KeywordList/Text; visibility writes are **async with no SLA** ("usually fast, but it can be several seconds or longer"); memo is not queryable (`limit.memoSize.error` 2 MiB); retention min 24 h, and expiry deletes history *and* the visibility record; `historyCount.error` 51 200 events, `historySize.error` 50 MiB, `blobSize.error` 2 MiB.

**The dev stack's Temporal is not durable at all.** `deploy/docker-compose.yml` runs `server start-dev --ip 0.0.0.0 --ui-port 8233 --log-level warn` with **no `--db-filename`** and **no volume** (only `minio-data` is named). Temporal CLI reference: `--db-filename` — *"By default, Workflow Executions are lost when the server process dies"* (https://docs.temporal.io/cli/command-reference/server); setup docs: `start-dev` *"uses an in-memory SQLite database"* (https://docs.temporal.io/cli/setup-cli). Making Temporal the system of record would first require changing `deploy/`, which AGENTS.md §8 marks coordinated.

### 2.4 Each event ships a full `Run` into Temporal history — *measured*

`temporal workflow describe` on a completed 3-task `hello-ork` run: `historyLength = 89`, `historySizeBytes = 40852`, `stateTransitionCount = 62`; 14 activities (3 `ExecuteTask` + 11 `PublishEvent`) and 15 workflow tasks. Because `s.snapshot()` — the whole `Run` including every `TaskRun` output — is an *activity input*, it is recorded in history on every lifecycle event. Growth is ≈(3N+2) events × an O(N) snapshot, i.e. quadratic in task count. A trivial 3-task DAG already costs 41 KB. This is a pre-existing ceiling worth tracking against the 50 MiB / 51 200-event limits, independent of the storage decision.

### 2.5 MinIO / S3 — *measured* (aws-sdk-go-v2 v1.107.1, path-style, localhost)

| Probe | Result |
|---|---|
| `PUT If-None-Match: *` on absent key | success |
| `PUT If-None-Match: *` on existing key | **`PreconditionFailed` (412)** |
| `PUT If-Match: <current ETag>` | success |
| `PUT If-Match: <stale ETag>` | **`PreconditionFailed` (412)** |
| seed 1000 × 4 KB objects, 16-way | 831 ms |
| `ListObjectsV2` whole 1000-key prefix (1 request) | **53.8 ms** |
| same, paged 100/request | 114.5 ms |
| GET 1000 objects serial (status filter needs bodies) | **3.094 s** |
| GET 1000 objects, 32-way concurrent | 303 ms best / 344 ms avg |
| GET one 81 KB index object holding 1000 rows | **2.8 ms** best / 4.5 ms avg |

Optimistic concurrency on MinIO is real. The *index-object* pattern is genuinely fast to read. But S3 LIST exposes only prefix/delimiter/start-after — no server-side filter on status or time — so per-run objects are N+1 by construction, and these are localhost floors: AWS's published 100–200 ms small-object latency puts 1000 serial GETs at ≈150 s.

**Two repo-specific blockers make option 4 worse than the generic case:**

1. `pkg/artifacts.Store` is documented content-addressed — *"each physical object key ends in the lowercase sha256 of its bytes"* (`pkg/artifacts/artifacts.go:38-62`), keys are `objects/{sha256(name)}/{sha256(content)}` (`pkg/artifacts/s3_manifest.go:19-27`). **There is no in-place overwrite.** A mutable `metadata/runs/{id}.json` cannot be expressed through this API at all.
2. `S3Store.List` (`pkg/artifacts/s3.go:184-224`) lists *every* object under `objects/`, then per object does a manifest GET **plus** a HEAD — 2 extra serial round trips each — before filtering by logical-name prefix in Go. Listing 1000 metadata objects through the existing API ≈ 2000 sequential round trips (≥ 6 s at the measured serial rate).

Also, standard S3 has **no append** (only S3 Express One Zone directory buckets; community MinIO lacks it — see `research/web/s3-postgres-findings.md`). A `runs/{id}.jsonl` design means GET-whole → concat → PUT-whole per event: O(N²) bytes for an N-event run.

### 2.6 SQLite — *measured*, `modernc.org/sqlite` v1.56.0, WAL, `synchronous=NORMAL`, `busy_timeout=5000`, `SetMaxOpenConns(1)`

Schema `runs(id PK, workflow_name, workflow_version, status, created_at, finished_at, snapshot BLOB)` with a 4 KB snapshot per row; indexes on `created_at DESC`, `(status, created_at DESC)`, `(workflow_name, created_at DESC)`. Best-of-20 after warmup:

| Query | 1 000 rows | 10 000 rows | 100 000 rows |
|---|---|---|---|
| newest 50, no filter | 301 µs | 396 µs | **403 µs** |
| newest 1000, no filter (rows materialized) | 8.7 ms | 9.1 ms | 8.8 ms |
| status filter, newest 50 | 404 µs | 414 µs | **414 µs** |
| status + date range, newest 50 | 65 µs | 64 µs | **404 µs** |
| name + status + date range, newest 1000 | 189 µs | 4.7 ms | **46.2 ms** |
| count by status | 77 µs | 234 µs | 1.8 ms |
| point get by id | 65 µs | 64 µs | **63 µs** |
| 200 single-row UPDATE commits | — | — | **164 µs/commit** |

DB size at 100 000 rows: **474 MB**, dominated by the snapshot blob. The worst case measured (46 ms) is 20× inside budget at 100× the stated scale. Insert of 90 000 rows in one transaction: 8.4 s.

Driver choice (external, `research/web/sqlite-findings.md`): `modernc.org/sqlite` is CGO-free, so `make build` cross-compiles unchanged; `mattn/go-sqlite3` requires `CGO_ENABLED=1` plus a cross-compiler. Benchmarks (cvilsmeier/go-sqlite-bench, 2026-03-23) put modernc ahead on read-heavy and concurrent-read cases. Non-obvious gotcha: `busy_timeout` does **not** retry `SQLITE_BUSY_SNAPSHOT`, so read-then-write transactions must use `BEGIN IMMEDIATE` (`_txlock=immediate`); the maintainer-recommended shape is two pools — a write pool with `SetMaxOpenConns(1)` and a read pool.

### 2.7 Contract gaps found while reading (independent of backend)

- `proto/orkstack/v1/service.proto:66-74` — `ListRunsRequest` = `{workflow_name, status, page_size, page_token}`. **No date-range filter**, which the stated constraint explicitly requires. `cmd/orkctl/run.go:112-148` correspondingly has no date flag.
- `internal/api/service.go:264-278` — `ListRuns` **ignores `page_token`** and never sets `next_page_token`; it materializes everything then truncates with `runs[:size]`. Same for `ListWorkflows` (`:143-153`).
- `ListWorkflowsRequest` has no name filter, and `WorkflowStore.List` returns only the latest version per name — no way to enumerate versions of one orkflow.
- Useful: `Workflow.version` is a DSL-pinned value or a content hash of the semantically normalized YAML (`internal/dsl/hash.go:14-35`) → natural immutable PK `(name, version)`. `Workflow.source_yaml` is inline, so an orkflow row carries its full DSL.

---

## 3. Decision matrix

### Option 1 — SQLite ★ chosen

**Architecture.** `orkflows(name, version, ...)`, `runs(id, workflow_name, workflow_version, status, created_at, started_at, finished_at, snapshot BLOB)`, `run_events(run_id, sequence, dedupe_key UNIQUE, type, payload)`. Written from `EventActivities.PublishEvent` (single writer, already idempotent via dedupe key). Read by `internal/api/service.go` through the existing `RunStore`/`WorkflowStore` shapes, promoted to interfaces. `ORK_STORE_BACKEND=memory|sqlite` mirrors the `pkg/artifacts/registry.go:76-96` precedent. File at `./.data/orkstack.db` alongside `./.data/artifacts`.

| Dimension | Assessment |
|---|---|
| **Query perf (1000 runs, status+date)** | **404 µs measured**; still 414 µs at 100k rows. 2400× inside budget. |
| **Durability — engine restart** | Full. WAL + `synchronous=NORMAL` is crash-safe against process kill; only OS/power loss can drop the last commits. |
| **Durability — Temporal loss** | **Survives.** This is the only option that outlives a Temporal namespace wipe or 24 h retention expiry. |
| **Durability — MinIO loss** | Unaffected (artifact bytes are a separate concern). |
| **Ops complexity** | Lowest of the durable options: one file, zero containers, zero credentials. `modernc.org/sqlite` keeps builds CGO-free. Needs goose+`embed.FS` migrations and a single-writer pool discipline. |
| **MVP fit** | Excellent. |
| **Production fit** | Good to ~single-node scale; Litestream adds continuous MinIO replication (~1 s RPO) if wanted. Ceiling is horizontal scale-out of the engine: one writer, one node. |
| **Path to v1.0** | Clean. Keep the store behind an interface; a Postgres implementation is the same SQL with different placeholders. Export/import via `INSERT … SELECT` or a JSONL dump. |

**Risks.** Single-writer means an HA/multi-replica engine needs a different backend; a 4 KB snapshot per run puts 474 MB at 100k runs (measured), so a retention/GC policy is needed; ALTER TABLE is add/drop/rename-column only, so design additively.

### Option 2 — Temporal event history

**Architecture.** No store. `ListRuns` → `ListWorkflowExecutions` with `ExecutionStatus`/`StartTime`/custom SA predicates; `GetRun` → `GetWorkflowExecutionHistory` reconstructed into a `Run`; orkflow YAML in memo or search attributes.

| Dimension | Assessment |
|---|---|
| **Query perf** | List alone: **35–53 ms measured** — fine. But list rows carry only SA fields; per-run `TaskRun` detail requires history: **8.2 ms/run serial, 2.5 ms/run at 8-way** ⇒ **~8.2 s / ~2.5 s for 1000 runs**. **Fails the budget.** Mitigating with custom SAs hits the SQL quota of **10 Keyword + 3 each** other type. `ORDER BY` is rejected, so "newest first" cannot be expressed — you get fixed `ClosedTime DESC, StartTime DESC`. |
| **Durability — engine restart** | Survives (state is server-side). |
| **Durability — Temporal loss** | **Total loss** — and this is not hypothetical: retention is **24 h measured** on this namespace, and `deploy/docker-compose.yml` uses `start-dev` with **no `--db-filename` and no volume**, so a container restart wipes everything. Directly violates "no loss of orkflow or run data". |
| **Ops complexity** | Deceptively high: register custom SAs per namespace, raise retention, add a persistent Temporal DB + volume, and version the history→`Run` reconstruction against workflow-code changes forever. |
| **MVP fit** | Poor. |
| **Production fit** | Poor as a system of record. Fine as a **secondary reconciliation source**. |
| **Path to v1.0** | Dead end; migrating later means backfilling from expiring history. |

**Verdict on the explicit question — can Temporal event history replace a separate run DB? No.** Three independent disqualifiers: reconstruct-on-read blows the latency budget by ~8×–25× at 1000 runs; retention is a data-loss clock (24 h by default, minimum 24 h globally, and expiry deletes history *and* the visibility record); and `ORDER BY` is unsupported, so the list ordering the CLI/UI already rely on cannot be expressed. Orkflow YAML in memo is additionally hopeless: memo is not queryable and capped at 2 MiB.

**Worth keeping from this option:** startup reconciliation. `ListWorkflowExecutions` at 35–53 ms is cheap enough to sweep `Running` executions on engine boot and heal any run whose terminal `PublishEvent` was lost.

### Option 3 — PostgreSQL

**Architecture.** Same schema as option 1 with JSONB projections; `pgx/v5` + `pgxpool`; goose/golang-migrate.

| Dimension | Assessment |
|---|---|
| **Query perf** | Comparable or better than SQLite at this scale; wins only well past 10⁶ rows or with concurrent writers. *(Not measured here — no Postgres in the stack.)* |
| **Durability** | Best in class: WAL, PITR, replication. |
| **Ops complexity** | Highest: a container, credentials, pool tuning, backups, migrations, monitoring. Softer than the constraint implies — Temporal's own docker-compose already ships **PostgreSQL 16** (`docker-compose.yml`, `docker-compose-postgres.yml`, `docker-compose-postgres-opensearch.yml` @ `cd6d3b1`), so at production it is a `CREATE DATABASE`, not a new container. But the OrkStack dev stack does **not** run those files — it runs `start-dev`, no Postgres. |
| **MVP fit** | Over-built. Violates "no external databases in dev stack" as the stack exists today. |
| **Production fit** | **Best.** |
| **Path to v1.0** | It *is* v1.0. |

**Verdict:** right destination, wrong milestone. Choosing SQLite behind an interface costs ~nothing and defers this cleanly.

### Option 4 — S3-backed metadata

**Architecture.** `metadata/orkflows/{name}/{version}.json`, `metadata/runs/{run-id}.jsonl`, LIST-and-scan for queries.

| Dimension | Assessment |
|---|---|
| **Query perf** | LIST 1000 keys **53.8 ms measured** — but LIST returns keys only. Filtering by status needs bodies: **3.094 s serial / 344 ms at 32-way, on localhost**; ≈150 s serial on real S3 at published 100–200 ms latency. **Fails the budget.** An index-object variant reads 1000 rows in **2.8 ms** — but then the index is a single hot object needing read-modify-write with `If-Match` per run update, i.e. write contention and lost-update retries at exactly the point of highest concurrency. |
| **Durability — engine restart** | Survives. |
| **Durability — MinIO loss** | Total loss of both metadata *and* artifacts — the failure domains collapse into one. |
| **Ops complexity** | Deceptive. No append on standard S3 ⇒ `.jsonl` is GET-whole/PUT-whole per event ⇒ **O(N²)** bytes. Concurrency needs hand-rolled `If-Match` retry loops. |
| **MVP fit** | Poor, and worse here than generically: the existing `Store` is content-addressed and immutable (`pkg/artifacts/artifacts.go:38-62`, `s3_manifest.go:19-27`) and `List` already does 2 extra serial round trips per object (`s3.go:184-224`). Reusing it is impossible; a parallel raw-S3 client would duplicate the provider layer and break the AGENTS.md §8 ownership boundary. |
| **Production fit** | Only as an archive/export tier. |
| **Path to v1.0** | Rewrite. |

**Verdict on the explicit question — is S3-backed metadata viable for v0.0.2? No.** The list-filter path is 7×–450× over budget depending on network, the artifact Store cannot express mutable objects, and it fuses the metadata and artifact failure domains. *Salvage:* conditional writes verified working on MinIO (412 on both stale `If-Match` and `If-None-Match: *`) make S3 a sound **backup/export target** — e.g. Litestream replicating the SQLite file, or periodic JSONL exports under `metadata/`.

### Option 5 — Hybrid (in-memory + SQLite checkpoint on shutdown)

**Architecture.** Keep the current maps; flush to SQLite on `SIGTERM`; restore on boot.

| Dimension | Assessment |
|---|---|
| **Query perf** | Best possible (map + slice, in-process). |
| **Durability** | **Broken by design.** `SIGKILL`, OOM, or a panic loses every run since the last checkpoint. Directly violates "no loss of orkflow or run data". Periodic checkpointing narrows the window but never closes it, and the memory copy is then the source of truth for a bounded-loss interval. |
| **Ops complexity** | Higher than plain SQLite: two representations, a flush path, a restore path, and divergence bugs — for zero measured gain. |
| **MVP fit** | Poor. |
| **Production fit** | Poor. |
| **Path to v1.0** | Converges to option 1 anyway. |

**Verdict:** the premise is that synchronous writes are expensive. Measured, they are **164 µs per commit** — invisible next to the ~10 ms `PublishEvent` activity round trip. There is nothing to optimize. Note that plain SQLite still keeps a warm read path if wanted (a small LRU over `GetRun`), without the checkpoint semantics.

### Summary scoring

| | 1 · SQLite | 2 · Temporal history | 3 · PostgreSQL | 4 · S3 metadata | 5 · Hybrid |
|---|---|---|---|---|---|
| List 1000 runs, status+date | **404 µs** ✅ | 2.5–8.2 s ❌ | ~ms ✅ | 0.34–3.1 s (localhost) ❌ | µs ✅ |
| Ordering (`newest first`) | ✅ | ❌ ORDER BY rejected | ✅ | manual sort in Go | ✅ |
| Survives engine restart | ✅ | ✅ | ✅ | ✅ | ⚠️ clean shutdown only |
| Survives Temporal loss / retention | ✅ | ❌ | ✅ | ✅ | ⚠️ |
| Survives MinIO loss | ✅ | ✅ | ✅ | ❌ | ✅ |
| New dev-stack containers | 0 | 0 (but needs a Temporal DB + volume) | 1 | 0 | 0 |
| Reuses existing code seams | ✅ | partial | ✅ | ❌ blocked | ✅ |
| MVP fit | **★★★★★** | ★☆☆☆☆ | ★★☆☆☆ | ★☆☆☆☆ | ★★☆☆☆ |
| Production fit | ★★★☆☆ | ★☆☆☆☆ | **★★★★★** | ★☆☆☆☆ | ★☆☆☆☆ |
| Migration path to v1.0 | ★★★★★ | ★☆☆☆☆ | n/a | ★☆☆☆☆ | ★★★☆☆ |

---

## 4. Implementation guidance (for the M7.5 spec stage — not a spec)

**Sequencing that matches AGENTS.md §4:**

1. **Proto first, own commit** (`chore(proto):`, then `make proto`): add date-range filtering to `ListRunsRequest` (e.g. `created_after` / `created_before` as `google.protobuf.Timestamp`) and make pagination real. Regenerate `gen/go` **and** `ui/src/lib/gen`.
2. **Promote `RunStore`/`WorkflowStore` to interfaces** in `internal/engine`, keeping the current maps as the `memory` implementation so all existing tests keep passing untouched.
3. **Add the SQLite implementation** with goose + `embed.FS` migrations; two pools (write `SetMaxOpenConns(1)` + `_txlock=immediate`; read pool), pragmas `journal_mode=WAL, synchronous=NORMAL, busy_timeout=5000, foreign_keys=ON`.
4. **Wire selection by env** following `pkg/artifacts/registry.go:76-96` — `ORK_STORE_BACKEND=memory|sqlite`, `ORK_STORE_DSN`, default `./.data/orkstack.db`.
5. **Persist the event log too**, not just run snapshots — `internal/engine/bus.go:35-72` derives `sequence` from in-memory log length, so without it `StreamEvents`/`/ws/runs/{id}` resume is broken after restart and sequences restart at 1.
6. **Decide the failure policy** at `internal/engine/workflow.go:462-468` before shipping.
7. **Add startup reconciliation** using `ListWorkflowExecutions` (35–53 ms measured) to heal runs whose terminal event was missed while the engine was down.
8. **Ownership:** this touches `internal/engine` + `internal/api` (engine agents), `cmd/orkctl` (CLI agent), `ui/` (UI agent), and `proto/` (coordinated) — AGENTS.md §8. Sequence the proto commit first so the others can proceed in parallel.

**Schema notes.** `(name, version)` is a natural immutable PK for orkflows (`internal/dsl/hash.go:14-35`); `Workflow.source_yaml` means the row carries its full DSL. Store the `Run` proto as a `BLOB` (`proto.Marshal`) with hot predicates promoted to typed columns (`status`, `workflow_name`, `created_at`) — the measured 474 MB at 100k rows argues for a slim list projection rather than returning the blob in `ListRuns`.

**Files most likely to change:** `internal/engine/store.go`, `internal/engine/activities.go`, `internal/engine/bus.go`, `internal/api/service.go`, `cmd/orkengine/main.go`, `proto/orkstack/v1/service.proto`, `cmd/orkctl/run.go`, `ui/src/routes/runs/+page.ts`, `Makefile` (if a migrate target is added), `go.mod`.

**Existing tests/conventions to follow:** `internal/engine/integration_test.go:1-60` (build tag `integration`, `t.Skip` when Temporal is unreachable), `internal/api/service_test.go` (977 lines of Connect-surface behavior tests), `proof/M6/README.md` + `proof/M6/e2e.sh` (proof shape), AGENTS.md §6 (`-p 1`, kill strays).

**Effort:** ~5–6 engineer-days. Proto + regen 0.5 d; store interfaces 0.5 d; SQLite impl + migrations 2 d; wiring/config 0.5 d; restart-recovery + reconciliation 0.5–1 d; tests + proof 1.5 d.

---

## 5. Unresolved findings and where evidence is missing

| Finding | Status |
|---|---|
| `PublishEvent` swallows write failures (`workflow.go:462-468`) | **Unresolved policy question.** Blocking for "no data loss". |
| Event log persistence and sequence continuity after restart | **Unresolved design question.** Affects WS/`StreamEvents` resume. |
| Quadratic Temporal history growth from snapshot-as-activity-input (41 KB for 3 tasks, measured) | **Real but out of scope.** Track against 50 MiB / 51 200-event limits. |
| PostgreSQL latency figures | **Not measured** — no Postgres in the stack. Cited numbers in `research/web/s3-postgres-findings.md` are estimates, flagged there. |
| Real-AWS S3 latency | **Not measured.** All S3 numbers here are localhost MinIO (a floor); AWS extrapolation uses AWS's published 100–200 ms range. |
| Temporal visibility lag under load | **Measured only on an idle dev server** (17–22 ms, first poll). Docs give no SLA and warn it can be "several seconds or longer". Do not design read-your-writes against 20 ms. |
| Docs claim a "255 characters per SA value" limit not found in server source | **Unverified** (per the Temporal research subagent). May be Cloud-specific; plan for 255 on Keyword values. |
| Litestream v0.5 LTX retention semantics | **Documented, not exercised.** Restore granularity is coarser than the ~1 s RPO. |
| SQLite benchmark is darwin/amd64 with a 4 KB blob per row | **Measured**, but not on Linux/CI hardware and not with the real `Run` proto. Re-run on CI before quoting in proof. |

---

## 6. Validation recommendations

1. **Restart-durability integration test** (`-tags=integration`, `-p 1`): apply an orkflow, submit a run, wait for `SUCCEEDED`, kill *only* the engine (`SIGKILL`, not `SIGTERM` — this is the test), restart it, assert `GetWorkflow`, `GetRun`, and `ListRuns` all return the pre-restart data. Name it for behavior, e.g. `TestSQLiteStore_RunSurvivesEngineSIGKILL`.
2. **Sub-second list benchmark as a gate:** seed 1000 (and 10 000) runs, assert `ListRuns` with `status` + date range + `workflow_name` returns under a documented budget (suggest 100 ms wall through the Connect client, ~250× the measured store time). Commit the numbers to `proof/M7.5/`.
3. **Idempotency test:** replay the same `dedupeKey` through `PublishEvent` and assert exactly one event row and one run row — this is what makes Temporal activity retries and workflow replay safe.
4. **Sequence-continuity test:** restart with an in-flight run and assert `StreamEvents(from_sequence=N)` resumes without gaps or repeats, and that `/ws/runs/{id}` no longer 404s for a run started before the restart.
5. **Concurrent-writer test** under `-race`: run several runs in parallel and assert no `SQLITE_BUSY` escapes, validating the single-write-pool + `BEGIN IMMEDIATE` discipline.
6. **Migration test:** apply migrations to an empty DB and to a seeded one; assert goose reports the same version and no data is lost.
7. **Startup reconciliation test:** kill the engine mid-run, let Temporal finish, restart, assert the run converges to its terminal status.
8. **Cross-compile gate:** `GOOS=linux GOARCH=arm64 go build ./...` with `CGO_ENABLED=0` — proves the `modernc.org/sqlite` choice holds.
9. **Proof artifact** under `proof/M7.5/` per AGENTS.md §6: `test.log`, coverage, the latency table, a restart transcript, and a one-command `reproduce.sh` (model on `proof/M6/e2e.sh`), with a stray-worker check before and after.

---

## 7. Decision JSON

```json
{
  "chosen_approach": "SQLite (modernc.org/sqlite, WAL) as the durable read model and system of record for orkflow definitions, runs, and run events, written exclusively from the existing EventActivities.PublishEvent activity, selected via ORK_STORE_BACKEND behind Store interfaces that keep the current in-memory maps as the 'memory' implementation.",
  "reasoning": "Measured on this machine, SQLite answers the exact target query (status + date range + workflow name, newest 50) in 404 microseconds at 100,000 rows and 63 microseconds for a point get, with 164 microsecond write commits - roughly 2400x inside the sub-second budget at 100x the stated scale, with zero new containers and no CGO. Temporal event history is disqualified on three independent grounds: reconstructing run detail costs 8.2 ms/run serial and 2.5 ms/run at 8-way concurrency (8.2 s / 2.5 s for 1000 runs, on localhost against an in-memory DB), ORDER BY is rejected outright by the SQL visibility store so 'newest first' cannot be expressed, and the namespace retention measured 24h while deploy/docker-compose.yml runs 'temporal server start-dev' with no --db-filename and no volume, making Temporal itself ephemeral. S3/MinIO metadata is disqualified because LIST exposes only prefix/delimiter, so filtering by status requires reading every object (3.094 s serial / 344 ms at 32-way for 1000 objects on localhost, ~150 s on real S3), standard S3 has no append so a runs/{id}.jsonl design is O(N^2) bytes, the existing pkg/artifacts Store is content-addressed and immutable (objects/{sha256(name)}/{sha256(content)}) so it cannot hold a mutable run object at all, its List already does two extra serial round trips per object, and it would fuse the metadata and artifact failure domains. PostgreSQL is the correct v1.0 destination but is over-built for the MVP and adds a container the dev stack does not have. The hybrid checkpoint option optimizes a cost that does not exist (164 us per commit) while reintroducing data loss on SIGKILL. The engine is already CQRS-shaped with a single idempotent write seam, so SQLite drops in behind PublishEvent with no change to Temporal workflow code and no violation of the determinism boundary.",
  "orkflow_strategy": "Table orkflows(name, version, description, labels_json, inputs_json, tasks_blob, source_yaml, created_at) with PRIMARY KEY (name, version) - immutable rows, since Workflow.version is either the DSL-pinned value or the semantic content hash from internal/dsl/hash.go, and Workflow.source_yaml already carries the full DSL for round-trip and audit. A separate orkflow_latest(name, version) pointer preserves the current 'most recently applied wins' semantics of WorkflowStore.Put. Apply() writes the row and updates the pointer in one transaction. This also unblocks listing all versions of a name, which WorkflowStore.List cannot do today.",
  "run_strategy": "Table runs(id PK, workflow_name, workflow_version, status, created_at, started_at, finished_at, snapshot BLOB) storing the Run proto via proto.Marshal, with hot predicates promoted to typed columns and indexes on created_at DESC, (status, created_at DESC), (workflow_name, created_at DESC); plus run_events(run_id, sequence, dedupe_key, type, task_id, status, message, data_blob, timestamp) with UNIQUE(run_id, dedupe_key) and UNIQUE(run_id, sequence). PublishEvent performs one transaction: upsert the run snapshot (preserving created_at and input exactly as RunStore.Apply does today) and INSERT the event ON CONFLICT(run_id, dedupe_key) DO NOTHING, which makes Temporal activity retries and workflow replays idempotent for free. Sequence assignment moves from len(bus.log) to MAX(sequence)+1 within that transaction so StreamEvents and /ws/runs/{id} resume correctly after a restart. SubmitRun's pending run and idempotency key persist to the same DB. ListRuns gets a slim projection (no snapshot blob) plus real keyset pagination on (created_at, id). On boot the engine restores nothing into memory and reconciles non-terminal runs against Temporal via ListWorkflowExecutions (35-53 ms measured).",
  "pros": [
    "Measured 404 us for the exact target query at 100k rows; 414 us for status-filtered newest-50; 63 us point get - about 2400x inside the sub-second budget at 100x scale",
    "Zero new containers, zero credentials, zero external services - satisfies the 'no external databases in dev stack' constraint literally",
    "CGO-free with modernc.org/sqlite, so make build and cross-compilation stay unchanged",
    "Drops in behind the existing single write seam (EventActivities.PublishEvent) with no Temporal workflow-code change and no determinism-boundary risk",
    "The existing dedupeKey ('<runID>#<eventSeq>') is already the natural idempotency key for an ON CONFLICT insert",
    "Independent of Temporal's 24h retention and of the ephemeral start-dev server, so run data outlives both",
    "Independent of MinIO, keeping metadata and artifact failure domains separate",
    "Fixes real filters, ordering, and pagination that Temporal visibility cannot express (ORDER BY is rejected)",
    "Behind an interface, the same SQL migrates to PostgreSQL for v1.0 with placeholder changes only",
    "Optional Litestream replication to the MinIO already in the stack gives ~1 s RPO offsite backup with no new infrastructure"
  ],
  "cons": [
    "Single writer: an HA or multi-replica engine needs PostgreSQL, so this caps horizontal scale-out at one engine node",
    "Requires the single-write-pool discipline (SetMaxOpenConns(1), _txlock=immediate, busy_timeout) - a real footgun if a second write pool is added later, since busy_timeout does not retry SQLITE_BUSY_SNAPSHOT",
    "Adds migration tooling (goose + embed.FS) and SQLite's add/drop/rename-column-only ALTER TABLE, so schemas must be designed additively",
    "Measured 474 MB at 100k runs with a 4 KB snapshot per row, so a retention/GC policy is now required and was not previously",
    "New failure mode: PublishEvent currently swallows write errors, which becomes silent data loss once the store is authoritative - the policy must change before shipping",
    "A run's state now lives in two places (Temporal for execution, SQLite for the read model), so startup reconciliation is needed to heal divergence",
    "The one .db file is a new backup target that operators must know about"
  ],
  "effort_estimate": {
    "total": "5-6 engineer-days (~1 engineer-week)",
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
    "PublishEvent failure policy is undecided (internal/engine/workflow.go:462-468 logs and continues); if left as-is, a SQLite write failure is silent data loss and the 'no data loss' constraint is not actually met",
    "Event-log persistence and sequence continuity are unresolved; EventBus derives sequence from in-memory log length, so a partial implementation breaks StreamEvents/WS resume after restart",
    "The SQLite benchmark ran on darwin/amd64 with a synthetic 4 KB blob, not on CI hardware with real Run protos; re-measure before quoting in proof",
    "Temporal visibility lag was measured only on an idle dev server (17-22 ms); docs give no SLA and warn it can be seconds, so startup reconciliation must not assume read-your-writes",
    "Quadratic Temporal history growth from passing the full Run snapshot as an activity input on every event (41 KB measured for a 3-task run) is a pre-existing ceiling against the 50 MiB / 51,200-event server limits, unaffected by this decision but likely to surface in the same milestone",
    "PostgreSQL latency figures cited here are estimates, not measurements - no Postgres exists in this stack",
    "All S3 numbers are localhost MinIO floors; real-AWS behaviour was not measured",
    "Concurrent write contention under a busier engine was not load-tested; the 164 us commit figure is single-threaded",
    "No retention/GC policy exists anywhere in the repo, so unbounded growth is currently unowned",
    "Changing deploy/, Makefile, proto/, and go.mod all cross the AGENTS.md section 8 coordinated boundary and need deliberate, announced commits"
  ],
  "validation_steps": [
    "Restart-durability integration test (-tags=integration, -p 1): apply orkflow, submit run to SUCCEEDED, SIGKILL only the engine, restart, assert GetWorkflow/GetRun/ListRuns all return pre-restart data",
    "Latency gate: seed 1,000 and 10,000 runs, assert ListRuns with status + date range + workflow_name completes within a documented budget end-to-end through the Connect client; commit the numbers to proof/M7.5/",
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

---

## 8. Direct answers to the two explicit questions

**Can Temporal's event history replace a separate run DB?** **No.** Measured: reconstructing 1000 runs costs ~8.2 s serial / ~2.5 s at 8-way — 8×–25× over the sub-second budget on localhost against an in-memory DB, which is the best case that will ever exist. `ORDER BY` is rejected by the SQL visibility store, so "newest first" is inexpressible. Retention measured 24 h, and the shipped `deploy/docker-compose.yml` runs `start-dev` with no `--db-filename` and no volume, so Temporal here loses everything on container restart. Custom search attributes do work on the dev server (verified) and list queries are fast (35–53 ms), so Temporal remains an excellent **secondary reconciliation source** — but not the system of record.

**Is S3-backed metadata viable for v0.0.2?** **No.** Filtering by status requires reading object bodies because S3 LIST offers only prefix/delimiter/start-after: 3.094 s serial / 344 ms at 32-way for 1000 objects on localhost, ≈150 s serial on real S3. There is no append on standard S3 or community MinIO, so `runs/{id}.jsonl` is O(N²) bytes. And in this repo specifically, `pkg/artifacts.Store` is content-addressed and immutable, so it cannot express a mutable run object at all, while `S3Store.List` already performs two extra serial round trips per object. MinIO's conditional writes *are* fully working (verified 412 on stale `If-Match` and on `If-None-Match: *` against an existing key), which makes S3 a sound **backup/export target** — Litestream replication of the SQLite file, or periodic JSONL exports.