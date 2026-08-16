---
date: 2026-08-14 23:05:42 MDT
researcher: Claude Opus 5 (Atomic research stage)
git_commit: d51cfd03b235ff508e461de5f344311472db5be5
branch: main
repository: orkstack
topic: "OrkStack v0.0.2 persistence architecture for orkflow definitions and runs (pre-M7.5 decision)"
tags: [research, storage, sqlite, temporal, visibility, s3, minio, postgres, engine, api]
status: complete
last_updated: 2026-08-14
last_updated_by: Claude Opus 5
breaking_changes_allowed: true
compatibility_context: "v0.0.1 is the first tag; no external downstream consumers are documented in the repo. Proto is the shared contract (AGENTS.md §1, §8) so proto edits must be deliberate and regenerated, but no backward-compatibility shim is owed to outside users."
---

# Research

## Research Question

Which persistence architecture should OrkStack commit to for orkflow definitions and runs
before M7.5, given the in-memory stores in `internal/engine/store.go`, the Connect surface in
`internal/api/service.go`, the Temporal determinism boundary in `internal/engine/workflow.go`,
`pkg/artifacts` (local FS + S3/MinIO), and a dev stack (`deploy/docker-compose.yml`) with only
Temporal + MinIO? Can Temporal's own persistence or the MinIO artifact store be the system of
record instead of a separate DB, under: survives engine restart, no data loss, sub-second CLI
list/filter (status, date range, orkflow name) over ~1000 runs?

## Compatibility Context

Breaking changes are allowed. `v0.0.1` (`d51cfd0`) is the only tag and no downstream consumers
are recorded. Proto (`proto/orkstack/v1/`) is the shared contract under AGENTS.md §1/§8: changes
go in a dedicated commit with `make proto`. Generated clients (`gen/go`, `ui/src/lib/gen`) must be
regenerated together.

## Summary

Measured on this machine against the live dev stack, the answer is decisive:

- **SQLite is the right choice**, by three orders of magnitude on the stated latency budget.
- **Temporal event history cannot be the run store.** Filtering works and is fast, but
  reconstruct-on-read is the killer: 8.5 ms/run serial, 2.6 ms/run at 8-way concurrency, so a
  1000-run list costs ~8.5 s serial / ~2.6 s concurrent on localhost with an in-memory DB. Add
  `ORDER BY` being rejected outright by the SQL visibility store, 24 h default retention, and the
  fact that `deploy/docker-compose.yml` runs `temporal server start-dev` with **no `--db-filename`
  and no volume** — Temporal here is itself ephemeral.
- **S3/MinIO metadata is not viable as the system of record for v0.0.2.** Conditional writes work
  (verified 412s on MinIO), and a single index object reads in ~3–4 ms, but any per-run object
  layout forces N+1: 1000 GETs took 3.09 s serial / 344 ms at 32-way — on localhost. And the
  existing `pkg/artifacts` Store is content-addressed and immutable
  (`objects/{sha256(name)}/{sha256(content)}`), so it cannot express a mutable `runs/{id}.json`
  at all without new API surface.
- The **hybrid (in-memory + SQLite checkpoint on shutdown)** option is strictly worse than plain
  SQLite: SQLite writes measured 164 µs per commit, so there is nothing to buy by deferring them,
  and a checkpoint-on-shutdown loses everything on `SIGKILL`.
- **PostgreSQL is the v1.0 target, not the v0.0.2 answer.** Notably, Temporal's own
  docker-compose already ships PostgreSQL 16, so the "no external DB" constraint is softer than it
  looks — but it is still an extra container plus pooling and migrations that MVP does not need.

Two contract gaps found while reading the repo, independent of the storage choice:
`ListRunsRequest` has **no date-range filter**, and `page_token`/`next_page_token` are declared but
never implemented in `internal/api/service.go`.

## Detailed Findings

### 1. Current state: everything is in-process memory

- `internal/engine/store.go:11-96` — `RunStore` is `map[string]*orkstackv1.Run` + an `order`
  slice, guarded by `sync.RWMutex`. Its doc comment already says "Temporal remains the source of
  truth; this store materializes Run/TaskRun snapshots for the API."
- `internal/engine/store.go:98-162` — `WorkflowStore` is `byName map[string]map[string]*Workflow`
  plus a `latest` map. No persistence, no `Delete`, no version listing (List returns only latest).
- `internal/engine/store.go:71-75` — `ListFilter` supports only `WorkflowName` and `Status`. No
  time bounds, no pagination cursor.
- `cmd/orkengine/main.go:106-136` — the process constructs `engine.NewEventBus()`,
  `engine.NewRunStore()`, and `engine.NewWorkflowStore()` at startup. Restart ⇒ every orkflow, run,
  and event is gone.
- `internal/engine/bus.go:15-72` — `EventBus` keeps `log map[string][]*Event`, `seen` (dedupe keys),
  and subscriber sets in memory, and it **owns sequence assignment**
  (`stored.Sequence = int64(len(b.log[runID])) + 1`). Persisting runs without persisting the event
  log would reset sequence numbering after restart.
- `internal/api/service.go:60-63,168-183` — the idempotency map (`idempotencyKey → runID`) and the
  run-id counter are also in-process only, so `SubmitRun` dedupe does not survive restart.
- `internal/api/ws.go:22-26` and `internal/api/service.go:308-313` — both `/ws/runs/{id}` and
  `StreamEvents` return 404/NotFound unless `svc.Runs().Get(runID)` succeeds. After a restart the
  UI cannot attach to a run that Temporal is still executing.

### 2. The determinism boundary already has the write seam

- `internal/engine/workflow.go:439-471` — `runState.emit` builds an `Event` and calls
  `workflow.ExecuteActivity(ctx, ActivityPublishEvent, event, s.snapshot(), dedupeKey)` on
  `EventQueue`, with `dedupeKey = fmt.Sprintf("%s#%d", runID, eventSeq)`. A failed publish is
  logged and the run continues ("Projection is observability, not orchestration").
- `internal/engine/activities.go:29-42` — `EventActivities.PublishEvent` is the single mutation
  point: `a.store.Apply(run)` then `a.bus.Publish(event, dedupeKey)`.
- Consequence: a durable store slots in behind `PublishEvent` with **no workflow-code change**.
  The dedupe key already makes the write idempotent across activity retries and replays, which is
  exactly what an `INSERT … ON CONFLICT DO NOTHING` on `(run_id, dedupe_key)` needs.
- Caveat in the same code path: `emit` swallows publish failures. If SQLite becomes the system of
  record, a swallowed write is silent data loss; the failure policy needs a deliberate decision.

### 3. Each event ships a full Run snapshot into Temporal history (measured)

`temporal workflow describe` on a completed 3-task `hello-ork` run
(`run-1786765702503407000-1`) reports `historyLength = 89`, `historySizeBytes = 40852`,
`stateTransitionCount = 62`; the event histogram is 14 activities (3 `ExecuteTask` + 11
`PublishEvent`) and 15 workflow tasks. So a trivial 3-task DAG already burns ~41 KB of Temporal
history, because `s.snapshot()` (the whole `Run`, including every `TaskRun` output) is an activity
*input* recorded in history on every lifecycle event. Growth is roughly quadratic in task count
(≈3N+2 events × an O(N) snapshot). Against the server defaults
(`historyCount.error` 51 200 events, `historySize.error` 50 MiB, `blobSize.error` 2 MiB) this is a
real ceiling for large DAGs, independent of the storage decision.

### 4. Temporal visibility: measured on the live dev server

Probes run against `orkstack-temporal-1` (`temporalio/temporal:latest`, `server start-dev`,
152 executions in `default`) with Go SDK v1.47.0 in-process:

| Probe | Result |
|---|---|
| `ListWorkflowExecutions` pageSize=100, no query | best 29.5 ms, avg 45.1 ms |
| `… Query: ExecutionStatus='Completed'` | best 45.0 ms, avg 52.8 ms |
| `… status + WorkflowType + StartTime >` | best 35.6 ms, avg 51.8 ms |
| `… ORDER BY StartTime DESC` | **error: `invalid query: operation is not supported: 'ORDER BY' clause`** |
| page through all 152 executions @1000/page | best 64.3 ms |
| `GetWorkflowExecutionHistory`, 1 run (89 events) | best 7.0 ms, avg 10.0 ms |
| history of 20 runs, serial | best 164 ms (**8.2 ms/run**) |
| history of 20 runs, 8-way concurrent | best 49.4 ms (**2.5 ms/run**) |
| custom SA `OrkWorkflowName` (Keyword) create + query | **works** on the SQLite dev server |
| visibility lag: start → visible by `WorkflowId` | 17.4–22.1 ms total incl. the ~6–12 ms `StartWorkflow`; first poll always hit |
| visibility lag: terminate → visible as `Terminated` | 17 ms |

Environment facts confirmed on the same server: `Config.WorkflowExecutionRetentionTtl = 24h0m0s`;
no custom search attributes registered by default; 34 predefined SAs including `ExecutionStatus`,
`StartTime`, `CloseTime`, `WorkflowType`, `TaskQueue`, `HistoryLength`, `HistorySizeBytes`.
(The `OrkWorkflowName` SA created during probing was removed again; the probe bucket was deleted.)

External corroboration (see `research/web/temporal-visibility-findings.md`): standard visibility was
removed in server v1.24 so everything is "advanced"; ORDER BY is unsupported in the SQL visibility
store by source
(`common/persistence/visibility/store/sql/visibility_store.go` "ORDER BY is not support in SQL
visibility store") and disabled by default on Elasticsearch
(`system.visibilityDisableOrderByClause` default true); per-namespace custom SA type quotas on SQL
are 10 Keyword and 3 each of Bool/Datetime/Double/Int/KeywordList/Text; visibility writes go through
an async internal task queue with no SLA ("usually fast, but it can be several seconds or longer");
memo is not queryable; retention minimum 24 h and expiry deletes both history and the visibility
record.

### 5. The dev stack's Temporal is itself ephemeral

`deploy/docker-compose.yml` runs `temporalio/temporal:latest` with
`server start-dev --ip 0.0.0.0 --ui-port 8233 --log-level warn`, **no `--db-filename`, no volume**
(only `minio-data` is a named volume). The Temporal CLI reference states `--db-filename`:
"By default, Workflow Executions are lost when the server process dies"
(https://docs.temporal.io/cli/command-reference/server), and the setup docs say `start-dev`
"uses an in-memory SQLite database" (https://docs.temporal.io/cli/setup-cli). So in the shipped dev
stack, `docker compose restart temporal` destroys all run history. Any design that makes Temporal
the system of record must first change `deploy/`, which AGENTS.md §8 marks as coordinated.

### 6. MinIO / S3 as metadata store: measured

Probes against `orkstack-minio-1` (localhost, path-style, aws-sdk-go-v2 v1.107.1):

| Probe | Result |
|---|---|
| `PUT If-None-Match: *` on absent key | success |
| `PUT If-None-Match: *` on existing key | **`PreconditionFailed` (412)** |
| `PUT If-Match: <current ETag>` | success |
| `PUT If-Match: <stale ETag>` | **`PreconditionFailed` (412)** |
| seed 1000 × 4 KB objects, 16-way | 831 ms |
| `ListObjectsV2` whole 1000-key prefix (1 request) | best 53.8 ms |
| same, paged 100/request | best 114.5 ms |
| GET 1000 objects serial (status filter needs bodies) | **3.094 s** |
| GET 1000 objects, 32-way concurrent | best 303 ms, avg 344 ms |
| GET one 81 KB index object holding 1000 rows | best 2.8 ms, avg 4.5 ms |

Optimistic concurrency on MinIO therefore works, and the *index-object* pattern is fast to read.
But the per-run-object pattern is N+1 by construction, because S3 LIST exposes only
prefix/delimiter/start-after — no server-side filtering on status or time. The localhost numbers are
a floor: AWS's published small-object latency (100–200 ms) makes 1000 serial GETs ≈ 150 s.

Two repo-specific blockers on reusing `pkg/artifacts` for metadata:

- The `Store` interface (`pkg/artifacts/artifacts.go:38-62`) is documented as content-addressed:
  "each physical object key ends in the lowercase sha256 of its bytes." S3 keys are
  `objects/{sha256(name)}/{sha256(content)}` (`pkg/artifacts/s3_manifest.go:19-27`). There is no way
  to overwrite `metadata/runs/{id}.json` in place — every mutation creates a new key.
- `S3Store.List` (`pkg/artifacts/s3.go:184-224`) lists **all** objects under `objects/`, then per
  object does a manifest GET (`readNameManifest`) plus a HEAD (`statKey`) — 2 extra serial round
  trips each — before filtering by logical-name prefix in Go. Listing 1000 metadata objects through
  this API means ~2000 sequential round trips (≥ 6 s by the measured serial GET rate).

Appending to `runs/{id}.jsonl` is also not possible: standard S3 has no append (only S3 Express One
Zone directory buckets do; community MinIO does not — see `research/web/s3-postgres-findings.md`).
Each event means GET-whole → concat → PUT-whole, i.e. O(N²) bytes for an N-event run.

### 7. SQLite: measured on the target query shapes

Benchmark (throwaway module in `/tmp`, `modernc.org/sqlite` v1.56.0, darwin/amd64,
`journal_mode=WAL, synchronous=NORMAL, busy_timeout=5000`, `SetMaxOpenConns(1)`), schema
`runs(id PK, workflow_name, workflow_version, status, created_at, finished_at, snapshot BLOB)` with
a 4 KB snapshot per row and indexes on `created_at`, `(status, created_at)`, `(workflow_name,
created_at)`. Best-of-20 after warmup:

| Query | 1 000 rows | 10 000 rows | 100 000 rows |
|---|---|---|---|
| newest 50, no filter | 301 µs | 396 µs | 403 µs |
| newest 1000, no filter (rows materialized) | 8.7 ms | 9.1 ms | 8.8 ms |
| status filter, newest 50 | 404 µs | 414 µs | 414 µs |
| status + date range, newest 50 | 65 µs | 64 µs | 404 µs |
| name + status + date range, newest 1000 | 189 µs | 4.7 ms | 46.2 ms |
| count by status | 77 µs | 234 µs | 1.8 ms |
| point get by id | 65 µs | 64 µs | 63 µs |
| 200 single-row UPDATE commits | — | — | 164 µs/commit |

DB size at 100 000 rows: 474 MB (dominated by the 4 KB snapshot blob). The worst case measured,
46 ms, is 20× inside the sub-second budget at 100× the stated scale.

### 8. Proto/API gaps to close regardless of backend

- `proto/orkstack/v1/service.proto:66-74` — `ListRunsRequest` = `{workflow_name, status, page_size,
  page_token}`. **No date-range filter**, which the constraint explicitly requires.
- `internal/api/service.go:264-278` — `ListRuns` ignores `page_token` and never sets
  `next_page_token`; it truncates with `runs[:size]` after materializing everything. Same in
  `ListWorkflows` (`internal/api/service.go:143-153`).
- `ListWorkflowsRequest` has no name filter and `WorkflowStore.List` returns only the latest version
  per name — there is no way to enumerate versions of one orkflow.
- `Workflow.version` is either the DSL-pinned value or a content hash of the semantically
  normalized YAML (`internal/dsl/hash.go:14-35`), which gives a natural immutable primary key
  `(name, version)`.
- `Workflow.source_yaml` is stored inline in the proto, so an orkflow row carries its full DSL.

### 9. Repo conventions the implementation must satisfy

- AGENTS.md §1.3: workflow code stays deterministic; writes only from activities — satisfied by
  writing through `EventActivities.PublishEvent`.
- AGENTS.md §5: behavior-named tests; integration tests behind `-tags=integration`, `t.Skip` when
  infra is absent (pattern at `internal/engine/integration_test.go:1-60`), run with `-p 1`.
- AGENTS.md §6: proof under `proof/<milestone>/` with `test.log`, coverage, a behavior trace, and a
  reproduce command — see `proof/M6/README.md` and `proof/M6/e2e.sh` for the established shape.
- AGENTS.md §6 also warns that a single shared `orkstack-tasks` queue means stray workers steal
  activities; a restart-durability test must kill and restart only the engine.
- `pkg/artifacts/registry.go:76-96` is the precedent for a pluggable, env-selected backend
  (`ORK_ARTIFACT_BACKEND`) — the same shape fits `ORK_STORE_BACKEND=memory|sqlite`.

## Code References

- `internal/engine/store.go:11-96` — in-memory `RunStore`, `Apply`/`Save`/`Get`/`List`
- `internal/engine/store.go:71-75` — `ListFilter` (name + status only)
- `internal/engine/store.go:98-162` — in-memory `WorkflowStore`
- `internal/engine/activities.go:29-42` — the single write seam (`PublishEvent`)
- `internal/engine/workflow.go:439-471` — `emit`, dedupe key, snapshot-as-activity-input, swallowed publish errors
- `internal/engine/bus.go:35-72` — in-memory event log and sequence assignment
- `internal/api/service.go:60-63,168-183` — in-memory idempotency map and run-id counter
- `internal/api/service.go:264-278` — `ListRuns` (no pagination, no date filter)
- `internal/api/ws.go:22-26` — WS bridge 404s when the run is not in `RunStore`
- `internal/dsl/hash.go:14-35` — content-hash versioning
- `pkg/artifacts/artifacts.go:38-62` — content-addressed `Store` contract
- `pkg/artifacts/s3.go:184-224` — `List` does N+1 (manifest GET + HEAD per object)
- `pkg/artifacts/s3_manifest.go:19-27` — `objects/{nameDigest}/{contentDigest}` key scheme
- `pkg/artifacts/registry.go:76-96` — backend registry precedent
- `deploy/docker-compose.yml` — `server start-dev`, no `--db-filename`, no temporal volume
- `cmd/orkctl/run.go:112-148` — `run list --workflow --status --limit` (no date flag)
- `proto/orkstack/v1/service.proto:66-74` — `ListRunsRequest`

## Architecture Documentation

The engine is already CQRS-shaped: Temporal is the execution source of truth, and `RunStore` is a
read model projected by one idempotent activity. The storage decision is therefore about
*materializing the read model durably*, not about moving orchestration state. That is why the
change is small: swap the `RunStore`/`WorkflowStore` implementations behind an interface, keep
`PublishEvent` as the only writer, and keep Temporal authoritative for in-flight execution.

## Historical Context (from research/)

- `research/docs/2026-08-14-…-m2-…-core-engine-dsl.md` — M2 engine/DSL implementation notes
- `research/docs/2026-08-14-…-m3-…-orker.md` — M3 Orker notes
- `research/docs/2026-08-14-…-m4-…-cli.md`, `research/docs/M4-implementation-notes-c7b5d9fa.md` — CLI notes
- `research/web/temporal-visibility-findings.md` — Temporal visibility limits with source links
- `research/web/sqlite-findings.md` — Go SQLite drivers, WAL, Litestream, migrations, alternatives
- `research/web/s3-postgres-findings.md` — S3/MinIO metadata limits and PostgreSQL comparison

## Open Questions

1. On `PublishEvent` write failure, should the run fail, or should the projection stay best-effort
   (current behavior at `internal/engine/workflow.go:462-468`)?
2. Should the event log be persisted (needed for `StreamEvents`/WS resume after restart), or only
   run snapshots? Sequence assignment currently derives from the in-memory log length.
3. Does the UI need the full `Run.snapshot` in list responses, or a slim projection? At 100k runs
   the blob dominates DB size (474 MB measured).
4. Should the engine reconcile with Temporal on startup (`ListWorkflowExecutions` for `Running`
   executions) to recover runs that finished while it was down?
5. Retention/GC policy for the runs table — nothing in the repo defines one.
