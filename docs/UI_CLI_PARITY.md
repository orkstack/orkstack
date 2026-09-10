# UI and CLI operator experience contract

This document specifies the operator improvements accepted on 2026-09-08.
It is an implementation and acceptance checklist, not a claim that its gates
have passed. `AGENTS.md` remains authoritative. Proto is the source of truth
for shared types; implement additions there and run `make proto` first.

Parity means that both clients can accomplish the same deployment operation
with the same selectors, defaults, results, and errors. Their presentation may
differ. Every run operation carries its namespace; an empty namespace means
`default`, and a mismatched namespace returns NotFound. Copies and downloads
must retain the namespace, pinned version, task, and attempt where applicable.

## Shared API changes

### Validation without applying

Add `OrkflowService.ValidateOrkflow(ValidateOrkflowRequest)` returning
`ValidateOrkflowResponse`:

- Request: `string yaml = 1`, `string namespace = 2`.
- Response: `Orkflow orkflow = 1`, `repeated ValidationIssue issues = 2`.
- `ValidationIssue`: `string field = 1`, `string message = 2`,
  `ValidationSeverity severity = 3`; enum values UNSPECIFIED, ERROR, WARNING.

The same preparation function serves validation and apply: compile, resolve
namespace, check allowed pools, resolve images, derive the effective version,
and calculate the definition digest. Validation never writes a definition,
pointer, execution, or subscription and never claims an apply concurrency
check has succeeded. Validation errors are issues with an actionable field
path when available; errors without a known field use an empty field, never
a guessed location. Dependency outages remain RPC errors. Apply continues to
check edit tokens and immutable-version conflicts at its own write boundary.
Warnings remain visible after validation and successful apply. The CLI is
`orkctl orkflow validate FILE`, including `--json`; invalid documents exit
nonzero after rendering the issues.

### Run browsing

Extend `ListRunsRequest` with `google.protobuf.Timestamp created_after = 6`
(inclusive) and `google.protobuf.Timestamp created_before = 7` (exclusive).
Validate timestamps and reject an inverted or empty interval. Add CLI
`run list --since RFC3339 --until RFC3339 --page-token TOKEN` and retain
`--status`, `--orkflow`, and `--limit` as page size. JSON returns the existing
response envelope including `next_page_token`; human output shows a usable
next-page command when one exists.

Use the existing `page_token` and `next_page_token` fields. A versioned opaque
token binds namespace, Orkflow, status, both time bounds, and the last returned
`created_at`/ID pair. Invalid tokens and changed filters return InvalidArgument;
clients clear the token when filters change. Page-size changes may retain the
cursor. Sort newest creation time first, then ID descending at equal times;
never parse an ID. Preserve the existing legacy missing-time fallback in
`engine.RunIndexTime`. Merge live and archive records by ID with the archive
preferred for terminal records. Apply bounds/cursor before each source's
limit and fetch enough to decide whether another matching row exists.

The cursor is a position, not a frozen snapshot: newly visible records before
it appear on refresh, and run statuses may change between reads. Do not label
a scan cut short by a budget as complete; return an actionable failure if a
continuation cannot be determined. Do not read process memory behind a wired
reader. Exercise both local and S3 archive paths and real Temporal visibility.

### Artifact retrieval

Add `RunService.ListRunArtifacts(ListRunArtifactsRequest)` returning
`ListRunArtifactsResponse` and
`RunService.ReadArtifact(ReadArtifactRequest)` returning a stream of
`ArtifactChunk`:

- List request: `string run_id = 1`, `string namespace = 2`,
  `string task_id = 3` (empty selects every task).
- `RunArtifact`: `string task_id = 1`, `ArtifactKind kind = 2`,
  `ArtifactRef artifact = 3`, `int32 attempt = 4`.
- List response: `repeated RunArtifact artifacts = 1`.
- Kind enum: UNSPECIFIED, OUTPUT, VALUES, STDOUT, STDERR.
- Read request: `string run_id = 1`, `string namespace = 2`,
  `string task_id = 3`, `ArtifactKind kind = 4`, `string name = 5`.
  `name` is the exact returned artifact name; require a matching reference.
- Chunk: `bytes data = 1` (at most 64 KiB), `ArtifactRef artifact = 2`
  (first chunk), `bool last = 3` (successful complete read).

Resolve every download against an authoritative, namespace-checked Run and
its task references. Include output artifacts, `values_ref`, and stdout/stderr
system artifacts. Never accept an arbitrary URI, object key, or local path.
Only expose attempts for which a reference is actually recorded; older logs
remain accessible through the existing attempt-aware log stream. Stream
bytes directly from `pkg/artifacts.Store.Get`, outside Temporal and events.
Missing/retired bytes are explicit errors. Surface read failures and verify
size/checksum before calling a download complete. A browser download must use
safe attachment naming; the CLI must not turn artifact names into paths.

CLI: `run artifacts RUN [--task TASK] [--json]` lists references and
`run artifact download RUN --task TASK --kind KIND --name NAME --output PATH`
downloads one. Write a temporary sibling and rename on success; refuse an
existing output unless an explicit overwrite flag is supplied. UI presents
the same task/kind/name/size/checksum metadata and download actions.

### Task attempts

Temporal owns retries, so a run snapshot carries only a task's final outcome:
`TaskRun.attempt` and `TaskRun.error` describe the last attempt, and every
earlier attempt's error is discarded when the next one is scheduled. The Orker
writes one immutable `TaskAttempt` record per attempt to the shared store
(`metadata/attempts/{run-id}/{task-id}/{attempt}.json`, `docs/STORAGE.md`), and
one RPC reads them back.

Add `RunService.ListTaskAttempts(ListTaskAttemptsRequest)` returning
`ListTaskAttemptsResponse`:

- Request: `string run_id = 1`, `string namespace = 2` (empty = `default`),
  `string task_id = 3` (empty selects every task of the run).
- Response: `repeated TaskAttempt attempts = 1`.
- `TaskAttempt` (`proto/orkstack/v1/run.proto`): `int32 schema_version = 1`,
  `string run_id = 2`, `string task_id = 3`, `int32 attempt = 4` (1-based),
  `AttemptOutcome outcome = 5`, `Error error = 6` (set only for FAILED),
  `int32 exit_code = 7`, `google.protobuf.Timestamp started_at = 8`,
  `finished_at = 9`, `string orker_id = 10`, `string pool = 11`,
  `string phase = 12`, `ImageRef image = 13`,
  `repeated ArtifactRef system_artifacts = 14`, `bool commit_adopted = 15`,
  `string build_id = 16`.
- `AttemptOutcome`: UNSPECIFIED, SUCCEEDED, FAILED, CANCELED, UNREPORTED.

There is no new stream. Clients refetch on triggers they already follow — a
task terminal event, the run's terminal event, and a higher attempt observed
on the log stream. The WebSocket bridge is unchanged.

Ordering is the response's contract, not a client's sort: tasks in the order
`Run.task_runs` lists them, then ascending by attempt within a task. Both
clients render that order as given.

Gaps are reported, never hidden. The highest attempt number is the maximum of
`TaskRun.attempt`, the highest stored record, and the log store's latest
attempt for that task; every number from 1 to that maximum with no record is
returned as `ATTEMPT_OUTCOME_UNREPORTED` carrying only `run_id`, `task_id` and
`attempt`, so no attempt is silently renumbered when an Orker dies before
filing its record. Clients must present an UNREPORTED row as missing evidence
("no record from the orker — it crashed or lost its heartbeat"), never as a
failure, and must not invent timing, an error, or a retry schedule for it.
`TaskRun.attempt` alone is not the attempt count: it is 0 for a task Temporal
timed out and is the committing attempt for an adopted commit.

A task that has not been attempted returns an empty list, which is not an
error and is distinct from a task whose attempts were reclaimed. Records live
on the same lifecycle as logs, so a run past cleanup returns empty. Scoping
matches every other run read: another namespace's run is NotFound, an unknown
`task_id` is NotFound, a missing `run_id` is InvalidArgument, and an engine
with no attempt store configured answers FailedPrecondition rather than a
timeline of UNREPORTED rows. `GetRun` and `ListRuns` are unchanged.

CLI: `run attempts RUN [--task TASK] [--json]` renders the timeline, and
`run get RUN --attempts` appends the same table; `--json` is the raw
`ListTaskAttemptsResponse` envelope. Two responses cannot share one JSON
document, so `run get --attempts --json` is refused and points at
`run attempts --json`. UI presents a per-task retry badge and an
attempt timeline with each failed attempt's error and a link that pins the log
pane to that attempt, retaining namespace, task and attempt.

### Webhook administration

Add a proto `WebhookService` with `RegisterWebhook`, `ListWebhooks`, and
`DeleteWebhook`. Subscriptions remain deployment-wide, matching the existing
CLI contract; the UI labels that scope explicitly rather than implying the
namespace picker filters them.

- `Webhook`: `string id = 1`, `string url = 2`,
  `repeated EventType events = 3`, `google.protobuf.Timestamp created_at = 4`.
- Register request: `string url = 1`, `repeated EventType events = 2`,
  `string secret = 3` (optional user-supplied signing secret).
- Register response: `Webhook webhook = 1`, `string secret = 2`.
- List request empty; response `repeated Webhook webhooks = 1`.
- Delete request `string id = 1`; response empty.

Accept only the existing RUN_STARTED, RUN_SUCCEEDED, RUN_FAILED, RUN_CANCELED
events; empty selects all. Preserve HTTP/HTTPS URL validation and one-time
secret disclosure. List never contains secrets. Store subscriptions in shared
object storage, with atomic create/delete and durable failure reporting;
both management and delivery matching must observe another replica's changes.
Do not use a startup-only snapshot or a process-only subscription registry in
production. The existing JSON routes may remain compatibility adapters over
the same service/storage; clients use generated types. Keep existing delivery
semantics and describe them accurately: durable registration does not make
in-flight delivery durable or exactly once. This work adds no scheduler or
delivery-state database. Storage failure must not produce successful registration
or deletion, and notification lookup failure must not fail a run.

### Run again

No new durable operation is needed. UI `Run again` loads the selected run into
a submission draft. CLI `run again RUN` reads that run and submits through
`SubmitRun`; support reviewed overrides for inputs and version plus a
`--dry-run` that prints the exact new submission. Default to the source's
pinned version and inputs. Never reuse its original idempotency key; allow a
fresh explicitly supplied key. A new run gets a new ID. Explain before submit
that this starts a new execution and may repeat external side effects. If the
pinned definition is unavailable, fail with its coordinates; never silently
switch to current. Original inputs may contain secrets and never belong in a
shareable or bookmarkable URL; a run is shared by its ID alone, and the
seeded draft exists only in the page that already holds that run.

## Capability and acceptance matrix

| Capability | UI behavior | CLI/shared behavior | Required observable acceptance |
| --- | --- | --- | --- |
| Cancellation | Active-run action with optional reason; disable duplicate requests and show cancellation requested until authoritative status changes | Existing `run cancel --reason` | Running fixture receives cancellation; reason survives; terminal state comes from server; errors remain actionable |
| Version-safe submission | Version picker marks current; links from a definition and a run retain pinned version; advanced idempotency key and empty-pool override are explicit | Existing submit version/key flags plus new `--allow-empty-pool` exposing the existing API field | Viewing an old version submits that version even after current moves; repeated same-key submit deduplicates according to existing server contract |
| Inputs/results | Collapsible run inputs/outputs, task values, metadata; copy and JSON export | Existing `run get --json`, artifact commands above | Successful fixture values and artifact metadata match CLI; empty values are distinct from unavailable values |
| Run filters/history | Status, Orkflow, page size, creation interval, refresh, next/previous; URL preserves state; distinguish empty deployment from no matches | List query/cursor above | Older page is reachable; no duplicate rows at equal timestamps; mixed live/archive results and cross-namespace rejection covered |
| Retained logs | Full retained-log download bypasses scrollback cap; older retained chunks can be browsed; search explicitly says loaded-log scope | Existing `run logs` task/attempt/from cursor and shell redirection | Download includes beyond pane cap; truncation notices remain visible; attempt selection and namespace retained; failed download never looks complete |
| Dashboard window | 15m, 6h, 24h and custom since; display selected time and preserve coverage caveats | Existing `stats --since` | Request time changes and rendered counts/coverage follow response; reload preserves selection |
| Webhooks | Deployment-wide list/register/delete, event selection, one-time secret copy | Proto-backed existing webhook commands | Second replica and restarted engine see registration/deletion; list excludes secret; storage failure errors; browser registration flow captured |
| Validate/apply | Validate current YAML without saving; field issues and warnings; retain warnings after apply | Shared validator and CLI validate | Invalid DAG and namespace mismatch agree; valid validation writes nothing; unavailable image errors actionable |
| Run again | Reviewed submission seeded from run, showing source and pinned version | CLI again plus dry-run | Inputs preserved, override respected, new ID, no implicit old-key reuse, unavailable version refuses |
| Typed inputs | String/number/boolean controls, required/default hints; object/array JSON editors; JSON alternative | Same submitted JSON values as `run submit --input` | Switching modes preserves values; false/zero/empty string not dropped; invalid numeric/JSON data cannot silently coerce |
| Editor protection | Warn on navigation with unsaved YAML; import local YAML; export current draft/source; conflict handling preserved | Existing file-based apply/edit/get | Import and export round-trip; dirty navigation guarded; successful save clears dirty state; validation alone does not clear it |
| Deep links/CLI copies | Preserve namespace, task, attempt, inspector tab; copy commands beside corresponding actions | Shell-escaped actual CLI syntax | Reload restores selection; copied commands include required namespace/version; quotes in values cannot become shell syntax |
| Task attempts | Per-task retry badge and attempt timeline; failed attempts show their own error; a row links to that attempt's logs | `run attempts RUN [--task]` and `run get --attempts`, same order and `--json` envelope | Retried fixture shows every attempt with its own outcome and error; an attempt with no record renders as UNREPORTED, not as a failure or a gap; another namespace's run is NotFound; a replica that ran nothing answers identically |
| Timing | Run elapsed time, task start/end and durations, attempt information where recorded | Existing timestamps in `run get --json`; readable durations may be added | Duration uses authoritative start/end; running elapsed updates; missing timestamps render unknown; no invented retry schedule |

Typed controls are presentation of existing input declarations, not a new
schema language. Unknown or complex declarations retain the JSON editor and
unknown user-supplied keys are not silently discarded. Browser UI must not
invent server validation requirements that the CLI bypasses.

## Local activity build/test parity decision

`orkctl activity build` and `orkctl activity test` are local developer
operations on the caller's filesystem and OCI runtime. They have no current
deployment API and therefore no browser execution equivalent. Surface them
in the activity UI as local tooling with accurate copyable commands and
prerequisites. On 2026-09-08 the user explicitly accepted these two commands
as a local-tool exception to UI/CLI parity. Copying a command is assistance,
not browser execution. Browser-triggered builds and contract tests are
outside this implementation's accepted scope.
Do not mount a Docker socket into the UI/engine, accept arbitrary server host
paths, or invent a privileged activity runner to manufacture parity. A future
execution equivalent requires a separately accepted contract for build-context
transfer, isolation, registry credentials, artifact custody, and cancellation.

## Evidence and completion

Behavior tests precede implementation. Unit tests cover selectors, cursor
validation, stream failure, value preservation, and no-write validation.
Real Temporal/MinIO integration covers merged history, artifact downloads,
replica/restart webhook persistence, cancellation and version pinning.
Playwright specs exercise operator flows against the real stack and retain
screenshots/transcripts in the proof directory. Run `make lint test`, required
integration tests, `make proof`, and `make proof-ui`; report each gate and the
fresh proof directory. A skip or absent prerequisite is not a pass. Commit
scoped Conventional Commits with the evidence; never commit `proof/`.
