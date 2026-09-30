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

### Artifact provenance

Add `RunService.GetArtifactProvenance(GetArtifactProvenanceRequest)` returning
`GetArtifactProvenanceResponse` — a unary read of the object index the run
archive keeps for cleanup (`docs/STORAGE.md`, "Artifact provenance"):

- Request: `string run_id = 1`, `string namespace = 2` (empty = `default`),
  `string task_id = 3`, `ArtifactKind kind = 4`, `string name = 5`,
  `string expect_sha256 = 6` (optional; a mismatch with the recorded digest is
  FailedPrecondition naming both).
- Response: `ArtifactRef artifact = 1` (as recorded on the run),
  `bool bytes_present = 2`, `ReferenceState own_reference = 3`
  (HELD, RELEASED, ABSENT), `google.protobuf.Timestamp released_at = 4`,
  `repeated ArtifactReference references = 5`,
  `int64 references_unreadable = 6`, `google.protobuf.Timestamp computed_at = 7`.
- `ArtifactReference`: `run_id`, `orkflow_name`, `orkflow_version`,
  `task_id`, `kind`, `run_status`, `run_created_at`.

The artifact is addressed exactly as a download is and resolved against the
authoritative, namespace-checked run; a URI or key is never accepted. Order is
the response's contract: `run_created_at` descending, ties by run id
descending, then `Run.task_runs` order and kind within one run. Three absences
are never conflated: bytes gone (`bytes_present=false`), reference released
by cleanup (RELEASED with `released_at`), and no evidence either way
(ABSENT). A store or listing failure is Unavailable, never `false` or an empty
list; more than 1000 markers is ResourceExhausted; an engine with no archive
or no artifact store answers FailedPrecondition. There is no stream and no
cache: a client asks when a person opens the disclosure.

CLI: `run artifact provenance RUN --task TASK --kind KIND --name NAME
[--expect-sha256 HEX] [--json]` prints the artifact, a BYTES line, a
REFERENCE line in one of three fixed wordings, and a REFERENCED BY table;
`--json` is the raw response. Missing bytes exit 0 — they are a finding, not
an error. UI: a **Provenance** toggle beside each artifact row's Download,
opening a panel with the same BYTES and REFERENCE wordings, one row per
reference linking to that run in the same namespace with the addressed run
marked `this run`, the unreadable footer, and the CLI equivalent.

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
timed out and is the committing attempt for an adopted commit. (`DiffRuns`
relies on the same shape and reads the committing attempt's record, then the
newest successful adopter's — see "Run diff" below.)

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

### Run diff

*"This run failed and last night's succeeded — what is different?"* Add
`RunService.DiffRuns(DiffRunsRequest)` returning `DiffRunsResponse` — one
unary read over what `GetRun`, `GetOrkflow` and `ListTaskAttempts` already
serve, computed on every call and stored nowhere:

- Request: `string from_run_id = 1`, `string to_run_id = 2`,
  `string namespace = 3` (empty = `default`; BOTH runs resolve in it),
  `repeated string only = 4` (empty = every field).
- `RunFieldDiff`: `task_id = 1` (empty for a run-level row), `field = 2`,
  `from_value = 3`, `to_value = 4`, `from_available = 5`, `to_available = 6`,
  `from_reason = 7`, `to_reason = 8`.
- Response: `Run from = 1`, `Run to = 2` (shallow: `task_runs`, `input` and
  `output` cleared), `same_orkflow = 3`, `same_definition_digest = 4`,
  `input_unified_diff = 5`, `repeated RunFieldDiff differences = 6`,
  `tasks_compared = 7`, `tasks_identical = 8`, `evidence_unavailable = 9`,
  `repeated string fields_compared = 10`.

Both ids resolve through the lookup every run read uses: another namespace's
run is NotFound, never compared. Equal ids, an empty id, or an unknown field
name are InvalidArgument (the last lists the valid fields). Two runs of
different orkflows are compared and flagged (`same_orkflow=false`), never
refused. A store error fails the call (Unavailable); it never degrades to
"unavailable" evidence.

**Vocabulary and order.** The field names, in canonical order, are
`definition`, `input` (run-level) and `presence`, `status`, `attempt`,
`pool`, `pin`, `orker`, `exit_code`, `error`, `duration`, `image`,
`platform_digest` (task-level). `differences` is ordered: the run-level
`definition` row, then the from-run's tasks in its `task_runs` order, then
tasks only the to-run has in its order; within a task, fields in canonical
order. Both clients render that order as given. There is no `input` row: the
canonical-JSON unified diff (keys sorted, integers without fraction, labelled
`<run id>/input.json`) is the row, printed verbatim.

**`only` and `presence`.** `only` narrows the fields and the reads — naming
neither `image` nor `platform_digest` never opens the orkflow or the attempt
store. `presence` is compared under every `only`: a task one run lacks
always yields exactly one `presence` row (`present`/`absent`) and no other,
so narrowing can never make an added or removed task count identical.
`fields_compared` is the resolved list (every field, or `only` plus
`presence`) and every row's `field` is one of its entries.

**Rendering — decided once, server-side.** Every value is a rendered string
a client prints verbatim. An available empty value is "does not apply" and
prints `-`; an unavailable side prints `unavailable` and its reason. The two
never look alike.

| `field` | Rendered value | `-` (available, empty) when | `unavailable` when |
| --- | --- | --- | --- |
| `presence` | `present` / `absent` | never | never |
| `status` | `SUCCEEDED`, `SKIPPED`, … | never | never |
| `attempt` | decimal, `0` included | never | never |
| `pool` | verbatim | never reached an Orker | never |
| `pin` | verbatim | not pinned | never |
| `orker` | `TaskRun.orker_id` | no Orker reported | never |
| `exit_code` | decimal | no Orker reported | never |
| `error` | `{code} ({category})`, or `{code}`; message and details are never compared | no error | never |
| `duration` | `finished − started` in Go's spelling (`1m3s`, `850ms`) | not terminal; SKIPPED; never reached an Orker | `timestamps_missing`, `timestamps_inverted` |
| `image` | the pinned definition's `Task.image.pinned`, else the outcome attempt record's | never | the definition is unavailable (`definition_unavailable`, `definition_digest_mismatch`) and no record answers; `task_not_in_definition` |
| `platform_digest` | the outcome attempt record's `image.platform_digest` | never reached an Orker; the container never ran | `no_attempt_store`, `no_outcome_attempt`, `attempt_record_missing` |

**The outcome attempt record.** For `image` (fallback) and
`platform_digest`, the record whose attempt is `TaskRun.attempt` answers —
which for an adopted commit is the committing attempt. When that record is
gone (a crash after `commit.json`, or cleanup), the newest record with
`commit_adopted` and outcome SUCCEEDED answers instead: an adopting retry
files its record under its own attempt number, carrying the committing
attempt's executed image from `commit.json`. A failed or canceled record is
never used. A definition whose `definition_digest` differs from the run's
pinned digest cannot answer (`definition_digest_mismatch`).

**Reason tokens.** A client renders the sentence; an unknown token (a later
build's) renders as itself, and the row is never dropped.

| token | sentence |
| --- | --- |
| `definition_unavailable` | the pinned definition is no longer stored |
| `definition_digest_mismatch` | the stored definition does not hash to the digest this run pinned |
| `task_not_in_definition` | the pinned definition has no task of this id |
| `no_attempt_store` | this engine has no task attempt store configured |
| `no_outcome_attempt` | the snapshot names no outcome attempt |
| `attempt_record_missing` | no record of the outcome attempt or of a retry that adopted its commit: reclaimed, or the Orker never filed one |
| `timestamps_missing` | the snapshot lacks a start or finish time |
| `timestamps_inverted` | the finish time precedes the start time |

CLI: `run diff FROM TO [--only FIELD,...] [--json]` prints a header (`Diff`,
`Namespace`, `Compared` when narrowed, `Orkflow`, `Status`, `Finished`,
`Definition`, `Input`), the input diff verbatim, a TASK/FIELD/FROM/TO table
(or `No task differences.`), the summary sentence `{differ} of {compared}
tasks differ; {identical} identical[; {n} field(s) unavailable].` and one
footnote per unavailable side, `{task} {field} in {run}: {reason}
({sentence})`. Digests are never abbreviated. `--json` is the raw
`DiffRunsResponse`; exit status is 0 whenever the RPC succeeded. UI: a
**Compare** action on the run page pre-selects the Orkflow's newest other
SUCCEEDED run and opens `/runs/{from}/compare?to={to}`, a deep-linkable page
with a swap link, the same table, summary and footnotes, and a
`CliEquivalent` for the command above.

### Task timing

Two facts an operator needs first — *was this task slow, or was it
waiting?* — are read off one snapshot and the attempt records, and no new
RPC, stream or `EventType` is added. `TaskRun.scheduled_at` (18) is the
instant the engine scheduled the task, the same `workflow.Now` the
`TASK_SCHEDULED` event carries; `TaskRun.started_at` (8) is when the Orker
began the attempt that decided the task (`ExecuteTaskResult.started_at`, or
`ActivityFailureDetail.started_at` for a failure), unset while the task runs
and for a task no Orker reported on. Each `TaskAttempt` lists
`phases` (17): every §7.1 phase the attempt passed through, contiguous, the
last ending at `finished_at`, the last one's name equal to `phase`.

Both clients derive, with one rule (`cmd/orkctl/timing.go`,
`ui/src/lib/timing.ts`): WAIT = `started_at − scheduled_at`, RUN =
`finished_at − started_at`, TOTAL = `finished_at − scheduled_at`, and for a
task decided on a retry the queue wait `attempts[1].started_at −
scheduled_at` on its pool. A quantity that does not apply (never scheduled,
not finished) is `-`/absent; a quantity that happened but cannot be known is
`unavailable`, never `0s`, with exactly one of seven reasons: `no orker
reported a start`, `clock skew`, `recorded before scheduled time existed`,
`attempt records reclaimed`, `no attempt record`, `record predates phase
spans`, `no task attempt store`. A snapshot with `started_at` and no
`scheduled_at` was written before the field existed: its TOTAL is dispatch to
finish and is labelled so.

CLI: `run timing RUN [--task TASK] [--json]` prints the table, the run's
summed wait and every scheduled task's attempts with their phases;
`run get RUN --timing` appends the same output, and `run get` gains a
`SCHEDULED` column. `--json` is a CLI-owned derived document, not a proto
envelope, so `run get --timing --json` is refused and points at
`run timing --json`. UI: a RUNNING task's card reads `Since scheduled …`; a
finished one draws a two-segment wait/run bar (an unavailable segment is
hatched and names its reason) with the `run timing --task` command beside
it; the attempt timeline lists each attempt's phases and, for a task decided
on a retry, its queue wait.

### Live attempt introspection

"What is this running task doing right now" is one unary read,
`RunService.InspectRun` (`proto/orkstack/v1/service.proto`). It reads the
heartbeat detail the Orker already sends and the cluster already holds for
every pending `ExecuteTask` attempt (`DescribeWorkflowExecution`), maps each
pending activity to its task through its scheduling event, and answers one
`LiveAttempt` per in-flight task: its state (`SCHEDULED`, `STARTED`,
`CANCEL_REQUESTED`), attempt number, scheduled/started instants, the task's
own timeout and grace, Temporal's backstop, and — only when a heartbeat from
THIS attempt exists — its phase, container id, bytes logged, Orker id, last
beat and the instant Temporal times the attempt out without another
(`heartbeat_deadline_at`). Nothing is stored, projected or streamed; a
terminal run answers `run_status` and no attempts without touching the
cluster, and a cluster error is `Unavailable`, never an empty list.

Unavailable is a word, never a zero: `heartbeat_available=false` carries one
of three reasons — `NOT_STARTED` (no Orker has the attempt),
`NO_HEARTBEAT_YET` (an Orker has it and no beat from this attempt has
arrived, including when Temporal still holds the previous attempt's detail,
which is never shown as the new attempt's), `UNDECODABLE` (a beat that is
not the Orker's shape). A last beat older than `task.HeartbeatStaleAfter`
(20 s, two ceiling ticks) is `heartbeat_stale`, computed server-side against
the answer's `computed_at`, which every age both clients print is measured
against.

CLI: `run inspect RUN [--task TASK] [--watch] [--interval D] [--json]`
prints one row per task (`STATE`, `PHASE`, `ORKER`, `CONTAINER`, `LOGGED`,
`RUNNING`, `HEARTBEAT`) and footers naming the reason, the stale sentence
with its deadline, the task timeout as a lower bound, the pool an attempt
waits on, the next retry, or a requested cancellation. `--watch` polls
(default 5 s, at least 1 s) until the run is terminal and exits with the
run's code; `--json` is the raw `InspectRunResponse` (one compact object per
line under `--watch`). UI: a RUNNING task's card polls the same read every
5 s while the page is visible and shows `Running for …`, a live line
(`Waiting for an orker · …`, `Phase run · 412.0 MiB logged · heartbeat 3s
ago`, `… — STALE`, `Attempt 3 scheduled for …`, `Cancelling · phase …`,
`Phase unavailable · <reason>`), the Orker the heartbeat names, and the
`run inspect --task` command beside it.

### Task pins

A **pin** is a named group of tasks of one orkflow that execute on one Orker
and share `/orkstack/shared`. Two fields on `TaskRun` (`proto/orkstack/v1/run.proto`)
carry that to both clients, and no new RPC, stream or `EventType` is added:

- `string pin = 16` — the pin this task belongs to, spelled as the author
  wrote it in `pin:`, and empty for a task that is not pinned.
- `string orker_id = 17` — the Orker that produced this task's outcome. It is
  populated for **every** task that reached an Orker, pinned or not, and is
  empty only when the task never got to one (skipped, canceled before
  dispatch) or its host died before reporting. An empty `orker_id` therefore
  means "never reached an Orker", never "not pinned". A member lost with its
  pin (`PIN_HOST_LOST`, `PIN_TIMEOUT`) reports nothing, so the engine backfills
  its `orker_id` from another member of the same pin — a pin never moves, so a
  sibling's value is that task's by construction. It stays empty only when no
  member of that pin ever reported one, and the failure's `error.details` then
  carries the Temporal session's raw OS host name as an explicitly labelled
  best-effort lead, which is not an `orker_id`.

Read together the two are the client-visible proof that a pin held: the
members of one pin show one `pin` and one `orker_id`. Neither field names a
queue, a session or a session id — `pin` is the user-facing word and Temporal's
Worker Session underneath it stays internal, exactly as **queue** stays
internal to **pool**.

Parity expectation: both clients show the same two fields for the same task.
`orkctl run get RUN` renders `POOL`, `PIN` and `ORKER` in the task table
between the status and the attempt, each rendering an absent value as `-`, and
`--json` carries them as protojson names them (`pin`, `orkerId`, omitted when
empty) off the unchanged `GetRunResponse` envelope. The UI run page states
`Pin <name>` and `Orker <id>` beside `Pool <pool>` on the task card, and tints
each pin's member cards with one accent colour derived from the pin name, so
"these ran together" is legible without reading the labels.

`orkctl run watch` needs no new column: it streams events, and the
`TASK_SCHEDULED` message the engine emits already reads
`task scheduled on pool <pool>, pin <name>` for a pinned task and
`task scheduled on pool <pool>` for an unpinned one.

`TaskAttempt` already carried `orker_id` and `pool` (see "Task attempts"); a
`pin` field there is additive and not part of this contract.

### Webhook administration

Add a proto `WebhookService` with `RegisterWebhook`, `ListWebhooks`, and
`DeleteWebhook`. Subscriptions are deployment-wide unless they name a scope
(see "Scoped subscriptions and task outcome events" below); the listing stays
deployment-wide, each row states its scope, and the UI says so explicitly
rather than implying the namespace picker filters it.

- `Webhook`: `string id = 1`, `string url = 2`,
  `repeated EventType events = 3`, `google.protobuf.Timestamp created_at = 4`,
  `string namespace = 5`, `string orkflow_name = 6`, `string task_id = 7`.
- Register request: `string url = 1`, `repeated EventType events = 2`,
  `string secret = 3` (optional user-supplied signing secret),
  `string namespace = 4`, `string orkflow_name = 5`, `string task_id = 6`.
- Register response: `Webhook webhook = 1`, `string secret = 2`.
- List request empty; response `repeated Webhook webhooks = 1`.
- Delete request `string id = 1`; response empty.

Accept the four run transitions (RUN_STARTED, RUN_SUCCEEDED, RUN_FAILED,
RUN_CANCELED) and the four task outcomes (TASK_SCHEDULED, TASK_STARTED,
TASK_SUCCEEDED, TASK_FAILED); TASK_LOG is refused. Empty selects every run
transition and no task event: task events are opt-in by name. Preserve HTTP/HTTPS URL validation and one-time
secret disclosure. List never contains secrets. Store subscriptions in shared
object storage, with atomic create/delete and durable failure reporting;
both management and delivery matching must observe another replica's changes.
Delivery matching reads the registry through `Store.ListForDelivery`, the one
read that carries each subscription's signing secret; `List` and `Get` — what
`ListWebhooks`, the JSON adapter, `orkctl` and the UI answer from — never do.
Until scoped subscriptions landed, the durable registry production wires
matched through the public `List`, so every production delivery went out with
no `X-Ork-Signature` header. Every delivery from the durable registry now
carries one, over the same body bytes, with the secret its registration
disclosed; no record is rewritten and no subscriber re-registers. A matched
subscription with no secret is still delivered, and logged at `Warn` as
`webhook delivery unsigned`.
Do not use a startup-only snapshot or a process-only subscription registry in
production. The existing JSON routes may remain compatibility adapters over
the same service/storage; clients use generated types. Keep existing delivery
semantics and describe them accurately: durable registration does not make
in-flight delivery durable or exactly once; what each delivery did is recorded
once it has settled (`delivery-records`), and the record is a log, never a
queue. This work adds no scheduler or
delivery-state database. Storage failure must not produce successful registration
or deletion, and notification lookup failure must not fail a run.

#### Scoped subscriptions and task outcome events

A subscription may name `namespace`, `orkflow_name` and `task_id`: three
exact-match strings, each empty meaning every, each validated as a slug
(`^[a-z][a-z0-9-]*$`, at most 200 bytes) and never sanitized. A run that names
no namespace is in `default`, so a subscription scoped to `default` matches
it. `task_id` is consulted only for task events — a `RUN_*` event passes it —
and registration refuses a `task_id` when no `TASK_*` event is named. A scope
names coordinates, not objects: nothing checks that the namespace, orkflow or
task exists, so a subscription may precede the orkflow it names. The record
stays flat at `metadata/webhooks/{id}.json`; a record written before scope
existed reads as deployment-wide, and a record whose scope is invalid is
refused on read rather than widened.

A task delivery carries the flat header plus `taskId` and `attempt`, and the
`X-Ork-Task-Id` header; `attempt` is omitted when Temporal has not numbered
one (`TASK_SCHEDULED`, `TASK_STARTED`, a task that timed out). A run
delivery's body and headers are byte-identical to what they were before. A
task that was skipped, or that was running or pending when its run ended, is
never delivered as a task outcome — the interpreter records a canceled running
task as `TASK_FAILED` carrying `CANCELED`, and the projection refuses it by
status — so a `TASK_FAILED` delivery always reads `"status":"FAILED"`, and the
run's terminal delivery is where a cancellation is read. Task events are
higher-cardinality hints on the same best-effort channel (four requests at
most, no ordering across deliveries, possibly twice across replicas); a
subscriber that must not miss one reconciles with `GetRun`. Every durable
lookup lists the whole registry, as before, now up to four more times per
task; there is no cache.

| Surface | Register | Read the scope | Refusal |
| --- | --- | --- | --- |
| CLI | `orkctl webhook register URL -n NS --orkflow NAME --task TASK --event E`; only an explicit `-n` narrows (`ORK_NAMESPACE` and the saved default do not apply) | `SCOPE` row/column: `ns/orkflow/task`, `*` per unset component, `ALL` when unscoped; `--json` carries `namespace`/`orkflowName`/`taskId` when set | Local, before any request, with the server's `ValidateScope` message |
| UI | Admin → Webhooks: three optional free-text inputs (a slug `pattern` catches typos), eight event checkboxes, a caveat under the task events | `Scope` column (`webhook-scope-{id}`), `Deployment-wide` when unscoped | Server `InvalidArgument` in the register `ErrorPanel` |

### Control-plane audit trail

`AuditService.ListAuditRecords` (`proto/orkstack/v1/audit.proto`) reads back
one immutable record per control-plane write RPC — succeeded or refused —
from `metadata/audit/` in the shared store, so any replica answers for a write
any replica recorded. Reads, `SubmitRun`, and trigger invocations are never
recorded. `orkctl admin audit [--since] [--until] [--action PREFIX]
[--principal NAME] [--limit N] [--page-token TOKEN] [-n NS] [--json]` and
Admin → Audit render the same records in the same order for the same filters.

- The request's `namespace` is a FILTER, and empty means **every** namespace —
  the one request in the API where it does not mean `default`. The CLI sends
  one only when `-n` is typed on that command line (never from
  `ORK_NAMESPACE` or `namespace use`); the page shows every namespace unless
  its scope control says otherwise. Both print the scope they used.
- The window defaults to the seven days before the engine's `now`; the
  response echoes the resolved `since`/`until`, which both clients print.
- The page token is versioned and binds namespace, action prefix, principal,
  and the window as sent; a token presented with different filters is
  `InvalidArgument` ("filters changed; drop the page token"), as for
  `ListRuns`. The CLI's next-page hint repeats the filters; the page keeps
  filters, cursor and page number in its URL and the path back per tab.
- `principal` is what a gateway asserted in `Ork-Principal`, never
  authenticated; an empty one renders `unattributed`, and both clients print
  the "asserted, not authenticated" caption under every rendering, empty
  page included. `ORK_PRINCIPAL` makes `orkctl` assert one.
- A scan past the engine's budget is `ResourceExhausted` and is shown as the
  failure it is — beside the window inputs in the UI, a non-zero exit in the
  CLI — never as an empty page.
- A `webhook.register` record is deployment-wide (empty namespace) even when
  the subscription is scoped; its detail is `url=… events=[…]`, followed by
  `scope=ns/orkflow/task` (`*` per unset component, as the `SCOPE` column
  renders it) when any scope component is set. The signing secret is never
  part of it.

### Run again

No new durable operation is needed. UI `Run again` loads the selected run into
a submission draft. CLI `run again RUN` reads that run and submits through
`SubmitRun` with `source_run_id` set to it, so the new run's origin is
`run-again <source>` — a claim the server checks (the source must exist in the
namespace, NotFound otherwise); `--origin-ref` and the dialog's **Origin
reference** attach a correlation reference, and the dialog's copied command is
`orkctl run again <source>` with the same flags; support reviewed overrides for inputs and version plus a
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
| Run filters/history | Status, Orkflow, page size, creation interval, refresh, next/previous; URL preserves filters, cursor and page number; the path back is per-tab; distinguish empty deployment from no matches | List query/cursor above | Older page is reachable; no duplicate rows at equal timestamps; mixed live/archive results and cross-namespace rejection covered |
| Retained logs | Full retained-log download bypasses scrollback cap; older retained chunks can be browsed; search explicitly says loaded-log scope | Existing `run logs` task/attempt/from cursor and shell redirection | Download includes beyond pane cap; truncation notices remain visible; attempt selection and namespace retained; failed download never looks complete |
| Dashboard window | 15m, 6h, 24h and custom since; the active window, custom included, is marked in the control and shown as a formatted instant; display selected time and preserve coverage caveats | Existing `stats --since` | Request time changes and rendered counts/coverage follow response; reload preserves selection |
| Webhooks | List/register/delete, run and task event selection, optional namespace/orkflow/task scope with a per-row scope cell, one-time secret copy | Proto-backed webhook commands with `-n`/`--orkflow`/`--task` and a `SCOPE` column | Second replica and restarted engine see registration/deletion and scope; list excludes secret; storage failure errors; a scoped task delivery and an invalid-scope refusal captured in the browser |
| Webhook deliveries | Admin → Webhooks **Deliveries** panel: caption with the resolved window and "every namespace" or the namespace, webhook/run/event/outcome/since/until/namespace filters, table with a delivered/failed pill, attempts and last status, an expanded row with the signature, the body's `sha256` and byte count ("body not stored") and the attempt timeline, pagination, `CliEquivalent`, a per-webhook `Deliveries` link, ErrorPanel with Retry; Redeliver behind `ConfirmDialog` stating one POST, now, signed, today's run, with the result inline | `webhook deliveries [--webhook] [--run] [--event] [--delivered] [--since] [--until] [--limit] [--page-token] [-n]`, `webhook delivery ID`, `webhook redeliver ID` (exit 1 when not delivered); `--json` is the raw response | Every settled delivery is one create-only record whose `body_sha256` and `signature` equal what the receiver read; a refused delivery lists four attempts that agree with the receiver's log; a record written on one replica is listed and redelivered by another; a redelivery is exactly one signed POST carrying `X-Ork-Redelivery-Of` and is recorded as a new delivery; a scan past its budget refuses; nothing reads a record to decide what to send |
| Validate/apply | Validate current YAML without saving; field issues and warnings; retain warnings after apply | Shared validator and CLI validate | Invalid DAG and namespace mismatch agree; valid validation writes nothing; unavailable image errors actionable |
| Run again | Reviewed submission seeded from run, showing source and pinned version | CLI again plus dry-run | Inputs preserved, override respected, new ID, no implicit old-key reuse, unavailable version refuses |
| Run origin | Run header line `run-origin`: kind chip, id linked to the source run or the Triggers tab, description, reference linked only when it is an http(s) URL, `asserted by …` principal with its not-authenticated title, `unrecorded` for a run recorded before origins existed; submit dialog **Origin reference** (`--origin-ref` in its copied command, `orkctl run again <source>` when opened from Run again); runs list **Started by** + **Origin id** filters and an **Origin** column, both in the URL (`origin`, `originId`) | `run submit --origin-ref`, `run again --origin-ref` (sends `source_run_id`), `run list --origin KIND[:ID]`, `ORIGIN` column (`-` when unrecorded), `run get` `Origin:`/`Reference:`/`Principal:` lines; trigger invoke `Ork-Origin-Reference` header | The kind is never taken from a client: `SUBMIT`/`RUN_AGAIN` from `SubmitRun`, `TRIGGER` from the trigger door; an over-long or control-character reference is `InvalidArgument` and mints nothing; a run-again of an unknown or other-namespace run is `NotFound`; live and archived runs of one origin listed together; a pre-origin run is `unrecorded` and matched by no origin filter |
| Schedules | Orkflow page **Schedules** tab (between Triggers and Edit): scope statement; create form (name, version `Current`/pinned, cron one per line, interval, time zone, overlap `skip`/`buffer-one`/`buffer-all`/`cancel-other`/`allow-all` defaulting to `skip`, Advanced jitter/catch-up/pause-on-failure/start/end/description/JSON input, create paused) with the server's refusal verbatim in `schedule-error`; one row per schedule with state pill (`active`, `paused` with the note in its title, `unavailable` with its reason), next fire, last run linked to the run (or `refused` with the reason in its title); Pause (reason dialog), Resume, Fire now behind a `ConfirmDialog` that sends no overlap override, Delete behind a `ConfirmDialog`, and `CliEquivalent` popovers for the recreating `schedule create` and the pause/resume/fire one-liners; a row expands into the next ten fire times, the recent fires (run link, `refused` with its reason, `preparing`, `unavailable`) and the counters line; run page origin `schedule` links back to the tab | `schedule create\|list\|get\|update\|pause\|resume\|fire\|backfill\|delete`, `--json` on each; `--overlap` slugs; on `fire`/`backfill` an absent `--overlap` is no override; `update` is read-modify-write and last writer wins | Every field is read from the cluster on each call and nothing is stored under `metadata/`; a fire starts an ordinary run through the submit preparation with origin `{SCHEDULE, sch-…, nominal time, name}`; a refused fire records the submit refusal verbatim; `run list --origin schedule:<id>` is the schedule's audit trail. **Backfill is CLI-only in v1**: a parity exception accepted with the `orkflow-schedules` contract, because a backfill enqueues many fires at once and is an operator recovery tool rather than a day-to-day action; the UI shows backfilled runs like any other fire |
| Typed inputs | String/number/boolean controls, required/default hints; object/array JSON editors; JSON alternative | Same submitted JSON values as `run submit --input` | Switching modes preserves values; false/zero/empty string not dropped; invalid numeric/JSON data cannot silently coerce |
| Editor protection | Warn on navigation with unsaved YAML; import local YAML; export current draft/source; conflict handling preserved | Existing file-based apply/edit/get | Import and export round-trip; dirty navigation guarded; successful save clears dirty state; validation alone does not clear it |
| Deep links/CLI copies | Preserve namespace, task, attempt, inspector tab; copy commands beside corresponding actions | Shell-escaped actual CLI syntax | Reload restores selection; copied commands include required namespace/version; quotes in values cannot become shell syntax |
| Orker inventory and cordon | Admin → Pools, per-pool **Show Orkers** disclosure: state pill (`ready`, `cordoned`, `unreachable` with its reason and no numbers), tasks, pins, last seen, open pins linking to their runs; **Cordon** through a confirm dialog that requires a reason and shows a server error in the dialog; one-click **Uncordon**; a shell-escaped CLI equivalent per row | `orkctl orker list [--pool]`, `get`, `cordon --reason`, `uncordon`, and `drain [--reason] [--timeout] [--watch]` (a client-side wait: exit 0 drained, 4 timed out naming what remains, 1 unreachable or uncordoned); `--namespace` is ignored — Orkers are deployment infrastructure | Same rows and states from `ActivityService.ListOrkers` on any replica; an unanswered Orker is unreachable, never idle; a blank reason is refused before any request; NotFound for an id nothing polls; every cordon is an `orker.cordon`/`orker.uncordon` audit record |
| Task attempts | Per-task retry badge and attempt timeline; failed attempts show their own error; a row links to that attempt's logs | `run attempts RUN [--task]` and `run get --attempts`, same order and `--json` envelope | Retried fixture shows every attempt with its own outcome and error; an attempt with no record renders as UNREPORTED, not as a failure or a gap; another namespace's run is NotFound; a replica that ran nothing answers identically |
| Task timing | Running card reads "Since scheduled"; finished card draws a wait/run bar with unavailable segments hatched and named; attempt timeline lists each attempt's phases and a retried task's queue wait | `run timing RUN [--task] [--json]`, `run get --timing`, `SCHEDULED` column on `run get` | A task submitted with no Orker polling waits at least the stimulus and its wait segment is wider than its run; every attempt lists its phases; a retried task shows both attempts and its queue wait; a reclaimed, pre-contract, skewed or unreported value renders `unavailable` with its reason, never `0s`; a replica that ran nothing prints the identical document |
| Live attempt introspection | A RUNNING card shows `Running for`, a live line with the phase, bytes logged and heartbeat age (or waiting / retry / cancelling / unavailable with its reason), STALE in amber with the deadline, and the Orker the heartbeat names; the poll pauses while the page is hidden | `run inspect RUN [--task] [--watch] [--interval] [--json]` | The three states and the three reasons render as words, never zeros; a stale heartbeat carries the instant Temporal times the attempt out; a previous attempt's carried-over detail is never shown as the current attempt's; a replica that started nothing answers identically; a terminal run answers with no attempts |
| Failure fingerprints | Dashboard **Recent failures** → *Fingerprints* disclosure (absent when nothing failed): one chip per dimension (`orkflow`…`orker`, four defaults ticked), rows in enum column order with runs/first/last, `unavailable` (muted italic, `data-unavailable`) for reclaimed image/phase and `-` for a value the run never had, the CLI's footer sentences, each row a link to `/runs` filtered by namespace, orkflow (when grouped), `status=failed` and the answered window; fetched on open and on each dashboard refresh; ErrorPanel with Retry on a failed read; CLI equivalent | `stats failures [--since] [--until] [--orkflow] [--group-by] [--category] [--limit]`, `--json` is the raw `GetFailureBreakdownResponse` | `sum(run_count)` equals `stats`' failed count for the window; three real failure modes group apart with their codes and categories; a reclaimed attempt record renders `unavailable` and is counted, never blank; a scan past its budget is refused, never truncated; a replica that started nothing answers identically |
| Run diff | Run page **Compare** action with the newest other SUCCEEDED run of the Orkflow pre-selected (any run id may be pasted); deep-linkable `/runs/{id}/compare?to={id}` page with swap, definition/orkflow chips, the verbatim input diff, a TASK/FIELD/FROM/TO table tinting only differing cells, the CLI's summary and footnotes, and `CliEquivalent` | `run diff FROM TO [--only FIELD,...]`, `--json` is the raw `DiffRunsResponse` envelope | Same rows, same strings, same order in both; another namespace's run is NotFound; a reclaimed attempt record renders `unavailable` with its reason and never `-`; a task only one run has keeps its `presence` row under any `--only`; a replica that started neither run answers identically |
| Artifact provenance | Per-row **Provenance** disclosure under Inspect: bytes line, reference line, referencing runs linked in the page's namespace with `this run` marked, unreadable footer, CLI equivalent; fetched on open, re-fetched on every open | `run artifact provenance RUN --task --kind --name [--expect-sha256] [--json]`, same wordings and order | Bytes present and missing both render; HELD, RELEASED and ABSENT each render their own wording; another namespace's runs are neither listed nor counted; a replica that started nothing answers identically; a second S3 run with identical bytes lists only itself |
| Namespace admission | Admin → Namespaces Hold and Max running columns; Hold… / Release hold row actions behind `ConfirmDialog` with `CliEquivalent`; hold banner on Orkflows, dashboard and the submit dialog with submit disabled; a cap refusal shown verbatim; Admin → Audit shows the `namespace.update` record's detail with the reason and cap | `namespace update --hold REASON`, `--release-hold`, `--max-running N`; `namespace get`/`list` render both; `run submit`, `run again` and a trigger invoke refused `failed_precondition` with the reason and the fix; `admin audit --json` carries `hold="…"`/`max_running=N` in the record's `detail` | Refusal leaves no run, no id and no claim; an accepted run finishes after a hold is set; a hold set on one replica refuses on the other; the count includes runs the other replica started; `since` survives a reason edit and is never client-supplied; the audit record of a hold names the reason, and of a release has no `hold=` |
| Tenant-scoped secret policy | Admin → Namespaces Allowed secrets column and one-per-line field on create and edit, with the `unrestricted` caption and the Orker-floor note; the Apply page's and the editor's Validate and Apply show the `tasks[i].secrets[j].source` issue; the run page shows `SECRET_NOT_PERMITTED` on a task the Orker refused | `namespace create/update --allow-secret PATTERN…`, `update --clear-allowed-secrets`; `namespace get`/`list` render the list or `unrestricted`; `orkflow validate` prints the field-anchored issue; `apply` and `run submit` refused `invalid_argument` naming the namespace, source, task, secret and list; the Orker refuses a source outside `ORK_ORKER_SECRET_ALLOW` with `SECRET_NOT_PERMITTED` before pulling an image | Validate and apply agree; a refused apply writes nothing; a refused submit mints no id and takes no claim; a list tightened on one replica refuses a submit on the other; a permitted secret's value reaches `/orkstack/input/secrets/<name>` and appears in no log, attempt record, event or archived object; a refused source is never retried; `..` is refused at every gate; an empty list on both halves reproduces today's behaviour byte for byte |
| Audit trail | Admin → Audit: scope statement (every namespace by default, one namespace on request), action/principal/window filters, table with `unattributed` for an absent principal and `refused {code}` for a refusal, pagination, caption, CLI equivalent; ErrorPanel with Retry on a failed read | `admin audit [--since] [--until] [--action] [--principal] [--limit] [--page-token] [-n]`, `--json` is the raw envelope; `ORK_PRINCIPAL` asserts a principal | A write on one replica is listed by another; a refusal is recorded with its code; reads and submits are not recorded; the header a gateway sets on browser requests is recorded verbatim; a scan past its budget refuses instead of answering empty |
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
