# Task pinning: one Orker for a group of tasks, via Temporal Worker Sessions

Status: **design, for review**. Nothing here is implemented. Every claim about
the Temporal SDK below was read from `go.temporal.io/sdk@v1.47.0` source in the
module cache (file:line cited); every claim about OrkStack was read from the
tree at the commit this document was written against. Where a claim could not
be verified from source it is marked **assumed** and repeated in §12.

## 0. The one-paragraph version

A **pin** is a named group of tasks in one Orkflow that execute on one Orker
process and share one read-write directory, `/orkstack/shared`, for the life
of the group. The author declares pins at the top level (`pins:`) and puts a
task in one with `pin: <name>`, the way `pool:` puts a task on a set of Orkers.
Underneath, `RunWorkflow` opens one Temporal Worker Session per (run, pin) and
schedules the pin's tasks as session activities; an Orker that sets
`ORK_ORKER_MAX_PINS` above zero becomes a pin host. A pin never survives its
host: if the Orker dies, every task of the pin fails with
`InfrastructureError/PIN_HOST_LOST` and the run fails — it is never silently
re-opened on another Orker, because the shared directory would not be there.
Everything else — pool routing, the per-attempt sealed workspace, the
`values.json`/artifact commit protocol, retries, cancellation — is unchanged,
and a document that declares no pin compiles, dispatches and replays
byte-for-byte as it does today.

## 1. Vocabulary (additions to AGENTS.md §7)

- **Pin** means a named group of tasks of one Orkflow that execute on one
  Orker process and share `/orkstack/shared`. It is the user-facing word: it
  appears in the DSL, the proto, `orkctl`, the UI, and every error message.
- **Session** means Temporal's Worker Session (`workflow.CreateSession` and
  its kin) — the mechanism a pin is implemented with. It is internal, exactly
  as **queue** is internal to **pool** (§7.1): no user-facing surface names a
  session, a session id, or a session task queue.
- **Pin host** means an Orker configured to accept pins
  (`ORK_ORKER_MAX_PINS > 0`). Every Orker still runs ordinary tasks; only a
  pin host runs pinned ones.

The pool/queue discipline is the model: one decision (which Orkers) has one
user word, and the Temporal noun underneath never leaks. `pin` is the second
user-facing word in that family, and it answers a narrower question than
`pool`: not *which set of Orkers* but *which one member of that set, for these
tasks, together*.

## 2. What exists today, and what a pin must not disturb

### 2.1 Pool routing is a set, resolved at submit, replayed from the plan

`pool:` names a set of Orkers. `internal/task/pool.go` is the single
translation from a pool to a Temporal task queue (`QueueForPool`,
`RouteForPool`, `PoolForQueue`, the `ork-pool-` prefix). At submit,
`PoolResolver.Plan` (`internal/engine/pools.go:397`) resolves every task's pool
to a route, probes the pool for a live Orker, refuses a deserted pool
(`PoolEmptyError`, overridable with `allow_empty_pool`), and binds the whole
route table into `RunPlan.Routes`. Inside the workflow, `routeForTask`
(`internal/engine/workflow.go:1071`) is a table lookup on that plan and nothing
else. A pin does not change any of that: a pinned task's pool is resolved,
checked and reported exactly as an unpinned one's, and the queue that pool maps
to is the queue the session is created *on* (§6.3).

### 2.2 The scheduling loop is one deterministic selector

`RunWorkflow` (`workflow.go:100`) scans `readyTasks()`, classifies each
(condition, upstream skip), calls `schedule()` which builds `ActivityOptions`
(pool queue, `StartToCloseTimeout` from the task budget plus the plan's transfer
budget, `HeartbeatTimeout = task.HeartbeatTimeout`, `WaitForCancellation`,
`retryPolicyFor`) and `ExecuteActivity(task.ActivityExecuteTask, input)`, then
blocks on a `workflow.Selector` over the in-flight futures registered in
declaration order. Failure is fail-fast only: `fail()` cancels `runCtx`, drains
in-flight futures through `cancelInFlight`, marks every still-`PENDING` task
`CANCELED`, and returns. There is no continue-on-error. A pin has to live
inside this loop without adding a second scheduler.

### 2.3 The workspace is per attempt, sealed, and destroyed on every exit

`NewWorkspace` (`internal/orker/workspace.go:105`) builds one 0700 root per
attempt under `ORK_ORKER_WORKDIR` (`makeAttemptDir`,
`oci_dispatcher.go:1702`, pattern `ork-<run>-<task>-<attempt>-*`), with
`input/` sealed to 0555/0444 before the container starts (`Seal`), `output/`
and `tmp/` at 1777+sticky, and a separate `secrets/` bind. `execute()`
(`oci_dispatcher.go:388`) defers `ws.Remove()` and `ws.RemoveSecrets()` so the
tree is gone on success, failure, panic and cancellation alike. Nothing
persists between two tasks of one run on one Orker today, and
`docs/OCI_ACTIVITY_CONTRACT.md` §6.4 says so as an invariant ("per-attempt
execution directories (removed on every exit path)"). A pin adds a *second*
directory with a *different* lifetime beside the per-attempt one; it changes
nothing about the per-attempt one.

### 2.4 Data flows by reference, never by shared disk

A downstream task learns about an upstream one only through `UpstreamRef`
(commit URI, `values.json` ref, artifact refs), which the Orker materializes
into the sealed `input/upstream/` and `input/artifacts/` trees
(`stageInputs`, `oci_dispatcher.go:762`). That contract stays the only
cross-task contract for unpinned tasks and stays *available* to pinned ones;
`/orkstack/shared` is an additive convenience (§8.6), not a replacement.

## 3. Temporal Worker Sessions as they exist in SDK v1.47.0

Read from `workflow/session.go` and `internal/session.go`,
`internal/internal_worker.go`, `internal/workflow.go`,
`internal/internal_workflow_testsuite.go`. The API surface is exactly:

| API | Behaviour (v1.47.0) |
|---|---|
| `workflow.CreateSession(ctx, *SessionOptions) (Context, error)` | Generates a session id with `SideEffect` (`internal/session.go:generateSessionID`), executes the internal activity `internalSessionCreationActivity` on **`<base queue>__internal_session_creation`**, where the base queue is `ActivityOptions.TaskQueue` of `ctx` (`CreateSession`, `internal/session.go`: `getActivityOptions(ctx).TaskQueueName`), then blocks on a `Selector` over a signal channel named by the session id and the creation future. The host answers by **signalling the workflow** (`SignalCreationResponse`) with its resource-specific task queue, `<resourceID>@<hostname>`. Returns a child context (`WithCancel`) carrying `SessionInfo{SessionID, HostName, SessionState}`. |
| `workflow.ExecuteActivity(sessionCtx, …)` | If the context carries an open session, the SDK **overrides** `ActivityOptions.TaskQueue` with the session's resource-specific queue for that call (`internal/workflow.go:1052-1064`); an explicit `TaskQueue` in the options is therefore harmless and ignored. If the session has failed, returns `ErrSessionFailed` immediately without scheduling. |
| `workflow.CompleteSession(ctx)` | Cancels the session context (which cancels the creation activity **and every user activity scheduled on it**), then runs `internalSessionCompletionActivity` on the host's queue with 3 s/3 s timeouts, and marks the session `Closed`. **Must be called from the same coroutine as `CreateSession`** (doc comment, `workflow/session.go`). |
| `workflow.GetSessionInfo(ctx)` | Returns the most recent `SessionInfo`; `SessionState` is `Open`, `Failed` or `Closed`. |
| `workflow.RecreateSession(ctx, token, opts)` | Re-creates a session **on the same resource-specific task queue** (`recreateSessionParams{Taskqueue}`). It does *not* give back the same worker process if that process died — the queue is `<uuid>@<host>` and the uuid is minted fresh per worker start (`internal_worker.go:526-527`) — and it preserves no host state. Not used by this design. |
| `SessionOptions` | `ExecutionTimeout` (**required**; it becomes the creation activity's `StartToCloseTimeout`, i.e. the session's whole lifetime), `CreationTimeout` (**required**; the creation activity's `ScheduleToStartTimeout`), `HeartbeatTimeout` (default 20 s; the creation activity heartbeats every `min(HeartbeatTimeout/3, 10s)`). |
| `worker.Options.EnableSessionWorker` | Starts two extra pollers beside the ordinary activity worker (`newSessionWorker`, `internal_worker.go:521`): a **creation worker** on `<queue>__internal_session_creation` with a fixed slot supplier of `MaxConcurrentSessionExecutionSize` (default 1000), and a **session activity worker** on `<uuid>@<hostname>`. |
| `worker.Options.MaxConcurrentSessionExecutionSize` | The per-process cap on open sessions; a creation request past the cap fails with a retryable "too many outstanding sessions" and the SDK retries creation (backoff 1 s × 1.1, max 10 s) until `CreationTimeout`. |
| Failure detection | Only the creation activity's heartbeat decides session health. If the host dies, the creation activity heartbeat-times-out, the SDK's goroutine sets `SessionStateFailed` and cancels the session context, so every user activity on it is cancelled (`createSession`, `internal/session.go`). "Session will not be marked as failed if an activity within it returns an error." |
| Concurrency inside a session | Nothing serializes activities on one session; the session activity worker has its own slot supplier (`params.Tuner.GetSessionActivitySlotSupplier()`). Two activities scheduled on one session context run concurrently on the same process. |
| Test support | `TestWorkflowEnvironment.SetWorkerOptions(worker.Options{EnableSessionWorker: true})` registers the internal creation/completion activities and a test session environment (`internal_workflow_testsuite.go:547, 2378`). `WorkflowTestSuite`-based tests of pinned runs are therefore possible without a cluster. |

**Replay safety.** Every construct `CreateSession` and `CompleteSession` use is
a `workflow.*` primitive whose outcome is in history: a `SideEffect` marker
(the uuid), two `ActivityTaskScheduled` commands (creation, completion), a
`WorkflowExecutionSignaled` event (the host's answer), a `Selector`, and a
`workflow.Go` coroutine. The SDK's own doc comment on `CreateSession` is what
we rely on for "created on the taskqueue user specified in ActivityOptions".
Nothing in the API reads a clock, the environment or the network from workflow
code. The one determinism obligation it adds to *us* is structural: the
coroutine that creates a session must be the one that completes it (§6.4).

**Versioning.** `worker.DeploymentOptions.UseVersioning`'s doc comment says it
"cannot be enabled at the same time as `EnableSessionWorker`"
(`internal/worker.go:80`), but the only enforced panic covers the deprecated
`UseBuildIDForVersioning` (`internal_worker.go:2312-2316`), and
`newSessionWorker` deliberately **strips** versioning from the session
activity worker while **keeping** it on the creation worker
(`internal_worker.go:534-547`). Under AGENTS.md §7.2 (`RunWorkflow` Pinned;
"a pinned workflow uses its own activity-worker version only while that
version has workers polling its activity channel; otherwise Temporal falls
through") this *should* mean: the creation activity of a pinned run routes to
same-version pin hosts, and the per-session activities route to the
unversioned `<uuid>@<host>` queue that only that host polls.

**Measured, and true** (§12.1 is closed):
`internal/engine/versioning_pin_integration_test.go`
(`TestIntegration_AnOpenPinStaysOnItsHostAcrossAVersionRollout`) runs two pin
hosts of one deployment on a real cluster and rolls the Current Version forward
while a pin is open. The creation activity is dispatched to a pin host of the
run's own version with versioning in effect; the members — including the one
scheduled after the switch — plus `ReleasePin` and the SDK's session completion
activity all land on the `<uuid>@<host>` queue of the process that opened the
pin, dispatched to a worker reporting `UseVersioning=false`. The run's unpinned
tasks keep the ordinary version-pinned routing, and a pinned run submitted
after the switch opens its pin on the new version. `worker.New` also accepts
the pair at construction (no panic; the enforced one covers only the deprecated
`UseBuildIDForVersioning`) — `cmd/orker`'s
`TestWorkerOptions_APinHostOnAVersionedDeploymentIsAcceptedByTheSDK`. The
combination remains unsupported *upstream*, so those two tests, not the doc
comment, are the contract.

## 4. YAML DSL surface

### 4.1 Recommended syntax

```yaml
name: build-and-test
version: "1.0.0"

pins:
  build:                       # a pin is declared once, by name
    timeout_seconds: 3600      # optional: the pin's whole lifetime; see 4.4

tasks:
  - id: clone
    uses: git
    pin: build                 # a task joins a pin by name, like `pool:`
    run_as_orker_user: true
    with: { op: clone, url: "{{ .inputs.repo }}", dest: /orkstack/shared/src }

  - id: compile
    uses: golang
    pin: build
    needs: [clone]             # `needs:` is unchanged and still required for order
    run_as_orker_user: true
    command: [sh, -c, "cd /orkstack/shared/src && go build -o /orkstack/shared/bin/app ./..."]

  - id: test
    uses: golang
    pin: build
    needs: [compile]
    run_as_orker_user: true
    command: [sh, -c, "cd /orkstack/shared/src && go test ./... > /orkstack/output/artifacts/report.txt"]

  - id: publish                # unpinned: runs wherever the pool sends it and
    uses: file                 # sees `test` only through its committed artifacts
    needs: [test]
    with: { op: checksum, src: '{{ artifact "test/report.txt" }}' }
```

- `pins:` is a top-level map from pin name to a (possibly empty) options
  object. `pins: { build: {} }` is legal.
- `pin:` on a task is a bare name and must name a declared pin.
- A pin's option today is only `timeout_seconds`. No other option is proposed;
  the pool stays on the task (§4.3).

### 4.2 Why a declared block plus a per-task reference, not an inline attribute

Three DSL idioms exist: a per-task scalar that references something declared
*outside* the document (`pool:` — pools live in the namespace and the
deployment), a per-task list that references other tasks (`needs:`), and a
per-task block of options (`retry:`). Only `inputs:` declares something at the
top level for tasks to reference.

An inline-only `pin_to_session: <name>` (option B in the brief) makes the pin
*implicit in the name*, and that is the property that disqualifies it: a typo
(`pin: buidl` on one task) silently creates a second, one-task pin instead of
an error. The DSL's rule is "a key the schema does not define is refused, never
ignored" and "detect missing references during validation" (AGENTS.md §7); an
implicit group is a reference that cannot be found to be missing. `needs:` is
the precedent — a `needs` entry that names no task is refused at compile (rule
4), and `pin:` gets the same rule (§4.5 rule 9). The declaration is also
*where the pin's own option lives*: a pin has a lifetime of its own (the
session's `ExecutionTimeout`, §6.3), and a lifetime is a property of the group,
not of any one task in it. Putting it on one task would raise "which task's
value wins", which is the shape of question the compiler exists to refuse.

The word is `pin`, not `session`, for the reason in §1.

### 4.3 Is a pin scoped to one pool?

**Yes, necessarily.** A session is created on one task queue and all its
activities run on the one process that answered from that queue (§3). A pin
spanning two pools is not implementable and would not mean anything. So:

- `pool:` stays on the task and stays the only pool selector (AGENTS.md §7.1
  is amended, not contradicted — §11).
- Rule 10 (§4.5): every task of a pin must name the same pool. The compiler
  compares the literal `pool:` values treating an absent pool and `main` as
  one value, because `QueueForPool` already treats them as one route
  (`pool.go:82`). `PoolResolver.Plan` re-checks after namespace-default
  resolution, on the route's pool, on the path a hand-built request cannot
  skip — mirroring how rule 7 is split between compiler and server today.

### 4.4 Conflicting requirements inside a pin

Each task of a pin is still its own container, so most per-task fields remain
independent and *should*: that is the point of a pin (clone with a git image,
build with a toolchain image, test with a third). Field by field:

| field | in a pin | reason |
|---|---|---|
| `uses`/`image`, `command`, `env`, `with`, `secrets`, `resources`, `network`, `timeout_seconds`, `cancellation_grace_period_seconds`, `retry`, `condition` | independent per task | one container per task, as today; the Orker applies each task's own policy |
| `pool` | must agree (rule 10) | §4.3 |
| `run_as_orker_user` | must agree (rule 11) | `/orkstack/shared` is 1777 with the sticky bit, like `output/`. Two tasks running as different uids can each create files the other cannot delete or overwrite — the classic sticky-bit failure. Requiring agreement removes the most common way for a pin to break on its second task. It does not remove the *image-declared* `USER` difference between two images when the flag is false; `ValidateOrkflow` should emit a `WARNING` (the parity contract already defines the severity) for a pin whose tasks leave the flag false, recommending `run_as_orker_user: true` across the pin. |
| `needs` | unchanged | §4.6 |
| `timeout_seconds` on the **pin** | optional; default derived | §6.3 |

### 4.5 Validation rules (extend `docs/DSL.md` "Validation rules", `internal/dsl/validate.go`)

9. Every `pin:` names a key of `pins:`; each pin name is a slug (`checkSlug`,
   ≤ 200 bytes — a pin name becomes a directory component on the Orker,
   §8.2, and a slug is what `safeComponent` need not rewrite).
10. Every task of one pin names the same pool, treating absent and `main` as
    equal.
11. Every task of one pin has the same `run_as_orker_user`.
12. Every declared pin is used by at least one task. A declaration nothing
    references is a typo, and refusing it is cheaper than a pin that opens a
    session for nobody. (It cannot open one — opening is lazy, §6.2 — but the
    document is still wrong.)
13. `pins.<name>.timeout_seconds`, when present, is ≥ 1.

Rules 9-13 are compiler rules (`validate`) and rule 10 is additionally a
`PoolResolver.Plan` rule on the resolved pool. Decoding stays strict:
`pins:` and `pin:` become known fields; nothing else changes.

### 4.6 `needs:` and the DAG inside a pin

A pin imposes **no ordering**. The DAG is the one scheduling model
(AGENTS.md §7: "Explicit `needs` dependencies create DAG edges"), and a second,
implicit ordering — "tasks in a pin run in declaration order" — would be a
hidden rule that changes behaviour by group membership alone. Two tasks in a
pin with no edge between them run concurrently on the same Orker and share
`/orkstack/shared` concurrently; that is the author's contract with their own
filesystem, exactly as it would be for two processes on one machine.
`docs/DSL.md` must say this in one sentence beside the example, because it is
the one place the Kestra analogy (which *does* serialize `workingDir` tasks)
misleads.

Cross-pin and pin-to-unpinned edges are ordinary edges. An unpinned downstream
task sees a pinned upstream only through committed artifacts (§8.6).

### 4.7 What happens to a pin when a task in it fails

Nothing new: the engine is fail-fast. A failed task fails the run
(`fail()`), which cancels every in-flight activity — pinned siblings included,
through the run context the session context is derived from (§6.2) — and marks
every never-started task `CANCELED` with `started_at` unset
(`markCanceled`). The pin is then closed (§6.4). There is no "pin-local"
failure that lets the rest of the run continue, because there is no
continue-on-error anywhere for the pin to be an exception to. Statuses:

| sibling state at failure | status | via |
|---|---|---|
| running on the pin host | `CANCELED` (or `FAILED` if it finished first) | `cancelInFlight` → `collect` |
| pending, needs met or not | `CANCELED` | `markCanceled` |
| already skipped | stays `SKIPPED` | — |

A task that *retries* (its own `retry:`) retries on the same host and sees
the same `/orkstack/shared` (§6.5); that is a retry, not a failure, and the
pin stays open.

## 5. Proto changes (`proto/` first, then `make proto`)

All additions are additive: no existing field number, name or type changes;
`buf breaking` WIRE and WIRE_JSON stay clean.

### 5.1 `proto/orkstack/v1/orkflow.proto`

```proto
// Pin is a named group of tasks that execute on ONE Orker process and share
// /orkstack/shared for the group's lifetime (docs/design/task-pinning-
// worker-session.md). It is the user-facing word; the Temporal session that
// implements it is internal, as a queue is to a pool.
message Pin {
  string name = 1;                  // slug, unique within the orkflow
  // timeout_seconds bounds the whole pin, from the moment its first task is
  // dispatched until its last task ends. 0 means derived: the sum of every
  // member task's wall-clock budget, which is the sequential worst case.
  int32 timeout_seconds = 2;
}

message Task {
  // ... 1-18 unchanged ...

  // pin names the Pin this task belongs to, or is empty. Every task of one
  // pin runs on one Orker process and sees the same /orkstack/shared. It
  // changes what runs and where, so it is part of canonicalOrkflow(Task) —
  // emitted ONLY when set, so a definition with no pin keeps its digest.
  string pin = 19;
}

message Orkflow {
  // ... 1-10 unchanged ...

  // pins declares every Pin a task may name, sorted by name at compile so
  // one document always compiles to one message. Part of canonicalOrkflow,
  // emitted ONLY when non-empty (see Task.pin).
  repeated Pin pins = 11;
}
```

Field 19 is the next free number on `Task` (2 and 8 are reserved and stay
reserved). 11 is next on `Orkflow`.

### 5.2 `proto/orkstack/v1/activity.proto` (the engine↔Orker seam)

```proto
// PinContext tells an Orker that this attempt belongs to a pin, and which
// opening of it. It is set ONLY on a pinned task's input, so an unpinned
// task's recorded payload is byte-identical to what it was before this
// field existed (the golden history fixture has no pins and must keep
// matching without re-recording).
message PinContext {
  string name = 1;
  // instance_id identifies ONE opening of the pin within one run. The
  // engine takes it from the session it opened; the Orker keys the shared
  // directory by it and trusts nothing else to name that directory.
  string instance_id = 2;
  // timeout_seconds is the pin's lifetime bound as the engine set it
  // (Pin.timeout_seconds or the derived sum). The Orker derives the shared
  // directory's lease from it (§8.4).
  int32 timeout_seconds = 3;
}

message ExecuteTaskInput {
  // ... 1-23 unchanged ...
  PinContext pin = 24;
}

// ReleasePinInput asks the pin host to remove the pin's shared directory.
// Scheduled by the engine on the pin's own session, so it lands on the
// host that owns the directory; a host that has no such directory returns
// success (idempotent).
message ReleasePinInput {
  string run_id = 1;
  string namespace = 2;
  PinContext pin = 3;
}

message DescribeOrkerResult {
  // ... 1-8 unchanged ...
  // max_concurrent_pins is ORK_ORKER_MAX_PINS: how many pins this Orker
  // hosts at once. 0 means it hosts none, and a submit that pins a task to
  // this Orker's pool is refused at submit rather than left waiting.
  int32 max_concurrent_pins = 9;
}
```

`ExecuteTaskResult.orker_id = 13` and `ActivityFailureDetail.orker_id = 9`
already exist and are what §9 reads.

### 5.3 `proto/orkstack/v1/run.proto`

```proto
message TaskRun {
  // ... 1-15 unchanged ...

  // pin is the Pin this task belongs to, as the author named it, or empty.
  // Two TaskRuns of one run with the same pin ran on the same Orker.
  string pin = 16;

  // orker_id is the Orker that produced this task's final outcome, read off
  // ExecuteTaskResult.orker_id or ActivityFailureDetail.orker_id. Set for
  // every task that reached an Orker, pinned or not; empty when the task
  // never reached one (skipped, canceled before dispatch) or when the Orker
  // died before reporting (heartbeat timeout).
  string orker_id = 17;
}
```

A session id is **not** placed on `TaskRun`: it is a Temporal noun (§1), and
the client-facing fact — "these tasks ran together, on that Orker" — is
carried by `pin` plus `orker_id`. The instance id is on the seam
(`PinContext.instance_id`) and in Temporal history (the `SideEffect` marker
and the signal name) for operators who need to correlate.

### 5.4 Canonical form (`internal/dsl/canonical.go`)

`Task.pin` and `Orkflow.pins` change what runs, so they must be in the
digest. But `canonicalTask` emits every whitelisted key "whether or not
zero" (§4.4 of the OCI contract), and adding a `"pin": ""` key to every task
would move the digest of **every existing definition** on its next apply. That
is not cosmetic: `sameStoredOrkflow` (`pkg/artifacts/orkflow_store.go:403`)
decides "unchanged document" by `proto.Equal` after clearing only
`created_at`; if the incoming document is stamped with a moved digest before
that comparison, an unchanged, source-hash-versioned document would be a
*changed* document under an *existing* version and be refused with
`already_exists`. Whether the stamp precedes the comparison is not something
this design should have to know.

So the rule is: **`pin` is emitted only when non-empty, and `pins` only when
non-empty**, and the existing golden vector (`canonical_test.go`,
`GoldenDefinitionDigest`) must pass unchanged as the proof that no unpinned
digest moved. A second golden case with a pin is added beside it. This is a
deliberate, documented exception to "emitted whether or not zero", made for
the same reason `definition_digest` was added additively — §12.7 records it
as a decision for the reviewer.

## 6. Engine: sessions inside `RunWorkflow`

### 6.1 State

`runState` gains:

```
pins map[string]*pinState        // by pin name; built in newRunState from wf.Pins
type pinState struct {
    name     string
    tasks    []string             // members in declaration order
    opened   bool                 // CreateSession has been asked for
    ready    workflow.Future      // settled when the session is Open, or with the creation error
    ctx      workflow.Context     // the session context; nil until ready
    openedAt time.Time            // workflow.Now when opening began (for PIN_TIMEOUT classification)
    done     workflow.Channel     // main loop → coroutine: every member is terminal
    closed   workflow.Future      // coroutine → main loop: CompleteSession has run
    lost     bool                 // the session failed (host died or timed out)
}
```

All of it is a pure function of the workflow input and history. Maps are
iterated through `s.order` / sorted pin names, never ranged directly
(`cancelInFlight`'s rule).

### 6.2 Opening a pin: lazy, non-blocking, in its own coroutine

A pin is opened when the **first** of its tasks is ready and classified to
run — never at run start. A pin whose every task is skipped by `condition` or
by an upstream skip is never opened and costs no Orker anything; a pin's
lifetime clock (§6.3) starts when work is actually about to start.

Opening is done in a `workflow.Go` coroutine, for two reasons that both
force it:

1. `CompleteSession` must run in the coroutine that ran `CreateSession` (§3),
   so *something* other than the main loop has to own the session's life.
2. `CreateSession` blocks until the host answers, bounded only by
   `CreationTimeout`. Under a saturated pool that is minutes. The main loop's
   own comment says "a fast branch can advance without waiting for an
   unrelated slow branch"; a blocking `CreateSession` in the loop would break
   exactly that for every unpinned branch of the run.

The coroutine:

```
go (per pin):
    base := workflow.WithActivityOptions(runCtx, ActivityOptions{TaskQueue: queueForPool(pin's pool)})
    sessionCtx, err := workflow.CreateSession(base, &SessionOptions{
        ExecutionTimeout: pinTimeout,            // §6.3
        CreationTimeout:  pinCreationTimeout,    // §6.3
        HeartbeatTimeout: task.HeartbeatTimeout, // 30 s; the SDK ticks every 10 s
    })
    settle pin.ready (ctx or err)
    if err != nil { settle pin.closed; return }
    wait pin.done                                // main loop says every member is terminal
    if session still Open:
        release := workflow.NewDisconnectedContext(sessionCtx)    // §12.2
        ExecuteActivity(WithActivityOptions(release, short opts), task.ActivityReleasePin, ReleasePinInput{...}).Get  // best effort, logged
    workflow.CompleteSession(sessionCtx)
    settle pin.closed
```

`base` derives from `runCtx`, so `cancelRun()` in `fail()` reaches the session
context and therefore every pinned activity (the §3.3 cancellation path is
unchanged). The `TaskQueue` set on `base` is the pool's queue from the plan —
`CreateSession` reads it to derive `<queue>__internal_session_creation`. No
queue name is computed anywhere new: `routeForTask` is still the only lookup.

`pin.ready` is registered in the main loop's `Selector` beside the in-flight
futures, in a fixed position (sorted pin names, after the task futures), so
readiness is consumed deterministically. While a pin is opening, its ready
members are held: `readyTasks()` excludes a task whose pin is `opened && !ready`
— they are still `PENDING`, nothing has been emitted for them, and they are
re-scanned the moment `pin.ready` settles.

### 6.3 Scheduling a pinned task, and the three timeouts

`schedule(ctx, id)` is called with `s.pins[t.Pin].ctx` instead of `runCtx`
for a pinned task; its body is unchanged except:

- `ExecuteTaskInput.Pin = &PinContext{Name, InstanceID: GetSessionInfo(ctx).SessionID, TimeoutSeconds: pinTimeout}`.
- `TaskRun.Pin = t.Pin`.
- The `TASK_SCHEDULED` message reads `task scheduled on pool <pool>, pin <name>`.

`ActivityOptions.TaskQueue` is still the pool queue and `TaskRun.pool` is still
`poolForTask` — the SDK swaps the queue for the session's at `ExecuteActivity`
(§3), and the *pool* is what the author wrote and what the client is told,
unchanged. `StartToCloseTimeout`, `HeartbeatTimeout`, `WaitForCancellation`
and `RetryPolicy` are per task, unchanged.

The pin adds two timeouts of its own, both set in workflow code from data the
workflow already holds:

| timeout | value | why |
|---|---|---|
| `SessionOptions.ExecutionTimeout` | `Pin.timeout_seconds`, or when 0: `Σ over members (timeoutSecondsFor + graceSecondsFor + plan.TransferBudgetSeconds)` plus `pinReleaseBudget` (a constant, 60 s, for the release activity) | It is the creation activity's `StartToClose`, i.e. the pin's total life. The sum is the sequential worst case; the engine cannot know the pin's internal parallelism cheaper than by computing it, and an over-estimate costs nothing while an under-estimate kills a healthy pin. A pure function of the definition and the plan: replays identically. |
| `SessionOptions.CreationTimeout` | constant `pinCreationTimeout = 5 * time.Minute` | How long to wait for *some* pin host in the pool to have a free slot. Submit already refused a pool with no pin host (§7.4), so this only covers saturation. A constant keeps the workflow free of configuration; making it a `Pin` option is a one-field addition later if needed (§12.4). |
| `SessionOptions.HeartbeatTimeout` | `task.HeartbeatTimeout` (30 s) | The dead-host detector for the pin, the same number the task heartbeat uses; the SDK caps its tick at 10 s. |

### 6.4 Closing a pin

After every state change that can make a task terminal (`collect`,
`markSkipped`, `markCanceled`), the loop checks each opened pin: when all
members are terminal it sends on `pin.done`. The coroutine then releases the
shared directory (best effort, on the pin host, §8.5) and calls
`CompleteSession`, which runs the SDK's 3 s completion activity and frees the
host's slot.

`succeed()` and `fail()` **wait for every opened pin's `closed` future** before
returning, on `eventCtx` (the disconnected context `fail()` already builds
when the run was cancelled). The wait is bounded by the release budget plus
the SDK's 3 s; it exists because a workflow that returns first abandons the
coroutine: the host would then hold the pin's slot until its creation
activity's next heartbeat learns the workflow is gone (≤ 10 s — the SDK
handles it, `sessionCreationActivity`'s `ctx.Done()` branch), but the release
activity would never run and the shared directory would wait for its lease
(§8.4). Prompt cleanup is worth the bounded wait.

### 6.5 Failure semantics, precisely

This is the sharpest tension in the feature and it is resolved by one rule:

> **A pin never moves.** The Orker that opened it is the only Orker that will
> ever run its tasks. If that Orker is lost, every member of the pin that has
> not already succeeded fails, and the run fails. The engine never re-creates
> a session for a pin, never falls back to the pool's ordinary queue, and
> never re-opens the pin on another host.

The reason is the feature's own promise. `/orkstack/shared` lives on one host's
disk (§8). Temporal's `RecreateSession` targets the *same resource queue*,
which no live process polls after a crash (the uuid is per worker start), and
even a hypothetical "new session on a new host" would hand the remaining tasks
an empty directory: `compile` would run against a `src/` that was never
cloned. That is a wrong result at best and a silently-committed wrong result
at worst — the outcome AGENTS.md §14 ranks below explicit failure. "An Orker
crash cannot corrupt Temporal workflow state" (§15) still holds: the session
failure is a history event and the run fails through `fail()` with a
machine-readable reason. What changes is §5's "if an Orker disappears,
Temporal must be able to retry elsewhere": for a pinned task it deliberately
cannot, and §11 amends the sentence to say so.

| event | detected by | task outcome | run outcome |
|---|---|---|---|
| no pin host in the pool | submit-time probe (§7.4) | — | refused at submit: `PoolNoPinHostError` |
| every pin host in the pool is full for `CreationTimeout` | `pin.ready` settles with the creation error | the task that triggered opening: `FAILED`, `Error{Code: "PIN_UNAVAILABLE", Category: INFRASTRUCTURE_ERROR, Retryable: false}`, message names the pool and the wait | `fail()` |
| pin host dies mid-pin | session `Failed` (creation-activity heartbeat, ≤ 30 s); session ctx cancelled; every in-flight member's future returns a cancellation | `collect` checks `GetSessionInfo(pin.ctx).SessionState == Failed` **before** `recordFailure`: status `FAILED` (not `CANCELED`), `Error{Code: "PIN_HOST_LOST", Category: INFRASTRUCTURE_ERROR, Retryable: false}`; members not yet scheduled would get `ErrSessionFailed` but never get that far — `fail()` marks them `CANCELED` | `fail()` |
| pin exceeds `ExecutionTimeout` | same mechanism (the creation activity times out → session `Failed`) | as above, but `Code: "PIN_TIMEOUT", Category: TIMED_OUT`, chosen when `workflow.Now − pin.openedAt ≥ ExecutionTimeout` (deterministic: `workflow.Now` and a recorded start) | `fail()` |
| a member task fails on its own | ordinary activity failure | as today | `fail()` (fail-fast), pin closed by §6.4 |
| a member task's attempt fails and `retry:` allows more | Temporal retries **on the session queue** → same host | the retry runs in a **fresh per-attempt workspace** but sees the **same `/orkstack/shared`**; commit adoption (§5.4 of the OCI contract) unchanged | — |
| operator cancels the run | `cancelRun` → session ctx → members SIGTERM/SIGKILL as today | `CANCELED` | `RUN_CANCELED` |
| pin host restarts *after* the pin finished but before `ReleasePin` | release activity fails (nobody polls the queue) | logged, never fails the run | boot reap removes the directory (§8.5) |

"Why retrying stopped" (`retry_state`) is populated as today; a `PIN_HOST_LOST`
task's `retry_state` will read `CANCEL_REQUESTED`, which is truthful about what
Temporal saw and why the code on the `Error` is the primary reason.

### 6.6 Determinism and the replay guard

No `workflow.GetVersion` gate is proposed. The pin code path is taken only when
the definition names a pin; no history recorded before this feature contains a
pinned task, and a `Pinned` `RunWorkflow` never replays an in-flight run on a
newer build anyway (§7.2). `recordFailure`'s comment gives the criterion: a
gate belongs to a change that alters the command stream of an execution that
could genuinely be replayed against the new code. This change alters the
command stream only of executions that could not exist before it.

Guards:

- `TestRunWorkflow_ReplaysCommittedV1History` and
  `TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends` must pass
  **unchanged** — `ExecuteTaskInput.pin` is unset for every task in the
  fixture, and protojson omits an unset message field, so the recorded
  payload bytes are identical. That is the proof of §10.
- A **second** golden fixture, `run-pinned-v1.json`, recorded by an extended
  `TestIntegration_RecordGoldenRunHistory` against a real cluster with a pin
  host, so the session command stream (marker, creation activity, signal,
  member activities, release, completion) is frozen from the day it lands.
  This is a new fixture for a new path, not a reset of the existing one, so
  it does not collide with §11.4's "one recording" rule — §12.8 asks the
  reviewer to confirm that reading.
- Unit coverage in `workflow_test.go` via
  `env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})` with the
  member activities mocked: open-lazy, held-until-ready, concurrent members
  share one session, `PIN_HOST_LOST` classification (the test suite can fail
  the creation activity), close-on-terminal, close-awaited-before-return.

## 7. Orker: becoming a pin host

### 7.1 Configuration (one setting; AGENTS.md §11.1 step 2)

| key | env | default | binary | meaning |
|---|---|---|---|---|
| `orker.pins.max_concurrent` | `ORK_ORKER_MAX_PINS` | `0` | `orker` | how many pins this Orker hosts at once. `0` hosts none and starts no session pollers — the Orker runs exactly as it does today. |

Added to `internal/config`'s `Schema`, `docs/DEPLOY.md` §1's table, and
`deploy/orkstack.defaults.yaml` (`make config-defaults`); the completeness
tests already refuse a setting missing from any of the three. Default `0`
rather than "on": `EnableSessionWorker` adds two pollers and two task queues
per process (§3), and a deployment that never set the key must not acquire
them.

### 7.2 Worker options (`cmd/orker/main.go` `workerOptions`)

```
EnableSessionWorker:                cfg.maxPins > 0,
MaxConcurrentSessionExecutionSize:  cfg.maxPins,
```

alongside the existing `DisableWorkflowWorker`, `DisableRegistrationAliasing`
and `MaxHeartbeatThrottleInterval` (the cap applies to the session creation
activity's heartbeats too, which is fine — the SDK already ticks at ≤ 10 s).
`cfg.deployment.Apply` still wraps it; §12.1 owns what happens when it is
versioned. A new activity is registered beside `ExecuteTask` and
`DescribeOrker`: `task.ActivityReleasePin = "ReleasePin"` (constant in
`internal/task/contract.go`, beside the other two, for the same reason they
are there).

### 7.3 `DescribeOrker`

Reports `MaxConcurrentPins` from options (`Activities.maxPins`), the way it
reports `MaxConcurrent`. The boot log line adds `"max_pins"`.

### 7.4 Submit-time refusal (`PoolResolver.Plan`)

For each pool that carries at least one pinned task, after the liveness probe
passes, `Plan` asks the pool's Orker to describe itself (the `OrkerOnPool`
path `Pools()` already uses) and refuses with a new `PoolNoPinHostError{
Namespace, Pool, Pins, Tasks}` when `max_concurrent_pins == 0`. Message:

> pool "gpu" (namespace "ci", pin "build", tasks: clone, compile, test) has
> live Orkers but none of them hosts pins, so the pin could never open. Start
> one with ORK_ORKER_POOL=gpu ORK_ORKER_MAX_PINS=1, or submit with
> allow_empty_pool to wait for one

`allow_empty_pool` downgrades it to a warning, the way it does for an empty
pool: the operator who overrides has been told what they are waiting for. The
error maps to the same Connect code `PoolEmptyError` maps to, in
`internal/api`. Limitation: the probe hears from *one* Orker (§12.6); the
documented deployment rule is that pin hosting is a **pool-wide** property —
every Orker of a pool sets the same `ORK_ORKER_MAX_PINS`.

### 7.5 Dispatcher changes (`internal/orker/oci_dispatcher.go`)

In `execute()`, between STEP 4 (workspace) and STEP 5 (staging), when
`in.GetPin() != nil`:

- `pinWS, err := d.Pins.Acquire(in)` — resolves or creates the pin directory
  (§8.2) keyed by `(run_id, pin.name, pin.instance_id)`, writes or refreshes
  `lease.json` (§8.4), and returns its `shared/` path. It is **not** deferred
  for removal.
- `ws.Mounts()` gains `{Source: pinWS.Shared, Destination: ContainerShared}`
  read-write (a new method `MountsWithShared`, or a field on `Workspace`; the
  existing method's promise "every source is a path this Orker created for
  this attempt" becomes "…for this attempt or for this attempt's pin", and
  both sources are still Orker-constructed — §9.1's "no arbitrary host path
  mounts" is intact because the definition cannot name a path).
- `containerSpec` adds label `"ork.pin": instance_id`.
- `WriteTaskJSON` receives the pin through `TaskContext` and emits
  `paths.shared` and `task.pin: {name, instanceId}`. Both are **omitted** for
  unpinned tasks. Additive; no `schemaVersion` bump (§3.2's "may only add
  optional fields"); a workload that ignores them loses nothing.

Everything else in the attempt — staging, sealing, secrets, run, validate,
commit, `ws.Remove()` and `ws.RemoveSecrets()` on every exit — is unchanged.

## 8. The shared workspace on the pin host

### 8.1 What it is and is not

`/orkstack/shared` is one read-write directory that every task of one pin,
every attempt of every such task, sees at the same container path with the
same contents, for the life of the pin. It is **not** an input tree (never
sealed), **not** an output tree (nothing in it is scanned, hashed, uploaded or
committed), and **not** where secrets go (never; §8.3). A task that wants a
file from `/orkstack/shared` to outlive the pin copies it to
`/orkstack/output/artifacts/` and it becomes an artifact like any other.

### 8.2 Where it lives, who owns it

```
<ORK_ORKER_WORKDIR>/
├── ork-<run>-<task>-<attempt>-XXXX/          per attempt, exactly as today (0700, removed on exit)
└── ork-pin-<run>-<pin>-<instance-id>/         per (run, pin, opening); 0700, Orker's user
    ├── lease.json                             0600; {"expiresAt": …, "runId": …, "pin": …}
    └── shared/                                1777 + sticky; bind-mounted rw at /orkstack/shared
```

Every component is Orker-constructed: `safeComponent(run_id)`,
`safeComponent(pin.name)` (a slug, so it is already safe), and the instance id
(a uuid the engine minted through `SideEffect`, validated by the Orker as
`^[0-9a-f-]{36}$` before it becomes a path segment — validated, never
sanitized, the secrets-name discipline). The 0700 root is what makes the 1777
child safe, the same sentence `workspace.go` says about the attempt root.

Two Orkers on one host use different work roots (`defaultOrkerID` makes that a
requirement already), so a pin directory belongs to exactly one Orker
identity. Two attempts or two concurrent members on one host share the same
`shared/` by construction — that is the feature.

### 8.3 Sealing and secrets between tasks

Nothing in the pin directory is ever sealed and nothing is ever unsealed: the
per-attempt `input/` is sealed per attempt exactly as today; `shared/` is
writable from creation to removal. Secrets stay in the per-attempt `secrets/`
bind and are wiped on every exit path exactly as today; the Orker never writes
a secret under `shared/`, and `docs/DSL.md` tells the author not to either
(a workload that copies a secret into `/orkstack/shared` has put it on disk
for the pin's lifetime — the same warning that applies to `output/`).

### 8.4 Lifetime: creation, lease, and the three ways it ends

- **Created** by the first member attempt that arrives with the pin's
  `instance_id` (`Acquire`, idempotent `MkdirAll`). The Orker has no hook at
  session creation — the creation activity is SDK-internal — and does not
  need one — except the cordon interceptor, which refuses or counts the
  creation and touches no directory
  (`docs/design/proposals/phase-2/orker-cordon-drain/spec.md` §5.4).
- **Leased**: `lease.json` records `expiresAt = firstArrival + pin.timeout_seconds + pinLeaseSlack` (slack = the release budget plus 5 min). The
  session's `ExecutionTimeout` is measured from *creation*, which strictly
  precedes first arrival, so the session is dead before the lease expires —
  the lease can never remove a directory a live session still uses.
- **Removed**, by whichever of three comes first:
  1. `ReleasePin` (§8.5): the engine's explicit request, on the pin host,
     after every member is terminal. The normal path; prompt.
  2. **Boot reap**: `Activities.Boot`, beside the container reaper, removes
     every `ork-pin-*` under the work root. This is not a heuristic: the
     session resource id is a fresh uuid per worker start
     (`internal_worker.go:526`), so no session — and no pin — can survive an
     Orker restart, and every pin directory found at boot is dead by
     construction. (The same sweep could remove leftover
     `ork-<run>-<task>-<attempt>-*` trees a crashed predecessor left behind,
     which nothing removes today; recommended, out of scope.)
  3. **Lease sweep**: a goroutine in `Activities` ticks every minute and
     removes any `ork-pin-*` whose `lease.json` has expired. It covers the one
     leak the other two cannot: an Orker that stays alive while the run is
     terminated from outside (an operator terminates the workflow; an engine
     that never reaches `fail()`). The SDK frees the *session* in that case
     (`sessionCreationActivity` sees its heartbeat refused and completes
     locally), but nothing OrkStack-side runs, so the directory would
     otherwise outlive everything. Removal uses `Workspace.Remove`'s
     chmod-then-`RemoveAll` walk, because a workload may have left 0500
     directories under `shared/`.

Under all three, the directory is "ephemeral execution directories" in §5's
sense — bounded, local, disposable, reconstructible by re-running — and
§11 records the one-line amendment that makes the pin's longer lifetime
explicit in the contract.

### 8.5 `ReleasePin`

`Activities.ReleasePin(ctx, *ReleasePinInput) error`: validate the instance
id, remove `ork-pin-<run>-<pin>-<instance>`, return nil if absent. Registered
on the ordinary Orker worker; because the engine schedules it on the session
context it is routed to the pin host's session queue (§3), so it always runs
where the directory is or not at all. Scheduled with `StartToClose` 30 s,
`MaximumAttempts` 3, and its failure is logged by the coroutine, never
returned: cleanup is not orchestration, and the lease is the backstop.

### 8.6 Data flow: additive, never a replacement

The existing contract is untouched and remains the only one the *engine*
knows about: a pinned task still writes `values.json` and artifacts to
`/orkstack/output`, the Orker still validates, hashes, uploads and commits
them, `ExecuteTaskResult` still carries references, and `upstreamFor` still
projects `UpstreamRef`s into downstream inputs. Consequences worth stating:

- A pinned task is **not required** to touch `/orkstack/shared`. A pin of
  ordinary artifact-passing tasks is legal and merely runs them on one host.
- An **unpinned** downstream task — and a task in a *different* pin — sees a
  pinned task's work only through committed artifacts. `/orkstack/shared` is
  invisible outside its pin.
- Inside a pin, `/orkstack/shared` is a second channel beside the committed
  one, and the two are not kept consistent by the platform: a task that wrote
  `bin/app` to `shared/` and *also* wants it durable writes it to
  `output/artifacts/` too. `docs/DSL.md` says this in the pin section.
- Retries see the shared directory as the previous attempt left it (§6.5).
  `task.json` already carries `task.attempt` and `idempotencyKey`; the
  document tells authors to make shared-state steps idempotent or
  attempt-aware, the same at-least-once caveat AGENTS.md §4 already makes
  about external side effects, now applied to the host's own disk.

## 9. Observability and API surface

- `TaskRun.pin` (from the definition at `schedule()`) and `TaskRun.orker_id`
  (from `ExecuteTaskResult.orker_id` in `collect`, and from
  `ActivityFailureDetail.orker_id` in `recordFailure`). `orker_id` is
  populated for every task, pinned or not — the fields already travel; the
  snapshot just never copied them — and it is the client-visible proof that a
  pin held: same `pin`, same `orker_id`. A member lost with its pin reports
  neither source, so `attributeLostPinHost` backfills its `orker_id` from a
  sibling of the same pin (§6.5's single-host invariant makes that the same
  Orker by construction); with no sibling ever reporting it stays empty and the
  failure's `Error.details` carries the Temporal session's raw OS host name as
  an explicitly labelled best-effort lead, which is not an `orker_id`.
- `TASK_SCHEDULED` message: `task scheduled on pool <pool>, pin <name>`. No
  new `EventType`: the Event contract has no pin-lifecycle type and this
  design does not add one; pin open/close is visible through member
  `TaskRun`s and, for operators, Temporal history.
- `TaskAttempt` (`run.proto`) already carries `orker_id`; a later addition of
  `pin` there is additive and out of scope.
- `orkctl run get` / `run watch` / `run attempts` print `pin` and `orker`
  beside `pool`; `--json` carries the fields as the proto names them.
- UI run page: beside `Pool <pool>` (`ui/src/routes/runs/[id]/+page.svelte:920`)
  render `Pin <name>` and `Orker <id>`, and group or colour members of one pin
  so "these ran together" is visible at a glance.
- `docs/UI_CLI_PARITY.md` gains a "Task pins" entry in "Shared API changes"
  listing the three new `TaskRun`/`DescribeOrkerResult` fields and the
  parity expectation.
- `DescribeOrkerResult.max_concurrent_pins` flows into the admin pool
  inventory where `max_concurrent` already shows.
- Orker logs: `pin acquired` / `pin released` / `pin reaped` /
  `pin lease expired` with run, pin, instance and path; the attempt record
  (`recordAttempt`) is unchanged.

## 10. Backward compatibility and defaults

Stated as checks an implementer must be able to point at:

1. **A document with no `pins:` and no `pin:`** compiles to the same
   `Orkflow` message as before (both new fields empty), canonicalizes to the
   same bytes (§5.4; the existing golden vector is the proof), and is stored
   and diffed as before.
2. **A run of such a document** executes `schedule()` on `runCtx` for every
   task, creates no session, emits the same events in the same order, and
   sends `ExecuteTaskInput`s that are byte-identical to today's (`pin` unset,
   omitted by protojson) — `TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends`
   and `TestRunWorkflow_ReplaysCommittedV1History` pass unchanged.
3. **An Orker with `ORK_ORKER_MAX_PINS` unset** builds the same
   `worker.Options` as today plus two zero-valued fields, starts no extra
   poller, creates no extra task queue, registers one extra activity
   (`ReleasePin`) nobody schedules, and reports `max_concurrent_pins: 0`,
   which protojson omits.
4. **Submit** for an unpinned document runs no describe-Orker call it did not
   run before.
5. Pinning is opt-in **per Orkflow author** (the document) *and* **per
   operator** (the Orker setting); a document that pins is refused at submit
   until an operator has opted a pool in, never left waiting.

## 11. Invariants this design touches

| where | today | proposed amendment | why justified |
|---|---|---|---|
| AGENTS.md §5, OCI contract §6.4 | "Persistent local state is limited to an OCI image cache, ephemeral execution directories, and optional transient download cache. If an Orker disappears, Temporal must be able to retry elsewhere." | add: "A pin's shared directory is an ephemeral execution directory whose lifetime is the pin's, bounded by a lease and reaped at boot. **A pinned task is the one deliberate exception to retry-elsewhere**: if its host disappears, the pin and its run fail explicitly (`PIN_HOST_LOST`); they are never re-opened on another Orker." | The alternative — retry elsewhere with an empty `/orkstack/shared` — is a silently wrong result, which §14 ranks below explicit failure. The exception is opt-in, named, and machine-readable. |
| AGENTS.md §7.1 | "`pool:` is the only user-facing execution selector." | "`pool:` is the only user-facing selector of *which Orkers*; `pin:` constrains a group of tasks to *one* member of that set. Neither exposes a queue, a session, or a host name." | `pin` selects nothing outside the pool; it narrows within it. The sentence's intent (no queue leaks) is preserved verbatim. |
| AGENTS.md §7.2 | "A pinned workflow uses its own activity-worker version only while that version has workers polling its activity channel; otherwise Temporal falls through to that channel's current version." | add: "A task pin is the one exception to that rule, and it is stricter rather than looser: its session is opened through a version-routed activity, but its member activities ride the session's own per-host channel, which is unversioned and polled by exactly one process — so a pin stays on the process that opened it across a rollout and never falls through to the current version." | Measured, not reasoned (§12.1): `internal/engine/versioning_pin_integration_test.go` rolls the Current Version forward while a pin is open, and the member scheduled after the switch still executes on the process that opened the pin. The sentence's intent — no silent mid-run version change — is strengthened, not weakened. |
| AGENTS.md §11 chaos list | eleven scenarios | add: a Worker Deployment Version rollout while a pin is open | §13 Phase 6's own row, now covered by a suite |
| AGENTS.md §7 vocabulary | Pool, Queue, … | add **Pin**, **Session** (§1) | same pattern as Pool/Queue |
| OCI contract §3.1 layout | four mounts | `/orkstack/shared` when pinned; `paths.shared` and `task.pin` in `task.json`, omitted otherwise | additive; per-attempt guarantees unchanged |
| OCI contract §4.4 canonical form | "emitted whether or not zero" | `pin`/`pins` emitted only when set | §5.4 |
| AGENTS.md §15 | eight product invariants | none changed | each is walked in §6.5 |

Nothing here creates a second scheduler, state machine, lock service or
execution authority: the pin is Temporal's session, its lifetime is Temporal's
creation-activity timeout, its failure is Temporal's heartbeat timeout, and
its only OrkStack-owned state is a directory with a lease.

## 12. Open questions and risks

Ordered by how much they could change the design.

1. ~~**Sessions × Worker Deployment Versioning (`ORK_BUILD_ID` set).**~~
   **CLOSED — measured, and it works.** The prediction held exactly:
   `worker.New` accepts `EnableSessionWorker` together with
   `DeploymentOptions.UseVersioning` (the enforced panic covers only the
   deprecated `UseBuildIDForVersioning`), the pin's creation activity is routed
   by version to a same-version pin host, and the pin's members — including one
   scheduled after a rollout made a newer version Current — stay on the process
   that opened the pin, over the session's unversioned `<uuid>@<host>` queue.
   Evidence: `internal/engine/versioning_pin_integration_test.go`
   (`TestIntegration_AnOpenPinStaysOnItsHostAcrossAVersionRollout`, two pin
   hosts and a rollout over an open pin) and `cmd/orker`'s
   `TestWorkerOptions_APinHostOnAVersionedDeploymentIsAcceptedByTheSDK`.
   `docs/DEPLOY.md` ("Pins on a versioned deployment") and AGENTS.md §7.2 now
   state this as a property, so the documented fallback — refusing to boot a
   pin host while versioned — was **not** implemented and must not be: it would
   forbid a configuration that is proven to work. What the closure does *not*
   buy: the combination is still unsupported upstream
   (`temporalio/sdk-go#1227`), and the pin is held by queue exclusivity rather
   than by version routing, so a pin is bound to a process — a same-version
   restart loses it, exactly as §6.5 says. Those two tests are therefore the
   contract, and an SDK upgrade that turns either red is the signal to
   reconsider the fallback.
2. ~~**Does `workflow.NewDisconnectedContext(sessionCtx)` keep the session
   routing?**~~ **CLOSED — it does, measured.** The assumption held: the
   disconnected child carries `SessionInfo` as a context value, so the release
   lands on the host that owns the directory. Evidence:
   `internal/engine/versioning_pin_integration_test.go`'s
   `assertReleaseLandedOnTheHost` (real cluster, two versions — the release
   reached the pin host and the newer version's Orker was never asked for it,
   which is the routing this item doubted), and `workflow_test.go`'s
   `TestRunWorkflow_LeavesAPinnedTaskThatFailedOnItsOwnToTheOrdinaryPath` for
   the cancelled half — the pin is still released after `fail()` has cancelled
   `runCtx`, so the activity is scheduled rather than refused. Both ride the
   single release path in `workflow.go` (`NewDisconnectedContext(sessionCtx)`),
   so there is no second, unmeasured variant. The lease sweep (§8.4) remains
   the backstop for the case this item worried about — a host that is gone —
   not for a context that lost its routing.
3. **`CompleteSession`'s same-coroutine rule** is why §6.2 uses a coroutine
   per pin. Emits from the coroutine (none are proposed; the coroutine emits
   nothing) would share the event counter; the design keeps all `emit`s in
   the main loop so sequence assignment stays in one place.
4. **`CreationTimeout` as a constant** (5 min). A saturated pool holds the
   pin's tasks — and only them — for up to that long, then fails the run with
   `PIN_UNAVAILABLE`. If that is the wrong trade, `Pin.creation_timeout_seconds`
   is a one-field addition. Not proposed now: fewer knobs.
5. **The session activity worker's slot count.** `GetSessionActivitySlotSupplier()`
   sizes the pin host's concurrency for *pinned* activities; its default was
   not verified. If it is small, two concurrent members of one pin could
   queue behind each other on the host. Verify; expose through the tuner if
   needed.
6. **One Orker answers the pool probe.** A mixed pool (some hosts with pins,
   some without) can be refused or accepted depending on who answered. The
   documented rule is "pin hosting is pool-wide"; an alternative is to make
   the check advisory. Recommendation: refuse, because "wait forever for a
   slot that does not exist" is the outcome the submit-time probe exists to
   prevent.
7. **Canonical-form deviation** (§5.4): emit `pin`/`pins` only when set,
   against §4.4's "whether or not zero". Decision requested. The alternative
   (a versioned canonical form) is larger than this feature.
8. **A second golden fixture** (`run-pinned-v1.json`) for the session command
   stream. §11.4 permits one recording of the *existing* fixture; adding a
   fixture for a new path is read here as additive. Decision requested.
9. **Sticky-bit uid clashes** are reduced by rule 11 and the
   `run_as_orker_user` warning but not eliminated for images with different
   baked-in `USER`s and the flag false. This is a documented author
   responsibility, not a platform guarantee.
10. **Temporal server version.** `RecreateSession` notes fixes before
    server v1.15.1; this design never recreates. No server minimum is added.
11. **Pin directory disk growth** is bounded only by the lease and the host's
    disk; no per-pin quota is proposed. `ORK_ORKER_TMPFS_BYTES` does not apply
    (it is a bind, not tmpfs). Out of scope; noted.
12. **`orkctl activity test`** runs one image through the dispatcher with no
    pin; unaffected. A `--pin` mode that mounts a scratch `shared/` is a
    possible later convenience.

## 13. Phased implementation plan

Each phase ends green on `make lint test`; integration phases add
`make test-integration` against `make infra-up`, and the last adds
`make proof`. Every phase follows §11.1's order (proto first; failing test
first).

**Phase 0 — Contract text.** AGENTS.md §5, §7 vocabulary, §7.1 (per §11);
`docs/DSL.md` (a "Pins" section after "Tasks", the five new validation rules,
the concurrency and shared-vs-committed sentences); OCI contract §3.1, §3.2,
§4.4, §6.4; `docs/DEPLOY.md` (the setting, the pool-wide rule, the
versioning caveat pending §12.1). Files: `AGENTS.md`, `docs/DSL.md`,
`docs/OCI_ACTIVITY_CONTRACT.md`, `docs/DEPLOY.md`.

**Phase 1 — Proto and canonical form.** §5.1-5.3 in
`proto/orkstack/v1/orkflow.proto`, `activity.proto`, `run.proto`; `make proto`.
`internal/dsl/canonical.go`: `pin` on the task map and `pins` on the orkflow
map, both only when non-empty; `canonical_test.go`: existing golden vector
unchanged (the test), new pinned vector. `internal/task/contract.go`:
`ActivityReleasePin`.

**Phase 2 — DSL.** `internal/dsl/compile.go`: `document.Pins map[string]pinDocument`,
`taskDocument.Pin`, compile to `Orkflow.Pins` sorted by name and `Task.Pin`.
`internal/dsl/validate.go`: rules 9-13 beside rule 7 (line ~112). Tests
table-driven; `examples/pinned-build.ork.yaml` (the §4.1 shape) compiled by
`TestCompile_AcceptsEveryShippedExample`. `ValidateOrkflow` warning for
rule-11-false pins (`internal/api`, where issues are assembled).

**Phase 3 — Engine.** `internal/engine/pools.go`: rule 10 on resolved pools;
`PoolNoPinHostError` and the describe call for pinned pools; `internal/api`
mapping. `internal/engine/workflow.go`: `pinState`, lazy open coroutine
(§6.2), held-ready members, `schedule` on the pin context with `PinContext`,
`TaskRun.Pin`/`OrkerId`, `collect`'s `PIN_HOST_LOST`/`PIN_TIMEOUT`
classification before `recordFailure`, close-on-terminal, `succeed`/`fail`
awaiting `closed`. `workflow_test.go` with `EnableSessionWorker` (§6.6);
`replay_test.go` unchanged and green; `golden_history_integration_test.go`
extended to record `run-pinned-v1.json` (pending §12.8). Integration:
`pool_routing_integration_test.go`-style suite proving two members land on one
Orker id and a killed host yields `PIN_HOST_LOST`; a versioning suite for
§12.1.

**Phase 4 — Orker.** `internal/config/config.go` setting + `docs/DEPLOY.md`
row + `make config-defaults`. `cmd/orker/main.go`: `workerOptions` (§7.2),
`ReleasePin` registration, boot log. `internal/orker/activity.go`:
`Options.MaxPins`, `DescribeOrker.MaxConcurrentPins`, `ReleasePin`, boot reap
of `ork-pin-*`, lease sweeper goroutine. `internal/orker/workspace.go`:
`ContainerShared`, `PinWorkspace{Root, Shared, LeasePath}`, `AcquirePin`,
`ReleasePin`, `taskJSONPaths.Shared` + `taskJSONTask.Pin` (omitempty).
`internal/orker/oci_dispatcher.go`: step 4b, the extra mount, the `ork.pin`
label. Tests: permissions match the layout (extend
`TestWorkspace_PermissionsMatchTheLayout`), acquire is idempotent across
attempts, secrets never under `shared/`, lease expiry removes, boot reap
removes; integration (`orphan_reap_integration_test.go`'s shape): two real
containers share a file through `/orkstack/shared`; SIGKILL the Orker
mid-pin and confirm the replacement reaps the directory.

**Phase 5 — Clients and parity.** `cmd/orkctl` run/watch/attempts output and
`--json`; `ui/src/routes/runs/[id]/+page.svelte` pin and Orker rendering with
a Playwright spec and screenshots; `docs/UI_CLI_PARITY.md` entry; admin pool
inventory shows `max_concurrent_pins`.

**Phase 6 — Evidence.** `make proof` with the new suites; chaos rows added to
the AGENTS.md §11 list: pin host killed while a member runs, killed between
members, killed after `commit.json` before completion (adoption on the same
host on retry), workflow terminated while a pin is open (lease sweep), full
pool (`PIN_UNAVAILABLE`), and — under `ORK_BUILD_ID` — a rollout with an open
pin (§12.1). Quote the proof directory in the commits.
