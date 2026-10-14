# AGENTS.md — OrkStack engineering contract

This file is authoritative for every human and AI agent working in this repo.
It defines the product boundary, runtime contracts, invariants, and evidence
required for a change to be done.

## 1. Mission and product boundary

OrkStack is a thin, opinionated durable execution platform. It owns the
contract between three boring primitives:

```text
Temporal        execution and durable workflow state
Object storage  artifacts, large data, and immutable records
OCI containers  computation
```

The north star is:

> Any OCI image can become a durable production activity without linking
> against an OrkStack SDK or running a custom worker service.

The activity model is Unix-like: it receives files, writes files, emits
stdout/stderr, and exits. The filesystem is the canonical activity API.

OrkStack must not become a generic workflow engine, another state machine, or
another plugin framework. Workloads must not need a required gRPC or HTTP
callback, SDK, sidecar protocol, message queue, custom RPC framework, or
application-specific worker daemon.

```text
YAML Orkflow ──apply──> ork ──compile──> Temporal ──dispatch──> Orker
                                                                    │
                                                               OCI image
                                                                    │
                                                              object storage
```

The `ork` engine, `orkctl` CLI, SvelteKit UI, Connect API, WebSocket bridge,
Orkers, and artifact store support this contract. They must not add another
durable execution authority.

## 2. Core architecture and ownership

### 2.1 Temporal owns execution

Temporal is the sole source of durable execution truth. It owns:

- Temporal workflow lifecycle and state;
- activity scheduling, retries, timers, cancellation, and signals;
- durable waiting and recovery;
- workflow history and execution status.

Temporal *workflow* code in `internal/engine` must be deterministic: no clocks,
network, randomness, or direct IO. Side effects belong in activities or the
Orker. Determinism is required of OrkStack's workflow code only, never of a
task: a container may read the clock, the network and its environment. Do not
create an OrkStack-owned execution-state database, Orkflow-state database,
scheduler, task-queue implementation, distributed lock service, retry
subsystem, timer subsystem, or state-transition engine. Cron scheduling
must use Temporal Schedules. Interaction with a running Temporal workflow must
use Temporal Signals, including approval, external completion, resume, and abort.

### 2.2 Object storage owns data and artifacts

Temporal history and payloads contain only small structured metadata and
references. The hard maximum serialized payload size is **256 KiB**. Anything
larger must be an Artifact. Never put large bytes in Temporal payloads or the
event stream.

Object storage holds artifacts, logs, immutable Orkflow-definition snapshots,
and immutable activity completion and per-attempt records. Object-store data is
not mutable workflow execution state (Temporal workflow state) and must never
become a second state machine. Temporal remains authoritative for execution state.

An Orkflow keeps its existing user-facing `name` and `version` identity, but
its canonical definition also has a SHA-256 digest. The immutable definition is
addressed by that digest, and every execution records both the pinned version
and the definition digest.

Artifacts are references, not bytes. The Orker materializes and uploads them
through `pkg/artifacts`. The production artifact backend is shared object
storage (S3-compatible storage); local filesystem storage is for local use.
Production infrastructure should consist primarily of Temporal, S3-compatible
object storage, OrkStack services, and Orkers. Additional durable systems are
not OrkStack requirements.

### 2.3 OCI owns computation

The OCI image is the computation ABI. An activity is not a Python, Go, or Java
plugin, a Temporal Activity SDK integration, an OrkStack SDK integration, or a
custom worker service. Any OCI-compatible image should be executable. An image
may know about OrkStack but must not need to know about it.

The Orker bridges Temporal activities and OCI execution. Activity containers
must not receive Temporal credentials or APIs, object-storage credentials, or
OrkStack control-plane credentials unless a user explicitly supplies them as
workload secrets. The Orker is the security boundary.

## 3. OCI Activity Contract

Treat the OCI Activity Contract as a versioned ABI. Do not communicate required
platform state through environment variables. The stable `idempotencyKey` must
remain unchanged across retries of one logical activity.

### 3.1 Files, values, and artifacts

The activity receives its inputs through files and writes its results to files.
The completion protocol validates `values.json` as the compact structured result.
Artifacts are regular files only. Reject directories, symlinks, devices,
sockets, and FIFOs. A directory hierarchy must be archived by the workload,
for example as `workspace.tar.gz`; the artifact layer has no directory
semantics. Artifacts remain opaque.

Structured runtime data — `values.json`, a run's submitted input, and its
recorded output — is always JSON, never YAML: it is the one format the
Temporal payload, the Connect API, `orkctl --json` and the UI's typed input
editor share (`docs/JSON.md`), so a copied CLI command reproduces exactly what
was submitted, and it has none of YAML's silent coercions (`no`, `on`,
octal-looking numbers). Documents a person authors — an Orkflow, an automation
document — stay YAML. Do not add a YAML input mode to the UI or CLI.

### 3.2 Exit status and logs

Exit code `0` means computation succeeded; the Orker then validates and commits
outputs. Any non-zero exit means the activity failed. Retry behavior belongs to
Temporal workflow policy, not magic exit codes such as 42, 43, or 44.

stdout and stderr are independent logs. Capture them independently, stream them
when possible, retain them in object storage, and associate them with the
activity attempt. Plain text must work without a logging SDK. Structured JSON
may be enhanced later, but is not part of the required ABI.

### 3.3 Cancellation and heartbeats

Temporal cancellation must reach the container:

```text
Temporal cancellation → Orker receives it → SIGTERM
                     → wait 30 seconds → SIGKILL if still alive
```

The 30-second cancellation grace period is the default. Ordinary Unix signal
handling must be enough for cleanup; no custom cancellation API is required.

The Orker should heartbeat the Temporal Activity while the container exists.
The container must not implement Temporal heartbeats or a heartbeat IPC
protocol. Application hangs are handled by the activity timeout. A structured
progress mechanism is a possible later extension, not a current requirement.

## 4. Durability, retries, and completion commits

Activities are **at-least-once**. Never claim exactly-once execution for
arbitrary external side effects. The stable `idempotencyKey` lets external
systems implement idempotency when they need it.

The suggested default retry policy is:

```text
maxAttempts:        3
initialInterval:    1s
backoffCoefficient: 2.0
maxInterval:        1m
```

Activities may override the policy; in the DSL a task without `retry:` makes
exactly one attempt. Keep these failure categories distinct:

```text
ActivityFailed
InfrastructureError
ContractViolation
TimedOut
Cancelled
```

Suggested defaults are:

```text
ActivityFailed       retry according to policy
InfrastructureError  retry
ContractViolation    do not retry
TimedOut             retry according to policy
Cancelled            never retry automatically
```

These are policy defaults, not exactly-once guarantees. An activity
implementation must not silently collapse the categories. As implemented, a
container that exceeds its own `timeout:` (`TimedOut`, `CONTAINER_TIMEOUT`) is
terminal rather than retried, because it would time out again on the same
wall clock; a heartbeat timeout (a lost Orker) stays retryable under the
task's policy (`docs/OCI_ACTIVITY_CONTRACT.md` §8.1).

### 4.1 Durable output commit

The Orker must use an object-storage commit protocol for activity completion:

```text
container succeeds → upload outputs → Orker crashes
                   → Temporal does not receive completion
                   → Temporal retries the activity
```

Successful completion conceptually does this, in order:

1. validate `values.json`;
2. calculate artifact hashes;
3. upload artifacts;
4. upload `values.json`;
5. write an immutable `commit.json` completion manifest;
6. report Activity success to Temporal.

Anything the engine itself records as a task's result (an await's resolution
values, a composed task's values) is written to object storage before the
history record that references it, the same order as a commit.

Use a deterministic commit key. At retry start, if a valid `commit.json` exists,
return its committed result and do not execute the container again. This only
prevents duplicate container execution after OrkStack has durably committed the
result; it does not make arbitrary external side effects exactly once.

A cached task's memo is advisory. A hit writes this run's own `commit.json`
referencing the source commit's content-addressed objects and starts no
container; every doubt about a memo (unreadable, mismatched, missing object,
timeout) is a miss that runs the task; the memo step fails an attempt only
when its own commit write fails. Deleting any memo changes no result.
Caching is the author's assertion that a task is safe to reuse; OrkStack
cannot verify it.

An accepted execution must not disappear. A durably committed activity result
must not become pending again. Object-storage failures must not silently produce
success. Every terminal failure must have a machine-readable reason and a
human-readable explanation.

## 5. Image resolution and Orker runtime boundary

Image tags may be accepted as input, but the Orker must resolve each image to a
digest before execution and record the immutable reference, for example:

```text
ghcr.io/acme/migrate@sha256:...
```

A running execution must never silently switch images because a tag moved.

The deployment's image policy (`orkctl admin image-policy`: `require_digest`,
`allowed_registries`, cosign public `signing_keys`) is a floor; a namespace may
only tighten it (`require_signed_by`), never loosen it. It is enforced at
apply, at submit, and by the Orker at dispatch against the policy selection
the run pinned at submit, so every attempt of a task is judged by one rule.
Only `cosign:` exists; it is a closed set, not an extension point (§13).

The Orker should be stateless and disposable. Persistent local state is limited
to an OCI image cache, ephemeral execution directories, and optional transient
download cache. If an Orker disappears, Temporal must be able to retry elsewhere.
A pin's shared directory is an ephemeral execution directory whose lifetime is
the pin's, bounded by a lease and reaped at Orker boot. A pinned task is the one
deliberate exception to retry-elsewhere: if its host disappears, the pin and its
run fail explicitly (`PIN_HOST_LOST`), and they are never re-opened on another
Orker, because the shared directory would not be there. Retrying elsewhere with
an empty `/orkstack/shared` would be a silently wrong result, which §14 ranks
below explicit failure.

The Orker is responsible for:

```text
poll Temporal
resolve the activity specification
check the completion commit
check the run's image policy
check a cached task's memo
resolve the OCI image and digest
create an isolated workspace
materialize input artifacts
resolve secret files
create and mount `/orkstack` workspace
capture stdout/stderr
heartbeat Temporal
handle cancellation
wait for the process
validate outputs
upload artifacts
write the completion manifest
return a compact ActivityResult
cleanup
```

Do not spread Docker-specific details through unrelated OrkStack components.
Keep the OCI runtime boundary clean enough for future Podman, containerd,
Kubernetes Jobs, ECS Tasks, or Nomad implementations. Do not implement all of
those runtimes now; portability is a boundary, not a mandate for a framework.

## 6. Security and resource controls

Executing arbitrary production activities is dangerous. Do not permit:

```text
--privileged
host PID namespace
host network
arbitrary host path mounts
Docker-socket mounts
Orker-credential mounts
```

Prefer `no-new-privileges`, default seccomp, resource limits, isolated working
directories, and rootless execution when supported. Initially support only
network modes `default` and `none`. Leave named, Orker-defined network profiles
for later; an Orkflow must not define arbitrary host-network configuration.

Activities may request resources such as:

```yaml
resources:
  cpu: "2"
  memory: 4Gi
```

Support activity-level `timeout` and `cancellationGracePeriod`. Orker policy
may cap requested resources. Workflows must not receive unlimited resources by
default. Put complicated execution logic in a container instead of adding
platform primitives.

Secrets come from secret references or explicitly supplied workload secrets,
never committed source. `.env` is gitignored. Shell commands must not
interpolate untrusted input without the templating layer's escaping. An `env:`
value is stored in the definition, so it is configuration and never a secret;
no `env:` value grants a credential, privilege or host access. Which
secret sources a task may name is bounded by its namespace's
`allowed_secrets` (checked at apply and again at submit) intersected with each
Orker's `ORK_ORKER_SECRET_ALLOW` floor (checked at dispatch); both bound names
and never store or compare a value. A bearer credential OrkStack mints for a
caller (a trigger token, an await callback token) is disclosed once and stored
only as a salted hash.

## 7. Orkflow execution model

Compile each Orkflow into an immutable internal representation and execute it
through one generic deterministic Temporal Workflow interpreter. Do not generate
bespoke Temporal source code for each Orkflow. The interpreter resolves
dependencies, schedules activities, waits on Temporal timers and Signals,
applies retry policies, handles cancellation, and records compact outputs.

A run's start payloads — its input, its planned definition and its plan — are
each a §2.2 payload and are refused at submit above 256 KiB. That is a bound on
a payload, not on an Orkflow: a limit on definition size beyond it is a
proposal, not a current hard invariant, and is not to be turned into one
without a separate accepted contract.

One definition limit is accepted (2026-10-11): an Orkflow declares at most
**1000 tasks**. It is a deployment-configurable default, not a fixed
invariant — `engine.orkflow_max_tasks` (`ORK_ORKFLOW_MAX_TASKS`) lowers or
raises it — refused `INVALID_ARGUMENT` at validate, apply and plan, naming the
count, the limit and the setting, and re-checked at submit
(`FAILED_PRECONDITION`). Every entry of `tasks:` counts once; a fan-out's
items (bounded by `max_items`) and a composed child's tasks (a separate run)
do not. It exists because a run's
tasks share one Temporal history; it may be raised once runs can
continue-as-new (`docs/DSL.md`, "How many tasks an Orkflow may declare").

Tasks form a DAG whose only edges are explicit `needs`. Declaration order
never sequences anything: a task becomes ready once every need has ended and
its join policy admits it, and a task with no `needs` (absent or `[]`) is
ready at run start, so independent tasks run in parallel. To order two tasks,
write `needs`. Detect missing references and dependency cycles during
validation; do not discover malformed DAGs during runtime.

The repository vocabulary is deliberate. The user-facing words below are the
ones the DSL, the proto, `orkctl`, the UI and every message use; a word marked
internal never appears on any of those surfaces.

- **Orkflow** means the user-authored YAML entity, its name, version, and
  compiled definition.
- **Temporal workflow** means Temporal's workflow context, history, workflow
  type, workflow ID, or workflow task.
- **Pool** means the user-facing set of Orkers.
- **Queue** means only an internal Temporal task queue and never a user-facing
  OrkStack concept.
- **Pin** means a named group of tasks of one Orkflow that execute on one Orker
  process and share `/orkstack/shared`.
- **Session** means Temporal's Worker Session, the mechanism a pin is
  implemented with. It is internal, exactly as **queue** is internal to
  **pool** (§7.1): no user-facing surface names a session, a session id, or a
  session task queue.
- **Generation** means one Temporal execution in a run's continue-as-new
  chain (§7.3). It is internal exactly as **session** and **queue** are: no
  user-facing surface — the API, `orkctl`, the UI, or an error message —
  names a generation, its execution, a checkpoint, a carry, or how many a run
  had; engine logs, metrics and `docs/DEPLOY.md` are operator surfaces and
  may. A run is one run, with one id, one event stream and one archive.
  User-facing text says a run "carries on" or is "carried forward", and calls
  its checkpoint its "saved state" (`RUN_STATE_*`); `RUN_HISTORY_LIMIT`
  speaks of the run's history as a whole.
- **Pin host** means an Orker configured to accept pins
  (`ORK_ORKER_MAX_PINS` above zero). Every Orker still runs ordinary tasks;
  only a pin host runs pinned ones.
- **Fan-out** means a task declaring `each:`: one DAG node that runs one
  **item** per element of an array. An item is identified by its 0-based
  index, never a string; `convert[3]` is a rendering only and nothing parses
  it.
- **Composed task** means a task whose `uses:` is `orkflow:<name>@<version>`.
  It starts a **child run** of that Orkflow and waits for it; the run that
  started it is the **parent run**. **Outputs** are an Orkflow's declared
  return contract (`outputs:`).
- **Awaited task** means a task declaring `await:`: it runs no container and
  waits until it is **resolved** from outside the run (completed with values
  or rejected with a reason) or its deadline passes. A **callback token** is
  the one-time bearer credential an `await:` with `callback: true` is issued
  at submit.
- **Cached task** means a task declaring `cache:` with `enabled: true`. A
  **memo** is one stored pointer from a memo key to a `SUCCEEDED` commit
  (§4.1); a memo step's outcome is a **hit**, **miss** or **bypassed**.
- **Join policy** means a task's `join:`: when it may start, decided from how
  its needs ended. One of `all_succeeded` (the default), `none_failed`,
  `any_failed`, `all_done`.
- **Failure handler** means a task whose join policy is `any_failed` or
  `all_done`. It is the only kind of task that starts in a failing run, and it
  never rescues the run: a run whose failure handlers ran still ends `FAILED`
  with its original reason.
- **Schedule** means an object bound to one Orkflow that starts runs on a
  schedule spec; a **fire** is one occasion it acted (on time, by backfill, or
  by hand). **Trigger** is the inbound plain-HTTP run starter, a different
  object; the manual form of a schedule is "fire", never "trigger".
- **Origin** means the immutable record of what started a run: a kind
  (`submit`, `trigger`, `schedule`, `parent-run`, `run-again`), the
  originating object's id, a caller-supplied reference and the principal.
- **Principal** means the `Ork-Principal` request header a deployment's
  gateway asserted, recorded verbatim on audit records and origins. It is not
  authenticated, and no surface may present it as a verified identity: every
  rendering says it was asserted.
- **Automation document** means one authored YAML file
  (`kind: Automation`) declaring a namespace's settings, schedules, triggers
  and webhooks by name (`docs/AUTOMATION.md`). Its plan
  (`apply --dry-run`) is unrelated to `orkctl run plan`, which simulates one
  run's DAG.
- **Run retention** means how long a finished run of one OrkStack namespace
  stays readable before it is retired, counted from when the run FINISHED,
  never from submission. It is a field of `orkstack.v1.Namespace`
  (`run_retention`), unset meaning inherit.
- **Maximum run retention** means the deployment-wide ceiling every
  namespace's run retention is bound by, and what a namespace that sets none
  inherits (`orkstack.v1.RetentionPolicy.max_run_retention`).
- **Hold** means a namespace closed to new runs, with the operator's reason
  and the instant the registry recorded it (`orkstack.v1.Namespace.hold`). It
  is an admission decision, never a retroactive one: a run already accepted
  finishes untouched, and releasing a hold starts nothing that was refused.
  Its sibling `max_running` bounds a namespace's runs in flight, counted from
  the cluster's execution index at submit.
- **Budget** means a namespace's bound on reserved cpu-seconds and
  GiB-seconds over a rolling period, soft (reported only) or enforced (the
  third check of the admission gate, after hold and `max_running`). It is a
  field of `orkstack.v1.Namespace` (`budget`), unset meaning unbounded.
- **Usage** means reservation × held time — the cpu and memory an Orker
  granted each attempt, multiplied by how long it held it — derived on read
  (`StatsService.GetUsage`), never measured utilization and never a stored
  counter.
- **Telemetry sink** means an operator-declared OTLP destination, named, in
  `ORK_TELEMETRY_SINKS`. `default` is reserved: it means the standard `OTEL_*`
  environment, not an entry in that YAML. As implemented, a table that
  declares a `default:` entry overrides the `OTEL_*` environment for the
  default destination; a table without one leaves the environment in charge.
- **Telemetry policy** means one namespace's selection among a deployment's
  telemetry sinks: which sink, whether task logs export, which attributes.
  It is a field of `orkstack.v1.Namespace` (`telemetry`), unset meaning
  inherit — the same shape `run_retention` already uses.
- **Release** means one build of OrkStack — `ork` and `orker` built and
  deployed together — identified by `ORK_BUILD_ID` within the deployment named
  by `ORK_DEPLOYMENT_NAME` (§7.2). The Temporal Worker Deployment Version it
  is implemented with is internal, exactly as a session is internal to a pin.
  (Unrelated to `orkctl namespace update --release-hold`, where "release" is
  a verb on a hold.) A release is **current** when it takes every new run not
  sent to a ramping release, **ramping** when it takes a stated percentage of
  new runs, **previous** when it took new runs once and takes none now, and a
  **candidate** when its processes have registered and it has never taken new
  runs. It is **drained** when the cluster reports that no run executing on
  it remains and no Orker of it hosts an open pin; a drained release's
  processes may be stopped.
- **Cordon** means a process-local flag on one Orker that stops it accepting
  new tasks and opening new pins while leaving everything in flight alone. It
  is delivered over the Orker's own control channel and is lost when the
  process exits.
- **Drain** means cordoning an Orker and waiting until it reports no running
  task and no open pin. It is a client-side wait over the inventory read
  (`orkctl orker drain`); nothing server-side holds drain state.
- **Control channel** and **presence channel** are internal, exactly as
  **queue** is: the Temporal task queue `ork-orker-<orker_id>` that one Orker
  polls for `DescribeOrker` and `CordonOrker`, and `ork-orkers`, which every
  Orker polls and nothing dispatches to, whose poller list is the fleet.

The Temporal mechanisms behind these words are internal, exactly as a queue is
internal to a pool: no CLI string, UI string, HTTP body, client-rendered proto
comment or error message names a task queue, session, Worker Deployment
Version, Signal, Timer, child workflow, parent-close policy, local activity,
marker, Temporal memo, search attribute, visibility query, Temporal Schedule id or
workflow id. (A proto comment on an engine-internal message may say what
implements it.)

An Orkflow name is unique within a namespace. Names are validated, never
sanitized, by the same rule at each boundary:
`^[a-z][a-z0-9-]*$`, at most 200 bytes, for namespaces, Orkflow names, task IDs,
and pools. The built-in `default` namespace always resolves, is never stored,
and cannot be deleted. Archive indexes are namespace-first.

Creating `default` writes nothing: an identical namespace specification is a
no-op, a different specification is a conflict, and deleting `default` is
always refused. A request with no namespace uses `default` on the wire.

Runs use IDs shaped `ork-{timestamp}-{sequence}`. The sequence is
`nonce<<32 | counter`, with one 32-bit cryptographic nonce per process and a
per-process counter. It is monotonic only within one replica. A run ID is the
single-use Temporal workflow ID; starting a duplicate must fail, never attach
to another run. Nothing may parse a run ID or derive meaning from its parts.
Order runs by `created_at`; compare IDs only as the documented tie-break on the
ListRuns read path.

Submission is the last place a run's routing and policy may be decided:
workflow code is deterministic and may not read a namespace document,
process configuration, or the cluster. `SubmitRun`'s preparation resolves the
namespace and Orkflow, applies the admission gate (hold, then `max_running`,
then an enforced budget), resolves each task's pool against the namespace's
allow-list and checks a live, capable Orker serves it, and then decides the
image-policy selection, telemetry selection, cache plan, every composed
child's plan, the origin and any await callback tokens. All of it rides the
replay-pinned `RunPlan` and is never re-read. Every refusal happens before a
run ID is minted or an idempotency key claimed, so a refused submission leaves
nothing behind. Admission is never retroactive. A guard that reads the
cluster's index or usage (`max_running`, an enforced budget) is a lock-free
read-then-act and therefore best-effort by a bounded overshoot; a lock would
be the distributed lock service §2.1 forbids.

Temporal is the read model for live runs — for a run that has continued as
new (§7.3), for its current generation. The events of its earlier generations
are read from the immutable event segments the run wrote at each
continuation, byte-for-byte the events Temporal recorded and verified by
SHA-256, and its state at a continuation from the immutable checkpoint the
next generation started from. Neither is execution state: Temporal decided
everything they hold, and they only carry it across the generation boundary
that Temporal's own retention may outlive. A segment found missing or altered
when a run is archived is recorded in the archive, and in any evidence bundle
exported from it, as an explicit gap, never dropped silently. A run's
executions are one run in every list and count: a question about which runs
are live never loses a run while one of its executions closes and the next one
starts. Finished runs past retention come from
the shared run archive. `RunStore` and `EventBus` are write-side helpers only:
they support candidate submission, archive/webhook writes, and the stream for a
run whose start failed. Never answer a read from process memory behind a wired
reader. Any replica must answer for a run it did not start and for a run whose
original replica is gone. Aggregates (stats, failures, flakes, usage, cache
hit rates) are derived on read from Temporal and the records already in object
storage; there is no stored counter or index database. Audit, delivery and
attempt records are immutable objects written once; they are records, never a
read path for a decision.

RunWorkflow assigns event sequence numbers. The bus honors supplied numbers and
assigns one only when none is supplied. Temporal history, the in-process log,
and archived event JSON must therefore remain identical, and clients can resume
by sequence from another replica.

### 7.1 Pools and Temporal task queues

`pool:` is the only user-facing selector of *which Orkers*; `pin:` constrains a
group of tasks to *one* member of that set. Neither exposes a queue, a session,
or a host name. No user-facing surface may expose `queue:` or a queue name.
`internal/task/pool.go` builds every channel name and is the complete mapping:
`QueueForPool`, `RouteForPool`, `DefaultRoute`, `PoolForQueue`, the single
`ork-pool-` prefix construction, and the Orker control (`ork-orker-<id>`) and
presence (`ork-orkers`) channels.

An empty pool and `main` both mean the deployment's configured default pool.
Other valid pools resolve to `ork-pool-<slug>`. The reserved
`ork-engine-events` channel and the Orker control and presence channels cannot
be a bound default. A task's pool must be resolved inside the engine; Temporal
queue names must not leak into the user contract.

The deployment's engine workflows use fixed internal channels: `RunWorkflow`
(including every child run) uses `ork-tasks` and engine events use
`ork-engine-events`, regardless of the `ORK_ORKER_QUEUE` override used for the
default activity pool.

The same rule extends to retention: the underlying cluster's own workflow
execution retention TTL is likewise internal, never a user-facing setting. It
is derived automatically from the maximum run retention plus an enforcement
margin (`archive.RetentionController`) and is never read from, or written to,
by a client. What a client sets is run retention and maximum run retention —
OrkStack's own vocabulary — never anything named "Temporal" on the wire, in
CLI help, or in the UI.

### 7.2 Versioning

An Orkflow version is immutable. Applying an edit creates a new version; runs
pin the version and its definition digest. A run continues on the definition it
started with even if the current pointer moves. The current pointer controls
new runs only. Moving the pointer across a breaking change to the declared
interface (`inputs:` and `outputs:`) is refused unless that promotion is
explicitly acknowledged (`--acknowledge-breaking`).

One Worker Deployment spans `ork` and `orker` for a release. They share
`ORK_DEPLOYMENT_NAME` and `ORK_BUILD_ID` and are released together. With
versioning enabled, `RunWorkflow` and `ScheduleFireWorkflow` are Pinned,
`IdempotencyClaimWorkflow` is AutoUpgrade, and the projection worker on
`ork-engine-events` is unversioned. A child run started by a Pinned parent
inherits the parent's version, so its plan is interpreted by the release that
built it. Versioning is off unless `ORK_BUILD_ID` is set; leave
`DefaultVersioningBehavior` unset when it is off. `orkctl release` (status,
ramp, promote, rollback, wait) is the supported rollout path, and it refuses a
ramp or promotion the target release's engines and Orkers could not carry. A
version must drain before its workers are decommissioned. A pinned workflow
uses its own activity-worker version only while that version has workers
polling its activity channel; otherwise Temporal falls through to that
channel's current version. A task pin (§7.1) is the one exception to that
rule, and it is stricter rather than looser: its session is opened through a
version-routed activity, but its member activities ride the session's own
per-host channel, which is unversioned and polled by exactly one process — so
a pin stays on the process that opened it across a rollout and never falls
through to the current version
(`internal/engine/versioning_pin_integration_test.go`, `docs/DEPLOY.md`).

Old histories must replay against new code; `internal/engine/replay_test.go`
and the recorded histories in `internal/engine/testdata/history/` protect this
contract. A new interpreter path must be entered only by a definition that
declares the new feature, which no older history holds, so every recorded
history replays unchanged without a version marker; each new command path gets
its own recorded history fixture. Because the activity channel can fall
through to another release, an activity-input field an older Orker would
reject or ignore (cache, join, fan-out items) is advertised as a capability on
`DescribeOrker`. Submit asks one Orker of each pool such a task would run on
and refuses with `FailedPrecondition`, naming the Orker and its build, when it
lacks the capability; a pool that does not answer the probe is logged, not
refused, so every Orker of one pool must run one release.

## 8. Workflow-native primitives

Use Temporal directly for orchestration:

```text
wait / sleep
Signal wait
manual approval
parallel dependencies
retry
timeout
cancellation
```

Do not start a container merely to sleep, wait for approval, or wait until a
future time. Do not add a custom scheduler, event bus, task queue, state
machine, or expression runtime for these operations. An awaited task
(`await:`) is the DSL form of a Signal wait, manual approval and external
completion: a Temporal Timer for its deadline and a Signal for its resolution,
and no container. A schedule is a Temporal Schedule; a composed task is a
Temporal child workflow of the same `RunWorkflow` interpreter; a fan-out's
items are ordinary activities in the interpreter's own in-flight set.

## 9. First-party activities

First-party activities (`images/`: `http`, `shell`, `file`, `json`, `git`,
`template`) must use exactly the same OCI contract as third-party activities.
Do not create a privileged internal plugin system. They should be reference
implementations of the public OCI contract.

The file activity remains intentionally small and Unix-like. Its basic
operations may include `copy`, `checksum`, `gzip`, `gunzip`, `tar`, `untar`, and
`concat`. Do not turn it into a general file-processing framework.

## 10. Developer experience and contract testing

Local development should be excellent. One command starts the whole local
stack (Temporal, MinIO, a registry, engine, Orker, and UI, health-checked):

```bash
make up
```

OrkStack itself must not require an additional external database. Developers
must be able to test an OCI activity without running a complete Orkflow:

```bash
orkctl activity test my-image
```

It runs the image through the same dispatcher an Orker uses and checks
container startup, input filesystem and permissions, valid context, valid
output, artifact-name rules, output-size limits, SIGTERM, exit-code handling,
and activity timeout. Keep the Bash, Go, and Python fixtures under
`test/fixtures/`; the Bash fixture is mandatory. If Bash needs an SDK, the
architecture has failed.

`orkctl task run` runs one task of an Orkflow locally through the same
dispatcher, renderer and workspace, against a run's recorded bytes or local
files. It writes nothing to the deployment; a local run is a new logical
activity with its own idempotency key, and its external side effects are
real.

## 11. Reliability, observability, and evidence

Reliability is a product property. Required integration and chaos coverage
includes:

- Orker termination while a container runs, after it exits, during artifact
  upload, and after `commit.json` but before Temporal completion;
- Orker restart, temporary object-storage failure, and temporary Temporal
  failure;
- exit `1`, exit `0` without `values.json`, malformed JSON, output-size excess,
  timeout, cancellation, ignored SIGTERM, unavailable input artifact, failed
  image pull, and a tag changing between executions;
- every task of a pin landing on ONE Orker while three real pin hosts poll the
  same pool, and no failover when that host is killed mid-container: the run
  fails `PIN_HOST_LOST`, and the survivors' own execution records and work
  roots show they were never handed the lost pin's tasks;
- a pin host lost in the gap BETWEEN two members: the member that already
  committed stays `SUCCEEDED` with its commit, values ref and artifact bytes
  still in the store, and only the member that could not continue fails;
- a pinned attempt that wrote `commit.json` and then died: the retry adopts
  that commit instead of running the container again, and a pinned retry after
  a non-committing failure still sees the `/orkstack/shared` its failed
  predecessor left;
- a run terminated from outside the workflow while a pin is open: no workflow
  code runs, so the pin's directory survives and the lease sweep is the only
  collector — and only once that lease has expired;
- every pin host in the pool saturated: the triggering task fails
  `PIN_UNAVAILABLE` after the full creation timeout rather than immediately,
  no pin directory is created for it, and the run already holding the slot is
  undisturbed;
- a Worker Deployment Version rollout while a task pin is open: the pin's
  remaining members must stay on the process that opened it, never move to the
  new version's Orker, and the run's unpinned tasks must keep their own
  version's routing (§7.2);
- a failing run runs its failure handlers to their own outcomes and ends
  `FAILED` with the original reason; an operator cancel starts none and stops
  running ones; an Orker killed under a running handler retries it per its
  own policy.

Every execution must eventually reach either a correct durable state or an
explicit actionable failure. There is no documented third state such as stuck,
half-completed, unknown, or needing database surgery. Do not call OrkStack
production-ready until required failure and recovery scenarios are proven.

OpenTelemetry (`internal/telemetry`) is the only telemetry API in the
codebase — no per-vendor code anywhere else. Exporters are selected by the
standard `OTEL_*` environment (or the config file's `telemetry:` block), and
everything is off by default: a deployment that sets none of it exports
nothing and opens no admin listener.
Telemetry is best-effort and non-blocking — an exporter that cannot reach its
collector is logged and counted, never retried in line, and a record that
cannot be queued is dropped; telemetry can never fail a task or a run.
Operational endpoints (`/healthz`, `/readyz`, `/metrics`, `/debug/pprof/`)
live on `ORK_ADMIN_ADDR`, never the API port. Workflow code never imports
OpenTelemetry — instrumentation is a side effect, and `RunWorkflow` must stay
a pure function of its history (§2). Activity authors need no observability
SDK; activity logs remain stdout/stderr.

### 11.1 Development workflow

Every code change follows this order:

1. For a process-boundary change, edit `proto/` first and run `make proto`.
2. For a new configuration setting: add it to `internal/config`'s `Schema`
   (its default bound to the code constant, `internal/config/config.go`),
   `docs/DEPLOY.md` §1's table, and regenerate
   `deploy/orkstack.defaults.yaml` with `make config-defaults`; the
   completeness tests refuse a setting that is missing from any of the
   three.
3. Write a behavior test that fails for the right reason.
4. Implement until it passes.
5. Run `make lint test`; for Temporal or storage changes, run the affected
   integration tests against `make infra-up`
   (`scripts/integration.sh ./internal/engine -run '^TestIntegration_X$'`, or
   `make test-integration` for all of them).
6. Produce the change's evidence with
   `make proof-feature TESTS='PKG:REGEX …' SPECS='FILE …'`, naming the tests
   and specs that prove it, and quote it as a feature proof (§11.2).
7. Commit with a Conventional Commit.
8. Merge through §11.3. The full proof of the merged tree is what makes the
   change done.

Unit tests are pure and table-driven. Temporal workflow tests use
`WorkflowTestSuite` / `TestWorkflowEnvironment` with mocked activities.
Integration tests use `-tags=integration`, `make infra-up`, and `infraAbsent`; a
missing prerequisite skips, and becomes a failure under `ORK_REQUIRE_INFRA=1`
(`make test-integration` sets it). Browser end-to-end tests live in `ui/e2e`
(Playwright, `cd ui && bun run test`) and drive the built UI against a real
engine, Orker, Temporal, and MinIO; the activity fixtures live in
`test/fixtures` and `test/tools` holds helpers those suites build. Do not use
sleeps for synchronization; use deadlines or test helpers. A skip is never a
pass: a harness must probe its prerequisites itself and fail, not skip, once
they are present.

Some suites have an activity container call a stub on the host at
`host.docker.internal`; a Linux host with plain Docker Engine needs one-time
host setup for that name (README, "Linux hosts"). It is host setup, never
Orker behavior: the Orker must not add `--add-host` or any host-network
configuration to activity containers (§6). The observability gate's
Prometheus reaches the admin listeners over that same default bridge:
`scripts/proof-observability.sh` attaches its own Prometheus container to the
bridge and binds that run's admin listeners to the bridge's gateway, for the
gate only. That is proof and development setup, never a `deploy/` default or a
production listener (README, "Linux hosts", says what it exposes).

Integration tests run concurrently: as separate processes within a proof, and
beside other agents' proofs on the same host. Each integration test owns these
resources and never shares them: its Temporal namespace
(`ORK_TEMPORAL_NAMESPACE` is a prefix it extends with its process and test);
its object-storage bucket (`internal/testsupport/s3bucket` in every Go
integration package), so a bucket-wide setting such as a lifecycle rule is its
own; its listening ports (allocated free at run time, never fixed); its Orker
work root (a fresh directory under `ORK_ORKER_WORKDIR` or `.data/work`,
named with its pid by `internal/testsupport/proof.WorkRootPattern`, never that
directory itself) and its Orker identity
(`internal/testsupport/proof.OrkerID`, or the default one derived from its own
work root, because an identity is host-wide: Boot locks it and reaps by it);
the binaries it runs (`internal/testsupport/testbin`: `ORK_TEST_BIN_DIR`, or a
directory it built into itself, never `bin/`); and the image tags it pushes
(only tags carrying its own process token). A test builds a binary only
through `testbin.Path` with a literal package path, which is how a feature
proof knows which packages a suite exercises. The first-party activity images
are pushed once per proof under `ORK_TEST_IMAGE_TAG`; tests and specs only
resolve that tag. A test never asserts on host-wide state: it counts
containers by `label=ork.orker=` or `label=ork.run=`, never with an unfiltered
`docker ps`. What cannot be owned is shared, and every test must tolerate it:
the Docker daemon, CPU and memory, the MinIO and registry servers (isolated by
bucket and tag), and the host's network interfaces.

Playwright specs are not yet isolated that far. A proof runs spec files one at
a time (`PROOF_UI_WORKERS=1`, after the integration phase), each spec still
listens on fixed ports, and a spec that does not create a bucket of its own
uses the development bucket, whose deployment-wide documents (retention, image
policy, audit) every such spec shares. Chromium shares the host's network
namespace, so a container started or stopped anywhere on the host aborts its
loads (`ui/e2e/test.ts`). Until each spec allocates its own ports and bucket
and the browser runs in a network namespace of its own, at most one proof runs
Playwright specs on a host at a time: the Playwright gate takes a host-wide
lock, so a feature proof's specs wait for a full proof's, and never run beside
them. Specs run by hand (`cd ui && bun run test`) take no lock; do not run them
while a proof's Playwright gate runs. The observability gate is the other
exception: its Prometheus scrape targets are fixed ports, so it runs under a
host-wide lock of its own. A test that proves something did NOT happen within a
window also observes, in that window, that the system was making progress, so
a slow host cannot make it pass.

Run integration suites through `scripts/integration.sh`, which
`make test-integration` and both proof tiers use. It compiles each package's
test binary once, runs its tests as separate processes in the package's
directory, `PROOF_JOBS` at a time (`PROOF_JOBS=1` runs them one after
another), and writes a ledger that is checked as §11.2 describes. By hand,
`go test -tags=integration -p 1 ./internal/engine -run '^TestX$'` still works
against `make infra-up`; set a unique `ORK_TEMPORAL_NAMESPACE` whenever
anything else may be running, and the same namespace and queues only for a
deliberate two-replica test. Set `ORK_ORKER_QUEUE` consistently on both
binaries when a private task channel is needed.

Stop only what you started, by its PID. Never kill by name or pattern:
`/bin/ork$` and `/bin/orker$` match every worktree's dev stack and every
running proof on the host. A proof runs in its own session and process group,
with its own binaries under `proof/<name>/bin/` and its own Temporal container;
`scripts/proof-stop.sh proof/<name>` (`make proof-stop PROOF=proof/<name>`)
stops exactly those. `make down` stops this worktree's dev stack: the process
groups `make up` recorded, and any process whose executable is this
worktree's `bin/ork` or `bin/orker`, identified by `/proc/<pid>/exe`, never by
its name. What failed
tests keep for their post-mortem (buckets, work roots) is listed, and removed
on request, by `scripts/proof-clean.sh`, which removes one only once the
process its name records is gone.

### 11.2 Evidence

No proof, no done. A change is finished when its behavior has been observed,
not when its code reads right. A change is **done** when it is on `main` and a
full proof passed on the exact tree `main` then points at (§11.3).
`scripts/proof.sh` produces evidence in one of two tiers. Every proof names
its tier in its directory name, the first line of its README, its result line
and `proof.json`. `proof.json` also records the commit; the tree hash of the
working tree, staged and untracked files included; whether that tree was
clean, and identical, when the proof started and when it ended; and whether
the proof is merge-eligible, with each reason it is not.

**Feature proof** (`make proof-feature`;
`scripts/proof.sh --tier feature [--base REF] [--tests PKG:REGEX]... [--specs FILE]...`)
is the evidence for a change on its branch. It supports the claim "ready for
the merge proof", never "done". Its directory is `proof/<name>-feature/`. Its
change set is the diff from the merge base with `--base` (default `main`) to
the working tree, untracked files included. Every changed path must match a
rule in `scripts/proof-scope.tsv`. A path that no rule matches, or whose rule
is `full` (`go.mod`, `go.sum`, the Makefiles, `buf.yaml`, `buf.gen.yaml`,
`.github/`, `deploy/` other than the generated defaults, the example
configuration and the observability profile, `scripts/` other than the
observability gate and the two measurement files, `internal/testsupport/`, and
`test/tools/proofrun/`), refuses the feature tier. A feature proof always runs
lint, build, the unit suites and `go vet -tags=integration`, and it runs the UI
type-check and unit suites when `ui/` or `proto/` changed or a spec runs. It
then runs:

- the change's **evidence**: every test whose source the diff changes,
  including every test that reaches a changed or deleted helper, type,
  variable or `TestMain` through its package's declarations (comment-only
  changes are no change); every test `--tests` names, where `REGEX` must match
  whole test names; every spec the diff changes or `--specs` names (a file
  under `ui/test/` names a UI unit test); every spec when the Playwright
  harness, its config or the UI's dependencies changed; the activity contract
  fixture when `images/` or `test/fixtures/` changed; the tests that read a
  changed example, and the examples spec, when `examples/` changed; the tests
  and specs that run a changed `test/tools/` tool, found by searching their
  sources; and the observability gate when a telemetry path changed;
- the **regression set**: in every integration package whose test binary
  depends on a changed package — through its imports, or through a binary its
  suite builds (an `images/` change counts as a change to `internal/orker`) —
  every test declared in an integration-tagged file, except those on the slow
  list (`scripts/proof-slow-tests.txt`, regenerated from a full proof's ledger
  by `make proof-slow-list FROM=proof/<name>`).

The regression set is never evidence. Every changed package with a change
outside its tests and its comments needs at least one evidence test whose
binary depends on it, and that test must be an integration test when
`scripts/proof-scope.tsv` marks the package as Temporal- or storage-facing. A
changed file under `ui/src/` needs an evidence spec, except that a change only
to `ui/src/lib/**/*.ts` may be proven by a changed or named UI unit test.
Missing evidence, a `--tests` pattern that matches no test, and a `--specs`
file that does not exist are invalid invocations. A feature proof passes only
if every evidence test and spec ran and passed; a skip, even an allowed one,
is not evidence. The README opens with
`SCOPED FEATURE PROOF — NOT MERGE EVIDENCE`. It lists the evidence that ran
for each changed package and path, and it names every gate, package, slow test
and spec it did not run, each with the reason. When Go packages the UI suite
exercises changed, it says that no spec ran for them. Its result line reads
`Result: **FEATURE PASS — not merge evidence**`.

**Full proof** (`make proof`; `make proof-ui` is the same) is the merge gate,
and it runs everything: lint; build; unit; `go vet -tags=integration`; the
first-party activity images, built and pushed from this tree under a tag of
the proof's own (`ORK_TEST_IMAGE_TAG`); every test of every package whose
integration build differs from its unit build, against real Temporal and
MinIO with `ORK_REQUIRE_INFRA=1`; the activity contract fixture, run on those
images; the UI type-check and unit suites; every Playwright spec; and the
observability gate, whenever the tree differs from `main` in a telemetry
path. That diff-conditioned observability gate is the only diff-conditioned
part of the full proof, and its base is not a choice: the full tier refuses
`--base`, a full proof whose recorded base is not `main` is not
merge-eligible, and `merge-check PROOF=` requires that base to be `main`'s
current tip. Concurrency is how the full proof is fast, never a reason to
run less: integration tests run as separate processes, `PROOF_JOBS` at a
time (default 4), beside the fixture and the UI type-check and unit suites,
and Playwright spec files run on `PROOF_UI_WORKERS` workers (default and
today's only safe value: 1). The unit and integration ledgers, and the
Playwright ledger, list every test that `go test -list` and
`playwright test --list` report, with the job that ran it, its outcome and its
duration. A listed test without an outcome fails the gate.
A skip also fails it, unless `scripts/proof-allowed-skips.txt` names the test
and the reason (an opt-in recorder, a helper subprocess); an allowed-skip
entry that names no listed test fails it too. A test whose binary crashed
while it ran is recorded as failed and never re-run; the tests behind it run
in a fresh process. A full proof is merge-eligible only if every gate ran and
passed, the ledgers are complete, the tree was clean and unchanged from start
to end, the proof ran its own Temporal, the activity image tag was its own,
and the network-change guard stayed within its ceiling. `PROOF_GATES`,
`PROOF_TEMPORAL=shared` and an externally set `ORK_TEST_IMAGE_TAG` each make
it ineligible. `PROOF_JOBS` and `PROOF_UI_WORKERS` do not: whatever the
concurrency, the ledgers show that every listed test ran and passed, and more
concurrency can only add load, which fails a test explicitly rather than
passing one falsely, because each negative window carries its positive
control. The full tier's result lines stay
`Result: **PASS**` and `Result: **FAIL**`; scripts read `proof.json`.

Both tiers exit 0 on pass, 1 when a gate failed, 2 on an invalid invocation
(an unknown base, an existing proof directory, missing evidence, a pattern
that matches nothing, a path the scope table refuses), and 77 when a
prerequisite is absent. Arguments and prerequisites are checked before any
output file is opened, so a refusal never leaves a file that reads like
evidence. A proof creates its own directory and refuses one that already
exists, so yesterday's output can never be mistaken for today's; a proof that
was stopped before its verdict leaves no README and no `proof.json`. Each
proof runs its own Temporal: the image and arguments of the `temporal` service
in `deploy/docker-compose.yml`, on a free port, labelled with the proof and
stopped when the proof exits. A later proof stops any such container whose
owning proof process is gone. Each proof uses the MinIO and registry of
`make infra-up`, isolated by image tag and, for the Go suites, by bucket, and
runs in its own process group with its own binaries. The README names the
commit, the tree, the base and merge base, the activity image digests, and
each gate's outcome. Screenshots and transcripts land beside the logs, and
integration tests that record a transcript or measurement write it under
`ORK_PROOF_DIR`, which the script sets. The UI e2e gate reports how many
navigations the network-change guard repeated; above the ceiling in
`ui/playwright.config.ts` (3 while the browser shares the host's network
namespace) the gate fails, because that evidence was taken on a host too noisy
to trust.

`proof/` is gitignored and evidence is never committed. The repository keeps
the reproduction (the test, spec, or script), and the commit message or pull
request quotes the evidence: the proof directory name and tier, the tree hash,
the gates that ran, and the lines that support each claim. A change to
behavior that only a browser can show (rendering, live streams, an operator
flow) is proven with a Playwright spec and its screenshots, never with a
description; a feature proof names that spec with `--specs`.

### 11.3 Merging

`main` moves only by fast-forward, and only to a commit whose tree a full
proof passed on. In a worktree, branch from `main`'s tip and merge the feature
branch, or several branches to share one proof, with `--no-ff`. When both
sides changed a file, resolve the union of behaviors against the merge base;
do not take one side wholesale. Run `make merge-check` (build, lint, unit and
`go vet -tags=integration`), then `make proof` on that clean candidate. When
the proof passes, amend the merge commit's message to quote it; the amend
leaves the tree, which the proof is bound to, unchanged. Then run
`make merge-check PROOF=proof/<name>` and fast-forward `main` with
`git merge --ff-only`. `merge-check PROOF=` refuses unless the proof is a full,
passing, merge-eligible proof of exactly the candidate's tree, the worktree is
clean, and both the candidate's first parent and the base the proof diffed
against are `main`'s current tip. If `main`
moved during the proof, rebuild the candidate on the new tip and prove it
again. Run one full proof at a time on a host until two concurrent full
proofs have both passed there. Pushing is a separate step, taken only when the
person asks.

## 12. Repository layout and engineering conventions

```text
proto/orkstack/v1/     Proto contract; edit here first
gen/go/                Generated Go; never hand-edit
cmd/ork/               Engine entrypoint (`bin/ork`)
cmd/orker/             Orker entrypoint
cmd/orkctl/            CLI entrypoint
internal/engine/       Temporal workflows and activities
internal/api/          Connect services and WebSocket bridge
internal/dsl/          YAML parse, validation, and Orkflow compilation
internal/orker/        Orker runtime and OCI activity dispatch
internal/task/         Temporal namespace, pool routing, and deployment identity
internal/config/       Configuration file schema and loader
internal/telemetry/    OpenTelemetry setup, sinks, admin listener, instrumentation
internal/archive/      Run retirement and the cluster retention controller
internal/automation/   Automation documents: parse, plan, export
internal/evidence/     Run evidence bundle format, writer, and offline verifier
internal/testsupport/  Test-only helpers shared by suites
internal/listfilter/   Structured list filters: allowlist, grammar, matcher
internal/enumname/     The JSON enum-name dialect shared by the API codec and list filters
images/                First-party OCI activity images; public ABI, no SDK
pkg/artifacts/         Local and S3-compatible artifact storage
pkg/logstream/         Live task-log line framing
pkg/triggers/          Inbound trigger registry and token hashing
pkg/webhooks/          Run-event webhook registry, signing, and delivery
pkg/schedules/         Schedule id shape (the schedule itself lives in Temporal)
ui/                    SvelteKit, Vite, Tailwind, and generated TS clients
deploy/                Temporal, object-storage, and registry development stack
examples/              Example Orkflows, automation documents, and custom images
docs/                  Reference material (indexed in README.md)
test/                  Activity contract fixtures and test-only helper tools
scripts/               Development helpers (`make up`, `make reset`)
```

Use Go 1.27, one module `github.com/orkstack/orkstack`, `gofmt`, `go vet`, and
wrapped errors. Prefer `context.Context` first and no panics in library code.
Connect serves h2c on `:8080` by default and the WebSocket bridge serves
`/ws/runs/{id}`. The UI uses Bun and Svelte 5 runes; generated clients live in
`ui/src/lib/gen`. The CLI uses Cobra and human output by default, with `--json`
for machine output. Configuration is one YAML file shared by both binaries
(`--config PATH`; the schema is `internal/config`, documented in
`docs/DEPLOY.md` §1), with `ORK_`/`OTEL_` environment variables overriding it
and localhost defaults —
including `ORK_TEMPORAL_NAMESPACE` (`ork-main`), `ORK_ORKER_QUEUE`
(`ork-tasks` for the default pool), `ORK_ARTIFACT_BACKEND`, and
`ORK_S3_ENDPOINT`. `deploy/orkstack.defaults.yaml` (`make config-defaults`,
or `ork --defaults` / `orker --defaults`) is the complete reference for every
key's default.

Proto is the single source of truth for cross-process types. Never hand-edit
generated files. Regenerate Go and UI clients when the proto changes. Each
service has its own file (`proto/orkstack/v1/<name>_service.proto`, or the
domain file that already holds it, such as `stats.proto`), holding the service
and the messages only it uses; a message two services share belongs in a
domain file both import. The UI builds one client per service, one module each
under `ui/src/lib/clients/`, each importing only its own generated file, so a
route loads only the schemas of the services it calls.

Agent ownership is split as follows:

- engine agents: `internal/engine`, `internal/api`, `cmd/ork`;
- Orker/activity agents: `internal/orker`, `images/`, `cmd/orker`;
- CLI agents: `cmd/orkctl`;
- UI agents: `ui/`;
- artifact agents: `pkg/artifacts`;
- coordinated changes: `proto/`, `go.mod`, `deploy/`, `Makefile`,
  `internal/telemetry`, `internal/config`, `internal/listfilter` and
  `internal/enumname` (engine and CLI agents share both), and this file.

Use Conventional Commits such as `feat(engine): ...`, `fix(orker): ...`,
`test(dsl): ...`, `docs: ...`, and `chore(proto): ...`. Keep commits scoped and
keep `main` buildable. Release tags use `vMAJOR.MINOR.PATCH`.

## 13. Explicit non-goals

Do not build:

- a visual workflow designer;
- an AI agent builder;
- a plugin marketplace;
- a business-process forms platform;
- hundreds of SaaS connectors;
- a custom scripting language;
- an embedded Python or JavaScript runtime;
- a custom event bus, state machine, scheduler, task queue, or Orkflow-state database;
- a complex expression language;
- a Kubernetes operator (an automation document is applied once by a client;
  there is no controller, watch, or stored desired state);
- a multi-cloud abstraction framework;
- PostgreSQL, MySQL, Redis, Kafka, NATS, RabbitMQ, Elasticsearch, ZooKeeper,
  etcd, SQLite, or another required durable system.

Temporal's own external requirements and a user's deployment requirements are
not OrkStack dependencies. Disposable caches and in-memory caches are allowed.
Focus on the smallest durable execution substrate.

## 14. Engineering decision hierarchy

Prioritize, in order:

1. correctness;
2. simplicity;
3. explicitness;
4. debuggability;
5. portability;
6. performance;
7. convenience.

Prefer files over protocols, processes over frameworks, immutable references
over mutable state, content hashes over implicit identity, boring APIs over
magical APIs, explicit failure over recovery magic, and standard OCI behavior
over proprietary runtime behavior. Avoid clever abstractions, premature
extensibility, and unnecessary infrastructure.

## 15. Product invariants

Unless an accepted contract explicitly changes one, these are non-negotiable:

- an accepted execution cannot disappear;
- a durably committed activity result cannot become pending again;
- an Orker crash or activity crash cannot corrupt Temporal workflow state;
- object-storage failure cannot silently produce successful execution;
- no activity needs an OrkStack SDK or control-plane network access;
- no Temporal workflow state exists exclusively inside an OrkStack process;
- every terminal failure has machine-readable and human-readable reasons;
- an engineer must never edit a database record to unstick an execution.

The objective is not feature count. It is the smallest durable execution
substrate engineers can trust with consequential production operations.
