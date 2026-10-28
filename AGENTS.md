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
event stream. The one exception is a run's own saved state copied into a
generation's history (§7.3): its checkpoint at a continuation and — for a
definition too large to travel as a start payload — its planned definition, an
immutable digest-addressed object under the run's own prefix. Both are copied
in chunks of at most 128 KiB, together at most the plan's checkpoint bound
(8 MiB by default) per generation, verified by SHA-256 and counted in that
generation's history. It exists because a generation's decisions must come
from recorded bytes; it is never an artifact's bytes, and nothing else may use
it.

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
recorded output — is always JSON on the wire, never YAML: it is the one format
the Temporal payload, the Connect API, `orkctl --json` and the UI's typed input
editor share (`docs/JSON.md`), so a copied CLI command reproduces exactly what
was submitted, and it has none of YAML's silent coercions (`no`, `on`,
octal-looking numbers). Documents a person authors — an Orkflow, an automation
document — stay YAML, and each begins with `apiVersion: orkstack/v1` and its
`kind:` (`Orkflow`, `Automation`). Inside one, runtime data is written as YAML
too — a task's `with:` (an http `body:`, jq `vars:`), an Orkflow's `inputs:`
defaults, an await's `schema:`, a schedule's `input:` — and becomes JSON only
through the strict mapping (`docs/JSON.md`, "Data written in an authored
document"), never a lenient decode: a plain scalar that YAML readers resolve
differently (`yes`, `0755`, `1:30`, a date), a number JSON cannot hold
exactly, and tags, anchors, aliases and merge keys are refused, naming the
path. Do not add a YAML input mode to the UI or CLI.

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

A run's start payloads — its input, its start definition and its plan — are
each a §2.2 payload and are refused at submit above 256 KiB. Where the
deployment enables it, a planned definition too large for that travels by
reference instead: the start carries a stub naming it by digest, and the run
copies the immutable definition object into its own history under §2.2's
saved-state exception, taking at most a quarter of that copy's bound,
compressed, and refused at submit beyond it. Those are bounds on what a run
carries, not on an Orkflow as authored: a further limit on definition size is a
proposal, not a current hard invariant, and is not to be turned into one
without a separate accepted contract.

One definition limit is accepted (2026-10-11; raised with continue-as-new,
§7.3): an Orkflow declares at most `engine.orkflow_max_tasks`
(`ORK_ORKFLOW_MAX_TASKS`) tasks — 4000 by default — a deployment-configurable
default that a deployment lowers or raises, refused `INVALID_ARGUMENT` at
validate, apply and plan, naming the count, the limit and the setting, and
re-checked at submit (`FAILED_PRECONDITION`). Every entry of `tasks:` counts
once; a fan-out's items (bounded by `max_items`) and a composed child's tasks
(a separate run) do not. A run's history is bounded per generation, but each
generation must still hold what its failure path would append for every task
not yet started, so a run's shape and the deployment's history, continuation
and in-flight settings bound how many tasks it may declare; `docs/DEPLOY.md`
states that bound in closed form for plain tasks, derived from measured
per-task costs and not itself a measured size, a stage of tasks that all
start together is the binding shape, the boot warns when the setting exceeds
it, and a run beyond it may stop `RUN_HISTORY_LIMIT`, explicitly. Staying
within that bound is necessary, not sufficient: a run within it can still stop
explicitly on history bytes, on the size of its saved state, or on large
inline outputs. The default 4000 is the largest size at which every measured
shape — a flat stage with long and with short tasks, a staged DAG and deep
chains of plain tasks with small outputs — completed within the measured
per-generation history, snapshot, in-flight, workflow-code, read-latency and
run-page thresholds; it is the largest measured value, is not raised without
re-running that evidence — that evidence is the task soak, a deferred test
(§11), so a change that raises the default quotes a feature proof naming it —
and above it a deployment runs unmeasured, warned at
boot. With continuation off a run has one history: the default is then at
most the declared history event limit divided by 50, and an explicit value
above that is refused at boot. A planned definition above 256 KiB travels by
reference (§2.2's saved-state exception), so the start-payload bound no longer
limits how many tasks an Orkflow declares; the run plan's own 256 KiB bound
still limits how many awaits and composed tasks one run tree holds, refused at
submit by name. A run's saved state holds every task's inline output, so a run
whose tasks return large inline outputs may stop `RUN_HISTORY_LIMIT` before
the ceiling, explicitly.

Tasks form a DAG whose only edges are explicit `needs`. Declaration order
never sequences anything: a task becomes ready once every need has ended and
its join policy admits it, and a task with no `needs` (absent or `[]`) is
ready at run start, so independent tasks run in parallel. To order two tasks,
write `needs`. Detect missing references and dependency cycles during
validation; do not discover malformed DAGs during runtime.

The repository vocabulary is deliberate. The user-facing words below are the
ones the DSL, the proto, `orkctl`, the UI and every message use; a word marked
internal never appears on any of those surfaces.

- **Orkflow** means the user-authored YAML entity (`apiVersion: orkstack/v1`,
  `kind: Orkflow`), its name, version, and compiled definition; its `version`
  is the Orkflow's own, `apiVersion` the document format's.
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
version must drain before its workers are decommissioned. A run that continues
as new keeps its pinned build, so a version drains only after its last open
chain ends. A generation never re-asks a version decision an earlier
generation made; on an unversioned fleet a later generation may run a newer
build, so a change whose mid-run adoption would alter a run's observable
behaviour must be decided where every run first meets it, not where a later
generation might. A pinned workflow
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
declares the new feature — which no older history holds, so every recorded
history replays unchanged without a version marker — or by a plan field the
submitting engine sets together with a `workflow.GetVersion` gate asked lazily
where the path first diverges (`lean-event-path`, `run-history-guard`,
`continue-as-new`, `run-in-flight-cap`, `definition-by-reference`,
`snapshot-overlay`), so a history recorded before the gate
replays its `DefaultVersion` branch; each new command path gets its own
recorded history fixture. Every tunable number that decides whether, which,
how many or where workflow code issues a command — a history ceiling or
reserve, a continuation budget, a hold line, a chunk or batch size, an
in-flight cap, a retry spacing — is pinned per run, in the plan the submitting
engine builds or in the carry a generation hands the next, never read from a
code constant at decision time, so a release that changes a default moves only
new runs. A code constant may decide a command only when this file fixes it
(the 256 KiB payload bound, the 128 KiB chunk bound) or it predates this rule
and a recorded history protects it; changing such a constant is a new command
path with its own gate. Because the activity channel can fall
through to another release, an activity-input field an older Orker would
reject or ignore (cache, join, fan-out items) is advertised as a capability on
`DescribeOrker`. Submit asks one Orker of each pool such a task would run on
and refuses with `FailedPrecondition`, naming the Orker and its build, when it
lacks the capability; a pool that does not answer the probe is logged, not
refused, so every Orker of one pool must run one release.

### 7.3 Generations

A run whose plan carries a continuation budget continues as new into a new
generation once its history has grown past that budget, at its next moment
with nothing of it in flight: no activity, child run, pin (opening, open or
closing) or event publish. Open awaits may cross; their deadlines are absolute
instants. The run id, the event sequence counter, `created_at`, `started_at`, a
reference to an immutable full snapshot of its state at the switch and every
version decision cross in the carry (`orkstack.v1.RunContinuation`, at most
256 KiB), and no event is emitted for the crossing. A continuation is not a
start: it keeps the run id, which every generation after the first reads from
its workflow id — its run-id argument is empty, so a release that predates
generations refuses it rather than running the run again — and it is not
refused by the single-use rule. The run's state crosses as an immutable
checkpoint, and each closed generation's events as an immutable event segment;
both are SHA-256 addressed, live under the run's snapshot prefix and are
reclaimed with it. The next generation reads its checkpoint into its own
history in chunks of at most the plan's chunk size (128 KiB), a bounded batch
per workflow task, each read retried by Temporal's own local-activity retry
policy until the plan's restore timeout; it verifies every part before
adopting any and emits nothing for it. A checkpoint larger than the plan's
bound is never written as a switch, and the run carries on in its current
history instead. A run holds NEW dispatch, never running work, only when its
history plus what its failure path still needs reaches the plan's hold line
(60% of the cluster's limit by default), and then for as long as the running
work takes — bounded by that work's own timeouts, never by a clock of its own —
giving the hold up only when an open pin's remaining work waits on an await.
Every run keeps at most its in-flight cap of activities, child runs and pins
pending; more wait PENDING as on a saturated pool, and the cap never holds
while nothing is in flight. A run that cannot carry on fails
`RUN_HISTORY_LIMIT` before the cluster's own limit and is never left to be
terminated. A generation that cannot load or verify its state fails
`RUN_STATE_*` explicitly and still publishes its terminal event; an
operator's cancel during that load cancels the read and ends the run
canceled.

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
stack (Temporal, SeaweedFS, a registry, engine, Orker, and UI, health-checked):

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

`make check` is the contributor's fast path; CI runs every proof
(docs/PROOFS.md).

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
  own policy;
- a run continuing as new with tasks still running: no running task is
  cancelled or dispatched twice, the switch happens only after all finish, and
  the stream has no gap or duplicate; a fan-out whose items outlast any fixed
  window still switches; a run whose budget is crossed by history bytes rather
  than events switches and is guarded the same way;
- an Orker killed during a hold, a resolution and an operator cancel racing
  the switch, an engine replica killed mid-hold, between the switch's writes
  and its commit, and right after a switch, the Temporal frontend unreachable
  during the switch and during the next generation's start, and a Worker
  Deployment Version rollout while a chain switches: each ends in a correct
  durable state with one run id and a contiguous stream, no late write from a
  replaced replica overwrites a newer snapshot, and no release, namespace or
  deletion count ever reads the switching run as gone;
- an await crossing a switch fires at its original instant and accepts a
  resolution after it; an open pin finishes on its host before the run
  switches, a pin waiting on an await gives the hold up, and a pin host lost
  during a hold fails `PIN_HOST_LOST`; a composed child in flight holds the
  switch;
- a saved state corrupted, missing, malformed, or of an unknown format at the
  next generation's start, and an operator cancel during that start: each ends
  explicitly (`RUN_STATE_*` or `RUN_CANCELED`) with a terminal event built
  from the run's exact state at the switch and an archive body; a start whose
  reads keep failing stays within its stated history bound;
- an earlier generation's history deleted while the run continues, and the
  projection worker down during the switch: `GetRun`, events, the live stream
  and the final archive are still complete; an event segment lost before the
  run is archived leaves an archive and an evidence bundle with an explicit
  gap, never a missing run;
- a run that can never go idle fails `RUN_HISTORY_LIMIT` before the cluster's
  limit; object storage unavailable at a switch never yields a switch without
  its objects, nor a silent success; a saved state over its bound never
  switches; a run with more work than its in-flight cap never exceeds the
  cluster's pending-activity limit and never stalls with nothing in flight;
- a run at the deployment's task ceiling and a run at the fan-out item
  ceiling complete across generations with their definition passed by
  reference and snapshot overlays on, each generation within the measured
  share of the cluster's history limits and replaying within the deadlock
  detector's budget, and every run within its in-flight cap. The merge
  gate proves this with the ceiling lowered below its default, at a size
  where the definition still passes by reference, overlays are written
  and the run carries on across at least two generations. The default
  ceilings themselves, and the overlay saving at a thousand tasks, are
  proven only by the deferred tests (below).

Scale and soak tests leave the per-merge proof only through
`scripts/proof-nightly-tests.txt`, and Playwright specs through its spec-side
twin `scripts/proof-nightly-specs.txt`. Each entry names the test or spec file,
the merge-gate tests or specs that cover the same behaviour at a smaller size
or by another route, and the date the user approved it. A test or spec is
listed only when such coverage exists, and the lists change only with the
user's approval. Every other test and spec, and every scenario above, except
as the last bullet states, stays in the merge gate: the run page at the task
ceiling is proven there by `ui/e2e/run-page-reduced-ceiling.spec.ts`, its
measurement at a ceiling lowered to 1,000, and at the 4,000-task default by
the nightly tier. A full proof records each listed test, and each test of a
listed spec file, as `deferred`, never as passed. It defers only the entries
its base's list also carries: an entry the change itself adds is run, and
recorded as it ran. The deferred tests and specs run in the nightly tier and
in the pre-release gate, and a release tag requires a passing pre-release
proof of the exact commit it names.

The merge gate's absolute wall-clock budgets — the soaks' `GetRun` p95, the run
page's 5 s — were calibrated on the 16-CPU reference host and are enforced
there: in every local proof of any tier, and in the pre-release gate wherever
it runs, which excuses no runner-size miss, so a tag rests on budgets that
held on the reference class (a CI release proof needs runners as large as the
reference, or `make proof-release` runs on the reference host). In the merge
gate and the nightly, on a CI runner with fewer CPUs, a miss of one is a
recorded known gap, never a pass and never a widened bound (accepted
2026-10-27): only under a row of `scripts/proof-host-speed.tsv` that the user
approved and the base carries identically, and for a Go test only up to the
bound scaled by the reference's CPUs over the runner's. The merger derives
each excusal itself from the part's own records (docs/PROOFS.md §12). A count,
a size, a history share, a deadlock-detector trip, a correctness assertion, or
a failure beside the timing miss gets no row and fails the proof wherever it
runs. What stays visible where a budget is excused is a bound on what the
soaks' live `GetRun` reads, which holds on every host: at most one pass over
the generation's history, in events and in bytes, and a bounded number of
saved-state objects. It bounds what is read, not what reading it costs: a
slower decode shows only in the wall-clock numbers, which the reference host
enforces, and the run page's 5 s has no such counterpart.

The nightly of `main` is GitHub's: `nightly.yml` runs the nightly tier on
`main` every day (05:17 UTC) and by dispatch. No local `make proof-nightly`
follows a merge: that interim rule, in force while the workflow did not run on
GitHub, was retired on 2026-10-27, once three consecutive nightlies of `main`
had passed on it. A release tag has its own gate, the pre-release proof above.
A merge is not held for a nightly of its own: a run speaks only for the commit
it measured (its `headSha`), never for `main`'s tip, so the nightly of a later
commit is a later run.

A failing nightly (the latest completed `nightly.yml` run on `main`) or
pre-release proof is a failing `main`: fix it, or revert the change that broke
it, before anything else merges, and never edit a list, a threshold or a
measurement file to make it pass. A red nightly clears only when a later run,
of the fix or of a commit after it, passes: the fix is the one merge made
meanwhile, a nightly of it is dispatched once it is pushed (docs/PROOFS.md
§10), and nothing else merges before that run passes. A nightly part whose
runner measured below the runner-health floor
(`scripts/proof-runner-floors.tsv`) before any test ran is a runner failure,
not a failing `main`: the proof records that part as `runner-degraded`, is
not a proof of the tree, and that one part runs again as a new part of the
same tree (§11.2); a part above the floor that fails is a failing `main`. The
race, race-repeat and fuzz parts are measured for information and no floor
binds them, so a failure there is a failing `main`. A
change that raises a
default the deferred tests measured (`engine.orkflow_max_tasks`, the fan-out
item maximum, or a continuation, history or in-flight default) quotes a
feature proof that names those tests with `--tests`. A regression that only a
deferred test can see is found by the next nightly run rather than at merge.
That is the accepted cost (2026-10-15).

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

0. `make check` is the fast path: build, vet, lint and the unit suites with
   `-short`, with no Docker and no Temporal. Run it before every push; CI
   runs it on every pull request.
1. For a process-boundary change, edit `proto/` first and run `make proto`.
2. For a new configuration setting: add it to `internal/config`'s `Schema`
   (its default bound to the code constant, `internal/config/config.go`),
   `docs/DEPLOY.md` §1's table, and regenerate
   `deploy/orkstack.defaults.yaml` with `make config-defaults`; the
   completeness tests refuse a setting that is missing from any of the
   three.
   For a change to an authored document's keys or words, or to a first-party
   image's interface label, regenerate `docs/schemas/` with
   `go run ./internal/schemas/cmd/genschemas`; `TestSchemas_AreGenerated`
   refuses a stale schema.
3. Dependencies are pinned: an exact module version, or an image digest,
   never a floating tag at runtime; a toolchain version is an exact line in
   `go.mod` or the workflow. Update by pull request that proves the change —
   `.github/dependabot.yml` opens them, and an update never merges on a green
   unit run alone. A new dependency follows the same rule as any other change.
4. Write a behavior test that fails for the right reason.
5. Implement until it passes.
6. Run `make lint test`; for concurrent code, `make test-race` too (the race
   gate's run, docs/PROOFS.md §2.3); for Temporal or storage changes, run the
   affected integration tests against `make infra-up`
   (`scripts/integration.sh ./internal/engine -run '^TestIntegration_X$'`, or
   `make test-integration` for all of them).
7. Documentation is part of the change. A change that alters behaviour users
   or operators see — DSL keys and rules, CLI commands and flags, API fields,
   error reasons, configuration, the activity contract, storage layout, proof
   and CI behaviour — updates the reference doc that describes it, and
   deletes the text it makes false, in the same change. A user-visible change
   adds its entry under CHANGELOG.md's `[Unreleased]` section (Keep a
   Changelog 1.1.0), a breaking change marked **BREAKING**. Specs and design
   documents still never go in the repository. A change is not done while a
   doc describes the old behaviour.
8. Produce the change's evidence with
   `make proof-feature TESTS='PKG:REGEX …' SPECS='FILE …'`, naming the tests
   and specs that prove it, and quote it as a feature proof (§11.2).
9. Commit with a Conventional Commit.
10. Merge through §11.3. The full proof of the merged tree is what makes the
    change done.

Unit tests are pure and table-driven. A parser of input OrkStack does not
control (a request body, a flag, an authored document, an object read back from
storage, a container's output) that is added or changed also gets a native Go
fuzz target (`FuzzXxx`) that states the properties the code claims, its seeds
drawn from the table tests; the seeds run in every unit gate, so they stay fast
and deterministic. `go test -list '^Fuzz' ./...` lists the targets there are;
a parser without one is not yet covered, not exempt. A crasher a fuzz run finds
is fixed at its root and kept under `testdata/fuzz/<FuzzName>/` as a regression
seed. Temporal workflow tests use
`WorkflowTestSuite` / `TestWorkflowEnvironment` with mocked activities.
A unit test bounds code cost by thread CPU time or an injected clock, never by
a sub-second wall-clock window; where the platform has no thread clock (off
Linux) the Go cost tests fall back to the wall clock.
Integration tests use `-tags=integration`, `make infra-up`, and `infraAbsent`; a
missing prerequisite skips, and becomes a failure under `ORK_REQUIRE_INFRA=1`
(`make test-integration` sets it). Browser end-to-end tests live in `ui/e2e`
(Playwright, `cd ui && bun run test`) and drive the built UI against a real
engine, Orker, Temporal, and the dev object store; the activity fixtures live in
`test/fixtures` and `test/tools` holds helpers those suites build. Do not use
sleeps for synchronization; use deadlines or test helpers. A skip is never a
pass: a harness must probe its prerequisites itself and fail, not skip, once
they are present.

Some suites have an activity container call a stub on the host at
`host.docker.internal`; a Linux host with plain Docker Engine needs one-time
host setup for that name (CONTRIBUTING.md, "Linux hosts"). It is host setup, never
Orker behavior: the Orker must not add `--add-host` or any host-network
configuration to activity containers (§6). The observability gate's
Prometheus reaches the admin listeners over that same default bridge:
`scripts/proof-observability.sh` attaches its own Prometheus container to the
bridge and binds that run's admin listeners to the bridge's gateway, for the
gate only. That is proof and development setup, never a `deploy/` default or a
production listener (CONTRIBUTING.md, "Linux hosts", says what it exposes).

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
the Docker daemon, CPU and memory, the SeaweedFS and registry servers (isolated by
bucket and tag), and the host's network interfaces.

Playwright specs are isolated from each other, and not yet from another
proof's. The browser runs in a network namespace of its own — Playwright's
browser server in the container `ui/e2e/browser-image.txt` pins by digest
(`ui/e2e/browser-server.ts`), reached over `connectOptions` with
`exposeNetwork: '<loopback>'`, so a spec's `127.0.0.1` URLs are unchanged —
and a container started or stopped anywhere on the host never reaches it;
`ui/e2e/network-change.spec.ts` proves that with a host-namespace browser as
the positive control, and the navigation guard's ceiling is zero repeats. The
run page at the task ceiling (its two spec files, at the default and at the
reduced ceiling) is still driven on the host's Chromium
(`ui/playwright.config.ts`, `projects`): alone, after every other file, with
tasks that open no network interface. Every spec file creates a bucket of its own (`ownBucket` in
`ui/e2e/harness.ts`, the same name shape as `s3bucket`, removed when the file
is done), so the deployment-wide documents (retention, image policy, audit,
webhooks, the namespace registry) are its alone, and a proof runs spec files
`PROOF_UI_WORKERS` at a time (default 2), after the integration phase; its
Temporal namespace is `temporalNamespace`'s, the prefix extended with the
file, its process and a stamp, and its Orkers' work roots are named with its
process's pid, as an integration test's are (`orkerEnv` in
`ui/e2e/harness.ts`). Each spec still listens on fixed ports — unique across
files, not across proofs — so at most one
proof runs Playwright specs on a host at a time: the Playwright gate takes a
host-wide lock, so a feature proof's specs wait for a full proof's, and never
run beside them. Specs run by hand (`cd ui && bun run test`) take no lock; do
not run them while a proof's Playwright gate runs. The observability gate is
the other exception: its Prometheus scrape targets are fixed ports, so it runs
under a host-wide lock of its own. A test that proves something did NOT happen within a
window also observes, in that window, that the system was making progress, so
a slow host cannot make it pass.

Run integration suites through `scripts/integration.sh`, which
`make test-integration` and the proofs use; docs/PROOFS.md §2 gives its
mechanics and the by-hand `go test -tags=integration` recipe.

Stop only what you started, by its PID. Never stop a process by its name or a
pattern: `/bin/ork$` and `/bin/orker$` match every worktree's dev stack and
every running proof on the host. `make down` stops this worktree's dev stack
(the process groups `make up` recorded, and any process whose executable is
this worktree's `bin/ork` or `bin/orker`, identified by `/proc/<pid>/exe`,
never by its name). `scripts/proof-stop.sh proof/<name>` stops a proof you
started, and `scripts/proof-clean.sh` lists and removes what a failed proof
kept; docs/PROOFS.md §10 says how.

Clean the host before any test or proof runs on it. What earlier runs left —
buckets that hold the object store's volumes, Orker work roots, activity
containers, proof Temporal containers — has failed later proofs, so before
`make proof*`, `scripts/integration.sh` or a by-hand
`go test -tags=integration`, the leftovers of runs whose processes are gone
are removed and what was removed is logged. Every local proof and every
`scripts/integration.sh` run does it first, by itself (`proofrun clean
-remove`; a proof's `setup.log` opens with
`cleanup: removed N buckets, N work roots, N containers of dead proofs`); a
by-hand run is preceded by `make proof-clean REMOVE=1`. It is the one
exception to stopping only what you started: it finds a leftover by the names
the proofs and runs of every worktree recorded and the pid each name carries
— an activity container by the test work root its workspace lies in, whatever
its Orker is called — and removes it only when that process is gone. It never touches anything of
a live process, of a proof still running, or whose owner it cannot tell, and
the dev stack stays up.

### 11.2 Evidence

No proof, no done. A change is finished when its behavior has been observed,
not when its code reads right. A change is **done** when it is on `main` and a
full proof passed on the exact tree `main` then points at (§11.3) — the
`proof` check of the pull request (or merge group) that landed it, a local
`make proof`, or a CI full proof of that exact tree downloaded and passed to
`make merge-check PROOF=` (§11.3(b)) — and when every reference doc that
describes what changed says the new thing, with a CHANGELOG.md `[Unreleased]`
entry for a user-visible change. Done is judged by the merge gate, and a deferred test's regression is
a failing `main`, found by the nightly or the pre-release proof. A feature
proof means "ready for the merge proof", never "done".

**The four tiers.** A **full proof** is the merge gate. A **feature proof**
is a change's evidence on its branch and never merge evidence. The **nightly
tier** runs only the tests `scripts/proof-nightly-tests.txt` lists and the
spec files `scripts/proof-nightly-specs.txt` lists, and two lanes of its own
— the race repeat (the concurrency-heavy packages' unit suites under the race
detector, several times in one run) and the fuzz lane (every Fuzz target for a
bounded time, a crasher failing it and its input kept) — and is never merge
evidence. The pre-release gate is the full tier with nothing
deferred, and it runs the observability gate unconditionally. A release tag
requires a passing pre-release proof of the exact commit. docs/PROOFS.md is
the reference for what each tier runs.

**Rules for every proof.**

- Never rerun a failing test, and never partially rerun a shard that produced
  a result. Only three kinds of part attempt may run again, none of which
  recorded a result: one whose runner GitHub annotated as lost; one whose own
  runner-health measurement at its start was below the floor
  (`scripts/proof-runner-floors.tsv`; the part is classified
  `runner-degraded`, and `proof.json` records the part, the metric, the
  measured value and the floor); and one whose job failed in a fetch or install
  step before its proof step started (`setup-failed`: the failed step is on
  the per-kind allowlist `setupStepNames` in `test/tools/proofrun/merge.go`,
  the plan and the build have no entry, and `proof.json` records the part, the
  attempt and the step). Such a part runs again whole, as a new part of the
  same tree, never a failed test in isolation, and `proof.json` records the
  rerun; any other second attempt makes the proof ineligible. Reruns of the
  three classes may be issued automatically (`.github/workflows/proof-rerun.yml`,
  off unless the repository enables it, at most two per run), by a planner that
  reuses the merger's classification and reruns only when every failed job is
  the `proof` job or a part of those classes. "Re-run failed jobs" is
  permitted when every failed part is eligible, and is never an answer to a
  part that recorded a result; the merger stays the only authority, so a
  rerun the planner got wrong makes a proof ineligible and never passes one.
  A degraded part's tests have no outcome of the tree, and a proof holding one
  with no later attempt is not a proof of the tree; a part above the floor
  whose tests fail is a test failure. The floor is measured once, before any
  gate: a runner that degrades later is not seen, and its failures stay test
  failures.
- Never edit the costs file, the slow list, the nightly list, the quarantine
  list, the host-speed table, the runner-health floors or the allowed skips to
  get a proof through. The nightly and quarantine lists, and the rows of the
  host-speed table, change only with the user's approval.
- A new top-level path needs a `scripts/proof-scope.tsv` row, and a path the
  table does not map refuses the feature tier.
- Read `proof.json`, never the README, for a verdict.
- A quarantined test still runs; its failure is recorded as
  `quarantined-fail`, never as a pass, and its entry carries an owner, a
  reason and a review date. An entry excuses nothing until `main` carries it,
  and nothing once its review date has passed.
- `make merge-check` asks for no nightly proof: the nightly of `main` is
  GitHub's (§11). Given one (`NIGHTLY=`), it refuses the merge unless that
  proof is a passing nightly proof of `main`'s tip.
- A release tag is created only through `make release-tag` or `release.yml`,
  and both refuse without a passing pre-release proof of the exact commit.
- A skip is never a pass. The ledgers list every test the binaries report; a
  listed test without an outcome, an unlisted test, and an allowance naming
  no test all fail the gate.
- Evidence is never committed; the commit or pull request quotes it, and for
  a queue merge the `proof` job's summary and artifact are the quote. A
  browser-only behaviour is proven by a Playwright spec and its screenshots.
- A proof directory is never reused, and a proof creates it only after its
  arguments and prerequisites are checked. Exit status: `0` pass, `1` a gate
  failed, `2` an invalid invocation, `77` a missing prerequisite.

**Feature proof** (`make proof-feature`;
`scripts/proof.sh --tier feature [--base REF] [--tests PKG:REGEX]... [--specs FILE]...`)
is the evidence for a change on its branch. Its directory is
`proof/<name>-feature/`. Its change set is the diff from the merge base with
`--base` (default `main`) to the working tree, untracked files included.
Every changed path must match a rule in `scripts/proof-scope.tsv`. A path that
no rule matches, or whose rule is `full` (`go.mod`, `go.sum`, the Makefiles,
`buf.yaml`, `buf.gen.yaml`, `.github/`, `deploy/` other than the generated
defaults, the example configuration and the observability profile, `scripts/`
other than the observability gate and the three measurement files
(`scripts/proof-slow-tests.txt`, `scripts/proof-test-costs.tsv` and
`scripts/proof-ui-costs.tsv`), `internal/testsupport/`, and
`test/tools/proofrun/`), refuses the feature tier. A feature proof always runs
lint, build, the unit suites and `go vet -tags=integration`, and it runs the UI
type-check and unit suites when `ui/` or `proto/` changed or a spec runs, the
site's check and build when a path the site reads changed, and the race gate
over the Go packages the change touches when it touches any. It then runs:

- the change's **evidence**: every test whose source the diff changes,
  including every test that reaches a changed or deleted helper, type,
  variable or `TestMain` through its package's declarations (comment-only
  changes are no change) — except that a test which is evidence only because
  it reaches a changed helper, type or variable is held to the slow list, as
  the regression set is, and the merge proof runs it; when every such test of
  a package is slow, the cheapest stays, and a changed `TestMain` still runs
  the whole package; every test `--tests` names, where `REGEX` must match
  whole test names; every spec the diff changes or `--specs` names (a file
  under `ui/test/` names a UI unit test); every spec when the Playwright
  harness, its config or the UI's dependencies changed; the activity contract
  fixture when `images/` or `test/fixtures/` changed; the tests that read a
  changed example, and the examples spec, when `examples/` changed; the tests
  and specs that run a changed `test/tools/` tool, found by searching their
  sources; the observability gate when a telemetry path changed; and the site
  gate when a changed path's scope row names `site` (the site itself, the
  documentation and markdown, the examples, the generated defaults, `proto/`,
  and the UI's design tokens, icons and dependencies);
- the **regression set**: in every integration package whose test binary
  depends on a changed package — through its imports, or through a binary its
  suite builds (an `images/` change counts as a change to `internal/orker`) —
  every test declared in an integration-tagged file, except those on the slow
  list (`scripts/proof-slow-tests.txt`, regenerated from a full proof's ledger
  by `make proof-slow-list FROM=proof/<name>`), which also holds back the
  evidence that only reaches a changed declaration. A test whose own source
  changed, and every `--tests` name, runs whatever the slow list says.

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
and it runs everything except the tests `scripts/proof-nightly-tests.txt`
lists, which its integration ledger records as `deferred` with their list
entry, and the spec files `scripts/proof-nightly-specs.txt` lists, whose tests
its Playwright ledger records the same way: lint; build; unit; `go vet -tags=integration`; the first-party activity
images, built and pushed from this tree under a tag of the proof's own
(`ORK_TEST_IMAGE_TAG`); every test of every package whose integration build
differs from its unit build, except the deferred ones, against real Temporal
and SeaweedFS with `ORK_REQUIRE_INFRA=1`; the activity contract fixture, run
on those images; the UI type-check and unit suites; the site's check and
build; the race gate (the unit suites under the race detector, `-short`);
every Playwright spec; and the observability gate, whenever the tree
differs from `main` in a telemetry path. That diff-conditioned observability
gate is the only diff-conditioned part of the full proof, and its base is not
a choice: the full tier refuses `--base`, a full proof whose recorded base is
not `main` is not merge-eligible, and `merge-check PROOF=` requires that base
to be `main`'s current tip. Concurrency is how the full proof is fast, never a
reason to run less: the fast gates (lint, build, unit, vet, the UI type-check
and unit suites, the site's check and build and the race gate) run first and
alone, and a failure there or in any later gate stops a local proof
(`PROOF_FAIL_FAST`, docs/PROOFS.md §2.2), which still writes its failing
verdict; integration
tests run as separate processes,
`PROOF_JOBS` at a time (default 6: measured clean in two full proofs on the
proof host, train-a-full in 79.0 min with integration 2805 s and
train-b2-full in 71.2 min with integration 2760 s, once a slow engine boot is
tolerated and passing jobs' buckets are removed while the phase runs; an
earlier run at six had failed on one boot that stalled 50 s on its fresh
bucket's first read), with the
activity fixture beside them once the heavy tests have run alone, and
Playwright spec files run on `PROOF_UI_WORKERS` workers (default 2: measured
clean; at 3 a load-sensitive spec failed in each of three runs). The
unit, race and integration ledgers, and the
Playwright ledger, list every test that `go test -list` and
`playwright test --list` report, with the job that ran it, its outcome and its
duration. A listed test without an outcome fails the gate. In the full tier,
`deferred` is a recorded outcome, not a missing one. It is legal only for a
test the nightly list names, or a test of a spec file the nightly spec list
names, and in every other tier it fails the gate. A skip
also fails it, unless `scripts/proof-allowed-skips.txt` names the test and the
reason (an opt-in recorder, a helper subprocess); an allowed-skip entry that
names no listed test fails it too. The race gate alone records a skip it
does not refuse: it runs `-short`, its ledger says so, and a test it skipped
counts only because the unit ledger, which runs the test without `-short`,
passed it (the nightly race repeat records its `-short` skips the same way and
has no unit ledger to hold them to); a data race
the detector reports fails the gate whatever the tests reported, and no
quarantine excuses it. A test whose binary crashed while it ran is
recorded as failed and never re-run; the tests behind it run in a fresh
process. A full proof is merge-eligible only if every gate ran and passed, the
ledgers are complete (their `deferred` entries match exactly the entries that
both the base's list and the tree's list carry, test and spec lists alike), the tree was clean and
unchanged from start to end, the proof ran its own Temporal, the activity
image tag was its own, and the network-change guard stayed within its ceiling.
A local proof is one plan and one part; a CI proof is one plan, every part of
the same tree, one build's image digests in every part, and each stack part
its own Temporal. `PROOF_GATES`, `PROOF_TEMPORAL=shared` and an externally set
`ORK_TEST_IMAGE_TAG` each make it ineligible. `PROOF_JOBS` and
`PROOF_UI_WORKERS` do not: whatever the concurrency, the ledgers show that
every listed test ran and passed, and more concurrency can only add load,
which fails a test explicitly rather than passing one falsely, because each
negative window carries its positive control. The full tier's result lines
stay `Result: **PASS**` and `Result: **FAIL**`; scripts read `proof.json`.

`proof/` is gitignored and evidence is never committed. The repository keeps
the reproduction (the test, spec, or script), and the commit message or pull
request quotes the evidence: the proof directory name and tier, the tree
hash, the gates that ran, and the lines that support each claim. The failed
proof's logs, ledgers, artifacts and recovery procedures are docs/PROOFS.md's
§§ 7–10.

### 11.3 Merging

`main` moves only by fast-forward, and only to a commit whose tree a full
proof passed on. There are two paths to that.

**(a) GitHub, once the gate is enforced.** The required `proof` check is the
full proof. By default it runs on the pull request, whose branch must contain
`main`'s tip, under the strict policy, and GitHub lands exactly the tree it
tested; with the optional merge queue it runs on the merge group, and the
queue lands that tree. A code-owner ruleset, which the enablement applies
unless it is told not to (`CODEOWNER_REVIEW=0`), requires the maintainer's
review for a change to the gate's own files.

**(b) Locally.** Branch from `main`'s tip and merge the feature branch, or
several branches to share one proof, with `--no-ff`. When both sides changed a
file, resolve the union of behaviors against the merge base; do not take one
side wholesale. Run `make merge-check` (build, lint, unit and
`go vet -tags=integration`), then `make proof` on that clean candidate, or
push the candidate to `validate/full/<name>` and download the
`proof-<run>-a<n>` artifact of its passing run (docs/PROOFS.md §10);
`make merge-check PROOF=` accepts it when it is a full, passing,
merge-eligible proof of exactly the candidate's tree whose base is `main`'s
tip. The artifact is downloaded, never retyped: its `proof.json` is the
evidence. When the proof passes, amend the merge commit's message to quote
it; the amend leaves the tree, which the proof is bound to, unchanged. Then
run `make merge-check PROOF=proof/<name>` and fast-forward `main` with
`git merge --ff-only`. `merge-check PROOF=` refuses unless the proof is a
full, passing, merge-eligible proof of exactly the candidate's tree, the
worktree is clean, and both the candidate's first parent and the base the
proof diffed against are `main`'s current tip. If `main` moved during the
proof, rebuild the candidate on the new tip and prove it again. Run one full
proof at a time on a host until two concurrent full proofs have both passed
there.

Until the merge gate is enforced (docs/PROOFS.md §6), path (b) is the merge
path, and a CI run of the candidate counts only through (b)'s
`merge-check PROOF=`, never as a substitute for it. Enabling it is an
outward-facing change, confirmed by the person first. Pushing is a separate
step, taken only when the person asks; once they have, `main` is pushed to
`origin` after every merge, so the nightly and the gate see the real tip.
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
internal/schemas/      JSON Schemas of the authored documents, generated from the parsers' key tables
internal/testsupport/  Test-only helpers shared by suites
internal/listfilter/   Structured list filters: allowlist, grammar, matcher
internal/enumname/     The JSON enum-name dialect shared by the API codec and list filters
internal/docscheck/    Documentation drift checks (test-only)
internal/apidocs/      docs/API.md, the client-facing API reference, generated from the proto comments
internal/dist/         Release artifacts: binaries, archives, checksums, the quickstart bundle, release guards
internal/healthcheck/  The `--healthcheck` probe of ork and orker
images/                First-party OCI activity images; public ABI, no SDK
pkg/artifacts/         Local and S3-compatible artifact storage
pkg/logstream/         Live task-log line framing
pkg/triggers/          Inbound trigger registry and token hashing
pkg/webhooks/          Run-event webhook registry, signing, and delivery
pkg/schedules/         Schedule id shape (the schedule itself lives in Temporal)
ui/                    SvelteKit, Vite, Tailwind, and generated TS clients
site/                  The landing page and docs site (Astro, Starlight): styled by ui/src/tokens.css,
                       its docs written for readers and citing this repository's docs
deploy/                The development stack (Temporal, object storage, registry), the release images'
                       Dockerfile, and the quickstart compose bundle
examples/              Example Orkflows, automation documents, and custom images
docs/                  Reference material (indexed in README.md; `CLI.md` is generated)
test/                  Activity contract fixtures and test-only helper tools
scripts/               Development helpers (`make up`, `make reset`) and the proof harness
                       (`proof.sh`, the gate lists)
.github/               CI: fast check, feature tier, sharded merge gate, nightly, release
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

The site (`site/`) takes its whole visual style from the UI's token file,
`ui/src/tokens.css`: it imports that file as it is and keeps no copy of it, and
it declares no colour or font of its own (`site/scripts/check-tokens.mjs`,
run by the `site` gate, refuses one). A change to the UI's look changes the
tokens, never the site, and reaches the site with no step of its own. A change
that leaves the site, landing page or docs, looking different from the UI is
not done. A UI change that alters what the site's screenshots show
regenerates them with `make site-media` in the same change. The site's docs
are written for people (`site/content/`, `site/docs.manifest.json`): short,
task-first pages — tutorials, one-task how-to guides, reference tables, short
concepts — that never copy this repository's normative docs. Each page cites
the repository docs it summarises, which stay the full reference, and the sync
refuses a page that cites none, a citation of a file or heading that does not
exist, and a doc under `docs/` that no page cites. The generated references
(orkctl, configuration, error reasons, API, changelog) are ported as they
are. A change to a behaviour a page describes updates that page too.

Agent ownership is split as follows:

- engine agents: `internal/engine`, `internal/api`, `cmd/ork`;
- Orker/activity agents: `internal/orker`, `images/`, `cmd/orker`;
- CLI agents: `cmd/orkctl`;
- UI agents: `ui/` and `site/`;
- artifact agents: `pkg/artifacts`;
- coordinated changes: `proto/`, `go.mod`, `deploy/`, `Makefile`, `.github/`,
  the gate lists (`scripts/proof-*`), `internal/telemetry`, `internal/config`,
  `internal/listfilter` and `internal/enumname` (engine and CLI agents share
  both), and this file.

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
