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
Orker. Do not create an OrkStack-owned execution-state database, Orkflow-state
database, scheduler, task-queue implementation, distributed lock service,
retry subsystem, timer subsystem, or state-transition engine. Cron scheduling
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
recorded output — is always JSON, never YAML. JSON is the one format the
Temporal `Payload`, the Connect API, `orkctl --json`, and the browser's typed
input editor already share (`docs/JSON.md`), so a copied CLI command
reproduces exactly what was submitted. JSON's `null`/`true`/`false`/number/
string types have no implicit coercion to trip a reader — YAML's unquoted
`no`, `yes`, `on`, and octal-looking numbers are exactly the silent-coercion
class of bug the typed-input UI exists to prevent. This does not change the
Orkflow DSL: a document a person authors — the Orkflow itself, with its
declared input schema — stays YAML (`docs/DSL.md`); only the runtime data a
system exchanges is JSON. Do not add a YAML input mode to the UI or CLI.

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

Activities may override the policy. Keep these failure categories distinct:

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
implementation must not silently collapse the categories.

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

Use a deterministic commit key. At retry start, if a valid `commit.json` exists,
return its committed result and do not execute the container again. This only
prevents duplicate container execution after OrkStack has durably committed the
result; it does not make arbitrary external side effects exactly once.

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
Leave room for later policies such as `requireDigest`, signed-image checks, and
allowed registries; do not build that policy system unless current work needs
it.

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
interpolate untrusted input without the templating layer's escaping.

## 7. Orkflow execution model

Compile each Orkflow into an immutable internal representation and execute it
through one generic deterministic Temporal Workflow interpreter. Do not generate
bespoke Temporal source code for each Orkflow. The interpreter resolves
 dependencies, schedules activities, waits on Temporal timers and Signals,
applies retry policies, handles cancellation, and records compact outputs.

A definition limit such as 256 KiB is a proposal, not a current hard invariant.
Do not turn that proposal into a hard limit without a separate accepted
contract.

Steps execute sequentially in declaration order by default. Explicit `needs`
dependencies create DAG edges and enable parallel execution. `needs: []` may
start immediately. Detect missing references and dependency cycles during
validation; do not discover malformed DAGs during runtime.

The repository vocabulary is deliberate:

- **Orkflow** means the user-authored YAML entity, its name, version, and
  compiled definition.
- **Temporal workflow** means Temporal's workflow context, history, workflow
  type, workflow ID, or workflow task.
- **Pool** means the user-facing set of Orkers.
- **Queue** means only an internal Temporal task queue and never a user-facing
  OrkStack concept.
- **Pin** means a named group of tasks of one Orkflow that execute on one Orker
  process and share `/orkstack/shared`. It is the user-facing word: it appears
  in the DSL, the proto, `orkctl`, the UI, and every error message.
- **Session** means Temporal's Worker Session, the mechanism a pin is
  implemented with. It is internal, exactly as **queue** is internal to
  **pool** (§7.1): no user-facing surface names a session, a session id, or a
  session task queue.
- **Pin host** means an Orker configured to accept pins
  (`ORK_ORKER_MAX_PINS` above zero). Every Orker still runs ordinary tasks;
  only a pin host runs pinned ones.
- **Run retention** means how long a finished run of one OrkStack namespace
  stays readable before it is retired, counted from when the run FINISHED,
  never from submission. It is a field of `orkstack.v1.Namespace`
  (`run_retention`), unset meaning inherit.
- **Maximum run retention** means the deployment-wide ceiling every
  namespace's run retention is bound by, and what a namespace that sets none
  inherits (`orkstack.v1.RetentionPolicy.max_run_retention`).
- **Telemetry sink** means an operator-declared OTLP destination, named, in
  `ORK_TELEMETRY_SINKS`. `default` is reserved: it means the standard `OTEL_*`
  environment, not an entry in that YAML.
- **Telemetry policy** means one namespace's selection among a deployment's
  telemetry sinks: which sink, whether task logs export, which attributes.
  It is a field of `orkstack.v1.Namespace` (`telemetry`), unset meaning
  inherit — the same shape `run_retention` already uses.
- **Hold** means a namespace closed to new runs, with the operator's reason
  and the instant the registry recorded it (`orkstack.v1.Namespace.hold`). It
  is an admission decision, never a retroactive one: a run already accepted
  finishes untouched, and releasing a hold starts nothing that was refused.
  Its sibling `max_running` bounds a namespace's runs in flight, counted from
  the cluster's execution index at submit.
- **Cordon** means a process-local flag on one Orker that stops it accepting
  new tasks and opening new pins while leaving everything in flight alone. It
  is delivered over the Orker's own control channel, is lost when the process
  exits, and is the user-facing word: it appears in the proto, `orkctl`, the
  UI, and every message.
- **Drain** means cordoning an Orker and waiting until it reports no running
  task and no open pin. It is a client-side wait over the inventory read
  (`orkctl orker drain`); nothing server-side holds drain state.
- **Control channel** and **presence channel** are internal, exactly as
  **queue** is: the Temporal task queue `ork-orker-<orker_id>` that one Orker
  polls for `DescribeOrker` and `CordonOrker`, and `ork-orkers`, which every
  Orker polls and nothing dispatches to, whose poller list is the fleet. Both
  are built in `internal/task/pool.go` and never appear on a user-facing
  surface.

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

Temporal is the read model for live runs. Finished runs past retention come from
the shared run archive. `RunStore` and `EventBus` are write-side helpers only:
they support candidate submission, archive/webhook writes, and the stream for a
run whose start failed. Never answer a read from process memory behind a wired
reader. Any replica must answer for a run it did not start and for a run whose
original replica is gone.

RunWorkflow assigns event sequence numbers. The bus honors supplied numbers and
assigns one only when none is supplied. Temporal history, the in-process log,
and archived event JSON must therefore remain identical, and clients can resume
by sequence from another replica.

### 7.1 Pools and Temporal task queues

`pool:` is the only user-facing selector of *which Orkers*; `pin:` constrains a
group of tasks to *one* member of that set. Neither exposes a queue, a session,
or a host name. No user-facing surface may expose `queue:` or a queue name. The
translation is centralized in `internal/task/pool.go`: `QueueForPool`,
`RouteForPool`, `PoolForQueue`, and the single `ork-pool-` prefix construction
are the complete mapping.

An empty pool and `main` both mean the deployment's configured default pool.
Other valid pools resolve to `ork-pool-<slug>`. The reserved
`ork-engine-events` channel cannot be a bound default. A task's pool must be
resolved inside the engine; Temporal queue names must not leak into the user
contract.

The deployment's engine workflows use fixed internal channels: `RunWorkflow`
uses `ork-tasks` and engine events use `ork-engine-events`, regardless of the
`ORK_ORKER_QUEUE` override used for the default activity pool.

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
new runs only.

One Worker Deployment spans `ork` and `orker` for a release. They share
`ORK_DEPLOYMENT_NAME` and `ORK_BUILD_ID` and are released together. With
versioning enabled, `RunWorkflow` is Pinned, `IdempotencyClaimWorkflow` is
AutoUpgrade, and the projection worker on `ork-engine-events` is unversioned.
Versioning is off unless `ORK_BUILD_ID` is set; leave
`DefaultVersioningBehavior` unset when it is off. A version must drain before
its workers are decommissioned. A pinned workflow uses its own activity-worker
version only while that version has workers polling its activity channel;
otherwise Temporal falls through to that channel's current version. A task pin
(§7.1) is the one exception to that rule, and it is stricter rather than looser:
its session is opened through a version-routed activity, but its member
activities ride the session's own per-host channel, which is unversioned and
polled by exactly one process — so a pin stays on the process that opened it
across a rollout and never falls through to the current version
(`internal/engine/versioning_pin_integration_test.go`, `docs/DEPLOY.md`). Old
histories must replay against new code; `internal/engine/replay_test.go`
protects this contract.


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
machine, or expression runtime for these operations.

## 9. First-party activities

First-party activities must use exactly the same OCI contract as third-party
activities. Do not create a privileged internal plugin system. They should be
reference implementations of the public OCI contract.

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
  version's routing (§7.2).

Every execution must eventually reach either a correct durable state or an
explicit actionable failure. There is no documented third state such as stuck,
half-completed, unknown, or needing database surgery. Do not call OrkStack
production-ready until required failure and recovery scenarios are proven.

OpenTelemetry (`internal/telemetry`) is the only telemetry API in the
codebase — no per-vendor code anywhere else. Exporters are selected by the
standard `OTEL_*` environment, and everything is off by default: a deployment
that sets none of it runs exactly as it did before this package existed.
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
5. Run `make lint test`; for Temporal or storage changes, run integration tests
   against `make infra-up` (`make test-integration`).
6. Produce the evidence with `make proof` (see below) and quote it.
7. Commit with a Conventional Commit.

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

On a Linux host with plain Docker Engine, `host.docker.internal` does not
exist by default; add it to `/etc/hosts` as `172.17.0.1` and, if ufw denies
input by default, allow TCP from `docker0` (see the README's "Linux hosts").
This is host setup, not Orker behavior: the Orker must not add `--add-host`
or any host-network configuration to activity containers (§6). On macOS,
Docker Desktop provides the name and nothing changes.

### 11.2 Evidence

No proof, no done. A change is finished when its behavior has been observed,
not when its code reads right. `make proof` (`scripts/proof.sh`) produces that
evidence: it probes the prerequisites, then runs lint, build, the unit suites,
the integration suites against the real Temporal and MinIO with
`ORK_REQUIRE_INFRA=1`, and the activity contract fixture, keeping every log
under `proof/<name>/` with a README naming the commit, the working tree, and
each gate's outcome. `make proof-ui` adds the UI type-check, unit, and
Playwright suites; their screenshots and transcripts land beside the logs.
Integration tests that record a transcript or measurement write it under
`ORK_PROOF_DIR`, which the script sets.

`proof/` is gitignored and evidence is never committed: the repository keeps
the reproduction (the test, spec, or script), and the commit message or pull
request quotes the evidence — the proof directory name, the gates that ran,
and the lines that support each claim. Exit codes are 0 pass, 1 fail, and 77
prerequisite absent. A proof script probes prerequisites before opening any
output file, so an absence never leaves a file that reads like evidence, and
every run writes a fresh directory, so yesterday's output can never be
mistaken for today's. A change to behavior that only a browser can show
(rendering, live streams, an operator flow) is proven with a Playwright spec
and its screenshots, never with a description.


Parallel integration runs need a unique `ORK_TEMPORAL_NAMESPACE`; set the same
namespace and queues only for a deliberate two-replica test. Set
`ORK_ORKER_QUEUE` consistently on both binaries when a private task channel is
needed. Kill stray `/bin/orker` and `/bin/ork` processes before manual runs,
never kill `orkctl` by using an unanchored pattern, clean up processes afterward,
and run integration packages with `-p 1`.

For manual cleanup, use anchored patterns so `orkctl` is not killed:

```bash
pkill -f '/bin/orker$'; pkill -f '/bin/ork$'; sleep 2
ps aux | grep -E 'bin/ork|bin/orker' | grep -v grep
```


Before finalizing a merge, run `make merge-check` (`make build`, `make lint`,
`make test`, and `go vet -tags=integration ./...`) on the merged tree. When both
sides changed a file, resolve the union of behaviors against the merge base;
do not take one side wholesale. Rerun affected integration suites after a merge.

## 12. Repository layout and engineering conventions

```text
proto/orkstack/v1/     Proto contract; edit here first
 gen/go/               Generated Go; never hand-edit
cmd/ork/               Engine entrypoint (`bin/ork`)
cmd/orker/             Orker entrypoint
cmd/orkctl/            CLI entrypoint
internal/engine/       Temporal workflows and activities
internal/api/          Connect services and WebSocket bridge
internal/dsl/          YAML parse, validation, and Orkflow compilation
internal/orker/        Orker runtime and OCI activity dispatch
internal/task/         Temporal namespace, pool routing, and deployment identity
internal/config/       Configuration file schema and loader
internal/telemetry/    OpenTelemetry setup, sinks, admin listener and per-signal instrumentation
internal/archive/      Archived-run cleanup worker
images/                First-party OCI activity images; public filesystem ABI, no required SDK
pkg/artifacts/         Local and S3-compatible artifact storage
pkg/logstream/         Live task-log line framing
ui/                    SvelteKit, Vite, Tailwind, and generated TS clients
deploy/                Temporal, object-storage, and registry development stack
examples/              Example Orkflows
docs/                  Design and reference material
test/                  Activity contract fixtures and test-only helper tools
scripts/               Development helpers (`make up`, `make reset`)
```

Use Go 1.27, one module `github.com/orkstack/orkstack`, `gofmt`, `go vet`, and
wrapped errors. Prefer `context.Context` first and no panics in library code.
Connect serves h2c on `:8080` by default and the WebSocket bridge serves
`/ws/runs/{id}`. The UI uses Bun and Svelte 5 runes; generated clients live in
`ui/src/lib/gen`. The CLI uses Cobra and human output by default, with `--json`
for machine output. Configuration is a YAML file (`--config PATH`, one
shared schema owned by `internal/config` and documented in
`docs/DEPLOY.md` §1, with `deploy/orkstack.defaults.yaml` — generated by
`make config-defaults`, or `ork --defaults` / `orker --defaults` — as the
full, always-current reference for every key's built-in default), with
`ORK_`/`OTEL_` environment variables overriding
whatever the file sets, and sane localhost defaults — including
`ORK_TEMPORAL_NAMESPACE` (default `ork-main`), `ORK_ORKER_QUEUE` (default
`ork-tasks`), `ORK_ARTIFACT_BACKEND`, and `ORK_S3_ENDPOINT`.

Proto is the single source of truth for cross-process types. Never hand-edit
generated files. Regenerate Go and UI clients when the proto changes.

Agent ownership is split as follows:

- engine agents: `internal/engine`, `internal/api`, `cmd/ork`;
- Orker/activity agents: `internal/orker`, `images/`, `cmd/orker`;
- CLI agents: `cmd/orkctl`;
- UI agents: `ui/`;
- artifact agents: `pkg/artifacts`;
- coordinated changes: `proto/`, `go.mod`, `deploy/`, `Makefile`,
  `internal/telemetry`, `internal/config`, and this file.

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
- a Kubernetes operator;
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
