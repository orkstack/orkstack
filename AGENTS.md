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

One kind of workload does not exit: a **service** (§3.4), a container an
Orker keeps running and exchanges files with while it runs. An event source's
container is one. A service is held to everything else said of an activity
here: any OCI image, files only, no SDK.

OrkStack must not become a generic workflow engine, another state machine, or
another plugin framework. Workloads must not need a required gRPC or HTTP
callback, SDK, sidecar protocol, message queue, custom RPC framework, or
application-specific worker daemon.

A service image is not such a daemon: no workload needs one in order to be
executed, and what it exchanges with its Orker is the files of the Orker
Service Contract and nothing else: no socket, no RPC, no library.

```text
YAML Orkflow ──create─> ork ──compile──> Temporal ──dispatch──> Orker
                                                                    │
                                                               OCI image
                                                                    │
                                                              object storage
```

The `ork` engine, `orkctl` CLI, SvelteKit UI, Connect API, WebSocket bridge,
Orkers, and artifact store support this contract. They must not add another
durable execution authority. The UI is not a process of its own: it is a
static build embedded in `ork`, which serves it on the API's own listener and
origin.

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
subsystem, timer subsystem, or state-transition engine. The one named
exception to the retry subsystem is one kind of re-dispatch: when the
cluster's heartbeat timeout ends a task's activity, or its Orker reports a
reason of the closed interruption class the run pinned at submit (§4), the
interpreter starts that activity again, within budgets pinned in the run's
plan. The interpreter treats such a heartbeat timeout as infrastructure,
without proof that the workload did not cause it, which is why the losses it
re-dispatches are a small budget. The one spacing it owns is a Temporal
Timer, the plan-pinned wait after a loss, and the same Timer's doubling,
plan-pinned back-off before it re-dispatches a task handed back before it
ever started once that task's never-started budget is spent; every retry of
a task's own failures, its spacing and its back-off stay Temporal's, and the
interpreter never re-runs a task for its own failure and never adds a reason
to the class without a change to this file. Cron scheduling
must use Temporal Schedules. Interaction with a running Temporal workflow must
use Temporal Signals, including approval, external completion, resume, and abort.
The two exceptions are a cancel request and a force cancel's termination (§7,
Force cancel); neither carries data into the workflow. A resume of a finished
run is not interaction with a running one: it is one reset of the run's closed
execution, and its plan travels as the reset's reason, a key naming an
immutable record written before the reset, which the reset run loads and
verifies at its reset point within its plan's restore timeout, or the fixed
`resume_plan_load_timeout` of 5 minutes when its plan pins none
(`docs/STORAGE.md`).

One record is the stated exception: for a task that holds an identity,
OrkStack records which attempts have held one, and refuses an identity to an
attempt beyond the number of attempts the run's admission records for the
task, or past the deadline that admission gives it. It is a record, never a
lock. It decides only whether an attempt may hold an identity: never whether
a task runs, is retried or has ended, which stay Temporal's.

A concurrency limit is held per namespace and key by a Temporal workflow of its
own (the concurrency gate), which exists only while the key has runs holding or
queued for its slots, as an idempotency key is held by its claim workflow: those
runs and tasks are that workflow's state, every change to it is serialized by
Temporal, and a run waits for it on its own Signal and Timer. It decides only
when an accepted run or task may begin, never where or by whom work is
dispatched; no OrkStack process holds any of its state, and a lost record is
rebuilt from the runs' own records in the cluster. It is the one permitted
semaphore, not a lock service for anything else. Its gauges come from the
concurrency census, a Temporal Schedule whose workflow reads the gates and
reports; it decides nothing.

### 2.2 Object storage owns data and artifacts

Temporal history and payloads contain only small structured metadata and
references. The hard maximum serialized payload size is **256 KiB**; a task's
activity input may exceed it by exactly its run context, a fixed reference of
at most `RunContextMaxBytes` (512 B, pinned per run), which no user-controlled
byte can use. Anything larger must be an Artifact. Never put large bytes in Temporal payloads or the
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
immutable activity completion and per-attempt records, each run tree's
immutable run-context object, what its tasks may read about their runs, and
a fan-out's immutable line indexes. The completion records of a run whose
results the engine records (§4.1) lie under a prefix an Orker's credential
may read and may not write. Who gave an awaited task's resolution, and why one
was rejected, lie with the run's saved state, where an Orker's credential does
not read. It holds each
event source's document, keyed by its namespace and name, and the immutable
service specs written under the source's id. It also holds the immutable
receipts of the messages event sources handled. A message's body and metadata
reach a run only as artifacts: the Orker that hosts the source stages them
under a prefix that belongs to no run, and the engine, once it has admitted the
run, copies them inside the store to the run's own key before the start that
references them (§4.1's order). No caller and no Orker hands a run a reference
or writes under a run for a message. The start carries only those references
and, when a source declares a mapping, the small values it copies into the
Orkflow's declared inputs (16 KiB by default, bounded by the deployment). A
message is bound to a run id, and to nothing else, by a write-once claim object
the engine creates with a conditional put and never rewrites, with a second
write-once object beside it once that run has started: records the store
arbitrates, as a commit is, never locks. Nothing is decided from a claim's age.
Temporal still decides whether a run exists while it holds it, and the run's
archive after that. A receipt is a record and never a read path for a decision.
It also holds what
decides access and identity, where only an engine's credential should reach:
the access document (roles, bindings, service accounts) and each namespace's
identity policy, with an immutable revision per change; the identity issuer's
published public keys; the authentication mode the deployment runs; sign-in
records; and, with each run's saved state, its admission record and the record
of the execution that started it, both written once and removed only with the
run's own objects. None is execution state, and the presence of none is read as
a run being alive: Temporal alone says whether a run is open and who holds its
attempt. None holds a private key, a task's token or a secret value; a sign-in
record holds its identity provider's refresh token, encrypted. Object-store data is
not mutable workflow execution state (Temporal workflow state) and must never
become a second state machine. Temporal remains authoritative for execution state.
The record of which attempts of a task have held an identity (§2.1) is one
create-only object a slot, never changed; it moves nothing.

Where task identity is on, every Orker's object-storage credential must
reach only what an Orker writes (artifacts, logs, commits, attempt records,
memos, probes) and, to read and never write, the results an engine attested
and the run contexts: no definition, namespace, setting, schedule or
event-source object (`metadata/schedules/`, `metadata/event-sources/`), nor
anything of a run's saved state or under `metadata/identity/`. The store
enforces that.
Each Orker tests its own credential at boot and hourly and reports the
result, and the check never pauses an Orker; by default
(`identity.unscoped_orkers: refuse`) the engine refuses a submission that
would hold an identity while any live Orker's credential is not confirmed
scoped. The engine's credential is not scoped, and whoever holds it
administers the deployment. Task identity therefore needs a store that
scopes a credential by prefix, which the local backend is not. An Orker's
credential still reaches the results of every namespace: one bucket is one
trust domain for results.

An Orkflow keeps its existing user-facing `name` and `version` identity, but
its canonical definition also has a SHA-256 digest. The immutable definition is
addressed by that digest, and every execution records both the pinned version
and the definition digest. A document that pins no `version:` is given one when
first created, a UUIDv7 in lowercase base32; a write-once object beside its
versions maps the document's content to it, so creating the document again stays a
no-op on every replica (`docs/STORAGE.md`). That object is a record, never a
lock, and a version stored under its content hash by an earlier release answers
the same lookup.

Artifacts are references, not bytes. The Orker materializes and uploads them
through `pkg/artifacts`. A run may reference an object another run of its
tree produced or received, and no user surface can supply a reference: the
engine resolves one from a recorded result, and writes the marker that claims
the object for a run before any record of that run references it. The production artifact backend is shared object
storage (S3-compatible storage); local filesystem storage is for local use.
Production infrastructure should consist primarily of Temporal, S3-compatible
object storage, OrkStack services, and Orkers. Additional durable systems are
not OrkStack requirements.

Artifact bytes of a retired run are reclaimed by the retention cleanup, when it
is enabled and its GC is set to delete, once the run that produced them and
every run that claims them have been retired for longer than the GC grace, and
every run created before the latest of those retirements, plus a margin, has
finished and been archived (`docs/STORAGE.md`, "Reclaiming space"). The GC
starts in dry-run, never takes a lock, never delays a retirement, keeps between
passes only a process-local low-water mark, a cursor and the write-once GC
records, decides that a run is retired only from its current archive body, its
namespace's retention, its resume record, its empty commit and attested
prefixes and the cluster, never from a sidecar alone, and on the local backend
reclaims nothing.

### 2.3 OCI owns computation

The OCI image is the computation ABI. An activity is not a Python, Go, or Java
plugin, a Temporal Activity SDK integration, an OrkStack SDK integration, or a
custom worker service. Any OCI-compatible image should be executable. An image
may know about OrkStack but must not need to know about it.

The Orker bridges Temporal activities and OCI execution. It also runs services (§3.4): an event source's container is started once and kept running. Activity containers
must not receive Temporal credentials or APIs, object-storage credentials, or
OrkStack control-plane credentials unless a user explicitly supplies them as
workload secrets. The Orker is the security boundary.

A task identity token is a credential OrkStack issues to one attempt,
because its task declared one. It names the attempt, expires within hours,
and is accepted by no OrkStack API: it is not a control-plane credential.

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
the Temporal payload, the Connect API, `orkctl -j` and the UI's typed input
editor share (`docs/JSON.md`), so a copied CLI command reproduces exactly what
was submitted, and it has none of YAML's silent coercions (`no`, `on`,
octal-looking numbers). Documents a person authors — an Orkflow, an automation
document, an activity interface — stay YAML, and each begins with
`apiVersion: orkstack/v1` and its `kind:` (`Orkflow`, `Automation`,
`ActivityInterface`, `EventSource`, `ServiceInterface`). Inside one, runtime data is written as YAML
too — a task's `with:` (an http `body:`, jq `vars:`), an Orkflow's `inputs:`
defaults, an await's `schema:`, a schedule's `input:`, an activity
interface's `required:` and `default:` — and becomes JSON only
through the strict mapping (`docs/JSON.md`, "Data written in an authored
document"), never a lenient decode: a plain scalar that YAML readers resolve
differently (`yes`, `0755`, `1:30`, a date), a number JSON cannot hold
exactly, and tags, anchors, aliases and merge keys are refused, naming the
path. Do not add a YAML input mode to the UI or CLI.

A message an event source receives (§7) is opaque bytes, handed over by its
container as a regular file. Where a source reads it to match or map, the
Orker reads it as one JSON value under the same strict rules (a second value,
a key written twice and invalid UTF-8 are refused), never coerced, and the
stored artifact is always the original bytes. A service image's interface is
an authored document of its own kind, `ServiceInterface`.

### 3.2 Exit status and logs

Exit code `0` means computation succeeded; the Orker then validates and commits
outputs. Any non-zero exit means the activity failed. Retry behavior belongs to
Temporal workflow policy, not magic exit codes such as 42, 43, or 44. A container
its runtime stopped from outside (its daemon shutting down, an operator's `stop`)
did not exit on its own, so its code says neither: it is an `InfrastructureError`
(`CONTAINER_STOPPED_BY_RUNTIME`), told from what the runtime observed, never from
the number. It is an interruption (§4) while the Orker was stopping, or when the
Orker observed its runtime go away during the attempt, its connection to the
runtime ending as the runtime stopped the container; an operator's `stop`
through a runtime that stays up is neither. Retry belongs to that policy, and
an interruption's re-dispatch to the interpreter (§2.1); the exit code never
decides either.

A standing service has no computation to finish, so its exit code says
nothing of success: any exit its Orker did not ask for, `0` included, ends
that attempt as a failure (`SERVICE_EXITED`, `SERVICE_FAILED`), and whoever
owns the service starts it again.

stdout and stderr are independent logs. Capture them independently, stream them
when possible, retain them in object storage, and associate them with the
activity attempt. Where the Orker resolved a secret for the attempt, each
occurrence of its value in either stream is replaced by `***` before anything
is streamed, retained or exported, unless the secret or the pool's setting
says otherwise. It is a guard against accidental disclosure and not a
boundary: `docs/SECURITY_MODEL.md` states what it cannot catch. Plain text
must work without a logging SDK. Structured JSON may be enhanced later, but
is not part of the required ABI.

### 3.3 Cancellation and heartbeats

Temporal cancellation must reach the container:

```text
Temporal cancellation → Orker receives it → SIGTERM
                     → wait 30 seconds → SIGKILL if still alive
```

The 30-second cancellation grace period is the default. Ordinary Unix signal
handling must be enough for cleanup; no custom cancellation API is required.

A service is stopped the same way, after its Orker has said so in a file
and finished its own side of each open exchange; on SIGTERM the container
ends the rest as its profile says and exits within the stop bound its
context file gives. A container that cannot be stopped does not hold
whatever owns it: after a stated bound the Orker reports it and takes no
further service until it is gone.

An Orker that is itself stopped accepts no new work: its first SIGTERM or
SIGINT makes it ask for none (a cordon whose cause is stopping, §7), and a pin
creation that still reaches it is refused before it opens, while running
tasks, the tasks its open polls still deliver and pins it opened before the
signal carry on for its stop grace (`orker.stop.grace`). Only then does it
stop what remains, and from then it refuses an open pin's next member. A
second signal ends the grace; a third exits at once.

An Orker that is itself stopped stops what it runs carefully: an unpinned
attempt whose container has not ended by itself gets the cancellation
sequence, heartbeated throughout, and the Orker reports it `ORKER_STOPPED`
itself before it exits. An attempt whose container ended by itself, or that is
adopting or writing a commit, completes and reports first. A pinned member
still running fails its pin `PIN_HOST_LOST`. A signal sent to the Orker's
process group never reaches a task's container.

The Orker should heartbeat the Temporal Activity while the container exists. A
heartbeat the cluster does not answer is not a cancellation: only an answer ends
an attempt, until nothing has answered for the heartbeat timeout.
The container must not implement Temporal heartbeats or a heartbeat IPC
protocol. Application hangs are handled by the activity timeout. A structured
progress mechanism is a possible later extension, not a current requirement.

A service is the one exception, and it is not a Temporal heartbeat: a
standing service says whether it is ready and healthy in a status file it
rewrites, and reads its Orker's own beat from a file, ending itself when
that beat stops, so that a container whose Orker is gone does not run on.
An activity implements neither.

### 3.4 Services

A service is a container an Orker starts from a service image and exchanges
files with while it runs. Treat the Orker Service Contract as a versioned
ABI beside the Activity Contract, with one profile per kind of service (an
event source; a credential or secret provider). An image names the contract
and profile versions it implements in labels, and an Orker runs it only
under versions both have. A service either stands until its Orker stops it,
or is started for one exchange and exits; its profile says which.

A service and its Orker exchange files and nothing else, in two mounts: one
the Orker writes and the container can only read, one the container writes
and the Orker only reads. The second is a bounded memory filesystem of the
service's own, sized so that every file a container replaces fits twice.
Each **lane** is one strictly sequential conversation; the
Orker writes the first step of every exchange and the container never starts
one; a file has a fixed name, is written and then renamed into place, and
says which exchange it belongs to; neither side deletes. The Orker opens
what a container wrote without following a link, copies it once, and uses
only the copy. There is no socket, no stdin and no data on stdout, and a
service has no `values.json`, no commit and no result.

A container never handles the material of two namespaces. An Orker hosts a
service only while it can give it that bounded filesystem and a network of
its own from which a probe finds that no instance-metadata endpoint
answers, nothing on the host's loopback can be opened, the cluster serves
no caller that presents no credential, and no other container can be
reached, unless its operator accepted one of the first, third or fourth in
its configuration, each by its own setting (§6). An Orker that finds one
of them otherwise while it hosts services stops hosting them. A standing
service must end itself when its Orker's beat stops: that is the image's
duty, which the contract test checks, because nothing of OrkStack is left on
a host whose Orker was killed. Bash must be able to
implement every profile with one loop per lane and no SDK; the Bash fixture
of each profile is mandatory.

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
exactly one attempt of its own. An **interruption** is an attempt ended by an
infrastructure fact of its run's pinned interruption class: its Orker handed
it back (`ORKER_STOPPED`; `CONTAINER_STOPPED_BY_RUNTIME` while the Orker was
stopping, or when it observed its runtime go away during the attempt, its
connection to the runtime ending as the runtime stopped the container), handed
it back before it started (`ORKER_STOPPED`, and the faults
`ATTEMPT_STORE_UNAVAILABLE`, `RUNTIME_STATE_UNKNOWN` and `PROFILE_DRIFTED`),
or was lost (the cluster's heartbeat timeout). It is not one of the task's
own attempts: the interpreter runs the task again (§2.1) under budgets pinned
at submit, one for hand-backs, a smaller one for losses with a cap per run
counted in lost Orkers, and one for tasks handed back before they started;
once the hand-back or loss budget is spent, or a fault before the start
repeats past its own small budget, that class spends the task's own
attempts, exactly as before, and fails with its real reason when they are
spent; a task handed back before it started because its Orker was stopping
never spends one. After a loss the interpreter first waits, until an instant
it records, the task's cancellation grace plus the heartbeat timeout, which
fences a live Orker that lost the cluster but not one killed while its
container runtime survives; a hand-back is always fenced and waits for
nothing. The never-started back-off doubles from its plan-pinned first wait
to at most 60 s, a ceiling this file fixes. An interruption never applies to
a pinned member (`PIN_HOST_LOST`, §5), to `TimedOut`, or to a reason outside
the class; a task may opt out (`retry.max_interruptions: 0`); and its re-run
repeats external side effects (at-least-once, under the same idempotency
key). Keep these failure categories distinct:

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
history record that references it, the same order as a commit. In a run whose
results the engine records, that object is the record the task's result is
taken from once the run applies it, and the history record only announces it. An attempt's
claim and its release marker are protocol objects that `ResumeRun` reads only
to raise the unknown-attempt warning; its attempt record stays a record.

Use a deterministic commit key. At retry start, if a valid `commit.json` exists,
return its committed result and do not execute the container again. This only
prevents duplicate container execution after OrkStack has durably committed the
result; it does not make arbitrary external side effects exactly once. The one
exception is an Orker's namespace guard (§5): an Orker that does not serve the
task's namespace reads no commit of it, so it fails the attempt explicitly
and terminally with `NAMESPACE_NOT_SERVED`, a `ContractViolation` that is
never retried. The commit stays where it is, durable and undeleted, and its
result never becomes pending again (§15).

**A run whose results the engine records.** A run that holds a task
identity, or belongs to a run tree in which one does, does not have its
Orkers write `commit.json`. An attempt of such a run asks the engine
before its container starts whether it may commit, so an attempt the
engine would refuse runs nothing. After step 4 the Orker hands the
manifest to the engine, which writes it, create-only, where no Orker's
storage credential can write, and only for an attempt the cluster
reports as held by the caller, at the pool the run was admitted with for
that task, dispatched as it was admitted. That write is step 5 and the
completion boundary, and "a valid `commit.json`" then means one an
engine wrote: an Orker adopts, takes a memo hit from, and stages a
container task's results from such records only, and ignores any other
object. An engine that cannot be asked is treated as object storage that
cannot be written: the attempt waits for a time fixed when the run was
submitted, then looks once more for a record the engine may have written
without being heard, and only if there is none fails
`InfrastructureError` (`COMMIT_ENGINE_UNAVAILABLE`) and is retried under
its policy. It never reports success without the record and never writes
around the engine. A result uploaded and not yet recorded when its Orker
is lost was not committed: the retry runs the container again.

In such a run the engine also records an awaited task's resolution and
a composed task's values, each create-only where no Orker's storage
credential can write: a resolution when the API accepts it from a
caller it authenticated, before the run is told, and the first one
recorded stands; a composed task's values from the record of its child
run's outputs, which the engine writes from the child's recorded
results. The run applies what the record says. The artifacts a child
returns are in its outputs' record and in the composed task's, and an
attempt stages what its run received only as the engine answers it,
from the records of the tasks that produced it. An artifact a failure
handler passes on from a need that failed is staged as the cluster
carries it, only for an input the run's admission names as a handler's
and whose producer has no record; every other input with no record is
refused. A resolution that
reaches such a run with no record is not applied: it is an event of the
run, and the task goes on awaiting. An engine writes such a record only
from what it checked itself, a caller or other records, never from what
a run's history says. Who gave a resolution, and a rejection's reason,
lie where only an engine reads. An attempt of such a run is given no
identity and has no result recorded, and on an honest Orker starts no
container, until the tasks its own task follows have their records:
every task it needs under the default join, and under `none_failed`
each need that always runs. An attempt that asks earlier fails
`InfrastructureError` (`REQUIRED_RESULT_NOT_RECORDED`) and is not
retried. A store that does not answer is never read as a record that
is missing. A composed task or a child run's end whose record the
store does not answer fails, with a reason that says the store could
not be read. An awaited task whose record could not be read when the
run was told of it stays open: it is read again when the run is told
again and at its deadline. At its deadline it takes the record when
the store made one in time; when the record cannot be read there it
is read once more, a time later that was fixed when the run was
submitted, and if it still cannot be read the task fails with such a
reason and never as timed out. A run that must record another event
while object storage refuses it ends as any run does then (§11).

A cached task's memo is advisory. A hit writes this run's own `commit.json`
referencing the source commit's content-addressed objects and starts no
container; every doubt about a memo (unreadable, mismatched, missing object,
timeout) is a miss that runs the task; the memo step fails an attempt only
when its own commit write fails. Deleting any memo changes no result.
Caching is the author's assertion that a task is safe to reuse; OrkStack
cannot verify it. In a run whose results the engine records, a hit is taken
only from a result an engine recorded at the same pool; any other memo is a
miss.

An accepted execution must not disappear, with the one exception §15 states. A
durably committed activity result must not become pending again. Object-storage
failures must not silently produce success. Every terminal failure must have a
machine-readable reason and a human-readable explanation.

## 5. Image resolution and Orker runtime boundary

Image tags may be accepted as input, but the Orker must resolve each image to a
digest before execution and record the immutable reference, for example:

```text
ghcr.io/acme/migrate@sha256:...
```

A running execution must never silently switch images because a tag moved.

The deployment's image policy (`orkctl admin image-policy`: `require_digest`,
`allowed_registries`, cosign public `signing_keys`) is a floor; a namespace may
only tighten it (`require_signed_by`), never loosen it. It is enforced when
an Orkflow is created, at submit, and by the Orker at dispatch against the policy selection
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
poll Temporal, within its declared capacity
resolve the activity specification
check the completion commit
check the run's image policy
check a cached task's memo
index the lines of an artifact a fan-out reads, when the engine asks: while
  committing the producer's artifacts, or as an internal activity of the
  fan-out's pool that starts no container
check the task against this Orker's own policy, once no commit or memo answers it
  (the exact order, cached and not: docs/OCI_ACTIVITY_CONTRACT.md §7.1)
check the task's profiles against this Orker's and the run's, after the
  commit check and before the memo step
resolve the OCI image and digest
bind a `gpu` profile's units, after the image is pulled and before the
  container is created
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
write the completion manifest, or hand it to the engine (§4.1)
return a compact ActivityResult
cleanup
```

For a service it hosts, an Orker is also responsible for the conversation:

```text
confirm with the engine what it is about to run, and again at intervals
check the image policy it was just given, and its own policy
read the image's contract and profile labels
create the two conversation mounts and the service's own network
obtain the service's secrets, identities and credentials, and keep them fresh
wait for the service's status
write every first step and its own beat; read every answer without following a link
heartbeat Temporal with the service's status, from the loop that checked every lane
stop the service when it is cancelled, cordoned, stopping, or no longer holds it
report a service it cannot stop, and take no other until it is gone, which it finds again at boot
record the attempt
```

An Orker that declares a capacity (`orker.capacity`) or a `gpu` profile admits
no task that would take its reservations above it. It reserves before it polls
the largest request the channel it polls can carry — cpu, memory and GPU units
— trims that reservation to the task's resolved request once the task is in
hand, and frees it when the attempt's container is removed, or when the
attempt ends without one. A pinned member is counted but never made
to wait, because it can run nowhere else; while the reservations stand at
or above capacity, the Orker polls for no new task. GPU units are bound, not
only counted: a task is given its whole set of units, each held by no other
task, and a pinned task holds none. One Orker on a host holds its `gpu`
profile at a time; another waits, paused, until it exits. The ledger is
process-local, exactly as a cordon is: never stored, never shared, gone
with the process. It is admission, not placement: Temporal decides which
polling Orker receives a task, and OrkStack never chooses a host. A line
index the Orker builds is admitted like a task, holding one activity slot
and the fixed reservation `docs/DEPLOY.md` states.

Every Orker of a pool has the same settings — caps, default request,
capacity, `max_concurrent`, secret schemes and floor, `env` allow-list,
registry credentials, runtime, pin hosting, profiles, stop grace, log masking, the
namespaces it serves, whether a secret may be a task's result — so a run never fails because of
which Orker took a task. Where they differ, a task fails explicitly with its
own reason. A committed result is never refused for any of them but the
namespaces an Orker serves (`orker.namespaces`), which it checks before it
reads anything: a task of a namespace it does not serve, or whose input names
a commit of another namespace, fails `NAMESPACE_NOT_SERVED`, a
`ContractViolation` that ends the task and is never retried, and the Orker
neither reads nor deletes any commit of it (§4.1). Unset, an Orker serves every namespace. A memo hit
is never refused for the caps, secret schemes, floor or runtime, but a cached
task is rendered, its image policy checked and its profiles checked before its
memo lookup, so a differing `env` allow-list, registry credentials or profile
can refuse it even where the memo would hit (`docs/DEPLOY.md`, "Every Orker of a pool has the same
settings").

A device or gpu profile may differ between Orkers of a pool while a roll changes it, under contract labels: a run pins the contract its image declares, or, when it declares none, the labels every Orker of the pool that answered at submit serves; an Orker serves it with a contract that owns a pinned label with a definition submit saw, or by the operator's `accepts:`, and an Orker that serves none fails the attempt before any container, a never-started fault.

Docker- and Podman-specific details live in the runtime files
(`internal/orker/runtime_docker.go` and `runtime_dialect.go`) and nowhere else:
do not spread them through unrelated OrkStack components. Podman is a dialect
of the one runtime, a value of spellings chosen by the CLI setting and the
engine it answers, never an interface or a plugin. Keep the OCI runtime
boundary clean enough for future containerd, Kubernetes Jobs, ECS Tasks, or
Nomad implementations. Do not implement all of those runtimes now; portability
is a boundary, not a mandate for a framework.

## 6. Security and resource controls

Executing arbitrary production activities is dangerous. Do not permit:

```text
--privileged
host PID namespace
host network
arbitrary host path mounts
Docker-socket mounts
Orker-credential mounts
capability additions, device cgroup rules, or a device an Orkflow names by path
```

Profile kinds are a closed set compiled into the Orker (`devices`, `gpu`), as
`cosign:` is (§5): a profile is data an operator writes, never code. A task
reaches a host device only through a **device profile** the Orker's operator
defined; an Orkflow names the profile, never a path, flag or CDI name. Only an
Orker that runs on its container daemon's own host offers a device profile,
and it proves that at boot and periodically. A device is a character device
of a class the Orker's compiled allow-list names.

A GPU reaches a task only as a counted, exclusive unit of a `gpu` profile,
through CDI. A CDI spec is operator data read only from directories root
alone can write, and its owners hold the trust of the host's root; it may
pass a unit's device nodes and groups only, and a hook, an environment
variable or a mount in it is refused. The one exception is an `nvidia.com`
spec (experimental, proven by emulation and not on NVIDIA hardware), admitted
only under the compiled grammar of the hooks, env and mounts NVIDIA's
generator writes, and only for a hook binary that is a row of the compiled
table of toolkit builds. The spec bytes and device nodes of a
profile the Orker audited stay fixed for its lifetime: any change is detected
before a container is started, is drift, and is never adopted; only a restart
audits again. No device grants a capability or `--privileged`, and the
mandatory profile is unchanged.

Prefer `no-new-privileges`, default seccomp, resource limits, isolated working
directories, and rootless execution when supported. Initially support only
network modes `default` and `none`. Leave named, Orker-defined network profiles
for later; an Orkflow must not define arbitrary host-network configuration.

A service's container is put on a network of its own, never the runtime's
shared one: a NAT'd bridge named `orksvc…`, IPv4 only and cut off from other
networks, with no other container on it. It is still network mode
`default` to whoever writes the service, and it can reach whatever the
host's network routes to. An Orker hosts services only while a probe from
such a network can open no instance-metadata endpoint, cannot open the
Orker's own admin listener, and is not served by the cluster, on any of
its ports the Orker can know, in plaintext or over TLS, when it presents
no credential: refused, timed out, turned away for want of a client
certificate or answered as unauthenticated, and nothing else counts. The
cluster's membership ports, which authenticate nobody, must not be
reachable from there at all. OrkStack's own authentication does not cover the cluster: a
long-lived container that could call it freely could act as any worker of
the deployment. An operator may accept a
reachable metadata endpoint (`ORK_ORKER_SERVICES_ALLOW_METADATA_REACH`) or
a cluster that asks its clients for no credential
(`ORK_ORKER_SERVICES_ALLOW_OPEN_CLUSTER`) in so many words, each of which
every boot logs. The probe also asks whether a container on another
service's network, or on the default one, can be reached, which is the
host firewall's doing and not the network's
(`ORK_ORKER_SERVICES_ALLOW_CONTAINER_REACH` accepts it), and whether
anything on the host's loopback can be opened, which nothing accepts. A
host that answers any of these open, twice, while it hosts services evicts
them as a cordon would; a probe that could not be run evicts nothing. A
service's secrets and its conversation with its Orker are held in memory,
and out of swap where the host allows it; its container cannot dump core.

Activities may request resources such as:

```yaml
resources:
  cpu: "2"
  memory: 4Gi
```

Support activity-level `timeout` and `cancellationGracePeriod`. Orker policy
caps requested resources, and an Orker that declares a capacity admits a task
only within it (§5). Workflows must not receive unlimited resources by
default. Put complicated execution logic in a container instead of adding
platform primitives.

Every Orker of a deployment writes results to one bucket, and its Temporal
credential can poll any pool, so namespaces and pools divide a deployment
among cooperating teams and are not a boundary between them. A trust zone
(production and development, or tenants that must not reach each other) is a
deployment of its own, with its own bucket, credentials, Temporal namespace,
engines and Orkers. An Orker's namespace list (§5) guards honest Orkers
against wrong routing; it is not that boundary (`docs/DEPLOY.md`, "Trust
zones: a deployment per zone").

Secrets come from secret references or explicitly supplied workload secrets,
never committed source. `.env` is gitignored. Shell commands must not
interpolate untrusted input without the templating layer's escaping. An `env:`
value is stored in the definition, so it is configuration and never a secret;
no `env:` value grants a credential, privilege or host access. Which
secret sources a task may name is bounded by its namespace's
`allowed_secrets` (checked when an Orkflow is created and again at submit) intersected with each
Orker's `ORK_ORKER_SECRET_ALLOW` floor (checked at dispatch); both bound names
and never store or compare a value. No floor admits the Orker's own sources —
its `ORK_*` and `OTEL_*` settings, its process and devices, its work root, its
configuration and the credentials it and its container CLI use, under their
own names or through a link that leads to or through one of them — and a
`file:` source is a plain absolute path of at most 4096 bytes: the Orker
refuses any other at dispatch, empty floor or not, and an operator gives a task
such a value only under a source of the task's own. A bearer credential
OrkStack mints for a caller (a trigger token, an await callback token) is
disclosed once and stored only as a salted hash.

A credential of the cluster's namespace is trusted with everything Temporal
carries: what a run does next, a Signal and so an approval, every event,
every Orker's channel. For a run whose results the engine records, a Signal
gives no approval, and a task started ahead of the tasks it follows is given
no identity and no recorded result (§4.1). By default each Orker holds one. A deployment may
narrow an Orker's on a cluster that can; OrkStack neither requires that nor
depends on it for anything it claims.

Task identity exists only where callers are authenticated (`auth.mode:
enforce`); a deployment that uses it is expected to reach the cluster with a
credential. Elsewhere the engine issues no token: a token's subject is only
as trustworthy as the permissions that produce it.

With `auth.mode: enforce` every request is authenticated and every RPC
requires a permission (`docs/ACCESS.md`). Roles and bindings are one document
in object storage, changed by compare-and-swap; each replica decides from a
copy at most `auth.policy_refresh` old, serves it stale for at most
`auth.policy_stale_limit` when the store cannot be read, and then fails
closed. An engine does not start in a lower mode than the deployment runs; the
mode is lowered by a command, never by one replica's configuration. A
service account stores public keys only, and an assertion it signs
authenticates one request and no other.

A decision the API made that an engine hands another engine through the
cluster, and the record of a run the engine ended, are signed with the
engines' signing keys and checked wherever they are acted on, read or
attributed; one that does not verify authorizes nothing and names nobody. A
deployment that verifies its callers holds such a key.

An event source starts runs from what a container reads, so the service
Orkers call for it answers only an Orker's credential, and no instance
runs, unless the API enforces authentication. An operator whose gateway is
the authentication may accept unauthenticated callers of the service
people call (`ORK_EVENT_SOURCES_ALLOW_ASSERTED`), which every boot logs:
sources can then be written, read and evaluated, and consume nothing.
Where callers are authenticated, whoever writes or resumes an event source
must also be allowed to submit the Orkflow it binds, and an Orker offers
messages only for a pool it holds that right at.

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
validate, create and plan, naming the count, the limit and the setting, and
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
(§11), so a change that raises the default quotes a nightly-tier proof that ran it —
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
- **Queue**, on a user surface, means only a concurrency limit's ordered line
  of accepted runs or tasks waiting for one key's slot (a run is *queued*); a
  Temporal task queue stays internal and is never named on any surface, and
  `pool:` remains the only selector of which Orkers run a task.
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
- **Profile** means a named, operator-defined Orker setting a task selects by
  name: a device profile (`devices:`, kind `devices` or `gpu`). A **unit** is
  one exclusively bound element of a `gpu` profile, identified by its 0-based
  position in the profile's list.
- **Profile contract** means one labelled definition of a profile, which a
  run pins and an image may declare (`devices:` in its interface); always
  written in full, it is unrelated to the OCI Activity Contract.
- **Pin host** means an Orker configured to accept pins
  (`ORK_ORKER_MAX_PINS` above zero). Every Orker still runs ordinary tasks;
  only a pin host runs pinned ones.
- **Fan-out** means a task declaring `each:`: one DAG node that runs one
  **item** per element of an array, or per non-empty line — or per batch of
  K consecutive lines (`batch:`) — of an artifact a task it needs produced
  (`over_artifact:`). An item is identified by its 0-based
  index, never a string; `convert[3]` is a rendering only and nothing parses
  it.
- **Composed task** means a task whose `uses:` is `orkflow:<name>@<version>`.
  It starts a **child run** of that Orkflow and waits for it; the run that
  started it is the **parent run**. **Outputs** are an Orkflow's declared
  return contract (`outputs:`). A composed task passes files to its child only
  as `artifacts:` bindings of the child's `type: artifact` inputs: references
  the engine resolves from recorded results, never bytes, staged only for the
  child's tasks that name them (`_inputs/<name>`).
- **Shared Orkflow** means an Orkflow whose definition lists, in
  `shared_with:`, other namespaces or other namespaces' Orkflows that may
  compose it (`uses: orkflow:<namespace>/<name>@<version>`); the namespace
  that owns it is its **owner**. Composition is the only way a run uses
  another namespace's Orkflow: its child run executes in the parent's
  namespace under that namespace's rules, and a submit, schedule or trigger
  never starts another namespace's Orkflow. The decision is
  `internal/authz.Authorize`'s alone, as is every decision whether a caller
  may do something.
- **Awaited task** means a task declaring `await:`: it runs no container and
  waits until it is **resolved** from outside the run (completed with values
  or rejected with a reason) or its deadline passes. A **callback token** is
  the one-time bearer credential an `await:` with `callback: true` is issued
  at submit.
- **Interruption** means an attempt ended by an infrastructure fact of its
  run's pinned interruption class (§4); the task runs again without spending
  its own attempts, within its run's budgets.
- **Sleeping task** means a task declaring `sleep:`: it runs no container and
  waits a fixed time on the run's own Temporal timer, then succeeds with no
  values; nothing outside the run resolves it. It is `SLEEPING` while it
  waits, and like an await it crosses a continuation with its absolute
  wake-up time.
- **Paused run** means a run that starts nothing new until it is unpaused:
  what is running finishes, its child runs are paused with it, and its
  status is `PAUSED`. A pause lasts at most its timeout; a run still paused
  then fails `RUN_PAUSE_TIMEOUT`, or keeps its own cause if it was already
  failing.
- **Engine-recorded result** means the completion record of a task of a
  run that holds a task identity anywhere in its run tree: written by the
  engine at the request of the Orker that holds the attempt (§4.1), never
  by an Orker. In such a run an awaited task's resolution and a composed
  task's values are engine-recorded results too, written by the engine itself.
  A task **follows** the tasks whose records it needs before it may start: its
  needs under the default join, and under `none_failed` those that always run.
  User-facing text says an approval is "on record", "not on record", or that
  its record "could not be read", which is never the same as "not on record".
  User-facing text says a run's results are "recorded by the engine".
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
- **Event source** means an object bound to one Orkflow of its namespace and
  one **source image** with its settings, which starts one run per
  **message** it accepts. Its name is fixed when it is created. It is a
  different object from a **trigger** (inbound HTTP) and a **schedule**. A
  **message** is one unit a broker delivers to it and is never called an
  event on its own: "event" stays a run's event, and where the word could be
  read either way each is named in full. A **message delivery** is one
  handing-over of a message by its broker, always written in full:
  "delivery" alone stays a webhook's. It is **paused** by an operator, or by
  OrkStack itself when a start is refused for a reason that is the source's
  or the Orkflow's, when every new message it receives is rejected, when its
  broker refuses its acknowledgements or, while answering everything else,
  confirms none of them for long, or when what it runs fails or is no longer
  permitted in a way trying again cannot change; a pause is stored with its
  code and reason and lifted only by a person. A broker that does not
  answer, and a container that keeps crashing, never pause one. A change to
  what its container is replaces its instances one at a time and only where
  the new one works; a change that does not work leaves the last one that
  did running.
- **Service** means a container an Orker starts from a **service image** and
  exchanges files with under the Orker Service Contract (§3.4). A **service
  profile** is what one kind of service exchanges with its Orker; a **source
  image** is a service image of the event-source profile. "Profile" alone
  stays an Orker's (its devices).
- **Instance** means one running container of an event source on one Orker;
  a source runs `instances` of them, each a competing consumer of the same
  broker object. A **lane** is one sequential conversation of an instance
  with its Orker, a contract word.
- **Source host** means an Orker configured to host instances
  (`ORK_ORKER_EVENT_SOURCE_MAX_INSTANCES` above zero) and fit to (§3.4).
  Every Orker still runs ordinary tasks; only a source host runs instances.
- **Receipt** means the immutable record of what was decided about one
  message delivery: accepted (with its run), a duplicate, filtered,
  rejected, released, dead-lettered or dropped. It is written by the Orker
  that settled the delivery, and is unrelated to a webhook's delivery record
  and to the audit trail.
- **Origin** means the immutable record of what started a run: a kind
  (`submit`, `trigger`, `schedule`, `parent-run`, `run-again`,
  `event-source`), the
  originating object's id, a caller-supplied reference and the principal. A
  child's origin also names its parent task, the parent's and root's Orkflow,
  and the root's origin.
- **Root run** means the run at the top of a composed chain, the one that
  started it. A child run's origin names it beside its parent
  (`origin.root_run_id`), copied down at each child start. A top-level run holds
  none. `origin.root_run_id=` lists every run below a root; the root is not one of them.
- **Principal** means who a request is from, as audit records and origins
  record it. With `auth.mode` `asserted`, the default, it is the
  `Ork-Principal` request header a deployment's gateway asserted, recorded
  verbatim and not authenticated. With `observe` or `enforce` it is the
  identity OrkStack verified from a credential, recorded with how, and the
  header is read nowhere. Every record and every rendering says which: no
  surface may present an asserted principal as a verified one. A force
  cancel's principal is shown from the signed record of that cancel, and as
  verified only when that record says so and verifies.
- **Force cancel** means an operator's cancel that ends a run by Temporal
  termination only after an ordinary cancel and a stated grace (at least the
  run's longest task cancellation grace plus 30 seconds) in which the run
  completed no workflow task, extended while it progresses up to a stated
  ceiling. It is recorded (who, why, when) in the termination's details and in
  audit records, read as CANCELED `RUN_FORCE_CANCELED` everywhere, and kept in
  every list, count and archive. No workflow code of the run runs after it: no
  failure handler runs, no output is produced, task states are as of its last
  event, and an open pin is left to its lease sweep. Without the caller's
  opt-in it never cascades, and composed children are only asked to cancel;
  with it, each descendant run is force-canceled under the same decision.
  OrkStack terminates a run only on a force cancel's verified decision: by the
  force-cancel workflow, or, after two failed supersedes or exhausted
  attempts, by the API in-process under the same decision, permission,
  cascade intent and staleness check; never by an activity another process
  could schedule. A force cancel whose record cannot be checked now, in a
  deployment that has keys or whose identity mode is above asserted, is an
  error on every read of the run, never a plain termination, and is not
  archived while a later read may decide it; one still undecided when the cluster is about to
  delete the run is archived CANCELED `RUN_FORCE_CANCEL_UNVERIFIED`, naming
  nobody. The archive body and an evidence bundle carry the signed record, and
  whoever reads one verifies it before it shows a principal as verified.
- **Automation document** means one authored YAML file
  (`kind: Automation`) declaring a namespace's settings, schedules, triggers
  and webhooks by name (`docs/AUTOMATION.md`). Its plan
  (`orkctl automation apply --dry-run`) is unrelated to `orkctl run plan`, which simulates one
  run's DAG.
- **Activity interface** means what an image declares about its `with:`
  block: the `org.orkstack.activity.interface` label, which is the ABI the
  engine reads, and the authored document it is packed from
  (`kind: ActivityInterface`, by convention `interface.ork.yaml` beside the
  Dockerfile; `docs/ACTIVITY_IMAGES.md`, "Declaring the interface"). The
  document is never applied, stored or read at run time. It is unrelated to
  an Orkflow's declared interface (`inputs:` and `outputs:`), which the
  promote gate compares.
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
- **Concurrency limit** means a bound per namespace and key on runs or tasks.
  The key is rendered at submit and is shared by the namespace's declarations;
  an authored key is at most 128 bytes, and an Orkflow that declares none has
  one OrkStack makes from its namespace and name. Strategies: `queue`
  (default), `refuse`, `skip`,
  `cancel_oldest`, `replace`. A run or task holds a **slot** of its key while
  it runs, waits **queued** for one, and is **superseded** when a newer one
  replaces it (`cancel_oldest`, `replace`).
- **Concurrency gate** and **concurrency census** are internal, exactly as a
  Temporal task queue is: the workflow that holds one key's slots (§2.1), and
  the engine's own Schedule that reads the gates and reports.
- **Usage** means reservation × held time — the cpu and memory an Orker
  granted each attempt, multiplied by how long it held it — derived on read
  (`StatsService.GetUsage`), never measured utilization and never a stored
  counter.
- **Series** means one `GetStats` answer's window cut into consecutive
  **buckets** of one width (`HOUR` or `DAY`) on one IANA zone's wall clock,
  each counting the runs created in it (`orkstackv1.StatsBucket`), derived on
  read from the same scan as the window's totals and never stored. A bucket is
  one stretch of that wall clock with one label (the local date, or date and
  hour). Where "bucket" could be read either way, a bucket of a series and the
  object-store bucket (`ORK_S3_BUCKET`) are each named in full.
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
- **Cordon** means a process-local state of one Orker that stops it accepting
  new tasks and opening new pins while leaving everything in flight alone,
  members of an open pin included. It stands while any of its causes holds;
  clearing one never touches another, and none is stored: all are lost when
  the process exits. Keeping a pool live decides only submit's admission: a
  run sent there is accepted and waits for an Orker that takes work. It never
  sends a task to a cordoned Orker, and a cordoned Orker keeps its pool live
  only if every cause it holds does.

  | Cause | Set by | Ended by | Pool stays live |
  |---|---|---|---|
  | operator | an operator, over the Orker's control channel | the operator | no |
  | store outage | the Orker, after consecutive failed object-storage checks | the Orker, after consecutive good ones | yes |
  | stopping | the Orker, on its first stop signal; from then it accepts no new task, pin or pin creation, and what its open polls still deliver runs in flight | the process's exit | yes, until the process exits |
  | profile drift | the Orker, when a profile no longer matches what it audited, or the GPU host lock directory was replaced | a restart | no |
  | profile unavailable | the Orker, when a profile's devices or units are absent, or the GPU host lock is held by another process, or fewer usable GPU units than a task may hold, or the container engine starting none of a gpu profile's unit probes, three passes in a row | the Orker, after consecutive good checks, or once the lock is held or the units are back, or once one starts | yes |
  | stuck service | the Orker, when a service container it stopped is not gone a minute after SIGKILL | the Orker, once the container is gone | no |

  An event-source instance is the one thing in flight a cordon does not
  leave alone: an operator's cordon and the stopping cordon stop it and hand
  it back, so that it starts on another source host; a store outage leaves
  it running, polling nothing. An Orker that cannot stop a service it hosts
  holds a cordon of its own until that container is gone.
- **Drain** means setting an operator's cordon on an Orker and waiting until it
  reports no running task, no open pin and no event-source instance. It is a client-side wait over the inventory read
  (`orkctl orker drain`); nothing server-side holds drain state. An Orker's
  own wait during its stop grace is called **stopping**, never draining.
- **Control channel** and **presence channel** are internal, exactly as
  **queue** is: the Temporal task queue `ork-orker-<orker_id>` that one Orker
  polls for `DescribeOrker` and `CordonOrker`, and `ork-orkers`, which every
  Orker polls and nothing dispatches to, whose poller list is the fleet.
  So is the engines' presence channel, `ork-engines`, which every engine
  polls and nothing dispatches to, whose poller list is the deployment's
  engines.
- **Source channel** and **source workflow** are internal, exactly as
  **queue** is: the Temporal task queue `ork-source-<pool>` that a pool's
  source hosts poll, and the engine workflow that owns one event source's
  instances. No user-facing surface names either: a user sees a source, its
  instances and the Orker each runs on.

The Temporal mechanisms behind these words are internal, exactly as a queue is
internal to a pool: no CLI string, UI string, HTTP body, client-rendered proto
comment or error message names a task queue, session, source channel, source workflow, Worker Deployment
Version, Signal, Timer, child workflow, parent-close policy, local activity,
marker, Temporal memo, search attribute, visibility query, Temporal Schedule id or
workflow id. (A proto comment on an engine-internal message may say what
implements it.)

An idempotency key is its caller's: where `auth.mode` is `enforce`, a key names
a run only to the principal who used it, and only for the submission it was
used for; a trigger's key is the trigger's, and for one claim's life after the
deployment last entered `enforce` a key claimed before that also names its run
to whoever may read that run, or to the trigger that started it.

An Orkflow name is unique within a namespace. Names are validated, never
sanitized, by the same rule at each boundary:
`^[a-z][a-z0-9-]*$`, at most 200 bytes, for namespaces, Orkflow names, task IDs,
and pools. The built-in `default` namespace always resolves, is never stored,
and cannot be deleted. Its namespace document is never stored, while a side
record such as its identity policy may be. Archive indexes are namespace-first.

Creating `default` writes nothing: an identical namespace specification is a
no-op, a different specification is a conflict, and deleting `default` is
always refused. A request with no namespace uses `default` on the wire.

New runs use IDs shaped `ork-` followed by a UUIDv7 in the lowercase base32 of
the TypeID encoding, 30 characters. Runs started before this shape keep
`ork-{timestamp}-{sequence}`, and the two coexist as opaque strings. An ID is
monotonic only within one replica. A run ID is the single-use Temporal workflow
ID; starting a duplicate must fail, never attach to another run. Nothing may
parse a run ID or derive meaning from its parts, its embedded time included.
Order runs by `created_at`; compare IDs only as the documented tie-break on the
ListRuns read path.

Submission is the last place a run's routing and policy may be decided:
workflow code is deterministic and may not read a namespace document,
process configuration, or the cluster. `SubmitRun`'s preparation resolves the
namespace and Orkflow, applies the admission gate (hold, then `max_running`,
then an enforced budget, then each declared concurrency limit's submit-time
check), resolves each task's pool against the namespace's
allow-list and checks, from the Orker inventory, that a live, capable Orker
serves it, refuses a task whose declared request exceeds its pool's per-task
cap, which is a pool-wide setting, asking the pool's Orkers over their own
control channels, and then decides the
image-policy selection, telemetry selection, cache plan, every composed
child's plan, the origin, any await callback tokens, and each task's
fairness key (its namespace) and weight. All of it rides the
replay-pinned `RunPlan` and is never re-read. A run that a schedule or a trigger starts
holds an identity only if the engine wrote that object,
the principal who bound it may submit the Orkflow when the run is admitted,
and someone consented to the identities the Orkflow asks for. Binding and
consent are acts a person names; saving an object unchanged is neither. That
decides a new run at its own admission; it is not retroactive. One thing is
asked again, by the API and never by workflow code: when an attempt asks for
an identity token, the engine checks that the namespace still allows that
audience, so removing one stops new tokens at the next refresh. It un-admits
nothing: the run goes on, and the attempt is refused its token with that
reason. A cached task's memo hit asks
nothing and is not covered. A submission is refused before an idempotency key
is claimed and before anything is started; only the cluster can refuse after
that, and a start it refuses is recorded as a failed run. The run's own ID is
minted once its preparation refuses nothing. A run that holds an identity has
its records written under that ID before the key is claimed, and a submission
that goes no further removes them, or counts those it cannot; an object it
wrote beside them is left to the sweep. After the claim a submission can still
end without an answer that it started: a start whose outcome is unknown is
never recorded as a failure, the key keeps naming its run ID, and a later
submission under the key finds that run or starts that same ID, until the
cluster could no longer tell whether it ever started; then it is refused. A
run ID is started at most once, so one key never yields two runs. An event
source's door runs the same preparation for each message and refuses before a
run ID is minted or an object written under a run. The run ID a message is bound
to is the only one that message can start, and any delivery of the message may
complete that start. Admission is
never retroactive. A guard that reads the
cluster's index or usage (`max_running`, an enforced budget) is a lock-free
read-then-act and therefore best-effort by a bounded overshoot. `max_running`
and an enforced budget stay read-then-act; a declared concurrency limit is
strict, held by its concurrency gate (§2.1), and only its submit-time refusal
is best-effort. The one exception to submission deciding everything is a
concurrency limit's gate (§2.1), which a run asks through a local activity
whose answer its history records.

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
read path for a decision. Four things in object storage are the stated exception, read
for one decision only, whether an attempt may be issued an identity token: a run's
admission, the record of a start an engine made, the set the API keeps of
each schedule's accepted records, which is maintained and not written once,
and the record of which attempts of a task have held an identity. None says
whether a run is open or who holds its attempt; Temporal does. For a run
whose results the engine records, its records are read for these decisions
and no other: whether an attempt may hold an identity or have its result
recorded, and whether an honest Orker starts its container; which resolution
an awaited task takes, and whether one that reached its deadline was answered
in time; what a composed task's values are; and whether a child run that
finished may report its outputs. Temporal still decides that a run is open,
that a task was started and when a deadline passed. No audit record is among
them. The object-index markers and the cleanup and GC
records are the one exception: the artifact GC reads them, with the run's
archive body and the cluster's index of runs, to decide a delete, and nothing
else decides from them. The run-objects list is not write-once: `PutRun`
rewrites it, only as the union of what it held and the run's references.

RunWorkflow assigns event sequence numbers. One event is not assigned by
RunWorkflow: the terminal event of a force-canceled run, which no workflow code
of the run can emit. Its force-cancel workflow publishes it with the next
number after the run's last recorded event, and every reader, the stream, the
archive and the evidence bundle build the same bytes from the run's last
recorded state and its termination record with one function, so they still
agree. The event carries no signature of its own: the record does, and the
event is a pure function of it and the run's last recorded state. Webhooks are delivered for it as for any terminal event. The bus honors
supplied numbers and assigns one only when none is supplied. Temporal history,
the in-process log, and archived event JSON must therefore remain identical,
and clients can resume by sequence from another replica.

### 7.1 Pools and Temporal task queues

`pool:` is the only user-facing selector of *which Orkers*; `pin:` constrains a
group of tasks to *one* member of that set. Neither exposes a queue, a session,
or a host name. A concurrency limit's line is not a channel and exposes none.
No user-facing surface may expose `queue:` or a queue name.
`internal/task/pool.go` builds every channel name and is the complete mapping:
`QueueForPool`, `RouteForPool`, `DefaultRoute`, `PoolForQueue`, the single
`ork-pool-` prefix construction, the Orker control (`ork-orker-<id>`) and
presence (`ork-orkers`) channels, and the engines' presence channel
(`ork-engines`).

A pool's source channel (`ork-source-<pool>`, `SourceQueueForPool`) is built
there too, by the rule that resolves the pool, and reserved like the others:
it carries the instances of event sources to that pool's source hosts, and
no pool or default may be bound to a name of that shape.

An empty pool and `main` both mean the deployment's configured default pool.
Other valid pools resolve to `ork-pool-<slug>`. The reserved
`ork-engine-events` channel, the Orker control and presence channels and the
engines' presence channel cannot be a bound default. A task's pool must be
resolved inside the engine; Temporal queue names must not leak into the user
contract. A metric about a pool is labelled with the pool (`ork.pool`), never
its channel.

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

An Orkflow version is immutable. Creating an edited document stores a new version; runs
pin the version and its definition digest. A run continues on the definition it
started with even if the current pointer moves. The current pointer controls
new runs only. Moving the pointer across a breaking change to the declared
interface (`inputs:` and `outputs:`) is refused unless that promotion is
explicitly acknowledged (`--acknowledge-breaking`).

One Worker Deployment spans `ork` and `orker` for a release. They share
`ORK_DEPLOYMENT_NAME` and `ORK_BUILD_ID` and are released together. With
versioning enabled, `RunWorkflow`, `ScheduleFireWorkflow` and
`WebhookDeliveryWorkflow` are Pinned,
`IdempotencyClaimWorkflow`, `ConcurrencyGateWorkflow`,
`ConcurrencyCensusWorkflow` and `ForceCancelWorkflow` are AutoUpgrade, and the
projection worker on
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
generation might. A pinned workflow's activity goes to its own version on
every activity channel that version has ever served, whether or not any of
its workers polls it now, and waits there for one; only on a channel that
version never served does Temporal send it to that channel's current version.
So stopping or cordoning every Orker of a version that still serves a pool,
before the version drains, strands its runs' tasks there until one returns.
That wait is the wait of a task on a pool no Orker serves: explicit, named
by the task's pending reason, and bounded only by the run's own `timeout:`.
A task pin (§7.1) is stricter still: its session is opened through a
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
`snapshot-overlay`, `fairness-keys`, `pin-creation-timeout`,
`fairness-pin-creation`, `retry-max-interval`, `task-publish-window`,
`emit-past-cancel`, `interruption-retry`, `schedule-fire-concurrency`,
`run-pause`, `pause-carry`, `pause-cascade`, `pause-deferred`,
`pausable-pin-timeout`, `run-resume`: asked by a reset run only, and not
compatibility but a guard for runs of this release, since a run reset by
hand past R replays a prefix with no marker), so a
history recorded before the gate replays its `DefaultVersion` branch; the
AutoUpgrade workflows gate every later change the same way under a prefix of
their own (`concurrency-gate-*`, `force-cancel-*`), each with its recorded
fixture; each
new command path
gets its own
recorded history fixture. Every tunable number that decides whether, which,
how many or where workflow code issues a command — a history ceiling or
reserve, a continuation budget, a hold line, a chunk or batch size, an
in-flight cap, a retry spacing — is pinned per run, in the plan the submitting
engine builds or in the carry a generation hands the next, never read from a
code constant at decision time, so a release that changes a default moves only
new runs. A code constant may decide a command only when this file fixes it
(the 256 KiB payload bound, the 128 KiB chunk bound, the 5-minute
`resume_plan_load_timeout` of §2.1) or it predates this rule
and a recorded history protects it; changing such a constant is a new command
path with its own gate. Because the activity channel can fall
through to another release, an activity-input field an older Orker would
reject or ignore (cache, join, fan-out items, fan-out lines) is advertised as a capability on
`DescribeOrker`. Submit asks every uncordoned Orker of each pool such a task
would run on, each on its own control channel, and refuses with
`FailedPrecondition`, naming the Orker and its build, when one lacks the
capability; a pool none of whose Orkers answers the probe is logged, not
refused, so every Orker of one pool must run one release. The interruption
class is the one exception to that rule, and it needs no capability: it
reaches the Orker inside the activity's own retry policy, and an Orker that
does not read it reports its failures as before, which only spends the task's
own attempts. Submit's capability and per-task-cap probes ask every Orker
whose causes keep its pool live, stopping ones included, because those are
the Orkers admission counts. The heartbeat-detail decoder an interruption is
classified from is a frozen copy, never the Orker's type, so a field the
Orker adds later changes no decision a recorded history made.

### 7.3 Generations

A run whose plan carries a continuation budget continues as new into a new
generation once its history has grown past that budget, at its next moment
with nothing of it in flight: no activity, child run, pin (opening, open or
closing) or event publish. Open awaits and sleeps may cross; their deadlines
and wake-up times are absolute instants. In a run whose results the engine records,
and in a run whose definition declares a run-level timeout, each of the
switch's two checks of the queued resolutions takes at most a count of them
fixed when the run was submitted, and in a run whose results the engine
records it reads each from its record there and makes at most one read that
goes unanswered. A resolution whose record could not be read neither holds nor
turns back the switch and is never a failed attempt; a ready run-level ending
abandons the switch. Such an await crosses open, its record standing, and is
taken when the run is told again or at the await's deadline. The run continues
as new only when no resolution it has not read is left queued after the last
check. Awaited tasks between the two reads of their deadlines hold the switch,
together, for at most a time fixed when the run was submitted and one read
each; one such group is open at a time, and a run has at most one for each of
its awaited tasks. While one holds the switch the run gives up its hold on new
dispatch. The run id, the event sequence counter, `created_at`, `started_at`, a
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
future time. A source image's container is not that: it holds a broker connection and receives, which no Timer or Signal can do. Do not add a custom scheduler, event bus, task queue, state
machine, or expression runtime for these operations. An awaited task
(`await:`) is the DSL form of a Signal wait, manual approval and external
completion: a Temporal Timer for its deadline and a Signal for its resolution,
and no container. In a run whose results the engine records, the Signal says
only that a resolution was recorded; the record is the resolution (§4.1). A sleeping task (`sleep:`) is the DSL form of wait / sleep:
one Temporal Timer and no Signal. A schedule is a Temporal Schedule; a
concurrency limit is one gate workflow per active key: an Update to take a
slot, Signals to grant and release; the engine's own internal Schedules (the
concurrency census) are created by each engine at boot if absent and are never
user objects, and are no **schedule** in §7's sense; a
composed task is a Temporal child workflow of the same `RunWorkflow`
interpreter; a fan-out's
items are ordinary activities in the interpreter's own in-flight set; a
fan-out over an artifact's lines is counted by one internal activity on an
Orker of its pool, which writes an immutable, digest-verified line index,
never the lines into history; a webhook
delivery is a short workflow of its own whose one-`POST` activity Temporal
retries under the ladder pinned into it, never a loop in the engine.

Fair dispatch across namespaces is Temporal's task-queue fairness, keyed
by namespace and pinned in the plan; it is never an OrkStack queue.
Enabling it is a Temporal setting of the deployment; without it dispatch
stays first-in first-out per queue.

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

`orkctl service test my-image` does the same for a service image: it runs
the image through the code an Orker hosts services with, plays the Orker's
side, and checks the labels, the status, what the image writes and how, a
step's rename and sequence, a late step, the Orker's beat stopping, the stop
sequence, and what an unasked exit means, then the image's profile (for a
source: a quiet poll, a message's shape and bounds, each verdict, the hold
bound, a refreshed credential). It gives the image no network unless asked
to. Keep the Bash, Go and Python fixtures of each profile under
`test/fixtures/`; the Bash one is mandatory.

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
  failure: a stall inside the heartbeat timeout leaves a task and its container
  untouched, plain or pinned; a cancel during a stall still reaches the container;
  past the timeout the Orker stops the container and the attempt is never
  reported cancelled; it is an interruption when the cluster's heartbeat
  timeout ended it (§4), and retried under its policy otherwise;
- an Orker stopped while a container runs: the task finishes on it within the
  stop grace and runs once; past the grace it is reported `ORKER_STOPPED` and
  its container is never running beside its next attempt; a stop during upload
  or commit commits; a pin finishes within the grace and fails `PIN_HOST_LOST`
  past it; a stopping Orker opens no pin and polls for no new task, a task its
  open polls deliver runs in flight and spends no attempt, and a pin asked of
  a stopping pin host opens on another that takes work or fails
  `PIN_UNAVAILABLE` within its bound; a task it hands back runs only on an
  Orker of its run's release, waiting for one rather than moving to another
  release; a second signal ends the grace, a third exits, and a signal to its
  process group never reaches a container;
- an interruption: a task without `retry:` whose Orker is handed back or
  killed runs again without spending its own attempt, after the loss wait
  when it was lost, and a loss whose wait spans a continuation is not
  re-dispatched before its recorded instant; a commit written before the loss
  is adopted, even when the ordinal claim fails; a pinned member stays
  `PIN_HOST_LOST`; a spent budget falls
  back to own attempts; a task handed back before it started because its
  Orker was stopping never spends one and is never failed for it, however
  many stopping Orkers it meets, while a store or profile fault before the
  start that repeats ends explicitly with its real reason; an interrupted
  unit crosses a continuation pending; a storm of interruptions stays within
  the run's history bound;
- exit `1`, exit `0` without `values.json`, malformed JSON, output-size excess,
  timeout, cancellation, ignored SIGTERM, unavailable input artifact, failed
  image pull, and a tag changing between executions;
- a device or CDI spec of a profile changed after boot, even to one that would
  pass the audit: the Orker cordons itself for drift, at the periodic audit or
  at the latest before its container starts, with no container started, and
  only a restart audits again;
- a `gpu` profile's units, on real devices: two tasks never hold one unit, a
  create that fails frees its units, a health probe never takes a unit a task
  was promised, a restarted Orker reaps or keeps only containers bound under
  the GPU lock it now holds, and a second Orker of the host waits, paused,
  until the first exits;
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
- an engine unreachable when an attempt of a run whose results the engine
  records has finished its container: inside the wait nothing fails and
  the result is recorded when an engine answers; past it the attempt
  fails `COMMIT_ENGINE_UNAVAILABLE`, is retried under its policy, and no
  result is recorded around the engine; a cancel during the wait ends the
  attempt cancelled;
- an Orker killed after its uploads and before the engine recorded the
  result: the retry runs the container again; killed after the record
  and before Temporal's completion: the retry adopts it and no container
  runs;
- a result or a memo written to the store by anything but an engine, for
  a task of such a run, before the task runs, between two tasks and
  under a cached task: never adopted, never hit, never staged;
- an Orker of another pool asking the engine to record a result for an
  attempt it took from the cluster: refused and recorded, and nothing is
  written; a task of such a run completed on the cluster with no record:
  its dependents fail explicitly, naming it;
- an engine lost between writing a record and answering: the Orker's
  repeat is answered with that record, and an attempt that never hears an
  answer finds the record before it fails; two attempts of one task
  asking at once: one record;
- an Orker whose role may not commit at its pool: the run is refused at
  submit, and an attempt that reaches such an Orker all the same is
  refused before any container starts;
- a resolution sent to a run whose results the engine records by
  anything but the API: an event of the run, its await left open, and
  the real resolution still applied; the same for a rejection;
- a task of such a run dispatched before an approval it follows was
  given, by a party answering the run's workflow tasks: an honest Orker
  starts no container; that party's own Orker is given no identity and
  has no result recorded, whatever it runs; each refusal names the task
  that has no record;
- an engine lost between recording a resolution and telling the run:
  the caller's repeat delivers it, a different resolution is refused
  and delivers it, and an await that reaches its deadline with a record
  takes that record; two different resolutions at once: one record, and
  the other caller is told so;
- an answer given after an await's deadline, while the run's state
  still shows the task awaiting: refused, and nothing recorded;
- a child run ended on the cluster with outputs of someone's choosing,
  and a child whose history was written so that it seems complete: the
  parent's task fails explicitly, or the child's outputs are not
  recorded, and no dependent starts;
- an awaited or a composed task's values changed in a dependent's
  input: the Orker stages what the record names and fails on the
  difference;
- an await of such a run that crosses a switch, and a resolution racing
  one: resolved once, from its record, with one run id and a contiguous
  stream; a failing run of this kind still runs its failure handlers to
  their own outcomes;
- many awaits of one run told of their answers while only the records
  are unreadable: none fails and none is applied unread, a sibling
  task that ends meanwhile is taken and its dependents started, and
  each answer is taken when the run is told again or at its
  deadline, if its record can be read then;
- an answer that is on record, repeated after its await's deadline:
  answered as a repeat and delivered, never as late and never as
  applied;
- an answer on record repeated many times, by one caller and by
  several replicas, while the run cannot read its record: the run
  makes one read a turn, a deadline, a sleep or a run timeout that
  is ready is taken before the next, no repeat spends the history
  the run's failure path needs, and the run is ended by none of
  them;
- an await of such a run whose record cannot be read at its deadline
  and can be read a fixed time later: the run takes the record and
  goes on, its dependents start, and no failure handler runs; a
  record the store finished after the deadline is never taken as in
  time, whatever time it holds;
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
- a paused run starts nothing new while what was running finishes, an open
  pin's members included; it carries its pause, its timeout and the requests
  it could not yet record across a continuation and still ends on time; an
  unpause, a cancel or its timeout ends it; a failing paused run's failure
  handlers wait, and its timeout keeps the original cause; a pause reaches a
  running child run, a child started without support for pausing is never
  sent it and is named in a warning, and a child whose parent's pause never
  lifts ends at its backstop; a run that cannot record a pause stops
  `RUN_HISTORY_LIMIT`, explicitly; where the cluster does not index paused
  runs nothing is written to the index and none is listed as paused; and a
  pause and an unpause sent through `orkctl` to two replicas, and their
  webhooks, reach the run and its subscribers;
- a pause under chaos: an engine replica killed while a run is paused,
  inside the workflow task that applies a pause, or while a child run
  applies the pause its parent cascaded, leaves each run paused once and
  starting nothing until its unpause; an unpause racing a running task's
  failure ends the run failed with that task's cause and runs its failure
  handlers once; and an unpause racing the pause's own expiry either
  unpauses the run or lets it fail `RUN_PAUSE_TIMEOUT`, never both;
- a run at the deployment's task ceiling and a run at the fan-out item
  ceiling complete across generations with their definition passed by
  reference and snapshot overlays on, each generation within the measured
  share of the cluster's history limits and replaying within the deadlock
  detector's budget, and every run within its in-flight cap. The merge
  gate proves this with the ceiling lowered below its default, at a size
  where the definition still passes by reference, overlays are written
  and the run carries on across at least two generations. The default
  ceilings themselves, and the overlay saving at a thousand tasks, are
  proven only by the deferred tests (below);
- a memo hit racing the GC, a child run staging its parent's object and a
  parent holding its child's returned object while the other is retired, a
  producer outliving its consumer's retention, two replicas reclaiming the
  same object, an engine killed mid-reclaim, and dry-run deleting nothing: no
  object a live or retained run references is ever deleted, and retirement is
  never delayed;
- a memo hit made by an Orker of any release, a hit racing its source's
  retirement, a run continuing as new or resumed while the GC walks the open
  runs, a run that finished without an archive, a forged or premature
  sidecar, an execution missing from the cluster, an engine clock skewed
  against the store, an engine restart or a Temporal outage between passes,
  and a restored bucket: no object a live, retained or unarchived run
  references is deleted, and none of them postpones reclamation beyond the
  oldest open run;
- an engine killed mid-acquire, mid-pass and mid-continue-as-new of a gate;
- a gate's record lost with runs queued and holding: restored in order,
  nothing granted past a queued run, and a rebuild with visibility down ending
  explicitly;
- a reset drops no holder of an answering gate, even with acquires racing it
  or the RPC stalling past the freeze window;
- a cold key after a clean close grants at once;
- late acquires, tombstone overflow, and sequential tasks on one key: no
  phantom holder and no false fence;
- a run terminated while holding or queued, and a stalled holder terminated by
  an operator: reclaimed within one sweep;
- a resumed run, including a second resume, a continued run and a run with
  task-only gates: `RUN_RESUMED`, read-back, `RUN_RESULTS_KEPT`, steps 4.1–4.4
  (E85's resume holder), every earlier holder released;
- a release during a frontend outage;
- a cancel and a supersede racing a grant;
- a Worker Deployment rollout with runs queued and a gate continuing as new;
- at a fire: an unavailable check, a full namespace queue, a `max_running`
  filled by queued runs, a failing count, or a rollout. The schedule is not
  paused, and a pre-0.4 fire history replays;
- a run in a workflow-task failure loop, holding a concurrency slot and an open
  pin, force-canceled: it ends CANCELED `RUN_FORCE_CANCELED` with its
  principal and one terminal event and webhook, its slot is released at once,
  its pin directory only after the lease, and it is archived; a healthy run
  whose tasks have long cancellation graces, force-canceled, ends
  `RUN_CANCELED` on its own path and is never terminated; an engine killed
  between the termination and the terminal event still publishes the event
  once;
- one trace id across a tree, a continuation and a fire; trust setting off → new
  trace + link.

Scale and soak tests leave the per-merge proof only through
`scripts/proof-nightly-tests.txt`, and Playwright specs through its spec-side
twin `scripts/proof-nightly-specs.txt`. Each entry names the test or spec file,
the merge-gate tests or specs that cover the same behaviour at a smaller size
or by another route, and the date the user approved it. A test or spec is
listed only when such coverage exists, and the lists change only with the
user's approval. Every other test and spec, and every scenario above, except
as the last bullet states and except the GPU-host lists
(`scripts/proof-gpu-tests.txt`, `scripts/proof-gpu-specs.txt`), stays in the merge gate: the run page at the task
ceiling is proven there by `ui/e2e/run-page-reduced-ceiling.spec.ts`, its
measurement at a ceiling lowered to 1,000, and at the 4,000-task default by
the nightly tier. A full proof records each listed test, and each test of a
listed spec file, as `deferred`, never as passed. It defers only the entries
its base's list also carries: an entry the change itself adds is run, and
recorded as it ran. The deferred tests and specs run in the nightly tier and
in the pre-release gate, and a release tag requires a passing pre-release
proof of the exact commit it names.

The tests `scripts/proof-gpu-tests.txt` lists, and the Playwright specs of its
twin `scripts/proof-gpu-specs.txt`, need the GPU host it names: a
proof without `ORK_PROOF_GPU_HOST=1` records them as `host-deferred`; a proof of
the **GPU-host tier** (a full proof, or the GPU gate alone, `make proof-gpu`,
run on that host with the flag) runs them; and a change to a path the list
names merges only with a passing GPU-host proof of its tree
(`make merge-check GPU=`).

The merge gate's absolute wall-clock budgets — the soaks' live `GetRun` p95,
the run page's and the item list's 5 s — were calibrated on the 16-CPU
reference host. Each is checked by the test that measures it, against its
reference bound scaled by the reference's 16 CPUs over the host's own when the
host has fewer, never below the reference bound
(`internal/testsupport/proof.Timing.Check`, and its twin `ui/e2e/budget.ts`;
decided 2026-11-09); the host's count is the larger of the CPUs the process
may use and the host's online CPUs, so restricting a proof to fewer cores
never widens a bound, and the test's output names the measured value and the
bound it was held to. A proof on a host of at least 16 CPUs therefore enforces
the reference budgets exactly, and a tag proved on a smaller CI runner rests
only on the scaled bound. Every other wall-clock bound of a test is held to its
reference on every host. A budget's reference bound changes only with the
user's approval. No table excuses a miss, and the merger reads no timing
record. A count, a size, a history share, a deadlock-detector trip and every
correctness assertion are never scaled and fail wherever they run. What bounds
the soaks' live `GetRun` on every host besides its budget is what it reads: at
most one pass over the generation's history, in events and in bytes, and a
bounded number of saved-state objects. It bounds what is read, not what reading
it costs: a slower decode shows only in the wall-clock numbers.

The nightly of `main` is GitHub's: `nightly.yml` runs the nightly tier on
`main` every day (05:17 UTC) and by dispatch. No local `make proof-nightly`
follows a merge: that interim rule, in force while the workflow did not run on
GitHub, was retired on 2026-10-27, once three consecutive nightlies of `main`
had passed on it. A release tag has its own gate, the pre-release proof above.
A merge is not held for a nightly of its own: a run speaks only for the commit
it measured (its `headSha`), never for `main`'s tip, so the nightly of a later
commit is a later run. The same workflow runs the vulnerability scan as jobs
of its own, because they need the network and so are in no proof: `make vuln`
over the Go module, `make vuln-js` (`bun audit`) over the UI's and the site's
JavaScript dependencies, and `make vuln-images` (a pinned Grype) over the
images a release of the commit publishes. A vulnerability whose vulnerable
code the module reaches, a high or critical JavaScript advisory, or a fixed
high or critical vulnerability in an image fails that run like any other
nightly failure; the fix is to bump the module, package or base image to the
version the report names. It runs the README's Quickstart on a clone of the
commit (`scripts/quickstart-smoke.sh`) as a job of its own too, because no
proof runs the getting-started path: a stack that no longer builds or starts
from a clone fails that run like any other nightly failure. It holds every
published release's notes to its section of the commit's CHANGELOG.md
(`make release-notes-check`) as a job of its own too, because it reads the
releases over the network: a release that differs fails that run, naming the
version and the first line that differs. `release.yml`
runs the same smoke itself before it publishes anything, so for a release
that one is a check, not a rule. It runs every shipped example on such a stack
as a job of its own (`scripts/quickstart-smoke.sh --examples`): created, submitted
with no inputs, ending where `examples/README.md` says. Those examples reach
`httpbin.org` and `github.com`, so no proof runs that job and `release.yml` does
not require it: the merge gate runs them against stand-ins
(`TestIntegration_ExamplesRunWithNoInputs`), and a failure of the job is a
regression on `main` unless its log shows one of those services was down. It
holds the table of NVIDIA's `nvidia-cdi-hook` builds an Orker accepts
(`internal/orker/cdi_hook_table.go`) to NVIDIA's signed package repository
as a job of its own too, `cdi-hook-table`
(`go run ./internal/orker/cmd/gencdihooks -check`), because it reads that
repository over the network: a toolkit release at or above the floor that the
table lacks, or a row that no longer derives, fails that run.

A failing nightly (the latest completed `nightly.yml` run on `main`) is a
regression on `main`, not a merge freeze (decided 2026-10-28): fixing it, or
reverting the change that broke it, is the next work, a nightly of the fix is
dispatched once it is pushed (docs/PROOFS.md §10), and other changes keep
merging through the merge gate meanwhile. Never edit a list, a threshold or a
measurement file to make it pass. A red nightly clears only when a later run,
of the fix or of a commit after it, passes. What it holds is the release: a
release tag requires a passing pre-release proof, which runs every deferred
test, so no tag is made while one fails, and a failing pre-release proof holds
its release the same way. The pre-release proof runs neither the nightly's own
lanes nor its vulnerability scan (`vuln`, `vuln-js` and `vuln-images`), so a
release is not tagged while either is red, and `release.yml` checks it: before
it asks for the approval, it refuses a commit whose latest completed nightly on
`main` did not pass (its proof, each of the scan's three jobs,
`release-notes`, `cdi-hook-table` and `quickstart`), or that no nightly measured, waiting a bounded time for one
still running. (`make
release-tag`, which publishes nothing, leaves it a rule.) A nightly's verdict
is its `proof.json`,
not the run's colour (docs/PROOFS.md §10, §12). A nightly part that fails is a
regression on `main`; only a part attempt that recorded no result (§11.2) runs
again, as a new part of the same tree. A
change that raises a
default the deferred tests measured (`engine.orkflow_max_tasks`, the fan-out
item maximum, or a continuation, history or in-flight default) quotes a
nightly-tier proof of the change (`make proof-nightly`, or `nightly.yml`
dispatched on its branch), which runs those tests. A regression that only a
deferred test can see is found by the next nightly run rather than at merge.
That is the accepted cost (2026-10-15).

Every execution must eventually reach either a correct durable state or an
explicit actionable failure. There is no documented third state such as stuck,
half-completed, unknown, or needing database surgery.

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
a pure function of its history (§2). Children continue the parent's trace from
plan-pinned context. Activity authors need no observability
SDK; activity logs remain stdout/stderr.

The shipped observability kit ingests metrics only by OTLP push and scrapes
nothing; `/metrics` on `ORK_ADMIN_ADDR` stays available to any scraper.
Shipped dashboards and alert rules (`deploy/observability/`) are optional
deploy material, query only bounded labels, and find a run through logs,
traces and exemplars. No UI, CLI, API or error text names or links an
observability backend (`TestSurfaces_NameNoObservabilityBackend`). A record
carrying a namespace leaves only on that namespace's selected sink, never on
another by fallback; the default sink carries namespace-free counts for the
operator.

### 11.1 Development workflow

Every code change follows this order:

0. `make check` is the fast path: build, vet, lint, the unit suites with
   `-short`, and the UI's type-check and unit suite, with no Docker and no
   Temporal. Run it before every push; CI
   runs it on every pull request. Lint is `go vet`, gofmt and staticcheck at
   the version `go.mod` pins (`go tool staticcheck`, its default checks, run
   as built and with `-tags=integration`): a finding is fixed in the code,
   and a false positive carries a line-level `//lint:ignore` that says why.
   `make vuln`, the pinned `govulncheck`, reads the vulnerability database
   over the network, so it is in no lint and no proof, where a network failure
   must never fail one; `ci.yml` and `nightly.yml` run it. `make vuln-js` and
   `make vuln-images`, the scan's other two lanes, need the network too;
   `nightly.yml` alone runs them.
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
   refuses a stale schema. When that changes an Orkflow or automation schema,
   or a change bumps ajv or ajv-formats, also regenerate the UI editor's
   validators with `cd ui && bun run gen:validators`;
   `TestSchemas_ValidatorsAreCurrent` and `ui/test/yamlValidators.test.ts`
   refuse stale ones.
3. Dependencies are pinned: an exact module version, or an image digest, never
   a floating tag at runtime; a toolchain version is an exact line in `go.mod`
   or the workflow; a tool the build runs (staticcheck, govulncheck) is a
   `tool` directive there, pinned like a module; a third-party action is
   pinned by its full commit SHA, with its release as a comment on the same
   line (`@<sha> # vX.Y.Z`). Update by pull request that proves the change —
   `.github/dependabot.yml` opens them, and an update never merges on a green
   unit run alone. A new dependency follows the same rule as any other change.
   Before building a server, stub, parser, protocol client, library or tool,
   test and development infrastructure included, look for an established
   project that already does it and use that, pinned as above. Build one only
   when the established project lacks a property OrkStack needs, and say in
   the code which project was weighed and why it was not used. A hand-written
   fake in a test is only for a fault a real system cannot produce on demand,
   and its doc says which one: the development identity provider is Dex, not a
   stub of our own.
4. Write a behavior test that fails for the right reason.
5. Implement until it passes.
6. Run `make lint test`; for concurrent code, `make test-race` too (the race
   gate's run, docs/PROOFS.md §2.3); for Temporal or storage changes, run the
   affected integration tests against `make infra-up`
   (`scripts/integration.sh ./internal/engine -run '^TestIntegration_X$'`, or
   `make test-integration` for all of them).
7. Documentation is part of the change. A change that alters behaviour users,
   operators or contributors see — DSL keys and rules, CLI commands and
   flags, API fields, error reasons, configuration, the activity contract,
   storage layout, proof and CI behaviour — updates the reference doc that
   describes it, and deletes the text it makes false, in the same change.
   Specs and design documents still never go in the repository. A change is
   not done while a doc describes the old behaviour.

   CHANGELOG.md is for the people who run or use OrkStack, and its entries
   name only changes that may affect them: behaviour, the DSL, the CLI, the
   API and its error reasons, configuration, the activity contract, storage
   layout, what a release publishes and any security fix to it, a
   deprecation, a breaking change. Every entry first passes one test: would a
   person who runs or uses OrkStack care about this at all? It stays if it
   changes what they type, configure, see or depend on, or needs action from
   them, and every **BREAKING** change and security fix always stays; internal
   minutiae (a removed example, the quickstart's internals, a reworded message
   nobody scripts against, a metric label only an internal dashboard sees, a
   glitch no user would have noticed) get no entry, in the changelog or the
   release notes. Such a change adds an entry under
   CHANGELOG.md's `[Unreleased]` section (Keep a Changelog 1.1.0), always in
   one format: one line, in plain words a user of OrkStack understands, on what
   changed for them and what they must do about it, if anything, at most 200
   characters before its closing links, and naming no internal type or field a
   user never types; then, in one closing parenthesis, an absolute link to the
   doc that holds the detail, where one does, and, once changes land through
   pull requests, a link to its pull request. One user-facing change is one
   entry, however many commits made it. A breaking change's entry is marked
   **BREAKING** and stands first in its subsection. A version's section opens
   with `### Features`, a short list of its headline items, a few words each,
   closed by one `**Upgrading:**` line on what to read before upgrading, since
   it opens the release's notes; Added, Changed and the rest after it stay the
   full record. `TestChangelog_EntriesAreOneShortLine`
   (`internal/docscheck`) refuses an entry or an Upgrading line over the
   limit, counted without its bullet, its mark and its closing links, and an
   entry that runs on to a second line. A released version's section and its
   release notes on GitHub must match: to change what a released version
   says, edit its section, then republish it with `make release-notes-publish
   VERSION=vX.Y.Z`, and never edit a release body by hand (docs/PROOFS.md
   §10). A change that cannot affect
   them gets no entry, however large: documentation edits; CI, workflows and the
   proof harness, unless the change alters what a release publishes; tests;
   refactors; a cosmetic change to the UI; internal tooling, such as the
   development stack (`make up`); and a dependency bump that cannot affect
   them. A bump that changes what they see, such as an activity image's tools,
   gets one, and so does every security fix to what a release publishes, a
   vulnerable dependency's bump among them, even when they see nothing change.
   A change with no entry still updates the reference doc that describes it.
8. Commit with a Conventional Commit that names the tests and specs that
   prove the change. A branch may hold several; what lands on `main` is one
   squash commit (§11.3), whose subject is a Conventional Commit and whose body
   names those tests and specs.
9. Merge through §11.3. The full proof of the merged tree is what makes the
   change done.

An AI coding agent that has the ECC plugin follows its language skills, not its
own defaults. Before it writes Go, it loads `ecc:golang-patterns`,
`ecc:golang-testing` and `ecc:tdd-workflow`. It reviews a Go diff with
`ecc:go-reviewer`, and failure paths with `ecc:silent-failure-hunter`, both on
a model other than the one that wrote the diff, and repeats until no CRITICAL
or HIGH finding remains. It resolves a build or vet failure with
`ecc:go-build-resolver`. UI and TypeScript changes use
`ecc:typescript-reviewer` and the `impeccable` skill. A change touching
authentication, secrets or a trust boundary also gets an
`ecc:security-reviewer` pass. The skills inform the work; they never excuse a
rule of this file.

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
(Playwright, `cd ui && bun run test`) and drive the built UI, served by the
engine under test from `ORK_UI_DIR` (`ui/build`), against that real engine, an
Orker, Temporal, and the dev object store; the activity fixtures live in
`test/fixtures` and `test/tools` holds helpers those suites build. Do not use
sleeps for synchronization; use deadlines or test helpers. A skip is never a
pass: a harness must probe its prerequisites itself and fail, not skip, once
they are present.

Some suites have an activity container call a stub on the host at
`host.docker.internal`; a Linux host with plain Docker Engine needs one-time
host setup for that name (CONTRIBUTING.md, "Linux hosts"). It is host setup, never
Orker behavior: the Orker must not add `--add-host` or any host-network
configuration to activity containers (§6). The observability gate needs no
such route: `ork` and `orker` push every signal over OTLP to the observability
profile's Collector on loopback, the kit's Prometheus scrapes nothing
(`TestKit_NoScrapeConfigs`), and the gate binds its admin listeners to
loopback.

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
through `testbin.Path`. The first-party activity images
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
tasks that open no network interface. `network-change.spec.ts` runs alone
after every other file too, because its host-namespace control sees every
host network change, another spec's activity containers included. Every spec file creates a bucket of its own (`ownBucket` in
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
host-wide lock, so one proof's specs wait for another's, and never run beside
them. Specs run by hand (`cd ui && bun run test`) take no lock; do
not run them while a proof's Playwright gate runs. The observability gate is
the other exception: its admin listeners bind fixed ports, so it runs under a
host-wide lock of its own. A test that proves something did NOT happen within a
window also observes, in that window, that the system was making progress, so
a slow host cannot make it pass.

Run integration suites through `scripts/integration.sh`, which
`make test-integration` and the proofs use; docs/PROOFS.md §2 gives its
mechanics and the by-hand `go test -tags=integration` recipe.

Stop only what you started, by its PID. Never stop a process by its name or a
pattern: `/bin/ork$` and `/bin/orker$` match every worktree's dev stack and
every running proof on the host. `make down` stops this worktree's dev stack:
the process groups `make up` recorded, each identified by its pid and the
start time `/proc/<pid>/stat` holds, so a pid the kernel has given another
process is never signalled, and any other process whose executable is this
worktree's `bin/ork` or `bin/orker`, identified by `/proc/<pid>/exe`, never
by its name. `ork` and `orker` make themselves undumpable, which hides
`/proc/<pid>/exe` from their own user, so a process the scripts never
recorded is not found. `scripts/proof-stop.sh proof/<name>` stops a proof you
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
entry for a change that may affect the people who run or use OrkStack
(§11.1, step 7). Done is judged by the merge gate; a deferred test's
regression is a regression on `main`, found by the nightly or the pre-release
proof (§11).

**The three tiers.** A **full proof** is the merge gate. The **nightly
tier** runs only the tests `scripts/proof-nightly-tests.txt` lists and the
spec files `scripts/proof-nightly-specs.txt` lists, and two lanes of its own
— the race repeat (the concurrency-heavy packages' unit suites under the race
detector, several times in one run) and the fuzz lane (every Fuzz target,
sharing one time budget for the whole lane, a crasher failing it and its input
kept) — and is never merge
evidence. The pre-release gate is the full tier with nothing
deferred except the entries it records `host-deferred`. A release tag
requires a passing pre-release proof of the exact commit. docs/PROOFS.md is
the reference for what each tier runs.

**Rules for every proof.**

- Never rerun a failing test, and never partially rerun a shard that produced
  a result. Only a part attempt that recorded no result may run again: one
  whose runner GitHub annotated as lost, and one, of any part but `plan` and
  `build`, whose job failed before its proof step started (`setup-failed`;
  `proof.json` records the part, the attempt and the step). Such a part runs
  again whole, as a new part of the same tree, never a failed test in
  isolation, and `proof.json` records the rerun; any other second attempt
  makes the proof ineligible. "Re-run failed jobs" is permitted when every
  failed part is one of those two, and is never an answer to a part that
  recorded a result. The merger is the only authority: a rerun it does not
  accept makes a proof ineligible and never passes one. A test that fails on a
  slow runner is a test failure, fixed at its root or quarantined.
- Never edit the costs file, the nightly list, the quarantine list or the
  allowed skips to get a proof through. The nightly and quarantine lists change
  only with the user's approval.
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
  `release.yml` also refuses a commit whose nightly did not pass, and it tags
  and publishes only from `main`, in one job, after one approval of its
  `release` environment, pushing the tag with a token of the GitHub App that
  environment holds and never through the API; `make release-tag` tags locally
  and publishes nothing. A release requires a passing GPU-host proof of the
  exact commit; `release.yml` does not check it, its release-environment
  approval summary says `GPU-host proof not checked`, and
  `make release-tag GPU=` checks it locally.
- A skip is never a pass. The ledgers list every test the binaries report; a
  listed test without an outcome, an unlisted test, and an allowance naming
  no test all fail the gate.
- Evidence is never committed; the commit or pull request quotes it, and for
  a queue merge the `proof` job's summary and artifact are the quote. A
  browser-only behaviour is proven by a Playwright spec and its screenshots.
- A proof directory is never reused, and a proof creates it only after its
  arguments and prerequisites are checked. Exit status: `0` pass, `1` a gate
  failed, `2` an invalid invocation, `77` a missing prerequisite.

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
build; the alerts gate (promtool on the observability kit's rules, at the
pinned Prometheus); the race gate (the unit suites under the race detector,
`-short`);
every Playwright spec; and the observability gate. Its base is not a choice:
the gate lists are anchored to `main` (an entry the change itself adds excuses
nothing and defers nothing), a full proof whose recorded base is not `main` is
not merge-eligible, and `merge-check PROOF=` requires that base to be `main`'s
current tip. Concurrency is how the full proof is fast, never a
reason to run less: the fast gates (lint, build, unit, vet, the UI type-check
and unit suites, the site's check and build, the alerts gate and the race
gate) run first and
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
names, and in every other tier it fails the gate.
`host-deferred` is legal in the full, nightly and pre-release tiers only for
an entry both the base's and the tree's `scripts/proof-gpu-tests.txt`
(or `proof-gpu-specs.txt`) carry; in the GPU-host tier it fails the gate. With `ORK_PROOF_GPU_HOST=1` a missing
GPU device, spec or lock group exits 77, never `host-deferred`, and
`proof.json` records whether the GPU gate ran. A skip
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

`main` moves only by fast-forward, and only to one squash commit: its parent
is `main`'s old tip, its subject a Conventional Commit (§12), and its tree one a
full proof passed on. A proof is bound to the tree, so the squash keeps the
proven tree exactly. There are two paths to that.

**(a) GitHub, once the gate is enforced.** The required `proof` check is the
full proof, and the required `pr-title` check holds the pull request's title to
a Conventional Commit. GitHub squash-merges the pull request: the title becomes
the commit's subject and the description its body, which names the tests and
specs and quotes the evidence. By default `proof` runs on the pull request,
whose branch must contain `main`'s tip, under the strict policy, and GitHub
lands exactly the tree it tested; with the optional merge queue it runs on the
merge group, and the queue lands that tree. A code-owner ruleset, which the
enablement applies unless it is told not to (`CODEOWNER_REVIEW=0`), requires
the maintainer's review for a change to the gate's own files. A Dependabot pull
request's `proof` fails until someone adds the `ci:proof` label, which starts the
proof (`proof-label.yml`, docs/PROOFS.md §5); a required check must never be
reported skipped, because GitHub counts that as passing. This path cannot
check `GPU=`: a CI proof records every row of `scripts/proof-gpu-tests.txt`
`host-deferred`, so a pull request that touches a path the list names merges
only through path (b), with `make merge-check GPU=` locally.

**(b) Locally.** Branch from `main`'s tip and squash the feature branch onto it
with `git merge --squash`, then one Conventional Commit. Several branches that
share one proof land as one commit: merge each onto that branch with `--no-ff`,
then fold the merges with `git reset --soft main` and one Conventional Commit,
which keeps their tree, and a proof taken before the fold with it. When both
sides changed a file, resolve the union of behaviors against the merge base; do
not take one side wholesale. Run `make merge-check` (build, lint, unit and
`go vet -tags=integration`), then prove that clean candidate. By default, push
it to `validate/full/<name>`, where CI runs the full proof sharded across
runners in a fraction of one host's time, and download the
`proof-<run>-a<n>` artifact of its passing run (docs/PROOFS.md §10); or run
`make proof` on it. `make merge-check PROOF=` accepts a CI proof when it is a
full, passing, merge-eligible proof of exactly the candidate's tree whose base
is `main`'s tip. The artifact is downloaded, never retyped: its `proof.json` is the
evidence. When the proof passes, amend the squash commit so its body quotes
it; the amend leaves the tree, which the proof is bound to, unchanged. Then
run `make merge-check PROOF=proof/<name>` and fast-forward `main` with
`git merge --ff-only`. `merge-check PROOF=` refuses unless the proof is a
full, passing, merge-eligible proof of exactly the candidate's tree, the
worktree is clean, the candidate is one commit with a Conventional Commit for
its subject, and both its parent and the base the proof diffed against are
`main`'s current tip. If `main` moved during the
proof, rebuild the candidate on the new tip and prove it again. Run one full
proof at a time on a host until two concurrent full proofs have both passed
there.

Until the merge gate is enforced (docs/PROOFS.md §6), path (b) is the merge
path, and a CI run of the candidate counts only through (b)'s
`merge-check PROOF=`, never as a substitute for it. Enabling it is an
outward-facing change, confirmed by the person first. Pushing to `origin`,
a `validate/full/<name>` candidate included, is a step taken only when the
person asks; once they have asked for `main`, it is pushed to `origin` after
every merge, so the nightly and the gate see the real tip.
## 12. Repository layout and engineering conventions

```text
proto/orkstack/v1/     Proto contract; edit here first
gen/go/                Generated Go; never hand-edit
cmd/ork/               Engine entrypoint (`bin/ork`)
cmd/orker/             Orker entrypoint
cmd/orkctl/            CLI entrypoint
internal/engine/       Temporal workflows and activities
internal/api/          Connect services and WebSocket bridge
internal/webui/        The web UI's build embedded into ork, and the handler that serves it at /
internal/dsl/          YAML parse, validation, and Orkflow compilation
internal/eachlines/    The line splitter, batching and line-index format a fan-out over an artifact reads
internal/orker/        Orker runtime and OCI activity dispatch; the runtime files
                       (runtime_docker.go, runtime_dialect.go) alone name Docker or Podman
internal/orker/servicehost/  The Orker Service Contract: the two mounts, steps, the status, the beat, stopping
internal/orker/sourcehost/   The event-source profile: lanes, the wait, receipts, the engine client
internal/task/         Temporal namespace, pool routing, and deployment identity
internal/config/       Configuration file schema and loader
internal/telemetry/    OpenTelemetry setup, sinks, admin listener, instrumentation
internal/dashboards/   The shipped Grafana dashboards, generated, with the drift tests
internal/alerts/       The shipped alert rules and runbooks, generated, with their promtool cases
internal/archive/      Run retirement, the archive sweep and the cluster retention controller
internal/automation/   Automation documents: parse, plan, export
internal/authn/        Who a request is from: trusted issuers, service accounts, client certificates, sign-in
internal/jwt/          The rules OrkStack holds a JWT to, on top of go-jose: algorithms, headers, claims, request binding; signing for the issuer
internal/authz/        The one decision whether an Orkflow may compose another namespace's (shared_with)
internal/identity/     Task identity: the issuer and its signing keys, what engines sign for each other, and the rules of the identity records
internal/evidence/     Run evidence bundle format, writer, and offline verifier
internal/schemas/      JSON Schemas of the authored documents, generated from the parsers' key tables
internal/testsupport/  Test-only helpers shared by suites
internal/payloadcrypt/ Payload encryption: the sealed envelope, a run's key in its memo, the data and failure converters
internal/listfilter/   Structured list filters: allowlist, grammar, matcher
internal/enumname/     The JSON enum-name dialect shared by the API codec and list filters
internal/quantity/     The cpu and memory quantity grammar shared by the DSL and the Orker
internal/runspan/      A run span's id and a child run's trace context, as string work with no OpenTelemetry
internal/runstatus/    The one classifier of a run's status, live or terminal, every reader asks
internal/stablejson/   The JSON bytes OrkStack hashes, stores or hands a container, written with encoding/json/v2
internal/typeid/       The time-ordered ids a run id and a minted Orkflow version are made of
internal/docscheck/    Documentation drift checks (test-only)
internal/apidocs/      docs/API.md, the client-facing API reference, generated from the proto comments
internal/dist/         Release artifacts: binaries, archives, checksums, release guards; the quickstart's guards
internal/healthcheck/  The `--healthcheck` probe of ork and orker
internal/buildinfo/    Which commit a binary was built from, as all three commands report it
internal/hostpolicy/   The Host allowlist every listener applies (API and admin)
internal/held/         The held check for an event source's instance: one function, which
                       the engine's door and the identity mint both call
images/                First-party OCI activity images and source images; public ABI, no SDK
pkg/artifacts/         Local and S3-compatible artifact storage
pkg/logstream/         Live task-log line framing and secret masking
pkg/triggers/          Inbound trigger registry and token hashing
pkg/eventsources/      The event source's domain type, its rules, match and map
pkg/webhooks/          Run-event webhook registry, signing, and delivery
pkg/schedules/         Schedule id shape (the schedule itself lives in Temporal)
ui/                    SvelteKit, Vite, Tailwind, and generated TS clients, built as a static SPA
                       that ork embeds (internal/webui)
site/                  The landing page and docs site (Astro, Starlight): styled by ui/src/tokens.css,
                       its docs written for readers and citing this repository's docs
deploy/                The development stack (Temporal, object storage, registry), the release images'
                       Dockerfile, and the quickstart's source build (deploy/source/; compose.yaml at the root)
examples/              Example Orkflows, automation documents, and custom images
docs/                  Reference material (indexed in README.md; `CLI.md` is generated)
test/                  Activity and service contract fixtures and test-only helper tools
scripts/               Development helpers (`make up`, `make reset`) and the proof harness
                       (`proof.sh`, the gate lists)
.github/               CI: fast check, sharded merge gate, title check, nightly, release
```

Use Go 1.27, one module `github.com/orkstack/orkstack`, `gofmt`, `go vet`, and
wrapped errors. Prefer `context.Context` first and no panics in library code.
Connect serves h2c on `:8080` by default, the WebSocket bridge serves
`/ws/runs/{id}`, and the UI is served at `/` on the same listener. The UI uses
Bun and Svelte 5 runes; generated clients live in
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
not done. The site's screenshots and recordings are regenerated with
`make site-media` on the final tree before the release that ships a UI
change they would show, not in that change (decided 2026-11-16). The site's docs
are written for people (`site/content/`, `site/docs.manifest.json`): short,
task-first pages — tutorials, one-task how-to guides, reference tables, short
concepts — that never copy this repository's normative docs. Each page cites
the repository docs it summarises, which stay the full reference, and the sync
refuses a page that cites none, a citation of a file or heading that does not
exist, and a doc under `docs/` that no page cites. The generated references
(orkctl, configuration, error reasons, API, changelog) are ported as they
are. A change to a behaviour a page describes updates that page too.

Agent ownership is split as follows:

- engine agents: `internal/engine`, `internal/api`, `cmd/ork`, `internal/authn`;
- Orker/activity agents: `internal/orker` (its `servicehost` and
  `sourcehost` included), `images/`, `cmd/orker`;
- CLI agents: `cmd/orkctl`;
- UI agents: `ui/` and `site/`;
- artifact agents: `pkg/artifacts`;
- coordinated changes: `proto/`, `go.mod`, `deploy/`, `Makefile`, `.github/`,
  the gate lists (`scripts/proof-*`), `internal/telemetry`, `internal/config`, `internal/held`, `internal/jwt`,
  `internal/listfilter` and `internal/enumname` (engine and CLI agents share
  both), `pkg/eventsources` (the engine and the Orker both link it), `internal/hostpolicy` (the API and admin listeners share it), `internal/webui` (UI and engine agents share it), and this file.

Use Conventional Commits such as `feat(engine): ...`, `fix(orker): ...`,
`test(dsl): ...`, `docs: ...`, and `chore(proto): ...`. The types are `feat`,
`fix`, `docs`, `chore`, `test`, `ci`, `refactor`, `perf`, `build`, `style` and
`revert`; the scope and a breaking change's `!` are optional. `proofrun
pr-title` is the rule. Keep commits scoped and keep `main` buildable. Release
tags use `vMAJOR.MINOR.PATCH`.

## 13. Explicit non-goals

Do not build:

- a visual workflow designer;
- an AI agent builder;
- a plugin marketplace;
- a code plugin or extension point; profile kinds (`devices`, `gpu`) are a compiled-in closed set
  (§6) and a profile is operator data, so a new kind is a change to this file
  and a release; the NVIDIA hook grammar is a closed set too, never an extension point;
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

A broker is supported by a source image, never by code in `ork` or `orker`:
neither links a broker's client library. A source image is an OCI image
under a public contract, as an activity image is; it is not a plugin, and
there is no loading, registration or marketplace. The set of service
profiles an Orker hosts is closed when it is built. Event sources are
optional and off by default (`ORK_EVENT_SOURCES_ENABLED`); NATS, SQS or any
other broker is never an OrkStack requirement.

A concurrency limit's line orders when accepted runs begin; it is Temporal
workflow state, dispatches nothing, and is not a task queue.

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
- an engineer must never edit a database record to unstick an execution;
- the artifact GC never deletes an object that a live run, a retained run or a
  finished run not yet archived references (as recorded in its current archive
  body, or in its history while it is unarchived), or that a commit still
  present can hand to a memo hit, except for a commit created by a suspended
  Orker inside the moment between the GC's last check and its delete
  (`docs/STORAGE.md`, "Reclaiming space").

A force cancel does not make an execution disappear: termination keeps its
history, and its terminal state is read from that history. A force cancel's
ending that could not be checked before the cluster deletes the run ends, by
the archive sweep's last pass, in an explicit archived state, not in the
exception above.

The first has one stated exception (accepted 2026-11-04): a finished run with no
archive is lost if object storage stays down until the cluster's delete time for
that run, which is fixed when the run closes and cannot be postponed. While that
risk stands, every pass of the archive sweep logs an error and reports a metric
with the runs at risk and when the first of them expires; that is what the
operator gets (`docs/DEPLOY.md`, "The archive sweep"). A run with an archive, and
a run not yet finished, are outside it.

The objective is not feature count. It is the smallest durable execution
substrate engineers can trust with consequential production operations.
