# OrkStack Architecture

OrkStack turns a declarative YAML Orkflow into durable execution on Temporal.
It is the thin contract between three primitives:

- **Temporal** owns workflow lifecycle, durable state, retries, timers,
  cancellation, signals, schedules and the live-run read model.
- **Object storage** owns artifacts, logs, immutable Orkflow definitions,
  run snapshots and archives, and immutable completion, attempt and audit
  records.
- **OCI containers** own computation. Any compatible image can be an activity
  without linking an OrkStack SDK.

`ork` (the engine) is the deterministic control plane and API; Orkers are
disposable execution workers that bridge Temporal, object storage and an OCI
runtime. Neither holds durable state, so any `ork` replica answers for any run
and any Orker of a pool can run any unpinned task. [AGENTS.md](./AGENTS.md) is
the contract these components obey; this page is the map.

```text
 orkctl / UI ──Connect, WebSocket──┐   triggers, await callbacks ──plain HTTP──┐
                                   ▼                                            ▼
                     ork: internal/api  (submit preparation, reads, audit) ◄────┘
                                   │ start RunWorkflow
                                   ▼
          Temporal ── ork-tasks: RunWorkflow, child runs, schedule fires
                   ── ork-engine-events: PublishEvent projection (ork)
                   ── ork-tasks / ork-pool-*: ExecuteTask (Orkers)
                                   │
                                   ▼
                     orker ──► OCI container ──► /orkstack files
                                   │
                                   ▼
                     object storage (pkg/artifacts)
```

## Components

### DSL (`internal/dsl`)

Parses `*.ork.yaml`, validates names, the dependency DAG and every task
feature (pins, `each:`, `await:`, `cache:`, `join:`, composition and
`outputs:`), resolves each `uses:` catalog name or registry reference to an
immutable image digest at apply time, and compiles one versioned
`orkstack.v1.Orkflow` with a canonical form and SHA-256 definition digest.
Malformed DAGs are refused here, never discovered at runtime. Tasks select a
user-facing `pool`; Temporal task-queue translation stays internal
(`internal/task/pool.go`).

### API (`internal/api`, served by `cmd/ork`)

Connect services (Orkflow, Run, Namespace, Activity, System, Webhook,
Trigger, Schedule, Stats, Cache, Automation, Audit) on one h2c port, plus
`/ws/runs/{id}` and the per-task log socket, and two plain-HTTP route families
meant for callers outside the operator's boundary: trigger invoke
(`POST /v1/triggers/{id}`) and the await callback door
(`POST /v1/runs/{run}/awaits/{task}[/reject]`). OrkStack does not
authenticate callers; the deployment's gateway does, and may assert a
principal in `Ork-Principal`. An interceptor writes one immutable audit record
for every control-plane write.

**Submit preparation** is the last point where anything outside the
definition can shape a run, because workflow code may read nothing but its
input and history. Every starter — `orkctl run submit`, `run again`, a
trigger, a schedule fire, a composed child — goes through it:

1. resolve the namespace and the pinned Orkflow version;
2. the admission gate: `hold`, then `max_running` (a read of the cluster's
   execution index), then an enforced budget (a usage read);
3. the pool plan: each task's pool, the namespace's allow-list, a live Orker
   on each pool, and the Orker capabilities the definition needs (fan-out,
   cache, join);
4. the image-policy selection, telemetry selection, pin and cache plans, and
   the plan of every composed child, transitively;
5. the origin, and one-time callback tokens for `callback: true` awaits;
6. an idempotency claim when a key is given, then the run ID and the
   `RunWorkflow` start.

Everything decided here rides the replay-pinned `RunPlan` and is never
re-read. A refusal happens before step 6, so it leaves nothing behind.

### Engine (`internal/engine`, `cmd/ork`)

- **`RunWorkflow`** is the one generic, deterministic interpreter. It walks
  the DAG, evaluates conditions and join policies, dispatches `ExecuteTask`
  activities with retry policies and timeouts, assigns event sequence numbers,
  and moves run snapshots out of history through a local activity, so history
  holds references only. Its feature paths:
  - **pins** open a Temporal Worker Session on one pin host and route every
    member through its per-host channel;
  - **fan-out** counts the `each:` array once (from the input, or a local
    activity recorded as a marker) and dispatches each item as its own
    activity with its own idempotency key, commit key, retries and timeout;
  - **composition** starts a child run as a Temporal child workflow of the
    same interpreter on `ork-tasks`, with its own run ID, parent-close policy
    `REQUEST_CANCEL`, and the plan the parent's submit built; the child
    inherits its parent's release;
  - **awaits** start a deadline Timer and wait for a resolution Signal; no
    container runs;
  - **caching** attaches a cache context to a cached task's dispatch; the
    memo decision is the Orker's;
  - **joins** classify each task from how its needs ended; once a run is
    failing, only failure handlers start, on a context fail-fast does not
    cancel, and the run still ends `FAILED`.
- **Projection.** Every event is published by a `PublishEvent` activity on
  `ork-engine-events`, which any `ork` replica may execute: it resolves the
  event's snapshot, feeds the write-side `RunStore`/`EventBus`, announces
  subscribable events to the webhook notifier and every event to telemetry
  (once per dedupe key, so retries and replays announce nothing twice), and on
  a terminal event writes the run archive from the system of record rather
  than its own partial bus.
- **Other workflows.** `IdempotencyClaimWorkflow` holds a submit key for
  `ORK_IDEMPOTENCY_TTL`. `ScheduleFireWorkflow` is the action of every
  Temporal Schedule: it runs submit preparation in a local activity and starts
  the run, or records the refusal.
- **Reads.** Live runs are read from Temporal (`TemporalRunReader`), finished
  runs past retention from the shared archive, never from process memory.
  Stats, failure fingerprints, flakes, usage and cache hit rates are derived
  on read from Temporal and records already in storage; nothing keeps a
  counter. `PlanRun` simulates a run's DAG without starting it.
- **Releases.** The engine reads and changes which release (Temporal Worker
  Deployment Version) takes new runs, and what each release still executes;
  nothing about a release is stored by OrkStack.
- **Fleet.** Every Orker polls the presence channel `ork-orkers`, whose
  poller list is the fleet, and its own control channel `ork-orker-<id>`,
  which carries `DescribeOrker` and `CordonOrker`.

### Orker (`internal/orker`, `cmd/orker`)

An Orker polls its pool's internal channel and executes the OCI Activity
Contract for one attempt: check for an existing durable commit, check the
image against the run's image policy (including a cosign signature), run a
cached task's memo step, resolve and pin the image, create an isolated
`/orkstack` workspace, materialize artifacts and secret files, run the
container, capture stdout and stderr independently, heartbeat the live phase,
forward cancellation as SIGTERM then SIGKILL, validate outputs, upload
artifacts, and write the immutable `commit.json` and attempt record before
reporting success to Temporal. A memo hit writes the run's own commit from the
source commit's content-addressed objects and starts no container.

Activity containers receive no Temporal, object-storage, or control-plane
credentials unless the user explicitly supplies them as workload secrets. The
Orker is the security boundary and is stateless beyond an image cache and
ephemeral workdirs (a pin's shared directory lives as long as its pin, bounded
by a lease). Docker, and Podman as a dialect of the same runtime, sit behind the `ContainerRuntime` seam.

### Activity images (`images/` and any OCI image)

The filesystem is the activity API:

- rendered configuration and metadata are regular files under
  `/orkstack/input`;
- compact structured output is `/orkstack/output/values.json`;
- output artifacts are regular files under `/orkstack/output/artifacts`;
- pinned tasks share `/orkstack/shared`;
- stdout and stderr are logs; exit zero succeeds and non-zero fails;
- SIGTERM, followed by SIGKILL after the grace period, is cancellation.

The first-party `activity-http`, `activity-shell`, `activity-file`,
`activity-json`, `activity-git` and `activity-template` images are reference
implementations of this same public contract, not privileged plugins. An image
may declare its `with:` interface in the `org.orkstack.activity.interface`
label, which validate and apply then check; its author writes it as an
`interface.ork.yaml` (`kind: ActivityInterface`) that `orkctl` packs into the
label at build time.

### Storage (`pkg/artifacts`)

Local and S3-compatible stores hold content-addressed artifacts and every
platform record: Orkflow definitions and current pointers, namespaces,
deployment settings (retention, image policy), triggers, webhooks and their
delivery records, audit records, commits, attempt records, memos, logs, run
snapshots and run archives. Writes that must not race use conditional create
or compare-and-swap. The key layout and its invariants are in
[docs/STORAGE.md](docs/STORAGE.md).

### Retention (`internal/archive`)

A finished run is retired at its namespace's run retention: the Temporal
execution is deleted explicitly and the run's OrkStack-owned storage is
reclaimed. The cluster's own retention TTL is derived from the maximum run
retention plus a margin, and is never a user setting.

### Clients (`cmd/orkctl`, `ui/`)

`orkctl` is a Cobra CLI with human output by default and `--json` for
automation; the SvelteKit UI uses the generated Connect clients. Both perform
the same operations with the same selectors and errors
([docs/UI_CLI_PARITY.md](docs/UI_CLI_PARITY.md)). `orkctl apply` also applies
automation documents by planning against `AutomationService` and sending each
planned write to its ordinary RPC.

### Telemetry (`internal/telemetry`)

OpenTelemetry is the only telemetry mechanism, and OTLP plus the scrape and
health endpoints on `ORK_ADMIN_ADDR` are the only way a signal leaves a
process; there is no per-vendor code. Everything is off by default. The hooks
live in activities, the Orker and the API, and **workflow code never imports
OpenTelemetry**: a `RunWorkflow` replay must stay a pure function of its
history, and an instrumentation call is a side effect. Activity images stay
SDK-free: stdout and stderr are already captured and durable.

A run's telemetry selection — which sink, whether task logs export, which
attributes — is decided at submit from its namespace's policy and carried on
the `RunPlan`. A namespace's sink carries that run's task logs, spans and
lifecycle metrics; the processes' own logs, `/metrics` and Temporal SDK
metrics stay on the default sink. See [docs/DEPLOY.md](docs/DEPLOY.md),
"Telemetry".

## Data flow of a run

1. `orkctl apply` compiles an Orkflow, resolves image references to digests,
   resolves composed references, and stores an immutable version addressed by
   its definition digest.
2. A starter calls submit; preparation decides routing and policy and starts
   `RunWorkflow` with the pinned definition and its `RunPlan`.
3. The interpreter schedules ready tasks on the channels derived from their
   pools, starts child runs, opens awaits, and expands fan-outs.
4. An Orker executes each attempt's pinned OCI image and durably commits its
   outputs, or adopts an existing commit or memo.
5. Each event is projected to subscribers and telemetry; the terminal event
   archives the run. CLI and UI read live runs from Temporal and finished ones
   from the archive, and resume event streams by sequence on any replica.
6. At its run retention the run is retired and its storage reclaimed.

See the [DSL reference](docs/DSL.md) for authoring and the
[OCI Activity Contract](docs/OCI_ACTIVITY_CONTRACT.md) for the authoritative
execution boundary.
