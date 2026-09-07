# OrkStack Architecture

## Overview

OrkStack turns a declarative YAML Orkflow into durable execution on Temporal.
It is the thin contract between three primitives:

- **Temporal** owns workflow lifecycle, durable state, retries, timers,
  cancellation, and signals.
- **Object storage** owns artifacts, logs, immutable Orkflow definitions, and
  immutable activity completion records.
- **OCI containers** own computation. Any compatible image can be an activity
  without linking an OrkStack SDK.

The engine is the deterministic control plane. Orkers are disposable execution
workers that bridge Temporal, object storage, and an OCI runtime.

## Components

### 1. DSL (`internal/dsl`)

Parses `*.ork.yaml`, validates names and the dependency DAG, resolves each
`uses:` catalog name or registry reference to an immutable image digest at
apply time, and compiles one versioned `orkstack.v1.Orkflow`. Tasks select a
user-facing `pool`; Temporal task-queue translation stays internal.

### 2. Engine (`internal/engine`, `cmd/ork`)

- Hosts the generic deterministic `RunWorkflow` interpreter on Temporal.
- Schedules `ExecuteTask` activities, honoring dependencies, retries, timeouts,
  cancellation, and signals.
- Keeps large bytes out of Temporal by carrying artifact references.
- Serves the Connect API and WebSocket bridge through `internal/api`.
- Uses Temporal as the read model for live runs and shared object storage for
  archived runs beyond retention; process memory is never durable truth.

### 3. Orker (`internal/orker`, `cmd/orker`)

An Orker polls a pool's internal Temporal channel and executes the OCI Activity
Contract. It checks for an existing durable completion, resolves and pins the
image, creates an isolated `/orkstack` workspace, materializes artifacts and
secret files, runs the container, captures stdout and stderr independently,
heartbeats, forwards cancellation, validates outputs, uploads artifacts, and
writes the immutable completion manifest before reporting success to Temporal.

Activity containers receive no Temporal, object-storage, or control-plane
credentials unless the user explicitly supplies workload secrets. The Orker is
the security boundary and is stateless beyond caches and ephemeral workdirs.

### 4. Activity images (`images/` and arbitrary third-party OCI images)

The filesystem is the activity API:

- rendered configuration and metadata are regular files under
  `/orkstack/input`;
- compact structured output is `/orkstack/output/values.json`;
- output artifacts are regular files under `/orkstack/output/artifacts`;
- stdout and stderr are logs; exit zero succeeds and non-zero fails;
- SIGTERM, followed by SIGKILL after the grace period, is cancellation.

The first-party `activity-http`, `activity-shell`, and `activity-file` images are
reference implementations of this same public contract, not privileged Go
plugins. A custom activity is just another OCI image.

### 5. Artifacts and completion records (`pkg/artifacts`)

Local and S3-compatible stores hold content-addressed artifacts, logs, immutable
definition snapshots, run archives, and completion records. The Orker uses
conditional create for `commit.json`; a retry reads an existing valid commit
and does not execute the container again. This bounds duplicate execution only
after OrkStack has durably committed the result; arbitrary external side
effects remain at-least-once.

### 6. API, CLI, and UI

- `internal/api` implements generated Connect services and `/ws/runs/{id}`.
- `cmd/orkctl` provides human output by default and `--json` for automation.
- `ui/` is the SvelteKit control surface using generated Connect clients.
- `ActivityService` exposes the configured activity catalog and live Orker
  description; there is no plugin service or registry.

### Telemetry (`internal/telemetry`)

OpenTelemetry is the only telemetry mechanism in the codebase, and OTLP plus a
few standard scrape endpoints are the only way a signal leaves a process —
there is no per-vendor code. Everything is off by default: with no `OTEL_*` or
`ORK_ADMIN_ADDR`/`ORK_LOG_*` set, `ork` and `orker` behave exactly as they did
before this package existed. The hooks live in three places only — activities,
the Orker, and the API — so **workflow code never imports OpenTelemetry**: a
`RunWorkflow` replay must stay a pure function of its history (§2, replay
safety), and an instrumentation call is exactly the kind of side effect that
would make it not one. Activity images stay SDK-free for the same reason
`images/` needed no OrkStack SDK in the first place: stdout and stderr are
already captured, already durable, and already enough (`docs/OCI_ACTIVITY_CONTRACT.md`).
A run's telemetry selection — which sink, whether task logs export, which
attributes — is decided once at submit from the namespace's policy and carried
on the `RunPlan` like `CommitRoot` is: replay-pinned, and unset when nothing is
configured. A namespace's chosen sink routes that run's task logs, spans and
lifecycle metrics; the engine and Orker's own process-level signals — their
own slog output, `/metrics`, the Temporal SDK's own metrics — stay on the
default sink regardless of what any namespace selects.
See `docs/DEPLOY.md`, "Telemetry", for the operator side.

## Data flow of a run

1. `orkctl apply` compiles an Orkflow, resolves image references to digests, and
   stores an immutable definition version.
2. `orkctl run submit` starts `RunWorkflow` with that pinned definition.
3. The workflow schedules ready tasks on the channels derived from their pools.
4. An Orker executes each task's pinned OCI image and durably commits outputs.
5. Temporal records task and run state; object storage retains bytes and archive
   records; CLI and UI read through the API and resume event streams by sequence.

See the [DSL reference](docs/DSL.md) for authoring and the
[OCI Activity Contract](docs/OCI_ACTIVITY_CONTRACT.md) for the authoritative
execution boundary.
