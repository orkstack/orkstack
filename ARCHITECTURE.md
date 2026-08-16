# OrkStack Architecture

## Overview

OrkStack turns a declarative YAML orkflow into a durable, observable execution
on Temporal. It separates **orchestration** (deciding what runs when — the
engine, on Temporal) from **execution** (doing the work — Orkers running
plugins). This mirrors the split that makes systems like Temporal, Argo, and
Kestra reliable: the control plane is deterministic and replayable; the data
plane is where side effects live.

## Components

### 1. DSL (`internal/dsl`)
Parses `*.ork.yaml` into the `orkstack.v1.Orkflow` proto. Responsibilities:
- Schema validation (required fields, unique task ids, known plugins).
- DAG validation (`needs` references exist; no cycles; topological soundness).
- Template pre-flight (references to `.inputs`, `.tasks.<id>.output`, and the
  `artifact` helper are well-formed).
Output is a compiled, versioned `Orkflow` (version = content hash unless the
author pins one).

### 2. Engine (`internal/engine`, `cmd/ork`)
- **Temporal client + worker** hosting the orchestration workflow.
- **`RunWorkflow`** (deterministic): interprets the orkflow's task DAG. For each
  task it schedules an **activity** (`ExecuteTask`) on the task queue, honoring
  `needs`, `condition`, `retry`, and `timeout`. It threads outputs/artifacts
  from completed tasks into the templating context of downstream tasks.
- **`ExecuteTask` activity** is a thin dispatcher: it does not run the plugin
  itself. Instead the engine routes task execution to Orkers. In v0 the activity
  is *implemented by the Orker worker* (the Orker registers the `ExecuteTask`
  activity on the shared task queue), so the engine schedules and the Orker runs.
  This keeps the engine free of plugin code and lets Orkers scale independently.
- **Run store**: an in-process, pluggable store (v0: in-memory + Temporal as the
  source of truth via queries/visibility) that materializes `Run`/`TaskRun` for
  the API. Temporal holds durable state; the store is a read model.
- **Event bus**: the engine publishes `orkstack.v1.Event`s (run/task lifecycle,
  logs) to an in-process pub/sub, consumed by the Connect `StreamEvents` RPC and
  the WebSocket bridge. Events are also derivable from Temporal history for
  resume/backfill (`from_sequence`).

### 3. API (`internal/api`)
- Connect handlers implementing `OrkflowService`, `RunService`, `PluginService`.
- WebSocket bridge at `/ws/runs/{id}` that subscribes to the event bus and
  forwards JSON-encoded events to browsers (the UI's realtime channel).
- Served over h2c so `connect-go` (CLI) and `connect-web` (UI) share one port.

### 4. Orker (`internal/orker`, `cmd/orker`)
- A Temporal worker that registers the `ExecuteTask` activity and a **plugin
  registry**. On each task: resolve input artifacts (download to a scratch dir
  via `pkg/artifacts`), render the plugin config template, invoke the plugin,
  collect output payload + produced artifacts (upload via `pkg/artifacts`), and
  return `ArtifactRef`s. Emits heartbeats and log events.
- Orkers are horizontally scalable and stateless beyond scratch space.

### 5. Plugins (`pkg/plugin`, `pkg/plugins/*`)
- `pkg/plugin` defines the `Plugin` interface and the `Task` context (input,
  artifact resolver, logger, workdir).
- **Every plugin selects its operation with one generic field, `op`** — the
  first line of every task, in every plugin. `op` lives entirely inside `with:`,
  so adding an operation touches one plugin and never the proto, the engine or
  the DSL. Each plugin declares its ops as an `enum` in `Describe()` and rejects
  an unknown `op` with a permanent error before any side effect. The convention,
  its rules and every builtin op are in
  [`docs/PLUGIN_DESIGN.md`](docs/PLUGIN_DESIGN.md). Builtin plugins:
  - `http` (`op: get | post | put | patch | delete | head`): perform an HTTP
    request; capture body as artifact; expose status, headers, parsed JSON on
    output.
  - `shell` (`op: exec | eval | script`): run a command or a script file in the
    scratch dir; capture stdout/stderr; expose exit code; optionally capture
    files as artifacts. `exec` and `script` fail the task on a non-zero exit,
    `eval` reports it as data.
  - `file` (`op: write | read | copy | delete`): manipulate files; produce or
    consume artifacts.
- The interface is designed so a plugin can later be packaged as a container
  image with the same input/output contract — and, being a plugin, with the same
  `op` convention.

### 6. Artifacts (`pkg/artifacts`)
- `Store` interface: `Put`, `Get`, `Stat`, `List`, `Presign`, `Delete`.
- Implementations: `local` (filesystem, dev/hosted) and `s3` (aws-sdk-go-v2,
  MinIO-compatible, works with any S3-compatible provider). Provider is chosen
  by `ORK_ARTIFACT_BACKEND`. New providers implement `Store` and register.

### 7. CLI (`cmd/orkctl`)
- Cobra app using generated `connect-go` clients: `apply`, `run submit`,
  `run get`, `run list`, `run watch` (streams events), `plugin list`,
  `artifact get`.

### 8. UI (`ui/`)
- SvelteKit app; `connect-web` for RPC; a WebSocket store subscribes to
  `/ws/runs/{id}` and drives a reactive run graph and task/log panes.

## Data flow of a run

1. `orkctl apply hello.ork.yaml` → the `ork` engine compiles + stores the `Orkflow`.
2. `orkctl run submit hello-ork --input url=...` → the engine starts a Temporal
   workflow (`RunWorkflow`) with the compiled DAG and input, in the
   `ORK_TEMPORAL_NAMESPACE` namespace (default `ork-main`). Runs are named
   `ork-{timestamp}-{sequence}`.
3. `RunWorkflow` schedules ready tasks as `ExecuteTask` activities on
   `ORK_ORKER_QUEUE` (default `ork-tasks`).
4. An Orker picks up the activity, resolves artifacts, runs the plugin, uploads
   outputs, returns refs.
5. The engine records the `TaskRun`, publishes events, and schedules newly-ready
   tasks until the DAG completes.
6. CLI `run watch` and the UI receive events in realtime; artifacts are readable
   from the store.

## Key decisions (and why)

- **Temporal as the state machine**: durable execution, retries, timeouts,
  visibility, and replay come for free; we don't reinvent a scheduler.
- **Orker-hosted activity**: keeps plugin/runtime code out of the deterministic
  engine and lets execution scale and be sandboxed independently.
- **Artifacts by reference**: keeps Temporal payloads small and lets large data
  live in purpose-built object storage.
- **Connect over gRPC+HTTP**: one proto contract, native clients for Go (engine,
  CLI) and the browser (UI), with server streaming plus a WS bridge for realtime.
- **Single Go module**: simpler dependency and refactor story for a young
  monorepo than multi-module `go.work`.

See `docs/DSL.md` and `docs/PLUGINS.md` for the authoring and extension surfaces.
