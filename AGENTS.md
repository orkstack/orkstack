# AGENTS.md — OrkStack engineering contract

This file is authoritative for every human and AI agent working in this repo.
Read it fully before changing code. It encodes the invariants that let many
agents build independently without breaking each other.

## 0. What OrkStack is

A workflow engine: users author workflows in a YAML **DSL**; the **engine**
compiles them and runs them on **Temporal**; **Orkers** are generic Temporal
workers that execute **plugins** (containerizable task implementations); a
**CLI** (`orkctl`) and a **SvelteKit UI** drive and observe runs in realtime; an
**artifact store** (local FS or S3/MinIO) carries files between tasks.

```
DSL (yaml) ──apply──> Engine ──compile──> Temporal ──dispatch──> Orker ──run──> Plugin
                        │  ▲                                                      │
                   Connect API                                              artifacts
                    + WS events  <───────────── events ─────────────────────────┘
                        ▲
              CLI (orkctl) / UI (SvelteKit)
```

## 1. Non-negotiable invariants (the contracts)

1. **Proto is the single source of truth.** All cross-process types live in
   `proto/orkstack/v1/*.proto`. Never hand-edit files under `gen/`. Change proto,
   run `make proto`, commit both.
2. **The task contract is fixed:** a plugin receives `(input Payload, inputArtifacts []ArtifactRef)`
   and returns `(output Payload, outputArtifacts []ArtifactRef, error)`. See
   `pkg/plugin`. Payloads are JSON-compatible structs; bytes travel only as
   artifacts, never inline in a Payload.
3. **Determinism boundary:** Temporal *workflow* code (in `internal/engine`) must
   be deterministic — no clocks, no network, no randomness, no direct IO. All
   side effects happen in *activities* or in Orker plugins. Violating this
   corrupts replay.
4. **Artifacts are references, not bytes.** Tasks exchange `ArtifactRef`s; the
   Orker materializes/uploads through `pkg/artifacts`. Never pipe large bytes
   through Temporal payloads or the event stream.
5. **Everything is versioned.** A `Workflow` has a `name` + `version`. Runs pin
   the version they executed.
6. **DONE means proven.** A change is done only when behavior tests pass and a
   proof artifact exists under `proof/` (see §6). No proof, not done.

## 2. Repository layout

```
proto/orkstack/v1/     Proto contract (edit here)
gen/go/                Generated Go (do not edit)
cmd/orkengine/         Engine binary entrypoint
cmd/orker/             Orker binary entrypoint
cmd/orkctl/            CLI binary entrypoint
internal/engine/       Temporal workflows + activities + run store
internal/api/          Connect service implementations + WS bridge
internal/dsl/          YAML DSL parse → validate → compile to Workflow proto
internal/orker/        Orker runtime: worker wiring, plugin dispatch
pkg/plugin/            Public plugin SDK + task contract (imported by plugins)
pkg/plugins/{http,shell,file}/  Builtin plugins
pkg/artifacts/         Artifact storage providers (local FS, S3/MinIO)
ui/                    SvelteKit + Vite + Tailwind app
deploy/                docker-compose (Temporal + MinIO)
examples/              Example workflows (*.ork.yaml)
docs/                  Design + reference docs
proof/                 Committed proof artifacts (logs, traces, recordings)
scripts/               Dev helper scripts
```

## 3. Toolchain & conventions

- **Go 1.25**, single module `github.com/orkstack/orkstack`. Format with `gofmt`;
  `make lint` (go vet + gofmt) must pass. Prefer `context.Context` first arg,
  wrapped errors (`fmt.Errorf("...: %w", err)`), no panics in library code.
- **Temporal Go SDK** (`go.temporal.io/sdk`). Task queues: engine dispatches to
  `orkstack-tasks` by default; a task may override via `queue`.
- **Connect** (`connectrpc.com/connect`) for RPC. Server serves h2c on
  `:8080` (configurable). WebSocket event bridge on the same server at
  `/ws/runs/{id}`.
- **UI**: Bun + SvelteKit (Svelte 5 runes) + Vite + Tailwind. Generated TS
  clients live in `ui/src/lib/gen`.
- **CLI**: Cobra. Human output by default, `--json` for machine output.
- **Config**: environment variables prefixed `ORK_` (e.g. `ORK_TEMPORAL_ADDRESS`,
  `ORK_ARTIFACT_BACKEND`, `ORK_S3_ENDPOINT`). Provide sane localhost defaults.

## 4. The development workflow (every change follows this)

1. If the change crosses a process boundary, update `proto/` first and run
   `make proto`.
2. Write a **behavior test** that fails for the right reason (test observable
   behavior, not implementation detail).
3. Implement until the test passes.
4. `make lint test` must be green. For anything touching Temporal or storage,
   run the integration test against `make infra-up`.
5. Produce/refresh the proof artifact (§6).
6. Commit with a Conventional Commit message (§7). Update the relevant milestone
   todo.

## 5. Testing standards (behavior first)

- **Unit**: pure logic (DSL compile, template rendering, plugin logic) with table
  tests. Fast, no network.
- **Workflow tests**: use Temporal's `testsuite.WorkflowTestSuite` /
  `TestWorkflowEnvironment` to test engine workflows with mocked activities.
- **Integration**: gated behind `-tags=integration` and a running
  `make infra-up`; exercise real Temporal + MinIO. Skip cleanly when infra is
  absent (`t.Skip`).
- **E2E**: full stack — CLI submits, Orker executes, artifacts land in MinIO, UI
  reflects it. Lives in `test/e2e` and drives real binaries.
- Name tests for behavior: `TestHTTPPlugin_CapturesResponseBodyAsArtifact`.
- No sleeps for synchronization; poll with deadlines or use test env helpers.

## 6. Proof artifacts (definition of DONE)

Every milestone writes evidence under `proof/<milestone>/`:
- `test.log` — full `go test -v` (or Playwright) output.
- `coverage.out` / coverage summary where meaningful.
- A behavior trace: e.g. a run's event stream JSON, an artifact roundtrip
  transcript, a CLI session recording, or UI screenshots/video.
- A short `README.md` stating what was proven and how to reproduce.
Proof must be reproducible via a single documented command.

## 7. Commits & branches

- Conventional Commits: `feat(engine): ...`, `fix(orker): ...`, `test(dsl): ...`,
  `docs: ...`, `chore(proto): ...`.
- Keep commits scoped to one concern; keep `main` buildable (`make build test`).
- Tag releases `vMAJOR.MINOR.PATCH`. First target: `v0.0.1`.

## 8. Boundaries between agents (avoid collisions)

- Engine agents own `internal/engine`, `internal/api`, `cmd/orkengine`.
- Orker/plugin agents own `internal/orker`, `pkg/plugin`, `pkg/plugins/*`,
  `cmd/orker`.
- CLI agents own `cmd/orkctl`.
- UI agents own `ui/`.
- Artifact agents own `pkg/artifacts`.
- **Shared/coordinated** (change deliberately, announce in commit): `proto/`,
  `go.mod`, `deploy/`, `Makefile`, this file. If you must change proto, do it in
  a dedicated commit and regenerate.

## 9. Security & safety

- The `shell` plugin executes commands — it runs only inside an Orker's sandbox
  boundary and never interpolates untrusted input without the templating layer's
  escaping. Document any command construction.
- Secrets come from env/secret refs, never committed. `.env` is gitignored.
