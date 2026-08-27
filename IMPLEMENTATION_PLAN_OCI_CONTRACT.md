# OCI Activity Contract Implementation Plan — Phases 1–3

**Status:** Ready for implementation  
**Base commit:** 91a441f  
**Target tag:** v0.0.5  
**Spec reference:** docs/OCI_ACTIVITY_CONTRACT.md (normative)  
**Grounding:** AGENTS.md §§1–15, accepted design

---

## Executive Summary

This plan implements the OCI Activity Contract design across three phases, cutting at clean seams to enable parallel team work. The contract replaces the in-process Go plugin registry with pure OCI execution: any OCI image becomes a durable production activity without linking against an SDK.

**Key invariants:**
- Container never holds platform credentials (security boundary is the Orker)
- Bytes never travel through Temporal (only references, except ≤4 KiB values.json)
- Engine never touches filesystem or registry inside workflow code (apply-time only)
- Execution contracts documented as an ABI, measured against evidence from §14

---

## Phase 1: Wire Freeze and Workspace

**Owner:** Engine agents (`internal/engine`, `internal/orker`, `internal/task`)  
**Acceptance gate:** Replay fixture reset + `make lint test`  
**Proof milestone:** M11.1

### Phase 1 Deliverables

#### 1.1 Proto Contract (§2)

**Files to create/modify:**

- **NEW:** `proto/orkstack/v1/activity.proto` (440 lines from spec §2.2)
  - `ImageRef` — resolved image with index digest, platform info, delivery method
  - `ResourceRequest` — CPU/memory in Kubernetes spelling
  - `NetworkMode` enum — NETWORK_MODE_DEFAULT, NETWORK_MODE_NONE only
  - `SecretRef` — workload-readable secret files
  - `WorkspaceLimits` — engine-wide deployment limits (256 KiB values, 5 GiB artifacts, 4 MiB commits)
  - `UpstreamRef` — fan-in references, no bytes
  - `ExecuteTaskInput` — activity coordinates, image, config, workspace limits, upstream refs
  - `CommitRef` — durable commit object reference with sha256 and etag
  - `ExecuteTaskResult` — success result with attempts, artifacts, platform info
  - `ActivityCommit` — immutable completion manifest (schema_version = 1)
  - `ActivityFailureDetail` — failure transport for unreachable artifacts
  - `DescribeOrkerResult` — Orker identity for pool liveness

- **MODIFY:** `proto/orkstack/v1/common.proto`
  - Add `FailureCategory` enum: ACTIVITY_FAILED, INFRASTRUCTURE_ERROR, CONTRACT_VIOLATION, TIMED_OUT, CANCELLED
  - Add `category: FailureCategory` field 5 to `Error`

- **MODIFY:** `proto/orkstack/v1/orkflow.proto`
  - `Task` field 2: reserve `plugin` (never reuse)
  - `Task` field 9: add `pool: string`
  - `Task` fields 10–14: add `uses`, `image`, `command`, `env`, `secrets`, `resources`, `network`, `run_as_orker_user`
  - Add imports: `google/protobuf/struct.proto`, `orkstack/v1/activity.proto`

- **MODIFY:** `proto/orkstack/v1/run.proto`
  - `TaskRun` field 2: reserve `plugin`
  - `TaskRun`: add `values_ref`, `commit`, `exit_code`, `system_artifacts`, `values_json` (bytes), `values_inlined`, `image`, `commit_adopted`, `attempt`
  - `Run`: add `orkflow_definition_digest`
  - `RunPlan`: add `commit_root`, `limits`, `transfer_budget_seconds`

- **MODIFY:** `proto/orkstack/v1/service.proto`
  - Delete `PluginService`, `PluginInfo`, `ListPluginsRequest`, `ListPluginsResponse` (mark as reserved)
  - Add `ActivityService` with methods: `ListActivities`, `DescribeOrker`
  - Add `ActivityInfo` message (name, registry reference, platforms)
  - Add `ListActivitiesRequest` / `ListActivitiesResponse`
  - Add `DescribeOrkerRequest` / `DescribeOrkerResponse`

**Acceptance criteria:**
- `make proto` succeeds
- `buf lint` passes (no diagnostics)
- `buf breaking --against <tree>` reports only the six intentional deletions (WIRE and WIRE_JSON modes pass)
- Generated Go files in `gen/go/` are deterministic

#### 1.2 Artifact Store Interface (§5)

**Files to create:**

- **NEW:** `pkg/artifacts/commit_store.go`
  - Interface `CommitStore`: methods `Put(ctx, key, data, opts)`, `Get(ctx, key)`, `Exists(ctx, key)`
  - `CommitKey(namespace, runID, taskID)` → deterministic key
  - In-memory implementation `MemoryStore` for testing

- **NEW:** `pkg/artifacts/ref_predictor.go`
  - Interface `RefPredictor`: methods to predict upload URIs before execution
  - Used in `OCIDispatcher.Validate` (§5.4 step 4)

**Acceptance criteria:**
- Interface is frozen for Phase 2 implementations
- In-memory implementation passes all tests
- `OCIDispatcher` call sites (steps 0, 4, 8 of §5.4) compile against the interface

#### 1.3 Orker Refactoring: OCIDispatcher and Workspace

**Files to create/modify:**

- **NEW:** `internal/orker/oci_dispatcher.go`
  - `OCIDispatcher` type: owns activity execution against OCI runtime
  - Constructor: takes `ContainerRuntime`, `CommitStore`, artifact store, limits
  - Methods: `Execute(ctx, input) → result | error`, `Validate(input)`, `validateOutput(dir, limits)`
  - Durable commit protocol (§5.4): steps 0–8 implemented

- **NEW:** `internal/orker/container_runtime.go`
  - Interface `ContainerRuntime`: abstract OCI runtime
  - Methods: `Create(ctx, config)`, `Start(ctx, containerID)`, `Wait(ctx, containerID)`, `Kill(ctx, containerID)`, `Remove(ctx, containerID)`, `LoadLayout(ctx, layoutPath, imageRef)`
  - `DockerRuntime` implementation using Docker daemon API

- **NEW:** `internal/orker/workspace.go`
  - Workspace construction per §3.1–§3.6: modes, permissions, mounts
  - `task.json` serialization per §3.2 table
  - `config.json` — rendered template from `ExecuteTaskInput.with` (Orker owns rendering)
  - `run.json` — run context
  - Input staging: materialize artifacts into `/orkstack/input/artifacts/`
  - Output scan §3.5: validate artifact names (UTF-8, max 128 chars, no `..`), count, total size
  - Readability check §3.1: stat then open for every output file
  - Limits enforcement: values.json ≤256 KiB, artifacts ≤256 count, ≤5 GiB each, stdout/stderr ≤16 MiB each

- **NEW:** `internal/orker/heartbeat.go`
  - Heartbeat loop: `HeartbeatTimeout` function
  - `WaitForCancellation` function: trap and forward SIGTERM → child process
  - SIGTERM → grace period → SIGKILL sequence (default 30s)

- **MODIFY:** `internal/orker/activity.go` (new `ExecuteTask` activity)
  - Replace plugin registry call with `OCIDispatcher.Execute`
  - Call `DescribeOrker` heartbeat probe periodically
  - Populate `Error.category` on failure paths (§8)

- **DELETE:** `pkg/plugins/*` and `pkg/plugin.Registry`
- **MOVE:** `pkg/logstream` — move linewriter, reuse for stdout/stderr capture

**Acceptance criteria:**
- `OCIDispatcher` executes a reference `alpine:3.20 echo "hello"` end-to-end
- Workspace modes and permissions match §3.1 evidence (§14.3)
- Output files are readable by Orker across bind-mount types (§14.8)
- Heartbeat integrates with Temporal activity lifecycle
- Plugin registry is removed, no lingering references

#### 1.4 Engine Refactoring: Task Dispatch and Failure Categories

**Files to modify:**

- **MODIFY:** `internal/engine/workflow.go`
  - Replace `plugin` input with `image` from resolved `Task.image`
  - Build `ExecuteTaskInput` proto: pack coordinates, image, `with:` block (raw, no render), workspace limits, upstream refs, idempotency key, commit URI
  - Pass `RunPlan.CommitRoot` to activity so Orker composes commit URI deterministically
  - Pack `RunPlan.Limits` and `RunPlan.TransferBudgetSeconds` into `ExecuteTaskInput.limits`
  - `ActivityOptions`: set `NonRetryableErrorTypes` for `FAILURE_CATEGORY_CONTRACT_VIOLATION`, `FAILURE_CATEGORY_CANCELLED`
  - Extract `ExecuteTaskResult` and populate `TaskRun.values_ref`, `commit`, `exit_code`, `system_artifacts`, `values_json` (inlined if ≤4 KiB)
  - Handle `ActivityFailureDetail` on error: extract artifacts, attempt, reason code
  - Populate `TaskRun.image` with platform_digest and platform

- **MODIFY:** `internal/engine/pool_probe.go`
  - Change probe from `ListPlugins` to `DescribeOrker` (§7.2 gate)
  - Verify pool liveness through the cut

- **MODIFY:** `internal/task/contract.go`
  - Document `ExecuteTaskInput` as the only Temporal activity argument
  - Encode payload as proto, not JSON struct (§2.3)
  - Wire size validation: fail `INPUT_TOO_LARGE` if ≥262144 bytes

- **MODIFY:** `internal/task/pool.go`
  - Ensure pool name → task queue routing is unchanged
  - Verify `ork-pool-<slug>` prefix only

**Acceptance criteria:**
- `ExecuteTaskInput` serializes deterministically
- Wire size validation catches oversized inputs
- `DescribeOrker` probe replaces `ListPlugins` end-to-end
- Failure categories route correctly to Temporal retry policies
- Pool routing is verified to be unchanged

#### 1.5 Security Profile (§9)

**Files to create:**

- **NEW:** `internal/orker/security_profile.go`
  - Security profile struct: `no-new-privileges`, `cap-drop=ALL`, `seccomp=builtin`, PIDs limit, memory limit
  - Per-pool policy enforcement (Orker-local, not engine config)
  - Refuse: `--privileged`, host PID/network, arbitrary host mounts, Docker socket, Orker credentials (§9.1)

**Acceptance criteria:**
- Refusals are enforced before container creation
- Security flags are applied consistently
- No privileged escapes

#### 1.6 First-Party Activity Images (Stub for Phase 1)

**Files to create (stub entry points):**

- **NEW:** `images/Makefile` — build target (empty for Phase 1, filled Phase 3)
- **NEW:** `images/activity-shell/Dockerfile` (stub)
- **NEW:** `images/activity-http/Dockerfile` (stub)
- **NEW:** `images/activity-file/Dockerfile` (stub)

**Acceptance criteria:**
- Phase 3 will implement; Phase 1 creates seams only

#### 1.7 Proof and Validation (§11.4, §14)

**Files to create/modify:**

- **MODIFY:** `internal/engine/testdata/history/run-succeeded-v1.json`
  - **ONE RESET:** Replace with new history that encodes proto `ExecuteTaskInput` (not JSON)
  - This is the only fixture reset allowed (§11.4)

- **NEW:** `proof/M11.1/Phase1_WireAndWorkspace.sh`
  - Proof script demonstrating:
    - Proto compiles and lints
    - `buf breaking` reports only six intentional deletions
    - Workspace modes/permissions per §14.3
    - Output readability check per §14.8
    - Heartbeat loop with cancellation per §14.5 and §14.14
    - Entry script correct per §14.18
    - `DescribeOrker` probe works end-to-end

- **NEW:** `proof/M11.1/README.md`
  - Reproduction command: `proof/M11.1/Phase1_WireAndWorkspace.sh`
  - Expected output: all probes pass
  - Exit 0 on success, 1 on failure

**Acceptance criteria:**
- `make lint test` passes
- `make merge-check` passes on merged tree
- Proof script reproduces all §14 evidence for Phase 1
- Replay test passes with new fixture

---

## Phase 2: Durability (Artifact Backends)

**Owner:** Artifact agents (`pkg/artifacts`)  
**Acceptance gate:** Chaos integration tests + proof + `make lint test`  
**Proof milestone:** M11.2  
**Constraint:** Does NOT open `internal/orker` or `internal/engine`

### Phase 2 Deliverables

#### 2.1 CommitStore Implementations

**Files to modify:**

- **MODIFY:** `pkg/artifacts/commit_store.go`
  - S3 `CommitStore` implementation: use conditional PutObject with `IfNoneMatch: "*"` (§5.3)
  - Local `CommitStore` implementation: temp file + `fsync` + `link(2)` for atomic publication (§5.3, §14.7)
  - Boot-time two-writer probe: verify conditional-write primitive (§14.1)
  - `CommitKey` function: deterministic key from namespace/runID/taskID

#### 2.2 RefFor and Content-Addressed Keys

**Files to modify:**

- **MODIFY:** `pkg/artifacts/ref.go`
  - `RefFor` function: compute artifact URI with content-addressed sixth segment (§5.4)
  - S3 backend: use object key = `<pool>/<namespace>/<run>/<task>/<name>/<sha256>`
  - Local backend: same structure in filename

#### 2.3 Durable Logs as System Artifacts

**Files to modify:**

- **MODIFY:** `pkg/artifacts/logstream.go` (moved from `pkg/logstream`)
  - Capture stdout/stderr as log streams
  - Upload as system artifacts `stdout.log`, `stderr.log` with URIs and SHAs
  - Enforce log-size limit per stream (default 16 MiB)

#### 2.4 Archive Write Side

**Files to modify:**

- **MODIFY:** `pkg/archive/writer.go`
  - Write `TaskRun` with `exit_code`, `commit`, `values_ref`, `system_artifacts`, `Error.category`
  - Archive query: read historical runs and their commits

**Acceptance criteria:**
- S3 conditional writes proven against real MinIO (§14.1)
- Local link-based publish proven atomic (§14.7)
- Both backends pass two-writer boot-time probe
- Content-addressed keys prevent overwrites (§5.4)
- Logs are durable and queryable

#### 2.5 Chaos Integration Tests (§5.5)

**Files to create:**

- **NEW:** `pkg/artifacts/integration_test.go` (tagged `integration`)
  - Test: Orker crash between temp write and link (kill -9)
  - Test: Second Orker boot finds existing commit, returns it
  - Test: Tampered embedded layout rejected with `IMAGE_DIGEST_MISMATCH` (§14.15)
  - Test: Orphan container detection (§14.17) — verify no `ork.run=<run>` survives boot

**Acceptance criteria:**
- All chaos scenarios pass (no silent corruption, no half-completed state)
- Proof attests to execution against real infrastructure

#### 2.6 Proof (§11.4, §14)

- **NEW:** `proof/M11.2/Phase2_Durability.sh`
  - Run chaos matrix: kill, tamper, orphan tests
  - Verify `IfNoneMatch` conditional write works (§14.1)
  - Verify `link(2)` is atomic (§14.7)
  - Verify content-addressed keys work

- **NEW:** `proof/M11.2/README.md`
  - Reproduction: `proof/M11.2/Phase2_Durability.sh`
  - Requires: `make infra-up`
  - Exit 0 on success

**Acceptance criteria:**
- Integration tests pass with `ORK_REQUIRE_INFRA=1`
- Proof runs against live S3-compatible store and Docker
- No flakes

---

## Phase 3: Images and First-Party Activities

**Owner:** DSL/CLI/UI agents (`internal/dsl`, `internal/api`, `cmd/orkctl`, `ui/`, `images/`)  
**Acceptance gate:** Activity catalog, test suite, UI integration + proof + `make lint test`  
**Proof milestone:** M11.3

### Phase 3 Deliverables

#### 3.1 Activity Catalog and Apply-Time Resolution (§4)

**Files to create/modify:**

- **NEW:** `internal/dsl/activity_catalog.go`
  - Activity catalog: engine config mapping catalog names to registry references
  - Built-in activities: `shell`, `http`, `file` → engine image references
  - User activity resolution: accept `uses: "ghcr.io/acme/migrate:v3"` or `uses: "shell"`
  - Apply-time resolution (NOT in workflow code): registry GET to resolve tag → index digest (§4.3)
  - Record both `reference` and `index_digest` in Task.image immutably

- **MODIFY:** `internal/dsl/compiler.go`
  - Call activity resolution at apply time, not run time
  - Fail `ApplyError` if reference is unresolvable
  - Record Orkflow definition digest (SHA-256 of canonical compiled definition)

- **NEW:** `internal/dsl/registry_resolver.go`
  - HTTP GET-based image resolution (§4.3): registry host + `/v2/<repo>/manifests/<ref>`
  - Follow redirect chains, handle 401/403
  - Return OCI image index digest
  - No auth unless explicitly supplied

**Acceptance criteria:**
- `orkctl orkflow apply` resolves and pins images
- Definition digest is deterministic across runs
- Re-apply unchanged document is idempotent (same version)

#### 3.2 First-Party Activities

**Files to create:**

- **MODIFY:** `images/Makefile`
  - Build targets for `activity-shell`, `activity-http`, `activity-file`
  - Push to registry (for dev: local-only)

- **MODIFY:** `images/activity-shell/Dockerfile`
  - Alpine 3.20 base, `jq`, no root
  - `USER 65532:65532`
  - `ENTRYPOINT ["/orkstack/entry.sh"]` (exec form, per §10.2 rule 7)
  - `umask 022` in entry script (rule 2a)

- **NEW:** `images/activity-shell/entry.sh`
  - Per §10.2: background workload, trap SIGTERM, write values on exit
  - Shapes 2b/2c: background-and-trap pattern
  - Line 29 double-wait caveat for SIGTERM path

- **NEW:** `images/activity-shell/README.md`
  - Shell activity contract: `command` (argv), `with:` config, stdout/stderr, exit code

- **NEW:** `images/activity-http/Dockerfile` and `entry.sh` (stub or basic HTTP implementation)
- **NEW:** `images/activity-file/Dockerfile` and `entry.sh` (stub or basic file operations)

**Acceptance criteria:**
- `make images` builds all three
- `activity-shell` passes §10.3 contract test suite (Bash fixture mandatory)
- Entry script proven by §14.18

#### 3.3 Activity Contract Test Suite (§10.3)

**Files to create:**

- **NEW:** `cmd/orkctl/activity_test.go`
  - Subcommand `orkctl activity test <image>`
  - Six assertions (§10.3):
    1. Container startup and signal handling (SIGTERM)
    2. Input filesystem mounted read-only
    3. Valid `/orkstack` context (task.json present)
    4. Valid output: values.json structure
    5. Artifact name validation (UTF-8, max 128 bytes, no `..`)
    6. Exit code and timeout handling

- **NEW:** `test/fixtures/bash_activity_contract.sh` (mandatory, runs under `bash`)
  - Build and run reference workload
  - Assert all six conditions
  - Control: `+rm`, `umask`, output readability

- **NEW:** `test/fixtures/go_activity_contract.go` (optional, reference implementation)
- **NEW:** `test/fixtures/python_activity_contract.py` (optional, reference implementation)

**Acceptance criteria:**
- Bash fixture runs end-to-end against a real image
- All six assertions pass for `activity-shell`
- Fixture can be used as a template for user images

#### 3.4 DSL Surface: `uses`, `image`, `command`, `env`, `secrets`, `resources`, `network`

**Files to modify:**

- **MODIFY:** `internal/dsl/parse.go`
  - Parse `uses:` into `Task.uses`
  - Parse `command:` as `Task.command` (string array)
  - Parse `env:` as `Task.env` (map)
  - Parse `secrets:` as `Task.secrets` (array of `{name, source}`)
  - Parse `resources:` as `Task.resources` (`{cpu, memory}`)
  - Parse `network:` as `Task.network` (NETWORK_MODE_DEFAULT or NETWORK_MODE_NONE)

- **MODIFY:** Examples in `examples/*.ork.yaml`
  - Retire templated image at `examples/container-example.ork.yaml:55`
  - Rewrite all examples to use `uses:` and first-party activities

**Acceptance criteria:**
- DSL examples parse and apply correctly
- `orkctl orkflow apply` does not fail on new fields

#### 3.5 API: ActivityService and DescribeOrker UI Seam

**Files to modify:**

- **MODIFY:** `internal/api/service.go`
  - Implement `ActivityService.ListActivities()` → list built-in + configured activities
  - Implement `ActivityService.DescribeOrker()` → read from `DescribeOrkerResult` cache

- **NEW:** `internal/api/activity_lister.go`
  - Seam replacing `PluginLister`
  - Returns activity name, registry reference, platforms

**Acceptance criteria:**
- `orkctl activity list` shows all activities
- `orkctl pool describe <pool>` shows Orker info via `DescribeOrker`

#### 3.6 UI Integration

**Files to modify:**

- **MODIFY:** `ui/src/routes/activities/+page.svelte`
  - Admin Activities page: list activities, show Orker status
  - Surface `Error.category` on failed tasks

- **MODIFY:** `ui/src/lib/components/TaskDetail.svelte`
  - Show task `image`, `command`, `env`, `secrets`
  - Display `Error.category` (not just code)

- **REGENERATE:** UI client types in `ui/src/lib/gen/`
  - After proto changes, run `buf generate` and update TS clients

**Acceptance criteria:**
- UI builds without errors
- Activities page renders correctly
- Error categories display properly

#### 3.7 Proof (§11.4, §14)

- **NEW:** `proof/M11.3/Phase3_ImagesAndActivities.sh`
  - Build all three images
  - Run Bash fixture against `activity-shell`
  - `orkctl activity test images/activity-shell:latest`
  - Verify all six assertions pass
  - Run example Orkflows using first-party activities

- **NEW:** `proof/M11.3/README.md`
  - Reproduction: `proof/M11.3/Phase3_ImagesAndActivities.sh`
  - Expected: all images build, tests pass, examples run

**Acceptance criteria:**
- Proof runs end-to-end
- Bash fixture demonstrates §14.18 entry script correctness
- All first-party images are proven

---

## Validation and Release (§14)

### Evidence Requirements

Every claim in the spec is paired with evidence in §14. Implementation must reproduce or reference each proof:

| Evidence | Phase | Validation |
|----------|-------|-----------|
| §14.1 Conditional writes | 2 | Boot-time two-writer probe against MinIO |
| §14.2 Local store limits | 1 | `CommitStore` interface design |
| §14.3 Security and workspace | 1 | `docker run` with specified flags |
| §14.4 Index digest resolution | 3 | Registry resolver against live registry |
| §14.5 Cancellation grace period | 1 | SIGTERM → grace → SIGKILL with `docker stop` |
| §14.6 Proto compilation | 1 | `buf build`, `buf lint`, `buf breaking` |
| §14.7 Atomic publication | 2 | `link(2)` vs `rename` vs `O_EXCL` test |
| §14.8 Output readability | 1 | Native vs shared mounts + `stat` detection |
| §14.9 Payload JSON round-trip | 1 | Proto design (use `bytes`, not `Struct`) |
| §14.10 `protojson` instability | 1 | `CommitRef.sha256` design (never re-marshal) |
| §14.12 OCI layout loading | 3 | `docker load` with OCI tarball |
| §14.13 Exit codes | 1 | Workload exit passthrough test |
| §14.14 Entry script SIGTERM | 1 | §14.18 built script under `docker stop` |
| §14.15 Embedded fallback digest | 3 | Hash root blob before import (tamper test) |
| §14.16 UTF-8 artifact names | 1 | Proto encoding rejects invalid bytes |
| §14.17 Orphan container cleanup | 2 | Boot-time reaping after Orker crash |
| §14.18 Entry script all ops | 1/3 | Build and run with all ops: exec, eval, cancel, success |

### Proof Artifacts

Each phase produces a proof directory with:
- **Proof script:** `proof/M11.{1,2,3}/Phase{1,2,3}_*.sh` (bash, idempotent, reproducible)
- **README:** reproduction command + prerequisites + expected output
- **Test output:** committed capture of proof run

### Acceptance Gates

**Phase 1:**
- `make lint test` passes
- `make merge-check` passes on merged tree
- Replay fixture reset (one-time, §11.4)
- Proof M11.1 reproduces

**Phase 2:**
- Integration tests pass with `-tags=integration`
- Proof M11.2 reproduces (requires `make infra-up`)
- No chaos flakes

**Phase 3:**
- UI builds and runs
- `orkctl activity test` passes for all three images
- Bash fixture proves entry script (§14.18)
- Proof M11.3 reproduces

**Tag v0.0.5:**
- All three phases complete
- All proofs reproducible
- No manual database edits needed to clean up

---

## Parallel Execution (§11 ownership model)

The three phases touch **disjoint packages** and can proceed in parallel:

- **Phase 1:** `proto/`, `internal/engine/`, `internal/orker/`, `internal/task/`
- **Phase 2:** `pkg/artifacts/` ONLY (not `internal/orker` or `internal/engine`)
- **Phase 3:** `internal/dsl/`, `internal/api/`, `cmd/orkctl/`, `ui/`, `images/`, `examples/`

**Seam interfaces Phase 1 freezes for Phase 2:**
- `artifacts.CommitStore` (interface, in-memory impl)
- `artifacts.RefPredictor` (interface for step-4 bound)
- `internal/orker.OCIDispatcher` (final signature)

**Seam interfaces Phase 1 freezes for Phase 3:**
- `internal/api.ActivityLister` (replaces `PluginLister`)
- `internal/api.ActivityService` (proto service)

---

## Model Selection (docs/models/model-selection.md)

### Phase 1

| Stage | Role | Model | Thinking | Rationale |
|-------|------|-------|----------|-----------|
| Spec interpretation | high | Claude Opus 5 | standard | Accurate API contract reading |
| Proto authoring | high | Claude Opus 5 | standard | Wire format decisions are critical |
| Workspace implementation | high | Claude Opus 5 | standard | Permissions and UNIX syscalls |
| Heartbeat/cancellation | high | Claude Opus 5 | standard | Signal-handling correctness is non-negotiable |
| Security profile | max | Claude Opus 5 | standard | Privilege escalation surface |
| Tests/proof | medium | Claude Haiku 4.5 | standard | Deterministic reproduction |

### Phase 2

| Stage | Role | Model | Thinking | Rationale |
|-------|------|-------|----------|-----------|
| Durable commit design | high | Claude Opus 5 | standard | Atomic primitives and failure modes |
| S3 backend | high | Claude Opus 5 | standard | Distributed system correctness |
| Chaos tests | high | Claude Opus 5 | standard | Reproducing failures deterministically |
| Content-addressed keys | medium | Claude Haiku 4.5 | standard | Implementation details |
| Proof/validation | medium | Claude Haiku 4.5 | standard | Test execution |

### Phase 3

| Stage | Role | Model | Thinking | Rationale |
|-------|------|-------|----------|-----------|
| Activity catalog | medium | Claude Haiku 4.5 | standard | Config routing, no new algorithms |
| Registry resolver | high | Claude Opus 5 | standard | HTTP/OCI distribution spec compliance |
| Entry script (10.2) | max | Claude Opus 5 | standard | Signal handling + POSIX shell semantics |
| UI integration | medium | Claude Haiku 4.5 | standard | Component updates, no algorithmic risk |
| Proof/test suite | medium | Claude Haiku 4.5 | standard | Test execution and fixtures |

---

## Milestones and Checkpoints

1. **M11.0:** Spec finalized (current: commit 91a441f, §14 evidence complete)
2. **M11.1:** Phase 1 complete → Replay fixture reset → `make lint test` → Proof M11.1
3. **M11.2:** Phase 2 complete → Chaos matrix passes → Proof M11.2
4. **M11.3:** Phase 3 complete → Activity test suite passes → Proof M11.3
5. **M11.4:** All three phases proven → `make merge-check` on main → Tag v0.0.5

---

## Key Constraints

- **One replay-fixture reset only** (§11.4): at end of Phase 1, never again
- **No Phase 2 opens `internal/orker` or `internal/engine`** (§11 ownership model)
- **Proto wire freeze at Phase 1 end:** no breaking changes afterward
- **Proof > assertion:** every claim must be reproducible in `proof/M11.{1,2,3}/`
- **No silent failures:** every terminal failure has machine-readable code + human message
- **Bytes never in Temporal:** only references and ≤4 KiB values.json

---

## Success Criteria

The implementation is complete when:

1. **All three phases are merged to main**
2. **Proof artifacts exist and reproduce:**
   - `proof/M11.1/Phase1_WireAndWorkspace.sh` → exit 0
   - `proof/M11.2/Phase2_Durability.sh` → exit 0 (with infrastructure)
   - `proof/M11.3/Phase3_ImagesAndActivities.sh` → exit 0
3. **All §14 evidence is reproducible**
4. **`make lint test` passes on merged main**
5. **`make merge-check` passes on merged main**
6. **No engineer needs to edit a database to clean up**
7. **Git tag v0.0.5 is created and pushed**

---

## Next Steps

1. **Phase 1 kickoff:** proto, workspace, dispatcher, heartbeat
2. **Phase 2 kickoff (parallel):** S3 backend, chaos tests
3. **Phase 3 kickoff (parallel):** activity catalog, images, test suite
4. **Weekly sync:** merge gates, proof validation, issue triage
5. **Final validation:** all proofs reproducible, all tests green
6. **Tag v0.0.5:** final merge, tag, release notes

