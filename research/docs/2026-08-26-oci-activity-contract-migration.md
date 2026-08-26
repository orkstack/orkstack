---
date: 2026-08-26 16:31:14 MDT
researcher: Claude Opus 5 (Atomic research stage)
git_commit: 55b73676589239ad6d950224ed36b37119a5018c
branch: main
repository: orkstack
topic: "How should OrkStack replace in-process Go plugin execution with a pure OCI Activity Contract, and what do the existing code and prior art dictate about each contract surface?"
tags: [research, codebase, oci, orker, plugin, artifacts, temporal, commit-protocol, image-digest]
status: complete
last_updated: 2026-08-26
last_updated_by: Claude Opus 5
breaking_changes_allowed: true
compatibility_context: "OrkStack has not shipped. The user states this is NOT a backward-compatible migration; no migration plans or compatibility shims are required. Existing Go plugin APIs, the plugin.Registry, and the internal/task Go activity seam are documented as current state, not as preservation constraints. Two hard preservation constraints survive anyway and are NOT compatibility shims: (a) internal/engine/replay_test.go + replay_payload_test.go pin a committed Temporal history fixture, and (b) any change to the ExecuteTask wire shape must either keep that fixture replayable or force a deliberate fixture reset."
---

# Research: migrating OrkStack from in-process Go plugins to a pure OCI Activity Contract

## Research Question

How should OrkStack replace its in-process Go plugin execution with a pure OCI Activity Contract, and what does the existing code plus prior art dictate about each contract surface — wire messages, `/orkstack/` workspace layout, durable commit protocol, image resolution split, `OCIDispatcher` design, activity lifecycle, failure categories, security constraints, first-party activity structure, image delivery, and the engine/Orker/container/object-store ownership split?

## Contract amendments received

None. No mid-run user message amended the objective or acceptance criteria during this stage.

## Contract conflict to escalate before the design stage

The launch objective contains two clauses that cannot both hold literally:

- `"This is NOT a backward-compatible migration—the system has not shipped yet, so no migration plans or compatibility shims are needed."`
- Acceptance criterion: `"Proto contract is forward-compatible (no breaking changes to existing code)."`

The narrowest reading consistent with both, and the one this report uses: **proto field numbers are never reused or renumbered** (the existing `reserved 8; reserved "queue";` precedent at `proto/orkstack/v1/orkflow.proto:18-52` is the in-repo pattern), while **Go-level APIs, the DSL surface, and the `internal/task` activity structs may be replaced outright**. That still leaves one genuinely breaking artifact — the committed replay fixture — analysed in §5 below. This should be confirmed by the user before the design stage commits to it.

---

## Executive conclusions

1. **The current `ExecuteTaskInput`/`ExecuteTaskResult` are plain Go structs, not proto** (`internal/task/contract.go:49-91`), serialized by Temporal's default data converter as `encoding/json` (`internal/engine/plan.go:23-26`, confirmed by `internal/engine/replay_payload_test.go:318-330`). AGENTS.md §11.1 ("For a process-boundary change, edit `proto/` first and run `make proto`") and §12 ("Proto is the single source of truth for cross-process types") therefore make promoting these to `proto/orkstack/v1/` a contract requirement, not a preference. **Caveat found in code:** `internal/engine/plan.go:23-26` documents that the data converter encodes a *top-level* protobuf message with `protojson` and everything else with `encoding/json`; this is why `RunPlan` travels beside the messages rather than inside one. A proto-defined `ExecuteTaskInput` passed as a top-level activity argument would flip to `protojson`, which changes the recorded payload bytes.

2. **The 256 KiB payload invariant is violated today and nothing enforces it.** No serialized-size guard exists anywhere on the execution path (`internal/engine/temporal.go:105-133`, `internal/engine/workflow.go:361-381`, `:384-423`). Three concrete sources: (a) the `http` plugin puts a parsed JSON body up to **8 MiB** plus every response header into the output payload (`pkg/plugins/http/http.go:38-40`, `:211-239`); (b) the `shell` plugin puts 8 KiB stdout + 8 KiB stderr heads into the payload (`pkg/plugins/shell/shell.go:50-58`, `:219-242`); (c) the engine copies the **full transitive** upstream closure — every ancestor's complete output payload and every `ArtifactRef` — into each downstream `ExecuteTaskInput` (`internal/engine/workflow.go:230-253`, `:322-331`, `:416-420`). Fan-in is quadratic across a chain: up to `n(n-1)/2` repeated upstream-result copies across one run's history.

3. **Activity cancellation cannot currently reach a running container, and this is the single largest correctness gap against AGENTS.md §3.3.** Temporal delivers cancellation to an activity *only through a heartbeat response* — an activity that never calls `RecordHeartbeat` never sees `ctx.Done()` ([Temporal heartbeat docs](https://docs.temporal.io/develop/go/activities/timeouts#activity-heartbeats), [SDK source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/activity/activity.go#L57-L79)). OrkStack heartbeats exactly four discrete phase strings — `start`, `rendered`, `executed`, `uploaded` — and none during the container run (`internal/orker/activity.go:115-141`, `:151-178`, `:322-327`). No `HeartbeatTimeout` is set anywhere (`internal/engine/workflow.go:361-366`). `WaitForCancellation` is unset, so it defaults false ([SDK `ActivityOptions`](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/activity.go#L100-L149)) and the workflow abandons the future without waiting for Orker cleanup. The container plugin also never sends SIGTERM at all — its only kill path is `docker kill --signal KILL` on timeout (`pkg/plugins/container/docker.go:184-188`, `:258-292`). **The `OCIDispatcher` must heartbeat continuously during the container run** or §3.3's `cancellation → SIGTERM → 30s → SIGKILL` cannot be implemented.

4. **The artifact `Store` interface cannot express a durable commit marker today.** `Store` is `Put/Get/Stat/List/Presign/Delete` with no conditional write (`pkg/artifacts/artifacts.go:40-60`). Worse, the two backends disagree on addressing: S3 keys are **name-addressed** by provenance — `orkflows/{orkflow}/{run-id}/{task-id}/{name}` (`pkg/artifacts/s3_manifest.go:31-49`, `pkg/artifacts/s3.go:439-442`) — while local keys are **content-addressed** — `objects/n-<base64(name)>/<sha256-of-content>` (`pkg/artifacts/artifacts.go:85-94`, `pkg/artifacts/local.go:121-136`). A retry cannot ask the local backend "does `commit.json` exist for this task?" because the key requires the content digest it does not yet have. This is a hard blocker on §4.1 for the local backend.

5. **The conditional-write primitive already exists in this repo, just not on `Store`.** `pkg/artifacts/namespace_store.go:57-79` defines `PutConditions{IfAbsent bool; IfMatch string}` and `:292-316` maps it to `PutObjectInput.IfNoneMatch = "*"` / `IfMatch`, returning `ErrPreconditionFailed` (`:102-107`). The same `IfNoneMatch: "*"` create-if-absent is used for immutable orkflow versions (`pkg/artifacts/orkflow_store.go:626-667`) and ETag CAS for `@head.json` (`pkg/artifacts/orkflow_head.go:305-345`) and `@current.json` (`pkg/artifacts/orkflow_current.go:187-228`). A `commit.json` claim has a proven, tested in-repo pattern to copy — and it matches external best practice exactly ([S3 conditional writes, Aug 2024](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/); MinIO has supported conditional `PutObject` since `RELEASE.2023-02-09T05-16-53Z`).

6. **No image, registry, or digest concept exists anywhere in the DSL, proto, or compiler.** `orkstack.v1.Task` is `id/plugin/with/needs/condition/timeout_seconds/retry/pool` (`proto/orkstack/v1/orkflow.proto:18-52`); the compiler validates `plugin` against a hard-coded allowlist `[]string{"container", "file", "http", "shell"}` (`internal/dsl/compile.go:59-62`, `internal/dsl/validate.go:67-74`). The only image string in the system lives inside the opaque `with:` map of the `container` plugin and is passed to Docker **verbatim, never resolved to a digest, never recorded** (`pkg/plugins/container/container.go:443-452`, `pkg/plugins/container/docker.go:105-112`). The definition digest that already exists (`internal/dsl/hash.go:28-30`) is `sha256(raw YAML bytes)` — a *source* hash, not a canonical-IR hash, and it is only computed when `version:` is absent (`internal/dsl/compile.go:186-193`).

7. **A naming collision is already in the codebase.** "Idempotency key" today means the *run submission* key claimed through `IdempotencyClaimWorkflow` (`internal/api/service.go:639-833`, `internal/engine/idempotency.go:35-71`). AGENTS.md §3's activity-level `idempotencyKey` is a different thing. The design must pick distinct spellings or the two will be conflated in code review, the CLI, and the UI.

8. **Prior art converges on exactly the shape AGENTS.md describes, with one important divergence.** Argo (`init` stages inputs → main runs → `wait` uploads then patches `report-outputs-completed=true`), Nextflow (`.command.sh`/`.command.out`/`.command.err`/`.exitcode`), and Cromwell (`script`/`rc`/`stdout`/`stderr`, `rc` published by temp-file rename) all validate "declared directory + files = ABI, wrapper owns publication, a last marker means done." **The divergence:** none of them makes the completion marker a *conditional create* in shared object storage, because none of them is trying to make the marker readable by a different, later host. OrkStack's cross-Orker requirement (§4.1 "return its committed result and do not execute the container again") makes S3 `If-None-Match: *` the right primitive and makes OrkStack's protocol stricter than its prior art, not looser.

---

## Detailed findings

### 1. Wire messages: what the code dictates

#### Current state

`internal/task/contract.go:49-91` — the complete current seam:

| Go field | JSON key | Notes |
|---|---|---|
| `RunID` | `run_id` | |
| `OrkflowName` | `orkflow_name` | |
| `OrkflowVersion` | `orkflow_version` | pinned version string, not a digest |
| `Namespace` | `namespace` | added M10.1 specifically to avoid a second fixture reset (`contract.go:55-63`) |
| `Pool` | `pool` | coordinate, not instruction |
| `TaskID` | `task_id` | |
| `Plugin` | `plugin` | dies under OCI |
| `With` | `with` | **raw, un-rendered** — rendering is deliberately Orker-side (`contract.go:7-13`) |
| `RunInput` | `run_input` | full run payload |
| `Upstream` | `upstream` | map of **transitive** ancestors → `{output, artifacts}` |
| `TimeoutSeconds` | `timeout_seconds` | also becomes `StartToCloseTimeout` |

`ExecuteTaskResult` is `{task_id, output, artifacts}` (`:86-91`). `ExecuteTaskResult.TaskID` crosses the wire but is **never read** — collection is keyed by the future's associated workflow-side `id` (`internal/engine/workflow.go:384-420`).

What is **absent** and required by the objective: image reference, resolved digest, activity `idempotencyKey`, resources (cpu/memory), `cancellationGracePeriod`, secret references, `exit_code`, commit reference, failure category.

#### Constraints on any replacement

- **Field numbers must never be reused.** `proto/orkstack/v1/orkflow.proto:18-52` already demonstrates the pattern (`reserved 8; reserved "queue";` after the v0.0.3 `queue:` removal, with a compiler error at `internal/dsl/removed.go:14-39`).
- **`orkstack.v1.ArtifactRef` already exists** with `name=1, uri=2, content_type=3, size_bytes=4, sha256=5, metadata=6` (`proto/orkstack/v1/common.proto:20-30`) and a dependency-free Go mirror at `pkg/plugin/plugin.go:17-26`, aliased as `artifacts.ArtifactRef` (`pkg/artifacts/artifacts.go:16-20`). Input/output artifact references should reuse this message rather than inventing a second shape.
- **`orkstack.v1.Error` already carries `code=1, message=2, details=3, retryable=4`** (`proto/orkstack/v1/common.proto:32-43`). A failure *category* has no home yet; `TaskRun.error` is an `Error` (`proto/orkstack/v1/run.proto:10-38`).
- **The Temporal encoder flips.** `internal/engine/plan.go:23-26` states the converter encodes a top-level protobuf message with `protojson` and everything else with `encoding/json`. A proto `ExecuteTaskInput` passed as the activity argument is a top-level message → `protojson`. Recorded fixture bytes change. See §5.

#### Open design questions (code does not decide)

- **OQ-1:** Does `ExecuteTaskInput` carry the *rendered* `with:` or keep the raw block? `contract.go:7-13` gives the determinism reason for raw (`artifact "<task>/<name>"` resolves to a local path only the Orker has). Under OCI, `artifact` references become workspace input paths the Orker still owns, so the reason survives — but the argument list (command/env) must be materialized into `/orkstack/input/` files, which is Orker-side work either way. Undecided.
- **OQ-2:** How is upstream fan-in bounded? Replacing full upstream payloads with references satisfies §1 but changes template semantics: `{{ .tasks.build.output.image }}` in `examples/container-example.ork.yaml` reads an upstream output field inline. Either the Orker fetches upstream `values.json` from object storage before rendering (extra I/O, plus a store dependency on the render path), or small values continue to travel and a size guard rejects oversized ones. Undecided.
- **OQ-3:** Whether `ExecuteTaskResult.output` survives at all, or whether the compact result is `{exit_code, commit_ref, artifacts[], failure_category}` with all structured values living only in `values.json`.

---

### 2. `/orkstack/` workspace layout: what exists and what is unspecified

**No committed design exists.** The strings `/orkstack`, `values.json`, and `commit.json` do not occur in `docs/`, `research/`, `.atomic/*-design.md`, `proof/`, or `internal/task/contract.go`. This is greenfield.

#### What the current Orker builds instead

Per-attempt scratch directory, created by `os.MkdirTemp(root, "ork-<runid>-<taskid>-<attempt>-*")` where root is `Options.WorkRoot` → `ORK_ORKER_WORKDIR` → `os.TempDir()` (`internal/orker/activity.go:252-268`, `:30-38`). It is removed by `defer os.RemoveAll` on **every** exit path — success, failure, panic, cancellation (`:130-132`).

Inside it today:
- `.artifacts/<safe(producer)>/<safe(name)>` — materialized upstream artifacts (`internal/orker/resolver.go:23`, `:97-122`).
- `.ork-container/stdout`, `.ork-container/stderr` — byte-exact stream captures (`pkg/plugins/container/container.go:85-87`, `:503-523`).
- `.ork-container/ephemeral/<name>` — scratch dirs, `chmod 0777`, removed after the run (`pkg/plugins/container/mounts.go:203-243`).
- `.ork-shell/script` — the shell plugin's rendered script (`pkg/plugins/shell/shell.go:164-181`).

`safeComponent` (`internal/orker/resolver.go:127-151`) is the existing rule for turning an opaque logical artifact name into one safe path component: `[A-Za-z0-9._-]` preserved, everything else `_`, truncated to 64, and a `-<8 hex of sha256>` suffix appended whenever the name was rewritten. Any `/orkstack/input/artifacts/` naming must reckon with the fact that **logical artifact names are opaque and may contain separators, dot segments, or invalid UTF-8**.

#### Hard constraint discovered: the daemon must see the workspace

`examples/container-example.ork.yaml:5-14` and `pkg/plugins/container/container.go:377-387` document that when the Docker daemon runs in a VM (colima, Lima, Rancher, Docker Desktop) it shares `$HOME` but **not** macOS temp directories, so bind sources under `/var/folders` or `/tmp` fail with `bind source path does not exist`. `make up` works around this by pointing `ORK_ORKER_WORKDIR` at `.data/work`; the container integration test probes for a bind-mountable root by actually running `alpine:3.20` against candidate directories (`internal/orker/container_integration_test.go:283-352`). A `/orkstack` mount inherits this constraint exactly.

#### Prior art for the layout (external)

| System | Input staging | Result values | Streams | Completion marker |
|---|---|---|---|---|
| Argo | `init` container downloads to declared `inputs.artifacts[].path` | `outputs.parameters[].valueFrom.path`; `outputs.result` capped at 256 KiB | `wait` sidecar saves logs | annotation `report-outputs-completed=true` after upload ([source](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/workflow/executor/executor.go#L970-L997)) |
| Argo emissary | — | — | `/var/run/argo/ctr/<name>/{stdout,combined}` | `/var/run/argo/ctr/<name>/exitcode` ([source](https://github.com/argoproj/argo-workflows/blob/8f0d28008aaf288fe73bc380c174a8623d73a786/workflow/executor/emissary/emissary.go#L27-L49)) |
| Tekton | `workspaces` default `/workspace/<name>` | `/tekton/results/<name>`, aggregate **4096 bytes** classic; sidecar-logs mode 4096 B/result, stable v0.61.0 | — | `/tekton/run/<n>/out` / `out.err` ([entrypoint README](https://github.com/tektoncd/pipeline/blob/bbc5b442326349b85db01c9d2a206619b4aedf57/cmd/entrypoint/README.md#L1-L54)) |
| Nextflow | staged by symlink (default), or `copy`/`link`/`rellink` | — | `.command.out`, `.command.err` | `.exitcode` ([TaskRun.groovy](https://github.com/nextflow-io/nextflow/blob/640ddc3542b7025fd34bb6baf92dd5b77e53c39d/modules/nextflow/src/main/groovy/nextflow/processor/TaskRun.groovy#L575-L584)) |
| Cromwell | — | — | `stdout`, `stderr` | `rc`, published by `mv $rcTmpPath $rcPath` ([source](https://github.com/broadinstitute/cromwell/blob/785071c718eac991a804bc283ebebb07c1673110/backend/src/main/scala/cromwell/backend/standard/StandardAsyncExecutionActor.scala#L457-L547)) |
| Airflow KPO | — | `/airflow/xcom/return.json`, must be valid JSON, pushed only on success | — | — |
| Kubernetes | — | `/dev/termination-log`, **4096 B/container, 12 KiB/pod** | — | — |

The convergent lesson: **the exit-status file / marker is the completion boundary, and it is written last.** Cromwell's temp-file-then-rename for `rc` is the local-filesystem analogue of S3 `If-None-Match: *`.

#### Open design questions

- **OQ-4:** Permission bits and ownership. AGENTS.md forbids `--privileged` but says nothing about `--user`. If the Orker runs containers as a non-root UID, `/orkstack/output/` must be group- or world-writable, or the UID must be known at mount time. The container plugin's current answer for ephemeral mounts is a blunt `chmod 0777` (`pkg/plugins/container/mounts.go:203-243`). Undecided.
- **OQ-5:** Whether `/orkstack/input/artifacts/` uses the existing `safeComponent` rewriting (lossy, digest-suffixed) or a manifest file mapping logical name → workspace path. The S3 backend already solves the analogous problem with a companion manifest at `manifests/{object-key}` containing magic `ORKNAME\x02` + key digest + exact name bytes (`pkg/artifacts/s3_manifest.go:144-187`) — that is the in-repo precedent for "the safe path is not the name."
- **OQ-6:** `values.json` size limit. Nothing in the repo enforces one. Prior art ranges 4 KiB (Tekton/K8s) to 256 KiB (Argo/Step Functions). AGENTS.md §1 says 256 KiB for the *Temporal payload*, which is a different budget if `values.json` is uploaded as an artifact and only *referenced* in the payload.
- **OQ-7:** stdout/stderr as workspace files vs. streamed only. §3.2 requires independent capture and streaming. Today the container plugin does both (`pkg/plugins/container/container.go:476-533`) but merges them into one live log stream with an in-band `[stderr] ` marker (`pkg/plugin/linewriter.go:20-49`), and `artifacts.LogChunk` has **no stdout/stderr field** (`pkg/artifacts/logs.go:66-77`), nor does `orkstack.v1.TaskLogChunk` (`proto/orkstack/v1/service.proto:303-326`). Whether §3.2's "independent logs" requires splitting the durable log stream is undecided.

---

### 3. Durable commit protocol: the blocker and the proven pattern

#### The blocker

| Backend | Key for a `Put(name="commit.json", ...)` | Deterministic before content is known? |
|---|---|---|
| S3 | `orkflows/{orkflow}/{run-id}/{task-id}/commit.json` (`pkg/artifacts/s3_manifest.go:31-49`) | **Yes** |
| Local | `objects/n-<b64("commit.json")>/<sha256-of-content>` (`pkg/artifacts/artifacts.go:85-94`, `pkg/artifacts/local.go:121-136`) | **No** |

`Store` has no `Exists`; `Stat(ctx, uri)` requires a full URI, which on local requires the content digest (`pkg/artifacts/artifacts.go:40-60`, `pkg/artifacts/local.go:241-270`). **Retry-start commit detection is not implementable on the local backend through the current `Store` interface.** AGENTS.md §2.2 says the production artifact backend is S3-compatible storage and "local filesystem storage is for local use," so a design may legitimately scope the commit protocol to S3 — but then `orkstack dev` (§10) either loses the protocol or needs a local commit path.

Also note S3 `Put` is an **unconditional** `PutObject` with no `IfNoneMatch` (`pkg/artifacts/s3.go:195-217`), so a second write to the same provenance key silently replaces the object. `commit.json` written through today's `Store` would be overwritable — the exact opposite of an immutable completion record.

#### The proven in-repo pattern to copy

`pkg/artifacts/namespace_store.go:57-79`:

```go
// PutConditions is the compare-and-swap a registry write may demand.
type PutConditions struct {
    IfAbsent bool
    IfMatch  string
}
```

mapped at `:292-316` to `PutObjectInput.IfNoneMatch = aws.String("*")` / `IfMatch`, with `ErrPreconditionFailed` at `:102-107`. The immutable-version create-if-absent is at `pkg/artifacts/orkflow_store.go:626-667`; the ETag-CAS pointer swap is at `pkg/artifacts/orkflow_head.go:305-345` (retried up to 3 attempts, `orkflow_store.go:546-592`) and `pkg/artifacts/orkflow_current.go:187-228` (2 attempts).

`pkg/artifacts/orkflow_store.go:497-500` is a directly relevant precedent: the code **explicitly does not claim a two-object transaction** and documents that process death between the version write and the head swap can leave a stored version that never became head. A commit protocol will face the same honesty requirement about artifacts uploaded before a `commit.json` that never landed.

#### External validation of the primitive

- S3 `PutObject` with `If-None-Match: *` returns **412 `PreconditionFailed`** if a current object exists; a concurrent delete/write race can return **409 `ConditionalRequestConflict`**, requiring retry ([AWS conditional-write behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-error-response)).
- MinIO supports conditional `PutObject` since `RELEASE.2023-02-09T05-16-53Z` ([commit `11c7ecb`](https://github.com/minio/minio/commit/11c7ecb5cf947e6428968571cafc0494bb572845)); **conditional multipart completion only since `RELEASE.2025-09-07T16-13-09Z`** ([commit `07c3a42`](https://github.com/minio/minio/commit/07c3a429bfed433e49018cb0f78a52145d4bedeb)). `deploy/docker-compose.yml:30` pins `minio/minio:latest`, so the local stack currently floats. **A one-shot conditional `PutObject` is portable; conditional `CompleteMultipartUpload` is not.** `commit.json` is small, so this favours a single-shot PUT.
- S3 has strong read-after-write and LIST consistency in all regions since 2020-12-01 ([announcement](https://aws.amazon.com/about-aws/whats-new/2020/12/amazon-s3-now-delivers-strong-read-after-write-consistency-automatically-for-all-applications/)). A successful conditional PUT is immediately visible to a later `HEAD`; a preliminary `HEAD` is a valid observation but **not** mutual exclusion.

#### Crash points from AGENTS.md §11, mapped to what the code does today

| Crash point | Current behavior (evidence) | Gap |
|---|---|---|
| Orker dies mid-container | `exec.CommandContext` kills the client; `docker rm --force --volumes` runs on a **detached** context (`pkg/plugins/container/docker.go:307-331`). Temporal retries after `StartToCloseTimeout` (default 300 s, `internal/engine/workflow.go:622-627`) — but only if `maximum_attempts > 1`, and the default is **1 attempt** (`:629-637`). | Container re-executes. Acceptable under at-least-once, but the default of 1 attempt means most tasks never retry at all. |
| Orker dies after container exits, before upload | Workdir `RemoveAll` never runs; outputs are lost with the host. | Full re-execution. |
| Orker dies mid-artifact-upload | S3 `Put` is per-object unconditional; partial object sets are possible (`pkg/artifacts/s3.go:195-227`). | Orphaned artifacts, no marker distinguishes them. |
| Orker dies after `commit.json`, before Temporal completion | **Not implemented — there is no `commit.json`.** | The entire §4.1 guarantee is absent. |
| Activity heartbeat lost | No `HeartbeatTimeout` set → Temporal only detects a dead worker at `StartToCloseTimeout` (`internal/engine/workflow.go:361-366`). | Up to 300 s of dead time by default. |

#### Open design questions

- **OQ-8:** The commit key. `orkflows/{orkflow}/{run-id}/{task-id}/commit.json` is the natural extension of the existing artifact layout and is already stable across retries (run + task do not change per attempt). But it collides with the artifact namespace: a workload that names an output artifact `commit.json` would write to the same key. Undecided.
- **OQ-9:** Whether the commit protocol lives on `artifacts.Store` (new methods, breaking both backends) or a new `artifacts.CommitStore` alongside `LogStore`/`RunArchive`/`OrkflowStore` — the repo's established pattern is a separate purpose-built store per concern (`pkg/artifacts/registry.go:54-154`, `logs.go:258-290`).
- **OQ-10:** What happens when the conditional PUT returns 412 on the *first* attempt of an activity (i.e. a commit exists that this Orker did not write). §4.1 says return the committed result; but the code must then also decide whether to trust artifact refs listed in a commit written by an unknown Orker version.
- **OQ-11:** Local-backend commit path. `LocalStore` already commits by `Rename` from an `O_EXCL` temp file (`pkg/artifacts/local.go:95-129`), and `os.Link`/`OpenFile(O_CREATE|O_EXCL)` on the destination would give create-if-absent — but `objectKey` would have to stop being content-addressed for this one object.

---

### 4. Image resolution split

#### What the code does today

Nothing. `pkg/plugins/container/container.go:443-452` validates only that `image` is non-empty and does not start with `-`. It is passed verbatim to `docker image inspect --format "{{.Id}}"` (`pkg/plugins/container/docker.go:105-112`), `docker pull <image>` (`:127-140`), and `docker run ... <image>` (`:206-222`), and returned unchanged as `output.image` (`container.go:331-338`). Pull policy is `missing` (default) / `always` / `never` (`container.go:455-466`). **A tag that moves between two runs silently changes what executes**, which AGENTS.md §5 forbids.

The digest infrastructure that *does* exist is for definitions, not images: `internal/dsl/hash.go:28-30` (`sha256` of raw YAML), immutable version objects under `metadata/orkflows/{ns}/{name}/{version}.json` created with `IfNoneMatch: "*"` (`pkg/artifacts/orkflow_store.go:626-667`), and `@head.json` / `@current.json` pointers.

#### Where apply-time resolution would sit

`internal/api/service.go:342-393` — `ApplyOrkflow` calls `dsl.Compile(yaml)`, then stamps `created_at`, then `OrkflowStore.Put`. Compilation itself is pure (`internal/dsl/compile.go:121-193`) and has no network. **A registry round-trip cannot go inside `Compile`** without breaking `TestCompile_AcceptsEveryShippedExample` and every hermetic compiler test; it belongs between `Compile` and `Put`, in the apply handler, where `created_at` is already stamped.

`internal/dsl/compile.go:59-69` — `KnownPlugins = []string{"container","file","http","shell"}` with an `Options.KnownPlugins` override — is the exact hook where a name → image-URI table would live. Note the compiler allowlist is **separate** from runtime dispatch: the Orker independently looks up `registry.Get(in.Plugin)` and errors `UNKNOWN_PLUGIN` (`internal/orker/activity.go:96-113`).

#### External facts that constrain the design

- Tag → digest without pulling layers: `HEAD /v2/<name>/manifests/<tag>` returns `Docker-Content-Digest`, required by the spec ([distribution-spec lines 200-214](https://github.com/opencontainers/distribution-spec/blob/fee21197eb94360ddfa6dda0b7edabcd12456809/spec.md#L200-L214)). Tooling: `docker buildx imagetools inspect`, `crane digest`, `skopeo manifest-digest`.
- **`docker manifest inspect --verbose` does not reliably print the parent index digest** for a multi-platform tag, and **`docker image inspect .RepoDigests` is the selected platform manifest's digest, not the index digest** ([Moby API v1.52 lines 1870-1884](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/api/docs/v1.52.yaml#L1870-L1884)). Deriving the digest from the local daemon after a pull is therefore the wrong mechanism for a multi-arch record.
- `docker pull repo@sha256:...` verifies content against the digest ([docs](https://docs.docker.com/reference/cli/docker/image/pull/#pull-an-image-by-digest-immutable-identifier)).
- A digest pin proves content identity, **not publisher identity** — cosign is the separate concern ([Sigstore verify](https://docs.sigstore.dev/cosign/verifying/verify/)). Docker Content Trust is being retired; `notary.docker.io` shuts down 2026-12-08 ([DCT docs](https://docs.docker.com/engine/security/trust/)).
- The `Accept` header set must be identical at resolve time and pull time, or content negotiation can select a different object ([spec lines 141-177](https://github.com/opencontainers/distribution-spec/blob/fee21197eb94360ddfa6dda0b7edabcd12456809/spec.md#L141-L177)).

#### Open design questions

- **OQ-12:** Index digest vs. platform digest. AGENTS.md §5's example is `ghcr.io/acme/migrate@sha256:...` without saying which. For a multi-arch builtin, recording the **index digest** preserves normal platform selection; recording the platform digest pins one arch. Undecided; both should probably be recorded.
- **OQ-13:** Where credentials for the apply-time resolve come from. The engine (`cmd/ork`) currently has no registry credentials at all, and AGENTS.md §2.3 says the Orker is the security boundary. An engine that must reach a private registry at apply time is a new credential surface.
- **OQ-14:** Whether an already-pinned `image@sha256:` in a definition is re-verified at apply or passed through.
- **OQ-15:** Whether the resolved digest lands in `orkstack.v1.Task` (a new field number ≥10) or in a parallel per-run resolution record. `Task` is embedded in the immutable version document, so putting the digest there makes the version document the immutable pin — which matches §7.2 ("runs pin the version and its definition digest") — but means re-applying identical YAML with a moved tag produces a different definition, and the current version derivation is `sha256(raw YAML)` which would **not** change. That is a real inconsistency to resolve.

---

### 5. `OCIDispatcher` and what survives from the current Orker

#### Dies

- `pkg/plugin/plugin.go:94-104` — the `Plugin` interface (`Name`/`Describe`/`Execute`).
- `pkg/plugin/plugin.go:106-143` — `Registry`, and `internal/orker/registry.go:13-20` (`BuiltinRegistry`).
- `pkg/plugin/plugin.go:63-80` — `ProducedArtifact` / `Result`.
- `pkg/plugins/{http,shell,file,container}` — all four Go implementations.
- `task.ActivityListPlugins` (`internal/task/contract.go:37-39`) — **but see the consumer list below.**

#### Survives largely intact

| File | Why it survives |
|---|---|
| `internal/orker/resolver.go:23-151` | `upstreamResolver` materializes upstream artifacts to local paths with a cache and `safeComponent` naming. Under OCI this becomes the `/orkstack/input/artifacts/` stager; the download logic (`:97-122`) is unchanged. |
| `internal/orker/render.go:36-168` | Template rendering with exactly two helpers, `artifact(ref)` and `env(name)` (`:152-167`), recursive over nested maps/lists (`:95-127`), `missingkey=error`, `<no value>` rejected (`:129-149`). Still needed to render whatever config becomes `/orkstack/input/config.json`. |
| `internal/orker/logsink.go:54-521` | Chunked log sink: 64 KiB flush, 500 ms interval, 16 MiB/attempt cap, 1 MiB/line, 3 upload tries, dense 1-based `Seq` advanced only on successful `PutLogChunk`, truncation notice, backpressure that waits without holding the mutex (`:354-389`). Keyed by `(RunID, TaskID, Attempt)`. Directly reusable for container stdout/stderr. |
| `pkg/plugin/linewriter.go:9-260` | Line framing: LF/CR/CRLF, 64 KiB `MaxStreamedLine` split, partial-line buffering, `[stderr] ` prefix and `[stdout] ` escaping, `DecodeStreamLine` inverse. The UI already decodes and tints stderr (proof M11.4). Should move out of `pkg/plugin` if that package dies. |
| `internal/orker/activity.go:252-268` | Per-attempt workdir creation and the `defer RemoveAll` discipline. |
| `internal/orker/activity.go:280-294`, `:297-319` | Log sink opened **before** anything else and closed **before** the function returns, so every chunk is durable before Temporal records the terminal event (`:115-123`). This ordering is load-bearing for the reader contract and must be preserved. |

#### `ListPlugins` has real downstream consumers

Removing it is not free. Current consumers:
- `internal/engine/temporal.go:155-188` — `TemporalStarter.ListPlugins`, 10 s `ScheduleToCloseTimeout`, `MaximumAttempts: 2`.
- `internal/engine/pool_probe.go:139-161` — `PluginsOnPool`, 5 s, names only.
- `internal/engine/pools.go:576-615` — populates `Pool.Plugins` for live pools.
- `internal/api/service.go:1423-1578` — `Service.ListPlugins`; default-pool failure → Connect `Unavailable`.
- `proto/orkstack/v1/service.proto:427-448` — `PluginInfo{name=1,version=2,description=3,input_schema=4,output_schema=5}`.
- `ui/src/routes/admin/plugins/+page.svelte` and `+page.ts`; `ui/src/routes/admin/pools/+page.svelte:121-134`.
- `cmd/orkctl/plugin.go`.
- Pool *liveness* is inferred from a successful `ListPlugins` probe — `proof/M11.6` fixed a "deaf poller" bug in exactly this path.

**OQ-16:** What replaces `ListPlugins` as the pool-liveness probe once there are no plugins to list. This is not a cosmetic removal; it is load-bearing for the Admin UI's pool health.

#### Heartbeating is the dispatcher's hardest requirement

Per §Executive-3. Concretely: the Go worker throttles heartbeats to `heartbeatTimeout * 0.8` when set, otherwise a default **30 s** interval capped at 60 s ([SDK source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/internal_task_handlers.go#L2597-L2618)). To honour a 30-second SIGTERM→SIGKILL grace with any timeliness, the dispatcher needs an explicit `HeartbeatTimeout` set by the engine **and** a heartbeat loop running for the container's whole lifetime. Setting `WaitForCancellation: true` is also required if the workflow must not mark the task terminal before the Orker finishes killing and cleaning up.

---

### 6. Activity lifecycle: the current sequence and where it must change

Current `Activities.ExecuteTask` (`internal/orker/activity.go:99-185`), in order:

1. nil-registry / unknown-plugin guard → `UNKNOWN_PLUGIN` non-retryable.
2. **Open log sink** (before anything else, `defer Close`) — `:115-123`.
3. `heartbeat("start")`.
4. `makeWorkDir` → failure is `WORKDIR_FAILED` non-retryable; `defer RemoveAll`.
5. Build `upstreamResolver`; render `with` → failure is `RENDER_FAILED` (non-retryable unless explicitly `plugin.Retryable`, `:337-345`).
6. `heartbeat("rendered")`.
7. `impl.Execute(ctx, ec)`.
8. **On plugin error:** upload produced artifacts **first**, swallowing upload errors to the log, then return the plugin's error (`:155-171`). The comment is explicit: replacing the plugin's error with a storage error "would tell the author their build failed because of the bucket." This behavior is proven by M11.3 and must be preserved under OCI — a failing container's stdout/stderr and named outputs still publish.
9. `heartbeat("executed")`; `upload(...)`; `heartbeat("uploaded")`; return `{TaskID, Output, Artifacts}`.

Missing versus the objective's lifecycle: no exit-code capture into the result, no output validation step, no commit, no image resolution, no cancellation grace.

Note `upload` (`:196-235`) treats an empty artifact name and an unopenable file as **non-retryable** `ARTIFACT_FAILED`, but a store `Put` failure as an **ordinary retryable** error (`:227-231`) — the existing retryable/permanent split at the artifact layer.

---

### 7. Failure categories: current mapping vs. AGENTS.md §4

#### What the Orker emits today (`internal/orker/activity.go:19-28`)

`UNKNOWN_PLUGIN`, `RENDER_FAILED`, `WORKDIR_FAILED`, `ARTIFACT_FAILED`, `PLUGIN_PERMANENT` — all `temporal.NewNonRetryableApplicationError(msg, type, cause)`. Plugin errors are **retryable by default** unless wrapped in `plugin.Permanent` (`:347-355`).

The container plugin's own classification (`pkg/plugins/container/container.go:363-399`):

| Condition | Retryability | Message |
|---|---|---|
| exit 0 | success | — |
| exit 125 (daemon refusal) | **retryable**, unless output contains `bind source path does not exist` | `container: docker could not start %s (exit code %d)...` |
| exit 126/127 | permanent | `container: %s cannot run the requested command (exit code %d)...` |
| timeout kill | permanent | `container: %s exceeded its timeout after %s and was killed (exit code %d)...` |
| any other nonzero | permanent | `container: %s exited with code %d...` |
| pull failure/timeout | retryable ordinary error | `container: pull %s failed (docker exited %d): %s` |

Docker reserved codes are named constants: 125 daemon refusal, 126 non-executable, 127 not found, 137 conventional SIGKILL (`pkg/plugins/container/docker.go:23-38`).

#### What the engine does with all of it

**Nothing categorical.** `internal/engine/workflow.go:384-402` flattens every activity error into `Error{Code: "TASK_FAILED", Message: err.Error(), Details: ...}` and terminates the run with `RUN_FAILED` (`:426-455`). The one exception: `temporal.IsCanceledError(cause)` or a canceled workflow context flips the terminal state to `STATUS_CANCELED` / `RUN_CANCELED`, projected on a `workflow.NewDisconnectedContext` (`:435-454`). Output-conversion failures get their own code `TASK_OUTPUT_INVALID` but still emit event type `TASK_FAILED` (`:404-414`).

**There is no failure-category concept in the engine, proto, or UI today.** `orkstack.v1.Error.retryable` (field 4, `proto/orkstack/v1/common.proto:32-43`) exists but is not populated from the Orker's application-error types.

#### The Temporal mechanics the mapping must use

- `ApplicationError.Type()` returns the caller's `errType` string ([SDK](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/error.go#L723-L733)); `RetryPolicy.NonRetryableErrorTypes` matches on that **exact string**, not `errors.Is` ([retry-policy docs](https://docs.temporal.io/encyclopedia/retry-policies#non-retryable-errors)). The constructor-level non-retryable flag always wins.
- `Cancelled` is not an `ApplicationError` at all: the worker must return `ctx.Err()` (`context.Canceled`) or `temporal.NewCanceledError(details...)`, and only **after** the server has delivered cancellation — returning `NewCanceledError` spontaneously is converted to an unexpected-cancel failure ([SDK](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/internal_task_pollers.go#L1638-L1676)). Returning `nil` after cancellation reports **success**.
- `TimedOut` is not returned by the worker at all — Temporal produces `*TimeoutError` with `TIMEOUT_TYPE_START_TO_CLOSE` or `TIMEOUT_TYPE_HEARTBEAT`. A container that exceeds its *own* `timeout` and is killed by the Orker is an Orker-authored failure, not a Temporal timeout. **These two "timeouts" are different and the mapping must say which is which.**

#### Open design question

- **OQ-17:** AGENTS.md §4 says `InfrastructureError` retries and `ContractViolation` does not. Docker exit 125 is currently retryable (daemon refusal is usually transient) *except* for the bind-source case. A malformed `values.json` is unambiguously `ContractViolation`. But a container that exits 0 without writing `values.json` — §11's explicit scenario — is a contract violation by the *image*, so non-retryable, even though retrying is harmless. Undecided which side of the line each concrete case falls on; the table needs to be enumerated exhaustively before implementation.

---

### 8. Security: what is already enforced and what §6 adds

#### Already true by construction (not by validation)

The container plugin **cannot** emit `--privileged`, `--network`, `--pid`, `--user`, `--cpus`, `--memory`, `--volume`, or `--security-opt`, because it has no config keys for them and the argv builder emits only `run --rm --name <n> [--label]* [--workdir] [--env]* [--mount]* <image> <cmd...>` (`pkg/plugins/container/docker.go:206-222`). There is no raw host-path mount source: sources are only artifact-resolved paths (always read-only) or plugin-created ephemeral dirs (`pkg/plugins/container/mounts.go:48-55`, `:90-101`, `:193-200`).

Mount target validation (`pkg/plugins/container/mounts.go:115-132`): non-empty, starts with `/`, not `/`, no `:` or `,`, clean after allowing one trailing slash. Duplicate targets rejected (`:106-109`). Comma in a resolved host path rejected because it would split the `--mount` value (`:163-168`). Env names may not be empty or contain `=`/whitespace (`pkg/plugins/container/config.go:36-47`).

**Notable gap:** there is no explicit prohibition on a container *target* path of `/var/run/docker.sock` — such a clean absolute path passes `validateContainerPath`. It is harmless today only because no host source can be a socket.

**No network mode is set at all** — Docker's unqualified `docker run` default applies. §6 requires supporting `default` and `none` explicitly, which means adding a config key and a `--network` flag where none exists.

#### What §6 requires that has no code

`no-new-privileges`, default seccomp (explicitly), `--cap-drop=ALL`, `--pids-limit`, `--memory`, `--cpus`, `--read-only`, `--user`, rootless support, `--network=none`, resource caps enforced by Orker policy, and `cancellationGracePeriod` as an activity field.

Exact flags and semantics are captured in `research/web/2026-08-26-oci-digest-pinning-and-isolation.md` §5 with Docker doc links per flag. Two facts worth surfacing:

- `--userns=host` is the **only** valid `docker run` userns value and it *reduces* isolation; user-namespace remapping is a daemon-level setting ([userns-remap docs](https://docs.docker.com/engine/security/userns-remap/)). A design that says "enable userns per container" is not implementable.
- The default seccomp profile denies ~44 of 300+ syscalls and `--security-opt seccomp=unconfined` disables it; `seccomp=builtin` selects the default explicitly ([seccomp docs](https://docs.docker.com/engine/security/seccomp/)).

#### Signals

`docker stop` sends the configured stop signal (SIGTERM unless `STOPSIGNAL`/`--stop-signal`), waits `--timeout` (Linux default 10 s), then SIGKILL ([docs](https://docs.docker.com/reference/cli/docker/container/stop/)). Current `--time` is a deprecated alias ([CLI source](https://github.com/docker/cli/blob/3f096b4ffd8e30329221c583db6dfc9279673b96/cli/command/container/stop.go#L48-L52)). §3.3's 30-second grace maps onto `docker stop --timeout 30`, or an explicit `docker kill --signal TERM` + wait + `docker kill --signal KILL` (the plugin's existing kill helper is `pkg/plugins/container/docker.go:184-188`).

**PID 1 caveat for first-party images:** a shell-form `ENTRYPOINT` runs the process as a child of a shell that does not forward signals, so SIGTERM never reaches the workload ([JSONArgsRecommended check](https://docs.docker.com/reference/build-checks/json-args-recommended/)). Every first-party entry script must end in `exec "$@"` or use exec-form `ENTRYPOINT`. `docker run --init` (tini) forwards signals and reaps zombies but does not fix an application that ignores SIGTERM.

---

### 9. First-party activity structure

#### What the four builtins currently are, and what each must become

| Plugin | `with:` surface (evidence) | Output payload | Artifacts |
|---|---|---|---|
| `http` | `op` (get/post/put/patch/delete/head), legacy `method`, `url`, `headers`, `body`, `capture_body_as`, `timeout_seconds` (`pkg/plugins/http/http.go:55-83`, `:241-311`) | `status`, all `headers` as name→list, `duration_ms`, and `json` when body ≤ **8 MiB** and parses (`:141-165`, `:211-239`). Non-2xx is **success**, not an error (`:118-182`). | body file with response Content-Type when `capture_body_as` set |
| `shell` | `op` (exec/eval/script), `command`, `script`, `interpreter`, `env`, `capture_stdout_as`, `capture_stderr_as`, `capture_paths` globs (`pkg/plugins/shell/shell.go:79-108`, `:431-489`) | `exit_code`, 8 KiB stdout head, 8 KiB stderr head; `eval` adds trimmed `value`; `script` adds `script_path` (`:50-58`, `:219-242`). `eval` treats nonzero exit as **data**; exec/script treat it as permanent failure (`:244-264`). | full streams; each `capture_paths` match named by workdir-relative slash path; absolute globs and `..` escapes rejected (`:253-306`) |
| `file` | `op` (write/read/copy/delete), `path`, `content`, `from`, `to`, `capture_as` (`pkg/plugins/file/file.go:41-87`) | `path` (relative slash), `size_bytes` (`:89-158`). Paths must stay inside `WorkDir` after cleaning (`:198-214`). `delete` is idempotent, produces no artifact (`:160-181`). | exactly one, per operation |
| `container` | `op`, `image`, `command`, `workdir`, `env` (with one-pass `$VAR`/`${VAR}` expansion, `config.go:11-34`), `mounts[]`, `pull`, `capture_stdout_as`, `capture_stderr_as`, `timeout_seconds` | `exit_code`, `image`, `image_pulled`, `container`, `timed_out`, `duration_ms` — explicitly **no** stdout/stderr (`container.go:328-340`) | named stream captures only |

AGENTS.md §9 names the intended file-activity operations as `copy`, `checksum`, `gzip`, `gunzip`, `tar`, `untar`, `concat` — a **different set** from the implemented `write/read/copy/delete`. That is a deliberate contract change to note, not a discrepancy to reconcile silently.

`docs/PLUGIN_DESIGN.md:1-5` establishes the `op` convention: *"Every OrkStack plugin exposes one generic operation selector, spelled `op`."* Every shipped example uses it and `TestCompile_AcceptsEveryShippedExample` enforces it mechanically. Whether `op` survives into `/orkstack/input/config.json` is undecided but the convention is load-bearing for all five examples in `examples/`.

`ARCHITECTURE.md:75-77` already states the direction verbatim: *"The interface is designed so a plugin can later be packaged as a container image with the same input/output contract — and, being a plugin, with the same `op` convention."*

#### Open design questions

- **OQ-18:** `http` currently returns non-2xx as success. Under exit-code semantics (§3.2: "any non-zero exit means the activity failed"), a Bash-only http image using `curl -f` would fail on 404. Preserving today's semantics requires the entry script to exit 0 and record `status` in `values.json`. Undecided.
- **OQ-19:** `shell`'s `eval` op returns `value` from trimmed stdout while treating nonzero exit as data. Under exit-code semantics this is inexpressible without the entry script swallowing the exit code — which is precisely the "magic exit code" pattern §3.2 forbids. Undecided.
- **OQ-20:** `file` operates on the task `WorkDir`, which under OCI is the container's `/orkstack` workspace, not the Orker host. Every `path:` in `examples/file-read.ork.yaml` and `examples/hello.ork.yaml` is workdir-relative. The examples will need rewriting, which will churn the compile tests and the shipped-example test.

---

### 10. Image delivery

Nothing exists. There is no image build target in the `Makefile` (verified: only `proto`, `tsgen`, `tools`, `build`, `tidy`, `test`, `cover`, `up/down/status/logs`, `reset`, `infra-*`, `run-*`, `lint`, `fmt`, `merge-check`). `deploy/docker-compose.yml` contains only `temporal`, `minio`, and `minio-setup`, all pinned to `:latest`. There is no `orkstack dev` command (AGENTS.md §10 aspiration) and no `orkstack activity test` harness.

External facts that bound the design:
- Git-context builds: fragment `#ref:subdir` (a commit used as `ref` must be full-length) or the newer structured `?branch=&subdir=&checksum=`; shallow clone with submodules ([build contexts](https://docs.docker.com/build/building/context/)).
- Obtaining the result digest: `--iidfile`/`--quiet` gives a **local image ID (config digest)**, not a registry manifest digest; `--metadata-file` with a registry export gives `containerimage.digest` ([buildx build](https://docs.docker.com/reference/cli/docker/buildx/build/)). A locally built, never-pushed image has **no registry manifest digest**, so §5's "resolve to a digest and record the immutable reference" is only literally satisfiable for pushed images.
- Docker Hub anonymous pulls: 100/6h per IPv4 or IPv6 `/64` ([usage docs](https://docs.docker.com/docker-hub/usage/)); pull-through mirror via CNCF Distribution `proxy.remoteurl` ([recipe](https://distribution.github.io/distribution/recipes/mirror/)).

**OQ-21:** How a locally built (unpushed) builtin satisfies the immutable-reference invariant. Recording the local config ID is not a registry digest and does not transfer to a second Orker.

---

### 11. Ownership table — what the code says today

| Fact | Engine (`cmd/ork`, `internal/engine`, `internal/api`) | Orker (`cmd/orker`, `internal/orker`) | Container | Object store |
|---|---|---|---|---|
| Orkflow YAML + compiled IR | yes (`internal/dsl`, `internal/api/service.go:342-393`) | no | no | immutable version doc: `metadata/orkflows/{ns}/{name}/{version}.json` |
| Which version runs | `@current.json`; apply commit point is `@head.json` (`docs/DEPLOY.md:308-325`) | no | no | pointers, ETag-CAS |
| Pool → task queue | yes, at submit (`internal/engine/pools.go:384-437`, `internal/task/pool.go:74-146`) | binds one queue at boot (`internal/task/pool_binding.go:45-91`) | no | no |
| `RunPlan` routes | recorded as workflow arg 4 (`internal/engine/temporal.go:132-133`) | receives resolved `Pool` as a coordinate only | no | no |
| Template rendering | **no** — deliberately (`internal/task/contract.go:7-13`) | yes (`internal/orker/render.go`) | no | no |
| Upstream artifact bytes | no | materializes to workdir `.artifacts/` (`resolver.go:97-122`) | sees only what is mounted | holds them |
| Docker daemon | no | yes, `os/exec` shell-out (`pkg/plugins/container/docker.go:54-75`) | is the workload | no |
| S3 credentials | yes (engine opens namespace/orkflow/archive/log stores) | yes (`artifacts.NewFromEnv`) | **never** — not in `plugin.ExecContext` (`pkg/plugin/plugin.go:46-61`) | — |
| Temporal credentials | yes | yes | **never** | — |
| Live logs | reads (`internal/api/logs.go:172-276`) | writes chunks (`internal/orker/logsink.go`) | writes stdout/stderr | `metadata/logs/{run}/{task}/{attempt}/{seq}` |
| Terminal run + events | archives (`internal/engine/archive.go`) | no | no | `metadata/runs/{id}.jsonl` + body-less index |
| Failure classification | flattens to `TASK_FAILED`/`RUN_FAILED` (`workflow.go:384-455`) | emits app-error types (`activity.go:19-28`) | exit code only | no |
| Image digest | **nobody** | **nobody** | — | **nowhere** |
| Commit record | **nobody** | **nobody** | — | **nowhere** |

---

## Replay-fixture blast radius (must be sized before any wire change)

Two guards, both on the committed fixture `internal/engine/testdata/history/run-succeeded-v1.json` (`internal/engine/replay_test.go:20-46`):

1. **`TestRunWorkflow_ReplaysCommittedV1History`** (`:72-94`) — command-stream replay. Breaks on: renaming `ExecuteTask` or `PublishEvent`, reordering/inserting/deleting activity or emit commands, changing ready-task scheduling order. `TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream` (`:97-172`) proves the guard actually catches an inserted emit.
2. **`TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends`** (`internal/engine/replay_payload_test.go:39-127`) — re-executes the workflow, feeds recorded results back, and compares **current `ExecuteTaskInput` JSON against recorded input per task**, event payloads as a multiset, event sequences `1..N`, and the final `Run` projection.

Consequences:
- Adding **any** encoded field to `ExecuteTaskInput` fails guard 2 (`:26-41`, `:129-145`).
- Renaming a Go field while preserving the JSON name and value passes both.
- Changing the workflow's 4th argument type fails fixture decoding — this already forced a reset once at M10.2 (`internal/engine/testdata/history/README.md:55-68`).
- Switching `ExecuteTaskInput` to a top-level proto message changes the encoder from `encoding/json` to `protojson` (`internal/engine/plan.go:23-26`) → guard 2 fails.

`RunWorkflow` is registered `VersioningBehaviorPinned` precisely because its command stream is replay-sensitive (`internal/engine/register.go:26-41`). **The migration requires a deliberate fixture reset**, recorded against a versioned worker, per the fixture README's procedure. `internal/task/contract.go:55-63` shows the project already batches wire additions into one slice specifically to avoid a second reset — the same discipline applies here: land the complete new wire shape in one change.

---

## Code references

- `internal/task/contract.go:1-91` — the whole engine↔Orker seam and its design rationale.
- `internal/orker/activity.go:99-185` — current lifecycle; `:19-28` error types; `:155-171` failed-task-still-publishes rule; `:322-327` heartbeat helper.
- `internal/orker/resolver.go:23-151` — artifact materialization; `:127-151` `safeComponent`.
- `internal/orker/render.go:36-168` — template engine and the two helpers.
- `internal/orker/logsink.go:54-521` — chunk sink, bounds, backpressure, truncation.
- `pkg/plugin/plugin.go:17-159` — `ArtifactRef`, `ExecContext`, `Plugin`, `Registry`, `RetryableError`.
- `pkg/plugin/linewriter.go:9-260` — line framing and stdout/stderr in-band markers.
- `pkg/plugins/container/docker.go:23-38` reserved exit codes, `:105-140` inspect/pull, `:184-188` kill, `:201-222` argv, `:245-292` run + timeout watcher, `:307-341` cleanup and grace.
- `pkg/plugins/container/mounts.go:33-46` mount spec, `:57-132` validation, `:174-243` sources.
- `pkg/plugins/container/container.go:178-193` op dispatch, `:222-341` run lifecycle, `:363-399` failure classification, `:443-466` image/pull validation, `:476-533` stream fan-out.
- `pkg/artifacts/artifacts.go:40-60` `Store`, `:74-149` local key codec.
- `pkg/artifacts/local.go:85-141` content-addressed Put.
- `pkg/artifacts/s3.go:168-235` name-addressed Put, `:439-469` URI build/parse.
- `pkg/artifacts/s3_manifest.go:31-187` key layout, sanitization, name manifests.
- `pkg/artifacts/namespace_store.go:57-107`, `:292-316` — `PutConditions` and its S3 mapping.
- `pkg/artifacts/orkflow_store.go:472-527`, `:626-667` — immutable version create-if-absent and the two-object honesty note.
- `pkg/artifacts/orkflow_head.go:223-345`, `orkflow_current.go:165-228` — ETag CAS.
- `pkg/artifacts/logs.go:13-164` — log key layout and `LogChunk`.
- `internal/engine/workflow.go:230-253` transitive closure, `:322-381` schedule + activity options, `:384-455` collect/fail/cancel, `:579-603` route lookup, `:622-653` timeout/retry defaults.
- `internal/engine/replay_test.go:20-211`, `replay_payload_test.go:26-330` — the two replay guards.
- `internal/dsl/compile.go:59-69` plugin allowlist, `:121-193` compile, `:196-260` normalization; `hash.go:8-30`; `validate.go:12-180`; `removed.go:14-39`.
- `proto/orkstack/v1/orkflow.proto:10-69`, `common.proto:9-43`, `run.proto:10-58`, `service.proto:258-326`, `:427-448`.
- `cmd/orker/main.go:23-183` — env surface, worker options, registration, shutdown.
- `internal/api/logs.go:43-370` — replay/follow streaming, attempt resolution, terminator.

## Architecture documentation

- `AGENTS.md` §1–§15 — the authoritative product contract this migration serves.
- `ARCHITECTURE.md:75-77` — the already-stated direction toward container-packaged plugins.
- `docs/PLUGINS.md:3-39`, `:165-190` — current plugin contract and the "large data leaves as artifacts" rule.
- `docs/PLUGIN_DESIGN.md:1-65`, `:154-204` — the `op` convention and its enforcement.
- `docs/DEPLOY.md:281-335`, `:427-443` — the seven S3 key families, pointer CAS, log retention.
- `docs/STORAGE.md` — an accepted 2026-08-14 SQLite decision record whose proposed implementation was superseded by the committed S3 architecture (M7.5–M7.6). Useful only as rejected-option rationale.
- `examples/container-example.ork.yaml` — the fullest statement of current container semantics, including the bind-mount visibility warning and the "an artifact helper renders a host path the container cannot see" explanation.

## Historical context (from research/ and proof/)

- `research/2026-08-14-...milestone-m3....md:36-51`, `:90-98` — the original engine/Orker seam design; no OCI workspace concept.
- `research/docs/2026-08-14-v002-storage-architecture-decision.md` and siblings — SQLite/S3/Temporal-visibility research; nothing on this migration.
- `.atomic/v004-design.md` — M11.1–M11.7 live-log design; discusses the Docker plugin, capture files, payload exclusion, backpressure. No generic OCI contract.
- **No prior committed research or design exists for `/orkstack`, `values.json`, `commit.json`, image-digest pinning, or replacing the Go plugin API.** This was confirmed by full-text search across `docs/`, `research/`, `.atomic/`, `proof/`, and the Go sources.
- Proof coverage that already exists and must not regress: `proof/M7.7` (container run, pull policy, env expansion, read-only artifact mounts, ephemeral cleanup, stream captures, timeout kill → exit 137, no leftover `ork.task` containers); `proof/M11.1` (live stdout/stderr while running, dense sequences, no bytes in Temporal history); `proof/M11.4` (browser live rendering with stderr tinted); `proof/M11.7` (two-replica follow through a replica that did not start the run); `proof/M7.4` (provenance-addressed artifact URIs, `ork-content-sha256` metadata); `proof/M7.6` (immutable JSONL run archive + body-less index).
- **No proof demonstrates a user-issued `CancelRun` killing a running container.** M7.7 proves *timeout*-driven termination only. Given finding §Executive-3 this is very likely because it does not currently work.

## Related research (this session)

- `research/web/2026-08-26-temporal-payloads-heartbeats-errors.md`
- `research/web/2026-08-26-oci-digest-pinning-and-isolation.md`
- `research/web/2026-08-26-filesystem-task-abi-prior-art.md`

## Open questions

Consolidated, numbered for the design stage. Each is something the code does **not** decide.

| # | Question |
|---|---|
| OQ-1 | Rendered vs. raw `with:` in the new wire input |
| OQ-2 | How upstream fan-in is bounded without breaking `{{ .tasks.X.output.Y }}` templates |
| OQ-3 | Whether a structured `output` payload survives in the result at all |
| OQ-4 | `/orkstack` permission bits and container UID |
| OQ-5 | Input artifact naming: `safeComponent` rewriting vs. a name manifest |
| OQ-6 | `values.json` size limit and where it is enforced |
| OQ-7 | Whether stdout/stderr become separate durable log streams |
| OQ-8 | The `commit.json` object key, and its collision with user artifact names |
| OQ-9 | New methods on `Store` vs. a separate `CommitStore` |
| OQ-10 | Behavior on a 412 from an unknown prior committer |
| OQ-11 | Local-backend commit path (content-addressing blocks name lookup) |
| OQ-12 | Index digest vs. platform digest for multi-arch |
| OQ-13 | Registry credentials at the engine for apply-time resolution |
| OQ-14 | Re-verification of an already-pinned `image@sha256:` |
| OQ-15 | Where the digest lives, and its inconsistency with `sha256(raw YAML)` versioning |
| OQ-16 | What replaces `ListPlugins` as the pool-liveness probe |
| OQ-17 | Exhaustive failure→category→retryability table |
| OQ-18 | `http` non-2xx-is-success under exit-code semantics |
| OQ-19 | `shell eval`'s nonzero-exit-is-data under exit-code semantics |
| OQ-20 | Rewriting workdir-relative example Orkflows |
| OQ-21 | Immutable reference for a locally built, unpushed image |
| OQ-22 | Spelling to disambiguate run-submission idempotency key from activity `idempotencyKey` |
| OQ-23 | Whether the replay fixture is reset once or the migration is split across two resets |

## Validation recommendations

Ordered by what would falsify a design fastest.

1. **Probe cancellation before designing around it.** Write a throwaway integration test: submit a run with a 120-second container, call `CancelRun`, and assert whether the container receives any signal. Finding §Executive-3 predicts it does not. Cost: minutes. Value: confirms the single biggest gap.
2. **Probe conditional PUT against the pinned MinIO.** `deploy/docker-compose.yml:30` uses `minio/minio:latest`. Run a `PutObject` with `IfNoneMatch: "*"` twice against the local stack and assert the second returns `ErrPreconditionFailed`; then pin the MinIO tag. `pkg/artifacts/namespace_store_integration_test.go` and `orkflow_head_race_integration_test.go` already exercise this path and are the models to copy.
3. **Measure a real fan-in payload.** Build a 6-task diamond where each task returns a 50 KB `json` from the `http` plugin, run it, and read the `ScheduledActivityTask` input sizes from Temporal history. This converts "quadratic fan-in" from an inference off `workflow.go:230-253` into a number.
4. **Decide the fixture reset up front.** Before any wire change, re-record `run-succeeded-v1.json` per `internal/engine/testdata/history/README.md`, or accept both replay guards failing for the duration of the migration. Do not discover this mid-implementation.
5. **Preserve the M11.1/M11.4/M11.7 log proofs as regression gates.** The log sink, line framing, and the sink-closes-before-return ordering (`internal/orker/activity.go:115-123`) are the most-proven part of the Orker and the easiest to break while rewriting dispatch.
6. **Keep `proof/M7.7`'s assertions as the OCI dispatcher's acceptance test.** It already asserts read-only artifact mounts, ephemeral cleanup, timeout kill with exit 137, and no leftover labelled containers — all of which the dispatcher must still satisfy.
7. **Add the missing `HeartbeatTimeout`.** Any design that claims §3.3 compliance needs `HeartbeatTimeout` set in `ActivityOptions` (`internal/engine/workflow.go:361-366`) plus a heartbeat loop; both are testable with `TestWorkflowEnvironment` and `env.CancelWorkflow()`.
8. **Write the contract test suite fixture first, in Bash.** AGENTS.md §10: *"If Bash needs an SDK, the architecture has failed."* A Bash image that reads `/orkstack/input/` and writes `/orkstack/output/values.json` is the cheapest possible falsification of an over-complicated workspace design, and it can be written before any Go changes.

## Unverified / explicitly not established

- I did not execute any test, build, or Docker command during this session. Every codebase claim is from reading source at commit `55b7367`; every external claim is linked.
- The claim that cancellation does not currently reach a container is an **inference** from (a) Temporal's documented heartbeat-gated cancellation delivery and (b) the four discrete heartbeat call sites in `internal/orker/activity.go`. It is consistent with the absence of any proof demonstrating cancel-kills-container, but it was not reproduced. Recommendation 1 above is how to confirm it.
- The quadratic fan-in figure is derived from `transitiveNeeds` + `upstreamFor` + the unfiltered `s.results` assignment, not measured. Recommendation 3 is how to confirm it.
- `deploy/docker-compose.yml` pins `minio/minio:latest`, so the running MinIO version — and therefore its conditional-write support — was not determined.
- Whether `protojson` vs `encoding/json` actually changes the recorded bytes for a given payload was reasoned from `internal/engine/plan.go:23-26` and `replay_payload_test.go:318-330`, not executed.
