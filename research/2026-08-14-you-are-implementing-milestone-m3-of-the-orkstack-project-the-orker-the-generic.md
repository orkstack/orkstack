# M3 Research Report — Orker + builtin plugins (`/Users/daniel/Code/orkstack-wt-m3` @ `71f3a13`, detached HEAD)

All claims below are backed by files read in this session. Anything I could not verify is marked **UNVERIFIED**.

---

## 0. Conclusions first

1. **The seam is already exactly what M3 needs; nothing outside scope must change.** `internal/task/contract.go` and `pkg/plugin/plugin.go` are complete and sufficient. The engine schedules `task.ActivityExecuteTask` on `queueFor(t)` (default `orkstack-tasks`) with **`StartToCloseTimeout` only, no `HeartbeatTimeout`** (`internal/engine/workflow.go:296-314`). So `activity.RecordHeartbeat` is *safe but never required* — no heartbeat timeout can fire; do not rely on heartbeats for liveness.
2. **Retries default to a single attempt** (`retryPolicyFor`, `workflow.go:490-511`: nil/`<=1` ⇒ `MaximumAttempts: 1`). `hello.ork.yaml` sets no retry, so every M3 task is one-shot. Non-retryable error construction is therefore *correctness hygiene*, not a test-visible behavior for hello-ork — but criterion 5 demands "unknown plugin fails with a clear, non-retryable error", so use `temporal.NewNonRetryableApplicationError`.
3. **The whole module is currently lint-clean**: `gofmt -l` (excluding `gen/`) returned nothing and `go vet ./...` was silent at `71f3a13`. The M2 README's lint blocker ("a baseline `pkg/*` file needs formatting") **no longer reproduces** — that file was almost certainly one of the `pkg/plugins/*/doc.go` stubs now in M3's scope. No out-of-scope file must be touched.
4. **Biggest hidden risk: queue cross-talk in `-tags=integration`.** `internal/engine/integration_test.go:83-112` starts an **activity-only worker on `task.DefaultQueue` that registers a stub `ExecuteTask`**. `go test -tags=integration ./...` runs packages in parallel against the same real Temporal, so an M3 Orker test's `ExecuteTask` can be stolen by the engine stub (and vice versa). Mitigation options in §F.
5. **Branch question is real**: HEAD is detached at the `main` merge commit; local branches are `main`, `m1-artifacts`, `m2-engine`. The convention is one branch per milestone ⇒ create `m3-orker` from `71f3a13` before committing.

---

## A. Engine seam

### A1. How ExecuteTask is scheduled
`internal/engine/workflow.go:296-314`:
```go
timeout := time.Duration(timeoutSecondsFor(t)) * time.Second   // default 300s (:479-484)
activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
    TaskQueue:           queueFor(t),        // t.Queue or task.DefaultQueue (:472-477)
    StartToCloseTimeout: timeout,
    RetryPolicy:         retryPolicyFor(t),  // MaximumAttempts:1 unless DSL retry
})
workflow.ExecuteActivity(activityCtx, task.ActivityExecuteTask, input)
```
- No `ScheduleToClose`, no `ScheduleToStart`, **no `HeartbeatTimeout`**.
- Activity is invoked **by string name** `"ExecuteTask"` ⇒ the Orker must use `RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: task.ActivityExecuteTask})`.
- Established worker options for an Orker-shaped worker (from the engine's own stub, `integration_test.go:83-86`): `worker.Options{DisableRegistrationAliasing: true, DisableWorkflowWorker: true}`. The engine's workflow worker uses `LocalActivityWorkerOnly: true` (`:76-79`), confirming the Orker is the sole consumer of `ExecuteTask` on the shared queue.
- Per-task `queue` override exists in proto; an Orker binary should accept `ORK_ORKER_QUEUE` (**UNVERIFIED** — no such env var exists yet; this is a design choice, defaulting to `task.DefaultQueue`).

### A2. Result consumption and error classification
`workflow.go:318-357` (`collect`):
- **`result.TaskID` is never checked.** Only `result.Output` and `result.Artifacts` are used. Criterion 5 still requires setting it.
- `result.Output` goes through `payloadFromPlugin` → `structpb.NewStruct` (`internal/engine/convert.go:21-31`). **This is a hard constraint: every value in a plugin `Output` must be structpb-encodable** — `string`, `bool`, `float64`/`int` (converted), `nil`, `[]any`, `map[string]any`. Non-conforming output fails the task with `TASK_OUTPUT_INVALID`. Notably `[]byte` and custom structs are illegal; `int` is accepted by `structpb.NewStruct` and comes back as a JSON number.
- Failure path: any activity error → `TaskRun.Error{Code: "TASK_FAILED", Message: err.Error()}` and the raw error is returned, failing the run. **Nothing inspects `temporal.ApplicationError` types, `NonRetryableErrorTypes`, or `plugin.RetryableError`.** Retry semantics are enforced solely by the Temporal retry policy, so non-retryability must be expressed via `temporal.NewNonRetryableApplicationError(msg, type, cause)` (which Temporal's retrier honors) — the engine just surfaces `err.Error()`, so **message clarity is what tests can assert**.

### A3. Upstream construction and payload round-trip
- `upstreamFor` (`workflow.go:270-278`) supplies the **full transitive `needs` closure** (`transitiveNeeds`, `:179-200`), keyed by task id, each `task.UpstreamResult{Output, Artifacts}` copied verbatim from what the Orker returned (`:353`).
- `ArtifactRef.Name` values arriving downstream are **exactly what the Orker put in `ExecuteTaskResult.Artifacts[i].Name`** — round-tripped through `artifactsFromPlugin`/`artifactsToPlugin` (`convert.go:34-69`) with no renaming. This is the field M3 controls end-to-end.
- `With: t.GetWith().AsMap()` (`:309`) — from `structpb`, so **all numbers arrive as `float64`**, and the input payload likewise (`payloadToPlugin`, `convert.go:14-20`). Additionally the whole `ExecuteTaskInput` struct crosses Temporal's default converter as JSON, reinforcing `float64`. Config parsing must accept `float64`, `int`, and `string` for `timeout_seconds`.
- `RunInput` is `ApplyInputDefaults(wf, input)` merged (`:141`), so declared DSL defaults (e.g. hello-ork's `url`) are present.

### A4. Log streaming channel
**There is none for the Orker in M3.** The event bus is written only by the `PublishEvent` activity on `engine.EventQueue`, invoked from workflow code (`workflow.go:439-470`, `internal/engine/activities.go`). The Orker has no bus handle and the activity input carries no log channel. Available mechanisms today:
- `activity.GetLogger(ctx)` (Temporal SDK structured logging → worker stdout),
- `activity.RecordHeartbeat(ctx, details)` (details visible in Temporal UI/describe; safe because no heartbeat timeout is set).
⇒ `plugin.ExecContext.Log` should be wired to the activity logger (plus an in-memory buffer for tests). Document this as an M3 limitation in the final report.

---

## B. Template rendering

### B5. Surface the DSL validates (`internal/dsl/template.go`, `docs/DSL.md:47-59`)
Compile-time checker `checkField` (`template.go:191-209`) accepts **only two roots**: `.inputs.<declared name>` and `.tasks.<id in needs closure>`; anything else is `unknown template root`. Helpers `artifact "<task>/<name>"` (must contain a `/`, producer must be upstream — `:245-258`) and `env "<NAME>"` (arity-checked only — `:259-262`). `index` forms are also validated (`checkIndex`). Stubs return `""` at compile time (`:16-21`), so the DSL never depends on real values. **Templates are checked on every string leaf of `with`, recursing into maps (sorted keys) and slices** (`checkValue`, `:37-60`) — the runtime renderer should mirror exactly that traversal.

Note the DSL parses with `Option("missingkey=zero")` (`:25`); that is a *parse-time* stub and does not bind the Orker's execute-time option.

### B6. Making unknown references fail loudly
Verified experimentally (Go 1.25.6, `/tmp/mk/m.go` run this session) with `Option("missingkey=error")`:
- `{{ .inputs.nope }}` over `map[string]any{}` ⇒ `template: x:1:10: executing "x" at <.inputs.nope>: map has no entry for key "nope"` ✅
- `{{ .tasks.ghost.output.f }}` over an empty `tasks` map ⇒ same class of error at the *first* missing key ✅
- `{{ .inputs.a.b }}` where `a` is a string ⇒ `can't evaluate field b in type interface {}` ✅ (fails even though missingkey doesn't apply)

Where `missingkey=error` does **not** fire, and needs a wrapper:
- a **nil** map value in the chain (e.g. `tasks.fetch.output` present but nil) — indexing a nil map yields the zero value, not an error. Guard by always constructing non-nil `map[string]any` for `inputs`, `tasks`, `tasks[id]`, and `tasks[id]["output"]`.
- a key present with a `nil` value renders `<no value>`; treat a rendered result containing `<no value>` as an error (the engine's condition code takes the falsy-set approach instead, `condition.go:12-27` — different requirement, don't copy it).

Recommended renderer: `template.New(taskID+"/"+path).Option("missingkey=error").Funcs(funcs).Parse(s)` then `Execute`, wrapping any error as `task %q: with.%s: render: %w`, and returning it as a **non-retryable** application error (a bad template will never succeed on retry).

Context shape must match the DSL and the engine's condition evaluator (`condition.go:56-66`):
```go
map[string]any{
  "inputs": map[string]any(in.RunInput),
  "tasks":  map[string]any{ id: map[string]any{"output": map[string]any(r.Output), "artifacts": r.Artifacts} },
}
```

### B7. What gets rendered
Mirror `checkValue`: render **string leaves only**; recurse into `map[string]any` and `[]any`; **do not render map keys**; leave `float64`, `bool`, `nil` untouched (literal-contract "preserve input verbatim"). A string with no `{{` should be passed through byte-identically (the DSL uses the same `strings.Contains(text, "{{")` shortcut, `template.go:63-65`) — this also avoids text/template's own escaping/trim surprises on plain shell scripts.

### B8. `env` allowlist
**No allowlist source exists in the repo today** (`env` is a stub returning `""`, `template.go:19`). Design decision required. Recommended, consistent with the `ORK_` config convention (§3 AGENTS.md): read `ORK_ORKER_ENV_ALLOWLIST` (comma-separated names, empty ⇒ empty allowlist). A non-allowlisted name should **error** — the literal contract says "for an enumerated error prefer the widest plausible trigger", and a silent empty string would corrupt a shell command. Document the choice in `docs/PLUGINS.md`/report since it is unspecified.

---

## C. Artifacts

### C9. Naming and resolution
`Store.Put(ctx, name, r, opts)` returns `ArtifactRef` with **`Name` set verbatim to the supplied logical name** (`pkg/artifacts/local.go:130-137`; S3 mirrors it at `s3.go:140`). Logical names are fully opaque — base64url-chunked into physical keys (`artifacts.go:89-116`), so any bytes are legal, including `/`.

Two viable schemes; **recommend the plain-name scheme**:
- **Plain**: `Put(ctx, prod.Name, ...)` ⇒ `ArtifactRef.Name == "body"`. Downstream `artifact "fetch/body"` splits on the first `/` into (`taskID`,`name`), looks up `in.Upstream["fetch"]`, then finds `Artifacts[i].Name == "body"`. Exact string compare, trivially testable, and it is the only scheme where `ExecuteTaskResult.Artifacts` names read naturally in the `Run` read model.
- Namespaced (`"<runID>/<taskID>/<name>"`) would force suffix matching and pollute `TaskRun.Artifacts[].name` in the UI.
Provenance belongs in `PutOptions.Metadata{"run_id","task_id","artifact"}` instead; storage is content-addressed anyway so plain names never collide destructively.

Resolution errors to enumerate: ref without `/`; unknown producer task (not in `Upstream`); producer present but no artifact of that name — each a clear, non-retryable failure.

### C10. Materialization
`Store.Get(ctx, uri) (io.ReadCloser, error)` streams exact bytes (`local.go:144-161`; S3 `:149-167`). Resolver design:
- Target file inside a `WorkDir/.artifacts/<taskID>/` subdir; **derive the filename by sanitizing the logical name, not by trusting it** (names may contain `/`, `..`, NUL — proven opaque by M1's tests). Safe pattern: `filepath.Join(dir, sanitize(name))` where sanitize replaces path separators, with a short content-hash suffix on collision.
- **Cache by ref string** in a mutex-guarded map so repeated `{{ artifact "fetch/body" }}` in one render (and across `with` leaves) returns the identical path and downloads once — required for "materializes to a real local file whose contents match the stored bytes".
- Surface `errors.Is(err, artifacts.ErrNotFound)` and `ErrInvalidURI` (`artifacts.go:63-72`) as **non-retryable** task failures; transient network errors from S3 stay retryable.

### C11. Env for MinIO
`ConfigFromEnv` (`pkg/artifacts/config.go:50-73`) reads `ORK_ARTIFACT_BACKEND` (default `local`), `ORK_ARTIFACT_DIR`, `ORK_S3_ENDPOINT`, `ORK_S3_REGION`, `ORK_S3_BUCKET`, `ORK_S3_ACCESS_KEY`, `ORK_S3_SECRET_KEY`, `ORK_S3_SESSION_TOKEN`, `ORK_S3_PATH_STYLE` (default `true`). **The running MinIO matches every default except the backend** (`DefaultS3Endpoint=http://localhost:9000`, `us-east-1`, `orkstack-artifacts`, `orkstack`/`orkstack-secret`, path-style true — `config.go:17-27`). So the integration test and `cmd/orker` only need:
```
ORK_ARTIFACT_BACKEND=s3
```
Infra confirmed live this session: `nc -z localhost 7233` succeeded; `curl http://localhost:9000/minio/health/live` returned 200.

---

## D. Orker runtime and `cmd/orker`

### D12. Per-attempt WorkDir
- Root: `os.MkdirTemp(rootFromEnv(), pattern)` where root defaults to `os.TempDir()` and is overridable via `ORK_ORKER_WORKDIR` (new env var, follows the `ORK_` convention). `MkdirTemp` guarantees uniqueness by itself; include readable identity in the pattern: `fmt.Sprintf("ork-%s-%s-%d-*", info.WorkflowExecution.RunID|in.RunID, in.TaskID, activity.GetInfo(ctx).Attempt)`.
- Cleanup: `defer os.RemoveAll(dir)` **in the activity function itself**, which covers success, plugin error, plugin panic (Temporal recovers panics above this frame only if the defer runs first — a defer in the same function does run during panic unwinding), context cancellation, and StartToClose timeout *from the worker's perspective* (the goroutine is canceled; the defer runs when the plugin returns). A plugin that ignores `ctx` and blocks forever leaks — acceptable, document it.
- Test observability: expose the chosen dir to the test via the `plugin.Log` callback or a test plugin that records `ec.WorkDir`; after the activity returns, assert `os.Stat(dir)` is `fs.ErrNotExist`. This is straightforward with `testsuite.TestActivityEnvironment`.

### D13. `cmd/orker` wiring
```go
c, err := client.DialContext(ctx, client.Options{
    HostPort:  envOr("ORK_TEMPORAL_ADDRESS", client.DefaultHostPort),
    Namespace: envOr("ORK_TEMPORAL_NAMESPACE", client.DefaultNamespace),
})
store, err := artifacts.NewFromEnv()
reg := plugin.NewRegistry(); reg.Register(httpplugin.New()); ... 
w := worker.New(c, envOr("ORK_ORKER_QUEUE", task.DefaultQueue), worker.Options{
    DisableRegistrationAliasing: true,
    DisableWorkflowWorker:       true,
})
w.RegisterActivityWithOptions(orker.NewActivities(reg, store).ExecuteTask,
    activity.RegisterOptions{Name: task.ActivityExecuteTask})
err = w.Run(worker.InterruptCh())  // graceful shutdown on SIGINT/SIGTERM
```
`worker.Run(worker.InterruptCh())` is the idiomatic graceful-shutdown form; the engine's own binary pattern (`cmd/orkengine`, **not read this session — UNVERIFIED**) may use Start/Stop with signal handling. Env-var names `ORK_TEMPORAL_ADDRESS`/`ORK_TEMPORAL_NAMESPACE` are confirmed used by `internal/engine/integration_test.go:37` and proof/M2's README. Heartbeat cadence: since no `HeartbeatTimeout` is configured, heartbeat purely for observability (e.g. once per plugin phase); Temporal throttles heartbeats internally.

### D14. Error mapping
```go
var re *plugin.RetryableError
if errors.As(err, &re) && !re.Retryable {
    return res, temporal.NewNonRetryableApplicationError(err.Error(), "PLUGIN_PERMANENT", err)
}
```
Plain errors stay retryable (subject to the task's policy, which defaults to 1 attempt). Use `NewNonRetryableApplicationError` for: unknown plugin name (`UNKNOWN_PLUGIN`), template render/parse failure, artifact-ref malformed / not found, invalid `with` config. The engine only prints `err.Error()`, so **messages must be self-describing** (include task id, plugin name, ref string).

---

## E. Plugins

### E15. `http`
- Config keys per `docs/PLUGINS.md:43-47`: `method` (default `GET`), `url` (required), `headers` (`map[string]any` → string values), `body` (string), `capture_body_as`, `timeout_seconds`. Numbers arrive as `float64` (§A3) — accept `float64`/`int`/`string`.
- Body strategy that satisfies "EXACT response bytes **and** parsed json": stream the body straight to a file in `WorkDir` (`io.Copy` to `os.CreateTemp`), then `json.Unmarshal` the file contents into `any` **only if** it decodes; non-JSON leaves `json` absent (or `nil`). When `capture_body_as` is set, that file becomes the `ProducedArtifact` with `ContentType` from the response header. When it is not set, still buffer to a file and delete it (or cap in memory) — never put bytes in `Output`.
- Output: `{"status": float64(resp.StatusCode), "headers": map[string]any, "json": <parsed>, "duration_ms": float64}` — all structpb-safe. Headers: flatten `http.Header` (`[]string`) into `[]any` or first-value strings; **must not** stay `map[string][]string`.
- Tests: `httptest.NewServer` only; assert byte-identical artifact contents for a body containing invalid UTF-8 / no trailing newline; assert non-JSON body still succeeds; assert timeout produces a retryable error.

### E16. `shell`
- `exec.CommandContext(ctx, "sh", "-c", command)` with `cmd.Dir = ec.WorkDir`. Go 1.20+ `Cmd.Cancel` + `Cmd.WaitDelay` are the correct way to make cancellation kill a process that ignores SIGINT (Go 1.25 available). Env: start from a minimal base plus the `env` map (`map[string]any` → `KEY=value`).
- Output: `exit_code` (`float64`), `stdout`, `stderr` — **truncated** with a documented cap (define a constant, e.g. 8 KiB, and note truncation in the output or docs). Full streams go to files for `capture_stdout_as`/`capture_stderr_as` artifacts.
- `capture_paths`: `[]any` of globs, resolved **relative to `WorkDir`** with `filepath.Glob(filepath.Join(WorkDir, pattern))`; reject absolute/`..` escapes. `**` is **not** supported by `filepath.Glob` — either document single-level globs or add a dependency (dependency additions touch `go.mod`, which AGENTS.md §8 marks shared/coordinated ⇒ prefer stdlib). Multiple matches ⇒ one artifact per file named by its `WorkDir`-relative path (or `<glob-name>/<rel>`); document the rule.
- Non-zero exit **fails the task**; classify as **permanent** (`plugin.Permanent`) — a deterministic command that exited 3 will exit 3 again, and hello-ork's single-attempt policy makes the distinction untestable but the semantics are right.

### E17. `file`
- `op`: `write` (needs `path`, `content`; writes into `WorkDir`, produces an artifact — hello-ork's `save` task), `read` (needs `path`; may be a materialized artifact path; output size, optionally produce artifact), `copy` (`from`→`to`), `delete` (`path`).
- **Path containment**: resolve every `path`/`from`/`to` against `WorkDir` and reject escapes. Go 1.24+ `os.Root` (`os.OpenRoot`) is the strongest tool and is already the pattern used by `pkg/artifacts/local.go:40-72` — reuse it. Caveat: materialized artifact paths from `{{ artifact ... }}` live under `WorkDir/.artifacts/...`, so they stay inside the root; absolute paths that resolve inside `WorkDir` should be accepted.
- Output for every op: `{"path": <workdir-relative or absolute>, "size_bytes": float64}`. For `delete`, `size_bytes` is the removed size (or 0 when already absent — delete should be idempotent unless the contract says otherwise; **unspecified**, so prefer permissive).

---

## F. Tests, proof, operations

### F18. Integration pattern
Established skip pattern (`internal/engine/integration_test.go:48-66`): `//go:build integration`; `net.DialTimeout("tcp", addr, 2s)` → `t.Skipf`; then `client.DialContext` → `t.Skipf` on failure. Mirror it for MinIO with an HTTP probe of `ORK_S3_ENDPOINT` before switching the backend to `s3`.

For the M3 test:
- Start **three** workers as the engine test does: workflow-only (`LocalActivityWorkerOnly: true`) on the task queue, the **real Orker** activity worker on the same queue, and the event projection worker on `engine.EventQueue`. Replace the stub from `integration_test.go:87-112` with `orker` wiring.
- Hermetic URL: `httptest.NewServer` serving fixed JSON; submit the run with `SubmitRunRequest.Input` overriding `url` (the DSL default is `https://httpbin.org/json` — must be overridden, see `integration_test.go:247`). **UNVERIFIED**: exact `SubmitRunRequest` input field shape — check `internal/api/service.go` + `gen/go` before writing.
- Assertions: run `STATUS_SUCCEEDED`, 3 task runs all succeeded, `fetch` `TaskRun.Artifacts[0].Uri` readable via `store.Get` and byte-equal to the httptest payload, and `transform` output `stdout` containing the served content (proving the materialized path was consumed).
- **Cross-talk hazard (§0.4)**: the engine package's integration test registers a competing `ExecuteTask` on `task.DefaultQueue`. Mitigations, in preference order: (a) run integration packages with `-p 1` in the proof script; (b) have the M3 test use a per-test queue derived from `task.DefaultQueue` — **but criterion 6 explicitly says "on task.DefaultQueue"**, so (a) is the contract-compliant choice; (c) flag it in the final report if the module-wide `go test -tags=integration ./...` proves flaky.
- Also note the engine test hardcodes `applied workflow name = hello-ork` and reads `../../examples/hello.ork.yaml`; both tests apply the *same* workflow name to a shared in-process store — but each test builds its own `engine.NewWorkflowStore()`, so no shared-state conflict beyond the Temporal queue.

### F19. Proof conventions
- `proof/M1/`: `README.md`, `test.log`, `coverage.txt`, `reproduce.sh`, plus behavior transcripts (`roundtrip.txt`, `presign.txt`, `roundtrip_transcript.go.txt`).
- `proof/M2/`: `README.md` (What is proven / Reproduce / Artifacts table / Requirement-evidence table), `test.log`, `coverage.out`, `event-stream.json`, `engine-smoke.log`, driven by `scripts/m2-proof.sh`.
⇒ `proof/M3/` should be: `README.md` (same four sections), `test.log` (unit + integration verbose), `artifact-handoff.txt` (fetch→shell→file transcript), `artifact-uris.txt` (MinIO `s3://orkstack-artifacts/objects/...` URIs), optional `orker-smoke.log`, and `scripts/m3-proof.sh` as the single reproduce command. Note `Makefile` and `scripts/` — `Makefile` is shared/coordinated per AGENTS.md §8; adding `scripts/m3-proof.sh` avoids touching it.

### F20. Operational blockers
| Item | Status |
|---|---|
| Detached HEAD | Confirmed (`## HEAD (no branch)`). Branches: `main`, `m1-artifacts`, `m2-engine`. **Create `m3-orker`** before committing; do not open a PR. |
| `make lint` at `71f3a13` | **Green** — `gofmt -l` empty, `go vet ./...` silent, verified this session. M2's lint blocker does not reproduce. |
| Out-of-scope files needing edits | **None found.** The only files in M3's scope are stubs: `cmd/orker/main.go` (prints "not implemented"), `internal/orker/doc.go`, `pkg/plugins/{http,shell,file}/doc.go` (package clauses `httpplugin`/`shellplugin`/`fileplugin` — keep those names). |
| `go.mod` | Adding a dependency (e.g. doublestar) would touch a shared/coordinated file — avoid; stdlib-only is achievable. |
| `internal/dsl.KnownPlugins` | Already `{"file","http","shell"}` (`internal/dsl/compile.go:25`) — no registration change needed. |

---

## G. Key code references

- `internal/task/contract.go:18-63` — queue name, activity name, `ExecuteTaskInput`/`Result`, and the design note assigning rendering to the Orker.
- `pkg/plugin/plugin.go:30-72,100-151` — `ArtifactResolver`, `ExecContext`, `ProducedArtifact`, `Registry` (`Register` **panics** on duplicates — register once at startup), `Retryable`/`Permanent`.
- `internal/engine/workflow.go:296-314` — activity options (no heartbeat timeout).
- `internal/engine/workflow.go:318-357` — result consumption; `TaskID` unchecked; structpb constraint.
- `internal/engine/workflow.go:472-511` — queue override, 300s default timeout, single-attempt default retry.
- `internal/engine/convert.go:14-69` — payload/artifact conversion both directions.
- `internal/engine/condition.go:39-49` — precedent that `artifact`/`env` are Orker-only.
- `internal/dsl/template.go:16-26,37-60,191-265` — the exact reference surface the Orker renderer must accept.
- `pkg/artifacts/artifacts.go:40-72` — `Store` interface and sentinel errors.
- `pkg/artifacts/config.go:17-73` — env vars; MinIO defaults already match the running infra.
- `pkg/artifacts/local.go:83-161` — `Put` returns `Name` verbatim; `Get` streams exact bytes.
- `internal/engine/integration_test.go:48-135` — skip pattern and the three-worker topology to reuse.
- `examples/hello.ork.yaml:18-43` — the three tasks M3 must make pass end to end.
- `docs/PLUGINS.md:41-58`, `docs/DSL.md:47-59` — normative plugin/template surface.

---

## H. Unresolved / to confirm during implementation

1. `SubmitRunRequest` input shape for overriding `url` with the httptest URL — **UNVERIFIED** (read `internal/api/service.go` and the generated proto).
2. `cmd/orkengine/main.go` shutdown idiom, to keep `cmd/orker` symmetric — **not read**.
3. `env` allowlist source is entirely undefined by existing code; the `ORK_ORKER_ENV_ALLOWLIST` proposal is my design, not repo precedent.
4. Truncation limits for `shell` stdout/stderr are undocumented anywhere; pick a constant and document it in `docs/PLUGINS.md` (docs are in scope only if `docs/` is unowned — AGENTS.md §8 does not assign `docs/`; `docs/PLUGINS.md:66` explicitly instructs plugin authors to document there, so updating it is sanctioned).
5. Whether `-tags=integration ./...` is safe in parallel against shared Temporal (§F18) — must be empirically confirmed; use `-p 1` in the proof script.
6. Whether `file` `delete` of a missing path should error — unspecified; recommend idempotent success per the literal contract's permissiveness rule.

## Contract amendments received

None. No mid-run user steering was received in this session.