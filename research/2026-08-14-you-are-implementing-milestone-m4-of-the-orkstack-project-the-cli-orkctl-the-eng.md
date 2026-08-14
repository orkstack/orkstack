Research complete. Findings below.

---

# M4 `orkctl` — Research Findings

Repo: `/Users/daniel/Code/orkstack-wt-m4`, branch `m4-cli`… (checked: `git branch --show-current` printed empty output in a detached/worktree context — **unverified branch name**; commit `71f3a13 merge: integrate M1 (artifacts) and M2 (engine) into main`). Go 1.25.6. Working tree clean; `make lint` exits **0** and `go test ./...` is **green** on this checkout (both run this session). Temporal `localhost:7233` and MinIO `localhost:9000` are **up** (`nc -z` succeeded).

## 0. Headline conclusions

1. **Both plain HTTP/1.1 and Go‑1.25 h2c clients work against this server — including the `StreamEvents` server stream.** Proven empirically this session by a throwaway probe test (`cmd/orkctl/zz_probe_test.go`, since deleted; tree is clean). Recommendation: use the same construction the repo already uses in `internal/api/service_test.go:97-99`:
   ```go
   tr := &http.Transport{}
   tr.Protocols = new(http.Protocols)
   tr.Protocols.SetUnencryptedHTTP2(true)
   client := &http.Client{Transport: tr}
   ```
   No `golang.org/x/net/http2` dependency needed (Go 1.25 `http.Protocols`). `internal/api/server.go:41-47` enables both HTTP/1.1 and unencrypted HTTP/2 server-side, so either client is accepted; connect-go's default **Connect protocol** streams fine over HTTP/1.1 (confirmed by probe: 3 events received live, ~300 ms apart). Do **not** use `connect.WithGRPC()` — that would force HTTP/2 (connectrpc docs: unary Connect doesn't need HTTP/2; gRPC streaming does).
2. **`ListPlugins` returns an empty list** against `orkengine` — `cmd/orkengine/main.go` never sets `api.Config.Plugins`, and `internal/api/service.go:349-356` returns the configured (possibly empty) set. `plugin list` tests must assert "works and emits valid empty JSON / `No plugins.`-style output", not "returns builtin plugins".
3. **`Apply` never returns warnings today.** `internal/api/service.go:103-115` builds `ApplyWorkflowResponse{Workflow: wf}` only; `internal/dsl` exposes `Compile`/`CompileFile`/`CompileWithOptions` returning `(*Workflow, error)` with no warnings channel. Probe confirmed `warnings=[]`. The CLI must render warnings **if present** but tests cannot expect any.
4. **Paging is not implemented server-side.** `ListWorkflows`/`ListRuns` honour only `page_size` (truncation) and never set `next_page_token` (`service.go:134-142`, `261-273`); `page_token` is ignored. The CLI may send `page_size` and loop on `next_page_token`, but the loop will terminate on the first page. Don't build tests that require multi-page behavior.
5. **Terminal-stream edge case that decides your exit code logic:** `StreamEvents` returns *immediately with zero events* when the bus says the terminal event is at or before `from_sequence` (`service.go:314-317`, `internal/engine/bus.go:94-102`). Therefore `run watch` must, after the stream ends, call `GetRun` to determine the final `Status` and the exit code — never infer only from observed events. Probe confirmed: replay with `from_sequence=0` after a terminal run replayed 3 backlog events then closed cleanly (n=3, err=nil); with `from_sequence >= terminalSeq` it would return 0 events.
6. **`spf13/cobra v1.10.2` is already in the module cache** (`$(go env GOMODCACHE)/github.com/spf13/cobra@v1.10.2`, plus `pflag@v1.0.10`), so adding the dependency works offline. Only `go.mod`/`go.sum` change — within scope lock.

## 1. Generated client surface (`gen/go/orkstack/v1/orkstackv1connect/service.connect.go`)

Constructors (all `(connect.HTTPClient, baseURL string, ...connect.ClientOption)`):
- `NewWorkflowServiceClient` (:78) → `Apply` (:111), `GetWorkflow` (:116), `ListWorkflows` (:121)
- `NewRunServiceClient` (:205) → `SubmitRun` (:252), `GetRun` (:257), `ListRuns` (:262), `CancelRun` (:267), `StreamEvents` (:272)
- `NewPluginServiceClient` (:377) → `ListPlugins` (:396)

`StreamEvents` returns `*connect.ServerStreamForClient[v1.Event]` (:195). Loop idiom: `for st.Receive() { ev := st.Msg() ... }`, then check `st.Err()`, always `defer st.Close()`.

Request fields (from `proto/orkstack/v1/service.proto`):
- `ApplyWorkflowRequest{yaml}` → `ApplyWorkflowResponse{workflow, warnings}`
- `GetWorkflowRequest{name, version}` (version "" = latest)
- `ListWorkflowsRequest{page_size, page_token}`
- `SubmitRunRequest{workflow_name, workflow_version, input Payload, idempotency_key}` → `SubmitRunResponse{run}`
- `GetRunRequest{run_id}`, `ListRunsRequest{workflow_name, status, page_size, page_token}`
- `CancelRunRequest{run_id, reason}` → `CancelRunResponse{run}` (returns the run **as stored**, i.e. pre-cancel status)
- `StreamEventsRequest{run_id, from_sequence}` (0 = from start)
- `ListPluginsRequest{}` → `ListPluginsResponse{plugins []PluginInfo{name,version,description,input_schema,output_schema}}`

## 2. Status / event taxonomy (`common.proto`, `run.proto`)

`Status`: UNSPECIFIED, PENDING, RUNNING, SUCCEEDED, FAILED, CANCELED, SKIPPED.
`EventType`: RUN_CREATED, RUN_STARTED, TASK_SCHEDULED, TASK_STARTED, TASK_LOG, TASK_SUCCEEDED, TASK_FAILED, RUN_SUCCEEDED, RUN_FAILED, RUN_CANCELED.

Server-side terminal set (`api.IsTerminalEvent`, `service.go:339-347`): RUN_SUCCEEDED, RUN_FAILED, RUN_CANCELED. `run watch` should mirror exactly this set. Suggested exit taxonomy (contract §5 only demands 0/non-zero):
- 0: success, incl. `STATUS_SUCCEEDED`
- 1: command/RPC error (unknown workflow → `connect.CodeNotFound`; unreachable server → `connect.CodeUnavailable`, both observed in the probe)
- non-zero (e.g. 2): watched run ended `STATUS_FAILED`; (e.g. 3) `STATUS_CANCELED` — criterion 5 only requires the FAILED path be proven.

`Run` fields for rendering: `id, workflow_name, workflow_version, status, input, output, error{code,message,details,retryable}, task_runs[]{task_id,plugin,status,attempt,output,artifacts,error,started_at,finished_at}, created_at, started_at, finished_at`.

## 3. In-process real-server harness recipe (criterion 3)

`internal/api/service_test.go:73-118` (`newFixture`) is the template and is fully reproducible from `cmd/orkctl`'s own test files — everything used is exported: `api.NewService`, `api.Config{Workflows,Runs,Bus,Starter,Plugins,Now,NewRunID}`, `api.NewMux`, `api.NewServer`, `api.RunStarter` interface (`service.go:19-23`), `engine.NewEventBus/NewRunStore/NewWorkflowStore`. Recipe:

```go
bus := engine.NewEventBus(); runs := engine.NewRunStore()
svc := api.NewService(api.Config{Workflows: engine.NewWorkflowStore(), Runs: runs, Bus: bus,
    Starter: fakeOrStub, NewRunID: deterministicIDs})
server := api.NewServer("127.0.0.1:0", api.NewMux(svc, api.MuxOptions{}))
ln, _ := net.Listen("tcp", "127.0.0.1:0"); go server.Serve(ln)
baseURL := "http://" + ln.Addr().String()   // pass to CLI as --server
t.Cleanup(func(){ ctx,cancel := context.WithTimeout(...,5*time.Second); defer cancel(); server.Shutdown(ctx) })
```
Then run the root Cobra command with `SetArgs([]string{"run","get",id,"--server",baseURL})` and captured `SetOut/SetErr` buffers. Note `api.NewServer` ignores the addr you pass when you `Serve(ln)` yourself — the listener wins (same as the fixture).

**Fake starter** (no Temporal): implement `api.RunStarter` in the test; publish synthetic events via `bus.Publish(event, dedupeKey string)` — note the **two-arg** signature (`internal/engine/bus.go:35`), dedupe key `""` is fine. This gives a fully hermetic, deterministic `run watch` failure test (publish RUN_STARTED then RUN_FAILED → assert non-zero exit) with **no sleeps**, satisfying AGENTS §5.

**Stub `ExecuteTask` worker** (criterion 6, requires real Temporal in the unit lane — prefer keeping the *unit* lane hermetic with the fake starter and putting Temporal-backed runs in the `integration` lane). Pattern to copy: `internal/engine/integration_test.go:70-135` — three workers:
- run worker on `task.DefaultQueue` with `worker.Options{DisableRegistrationAliasing:true, LocalActivityWorkerOnly:true}`, registering `engine.RunWorkflow` as `engine.WorkflowName`;
- **stub activity worker** on `task.DefaultQueue`, `DisableWorkflowWorker:true`, registering a `func(ctx, task.ExecuteTaskInput) (task.ExecuteTaskResult, error)` under `activity.RegisterOptions{Name: task.ActivityExecuteTask}` — return an error to force `RUN_FAILED`;
- event worker on `engine.EventQueue` registering `engine.NewEventActivities(bus, runs).PublishEvent` as `engine.ActivityPublishEvent`.
Because the engine's run worker is `LocalActivityWorkerOnly`, it cannot steal `ExecuteTask` from your stub even when the engine process runs separately — this is what makes criterion 7's "real engine process + test-local stub worker" viable.

**Skip pattern** (copy `integration_test.go:48-53`): `net.DialTimeout("tcp", addr, 2*time.Second)` → `t.Skipf`, then `client.DialContext` failure → `t.Skipf`. Address from `ORK_TEMPORAL_ADDRESS`, default `client.DefaultHostPort`.

## 4. Engine wiring an integration harness must replicate

`cmd/orkengine/main.go`: `ORK_TEMPORAL_ADDRESS`, `ORK_TEMPORAL_NAMESPACE`, `ORK_HTTP_ADDR` (default `api.DefaultHTTPAddr` = `:8080`), `ORK_WS_ALLOWED_ORIGINS`; `api.NewService{Workflows: engine.NewWorkflowStore(), Runs, Bus, Starter: engine.NewTemporalStarter(temporalClient, task.DefaultQueue)}`. Stores are **in-memory** — a restarted engine forgets workflows and runs, so the integration test must `apply` before `submit` in the same engine lifetime. `scripts/m2-proof.sh:50-52` shows the established way to boot the engine on a non-default port for proof (`ORK_HTTP_ADDR=:18080 go run ./cmd/orkengine &`); reuse that shape for `scripts/m4-proof.sh`.

`TemporalStarter.CancelRun` (`internal/engine/temporal.go:47-52`) calls `client.CancelWorkflow`; cancelling an unknown/completed workflow returns an error → surfaced as `CodeInternal`. So `run cancel` "is accepted" is easiest to prove with the fake starter (in-process lane), not against a finished real run.

## 5. `apply` and `--input` semantics

- `apply` is a **raw YAML upload**: the CLI reads the file and sends `ApplyWorkflowRequest{yaml: string(bytes)}`; compilation happens server-side (`service.go:107`, `dsl.Compile`). Compile errors come back as `CodeInvalidArgument` — render the message, exit non-zero. Do **not** import `internal/dsl` client-side.
- Probe output for `examples/hello.ork.yaml` (protojson, real server): `{"name":"hello-ork","version":"0.1.0","description":"…","inputs":{"url":{"default":"https://httpbin.org/json","type":"string"}},"tasks":[{"id":"fetch","plugin":"http","with":{…},"timeoutSeconds":300}, …]}` — useful for the golden `apply` output (`hello-ork` / `0.1.0` / 3 tasks).
- Input defaults are applied **server-side** by `engine.ApplyInputDefaults` (`internal/engine/defaults.go:18`), so `run submit hello-ork` with no `--input` yields `input.data.url = https://httpbin.org/json` (probe-confirmed). Declared DSL types (`docs/DSL.md:19-24`): `string | number | boolean | object | array`.
- `--input k=v` parsing rule that fits `Payload{google.protobuf.Struct data}`: parse `v` as JSON first (so `3`, `true`, `null`, `[1,2]`, `{"a":1}` become typed); fall back to the raw string when JSON parsing fails (so `https://…` stays a string). Add explicit escape hatches: `--input-json '<json object>'` and/or `k:=<json>`; build the struct with `structpb.NewStruct(map[string]any)`. Keys keep insertion form verbatim (no normalization) per the literal contract.

## 6. Rendering and `--json`

- Use **`protojson`** for `--json` (repo precedent: `internal/api/service_test.go` and `integration_test.go` both import it). It yields stable lowerCamelCase field names and enum names as strings (`"STATUS_PENDING"`, `"EVENT_TYPE_RUN_FAILED"`), and `int64` as strings (`"sequence":"1"`). `encoding/json` on protobuf structs is unsafe/unsupported. For list commands, marshal each message with protojson and assemble the array yourself (e.g. `{"runs":[…]}` via `protojson.Marshal(resp.Msg)` on the whole response message — simplest and stable). Criterion 4 says the test must parse with `encoding/json` — parsing protojson output with `encoding/json` works fine.
- Human tables: `text/tabwriter` (`tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)`), status rendered without the `STATUS_` prefix. Keep timestamps stable for golden tests — either omit them from golden-tested output or inject a fixed clock via deterministic `NewRunID`/fixture times (`api.Config.Now`, `NewRunID` are injectable; the fixture uses `run-%03d`).
- Golden tests: standard Go pattern — `-update` flag writing `testdata/*.golden`, compare with `bytes.Equal`. Golden-test `apply`, `run get`, `run list` (criterion 4).

## 7. Cobra structure (library guidance)

- Root `orkctl` with persistent flags `--server` (default `http://localhost:8080`, env `ORK_SERVER`), `--json`, `--timeout` (duration). Cobra has no native env binding: read the env in `PersistentPreRunE` when the flag is not `Changed`, or default to `os.Getenv("ORK_SERVER")` — avoid viper (extra dep).
- Use `RunE` everywhere and set `SilenceUsage: true` + `SilenceErrors: true` **on the root only** (cobra issue #340: root suffices). Print errors yourself to `cmd.ErrOrStderr()`.
- Exit codes without `os.Exit` in command code: define `type exitError struct{ code int; err error }`; commands return it; `main()` does `if err := root.Execute(); err != nil { … os.Exit(codeOf(err)) }` (cobra issue #2124 discusses exactly this). Tests call an `Execute(args, stdout, stderr) int` helper and assert the int — no process spawn, and stdout is captured via `cmd.SetOut/SetErr` + `SetArgs` (cobra's own `command_test.go` idiom).
- Timeout mapping: apply `context.WithTimeout(cmd.Context(), timeout)` to unary calls. For `run watch`/`--watch`, `--timeout` should either be ignored or treated as a separate, larger overall deadline — a 30 s unary default would kill a legitimate long stream. Recommend: unary deadline = `--timeout`; stream deadline = only `--timeout` when the user explicitly set it, else unbounded until terminal event (document this).
- Command tree: `apply <file>`; `workflow list|get <name> [--version]`; `run submit <workflow> [--input k=v]… [--input-json] [--version] [--idempotency-key] [--watch]`; `run get <id>`; `run list [--workflow] [--status] [--limit]`; `run watch <id> [--from-sequence]`; `run cancel <id> [--reason]`; `plugin list`; `version`.

## 8. Conventions, lint, proof

- AGENTS §3: Cobra, human default + `--json`, `ORK_` env prefix, `context.Context` first arg, `%w` wrapping, no panics. §5: behavior-named tests (`TestRunWatch_ExitsNonZeroWhenRunFails`), no sleeps. §7: Conventional Commits (`feat(cli): …`, `test(cli): …`).
- `make lint` = `go vet ./...` + `gofmt -l` over non-`gen` files; it passes today (verified), so the M2-era gofmt blocker in `proof/M2/README.md` no longer applies on this checkout. Keep it that way; `gofmt` your new files.
- `proof/M4/` should mirror `proof/M2/`: `README.md` (what was proven + prerequisites + single reproduce command), `test.log`, `coverage.out`, a behavior trace. Add `scripts/m4-proof.sh` modeled on `scripts/m2-proof.sh` (`set -euo pipefail`, `cd "$(dirname "$0")/.."`, tee everything into `test.log`, boot engine on `ORK_HTTP_ADDR=:18080`, kill on exit). Criterion 9 artifacts: `test.log`, `cli-session.txt` (apply → submit --watch → live events → run get, real output), `json-sample.json`, `README.md`. **Caveat:** `scripts/` is not in the criterion‑10 scope list; the safest reading is to place the reproduce script at `proof/M4/reproduce.sh` — `proof/M1/reproduce.sh` is exactly that precedent — rather than `scripts/`.

## 9. Risks / unresolved

- **Criterion 7 vs. no Orker:** a real-engine `submit --watch` will never terminate unless the integration test itself runs the stub `ExecuteTask` worker on `task.DefaultQueue` against the same Temporal namespace. This is permitted by criterion 6 and is the only workable design; call it out in `proof/M4/README.md`.
- **Proof transcript realism:** the recorded CLI session will show a run driven by a *stub* task worker (M3 absent). Say so explicitly in the transcript header.
- **`plugin list` is empty** against a real engine — the proof transcript should include it and show the empty result rather than implying builtin plugins exist.
- **Concurrent M3 agent** owns `internal/orker`, `pkg/plugins/*`, `cmd/orker`; `go.mod`/`go.sum` are the only shared files you touch (cobra + pflag additions) — expect a trivial merge there.
- Branch name unverified (`git branch --show-current` returned empty in this worktree); confirm before committing.

## Contract amendments received

None. No mid-run user messages amended the objective or acceptance criteria during this stage.