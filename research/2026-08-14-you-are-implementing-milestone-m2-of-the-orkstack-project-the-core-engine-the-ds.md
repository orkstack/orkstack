# M2 Research Report

## Conclusions

- The repository is still a scaffold. `internal/dsl`, `internal/engine`, `internal/api`, and `cmd/orkengine` have no implementation. Current `git status` is clean at detached `HEAD`.
- Use:
  - Temporal SDK `go.temporal.io/sdk v1.47.0`
  - YAML parser `gopkg.in/yaml.v3 v3.0.1`
  - WebSockets `github.com/coder/websocket v1.8.15`
  - Native Go 1.25 h2c via `http.Server.Protocols`; do not add `x/net` unless legacy h2c support is required.
- Do not change `internal/task/contract.go` or proto for the normal M2 path.
- The main contract gaps are:
  1. `Task.condition` may require Orker rendering, but `ExecuteTaskInput` has no condition field.
  2. `EventType` has no `EVENT_TYPE_TASK_SKIPPED`.
  3. `ExecuteTaskResult` carries no activity attempt count.
  4. `Makefile build` references absent `cmd/orker` and `cmd/orkctl`, outside M2 scope.

The infrastructure check passed during research: Temporal `localhost:7233`, Temporal UI `:8233`, and MinIO `:9000` were reachable.

---

## 1. DSL compiler

### Fixed mappings

Local contracts:

- `docs/DSL.md:5-15,16-26,28-45,61-68`
- `proto/orkstack/v1/workflow.proto:10-46`
- `examples/hello.ork.yaml:4-44`
- `internal/task/contract.go:42-51`

Recommended compiler API:

```go
func Compile(source []byte) (*orkstackv1.Workflow, error)
func CompileFile(path string) (*orkstackv1.Workflow, error)
```

Use private YAML structs with `yaml` tags:

- top-level `name`, `version`, `description`, `labels`, `inputs`, `tasks`
- task `id`, `plugin`, `needs`, `condition`, `timeout_seconds`, `retry`, `queue`, `with`

Map:

- `inputs` → `structpb.Struct`
- task `with` → `structpb.Struct`
- retry fields → `orkstackv1.RetryPolicy`
- raw source bytes → `Workflow.SourceYaml`
- omitted timeout → `300`
- pinned version → preserve exactly
- unpinned version → canonical SHA-256 hash

`structpb.NewStruct` accepts nested maps, slices, strings, numbers, booleans, and nulls. Its numeric values become protobuf `double`s, so large integer precision is a known limitation (`structpb.NewValue`, protobuf v1.36.12).

### Parser choice

Use `gopkg.in/yaml.v3 v3.0.1`.

Reasons:

- Supports `yaml` tags and `yaml.Node`.
- Supports `Decoder`.
- Provides source locations for useful errors.
- Default decoder construction enables unique mapping keys.
- `KnownFields(true)` exists, but should not be enabled for M2 because rejecting every unknown YAML field would add a validation rule not listed in `docs/DSL.md`.

`sigs.k8s.io/yaml v1.6.0` is a valid alternative, but it first converts YAML to JSON, uses `json` tags, and loses some YAML-specific behavior. `UnmarshalStrict` adds unknown/duplicate-field rejection. It is not needed here.

The upstream YAML project now treats v3 as frozen/security-only and recommends v4, but v4 would be a breaking migration. Keep v3 for M2.

### Six required validation rules

Implement exactly these rules:

1. Workflow `name` is present and matches:

   ```text
   ^[a-z][a-z0-9-]*$
   ```

2. Task IDs are unique and match the same slug rule.
3. Every `needs` target exists.
4. The `needs` graph is acyclic.
5. `plugin` is non-empty and known.
6. Every template parses and references only declared inputs and transitive upstream tasks.

Do not add slug rules for labels, input names, artifact names, or arbitrary `with` keys unless required by template syntax.

The documented built-in plugin names are `http`, `shell`, and `file` (`docs/PLUGINS.md:41-58`). Since no runtime plugin registry exists yet, M2 should use a default known-name set containing those three, with an option for a future registry-backed validator.

### Template validation

Walk every string recursively in:

- task `with`
- task `condition`

Parse with `text/template` and register helper stubs before parsing:

```go
template.FuncMap{
    "artifact": func(string) string { return "" },
    "env":      func(string) string { return "" },
}
```

This makes the example’s `artifact "fetch/body"` expression parse successfully without executing it.

Use the template parse tree to validate:

- `.inputs.<name>` exists in declared inputs.
- `.tasks.<id>.output...` refers to a task in the current task’s transitive `needs` closure.
- `artifact "<task>/<name>"` refers to an upstream task and has a non-empty reference.
- `env "<NAME>"` is syntactically valid; the Orker performs allowlist checks.

Do not render `with` during compilation. `internal/task/contract.go:7-13` explicitly assigns rendering and artifact resolution to the Orker.

### Stable content hash

For an unpinned version:

1. Decode the YAML into a semantic tree.
2. Remove the `version` field from the tree.
3. Preserve sequence order.
4. Recursively normalize maps to string-keyed JSON-compatible maps.
5. Marshal with `encoding/json.Marshal`.
6. Hash those exact canonical JSON bytes using SHA-256.
7. Store lowercase 64-character hexadecimal output in `Workflow.Version`.

`encoding/json` sorts string map keys before encoding (`Go 1.25.6 stdlib `encoding/json`), so YAML map ordering does not affect the hash. Comments and whitespace also do not affect it. Task list order remains significant.

Do not hash the raw YAML bytes: that would make formatting and map ordering change the version.

Always set:

```go
workflow.SourceYaml = string(source)
```

from the original bytes, never from re-serialized YAML.

`CreatedAt` is not a DSL field. Set it in the API/store outside Temporal, or leave it nil in the pure compiler.

### Example acceptance

`examples/hello.ork.yaml` should compile with:

- name `hello-ork`
- pinned version `0.1.0`
- three tasks: `fetch`, `transform`, `save`
- `transform.needs == ["fetch"]`
- `save.needs == ["transform"]`
- valid `artifact` and `.tasks.transform.output.stdout` templates

---

## 2. Deterministic `RunWorkflow`

### Suggested workflow shape

```go
type RunWorkflowInput struct {
    RunID     string
    Workflow  *orkstackv1.Workflow
    RunInput  *orkstackv1.Payload
}

func RunWorkflow(
    ctx workflow.Context,
    input RunWorkflowInput,
) (*orkstackv1.Run, error)
```

Register it with a stable name:

```go
worker.RegisterWorkflowWithOptions(
    RunWorkflow,
    workflow.RegisterOptions{Name: "RunWorkflow"},
)
```

Temporal v1.47 uses `client.ExecuteWorkflow`, not `StartWorkflow`.

### Deterministic DAG scheduling

Use task IDs sorted lexically for every ready-set scan.

For each DAG wave:

1. Find pending tasks whose dependencies are terminal.
2. If any dependency failed, fail the run and do not schedule the task.
3. If any dependency is skipped, mark the task skipped.
4. Evaluate the task condition.
5. Schedule every ready activity before waiting for any result.
6. Consume futures in sorted task-ID order.
7. Record results and repeat.

Scheduling all futures first provides parallel fan-out without native goroutines. If `workflow.NewSelector` is used, never assign event sequence numbers based on whichever future the selector returns first; the SDK documents that multiple ready selector branches may be chosen in arbitrary order. Sorted future consumption gives stable event ordering.

Temporal workflow code must not use:

- `time.Now`
- native goroutines
- native channels or `select`
- network or filesystem I/O
- direct event-bus calls
- unsorted map iteration

Use `workflow.Now(ctx)` for timestamps.

### Activity invocation

For each task:

```go
options := workflow.ActivityOptions{
    TaskQueue:          task.Queue,
    StartToCloseTimeout: time.Duration(timeoutSeconds) * time.Second,
    RetryPolicy:         retryPolicy,
}
activityCtx := workflow.WithActivityOptions(ctx, options)

future := workflow.ExecuteActivity(
    activityCtx,
    task.ActivityExecuteTask,
    task.ExecuteTaskInput,
)
```

The actual fixed values are:

- activity name: `task.ActivityExecuteTask`
- default queue: `task.DefaultQueue`
- default queue value: `orkstack-tasks`

Use the string activity name, not a function reference. The Temporal SDK supports string activity names and this permits the Orker to register the fixed external name.

If `Task.Queue` is empty, use `task.DefaultQueue`.

### Retry and timeout conversion

`ActivityOptions.StartToCloseTimeout` is required by Temporal. Use the DSL timeout, defaulting to 300 seconds.

The DSL retry policy maps to:

```go
&temporal.RetryPolicy{
    MaximumAttempts:    ...,
    InitialInterval:    ...,
    BackoffCoefficient: ...,
    MaximumInterval:    ...,
}
```

The proto comment states that maximum attempts `0` or `1` means no retries. Therefore an absent or zero policy must become `MaximumAttempts: 1`; leaving the pointer nil would invoke Temporal’s default unlimited retry policy.

Temporal defaults documented in v1.47:

- initial interval: 1 second
- backoff coefficient: 2
- maximum interval: 100 × initial interval
- maximum attempts: 0 means unlimited

The DSL explicitly gives seconds, so convert each integer field to `time.Duration * time.Second`.

Numeric retry validation is not one of the six DSL rules. The implementation should apply documented defaults and avoid inventing new DSL rejection rules, while still ensuring Temporal receives valid options.

### Fixed seam data conversion

The engine must send:

- raw `Task.With`
- run input
- upstream output and artifact references
- timeout
- task and workflow identity

It must not render templates.

Convert:

- protobuf `Payload.Data.AsMap()` → `plugin.Payload`
- protobuf `ArtifactRef` → `plugin.ArtifactRef`
- `Task.With.AsMap()` → `map[string]any`
- `ExecuteTaskResult.Output` → protobuf `Payload`
- `ExecuteTaskResult.Artifacts` → protobuf artifact references

Build `Upstream` from completed task results using sorted dependency IDs. Do not range over a map in workflow code.

### Conditions and the fixed contract gap

`docs/DSL.md:35,49-59` says conditions use the same template context as `with`. `internal/task/contract.go` says Orkers render conditions, but `ExecuteTaskInput` has no `Condition` field.

This makes full support for conditions involving `artifact` or `env` impossible without changing the fixed seam or adding a new Orker activity.

The narrow M2-compatible approach is:

- Evaluate only deterministic literal conditions and simple selectors over submitted inputs, such as:
  - `false`
  - `0`
  - empty string
  - `{{ .inputs.enabled }}`
- Use upstream output values only where the workflow already has the referenced result.
- Do not resolve artifacts or environment variables in workflow code.
- Do not render task `with` values in the engine.
- Test the required unmet condition with an inputs-only or literal false condition.

Conditions requiring Orker-only artifact/environment rendering remain an explicit unresolved contract gap. A fully general solution needs either:

- an additive `Condition` field in `ExecuteTaskInput`, or
- a dedicated condition activity implemented by the Orker.

Neither is allowed by the current scope lock.

### Skip propagation

Recommended rule:

- An unmet condition marks the task `STATUS_SKIPPED`.
- A task with any skipped dependency is also `STATUS_SKIPPED`.
- Skip propagation continues transitively.
- A failed dependency fails the run rather than silently skipping the dependent.

There is no `EVENT_TYPE_TASK_SKIPPED` in `run.proto`. Until an additive proto change is approved, represent a skip in the read model as `TaskRun.Status = STATUS_SKIPPED`; if an event is required, use an existing task lifecycle event with `Status = STATUS_SKIPPED` and a clear message. A distinct event type would be cleaner but is not currently part of the contract.

### Failure and attempts

After Temporal exhausts retries:

- mark the task run failed
- emit a task-failed event
- mark the run failed
- return a workflow error

`ExecuteTaskResult` contains no attempt number. The engine cannot accurately populate `TaskRun.Attempt` from the fixed activity result. Tests can prove retry count using `activity.GetInfo(ctx).Attempt` or a mock counter. Accurate persisted attempt state requires an additive seam change.

### Required workflow tests

Use `testsuite.WorkflowTestSuite` and `TestWorkflowEnvironment`.

For string activity mocking:

1. Register the activity under `ExecuteTask`.
2. Configure `env.OnActivity("ExecuteTask", ...)`.
3. Execute the workflow.
4. Assert `GetWorkflowError`, `GetWorkflowResult`, and mock expectations.

Tests should cover:

- `TestRunWorkflow_ExecutesLinearNeedsInOrder`
- `TestRunWorkflow_FansOutIndependentTasks`
- `TestRunWorkflow_SkipsUnmetCondition`
- `TestRunWorkflow_RetriesThenFailsRun`
- `TestRunWorkflow_PassesUpstreamOutputAndArtifacts`

For fan-out, return activity results through controlled channels or record that all independent activities were scheduled before any dependent task was invoked. Avoid sleeps.

Temporal sources:

- https://docs.temporal.io/develop/go/activities/execution
- https://pkg.go.dev/go.temporal.io/sdk/workflow
- https://pkg.go.dev/go.temporal.io/sdk/testsuite
- https://pkg.go.dev/go.temporal.io/sdk/client
- https://pkg.go.dev/go.temporal.io/sdk/worker

---

## 3. Event bus and run read model

### Recommended design

Do not publish directly from workflow code.

Use a dedicated engine activity, for example:

```go
type PublishEventInput struct {
    Event *orkstackv1.Event
    Run   *orkstackv1.Run
}

func (a *EventActivities) PublishEvent(
    context.Context,
    PublishEventInput,
) error
```

The activity:

1. Deduplicates by `(run_id, sequence)`.
2. Updates the in-memory run read model from the supplied run snapshot.
3. Appends the event to the per-run event log.
4. Notifies subscribers.

The workflow owns:

- event order
- sequence assignment
- event timestamps
- run/task state transitions

The activity owns:

- in-process store mutation
- event publication
- synchronization and subscriber notification

Use `workflow.Now(ctx)` for `Event.Timestamp` and run timestamps. Increment a workflow-local sequence counter before each event. Wait for each publish activity before publishing the next event.

This remains replay-safe because the workflow only schedules activities and carries deterministic state. It also works when `orkengine` hosts both the Temporal worker and HTTP server: both use the same `EventBus` and `RunStore` instance.

Use a separate queue such as `orkstack-engine-events` for event activities so Orkers polling `orkstack-tasks` cannot receive engine projection activities. Start a second engine worker for that queue.

### Bus behavior

The bus should provide:

```go
Publish(event *orkstackv1.Event, run *orkstackv1.Run) error
Events(runID string, fromSequence int64) []*orkstackv1.Event
Subscribe(ctx context.Context, runID string, fromSequence int64) (<-chan *orkstackv1.Event, func())
GetRun(runID string) (*orkstackv1.Run, bool)
```

Register the subscriber while taking the backlog snapshot to avoid a publish gap.

Recommended resume semantics:

- `from_sequence == 0`: send all events.
- `from_sequence > 0`: send events with sequence greater than the supplied value.

This treats `from_sequence` as the last-seen cursor. The caller passes the last received sequence after reconnecting.

Store cloned protobuf messages. Never expose mutable internal pointers.

Temporal history polling or `DescribeWorkflowExecution` can support recovery after process restart, but it is not a good primary live-event path:

- it adds polling delay
- it does not directly push to connected streams
- it requires another Temporal client path
- it complicates event ordering

The activity projection is the best M2 path for live events and the real-Temporal integration test.

---

## 4. Connect API and WebSocket

### Generated Connect handlers

Use the generated constructors. Do not edit generated files.

The handler signatures are defined in:

- `gen/go/orkstack/v1/orkstackv1connect/service.connect.go`
- `proto/orkstack/v1/service.proto:11-89`

Implement:

- `WorkflowService.Apply`
- `WorkflowService.GetWorkflow`
- `WorkflowService.ListWorkflows`
- `RunService.SubmitRun`
- `RunService.GetRun`
- `RunService.ListRuns`
- `RunService.CancelRun`
- `RunService.StreamEvents`
- `PluginService.ListPlugins`

Apply:

1. Call `dsl.Compile`.
2. Store by name/version.
3. Return the compiled workflow.

Get:

- exact version when supplied
- latest version when empty

Submit:

1. Resolve workflow name/version.
2. Create a pending run in the read model.
3. Call Temporal `client.ExecuteWorkflow`.
4. Use `StartWorkflowOptions{ID: runID, TaskQueue: task.DefaultQueue}`.
5. Pass `"RunWorkflow"` and `RunWorkflowInput`.
6. Return the pending run.

Use a narrow `WorkflowStarter` interface in API tests rather than mocking the full Temporal `client.Client`.

### StreamEvents

Generated implementation shape:

```go
func (s *RunService) StreamEvents(
    context.Context,
    *connect.Request[orkstackv1.StreamEventsRequest],
    *connect.ServerStream[orkstackv1.Event],
) error
```

Implementation:

1. Validate `run_id`.
2. Subscribe to the bus with `from_sequence`.
3. Send backlog in sequence order.
4. Send live events as they arrive.
5. Stop on request context cancellation or terminal stream completion.
6. Return Connect `CodeNotFound` for an unknown run.

`connect.ServerStream.Send` is not safe for concurrent calls, so use one sending loop.

Connect source:

- https://github.com/connectrpc/connect-go/blob/1291a7dcac19b00490f935dce18f44f301fc58f6/handler.go
- https://github.com/connectrpc/connect-go/blob/1291a7dcac19b00490f935dce18f44f301fc58f6/handler_stream.go

### h2c

Go 1.25.6 supports unencrypted HTTP/2 directly:

```go
protocols := new(http.Protocols)
protocols.SetHTTP1(true)
protocols.SetUnencryptedHTTP2(true)

server := &http.Server{
    Addr:      addr,
    Handler:   mux,
    Protocols: protocols,
}
```

This serves:

- HTTP/1.1 Connect requests
- h2c Connect/gRPC requests
- WebSocket HTTP/1.1 upgrades

`golang.org/x/net/http2/h2c` is deprecated in `x/net v0.58.0`. Use it only if pre-Go-1.24 compatibility is required.

h2c docs:

- https://pkg.go.dev/golang.org/x/net/http2/h2c
- https://connectrpc.com/docs/go/deployment/

There is no health RPC in the proto. A `ListWorkflows` request is the available health/list check.

### WebSocket

Use `github.com/coder/websocket v1.8.15`.

The route is:

```text
/ws/runs/{id}
```

Use:

```go
conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{...})
```

Use `CloseRead` for a write-only connection so control frames remain handled.

Use `protojson.Marshal` for each event, then write text frames directly:

```go
payload, err := protojson.Marshal(event)
err = conn.Write(ctx, websocket.MessageText, payload)
```

Do not use `encoding/json` on generated protobuf structs. It produces Go-struct JSON rather than protobuf JSON and may encode enums and timestamps differently. `coder/websocket/wsjson` uses `encoding/json`, so it is not suitable for this event format unless a separate wire struct is deliberately defined.

Configure allowed origins explicitly for browser clients. `coder/websocket` rejects cross-origin requests by default.

WebSocket docs:

- https://pkg.go.dev/github.com/coder/websocket
- https://github.com/coder/websocket/blob/9c8faadccd1b679e811a79ce506f8a10237251ad/accept.go

### PluginService

An empty response is acceptable in M2:

```go
return connect.NewResponse(&orkstackv1.ListPluginsResponse{}), nil
```

If a registry is supplied, convert descriptors to `PluginInfo` and sort by plugin name before returning.

---

## 5. `cmd/orkengine`

Configuration:

- `ORK_TEMPORAL_ADDRESS`, default `localhost:7233`
- `ORK_TEMPORAL_NAMESPACE`, default `default`
- `ORK_HTTP_ADDR`, default `:8080`

Startup:

1. Dial Temporal with `client.Dial` or `client.DialContext`.
2. Create the workflow worker on `task.DefaultQueue`.
3. Register `RunWorkflow` as `"RunWorkflow"`.
4. Create an engine-event worker on the event queue.
5. Register the event projection activity.
6. Build the API service with the workflow store, run store, and bus.
7. Mount generated Connect handlers.
8. Mount `/ws/runs/`.
9. Start the HTTP server.
10. On SIGINT/SIGTERM, shut down HTTP, stop workers, and close the Temporal client.

Temporal client facts for v1.47:

```go
run, err := temporalClient.ExecuteWorkflow(
    ctx,
    client.StartWorkflowOptions{
        ID:        runID,
        TaskQueue: task.DefaultQueue,
    },
    "RunWorkflow",
    input,
)
```

The returned `WorkflowRun` exposes:

```go
GetID()
GetRunID()
Get(ctx, valuePtr)
```

Some older public comments still say `GetWorkflowID`; use `GetID` with v1.47.0.

---

## 6. Dependencies

Recommended direct `go.mod` additions:

```go
require (
    connectrpc.com/connect v1.20.0
    github.com/coder/websocket v1.8.15
    go.temporal.io/sdk v1.47.0
    google.golang.org/protobuf v1.36.12
    gopkg.in/yaml.v3 v3.0.1
)
```

Observed versions on 2026-08-13:

| Module | Version | Use |
|---|---:|---|
| `go.temporal.io/sdk` | `v1.47.0` | workflow, client, worker, testsuite |
| `gopkg.in/yaml.v3` | `v3.0.1` | DSL parsing |
| `github.com/coder/websocket` | `v1.8.15` | WebSocket endpoint |
| `golang.org/x/net` | `v0.58.0` | optional legacy h2c only |
| `sigs.k8s.io/yaml` | `v1.6.0` | alternative parser, not selected |
| `github.com/gorilla/websocket` | `v1.5.3` | alternative, not selected |

Run:

```bash
go mod tidy
```

Do not add `x/net` for h2c on Go 1.25.6 unless the implementation intentionally uses deprecated `h2c.NewHandler`.

---

## 7. Integration test

Use a file with:

```go
//go:build integration
```

Test flow:

1. Check `localhost:7233` with a short `net.DialTimeout`.
2. Call `t.Skip` if Temporal is unreachable.
3. Dial namespace `default`.
4. Start a real worker.
5. Register:
   - `RunWorkflow`
   - `ExecuteTask` under `task.ActivityExecuteTask`
   - event projection activity
6. Compile and apply `examples/hello.ork.yaml`.
7. Submit the workflow.
8. Stream events from sequence zero.
9. Wait for `EVENT_TYPE_RUN_SUCCEEDED`.
10. Assert:
    - run status is `STATUS_SUCCEEDED`
    - exactly three task runs exist
    - all three task runs are `STATUS_SUCCEEDED`
    - event sequences are strictly increasing
    - events were observed

The stub activity should echo the task input and return a small output payload. It must not run real plugins or access MinIO.

Use deadline-based polling or stream completion. Do not use fixed sleeps.

---

## 8. Proof commands

Suggested proof workflow:

```bash
make infra-up

mkdir -p proof/M2

go test ./... -count=1 -v \
  | tee proof/M2/test.log

ORK_PROOF_EVENT_FILE=proof/M2/event-stream.json \
  go test -tags=integration ./... -count=1 -v \
  | tee -a proof/M2/test.log

make lint

go build ./cmd/orkengine
```

The integration test can write the captured event list as a JSON array using `protojson`.

For a server smoke check:

```bash
ORK_HTTP_ADDR=:18080 go run ./cmd/orkengine &
ENGINE_PID=$!

curl -fsS \
  -X POST \
  -H 'Content-Type: application/json' \
  -d '{}' \
  http://127.0.0.1:18080/orkstack.v1.WorkflowService/ListWorkflows

kill "$ENGINE_PID"
```

`proof/M2/README.md` should record:

- Temporal and namespace prerequisites
- exact commands
- default environment values
- how the event JSON was captured
- expected run/task statuses

---

## 9. Unresolved findings

1. **Condition seam conflict**  
   Full condition rendering cannot occur in the engine without violating the Orker-rendering rule or changing the fixed activity contract. M2 can safely test literal and inputs-only conditions; artifact/env conditions need a later seam decision.

2. **No skipped event enum**  
   `Status` supports `STATUS_SKIPPED`, but `EventType` does not have a task-skipped value. Avoid a proto change unless reviewers require a distinct event type.

3. **Attempt field**  
   Temporal retries happen below the workflow future. The fixed result does not report attempts. Test retry behavior with mock counters; do not claim exact persisted attempt values.

4. **Latest workflow semantics**  
   `GetWorkflow(version="")` means latest, but the repository does not define whether latest means most recently applied or highest version. Store the most recently applied version for M2 and document it.

5. **Event resume cursor**  
   The proto does not state whether `from_sequence` is inclusive. The recommended implementation treats it as an exclusive last-seen cursor.

6. **Build target scope**  
   `Makefile:26-30` references missing `cmd/orker` and `cmd/orkctl`. `go build ./cmd/orkengine`, `make lint`, and `go test ./...` can be validated within M2, but `make build` cannot become green without out-of-scope binaries.

7. **Current implementation proof**  
   No M2 behavior, integration, or proof files exist yet. The recommendations above are design findings, not claims that the behavior currently works.

## Contract amendments received

None.