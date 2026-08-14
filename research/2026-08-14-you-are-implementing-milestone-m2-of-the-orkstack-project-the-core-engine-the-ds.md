# M2 follow-up research — `aa63feb`

## Conclusion

Seven review findings still apply. The existing DSL, deterministic workflow, Connect API, WebSocket bridge, mocked tests, and proof structure are otherwise sound.

The minimal fixes are:

1. Set `LocalActivityWorkerOnly: true` on the shared-queue engine worker.
2. Keep the EventBus mutex held through subscriber enqueue.
3. Reserve idempotency keys before starting Temporal.
4. Make subscriptions report when the requested cursor already passed a terminal event.
5. Merge declared input defaults in both API submission and workflow state.
6. Extend template validation to unknown roots, `index`, and `ChainNode` references.
7. Resolve the `pkg/plugin/plugin_test.go` scope conflict before claiming DONE.

No proto, generated code, `internal/task/contract.go`, Orker, plugin, or artifact changes are needed.

## Contract amendments received

None. The inherited objective and acceptance criteria remain unchanged.

## Findings that still apply

### 1. Shared worker can steal Orker activities — P1

`cmd/orkengine/main.go:84-89` creates a default worker on `task.DefaultQueue`. Temporal task queues do not route by registered type. The worker therefore polls both workflow and regular activity tasks, despite registering no `ExecuteTask` activity.

The review recorded a real failure where the engine consumed `ExecuteTask` and returned `ActivityNotRegisteredError`.

**Fix:**

```go
worker.Options{
    DisableRegistrationAliasing: true,
    LocalActivityWorkerOnly:     true,
}
```

Temporal v1.47 documents `LocalActivityWorkerOnly` as allowing workflow and local-activity handling while disabling regular activity execution. The dedicated event worker should also use `DisableWorkflowWorker: true`.

Add a production-shaped integration setup:

- workflow-only engine worker on `task.DefaultQueue`;
- activity-only stub/Orker worker on `task.DefaultQueue`;
- activity-only projection worker on `engine.EventQueue`.

### 2. EventBus delivery can reorder sequences — P2

`internal/engine/bus.go:62-70` assigns and stores a sequence under `b.mu`, unlocks, then enqueues to subscribers. Concurrent publishers can enqueue sequence 2 before sequence 1.

The review reproduced inversions among thousands of events.

**Fix:**

Keep `b.mu` held while calling each subscriber’s non-blocking `enqueue`. `enqueue` only appends to an internal queue; it does not block on the client-facing channel. This serializes sequence assignment and per-subscriber enqueue order.

Add a concurrent regression test with one subscriber and many publishers asserting delivery is exactly `1..N`.

### 3. Idempotency keys are not reserved atomically — P2

`internal/api/service.go:162-171` checks the map, unlocks, starts the run, and records the key only afterward. Concurrent requests all pass the check.

The review observed 32 starts and 32 run IDs for one key.

**Fix:**

Add a dedicated idempotency mutex. For a keyed request:

1. lock;
2. return the existing stored run if present;
3. allocate the run ID;
4. save the pending run;
5. reserve `idempotencyMap[key]`;
6. unlock;
7. publish `RUN_CREATED`;
8. call `StartRun`.

Do not remove the reservation if Temporal start fails; the key must still deduplicate to the recorded failed run unless the contract later defines retry semantics.

Add a barrier test with many simultaneous submissions and assert one starter call, one run ID, and one stored run.

### 4. Resumed streams hang after the terminal cursor — P2

`internal/api/service.go:275-291` and `internal/api/ws.go:47-71` only stop after receiving a terminal event. If `from_sequence` already includes the terminal event, the backlog is empty and no future event arrives.

The review reproduced Connect and WebSocket timeouts against a completed run.

**Fix:**

Have `EventBus.Subscribe` return a third value such as `terminalAlreadyReached`, computed under the same bus lock as backlog capture and subscription registration.

- If the terminal event’s sequence is greater than `from_sequence`, stream backlog/live events normally.
- If its sequence is less than or equal to `from_sequence`, return cleanly from Connect.
- For WebSocket, send a normal WebSocket close frame and return.

Add both terminal-cursor tests:

- Connect receives zero messages and ends with no stream error.
- WebSocket read ends with `StatusNormalClosure`.

### 5. Declared input defaults are not passed to tasks — P2

`internal/api/service.go:173-191` and `internal/engine/workflow.go:295-304` pass only the submitted payload. `examples/hello.ork.yaml` declares a default for `inputs.url`, but an omitted input produces an empty `RunInput`.

The fixed activity seam has no workflow schema, so the Orker cannot recover this default.

**Fix:**

Add an internal helper, preferably in `internal/engine`, that:

- clones the caller payload;
- reads `Workflow.Inputs`;
- adds each declaration’s `default` only when the caller did not provide that field;
- preserves caller values, including explicit `null`;
- leaves undeclared fields unchanged.

Use it:

- in `SubmitRun` before saving the run and calling `StartRun`;
- in `newRunState` before building `RunInput`, covering direct workflow starts and preventing double-merge problems.

Keep `Task.With` raw and continue passing upstream output/artifact references unchanged.

Add tests for:

- omitted `url` receives the default;
- caller-provided `url` wins;
- the integration stub observes the default in `ExecuteTaskInput.RunInput`.

### 6. Template validation misses indirect references — P2

`internal/dsl/template.go:112-158` checks direct `FieldNode`s but does not validate:

- unknown roots such as `.notAContextValue`;
- bare `.inputs` and `.tasks`;
- `index .inputs "missing"`;
- `index .tasks "non-upstream"`;
- `(index .tasks "a").output`.

Go’s parser represents these as:

- `FieldNode`;
- `CommandNode` with an `IdentifierNode` named `index`;
- `ChainNode` containing a nested `PipeNode`.

**Fix:**

Extend the AST walker:

- reject roots other than `inputs`, `tasks`, or permitted relative `.` values;
- require a named input after `.inputs`;
- validate direct `.tasks.<id>` references against the transitive `needs` closure;
- recurse through `ChainNode.Node`;
- special-case `index` and validate literal string keys against declared inputs or upstream task IDs;
- retain the existing `artifact` and `env` helper stubs before `template.Parse`;
- preserve relative-field behavior inside `with` and `range`.

Add compile tests for the invalid cases above and valid indexed references. For dynamic index keys, validate the base root but do not invent a new rejection rule when no concrete key exists.

### 7. `pkg/plugin/plugin_test.go` violates the immutable scope — P3

`git diff main...HEAD` shows a two-line gofmt-only edit in `pkg/plugin/plugin_test.go`.

The scope contract says `pkg/*` must remain unchanged. However, the baseline file is not gofmt-compliant, and `Makefile:78` checks it. The current branch passes lint only because the file was reformatted.

This is a real contract conflict:

- reverting the file honors scope but makes `make lint` fail;
- keeping it makes lint pass but violates the scope lock;
- changing `Makefile` to exclude it is also outside the stated M2 scope.

This cannot be silently resolved by implementation. Obtain an explicit user/owner amendment, or fix the baseline in a separate foundation change. Do not claim all criteria pass while hiding this conflict.

## Proven design to preserve

### DSL

The current implementation already matches the fixed DSL contract:

- `yaml.v3 v3.0.1`;
- exact `source_yaml` preservation;
- semantic SHA-256 content hash for unpinned versions;
- JSON encoding gives stable map-key ordering;
- pinned versions remain verbatim;
- `inputs` and `with` become `structpb.Struct`;
- retry, timeout, queue, DAG, plugin, slug, and template checks are present.

Do not enable strict unknown-field rejection: the six documented rules do not require it, and current behavior accepts unspecified YAML fields.

### Temporal workflow

`internal/engine/workflow.go` is already replay-safe:

- `workflow.Now`;
- activities for event/read-model side effects;
- raw `With`;
- upstream output/artifact references;
- `StartToCloseTimeout`;
- mapped Temporal retry policy;
- queue override/default;
- all ready activities scheduled before fixed-order future collection.

The current fixed-order future collection is deterministic and already proves fan-out. No selector redesign is required.

### Event projection

The existing `PublishEvent` activity on `engine.EventQueue` is the correct boundary:

- workflow code stays free of direct I/O;
- the activity updates `RunStore` and publishes to `EventBus`;
- dedupe keys protect activity retries;
- the API and WebSocket share the same bus.

Do not replace this with workflow-side channels, queries, or direct store access.

### Connect, h2c, WebSocket

The current choices are correct:

- generated `NewServerStreamHandler`;
- `connect.ServerStream[Event]`;
- Go 1.25 `http.Server.Protocols`;
- `SetHTTP1(true)` and `SetUnencryptedHTTP2(true)`;
- `coder/websocket`;
- `protojson` for WebSocket text frames;
- empty `ListPlugins` response is valid for M2.

`golang.org/x/net/http2/h2c` is not needed directly. The current `x/net v0.55.0` entry is indirect through Temporal; the h2c package is deprecated in newer x/net releases.

## Dependency state

Observed with `go list -m`:

```text
connectrpc.com/connect       v1.20.0
github.com/coder/websocket   v1.8.15
go.temporal.io/sdk           v1.47.0
google.golang.org/protobuf   v1.36.12
gopkg.in/yaml.v3             v3.0.1
github.com/stretchr/testify  v1.11.1
```

The integration test directly imports `go.temporal.io/api`; `go mod tidy -diff` currently recommends moving `go.temporal.io/api v1.63.4` from indirect to direct. Run `go mod tidy` after implementation and review that shared `go.mod` change.

## Validation and proof

Add focused tests for:

```text
internal/dsl:
  unknown root, bare roots, index input/task, chained index

internal/engine:
  concurrent EventBus ordering
  input-default propagation into ExecuteTaskInput.RunInput

internal/api:
  concurrent idempotency
  Connect terminal-cursor completion
  WebSocket terminal-cursor completion

cmd/orkengine:
  workflow worker uses LocalActivityWorkerOnly
```

Use the real Temporal test with split workers and no stale pre-fix `orkengine` process:

```bash
make lint
go test ./... -count=1 -v
go test -race ./internal/dsl ./internal/engine ./internal/api
go test -tags=integration ./... -count=1 -v
go build -o /tmp/orkengine ./cmd/orkengine
```

For proof regeneration:

```bash
make infra-up
./scripts/m2-proof.sh
```

The integration test must retain its clean skip path when `localhost:7233` is unreachable. Refresh:

- `proof/M2/test.log`;
- `proof/M2/event-stream.json`;
- `proof/M2/README.md`.

The current proof contains a valid earlier 12-event successful run, but its log does not prove the current checkout because the review reproduced the worker collision afterward.

## Evidence audited

- Current HEAD: `aa63feb`.
- Current default `go test ./... -count=1`: passed.
- Review artifact: integration failure from `ActivityNotRegisteredError`, concurrent event inversions, concurrent idempotency failure, terminal stream hangs, missing defaults, and template gaps.
- Current source confirms all seven defect locations.
- Current `go.mod` and module versions were checked with `go list -m`.
- Baseline `pkg/plugin/plugin_test.go` was checked with `gofmt -d`; reverting it causes the existing lint rule to fail.
- Current h2c test proves negotiated HTTP/2 over plaintext.
- Current proof event JSON contains ordered sequences 1–12 and a successful three-task run.

## Official upstream references

- Temporal Go SDK worker options: <https://pkg.go.dev/go.temporal.io/sdk/worker#Options>
- Temporal activity execution: <https://docs.temporal.io/develop/go/activities/execution>
- Temporal activity timeouts/retries: <https://docs.temporal.io/develop/go/activities/timeouts>
- Connect Go server streaming: <https://pkg.go.dev/connectrpc.com/connect#NewServerStreamHandler>
- Connect deployment and h2c: <https://connectrpc.com/docs/go/deployment/>
- Go `text/template`: <https://pkg.go.dev/text/template>
- Protobuf JSON: <https://pkg.go.dev/google.golang.org/protobuf/encoding/protojson>
- coder WebSocket API: <https://pkg.go.dev/github.com/coder/websocket>
- Deprecated x/net h2c package: <https://pkg.go.dev/golang.org/x/net/http2/h2c>