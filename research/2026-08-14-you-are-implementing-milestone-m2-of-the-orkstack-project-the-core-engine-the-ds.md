# M2 follow-up research — HEAD `4a61814`

## Conclusions

Three findings still apply:

1. **DSL template validation is incomplete.** It misses named `define` trees, `$`-rooted `parse.VariableNode` paths, and `with`/`range` `else` branches.
2. **Failed Temporal starts emit no terminal event.** `StartRun` failure marks the run failed but leaves Connect and WebSocket streams open.
3. **`make lint` conflicts with the immutable scope.** It fails on pre-existing `pkg/plugin/plugin_test.go`; fixing it requires an owner-approved scope amendment or a foundation-branch fix.

No new dependency, proto, generated-code, task-contract, Orker, or plugin change is needed.

## Current evidence

Verified at the current checkout:

- `go test ./... -count=1` passes.
- `go test -tags=integration ./... -count=1` passes against Temporal.
- The unreachable-Temporal integration test skips cleanly.
- `go build -o /tmp/orkengine-m2-current ./cmd/orkengine` passes.
- `make lint` fails:

```text
go vet ./...
gofmt needed:
./pkg/plugin/plugin_test.go
make: *** [lint] Error 1
```

- `git diff --exit-code main -- proto gen internal/task pkg internal/orker cmd/orker` passes; those paths remain unchanged.
- The current DSL probe accepted all of these invalid cases:
  - `{{ $.inputs.missing }}`
  - `{{ $.tasks.ghost.output.x }}`
  - an invalid reference inside `{{define "bad"}}...{{end}}`
  - invalid references in `with` and `range` `else` branches

Evidence: `internal/dsl/template.go:62-143`, `internal/api/service.go:210-234`, `Makefile:75-78`, `pkg/plugin/plugin_test.go:9-14`, and the current command results above.

## Finding 1: validate every template tree and path

### Cause

`checkString` parses the template but walks only `tmpl.Tree.Root`:

```text
internal/dsl/template.go:62-73
```

Named definitions are associated templates with their own parse trees. The Go source documents that `Template.Templates()` returns all associated templates, and its implementation iterates a map, so validation should sort by `Template.Name()` before walking.

The Go 1.25.6 parse tree confirms:

- `parse.VariableNode.Ident` contains the variable path, with `$` as the first element for `$.inputs.x`.
- `parse.BranchNode` has separate `Pipe`, `List`, and `ElseList` fields.

Sources:

- `/usr/local/Cellar/go/1.25.6/libexec/src/text/template/template.go:142-154`
- `/usr/local/Cellar/go/1.25.6/libexec/src/text/template/parse/node.go:390-424`
- `/usr/local/Cellar/go/1.25.6/libexec/src/text/template/parse/node.go:843-852`
- Official docs: <https://pkg.go.dev/text/template/parse>

The official `text/template` docs state that `with` and `range` leave dot unchanged in their `else` branch. Current code incorrectly marks both branches as rebound:

```text
internal/dsl/template.go:135-143
```

Sources:

- `/usr/local/Cellar/go/1.25.6/libexec/src/text/template/doc.go:105-119`
- `/usr/local/Cellar/go/1.25.6/libexec/src/text/template/doc.go:144-152`
- Official docs: <https://pkg.go.dev/text/template>

### Minimal fix

Limit changes to `internal/dsl/template.go` and its tests:

1. Iterate every `tmpl.Templates()` result, including named definitions.
2. Sort templates by name for stable diagnostics.
3. Add a `parse.VariableNode` case:
   - For `Ident[0] == "$"`, validate `Ident[1:]` through the existing input/task checks.
   - `$` remains the root even inside a rebound `with`/`range` body.
   - Do not reject ordinary `$name` locals unless declaration tracking is added; that would introduce a new restriction.
4. Split branch context:
   - `if`: list and else retain the incoming rebound state.
   - `with`/`range`: list is rebound; else retains the incoming state.
5. Extend `checkIndex` to recognize `$`-rooted variables. Otherwise valid expressions such as `index $.inputs "url"` will be mistaken for a bare `.inputs` reference.

Add focused regressions in `internal/dsl/compile_test.go` for:

- `$` undeclared input.
- `$` unknown task.
- `$` non-upstream task.
- Invalid reference inside a named `define`.
- Invalid reference in `with` `else`.
- Invalid reference in `range` `else`.
- Valid `$` input/task references.
- Valid `index $.inputs "name"` control case.

These changes preserve the existing YAML parser, helper stubs, raw `with`, and content-hash behavior. The current dependency set is sufficient: `gopkg.in/yaml.v3 v3.0.1`; no new module is needed.

## Finding 2: failed Temporal start must terminate streams

### Cause

`startPreparedRun` currently:

1. Publishes `RUN_CREATED`.
2. Calls `Starter.StartRun`.
3. Saves `STATUS_FAILED` on error.
4. Returns without publishing `RUN_FAILED`.

Location:

```text
internal/api/service.go:210-234
```

The existing stream implementations already terminate correctly when they receive a terminal event:

- Connect returns after `IsTerminalEvent`.
- WebSocket writes the terminal event, then closes normally.
- `EventBus.Subscribe` supports backlog and terminal-cursor handling.

Sources:

- `internal/api/service.go:288-335`
- `internal/api/ws.go:47-75`
- `internal/engine/bus.go:94-129`

### Minimal fix

After saving the failed run, synchronously publish:

- `RunId: runID`
- `Type: EVENT_TYPE_RUN_FAILED`
- `Status: STATUS_FAILED`
- `Message: err.Error()`
- timestamp from `s.now().UTC()`
- dedupe key such as `runID + "#failed"`

The distinct dedupe key matters because `runID + "#created"` already identifies the creation event.

No bus, Connect, WebSocket, proto, or Temporal workflow change is required. The event becomes sequence 2 after `RUN_CREATED`; later Connect and WebSocket subscribers receive both events and close cleanly.

Add API regressions that:

- Force `StartRun` to fail.
- Assert the stored run is `STATUS_FAILED`.
- Read `StreamEvents` from sequence 0 and assert `RUN_CREATED`, then `RUN_FAILED`, followed by clean stream completion.
- Read `/ws/runs/{id}` and assert the same JSON events, then a normal WebSocket close.

## Finding 3: lint and immutable scope conflict

`Makefile:78` runs `gofmt -l` over every Go file except `gen`, including `pkg/plugin/plugin_test.go`.

The only required formatting change is:

```diff
-func (f fakePlugin) Name() string          { return f.name }
-func (f fakePlugin) Describe() Descriptor   { return Descriptor{Name: f.name, Version: "0"} }
+func (f fakePlugin) Name() string         { return f.name }
+func (f fakePlugin) Describe() Descriptor { return Descriptor{Name: f.name, Version: "0"} }
```

But M2 explicitly requires `pkg/*` to remain unchanged. Excluding the file in `Makefile` would weaken the lint gate and violate scope.

This is an immutable contract conflict. It requires one of:

1. An explicit user amendment allowing the format-only `pkg/plugin/plugin_test.go` change.
2. A separate foundation-branch commit formatting that file, followed by rebasing or merging M2.
3. No M2 completion claim until one of the above occurs.

The current request authorizes formal identification of this conflict but does not authorize a `pkg/*` change. Therefore it remains a blocker.

## Existing implementation and dependencies

The rest of M2 is in place and aligned with the fixed seams:

- DSL compiler: `internal/dsl`, YAML v3, `structpb`, SHA-256 semantic canonical JSON hash.
- Temporal orchestration: `internal/engine/workflow.go`, deterministic iteration and `workflow.Now`.
- Event projection: dedicated `PublishEvent` activity and `EventQueue`.
- API: Connect handlers, standard-library HTTP/2 cleartext via `http.Protocols`, coder WebSocket, `protojson`.
- Binary: `cmd/orkengine`.
- Dependencies already present in `go.mod` include:
  - `connectrpc.com/connect v1.20.0`
  - `github.com/coder/websocket v1.8.15`
  - `go.temporal.io/sdk v1.47.0`
  - `gopkg.in/yaml.v3 v3.0.1`
  - `google.golang.org/protobuf v1.36.12`
  - `golang.org/x/net v0.55.0` indirect

The remaining fixes need only the standard library and existing packages.

## Validation and proof refresh

After implementing the two code fixes:

```bash
gofmt -w internal/dsl internal/api
go test ./... -count=1
go test -tags=integration ./... -count=1
go test -race ./cmd/orkengine ./internal/dsl ./internal/engine ./internal/api -count=1
go build -o /tmp/orkengine-m2 ./cmd/orkengine
```

Verify the skip path:

```bash
ORK_TEMPORAL_ADDRESS=127.0.0.1:7999 \
  go test -tags=integration ./internal/engine \
  -run '^TestIntegration_HelloOrkRunSucceedsOnRealTemporal$' -count=1 -v
```

Then regenerate:

```bash
./scripts/m2-proof.sh
```

With the lint conflict unresolved, the script should continue recording evidence but exit nonzero and state the blocker. After a foundation fix or approved scope amendment, it should exit 0.

Refresh:

- `proof/M2/test.log`
- `proof/M2/README.md`
- `proof/M2/event-stream.json` if a new real run is captured
- coverage and smoke logs as needed

Commit only the allowed changes with a Conventional Commit. Do not open a pull request.

## Contract amendments received

None. The latest instruction says the inherited objective and acceptance criteria remain unchanged and confirms that this stage is research-only.