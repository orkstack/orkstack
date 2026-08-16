# OrkStack Plugins

A plugin is a self-contained task implementation. It takes an input payload plus
input artifacts, performs work, and produces an output payload plus output
artifacts. This is the same contract a containerized task would honor, so
builtin Go plugins and future container plugins are interchangeable.

## The contract (`pkg/plugin`)

```go
type Plugin interface {
    // Name is the DSL `plugin:` identifier (e.g. "http").
    Name() string
    // Describe returns metadata + JSON schemas for input/output (for discovery).
    Describe() Descriptor
    // Execute runs the task.
    Execute(ctx context.Context, task *ExecContext) (*Result, error)
}

type ExecContext struct {
    Config     map[string]any      // rendered `with:` config
    Inputs     Payload             // run/task input payload
    Artifacts  ArtifactResolver    // resolve upstream artifacts to local paths
    WorkDir    string              // scratch directory, cleaned up after
    Log        func(format string, args ...any)  // streams to the event bus
}

type Result struct {
    Output    Payload             // JSON-compatible output data
    Artifacts []ProducedArtifact  // files to upload, keyed by logical name
}
```

Rules:
- Plugins must be **idempotent-friendly**: Temporal may retry `ExecuteTask`.
  Avoid non-repeatable side effects, or guard them.
- Never write outside `WorkDir` unless the task explicitly targets a path.
- Large data leaves as artifacts, never inline in `Output`.
- Return a structured error; set retryable where appropriate.

## The `op` convention

Every plugin selects its operation with one generic field, `op`, declared as an
`enum` in `Describe()` and validated before any side effect; an unknown `op`
fails the task permanently. The rules, the rationale and every builtin op live
in [PLUGIN_DESIGN.md](PLUGIN_DESIGN.md).

## Builtin plugins (v0)

### `http` — [reference](../pkg/plugins/http/README.md)
Perform an HTTP request.
`with`: `op` (`get|post|put|patch|delete|head`, default `get`), `method` (the
pre-`op` spelling, any verb), `url`, `headers` (map), `body` (string),
`capture_body_as` (artifact name), `timeout_seconds`.
`output`: `status`, `headers`, `json` (parsed if JSON), `duration_ms`.

### `shell` — [reference](../pkg/plugins/shell/README.md)
Run a command in `WorkDir`.
`with`: `op` (`run|eval`, default `run`), `command` (string, templated), `env`
(map), `capture_stdout_as`, `capture_stderr_as`, `capture_paths` (globs →
artifacts).
`output`: `exit_code`, `stdout` (truncated), `stderr` (truncated), `value`
(`eval` only). `run` fails the task on a non-zero exit; `eval` reports it as
data.

### `file` — [reference](../pkg/plugins/file/README.md)
File manipulation.
`with`: `op` (`write|read|copy|delete`, required), `path`, `content`, `from`,
`to`, `capture_as`.
`output`: `path`, `size_bytes`. Produces/consumes artifacts as appropriate.

## Adding a plugin

1. Create `pkg/plugins/<name>/` implementing `plugin.Plugin`.
2. Give it an `op` field that follows [PLUGIN_DESIGN.md](PLUGIN_DESIGN.md).
3. Register it in the Orker's registry (`internal/orker`).
4. Add table-driven behavior tests (including the unknown-op case) + a proof
   trace.
5. Document `op`/`with`/`output` in `pkg/plugins/<name>/README.md` and here.
