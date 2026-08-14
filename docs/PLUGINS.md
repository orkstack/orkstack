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

## Builtin plugins (v0)

### `http`
Perform an HTTP request.
`with`: `method`, `url`, `headers` (map), `body` (string), `capture_body_as`
(artifact name), `timeout_seconds`.
`output`: `status`, `headers`, `json` (parsed if JSON), `duration_ms`.

### `shell`
Run a command in `WorkDir`.
`with`: `command` (string, templated), `env` (map), `capture_stdout_as`,
`capture_stderr_as`, `capture_paths` (globs → artifacts).
`output`: `exit_code`, `stdout` (truncated), `stderr` (truncated).

### `file`
File manipulation.
`with`: `op` (`write|read|copy|delete`), `path`, `content`, `from`, `to`.
`output`: `path`, `size_bytes`. Produces/consumes artifacts as appropriate.

## Adding a plugin

1. Create `pkg/plugins/<name>/` implementing `plugin.Plugin`.
2. Register it in the Orker's registry (`internal/orker`).
3. Add table-driven behavior tests + a proof trace.
4. Document `with`/`output` here.
