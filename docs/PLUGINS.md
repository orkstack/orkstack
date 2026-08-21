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
    Log        func(format string, args ...any)  // streams to the task's LOG
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

## Streaming a task's output (`plugin.LineWriter`)

`ExecContext.Log` is the task's **live log stream**: numbered chunks in the
artifact store, readable while the task runs through `StreamTaskLogs`,
`orkctl run logs -f` and the UI log pane. It is never the run's event stream
and never a Temporal payload — task output is bytes, and bytes travel as
stored objects (AGENTS.md §1.4).

A plugin that runs a child process gives each of its streams a
`plugin.NewLineWriter(log, prefix)` in the `io.MultiWriter` beside its capture
file, so the live log is a third reader of the same bytes and the artifact is
untouched:

```go
stdoutLive := plugin.NewLineWriter(ec.Log, "")                    // bare
stderrLive := plugin.NewLineWriter(ec.Log, plugin.StderrMarker)   // "[stderr] "
cmd.Stdout = io.MultiWriter(stdoutFile, stdoutHead, stdoutLive)
cmd.Stderr = io.MultiWriter(stderrFile, stderrHead, stderrLive)
defer stdoutLive.Close()  // flushes a final line with no terminator
defer stderrLive.Close()
```

**Framing.** `\n` ends a line; so does a bare `\r`, and `\r\n` counts once.
Bare CR is a terminator on purpose: a redrawing tool (`docker pull`, `pip`,
`curl -#`) writes a whole session with no `\n`, and treating it as one line
would deliver 64 KiB blobs minutes late. A line split across reads is joined
and emitted exactly once; a run of 64 KiB with no terminator is emitted as a
piece; `Close` flushes the last unterminated line. Writes never fail, so a log
can never fail a task.

**One stream, two file descriptors.** A task attempt has ONE log stream — one
logger, one sink, one dense sequence — which is what makes a resume cursor a
single number and the ordering total and gap-free. stdout and stderr are told
apart IN BAND: stderr lines carry `[stderr] `, stdout lines are bare, and so
are the plugin's own lifecycle lines. Both readers show the marker verbatim
today: `orkctl run logs` prints the stored line, and so does the UI pane
(`ui/src/lib/components/TaskLogPane.svelte`), which colours a line only by
whether the Orker wrote it. A pane that recognizes the marker, tints the line
and strips it is M11.4's work; the CLI keeps the verbatim marker either way.

**The tag is unambiguous, because the writer escapes what would imitate it.**
A stdout line that itself begins with `[stderr] ` (or with `[stdout] `) is
emitted behind `[stdout] `, and nothing else is:

| stream | what the application wrote | what the log carries |
|---|---|---|
| stdout | `hello` | `hello` |
| stdout | `[stderr] hello` | `[stdout] [stderr] hello` |
| stderr | `hello` | `[stderr] hello` |
| stderr | `[stderr] hello` | `[stderr] [stderr] hello` |

`plugin.DecodeStreamLine` is the inverse and the one place the rule lives, so
every reader gets the same answer: strip a leading `[stderr] ` and the line is
stderr, strip a leading `[stdout] ` and it is stdout, and anything else is
stdout as it always was. Without the escape, a container printing
`[stderr] same text` on stdout while another wrote `same text` on stderr
produced two identical log lines, and no reader could tell their origins apart.

**A failure's error carries no output.** A plugin that fails a task returns an
error, and that error is not a log line: Temporal records it, the workflow
copies it onto `TaskRun.Error` and `TASK_FAILED`, and the run's failure copies
it again onto `Run.Error` and `RUN_FAILED`. So a plugin's failure names what
only it knows — the exit code, the kill, the daemon-level cause — and points at
the log stream and the capture artifacts, which hold every byte (AGENTS.md
§1.4). The container plugin is the worked example (`runFailure`).

**Bounds.** Nothing here is per-line or per-byte to the store, and nothing
grows without limit:

| bound | value | knob | what happens at it |
|---|---|---|---|
| line framing buffer | < 64 KiB per stream | — | a longer run is emitted as pieces |
| flush size | 64 KiB | `ORK_ORKER_LOG_FLUSH_BYTES` | the buffer is uploaded as one chunk |
| flush interval | 500 ms | `ORK_ORKER_LOG_FLUSH_INTERVAL` | a slow task is still visible promptly, and no two uploads are closer together than this |
| one stored line | 1 MiB | — | split across chunks |
| one attempt | 16 MiB | `ORK_ORKER_LOG_MAX_BYTES` | a final `[ork] log truncated…` chunk, then nothing |

The two bounds hold TOGETHER, which is what makes the second number a rate:
a size-triggered flush waits until the interval has passed since the last
upload, so the store is written at most twice a second however small
`ORK_ORKER_LOG_FLUSH_BYTES` is set. Below the default the size bound buys
smaller chunks, never more of them. (Memory outranks the pace: past 64 KiB
buffered the chunk goes out whatever the clock says.)

A chatty task therefore costs a few hundred uploads rather than a million. The
flush runs under the sink's lock, so a writer BLOCKS while a chunk uploads; the
block reaches the process's pipe and the process blocks on `write(2)`. That
chain is the memory bound. **Capture artifacts are files and ignore the log
cap** — a truncated log never means a truncated artifact.

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
Run a command or a script in `WorkDir`.
`with`: `op` (`exec|eval|script`, default `exec`), `command` (string, templated,
for `exec`/`eval`), `script` + `interpreter` (for `script`), `env` (map),
`capture_stdout_as`, `capture_stderr_as`, `capture_paths` (globs → artifacts).
`output`: `exit_code`, `stdout` (truncated), `stderr` (truncated), `value`
(`eval` only), `script_path` (`script` only). `exec` and `script` fail the task
on a non-zero exit; `eval` reports it as data.

### `container` — [reference](../pkg/plugins/container/README.md)
Run a task inside a Docker container.
`with`: `op` (`run|pull`, default `run`), `image` (required), `command` (string
run by the image's `sh -c`, or a list used as argv), `workdir`, `env` (map),
`mounts` (list of `artifact:`/`ephemeral:` binds), `pull`
(`missing|always|never`), `capture_stdout_as`, `capture_stderr_as`,
`timeout_seconds`.
`output`: `exit_code`, `image`, `image_pulled`, `container`, `timed_out`,
`duration_ms` — and **no `stdout` or `stderr`**. A non-zero exit fails the task.

An output payload is a Temporal payload, and a task's output is bytes, which
travel as artifacts (AGENTS.md §1.2). The container's own output has two
readers instead, both of them whole where a truncated string never was: the
live log stream, and the capture artifacts named by `capture_stdout_as` /
`capture_stderr_as`. A downstream task reads the second with
`{{ artifact "<task>/<name>" }}`.

The application's stdout and stderr **stream live** while the container runs,
under the rules above: framed lines, stderr marked `[stderr] `, the capture
artifacts still byte-exact. So `orkctl run logs <run> --task <id> -f` follows a
long-lived container instead of waiting for it.

A failing container publishes its captures too. The task fails with the
plugin's error, and the artifacts it produced are uploaded before that error is
returned — the output of a failing build is the output somebody opens the run
to read.

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
