# `shell` plugin

Run a command with `sh -c` in the task work directory and capture its output.

## Ops

`op` is the generic operation selector every OrkStack plugin shares
(see [docs/PLUGIN_DESIGN.md](../../../docs/PLUGIN_DESIGN.md)). For `shell` it
decides what a non-zero exit **means**.

| `op`   | meaning                                                                    |
|--------|----------------------------------------------------------------------------|
| `run`  | The command is the work. A non-zero exit fails the task permanently.       |
| `eval` | The command is a question. Any exit succeeds; the answer is `output.value` (stdout, trimmed) and the caller branches on `output.exit_code`. |

- Default: `run`, which is exactly the behavior of an orkflow written before
  `op` existed.
- An unknown `op` fails the task **permanently, without running the command**:
  `shell: unknown op "exec" (want run or eval)`.
- Both ops accept the same arguments and capture the same artifacts. `eval`
  captures its artifacts even when the command exits non-zero.

Use `eval` for probes and measurements whose failure is information (is this
tool installed? how many rows matched?) and `run` for the work itself.

## `with`

| field               | applies to | description                                               |
|---------------------|------------|-----------------------------------------------------------|
| `op`                | all        | operation selector (above)                                |
| `command`           | all        | command run by `sh -c` in the workdir (**required**)      |
| `env`               | all        | extra environment variables                               |
| `capture_stdout_as` | all        | artifact name for the full stdout stream                  |
| `capture_stderr_as` | all        | artifact name for the full stderr stream                  |
| `capture_paths`     | all        | globs, relative to the workdir, captured as artifacts     |

The command does **not** inherit the Orker's environment. It sees `PATH`, plus
`HOME`/`PWD`/`TMPDIR` pointing at its own workdir, plus whatever `env` declares.
`capture_paths` patterns may not escape the workdir.

## `output`

| field       | op        | description                                          |
|-------------|-----------|------------------------------------------------------|
| `exit_code` | `run`, `eval` | process exit code                                |
| `stdout`    | `run`, `eval` | stdout, truncated to `MaxCapturedOutput` (8 KiB) |
| `stderr`    | `run`, `eval` | stderr, truncated to `MaxCapturedOutput` (8 KiB) |
| `value`     | `eval`    | stdout with surrounding whitespace trimmed           |

The payload strings are truncated on purpose; the full streams are available
through `capture_stdout_as` / `capture_stderr_as`.

## Example

```yaml
- id: measure
  plugin: shell
  with:
    op: eval
    command: wc -l < data.csv | tr -d ' '

- id: build
  plugin: shell
  needs: [measure]
  with:
    op: run
    command: ./build.sh
    capture_stdout_as: build-log
```

Runnable: [`examples/shell-eval.ork.yaml`](../../../examples/shell-eval.ork.yaml).

## Safety

`shell` executes arbitrary commands and runs only inside an Orker's sandbox
boundary. Command strings are rendered by the DSL templating layer; never
interpolate untrusted input without it.
