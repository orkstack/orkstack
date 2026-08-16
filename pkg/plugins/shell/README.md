# `shell` plugin

Run a command or a script in the task work directory and capture its output.

## Ops

`op` is the generic operation selector every OrkStack plugin shares
(see [docs/PLUGIN_DESIGN.md](../../../docs/PLUGIN_DESIGN.md)). For `shell` it
decides **how the body runs** and **what a non-zero exit means**.

| `op`     | body       | meaning                                                              |
|----------|------------|----------------------------------------------------------------------|
| `exec`   | `command`  | The command is the work: `sh -c command`. A non-zero exit fails the task permanently. |
| `eval`   | `command`  | The command is a question: `sh -c command`. Any exit succeeds; the answer is `output.value` (stdout, trimmed) and the caller branches on `output.exit_code`. |
| `script` | `script`   | A program is the work. The body is written to a file in the workdir and executed by `interpreter` (default `sh`) or by its own `#!` line. A non-zero exit fails the task permanently. |

- Default: `exec`.
- An unknown `op` fails the task **permanently, without running anything**:
  `shell: unknown op "frobnicate" (want exec, eval or script)`.
- All three ops capture the same artifacts. `eval` captures them even when the
  command exits non-zero.

Use `eval` for probes and measurements whose failure is information (is this
tool installed? how many rows matched?), `exec` for a one-liner that is the
work, and `script` when the work is a multi-line program: it keeps its own
`$0`, needs no `;`/`&&` chaining, and may pick its interpreter with a shebang.

## `with`

| field               | applies to      | description                                                    |
|---------------------|-----------------|----------------------------------------------------------------|
| `op`                | all             | operation selector (above)                                     |
| `command`           | `exec`, `eval`  | one-liner run by `sh -c` in the workdir (**required**)         |
| `script`            | `script`        | program body, written to `.ork-shell/script` and run (**required**) |
| `interpreter`       | `script`        | program that runs the body; default `sh`, or the body's `#!` line |
| `env`               | all             | extra environment variables                                    |
| `capture_stdout_as` | all             | artifact name for the full stdout stream                       |
| `capture_stderr_as` | all             | artifact name for the full stderr stream                       |
| `capture_paths`     | all             | globs, relative to the workdir, captured as artifacts          |

The command does **not** inherit the Orker's environment. It sees `PATH`, plus
`HOME`/`PWD`/`TMPDIR` pointing at its own workdir, plus whatever `env` declares.
`capture_paths` patterns may not escape the workdir.

## `output`

| field         | op                        | description                                      |
|---------------|---------------------------|--------------------------------------------------|
| `exit_code`   | all                       | process exit code                                |
| `stdout`      | all                       | stdout, truncated to `MaxCapturedOutput` (8 KiB) |
| `stderr`      | all                       | stderr, truncated to `MaxCapturedOutput` (8 KiB) |
| `value`       | `eval`                    | stdout with surrounding whitespace trimmed       |
| `script_path` | `script`                  | workdir-relative path of the written script      |

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
    op: exec
    command: ./build.sh
    capture_stdout_as: build-log

- id: package
  plugin: shell
  needs: [build]
  with:
    op: script
    script: |
      set -eu
      mkdir -p dist
      printf 'rows=%s\n' "{{ .tasks.measure.output.value }}" > dist/manifest.txt
    capture_paths: ["dist/*"]
```

Runnable: [`examples/shell-eval.ork.yaml`](../../../examples/shell-eval.ork.yaml).

## Safety

`shell` executes arbitrary commands and runs only inside an Orker's sandbox
boundary. Command and script strings are rendered by the DSL templating layer;
never interpolate untrusted input without it.
