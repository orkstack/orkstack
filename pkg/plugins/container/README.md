# `container` plugin

Run a task inside a Docker container: mounted inputs, expanded environment,
captured output streams, an enforced time budget, and nothing left behind.

The plugin drives the **Docker CLI**; there is no Kubernetes path and no
container runtime API dependency. The Orker's host needs a reachable daemon.

## Ops

`op` is the generic operation selector every OrkStack plugin shares
(see [docs/PLUGIN_DESIGN.md](../../../docs/PLUGIN_DESIGN.md)).

| `op`   | meaning                                                             |
|--------|---------------------------------------------------------------------|
| `run`  | The container **is the work**. A non-zero exit fails the task.       |
| `pull` | Only fetch the image, so a later task starts without the download.   |

- Default: `run`.
- An unknown `op` fails the task **permanently**, before the daemon is touched:
  `container: unknown op "frobnicate" (want run or pull)`.

## `with`

| field               | applies to | description                                                        |
|---------------------|------------|--------------------------------------------------------------------|
| `op`                | all        | operation selector (above)                                          |
| `image`             | all        | image reference, e.g. `alpine:3.20` (**required**)                  |
| `command`           | `run`      | string run by the image's `sh -c`, or a list used verbatim as argv  |
| `workdir`           | `run`      | working directory inside the container                              |
| `env`               | `run`      | map of environment variables                                        |
| `mounts`            | `run`      | list of bind mounts (below)                                         |
| `pull`              | `run`      | `missing` (default), `always`, `never`                              |
| `capture_stdout_as` | `run`      | artifact name for the full stdout stream                            |
| `capture_stderr_as` | `run`      | artifact name for the full stderr stream                            |
| `timeout_seconds`   | `run`      | wall-clock budget; an over-running container is killed              |

### `command`

A **list** is the container's argv, used exactly as written — no shell, no
splitting, no quoting rules:

```yaml
command: ["/bin/echo", "one argument with spaces"]
```

A **string** is a shell one-liner handed to the image's own `sh -c`. The plugin
never splits it itself, because splitting is where quoting bugs live:

```yaml
command: |
  set -eu
  make build && make test
```

Omit `command` to run the image's own entrypoint.

### `env`

Values are rendered twice, by two layers that do different jobs:

1. the engine's template layer resolves `{{ .inputs.x }}` and
   `{{ .tasks.y.output.z }}`, like every other `with` value;
2. the plugin then expands `$VAR` and `${VAR}` **between the task's own
   variables**.

```yaml
env:
  RELEASE: "{{ .inputs.release }}"     # -> v1.4.2
  ARCHIVE: "orkstack-${RELEASE}.tar.gz" # -> orkstack-v1.4.2.tar.gz
  PRICE: "cost: $$5"                    # -> cost: $5
```

Every reference reads the *declared* value, never an already-expanded one, so
the result never depends on map order and a cycle is impossible. An undeclared
name expands to empty, as a shell would; `$$` is a literal `$`.

### `mounts`

Each entry has a container `path` plus exactly one source:

```yaml
mounts:
  - artifact: "fetch/body"   # an upstream artifact
    path: /input/body.json   # always read-only
  - ephemeral: scratch       # a fresh directory, created before the run
    path: /scratch           # and removed after it
```

- **`artifact`** takes the same `"<task>/<name>"` reference as the `artifact`
  template helper. The Orker materializes it, and the plugin mounts it
  **read-only** — an upstream artifact is a shared, immutable input, and
  `read_only: false` on one is rejected rather than honored.
- **`ephemeral`** is scratch space for one attempt: created before the run,
  removed after it, on success and failure alike. Its name is a plain
  `[A-Za-z0-9._-]` token; the directory lives inside the task workdir.
- `path` must be absolute, clean, not `/`, and free of `:` and `,`. Two mounts
  may not target the same path.

Mounts are passed as `--mount type=bind`, never `--volume`. When the daemon
cannot see the host path, `--volume` silently mounts an empty directory and the
task reads nothing; `--mount` refuses to start. A task that cannot see its input
must say so rather than succeed on emptiness.

### `pull`

| value     | behavior                                                            |
|-----------|---------------------------------------------------------------------|
| `missing` | Use the local image; fetch it only if absent. **Default.**          |
| `always`  | Fetch before every run, so a moving tag is always current.          |
| `never`   | Local only. A missing image fails the task permanently.             |

`output.image_pulled` reports whether this run fetched the image.

### `timeout_seconds`

The container's own budget, separate from the task's Temporal timeout. On
expiry the plugin asks the **daemon** to SIGKILL the container — killing the CLI
would leave the container running — and the task fails with the resulting exit
code (`137`). Only if the daemon does not answer within the kill grace does the
plugin drop the CLI as well.

Set the task's `timeout_seconds` higher than the plugin's, so the container's
own budget is what expires first and the failure names the container.

## `output`

| field          | description                                                    |
|----------------|----------------------------------------------------------------|
| `exit_code`    | the container's exit status                                    |
| `image`        | the image that ran                                             |
| `image_pulled` | whether this run fetched the image                             |
| `container`    | the container name, for correlating with `docker` on the host  |
| `timed_out`    | whether the budget expired                                     |
| `duration_ms`  | wall-clock run duration                                        |

**No `stdout`, no `stderr`.** An output payload is a Temporal payload — the
activity result, the workflow's state, the run's stored task output — and a
task's output is bytes, which travel as artifacts and never inline
(AGENTS.md §1.2). Every field above is something only the plugin knows.

What the container printed has two readers, and both are complete where a
truncated string never was:

| you want | read |
|---|---|
| to watch it, or read it afterwards | `orkctl run logs <run-id> --task <id>` (add `-f` to follow) |
| a downstream task to consume it | name it with `capture_stdout_as` / `capture_stderr_as`, then `{{ artifact "<task>/<name>" }}` |

A downstream task that used to interpolate `{{ .tasks.build.output.stdout }}`
mounts or reads the capture artifact instead —
[`examples/container-example.ork.yaml`](../../../examples/container-example.ork.yaml)
is the worked example. There is no shim: nothing was deployed, and the field is
simply gone.

## Live output

The application's stdout and stderr reach the task's **live log stream** while
the container runs — through `StreamTaskLogs`, `orkctl run logs -f` and the UI
log pane — not only as capture artifacts after it exits:

```sh
orkctl run logs <run-id> --task build -f
```

Three writers see each stream, and none of them replaces another: the capture
file (byte-exact), the live log, and a small in-memory head the plugin reads to
recognize one daemon-level failure and never quotes.

- **Lines, not reads.** Bytes are framed into whole lines by
  `plugin.LineWriter`. A line split across two docker reads arrives once; a
  final line with no newline is flushed when the container exits, including
  when it exits non-zero; `\n` and a bare `\r` both end a line and `\r\n`
  counts once, so a progress redraw streams instead of piling up.
- **stdout and stderr stay apart.** One attempt has one log stream, so stderr
  lines carry the in-band marker `[stderr] ` and stdout lines are bare. The
  plugin's own lifecycle lines (`container: running …`) are bare too. A stdout
  line that would IMITATE the marker — one that itself starts with `[stderr] `
  or `[stdout] ` — is emitted behind `[stdout] `, so the two streams are never
  confusable and `plugin.DecodeStreamLine` recovers the origin and the original
  line. Ordinary stdout is untouched.
- **A chatty container is bounded.** The framer holds under 64 KiB per stream;
  the Orker uploads a chunk when 64 KiB has accumulated AND 500 ms has passed
  since the last upload, and in any case within 500 ms
  (`ORK_ORKER_LOG_FLUSH_BYTES`, `ORK_ORKER_LOG_FLUSH_INTERVAL`) — the two
  bounds together cap the write rate at one object per interval however small
  the size bound is set — and stops at
  16 MiB per attempt (`ORK_ORKER_LOG_MAX_BYTES`) with a visible
  `[ork] log truncated…` notice. Backpressure is real: a blocked upload blocks
  the writer, which fills the pipe, which blocks the container on `write(2)`.
  **The cap never truncates an artifact** — those are files.

No log byte enters a Temporal payload or the run's event stream (AGENTS.md
§1.4); the whole rule is in
[docs/PLUGINS.md](../../../docs/PLUGINS.md#streaming-a-tasks-output-pluginlinewriter).

## Failure and retry

A failure is retried only when retrying could plausibly change the answer.

| situation                                  | outcome                       |
|--------------------------------------------|-------------------------------|
| container exits non-zero                   | permanent, with the exit code |
| container exceeds `timeout_seconds`        | permanent, exit code `137`    |
| entrypoint missing or not executable (`126`/`127`) | permanent             |
| daemon could not start the container (`125`) | retryable                   |
| image pull failed                          | retryable                     |
| `pull: never` and the image is absent      | permanent                     |
| unknown `op`, bad mount, malformed `env`   | permanent, before any side effect |
| daemon cannot see a mounted host path      | permanent, naming `ORK_ORKER_WORKDIR` |

**A failure quotes nothing the container wrote.** A success carries an 8 KiB
head of each stream on its output payload; a failure carries no payload at all,
and its error names the image, the exit code and where the output is:

```
container: alpine:3.20 exited with code 42. What it printed is in the task's
log (`orkctl run logs <run-id> --task <task-id>`; stderr lines carry the
`[stderr] ` marker), and byte-exact in its capture_stdout_as /
capture_stderr_as artifacts when the task names them.
```

The error is the one thing here that crosses into Temporal: it is recorded as
the activity's failure and copied onto `TaskRun.Error`, `TASK_FAILED`,
`Run.Error` and `RUN_FAILED`. Quoting output into it would put a task's stream
into run history and onto the event stream, which is what AGENTS.md §1.4
forbids — and the daemon's own diagnostics are on the container's stderr, so
they are in the log with everything else.

## Cleanup

Every run releases what it created, on success, failure, timeout and
cancellation:

- the container runs with `--rm` and is force-removed afterwards regardless;
- every ephemeral mount is removed and the removal is logged;
- every container carries `--label ork.task=<task-id>`, so a stray is traceable:
  `docker ps -a --filter label=ork.task=build`.

## Host requirements

- A reachable Docker daemon. `ORK_DOCKER_BIN` overrides which CLI is driven
  (default: `docker` from `PATH`); a missing CLI fails the task permanently.
- **The Orker's work directory must be bind-mountable by the daemon.** It holds
  both the materialized artifacts and the ephemeral mounts. On Linux this is
  automatic. When the daemon lives in a VM (colima, Lima, Rancher Desktop,
  Docker Desktop) it sees only the paths that VM shares, and it reports an
  unshared path as `bind source path does not exist` — which reads like a
  missing file and sends you hunting in the wrong place. The path is there; the
  VM simply has no window onto it.

  Neither macOS temp location is shared: not `/var/folders/...`, which is the
  Orker's own default, and not `/tmp`. What these VMs do share is the user's
  home. `make up` therefore sets `ORK_ORKER_WORKDIR` to `.data/work` inside the
  repository and probes the daemon before starting the Orker, so a bad path is
  a warning at startup instead of a failed task later. Running the Orker by
  hand, set it yourself:

  ```sh
  export ORK_ORKER_WORKDIR="$HOME/.ork/work"     # any path the VM shares
  colima ssh -- mount | grep virtiofs            # colima: what it shares
  ```

## Security

The plugin builds an argv slice and never a shell command line, so no config
value can inject a flag or a second command (AGENTS.md §9). An `image` starting
with `-` is rejected because Docker would read it as an option, and an `env`
name containing `=` or whitespace is rejected because it would silently redefine
a different variable. The one shell involved is the container's own, when
`command` is a string — inside the container's isolation boundary.

## Example

```yaml
- id: build
  plugin: container
  needs: [seed]
  timeout_seconds: 180
  with:
    op: run
    image: "alpine:3.20"
    pull: missing
    workdir: /work
    timeout_seconds: 60
    env:
      RELEASE: "{{ .inputs.release }}"
      ARCHIVE: "orkstack-${RELEASE}.tar.gz"
    mounts:
      - artifact: "seed/manifest"
        path: /input/manifest.txt
      - ephemeral: scratch
        path: /work
    command: |
      set -eu
      cat /input/manifest.txt
      cp /input/manifest.txt /work/manifest.copy.txt
    capture_stdout_as: build-log
    capture_stderr_as: build-diagnostics
```

Runnable: [`examples/container-example.ork.yaml`](../../../examples/container-example.ork.yaml).

## Tests

```bash
go test ./pkg/plugins/container/                      # stand-in Docker CLI, no daemon
go test -tags=integration ./pkg/plugins/container/    # a real daemon
```

The unit tests drive a stand-in CLI that records every invocation argv-by-argv,
so the exact `docker run` command is asserted rather than assumed. The
integration tests use `alpine:3.20` (override with `ORK_TEST_CONTAINER_IMAGE`),
skip cleanly without a daemon, and put their work directories somewhere the
daemon can bind-mount (override with `ORK_TEST_CONTAINER_TMPDIR`).
