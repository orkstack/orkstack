# activity-shell

Runs a command, a script, or an expression inside the activity container and
reports what happened. It is a reference implementation of the OCI Activity
Contract (`docs/OCI_ACTIVITY_CONTRACT.md`): it uses exactly the public ABI, has
no OrkStack SDK, and reads its whole world out of `/orkstack`. Its
`entry.sh` is the reference for the contract's entry-script rules (§10.1–§10.2).

```yaml
- id: migrate
  uses: shell
  with:
    op: exec
    command: ["/bin/sh", "-c", "echo hello"]
```

## Configuration

The `with:` block is rendered by the Orker and delivered as
`/orkstack/input/config.json`. This image reads four keys.

| Key | Type | Ops | Meaning |
|---|---|---|---|
| `op` | string | — | `exec` (default), `script`, or `eval` |
| `command` | array of strings | `exec`, `eval` | The workload argv. Also passed as `$@` to `script`. |
| `script` | string | `script` | The program text, written to `/orkstack/tmp/script` |
| `interpreter` | string | `script` | Overrides the script's shebang, e.g. `python3` |

### `command` is argv, never a shell string

`command` is a **JSON array**, and each element becomes exactly one argv
element. It is never handed to `sh -c`. A rendered value therefore cannot
become code, whatever it contains (the contract's §10.1 rule 8):

```yaml
command: ["echo", "hello; touch /orkstack/output/artifacts/PWNED"]
```

prints the payload as data and creates no file.

If you genuinely want a shell, ask for one in the document, where the boundary
is visible:

```yaml
command: ["sh", "-c", "grep -c ERROR /orkstack/input/artifacts/build/app.log"]
```

## Operations

### `op: exec`

Runs `command` as argv. **The child's exit status is the activity's exit
status** — there are no magic exit codes (AGENTS.md §3.2). Exit `0` succeeds;
anything else fails the activity, and Temporal's retry policy decides what
happens next.

```json
{"exitCode": 0}
```

### `op: script`

Writes `script` to `/orkstack/tmp/script`, makes it executable, and runs it.
The interpreter is `interpreter` if set, the script's own shebang if it has
one, and `sh` otherwise. Any `command` elements are passed to the script as
positional arguments. The exit status propagates, exactly as `exec`.

```json
{"scriptPath": "/orkstack/tmp/script", "exitCode": 3}
```

### `op: eval`

Runs `command` as argv and captures its stdout as **data**.

**A non-zero child exit is not a failure here.** The evaluation ran, so the
activity succeeded and the child's status is reported in `exitCode` for the
Orkflow to branch on. This is one image's documented contract, not a platform
rule; it is how the platform avoids inventing a magic exit code. `exec` and
`script` propagate the status instead.

```json
{"value": "HELLO-ON-STDOUT", "exitCode": 0}
```

The workload's stdout still reaches the container log stream and the durable
`stdout.log`: `eval` captures it by redirection, never by command
substitution, which would also lose cancellation (the contract's §10.1
rule 2b).

An unknown `op` exits `2` with a message on stderr.

## Output

| Path | Meaning |
|---|---|
| `/orkstack/output/values.json` | Written by this image on every op. Always a JSON object. |
| `/orkstack/output/artifacts/<name>` | Any regular file the workload writes here becomes an output artifact, named exactly as the file. |

The scan of `output/artifacts/` is not recursive, and a directory, symlink,
device, socket or FIFO there fails the activity with
`ContractViolation/ARTIFACT_NOT_A_FILE`. Archive a hierarchy yourself,
e.g. as `workspace.tar.gz`. Names must be valid UTF-8.

## Interface

The image declares its `with:` contract in the
`org.orkstack.activity.interface` label (`docs/ACTIVITY_IMAGES.md`,
"Declaring the interface"), so `orkctl orkflow validate` and apply check a
task's `with:` before anything runs: a missing required key or an unknown
`op` is an error, an undeclared key a warning. Three ops, `exec` when `op`
is absent, and every `values.json` key of Output — `value` is the captured
stdout of `op: eval`. The label, as the Dockerfile carries it:

```dockerfile
LABEL org.orkstack.activity.interface='{"schemaVersion":1,"ops":{"exec":{"fields":{"command":{"type":"array","required":true}}},"script":{"fields":{"script":{"type":"string","required":true},"interpreter":{"type":"string"},"command":{"type":"array"}}},"eval":{"fields":{"command":{"type":"array","required":true}}}},"defaultOp":"exec","values":{"exitCode":{"type":"number"},"scriptPath":{"type":"string"},"value":{"type":"string"}}}'
```

`orkctl activity inspect shell` renders it as a table.

## stdout, stderr, and cancellation

stdout and stderr are captured independently and byte-exact. Plain text is
enough; there is no logging SDK.

Cancellation arrives as an ordinary SIGTERM, is forwarded to the workload, and
the workload gets `limits.cancellationGracePeriodSeconds` to clean up before
SIGKILL. Ordinary Unix signal handling is all that is required of a workload.
The entry script runs the workload in the background under a TERM trap rather
than `exec`ing it, so that both halves of the contract hold: the workload
observes the signal **and** `values.json` still gets written (the contract's
§10.1 rule 2a).

## Environment

The platform sets no variable: everything about the activity — run identity,
image digest, limits, paths, input artifacts, secret paths — is in
`/orkstack/input/task.json`. The image sets none either, and the workload
inherits the task's `env:` unchanged, plus what the container runtime adds
(under Docker, `HOSTNAME`).

## Building

```bash
make -C images image-shell
```
