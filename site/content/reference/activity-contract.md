# Activity contract

What a container receives, what it must write, and what its exit status means. An image that follows this contract runs as an activity. It needs no OrkStack SDK and no custom worker.

## Example

An excerpt from the shell example, [`examples/custom-image-shell/entrypoint.sh`](https://github.com/orkstack/orkstack/blob/main/examples/custom-image-shell/entrypoint.sh). It sets the paths the contract defines:

```sh
set -u
umask 022

IMAGE=custom-image-shell
WORKSPACE=/orkstack
TASK=$WORKSPACE/input/task.json
CONFIG=$WORKSPACE/input/config.json
VALUES=$WORKSPACE/output/values.json
ARTIFACTS=$WORKSPACE/output/artifacts
MAX_NAME_BYTES=128
```

The whole image is a POSIX shell script that uses `jq`. It has no SDK.

## Files the container sees

The working directory is `/orkstack`, and the container reads every input file.

| Path | Access | Contents |
|---|---|---|
| `/orkstack/input/task.json` | read-only | The activity context: the task, the run, and the idempotency key. |
| `/orkstack/input/config.json` | read-only | The task's rendered `with:` block. `{}` when it sets none. |
| `/orkstack/input/run.json` | read-only | The run's input. |
| `/orkstack/input/upstream/<task>/values.json` | read-only | An upstream task's `values.json`. A fan-out has one directory per item, numbered from `00000`. |
| `/orkstack/input/artifacts/<task>/<name>` | read-only | An upstream task's artifact. |
| `/orkstack/input/secrets/<name>` | read-only | A secret, as its exact bytes. |
| `/orkstack/output/values.json` | read-write | The result. Required, and one JSON object. |
| `/orkstack/output/artifacts/<name>` | read-write | One file per output artifact. Regular files only, directly in the directory. |
| `/orkstack/tmp` | read-write | Scratch space. |
| `/orkstack/shared` | read-write | Only for a task of a pin. Every task of the pin sees it. |

`/tmp` inside the container is a separate tmpfs, 64 MiB by default.

## Idempotency key

`task.json` has an idempotency key of the form `{namespace}/{runId}/{taskId}`. It stays the same across the retries of one activity, and a fan-out item adds its index. Pass it to any external system that must deduplicate.

## Exit status and result

| What the container does | What happens |
|---|---|
| Exits `0`, and `values.json` is one JSON object | The task succeeds, and its outputs are committed. |
| Exits `0`, with no `values.json` | The task fails with `VALUES_MISSING`. It is not retried. |
| Exits `0`, with `values.json` that is not an object, or is over 256 KiB | The task fails with `VALUES_MALFORMED` or `VALUES_TOO_LARGE`. It is not retried. |
| Exits with any non-zero status | The task fails with `CONTAINER_EXIT_NONZERO`. It is retried under the task's `retry:`. |
| Runs past `timeout_seconds` | The task fails with `CONTAINER_TIMEOUT`. It is not retried. |

There is no table of special exit codes: every non-zero status fails the attempt the same way, and the task's `retry:` decides whether it runs again.

## Failure categories

| Category | Retried by default | Example reason |
|---|---|---|
| `ActivityFailed` | Per the task's `retry:` | `CONTAINER_EXIT_NONZERO` |
| `InfrastructureError` | Per the task's `retry:`, except host misconfigurations | `IMAGE_PULL_FAILED` |
| `ContractViolation` | Never | `VALUES_MISSING` |
| `TimedOut` | Never, by default | `CONTAINER_TIMEOUT` |
| `Cancelled` | Never automatically | `TASK_CANCELED` |

## Output streams

stdout and stderr are captured separately, byte for byte. Plain text works, with no logging library. Each stream is kept for the attempt, up to 16 MiB. Read it with `orkctl run logs`, or follow it live with `-f`.

## Cancellation

When a run cancels a task, the container receives `SIGTERM`. After the grace period, it receives `SIGKILL`. The grace period is `cancellation_grace_period_seconds`, 30 seconds by default. Handle `SIGTERM` to clean up. No other cancellation interface exists.

## Environment

The container gets the image's own environment, with the task's `env:` on top. OrkStack adds no `ORK_*` variable and no credential.

## Security

Each container has a read-only root filesystem, no Linux capabilities, `no-new-privileges`, the default seccomp profile, a process limit, and CPU and memory limits that are never unlimited. It never receives the Docker socket, a host path other than its own workspace, host namespaces, or privileged mode. It receives no Temporal, object-storage or OrkStack credential unless you pass one as a workload secret.

## See also

- [OCI_ACTIVITY_CONTRACT.md, “3. The `/orkstack` workspace”](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#3-the-orkstack-workspace): the layout, and the rules for each file
- [OCI_ACTIVITY_CONTRACT.md, “8. Failure categories and error handling”](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#8-failure-categories-and-error-handling): every reason code
- [OCI_ACTIVITY_CONTRACT.md, “9. Security”](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#9-security): what each container is denied
- [ACTIVITY_IMAGES.md](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md): the first-party images, and writing your own
- Next: [Build an activity image](/docs/how-to/build-an-activity-image/), [Limits](/docs/reference/limits/)
