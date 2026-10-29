# Test an activity image

Check an image against the activity contract before a run uses it. Then run one task of your Orkflow on your own machine.

## Example

Build the image, then run the six checks against it. `./my-image` is a folder with a Dockerfile in it:

```bash
orkctl activity build ./my-image --tag localhost:5001/acme/my-image:dev
orkctl activity test localhost:5001/acme/my-image:dev
```

The test needs a pushed image, because it runs the digest the registry returns. It prints each check as `PASS`, `FAIL` or `SKIP`, and it exits non-zero when a check fails.

`activity test` drives the image with `op: exec` and a `command`, as [activity-shell](/docs/reference/first-party-activities/#shell) does. The image needs `sh` and `jq`, and it must accept that config. An image with its own `with:` keys is refused before any check runs, with the message "cannot be driven". Test such an image with `orkctl task run`, below.

## The six checks

| Check | Fails when |
|---|---|
| The container starts, writes `values.json`, and its stdout reaches the log | It does not start, writes no `values.json`, or its output is missing from the log. |
| `/orkstack/input` is read-only | The image can write to the input tree. |
| `task.json` is complete | A field the contract lists is missing, or the image's digest is not the one that ran. |
| Artifact names round-trip, and a directory is refused | An artifact name changes, or a directory in `output/artifacts/` is accepted. |
| Exit codes are not special | `1`, `42`, `125`, `126` or `127` passes, or `0` fails. |
| SIGTERM reaches the program within the grace period | The program does not see `SIGTERM`, or it does not stop in time. |

A `SKIP` is not a pass. The `--json` output lists each check with its status.

## Run one task locally

`orkctl task run` runs one task of an Orkflow on your machine. It uses the same dispatcher, renderer and workspace as an Orker. It writes nothing to the deployment.

The example Orkflow [`examples/custom-image.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/custom-image.ork.yaml) reads an upstream file. Supply that file's values by hand:

```bash
orkctl task run --file examples/custom-image.ork.yaml --task report \
  --output probe='{"idempotencyKey":"x","attempt":1,"taskId":"probe","argv":[]}'
```

Build and push the example images first, because `--file` is validated by the engine. The engine answers at `--server`, which defaults to `http://localhost:8080`.

| Flag | Meaning |
|---|---|
| `--file` | Run this document instead of the stored one. It is validated first. |
| `--task` | The task to run. Required. |
| `--output TASK=JSON` | Supply an upstream task's `values.json`. Repeatable. |
| `--artifact TASK/NAME=PATH` | Supply an upstream artifact from a local file. Repeatable. |
| `--input key=value` | A run input. Repeatable. |
| `--item N` | Run one item of a fan-out. |
| `--from-run RUN` | Read the upstream values and artifacts from a finished run. |
| `--secret NAME=file:PATH` or `NAME=env:VAR` | Supply a declared secret. |
| `--omit-secret NAME` | Run without a declared secret. |
| `--render-only` | Print the rendered `config.json` and stop before the container starts. |

Add `--keep` to keep the task's files on disk. `orkctl task run` prints the path.

## When a check or a run fails

`orkctl task run` exits with these codes:

| Exit | Meaning |
|---|---|
| `0` | The task succeeded. With `--render-only`, the config rendered. |
| `2` | The task failed. |
| `3` | You interrupted it. |
| `1` | The command could not run the task. This includes problems on your machine. |

The first interrupt stops the container and waits for it. A second one leaves the container running. The next `orkctl task run` on the same machine, with the same work directory and container engine, removes it. It never removes the container of another run that is still going.

A local run is a new activity with its own idempotency key. It never reuses the source run's key. Any external side effect the task performs is real, so take care with a task that writes to a live system.

## Limits

- `activity test` needs a working container runtime.
- `activity test` needs a pushed image, because it tests the digest an Orker would run.
- The Go and shell examples ignore `op`, so the six checks do not describe them. Test them with `orkctl task run`.
- A local run has no commit, no attempt record and no log in the deployment.

## See also

- [OCI Activity Contract, section 10.3](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#103-the-contract-test-suite): the six checks, in full
- [OCI Activity Contract, section 10.4](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#104-running-one-task-locally): running one task locally
- [Activity images and the `op` convention](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#activity-images-and-the-op-convention): the `op` shape the test expects
- [Build an activity image](/docs/how-to/build-an-activity-image/): the image this page tests
