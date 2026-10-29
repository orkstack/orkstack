# Build an activity image

Package a program as an OCI image, and run it as a task. The image reads files from `/orkstack/input`, writes files to `/orkstack/output`, and exits.

## Example

The program in [`examples/custom-image`](https://github.com/orkstack/orkstack/blob/main/examples/custom-image/) is written in Go, and it has no OrkStack code. The end of its Dockerfile looks like this, with the interface label left out:

```dockerfile
FROM scratch
COPY --from=build /out/report /report
# never root
USER 65532:65532
# exec form: SIGTERM reaches the program itself
ENTRYPOINT ["/report"]
```

A task that runs it, from [`examples/custom-image.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/custom-image.ork.yaml):

```yaml
  - id: probe
    uses: localhost:5001/orkstack-examples/custom-image:dev
    network: none
```

Build, push and apply the example:

```bash
orkctl activity build examples/custom-image --tag localhost:5001/orkstack-examples/custom-image:dev
orkctl activity build examples/custom-image-shell --tag localhost:5001/orkstack-examples/custom-image-shell:dev
orkctl apply examples/custom-image.ork.yaml
orkctl run submit custom-image --watch
```

The example names both images, so build both. `orkctl activity build` pushes each image and prints its pinned reference.

The `probe` task has no `with:` block. The image reads its context, writes `values.json` and exits `0`.

## Read the inputs

The image gets a read-only `/orkstack/input` tree:

| Path | Contents |
|---|---|
| `task.json` | The attempt: the run's idempotency key, the task id and the attempt number. |
| `config.json` | The task's `with:` block after rendering. `{}` when the task has none. |
| `run.json` | The run's input. |
| `upstream/<task>/values.json` | The `values.json` of an earlier task. |
| `artifacts/<task>/<name>` | An artifact of an earlier task. In `with:`, write `{{ artifact "<task>/<name>" }}` to get its path. |

Reject a `task.json` whose `schemaVersion` you do not know. The example image accepts only `1`.

## Write the outputs

- Always write `/orkstack/output/values.json`. It must be a JSON object, and it is limited to 256 KiB by default. Write `{}` when there is no result.
- Write each artifact as a regular file directly in `/orkstack/output/artifacts/`. The file name is the artifact's name. A directory, symbolic link or special file fails the attempt. Archive a directory first, for example as `workspace.tar.gz`.
- Run `umask 022` before you write. Otherwise the Orker, the OrkStack process that runs your container, may not be able to read your files.
- Print plain text. stdout and stderr are kept as separate logs.

## Exit and stop

Exit `0` when the work succeeds. Any other status fails the attempt. There are no special exit codes. Retries come from the task's [`retry:`](/docs/reference/orkflow-keys/#retry) key.

When a task is cancelled, the container gets `SIGTERM`. It has 30 seconds to exit, then it gets `SIGKILL`. Set `cancellation_grace_period_seconds` on the task to change the window. Keep `ENTRYPOINT` in exec form, as the example does, so your program receives `SIGTERM` directly.

## Dockerfile rules

- Run as a non-root user, such as `USER 65532:65532`.
- Put each comment on its own line. A trailing comment on a `USER` line stops every container from starting.
- Do not declare `VOLUME`. It hides the workspace mounts.
- Pin the base image by digest, as the shell example does.
- Write only to `/orkstack/output`, `/orkstack/tmp` and `/tmp`. The root filesystem is read-only. Set `HOME=/orkstack/tmp` if a tool needs a home directory.

## Build and pin

| Flag | Meaning |
|---|---|
| `--tag` | The registry reference to build and push. Required. |
| `--platform` | Target platforms, such as `linux/amd64,linux/arm64`. |
| `--push` | Push after the build. Default `true`. |
| `--interface` | An `interface.ork.yaml` to pack into the image's interface label. |

`orkctl activity build <path>` prints a reference with `@sha256:` in it. Paste that reference into `uses:` if your deployment requires digests. A tag resolves to a digest when you apply.

A local image that was never pushed has no registry digest, so the deployment cannot pin it. Use `--push=false` only to smoke-test an image.

## Declare the interface

Write the `with:` keys your program reads in an `interface.ork.yaml` next to the Dockerfile:

```yaml
apiVersion: orkstack/v1
kind: ActivityInterface
fields:
  src:
    type: string
    required: true
  as:
    type: string
    default: report.txt
```

Build with `orkctl activity build <path> --tag <ref> --interface interface.ork.yaml`. It packs the file into the image's `org.orkstack.activity.interface` label. With another build tool, pass the value `orkctl activity interface pack interface.ork.yaml` prints to its `--label`.

`orkctl apply` then checks each task's `with:` keys against the label before any run starts. The interface is optional. The container never reads it. `orkctl activity inspect <reference>` shows what the image declares.

## When an attempt fails

| Reason | Meaning |
|---|---|
| `VALUES_MISSING` | The program exited `0` and wrote no `values.json`. |
| `VALUES_MALFORMED` | `values.json` is not a JSON object. |
| `VALUES_TOO_LARGE` | `values.json` is over 256 KiB. Write the result as an artifact. |
| `ARTIFACT_NOT_A_FILE` | `output/artifacts/` holds a directory, symbolic link or special file. |
| `ARTIFACT_NAME_NOT_UTF8` | An artifact name is not valid UTF-8. |
| `ARTIFACT_LIMIT_EXCEEDED` | There are too many artifacts, or one is too large. |
| `OUTPUT_NOT_READABLE` | The Orker cannot read a file the program wrote. Run `umask 022`. |
| `CONTAINER_EXIT_NONZERO` | The program exited with a non-zero status. Read its stderr log. |
| `CONTAINER_TIMEOUT` | The task ran past `timeout_seconds`. It is not retried. |

A contract violation is never retried. A non-zero exit is retried only when the task has `retry:` and attempts remain. Every reason code is listed in [Reasons](/docs/reference/reasons/).

## Limits

- The image gets no OrkStack or object-storage credentials. Its only secrets are the ones the task names in `secrets:`.
- A task can run more than once. An attempt can repeat after a crash, so make the work safe to repeat. External systems can use the idempotency key to skip work they already did.
- By default a task can write 256 artifacts, and each can be up to 5 GiB.
- The Orker's policy can cap the `resources:` a task asks for.

## See also

- [Activity images and the `op` convention](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#activity-images-and-the-op-convention): the conventions for images that share one shape
- [Declaring the interface](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#declaring-the-interface): the `with:` label and `interface.ork.yaml`
- [OCI Activity Contract, section 3](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#3-the-orkstack-workspace): the workspace, file by file
- [OCI Activity Contract, section 10.1](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#101-the-dockerfile-and-entry-script-rules): the Dockerfile and entry-script rules
- [Test an activity image](/docs/how-to/test-an-activity-image/): check your image before a run uses it
