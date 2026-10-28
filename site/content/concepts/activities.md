# Activities

Learn what a task's image receives, what it must write, and what OrkStack does with its exit status and its logs. An activity is any OCI image that reads files, writes files and exits.

```yaml
  - id: report-shell
    uses: localhost:5001/orkstack-examples/custom-image-shell:dev
    needs:
      - probe
    network: none
    command:
      - "{{ .inputs.label }}"
      - is-not-rendered
    with:
      label: '{{ .inputs.label }}'
      src: upstream/probe/values.json
      as: report-shell.txt
```

The `uses:` line names the image, and that is all the activity needs. The image needs no SDK, no `op` and no catalog name.

## What the container receives

OrkStack writes the inputs as files under `/orkstack/input/`. The container only reads them.

| Path | What it holds |
|---|---|
| `config.json` | The task's `with:` block, rendered. |
| `task.json` | The task's context: its ID, its `idempotencyKey`, its attempt number, and its item when it is part of a fan-out. |
| `upstream/<task>/values.json` | The result of each earlier task it depends on. |
| `artifacts/<task>/` | The artifacts of those tasks. |
| `secrets/` | The secret files the task declared. |

## What the container writes

Results go under `/orkstack/output`.

| Path | Rule |
|---|---|
| `values.json` | Required. A JSON object, at most 256 KiB by default. A workload with no result writes `{}`. |
| `artifacts/` | One regular file per artifact. A directory here fails the attempt, so archive a tree into one file first. |
| stdout and stderr | Two logs, kept apart. Plain text works, and no logging SDK is needed. |

Anything larger than the value limit is an artifact, not a value.

## Exit status and cancellation

Exit `0` means success. Any other status means failure. OrkStack reads no special exit codes: retries follow the task's `retry:` policy.

When you cancel a run, OrkStack sends `SIGTERM` to the container. It waits 30 seconds by default, then sends `SIGKILL`. A task can change that wait with `cancellation_grace_period_seconds`. An image that needs to clean up should handle `SIGTERM`.

## What the container cannot reach

The container gets no OrkStack, Temporal or storage credentials, and the platform adds no `ORK_` environment variables. Its environment is the image's own, plus the task's `env:`, plus what the container runtime adds, such as the hostname. Its root filesystem is read-only.

Values that must stay secret belong in `secrets:`, which the Orker resolves and mounts as files. See [Security](/docs/concepts/security/) for the full boundary.

## See also

- [docs/OCI_ACTIVITY_CONTRACT.md, The one-paragraph version](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#0-the-one-paragraph-version): the contract in one paragraph.
- [docs/OCI_ACTIVITY_CONTRACT.md, The /orkstack workspace](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#3-the-orkstack-workspace): every path, its mode, and its limits.
- [docs/ACTIVITY_IMAGES.md](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md): the conventions the first-party images share.
