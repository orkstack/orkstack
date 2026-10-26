# Pass files between tasks

A task writes files, which OrkStack stores as artifacts. The next task reads them by name.

## Example

In [`examples/poll-until-ready.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/poll-until-ready.ork.yaml), `fetch` saves the response body as the artifact `export.json`. `summarize` reads that artifact:

```yaml
  - id: fetch
    uses: http
    needs: [wait-ready]
    retry: { maximum_attempts: 3, initial_interval_seconds: 2 }
    with:
      op: get
      url: '{{ .inputs.export_url }}'
      captureBodyAs: export.json
      timeoutSeconds: 60

  - id: summarize
    uses: json
    needs: [fetch]
    network: none
    env:
      TZ: Europe/Berlin
    with:
      op: query
      src: '{{ artifact "fetch/export.json" }}'
      filter: |
        {
          rows: (.rows | length),
          total: ([.rows[].amount] | add),
          generated_local: (.generated_at | fromdateiso8601 | strflocaltime("%Y-%m-%d %H:%M"))
        }
```

List the artifacts of one task:

```bash
orkctl run artifacts <run-id> --task fetch
```

`captureBodyAs` saves the exact response bytes. `artifact "fetch/export.json"` gives `summarize` the path to that file.

## Write a file

- Any regular file directly in `/orkstack/output/artifacts/` becomes one artifact, named as the file.
- The scan does not look in subfolders.
- A directory, symlink, device, socket or named pipe there fails the task. Archive a folder into one file first, for example `workspace.tar.gz`.
- Write the small result to `/orkstack/output/values.json`. It must be one JSON object. A task with no result writes `{}`.

## Read a file

| Path in the task | What it holds |
|---|---|
| `/orkstack/input/artifacts/<task>/<name>` | An artifact of an earlier task. Read-only. |
| `/orkstack/input/upstream/<task>/values.json` | An earlier task's `values.json`. Read-only. |

- In `with:`, `{{ artifact "<task>/<name>" }}` gives the path. The helper does not work in `condition:`.
- A task reads only the tasks in its `needs:`, directly or through them. Any other task name fails validation.
- OrkStack checks each file against its recorded SHA-256 before the task starts.
- A fan-out's items are one folder per item, in item order. See [Fan out over a list](/docs/how-to/fan-out/).

## Limits

- `values.json` holds at most 256 KiB. A larger result belongs in an artifact.
- A task writes at most 256 artifacts, and each one is at most 5 GiB. An operator changes these with `ORK_ARTIFACT_MAX_COUNT` and `ORK_ARTIFACT_MAX_BYTES`.
- File names must be valid UTF-8.

## When a file fails

| Code | Cause | Retried under `retry:` |
|---|---|---|
| `VALUES_MISSING` | The task exited 0 and wrote no `values.json`. | no |
| `VALUES_MALFORMED` | `values.json` is not one JSON object. | no |
| `VALUES_TOO_LARGE` | `values.json` is over 256 KiB. | no |
| `ARTIFACT_NOT_A_FILE` | A directory, symlink, device, socket or named pipe is in `output/artifacts/`. | no |
| `ARTIFACT_LIMIT_EXCEEDED` | Too many artifacts, or one is too large. | no |
| `OUTPUT_NOT_READABLE` | The Orker cannot open a file the task wrote. Write files with `umask 022`. | no |
| `INPUT_ARTIFACT_UNAVAILABLE` | The store did not return an upstream file. | yes |
| `INPUT_ARTIFACT_CORRUPT` | The bytes do not match the recorded SHA-256. | no |

## See also

- [OCI Activity Contract, §3.1 to §3.5](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#31-layout): the file layout, the outputs and the limits
- [DSL reference, “Templating”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#templating): the template context
- [Activity contract reference](/docs/reference/activity-contract/): paths, files, exit codes and limits
