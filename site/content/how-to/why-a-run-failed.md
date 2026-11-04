# Find why a run failed

Go from a failed run to its cause in a few steps: the run's error, the failed try, its stderr, and what changed since a run that worked. Then fix the cause and run again.

<!-- site-media: ui-failure -->

## 1. Read the run's error

In the UI, a failed run shows a banner with **View stderr**. The side cards **Why it failed** and **How it failed** explain the failure on the same page.

In the terminal, `run get` shows the run's own error as `code: message`, and each task's final outcome:

```bash
orkctl run get <run-id>
```

## 2. Look up the reason

Every failure has a reason code and a message. The code is the part to match on. Common ones:

| Code | What happened | What to check |
|---|---|---|
| `CONTAINER_EXIT_NONZERO` | The container exited with a non-zero status. | Read the try's stderr. The status alone does not say what failed. It is retried under the task's `retry:`. |
| `CONTAINER_TIMEOUT` | The container ran past its `timeout_seconds`. | Find the hang, or raise `timeout_seconds` if the work is slow. It is not retried. |
| `VALUES_MISSING` | The container exited `0` but wrote no `values.json`. | Write a JSON object to `/orkstack/output/values.json`. A task with no result writes `{}`. |
| `IMAGE_PULL_FAILED` | The Orker could not pull the pinned image in time. | Check the registry and the Orker host's network. |
| `PIN_HOST_LOST` | The host of a pin stopped while the pin was open. | Run again. A pin never moves to another host. |

The [error reasons](/docs/reference/reasons/) list every code. Each one says whether it is retried.

## 3. Read every try

A retry hides the earlier tries in the task's final outcome. List them:

```bash
orkctl run attempts <run-id> --task convert       # every try, with its error and exit status
orkctl run logs <run-id> --task convert --attempt 2
```

In the UI, the attempt timeline shows each failed try's own error. Choose a try to open its log.

## 4. Find what changed

Compare the failed run with the last run that worked:

```bash
orkctl run diff <good-run-id> <failed-run-id>
orkctl run diff <good-run-id> <failed-run-id> --only status,image,error,orker
```

The diff covers the definition, the input, and each task's status, attempt, pool, Orker, exit code, error, duration and image. A value that does not apply shows as `-`. Evidence that is gone shows as `unavailable`, with the reason under the table.

In the UI, **Compare** on the run page opens the newest other succeeded run of the same Orkflow.

## 5. Check whether it is a pattern

```bash
orkctl stats failures --since 24h                       # grouped by Orkflow, task, code and category
orkctl stats failures --since 6h --group-by task,code,image,phase
```

`stats failures` groups the window's failed runs, worst first. A group whose evidence is gone says `unavailable`.

## 6. Fix it and run again

A run again uses the version it pinned. To run the fix, apply the Orkflow again, then submit it:

```bash
orkctl apply my-orkflow.ork.yaml
orkctl run submit my-orkflow --watch
```

To check one task on this machine, render it with the failed run's recorded inputs. This starts no container:

```bash
orkctl task run --from-run <run-id> --task convert --render-only
```

Drop `--render-only` to run the task here. It writes nothing to the deployment, but any external side effect it has is real.

`orkctl task run` needs Docker, so run it with the installed binary, not the `orkctl` alias. See [Run it locally](/docs/get-started/run-locally/#install-orkctl-on-your-machine).

## See also

- [docs/TROUBLESHOOTING.md, "Reading a failed run"](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#reading-a-failed-run): the four steps in full
- [docs/TROUBLESHOOTING.md, "Failures"](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#failures): one entry per failure code, with its cause and fix
- [docs/TROUBLESHOOTING.md, "Reproducing a failed task on this machine"](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#reproducing-a-failed-task-on-this-machine): the local reproduction commands
- [docs/REASONS.md, "Container, image and workspace"](https://github.com/orkstack/orkstack/blob/main/docs/REASONS.md#container-image-and-workspace): every reason in this group, and whether it is retried
- [docs/STATS.md, "6. Failure fingerprints"](https://github.com/orkstack/orkstack/blob/main/docs/STATS.md#6-failure-fingerprints): how failures are grouped
- [docs/CLI.md, "orkctl run diff"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-run-diff): every diff flag
