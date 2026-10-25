# Handle a failed run

Add a failure handler to clean up or undo work when a step fails. The run still ends `FAILED`, with its first error.

## Example

In [`examples/order-saga.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/order-saga.ork.yaml), `release` deletes the reservation when `reserve` or a step after it fails. The reservation may exist even if the request itself failed:

```yaml
  - id: release
    uses: http
    needs: [reserve, reserved, charge, charged, ship, shipped]
    join: any_failed
    retry: { maximum_attempts: 5, initial_interval_seconds: 2 }
    with:
      op: delete
      url: '{{ .inputs.api }}/orders/{{ .tasks.prepare.output.result.id }}/reservation'
      timeoutSeconds: 20
```

`join: any_failed` makes `release` a failure handler. It runs only when one of its needs fails. Its `retry:` allows five attempts.

Plan the failing path before you run anything. Apply the block first, because the example composes it:

```bash
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/order-saga.ork.yaml
orkctl run plan order-saga --assume-failed reserve --assert-runs release --assert-skipped refund
```

`--assume-failed` plans the run as if `reserve` had failed.

## Choose the join

| `join:` | The task starts when its needs |
|---|---|
| `all_succeeded` (default) | all succeeded |
| `none_failed` | none failed or was cancelled, and at least one succeeded. Use it to continue after a branch. |
| `any_failed` | at least one failed, after its retries. A failure handler. |
| `all_done` | all finished, however they ended. A failure handler. It also runs in a run that succeeds. |

A task whose join is not met is skipped.

## What a handler does

- A failure handler is the only kind of task that starts in a failing run. Other tasks that have not started are stopped.
- A handler covers only the needs it lists.
- A handler's own failure stops nothing, and it does not replace the run's error.
- An operator's cancel starts no handler. If the run was already failing, the cancel stops its running handlers.
- While the handlers run, the run's status stays `RUNNING`, and its error is already set.

## Read how the needs ended

A handler finds `/orkstack/input/task.json`. Its `join` object lists each need with a `status`: `SUCCEEDED`, `FAILED`, `SKIPPED` or `CANCELED`. A need that failed may also include its error.

```bash
jq '.join' /orkstack/input/task.json
```

A failed need's output files are listed in `inputs.artifacts`, with `producer` set to that need. A need that failed before it wrote a file has no entry.

## Undo work safely

- A handler with no `retry:` makes one attempt. If the Orker running it is lost, the handler fails. Give a compensation a `retry:`, and make it safe to run twice.
- A task that the run stopped while it ran reads `CANCELED`, not `FAILED`. `any_failed` does not start for it. When that matters, use `all_done` and read the status in `task.json`.
- To let a failure pass without failing the run, have the task write the outcome as a value. A later `condition:` reads that value. See [Run a task only when a condition holds](/docs/how-to/conditions/).

## Limits

- A failure handler cannot be pinned, fanned out or cached.
- An ordinary task cannot need a failure handler.
- OrkStack has no `on_failure:` hook and no `continue_on_error`.

## See also

- [DSL reference, “Joins and failure handlers”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#joins-and-failure-handlers-join): every rule, status and message
- [ORKFLOW_PATTERNS, “Undo what was done: failure handlers and sagas”](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md#undo-what-was-done-failure-handlers-and-sagas): the saga pattern this example follows
- [`examples/order-saga.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/order-saga.ork.yaml): the whole saga, with its plan checks
