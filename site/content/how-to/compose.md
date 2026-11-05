# Run one Orkflow inside another

Add a task with `uses: orkflow:<name>@<version>` to start a child run of that Orkflow and wait for it. Its declared outputs become the task's values.

## Example

In [`examples/approval-gate.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/approval-gate.ork.yaml), the `announce` task runs the `notify-block` Orkflow at version `0.1.0`:

```yaml
  - id: announce
    uses: orkflow:notify-block@0.1.0
    needs:
      - assess
      - apply
    join: none_failed
    with:
      url: '{{ .inputs.notify_url }}'
      subject: change
      text: '{{ if index .tasks "apply" }}applied{{ else }}declined{{ end }}: {{ .tasks.assess.output.result.summary }}{{ with index .tasks "approval" }} (approver: {{ .output.approver }}){{ end }}'
```

The block declares one output in [`examples/notify-block.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/notify-block.ork.yaml):

```yaml
outputs:
  status: '{{ .tasks.post.output.status }}'
```

A task that needs `announce` reads it as `{{ .tasks.announce.output.status }}`. No task in this example does.

Apply the block first, in the same namespace. Give `notify_url` an address that takes a POST:

```bash
orkctl apply examples/notify-block.ork.yaml
orkctl apply examples/approval-gate.ork.yaml
orkctl run submit approval-gate --input notify_url=https://hooks.example.com/change --watch
orkctl run get <run-id> --tree
```

The default change needs no approval, so the run applies it and `announce` starts the child.

## Pin the version

- The version is required. `orkflow:<name>` alone is refused.
- The child is resolved at apply, in the same namespace. Composition never crosses a namespace.
- A later promotion of the child does not change a parent that is already running.

## Pass input and read outputs

- `with:` is the child's input. The engine renders it. A child run receives values, never files. Pass a reference that the child can fetch, such as a digest or a URL.
- Apply checks `with:` against the child's `inputs:`.
- The child's `outputs:` become the task's values. Read them as `{{ .tasks.<id>.output.<name> }}`, or `{{ index .tasks "<id>" "output" "<name>" }}` when the id has a hyphen. Every value is a string.
- A child with no `outputs:` gives `{}`. Reading an output the child does not declare fails at apply.

## What a composed task allows

A composed task takes only `id`, `needs`, `condition`, `join`, `uses` and `with`. The child's own tasks set the pool, pin, retries, timeouts, cache and fan-out.

## When a child fails

| Code | Category | Result |
|---|---|---|
| `CHILD_RUN_FAILED` | the child's own category | The composed task fails. The message names the child run. |
| `CHILD_RUN_CANCELED` | Cancelled | The composed task ends `CANCELED`. |

A composed task is never retried. Its child's tasks have their own retries. Cancelling a child cancels the parent. Cancelling the parent cancels its running children, and waits for them to end.

## Find the child run

- `orkctl run get <run-id> --tree` prints every child run under its parent.
- A child has its own run ID, logs and events. Read it with `orkctl run get <child-run-id>`.
- `orkctl orkflow callers <name>` lists what in the namespace starts or composes the Orkflow.
- A child's awaits send their tokens to nobody. Resolve them with `orkctl run resolve <child-run-id> --task <task>`.

## Limits

- A run can have at most four levels of child runs below it by default. An operator can lower the limit with `ORK_COMPOSE_MAX_DEPTH`.
- Admission is decided once, at the parent's submit. A hold or the namespace's running-run limit does not refuse a child later.
- Children count toward the namespace's `max_running`.

## See also

- [DSL reference, “Composition”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#composition-uses-orkflow): every rule and error code
- [ORKFLOW_PATTERNS, “Reuse a block with declared outputs”](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md#reuse-a-block-with-declared-outputs): the pattern this example shows
- [Versions](/docs/concepts/versions/): how a pinned version is chosen and promoted
