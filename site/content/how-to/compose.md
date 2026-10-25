# Run one Orkflow inside another

Add a task with `uses: orkflow:<name>@<version>` to start a child run of that Orkflow and wait for it. Its declared outputs become the task's values.

## Example

In [`examples/composed-release.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/composed-release.ork.yaml), the `release` task runs the `release-block` Orkflow at version `0.1.0`. `announce` reads two of the block's outputs:

```yaml
  - id: release
    uses: orkflow:release-block@0.1.0
    needs: [build]
    with:
      image: '{{ .tasks.build.output.value }}'

  - id: announce
    uses: shell
    needs: [release]
    with:
      op: exec
      command: ["sh", "-c", "echo released {{ sh .tasks.release.output.digest }} at {{ sh .tasks.release.output.url }}"]
```

The block declares those two outputs in [`examples/release-block.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/release-block.ork.yaml):

```yaml
outputs:
  digest: '{{ .tasks.sign.output.value }}'
  url: '{{ .tasks.publish.output.value }}'
```

Apply the block first, in the same namespace:

```bash
orkctl apply examples/release-block.ork.yaml
orkctl apply examples/composed-release.ork.yaml
orkctl run submit composed-release --watch
orkctl run get <run-id> --tree
```

## Pin the version

- The version is required. `orkflow:<name>` alone is refused.
- The child is resolved at apply, in the same namespace. Composition never crosses a namespace.
- A later promotion of the child does not change a parent that is already running.

## Pass input and read outputs

- `with:` is the child's input. The engine renders it. A child run receives values, never files. Pass a reference that the child can fetch, such as a digest or a URL.
- Apply checks `with:` against the child's `inputs:`.
- The child's `outputs:` become the task's values. Read them as `{{ .tasks.<id>.output.<name> }}`. Every value is a string.
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
