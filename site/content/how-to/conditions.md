# Run a task only when a condition holds

Add `condition:` to run a task only when a template is true. Add `join:` to continue after a branch that was skipped.

## Example

In [`examples/decide-and-route.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/decide-and-route.ork.yaml), the `decide` task picks a route. The `canary` task runs only for the `canary` route:

```yaml
  - id: canary
    uses: shell
    needs:
      - decide
    condition: '{{ eq .tasks.decide.output.result.route "canary" }}'
    with:
      op: eval
      command:
        - printf
        - "%s"
        - "canary: 5%, then 50%, then 100% of traffic"
```

Plan the path before anything runs:

```bash
orkctl apply examples/decide-and-route.ork.yaml
orkctl run plan decide-and-route --output decide='{"result":{"route":"canary","notify":true}}' --assert-runs canary --assert-skipped direct
```

`orkctl run plan` reports which tasks would run, and which would be skipped and why. `--assert-runs` and `--assert-skipped` exit with status 4 when the plan disagrees with them.

## Write the template

- The engine evaluates a condition once, before the task starts. It can read `.inputs` and `.tasks.<id>.output`.
- A condition can use Go template functions such as `eq`, `ne`, `and`, `or`, `not` and `index`. It cannot use the `artifact`, `env` or `sh` helpers.
- Compute a value in a task. A condition only reads and compares.

## Know what is false

A condition is false when its text, trimmed and lowercased, is one of these:

`""` (empty), `false`, `0`, `no`, `off`, `null`, `<no value>`, `[]`, `map[]`, `<nil>`, `unspecified`.

Anything else is true. So compare an enum with `eq`, as the example does. Do not render free text on its own. `No thanks` is true, and `no` is false.

## Skip and re-join

A task whose condition is false is `SKIPPED`. A task that needs a skipped task is skipped too, unless its `join:` allows it.

Use `join: none_failed` to run after whichever branch ran. The same example re-joins its three branches like this:

```yaml
  - id: report
    uses: json
    needs:
      - canary
      - direct
      - hold
    join: none_failed
    network: none
    with:
      op: query
      src: '{{ if index .tasks "canary" }}upstream/canary/values.json{{ else if index .tasks "direct" }}upstream/direct/values.json{{ else }}upstream/hold/values.json{{ end }}'
      filter: '{plan: .value}'
```

A skipped task is absent from `.tasks`. Reading its output fails the task. The `index` guard picks the branch that ran, so `report` never reads the skipped one.

## Limits

- A condition reads only `.inputs` and upstream outputs. It cannot read a file.
- A value that a condition needs must be an output of an earlier task. Plan with `--output TASK=JSON` to supply it. `orkctl run plan` names every condition that read an output you did not supply.

## See also

- [DSL reference, “Templating”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#templating): the context, the helpers and the false words
- [DSL reference, “Joins and failure handlers”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#joins-and-failure-handlers-join): how a skipped need is handled
- [ORKFLOW_PATTERNS, “Decide in a task, branch on its value”](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md#decide-in-a-task-branch-on-its-value): the pattern this example shows
- [ORKFLOW_PATTERNS, “Branch and re-join”](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md#branch-and-re-join): the re-join pattern
- [Handle a failed run](/docs/how-to/handle-failure/): the other join words
