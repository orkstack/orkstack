# Fan out over a list

Run one task once for every element of an array. Each element becomes its
own **item**, with its own attempts, logs and artifacts.

## Example

In [`examples/each-fan-out.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/each-fan-out.ork.yaml),
`list` writes four parts, and `convert` runs once per part:

```yaml
  - id: convert
    uses: shell
    needs:
      - list
    each:
      over: '{{ .tasks.list.output.result }}'
      as: part
      max_items: 50
      max_parallel: 2
    retry:
      maximum_attempts: 2
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        # `sh` quotes the item's value for the shell (AGENTS.md §6).
        printf '%s\n' {{ sh .each.part.name }} > /orkstack/output/artifacts/part.txt
        echo converted {{ sh .each.part.name }} "({{ .each.index }} of {{ .each.count }})"
```

Run the whole file:

```bash
orkctl apply examples/each-fan-out.ork.yaml
orkctl run submit each-fan-out --watch
```

`convert` runs four items, two at a time. The run page shows each one:

<!-- site-media: ui-fanout -->

## The `each:` keys

| Key | Required | What it does |
|---|---|---|
| `over` | yes | The array, as one template: `{{ .tasks.<id>.output.<key> }}` (the task must be in `needs:`) or `{{ .inputs.<name> }}`. |
| `as` | no | The element's name under `.each`. Default `item`. |
| `max_items` | yes | The most items this task may run: 1 to 10,000. More elements fail the task before any item starts. |
| `max_parallel` | no | How many items run at once. Default: no limit of the task's own. |

## Use the item

In `with:`, each item sees:

| Template | Value |
|---|---|
| `{{ .each.part }}` | the element, under the name `as:` gives it; `{{ .each.part.name }}` reads a key |
| `{{ .each.index }}` | the item's position, from 0 |
| `{{ .each.count }}` | how many items there are |

In a shell script, write `{{ sh .each.part.name }}` so the value arrives
quoted.

## Read every item's results

A task that lists the fan-out in its own `needs:` gets each item's files, in
item order:

```text
/orkstack/input/upstream/convert/00000/values.json
/orkstack/input/artifacts/convert/00000/part.txt
/orkstack/input/artifacts/convert/00001/part.txt
…
```

## Look at one item

```bash
orkctl run get <run-id> --expand convert          # a row per item
orkctl run logs <run-id> --task convert --item 2  # one item's log
```

The run page shows the items as they run, and opens one with a click:

<!-- site-media: ui-fanout-run -->

## When an item fails

An item retries on its own, under the task's `retry:`. If it still fails, the
task and the run fail with that item's error: items still running are
cancelled, and items not yet started never run.

## Limits

- An Orkflow declares at most 1,000 items in total by default, the sum of
  every `max_items`. An operator changes that with `ORK_EACH_MAX_ITEMS`.
- A fan-out cannot be pinned or nested, and it has no per-item condition:
  filter the array in the task that writes it.
- Pass a large array from an upstream task's output, not as a run input:
  every item carries the run's input with it.

## See also

- [DSL reference, “Fan-out”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#fan-out-each): every rule
- [`examples/each-fan-out.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/each-fan-out.ork.yaml): the example, runnable
