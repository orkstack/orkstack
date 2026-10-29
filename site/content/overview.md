# OrkStack documentation

Write a multi-step job in YAML, run it, and read what it produced. Each step is an ordinary container image.

## What it does

An **Orkflow** is a YAML document. It lists **tasks**, and each task's `needs:` says what must finish first. A task runs one **activity**: a container image that reads its inputs from files, writes its results to files, and exits.

OrkStack runs the tasks in that order. It applies the retries and timeouts you set, and it cancels work when you ask. Once a task's result is committed, a retry returns that result without running the container again. If an Orker is lost, the run carries on or fails with a reason.

This excerpt fans `convert` out over the array that `list` computes:

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
    # ... its retry and script are in examples/each-fan-out.ork.yaml

  - id: merge
    uses: shell
    needs:
      - convert
    # ...
```

`convert` runs once per item, two at a time. `merge` starts when every item has ended.

<!-- site-diagram: each-fan-out-run -->

## Start here

- [Your first Orkflow](/docs/tutorials/first-orkflow/): start OrkStack with Docker, run an Orkflow, and read its results. About ten minutes.
- [Your first activity image](/docs/tutorials/first-activity/): build a container from a shell script and run it as a task.
- [Examples](/docs/examples/): every example Orkflow, what it shows and what it needs.
- [How it works](/docs/concepts/how-it-works/): the parts, and how a run moves between them.

## Find what you need

| You want to | Read |
|---|---|
| write an Orkflow | [Orkflow keys](/docs/reference/orkflow-keys/), [Fan out over a list](/docs/how-to/fan-out/), [Await a task](/docs/how-to/await/) |
| make an image into a task | [Build an activity image](/docs/how-to/build-an-activity-image/), [Activities](/docs/concepts/activities/) |
| run on a schedule or from outside | [Schedules](/docs/how-to/schedules/), [Triggers](/docs/how-to/triggers/), [Automation documents](/docs/how-to/automation/) |
| run a deployment | [Deploy](/docs/how-to/deploy/), [Namespaces](/docs/how-to/namespaces/) |
| look up a command | [orkctl reference](/docs/reference/cli/) |
| look up a word | [Glossary](/docs/concepts/glossary/) |
| change OrkStack | [Contributing](/docs/contributing/) |

## Status

OrkStack has not reached 1.0. v0.1.0 is its first release, and until 1.0 any release may include breaking changes. Each one is listed in the [changelog](/docs/reference/changelog/), marked **BREAKING**.

## Where the details live

These pages summarize the repository's own docs. Each page links to the full reference on GitHub, with every key, rule and default. [How OrkStack is tested](/docs/concepts/proven/) explains what the test suite covers.

## See also

- [README.md](https://github.com/orkstack/orkstack/blob/main/README.md): the project front page.
- [ARCHITECTURE.md](https://github.com/orkstack/orkstack/blob/main/ARCHITECTURE.md): the components, and the flow of a run.
