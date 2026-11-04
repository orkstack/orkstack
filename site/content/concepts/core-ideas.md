# Core ideas

Eleven words carry most of OrkStack. Learn them once, and the rest of these docs read easily. The full list is in the [glossary](/docs/concepts/glossary/).

## The words in one example

This Orkflow has two tasks. `hello` runs first, and `goodbye` waits for it because of `needs:`:

```yaml
apiVersion: orkstack/v1
kind: Orkflow
name: greet
version: "0.1.0"

inputs:
  who:
    type: string
    default: world

tasks:
  - id: hello
    uses: shell
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        echo "hello {{ sh .inputs.who }}"

  - id: goodbye
    uses: shell
    needs:
      - hello
    with:
      op: script
      interpreter: sh
      script: |
        set -eu
        echo "goodbye {{ sh .inputs.who }}"
```

## The words

| Word | What it means |
|---|---|
| **Orkflow** | A YAML document that lists tasks and the order they run in. Applying it stores an immutable version. |
| **Task** | One step of an Orkflow. Its `id:` names it, and `needs:` lists the tasks that must finish first. |
| **Activity** | The container image a task runs, named by `uses:`. It reads files, writes files and exits. `shell` is a first-party activity. |
| **Artifact** | A file a task writes to `/orkstack/output/artifacts/`. Later tasks receive it as a file. |
| **Values** | A task's structured result, a JSON object in `values.json`. |
| **Run** | One execution of an Orkflow version. It has its own ID and its own event stream. |
| **Namespace** | A scope for Orkflows, runs, pools and settings. The built-in namespace is `default`. |
| **Orker** | A worker process that pulls each task's image and runs it as a container. |
| **Pool** | The set of Orkers a task may run on. `pool:` names it, and `main` is the default. |
| **Pin** | A group of tasks that run on one Orker and share `/orkstack/shared`. |
| **Item** | One element of a fan-out, counted from 0. A fan-out is a task with `each:`. |

Two more words come up in the docs. An **awaited task** waits for someone outside the run to finish it, and a **schedule** starts runs at set times.

## What a run promises

A task runs at least once. If the Orker running it goes away, the task is retried. Its result is written to object storage before the run counts it as done, so a retry that finds that result returns it and does not run the container again.

Run state is kept by Temporal, which keeps runs durable. A run is not lost when an OrkStack process restarts. It carries on, or it fails with a reason that says why.

## Next

- [Your first Orkflow](/docs/get-started/first-orkflow/): apply this kind of document, run it and read the results.
- [Inputs and outputs](/docs/build/inputs-and-outputs/): how `inputs:` and `outputs:` work.

## See also

- [README, "Core concepts"](https://github.com/orkstack/orkstack/blob/main/README.md#core-concepts): the same ideas, with a fan-out example
- [docs/DSL.md, "Tasks"](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#tasks): every task key
- [docs/DSL.md, "Top-level fields"](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#top-level-fields): every key of an Orkflow
- [docs/DSL.md, "Pins"](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#pins): how a pin keeps tasks on one Orker
