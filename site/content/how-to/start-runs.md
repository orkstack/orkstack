# Start runs

Create an Orkflow version once, then start a run of it as often as you need. Start runs from the UI or from `orkctl`, pass the input as JSON, plan a run before it starts, and cancel it with a reason.

## Create, then submit

```bash
orkctl orkflow create examples/each-fan-out.ork.yaml
orkctl run submit each-fan-out -w
```

`orkflow create` stores an immutable version of the Orkflow and starts nothing. `run submit` starts a run of the current version and, with `--watch`, follows it until it ends.

In the UI, open Runs and choose **Submit a run**. To store a new version first, open **Orkflows** and choose **Create Orkflow**.

## Pass the input

A run's input is JSON, never YAML. Give it one of two ways:

```bash
orkctl run submit my-orkflow -i count=3 -i 'tags=["a","b"]'
orkctl run submit my-orkflow --input-json '{"count": 3, "tags": ["a", "b"]}'
```

| Flag | What it does |
|---|---|
| `--input key=value` | One key. A value that parses as JSON keeps its type, so `3` is a number. Any other value is a string. Repeat the flag for more keys. |
| `--input-json '{...}'` | One JSON object. The `--input` values are merged over it. |

For the rules, see [JSON values](/docs/reference/json/).

## Plan before you start

`orkctl run plan` shows which tasks a run would execute and which it would skip, and why. It starts nothing and stores nothing.

```bash
orkctl run plan my-orkflow -i count=3
orkctl run plan my-orkflow --version 0.3.0
```

A plan proves the wiring, not the behaviour. It does not check pools, holds or limits. `run submit` checks those.

For CI, `--assert-runs` and `--assert-skipped` make the plan fail with exit code 4 when it contradicts what you expect.

## Repeat a submission safely

Pass `--idempotency-key` with a key of your own. The key is scoped to the namespace. A repeated submission with the same key returns the run that the first one started, not a second run. A key stays bound for 24 hours by default.

```bash
orkctl run submit my-orkflow --idempotency-key nightly-2026-11-03
```

A keyed repeat is refused while the namespace is on hold or at its run limit. See [Namespaces, holds and budgets](/docs/operate/namespaces/).

## Run again

`run again` starts a new run from an earlier run's pinned version and input.

```bash
orkctl run again ork-…                              # the same version and input
orkctl run again ork-… -i count=5 --dry-run    # print the new submission, start nothing
orkctl run again ork-… --version 0.4.0 --no-cache   # a new version, and no cached results
```

The new run has a new ID, and the original idempotency key is never reused. In the UI, open a run and choose **Run again**. It loads the same settings into a draft you review before you submit.

Run again starts a new execution, and that execution can repeat side effects outside OrkStack. Check the tasks before you run again.

## Cancel with a reason

```bash
orkctl run cancel ork-… --reason "wrong input, rerunning"
```

The reason is recorded on the run. The run records who asked, as the principal your gateway asserted. OrkStack does not verify that name. In the UI, open the run and choose **Cancel run**.

## See also

- [docs/UI_CLI_PARITY.md, "Submitting runs"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#submitting-runs): the origin of each run, and the UI and CLI forms
- [docs/UI_CLI_PARITY.md, "Run again"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#run-again): what the new run reuses, and what it does not
- [docs/CLI.md, "orkctl run submit"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-run-submit): every submit flag
- [docs/CLI.md, "orkctl run plan"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-run-plan): the plan's flags and CI assertions
- [docs/CLI.md, "orkctl run cancel"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-run-cancel): cancellation, and who it records
- [docs/DEPLOY.md, "1. Configuration"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration): `engine.idempotency_ttl` and the other settings
