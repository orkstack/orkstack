# Watch a run

Follow a run as it goes: which tasks have run, what each try wrote, and where the time went. This page covers the UI and `orkctl`, and what the waiting states mean.

## Find the run

```bash
orkctl run list --filter status=FAILED
orkctl run list --filter orkflow_name=payments-release --filter status=FAILED
```

The list is newest first. Filters can name `status` (`PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELED`, `SKIPPED`), `orkflow_name`, `created_at`, `origin.kind`, `origin.id` (with `origin.kind`, to name one trigger, schedule or source run) and `labels.KEY`, a run's label by its key. Add `-n NAMESPACE` when the run is not in `default`.

## Follow it in the UI

Open Runs, then the run. The run page draws the tasks as a graph, lists them with a row each, and shows a fan-out as a grid of items. Choose a task or an item to open its log in the log pane.

<!-- site-media: ui-run-live -->

The page updates live while the run is going. If the connection drops, it picks up where it left off. If it cannot reconnect, the badge reads `STOPPED`, and **Retry live updates** starts again.

Side cards, such as the Task map, show more about the run. The run's actions are **Evidence**, **Compare**, **Events**, **Cancel run…** and **Run again**. The terminal icon on a screen opens a list of the `orkctl` commands for that screen.

## Follow it in the terminal

```bash
orkctl run watch <run-id>                   # stream events until the run ends
orkctl run inspect <run-id> --watch         # what each running task is doing now
orkctl run get <run-id>                     # every task's final outcome
orkctl run get <run-id> --expand convert    # one row per item of a fan-out
orkctl run logs <run-id> --task convert -f  # follow one task's output
orkctl run logs <run-id> --task convert --item 2   # one item's output
orkctl run get <run-id> --tree              # the child runs a composed task started
```

`run watch` and `run inspect --watch` exit when the run ends. The exit code is `0` for a success, `2` for a failure and `3` for a cancel. Use it in scripts.

If the engine drops the stream, `run watch` picks up after the last event it printed, and says so on stderr.

## Read the task states

Each task shows one of these states. A run is `Running`, `Succeeded`, `Failed` or `Canceled`.

| State | What it means | What to do |
|---|---|---|
| Pending | The engine has not started the task yet. The run is at its in-flight limit, its history is near its limit, or a pin is opening. | Nothing. The run continues as its running work finishes. |
| Waiting for capacity | The task is ready, and every Orker of its pool is at the capacity it declared. | Wait, or add an Orker to the pool, or raise an Orker's declared capacity. `run inspect` shows what is reserved against what is admitted. |
| Waiting for an Orker | No Orker has picked the task up. | Check that an Orker serves the pool: `orkctl pool list` and `orkctl orker list`. |
| Running | An Orker is running the task's container. | Read its log, or `run inspect` for its heartbeat. |
| Awaiting | The task waits for someone outside the run to complete or reject it, or for its deadline. | Resolve it, as below. |
| Skipped | The task's condition or join policy said not to run it. | Check the condition with `orkctl run plan`. |

`orkctl run inspect` marks a heartbeat as `STALE` when the Orker has stopped reporting. The attempt is timed out at the deadline it names.

### Resolve an awaited task

```bash
orkctl run resolve <run-id> --task approve --values '{"approved": true}'
orkctl run resolve <run-id> --task approve --reject --reason "not this week"
```

A completion's values become the task's `values.json`, byte for byte. A rejection fails the task with `AWAIT_REJECTED`. The run applies the answer next, and a resolution that loses a race is recorded on the run.

## Read every try

A retry does not show in the task's final outcome. Use attempts to see each try:

```bash
orkctl run attempts <run-id> --task convert       # every try, oldest first
orkctl run logs <run-id> --task convert --attempt 2
orkctl run timing <run-id> --task convert         # wait, run and total time
```

The UI's attempt timeline lists each try, with its own error and a link that opens that try's log.

A try whose Orker died before it filed a record shows as `UNREPORTED`. That is missing evidence, not a failure.

A value that cannot be known prints as `unavailable`, with the reason. It never prints as zero.

## Artifacts

```bash
orkctl run artifacts <run-id> --task convert
orkctl run artifact download <run-id> --task convert --name part.txt --output part.txt
```

A fan-out's artifact needs `--item`. `download` refuses to overwrite an existing file unless you pass `--overwrite`.

`download --output` writes a file, which the `orkctl` alias cannot do. Use the installed binary, as [Run it locally](/docs/get-started/run-locally/#install-orkctl-on-your-machine) explains.

## Limits

- Logs and attempt records follow the run's retention. After cleanup, a run's logs and tries are gone. The archived run record stays.
- Each try keeps at most 16 MiB of stdout and 16 MiB of stderr.

## See also

- [docs/UI_CLI_PARITY.md, "Live attempt introspection"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#live-attempt-introspection): the live line, the heartbeat and the capacity footer
- [docs/UI_CLI_PARITY.md, "Task attempts"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#task-attempts): how each try is recorded and read back
- [docs/UI_CLI_PARITY.md, "Task timing"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#task-timing): the wait, run and total rules
- [docs/UI_CLI_PARITY.md, "Artifact retrieval"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#artifact-retrieval): downloads and their checks
- [docs/UI_CLI_PARITY.md, "Awaited tasks"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#awaited-tasks): resolving an await from outside the run
- [docs/CLI.md, "orkctl run watch"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-run-watch): the exit codes and the resume rule
- [docs/DEPLOY.md, "Declaring an Orker's capacity"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#declaring-an-orkers-capacity): when a task waits for capacity
- [docs/TROUBLESHOOTING.md, "Running"](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#running): a run that stays Running or Pending
