# Inspect a run

Read a run's status, each task, every try of each task and what it wrote. Export a finished run as an evidence bundle that anyone can verify. Add `-n NAMESPACE` when the run is not in `default`.

## Example

```bash
orkctl run get <run-id>                          # the run, and each task's final outcome
orkctl run attempts <run-id> --task convert      # every try, with its error and exit status
orkctl run logs <run-id> --task convert --attempt 2
```

<!-- site-media: ui-run-finished -->

The web UI shows the same run page. Each failed try has its own error, and a link that opens that try's log.

## Find runs

```bash
orkctl run list --filter 'status=FAILED'
orkctl run list --filter 'orkflow_name=payments-release' --filter 'status=FAILED'
```

The list is newest first. Filter on `status` (`PENDING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELED`, `SKIPPED`), `orkflow_name`, `created_at` and `origin.kind`.

## Watch a run in progress

- `orkctl run inspect <run-id> --watch` shows each running task, its state and the state of its last heartbeat. A heartbeat marked `STALE` means the Orker has gone.
- `orkctl run watch <run-id>` streams events until the run ends. It exits `0` on success, `2` on failure and `3` on cancel.
- `orkctl run logs <run-id> --task convert -f` follows one task's output live.

## Read the timing

`orkctl run timing <run-id>` splits each task's time into three parts: wait for an Orker, run, and total. A value that cannot be known prints as `unavailable`, with the reason.

## Fan-out and composed runs

- `orkctl run get <run-id> --expand convert` prints one row per item of a fan-out.
- `orkctl run logs <run-id> --task convert --item 2` prints one item's output.
- `orkctl run get <run-id> --tree` prints the child runs a composed task started.

## Artifacts and evidence

```bash
orkctl run artifacts <run-id> --task convert
orkctl run artifact download <run-id> --task convert --name part.txt --output part.txt
orkctl run evidence export <run-id> --output run.tar.gz    # finished runs only
orkctl run evidence verify run.tar.gz                      # offline: needs no server
```

A bundle holds the run's record, the Orkflow's source, each task's values and artifacts, every try's record and logs, and a manifest of hashes. `verify` checks every member against that manifest.

## Compare two runs

`orkctl run diff <from-run-id> <to-run-id>` compares the two runs field by field. `--only` narrows the comparison to the fields you name.

## Find patterns across runs

```bash
orkctl stats failures --since 24h      # failed runs, grouped by what failed and why
orkctl stats flakes --since 168h       # tasks that needed more than one try
```

## Limits

- Logs and attempt records follow the run's retention. Once cleanup retires a run, its logs and tries are gone. The archived run record stays.
- Each try keeps at most 16 MiB of `stdout` and 16 MiB of `stderr`. Past the cap, the Orker stores one truncation notice.
- A try whose Orker died before it reported shows as `UNREPORTED`. That is missing evidence, not a failure.

## See also

- [UI_CLI_PARITY.md, “Task attempts”](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#task-attempts): how each try is recorded and read back
- [UI_CLI_PARITY.md, “Task timing”](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#task-timing): the wait, run and total rules
- [RUN_EVIDENCE.md](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md): the bundle format and what `verify` checks
- [STATS.md](https://github.com/orkstack/orkstack/blob/main/docs/STATS.md): where the numbers on stats pages come from
- Command reference: [orkctl](/docs/reference/cli/)
- Symptoms: [Troubleshoot](/docs/how-to/troubleshoot/)
