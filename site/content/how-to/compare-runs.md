# Compare two runs

See every difference between two runs, field by field. Use it to answer a question like "this run failed and last night's passed: what is different?"

## Compare in the terminal

```bash
orkctl run diff <from-run-id> <to-run-id>
orkctl run diff <from-run-id> <to-run-id> --only status,image,error
orkctl run diff <from-run-id> <to-run-id> -j
```

The comparison covers:

- **Run level:** the pinned definition and the input.
- **Task level:** presence, status, attempt, pool, pin, Orker, exit code, error, duration, image, platform digest and whether the result came from a cache.

`--only` narrows the fields to the ones you name, separated by commas. Presence is always compared, so a task that only one run has still appears.

Only differences are listed. A task that is identical in both runs does not show. Each row gives the field, and the value from each run.

Read the cells this way:

| Cell | Meaning |
|---|---|
| `-` | The value does not apply. For example, a task never reached an Orker. |
| `unavailable` | The evidence is gone, and the reason prints below the table. It is never shown as a value. |

The comparison runs on the engine every time you ask. Nothing is stored. Runs of different Orkflows are compared and flagged, not refused.

The exit code is `0` whenever the comparison succeeds, whether the runs match or not. Scripts should read `--json`.

## Compare in the UI

On a run's page, choose **Compare**. The page pre-selects the newest other succeeded run of the same Orkflow. It shows the same table and summary, with a link that swaps the two runs. The page has its own address, so you can share it.

## See also

- [docs/UI_CLI_PARITY.md, "Run diff"](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#run-diff): the fields, their rendering, and the reasons a value is unavailable
- [docs/CLI.md, "orkctl run diff"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-run-diff): every flag
- [docs/RUN_EVIDENCE.md, "States"](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#states): what it means when a record is reclaimed
