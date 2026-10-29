# Troubleshoot

Find the symptom you see, then read its likely cause and fix. Each symptom links to its full entry in the troubleshooting guide, which names the code that raises the error and the rule behind it.

## Example

Three commands answer most questions about a run:

```bash
orkctl run get <run-id> --attempts                  # each task's final outcome, and every try
orkctl run attempts <run-id> --task <task>          # each try's error, exit status and Orker
orkctl run logs <run-id> --task <task>              # what the task wrote
```

## Submitting

A refused submission leaves nothing behind. Fix the cause and submit again.

| Symptom | Likely cause | Fix |
|---|---|---|
| [“has no live Orkers, so nothing would pick these tasks up”](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#has-no-live-orkers-so-nothing-would-pick-these-tasks-up) | No Orker serves the pool. | Start an Orker with `ORK_ORKER_POOL=<pool>`, or submit with `--allow-empty-pool` to let the run wait. |
| [“cannot tell whether pool … has live Orkers”](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#cannot-tell-whether-pool--has-live-orkers) | The cluster did not answer in time. | Retry. Do not use `--allow-empty-pool` for this error. |
| [“has live Orkers but none of them hosts pins”](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#has-live-orkers-but-none-of-them-hosts-pins) | No Orker in the pool sets `ORK_ORKER_MAX_PINS` above zero. | Set it on every Orker of that pool, then restart them. |
| [“… is on hold” (`NAMESPACE_HELD`)](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#-is-on-hold-namespace_held) | An operator held the namespace. | Find out why, then run `orkctl namespace update <ns> --release-hold`. |
| [“already has N runs in flight” (`MAX_RUNNING_REACHED`)](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#already-has-n-runs-in-flight-which-is-its-max_running-max_running_reached) | The namespace is at its `--max-running` cap. | Wait for runs to finish, or raise the cap. |
| [“… at or above its enforced budget” (`BUDGET_EXCEEDED`)](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#-has-reserved--at-or-above-its-enforced-budget-budget_exceeded-or--could-not-be-measured-budget_unmeasurable) | Reserved usage reached the namespace's budget. | Raise `--budget-cpu-seconds` or `--budget-gib-seconds`. |
| [“could not be measured” (`BUDGET_UNMEASURABLE`)](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#-has-reserved--at-or-above-its-enforced-budget-budget_exceeded-or--could-not-be-measured-budget_unmeasurable) | The budget period holds more runs than a usage read covers. | Shorten `--budget-period`. |
| [`namespace "…" does not allow pool "…"`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#namespace--does-not-allow-pool-) | The namespace's pool list leaves out the task's pool. | Change the task's `pool:`, or add the pool with `--allow-pool`. |
| [“a submit or an apply is refused by the image policy”](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-submit-or-an-apply-is-refused-by-the-image-policy) | The image is outside the registry floor, lacks a digest, or lacks a required signature. | Use an allowed registry and a digest. Sign the image with a key the namespace names. |

## Runs that do not move

| Symptom | Likely cause | Fix |
|---|---|---|
| [My run stays `RUNNING` and nothing happens](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#my-run-stays-running-and-nothing-happens) | Every Orker of the pool is busy, no Orker serves it, its Orkers are cordoned, or one died holding a try. | Run `orkctl orker list`. Add Orkers to a busy pool, restart a dead Orker, or run `orkctl orker uncordon <orker-id>`. |
| [My run's tasks stay `PENDING`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#my-runs-tasks-stay-pending) | Not a failure. The run is at its in-flight cap, its history is near its hold line, or a pin is opening. A task waiting for a busy pool is `RUNNING`, not `PENDING`. | Wait. The run carries on as its running work finishes. |
| [I stopped an Orker and its tasks failed](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#i-stopped-an-orker-and-its-tasks-failed) | Stopping an Orker cancels the work it is running. | Drain the Orker first: `orkctl orker drain <orker-id>`. |
| [A run shows `CANCELED`, not `FAILED`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-run-shows-canceled-not-failed) | Someone canceled the run, or it was terminated from outside the engine. | Run `orkctl run get <run-id>`. Its error code says which. |
| [A task ran again](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-ran-again) | A task runs at least once. A retry or a lost completion runs it again. | Make the external effect idempotent, keyed on the idempotency key. Do not add `retry:` to a task that is unsafe to run twice. |

## Task and run failures

| Symptom | Likely cause | Fix |
|---|---|---|
| [`CONTAINER_EXIT_NONZERO`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-container_exit_nonzero-the-container-exited-n) | The container exited with a non-zero status. | Read that try's logs with `orkctl run logs <run-id> --task <task> --attempt <n>`, then fix the workload. |
| [`CONTAINER_TIMEOUT`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-container_timeout-exceeded-its-timeout-of-n-seconds) | The container ran past `timeout_seconds`. It is not retried. | Raise `timeout_seconds` if the work is slow. Otherwise find the hang. |
| [`VALUES_MISSING`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-values_missing-exited-0-but-wrote-no-valuesjson) | The container exited `0` without writing `values.json`. | Write a JSON object. Write `{}` when there is no result. |
| [`VALUES_MALFORMED` or `VALUES_TOO_LARGE`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-values_malformed-or-values_too_large) | `values.json` is not one JSON object, or it is over 256 KiB. | Write one object. Move large data into an artifact. |
| [`IMAGE_PULL_FAILED`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-image_pull_failed) | The Orker could not pull the image in time. | Check the registry, and the Orker's network. |
| [`WORKDIR_NOT_BINDABLE` or `WORKSPACE_NOT_BINDABLE`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-workdir_not_bindable-or-workspace_not_bindable-bind-source-path-does-not-exist) | The Docker daemon cannot see the Orker's work directory, as with a daemon in a virtual machine, or the path holds a comma or a control character, which a container mount cannot carry. | Set `ORK_ORKER_WORKDIR` to a plain path the daemon shares with the host. |
| [`SECRET_UNRESOLVED` or `SECRET_NOT_PERMITTED`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-secret_unresolved-secret_not_permitted-or-secret_scheme_forbidden) | The variable or file is missing on that Orker, or the source is outside its allowed floor. | Set it on that host, or ask the operator to allow the source. |
| [`PIN_HOST_LOST` or `PIN_TIMEOUT`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-pin_host_lost-or-pin_timeout) | `PIN_HOST_LOST`: the pin's Orker stopped while the pin was open; a pin never moves to another Orker. `PIN_TIMEOUT`: the pin stayed open past its `timeout_seconds`. | Start a new run with `orkctl run again <run-id>`. Drain a pin host before you stop it. For `PIN_TIMEOUT`, raise the pin's `timeout_seconds`. |
| [`PIN_UNAVAILABLE`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-pin_unavailable) | Every pin host stayed full or cordoned through the creation timeout. | Add a pin host, uncordon one, or reduce the pins open at once. |
| [`ORKER_HEARTBEAT_TIMEOUT`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-task-failed-with-orker_heartbeat_timeout-or-activity_timeout) | The Orker stopped sending heartbeats, so it died or lost contact. | Restart the Orker. The task retries under its `retry:` policy. |
| [`RUN_HISTORY_LIMIT`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-run-failed-with-run_history_limit) | The run would exceed its history limit, so it stopped on purpose. | Split the Orkflow into composed Orkflows, and keep task outputs small. |
| [`EACH_TOO_MANY_ITEMS`](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-fan-out-failed-with-each_too_many_items-each_over_not_array-or-each_over_missing) | The array has more elements than `max_items`. | Raise `max_items`, or filter the array before the fan-out. One Orkflow's `max_items` add up to at most 1,000 by default (`ORK_EACH_MAX_ITEMS`). |

## Limits

- This table covers the common symptoms. The [troubleshooting guide](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md) has every entry, including the local stack.
- Every terminal failure has a machine-readable code and a readable message. A failure without either is a defect to report.

## See also

- [TROUBLESHOOTING.md](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md): every symptom, cause and fix
- [Error reasons](/docs/reference/reasons/): every reason code
- [Inspect a run](/docs/how-to/inspect-a-run/): the commands behind the examples above
- [Activity contract](/docs/reference/activity-contract/): the failure categories a task's exit and outputs produce
