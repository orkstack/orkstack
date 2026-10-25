# Manage namespaces

Give each team or environment its own namespace, then bound what it may do: pools, secrets, runs in flight, budgets and retention. A namespace holds its own Orkflows and runs.

## Example

```bash
orkctl namespace create payments \
  --description "Payments team" \
  --max-running 50 \
  --run-retention 24h
orkctl namespace get payments
```

Creating a namespace again with the same settings does nothing. A different setting is refused, and the error names the field.

## Create, read and change

| Command | What it does |
|---|---|
| `orkctl namespace create NAME` | Registers a namespace. |
| `orkctl namespace list` | Lists namespaces. Add `--filter 'name^team-'` to narrow the list. |
| `orkctl namespace get [NAME]` | Shows one namespace. Without a name, it shows the one you act in. |
| `orkctl namespace update NAME` | Changes the flags you pass. Other settings stay as they are. |
| `orkctl namespace use NAME` | Saves the namespace as your default for later commands. |
| `orkctl namespace delete NAME --yes` | Removes a namespace that holds no Orkflows. `default` cannot be deleted. |

Names use lowercase letters, digits and hyphens, start with a letter, and are at most 200 bytes long.

## Hold a namespace

A hold closes a namespace to new runs during an incident. Runs already accepted keep going.

```bash
orkctl namespace update payments --hold "Duplicate charges under investigation"
orkctl namespace update payments --release-hold
```

Releasing the hold starts nothing that was refused while it was set. Refused submissions are not queued.

## Cap runs in flight

`--max-running 50` refuses a submission when the namespace already has 50 runs in flight. `0` means no limit. Replicas count without a lock, so a burst of submissions can go a few runs over the cap.

## Set a budget

A budget bounds the cpu and memory a namespace reserves over a rolling period. Reservation is what each attempt was granted, multiplied by how long it held it. It is not measured use.

```bash
orkctl stats usage --since 720h --group-by namespace          # what each namespace reserved
orkctl namespace update analytics --budget-cpu-seconds 5000000 --budget-period 720h
orkctl -n analytics stats usage --since 720h                  # does the measurement answer?
orkctl namespace update analytics --budget-enforce            # now refuse new runs at the budget
```

A budget is soft until you set `--budget-enforce`. A soft budget only reports. The period runs from 1 hour to 744 hours. Check that `stats usage` answers before you enforce a budget. An enforced budget that cannot be measured refuses every submission.

## Keep finished runs for less time

Retention counts from when a run finished. A deployment sets the longest any namespace may keep runs. A namespace can keep runs for less.

```bash
orkctl admin retention set 72h
orkctl namespace update payments --run-retention 24h
```

The engine retires runs at their retention only when `ORK_CLEANUP_ENABLED=true`. Otherwise every run is kept for the deployment maximum plus a margin (`ORK_CLEANUP_RETENTION_BUFFER`, 24 hours by default).

## Limit what a namespace may use

| Flag | Effect |
|---|---|
| `--allow-pool POOL` | Lets the namespace run tasks on that pool. Repeat it for each pool. Empty means no restriction. |
| `--allow-secret 'env:SANDBOX_*'` | Lets tasks name only matching secret sources. On update, it replaces the whole list. |
| `--require-signed-by cosign:KEY` | Requires every image the namespace runs to have a signature by that key. |

A namespace can narrow the deployment's rules. It cannot loosen them.

## When a submission is refused

A refusal happens before a run exists, so nothing is left behind.

| Reason | Meaning | Fix |
|---|---|---|
| `NAMESPACE_HELD` | The namespace is on hold. | Release the hold once the reason is cleared. |
| `MAX_RUNNING_REACHED` | The namespace has as many runs in flight as its cap. | Wait for runs to finish, or raise `--max-running`. |
| `BUDGET_EXCEEDED` | Reserved usage has reached the enforced budget. | Raise the budget, or wait for the period to roll on. |
| `BUDGET_UNMEASURABLE` | The period holds more runs than a usage read covers, or no store answers. | Shorten `--budget-period`. |

## Limits

- Admission is best-effort. Replicas read the count without a lock, so a cap can overshoot by a few runs.
- A hold never cancels a run. To stop one, use `orkctl run cancel`.
- A namespace's allow-lists apply at apply and at every submit. They never apply retroactively to a run already accepted.

## See also

- [DEPLOY.md, “Namespace admission”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#namespace-admission-hold-and-max_running): hold and `max_running`, in full
- [DEPLOY.md, “Namespace budgets”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#namespace-budgets-reserved-consumption-soft-or-enforced): the budget rules
- [DEPLOY.md, “Run retention”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#run-retention-a-deployment-maximum-a-value-per-namespace): retention and cleanup
- [DEPLOY.md, “Secrets on a shared pool”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#secrets-on-a-shared-pool): the secret floor and each namespace's list
- [DEPLOY.md, “Image supply-chain policy”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#image-supply-chain-policy): signatures and the image floor
- Concept: [Namespaces](/docs/concepts/namespaces/)
- Command reference: [orkctl](/docs/reference/cli/)
