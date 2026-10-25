# Retry a task and set its timeouts

Give a task more than one attempt with `retry:`. Set how long each attempt may run with `timeout_seconds:`.

## Example

In [`examples/poll-until-ready.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/poll-until-ready.ork.yaml), `wait-ready` checks a status endpoint on each attempt. It exits non-zero until the endpoint reports ready:

```yaml
  - id: wait-ready
    uses: shell
    timeout_seconds: 30
    retry:
      maximum_attempts: 20
      initial_interval_seconds: 5
      backoff_coefficient: 1.5
      maximum_interval_seconds: 60
    with:
      op: exec
      command:
        - sh
        - -c
        - 'wget -q -O - "$1" | jq -e ".ready == true" > /dev/null'
        - wait-ready
        - '{{ .inputs.status_url }}'
```

Run it:

```bash
orkctl apply examples/poll-until-ready.ork.yaml
orkctl run submit poll-until-ready --input status_url=https://status.example.com/exports/42 --input export_url=https://files.example.com/exports/42.json --watch
orkctl run attempts <run-id>
```

Each attempt may run for 30 seconds. The task makes up to 20 attempts. The wait before the first retry is 5 seconds. Each wait is 1.5 times the one before, and it is capped at 60 seconds. No container runs between attempts.

## Set the retry policy

Leave `retry:` out, and the task makes one attempt.

| Key | Default | Meaning |
|---|---|---|
| `maximum_attempts` | `1` | Attempts in all, the first one included. `0` and `1` mean one attempt. |
| `initial_interval_seconds` | `1` | The wait before the first retry. |
| `backoff_coefficient` | `2.0` | Each wait is this many times the one before. |
| `maximum_interval_seconds` | `100` | The longest wait. |

Each attempt gets a fresh workspace. The idempotency key stays the same across attempts, so an outside system can match the retries to one activity.

## Set the timeouts

| Key | Default | Meaning |
|---|---|---|
| `timeout_seconds` | `300` | How long one attempt's container may run. |
| `cancellation_grace_period_seconds` | `30` | The time between SIGTERM and SIGKILL when the task is cancelled. |

A cancelled container gets SIGTERM first. It has the grace period to clean up. Then it gets SIGKILL.

## Which failures retry

| Failure | Retried under `retry:` |
|---|---|
| `ActivityFailed`: the container exited non-zero | yes |
| `InfrastructureError`: storage, the registry or the runtime failed, or an Orker stopped answering | yes, except for a host misconfiguration, which fails at once |
| `TimedOut`: the container ran past `timeout_seconds` | no |
| `ContractViolation`: the image or the definition broke the contract | no |
| `Cancelled`: the task was cancelled | no |

A container that runs past its timeout fails once. It would run past the timeout again on the next attempt.

## Durable results

The Orker writes a task's result to storage before it reports success. If that result is already stored, the next attempt uses it. The container does not run again.

<!-- site-diagram: durable-commit -->

Activities run at least once. A retry can repeat an effect outside OrkStack, such as a payment or an email. Make those calls safe to repeat. Key them on the idempotency key.

## When a retry is not enough

When the last attempt fails, the task fails with its error, and the run fails with it. Read every attempt with `orkctl run attempts <run-id>`.

A failing run still runs its failure handlers. See [Handle a failed run](/docs/how-to/handle-failure/).

## See also

- [OCI Activity Contract, §8, failure categories](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#8-failure-categories-and-error-handling): every reason code and whether it retries
- [OCI Activity Contract, §5, durable commit](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#5-durable-commit-protocol): the commit order and the retry that adopts it
- [DSL reference, “Tasks”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#tasks): the task keys and their defaults
- [Durability](/docs/concepts/durability/): why a result is stored before it is reported
