# Cordon and drain an Orker

Take an Orker out of service without stopping the work it holds. Cordon it so it takes nothing new, drain it until it holds nothing, then stop the process.

## Example

```bash
orkctl orker list -F pool=build
orkctl orker cordon orker-7f3a --reason "runtime patch CVE-2026-1234"
orkctl orker drain orker-7f3a --timeout 4h -w
```

`drain` prints a line each time the running tasks or open pins change. When it exits `0`, the Orker is empty:

```text
orker-7f3a is drained; it is safe to stop this process.
```

## Read the exit code

| Exit | Meaning | What to do |
|---|---|---|
| `0` | The Orker reports no running task and no open pin | Stop the process |
| `4` | `--timeout` passed with work still on the host | Wait longer, or accept that stopping fails the open pins' runs with `PIN_HOST_LOST` |
| `1` | The Orker stopped answering, or was uncordoned or restarted | Check the Orker. A restart clears the cordon. |

`orkctl orker drain` is a loop in `orkctl`. Nothing on the server holds drain state.

## What a cordon does

- **New tasks:** the Orker stops asking for work. A task already on its way may still land, and it runs like any other in-flight task that drain waits for.
- **New pins:** the Orker refuses to open a pin. When every pin host of the pool is cordoned, a task that needs a pin fails `PIN_UNAVAILABLE` after about five minutes.
- **Work in flight:** running containers finish, and open pins finish their remaining members on this host.
- **Listing:** a cordoned Orker stays in `orkctl orker list` with its reason, so the drain can watch it.

Each cordon and uncordon is written to the audit trail. See [Audit](/docs/operate/audit/).

## When an Orker cordons itself

An Orker also cordons itself while object storage does not answer it, so tasks wait in the pool instead of using up their retries. `orkctl orker list` shows `cordoned (object storage unreachable)`, and Admin → Pools stamps the row `Object Storage Unreachable`.

- It resumes by itself after a few good checks. You don't need to do anything once the store is back.
- `orkctl orker uncordon` clears only a cordon an operator set. It can't override an outage that is still failing.
- `orkctl orker drain` sets an operator's cordon too, so a recovery in the middle of a drain can't put the Orker back to work.
- While every Orker of a pool holds this cordon, a new run sent there is accepted and waits. `orkctl run inspect` and the run page say it is waiting for object storage. Starting another Orker doesn't help, because it needs the same store.

It can't protect everything: an attempt that reaches the store before the Orker cordons itself still fails and uses an attempt, a task that starts a pin still fails `PIN_UNAVAILABLE`, and runs that close without an Orker, such as a cancel, are not held back. To turn it off, set `ORK_ORKER_STORE_CHECK_ENABLED=false`.

## Stop the process safely

A stop hands back the tasks still running on the Orker. Each container gets `SIGTERM`, then `SIGKILL` after its cancellation grace period (30 seconds by default), and the Orker reports each attempt `ORKER_STOPPED` before it exits. The task runs again on another Orker if its retry policy allows, or fails `ORKER_STOPPED`. A task whose container already finished reports its own result.

- Give the process manager at least the longest grace period its tasks use. A stop takes that long, plus a few seconds to report and record the attempts.
- A signal to the Orker's whole process group, such as Ctrl-C, never reaches a task's container.
- A second `SIGTERM` changes nothing. A third exits at once, and leaves what a crash leaves.
- `SIGTERM` cordons the Orker, so it takes no new task, but it does not wait for running ones. To let work finish first, cordon and drain, then stop.

In Compose, give the Orker's container a longer stop grace than its tasks' grace, and run it with an init so the stop reaches the process:

```yaml
services:
  orker:
    init: true
    stop_grace_period: 45s
```

## Bring the Orker back

```bash
orkctl orker uncordon orker-7f3a
```

A cordon is a flag in the Orker process only. A restarted Orker comes back ready, and it is not listed as cordoned. Restarting an Orker that hosts pins fails those pins' runs.

## Limits

- A cordon does not survive a restart. Cordon again after any restart that must stay out of service.
- `ORK_ORKER_ID` must be unique across the whole deployment. Two Orkers that share one show as one row, and a cordon can land on either.
- A stopped Orker still counts as live for up to 90 seconds after it stops polling.

## See also

- [DEPLOY.md, "Removing an Orker safely: cordon and drain"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#removing-an-orker-safely-cordon-and-drain): what a cordon does, and why it lives only in the process
- [DEPLOY.md, "When object storage goes away"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#when-object-storage-goes-away): the check, its settings and its limits
- [DEPLOY.md, "Pin hosting is a pool-wide setting"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#pin-hosting-is-a-pool-wide-setting): the pin host rules a drain relies on
- [UPGRADING.md, "Rolling forward"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#rolling-forward): draining the Orkers of an old release
- [CLI.md, "orkctl orker drain"](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-orker-drain): flags and exit codes
- [PRODUCTION_CHECKLIST.md, "Draining and removing Orkers"](https://github.com/orkstack/orkstack/blob/main/docs/PRODUCTION_CHECKLIST.md#draining-and-removing-orkers): the items to check before a stop
