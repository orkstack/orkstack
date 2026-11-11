# Pools, Orkers and pins

A pool is a set of Orkers that run the same tasks. Start Orkers in a pool, size each one to its host, and check the fleet with `orkctl pool list` or the Pools page.

## Example

On each Orker host, join the `gpu` pool, cap the tasks it runs at once, and check the result:

```bash
ORK_ORKER_POOL=gpu ORK_ORKER_MAX_CONCURRENT=4 orker --config orkstack.yaml

orkctl pool list -n payments                 # the pool, and how many Orkers serve it
orkctl orker list -F pool=gpu          # each Orker in the pool, with its load
```

A task that names no pool runs on its namespace's default pool, which `orkctl namespace update NS --default-pool POOL` sets. A namespace with no default pool set uses the deployment's default pool, `main`. To change the deployment's default, set `ORK_ORKER_POOL` to the same value on `ork` and on every Orker of that pool. Leave it unset on `ork` otherwise.

## Join a pool

- `main` is the built-in pool. An Orker with no pool setting serves it.
- Set the pool on each Orker with `ORK_ORKER_POOL`, or `pool.name` in the config file. Do not set it on `ork` to join a pool.
- If `ORK_ORKER_QUEUE` is also set on a process, the two must agree, or that process refuses to boot.
- A task chooses its pool with `pool:` in the Orkflow ([Pins](/docs/build/choose-where-tasks-run/) explains the choice).
- A namespace may dispatch onto only the pools it allows: `orkctl namespace update payments --allow-pool gpu`. An empty list means no restriction.
- A submit into a pool with no live Orkers is refused, and the refusal names the pool and the fix.

## Size each Orker

| Setting | Default | What it sets |
|---|---|---|
| `ORK_ORKER_MAX_CONCURRENT` | `16` | Tasks this Orker runs at once. Later tasks wait in the pool. |
| `ORK_ORKER_MAX_CPU`, `ORK_ORKER_MAX_MEMORY` | `8`, `16Gi` | The most one task may request. A larger request is refused, never clamped. |
| `ORK_ORKER_DEFAULT_CPU`, `ORK_ORKER_DEFAULT_MEMORY` | `1`, `1Gi` | What a task that requests nothing gets. |
| `ORK_ORKER_CAPACITY_CPU`, `ORK_ORKER_CAPACITY_MEMORY` | empty | What this Orker admits tasks within. |

Keep the ceiling within what the host can hold at the per-task cap:

```text
max_concurrent ≤ min(host cpus ÷ max_cpu, host memory ÷ max_memory)
```

A 16-CPU, 64 GiB host with the default caps (8 CPU, 16 GiB) fits two tasks at once, however small they are. Lower the caps to what the pool's tasks really request to run more.

To admit tasks by what the host holds, declare a capacity. Without one, admission counts tasks only, and the Orker logs that at boot. With one, a poll is granted only while a per-task cap fits beside what is already reserved. An Orker whose capacity equals one cap runs one task at a time.

## Keep one pool on one set of settings

Every Orker of a pool should have the same caps, default request, capacity, `ORK_ORKER_MAX_CONCURRENT`, secret floor, `env` allow-list, registry credentials, runtime and pin hosting setting. Temporal hands a task to whichever Orker polls first, so a difference shows up as a task that fails on the Orker that will not run it, with its own reason. A run never fails because of which Orker took a task while the settings match.

To change a setting with runs in flight, widen every Orker before any task needs more, and narrow only after nothing needs the wider setting. The full order is in [DEPLOY.md, "Changing a setting with runs in flight"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#changing-a-setting-with-runs-in-flight).

## Host pins

A pin is a named group of tasks of one Orkflow that run on one Orker and share `/orkstack/shared`.

- Set `ORK_ORKER_MAX_PINS` above `0` on every Orker of a pool that runs pins. The default `0` hosts none.
- A pool with no pin host refuses a pinning document at submit, so pins are opt-in by the author and by the operator.
- A pin never moves to another Orker. If its host is lost, the pin's run fails `PIN_HOST_LOST`.
- To take a pin host out of service, [cordon and drain it](/docs/operate/cordon-and-drain/).

## See the fleet

`orkctl pool list` reports three states, and each needs a different action:

| ORKERS | Meaning | What to do |
|---|---|---|
| `1`, `2`, … | That many Orkers are serving the pool now | Nothing |
| `0 (no live orkers)` | Measured: nobody serves the pool | Start an Orker with `ORK_ORKER_POOL` |
| `unknown (unreachable)` | Not measured: the cluster did not answer | Ask again |

An Orker that stopped in the last 90 seconds still counts here, and shows as
`unknown (unreachable)` in `orkctl orker list`. So an Orker restarted under
another id reads as one too many until then. Set `ORK_ORKER_ID` to keep its id.

`orkctl orker get ID` shows one Orker's state, load and open pins. In the web UI, the Pools page shows a card for each pool and an Orkers table with a **Cordon** action.

Each Orker needs an `ORK_ORKER_ID` that no other Orker in the deployment uses, even on another host. The default is derived from the host and its work directory, which is unique on a host. In a container, set it explicitly.

## See also

- [DEPLOY.md, "Running M Orker pools"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-m-orker-pools): how a process joins a pool, and what the engine does with a task that names none
- [DEPLOY.md, "Sizing an Orker's task ceiling"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#sizing-an-orkers-task-ceiling): the ceiling, and how to size it
- [DEPLOY.md, "Declaring an Orker's capacity"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#declaring-an-orkers-capacity): admission, the ledger and the capacity metrics
- [DEPLOY.md, "Every Orker of a pool has the same settings"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#every-orker-of-a-pool-has-the-same-settings): the settings a pool shares
- [DEPLOY.md, "Pin hosting is a pool-wide setting"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#pin-hosting-is-a-pool-wide-setting): why pin hosting is set on the whole pool
- [DSL.md, "Pins"](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#pins): how an Orkflow declares a pin
- [DEPLOY.md, "The admin surface: what a deployment is made of, and who is serving it"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-admin-surface-what-a-deployment-is-made-of-and-who-is-serving-it): what each pool read reports
- Command reference: [orkctl pool list](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-pool-list)
- Settings: [Orker settings](/docs/configuration/orker/)
