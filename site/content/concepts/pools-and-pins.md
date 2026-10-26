# Pools, Orkers and pins

Learn which Orkers may run a task, and when a group of tasks must run on one Orker together.

```yaml
pins:
  build:                    # declared once, by name
    timeout_seconds: 900    # optional; 0 or absent means derived from the members

tasks:
  # ...
  - id: write
    uses: shell
    pin: build
    run_as_orker_user: true
```

The full example is [`examples/pinned-build.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/pinned-build.ork.yaml). Two tasks join `build`, and they run on the same Orker.

## Pools: which Orkers may run a task

A pool is a set of Orkers. A task's `pool:` names one. A task with no `pool:` runs on its namespace's default pool, which is `main` unless the namespace sets another.

A namespace can limit the pools its tasks may name, with `--allow-pool`. A task that names a pool with no live Orkers is refused when you submit the run. `orkctl run submit --allow-empty-pool` submits it anyway, and the task waits for an Orker. `orkctl pool list` shows the pools a namespace may use, and the Orkers serving each.

## Pins: one Orker, together

A pin is a named group of tasks. The group runs on one Orker, in one process, and shares `/orkstack/shared` for as long as the group is open. Tasks outside the pin cannot see that directory.

A pin adds no ordering. Two tasks in a pin run side by side unless `needs:` puts one after the other. Every task in a pin names the same pool and the same `run_as_orker_user`, and a pin cannot span two pools.

A pin needs a **pin host**: an Orker started with `ORK_ORKER_MAX_PINS` above zero. Set it on every Orker of the pool. A pool with no pin host refuses a pinned run at submit.

If every pin host stays full for five minutes, the task fails with `PIN_UNAVAILABLE`. The wait is five minutes unless the deployment sets `ORK_PIN_CREATION_TIMEOUT`. If a pin's host is lost, the run fails with `PIN_HOST_LOST`. A pin never moves to another Orker, because the shared directory would not come with it.

| | Pool | Pin |
|---|---|---|
| Chooses | which Orkers may run a task | which one Orker runs a group of tasks |
| Set with | `pool:` on a task | `pins:`, and `pin:` on each task |
| Shares files | no | `/orkstack/shared`, within the pin |
| When the host is lost | the task retries on another Orker | the run fails with `PIN_HOST_LOST` |

## See also

- [docs/DSL.md, Pins](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#pins): the pin rules, the timeout and the failure reasons.
- [docs/DSL.md, Tasks](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#tasks): the `pool:` and `pin:` keys in context.
- [docs/DEPLOY.md, Pin hosting is a pool-wide setting](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#pin-hosting-is-a-pool-wide-setting): how to set `ORK_ORKER_MAX_PINS` for a pool.
