# Route tasks to Orkers, and pin tasks together

Send a task to a pool of Orkers with `pool:`. Put tasks in a pin to run them on one Orker, where they share a directory.

## Example

In [`examples/pinned-checkout.ork.yaml`](https://github.com/orkstack/orkstack/blob/main/examples/pinned-checkout.ork.yaml), the tasks share the pin `workspace`. The pin is declared once:

```yaml
pins:
  workspace:
    timeout_seconds: 1800
```

`checkout` clones the repository into the pin's shared directory:

```yaml
  - id: checkout
    uses: git
    needs: [resolve]
    pin: workspace
    run_as_orker_user: true
    retry: { maximum_attempts: 3, initial_interval_seconds: 2 }
    secrets:
      - name: git-token
        source: env:GIT_TOKEN
    with:
      op: clone
      url: '{{ .inputs.repo }}'
      ref: '{{ .tasks.resolve.output.commit }}'
      tokenSecret: git-token
      dest: /orkstack/shared/src
```

`validate`, `merge` and `package` also name `pin: workspace`. `checksum` runs outside the pin. It reads the one artifact the pin wrote.

Run it. The pool needs a pin host, which an operator starts with `ORK_ORKER_MAX_PINS` above zero:

```bash
orkctl apply examples/pinned-checkout.ork.yaml
orkctl run submit pinned-checkout --input repo=https://git.example.com/acme/app.git --input environment=prod --watch
```

## Send a task to a pool

A pool is a set of Orkers. Orkers join a pool with `ORK_ORKER_POOL=gpu`. A task names its pool with `pool:`:

```yaml
  - id: train
    uses: shell
    pool: gpu
```

- A task with no `pool:` runs on the namespace's default pool. That is `main`, unless the namespace sets another.
- `pool: main` names the deployment's default pool. It equals an omitted `pool:` unless the namespace sets another default pool.
- `orkctl pool list` shows each pool and how many Orkers serve it.
- A submit is refused when a task's pool has no live Orkers. Add `--allow-empty-pool` to submit anyway. The task then waits for an Orker.
- `orkctl namespace update <name> --allow-pool gpu` limits the pools that the namespace's tasks may name. Repeat the flag for each pool. Without it, the namespace has no limit.

## Pin tasks together

- Declare the pin under `pins:`. Set `timeout_seconds` for its whole lifetime. Without it, the lifetime is the sum of its tasks' timeouts.
- Put `pin: <name>` on each member. A pin name that is not declared fails validation.
- Every task of one pin must name the same pool and the same `run_as_orker_user`. Set `run_as_orker_user: true` on each one.
- The pin's tasks run on one Orker. They share `/orkstack/shared`, which is read-write.
- A pin does not order tasks. Use `needs:` for order. Pinned tasks with no `needs:` between them run at the same time.
- A retry runs on the same Orker. It sees the shared directory as the last attempt left it.
- When the pin closes, its directory is removed. Copy a file that must survive into `/orkstack/output/artifacts/`.
- An awaited task cannot join a pin. A composed task cannot either. A pinned task cannot be cached.

## When a pin fails

| Code | Meaning |
|---|---|
| `PIN_UNAVAILABLE` | Every pin host in the pool stayed full for the pin creation timeout, 5 minutes unless `ORK_PIN_CREATION_TIMEOUT` says otherwise. The task fails before it reaches an Orker, and no directory is made for it. |
| `PIN_HOST_LOST` | The Orker hosting the pin stopped answering. The run fails. The pin never moves to another Orker, because its directory is not there. |
| `PIN_TIMEOUT` | The pin was open for its whole lifetime. |

## Limits

- A pool with no pin host refuses a pinned task at submit.
- The default for `ORK_ORKER_MAX_PINS` is `0`, which hosts no pins. Set it on every Orker of the pool. Mixed settings make the submit check unreliable.

## See also

- [DSL reference, “Pins”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#pins): every rule and failure code
- [DSL reference, “Tasks”](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#tasks): the `pool:` and `pin:` keys
- [ORKFLOW_PATTERNS, “Share a working tree: pins”](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_PATTERNS.md#share-a-working-tree-pins): the pattern this example shows
- [DEPLOY, “Pin hosting is a pool-wide setting”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#pin-hosting-is-a-pool-wide-setting): the operator setting
