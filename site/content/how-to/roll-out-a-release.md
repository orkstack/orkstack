# Roll out a release

Start the new release beside the old one, send it new runs, and stop the old one after its runs finish. A run stays on the release that started it, so a run in progress never moves.

## Example

Replace the build ids with your own. Start the new `ork` and `orker` processes with the new build id, one set on each host:

```bash
NEW=2026.08.23-c4d5e6f
OLD=2026.08.16-a1b2c3d

ORK_BUILD_ID=$NEW ork --config orkstack.yaml     # on each engine host
ORK_BUILD_ID=$NEW orker --config orkstack.yaml   # on each Orker host

orkctl release status                     # the new release reads "ready" when nothing blocks it
orkctl release promote "$NEW"             # new runs start on the new release
orkctl release wait "$OLD" --timeout 2h   # exits 0 once the old release has drained
```

## Before you start

- Every `ork` replica, and an Orker in every pool the release serves, must run the new build id.
- A pool that hosts pins needs a pin host on the new build.
- `orkctl release status` lists every blocker and what to start. `ramp` and `promote` refuse, with nothing changed, while a blocker stands.

## The steps

1. **Start the new release beside the old.** Keep the old processes running. The two releases share one address and one set of pools.
2. **Check the new release.** `orkctl release status` shows `ready` when nothing blocks it.
3. **Ramp, if you want to.** `orkctl release ramp "$NEW" --percent 10` sends 10% of new runs to the new release. Watch your error rate, then raise the share. `--percent 0` sends every new run back to the old release.
4. **Promote.** `orkctl release promote "$NEW"` makes the new release take every new run. The command names the current release and asks first. In a script, add `--yes`.
5. **Wait for the old release to drain.** `orkctl release wait "$OLD" --timeout 2h` exits `0` when no run of the old release is executing and no Orker of it holds an open pin. Exit `4` means the timeout passed with work left. The command names what remains.
6. **Stop the old processes.** Do this only after `release wait` exits `0`. Drain an Orker that hosts a pin before you stop it: `orkctl orker drain <orker-id>`.

## Roll back

```bash
orkctl release rollback "$OLD"
orkctl release wait "$NEW" --timeout 2h
```

New runs go back to the old release at once. Runs already on the new release finish there. Keep the new processes running until its `release wait` exits `0`.

## Releases that add features

An older release cannot run every Orkflow construct. Apply a document that uses a new construct only after the release that added it is current. Constructs such as `await:`, `each:`, `join:` and `cache:` behave differently on an older release. Before you roll back past a construct, stop applying documents that use it.

## Turning versioning on for a running system

Versioning on a system that already runs unversioned work is a separate procedure. Drain first: hold each namespace, let running runs finish, then promote the first release. The full procedure is in the deployment manual.

## Limits

- A release that cannot take new runs is refused. The refusal names the reason.
- Drain is a loop in `orkctl`. Nothing on the server holds rollout state.
- A rollback needs the old release's processes to be running. Once they are gone, the rollback is refused.

## See also

- [DEPLOY.md, “3. Rolling a release forward”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#3-rolling-a-release-forward): each step, with its output
- [DEPLOY.md, “4. Rolling back”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#4-rolling-back): rollback and the runs that stay on the newer release
- [DEPLOY.md, “3.5 Turning versioning on”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#35-turning-versioning-on-for-a-system-that-is-already-running): the first release on a running system
- [UPGRADING.md, “Upgrading the deployment”](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#upgrading-the-deployment): the upgrade rules
- Command reference: [orkctl](/docs/reference/cli/)
