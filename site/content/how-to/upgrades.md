# Upgrades

Upgrade by starting the new release beside the old one, sending it new runs, and stopping the old one once it drains. Read the changelog first, and roll back if a step fails.

## Read the changelog

Read every changelog section between your version and the one you install. Entries marked **BREAKING** say what you must do before or during the upgrade. The [changelog](/docs/reference/changelog/) lists them by version.

## Check before you start

```bash
ork --config /etc/orkstack/orkstack.yaml --check   # validates the file, starts nothing
orkctl admin info                                  # the build and versioning state of the replica that answers
orkctl release status                              # current, ramping and candidate releases, and what blocks each
```

`--check` exits `0` or `1`, and names the first error. `admin info` describes the replica that answers, so check each one.

## Set the release identity

Set `ORK_DEPLOYMENT_NAME` and `ORK_BUILD_ID` on every `ork` and every `orker`. The two binaries of one release share them, and the pinning guarantee depends on that. Each binary prints the version it resolved at boot, and the two lines must match.

Versioning is off while `ORK_BUILD_ID` is unset. Set the same build id on every process to turn it on, as [Turn versioning on](#turn-versioning-on) describes.

Use the `orkctl` of the release you operate. An older `orkctl` that writes a held namespace drops the hold.

## Roll forward

1. Start every `ork` replica of the new build, and an `orker` in every pool it serves. Keep the old processes running.
2. Check the new release with `orkctl release status`. It reads `ready` when nothing blocks it.
3. Optionally send a share of new runs to it: `orkctl release ramp "$NEW" --percent 10`.
4. Make it current: `orkctl release promote "$NEW"`.
5. Wait for the old release to drain: `orkctl release wait "$OLD" --timeout 2h`. It exits `0` once no run of the old release is executing, and no Orker of it holds an open pin.
6. Stop the old processes only after `release wait` exits `0`. Drain any Orker that hosts a pin first.

The full procedure, with its output, is in [Roll out a release](/docs/operate/roll-out-a-release/).

## Do not apply new constructs too early

A release that predates a construct cannot run it correctly. Apply a document that uses a new construct, such as `await:`, `each:`, `join:`, `cache:` or a composed task, only after the release that added it is current. Schedules must be paused or deleted before a rollback past the release that introduced them.

## Roll back

```bash
orkctl release rollback "$OLD" --yes
orkctl release wait "$NEW" --timeout 2h
```

New runs go back to the old release at once. Runs already on the new release finish there, so keep its processes running until `release wait` exits `0`.

Some rollbacks need preparation first. Each one below names what to change, then a wait for the runs that use it to finish:

- join policies: hold each namespace whose versions declare a failure handler, then roll back;
- a fan-out maximum above 1,000 items: lower `engine.each_max_items` to 1,000;
- definitions passed by reference: set `engine.definition_by_reference` to `false` on every replica, then wait for the runs started that way to finish;
- snapshot overlays: set `engine.snapshot_overlays` to `false` on every replica, then wait for the runs started while it was on to finish;
- the Temporal Go SDK 1.50, which is release 0.1.1, with versioning off: hold each namespace that takes runs, wait until `orkctl run list -n NS --filter 'status=PENDING,RUNNING'` lists no run, roll back, then release each hold with `orkctl namespace update NS --release-hold`. Count every open run, however old. Resolve or cancel an awaited task rather than wait for its deadline. With versioning on, this case needs nothing more: a run that 0.1.1 started finishes on 0.1.1.

Do not roll back below the release that introduced reading runs carried into a fresh history. Every release from 0.1.0 contains it.

## Turn versioning on

Versioning is off while `ORK_BUILD_ID` is unset. A run that started before versioning has nothing pinning it. When the first versioned release becomes current, such a run moves onto that release at its next task, so a partly finished run can meet a different build. To avoid that, drain first:

1. Hold each namespace that takes runs: `orkctl namespace update NS --hold "versioning rollout"`.
2. Let the running runs finish. `orkctl run list -n NS --filter 'status=PENDING,RUNNING'` lists the ones still open.
3. Make the first versioned release current: `orkctl release promote "$FIRST"`. It refuses while a run that started before versioning is still executing.
4. Release each hold: `orkctl namespace update NS --release-hold`.

Set `ORK_DEPLOYMENT_NAME` and `ORK_BUILD_ID` on every process first, as [Set the release identity](#set-the-release-identity) says.

## Check the defaults

Some defaults changed in later releases. For example, `orker.max_concurrent` is `16`, and it used to be the Temporal SDK's own `1000`. The generated defaults file shows each default on the release you run:

```bash
ork --defaults
```

Set any key yourself if you need an older value.

## See also

- [DEPLOY.md, "2. One deployment spans both binaries"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#2-one-deployment-spans-both-binaries): the shared deployment and build identity
- [DEPLOY.md, "3. Rolling a release forward"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#3-rolling-a-release-forward): each step, in full
- [DEPLOY.md, "4. Rolling back"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#4-rolling-back): rollback and the runs that stay on the newer release
- [UPGRADING.md, "Upgrading the deployment"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#upgrading-the-deployment): the upgrade rules
- [UPGRADING.md, "Breaking changes"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#breaking-changes): the changes that need action
- [UPGRADING.md, "Defaults that moved"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#defaults-that-moved): settings whose defaults changed
- [UPGRADING.md, "Rolling back"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#rolling-back): the floors and the preparation for each
- [CHANGELOG.md](https://github.com/orkstack/orkstack/blob/main/CHANGELOG.md): every release's changes
- [Roll out a release](/docs/operate/roll-out-a-release/): the rollout steps, with example output
