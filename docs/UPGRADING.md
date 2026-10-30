# Upgrading OrkStack

This guide is the procedure for moving OrkStack forward and back. It starts with the rule for breaking changes, then covers Orkflow versions and the deployment itself. Each step names the reference that holds the detail, and the commands are the ones the references give.

## Breaking changes

- Every breaking change is listed in [CHANGELOG.md](../CHANGELOG.md), in the **Changed** or **Removed** section, and marked **BREAKING**. The changelog follows the Keep a Changelog 1.1.0 format named in its header.
- OrkStack makes no promise of deprecation windows beyond that listing.
- Releases follow Semantic Versioning from the first, 0.1.0 ([CHANGELOG.md](../CHANGELOG.md), header). Its section says only "First release.": there is no earlier release to upgrade from, so it lists no breaking change. Every release is on the [releases page](https://github.com/orkstack/orkstack/releases).

Before you upgrade, read the **BREAKING** entries in the section of every release after the one you run. Each release's section is also the notes on its GitHub release page.

## Release artifacts

Each release is published by `release.yml`, and only after its pre-release
proof passed, the nightly of its commit passed, the quickstart's compose file
ran end to end on the images it publishes, the README's Quickstart ran as
written on a clone of its commit, and a maintainer approved the publication
([PROOFS.md](./PROOFS.md) §5). It publishes:

- **On the GitHub release**, under names that carry no version, so that
  `https://github.com/orkstack/orkstack/releases/latest/download/<name>`
  always names the newest one:
  - `orkctl-linux-amd64.tar.gz`, `orkctl-linux-arm64.tar.gz`,
    `orkctl-darwin-amd64.tar.gz`, `orkctl-darwin-arm64.tar.gz`,
    `orkctl-windows-amd64.zip`, `orkctl-windows-arm64.zip`;
  - `ork-linux-amd64.tar.gz`, `ork-linux-arm64.tar.gz` (the `ork` binary
    carries the web UI and serves it on its API port),
    `orker-linux-amd64.tar.gz`, `orker-linux-arm64.tar.gz`;
  - `SHA256SUMS`, over every file above: `sha256sum --ignore-missing -c
    SHA256SUMS` checks the ones you downloaded;
  - `SHA256SUMS.sig`, the release key's cosign signature over `SHA256SUMS`,
    and `cosign.pub`, that key's public half: the same file as
    `deploy/cosign.pub` at the release's tag.
- **On `ghcr.io/orkstack`**, for linux/amd64 and linux/arm64: `ork`, `orker`,
  `orkctl`, and the six first-party activity images `activity-shell`,
  `activity-http`, `activity-file`, `activity-json`, `activity-git` and
  `activity-template`. There is no UI image: `ork` serves the UI. Each is
  tagged `X.Y.Z`, which never moves, and `X.Y`, which moves to each patch
  release of that line. Deploy by digest, or by `X.Y.Z`.

Each image is signed by its digest with the release key, in the layout
OrkStack's image policy verifies, so a namespace can require that key
(`--require-signed-by`). [DEPLOY.md](./DEPLOY.md), "Image supply-chain
policy", says how to trust it, and how to verify an image or `SHA256SUMS` by
hand.

The built-in activity catalog names the activity images by their `X.Y` tag,
so a release's `ork` resolves `uses: shell` to that release line's image at
apply time and pins its digest; `release.yml` refuses a version whose `X.Y`
the catalog does not name. The notes on the GitHub release page are the
version's `## [X.Y.Z]` section of [CHANGELOG.md](../CHANGELOG.md), word for
word, without its heading.

Running the published images is set out in [DEPLOY.md](./DEPLOY.md),
"Running the published images". A deployment that turns versioning on sets
`ORK_BUILD_ID` itself, one value per release, as with any other build.

## Orkflow versions

An Orkflow version is immutable. Applying an edit creates a new version, and applying an unchanged file writes nothing ([DSL.md](./DSL.md), "`version`"). A run pins the version it started with and the definition digest that version carries ([AGENTS.md](../AGENTS.md) §2.2), so it finishes on the definition it began with.

The current pointer is the only thing that moves what starts. Until someone promotes an Orkflow, it runs its most recently applied version. Promoting moves the pointer for new runs only, and applying a new version does not move it ([DSL.md](./DSL.md), "Which version runs").

```bash
orkctl apply deploy.ork.yaml                 # an edited file: a new version; an unchanged one: nothing written
orkctl orkflow versions deploy               # every version, newest first, CURRENT marked
orkctl orkflow diff deploy 1.3.0 1.4.0       # what text changed between two versions
orkctl orkflow compat deploy 1.4.0           # what promoting 1.4.0 would break, from its inputs: and outputs:
orkctl orkflow callers deploy                # the composing Orkflows, triggers and schedules that name deploy
orkctl orkflow promote deploy 1.4.0          # move the current pointer for new runs
orkctl orkflow promote deploy 1.4.0 --acknowledge-breaking   # only when compat reports a breaking change
orkctl orkflow promote deploy 1.3.0          # roll back: the same command, gated the same way
```

Promotion compares the declared interface of the target with the version that runs now. The declared interface is `inputs:`, with `type`, `required` and `default`, and `outputs:`, by name. An input added as required with no default, an input removed, an input retyped, and an output removed are breaking. A breaking promotion is refused until you pass `--acknowledge-breaking`. Rolling back is gated the same way: rolling back from a version that added an input removes that input, so the rollback needs the flag too ([DSL.md](./DSL.md), "Promoting compares declared interfaces").

Applying is never refused for a breaking change. The apply stores the version with warnings at the fields that changed. A composing Orkflow pins its child's version, so it meets a breaking change only when its author moves the pin ([DSL.md](./DSL.md), "Promoting compares declared interfaces").

## Upgrading the deployment

### One release at a time

`ork` and `orker` of one release are built and deployed together. They share `ORK_DEPLOYMENT_NAME` and `ORK_BUILD_ID`, and the pinning guarantee depends on that ([DEPLOY.md](./DEPLOY.md), "2. One deployment spans both binaries"). Set both variables on every `ork` and every `orker`. Each binary prints the version it resolved at boot, and the two lines must match.

Use the `orkctl` of the release you are operating. An older `orkctl` writing a held namespace drops the hold, and a release that predates a field discards that field ([DEPLOY.md](./DEPLOY.md), "Namespace admission: `hold` and `max_running`").

Before you start, check the configuration and the fleet:

```bash
ork --check --config /etc/orkstack/orkstack.yaml   # validates the file and prints the effective configuration; starts nothing
orkctl admin info                                  # the build and versioning state of the replica that answers
orkctl release status                              # current, ramping and candidate releases, and what blocks each
```

`--check` exits 0 or 1, naming the first error. An unknown key is refused at boot, naming its full path, and an environment variable overrides the file ([DEPLOY.md](./DEPLOY.md), "1. Configuration").

### Rolling forward

Steps 0, 2, 3, 4 and 5 are `orkctl release` commands, and any `ork` replica answers them. Steps 1 and 6 start and stop processes. The cluster's routing is the only record of which release takes new runs, so nothing needs to be stored between steps ([DEPLOY.md](./DEPLOY.md), "3. Rolling a release forward").

0. **First versioned release only.** Until a release is current, it runs nothing. Make the first release current before you rely on it:

   ```bash
   orkctl release promote "$FIRST"
   ```

   `promote` is refused while the release cannot carry new runs, and the refusal names what to start.

1. **Start the new release alongside the old.** Run every `ork` replica of the new build, and an `orker` in every pool the release dispatches to. Where the old release hosts pins, the new one needs a pin host too. Do not stop anything yet. While both releases' replicas serve the web UI behind one address, a page of one release can miss an asset on a replica of the other and have to load again; session affinity avoids it ([DEPLOY.md](./DEPLOY.md), "The web UI").

2. **Check that the new release can carry the work.**

   ```bash
   orkctl release status
   ```

   The new release reads `ready` when nothing blocks it. Otherwise each blocker is listed with what to start.

3. **Ramp, optionally.** Send a share of new runs to the new release, and watch error rates:

   ```bash
   orkctl release ramp "$NEW" --percent 10
   ```

   `--percent 0` returns every new run to the old release.

4. **Make the new release current.**

   ```bash
   orkctl release promote "$NEW"
   ```

   From then on every new run executes on the new release. Every run already in flight stays on the old one and finishes there.

5. **Wait for the old release to drain. Do not skip this step.**

   ```bash
   orkctl release wait "$OLD" --timeout 2h
   ```

   The command exits 0 only when the cluster reports the old release drained, no Orker of it hosts an open pin, and every Orker answered. It exits 4 when `--timeout` passes first, and 1 when the old release still takes new runs, when its state is unknown, or when the engine stops answering. A run holds its release while it waits on an awaited task (up to 30 days), while a fan-out item is unsettled, while a composed child runs, and while a failing run's failure handlers run.

6. **Stop the old release** once `release wait` exits 0. Stop every replica and every Orker of the old build. An Orker that hosts an open pin must not stop before its pin closes; `orkctl orker drain` waits for that ([DEPLOY.md](./DEPLOY.md), "Removing an Orker safely: cordon and drain").

Stopping the old release while it still drains strands its runs. Their remaining tasks stay unstarted, and the runs stay `RUNNING` until the old workers return. The new release does not pick those tasks up ([DEPLOY.md](./DEPLOY.md), "3. Rolling a release forward").

Never decommission the previous release until the next one is proven. A pinned run must finish on the build that started it.

### Upgrading Orkflows with the release

A stored Orkflow version outlives the release that applied it. An older engine refuses to apply a construct it does not know. Asked to run a stored version that uses one, it drops the unknown fields. So apply a document that uses a new construct only once the release that introduced it is current, or on every replica for an unversioned deployment ([DEPLOY.md](./DEPLOY.md), "Releases that add Orkflow constructs").

What an older release does with each construct, from the same reference:

| Construct | An older release running a stored version that uses it |
|---|---|
| `await:` | Treats the await as a container task and fails it. The failure is explicit |
| `each:` | Runs the node once as a plain task, staging no items for its dependants. Silent unless `with:` reads `.each`, which fails the task |
| `uses: orkflow:` | Cannot decode the start, so the run waits for a newer worker, which under a pinned run may never come. A composed task that reaches an older Orker fails `IMAGE_NOT_PINNED` |
| `outputs:` | Returns the leaf aggregation instead of the declared outputs. Silent |
| `join:` | Runs every task as `all_succeeded`. A failure handler that uses `any_failed` then runs after its needs succeeded. Silent and destructive |
| `cache:` | Runs every task uncached. Memos left behind are inert |
| schedules | Cannot fire. Pause or delete every schedule first with `orkctl schedule pause` or `orkctl schedule delete` |

Some runs need a newer engine than the one that could otherwise decide them. A submit that needs such a run is refused while an older `ork` process is polling, and the refusal names the count of processes. A fan-out above 1,000 items, and a definition passed by reference, are the two refusals of this kind. A snapshot overlay is never refused: a run whose engines cannot all be sure of overlays writes full snapshots instead ([DEPLOY.md](./DEPLOY.md), "Engine capabilities and the fleet check"). Submit refuses a run whose fan-out, cached task or failure handler would reach a pool whose Orkers predate that feature, so run every Orker of one pool on one release ([DEPLOY.md](./DEPLOY.md), "Fan-out, caching and failure handlers need every Orker of a pool on a release that supports them").

### Rolling back

```bash
orkctl release rollback "$OLD" --yes     # --yes is required only when standard input is not a terminal
orkctl release wait "$NEW" --timeout 2h
```

`rollback` makes a release that was current before the current one current again, and runs the same check as `promote`. New runs go back to the old release at once. Runs already pinned to the new release keep finishing there, so keep the new release's workers running until they drain ([DEPLOY.md](./DEPLOY.md), "4. Rolling back").

Rolling back past a release that added Orkflow constructs needs the preparation in the table above. Four cases need more than a rollback:

| The release being rolled back past added | Before you roll back |
|---|---|
| Join policies | Hold each namespace whose current versions declare a failure handler: `orkctl namespace update NS --hold "rolling back past join policies"`. Roll back, then release the hold only once no current version declares one, or after you roll forward again ([DEPLOY.md](./DEPLOY.md), "Releases that add Orkflow constructs") |
| A fan-out maximum above 1,000 items | Lower `engine.each_max_items` to at most 1,000, then wait until every run created before the change has finished ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |
| Definitions passed by reference | Set `engine.definition_by_reference` to `false` on every replica, then wait until every run started with its definition by reference has finished. An older release cannot read such a run's definition ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |
| Snapshot overlays | Set `engine.snapshot_overlays` to `false` on every replica, then wait until every run started while it was on has finished ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |

Two floors do not move. Never roll back below the first release that reads runs carried into a fresh history. Turn `engine.continuation.enabled` off first, and let the runs that carry on finish. Also, once any run's archive records a lost range of its events (a gap line), a rollback below the release that introduced that line is forbidden for good, because an older release refuses the line ([DEPLOY.md](./DEPLOY.md), "4. Rolling back").

### Turning versioning on for a running system

A run that started before versioning has nothing pinning it. When the first release becomes current, such a run upgrades onto that release at its next workflow task. A partial history therefore meets a different build, and pinning cannot protect it ([DEPLOY.md](./DEPLOY.md), "3.5 Turning versioning on for a system that is already running").

The safe path is to drain first:

1. Hold each namespace that takes runs: `orkctl namespace update NS --hold "versioning rollout"`.
2. Let the running runs finish.
3. Make the first release current, as in step 0 above.

`orkctl release promote` refuses while a run started before any release was current is still executing. The live switch, without draining, is a raw cluster procedure. It is safe only when the versioned release is the same binary as the unversioned one already serving, and OrkStack cannot verify that for you ([DEPLOY.md](./DEPLOY.md), "3.5 Turning versioning on for a system that is already running").

### Replay across releases

A run keeps the build that started it, so a pinned run finishes on old code. The reverse case, where current code replays a history an older build wrote, is guarded. The recorded histories in [internal/engine/testdata/history/README.md](../internal/engine/testdata/history/README.md) are replayed by the test suite, and a change that would alter a recorded command stream fails them. A deliberate change needs a version gate, a new recorded history, or a fixture re-recorded with a reason in its commit ([DEPLOY.md](./DEPLOY.md), "6. Replay safety across versions").

### Defaults that moved

Some defaults changed after their first release. Read the setting in the generated [deploy/orkstack.defaults.yaml](../deploy/orkstack.defaults.yaml) on the release you run, and set the key yourself if you need the older behaviour.

| Setting | What changed | What to do |
|---|---|---|
| `orker.max_concurrent` (`ORK_ORKER_MAX_CONCURRENT`) | The default is 16 tasks at once. Before 2026-10 it was the Temporal SDK's own 1000, so an Orker that relied on it now takes at most 16 tasks, and the rest wait in the pool | Set the key to the size your host supports ([DEPLOY.md](./DEPLOY.md), "Sizing an Orker's task ceiling") |
| `engine.continuation.enabled` (`ORK_CONTINUATION_ENABLED`) | On by default since the release after the one that first carried runs on | To turn it off, wait for the runs that carry on to finish before a rollback below that release ([DEPLOY.md](./DEPLOY.md), "1. Configuration") |
| `engine.definition_by_reference` (`ORK_DEFINITION_BY_REFERENCE`) | On by default from the release after the one that first had it | Before a rollback below that release, set it `false` on every replica and wait for the runs started with it ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |
| `engine.snapshot_overlays` (`ORK_SNAPSHOT_OVERLAYS`) | On by default from the release after the one that first had it | Before a rollback below that release, set it `false` on every replica and wait for the runs started while it was on ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |
