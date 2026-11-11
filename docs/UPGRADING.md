# Upgrading OrkStack

This guide is the procedure for moving OrkStack forward and back. It starts with the rule for breaking changes, then covers Orkflow versions and the deployment itself. Each step names the reference that holds the detail, and the commands are the ones the references give.

## Breaking changes

- Every breaking change is listed in [CHANGELOG.md](../CHANGELOG.md), in the section of its kind (most often **Changed** or **Removed**), and marked **BREAKING**. The changelog follows the Keep a Changelog 1.1.0 format named in its header.
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
  - `SHA256SUMS.sigstore.json`, the release key's cosign signature over
    `SHA256SUMS` as cosign's bundle, `SHA256SUMS.sig`, the same signature
    alone (for openssl; v0.1.0 attaches only this one), and `cosign.pub`,
    that key's public half: the same file as `deploy/cosign.pub` at the
    release's tag.
- **On `ghcr.io/orkstack`**, for linux/amd64 and linux/arm64: `ork`, `orker`,
  `orkctl`, and the six first-party activity images `activity-shell`,
  `activity-http`, `activity-file`, `activity-json`, `activity-git` and
  `activity-template`. There is no UI image: `ork` serves the UI. Each is
  tagged `X.Y.Z`, which never moves, and `X.Y`, which moves to each patch
  release of that line. Deploy by digest, or by `X.Y.Z`.

Each image is signed by its digest with the release key, in a layout
OrkStack's image policy verifies, so a namespace can require that key
(`--require-signed-by`). v0.1.0's images are signed in cosign's legacy
`.sig` layout; 0.2.0 and later sign in cosign 3's bundle, which an engine or
Orker of v0.1.0 does not read: it refuses such an image `IMAGE_NOT_SIGNED`.
An `X.Y` tag moves only within its release line, so 0.2.0 leaves the `0.1` tag
v0.1.0's built-in catalog names on v0.1.0's images, and a deployment that stays
on v0.1.0 is unaffected by it. A later 0.1 release would move that tag to
bundle-signed images, so a v0.1.0 deployment that requires a signer and must
stay there names v0.1.0's images by digest in `ORK_ACTIVITY_CATALOG`.
Upgrade the Orkers with the engines: once an engine is upgraded it accepts a
bundle-signed image when an Orkflow is created and at submit, and where the run's namespace
requires a signer, a v0.1.0 Orker still polling refuses that run's task at
dispatch with `IMAGE_NOT_SIGNED`, a contract violation, which is never
retried, so the run fails. A namespace that requires no signer is not
affected.
[DEPLOY.md](./DEPLOY.md), "Image supply-chain policy", says how to trust the
key, and how to verify an image or `SHA256SUMS` by hand.

The built-in activity catalog names the activity images by their `X.Y` tag,
so a release's `ork` resolves `uses: shell` to that release line's image when
an Orkflow is created and pins its digest; `release.yml` refuses a version whose `X.Y`
the catalog does not name. The notes on the GitHub release page are the
version's `## [X.Y.Z]` section of [CHANGELOG.md](../CHANGELOG.md), word for
word, without its heading, followed by each image's digest and the commands
that verify it.

Running the published images is set out in [DEPLOY.md](./DEPLOY.md),
"Running the published images". A deployment that turns versioning on sets
`ORK_BUILD_ID` itself, one value per release, as with any other build.

## Orkflow versions

An Orkflow version is immutable. Creating an edit stores a new version, and creating an unchanged file again writes nothing ([DSL.md](./DSL.md), "`version`"). That holds across this release's change to assigned versions: a document stored under its sha256 hash stays that version, and its next edit lands a UUIDv7 beside it. During a rolling upgrade, while a replica of the older release runs beside a new one, one unpinned document created on both can be stored twice: under its hash by the old replica and under a UUIDv7 by the new one, with the head on whichever landed last. Both are valid, and nothing needs repairing. Rolled back to a release that derives hashes, a document first created under a UUIDv7 is given a hash version the next time it is created; the versions it has remain, and nothing is rewritten. A run pins the version it started with and the definition digest that version carries ([AGENTS.md](../AGENTS.md) §2.2), so it finishes on the definition it began with.

The current pointer is the only thing that moves what starts. Until someone promotes an Orkflow, it runs its most recently created version. Promoting moves the pointer for new runs only, and creating a new version does not move it ([DSL.md](./DSL.md), "Which version runs").

```bash
orkctl orkflow create deploy.ork.yaml        # an edited file: a new version; an unchanged one: nothing written
orkctl orkflow versions deploy               # every version, newest first, CURRENT marked
orkctl orkflow diff deploy 1.3.0 1.4.0       # what text changed between two versions
orkctl orkflow compat deploy 1.4.0           # what promoting 1.4.0 would break, from its inputs: and outputs:
orkctl orkflow callers deploy                # the composing Orkflows, triggers and schedules that name deploy
orkctl orkflow promote deploy 1.4.0          # move the current pointer for new runs
orkctl orkflow promote deploy 1.4.0 --acknowledge-breaking   # only when compat reports a breaking change
orkctl orkflow promote deploy 1.3.0          # roll back: the same command, gated the same way
```

Promotion compares the declared interface of the target with the version that runs now. The declared interface is `inputs:`, with `type`, `required` and `default`, and `outputs:`, by name. An input added as required with no default, an input removed, an input retyped, and an output removed are breaking. A breaking promotion is refused until you pass `--acknowledge-breaking`. Rolling back is gated the same way: rolling back from a version that added an input removes that input, so the rollback needs the flag too ([DSL.md](./DSL.md), "Promoting compares declared interfaces").

Creating a version is never refused for a breaking change. The create stores the version with warnings at the fields that changed. A composing Orkflow pins its child's version, so it meets a breaking change only when its author moves the pin ([DSL.md](./DSL.md), "Promoting compares declared interfaces").

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

Every Orker runs its boot probe's image before it takes a task, and refuses to start, `BIND_PROBE_IMAGE_UNAVAILABLE`, when it is not local and cannot be pulled ([REASONS.md](./REASONS.md)). A release that moves that image says so in its CHANGELOG section, marked **BREAKING**. On an air-gapped or rate-limited host, mirror or load the new image before the release's Orkers start: from 0.2.0, which moved the boot probe to Alpine 3.24, it is `docker.io/library/alpine:3.24@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a`.

### Rolling forward

Steps 0, 2, 3, 4 and 5 are `orkctl release` commands, and any `ork` replica answers them. Steps 1 and 6 start and stop processes. The cluster's routing is the only record of which release takes new runs, so nothing needs to be stored between steps ([DEPLOY.md](./DEPLOY.md), "3. Rolling a release forward").

0. **First versioned release only.** Until a release is current, it runs nothing. Make the first versioned release current before you rely on it:

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

A stored Orkflow version outlives the release that created it. An older engine refuses to create a version that uses a construct it does not know. Asked to run a stored version that uses one, it drops the unknown fields. So create a version that uses a new construct only once the release that introduced it is current, or on every replica for an unversioned deployment ([DEPLOY.md](./DEPLOY.md), "Releases that add Orkflow constructs").

What an older release does with each construct, from the same reference:

| Construct | An older release running a stored version that uses it |
|---|---|
| `await:` | Treats the await as a container task and fails it. The failure is explicit |
| `each:` | Runs the node once as a plain task, staging no items for its dependants. Silent unless `with:` reads `.each`, which fails the task |
| `uses: orkflow:` | Cannot decode the start, so the run waits for a newer worker, which under a pinned run may never come. A composed task that reaches an older Orker fails `IMAGE_NOT_PINNED` |
| `outputs:` | Returns the values of the tasks no other task needs instead of the declared outputs. Silent |
| `join:` | Runs every task as `all_succeeded`. A failure handler that uses `any_failed` then runs after its needs succeeded. Silent and destructive |
| `cache:` | Runs every task uncached. Memos left behind are inert |
| schedules | Cannot fire. Pause or delete every schedule first with `orkctl schedule pause` or `orkctl schedule delete` |

From v0.2.0, a `retry:` with a negative interval, a `maximum_interval_seconds` below its `initial_interval_seconds`, or a `backoff_coefficient` that is set below 1 or not finite ([DSL.md](./DSL.md), rule 47) is refused whatever its `maximum_attempts`: validate reports an error at the field, and apply and plan refuse the document. A version stored before keeps starting, its retry raised to the nearest policy the cluster accepts, so fix the document before you next apply it.

A run that v0.1.0 already left stuck on such a retry, or on a `retry:` whose `initial_interval_seconds` is above 100 and that sets no maximum, stays `RUNNING` with that task never starting: the cluster refuses its policy each time the run schedules it. On a deployment with versioning ([DEPLOY.md](./DEPLOY.md), "3. Rolling a release forward"), the run stays on v0.1.0, never reaches the new release's raise, and keeps `orkctl release wait` on v0.1.0 from exiting 0. Before you wait for v0.1.0 to drain, find such runs — `orkctl run list -F status=RUNNING` in each namespace lists the candidates, and `orkctl run get` shows the retrying task still `PENDING` long after its needs ended — and cancel each with `orkctl run cancel`. If one is still `RUNNING` afterwards, terminate it with `temporal workflow terminate --workflow-id <run id> --reason "stuck on a refused retry" --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"`, which ends it `RUN_TERMINATED`. Submit the work again on the new release if it is still needed. On a deployment without versioning, the new release's engine takes the stuck run's next attempt and schedules the raised policy.

Some runs need a newer engine than the one that could otherwise decide them. A submit that needs such a run is refused while an older `ork` process is polling, and the refusal names the count of processes. A fan-out above 1,000 items, and a definition passed by reference, are the two refusals of this kind. A snapshot overlay is never refused: a run whose engines cannot all be sure of overlays writes full snapshots instead ([DEPLOY.md](./DEPLOY.md), "Engine capabilities and the fleet check"). Submit refuses a run whose fan-out, cached task or failure handler would reach a pool whose Orkers predate that feature, so run every Orker of one pool on one release ([DEPLOY.md](./DEPLOY.md), "Fan-out, caching and failure handlers need every Orker of a pool on a release that supports them").

### Rolling back

```bash
orkctl release rollback "$OLD" --yes     # --yes is required only when standard input is not a terminal
orkctl release wait "$NEW" --timeout 2h
```

`rollback` makes a release that was current before the current one current again, and runs the same check as `promote`. New runs go back to the old release at once. Runs already pinned to the new release keep finishing there, so keep the new release's workers running until they drain ([DEPLOY.md](./DEPLOY.md), "4. Rolling back").

Rolling back past a release that added Orkflow constructs needs the preparation in the table above. Five cases need more than a rollback:

| The release being rolled back past added | Before you roll back |
|---|---|
| Join policies | Hold each namespace whose current versions declare a failure handler: `orkctl namespace update NS --hold "rolling back past join policies"`. Roll back, then release the hold only once no current version declares one, or after you roll forward again ([DEPLOY.md](./DEPLOY.md), "Releases that add Orkflow constructs") |
| A fan-out maximum above 1,000 items | Lower `engine.each_max_items` to at most 1,000, then wait until every run created before the change has finished ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |
| Definitions passed by reference | Set `engine.definition_by_reference` to `false` on every replica, then wait until every run started with its definition by reference has finished. An older release cannot read such a run's definition ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |
| Snapshot overlays | Set `engine.snapshot_overlays` to `false` on every replica, then wait until every run started while it was on has finished ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |
| The Temporal Go SDK 1.50 (0.2.0) | With versioning on, nothing more: a run 0.2.0 started finishes on 0.2.0. With versioning off, hold each namespace that takes runs, wait until `orkctl run list -n NS -F 'status=PENDING,RUNNING'` lists no run in any of them, roll back, then release each hold: `orkctl namespace update NS --release-hold`. Count every open run, however old: any run open while 0.2.0 ran may carry the marker. A run waiting on an await stays open until the await is resolved or its deadline passes, so resolve or cancel it rather than wait. An older engine cannot replay a run in which 0.2.0 recorded an ordered cancellation, and the run stays `RUNNING` until a 0.2.0 engine picks it up. For the same reason, replace every engine replica of an unversioned fleet together when rolling forward to 0.2.0 ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |

Two floors do not move. Never roll back below the build that introduced reading runs carried into a fresh history; every release from 0.1.0 contains it. Turn `engine.continuation.enabled` off first, and let the runs that carry on finish. Also, once any run's archive records a gap in its events (a gap line), a rollback below the release that introduced that line is forbidden for good, because an older release refuses the line ([DEPLOY.md](./DEPLOY.md), "4. Rolling back").

### Turning versioning on for a running system

A run that started before versioning has nothing pinning it. When the first versioned release becomes current, such a run upgrades onto that release at its next workflow task. A partial history therefore meets a different build, and pinning cannot protect it ([DEPLOY.md](./DEPLOY.md), "3.5 Turning versioning on for a system that is already running").

The safe path is to drain first:

1. Hold each namespace that takes runs: `orkctl namespace update NS --hold "versioning rollout"`.
2. Let the running runs finish.
3. Make the first versioned release current, as in step 0 above.

`orkctl release promote` refuses while a run started before any release was current is still executing. The live switch, without draining, is a raw cluster procedure. It is safe only when the versioned release is the same binary as the unversioned one already serving, and OrkStack cannot verify that for you ([DEPLOY.md](./DEPLOY.md), "3.5 Turning versioning on for a system that is already running").

### Replay across releases

A run keeps the build that started it, so a pinned run finishes on old code. The reverse case, where current code replays a history an older build wrote, is guarded. The recorded histories in [internal/engine/testdata/history/README.md](../internal/engine/testdata/history/README.md) are replayed by the test suite, and a change that would alter a recorded command stream fails them. A deliberate change needs a version gate, a new recorded history, or a fixture re-recorded with a reason in its commit ([DEPLOY.md](./DEPLOY.md), "6. Replay safety across versions").

### Defaults that moved

Some defaults changed after the setting was introduced. Read the setting in the generated [deploy/orkstack.defaults.yaml](../deploy/orkstack.defaults.yaml) on the release you run, and set the key yourself if you need the older behaviour.

| Setting | What changed | What to do |
|---|---|---|
| `orker.max_concurrent` (`ORK_ORKER_MAX_CONCURRENT`) | The default is 16 tasks at once. Before 2026-10 it was the Temporal SDK's own 1000, so an Orker that relied on it now takes at most 16 tasks, and the rest wait in the pool | Set the key to the size your host supports ([DEPLOY.md](./DEPLOY.md), "Sizing an Orker's task ceiling") |
| `engine.continuation.enabled` (`ORK_CONTINUATION_ENABLED`) | On by default since the release after the one that first carried runs on | To turn it off, wait for the runs that carry on to finish before a rollback below that release ([DEPLOY.md](./DEPLOY.md), "1. Configuration") |
| `engine.definition_by_reference` (`ORK_DEFINITION_BY_REFERENCE`) | On by default from the release after the one that first had it | Before a rollback below that release, set it `false` on every replica and wait for the runs started with it ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |
| A task's `retry:` with no `maximum_interval_seconds` | From v0.2.0 the longest wait between two attempts is 60 seconds, or the initial interval when that is longer. It was 100 seconds. A run keeps the number it was submitted with, so runs in flight across the upgrade keep 100 seconds | To keep 100 seconds for new runs, set `maximum_interval_seconds: 100` in the task's `retry:` ([DSL.md](./DSL.md), "Tasks") |
| `engine.snapshot_overlays` (`ORK_SNAPSHOT_OVERLAYS`) | On by default from the release after the one that first had it | Before a rollback below that release, set it `false` on every replica and wait for the runs started while it was on ([DEPLOY.md](./DEPLOY.md), "4. Rolling back") |

## Upgrading from 0.1.0 to 0.2.0

Every **BREAKING** entry of 0.2.0's [CHANGELOG.md](../CHANGELOG.md) section is listed here with what to do. Do these before the release's engines and Orkers start. A stored Orkflow version is immutable, so a step that changes a document means creating and promoting a new version ([Orkflow versions](#orkflow-versions)).

| Change | What to do |
|---|---|
| An Orker's own secret sources | Move each task that names one to a source of its own, and set `ORK_ORKER_SECRET_ALLOW` ([The Orker's secret floor](#the-orkers-secret-floor)) |
| A `file:` secret source's name | Write it as a plain absolute path ([`file:` secret sources](#file-secret-sources)) |
| A request's `Host`, on both listeners | Set `ORK_ALLOWED_HOSTS` and `ORK_ADMIN_ALLOWED_HOSTS` ([Allowed hosts](#allowed-hosts)) |
| The images' signatures | Upgrade the Orkers with the engines ([Release artifacts](#release-artifacts)) |
| The Temporal Go SDK 1.50 | With versioning off, replace every engine replica together when rolling forward, and roll back below 0.2.0 only when no run is open ([Rolling back](#rolling-back)) |
| jq 1.8 in the first-party images | Check your filters and commands ([jq 1.8 in the first-party images](#jq-18-in-the-first-party-images)) |
| gomplate 5 in the `template` image | Replace the names it removed ([gomplate 5 in the `template` image](#gomplate-5-in-the-template-image)) |
| The boot probe's image | Mirror or load it on an air-gapped or rate-limited host ([One release at a time](#one-release-at-a-time)) |
| A `retry:` that is refused | Fix the document before you next apply it ([Upgrading Orkflows with the release](#upgrading-orkflows-with-the-release)) |

`uses: shell` and the other first-party names resolve to the images' `0.2` tag when an Orkflow is applied. A version already applied keeps the image it pinned, so an image's new tools reach a task when you next apply its Orkflow. A host that mirrors the images needs the `0.2` images before that apply.

### The Orker's secret floor

No `ORK_ORKER_SECRET_ALLOW` floor admits an Orker's own sources, whether it is empty or not. A task that names one fails `SECRET_NOT_PERMITTED`, after the commit check and a cached task's memo step and before its image is pulled, under `orkctl task run` as on an Orker. The refusal says only that the source is one of the Orker's variables or one of its files, never which.

[SECURITY_MODEL.md](./SECURITY_MODEL.md), "The Orker's own sources", has the whole list. In short:

- **Variables:** `env:ORK_*`, `env:OTEL_*`, the proxy settings (`env:HTTP_PROXY`, `env:HTTPS_PROXY`, `env:ALL_PROXY` and their lower-case forms), the container CLI's (`env:DOCKER_*`, `env:CONTAINERS_*`, `env:CONTAINER_HOST`, `env:CONTAINER_CONNECTION`, `env:CONTAINER_SSHKEY`, `env:CONTAINER_PASSPHRASE`, `env:REGISTRY_AUTH_FILE`), the registry credential helpers' (`env:AWS_*`, `env:AZURE_*`, `env:CLOUDSDK_*`, `env:GOOGLE_APPLICATION_CREDENTIALS`), and each telemetry sink header's source.
- **Files:** `/proc`, `/dev`, `/run/secrets` and `/var/run/secrets`; the work root (`ORK_ORKER_WORKDIR`; unset, the temp directory, `/tmp` on most Linux hosts); the Orker's configuration file and each `{file: PATH}` it reads; each telemetry sink's header file; the credential files its settings name; `ORK_ARTIFACT_DIR` when the artifact backend is `local`; the container CLI's configuration and the container engine's state. A symbolic or hard link to one of these is refused too.

Before the release's Orkers start:

1. **Find the tasks.** Search the `source:` of each `secrets:` entry in your Orkflow documents for those names. That includes a Docker or Compose secret read from `/run/secrets`, and a file under `/tmp` on an Orker with no `ORK_ORKER_WORKDIR`.
2. **Give each a source of its own.** Set the value on the Orker under a variable of its own, such as `TASK_DB_PASSWORD`, or mount it in a directory of the tasks' own, such as `/etc/orkstack/task-secrets`. Change the task's `source:` to `env:TASK_DB_PASSWORD` or `file:/etc/orkstack/task-secrets/db-password`, then apply and promote the new version. A composing Orkflow pins its child's version, so move that pin too.
3. **Set the floor on every Orker** to exactly those sources (`orker.secret_allow`):

   ```bash
   ORK_ORKER_SECRET_ALLOW='env:TASK_*,file:/etc/orkstack/task-secrets/*'
   ```

   Left empty, the floor resolves every other source of an allowed scheme, and the Orker logs a warning at boot that names the setting.
4. **Drop any floor entry that can match only the Orker's own sources**, such as `env:ORK_*`, `env:AWS_*` or `file:/run/secrets/*`: it stops the Orker at boot. So does an entry under the work root, such as `file:/tmp/*` on an Orker with no `ORK_ORKER_WORKDIR`. A broad entry such as `env:*` still boots, and does not reach the Orker's own sources.

A run already in flight keeps the version it started with, so it still names the old source. With versioning on, it finishes on the old release, whose Orkers keep running until `orkctl release wait` says that release has drained. With versioning off, let it finish or cancel it before its Orkers are replaced: a 0.2.0 Orker fails that task `SECRET_NOT_PERMITTED`, a contract violation that is never retried, so the run fails.

The Kubernetes bases in `deploy/kubernetes` already set the floor to `file:/etc/orkstack/task-secrets/*`, so a task on them that reads that Secret needs nothing ([DEPLOY.md](./DEPLOY.md), "Secrets on a shared pool").

### `file:` secret sources

A `file:` source must be a plain absolute path, with no `..`, `.` or empty segment, of at most 4096 bytes, under every floor, the empty one included. Any other name fails `SECRET_NOT_PERMITTED`. A non-empty floor already refused such a name, except one longer than 4096 bytes. Write it plainly: `file:/etc/orkstack/task-secrets/db-password`, not `file:/etc/orkstack/./task-secrets//db-password`.

### Allowed hosts

Both listeners answer `403` to a request whose `Host` they do not admit, which stops DNS rebinding through a public name. With nothing set, each admits an IP address, `localhost` and every name under it, a name with no dot such as the quickstart's `ork` or a Kubernetes Service from its own namespace, and the host its own address names (`ORK_HTTP_ADDR`, `ORK_ADMIN_ADDR`).

- **The API listener.** Set `ORK_ALLOWED_HOSTS` (`engine.allowed_hosts`) on every `ork` to each name with a dot the engine is reached by: a Kubernetes name such as `ork.orkstack` or `ork.orkstack.svc.cluster.local`, or any FQDN; the browser's name, behind a proxy that keeps the browser's `Host`; and the name `orkctl`, `curl` or a trigger caller dials. An entry may also be a pattern such as `*.example.com` for every name under it, or `*` for any `Host`, which turns the check off ([DEPLOY.md](./DEPLOY.md), "The web UI").
- **The admin listener** (`ORK_ADMIN_ADDR`, on `ork` and `orker`). Set `ORK_ADMIN_ALLOWED_HOSTS` (`telemetry.admin_allowed_hosts`) on each binary a scraper reads by a name with a dot, such as a Prometheus static target `host.docker.internal` or a Kubernetes Service FQDN. A scraper that dials an IP address needs nothing ([DEPLOY.md](./DEPLOY.md), "7. Telemetry").

Set the environment variables before the upgrade. Set the configuration file's keys with the release's binaries, because an older binary refuses a key it does not know.

### jq 1.8 in the first-party images

Every first-party activity image ships jq 1.8 (was 1.7) and BusyBox 1.37 (was 1.36), the `git` image git 2.54 (was 2.45), and the `http` image curl 8.23 (was 8.14). jq 1.8 changes what some filters compute, both a `json` task's filter and any `jq` a `shell` task's command runs:

- `indices`, `index` and `rindex` count code points rather than bytes;
- `tonumber` refuses leading or trailing white space;
- `ltrimstr` and `rtrimstr` fail on a non-string input or argument;
- `last(empty)` yields nothing;
- `limit` fails on a negative count;
- `pow10` is gone (use `exp10`);
- an `as` binding takes the whole expression before it, operators included (`[-1 as $x | 1, $x]` gives `[1,-1]`, was `[-1,-1]`);
- a `#` comment that ends in a backslash continues on the next line.

A `template` task's `jq` is gomplate's own (gojq 0.12.19, was 0.12.17), where such a binding, a parse error before, now binds the whole expression too.

### gomplate 5 in the `template` image

The `template` activity image renders with gomplate 5.3.1 (was 4.3.3). gomplate 5 removed most of the names gomplate 4 still served with a deprecation warning, so a template that calls one fails to render:

| Removed | Use |
|---|---|
| `contains`, `hasPrefix`, `hasSuffix`, `split`, `splitN`, `trim` | `strings.Contains`, `strings.HasPrefix`, `strings.HasSuffix`, `strings.Split`, `strings.SplitN`, `strings.Trim`, with the input last: `{{ contains .s "x" }}` becomes `{{ strings.Contains "x" .s }}` |
| `bool`, `conv.Bool` | `conv.ToBool` |
| `strings.Sort` | `coll.Sort` |
| `net.ParseIP`, `net.ParseIPPrefix`, `net.ParseIPRange` | `net.ParseAddr`, `net.ParsePrefix`, `net.ParseRange` |

Bare `slice` does not fail but now slices (Go's builtin): build a list with `coll.Slice`. A `GOMPLATE_CONFIG` file that lists `templates` as a sequence no longer parses (gomplate 5 reads a map), and the image's `--missing-key error` now beats the file's `missingKey`. The `aws` functions run on AWS SDK v2 and no longer read `AWS_META_ENDPOINT`: set `AWS_EC2_METADATA_SERVICE_ENDPOINT` instead. The key generators and the RSA and AES functions no longer need `GOMPLATE_EXPERIMENTAL`, and the image's README lists which of them make a cached render wrong.

### Connect 1.21's error codes

Connect 1.21 codes four failures differently:

| Failure | Now | Was |
|---|---|---|
| A connection reset under a call, when the Go client's read is the first to meet it | `canceled` | `unavailable` before any response, `invalid_argument` between a stream's messages, `unknown` inside a message |
| A connection that ends between a stream's messages: the engine closing it, or a reset the client's own write meets first | `internal` | `invalid_argument` |
| A stream that ends cleanly partway through a message | `internal` | `invalid_argument` |
| An engine's write to a client that has gone away | `canceled` | `unknown` |

Such an end is still `unavailable` before any response and `unknown` inside a message. `orkctl run watch` and `orkctl run inspect -w` resume on a `canceled` they did not cause as on `unavailable`, so a watch whose stream is reset partway through now resumes where it used to stop. A Ctrl-C still ends them. A connection that ends partway through a stream (`internal` or `unknown`) ended them too before v0.3.0, which resumes it. Other `orkctl` commands print the new code for the same failure, with the same exit status. None of the four reaches an audit record, and no OrkStack metric carries a Connect code.

### The examples

Every example in `examples/` but the `custom-image` recipe runs as it stands: apply it and submit it with no inputs. Its defaults reach `httpbin.org` and `github.com`, so it needs internet access, and `--input` points it at your own. `order-saga` and `approval-gate` still need `notify-block` stored first. The changed examples have new versions, and a store keeps any older one it holds, so a composing Orkflow of yours that names `notify-block@0.1.0` is unaffected. If you adapted one, or submit one with your own inputs:

- `order-saga` sends the shipment to a new input, `shipping_api`. Pass it too.
- `poll-until-ready` now waits for a 2xx answer, not for `{"ready": true}`.
- `config-from-git` and `pinned-checkout` no longer mount `GIT_TOKEN` or read their template, services and config from the repository: those are inputs now. Their headers say how to add a token for a private repository.
- `examples/automation/examples.ork.yaml` declares `default`, not `examples`, and fires once a day, not every 15 minutes, pausing itself after a failure.
- `custom-image` runs images you build, so it is now a recipe beside their build, `examples/custom-image/custom-image.ork.yaml`, whose header gives the steps. It is gone from the Apply page's gallery.

## Upgrading from 0.2.0 to 0.3.0

### Orkflows are created, automation documents applied

orkctl's `apply` is gone, with no alias: each kind of document has a command of its own, under its own noun, and each refuses the other's `kind:` by name, pointing at the command that takes it ([CHANGELOG.md](../CHANGELOG.md)).

| orkctl 0.2.0 | orkctl 0.3.0 |
|---|---|
| `apply deploy.ork.yaml` (`kind: Orkflow`) | `orkflow create deploy.ork.yaml` |
| `apply automation.ork.yaml` (`kind: Automation`) | `automation apply automation.ork.yaml` |
| `export -n payments` | `automation export -n payments` |
| `webhook register URL` | `webhook create URL` |

`--dry-run` and `--check` move with each command, and `--check` still exits 4 when something would change; `--prune`, `--expect-plan` and `--secrets-out` belong to `orkctl automation apply` alone. Output says `Created orkflow …` and `Created webhook …` where it said `Applied` and `Registered`.

A directory, or a list of files, that holds both kinds is refused by either command, so a pipeline that ran one `apply environments/prod/` now runs two steps, Orkflows first, because an automation document's plan reads only the Orkflow versions already stored:

```bash
orkctl orkflow create environments/prod/orkflows/
orkctl automation apply environments/prod/automation.ork.yaml
```

Move the Orkflows and the automation document into separate directories, or name the files. The RPCs keep their names (`ApplyOrkflow`, `RegisterWebhook`, `PlanAutomation`), so an API client needs no change. In the UI, the Apply page is now **Create Orkflow**, at `/orkflows/+new`, reached from the Orkflows page; the sidebar no longer lists it, and the editor's button reads **Create version**. The `approval-gate` and `order-saga` examples are now version `0.2.1`: only their descriptions changed, to name the new command, so a store holding `0.2.0` creates the new file beside it.

### Flag shorthands

orkctl gained shorthands for its most used flags, and the docs and the UI print them: `-j` `--json`, `-t` `--task`, `-s` `--since`, `-w` `--watch`, `-F` `--filter`, `-i` `--input`, `-g` `--group-by`, and `-o` `--output` and `-f` `--file` where they had none. The long forms keep working, so nothing needs changing ([CLI.md](./CLI.md)).
