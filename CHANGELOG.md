# Changelog

Every change that may affect the people who run or use OrkStack is recorded
here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Each
breaking change is marked **BREAKING** and listed first in its section. A
version opens with its Features, a short list of its headline items, and a line
on what to read before upgrading; the sections after it are the full record.

Such a change lands with its entry under `[Unreleased]`, saying what they must
do about it, if anything; a change that cannot affect them, such as a
documentation or test change, has none. Each entry is one line in plain words,
with a link to the doc that holds the detail. At a release that section becomes
the version's own, and its text is the release's notes on GitHub, word for word.

## [Unreleased]

## [0.3.0] - 2026-11-10

### Features

- Sleeping tasks ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#sleeping-tasks)).
- Orkflows shared across namespaces ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#sharing-across-namespaces)).
- Webhook retries that survive an engine restart ([WEBHOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#attempts-and-retries)).

**Upgrading:** `orkctl apply` is replaced by `orkctl orkflow create` and `orkctl automation apply`, so read the **BREAKING** entries and the upgrade guide first ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#upgrading-from-020-to-030)).

### Added

- A task can wait a fixed time with `sleep: {seconds: N}`, up to 30 days, without a container or an Orker. A client that switches over run or task statuses should handle the new `SLEEPING` ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#sleeping-tasks)).
- An Orkflow can share itself with other namespaces (`shared_with:`), which compose it as `orkflow:<namespace>/<name>@<version>`. Until callers are authenticated, it guards against mistakes, not attacks ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#sharing-across-namespaces)).
- A child run now names the root run of its chain on the run page and in `orkctl run get`, and `orkctl run list -F origin.root_run_id=ID` lists every run below that root ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#run-origin)).
- `orkctl` has short forms for its most used flags, such as `-j` for `--json`, `-t` for `--task` and `-f` for `--file`. The long forms still work ([CLI.md](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md)).
- Webhook deliveries now carry the run event's `sequence`, also as the `X-Ork-Sequence` header, so a receiver can order them. A receiver that refuses unknown body fields must accept it ([WEBHOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#duplicates-and-ordering)).
- The API now reports where each task's time went (wait, run and total), and `orkctl run get -j` and `orkctl run attempts -j` print it ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#task-timing)).
- `orkctl run get` now shows a `TASK VALUES` table of what each task returned. A script that reads its human output sees one more table, so read `-j` instead ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#reading-runs)).
- The Schedules tab can edit a schedule (its version pin, times, overlap policy, input and description), as `orkctl schedule update` does ([ORKFLOW_SCHEDULES.md](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_SCHEDULES.md)).

### Changed

- **BREAKING** `orkctl apply` is gone: use `orkctl orkflow create` for Orkflows and `orkctl automation apply` for automation documents. `export` and `webhook register` became `automation export` and `webhook create` ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#upgrading-from-020-to-030)).
- **BREAKING** The object store's keys no longer default to the development key. Set `ORK_S3_ACCESS_KEY` and `ORK_S3_SECRET_KEY`, or leave both unset to sign with an instance or pod role ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-object-stores-credentials)).
- **BREAKING** A run now keeps at most 500 tasks, child runs and pins in flight by default, down from 1,000 (`ORK_RUN_MAX_IN_FLIGHT`), and wide runs start much faster. Runs already started keep their cap ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#a-run-that-pauses-new-tasks)).
- **BREAKING** New run ids, and the version an Orkflow gets when it pins none, are time-ordered ids; older ones stay valid. Planning a file no longer reports a version, so upgrade `orkctl` with the engine ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#version)).
- **BREAKING** A config file, `ORK_TELEMETRY_SINKS`, `ORK_ACTIVITY_CATALOG` or Orkflow file holding a second YAML document after `---` is now refused, where it was silently ignored. Remove what follows the first ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration), [DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md)).
- **BREAKING** A number with a fraction for an Orkflow's integer key, such as `timeout_seconds: 1.5`, is now refused where it was cut toward zero. Write the whole number; stored versions keep running ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#editor-support)).
- **BREAKING** YAML is read by a new library: an explicit `!!merge` tag and `<<` as a value are now refused, and YAML errors are worded differently, so update scripts that match them ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#validation-rules-enforced-by-internaldsl)).
- **BREAKING** JSON that writes a key twice, as in a `values.json`, an await's values or `--input-json`, is now refused. HTTP bodies and `orkctl`'s config must also spell each key exactly, case included ([JSON.md](https://github.com/orkstack/orkstack/blob/main/docs/JSON.md#text-the-codec-will-not-change)).
- **BREAKING** The object store must now refuse an `If-Match` write to a missing key and honour `If-Match` on a delete: the Orker and the engine refuse to start on one that does not. SeaweedFS does both ([STORAGE.md](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md)).
- Webhook retries now survive an engine restart, and the pauses between them last about a second. If you set bucket lifecycle rules, give `metadata/delivery-bodies/` one of a day or more ([WEBHOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#attempts-and-retries)).
- The run page opens a task's panel under its row, holding its Orker and details (**Show Orkers** is gone), and shows finish times, an awaited task's values and an Edit button ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md)).
- Each namespace has a page of its own under Admin → Namespaces, and a webhook's, trigger's or schedule's edit opens at an address you can link to.

### Fixed

- A write to the object store whose answer was lost now counts as done, instead of failing a promotion, create, commit or setting ([STORAGE.md](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md)).
- Deleting an Orkflow no longer erases one created again under its name meanwhile, and a promotion or create that lands during the delete is refused as deleted, not left half in place ([STORAGE.md](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md)).
- A brief Temporal outage no longer ends running tasks. A container now runs on until its heartbeat goes unanswered for 30 seconds, then fails as a retryable `ORKER_HEARTBEAT_TIMEOUT` ([OCI_ACTIVITY_CONTRACT.md](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#63-heartbeats-and-cancellation)).
- A container stopped by its daemon, or by someone through the runtime, now fails `CONTAINER_STOPPED_BY_RUNTIME`, retried under the task's `retry:`, instead of passing for its own exit ([REASONS.md](https://github.com/orkstack/orkstack/blob/main/docs/REASONS.md)).
- `orkctl run watch` and `orkctl run logs -f` now resume when the engine replica serving them stops, and a run's events arrive in order, a missing one marked `STREAM_GAP` ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md)).
- The `git` and `file` images no longer leave directories an Orker cannot remove. Remove what earlier attempts left as the troubleshooting guide says ([TROUBLESHOOTING.md](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#attempt-directories-pile-up-under-the-work-root)).
- Released binaries now report the commit they came from, where `orkctl version` said `commit: unknown`, and `ork --version` and `orker --version` are new ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md)).

### Security

- An Orker's boot now removes the attempt directories, and the secret files in them, that its killed predecessor under the same `ORK_ORKER_ID` left behind ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#what-removes-a-tasks-container)).
- The web UI's content security policy no longer allows `'unsafe-eval'`. A gateway that adds its own policy for the UI can drop it too ([SECURITY_MODEL.md](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#transport-listeners-and-configuration)).
- `orkctl` now escapes control and format characters in every value it prints, human or `--json`, so a value a task or input holds cannot act on your terminal.
- `ork`, `orker` and `orkctl` are built with Go 1.27.4, which fixes a crash a request could cause in the engine's HTTP server (GO-2026-6859).
- The six first-party activity images now carry zlib 1.4, which fixes CVE-2026-4738.

## [0.2.0] - 2026-11-05

### Features

- A new web UI.
- Orkers that wait out an object storage outage ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#when-object-storage-goes-away)).
- Run counts per day or hour ([CLI.md](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-stats)).

**Upgrading:** this release tightens what secrets and listener host names are admitted, so read the **BREAKING** entries and the upgrade guide first ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#upgrading-from-010-to-020)).

### Added

- An Orker now cordons itself while object storage is unreachable, so tasks wait instead of spending retries, and resumes on its own ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#when-object-storage-goes-away)).
- `orkctl stats --by day` and `--by hour` count the runs created per day or per hour, in your own time zone ([CLI.md](https://github.com/orkstack/orkstack/blob/main/docs/CLI.md#orkctl-stats)).
- A task waiting because every Orker of its pool is at capacity now says so, on the run page and in `orkctl run inspect` ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#declaring-an-orkers-capacity)).
- A canceled run shows who canceled it, as your gateway asserted it in `Ork-Principal`. The name is never authenticated.
- An activity image can describe itself, its ops and its keys, with an icon: add `icon:` and `description:` to `interface.ork.yaml` and build it with this release's `orkctl` ([ACTIVITY_IMAGES.md](https://github.com/orkstack/orkstack/blob/main/docs/ACTIVITY_IMAGES.md#describing-the-activity)).
- The image policy also verifies cosign 3's signature format, so you can sign your own images with cosign 2 or 3 ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#image-supply-chain-policy)).
- Each release page lists every image's digest with the commands that verify it, and the release attaches `SHA256SUMS.sigstore.json` ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#release-artifacts)).

### Changed

- **BREAKING** No secret floor admits an Orker's own sources, such as `env:ORK_*`, `env:AWS_*` or its work root, so a task naming one fails `SECRET_NOT_PERMITTED`. Give each such task a source of its own ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#the-orkers-secret-floor)).
- **BREAKING** A `file:` secret source must be a plain absolute path of at most 4096 bytes, with no `.`, `..` or empty segment ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#file-secret-sources)).
- **BREAKING** The API and admin listeners now answer `403` to a host name they do not admit, to stop DNS rebinding. List your dotted names in `ORK_ALLOWED_HOSTS` and `ORK_ADMIN_ALLOWED_HOSTS` ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#allowed-hosts)).
- **BREAKING** Release images are signed in cosign 3's bundle format, which a v0.1.0 Orker cannot read, so upgrade the Orkers with the engines where a namespace requires a signer ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#release-artifacts)).
- **BREAKING** An engine of 0.1.0 cannot replay some runs of 0.2.0. Without versioning, roll back only when no run is open, and roll forward by replacing every engine together ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#4-rolling-back)).
- **BREAKING** The first-party images ship jq 1.8, which changes what some filters compute in `json` tasks and in `jq` run by `shell` tasks. Check them before you next apply ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#jq-18-in-the-first-party-images)).
- **BREAKING** The `template` image renders with gomplate 5, which removed the functions gomplate 4 deprecated. Replace them before you next apply ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#gomplate-5-in-the-template-image)).
- **BREAKING** The Orker's boot probe now needs `alpine:3.24`. On an air-gapped or rate-limited host, mirror or load it before you upgrade ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#one-release-at-a-time)).
- **BREAKING** A `retry:` with negative or out-of-order intervals, or a `backoff_coefficient` below 1, is now refused. Fix it before you next apply; stored versions keep starting ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#upgrading-orkflows-with-the-release)).
- `uses: shell` and the other first-party names now resolve to the images' `0.2` tag when you apply. A host that mirrors the images needs the `0.2` ones first ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#upgrading-from-010-to-020)).
- A `retry:` with no `maximum_interval_seconds` now waits at most 60 seconds between attempts, not 100. Set it to 100 to keep the old spacing ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#defaults-that-moved)).
- Some broken connections now answer `canceled` or `internal`, and a refused `CancelRun` a retryable `UNAVAILABLE` or `RESOURCE_EXHAUSTED`, where clients saw others. Check clients that switch on them ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#connect-121s-error-codes)).
- The web UI has a new design. The Runs page opens with the last day's counts, a run page shows what changed since the last run, and cancelling a run or cordoning an Orker asks for a reason.

### Fixed

- A finished run that did not archive itself is now archived by a sweep, instead of vanishing at retention. Alert on `ork.archive.sweep.unconfirmed` ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules)).
- A run's event stream no longer skips a number silently when storage refused its record: it sends `STREAM_GAP`, which a client that switches on event types should handle ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#6-replay-safety-across-versions)).
- A task whose `retry:` starts above the default maximum interval no longer leaves its run stuck. Cancel any run v0.1.0 already left stuck ([UPGRADING.md](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#upgrading-orkflows-with-the-release)).
- An Orker that cannot use its object store at boot now stops with `COMMIT_STORE_UNAVAILABLE`. Add it to any alert on `COMMIT_STORE_NOT_CONDITIONAL` ([REASONS.md](https://github.com/orkstack/orkstack/blob/main/docs/REASONS.md)).

### Security

- Text an image or a registry supplies, and `orkctl --json` output, now escape control and format characters, so neither can drive your terminal.
- The web UI is served under a strict content security policy, so a script a proxy injects into the page is blocked.
- The first-party activity images move to Alpine 3.24, as Alpine 3.20 is past its support window.
- The `template` image still carries go-jose v4.1.4 (GHSA-mgcc-cww5-fp2q) inside gomplate 5.3.1. It moves when gomplate releases the fix ([README](https://github.com/orkstack/orkstack/blob/main/images/activity-template/README.md)).

## [0.1.0] - 2026-10-30

First release.
