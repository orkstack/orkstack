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

### Features

- Authentication and roles for API callers and Orkers.
- Temporal over TLS, mutual TLS or an API key.
- Drain-first Orker stops.
- Secrets masked in task logs.
- A whole-run `timeout:`.
- GPU profiles through CDI.
- Task identity: short-lived tokens, credentials and provider secrets for tasks.

**Upgrading:** upgrade every engine before any Orker, and read the **BREAKING** entries first ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#3-rolling-a-release-forward)).

### Added

- Child runs continue their parent's trace, each run has an `ork.run` span, and task.json's `trace` key is now part of the activity contract ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#traces)).
- On a versioned deployment, `ork.pool.release.backlog.tasks` and `.age` show each pool's waiting tasks by release, so a stranded release is told from demand ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#a-pools-backlog-as-metrics)).
- Added GPU profiles: a host-native Orker can give tasks exclusive GPU units through CDI with `devices:` and `resources.gpu`; tested on AMD and Intel GPUs. ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-gpu-orker))
- Experimental: `gpu` profiles accept NVIDIA CDI specs (toolkit 1.19.1+, root-owned mount sources, `--disable-hook update-ldcache`); emulation-proven, not tested on NVIDIA hardware. ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-gpu-orker))
- An Orker with any profile refuses to boot unless its engine's default runtime is runc or crun, checked by name and by binary; set the engine's default runtime back. ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-gpu-orker))
- An Orker with a gpu profile refuses to boot unless `orker.cdi.spec_dirs` holds every directory its engine reads CDI specs from; add the missing ones. ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-gpu-orker))
- Tasks now see their run's origin, start time, deadline, parents and attempt limit in task.json, marked by `run.context` ([OCI_ACTIVITY_CONTRACT.md](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#32-inputtaskjson)).
- `orkctl admin principal list` and Access → People list who signed in over a window (30 days by default), newest first, with their name and when they last signed in ([ACCESS.md](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#in-the-web-ui)).
- The audit trail filters on `decision` (`allowed`, `denied`, `would-deny`) and `principal_verified`, and Admin → Audit has one-click Denied, Would deny (observed) and Principal not verified filters ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#filtering-lists)).
- An engine now boots on a Temporal credential without admin rights when its namespace's retention already covers OrkStack's; a shorter one refuses the boot, naming the retention to set ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-without-temporal-admin-rights)).
- A composed task can pass files to its child run: bind the child's `type: artifact` inputs under `artifacts:`, and a child task reads one as `{{ artifact "_inputs/<name>" }}` ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#passing-artifacts-to-a-child-run)).
- An Orkflow can return files: an `outputs:` entry `{artifact: <task>/<name>}` becomes the composing task's artifact, read downstream as `{{ artifact "<task>/<output>" }}` ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#outputs)).
- The run page, `orkctl run get`, `orkctl run artifact download --task _inputs` and evidence bundles show and fetch the artifacts a child run received and returned ([RUN_EVIDENCE.md](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md)).
- An Orkflow can declare `concurrency:` to limit how many of its runs hold one key at once; the rest wait in line, and a paused run gives its slot back ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#concurrency-limits-concurrency)).
- A concurrency limit can `refuse` or `skip` a run at a full key instead of queueing it; a schedule's fire a limit keeps from starting completes and says why, so the schedule never pauses for it ([ORKFLOW_SCHEDULES.md](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_SCHEDULES.md#a-fire-a-concurrency-limit-keeps-from-starting)).
- The run page shows why a run waits for its concurrency slot and behind whom, Runs gets a Queued tile, and `orkctl orkflow get`, `orkctl stats` and the UI name a limit and count queued runs ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#concurrency-limits-concurrency)).
- `orkctl run list -F queued=true`, `queued_on=KEY` and `holding=KEY`, and the stats' `queued` and `executing` counts, show who waits for and holds a key ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#filtering-lists)).
- `orkctl concurrency list` and `get`, and Operate → Concurrency, show who holds each concurrency key and who waits, read from the limit itself ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#concurrency-limits)).
- A composed Orkflow keeps its own concurrency limit: the child run waits in line inside its composed task, and may not hold a key a run above it holds ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#concurrency-limits-concurrency)).
- `orkctl admin concurrency reset` rebuilds a concurrency limit's record that does not answer or holds a slot no run has; no run is stopped ([RUNBOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/RUNBOOKS.md#resetting-a-concurrency-limit)).
- The observability kit alerts on concurrency limits (a dead census, stuck lines, waits that never start, slots not freed), and new counters record requests, waits, slots taken back and rebuilds ([RUNBOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/RUNBOOKS.md#concurrency-limits)).
- `orkctl run cancel --force` ends a run that makes no progress after a cancel; it reads `RUN_FORCE_CANCELED`, names who and why, and needs the new `run.cancel.force` permission ([TROUBLESHOOTING.md](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-run-does-not-end-after-orkctl-run-cancel)).
- `orkctl run cancel --force --cascade` also force-cancels a run's open child runs, each with its own grace; `--dry-run` lists them, `--stop` stops it ([TROUBLESHOOTING.md](https://github.com/orkstack/orkstack/blob/main/docs/TROUBLESHOOTING.md#a-run-does-not-end-after-orkctl-run-cancel)).
- New `engine.runs.force_cancel_*` settings bound how long a force cancel waits and what it reads ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md)).
- Webhook subscribers: a force-canceled run's `RUN_CANCELED` shows its tasks as of its last event, with no failure-handler events before it ([WEBHOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#events)).
- Webhooks can subscribe to `RUN_QUEUED` and `TASK_QUEUED` by name; a webhook with no events still receives only a run's start and end ([WEBHOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md)).
- `orkctl admin restore reconcile` removes, after a restore of Temporal, the runs cleanup had retired that the restore brought back; it needs the new `system.restore` permission ([RUNBOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/RUNBOOKS.md#backup-and-disaster-recovery)).
- Prometheus alert rules for the observability kit, each with a runbook, plus runbooks for Temporal outages and for backup and restore ([RUNBOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/RUNBOOKS.md)).
- `ork.orker.stop_watch.failures{reason}` counts attempts whose container's event stream an Orker could not open, each also logged as a warning ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#lifecycle-metrics)).
- The observability kit's Grafana now provisions two read-only dashboards, OrkStack and OrkStack / Run, with linked logs, traces and metrics ([README](https://github.com/orkstack/orkstack/blob/main/deploy/observability/README.md#dashboards)).
- A run's record says when it holds tasks back itself, at its in-flight limit or while it carries on (`Run.dispatch_hold`) ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#a-run-that-pauses-new-tasks)).
- Each engine exports every pool's backlog, `ork.pool.backlog.tasks` and `.age`, its rates and `ork.pool.pollers`, to scale Orkers on; `engine.pool_metrics.*` picks the pools ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#a-pools-backlog-as-metrics)).
- Metrics for retention cleanup, webhook deliveries, schedules and HTTP triggers, and the `telemetry.object_labels` setting that adds schedule names and webhook ids to them ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#lifecycle-metrics)).
- A fan-out can run every item even when some fail (`each.on_item_failure: continue`, optionally bounded by `max_failed_items`), and its failure handlers get the items that succeeded ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#when-an-item-fails-on_item_failure-continue)).
- A fan-out can run one item per line, or per `batch:` of lines, of an upstream task's or artifact input's file: `each.over_artifact` with `format: jsonl` or `lines` ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#fan-out-over-an-artifacts-lines-over_artifact)).
- Pause a run with `orkctl run pause`: running work finishes, nothing new starts and its child runs pause too, until `orkctl run unpause` ([API.md](https://github.com/orkstack/orkstack/blob/main/docs/API.md#pauserunrequest)).
- A pause lasts at most its timeout (`engine.run_pause_timeout`, 7 days by default), and `run list --filter status=PAUSED` lists paused runs ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-paused-run-index)).
- Webhooks can subscribe to `RUN_PAUSED` and `RUN_UNPAUSED`; like task events they are opt-in, so a subscription with no events does not receive them ([WEBHOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md)).
- `ork.orker.task.wait.duration` measures how long a task waited for an Orker, and `ork.orker.memo.lookups`, `.reused` and `.writes` count a cached task's memo ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#lifecycle-metrics)).
- Every Orker exports `ork.orker.present`, `ork.orker.task_log.export_enabled` and `ork.orker.attempt_records`, which counts attempts left with no record; its resource carries `ork.orker_id` ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-orkers-load-as-metrics)).
- Operator twins: `ork.deployment.runs`, `.tasks`, `.runs.refused`, `.run.continuations` and `.orker.*` count every namespace on the default sink, with no names ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#operator-twins)).
- Tasks can declare `identities:`, `credentials:` and secrets read from a provider, on a deployment that enforces authentication and runs an issuer ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#task-identity-identities)).
- Two provider types, `aws` and `vault`, under the new `providers` setting: a task logs in as its attempt and gets credentials or secrets as files ([PROVIDERS.md](https://github.com/orkstack/orkstack/blob/main/docs/PROVIDERS.md)).
- `orkctl run records -n NS --not-applied` finds answers a run has not taken after a store outage, and `--redeliver` tells open runs again ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#after-an-outage)).
- Evidence bundles hold the engine's records of a run that uses task identity, answers never applied included; one the store lacks is a `missing` member ([RUN_EVIDENCE.md](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#records)).
- Engines register a new search attribute, `OrkflowResultsRecorded`; add it by hand where they may not, or `orkctl run records --not-applied` is refused ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#temporal-search-attributes)).
- `orkctl run tokens` lists the identity tokens a run was issued, never a token, and `orkctl run inspect` shows each identity an attempt holds ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#what-was-issued)).
- Schedules, triggers and event sources whose Orkflow asks for an identity need `--consent=DIGEST` to the digest shown; plans and promotions warn when a consent is stale ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#consent)).
- Namespaces now hold an identity policy (audiences, grants, attempts beyond retries), set with `orkctl namespace update` or `settings.identity`; tasks cannot use it yet ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#a-namespaces-identity-policy)).
- The engine records every task result of a run that uses task identity; Orkers adopt, stage and reuse only those records ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#results-the-engine-records)).
- New `identity.results.commit_wait` (15m): how long an Orker waits for an engine to record a result of a run that uses task identity ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#results-the-engine-records)).
- New `identity.attempts.*`, `identity.token.*`, `identity.issuance.records` and `identity.unscoped_orkers` settings ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md)).
- New `orker.identity.*` and `orker.log.mask_grace` settings: an Orker that reaches an engine keeps identity tokens on memory-backed storage it chooses at boot and reports in `DescribeOrker` ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#on-the-orker)).
- New `event_sources.*` settings, `orkctl event-source` commands and `kind: EventSource` documents store and evaluate event sources; none consumes anything yet ([EVENT_SOURCES.md](https://github.com/orkstack/orkstack/blob/main/docs/EVENT_SOURCES.md)).
- New `orker.event_sources.*` and `orker.services.*` settings, and `kind: ServiceInterface` documents for service images; no Orker hosts a source yet ([ORKER_SERVICE_CONTRACT.md](https://github.com/orkstack/orkstack/blob/main/docs/ORKER_SERVICE_CONTRACT.md)).
- New `orkctl service test IMAGE` checks an event-source image against the service contract on a local container runtime, in seven checks; no Orker runs sources yet ([EVENT_SOURCE_IMAGES.md](https://github.com/orkstack/orkstack/blob/main/docs/EVENT_SOURCE_IMAGES.md#testing-a-source-image)).
- OrkStack can now authenticate API callers itself and decide each request with roles: set `auth.mode` to `observe`, then `enforce`; off by default ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md)).
- `orkctl admin auth status` and `orkctl admin auth lower` show and lower the deployment's authentication mode ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#the-mode-record)).
- `orkctl --token-file` (`ORK_TOKEN_FILE`) presents a bearer token, and `--ca-file`, `--client-cert` and `--client-key` set TLS roots and a client certificate ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#from-orkctl)).
- An Orker reaches the engine as itself with `orker.engine.*` (a token file or a client certificate) and refuses to boot if the engine does not accept it or it names another Orker ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#an-orkers-credential)).
- `auth.orkers` binds Orkers to the `orker` role at a pool from configuration ([ACCESS.md](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#bindings-from-configuration)).
- `orkctl whoami` shows who the engine verified you as and what you may do; `orkctl` warns when `ORK_PRINCIPAL` is set but not used ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#who-am-i)).
- With authentication on, `orkctl admin role`, `binding`, `service-account`, `principal` and `access` manage and explain access; a namespace admin binds roles in its own namespace ([ACCESS.md](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#changing-it)).
- `orkctl admin access acknowledge REVISION SHA256` clears, on every engine, the banner for an access document written around the API; `admin access history` now shows each digest ([ACCESS.md](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#revisions-and-a-document-written-around-the-api)).
- `ork` and `orker` can reach Temporal over TLS, mutual TLS or with an API key (`temporal.tls.*`, `temporal.api_key_file`); the files are re-read when rotated, with no restart ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#temporal-over-tls)).
- With authentication on, who started or canceled a run is marked verified, not asserted, on the run page, in the runs list and by `orkctl run get` and `run list` ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md)).
- `orkctl admin audit` marks each principal `(verified)` or `(asserted)`, so a caller OrkStack verified is no longer shown as asserted ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md)).
- `orker.metadata_probe: warn` or `refuse` checks at boot whether a task could reach the host's instance metadata service, and refuses to start under `refuse` (`METADATA_ENDPOINT_REACHABLE`) ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#keeping-tasks-away-from-the-hosts-identity)).
- Each schedule and trigger now records who bound it and their consent; `orkctl schedule update` and `trigger update` take `--rebind`, and older ones have none until rebound ([ORKFLOW_SCHEDULES.md](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_SCHEDULES.md#who-a-schedule-runs-as)).
- A binding at a namespace or an Orkflow is now pinned to the one it was saved for, and does not carry over to one deleted and made again under its name; `--repin` pins it again ([ACCESS.md](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md#pinned-bindings)).
- OrkStack can be a task identity issuer: `identity.issuer` and its signing keys publish a discovery document, a key set and a SPIFFE bundle; `orkctl admin identity` exports them and retires keys ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md)).
- An Orker treats the files the engine's key settings name as its own, so no task's secret reads one, and logs an error if it can open one; `orker.refuse_readable_engine_keys` refuses the boot instead ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#an-engines-key-files-and-an-orker-on-one-host)).
- With `auth.mode: enforce`, an idempotency key belongs to the caller who used it: another caller's same key is another key ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#idempotency-keys)).
- With `auth.mode` `observe` or `enforce`, `identity.signing.key_files` must list a key, also with no `identity.issuer`; the keys then sign only what engines hand each other ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#signing-keys-with-no-issuer)).
- `identity.keys_copy_file` keeps the published signing keys on disk, so an engine restarted while object storage is away still signs and verifies for `identity.keys_copy_max_age` ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#the-copy-on-disk)).
- `orkctl admin identity keys retire --compromised-from T` retires a leaked key so that nothing it signed from T on verifies ([TASK_IDENTITY.md](https://github.com/orkstack/orkstack/blob/main/docs/TASK_IDENTITY.md#rotation-with-no-outage)).
- Each Orker now checks that its storage credential cannot reach the control plane's objects and reports it on `DescribeOrker`; the engine logs every Orker that is not ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#an-orkers-own-storage-credential)).
- A task, even without `retry:`, runs again when its Orker stops or is lost, spending no attempt (`engine.interruptions.*`, `retry.max_interruptions`); `task.attempt` may repeat: key on `idempotencyKey` ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#tasks)).
- `orkctl run get` says why a waiting task is not running: when it runs again after an interruption, or that no Orker of its pool or release takes work, naming the stopping and cordoned ones ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#live-attempt-introspection)).
- A task its Orker could not start (the store refused it, or a device profile drifted) runs again elsewhere up to five times in a row before spending its own attempts ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#tasks)).
- A container whose start the daemon cannot account for fails `RUNTIME_STATE_UNKNOWN` and runs again; it may have run, so key side effects on `idempotencyKey` ([REASONS.md](https://github.com/orkstack/orkstack/blob/main/docs/REASONS.md#task-outcomes)).
- An Orkflow can bound a whole run with a top-level `timeout:`; at the deadline the run fails `RUN_TIMED_OUT` and its failure handlers get `engine.run_timeout_handler_grace` to finish ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#run-timeout-timeout)).
- Two engine metrics count run timeouts per namespace: `ork.run.timeouts` (runs their own `timeout:` ended) and `ork.run.timeout.hard_ends` (runs whose failure handlers it stopped) ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#7-telemetry)).
- The archive sweep now says when it is blind: alert on `ork.archive.sweep.list_failed` and a stale `ork.archive.sweep.last_success`; an undated expiry reads 0 ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules)).
- People sign in to the web UI with your identity provider: give an issuer a `login:` block, `auth.public_url` and `auth.signin.key_files` ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#sign-in-for-the-web-ui)).
- The web UI's Access screens show and change bindings, roles, service accounts and people, explain a decision and diff revisions; the audit trail shows each request's decision ([ACCESS.md](https://github.com/orkstack/orkstack/blob/main/docs/ACCESS.md)).
- `orkctl login` signs you in from a terminal, in a browser or with `--device`, and keeps the login; `orkctl logout` forgets and revokes it ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#from-orkctl)).
- Service accounts now authenticate: `orkctl --service-account` signs each request, `orkctl auth assertion` signs one for another tool, and an Orker may use `service_account` ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#what-the-api-accepts)).
- `orkctl --break-glass-reason` sends a break-glass key's reason with each request ([AUTHENTICATION.md](https://github.com/orkstack/orkstack/blob/main/docs/AUTHENTICATION.md#the-first-admin-and-break-glass)).
- Every Orker now exports its load: `ork.orker.running_tasks`, `open_pins`, `max_concurrent`, and `ork.orker.cordoned` by cause, each labelled with its pool ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-orkers-load-as-metrics)).
- `ork.telemetry.export_errors` counts failed telemetry exports by signal, and failures are logged at most once a minute per signal ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#export-errors)).
- `ork.store.requests`, `ork.store.request.duration` and `ork.store.bytes` count each process's calls to the S3 bucket by operation, kind of record and outcome ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#object-store-requests)).
- Each engine reports its clock against the object store's as `ork.store.clock_skew`, probed once per `cleanup.interval` when metrics are exported ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-store-clock)).
- `rpc.server.call.duration` records every API call by service, method and code, and `ork.api.streams` the streams and WebSockets open now ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#api-calls-and-streams)).
- `ork.runs.open` reports the deployment's open runs, read by a census under a per-replica budget, with `ork.census.requests`, `skipped` and `last_success` ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#censuses)).
- `ORK_TELEMETRY_METRIC_CARDINALITY_LIMIT` bounds each instrument's series and `ORK_TELEMETRY_CENSUS_REQUESTS_PER_MINUTE` an engine's census budget ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration)).
- Each task attempt's record now holds the CPU time and peak memory its container used, or why they were not measured; `orker.usage_sample_interval` sets how often (`0s` turns it off) ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration)).
- Orkers log each task attempt's CPU and memory as `ork.record=usage` records on its run's telemetry sink, every `orker.usage_log_interval`, and report running totals as gauges ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration)).
- Retention cleanup can now delete retired runs' artifact bytes on S3 (`ORK_CLEANUP_ARTIFACT_GC`); it starts in `dry_run`, and `orkctl admin gc preview` shows what `delete` would do ([STORAGE.md](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md#the-artifact-gc)).
- Reading an artifact the GC deleted answers `ARTIFACT_RECLAIMED`, and an evidence bundle records it as `artifact_reclaimed` ([REASONS.md](https://github.com/orkstack/orkstack/blob/main/docs/REASONS.md#reads-of-reclaimed-artifacts)).
- Device profiles: an Orker on the device's own host can pass named character devices, such as USB serial ports, to the tasks that select them with `devices:` ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-device-orker)).
- Device profiles can change without new names: roll a new contract in on the pool's Orkers, and let images declare the contract they need (`devices:` in their interface) ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#changing-a-profile)).
- `orkctl orker list` shows a profile's contracts, and `orkctl run attempts` and the run page the contract each attempt ran under ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#changing-a-profile)).
- `orkctl orker list` says when the engine read the fleet, as an "As of" line ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#changing-a-profile)).
- Admin → Pools, `orkctl orker list` and `orkctl orker get` show each Orker's device profiles and why a profile holds an Orker's cordon ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-device-orker)).

### Changed

- **BREAKING:** an API request's `traceparent` is now a link, not its parent: each request starts its own trace; set `telemetry.trust_inbound_trace` to keep joining the caller's ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#traces)).
- **BREAKING:** evidence bundles are now `schema_version` 3, and `orkctl run evidence verify` reads only that: verify an older bundle with the orkctl that exported it ([RUN_EVIDENCE.md](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#versions)).
- **BREAKING:** a webhook scoped to an Orkflow hears only the namespace's own, not another's shared one of that name; drop the name and read the payload's new `orkflowNamespace` to hear it ([WEBHOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#scope)).
- A child run's origin now names the composed task that started it, its parent's and root's Orkflow, and what started the root, in the API, `orkctl run get` and the run page ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#run-origin)).
- A task whose container a Docker daemon stopped as it shut down now runs again without spending an attempt; an operator's stop still spends one ([REASONS.md](https://github.com/orkstack/orkstack/blob/main/docs/REASONS.md#task-outcomes)).
- **BREAKING:** the metric `ork.archive.cleanup.runs` is gone; read `ork.cleanup.runs`, by namespace and outcome, instead ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#lifecycle-metrics)).
- **BREAKING:** runs have a new status, `PAUSED`: a client that treats any status but `PENDING` and `RUNNING` as finished must treat `PAUSED` as still running ([API.md](https://github.com/orkstack/orkstack/blob/main/docs/API.md#status)).
- `orker.profiles.probe_image` is now optional: a release Orker probes with its own release image, signature- and build-checked at boot; set it for a mirror, an air-gapped host or a source build ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-a-device-orker))
- The release images are rebuilt on current alpine and distroless bases, and the Orker image's Docker CLI is now 29.9.3; nothing to do ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-the-published-images)).
- The observability kit's Prometheus receives metrics only over OTLP and scrapes nothing, so `ork` and `orker` need no admin listener for it ([README](https://github.com/orkstack/orkstack/blob/main/deploy/observability/README.md#point-ork-and-orker-at-it)).
- With the artifact GC in `dry_run` (the default) or `delete`, a retired run keeps its reference markers until the GC has decided; provenance still shows it released ([STORAGE.md](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md#the-artifact-gc)).
- **BREAKING:** an Orkflow whose top-level key is a merge key (`<<`), an alias, or anchored or tagged is now refused; write each top-level key out as a plain name ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#top-level-fields)).
- **BREAKING:** an idempotency key reused for a different Orkflow, version or input is now refused (`already_exists`), not answered with the first run; use one key for one submission ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#5-ops-notes)).
- **BREAKING:** Orkers stop drain-first: on SIGTERM they take no new work and let running tasks finish for up to `orker.stop.grace` (24h by default, at most 31 days); raise your stop timeout to match ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#stop-timeouts)).
- **BREAKING:** task logs now show `***` in place of a task's secrets, so `stdout.log` differs from the output there; set `mask: false` on a secret, or `orker.log.mask_secrets: false`, to keep it ([SECURITY_MODEL.md](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#masking-and-its-limits)).
- **BREAKING:** a task whose `values.json` holds one of its own secrets now fails `VALUES_CONTAIN_SECRET`; return a reference to the secret, not its value ([OCI_ACTIVITY_CONTRACT.md](https://github.com/orkstack/orkstack/blob/main/docs/OCI_ACTIVITY_CONTRACT.md#94-secrets)).
- **BREAKING:** Go runtime metrics use OpenTelemetry's names, also over OTLP (`go_goroutines` is now `go_goroutine_count`; `go_memstats_*` change); update dashboards and alerts by the mapping ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#go-runtime-metrics)).
- **BREAKING:** telemetry naming a sink a process does not declare is dropped and counted on `ork.telemetry.records_dropped`, not sent to the default sink; `ork.telemetry.sink_unknown` is gone ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#named-sinks)).
- `ork.orker.task.duration` now carries its attempt's `outcome`, as documented, so failed attempts' times are told from successful ones ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#lifecycle-metrics)).
- The engine's `run event` log line no longer names the task, Orkflow or selection attributes of a run on a named sink; it keeps the run id and namespace ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#where-a-namespaces-records-go)).
- A namespace's continuation metrics go to its own sink, not the default; the default sink counts every namespace's on `ork.deployment.run.continuations` ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#where-a-namespaces-records-go)).
- The Orkflow name is now a default label on run metrics; turn it off with `ORK_TELEMETRY_ORKFLOW_LABEL=false` ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-orkflow-label)).
- each process reports its telemetry under its own `service.instance.id` and `ork.component`, and `OTEL_RESOURCE_ATTRIBUTES` can no longer override these or `ork.deployment` ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#process-identity)).
- `orkctl orker list` and Admin → Pools show when a stopping Orker's grace ends, and `orkctl release status` and `wait` warn of a pool where a release's runs have no Orker taking work ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#3-rolling-a-release-forward)).
- `scripts/k8s-orker-check.sh` now also checks drain-first stops on your cluster, and leaves a sixth Orkflow, `k8s-orker-drain`, in the namespace it runs in ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#orkers-on-kubernetes)).
- The cluster now keeps a finished run at least `engine.idempotency_ttl` plus 5 minutes, plus the cleanup margin, when the maximum run retention is shorter; runs are retired when they were ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules)).
- The Orker numbers each task's attempts itself and starts nothing until the cluster answers its first heartbeat; a store that refuses the number fails `ATTEMPT_STORE_UNAVAILABLE` ([STORAGE.md](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md#invariants-the-layout-carries)).
- Upgrade every Orker of a pool together: an older Orker beside a newer one can overwrite its attempt records, and a submit that probes the pool refuses the mix ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#upgrading-a-pools-orkers)).
- Upgrade every engine before any Orker: an engine of v0.3.1 or earlier cannot read a v0.4.0 Orker's answers and shows it unavailable ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#3-rolling-a-release-forward)).
- A submit is refused when a task with a `mask: false` secret would run on a pool with an Orker older than v0.4.0, which would fail it; upgrade the pool's Orkers ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#fan-out-caching-and-failure-handlers-need-every-orker-of-a-pool-on-a-release-that-supports-them)).
- On Linux, `ork` and `orker` can no longer be core-dumped or traced by their own user; set `process.allow_debug: true` to attach a debugger ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-processes-themselves)).
- A stopped Orker now lets finished tasks report, reports the rest `ORKER_STOPPED` at once instead of timing out, and a signal to its process group no longer reaches containers ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#removing-an-orker-safely-cordon-and-drain)).
- A `5xx` answer to a trigger's or an await callback's sender now says only what to do and any reason code; the cause is in the engine's log ([ORKFLOW_TRIGGERS.md](https://github.com/orkstack/orkstack/blob/main/docs/ORKFLOW_TRIGGERS.md#invoking-a-trigger)).

### Fixed

- A failure handler that awaits or sleeps while its run carries on is no longer canceled when the run then fails; upgrade with no run in that state ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#joins-and-failure-handlers-join)).
- The run page reads the evidence of a run whose event stream has a gap, instead of refusing its manifest ([RUN_EVIDENCE.md](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#a-run-whose-stream-has-a-gap)).
- An artifact's provenance names the owner of a shared Orkflow whose child run holds it, instead of reading as the namespace's own Orkflow of that name ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#artifact-provenance)).
- A long run with a skipped fan-out, or a pin with a skipped member and others still to run, no longer stops `RUN_STATE_CORRUPT` when it carries on; run it again ([DSL.md](https://github.com/orkstack/orkstack/blob/main/docs/DSL.md#fan-out-each)).
- The evidence bundle of a shared Orkflow's child run holds its owner's definition, which needs `orkflow.read` there, instead of calling it deleted or mismatched ([RUN_EVIDENCE.md](https://github.com/orkstack/orkstack/blob/main/docs/RUN_EVIDENCE.md#states)).
- A run diff reads a shared Orkflow's child run from its owner's definition, and no longer calls it the same Orkflow as the namespace's own of that name ([UI_CLI_PARITY.md](https://github.com/orkstack/orkstack/blob/main/docs/UI_CLI_PARITY.md#run-diff)).
- The Admin pages no longer hang when an activity image's registry does not answer: the activity catalog lists it with `resolve_error` within 10 seconds ([API.md](https://github.com/orkstack/orkstack/blob/main/docs/API.md)).
- A stopping Orker no longer exits before the cluster has a finished task's result, which made the task run again on another Orker, spending an attempt ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#stop-timeouts)).
- A task that succeeded just before its Orker stopped no longer fails `context canceled`: the Orker stops its worker only once the cluster has the result ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#removing-an-orker-safely-cordon-and-drain)).
- An Orker that is killed or crashes no longer leaves `docker` or `podman` CLI processes running behind it on Linux ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#stop-timeouts)).
- `orkctl run watch` and `run logs -f` now resume, rather than fail with `protocol error: incomplete envelope`, when the engine's connection closes as a stream starts
- Schedule calls no longer answer `internal` while Temporal is down or rate-limiting, and a submit Temporal rate-limited answers `resource_exhausted`; ask again ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#5-ops-notes)).
- Task logs no longer lose a task's last output when the task is cancelled or its Orker stops while a chunk is uploading ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#live-task-logs-where-they-are-what-bounds-them-how-long-they-stay)).
- With more than one engine, a namespace's run list, stats and running count could miss runs started by an engine that booted before its Temporal namespace was ready; they are now included ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#5-ops-notes)).
- A keyed submission whose claim or start answer was lost no longer leaves its key naming a run that never starts, or a failed run beside a live one; retry under the same key ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#5-ops-notes)).
- Admin → Pools: a pool's card now updates when you cordon or uncordon one of its Orkers, instead of keeping its old state until the page is reloaded ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#removing-an-orker-safely-cordon-and-drain)).
- The run page shows a task's interruption notice; it was dropped from the live and replayed event stream, and a fan-out item stayed Running.

### Security

- **BREAKING:** webhooks now refuse loopback, link-local (cloud metadata), private and other non-public addresses, at registration and delivery; list receivers' networks in `webhooks.allowed_networks` ([WEBHOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#where-a-delivery-may-go)).
- A webhook's URL is no longer written into Temporal with its deliveries, and attempt errors are now a code and a short sentence; rotate a URL that holds a credential ([WEBHOOKS.md](https://github.com/orkstack/orkstack/blob/main/docs/WEBHOOKS.md#what-the-cluster-holds)).
- An await's callback URL, a trigger's URL and the web UI no longer tell a caller with no credential why a read failed; the engine's log has the cause.
- Any Orker can write results every namespace reads: give each trust zone its own deployment and bucket, and set `ORK_ORKER_NAMESPACES` so an honest Orker refuses other namespaces' tasks ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#trust-zones-a-deployment-per-zone)).
- The six first-party activity images now carry OpenSSL 3.5.9-r1, which fixes CVE-2026-88823, CVE-2026-95691, CVE-2026-95692 and CVE-2026-107496.

## [0.3.1] - 2026-11-12

### Features

- Cleanup keeps a run whose execution went missing until its own retention passes.

**Upgrading:** Deployments with `ORK_CLEANUP_ENABLED=true` should upgrade now; there is nothing else to do ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules)).

### Fixed

- Cleanup no longer deletes a run whose execution the cluster lost before the run's own retention passed; it keeps it and counts it (`ork.archive.cleanup.runs`) ([DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#retention-cleanup-and-bucket-lifecycle-rules)).

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
