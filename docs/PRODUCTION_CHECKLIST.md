# Production checklist

[AGENTS.md](../AGENTS.md) §11 forbids calling OrkStack production-ready until its required failure and recovery scenarios are proven. This checklist is how to run OrkStack carefully while that proof is incomplete. It is not a readiness claim.

Read [LIMITATIONS.md](./LIMITATIONS.md) before you plan a deployment, because several items below work around one of its limits. Each item names the reference that holds the detail, and the commands are the ones those references give.

## Temporal

- [ ] Operate the Temporal cluster yourself. OrkStack connects with `temporal.address` and `temporal.namespace` and has no TLS or credential setting for that connection, so a hosted Temporal cluster is not supported yet ([LIMITATIONS.md](./LIMITATIONS.md), "Engine, cluster and gateway").
- [ ] Give each environment its own Temporal namespace. `ORK_TEMPORAL_NAMESPACE` is the hard partition, and nothing crosses it ([DEPLOY.md](./DEPLOY.md), "5. Ops notes").
- [ ] Run one OrkStack deployment per Temporal namespace. The engine's own channels are fixed, so two deployments in one namespace would compete for them ([DEPLOY.md](./DEPLOY.md), "5. Ops notes").
- [ ] Grant the engine's identity the permissions the release commands need, if the cluster sits behind an authorizer. Without them those commands answer `permission_denied` ([DEPLOY.md](./DEPLOY.md), "Underneath: the cluster commands").
- [ ] Let each engine register its search attributes at boot. If the cluster refuses, the engine warns, lists walk the whole index, and a list that cannot finish that walk fails rather than returning a short one ([DEPLOY.md](./DEPLOY.md), "5. Ops notes").
- [ ] Leave the cluster's own retention alone. OrkStack derives it from the maximum run retention plus a margin, and no client sets it ([DEPLOY.md](./DEPLOY.md), "Run retention: a deployment maximum, a value per namespace").
- [ ] Plan backups for both stores. Temporal holds live runs, the bucket holds definitions, artifacts and archives, and OrkStack ships no backup or restore command ([DEPLOY.md](./DEPLOY.md), "Nothing durable lives in a process").

## Object storage

- [ ] Use one S3-compatible bucket for every `ork` and every `orker`. Set `ORK_ARTIFACT_BACKEND=s3` and the same `ORK_S3_*` values on both binaries, because the `local` default is one directory per process ([DEPLOY.md](./DEPLOY.md), "The bucket stops being optional at the second replica").
- [ ] Use a store that implements conditional `PutObject`. Each Orker probes it at boot and refuses to start with `COMMIT_STORE_NOT_CONDITIONAL` on a store that cannot ([DEPLOY.md](./DEPLOY.md), "The bucket stops being optional at the second replica").
- [ ] Replace the development credentials with your own. The defaults are the development store's, and any `storage.s3.*` secret can be read from a file by a mapping with a single `file:` key naming its path ([DEPLOY.md](./DEPLOY.md), "1. Configuration").
- [ ] Set `ORK_S3_PATH_STYLE` for your store. It defaults to `true`, which the development store needs, and AWS S3 does not ([DEPLOY.md](./DEPLOY.md), "1. Configuration").
- [ ] Confirm that the store supports `DeleteObjects`. Run cleanup empties each run's prefixes with it ([DEPLOY.md](./DEPLOY.md), "Retention cleanup and bucket lifecycle rules").
- [ ] Never expire anything under `metadata/snapshots/`, `metadata/commits/`, `metadata/runs/`, `metadata/index/`, or the definition, registry, settings, trigger and webhook prefixes. Live reads and the archive depend on them ([DEPLOY.md](./DEPLOY.md), "Retention cleanup and bucket lifecycle rules").
- [ ] Add expiry rules only to the prefixes the lifecycle table lists as safe to expire: task logs, attempt records, artifact bytes, memos (no shorter than `ORK_CACHE_MAX_TTL`) and webhook deliveries. Each expiry must be longer than any run lasts, because expired artifact bytes fail the input staging of a running run's dependants ([DEPLOY.md](./DEPLOY.md), "Retention cleanup and bucket lifecycle rules").
- [ ] Add one bucket-wide rule that aborts incomplete multipart uploads. A killed Orker leaves its parts in the bucket, invisible and still billed. SeaweedFS removes them itself ([DEPLOY.md](./DEPLOY.md), "Retention cleanup and bucket lifecycle rules").
- [ ] Decide how long the audit trail stays. Nothing reclaims `metadata/audit/`, and OrkStack has no setting for it, so a lifecycle rule on that prefix is your retention decision ([DEPLOY.md](./DEPLOY.md), "Attributing control-plane writes").

## Replicas and pools

- [ ] Give every `ork` replica identical configuration: the Temporal address and namespace, `ORK_DEPLOYMENT_NAME`, `ORK_BUILD_ID`, the pool and the bucket. Set `ORK_HTTP_ADDR` differently only where replicas share a host ([DEPLOY.md](./DEPLOY.md), "Running N `ork` replicas").
- [ ] Put one address in front of the replicas, with a load balancer, an ingress or DNS round robin. No affinity to one replica is needed, because any replica answers for any run in the namespace the request names ([DEPLOY.md](./DEPLOY.md), "Running N `ork` replicas"). While replicas of two releases share that address during a rollout, affinity keeps a browser's UI page on one release ([DEPLOY.md](./DEPLOY.md), "The web UI").
- [ ] Route the whole host to `ork`, not only the API's paths: every replica serves the web UI at `/` on its API port. Forward WebSocket upgrades on `/ws/`, keep the browser's `Host` header, and serve the UI at the root of a host ([DEPLOY.md](./DEPLOY.md), "The web UI").
- [ ] Set `ORK_WS_ALLOWED_ORIGINS` on every replica only for a browser page served from another origin, or behind a proxy that rewrites `Host`. The UI the engine serves is on its own origin and needs none; an origin not admitted has its WebSocket upgrade refused with 403 ([DEPLOY.md](./DEPLOY.md), "Running N `ork` replicas").
- [ ] Set `ORK_ORKER_POOL` to the same value on both binaries. A task that names no pool runs on the engine's default route, which must match the Orkers that serve it ([DEPLOY.md](./DEPLOY.md), "Running M Orker pools").
- [ ] Give every Orker an `ORK_ORKER_ID` that is unique across the whole deployment. Two Orkers that share one show as a single row, and a cordon can land on either ([DEPLOY.md](./DEPLOY.md), "Removing an Orker safely: cordon and drain").
- [ ] Run every Orker of one pool on one release. Fan-out, caching and failure handlers are checked against every uncordoned Orker of each pool, so one Orker of an older build refuses them for its whole pool until it is upgraded or cordoned ([DEPLOY.md](./DEPLOY.md), "Fan-out, caching and failure handlers need every Orker of a pool on a release that supports them").
- [ ] Restrict each namespace to the pools it may use, with `orkctl namespace update NAME --allow-pool …`. An empty list means no restriction ([DEPLOY.md](./DEPLOY.md), "Running M Orker pools").
- [ ] Check pool health with `orkctl pool list` before and after a change. A pool with no live Orkers reads `0`, measured, and a pool the cluster could not answer for reads `unknown`, never `0` ([DEPLOY.md](./DEPLOY.md), "The admin surface: what a deployment is made of, and who is serving it").
- [ ] Set `ORK_ORKER_MAX_PINS` above zero on every Orker of a pool that runs pins, and leave it at zero elsewhere. A pool with no pin host refuses a pinning document at submit ([DEPLOY.md](./DEPLOY.md), "Pin hosting is a pool-wide setting").
- [ ] Plan for `PIN_HOST_LOST` in every pinned workflow. A pinned task never fails over, so a lost pin host fails the pin ([LIMITATIONS.md](./LIMITATIONS.md), "Execution, retries and cancellation").
- [ ] Set `ORK_ORKER_WORKDIR` to a path the container daemon can bind-mount. The default temporary directory is not shared by some VM-based daemons, and a task then fails with a path that plainly exists ([DEPLOY.md](./DEPLOY.md), "1. Configuration").

## Orker capacity and task limits

- [ ] Size `orker.max_concurrent` to the host and to the tasks it runs. The default is 16, and the Orker never derives it from the host ([DEPLOY.md](./DEPLOY.md), "Sizing an Orker's task ceiling").
- [ ] Set `orker.resources.max_cpu` and `orker.resources.max_memory` to what one task may take on that host. A request above the cap is refused, never clamped, and no task receives unlimited resources ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "9.3 Resource policy").
- [ ] Give every task that matters an explicit `timeout_seconds` and, where a retry is safe, a `retry:`. A task makes one attempt by default, with a 300-second timeout, and a container that times out is not retried ([DSL.md](./DSL.md), "Tasks"; [OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), "8.1 The five categories").
- [ ] Write `network: none` on every task that needs no network. Only `default` and `none` exist ([DSL.md](./DSL.md), "Tasks").
- [ ] Keep `engine.run_max_in_flight` at its default of 1,000 unless you know your cluster's pending-activity limit. A run above the cluster's limit would stall ([DEPLOY.md](./DEPLOY.md), "A run that pauses new tasks").
- [ ] Set `engine.history.max_events` and `engine.history.max_bytes` to the cluster's real limits, if an operator changed them. A value above the real limit defeats the guard ([DEPLOY.md](./DEPLOY.md), "1. Configuration").
- [ ] Keep `engine.orkflow_max_tasks` at 4,000 unless you accept that the deployment runs unmeasured above it. The boot warns when you exceed it ([DEPLOY.md](./DEPLOY.md), "Task ceiling and the flat-stage bound").
- [ ] Keep `engine.each_max_items` at 1,000 unless a fan-out needs more. Above 1,000 it needs `engine.continuation.enabled` ([DEPLOY.md](./DEPLOY.md), "1. Configuration").

## Images and supply chain

- [ ] Require digests with `orkctl admin image-policy set --require-digest`, so a moving tag cannot stand in for an image. A catalog name is exempt, so write catalog references with digests where that matters ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").
- [ ] Set `--allowed-registry` to the prefixes your images come from. A prefix matches on `/` boundaries, so `ghcr.io/acme/` does not match `ghcr.io/acmeevil/` ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").
- [ ] Register cosign public keys with `--signing-key NAME=cosign.pub`, and have each namespace that runs signed images require one with `--require-signed-by cosign:NAME`. A namespace can tighten the floor and cannot loosen it ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").
- [ ] Remember that during a rolling upgrade the image policy is enforced only by replicas that know it ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").
- [ ] Set `ORK_ORKER_REGISTRY_AUTH_FILE` only if signatures for your images sit in a private registry. The file is used only for the reads that verify a signature, it grants no push, and it never reaches a container ([DEPLOY.md](./DEPLOY.md), "1. Configuration").
- [ ] Test each image with `orkctl activity test` before you pin it. The test runs the image through the same dispatcher an Orker uses ([CLI.md](./CLI.md), `orkctl activity test`).
- [ ] Read the image digest of each attempt when you audit a run. Every digest that ever ran is recorded per attempt, with `orkctl run attempts` ([DEPLOY.md](./DEPLOY.md), "Image supply-chain policy").

## Secrets

- [ ] Set `ORK_ORKER_SECRET_ALLOW` on every Orker of a shared pool. It is the operator's floor, and it is checked by the Orker at dispatch ([DEPLOY.md](./DEPLOY.md), "Secrets on a shared pool").
- [ ] Set each namespace's `allowed_secrets` with `orkctl namespace update NAME --allow-secret 'env:SANDBOX_*'`. It is checked at apply and at every submit, and it can only narrow the floor ([DEPLOY.md](./DEPLOY.md), "Secrets on a shared pool").
- [ ] Keep secret values out of definitions. A secret reaches a task as a read-only file, and an `env:` value is stored in the definition, so it is configuration and never a secret ([DSL.md](./DSL.md), "Tasks"; [ORKFLOW_PATTERNS.md](./ORKFLOW_PATTERNS.md), "Environment variables are configuration").
- [ ] Check `orker.secret_schemes` (default `env,file`) against the sources your tasks name ([DEPLOY.md](./DEPLOY.md), "1. Configuration").
- [ ] Keep `ORK_PPROF_ENABLED` off unless you are profiling. An Orker's heap holds resolved secret bytes ([DEPLOY.md](./DEPLOY.md), "1. Configuration").

## Gateway and identity

- [ ] Put a gateway in front of `ork` that authenticates callers. OrkStack does not authenticate callers, except that the trigger invoke and the await callback check their own bearer tokens. The UI is served by `ork` on the same port, so the gateway's authentication covers it too ([ARCHITECTURE.md](../ARCHITECTURE.md), "API (`internal/api`, served by `cmd/ork`)").
- [ ] Terminate TLS at the gateway. The engine's API listener serves plain HTTP/1.1 and unencrypted HTTP/2 ([LIMITATIONS.md](./LIMITATIONS.md), "Engine, cluster and gateway").
- [ ] Have the gateway overwrite `Ork-Principal` with the identity it established, and never pass a client's value through. The engine records the header verbatim, and it is an assertion, not a verified identity ([DEPLOY.md](./DEPLOY.md), "Attributing control-plane writes").
- [ ] Decide what faces callers. `POST /v1/triggers/{id}` and the await callback are the two routes outside the operator's boundary, and OrkStack queues, retries and rate-limits neither. The UI at `/`, with its assets and routes, is operator surface on the same port, so a gateway that routes by path sends it to `ork` too ([DEPLOY.md](./DEPLOY.md), "5. Ops notes").
- [ ] Keep `ORK_ADMIN_ADDR` on a private address. It serves the health, readiness and metrics endpoints and never the API port, and it must never be bound to a Docker bridge gateway in a deployment ([DEPLOY.md](./DEPLOY.md), "7. Telemetry"; [README.md](../README.md), "Linux hosts").
- [ ] Rotate a trigger token by deleting the trigger and creating a new one. Rotation yields a new id and URL, and the new token is shown once ([ORKFLOW_TRIGGERS.md](./ORKFLOW_TRIGGERS.md), "What triggers do not do").
- [ ] Read the audit trail with `orkctl admin audit`, and treat each principal in it as asserted rather than verified ([DEPLOY.md](./DEPLOY.md), "Attributing control-plane writes").

## Run retention and cleanup

- [ ] Set a deployment maximum with `orkctl admin retention set`, and a shorter run retention on any namespace that needs one ([DEPLOY.md](./DEPLOY.md), "Run retention: a deployment maximum, a value per namespace").
- [ ] Enable cleanup on purpose. Retention is enforced only with `ORK_CLEANUP_ENABLED=true`, and cleanup deletes durable data. With it off, every run is kept for the deployment maximum plus the enforcement margin ([DEPLOY.md](./DEPLOY.md), "Run retention: a deployment maximum, a value per namespace").
- [ ] Read the confirmation before you lower the maximum. It states what the next cleanup pass removes: the logs, attempt records, snapshots and commits of finished runs older than the new value, in each namespace without a run retention of its own ([DEPLOY.md](./DEPLOY.md), "Run retention: a deployment maximum, a value per namespace").
- [ ] Remember that retention counts from when a run finished, not from when it was submitted ([DEPLOY.md](./DEPLOY.md), "Run retention: a deployment maximum, a value per namespace").
- [ ] Export an evidence bundle for each run you must keep, with `orkctl run evidence export`, and verify it offline with `orkctl run evidence verify` ([RUN_EVIDENCE.md](./RUN_EVIDENCE.md)).

## Admission and budgets

- [ ] Use a namespace hold for an incident. `orkctl namespace update NS --hold "REASON"` refuses new runs and never cancels a run already accepted ([DEPLOY.md](./DEPLOY.md), "Namespace admission: `hold` and `max_running`").
- [ ] Treat `--max-running` as a guard, not a semaphore. Submits racing across replicas can each be admitted, so a burst can land a few runs over the cap ([DEPLOY.md](./DEPLOY.md), "Namespace admission: `hold` and `max_running`").
- [ ] Check that `orkctl stats usage --since <period>` answers before you set `--budget-enforce`. An enforced budget that cannot be measured refuses new runs ([DEPLOY.md](./DEPLOY.md), "Namespace budgets: reserved consumption, soft or enforced").
- [ ] Use a soft budget for reporting. It notifies nothing, so put any alert on the threshold in your telemetry pipeline ([DEPLOY.md](./DEPLOY.md), "Namespace budgets: reserved consumption, soft or enforced").
- [ ] Read usage as a reservation multiplied by held time, not as measured utilisation or cost ([STATS.md](./STATS.md), "8. Usage").

## Releases

- [ ] Set `ORK_DEPLOYMENT_NAME` and `ORK_BUILD_ID` on every `ork` and every `orker`, and make the values identical for each release. Versioning stays off until `ORK_BUILD_ID` is set, and a deployment name may not contain a dot ([DEPLOY.md](./DEPLOY.md), "Versioning is off until you set a build id").
- [ ] Make the first versioned release current with `orkctl release promote` before you rely on it. Until a release is current it runs nothing ([DEPLOY.md](./DEPLOY.md), "3. Rolling a release forward").
- [ ] Check `orkctl release status` before every change. Each blocker is listed with what to start ([DEPLOY.md](./DEPLOY.md), "3. Rolling a release forward").
- [ ] Ramp a new release before you promote it, which the reference recommends, and watch error rates while the ramp runs ([DEPLOY.md](./DEPLOY.md), "3. Rolling a release forward").
- [ ] Wait for the old release to drain with `orkctl release wait`, and stop it only after the wait exits 0 ([DEPLOY.md](./DEPLOY.md), "3. Rolling a release forward").
- [ ] Know the rollback path before you need it. `orkctl release rollback` returns new runs to the release that was current before, and runs pinned to the release you left keep finishing there, so its workers must keep running until they drain ([DEPLOY.md](./DEPLOY.md), "4. Rolling back").
- [ ] Keep the last replica of a release up until that release reports drained ([DEPLOY.md](./DEPLOY.md), "5. Ops notes").
- [ ] Before a rollback below a release, check the floors in [UPGRADING.md](./UPGRADING.md), "Rolling back". Never roll back below the first release that reads runs carried into a fresh history.

## Draining and removing Orkers

- [ ] Drain an Orker before you stop it if it may host pins. `orkctl orker cordon ID --reason "REASON"`, then `orkctl orker drain ID --timeout 4h --watch` ([DEPLOY.md](./DEPLOY.md), "Removing an Orker safely: cordon and drain").
- [ ] Stop the process only when `orkctl orker drain` exits 0. Exit 4 means the timeout passed with work still on the host, and stopping then fails the runs whose pins it holds with `PIN_HOST_LOST` ([DEPLOY.md](./DEPLOY.md), "Removing an Orker safely: cordon and drain").
- [ ] Remember that a cordon is a flag in the Orker process, so a restarted Orker comes back ready ([DEPLOY.md](./DEPLOY.md), "Removing an Orker safely: cordon and drain").
- [ ] Remember that `SIGTERM` stops polling and cancels in-flight work, and does not cordon the Orker first ([DEPLOY.md](./DEPLOY.md), "Removing an Orker safely: cordon and drain").
- [ ] Give the Orker's process manager longer to stop it than the longest `cancellation_grace_period_seconds` its tasks use, 30 s by default: `terminationGracePeriodSeconds`, `TimeoutStopSec` or `docker stop --time`. A stopped Orker sends each container `SIGKILL` after that grace and writes each attempt's record before it exits ([DEPLOY.md](./DEPLOY.md), "Removing an Orker safely: cordon and drain").

## Observability

- [ ] Leave telemetry off until you need it. With no `OTEL_*`, `ORK_ADMIN_ADDR` or `ORK_LOG_*` variable set, nothing is exported and no listener opens ([DEPLOY.md](./DEPLOY.md), "7. Telemetry").
- [ ] Select exporters with the standard `OTEL_METRICS_EXPORTER`, `OTEL_TRACES_EXPORTER` and `OTEL_LOGS_EXPORTER` variables, and point an OTLP endpoint at your collector. The OTLP protocol is `http/protobuf` ([DEPLOY.md](./DEPLOY.md), "Recipes").
- [ ] Declare a second destination as a named sink in `ORK_TELEMETRY_SINKS`, then select it for a namespace with `--telemetry-sink`. Without a `default:` entry there, the `OTEL_*` environment is the default sink; with one, that entry overrides the environment for the default destination ([DEPLOY.md](./DEPLOY.md), "Named sinks"; "Per-namespace policy").
- [ ] Keep task-log export off unless you want the lines in your logs. `ORK_ORKER_TASK_LOG_EXPORT` needs `OTEL_LOGS_EXPORTER=otlp` ([DEPLOY.md](./DEPLOY.md), "1. Configuration").
- [ ] Keep high-cardinality labels off, which is their default, unless you accept the label cost ([DEPLOY.md](./DEPLOY.md), "High-cardinality labels").
- [ ] Set `ORK_LOG_FORMAT=json` if your log pipeline reads JSON. The default is `text` ([DEPLOY.md](./DEPLOY.md), "1. Configuration").
- [ ] Expect telemetry never to fail a task or a run. An exporter that cannot reach its collector is logged and counted, and a record that cannot be queued is dropped ([DEPLOY.md](./DEPLOY.md), "7. Telemetry").

## Before you depend on it

- [ ] Rehearse, in a deployment that carries no real work, each procedure this checklist relies on: a rollout and a rollback, an Orker drain, and a cleanup pass with retention enabled. Do not rely on a procedure under pressure until you have run it ([DEPLOY.md](./DEPLOY.md), "3. Rolling a release forward").
- [ ] Before each upgrade, re-read [LIMITATIONS.md](./LIMITATIONS.md) and, in [CHANGELOG.md](../CHANGELOG.md), the section of every release after the one you run, because breaking changes are announced there ([UPGRADING.md](./UPGRADING.md), "Breaking changes").
