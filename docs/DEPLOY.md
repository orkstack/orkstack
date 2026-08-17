# Deploying OrkStack

How to run OrkStack as a replicated deployment, and how to roll a new release
out without breaking runs that are already executing.

The short version, twice.

**Topology.** Every `ork` replica is interchangeable: nothing durable lives in
a process, so any replica answers for any run — including one it never started
and one whose replica has since died. Add replicas for API and workflow
capacity, add Orker pools for task execution. Neither holds state (§0).

**Releases.** `ork` and `orker` of one release share a **Worker Deployment
Version**. A run is **pinned** to the version that started it and finishes
there, whatever you deploy next. That guarantee costs you two rules — make each
version **Current** when you want it to serve (the first one especially: until
then it serves nothing), and do not stop the old workers until their version
reports `Drained`.

Everything here is executed against the built binaries. The topology claims and
a rollout across a live two-replica deployment: `proof/M8.5/ha-deploy.sh`, one
command. The release procedure, transition by transition:
`proof/M8.4/deploy-runbook.sh`. Their transcripts are the evidence for the
claims below.

---

## 0. Topology — what you run, and where state lives

```
                         one address (LB / ingress / DNS RR)
                                        │
           ┌────────────────┬───────────┴────┬────────────────┐
       ork replica 1    ork replica 2       …            ork replica N
       ORK_HTTP_ADDR    ORK_HTTP_ADDR                  ORK_HTTP_ADDR
       :8081            :8082                          :808N
           └────────────────┴───────────┬────┴────────────────┘
                                        │   one Temporal namespace
                              ┌─────────┴─────────┐
                              │     Temporal      │  ork-tasks           run workflows + idempotency claims
                              │                   │  ork-engine-events   projection
                              └─────────┬─────────┘  ORK_ORKER_QUEUE…    task activities, one queue per pool
           ┌────────────────┬───────────┴────┬────────────────┐
     Orker pool 1      Orker pool 2         …           Orker pool M
           └────────────────┴───────────┬────┴────────────────┘
                                        │
                              one S3 / MinIO bucket
                    definitions · artifacts · finished runs
```

Two independent dials. **Replicas** carry the API and the run workflows;
**Orker pools** carry task execution. Neither holds state, so both scale by
adding processes and both survive losing one.

### Nothing durable lives in a process

| state | where it lives | code |
|---|---|---|
| orkflow definitions | the bucket | `artifacts.S3OrkflowStore`, opened at boot by `openOrkflowStore` (`cmd/ork/main.go`). It keeps no cache, so an `apply` on one replica is visible to every other |
| live runs — status, task state, event stream | Temporal history | `engine.TemporalRunReader` (`internal/engine/reader.go`), wired as the API's `RunReader` |
| finished runs, past retention | the bucket — **once `ORK_ARTIFACT_BACKEND=s3`** | `engine.NewRunArchiver` over `artifacts.NewRunArchive`, written by the terminal `PublishEvent`. The default backend is `local`: a directory on each host, so replicated it is one archive per replica (§1) |
| idempotency keys | Temporal | `engine.IdempotencyClaimWorkflow` (`internal/engine/claim.go`), held for `ORK_IDEMPOTENCY_TTL` |
| artifacts | the bucket — **once `ORK_ARTIFACT_BACKEND=s3`** | `pkg/artifacts`; events and payloads carry refs, never bytes. Same default, same consequence: on `local` an Orker writes to its own disk and no other host can read the bytes back |
| the in-process `RunStore` / `EventBus` | one process, and only that one | **write side only**: the candidate run a submission creates, the stream of a run whose start failed, and the archive + webhook write path. Behind a wired reader they are never read (`internal/api/service.go`, `LookupRun`) |

So a replica owns nothing. Kill one and you lose its in-flight HTTP
connections and nothing else: its runs keep executing on another replica's
workers, and a client that reconnects anywhere reads the same run. That is
measured, not argued — `proof/M8.5/ha-transcript.txt` phase 4 kills the
submitting replica with `-9` mid-run and the run finishes.

**One variable decides whether two of those rows are true.**
`ORK_ARTIFACT_BACKEND` defaults to `local` — a directory per process
(`pkg/artifacts/config.go`) — which is right for `make run-ork` on a laptop
and wrong for every topology in this document. A replicated deployment sets
`s3` and the `ORK_S3_*` values **on both binaries** (§1). Orkflow definitions
are the one piece of state that needs no decision: the engine's store is S3
whatever the backend says (`openOrkflowStore`, `cmd/ork/main.go`), so `ork`
needs `ORK_S3_*` either way.

### Running N `ork` replicas

- **Identical configuration on every replica.** Same `ORK_TEMPORAL_ADDRESS`,
  `ORK_TEMPORAL_NAMESPACE`, `ORK_ORKER_QUEUE`, `ORK_DEPLOYMENT_NAME`,
  `ORK_BUILD_ID`, and the same bucket — `ORK_ARTIFACT_BACKEND=s3` with one set
  of `ORK_S3_*`. A replica differing in any of those is a different deployment
  wearing the same address.
- **`ORK_HTTP_ADDR` is the one variable that must differ** when replicas share
  a host. On separate hosts it does not have to.
- **No sticky sessions, no leader.** `GetRun`, `ListRuns`, `StreamEvents` and
  `/ws/runs/{id}` are answered from Temporal, so any replica serves any run.
- **Reconnects resume rather than replay.** `StreamEvents.from_sequence`
  (`orkctl run watch --from-sequence`) and `/ws/runs/{id}?from_sequence=N`
  continue after an event number, and the number is the producer's — the
  workflow assigns it — so resuming against a *different* replica yields the
  same stream with nothing repeated and nothing skipped.
- **Readiness: there is no `/healthz` yet.** Probe with a cheap unary call —
  `OrkflowService.ListOrkflows`, i.e. `orkctl --server … orkflow list`. Every
  script under `proof/` that starts today's engine binary gates on exactly
  that; it reaches the bucket the replica loads definitions from, so it fails
  while the replica cannot serve. (`proof/M6/e2e.sh` is the one exception, and
  not a counter-example: it predates the M7.1 rename and drives `bin/orkengine`
  with that release's `workflow list`.)
- **Browser clients need `ORK_WS_ALLOWED_ORIGINS`** set on every replica, since
  any of them may take the socket.
- **Every replica is also a worker host.** It polls `ork-tasks` (run workflows
  and idempotency claims) and `ork-engine-events` (projection). Adding a
  replica therefore adds workflow capacity as well as API capacity, and it is
  why a run outlives the replica that started it: the next workflow task goes
  to another replica's poller.

### Running M Orker pools

- **One pool = one queue = one set of Orker processes** sharing an
  `ORK_ORKER_QUEUE`. Add processes to a pool for throughput; add a pool for
  isolation — a GPU host, a pool with different plugins, a pool near some data.
- **The engine's default pool must match.** The engine dispatches every task
  that names no `queue:` to its own `ORK_ORKER_QUEUE`, so the variable is set
  on both binaries or runs strand in `RUNNING` (§2, §5). A task's own `queue:`
  in the DSL outranks it, and is how one step reaches a dedicated pool
  (`docs/DSL.md`).
- **Under versioning, every pool a release dispatches to needs an Orker of that
  release's version.** A pool left on the old build is not "lagging": it is the
  pinned-activity exception (§2) firing, which sends that queue's tasks to
  whatever is current there — including tasks of a run pinned elsewhere. Roll
  every pool together, and check each queue appears in
  `describe-version` (§3 step 2).
- **A pool can name itself.** `ORK_ORKER_ENV_ALLOWLIST=ORK_BUILD_ID` on the
  Orker lets a task render `{{ env "ORK_BUILD_ID" }}`; the template resolves in
  the Orker process (`internal/orker/render.go`), so the task output names the
  build that executed it. `proof/M8.5` uses this to show a task scheduled
  *after* a rollout still executed on the old build — from the plugin's own
  output, independent of what the server records.

---

## 1. Configuration

Every setting is an environment variable prefixed `ORK_`. These are the ones a
deployment cares about.

| variable | default | set on | effect |
|---|---|---|---|
| `ORK_TEMPORAL_ADDRESS` | `localhost:7233` | both | Temporal frontend. The default is the SDK's `client.DefaultHostPort` |
| `ORK_TEMPORAL_NAMESPACE` | `ork-main` | both | hard partition; nothing crosses it. Provisioned at boot |
| `ORK_ORKER_QUEUE` | `ork-tasks` | **both** | queue Orkers poll and the engine dispatches **task activities** to. It does not move the engine's own queues (§2) |
| `ORK_ORKER_ENV_ALLOWLIST` | *(empty)* | `orker` | comma-separated names `{{ env "NAME" }}` may read in a task's `with:` — resolved on the Orker, so the value is the pool's |
| `ORK_ORKER_WORKDIR` | temp dir | `orker` | root under which each task attempt gets its own working directory |
| `ORK_DEPLOYMENT_NAME` | `orkstack` | **both** | the Worker Deployment both binaries join |
| `ORK_BUILD_ID` | *(empty)* | **both** | this release's identity. **Empty means versioning is off** |
| `ORK_HTTP_ADDR` | `:8080` | `ork` | Connect API + WebSocket bridge |
| `ORK_WS_ALLOWED_ORIGINS` | *(empty)* | `ork` | comma-separated browser origins allowed onto `/ws/runs/{id}` |
| `ORK_IDEMPOTENCY_TTL` | `24h` | `ork` | how long an idempotency key stays bound |
| `ORK_ARTIFACT_BACKEND` | `local` | **both** | where artifacts and the run archive live. **A replicated deployment must set `s3`** — `local` is a directory on the host, so each replica and each Orker would keep its own (§0) |
| `ORK_ARTIFACT_DIR` | `./.data/artifacts` | both | filesystem root for the `local` backend; ignored under `s3` |
| `ORK_S3_ENDPOINT` | `http://localhost:9000` | both | S3 / MinIO endpoint. `ork` reads orkflow definitions from S3 whatever `ORK_ARTIFACT_BACKEND` says, so this one is never optional for the engine |
| `ORK_S3_REGION` | `us-east-1` | both | |
| `ORK_S3_BUCKET` | `orkstack-artifacts` | both | one bucket holds definitions, artifacts and archived runs |
| `ORK_S3_ACCESS_KEY` | `orkstack` | both | the dev credential from `deploy/docker-compose.yml`. Supply a real one |
| `ORK_S3_SECRET_KEY` | `orkstack-secret` | both | as above |
| `ORK_S3_PATH_STYLE` | `true` | both | path-style addressing, which MinIO requires |

### The bucket stops being optional at the second replica

`local` is the single-host default, and the one setting that breaks a
replicated deployment quietly: each process keeps its own directory, so a run
archived by replica 1 is invisible to replica 2 and an artifact written by one
Orker cannot be materialized by another. Set the same values on every `ork`
and every `orker`:

```bash
export ORK_ARTIFACT_BACKEND=s3
export ORK_S3_ENDPOINT="https://s3.internal.example"   # or http://minio:9000
export ORK_S3_BUCKET=orkstack-artifacts
export ORK_S3_REGION=us-east-1
export ORK_S3_ACCESS_KEY=…        # never the docker-compose dev credentials
export ORK_S3_SECRET_KEY=…
export ORK_S3_PATH_STYLE=true     # MinIO needs path style; AWS S3 does not
```

One bucket, three key families (`pkg/artifacts`):

| keys | what |
|---|---|
| `metadata/orkflows/{name}/{version}.json` | orkflow definitions — written by `apply`, read at every boot |
| `metadata/runs/{run-id}.jsonl` and `metadata/index/runs/…` | archived runs: the body, and the key-only index `ListRuns` scans |
| `orkflows/{orkflow}/{run-id}/{task-id}/{name}` | artifacts, addressed by provenance |

`proof/M8.5/ha-deploy.sh` exports exactly that block, and phase 7 of
`proof/M8.5/ha-transcript.txt` reads a finished run back out of the bucket —
with a control showing nothing was written to local disk.

### Versioning is off until you set a build id

With no `ORK_BUILD_ID`, both binaries register unversioned workers and behave
exactly as they did before versioning existed. That is what keeps a
single-process `make run-ork` + `make run-orker` working with no configuration
at all, and it is deliberate: a build id is the release's identity and there is
no sane value to invent for it.

`ORK_DEPLOYMENT_NAME` *does* have a default, so **one variable turns versioning
on in the workers**:

```bash
export ORK_BUILD_ID="2026.08.16-a1b2c3d"     # → deployment "orkstack", version "orkstack.a1b2c3d"
```

Both binaries print what they resolved, on every boot:

```
level=INFO msg="worker deployment versioning" enabled=true deployment=orkstack \
  build_id=2026.08.16-a1b2c3d version=orkstack.2026.08.16-a1b2c3d \
  run_workflow_behavior=Pinned claim_workflow_behavior=AutoUpgrade event_worker=unversioned
```

A deployment name may not contain `.` — the cluster reserves it. Both binaries
refuse to start on one rather than let the server reject it on the first poll.

### …and the first versioned release needs one call on the cluster

Registering a version does not route anything to it. Until the deployment has
a **Current Version**, the task queue's current version is nil, and

> A nil Current Version represents all the unversioned workers
>
> — `api@v1.63.4/workflowservice/v1/request_response.pb.go`,
> `SetWorkerDeploymentCurrentVersionRequest.BuildId`

which a versioned release does not have. So the first release that sets
`ORK_BUILD_ID` boots two healthy, polling workers that receive **nothing**:
submitted runs sit at `HistoryLength 2` (`WorkflowExecutionStarted`,
`WorkflowTaskScheduled`), no pending activities, indefinitely. Nothing in the
logs says why, because nothing is wrong with the workers.

One call fixes it, and no worker has to restart:

```bash
temporal worker deployment set-current-version \
  --deployment-name orkstack --build-id "$ORK_BUILD_ID"
```

Backlogged runs start within seconds of it. §3 step 0 is the full first-release
procedure; every release after that inherits a Current Version from its
predecessor, and takes over with the same call at §3 step 4.

Measured, both ways: `proof/M8.4/deploy-runbook-transcript.txt` phase 1 (the
real binaries) and `TestIntegration_AVersionedReleaseIsIdleUntilItsVersionIsCurrent`
(`internal/engine`), which watches a versioned release receive no workflow task
at all for 15s and then complete the same run 1s after the routing change.

---

## 2. One deployment spans both binaries

`ork` and `orker` are **released in lockstep as one version**. This is not a
convention; it is what makes the pinning guarantee hold.

Temporal routes the activities of a `PINNED` workflow to the workflow's own
Deployment Version — with one exception:

> Activities of `PINNED` workflows are sent to the same Deployment Version.
> Exception … when the activity Task Queue workers are not present in the
> workflow's Deployment Version, in which case the activity will be sent to the
> **Current Deployment Version of its own task queue**.
>
> — `api@v1.63.4/enums/v1/workflow.pb.go`, `VERSIONING_BEHAVIOR_PINNED`

So if the Orker polling `ORK_ORKER_QUEUE` does not carry the engine's exact
`(ORK_DEPLOYMENT_NAME, ORK_BUILD_ID)`, the exception fires and an in-flight
run's tasks land on whatever is current — new plugin code, mid-run. Setting
`ORK_BUILD_ID` on the engine and forgetting it on the Orker is therefore the
one misconfiguration to watch for. Check both boot lines print the same
`version=`.

### What is versioned, and what is not

| worker | queue | versioned | behaviour |
|---|---|---|---|
| run worker (`ork`) | `ork-tasks` — **fixed** | yes | `RunWorkflow` = **Pinned**, `IdempotencyClaimWorkflow` = **AutoUpgrade** |
| Orker (`orker`) | `ORK_ORKER_QUEUE` (default `ork-tasks`) | yes | task activities only; must match the engine |
| projection worker (`ork`) | `ork-engine-events` | **no** | `PublishEvent` |

**`ORK_ORKER_QUEUE` does not move the engine.** It names the queue Orkers poll
and the queue the engine dispatches task activities to. The queue that hosts
`RunWorkflow` and the idempotency claims is `internal/task.DefaultQueue` — the
constant `ork-tasks` — whatever `ORK_ORKER_QUEUE` says (`cmd/ork/main.go`,
`queuesFor`; `TestQueues_OrkerQueueMovesTaskExecutionOnly`). With the variable
unset both names are `ork-tasks` and the distinction is invisible; set it, and
one release polls two queues:

```
msg="temporal workers started" task_queue=ork-tasks event_queue=ork-engine-events \
  orker_queue=ork-tasks-gpu-pool
```

Under versioning both appear, separately, in that version's poller list — the
engine's queue as a **workflow** poller and the Orker's as an **activity**
poller. That is the shape §3 step 2 checks, and reading it as one name aborts
healthy rollouts.

**Why `RunWorkflow` is Pinned.** It interprets a DAG whose scheduling order,
event sequence and activity arguments are decided by current code. Change the
ready-task ordering, the event counter, or the shape of the task activity
input, and replaying a half-finished history against the new build produces a
different command stream — a non-determinism error that kills a run the
operator did nothing to. AutoUpgrade would hand exactly that history to the new
build on its next workflow task. Pinned lets the run finish on the code that
started it.

**Why the claim workflow is AutoUpgrade.** Its history is one update and one
timer, its command stream is independent of run semantics, and it lives for
`ORK_IDEMPOTENCY_TTL` (default 24h). Pinning it would strand a long-lived key
holder on a version you want to decommission, and that version would never
report `Drained`.

**Why the projection worker is unversioned.** `PublishEvent` writes the read
model and the archive from arguments the workflow already computed. It carries
no run semantics, and it must stay executable by *any* live replica of *any*
version — that is what lets every replica answer a read for every run. With no
versioned poller on `ork-engine-events`, the exception clause above routes
those activities to the queue's own current version, i.e. to the unversioned
pollers. `internal/engine/versioning_rollout_integration_test.go` asserts this
directly: in a rollout, a v1-pinned run's remaining events are published by a
replica the v1 release does not contain.

---

## 3. Rolling a release forward

```bash
export ORK_DEPLOYMENT_NAME=orkstack
export ORK_TEMPORAL_NAMESPACE=ork-main
NEW=2026.08.16-a1b2c3d      # v2
OLD=2026.08.09-9f8e7d6      # v1, currently serving
```

Steps 1–6 are every release that has a predecessor. **The very first versioned
release of a deployment has none, so start at step 0** — skip it and the
release runs nothing at all (§1).

0. **First release only: make it Current before you rely on it.**

   ```bash
   FIRST=2026.08.02-0a1b2c3
   ORK_BUILD_ID=$FIRST ./bin/ork &
   ORK_BUILD_ID=$FIRST ./bin/orker &

   # both queues must report pollers first — same check as step 2
   temporal worker deployment describe-version \
     --deployment-name orkstack --build-id "$FIRST"

   temporal worker deployment set-current-version \
     --deployment-name orkstack --build-id "$FIRST"

   temporal worker deployment list      # CurrentVersionBuildID must not be empty
   ```

   Runs submitted before that call are backlogged, not lost: they start within
   seconds of it and no worker restarts. If you are switching a system that is
   already serving unversioned traffic, read §3.5 first — runs in flight across
   the switch behave differently from runs in flight across steps 1–6.

1. **Start v2 alongside v1.** Same namespace, same queues, new `ORK_BUILD_ID`.
   Do not stop anything yet.

   ```bash
   ORK_BUILD_ID=$NEW ./bin/ork &
   ORK_BUILD_ID=$NEW ./bin/orker &
   ```

   In a replicated deployment (§0) that means **all** of v2: every `ork`
   replica and an Orker in every pool the release dispatches to. v1's replicas
   stay up and keep serving the API — the two versions coexist behind one
   address, and which version a new run executes on is the deployment's
   Current Version, never the replica the client happened to reach. `proof/M8.5`
   shows this directly: after the switch, a run submitted **to a v1 replica**
   executes wholly on v2.

2. **Check v2 polls every queue it should**, engine and Orker both:

   ```bash
   temporal worker deployment describe-version \
     --deployment-name orkstack --build-id "$NEW"
   ```

   Two entries must be there, one per role. They are two different queue names
   whenever `ORK_ORKER_QUEUE` is set (§2):

   ```
   Task Queues:
     Name                  Type
     ork-tasks             workflow     <- the engine's run worker (fixed name)
     ork-tasks-gpu-pool    activity     <- the Orker, i.e. ORK_ORKER_QUEUE
   ```

   Take the two names from v2's own boot line — `task_queue=` and
   `orker_queue=` — rather than assuming them. A missing **activity** entry
   means v2's engine is running without v2 Orkers: stop here, because a pinned
   run's tasks would fall through to that queue's own current version and land
   on the wrong plugin code. A missing **workflow** entry means v2's Orker is
   running without a v2 engine. Do not expect `ORK_ORKER_QUEUE` to appear under
   `workflow`: it never does, and treating that as a failure aborts a healthy
   rollout.

3. **Ramp** (optional but recommended):

   ```bash
   temporal worker deployment set-ramping-version \
     --deployment-name orkstack --build-id "$NEW" --percentage 10
   ```

   New runs split between the versions. Watch error rates. Raise the percentage
   as confidence grows.

4. **Make v2 current:**

   ```bash
   temporal worker deployment set-current-version \
     --deployment-name orkstack --build-id "$NEW"
   ```

   From this moment every **new** run executes on v2. Every run already in
   flight stays on v1 and finishes there.

   `temporal worker deployment list` reports v2 as Current the instant the call
   returns. That is the deployment's routing **config** — your intent, recorded
   by the deployment's own workflow. Dispatch reads something else: the task
   queue's own current version, which the server syncs to the queues just
   afterwards. On a healthy cluster the gap is milliseconds and no run is lost
   either way (a run started a moment early simply runs on v1 and is pinned
   there). It matters if you **automate** a check around the switch: assert on
   the queue, or you are asserting on a fact the dispatcher has not seen yet.
   That mistake made this milestone's own runbook proof red one execution in
   three; see `proof/M8.4/README.md`.

   ```bash
   # both physical queues, engine and Orker, must report the new version
   go run ./proof/M8.4/tools/queue-routing \
     -namespace "$ORK_TEMPORAL_NAMESPACE" \
     -queue ork-tasks:workflow -queue ork-tasks-gpu-pool:activity \
     -want "orkstack.$NEW"
   ```

   It is a Go program because the CLI does not surface this: `versioning_info`
   comes back only in `DescribeTaskQueue`'s default api mode, and it is per
   **physical** queue — the engine's queue carries the version on its workflow
   side, the Orker's on its activity side, and asking either about the other
   answers `__unversioned__`.

5. **Wait for v1 to drain — do not skip this:**

   ```bash
   temporal worker deployment describe-version \
     --deployment-name orkstack --build-id "$OLD" \
     | grep -i drainage
   # DrainageStatus: Draining  → v1 still has pinned runs executing
   # DrainageStatus: Drained   → safe to stop v1
   ```

   `Draining` means pinned runs still need v1's workers. Stopping them there
   leaves those runs stuck with no worker to advance them; they resume if you
   bring v1 back, but until then they make no progress. Drainage is refreshed
   on a delay of a few minutes, so poll.

6. **Stop v1** once it reports `Drained` — every replica and every Orker
   carrying that build id. Stopping *some* of them earlier is safe (§5); the
   version is what must not disappear while runs are pinned to it.

A rollout done this way is proven end to end on the real binaries twice, from
two angles. `proof/M8.4/deploy-runbook-transcript.txt` (phase 3) walks the
transitions one release pair at a time: the run in flight when v2 became
Current finished on v1 and **both** of its tasks — including the one scheduled
after the switch — were executed by v1's Orker, while the run submitted after
the switch ran wholly on v2. `proof/M8.5/ha-transcript.txt` does it across a
live **two-replica** v1 deployment, and adds the plugin's own account: the task
scheduled after the switch printed the build id of the Orker that ran it, `v1`.

---

## 3.5 Turning versioning on for a system that is already running

Steps 1–6 move runs between two versions. Turning versioning on for the first
time moves them from *no* version, and that is not symmetric: a run that
started unversioned has **nothing pinning it**.

> Workflow execution does not have a Versioning Behavior and is called
> Unversioned. This is the legacy behavior.
>
> — `api@v1.63.4/enums/v1/workflow.pb.go`, `VERSIONING_BEHAVIOR_UNSPECIFIED`

So when the first version becomes Current, an unversioned run in flight
**upgrades onto it** at its next workflow task, and is pinned there from then
on. Measured, with the real binaries, in
`proof/M8.4/deploy-runbook-transcript.txt` (phase 2): a run whose first task
was executed by an unversioned Orker finished on `orkstack-runbook.v1`, and its
second task ran on v1's Orker.

That is convenient — nothing strands — but it is the one moment when a
**partial history meets a different build**: the new version replays what the
unversioned build wrote. Pinning cannot protect it, because the run was not
pinned when it started. Two ways to handle it:

- **Drain first (safest).** Stop submitting, let running runs finish, then do
  §3 step 0. Nothing is ever replayed across builds.
- **Switch live.** Acceptable when the versioned release is the same code as
  the unversioned one already serving — the usual case, since a build id is a
  label on a build you are already running. This is exactly what the committed
  history replay guard covers (§6): it proves current code still reproduces an
  older build's command stream.

Either way, keep the old workers up until their runs are done. That rule is
not about versioning: an Orker killed mid-task loses that attempt until its
`timeout_seconds` expires, and the default retry policy is a single attempt
(`internal/engine/workflow.go`, `retryPolicyFor`), so the task fails.

---

## 4. Rolling back

```bash
temporal worker deployment set-current-version \
  --deployment-name orkstack --build-id "$OLD"
```

New runs go back to v1 immediately. **Runs already pinned to v2 keep finishing
on v2** — that is the intended outcome, not a fault, and it is why v2's workers
must stay up until *they* drain.

Both halves are measured, on the real binaries, in
`proof/M8.5/ha-transcript.txt` (phase 6): a run was put in flight on v2, v1 was
made Current again, and a run submitted **to the v2 replica** afterwards
executed on v1 — while the in-flight run finished on v2, its task scheduled
*after* the rollback executed by v2's Orker.

This only works while v1's workers are still running, which is the real rule
behind the drain wait: **never decommission the previous version until the next
one is proven.** Pinning cuts both ways: after a rollback, v2 is the version you
cannot decommission yet.

If v2 is already gone and a run is stranded on it, the escape hatch is a
per-execution versioning override:

```bash
temporal workflow update-options \
  --workflow-id ork-1786941899450567000 \
  --versioning-override-behavior pinned \
  --versioning-override-deployment-name orkstack \
  --versioning-override-build-id "$OLD"
```

That moves one run onto a version that did not start it, so the determinism
argument in §2 applies in full: if the two builds disagree about the command
stream, the run fails its next workflow task. Use it to rescue a run you would
otherwise abandon, not as a routine tool.

### Back to unversioned workers

To take a deployment out of versioned routing altogether — an escape hatch,
not a release step — point its Current Version at the unversioned workers:

```bash
temporal worker deployment set-current-version \
  --deployment-name orkstack --unversioned
```

New runs then go to whatever unversioned pollers exist, so start those first;
`--allow-no-pollers` overrides the check if you have to do it in the other
order, and leaves new runs backlogged until an unversioned worker appears.
Runs already pinned to a version still need that version's workers.

---

## 5. Ops notes

- **Run ids** are `ork-{timestamp}-{sequence}` and single-use for all time.
  Nothing may parse or order by one — the sequence carries a per-process nonce
  in its high half (`internal/engine/runid.go`), so it is comparable only
  within one replica and means nothing across two.
- **Any replica answers any run** (§0). Live runs are read from Temporal,
  finished ones from the archive in the bucket — the half that needs
  `ORK_ARTIFACT_BACKEND=s3` (§1), because on the default `local` backend each
  replica archives to its own disk and only that replica can answer for a run
  past Temporal retention. Nothing durable lives in a process, so a read does
  not need to reach the replica that started the run — or that replica to
  still exist.
- **A double submit is settled by the cluster, not by a replica.** One
  `--idempotency-key` sent to two replicas at once returns one run id, because
  the claim is a Temporal workflow (`engine.IdempotencyClaimWorkflow`), not
  process memory. Retries are safe from any address behind the load balancer.
- **`ORK_ORKER_QUEUE` must be set on both binaries.** Set it on only one and
  runs strand in `RUNNING`: the Orker polls one queue while the engine
  dispatches to another. It moves **task execution only** — the engine's own
  workers stay on `ork-tasks` and `ork-engine-events` (§2). A task's own
  `queue:` in the DSL outranks the engine default and is how a step reaches a
  dedicated Orker pool (`docs/DSL.md`).
- **One namespace, one deployment.** The engine's queue name is fixed, so two
  OrkStack deployments in one namespace would fight over the current version of
  `ork-tasks`. Give each environment its own namespace, as above.
- **`ORK_TEMPORAL_NAMESPACE` is the hard partition.** Nothing — no workflow, no
  activity, no queue, and no worker deployment — crosses it. Parallel test or
  staging environments get their own namespace.
- **Rolling a replica.** Stop it whenever you like: its runs continue on the
  other replicas' workers, and a client reconnects anywhere with
  `from_sequence`. Under versioning the constraint is the *version*, not the
  replica — do not take the **last** replica of a version down until that
  version reports `Drained` (§3 step 5).
- **Process hygiene.** Before any manual end-to-end, kill stray workers:

  ```bash
  pkill -f '/bin/orker$'; pkill -f '/bin/ork$'; sleep 2
  ps aux | grep -E 'bin/ork|bin/orker' | grep -v grep   # must be empty
  ```

  Anchor both patterns with `/` and `$`, as `AGENTS.md` §6 does. Unanchored,
  `bin/ork` also matches `bin/orkctl` — the cleanup then kills the CLI you are
  about to drive the deploy with.

---

## 6. Replay safety across versions

A pinned run finishing on an old build is Temporal replaying an old history
against that build's code — fine by construction. The risk is the reverse:
current code meeting a history an older build wrote. That happens on a
rollback, on a versioning override, after any worker restart, and — the one
case a rollout cannot avoid — when a run that started unversioned upgrades onto
the first version (§3.5).

`internal/engine/replay_test.go` guards it. It replays a **committed** history
(`internal/engine/testdata/history/run-succeeded-v1.json`), recorded from a real
multi-task run by an earlier build, against whatever the workflow code is
today. It runs in `make test`, with no cluster.

If it fails, current code no longer reproduces that command stream. Either
restore it (`workflow.GetVersion` for a deliberate change), or — if the
contract genuinely changed and no old run can still be in flight — re-record
the fixture and say so in the commit:

```bash
ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ \
  -run RecordGoldenRunHistory
```

Re-recording resets the guard to today. Do it deliberately, never to make a
red test green.
