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
| live runs — status, task state, event stream | Temporal history | `engine.TemporalRunReader` (`internal/engine/reader.go`), handed to the API as `Binding.Reader` by `engine.NamespaceRouter` (`internal/engine/router.go`) |
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
  `/ws/runs/{id}` are answered from Temporal, so any replica serves any run —
  inside the namespace the request names. A run id addresses a run within a
  namespace, not across the deployment: every one of those four carries the
  namespace (`/ws/runs/{id}?namespace=…`), an absent one is `default`, and a
  run of another namespace is `NotFound`.
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
  any of them may take the socket. Unset, the engine allows same-origin only,
  which is right for production and wrong the moment the UI is served from its
  own port: the upgrade is refused with 403, the run page never goes live and
  its event log stays empty, while a page refresh still works because that
  reads through Connect rather than the socket. The engine logs
  `websocket upgrade refused` with the origin, the host and this variable's
  value, and `make up` probes the handshake and refuses to report a stack the
  browser cannot stream from.
- **Every replica is also a worker host.** It polls `ork-tasks` (run workflows
  and idempotency claims) and `ork-engine-events` (projection). Adding a
  replica therefore adds workflow capacity as well as API capacity, and it is
  why a run outlives the replica that started it: the next workflow task goes
  to another replica's poller.

### Running M Orker pools

- **A pool is a set of Orkers, and `ORK_ORKER_POOL` is how a process joins
  one.** Start Orkers with `ORK_ORKER_POOL=gpu` and they serve the pool a task
  reaches by writing `pool: gpu` (`docs/DSL.md`). Add processes to a pool for
  throughput; add a pool for isolation — a GPU host, a pool with different
  plugins, a pool near some data. `main` is the built-in pool and needs no
  configuration: `make run-ork` + `make run-orker` with nothing set is one
  Orker serving `main`.
- **`ORK_ORKER_QUEUE` is the operator escape hatch underneath it**, naming the
  raw Temporal task queue directly. It exists for the test harnesses, which
  isolate concurrent runs by pinning a queue no other agent polls (AGENTS.md
  §6). Prefer `ORK_ORKER_POOL`: a pool is a name the whole system speaks, a
  raw queue is nobody's pool and is reported as `main` with a boot log saying
  so.
- **Set both and they must agree, or the process refuses to boot.** The check
  is `task.ResolvePoolBinding`, and both binaries run it:

  | set | channel polled / dispatched to | pool reported |
  |---|---|---|
  | neither | `ork-tasks` | `main` |
  | `ORK_ORKER_POOL=gpu` | `ork-pool-gpu` | `gpu` |
  | `ORK_ORKER_QUEUE=ork-tasks-m7-9` | that queue, verbatim | `main`, with a boot log naming the raw queue |
  | both, agreeing | that queue | that pool |
  | both, disagreeing | **boot fails** | `ORK_ORKER_POOL=gpu resolves to "ork-pool-gpu" but ORK_ORKER_QUEUE="ork-tasks-m7-9"` |

  Refusing is the point. Silently picking a winner is how an Orker ends up
  polling one channel while the engine dispatches to another, which strands
  every run in `RUNNING` with nothing to say why.
- **The engine's default route must match its Orkers.** A task that names no
  `pool:` runs on its namespace's `default_pool`, then on `main`. Setting
  `ORK_ORKER_POOL` (or `ORK_ORKER_QUEUE`) on the engine pins that default
  route to the same channel its Orkers poll, so the variable goes on **both**
  binaries. A task that names a pool of its own goes to that pool's channel
  instead, which is how one step reaches a dedicated pool (`docs/DSL.md`) —
  with one exception that is not an exception: `pool: main` names *this
  deployment's default pool*, not the channel `ork-tasks`, so it follows
  whatever you bound here, exactly as an omitted `pool:` does. `main` is a
  word for the default, never an override of it, so no document can escape an
  operator's isolation by naming it (`task.RouteForPool`,
  `runState.routeForTask`,
  `TestPoolResolver_NamingTheDefaultPoolCannotEscapeTheOperatorsRoute`).
- **One set of Orkers has exactly one name.** With `ORK_ORKER_POOL=gpu` on both
  binaries the deployment's default channel *is* gpu's channel, so `main` and
  `gpu` are two words for the same machines. Everything reports the second:
  `ListPools` returns one pool, named `gpu`, marked default; a task that named
  no pool is scheduled, projected and logged on `gpu`. The collapse happens in
  one function (`task.RouteForPool`) and nowhere else. Reporting both would
  give an operator two pools to size and two to drain when there is one, and
  tell a client a task ran on `main` while gpu's Orkers ran it.
- **A submit into a pool with no live Orkers is refused, at submit.** The
  engine asks Temporal which workers are polling the pool's channel and
  refuses the submission when the answer is none, naming the pool, the tasks
  and the fix:

  ```
  pool "gpu" (namespace "payments", tasks: settle) has no live Orkers, so nothing
  would pick these tasks up. Start one with ORK_ORKER_POOL=gpu, or submit with
  allow_empty_pool to wait for one
  ```

  The window is 60 seconds of poller freshness, applied to each poller
  separately: a worker that stopped polling more than 60 seconds ago is not
  counted, however busy its neighbours are. Temporal remembers a poller for
  minutes after its worker died, so a pool you scaled from three Orkers to one
  would otherwise report three for as long as the survivor kept the newest
  record fresh. Two workers of one release can also be described twice while a
  version is being promoted, so the count is per worker identity, not per
  record.

  The residue is bounded and deliberate: inside those 60 seconds a stale record
  can still say "live" just after an Orker stopped. That degrades to the old
  behaviour — a run that waits — never to a false rejection of a healthy
  submit. `SubmitRunRequest.allow_empty_pool` submits anyway; it is off by
  default and taking it logs `WARN submit into empty pool` with the pool and
  the tasks.

  A cluster that cannot be asked is a third answer, and it is kept apart from
  the other two. "No Orkers" is `failed_precondition` — start one, or take the
  override. A probe that got no reply at all (Temporal unreachable, or slower
  than its five-second bound) measured nothing, and the submission it refuses
  may be perfectly good, so it is `unavailable`: ask again. Reported as
  `invalid_argument` it would send every client off to fix a request with
  nothing wrong with it for as long as the cluster was unwell.
- **A namespace's `allowed_pools` bounds what it may dispatch onto**, and it is
  enforced on the server at both apply and submit, so no client can skip it.
  An empty list means no restriction.

  The list is read in the same vocabulary as the dispatch — the collapsed name
  of the bullet above. With `ORK_ORKER_POOL=gpu` on the engine, a task that
  names no pool is dispatched to, stamped as and reported on `gpu`, so a
  namespace whose list is `[main]` is refused there and one whose list is
  `[gpu]` accepts `pool: main`:

  ```
  namespace "payments" does not allow pool "gpu" for task "settle" (allowed: main);
  this deployment dispatches pool "main" onto "gpu"'s Orkers, so allow "gpu" or
  drop the override
  ```

  Every pool a run reports is therefore one its namespace allows. Bind an
  execution channel to a pool's own queue and the namespaces on that
  deployment must allow that pool by name; `main` there names Orkers this
  deployment does not have.
- **"Is anything serving my pools?" has an answer.**
  `NamespaceService.ListPools` reports each pool's name, its live Orker count
  and the plugins those Orkers run. The count is the one described above — the
  Orkers polling now, one per worker — so scaling a pool down shows up in it
  within the window rather than minutes later. Every number is read from the
  cluster, so any replica answers, including one that started nothing. It
  reports each pool from the same channel a submit dispatches it to, so an
  engine using the `ORK_ORKER_QUEUE` escape hatch reports its default pool
  from that raw queue — the Orkers it accepts runs onto are the Orkers it
  says the pool has. A `orkctl` command for it arrives with the namespace
  CLI; until then it is one Connect call.
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
| `ORK_ORKER_POOL` | `main` | **both** | the pool an Orker joins, and the pool the engine dispatches a task naming none to. The OrkStack-level setting; prefer it |
| `ORK_ORKER_QUEUE` | *(derived from the pool)* | **both** | operator escape hatch: the raw execution channel, for test harnesses that isolate concurrent runs (AGENTS.md §6). Set with `ORK_ORKER_POOL` and the two must agree or the process refuses to boot. It does not move the engine's own channels (§2) |
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

One bucket, four key families (`pkg/artifacts`):

| keys | what |
|---|---|
| `metadata/orkflows/{namespace}/{name}/{version}.json` | orkflow definitions — written by `apply`, read at every boot |
| `metadata/namespaces/{name}.json` | namespace registry documents; `default` is synthesized when absent |
| `metadata/runs/{run-id}.jsonl` and `metadata/index/runs/{namespace}/…` | archived runs: the body, and the key-only index `ListRuns` scans |
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

One call fixes it, and no worker has to restart. It has to be told **where**,
though: the `temporal` CLI reads none of the `ORK_` variables above. Its own
defaults are the namespace `default` on `localhost:7233`, so a bare command
does not fail — it succeeds, against a namespace nobody deployed to, or on a
laptop rather than the cluster. Every invocation in this document therefore
passes `--namespace` and `--address`, and names the deployment through
`$ORK_DEPLOYMENT_NAME` rather than repeating a literal that §1 already owns.

```bash
temporal worker deployment set-current-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$ORK_BUILD_ID" \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
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

One value is refused: `ORK_ORKER_QUEUE=ork-engine-events` is the projection
worker's own queue, so every task activity would be dispatched to it and the
pool a document names would resolve to the engine's internals. Both binaries
report it and exit at boot rather than starting — `task.ResolvePoolBinding`
refuses the channel wherever it came from, and `ork` checks it again against
its own queues (`cmd/ork/main.go`, `queues.validate`;
`TestQueues_RefuseAnExecutionChannelThatIsTheEnginesOwnEventQueue`).

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
export ORK_DEPLOYMENT_NAME=orkstack          # §1's default, stated once
export ORK_TEMPORAL_NAMESPACE=ork-main
export ORK_TEMPORAL_ADDRESS=localhost:7233   # the frontend of the cluster you mean
NEW=2026.08.16-a1b2c3d      # v2
OLD=2026.08.09-9f8e7d6      # v1, currently serving
```

The last two are for **you**; the CLI reads neither (§1), which is why every
command below hands it both. `proof/M8.5/ha-deploy.sh` runs this whole
sequence exactly this way — that is the version of these commands that is
known to work.

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
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$FIRST" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"

   temporal worker deployment set-current-version \
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$FIRST" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"

   # CurrentVersionBuildID must not be empty
   temporal worker deployment list \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
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
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$NEW" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
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
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$NEW" --percentage 10 \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
   ```

   New runs split between the versions. Watch error rates. Raise the percentage
   as confidence grows.

4. **Make v2 current:**

   ```bash
   temporal worker deployment set-current-version \
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$NEW" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
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
     -namespace "$ORK_TEMPORAL_NAMESPACE" -address "$ORK_TEMPORAL_ADDRESS" \
     -queue ork-tasks:workflow -queue ork-tasks-gpu-pool:activity \
     -want "$ORK_DEPLOYMENT_NAME.$NEW"
   ```

   It is a Go program because the CLI does not surface this: `versioning_info`
   comes back only in `DescribeTaskQueue`'s default api mode, and it is per
   **physical** queue — the engine's queue carries the version on its workflow
   side, the Orker's on its activity side, and asking either about the other
   answers `__unversioned__`.

5. **Wait for v1 to drain — do not skip this:**

   ```bash
   temporal worker deployment describe-version \
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$OLD" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS" \
     | grep -i drainage
   # DrainageStatus: Draining  → v1 still has pinned runs executing
   # DrainageStatus: Drained   → safe to stop v1
   ```

   `Draining` means pinned runs still need v1's workers. Stopping them there
   leaves those runs stuck with no worker to advance them; they resume if you
   bring v1 back, but until then they make no progress. Drainage is refreshed
   on a delay of a few minutes, so poll.

   Measured, not asserted:
   `TestIntegration_APinnedRunStrandsWhenItsVersionsOrkerStopsBeforeDrained`
   (`internal/engine`) stops v1's Orker with v2 Current and watches a run
   pinned to v1 for 45 seconds — its task stays scheduled and unstarted, the
   run stays `RUNNING`, and it finishes on v1 within a second of v1's Orker
   coming back.

   The same test holds the other half, which is the dangerous one: **v2 does
   not pick those tasks up.** The exception in §2 routes an activity to the
   Current Version when the workflow's own version has no workers on that task
   queue, and if it ever fired here, skipping this step would silently run an
   in-flight run's remaining tasks on new plugin code — the thing this whole
   document exists to prevent. The test fails the day that changes.

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

- **Drain first (safest, and the only safe option when the code changed).**
  Stop submitting, let running runs finish, then do §3 step 0. Nothing is ever
  replayed across builds.
- **Switch live.** Safe only when the versioned release is the *same binary*
  as the unversioned one already serving — the usual case, since a build id is
  a label on a build you are already running. Same binary means the same
  command stream by construction, which is the only thing that makes replaying
  an unversioned run's partial history onto the new version a non-event.

  It is **not** made safe by the replay guards in §6, and this document used to
  claim it was. Those guards compare current code against a fixture recorded at
  a known commit; they say nothing about whatever build is running in your
  cluster right now, and a fixture is only "older" than the code once someone
  commits a change after recording it.

  Worse, a live switch across an actual code change is exactly what breaks. The
  M8 engine inserted `RUN_CREATED` as command 1 of every run
  (`internal/engine/workflow.go`; before M8 that event was published in-process
  by the API and never entered a history), so an unversioned run started by a
  pre-M8 engine cannot be upgraded onto an M8 version: its next workflow task
  fails with a determinism violation. Measured, not assumed —
  `TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream`
  (`internal/engine/replay_test.go`) replays the committed history against a
  body carrying one extra emit and the SDK rejects it with `TMPRL1100`.
  Upgrading a pre-M8 system therefore means draining first.

Either way, keep the old workers up until their runs are done. That rule is
not about versioning: an Orker killed mid-task loses that attempt until its
`timeout_seconds` expires, and the default retry policy is a single attempt
(`internal/engine/workflow.go`, `retryPolicyFor`), so the task fails.

---

## 4. Rolling back

```bash
temporal worker deployment set-current-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$OLD" \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
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
  --versioning-override-deployment-name "$ORK_DEPLOYMENT_NAME" \
  --versioning-override-build-id "$OLD" \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
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
  --deployment-name "$ORK_DEPLOYMENT_NAME" --unversioned \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
```

New runs then go to whatever unversioned pollers exist, so start those first;
`--allow-no-pollers` overrides the check if you have to do it in the other
order, and leaves new runs backlogged until an unversioned worker appears.
Runs already pinned to a version still need that version's workers.

---

## 5. Ops notes

- **Run ids** are `ork-{timestamp}-{sequence}` and single-use for all time.
  Nothing may parse one or derive meaning from its parts — the sequence
  carries a per-process nonce in its high half (`internal/engine/runid.go`),
  so it is comparable only within one replica and means nothing across two.
  Order runs by `created_at`. The one exception, and it is not a licence to
  order by id: two places compare ids to break a tie between runs with the
  *same* timestamp, so a page is stable rather than arbitrary — `mergeRuns`
  (`internal/api/service.go`, guarded by
  `TestListRuns_OrdersRunsByTimeNotByRunID`) and `RunQuery.finish`, which
  orders one page of the archive index (`pkg/artifacts/archive.go`). Those two
  are the whole list; `proof/M8.5/docs-claims.sh` fails if a third appears.
- **Any replica answers any run** (§0). Live runs are read from Temporal,
  finished ones from the archive in the bucket — the half that needs
  `ORK_ARTIFACT_BACKEND=s3` (§1), because on the default `local` backend each
  replica archives to its own disk and only that replica can answer for a run
  past Temporal retention. Nothing durable lives in a process, so a read does
  not need to reach the replica that started the run — or that replica to
  still exist.
- **Listing one namespace's runs is a question the cluster answers.** Each
  engine registers the `OrkflowNamespace` search attribute on its Temporal
  namespace at boot (`task.EnsureRunSearchAttributes`, beside the namespace
  itself) and stamps every run it starts with the namespace that run belongs
  to, so `run list -n payments` is a visibility predicate rather than a filter
  over a page of everyone's runs. That distinction is a tenancy one: filtered
  afterwards, how far the list could see was set by how busy the OTHER
  namespaces were, and a namespace with one live run behind a neighbour's
  hundred was told it had none.

  A cluster that refuses the registration — the operator API behind an
  authorizer, a visibility store without custom attributes — still boots, and
  says so once:

  ```
  WARN visibility does not index a run's orkflow namespace
       search_attribute=OrkflowNamespace
       consequence="listing a namespace's live runs scans the whole index"
  ```

  The reader then reads the index to the end instead of trusting a predicate
  the cluster cannot apply, which is slower and still the tenant's own answer;
  a list that cannot finish even that walk fails rather than returning a short
  one. Runs started *before* the attribute existed carry the namespace in
  their memo but not in the index, so they are listed by id and, once
  finished, from the archive — whose key already begins with the namespace.
- **A double submit is settled by the cluster, not by a replica.** One
  `--idempotency-key` sent to two replicas at once returns one run id, because
  the claim is a Temporal workflow (`engine.IdempotencyClaimWorkflow`), not
  process memory. Retries are safe from any address behind the load balancer.
  A key is scoped to the ork namespace it was submitted into, so two tenants
  can both submit `nightly` and get their own run
  (`api.submission.claimKey`); unscoped, the second would be answered with the
  first tenant's run id and its work would never start.
- **`ORK_ORKER_QUEUE` must be set on both binaries.** Set it on only one and
  runs strand in `RUNNING`: the Orker polls one queue while the engine
  dispatches to another. It moves **task execution only** — the engine's own
  workers stay on `ork-tasks` and `ork-engine-events` (§2). A task's own
  `pool:` in the DSL is how a step reaches a dedicated Orker pool
  (`docs/DSL.md`), and it cannot be used to escape this setting: `pool: main`
  is the name of whatever queue you bound here, so it follows the override
  rather than outranking it, and no document can route itself back onto
  `ork-tasks` by naming the default pool
  (`TestPoolResolver_NamingTheDefaultPoolCannotEscapeTheOperatorsRoute`).
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

`internal/engine/replay_test.go` and `internal/engine/replay_payload_test.go`
guard it, together. Both read the same **committed** history
(`internal/engine/testdata/history/run-succeeded-v1.json`), recorded from a real
multi-task run by a versioned worker, and both run in `make test` with no
cluster.

**What the fixture is, exactly.** It was recorded by the commit that added it,
so on that day it was current code replaying its own command stream. It is a
frozen stream that AGES into a cross-version guard: every commit after that one
is measured against a build genuinely older than itself. It is not a recording
of any build you are running, and it cannot vouch for one (§3.5).

They catch different halves, and neither is enough alone:

| guard | what it compares | what it catches |
|---|---|---|
| `TestRunWorkflow_ReplaysCommittedV1History` | Temporal's own replayer: command types and their order | a reordered, added or deleted activity; a changed scheduling order |
| `TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream` | the same replayer, run against a body with one extra emit — it must FAIL | the guard above going blind: it is the proof the fixture can reject an inserted command, on the day it is recorded rather than one change later |
| `TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends` | the payloads, by re-executing the fixture's own input against the recorded ones | a changed `task.ExecuteTaskInput`, a renamed event type, a reworded message, a changed `Run` projection |

The replayer never looks inside a payload: adding a field to
`task.ExecuteTaskInput` replays perfectly clean, and an Orker of the recorded
build would silently drop it. That is why the third guard exists.

If any of them fails, current code no longer produces what the recorded build
did. Either restore it (`workflow.GetVersion` for a deliberate command-stream
change), or — if the contract genuinely changed and no old run can still be in
flight — re-record the fixture and say so in the commit:

```bash
ORK_RECORD_GOLDEN_HISTORY=1 go test -tags=integration ./internal/engine/ \
  -run RecordGoldenRunHistory
```

Re-recording resets the guard to today. Do it deliberately, never to make a
red test green.
