# Deploying OrkStack

How to roll a new OrkStack release out without breaking runs that are already
executing.

The short version: `ork` and `orker` of one release share a **Worker Deployment
Version**. A run is **pinned** to the version that started it and finishes
there, whatever you deploy next. That guarantee costs you two rules — make each
version **Current** when you want it to serve (the first one especially: until
then it serves nothing), and do not stop the old workers until their version
reports `Drained`.

Everything here is executed against the built binaries by
`proof/M8.4/deploy-runbook.sh`, whose transcript is the evidence for the
claims below.

---

## 1. Configuration

Every setting is an environment variable prefixed `ORK_`. These are the ones a
deployment cares about.

| variable | default | set on | effect |
|---|---|---|---|
| `ORK_TEMPORAL_ADDRESS` | `127.0.0.1:7233` | both | Temporal frontend |
| `ORK_TEMPORAL_NAMESPACE` | `ork-main` | both | hard partition; nothing crosses it. Provisioned at boot |
| `ORK_ORKER_QUEUE` | `ork-tasks` | **both** | queue Orkers poll and the engine dispatches **task activities** to. It does not move the engine's own queues (§2) |
| `ORK_DEPLOYMENT_NAME` | `orkstack` | **both** | the Worker Deployment both binaries join |
| `ORK_BUILD_ID` | *(empty)* | **both** | this release's identity. **Empty means versioning is off** |
| `ORK_HTTP_ADDR` | `:8080` | `ork` | Connect API + WebSocket bridge |
| `ORK_IDEMPOTENCY_TTL` | `24h` | `ork` | how long an idempotency key stays bound |
| `ORK_ARTIFACT_BACKEND`, `ORK_S3_*` | see `docs/STORAGE.md` | both | artifact + archive storage |

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
> — `api@v1.63.5/workflowservice/v1/request_response.pb.go`,
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
Deployment Version — with one exception, quoted from
`api/enums/v1/workflow.pb.go`:

> Activities of `PINNED` workflows are sent to the same Deployment Version.
> Exception … when the activity Task Queue workers are not present in the
> workflow's Deployment Version, in which case the activity will be sent to the
> **Current Deployment Version of its own task queue**.

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

6. **Stop v1** once it reports `Drained`.

A rollout done this way is proven end to end on the real binaries in
`proof/M8.4/deploy-runbook-transcript.txt` (phase 3): the run in flight when v2
became Current finished on v1 and **both** of its tasks — including the one
scheduled after the switch — were executed by v1's Orker, while the run
submitted after the switch ran wholly on v2.

---

## 3.5 Turning versioning on for a system that is already running

Steps 1–6 move runs between two versions. Turning versioning on for the first
time moves them from *no* version, and that is not symmetric: a run that
started unversioned has **nothing pinning it**.

> Workflow execution does not have a Versioning Behavior and is called
> Unversioned. This is the legacy behavior.
>
> — `api@v1.63.5/enums/v1/workflow.pb.go`, `VERSIONING_BEHAVIOR_UNSPECIFIED`

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

This only works while v1's workers are still running, which is the real rule
behind the drain wait: **never decommission the previous version until the next
one is proven.**

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
  Nothing may parse or order by one.
- **Any replica answers any run.** Live runs are read from Temporal, finished
  ones from the archive in S3; nothing durable lives in a process, so a read
  does not need to reach the replica that started the run.
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
- **Process hygiene.** Before any manual end-to-end, kill stray workers:

  ```bash
  pkill -f 'bin/orker'; pkill -f 'bin/ork'; sleep 2
  ps aux | grep -E 'bin/ork|bin/orker' | grep -v grep   # must be empty
  ```

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
