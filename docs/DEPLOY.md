# Deploying OrkStack

How to roll a new OrkStack release out without breaking runs that are already
executing.

The short version: `ork` and `orker` of one release share a **Worker Deployment
Version**. A run is **pinned** to the version that started it and finishes
there, whatever you deploy next. That guarantee costs you one rule — do not
stop the old workers until their version reports `Drained`.

---

## 1. Configuration

Every setting is an environment variable prefixed `ORK_`. These are the ones a
deployment cares about.

| variable | default | set on | effect |
|---|---|---|---|
| `ORK_TEMPORAL_ADDRESS` | `127.0.0.1:7233` | both | Temporal frontend |
| `ORK_TEMPORAL_NAMESPACE` | `ork-main` | both | hard partition; nothing crosses it. Provisioned at boot |
| `ORK_ORKER_QUEUE` | `ork-tasks` | **both** | queue Orkers poll and the engine dispatches to |
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
on**:

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
| run worker (`ork`) | `ORK_ORKER_QUEUE` | yes | `RunWorkflow` = **Pinned**, `IdempotencyClaimWorkflow` = **AutoUpgrade** |
| Orker (`orker`) | `ORK_ORKER_QUEUE` | yes | activities only; must match the engine |
| projection worker (`ork`) | `ork-engine-events` | **no** | `PublishEvent` |

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

1. **Start v2 alongside v1.** Same namespace, same queue, new `ORK_BUILD_ID`.
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

   The task-queue list must contain `ORK_ORKER_QUEUE` for both a workflow and
   an activity poller. If the Orker's queue is missing, v2's engine is running
   without v2 Orkers — stop here.

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

---

## 5. Ops notes

- **Run ids** are `ork-{timestamp}-{sequence}` and single-use for all time.
  Nothing may parse or order by one.
- **Any replica answers any run.** Live runs are read from Temporal, finished
  ones from the archive in S3; nothing durable lives in a process, so a read
  does not need to reach the replica that started the run.
- **`ORK_ORKER_QUEUE` must be set on both binaries.** Set it on only one and
  runs strand in `RUNNING`: the Orker polls one queue while the engine
  dispatches to another. A task's own `queue:` in the DSL outranks the engine
  default and is how a step reaches a dedicated Orker pool (`docs/DSL.md`).
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
current code meeting a history an older build wrote, which happens on a
rollback, on a versioning override, and after any worker restart.

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
