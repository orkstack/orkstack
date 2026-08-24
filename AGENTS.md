# AGENTS.md — OrkStack engineering contract

This file is authoritative for every human and AI agent working in this repo.
Read it fully before changing code. It encodes the invariants that let many
agents build independently without breaking each other.

## 0. What OrkStack is

An orkflow engine: users author **orkflows** in a YAML **DSL**; the **engine**
(`ork`) compiles them and runs them on **Temporal**; **Orkers** are generic
Temporal workers that execute **plugins** (containerizable task implementations);
a **CLI** (`orkctl`) and a **SvelteKit UI** drive and observe runs in realtime; an
**artifact store** (local FS or S3/MinIO) carries files between tasks.

```
DSL (yaml) ──apply──> ork ──compile──> Temporal ──dispatch──> Orker ──run──> Plugin
                       │  ▲                                                    │
                  Connect API                                            artifacts
                   + WS events  <──────────── events ─────────────────────────┘
                       ▲
             CLI (orkctl) / UI (SvelteKit)
```

## 1. Non-negotiable invariants (the contracts)

1. **Proto is the single source of truth.** All cross-process types live in
   `proto/orkstack/v1/*.proto`. Never hand-edit files under `gen/`. Change proto,
   run `make proto`, commit both.
2. **The task contract is fixed:** a plugin receives `(input Payload, inputArtifacts []ArtifactRef)`
   and returns `(output Payload, outputArtifacts []ArtifactRef, error)`. See
   `pkg/plugin`. Payloads are JSON-compatible structs; bytes travel only as
   artifacts, never inline in a Payload.
3. **Determinism boundary:** Temporal *workflow* code (in `internal/engine`) must
   be deterministic — no clocks, no network, no randomness, no direct IO. All
   side effects happen in *activities* or in Orker plugins. Violating this
   corrupts replay.
4. **Artifacts are references, not bytes.** Tasks exchange `ArtifactRef`s; the
   Orker materializes/uploads through `pkg/artifacts`. Never pipe large bytes
   through Temporal payloads or the event stream.
5. **Everything is versioned.** An `Orkflow` has a `name` + `version`. Runs pin
   the version they executed, and are identified as `ork-{timestamp}-{sequence}`.
   That shape is the whole of the contract, and the parts are not:
   - `sequence` is `nonce<<32 | counter` — 32 bits of `crypto/rand` drawn once
     per process, over a per-process counter (`internal/engine/runid.go`). It is
     monotonic *within* one replica and meaningless across two, which is what
     stops two replicas issuing the same id in one clock tick.
   - a run id is the Temporal workflow id and is **single-use for all time**:
     `StartRun` sets `REJECT_DUPLICATE` + `CONFLICT_POLICY_FAIL` +
     `WorkflowExecutionErrorWhenAlreadyStarted` (`internal/engine/temporal.go`),
     so a reused id is an error, never a silent attach to someone else's run.
   - **nothing may parse a run id or derive meaning from its parts.** Order
     runs by time (`created_at`). Exactly two places compare ids, both only to
     break a tie between two equal timestamps, and both on the `ListRuns` read
     path: `mergeRuns` (`internal/api/service.go`, guarded by
     `TestListRuns_OrdersRunsByTimeNotByRunID`) and `RunQuery.finish`, which
     orders one page of the archive index (`pkg/artifacts/archive.go`). Those
     two are the whole list, and `proof/M8.5/docs-claims.sh` holds the tree to
     it. A tie-break is not a licence to order by id.
6. **DONE means proven.** A change is done only when behavior tests pass and a
   proof artifact exists under `proof/` (see §6). No proof, not done.
7. **Temporal is the read model for live runs.** `GetRun`, `ListRuns`,
   `StreamEvents` and `/ws/runs/{id}` are answered from the run's Temporal
   history through `engine.TemporalRunReader` (`internal/engine/reader.go`),
   handed to the API as `Binding.Reader` by the namespace router
   (`internal/engine/router.go`) — the `RunReader` seam is declared there
   because a `Binding` cannot name the API's type without a cycle, and it is
   consumed in `internal/api/service.go`; finished
   runs past retention come from the run archive, which is the bucket in any
   replicated deployment — `ORK_ARTIFACT_BACKEND` defaults to a directory per
   process, so `s3` stops being optional at the second replica
   (`docs/DEPLOY.md` §0). The in-process `RunStore`/`EventBus` are
   **write-side** helpers — the candidate run a submission creates, the archive
   and webhook write path, and the one stream that never reaches Temporal (a
   run whose start failed). Never answer a read from process memory behind a
   wired reader: any replica must answer for any run, including one it never
   started and one whose replica is gone.
8. **Event sequence is producer-assigned.** `RunWorkflow` numbers every event
   it emits (`internal/engine/workflow.go`); the bus honours that number and
   assigns one only when given none (`internal/engine/bus.go`). That is what
   makes the three streams — Temporal history, the in-process log, the archived
   JSON — identical, and what lets a client resume with `from_sequence` against
   a different replica.
9. **One word, one thing: the entity is an Orkflow.** What a user authors, the
   engine stores and a run executes is an **orkflow** — `orkflow_name`,
   `orkflow_version`, `OrkflowService`, `--orkflow`, `orkctl orkflow list`.
   The word *workflow* in this repo means a **Temporal** workflow and nothing
   else: `workflow.Context`, `StartWorkflowOptions`, `WorkflowReplayer`, the
   registered type `RunWorkflowType = "RunWorkflow"`, `ClaimWorkflowType`, a
   workflow id, a workflow task. So an identifier pairing *workflow* with
   *name* or *version* is always wrong: the thing it names is either an
   orkflow, or a Temporal workflow TYPE, and it must say which. The rename
   landed at M9 with no shim and no dual-read, because nothing was deployed;
   `proof/M9/naming-sweep.sh` is what holds the tree to it.

   **The same rule for where a task runs: the word is `pool`.** A **pool** is
   a set of Orkers, and it is what a user names — `pool:` in the DSL, the
   `Pool` message, `TaskRun.pool`, `ListPools`, `orkctl namespace create
   --default-pool/--allow-pool`, `ORK_ORKER_POOL`, the pool cards on the
   dashboard. A **queue** is a Temporal task queue and
   means nothing else in this repo; it appears on **no user-facing surface**:
   not in `proto/`, not in the DSL, not in an `orkctl` flag or its help text,
   not in `ui/src` outside `gen/`, not in `docs/DSL.md`, not in a shipped
   example. `queue:` was removed from the DSL at M10.1 with no alias — a
   document that still writes it fails to compile with a message naming
   `pool:` (`internal/dsl/removed.go`). `proof/M10.1/naming-sweep.sh` is what
   holds the tree to it, and it bans the substring, so `task_queue`,
   `queueName` and `ORK_TEMPORAL_NAMESPACE` cannot walk through the door a
   word boundary would leave open.

   **The translation lives in `internal/task/pool.go` and nowhere else.**
   `QueueForPool` / `RouteForPool` / `PoolForQueue` are the whole mapping, and
   the prefix `ork-pool-` is constructed in exactly one place —
   `poolQueuePrefix`, `internal/task/pool.go`. Elsewhere the string appears
   only inside prose.
   `""` and `"main"` both resolve to the deployment's *configured* default
   channel, so `pool: main` and no pool are one route by construction under
   every `ORK_ORKER_QUEUE` override — which is why a document cannot escape an
   agent's isolation by naming the default pool (§6). Any other valid pool is
   `ork-pool-<slug>`; `ork-engine-events` as the bound default is refused
   (`ErrReservedQueue`). Write a second mapping anywhere and the two spellings
   of one pool stop being one pool.
10. **A namespace is the tenancy coordinate, and `default` is a name like any
    other.** An orkflow name is unique WITHIN a namespace, and every read and
    write that carries a run or an orkflow carries the namespace it belongs to.
    Three invariants hold it up:
    - **One slug rule, stated where each boundary needs it, over shared test
      vectors.** `^[a-z][a-z0-9-]*$`, ≤200 bytes, for a namespace, an orkflow
      name, a task id and a pool: `internal/dsl/compile.go` (`slugPattern`,
      `MaxSlug`), `pkg/artifacts/namespace_store.go` (`namespaceNamePattern`,
      `maxPoolName`) and `internal/task/pool.go` (`poolPattern`,
      `MaxPoolName`) — restated at each boundary because a hand-built request
      never went through the compiler, and held to ONE rule by a shared corpus
      (`TestNamespaceName_RegistryAndCompilerAgree`,
      `pkg/artifacts/namespace_name_test.go`, the only place that package names
      `internal/dsl`). Names are **validated, never sanitized** — a name that
      has to be repaired to be stored is a name that can be spelled two ways,
      and a key built from it then addresses somebody else's tenant.
    - **`default` always resolves, and is never stored or deleted.** A request
      naming no namespace means `default` on the wire (the empty string), and
      reading it answers a synthesized document
      (`artifacts.DefaultNamespaceDocument`) whether or not anything was ever
      written. Creating it writes nothing — an identical spec is the no-op it
      looks like, a different one is the conflict any collision is — and
      deleting it is refused unconditionally: it holds every orkflow that names
      none (`internal/api/namespaces.go`).
    - **The archive index is namespace-first.**
      `metadata/index/runs/{namespace}/{orkflow}/{status}/{stamp}/{run-id}` —
      eight segments, `runIndexSegments = 8`, `pkg/artifacts/archive.go`. The
      namespace is the first thing after the prefix so that listing one
      tenant's runs narrows in the LIST call, not in a filter afterwards: a
      post-filter would make one tenant's page scan every tenant's keys, and a
      run's namespace would be recoverable only by reading its body.

## 2. Repository layout

```
proto/orkstack/v1/     Proto contract (edit here)
gen/go/                Generated Go (do not edit)
cmd/ork/               Engine binary entrypoint (bin/ork)
cmd/orker/             Orker binary entrypoint
cmd/orkctl/            CLI binary entrypoint
internal/engine/       Temporal workflows + activities + run store
internal/api/          Connect service implementations + WS bridge
internal/dsl/          YAML DSL parse → validate → compile to Orkflow proto
internal/orker/        Orker runtime: worker wiring, plugin dispatch
internal/task/         Temporal plumbing: namespace, queue names, deployment identity
pkg/plugin/            Public plugin SDK + task contract (imported by plugins)
pkg/plugins/{http,shell,file}/  Builtin plugins
pkg/artifacts/         Artifact storage providers (local FS, S3/MinIO)
ui/                    SvelteKit + Vite + Tailwind app
deploy/                docker-compose (Temporal + MinIO)
examples/              Example orkflows (*.ork.yaml)
docs/                  Design + reference docs
proof/                 Committed proof artifacts (logs, traces, recordings)
scripts/               Dev helper scripts
```

## 3. Toolchain & conventions

- **Go 1.25**, single module `github.com/orkstack/orkstack`. Format with `gofmt`;
  `make lint` (go vet + gofmt) must pass. Prefer `context.Context` first arg,
  wrapped errors (`fmt.Errorf("...: %w", err)`), no panics in library code.
- **Temporal Go SDK** (`go.temporal.io/sdk`). Namespace: `ORK_TEMPORAL_NAMESPACE`,
  default `ork-main` (auto-provisioned at binary boot). Task activities resolve
  a user-facing pool to an internal Temporal task queue; the engine's raw
  default is `ORK_ORKER_QUEUE`, default `ork-tasks`, set on both binaries. A
  task's `pool:` is resolved inside the engine and never exposes that queue.
  The engine's own workflows always live on the fixed `ork-tasks` and
  `ork-engine-events`, whatever `ORK_ORKER_QUEUE` says (`cmd/ork/main.go`,
  `queuesFor`). **For parallel agent runs, give each agent its own namespace
  (see §6 below).**
- **Connect** (`connectrpc.com/connect`) for RPC. Server serves h2c on
  `:8080` (configurable). WebSocket event bridge on the same server at
  `/ws/runs/{id}`.
- **UI**: Bun + SvelteKit (Svelte 5 runes) + Vite + Tailwind. Generated TS
  clients live in `ui/src/lib/gen`.
- **CLI**: Cobra. Human output by default, `--json` for machine output.
- **Config**: environment variables prefixed `ORK_` (e.g. `ORK_TEMPORAL_ADDRESS`,
  `ORK_ARTIFACT_BACKEND`, `ORK_S3_ENDPOINT`). Provide sane localhost defaults.

## 4. The development workflow (every change follows this)

1. If the change crosses a process boundary, update `proto/` first and run
   `make proto`.
2. Write a **behavior test** that fails for the right reason (test observable
   behavior, not implementation detail).
3. Implement until the test passes.
4. `make lint test` must be green. For anything touching Temporal or storage,
   run the integration test against `make infra-up`.
5. Produce/refresh the proof artifact (§6).
6. Commit with a Conventional Commit message (§7). Update the relevant milestone
   todo.

### 4A. Agent/model routing and review discipline

Agent capability is selected per task, not assigned permanently to a role. Score
architectural novelty, cross-module coupling, blast radius, concurrency or
distributed-state complexity, requirement ambiguity, deterministic-verification
difficulty, security or data-integrity impact, and migration or compatibility
risk from 0 to 2 each. Use `docs/MODEL_SELECTION.md` for the risk bands and
catalog-checked model routes.

- Use the least costly model that can meet the engineering bar. Sonnet is the
  default implementer once the design, contracts and acceptance criteria are
  explicit. Use Opus for difficult implementation, non-local debugging, subtle
  state or concurrency, security-sensitive work, or evidence that Sonnet is
  insufficient. Reserve Fable for unresolved high-impact architecture,
  irreversible choices, severe ambiguity, or arbitration.
- Run deterministic checks before expensive review: compile, type, unit,
  integration, schema, lint, security, migration, fuzz, property and runtime
  checks that apply. A model's claim that a check passed is never evidence.
- Reviewers get fresh context and inspect the specification, diff, contracts,
  tests and runtime evidence rather than the implementer's reasoning history.
  Start with Luna; use Terra for non-trivial semantic or invariant review; use
  Sol only for critical changes, disagreement or unresolved severe findings.
- OpenCode Go agents are adversaries by default. They try to falsify the change
  and report evidence-backed findings and executable probes; they do not edit
  implementation code unless explicitly assigned repair ownership.
- Classify and consolidate findings before repair: confirmed defect, probable
  defect, ambiguity, architectural concern, test deficiency, style preference,
  duplicate or false positive. Do not create churn from style-only findings.
- Repairs normally return to the original implementer with the smallest useful
  packet: the confirmed finding, failing evidence, acceptance item, affected
  region and violated invariant. If the same model fails to fix a materially
  identical issue twice, escalate Sonnet to Opus and unresolved Opus reasoning
  to Fable or an arbiter. All loops have a fixed iteration limit.
- Preserve provider diversity where it lowers correlated error, but do not
  force every fallback chain to use every provider. A fallback handles
  availability failure; it does not replace evidence-based escalation.
- Treat subscriptions as finite budgets. Below 60% use normal routing; at
  60–80% avoid Fable; at 80–90% reserve Fable, Opus and Sol for blocking or
  high-risk work; above 90% use frontier capacity only for project-blocking or
  high-risk work. Prefer cheap adversaries for routine analysis.
- Record useful routing metrics when available: first-pass acceptance, repair
  rounds, defects and false positives by reviewer tier, escalations, tokens,
  cost, latency, escaped defects, post-acceptance test failures, frontier use
  and reviewer disagreement. Adjust task-class routing from evidence.

The workflow graph must keep architecture, implementation, deterministic gates,
independent review, adversarial analysis, repair and final evidence as separate
steps. Acceptance requires executable evidence and independent review; model
consensus is supporting evidence only.

## 5. Testing standards (behavior first)

- **Unit**: pure logic (DSL compile, template rendering, plugin logic) with table
  tests. Fast, no network.
- **Workflow tests**: use Temporal's `testsuite.WorkflowTestSuite` /
  `TestWorkflowEnvironment` to test engine workflows with mocked activities.
- **Integration**: gated behind `-tags=integration` and a running
  `make infra-up`; exercise real Temporal + MinIO. Skip cleanly when infra is
  absent — through the package's `infraAbsent` helper, never a bare `t.Skip`,
  so that a proof run can set `ORK_REQUIRE_INFRA=1` and turn the same absence
  into a failure (§6).
- **E2E**: full stack — CLI submits, Orker executes, artifacts land in MinIO, UI
  reflects it. Lives in `test/e2e` and drives real binaries.
- Name tests for behavior, after one from the tree:
  `TestShellPlugin_CapturesStdoutAndStderrAsArtifacts`,
  `TestIntegration_SecondReplicaServesARunItNeverStarted`.
- No sleeps for synchronization; poll with deadlines or use test env helpers.

## 6. Proof artifacts & integration test isolation (definition of DONE)

Every milestone writes evidence under `proof/<milestone>/`:
- `test.log` — full `go test -v` (or Playwright) output.
- `coverage.out` / coverage summary where meaningful.
- A behavior trace: e.g. a run's event stream JSON, an artifact roundtrip
  transcript, a CLI session recording, or UI screenshots/video.
- A short `README.md` stating what was proven and how to reproduce.

### A skipped prerequisite is not a pass

`go test` exits 0 for a package whose every test called `t.Skip`, and a proof
script that reads only an exit code cannot tell *proved* from *declined to
run*. M8.5 was caught doing exactly that: on a machine with no cluster its
`ha-deploy.sh` skipped, exited 0, and `reproduce.sh` printed
`M8.5 PROOF: PASS` over artifacts left by an earlier execution. Three rules
close it, and every script under `proof/` obeys them:

- **Exit 0 = PASS, 1 = FAIL, 77 = SKIPPED/INCOMPLETE.** 77 is automake's skip
  code: a prerequisite was absent, so nothing was measured. A caller
  propagates it and never records it as green.
- **Probe, then require.** A script that finds the cluster up exports
  `ORK_REQUIRE_INFRA=1`, which makes every "infrastructure is absent" skip in
  the module a failure instead. A skip is then not a fact about the
  environment; it is a test opting out of the proof.
- **An artifact counts only if this run wrote it.** Existence proves nothing —
  yesterday's file exists too. `proof/M8.5/reproduce.sh` compares every
  artifact against a marker stamped at startup and reports the rest as STALE.

A skipped run must also leave the last real transcript alone: `ha-deploy.sh`
decides its prerequisites *before* it opens the file it writes.

### Integration tests share one Temporal namespace — isolate parallel runs by namespace

Integration tests and real binaries default to the **same Temporal namespace**
(`ork-main`) and queue (`ork-tasks`). When **multiple agents run in parallel**
(e.g. concurrent milestone worktrees), they **steal activities from each other's
runs** — the failure looks like a code bug but is not: you get missing lifecycle
events, `observed 1 task inputs, want 3`, or a run that hangs until the stream
deadline. **Both M7.7 and M7.8 experienced this collision mid-milestone.**

**Prevention: give each concurrent agent its own Temporal namespace.**

```bash
# Unique per milestone worktree. Both binaries provision it at boot.
export ORK_TEMPORAL_NAMESPACE="ork-m7-7"
export ORK_TEMPORAL_NAMESPACE="ork-m7-8"
```

`ork` and `orker` both read `ORK_TEMPORAL_NAMESPACE` and call
`task.EnsureNamespace` at startup (`cmd/ork/main.go`, `cmd/orker/main.go`), so
an unused name is registered on first boot. A namespace is a hard partition:
no workflow, activity or queue crosses it, so no cross-agent theft is possible.

**The deliberate exception: a two-replica test needs ONE namespace for BOTH
replicas.** Isolation is per agent, not per process. A test that proves any
replica answers for any run, or that one idempotency key survives two
replicas, must run its engines in the *same* namespace on the *same* queues —
that sharing is the condition under test, and splitting it would make the test
fail for a reason unrelated to the property
(`cmd/orkctl/tworeplica_read_integration_test.go`,
`cmd/orkctl/idempotency_two_engine_integration_test.go`). Both keep the rule
above by suffixing a per-run namespace that the pair shares and no other agent
is inside (`isolatedNamespace`). Say so in a comment when you do it, or the
next reader will "fix" it.

**`ORK_ORKER_QUEUE` isolates task execution.** Set it on both binaries and the
Orker polls that queue while the engine dispatches every task that names no
`pool:` of its own to it (`TemporalStarter.StartRun` binds the name into the
run's workflow input; workflow code is deterministic and cannot read the
environment itself). A task that writes `pool: main` goes there too, whatever
shape the override has: `main` names this deployment's default pool, not the
queue `ork-tasks`, so the override moves it, and one pool name means one set
of Orkers. A document cannot escape the isolation onto the shared queue by
naming the default pool, nor by naming the pool it was already reported on
(`runState.routeForTask`). Set it on only one of the two and the run strands
in RUNNING: the Orker listens to one queue while the engine dispatches to
another.

```bash
# Both processes, same value.
export ORK_ORKER_QUEUE="ork-tasks-m7-9"
```

A queue partitions task execution only — run workflows and engine events still
share their queues, so a namespace is the stronger boundary and the one to use
for parallel agents. A task's own `pool:` in the DSL resolves to its dedicated
Orker pool, which is how one step reaches a separate worker group
(`docs/DSL.md`). Proof for both: `proof/queue-routing/`, which
`proof/M10.1/proof-claims.sh` holds to this tree — a proof this file points a
reader at is a claim about the tree, and M10.1 renamed five symbols that proof
argued from before anything noticed.

**In test harnesses** that start their own engine and orker, give the pair a
unique namespace, and a private queue when the test needs one
(`cmd/orkctl/integration_test.go` does both — it also assigns a pool to each
task through `writePoolPinnedOrkflow`, which is still the way to prove
per-task routing):

```go
// Before starting the engine and orker:
os.Setenv("ORK_TEMPORAL_NAMESPACE", "ork-test-"+uniqueSuffix)
```

### A namespace holds 100 Worker Deployments, and versioning tests spend them

`TestIntegration_PinnedRunFinishesOnItsOwnVersionWhileNewRunsMoveOn` and
`TestIntegration_AVersionedReleaseIsIdleUntilItsVersionIsCurrent` register a
**new deployment per execution** in one fixed namespace
(`internal/engine/versioning_rollout_integration_test.go`, `ork-m8-4-worker-versioning`
unless `ORK_TEMPORAL_NAMESPACE` says otherwise), and Temporal caps a namespace
at 100. Run the suite fifty times and the fifty-first fails like this:

```
WARN  Failed to poll for task … Error reached maximum deployments in namespace (100)
--- FAIL: … never reported pollers on … (last error: Worker Deployment Version not found)
```

Another failure that looks like a code bug and is not. A deployment cannot be
deleted while it holds versions, and its current version cannot be deleted at
all, so the cheap reset is the namespace — the tests provision it again at the
next boot (`task.EnsureNamespace`):

```bash
temporal operator namespace delete --namespace ork-m8-4-worker-versioning \
  --address 127.0.0.1:7233 --yes
```

**Process cleanup remains mandatory** even with isolation. Before any new
run, kill all stray processes:

```bash
pkill -f '/bin/orker$'; pkill -f '/bin/ork$'; sleep 2
ps aux | grep -E 'bin/ork|bin/orker' | grep -v grep   # must be empty
```

The engine binary is `bin/ork` (§2). Anchor both patterns with `/` and `$`:
unanchored, `bin/ork` also matches `bin/orkctl` and kills the CLI you are
running the proof with.

Never leave a backgrounded engine/orker running after a manual e2e; always kill
what you start. Run integration suites with `-p 1` to stop packages from polling
concurrently.

Proof must be reproducible via a single documented command.

## 7. Commits & branches

- Conventional Commits: `feat(engine): ...`, `fix(orker): ...`, `test(dsl): ...`,
  `docs: ...`, `chore(proto): ...`.
- Keep commits scoped to one concern; keep `main` buildable (`make build test`).
- Tag releases `vMAJOR.MINOR.PATCH`. First target: `v0.0.1`.

### Gate every merge on the merged tree, not on the branch

A green branch proves nothing about the merge. Two milestones can each pass
every test and still produce a broken `main`, because the conflict is created
by the merge itself and no branch CI ever compiles that combination.

**This already happened twice, in one merge:**

| what broke | how | found by |
|---|---|---|
| `internal/api/service.go` | M7.8's merge took the branch side whole, reverting M7.5's durable orkflow store. `Config.Orkflows` went back to the in-memory type while `cmd/ork` still wired `artifacts.S3OrkflowStore` | `make build` — but only after both milestones were declared done |
| `requireMinIO` in `internal/engine` | M7.5 and M7.6 each added the helper to their own `_integration_test.go`; merged, the package has it twice | nothing — hidden behind `//go:build integration` until the M7.5 proof was rerun |

**Before finalizing any merge** (after resolving conflicts, before the merge
commit is final — `git merge --no-commit`, or amend/reset if it is already
written):

```bash
make merge-check
```

which is `make build` + `make lint` + `make test` + `go vet -tags=integration
./...`. The last one is not optional: build-tagged files are invisible to
`make build` and `make test`, so a duplicate helper or a stale signature there
survives every default check and only surfaces when someone reruns a proof.

**When a merge conflicts in a file both sides changed**, resolve it as a union
of the two behaviors, and verify against the base:

```bash
git merge-file -p ours.go base.go theirs.go     # base = git merge-base A B
```

Never resolve by taking one side whole unless that file is untouched on the
other. Afterwards, list what the resolution dropped:

```bash
# for each file differing between the parents, compare the result to both
git diff --name-only $MERGE^1 $MERGE^2
```

**Rerun the affected milestones' `proof/*/reproduce.sh` after a merge that
touched their code.** A proof taken on a branch describes that branch. Once
merged, the proof is a claim about `main` that nothing has checked.

## 8. Boundaries between agents (avoid collisions)

- Engine agents own `internal/engine`, `internal/api`, `cmd/ork`.
- Orker/plugin agents own `internal/orker`, `pkg/plugin`, `pkg/plugins/*`,
  `cmd/orker`.
- CLI agents own `cmd/orkctl`.
- UI agents own `ui/`.
- Artifact agents own `pkg/artifacts`.
- **Shared/coordinated** (change deliberately, announce in commit): `proto/`,
  `go.mod`, `deploy/`, `Makefile`, this file. If you must change proto, do it in
  a dedicated commit and regenerate.

## 9. Security & safety

- The `shell` plugin executes commands — it runs only inside an Orker's sandbox
  boundary and never interpolates untrusted input without the templating layer's
  escaping. Document any command construction.
- Secrets come from env/secret refs, never committed. `.env` is gitignored.

## 10. Deployment & versioning

How to operate this lives in `docs/DEPLOY.md`. These are the invariants a code
change must not break.

- **One Worker Deployment spans both binaries.** `ork` and `orker` of one
  release share `ORK_DEPLOYMENT_NAME` + `ORK_BUILD_ID`
  (`internal/task/versioning.go`) and are released in lockstep. Temporal routes
  a pinned workflow's activities to its own version only while that version has
  workers on the activity queue; without them they fall through to that queue's
  current version — new plugin code, mid-run.
- **The three registered behaviours** (`cmd/ork/main.go`): `RunWorkflow` is
  **Pinned**, `IdempotencyClaimWorkflow` is **AutoUpgrade**, the projection
  worker on `ork-engine-events` is **unversioned**. Each is load-bearing.
  Pinning the claim workflow strands a 24h key holder on a version that then
  never drains; versioning the projection worker takes the read model away from
  replicas of every other version; AutoUpgrading `RunWorkflow` hands a
  half-finished history to a new build.
- **Versioning is off unless `ORK_BUILD_ID` is set** (`Deployment.Versioned`),
  which is what keeps `make run-ork` + `make run-orker` working with no
  configuration. Keep the default off, and keep `DefaultVersioningBehavior`
  unset while `UseVersioning` is false — the SDK panics on that combination.
- **A version must drain before it is decommissioned.** Runs pinned to it still
  need its workers; `temporal worker deployment describe-version` reports
  `DrainageStatus`.
- **Old histories must replay against new code.** `internal/engine/replay_test.go`
  replays a committed history in `make test`. Re-record the fixture
  deliberately, never to turn a red test green.
- Proof: `proof/M8.4/` (versioning, rollout, runbook) and `proof/M8.5/` (two
  engine replicas and two Orker builds, live, in one command).
