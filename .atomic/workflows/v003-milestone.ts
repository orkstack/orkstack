// .atomic/workflows/v003-milestone.ts
//
// OrkStack v0.0.3: namespaces, orkflow editing and versioning, live task logs,
// and a dashboard.
//
// The problem this exists for: an OrkStack install today is one flat catalog.
// Every orkflow shares a name space, every task lands on one Orker pool, an
// orkflow can only be replaced (never edited or rolled back), a task's output
// is invisible until it finishes and lands as an artifact, and there is no
// aggregate view of anything. Four features, one contract change underneath
// them all.
//
// The contract decision this milestone is built on: engineers never see a
// Temporal concept. The user-facing model is exactly two nouns —
//
//     namespace   organization and isolation boundary
//     pool        the set of Orkers a task runs on
//
// A pool resolves to a Temporal task queue INSIDE the engine, and which
// Temporal namespace a run lands in is an operator deployment decision that
// appears in no proto field, no DSL key, no CLI flag and no UI control. That
// is AGENTS.md §1.9's "one word, one thing" applied one layer up: `queue` is a
// Temporal word, so the DSL's `queue:` field goes and `pool:` replaces it.
//
// Shape: eight STACKED implementation slices, each in its own worktree branched
// from the previous VERIFIED one, each executed by the repo's milestone-finish
// child (deterministic ctx.tool gates + dual skeptical review + bounded
// repair). Then a fresh-context adversarial pass proposes falsification probes
// which THIS workflow executes as real commands, a bounded repair loop, and a
// merge gate that honours AGENTS.md §7 (merge-check on the MERGED tree).
//
// Nothing here trusts a model's claim that something passes: every gate,
// probe, and merge check is a durable ctx.tool node running a real command,
// and its real exit status drives what happens next.

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

import { workflow } from "@bastani/workflows";
import { Type, type Static } from "typebox";

import milestoneFinish from "./milestone-finish.js";
import {
  ADVERSARY_ROUTE,
  ARCHITECTURE_ROUTE,
  REPORT_ROUTE,
  implementationRoute,
} from "./support/model-policy.js";

// Routes follow evidence and task risk. This milestone changes several
// cross-process contracts, so architecture uses Opus/high. Fable is reserved
// for an explicit unresolved architectural block; OpenCode Go handles the
// adversarial pass; Sonnet owns implementation and repair.
const DESIGN_ROUTE = ARCHITECTURE_ROUTE;
const ADVERSARY_ROUTE_DEFAULT = ADVERSARY_ROUTE;
const REPORT_ROUTE_DEFAULT = REPORT_ROUTE;

function repairRoute(escalated = false) {
  return implementationRoute(escalated);
}

// ---------------------------------------------------------------------------
// Command execution. Everything deterministic goes through these.
// ---------------------------------------------------------------------------
type Probe = {
  label: string;
  argv: string[];
  ok: boolean;
  status: number;
  stdout: string;
  stderr: string;
};

const TAIL = 4000;

function tail(text: string): string {
  return text.length > TAIL ? text.slice(-TAIL) : text;
}

function run(argv: string[], cwd: string, timeoutMs = 45 * 60_000) {
  return spawnSync(argv[0]!, argv.slice(1), {
    cwd,
    encoding: "utf8",
    timeout: timeoutMs,
    maxBuffer: 64 * 1024 * 1024,
    env: process.env,
  });
}

function probe(argv: string[], cwd: string, label: string): Probe {
  const result = run(argv, cwd);
  return {
    label,
    argv,
    ok: result.status === 0,
    status: result.status ?? -1,
    stdout: tail(result.stdout ?? ""),
    stderr: tail(result.stderr ?? ""),
  };
}

function summarize(probes: Probe[]): string {
  return probes
    .map((p) => `${p.ok ? "PASS" : "FAIL"} (${p.status}) ${p.argv.join(" ")}`)
    .join("\n");
}

function failureDetail(probes: Probe[]): string {
  return probes
    .filter((p) => !p.ok)
    .map((p) =>
      [
        `--- ${p.label}: ${p.argv.join(" ")} (exit ${p.status})`,
        p.stdout.trim() ? `stdout:\n${p.stdout}` : "",
        p.stderr.trim() ? `stderr:\n${p.stderr}` : "",
      ]
        .filter(Boolean)
        .join("\n"),
    )
    .join("\n\n");
}

// ---------------------------------------------------------------------------
// Gates.
//
// GO_CHECKS is build + lint + unit + integration-compile, then the integration
// suites against the live cluster. UI_CHECKS adds the frontend gates, because
// `make merge-check` does not compile, type-check or build ui/ at all — a
// Svelte slice can be green by every Go measure and still ship a broken app.
// ---------------------------------------------------------------------------
const GO_CHECKS: string[][] = [
  ["make", "merge-check"],
  ["go", "test", "-tags=integration", "-p", "1", "./...", "-count=1"],
];

const UI_CHECKS: string[][] = [
  ...GO_CHECKS,
  ["bash", "-lc", "cd ui && bun install && bun run check && bun run build"],
];

// ---------------------------------------------------------------------------
// The slices. Stacked: each is branched from the previous VERIFIED slice, so
// slice N+1 builds on N's committed work. Each is independently buildable,
// testable and provable — a slice is only "done" when a real command says so.
//
// Order is forced by the contract: the proto/DSL/store coordinate (M10.1) must
// exist before routing can use it (M10.2), routing before the API and CLI
// expose it (M10.3), and the three feature slices before the UI renders them.
// ---------------------------------------------------------------------------
type Slice = {
  id: string;
  branch: string;
  dir: string;
  objective: string;
  acceptance: string;
  scopeLock: string;
  checks: string[][];
};

const SLICES: Slice[] = [
  {
    id: "M10.1",
    branch: "m10-1-namespace-contract",
    dir: "m10-1-namespace-contract",
    objective:
      "Make namespace a first-class coordinate of every orkflow and run, and replace the Temporal-leaking `queue:` DSL field with `pool:`.",
    acceptance: [
      "1. Proto gains: Orkflow.namespace, Run.namespace, a Namespace message (name, description, labels, default_pool, allowed_pools, created_at, updated_at), and Task.pool REPLACING Task.queue. Regenerate with `make proto` and `make tsgen`; gen/ is never hand-edited (AGENTS.md §1.1).",
      "2. No user-facing surface names a Temporal concept. `pool` is the only word for where a task executes, and no proto field, DSL key, CLI flag or UI control mentions a queue or a Temporal namespace. Commit a gate script proof/M10.1/naming-sweep.sh, modeled on proof/M9/naming-sweep.sh, that FAILS if proto/, internal/dsl/, cmd/orkctl/, ui/src/ (excluding ui/src/lib/gen) or docs/DSL.md reintroduce either word as a user-facing term. The word `queue` remains correct inside internal/task/, internal/engine/, internal/orker/ and cmd/ wiring, because there it means a Temporal task queue.",
      "3. DSL: a top-level `namespace:` (optional; absent means the `default` namespace) and a task-level `pool:`. A document still using `queue:` is a compile error whose message names `pool:` as the replacement — not a silent alias.",
      "4. Orkflow store keys become metadata/orkflows/{namespace}/{name}/{version}.json. The namespace component is VALIDATED exactly as name and version already are (pkg/artifacts/orkflow_store.go), so no input can escape the prefix or collide with another namespace's key. Prove two namespaces holding the same orkflow name never read each other's documents.",
      "5. The run archive index gains a namespace segment ahead of the orkflow segment, so a namespace-scoped query stays a single prefix scan and ParseRunIndexKey round-trips. Update the segment-count constant and its tests.",
      "6. A namespace registry: metadata/namespaces/{name}.json with Put/Get/List/Delete, same validation discipline. Delete REFUSES while the namespace still holds orkflows and says how many. The `default` namespace always resolves and needs no creation.",
      "7. Nothing is deployed, so this is a clean cut like M9: no shim, no dual-read, no migration path. Move existing tests, examples/*.ork.yaml and docs to the new spelling in this slice.",
      "8. proof/M10.1/ with README.md, test.log, naming-sweep.sh and a reproduce.sh that regenerates it.",
      "9. The event messages the engine SHIPS TO CLIENTS name the pool, never the queue. internal/engine/workflow.go emits `task scheduled on queue <queue>` for EVENT_TYPE_TASK_SCHEDULED; cmd/orkctl/watch.go prints that string in its MESSAGE column and ui/src/routes/runs/[id]/+page.svelte renders it, so an author who wrote `pool: gpu` is currently told about `queue ork-pool-gpu` — Temporal's word plus an internal queue name they never typed. Emit the POOL (task.PoolForQueue exists for exactly this and must gain a real caller), and add a behavior test over the EMITTED EVENT STREAM asserting the message names the pool and not the queue. The naming gate cannot catch this — it correctly exempts internal/engine/, so its PASS is not evidence about strings the engine sends to a client. Any user-visible string the gate cannot see must be covered by a test instead.",
      "10. Do not document a field that cannot be read. `namespace:` compiles and stores, but every API read path (GetOrkflow, ListOrkflows, SubmitRun in internal/api/service.go) is pinned to the default namespace until M10.3, so applying `namespace: payments` today succeeds, writes the object, and then nothing can read or run it. Close the black hole in this slice: ApplyOrkflow REJECTS a non-default namespace with an error naming M10.3 as the slice that enables it, and docs/DSL.md states the limitation next to the field. internal/api is otherwise out of scope for this slice — this guard is the only change permitted there, and M10.3 removes it.",
      "11. `pool: main` and an omitted pool must reach the SAME Orkers under every configuration, by construction rather than by coincidence. Today task.QueueForPool hardcodes DefaultPool -> DefaultQueue while an unpinned task follows the engine's configured orker queue, so they diverge the moment ORK_ORKER_QUEUE is overridden: with ORK_ORKER_QUEUE=ork-pool-gpu an unpinned task goes to ork-pool-gpu and `pool: main` goes to ork-tasks. That breaks the queue-isolation contract in AGENTS.md §6, where an override is what keeps one agent's tasks away from another's. Make `main` resolve to the deployment's default orker channel, whatever it is configured to be, and add a table test asserting the two routes are identical for at least: the default, a plain override, an override that is itself pool-shaped, and empty.",
      "12. The namespace registry must accept EXACTLY the names a DSL document can reference, no more. artifacts.NamespaceKey currently accepts `Payments`, `pay.ments`, `1payments` and `a_b`, none of which satisfy the DSL slug rule at internal/dsl/compile.go, so a namespace can be created and written to and then never named in YAML. Apply the slug rule at the registry boundary and test the accept/reject sets agree, the way pool names already do.",
    ].join("\n"),
    scopeLock:
      "proto/, gen/, internal/dsl/, pkg/artifacts/, internal/engine/ (store seams only), examples/, docs/DSL.md, proof/M10.1/",
    checks: GO_CHECKS,
  },
  {
    id: "M10.2",
    branch: "m10-2-pool-routing",
    dir: "m10-2-pool-routing",
    objective:
      "Route every task to its namespace's Orker pool, enforce that a namespace cannot dispatch onto a pool it is not allowed, and fail a submit fast when a pool has no Orkers.",
    acceptance: [
      "1. Resolution order is: a task's own `pool:` -> the namespace's `default_pool` -> the built-in `main` pool. It is resolved AT SUBMIT inside TemporalStarter.StartRun and bound into the workflow input, exactly as orkerQueue already is — workflow code stays deterministic and never reads configuration (AGENTS.md §1.3).",
      "2. Pool `main` maps to task.DefaultQueue (`ork-tasks`), so `make run-ork` + `make run-orker` with no configuration behave exactly as today. Any other pool maps to a deterministic queue name derived from the pool name; state the mapping in one place and test it.",
      "3. `allowed_pools` is ENFORCED, not documented. Applying or submitting an orkflow whose task names a pool outside its namespace's allowed set fails with an error naming the namespace, the task and the pool. Enforce it at the API boundary, not only in the DSL compiler, so no client can bypass it.",
      "4. A submit whose resolved pool has NO live Orkers fails fast, using Temporal's task-queue poller information rather than a guess. This converts the stranded-RUNNING failure AGENTS.md §6 describes into an error at submit time. If you add an override to submit anyway, it must not be the default and must be logged.",
      "5. An Orker joins a pool with ORK_ORKER_POOL. ORK_ORKER_QUEUE stays as the raw operator-level override used by the test harnesses; document which is which and make them agree on the same queue when both are set (and error when they disagree).",
      "6. Pools are observable: an RPC returns each pool's name, its live Orker count and the plugins those Orkers run. Any replica answers, including one that started nothing.",
      "7. internal/engine gains a namespace router seam that resolves an ork namespace to the Temporal client and reader serving it. It holds exactly ONE entry by default and is exposed by no user-facing surface. The point is that per-namespace hard partitioning later becomes a configuration change instead of a proto change; GetRun(run_id) must keep working with no namespace argument today.",
      "8. An integration test with TWO Orkers on DIFFERENT pools proves all three properties: a task pinned to pool A executes on A's Orker and never on B's; a task naming a disallowed pool is rejected; a submit to an empty pool fails at submit instead of hanging. Isolate it per AGENTS.md §6.",
      "9. proof/M10.2/ with README.md, test.log, a two-pool transcript showing which Orker ran what, and reproduce.sh.",
    ].join("\n"),
    scopeLock:
      "internal/engine/, internal/task/, internal/orker/, internal/api/, cmd/ork/, cmd/orker/, proto/ (only if a field is missing), docs/DEPLOY.md, proof/M10.2/",
    checks: GO_CHECKS,
  },
  {
    id: "M10.3",
    branch: "m10-3-namespace-api-cli",
    dir: "m10-3-namespace-api-cli",
    objective:
      "Give namespaces a full API and CLI surface, and scope every existing list, get and submit by namespace.",
    acceptance: [
      "1. NamespaceService: Create, Get, List, Update, Delete. Create is idempotent for an identical spec. Delete refuses while the namespace holds orkflows.",
      "2. ListOrkflows, GetOrkflow, ListRuns and SubmitRun all carry a namespace; empty means `default`. An orkflow name is unique WITHIN a namespace, not globally. Prove two namespaces can hold the same orkflow name, that listing one never shows the other's orkflows, and that listing runs in one never shows the other's runs.",
      "3. orkctl: `orkctl namespace create|list|get|update|delete`, a global `--namespace`/`-n` flag, and a persisted default in the CLI config so a user is not retyping it. Every command supports `--json`.",
      "4. Errors are actionable: submitting into a missing namespace says which namespace, and lists the namespaces that do exist.",
      "5. Golden CLI tests cover both human and --json output for the new commands, following cmd/orkctl/golden_test.go.",
      "6. M10.1 left an interim guard in ApplyOrkflow rejecting a non-default namespace, because the read paths could not honour it yet. This slice is what it was waiting for: REMOVE the guard, and prove end to end that a non-default namespace now applies, lists, reads and submits. A guard that outlives the slice it was waiting for is a bug.",
      "7. A run's namespace survives every snapshot merge. engine.mergeRunSnapshot drops Namespace when a later snapshot omits it, so a run silently becomes `default` — and because the namespace is what authorizes a namespace-scoped read or cancel, that is a tenancy boundary failing open, not a display bug. Carry the field the way the other identity fields are carried, and add a test that merges a later snapshot with no namespace and asserts the original survives.",
      "8. Namespace validation is enforced on READ paths, not only on write. ListOrkflows, ListRuns and GetNamespace accept `Payments`, `pay ments`, `1payments` and `pay/ments` and fail somewhere downstream instead of returning InvalidArgument at the boundary. artifacts.NamespaceKey already knows the rule; wire it into the read paths and return InvalidArgument, so a malformed name is one clear error rather than a NotFound that looks like an empty namespace.",
      "9. The proto and the implementation must agree about UpdateNamespace. service.proto documents an empty etag as an unconditional write; internal/api/namespaces.go writes with IfAbsent:true and fails FailedPrecondition. Pick the behavior you want, make the code do it, and make the comment say it. A proto comment that contradicts the server is worse than no comment: every client is written against it.",
      "10. ListPools must survive an unreachable orkflow store. pools.go comments that a store failure is non-fatal and falls back to the pools the namespaces define, and then errors instead. Either implement the documented fallback or delete the claim — and test it with a store that fails, because a fallback nobody exercises is a fallback that does not work.",
      "11. proof/M10.3/ with README.md, test.log, a CLI session transcript covering two isolated namespaces, and reproduce.sh.",
    ].join("\n"),
    scopeLock:
      "internal/api/, cmd/orkctl/, proto/ (only if a field is missing), docs/, proof/M10.3/",
    checks: GO_CHECKS,
  },
  {
    id: "M10.4",
    branch: "m10-4-orkflow-versions",
    dir: "m10-4-orkflow-versions",
    objective:
      "Make an orkflow's current version an explicit, movable pointer, and give the API and CLI the version operations an editor needs.",
    acceptance: [
      "1. Versions stay immutable. A per-orkflow `current` pointer decides what an empty version resolves to. When no pointer was ever set, resolution falls back to today's rule (most recently applied), so nothing regresses for an orkflow nobody has promoted.",
      "2. API: ListOrkflowVersions (ordered, marking which is current), SetCurrentOrkflowVersion, and a way to diff two versions' source_yaml. Decide server-side versus client-side diff and justify the choice in a comment.",
      "3. Promotion is safe under two replicas: two concurrent promotions converge on one value, and a reader NEVER observes a pointer to a version that does not exist. Promoting to an unknown version is rejected. Prove the two-replica property with a real test, not an argument.",
      "4. Applying edited YAML creates a NEW version and never rewrites an existing one. Re-applying byte-identical YAML is a no-op that creates no version and churns no object — prove it by comparing stored bytes before and after.",
      "5. Optimistic concurrency: applying an edit derived from a stale version is DETECTED and reported, not silently clobbering a newer one. State the mechanism (expected-version token or content hash) in the proto comment.",
      "6. orkctl: `orkctl orkflow versions`, `promote`, `diff`, and `edit` ($EDITOR round-trip that fetches source_yaml, opens it, and applies the result). All namespace-scoped, all with --json where output is data.",
      "7. A run pinned to a version still executes THAT version after the pointer moves away from it. Prove it with a run in flight across a promotion.",
      "8. proof/M10.4/ with README.md, test.log, a promote-then-rollback transcript, and reproduce.sh.",
    ].join("\n"),
    scopeLock:
      "proto/, pkg/artifacts/, internal/api/, internal/engine/, cmd/orkctl/, docs/, proof/M10.4/",
    checks: GO_CHECKS,
  },
  {
    id: "M10.5",
    branch: "m10-5-task-logs",
    dir: "m10-5-task-logs",
    objective:
      "Stream a task's output while it runs, readable from any replica, without putting bytes through Temporal or the event stream.",
    acceptance: [
      "1. The Orker writes numbered log chunks to the artifact store DURING execution, under a key layout that makes order explicit rather than inferred from a listing (for example metadata/logs/{run}/{task}/{attempt}/{seq}). Flush is bounded by BOTH size and elapsed time, so a chatty task does not write per line and a slow task still appears promptly.",
      "2. plugin.Logger and the shell plugin's stdout/stderr feed that sink, so a shell task's output is visible WHILE it runs and not only afterwards as an artifact. The existing stdout/stderr artifacts keep working unchanged.",
      "3. A StreamTaskLogs RPC and a WebSocket route replay existing chunks in order, then follow new ones, and support resume the way StreamEvents supports from_sequence. Ordering is total and gap-free per (run, task, attempt), and the stream ends when the task reaches a terminal state.",
      "4. Any replica serves any run's logs — including a run it never started, one whose Orker has exited, and one that finished before the reader connected.",
      "5. Bytes NEVER enter a Temporal payload or the Event stream (AGENTS.md §1.4). The event stream may announce that logs exist; it must not carry them. Add a test that fails if a log line appears in run history.",
      "6. Bounded: a runaway task cannot exhaust the Orker's memory or the engine's. State the cap, and make truncation VISIBLE in the stream rather than silent.",
      "7. orkctl: `orkctl run logs <run> [--task <id>] [-f]` follows live and exits when the run reaches a terminal state.",
      "8. An integration test submits a task that emits output slowly, follows its logs from a SECOND engine replica, and asserts ordered lines arrive BEFORE the run finishes, plus the whole log is readable after. A test that only reads logs post-mortem does not prove streaming.",
      "9. proof/M10.5/ with README.md, test.log, a live-follow transcript with timestamps showing lines arriving mid-run, and reproduce.sh.",
    ].join("\n"),
    scopeLock:
      "proto/, pkg/artifacts/, pkg/plugin/, pkg/plugins/, internal/orker/, internal/api/, cmd/orkctl/, docs/, proof/M10.5/",
    checks: GO_CHECKS,
  },
  {
    id: "M10.6",
    branch: "m10-6-stats",
    dir: "m10-6-stats",
    objective:
      "Answer dashboard questions from Temporal and the archive index, adding no new write path.",
    acceptance: [
      "1. A stats RPC returns an overview scoped by namespace and time window: runs by status, success rate, p50/p95 run duration, the most-failing orkflows, currently running runs, and pool health (live Orkers per pool). Every field must be answerable from the two sources below — drop or redesign any that is not.",
      "2. Live runs come from Temporal visibility; finished runs come from archive index prefix scans that read KEYS ONLY. No aggregate may be computed by fetching run bodies, and none may be read from process memory (AGENTS.md §1.7).",
      "3. A short-lived cache keeps a polling dashboard from becoming a scan storm. It is an optimization that CANNOT change an answer; its TTL is documented and configurable, and a test proves a cached answer equals the uncached one.",
      "4. Any replica returns identical numbers, including a replica that started no runs.",
      "5. A correctness test seeds a known set of runs across TWO namespaces and asserts exact counts, success rate and percentiles — including that namespace A's numbers exclude namespace B's entirely.",
      "6. orkctl: `orkctl stats [--namespace] [--since]`, human output by default and --json for machines.",
      "7. proof/M10.6/ with README.md, test.log, a transcript showing the same numbers from two replicas, and reproduce.sh.",
    ].join("\n"),
    scopeLock:
      "proto/, internal/api/, internal/engine/, pkg/artifacts/, cmd/orkctl/, docs/, proof/M10.6/",
    checks: GO_CHECKS,
  },
  {
    id: "M10.7",
    branch: "m10-7-ui-namespaces-editor",
    dir: "m10-7-ui-namespaces-editor",
    objective:
      "Make the UI namespace-aware, and let a user edit an orkflow and manage its versions without leaving the browser.",
    acceptance: [
      "1. A namespace switcher scopes every page (orkflows, runs, submit, apply). The selection survives navigation and reload AND appears in the URL, so a link a user pastes to a colleague opens the same namespace.",
      "2. The orkflow detail page gains: a version list marking the current version, a YAML editor whose validation errors come from the real ApplyOrkflow response before anything is stored, a diff between any two versions, and a promote action with a confirmation step.",
      "3. Editing an orkflow that changed underneath you is detected and reported using M10.4's concurrency mechanism, never silently overwriting a newer version.",
      "4. Svelte 5 runes and the existing Tailwind conventions. Clients in ui/src/lib/gen are regenerated with `make tsgen` and never hand-edited.",
      "5. Every new view has loading, empty and error states, and no view can show a spinner forever — a failed request must say what failed and offer a retry.",
      "6. Playwright coverage for: switching namespace, editing and applying, promoting and rolling back, and diffing two versions. `cd ui && bun run check && bun run build` is clean.",
      "7. proof/M10.7/ with README.md, Playwright output, screenshots of each new view, and reproduce.sh.",
    ].join("\n"),
    scopeLock: "ui/, proof/M10.7/",
    checks: UI_CHECKS,
  },
  {
    id: "M10.8",
    branch: "m10-8-ui-logs-dashboard",
    dir: "m10-8-ui-logs-dashboard",
    objective:
      "Show live task logs and a real dashboard in the UI, write the v0.0.3 operating story down, and prove the whole milestone in one command.",
    acceptance: [
      "1. The run detail page gains a live log pane per task: it follows while the task runs, preserves order, keeps scrollback, pauses following when the user scrolls up, reconnects with resume after a dropped socket, and does not freeze the tab on a chatty task.",
      "2. A dashboard home page shows M10.6's stats scoped to the selected namespace, with pool health and recent failures. An unavailable number is shown as unavailable — never as zero.",
      "3. Docs: DSL.md documents `namespace:` and `pool:`; DEPLOY.md documents Orker pools, ORK_ORKER_POOL, log storage and log retention; AGENTS.md gains the namespace and pool invariants, including a §1.9 clause stating that `pool` is the user-facing word and `queue` is Temporal's. Every claim must match code.",
      "4. M10.1's naming gate is re-run against this tree and passes.",
      "5. One consolidated proof runs the real stack: two namespaces, two Orker pools, an orkflow edited and promoted, a live log follow, and a dashboard read — with the reads served by a replica that started nothing.",
      "6. proof/M10.8/ with README.md, test.log, the live transcript, screenshots, and a reproduce.sh that performs all of it in ONE command. It must exit 77 (not 0) when a prerequisite is absent, and must not overwrite the previous transcript when it declines to run (AGENTS.md §6).",
      "7. `make merge-check` passes, the integration suite passes with -p 1, and `cd ui && bun run check && bun run build` is clean.",
    ].join("\n"),
    scopeLock: "ui/, docs/, AGENTS.md, proof/M10.8/, scripts/",
    checks: UI_CHECKS,
  },
];

const probePlan = Type.Object(
  {
    findings: Type.Array(Type.String(), {
      description:
        "Specific, evidence-backed concerns with file:line references. Empty when none.",
    }),
    probes: Type.Array(
      Type.Object({
        name: Type.String({
          description: "Short probe id, e.g. namespace-b-cannot-read-namespace-a.",
        }),
        argv: Type.Array(Type.String(), {
          minItems: 1,
          description:
            "Command to run from the repository root. Must be non-interactive and terminate on its own.",
        }),
        requirement: Type.String({
          description: "Which acceptance item or risk this probe falsifies.",
        }),
        expectation: Type.String({
          description: "What a PASSING result looks like (exit 0 means the property holds).",
        }),
      }),
      { description: "Executable falsification probes. Exit 0 must mean the property HOLDS." },
    ),
  },
  { additionalProperties: false },
);
type ProbePlan = Static<typeof probePlan>;

export default workflow({
  name: "v003-milestone",
  description:
    "OrkStack v0.0.3: namespaces and Orker pools, orkflow editing and version management, live task log streaming, and a stats dashboard, as stacked verified slices with adversarial probes and a merge gate.",
  inputs: {
    repo_dir: Type.String({
      description: "Repository root (the main worktree).",
      default: "/Users/daniel/Code/orkstack",
    }),
    base_branch: Type.String({ default: "main" }),
    worktree_root: Type.String({
      description: "Directory that will hold the per-slice worktrees.",
      default: "/Users/daniel/Code/orkstack-worktrees",
    }),
    max_rounds_per_slice: Type.Number({ default: 4 }),
    max_repair_rounds: Type.Number({
      default: 2,
      description: "Bounded repair rounds after the adversarial probe pass.",
    }),
    merge_to_base: Type.Boolean({
      default: true,
      description:
        "Merge the last verified slice into base_branch, gated by make merge-check on the merged tree.",
    }),
  },
  outputs: {
    slices_verified: Type.Number(),
    slices_total: Type.Number(),
    stopped_at: Type.String(),
    probe_summary: Type.String(),
    merged: Type.Boolean(),
    report: Type.String(),
  },
  run: async (ctx) => {
    const repo = String(ctx.inputs.repo_dir);
    const baseBranch = String(ctx.inputs.base_branch);
    const worktreeRoot = String(ctx.inputs.worktree_root);
    const maxRounds = Math.max(1, Number(ctx.inputs.max_rounds_per_slice) || 4);
    const maxRepairs = Math.max(0, Number(ctx.inputs.max_repair_rounds) || 0);
    const mergeToBase = ctx.inputs.merge_to_base !== false;

    // -----------------------------------------------------------------------
    // Preflight. Real commands: a red tree or absent infrastructure must stop
    // this before any agent writes code, otherwise every later signal is noise.
    // -----------------------------------------------------------------------
    const preflight = (await ctx.tool("preflight", { repo }, async () => {
      const checks: Probe[] = [
        probe(["git", "status", "--porcelain"], repo, "clean-tree"),
        probe(["git", "rev-parse", "--abbrev-ref", "HEAD"], repo, "branch"),
        probe(
          [
            "docker",
            "exec",
            "orkstack-temporal-1",
            "temporal",
            "operator",
            "cluster",
            "health",
            "--address",
            "127.0.0.1:7233",
          ],
          repo,
          "temporal-health",
        ),
        probe(["curl", "-sf", "http://localhost:9000/minio/health/live"], repo, "minio-health"),
        probe(["bash", "-lc", "command -v bun"], repo, "bun-present"),
        probe(["make", "merge-check"], repo, "baseline-merge-check"),
      ];
      const dirty = checks[0]!.stdout.trim() !== "";
      const blocking = checks.filter((c) => c.label !== "clean-tree" && !c.ok);
      return {
        dirty,
        summary: summarize(checks),
        detail: failureDetail(checks),
        ok: !dirty && blocking.length === 0,
      };
    })) as { dirty: boolean; summary: string; detail: string; ok: boolean };

    if (!preflight.ok) {
      return ctx.exit({
        status: "failed",
        reason: `preflight failed:\n${preflight.summary}\n\n${preflight.detail}`,
        outputs: {
          slices_verified: 0,
          slices_total: SLICES.length,
          stopped_at: "preflight",
          probe_summary: preflight.summary,
          merged: false,
          report: `Preflight refused to start.\n\n${preflight.summary}`,
        },
      });
    }

    // -----------------------------------------------------------------------
    // Design. One pass that fixes the shape of all eight slices and writes it
    // to a file the slice children READ, so the contract is decided once
    // instead of drifting eight times. The namespace/pool coordinate touches
    // every slice, so it cannot be re-litigated per slice.
    // -----------------------------------------------------------------------
    const designPath = join(repo, ".atomic", "v003-design.md");
    const design = await ctx.task("design", {
      context: "fresh",
      model: DESIGN_ROUTE.model,
      fallbackModels: DESIGN_ROUTE.fallbackModels,
      prompt: [
        "<keepContext>",
        "You are designing, NOT implementing. Produce one design document and write it to:",
        designPath,
        "",
        "Decisions already made by the repository owner. They are INPUTS, not open questions:",
        "- The user-facing model is exactly two nouns: `namespace` (organization and isolation) and `pool` (the set of Orkers a task runs on).",
        "- Engineers must never see a Temporal concept. No proto field, DSL key, CLI flag or UI control may expose a Temporal namespace or a task queue. A pool resolves to a queue INSIDE the engine.",
        "- Which Temporal namespace a run lands in is an operator deployment decision, expressed in engine configuration only. Do not add it to the proto or the DSL.",
        "- The DSL's `queue:` field is REMOVED and replaced by `pool:`. Nothing is deployed, so this is a clean cut with no shim, exactly as the M9 rename was.",
        "- Orkflow versions stay immutable; a movable `current` pointer decides what an empty version resolves to.",
        "- Task logs travel as numbered chunks in the artifact store, and the engine tails them. They never travel through Temporal payloads or the event stream.",
        "- Dashboard stats are derived on read from Temporal visibility plus archive-index key scans, with a short cache. No new write path.",
        "",
        "Constraints that must hold in every recommendation:",
        "- Temporal workflow code must stay deterministic (AGENTS.md §1.3). No clock, network, randomness or IO inside workflow functions.",
        "- Proto is the source of truth for cross-process types; gen/ is never hand-edited (AGENTS.md §1.1).",
        "- Artifacts stay references, never bytes in payloads or the event stream (AGENTS.md §1.4).",
        "- Temporal is the read model for live runs and the archive for finished ones; never answer a read from process memory (AGENTS.md §1.7).",
        "- The repo pins go.temporal.io/sdk v1.47.0 against server 1.31.2. Only recommend APIs that exist in THAT SDK version — verify against the module cache, do not recall from memory.",
        "</keepContext>",
        "",
        "Repository: " + repo,
        "",
        "Design the following eight stacked slices as one coherent whole, because they share one coordinate and touch the same files in sequence:",
        ...SLICES.map((s) => `\n## ${s.id} — ${s.objective}\n${s.acceptance}`),
        "",
        "Research first, with evidence:",
        "1. Read proto/orkstack/v1/*.proto, internal/api/service.go, internal/engine/{workflow,temporal,reader,activities,bus}.go, internal/dsl/{compile,validate}.go, pkg/artifacts/{orkflow_store,archive,s3}.go, internal/orker/activity.go, cmd/ork/main.go, cmd/orker/main.go, and the UI routes under ui/src/routes.",
        "2. Read the actual SDK surface in the Go module cache for anything you rely on: task-queue description and poller information, visibility count/list APIs, and start options. Cite file paths and exact type and field names.",
        "3. Probe the live dev server where behavior is uncertain (docker exec orkstack-temporal-1 temporal ...). In particular, confirm exactly how to learn whether a task queue has live pollers, and what it reports when it has none.",
        "",
        "Then decide and write down, concretely:",
        "- The exact proto diff for all eight slices, as one coherent contract, so slice N does not have to reopen slice N-1's messages.",
        "- The pool-to-queue mapping function, and why `main` must map to the existing default.",
        "- Where allowed_pools is enforced so no client can bypass it.",
        "- The namespace router seam's interface, and the argument that GetRun(run_id) keeps working with one entry.",
        "- The current-version pointer's storage shape and its concurrency story under two replicas, given S3 has no compare-and-swap. If you need a CAS, say exactly which one MinIO and S3 both support in this repo's client version, or design so you do not need one.",
        "- The log chunk key layout, flush policy, ordering and resume token, truncation behavior, and retention.",
        "- The stats queries, field by field, naming the source for each and the cost of each.",
        "- The UI information architecture across all four features: where the namespace switcher lives, how the editor and version list are laid out, and where logs and the dashboard sit.",
        "For each slice also give: the behavior test that would fail before and pass after, and the failure modes you rejected. Where two mechanisms are viable, name the trade-off and pick one.",
        "",
        "Be concrete and short. No restating the task back. Write the file, then reply with a one-paragraph summary of the decisions.",
      ].join("\n"),
    });

    // -----------------------------------------------------------------------
    // Stacked slices. Slice N+1 branches from slice N's VERIFIED head, so the
    // stack always builds on proven work. First failure stops the stack;
    // earlier verified slices stay verified.
    // -----------------------------------------------------------------------
    let previousRef = baseBranch;
    let verified = 0;
    let stoppedAt = "";
    let lastGoodDir = repo;
    let lastGoodBranch = baseBranch;
    let lastGoodChecks = GO_CHECKS;
    const sliceReports: string[] = [];

    for (const slice of SLICES) {
      const dir = join(worktreeRoot, slice.dir);

      const prepared = (await ctx.tool(
        `prepare-${slice.dir}`,
        { dir, branch: slice.branch, from: previousRef },
        async () => {
          if (!existsSync(worktreeRoot)) mkdirSync(worktreeRoot, { recursive: true });
          if (existsSync(join(dir, ".git"))) {
            // Reusing a worktree from an earlier run. The previous slice's
            // branch may have moved since this one forked off it — an
            // adversarial repair round commits onto the last VERIFIED branch,
            // which is exactly the branch this slice was cut from. Merge it
            // forward or those repairs are silently dropped at the final
            // merge, which takes only the LAST branch.
            // A dirty reused worktree is restored, not refused. A slice only
            // becomes the stack's base by passing verification, which requires
            // a clean tree — so anything dirty here arrived AFTER that, from a
            // probe round regenerating a transcript or a child that died
            // mid-edit. Neither is work worth keeping, and refusing it strands
            // the whole stack on a file nobody committed.
            const dirty = run(["git", "status", "--porcelain"], dir);
            const discarded = (dirty.stdout ?? "").trim();
            if (discarded !== "") {
              run(["git", "checkout", "--", "."], dir);
              run(["git", "clean", "-fd"], dir);
            }
            const restack = run(["git", "merge", "--no-edit", previousRef], dir, 5 * 60_000);
            if (restack.status !== 0) {
              // A conflicting restack is not a broken stack, it is WORK, and
              // it is this slice's work: the base moved because slice N-1 was
              // repaired in the same code this slice rewrote. Abort the merge
              // so the child starts from a clean tree, and hand it the
              // conflict as its first task, under the same reviewers and gates
              // as everything else. Resolving it here would put an unreviewed
              // integration of two slices onto the branch.
              const conflicts = run(
                ["git", "diff", "--name-only", "--diff-filter=U"],
                dir,
              );
              const files = (conflicts.stdout ?? "").trim();
              run(["git", "merge", "--abort"], dir);
              return {
                dir,
                action: "reused-conflict",
                ok: true,
                conflicted: true,
                conflicts: files,
                detail: `restacking ${previousRef} into ${slice.branch} conflicts in:\n${files}`,
              };
            }
            return {
              dir,
              action: "reused",
              ok: true,
              conflicted: false,
              conflicts: "",
              detail: [
                discarded ? `discarded uncommitted leftovers before restacking:\n${discarded}` : "",
                tail((restack.stdout ?? "") + (restack.stderr ?? "")),
              ]
                .filter(Boolean)
                .join("\n"),
            };
          }
          const add = run(
            ["git", "worktree", "add", "-b", slice.branch, dir, previousRef],
            repo,
            5 * 60_000,
          );
          return {
            dir,
            action: "created",
            ok: add.status === 0,
            conflicted: false,
            conflicts: "",
            detail: tail((add.stdout ?? "") + (add.stderr ?? "")),
          };
        },
      )) as {
        dir: string;
        action: string;
        ok: boolean;
        conflicted: boolean;
        conflicts: string;
        detail: string;
      };

      if (!prepared.ok) {
        stoppedAt = `${slice.id} (worktree)`;
        sliceReports.push(`${slice.id}: worktree preparation failed\n${prepared.detail}`);
        break;
      }

      const child = await ctx.workflow(milestoneFinish, {
        stageName: `slice ${slice.id}`,
        inputs: {
          milestone: slice.id,
          objective: slice.objective,
          acceptance: [
            prepared.conflicted
              ? [
                  "<keepContext>",
                  `STEP 0, BEFORE ANY OTHER WORK: restack this branch onto ${previousRef}.`,
                  "",
                  `${previousRef} moved after ${slice.branch} was cut from it — an adversarial`,
                  "repair round fixed defects in the SAME code this slice rewrote. Run:",
                  "",
                  `    git merge --no-commit --no-ff ${previousRef}`,
                  "",
                  `and resolve the conflicts. They are in:\n${prepared.conflicts}`,
                  "",
                  "Resolve as a UNION OF BEHAVIORS, per AGENTS.md §7. Do NOT take either side",
                  "whole unless the file is untouched on the other. Both sides are correct about",
                  "different things: the base slice owns the primitives and the invariants it was",
                  "repaired to guarantee; this slice owns the architecture built on top of them.",
                  "The union normally means calling the base's repaired primitive from this",
                  "slice's newer structure, NOT reinstating an older call site the repair removed.",
                  "",
                  "Use the base to see what each side actually changed:",
                  `    git merge-file -p ours.go $(git merge-base HEAD ${previousRef}):file theirs.go`,
                  `    git diff --name-only HEAD ${previousRef}`,
                  "",
                  "Regenerate, never hand-merge, anything under gen/ or ui/src/lib/gen: resolve",
                  "proto/ first, then `make proto` and `make tsgen` and take the generated result.",
                  "",
                  "The merged tree must COMPILE and pass every gate before you continue to the",
                  "acceptance items below. When you are done, state in your final message exactly",
                  "what each side contributed and what the resolution dropped, if anything.",
                  "</keepContext>",
                  "",
                ].join("\n")
              : "",
            slice.acceptance,
            "",
            "GENERAL (applies to every item above):",
            `- The agreed design for this work is at ${designPath}. Read it before implementing. If you deviate, say so and justify it with evidence.`,
            "<keepContext>",
            "- Engineers must never see a Temporal concept. `pool` is the user-facing word for where a task runs; `queue` and `Temporal namespace` are engine-internal and must not appear in proto, the DSL, orkctl flags or the UI.",
            "- Behavior tests first (AGENTS.md §4): a test that fails for the right reason, then the implementation.",
            "- Integration tests must genuinely exercise real Temporal and MinIO, and skip cleanly through the package's infraAbsent helper when absent. Never fake a pass, and never weaken a test to make it green.",
            `- Isolate every integration run you start: ORK_TEMPORAL_NAMESPACE=ork-${slice.dir} (AGENTS.md §6). Kill every engine and orker you start; anchor the patterns as '/bin/ork$' and '/bin/orker$'.`,
            "- Proto changes go through `make proto` and `make tsgen`. Never hand-edit anything under gen/ or ui/src/lib/gen.",
            "- DONE means proven: the proof directory must be regenerable by its own reproduce.sh, and its README must state what was proven and how.",
            "- Finish with a CLEAN worktree. `git status --porcelain` must be empty when you stop: regenerate proof artifacts first, then commit them. The parent verifies your branch independently and treats any uncommitted file as a failed slice, however green the gates are.",
            "</keepContext>",
          ].join("\n"),
          scope_lock: slice.scopeLock,
          git_worktree_dir: dir,
          branch: slice.branch,
          base_branch: previousRef,
          checks: slice.checks,
          max_rounds: maxRounds,
        },
      });

      if (child.exited === true) {
        stoppedAt = `${slice.id} (child exited: ${child.exitReason ?? child.status})`;
        sliceReports.push(
          `${slice.id}: child workflow exited early — ${child.exitReason ?? child.status}`,
        );
        break;
      }

      // The child says it is done. Verify that independently, with real
      // commands, in its own worktree. A model's claim is not evidence.
      const verification = (await ctx.tool(
        `verify-${slice.dir}`,
        { dir, branch: slice.branch },
        async () => {
          const committed = probe(["git", "status", "--porcelain"], dir, "committed");
          let uncommitted = committed.stdout
            .split("\n")
            .filter((l) => l.trim() !== "" && !/\.atomic|research\//.test(l));

          // Running this repo's gates REGENERATES committed proof artifacts of
          // EARLIER milestones — proof/M8.5/docs-claims-sweep.txt asserts the
          // docs still match the tree, and this milestone changes the tree, so
          // its output legitimately moves. AGENTS.md §7 says to rerun an
          // affected milestone's proof after touching its code, which makes
          // the regenerated file the correct evidence rather than dirt. Commit
          // it and carry on; stalling an eight-slice stack on a text file the
          // suite rewrites deterministically is the wrong trade. Dirt anywhere
          // else, including this slice's OWN proof directory, still fails: the
          // slice owns that evidence and must commit it deliberately.
          const ownProof = `proof/${slice.id}/`;
          const isForeignProof = (line: string) => {
            const path = line.slice(3).trim();
            return (
              line.startsWith(" M ") && path.startsWith("proof/") && !path.startsWith(ownProof)
            );
          };
          let refreshed: string[] = [];
          if (uncommitted.length > 0 && uncommitted.every(isForeignProof)) {
            refreshed = uncommitted.map((l) => l.slice(3).trim());
            run(["git", "add", ...refreshed], dir);
            run(
              [
                "git",
                "commit",
                "-m",
                `chore(proof): regenerate an earlier milestone's evidence on ${slice.branch}\n\nThe gates rewrote ${refreshed.join(", ")}. AGENTS.md §7: a proof taken on an\nearlier tree is a claim about this one that nothing has checked, so the\nregenerated output is committed rather than discarded.`,
              ],
              dir,
            );
            uncommitted = [];
          }

          const gates = slice.checks.map((argv, i) => probe(argv, dir, `gate-${i}`));
          const head = run(["git", "rev-parse", "HEAD"], dir);
          return {
            ok: uncommitted.length === 0 && gates.every((g) => g.ok),
            head: (head.stdout ?? "").trim(),
            uncommitted,
            refreshed,
            summary: summarize(gates),
            detail: failureDetail(gates),
          };
        },
      )) as {
        ok: boolean;
        head: string;
        uncommitted: string[];
        refreshed: string[];
        summary: string;
        detail: string;
      };

      const approved = Boolean(child.outputs?.approved);
      if (!approved || !verification.ok) {
        stoppedAt = slice.id;
        sliceReports.push(
          [
            `${slice.id}: NOT verified.`,
            `  reviewers approved: ${approved}`,
            `  independent gates:\n${verification.summary}`,
            verification.uncommitted.length > 0
              ? `  uncommitted: ${verification.uncommitted.join(", ")}`
              : "",
            verification.detail ? `\n${verification.detail}` : "",
          ]
            .filter(Boolean)
            .join("\n"),
        );
        break;
      }

      verified += 1;
      previousRef = slice.branch;
      lastGoodDir = dir;
      lastGoodBranch = slice.branch;
      lastGoodChecks = slice.checks;
      sliceReports.push(
        `${slice.id}: verified at ${verification.head.slice(0, 9)} on ${slice.branch} (rounds: ${String(child.outputs?.rounds_used ?? "?")})`,
      );
    }

    // -----------------------------------------------------------------------
    // Adversarial pass. A fresh, skeptical reader of the FINISHED stack writes
    // falsification probes; this workflow executes them for real. Model
    // opinion selects the probes, the shell decides the verdict.
    // -----------------------------------------------------------------------
    let probeSummary = "no slices verified; adversarial pass skipped";
    let adversarialApproved = false;
    let previousAdversarialKey = "";
    if (verified > 0) {
      for (let round = 1; round <= maxRepairs + 1; round += 1) {
        const adversary = await ctx.task(`adversary-${round}`, {
          context: "fresh",
          schema: probePlan,
          model: ADVERSARY_ROUTE_DEFAULT.model,
          fallbackModels: ADVERSARY_ROUTE_DEFAULT.fallbackModels,
          prompt: [
            "<keepContext>",
            "You are a grumpy but fair distributed-systems and multi-tenancy reviewer with fresh context.",
            `Working tree: ${lastGoodDir} (branch ${lastGoodBranch}).`,
            "Your job is to FALSIFY the claims below with executable probes, not to restate them.",
            "Do NOT modify implementation code. Return only evidence-backed findings and probes.",
            "Do NOT invent requirements beyond these claims. Every finding needs file:line evidence.",
            "Every probe must be non-interactive, terminate on its own, and be written so that",
            "EXIT 0 MEANS THE PROPERTY HOLDS. Prefer `go test -run ...` over shell pipelines.",
            "</keepContext>",
            "",
            "Claims to falsify:",
            ...SLICES.slice(0, verified).map((s) => `\n## ${s.id}\n${s.acceptance}`),
            "",
            "Where the real risk lives, and what a lazy review would miss:",
            "- A namespace that is filtered in the API but is NOT part of the storage key, so two namespaces can collide, or one can read the other's orkflows by asking directly.",
            "- allowed_pools enforced only in the DSL compiler or only in the CLI, so a direct API call lands work on another team's Orkers.",
            "- A pool liveness check that passes because it described the wrong queue name, or that reports healthy when zero Orkers poll.",
            "- Logs that are called streaming but only appear after the task finishes; or ordering that depends on S3 listing order rather than an explicit sequence; or a resume that silently drops or duplicates a chunk.",
            "- Log bytes that reach Temporal history or the event stream despite the claim that they do not.",
            "- Stats computed by reading run bodies, or served from process memory, so a second replica disagrees; or percentiles computed over a page rather than the window.",
            "- A version pointer that can reference a missing version, a promote that loses a concurrent promote, or a re-apply of identical YAML that churns a new version.",
            "- A run in flight that changes version because the pointer moved under it.",
            "- A test that passed because it SKIPPED (missing infrastructure proves nothing), or a proof artifact left over from an earlier run.",
            "- Temporal vocabulary leaking into proto, the DSL, orkctl flags or the UI.",
            "- A UI that polls on a timer and calls it live, or a log pane that grows without bound.",
            "",
            "Read the code and the committed proof artifacts first. Then return structured_output:",
            "findings (evidence-backed, may be empty) and probes (commands this workflow will execute).",
          ].join("\n"),
        });

        const plan = adversary.structured as ProbePlan | undefined;
        const planned = (plan?.probes ?? []).slice(0, 12);

        const results = (await ctx.tool(
          `run-probes-${round}`,
          { count: planned.length, dir: lastGoodDir },
          async () => {
            const out = planned.map((p) => ({
              ...probe(p.argv, lastGoodDir, p.name),
              requirement: p.requirement,
              expectation: p.expectation,
            }));
            // Probes are READS, and must leave no trace. Several of them run a
            // proof's reproduce.sh, which regenerates its transcripts by
            // design, so a probe round reliably dirties the worktree. Left
            // there it deadlocks the NEXT run: prepare will not restack a
            // dirty tree, and the slice can never start. Restore to HEAD and
            // drop anything a probe created. The slice's own work is committed
            // by this point — verification refused it otherwise.
            run(["git", "checkout", "--", "."], lastGoodDir);
            run(["git", "clean", "-fd"], lastGoodDir);
            return out;
          },
        )) as (Probe & { requirement: string; expectation: string })[];

        probeSummary = results.length > 0 ? summarize(results) : "adversary proposed no probes";
        const failed = results.filter((r) => !r.ok);
        const findings = plan?.findings ?? [];

        if (failed.length === 0 && findings.length === 0) {
          adversarialApproved = true;
          break;
        }
        if (round === maxRepairs + 1) break;

        const repairKey = [
          ...findings,
          ...failed.map((result) => `${result.label}:${result.stdout}:${result.stderr}`),
        ].join("\n");
        const escalate = repairKey !== "" && repairKey === previousAdversarialKey;
        previousAdversarialKey = repairKey;
        const route = repairRoute(escalate);
        await ctx.task(`repair-${round}`, {
          context: "fork",
          model: route.model,
          fallbackModels: route.fallbackModels,
          cwd: lastGoodDir,
          prompt: [
            "<keepContext>",
            `Repair round ${round}. Work in ${lastGoodDir} on branch ${lastGoodBranch}.`,
            "You are the original slice implementer repairing your own scope. First classify each",
            "finding as confirmed, probable, ambiguity, test deficiency, style, duplicate or false",
            "positive. Fix confirmed defects and required test gaps only; resolve material ambiguity",
            "against the acceptance contract. Do not weaken, skip or delete tests, and do not edit",
            "outside the scope of the slices already implemented. Commit on this branch when done.",
            "If a finding is factually wrong, disprove it with file:line evidence instead of ignoring it.",
            "</keepContext>",
            "",
            "Adversarial review findings:",
            ...findings.map((f) => `- ${f}`),
            "",
            failed.length > 0
              ? `Probes that FAILED (real command output):\n\n${failureDetail(failed)}`
              : "No probe failed, but the findings above still stand.",
            "",
            `After fixing, re-run these gates yourself and commit only when they are green:\n${lastGoodChecks
              .map((c) => `  ${c.join(" ")}`)
              .join("\n")}`,
          ].join("\n"),
        });

        const regate = (await ctx.tool(`regate-${round}`, { dir: lastGoodDir }, async () => {
          const probes = lastGoodChecks.map((argv, i) => probe(argv, lastGoodDir, `gate-${i}`));
          return {
            ok: probes.every((p) => p.ok),
            summary: summarize(probes),
            detail: failureDetail(probes),
          };
        })) as { ok: boolean; summary: string; detail: string };

        if (!regate.ok) {
          probeSummary = `${probeSummary}\n\nrepair round ${round} left gates red:\n${regate.summary}`;
        }
      }
    }

    // -----------------------------------------------------------------------
    // Merge gate. AGENTS.md §7: the merge RESULT is what must be green, and a
    // branch being green says nothing about it. Merge without committing, gate
    // on the merged tree, and only then write the merge commit.
    // -----------------------------------------------------------------------
    let merged = false;
    let mergeSummary = "skipped";
    if (mergeToBase && verified === SLICES.length && adversarialApproved) {
      const mergeResult = (await ctx.tool(
        "merge-and-gate",
        { branch: lastGoodBranch, into: baseBranch },
        async () => {
          const steps: Probe[] = [];
          steps.push(probe(["git", "checkout", baseBranch], repo, "checkout-base"));
          if (!steps[0]!.ok) {
            return { ok: false, summary: summarize(steps), detail: failureDetail(steps) };
          }
          steps.push(
            probe(["git", "merge", "--no-commit", "--no-ff", lastGoodBranch], repo, "merge"),
          );
          if (!steps[1]!.ok) {
            run(["git", "merge", "--abort"], repo);
            return { ok: false, summary: summarize(steps), detail: failureDetail(steps) };
          }
          const gate = probe(["make", "merge-check"], repo, "merged-tree-merge-check");
          steps.push(gate);
          const uiGate = probe(
            ["bash", "-lc", "cd ui && bun install && bun run check && bun run build"],
            repo,
            "merged-tree-ui-check",
          );
          steps.push(uiGate);
          if (!gate.ok || !uiGate.ok) {
            run(["git", "merge", "--abort"], repo);
            return { ok: false, summary: summarize(steps), detail: failureDetail(steps) };
          }
          steps.push(
            probe(
              [
                "git",
                "commit",
                "-m",
                `Merge ${lastGoodBranch}: v0.0.3 — namespaces, orkflow versions, live logs, dashboard\n\nGated on the merged tree with make merge-check and the UI checks (AGENTS.md §7).`,
              ],
              repo,
              "commit",
            ),
          );
          return {
            ok: steps.every((s) => s.ok),
            summary: summarize(steps),
            detail: failureDetail(steps),
          };
        },
      )) as { ok: boolean; summary: string; detail: string };
      merged = mergeResult.ok;
      mergeSummary = mergeResult.ok
        ? mergeResult.summary
        : `${mergeResult.summary}\n${mergeResult.detail}`;
    }

    const report = await ctx.task("report", {
      context: "fresh",
      model: REPORT_ROUTE_DEFAULT.model,
      fallbackModels: REPORT_ROUTE_DEFAULT.fallbackModels,
      prompt: [
        "Write a short, factual completion report for an engineer who was away.",
        "No preamble, no restating the task, no praise. State what is now true, what is not,",
        "and the exact next action if anything is unfinished.",
        "",
        `Slices verified: ${verified}/${SLICES.length}`,
        stoppedAt ? `Stopped at: ${stoppedAt}` : "Stopped at: nothing, the stack completed",
        "",
        "Per-slice result:",
        ...sliceReports,
        "",
        `Adversarial probe results. These probes ran against ${lastGoodBranch}, the last VERIFIED`,
        "slice — NOT against the slice the stack stopped at. Attribute them to that branch, and do",
        "not report a probe failure as the reason a later slice failed. Real command exits:",
        probeSummary,
        "",
        `Merge into ${baseBranch}: ${merged ? "done, gated on the merged tree" : mergeSummary}`,
        "",
        `Design document: ${designPath}`,
        `Design decisions summary: ${design.text.slice(0, 1500)}`,
      ].join("\n"),
    });

    return {
      slices_verified: verified,
      slices_total: SLICES.length,
      stopped_at: stoppedAt || "none",
      probe_summary: probeSummary,
      merged,
      report: report.text,
    };
  },
});
