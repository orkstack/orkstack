// .atomic/workflows/production-hardening.ts
//
// Make OrkStack safe to run as more than one engine process, and adopt
// Temporal Worker Deployment Versioning (GA) end to end.
//
// The problem this exists for: the engine keeps live run state in memory
// (EventBus, RunStore, the idempotency map) while the activity that feeds that
// state is dispatched to a SHARED queue every replica polls. With two engines,
// one run's events scatter across replicas, idempotency keys stop deduping,
// and run ids are only per-process unique. Separately, nothing pins workflow
// code to a build, so deploying new engine code while runs are in flight can
// break replay — the determinism invariant in AGENTS.md §1.3.
//
// Shape: five STACKED implementation slices, each in its own worktree branched
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

// ---------------------------------------------------------------------------
// Models. Roles per docs/models/model-selection.md; every id and thinking
// level below was checked against the configured catalog before launch.
//   design            -> planning/synthesis                      => high
//   adversarial probe -> high cost of error (silent corruption)  => max
//   repair            -> triage/repair                           => high
//   report            -> final reporting                         => medium
// Slice children pin their own cross-provider chains; they are not overridden.
// ---------------------------------------------------------------------------
const DESIGN_MODEL = "anthropic/claude-opus-5:high";
const DESIGN_FALLBACKS = [
  "openai-codex/gpt-5.6-luna:high",
  "opencode-go/kimi-k3:max",
];

const ADVERSARY_MODEL = "openai-codex/gpt-5.6-sol:max";
const ADVERSARY_FALLBACKS = [
  "anthropic/claude-opus-5:max",
  "openai-codex/gpt-5.6-luna:max",
];

const REPAIR_MODEL = "anthropic/claude-opus-5:high";
const REPAIR_FALLBACKS = [
  "openai-codex/gpt-5.6-luna:high",
  "opencode-go/kimi-k3:max",
];

const REPORT_MODEL = "anthropic/claude-sonnet-5:medium";
const REPORT_FALLBACKS = ["anthropic/claude-opus-5:medium"];

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
// The slices. Stacked: each is branched from the previous VERIFIED slice, so
// slice N+1 builds on N's committed work. Each is independently buildable,
// testable and provable — a slice is only "done" when a real command says so.
// ---------------------------------------------------------------------------
type Slice = {
  id: string;
  branch: string;
  dir: string;
  objective: string;
  acceptance: string;
  scopeLock: string;
};

const SLICES: Slice[] = [
  {
    id: "M8.1",
    branch: "m8-1-run-identity",
    dir: "m8-1-run-identity",
    objective:
      "Make a run id unique across engine replicas, not just within one process.",
    acceptance: [
      "1. Two engine processes generating run ids concurrently never collide. The id is the Temporal workflow id, so a collision is a failed or hijacked run.",
      "2. The documented shape from AGENTS.md §1.5 is preserved: ork-{timestamp}-{sequence}. Do not switch to a bare UUID.",
      "3. The sequence component no longer restarts at 1 in every process, so ids are not misleading across replicas.",
      "4. A behavior test proves uniqueness under concurrency across SEPARATE processes (not just goroutines in one).",
      "5. Existing run ids remain valid: nothing parses or orders runs in a way this breaks. Check cmd/orkctl, internal/api and the UI for id assumptions.",
      "6. proof/M8.1/ with README.md, test.log and a reproduce.sh that regenerates it.",
    ].join("\n"),
    scopeLock:
      "internal/api/, internal/engine/, cmd/orkctl/ (only if it parses ids), proof/M8.1/",
  },
  {
    id: "M8.2",
    branch: "m8-2-idempotency",
    dir: "m8-2-idempotency",
    objective:
      "Make idempotency keys dedupe across engine replicas by making Temporal the dedupe authority instead of a process-local map.",
    acceptance: [
      "1. api.Service.idempotencyMap no longer decides deduplication. Two replicas receiving the same idempotency key concurrently converge on ONE run.",
      "2. Use Temporal's own mechanism (for example a deterministic workflow id derived from the key plus WorkflowIDConflictPolicy USE_EXISTING, or an equivalent documented GA API in SDK v1.47). Justify the choice in code comments.",
      "3. A failed start stays deduplicated to its recorded failed run, which is the behavior the current in-process path documents.",
      "4. An integration test starts TWO real engine processes against real Temporal, submits the same idempotency key to both concurrently, and asserts exactly one run exists and both callers see it.",
      "5. Existing single-engine idempotency tests still pass unchanged in meaning.",
      "6. proof/M8.2/ with README.md, test.log and reproduce.sh.",
    ].join("\n"),
    scopeLock: "internal/api/, internal/engine/, proof/M8.2/",
  },
  {
    id: "M8.3",
    branch: "m8-3-read-model",
    dir: "m8-3-read-model",
    objective:
      "Answer live run reads from Temporal so any replica can serve any run, instead of from process-local EventBus/RunStore state.",
    acceptance: [
      "1. GetRun, ListRuns, StreamEvents and the WebSocket bridge return correct, COMPLETE results from a replica that did not start the run and did not execute its PublishEvent activities.",
      "2. The event stream contract holds on that replica: strictly increasing sequences, no gaps, and a terminal event that ends the stream.",
      "3. Temporal is the system of record for live runs (workflow query and/or history), the S3 archive stays the record for finished runs, and any in-process cache is an optimization that cannot change an answer.",
      "4. An integration test runs TWO real engine replicas sharing one namespace and the ork-engine-events queue: submit on A, then GetRun and StreamEvents on B, asserting the full ordered stream and terminal status. It must fail if the read model regresses to process-local state.",
      "5. Workflow determinism is preserved: no new non-deterministic call inside workflow code, and any new query handler is registered deterministically.",
      "6. proof/M8.3/ with README.md, test.log, a two-replica transcript, and reproduce.sh.",
    ].join("\n"),
    scopeLock:
      "internal/api/, internal/engine/, cmd/ork/, ui/src/lib/ (only if the stream contract changes), proof/M8.3/",
  },
  {
    id: "M8.4",
    branch: "m8-4-worker-versioning",
    dir: "m8-4-worker-versioning",
    objective:
      "Adopt Temporal Worker Deployment Versioning (GA) for Orkers and the engine, so deploying new code cannot break in-flight runs.",
    acceptance: [
      "1. Both binaries register with Worker Deployment Versioning using SDK v1.47's worker.Options.DeploymentOptions (UseVersioning, Version{DeploymentName, BuildId}, DefaultVersioningBehavior). Deployment name and build id come from ORK_ prefixed env with sane defaults; document them.",
      "2. RunWorkflow declares an explicit VersioningBehavior. Pinned is the default for the run workflow unless you can justify AutoUpgrade in comments with reference to what replay would do.",
      "3. Task activities of a pinned run execute on the same version as the workflow, so an Orker rollout cannot move an in-flight run's tasks onto new plugin code mid-run. Prove it, do not assume it.",
      "4. An integration test proves the GA rollout property against the real cluster: start a run on build v1, make v2 the current version while it is in flight, and assert the in-flight run completes on v1 while a NEWLY submitted run executes on v2. The dev server supports this (temporal worker deployment list works on server 1.31.2).",
      "5. A replay guard exists for cross-version determinism: commit at least one golden workflow history JSON and a WorkflowReplayer test that replays it against current code. The existing replay check records and replays with the SAME code, so it cannot catch old-history-meets-new-code.",
      "6. Versioning is off by default or degrades cleanly when the env is unset, so a single-process dev run still works with no configuration.",
      "7. proof/M8.4/ with README.md, test.log, a rollout transcript showing both versions, and reproduce.sh.",
    ].join("\n"),
    scopeLock:
      "cmd/ork/, cmd/orker/, internal/engine/, internal/orker/, internal/task/, docs/, proof/M8.4/",
  },
  {
    id: "M8.5",
    branch: "m8-5-ha-docs-proof",
    dir: "m8-5-ha-docs-proof",
    objective:
      "Prove the whole stack runs as a replicated, versioned deployment, and write the operating story down.",
    acceptance: [
      "1. docs/DEPLOY.md describes running OrkStack for real: multiple engine replicas, Orker pools, namespace and queue boundaries, the versioned rollout procedure (set current version, ramp, drain), and what to do when a deploy must be rolled back.",
      "2. AGENTS.md is updated where it is now wrong or incomplete: §1 invariants if the read model or id contract changed, §6 isolation guidance, and a deployment/versioning note. Keep it factual — every claim must match code.",
      "3. A consolidated proof runs the real thing: two engine replicas plus at least two Orker builds against real Temporal and MinIO, a run served from the replica that did not start it, an idempotent double submit, and a versioned rollout with an in-flight run finishing on the old build.",
      "4. proof/M8.5/ with README.md, test.log, the live transcript, and a reproduce.sh that performs all of it in one command.",
      "5. make merge-check passes, and the full integration suite passes with -p 1.",
    ].join("\n"),
    scopeLock: "docs/, AGENTS.md, proof/M8.5/, scripts/, deploy/",
  },
];

// Deterministic gates every slice must pass. merge-check is build + lint +
// unit + integration COMPILE; the second command actually runs the integration
// suites against the live cluster.
const SLICE_CHECKS: string[][] = [
  ["make", "merge-check"],
  ["go", "test", "-tags=integration", "-p", "1", "./...", "-count=1"],
];

const probePlan = Type.Object(
  {
    findings: Type.Array(Type.String(), {
      description:
        "Specific, evidence-backed concerns with file:line references. Empty when none.",
    }),
    probes: Type.Array(
      Type.Object({
        name: Type.String({ description: "Short probe id, e.g. pinned-run-survives-rollout." }),
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
  name: "production-hardening",
  description:
    "Make OrkStack multi-replica safe (run ids, idempotency, live read model) and adopt Temporal Worker Deployment Versioning, as stacked verified slices with adversarial probes and a merge gate.",
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
        probe(
          [
            "docker",
            "exec",
            "orkstack-temporal-1",
            "temporal",
            "worker",
            "deployment",
            "list",
            "--address",
            "127.0.0.1:7233",
            "--namespace",
            "ork-main",
          ],
          repo,
          "worker-versioning-available",
        ),
        probe(["curl", "-sf", "http://localhost:9000/minio/health/live"], repo, "minio-health"),
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
    // Design. One pass that fixes the Temporal-native approach for all five
    // slices and writes it to a file the slice children READ, so the design is
    // not re-litigated (or drifted) five times.
    // -----------------------------------------------------------------------
    const designPath = join(repo, ".atomic", "production-hardening-design.md");
    const design = await ctx.task("design", {
      context: "fresh",
      model: DESIGN_MODEL,
      fallbackModels: DESIGN_FALLBACKS,
      prompt: [
        "<keepContext>",
        "You are designing, NOT implementing. Produce one design document and write it to:",
        designPath,
        "",
        "Constraints that must hold in every recommendation:",
        "- Temporal workflow code must stay deterministic (AGENTS.md §1.3). No clock, network, randomness or IO inside workflow functions.",
        "- Proto is the source of truth for cross-process types; gen/ is never hand-edited (AGENTS.md §1.1).",
        "- Artifacts stay references, never bytes in payloads or the event stream (AGENTS.md §1.4).",
        "- The repo pins go.temporal.io/sdk v1.47.0 against server 1.31.2. Only recommend APIs that exist in THAT SDK version — verify against the module cache, do not recall from memory.",
        "</keepContext>",
        "",
        "Repository: " + repo,
        "",
        "Design the following five stacked slices as one coherent whole, because they touch the same files in sequence:",
        ...SLICES.map((s) => `\n## ${s.id} — ${s.objective}\n${s.acceptance}`),
        "",
        "Research first, with evidence:",
        "1. Read internal/api/service.go, internal/engine/{workflow,temporal,activities,bus,store}.go, cmd/ork/main.go, cmd/orker/main.go.",
        "2. Read the actual SDK surface in the Go module cache for: workflow queries, GetWorkflowHistory / history streaming, StartWorkflowOptions conflict and reuse policies, worker.Options.DeploymentOptions, RegisterWorkflowOptions.VersioningBehavior, and the WorkerDeployment client handle. Cite file paths and exact type/field names.",
        "3. Probe the live dev server where behavior is uncertain (docker exec orkstack-temporal-1 temporal ...).",
        "",
        "For each slice produce: the chosen mechanism and why, the exact SDK calls, what changes in which file, the determinism argument, the behavior test that would fail before and pass after, and the failure modes you rejected. Where two mechanisms are viable, name the trade-off and pick one.",
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
    const sliceReports: string[] = [];

    for (const slice of SLICES) {
      const dir = join(worktreeRoot, slice.dir);

      const prepared = (await ctx.tool(
        `prepare-${slice.dir}`,
        { dir, branch: slice.branch, from: previousRef },
        async () => {
          if (!existsSync(worktreeRoot)) mkdirSync(worktreeRoot, { recursive: true });
          if (existsSync(join(dir, ".git"))) {
            return { dir, action: "reused", ok: true, detail: "" };
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
            detail: tail((add.stdout ?? "") + (add.stderr ?? "")),
          };
        },
      )) as { dir: string; action: string; ok: boolean; detail: string };

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
            slice.acceptance,
            "",
            "GENERAL (applies to every item above):",
            `- The agreed design for this work is at ${designPath}. Read it before implementing. If you deviate, say so and justify it with evidence.`,
            "- Behavior tests first (AGENTS.md §4): a test that fails for the right reason, then the implementation.",
            "- Integration tests must genuinely exercise real Temporal and MinIO, and skip cleanly when absent. Never fake a pass.",
            `- For any integration run of your own, isolate it: ORK_TEMPORAL_NAMESPACE=ork-${slice.dir} (AGENTS.md §6). Kill every worker you start.`,
            "- DONE means proven: the proof directory must be regenerable by its own reproduce.sh, and its README must state what was proven.",
          ].join("\n"),
          scope_lock: slice.scopeLock,
          git_worktree_dir: dir,
          branch: slice.branch,
          base_branch: previousRef,
          checks: SLICE_CHECKS,
          max_rounds: maxRounds,
        },
      });

      if (child.exited === true) {
        stoppedAt = `${slice.id} (child exited: ${child.exitReason ?? child.status})`;
        sliceReports.push(`${slice.id}: child workflow exited early — ${child.exitReason ?? child.status}`);
        break;
      }

      // The child says it is done. Verify that independently, with real
      // commands, in its own worktree. A model's claim is not evidence.
      const verification = (await ctx.tool(
        `verify-${slice.dir}`,
        { dir, branch: slice.branch },
        async () => {
          const probes = [
            probe(["git", "status", "--porcelain"], dir, "committed"),
            probe(["make", "merge-check"], dir, "merge-check"),
            probe(
              ["go", "test", "-tags=integration", "-p", "1", "./...", "-count=1"],
              dir,
              "integration",
            ),
          ];
          const head = run(["git", "rev-parse", "HEAD"], dir);
          const uncommitted = probes[0]!.stdout
            .split("\n")
            .filter((l) => l.trim() !== "" && !/\.atomic|research\//.test(l));
          return {
            ok: uncommitted.length === 0 && probes[1]!.ok && probes[2]!.ok,
            head: (head.stdout ?? "").trim(),
            uncommitted,
            summary: summarize(probes.slice(1)),
            detail: failureDetail(probes.slice(1)),
          };
        },
      )) as {
        ok: boolean;
        head: string;
        uncommitted: string[];
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
    if (verified > 0) {
      for (let round = 1; round <= maxRepairs + 1; round += 1) {
        const adversary = await ctx.task(`adversary-${round}`, {
          context: "fresh",
          schema: probePlan,
          model: ADVERSARY_MODEL,
          fallbackModels: ADVERSARY_FALLBACKS,
          prompt: [
            "<keepContext>",
            "You are a grumpy but fair distributed-systems reviewer with fresh context.",
            `Working tree: ${lastGoodDir} (branch ${lastGoodBranch}).`,
            "Your job is to FALSIFY the claims below with executable probes, not to restate them.",
            "Do NOT invent requirements beyond these claims. Every finding needs file:line evidence.",
            "Every probe must be non-interactive, terminate on its own, and be written so that",
            "EXIT 0 MEANS THE PROPERTY HOLDS. Prefer `go test -run ...` over shell pipelines.",
            "</keepContext>",
            "",
            "Claims to falsify:",
            ...SLICES.slice(0, verified).map((s) => `\n## ${s.id}\n${s.acceptance}`),
            "",
            "Where the real risk lives, and what a lazy review would miss:",
            "- A test that passes because it silently SKIPPED (missing infrastructure) proves nothing.",
            "- A two-replica test where both replicas share process memory is not a two-replica test.",
            "- A read model that 'works' only because the same process happened to project the events.",
            "- Worker versioning configured but not actually pinning: an in-flight run that migrates to a new build.",
            "- A replay guard that records and replays with the SAME code, which cannot catch cross-version drift.",
            "- Determinism violations added inside workflow functions (clock, map iteration order, network, randomness).",
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
          async () =>
            planned.map((p) => ({
              ...probe(p.argv, lastGoodDir, p.name),
              requirement: p.requirement,
              expectation: p.expectation,
            })),
        )) as (Probe & { requirement: string; expectation: string })[];

        probeSummary = results.length > 0 ? summarize(results) : "adversary proposed no probes";
        const failed = results.filter((r) => !r.ok);
        const findings = plan?.findings ?? [];

        if (failed.length === 0 && findings.length === 0) break;
        if (round === maxRepairs + 1) break;

        await ctx.task(`repair-${round}`, {
          context: "fork",
          model: REPAIR_MODEL,
          fallbackModels: REPAIR_FALLBACKS,
          cwd: lastGoodDir,
          prompt: [
            "<keepContext>",
            `Repair round ${round}. Work in ${lastGoodDir} on branch ${lastGoodBranch}.`,
            "Fix ROOT CAUSES. Do not weaken, skip or delete tests, and do not edit outside the",
            "scope of the slices already implemented. Commit on this branch when done.",
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
            "After fixing, re-run `make merge-check` and the integration suite yourself, then commit.",
          ].join("\n"),
        });

        const regate = (await ctx.tool(
          `regate-${round}`,
          { dir: lastGoodDir },
          async () => {
            const probes = [
              probe(["make", "merge-check"], lastGoodDir, "merge-check"),
              probe(
                ["go", "test", "-tags=integration", "-p", "1", "./...", "-count=1"],
                lastGoodDir,
                "integration",
              ),
            ];
            return { ok: probes.every((p) => p.ok), summary: summarize(probes), detail: failureDetail(probes) };
          },
        )) as { ok: boolean; summary: string; detail: string };

        if (!regate.ok) {
          probeSummary = `${probeSummary}\n\nrepair round ${round} left gates red:\n${regate.summary}`;
        }
      }
    }

    // -----------------------------------------------------------------------
    // Merge gate. AGENTS.md §7: the merge result is what must be green, and a
    // branch being green says nothing about it. Merge without committing, gate
    // on the merged tree, and only then write the merge commit.
    // -----------------------------------------------------------------------
    let merged = false;
    let mergeSummary = "skipped";
    if (mergeToBase && verified === SLICES.length) {
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
          if (!gate.ok) {
            run(["git", "merge", "--abort"], repo);
            return { ok: false, summary: summarize(steps), detail: failureDetail(steps) };
          }
          steps.push(
            probe(
              [
                "git",
                "commit",
                "-m",
                `Merge ${lastGoodBranch}: multi-replica safety and Temporal worker versioning\n\nGated on the merged tree with make merge-check (AGENTS.md §7).`,
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
      mergeSummary = mergeResult.ok ? mergeResult.summary : `${mergeResult.summary}\n${mergeResult.detail}`;
    }

    const report = await ctx.task("report", {
      context: "fresh",
      model: REPORT_MODEL,
      fallbackModels: REPORT_FALLBACKS,
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
        "Adversarial probe results (real command exits):",
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
