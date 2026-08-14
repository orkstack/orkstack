// .atomic/workflows/milestone-finish.ts
//
// Finish and prove out a partially-implemented OrkStack milestone from its
// CURRENT worktree state (it does not restart work from scratch).
//
// Why this exists: the builtin `ralph`/`goal` workflows route models
// internally, so a provider-wide 429 blocks the whole run. This workflow pins
// an explicit model per ROLE with CROSS-PROVIDER fallback chains, so a rate
// limit or usage-limit exhaustion on one provider advances to the other
// instead of blocking. Primaries are deliberately spread across OpenAI and
// Anthropic to balance subscription burn.
//
// Model roles follow docs/models/model-selection.md:
//   worker/orchestrator -> gpt-5.6-luna  (frontier, best value workhorse)
//   judgment gate       -> gpt-5.6-sol   (accuracy ceiling; a wrong verdict
//                                         discards a whole loop)
//   second reviewer     -> claude-opus-5 (different family => decorrelated
//                                         review errors, per the guide)
//
// Verification is authoritative: every check runs through a durable
// ctx.tool(...) node and the REAL exit status feeds review/repair. Models
// never self-report whether the build or tests passed.

import { workflow } from "@bastani/workflows";
import { Type, type Static } from "typebox";

// ---------------------------------------------------------------------------
// Model chains. Each entry is "provider/id:thinking".
// Every chain spans BOTH providers so no single quota can block a stage.
// ---------------------------------------------------------------------------
const WORKER_MODEL = "openai-codex/gpt-5.6-luna:max";
const WORKER_FALLBACKS = [
  "openai-codex/gpt-5.6-terra:max",
  "anthropic/claude-opus-5:high",
  "openai-codex/gpt-5.5:high",
];

const JUDGE_MODEL = "openai-codex/gpt-5.6-sol:max";
const JUDGE_FALLBACKS = [
  "openai-codex/gpt-5.6-terra:max",
  "anthropic/claude-opus-5:max",
];

// Second reviewer intentionally leads with Anthropic: a different model family
// decorrelates review errors, and it splits load off the OpenAI subscription.
const JUDGE_B_MODEL = "anthropic/claude-opus-5:max";
const JUDGE_B_FALLBACKS = [
  "openai-codex/gpt-5.6-terra:max",
  "openai-codex/gpt-5.6-sol:max",
];

const reviewDecision = Type.Object(
  {
    approved: Type.Boolean({
      description: "True only when every acceptance item is verifiably met.",
    }),
    findings: Type.Array(Type.String(), {
      description:
        "Specific, evidence-backed, actionable defects. Empty when approved.",
    }),
  },
  { additionalProperties: false },
);

type ReviewDecision = Static<typeof reviewDecision>;

interface CheckOutcome {
  readonly label: string;
  readonly argv: readonly string[];
  readonly ok: boolean;
  readonly exitCode: number;
  readonly output: string;
}

// Runs a command and REPORTS the outcome instead of throwing, so a failing
// gate becomes evidence for the repair stage rather than crashing the run.
// Output is tail-capped to keep prompts small.
function probe(argv: readonly string[], cwd: string, label: string): CheckOutcome {
  try {
    const result = Bun.spawnSync([...argv], { cwd, stdout: "pipe", stderr: "pipe" });
    const stdout = result.stdout.toString();
    const stderr = result.stderr.toString();
    const merged = `${stdout}${stderr}`.trim();
    const lines = merged.split("\n");
    const tail = lines.length > 60 ? lines.slice(-60).join("\n") : merged;
    return {
      label,
      argv,
      ok: result.exitCode === 0,
      exitCode: result.exitCode ?? -1,
      output: tail,
    };
  } catch (error) {
    return {
      label,
      argv,
      ok: false,
      exitCode: -1,
      output: `failed to execute: ${String(error)}`,
    };
  }
}

function summarize(outcomes: readonly CheckOutcome[]): string {
  return outcomes
    .map((o) => `${o.ok ? "PASS" : "FAIL"} [${o.label}] ${o.argv.join(" ")}`)
    .join("\n");
}

function failureDetail(outcomes: readonly CheckOutcome[]): string {
  return outcomes
    .filter((o) => !o.ok)
    .map(
      (o) =>
        `### FAILED: ${o.label} (exit ${o.exitCode})\n$ ${o.argv.join(" ")}\n${o.output}`,
    )
    .join("\n\n");
}

export default workflow({
  name: "milestone-finish",
  description:
    "Finish a partially-built milestone in its worktree: deterministic gates, cross-provider model chains, skeptical review, bounded repair.",
  inputs: {
    milestone: Type.String({ description: "Milestone id, e.g. M3." }),
    objective: Type.String({ description: "What this milestone must deliver." }),
    acceptance: Type.String({ description: "Immutable definition of done." }),
    scope_lock: Type.String({ description: "Files/dirs this run may modify." }),
    git_worktree_dir: Type.String({ description: "Existing worktree path." }),
    branch: Type.String({ description: "Branch the work must land on." }),
    base_branch: Type.String({ default: "main" }),
    checks: Type.Array(Type.Array(Type.String(), { minItems: 1 }), {
      minItems: 1,
      description: "Deterministic gate commands, e.g. [['go','build','./...']].",
    }),
    max_rounds: Type.Number({ default: 4 }),
  },
  outputs: {
    milestone: Type.String(),
    approved: Type.Boolean(),
    rounds_used: Type.Number(),
    gate_summary: Type.String(),
    head_commit: Type.String(),
  },
  worktreeFromInputs: {
    gitWorktreeDir: "git_worktree_dir",
    baseBranch: "base_branch",
  },
  run: async (ctx) => {
    const milestone = String(ctx.inputs.milestone);
    const objective = String(ctx.inputs.objective);
    const acceptance = String(ctx.inputs.acceptance);
    const scopeLock = String(ctx.inputs.scope_lock);
    const branch = String(ctx.inputs.branch);
    const cwd = ctx.cwd ?? String(ctx.inputs.git_worktree_dir);
    const checks = ctx.inputs.checks as readonly (readonly string[])[];
    const maxRounds = Math.max(1, Number(ctx.inputs.max_rounds) || 4);

    // Contract text reused verbatim in every stage prompt. Wrapped in
    // keepContext so compaction cannot erode the scope lock or the DoD.
    const contract = [
      "<keepContext>",
      `MILESTONE: ${milestone}`,
      `OBJECTIVE: ${objective}`,
      "",
      "ACCEPTANCE (immutable definition of done — do not reinterpret or narrow):",
      acceptance,
      "",
      `SCOPE LOCK: ${scopeLock}`,
      `You are in an existing git worktree at ${cwd} on branch ${branch}.`,
      "Work is ALREADY PARTIALLY IMPLEMENTED here — read the existing code first",
      "and CONTINUE it. Do not restart from scratch and do not revert prior work.",
      "Commit your work on this branch. Do NOT open a pull request.",
      "</keepContext>",
    ].join("\n");

    // Ensure the worktree is on the expected branch before any agent touches it.
    await ctx.tool("ensure-branch", { branch, cwd }, async () => {
      const current = Bun.spawnSync(["git", "branch", "--show-current"], {
        cwd,
        stdout: "pipe",
        stderr: "pipe",
      });
      const name = current.stdout.toString().trim();
      if (name === branch) return { branch: name, action: "already-on-branch" };
      const probeRef = Bun.spawnSync(
        ["git", "show-ref", "--verify", "--quiet", `refs/heads/${branch}`],
        { cwd, stdout: "pipe", stderr: "pipe" },
      );
      const argv =
        probeRef.exitCode === 0
          ? ["git", "switch", branch]
          : ["git", "switch", "-c", branch];
      const sw = Bun.spawnSync(argv, { cwd, stdout: "pipe", stderr: "pipe" });
      if (sw.exitCode !== 0) {
        throw new Error(`could not switch to ${branch}: ${sw.stderr.toString()}`);
      }
      return { branch, action: argv.join(" ") };
    });

    let approved = false;
    let roundsUsed = 0;
    let gateSummary = "";

    for (let round = 1; round <= maxRounds; round += 1) {
      roundsUsed = round;

      // ---- Deterministic gates. Real exit codes, durably checkpointed. ----
      const outcomes: CheckOutcome[] = [];
      for (const [index, argv] of checks.entries()) {
        const label = `check-${index + 1}`;
        const outcome = await ctx.tool(
          `${label}-round-${round}`,
          { argv, round },
          async () => probe(argv, cwd, label),
        );
        outcomes.push(outcome as CheckOutcome);
      }
      gateSummary = summarize(outcomes);
      const gatesGreen = outcomes.every((o) => o.ok);

      // ---- Gates red: repair against real failure output, then re-gate. ----
      if (!gatesGreen) {
        if (round === maxRounds) break;
        await ctx.task(`repair-gates-${round}`, {
          context: "fork",
          model: WORKER_MODEL,
          fallbackModels: WORKER_FALLBACKS,
          prompt: [
            contract,
            "",
            `Deterministic verification FAILED in round ${round}. These are real`,
            "command outputs from this worktree, not opinions. Fix the ROOT CAUSE",
            "so every gate passes. Do not weaken, skip, or delete tests to make",
            "them pass, and do not edit outside the scope lock.",
            "",
            failureDetail(outcomes),
            "",
            "Gate summary:",
            gateSummary,
            "",
            "Then commit your work on the branch.",
          ].join("\n"),
        });
        continue;
      }

      // ---- Gates green: two independent skeptical reviewers. ----
      const reviewPrompt = [
        contract,
        "",
        "All deterministic gates currently PASS:",
        gateSummary,
        "",
        "You are a GRUMPY BUT FAIR reviewer with fresh context. Passing tests are",
        "NOT sufficient evidence: verify the tests actually prove the acceptance",
        "items, that integration tests genuinely exercise real infrastructure",
        "rather than skipping, and that nothing was stubbed, weakened, or faked to",
        "get green. Inspect the real code and the committed proof artifacts.",
        "",
        "Do NOT invent requirements beyond the acceptance contract. Report only",
        "specific, actionable, evidence-backed defects with file:line references.",
        "Approve only when every acceptance item is verifiably met.",
        "Return structured_output with approved and findings.",
      ].join("\n");

      const reviews = await ctx.parallel(
        [
          {
            name: `review-a-${round}`,
            context: "fresh",
            schema: reviewDecision,
            model: JUDGE_MODEL,
            fallbackModels: JUDGE_FALLBACKS,
            prompt: reviewPrompt,
          },
          {
            name: `review-b-${round}`,
            context: "fresh",
            schema: reviewDecision,
            model: JUDGE_B_MODEL,
            fallbackModels: JUDGE_B_FALLBACKS,
            prompt: reviewPrompt,
          },
        ],
        { concurrency: 2 },
      );

      const decisions = reviews.map((r) => r.structured as ReviewDecision);
      const findings = decisions.flatMap((d) => d?.findings ?? []);
      // Both reviewers must approve. Either one objecting sends it back.
      approved = decisions.every((d) => d?.approved === true);

      if (approved) break;
      if (round === maxRounds) break;

      await ctx.task(`repair-review-${round}`, {
        context: "fork",
        model: WORKER_MODEL,
        fallbackModels: WORKER_FALLBACKS,
        prompt: [
          contract,
          "",
          `Round ${round} review did NOT approve. Fix every finding below, then`,
          "re-run the relevant checks yourself and commit. Do not weaken tests.",
          "If a finding is factually wrong, prove it with file:line evidence in",
          "your final message instead of silently ignoring it.",
          "",
          ...findings.map((f) => `- ${f}`),
        ].join("\n"),
      });
    }

    // Work must be committed to count as done.
    const head = await ctx.tool("require-committed", { branch }, async () => {
      const pending = Bun.spawnSync(["git", "status", "--porcelain"], {
        cwd,
        stdout: "pipe",
        stderr: "pipe",
      });
      const dirty = pending.stdout
        .toString()
        .split("\n")
        .filter((line) => line.trim() !== "")
        // Agent scratch is not part of the deliverable.
        .filter((line) => !/\.atomic|research\//.test(line));
      const rev = Bun.spawnSync(["git", "rev-parse", "HEAD"], {
        cwd,
        stdout: "pipe",
        stderr: "pipe",
      });
      return {
        commit: rev.stdout.toString().trim(),
        uncommitted: dirty,
      };
    });

    const headResult = head as { commit: string; uncommitted: string[] };

    return {
      milestone,
      approved,
      rounds_used: roundsUsed,
      gate_summary: gateSummary,
      head_commit: headResult.commit ?? "",
    };
  },
});
