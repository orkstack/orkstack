// .atomic/workflows/v004-continue.ts
//
// OrkStack v0.0.4, part two: finish what run 3272c190 could not.
//
// That run verified M11.1 (container application logs in the live stream) and
// M11.2 (version-scoped editing), then repaired both twice under adversarial
// probes. It stopped at M11.3: the Admin surface branch passes every gate with
// a clean tree, but its reviewer withheld approval over three findings, and the
// final probe round found five more defects in the code beneath it.
//
// So this run does NOT start from scratch and does NOT re-run the design stage
// — .atomic/v004-design.md already holds the agreed contract. It starts from
// the REPAIRED m11-2 branch and runs five slices:
//
//   M11.3  restack m11-3 onto the repairs, then fix all EIGHT findings
//   M11.4  run page: scoped task log panels, on-demand event view
//   M11.5  version-scoped editor in the UI
//   M11.6  the Admin section
//   M11.7  docs and one-command consolidated proof
//
// Two decisions the repository owner made after reading the last run's report,
// which are INPUTS here and not open questions:
//
//   1. The container plugin's task output payload MUST STOP carrying stdout
//      and stderr strings. AGENTS.md §1.2 says bytes travel only as artifacts;
//      the plugin has been contradicting that since it was written, and M11.1's
//      acceptance is what finally made it testable. Nothing is deployed, so it
//      is a clean cut: remove the fields, carry output through the live log
//      stream and the capture artifacts, and update every doc, example and
//      template that referenced them.
//   2. M11.3's repair comes before the UI slices, so the UI is built on a
//      backend whose reads are honest.
//
// Two process changes, both paid for by the last run:
//
//   - ROUND 1 IMPLEMENTS. Every slice in the previous run spent its first
//     round producing nothing — a reviewer then reported "the branch carries
//     zero work", and the repair round did the whole slice. That wasted one
//     round of four on every slice and cost M11.3 the budget it needed. The
//     instruction is now explicit in the acceptance preamble.
//   - Round budget is 6, not 4. M11.3 exhausted four rounds with a scope
//     dispute in the middle and had nothing left when a real finding arrived.
//
// Everything else is the shape that worked: stacked worktrees, deterministic
// ctx.tool gates, dual skeptical review, independent parent verification,
// adversarial probes executed as real commands, and a merge gated on the
// MERGED tree (AGENTS.md §7).

import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { join } from "node:path";

import { workflow } from "@bastani/workflows";
import { Type, type Static } from "typebox";

import milestoneFinish from "./milestone-finish.js";
import {
  ADVERSARY_ROUTE,
  REPORT_ROUTE,
  implementationRoute,
} from "./support/model-policy.js";

// The contract already exists, so this continuation starts with cheap
// OpenCode Go falsification. Repairs stay with the slice implementer: Sonnet
// first, then Opus only after repeated failure. Reporting uses a cheap factual
// route. See docs/MODEL_SELECTION.md.
const ADVERSARY_ROUTE_DEFAULT = ADVERSARY_ROUTE;
const REPORT_ROUTE_DEFAULT = REPORT_ROUTE;

function repairRoute(escalated = false) {
  return implementationRoute(escalated);
}

// ---------------------------------------------------------------------------
// Command execution.
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

const GO_CHECKS: string[][] = [
  ["make", "merge-check"],
  ["go", "test", "-tags=integration", "-p", "1", "./...", "-count=1"],
];

const UI_CHECKS: string[][] = [
  ...GO_CHECKS,
  ["bash", "-lc", "cd ui && bun install && bun run check && bun run build"],
];

// ---------------------------------------------------------------------------
// The eight findings M11.3 must close. Written out verbatim, with file:line,
// because a finding summarized loosely is a finding fixed loosely — and
// because the last run proved that a bare path lock without an explicit list
// of permitted work produces either scope panic or scope creep.
// ---------------------------------------------------------------------------
const M113_FINDINGS = [
  "F1 (reviewer, ListPools contradicts its own proof): `internal/engine/pools.go:559-563` aborts on ONE pool-probe error and `internal/api/pools.go:133-137` then returns Unavailable for the whole RPC, while `proof/M11.3/README.md:29-34` and `docs/DEPLOY.md:1025-1030` say an unreachable source only NARROWS the answer. No ListPools test exercises a liveness-probe error (`unreachablePools` is used only for submit behavior, `internal/api/pools_test.go:332-369`). Make the code do what the documents claim — report the pools it could reach, mark the rest unavailable — and add the missing behavior test.",
  "F2 (reviewer, an unproven logging claim): pool plugin failures are silently swallowed at `internal/engine/pools.go:571-577` although `proof/M11.3/README.md:29-32` claims every unreachable source is logged, and `TestListPools_StillReportsAPoolWhosePluginReadFailed` (`internal/api/inventory_test.go:419-449`) wires no logger and asserts no log. Log the failed pool and source and ASSERT it, or delete the claim. A claim no test exercises is not evidence.",
  "F3 (reviewer, ListPlugins loses reachable work): `internal/api/service.go:1569-1577` returns on the FIRST namespace-catalog failure, discarding pool names already learned. Keep what was reachable, report what was not.",
  "F4 (probe, §1.2 payload leak — OWNER DECISION): the container plugin puts truncated stdout/stderr strings on the task output payload (`pkg/plugins/container/container.go`, the `\"stdout\": streams.stdoutHead.String()` / `\"stderr\": streams.stderrHead.String()` pair), so application bytes enter Temporal history. AGENTS.md §1.2 says bytes travel only as artifacts. REMOVE both fields from the payload. Output reaches a reader through the live log stream (M11.1) and the capture artifacts; `exit_code`, `image`, `image_pulled`, `container`, `timed_out` and `duration_ms` stay. Nothing is deployed, so this is a clean cut with no shim: update `docs/PLUGINS.md`, the plugin's own doc comment, every example and template that referenced `.output.stdout`/`.output.stderr`, and the tests. `TestIntegration_ContainerTaskLogBytesNeverEnterRunHistory` and `TestIntegration_AFailingContainerPutsNoOutputInRunHistory` must pass.",
  "F5 (probe, artifacts lost on failure): `TestContainerPlugin_NamedCapturesRemainAvailableOnFailure` and `TestExecuteTask_UploadsProducedArtifactsBeforeReturningPluginFailure` fail. A task that fails must still publish the captures it produced — the output of a failing container is exactly what an engineer needs. Upload produced artifacts BEFORE returning the plugin failure.",
  "F6 (probe, log batching defeated): `internal/orker` log batching collapses to near-per-line uploads under the byte-flush setting (probe ran with `LogFlushBytesVar=1` and batching was effectively disabled). M11.1 acceptance 4 requires a chatty task not to become a write storm. Fix the flush policy so BOTH bounds work together, and cover it with a test that fails on the per-line behavior.",
  "F7 (probe, a test that can pass on truncated output): `awaitArchivedEventText` in `cmd/orkctl/tasklogs_two_engine_integration_test.go` never checks `stream.Err()`, so a dropped stream reads as an accepted result. Check the error and fail on it. A helper that cannot distinguish complete from truncated makes every assertion above it worthless.",
  "F8 (probe, a preflight that proves nothing): `proof/M11.1/reproduce.sh` never touches the configured S3 bucket before the live run (no `ORK_S3_BUCKET` / HeadBucket / PutObject check), so a bucket misconfiguration surfaces as a confusing mid-run failure instead of a clean 77. Probe the bucket in preflight and exit 77 when it is absent or unwritable (AGENTS.md §6).",
].join("\n");

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
  // M11.3 is VERIFIED and no longer part of this stack. Its branch head
  // (2b2006b) carries the adversarial repairs that landed after verification:
  // the bounded-wave pool catalogue scan and the activity id that no longer
  // spells itself with a clock. M11.4 was cut from that branch BEFORE those
  // repairs, which is why its parent gate failed on a pool probe timing out
  // under load — restacking onto the repaired head is the first thing its
  // child does.
  {
    id: "M11.4",
    branch: "m11-4-run-page-logs-events",
    dir: "m11-4-run-page-logs-events",
    objective:
      "Rework the run detail page: task logs live in their own scoped panels, and the event stream becomes a visual, on-demand view.",
    acceptance: [
      "0. THIS SLICE IS ALREADY BUILT AND ALREADY APPROVED. Its features, browser tests and proof are committed on m11-4-run-page-logs-events, and two reviewers approved them with zero findings. It failed ONE thing: the parent's independent `go test -tags=integration -p 1 ./...` run, in cmd/orkctl, with `list pools: a pool could not be probed ... namespace=vision pool=gpu error=\"context deadline exceeded\"` — a live probe timing out on a loaded host, not an assertion failure. This branch was cut from m11-3-admin-surface BEFORE the two adversarial repairs that address exactly that (the bounded-wave pool catalogue scan, and the activity id that no longer spells itself with a clock), so STEP 0 is the restack, and the first thing to do after it is run the full integration suite and see whether the timeout is gone. If it is gone, do nothing else: re-prove, commit, stop. If it persists, fix it at the source — a probe deadline that cannot survive a loaded machine is too tight — WITHOUT weakening the distinction between a pool reported unavailable and a pool reported empty, which is the property the test exists to prove. Do not rewrite the UI work that already passed review.",
      "1. Task logs are isolated from events and scoped per task: each task keeps its own log panel (M10.5's TaskLogPane is the base), a task with retries scopes by attempt with a selector, and the container application's own stdout/stderr — streamed since M11.1 — is visible live, with stderr visually distinct from stdout (the design's in-band marker is the source of that distinction; render it, do not re-derive it).",
      "2. The event stream is shown on request: the page loads WITHOUT the event log occupying it, and an explicit control (the design picks expand vs tab vs URL state and says why) opens a redesigned event view. Closing it must not break live task updates — one socket keeps feeding the task-state reducer and the capped event buffer; only the event LIST rendering is on demand. State that split in a comment where the page wires the socket.",
      "3. The redesigned event view is legible at a glance: grouped or styled by kind (run lifecycle vs task lifecycle vs failures), timestamps and sequence aligned, failures visually distinct. Keep the existing dark zinc/Tailwind conventions; no new framework or component library.",
      "4. Live behaviors from v0.0.3 survive: a pane follows while the task runs, pauses following on scroll-up, resumes with from_sequence after a dropped socket, and a chatty task does not freeze the tab — bounded rendering, not hope.",
      "5. Svelte 5 runes and existing conventions; clients regenerated with `make tsgen` if the proto moved, never hand-edited. Loading, empty, error and stream-broken states all handled; nothing spins forever.",
      "6. Playwright coverage: scoped log view, attempt selection on a retried task, container output arriving live, opening and closing the event view while task state keeps updating. `cd ui && bun run check && bun run build` clean.",
      "7. proof/M11.4/ with README.md, Playwright output, before/after screenshots, reproduce.sh.",
    ].join("\n"),
    scopeLock: "ui/, proof/M11.4/",
    checks: UI_CHECKS,
  },
  {
    id: "M11.5",
    branch: "m11-5-version-scoped-editor",
    dir: "m11-5-version-scoped-editor",
    objective:
      "Let the orkflow detail page edit ANY version: select it, see its contents, apply the edit as a new version.",
    acceptance: [
      "1. The version list is the entry point to editing. Selecting a version shows THAT version's source — the page never loads head's contents when another version is selected, which is the exact bug this milestone exists to kill. A per-version action starts an edit based on the selected version, pre-filled with its source.",
      "2. Applying sends base = the selected version and expected_head = the head observed when the source was fetched (M11.2's contract). Success shows the new version and updates the markers: head moves to the new version, current stays where it was promoted to. Applying an unchanged document is a visible no-op, not a new version.",
      "3. A stale head is reported, never clobbered: the refusal names both versions and offers a way forward (reload the newest and keep the edit) instead of discarding the user's work.",
      "4. Loading, empty and error states for every version action; validation errors still come from the real ApplyOrkflow response before anything is stored.",
      "5. Playwright coverage of the owner's scenario verbatim: apply two versions, promote the OLDER one, open Edit on the CURRENT version and assert the editor shows current's contents (not head's), edit, apply, and assert the new version exists with head moved and current untouched.",
      "6. proof/M11.5/ with README.md, Playwright output, screenshots, reproduce.sh.",
    ].join("\n"),
    scopeLock: "ui/, proof/M11.5/",
    checks: UI_CHECKS,
  },
  {
    id: "M11.6",
    branch: "m11-6-admin-ui",
    dir: "m11-6-admin-ui",
    objective:
      "Add the Admin section to the UI: manage namespaces, see pool health and plugin inventory, and read system information.",
    acceptance: [
      "1. A new Admin area, reachable from the main navigation and deep-linkable, implementing the design document's Admin feature list on M11.3's RPCs: namespace create/update/delete with default_pool and allowed_pools, pool health with live Orker counts and per-pool plugins, plugin inventory, and system information.",
      "2. Writes are honest: an update sends the etag it read and a conflict is shown as one; a refused delete names the orkflow count it was refused with; the `default` namespace is visibly undeletable; forms validate locally before the server has to repeat itself.",
      "3. Partial answers are shown as partial. After M11.3, a pool whose plugin inventory could not be read completely still reports its counts and says what is unknown — render that distinction rather than flattening it. Nothing unavailable is ever drawn as zero.",
      "4. The Admin area and the namespace switcher coexist without confusion: the design states which views are global and which respect the selected namespace, and the UI says so where it matters.",
      "5. Loading, empty and error states everywhere.",
      "6. Playwright coverage against the live stack: create a namespace with a pool policy, update it, attempt a delete that is refused, read pool health and system info. `cd ui && bun run check && bun run build` clean.",
      "7. proof/M11.6/ with README.md, Playwright output, screenshots, reproduce.sh.",
    ].join("\n"),
    scopeLock: "ui/, proof/M11.6/",
    checks: UI_CHECKS,
  },
  {
    id: "M11.7",
    branch: "m11-7-proof-docs",
    dir: "m11-7-proof-docs",
    objective:
      "Write the v0.0.4 operating story down and prove the whole milestone in one command.",
    acceptance: [
      "1. Docs match the tree: docs/PLUGINS.md documents container log streaming AND the removed stdout/stderr payload fields (F4) with what replaced them; docs/DEPLOY.md documents the Admin surface, pool-health partiality and log volume; AGENTS.md gains a clause ONLY if a new load-bearing invariant was actually established — state which, or state none.",
      "2. proof/M10.1/naming-sweep.sh is re-run against this tree and passes, and the M8.5 docs-claims sweep agrees with the new docs.",
      "3. One consolidated proof runs the real stack in ONE command: submit an orkflow whose container task runs a chatty application and follow its application-level logs live (CLI and UI); promote an older version, edit the promoted one in the UI and apply a new version; in the Admin section create a namespace, set its pool policy and read pool health — with the reads served by a replica that started nothing.",
      "4. proof/M11.7/ with README.md, test.log, the live transcript, UI screenshots, and a reproduce.sh that performs all of it in ONE command. It exits 77 — not 0 — when a prerequisite is absent, decides its prerequisites BEFORE opening its transcript, never overwrites the previous transcript when it declines to run, and probes the artifact bucket the way F8 requires (AGENTS.md §6).",
      "5. `make merge-check` passes, the integration suite passes with -p 1, and `cd ui && bun run check && bun run build` is clean.",
    ].join("\n"),
    scopeLock: "ui/, docs/, AGENTS.md, proof/M11.7/, scripts/",
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
        name: Type.String({ description: "Short probe id." }),
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
  name: "v004-continue",
  description:
    "OrkStack v0.0.4 part two: close the Admin surface's eight outstanding findings, then the three UI slices and the consolidated proof, stacked on the repaired m11-2 branch and merged to main behind a merged-tree gate.",
  inputs: {
    repo_dir: Type.String({
      description: "Repository root (the main worktree).",
      default: "/Users/daniel/Code/orkstack",
    }),
    stack_base: Type.String({
      description:
        "Branch the stack builds on: the last VERIFIED slice of the previous run, including its adversarial repairs.",
      default: "m11-3-admin-surface",
    }),
    merge_target: Type.String({
      description: "Branch the finished stack merges into, gated on the merged tree.",
      default: "main",
    }),
    worktree_root: Type.String({
      default: "/Users/daniel/Code/orkstack-worktrees",
    }),
    max_rounds_per_slice: Type.Number({
      default: 6,
      description:
        "Raised from the previous run's 4: M11.3 exhausted its budget on a scope dispute and had nothing left when a real finding arrived.",
    }),
    max_repair_rounds: Type.Number({ default: 2 }),
    merge_to_target: Type.Boolean({ default: true }),
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
    const stackBase = String(ctx.inputs.stack_base);
    const mergeTarget = String(ctx.inputs.merge_target);
    const worktreeRoot = String(ctx.inputs.worktree_root);
    const maxRounds = Math.max(1, Number(ctx.inputs.max_rounds_per_slice) || 6);
    const maxRepairs = Math.max(0, Number(ctx.inputs.max_repair_rounds) || 0);
    const mergeToTarget = ctx.inputs.merge_to_target !== false;

    const designPath = join(repo, ".atomic", "v004-design.md");

    // -----------------------------------------------------------------------
    // Preflight. Same real commands as the first run, plus a check that the
    // base branch actually exists — this stack is meaningless without the
    // repairs that landed on it.
    // -----------------------------------------------------------------------
    const preflight = (await ctx.tool("preflight", { repo, stackBase }, async () => {
      const checks: Probe[] = [
        probe(["git", "status", "--porcelain"], repo, "clean-tree"),
        probe(["git", "rev-parse", "--verify", stackBase], repo, "stack-base-exists"),
        probe(["docker", "info", "--format", "{{.ServerVersion}}"], repo, "docker-daemon"),
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
        probe(["bash", "-lc", `test -f ${JSON.stringify(designPath)}`], repo, "design-doc-present"),
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
          report: `Preflight refused to start.\n\n${preflight.summary}\n\n${preflight.detail}`,
        },
      });
    }

    let previousRef = stackBase;
    let verified = 0;
    let stoppedAt = "";
    let lastGoodDir = repo;
    let lastGoodBranch = stackBase;
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
            // M11.3's worktree already exists from the previous run and its
            // branch was cut BEFORE the adversarial repairs landed on the base.
            // Restacking is therefore the normal path here, not the exception.
            const dirty = run(["git", "status", "--porcelain"], dir);
            const discarded = (dirty.stdout ?? "").trim();
            if (discarded !== "") {
              run(["git", "checkout", "--", "."], dir);
              run(["git", "clean", "-fd"], dir);
            }
            const restack = run(["git", "merge", "--no-edit", previousRef], dir, 5 * 60_000);
            if (restack.status !== 0) {
              const conflicts = run(["git", "diff", "--name-only", "--diff-filter=U"], dir);
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
                  `${previousRef} moved after ${slice.branch} was cut from it — adversarial`,
                  "repair rounds fixed real defects in the code this slice builds on. Run:",
                  "",
                  `    git merge --no-commit --no-ff ${previousRef}`,
                  "",
                  `and resolve the conflicts. They are in:\n${prepared.conflicts}`,
                  "",
                  "Resolve as a UNION OF BEHAVIORS, per AGENTS.md §7. Do NOT take either side",
                  "whole unless the file is untouched on the other. The base owns the repaired",
                  "primitives and the invariants they guarantee; this branch owns the surface",
                  "built on top. The union normally means calling the base's repaired primitive",
                  "from this branch's newer structure, NOT reinstating a call site the repair removed.",
                  "",
                  "Regenerate, never hand-merge, anything under gen/ or ui/src/lib/gen.",
                  "",
                  "The merged tree must COMPILE and pass every gate before you continue to the",
                  "acceptance items below. Say in your final message what each side contributed.",
                  "</keepContext>",
                  "",
                ].join("\n")
              : "",
            slice.acceptance,
            "",
            "GENERAL (applies to every item above):",
            `- The agreed design for this milestone is at ${designPath}. Read it before implementing. If you deviate, say so and justify it with evidence.`,
            "<keepContext>",
            "- ROUND 1 IMPLEMENTS. Do not spend your first round auditing the branch and reporting that it carries no work. Read what you need, then WRITE CODE and COMMIT. Every slice of the previous run wasted a round this way, and one of them ran out of budget because of it.",
            "- Behavior tests first (AGENTS.md §4): a test that fails for the right reason, then the implementation.",
            "- Integration tests must genuinely exercise real Temporal, MinIO and docker, and skip cleanly through the package's infraAbsent helper when absent — never a bare t.Skip, which lets a proof run count an absence as a pass.",
            `- Isolate every integration run you start: ORK_TEMPORAL_NAMESPACE=ork-${slice.dir} (AGENTS.md §6). Kill every engine and orker you start; anchor the patterns as '/bin/ork$' and '/bin/orker$'.`,
            "- Proto changes go through `make proto` and `make tsgen`. Never hand-edit gen/ or ui/src/lib/gen.",
            "- If you believe an acceptance item is unsatisfiable within your scope, say so explicitly with the item number and file:line evidence and leave it unimplemented. Do NOT widen your scope, and do NOT stop to ask — the parent judges that, and a stage that blocks on a question stalls the whole stack.",
            "- Never remove a failure mode by weakening a test, and never add one to a read that worked before your branch.",
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

      const verification = (await ctx.tool(
        `verify-${slice.dir}`,
        { dir, branch: slice.branch },
        async () => {
          const committed = probe(["git", "status", "--porcelain"], dir, "committed");
          let uncommitted = committed.stdout
            .split("\n")
            .filter((l) => l.trim() !== "" && !/\.atomic|research\//.test(l));

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
                `chore(proof): regenerate an earlier milestone's evidence on ${slice.branch}\n\nThe gates rewrote ${refreshed.join(", ")}. AGENTS.md §7: a proof taken on an\nearlier tree is a claim about this one that nothing has checked.`,
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
    // Adversarial pass over the finished stack.
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
            "You are a grumpy but fair distributed-systems and frontend reviewer with fresh context.",
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
            "The previous run's probes already found and FIXED these; check they stayed fixed:",
            "- Application bytes reaching Temporal history from the container plugin, on the success path or in a failure error.",
            "- stdout and stderr rendering identically in the log stream.",
            "- An expected-head token that is not an atomic compare-and-set, or a refused apply leaving an addressable version.",
            "- An orkflow with no head pointer becoming unappliable.",
            "- A docker or infrastructure test that passes by SKIPPING, and a proof script that prints PASS over failed publication.",
            "",
            "Where the new risk lives:",
            "- A pool-health read that still refuses the whole answer when one source is unreachable, or that reports unknown as zero.",
            "- A log pane that grows without bound, a UI that polls on a timer and calls it live, or an event view whose closure breaks live task updates.",
            "- An editor that loads head's contents when another version is selected.",
            "- Admin writes that skip validation, delete the default namespace, or drop an etag.",
            "- A Playwright test that asserts nothing, or screenshots that prove a page rendered but not that it worked.",
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
            "against the acceptance contract. Do not weaken, skip or delete tests. Do not add a failure",
            "mode to a read that worked before. Do not touch other subsystems beyond the finding's",
            "scope. Commit on this branch when done.",
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
    // Merge gate on the MERGED tree (AGENTS.md §7).
    // -----------------------------------------------------------------------
    let merged = false;
    let mergeSummary = "skipped";
    if (mergeToTarget && verified === SLICES.length && adversarialApproved) {
      const mergeResult = (await ctx.tool(
        "merge-and-gate",
        { branch: lastGoodBranch, into: mergeTarget },
        async () => {
          const steps: Probe[] = [];
          steps.push(probe(["git", "checkout", mergeTarget], repo, "checkout-target"));
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
                `Merge ${lastGoodBranch}: v0.0.4 — container logs live, version-scoped editing, Admin\n\nContainer application output now reaches readers through the live log stream and\ncapture artifacts only; the task payload no longer carries stdout/stderr bytes\n(AGENTS.md §1.2). Gated on the merged tree with make merge-check and the UI\nchecks (AGENTS.md §7).`,
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
        `This run continued a previous one that verified M11.1 and M11.2 and stopped at M11.3.`,
        `Slices verified in THIS run: ${verified}/${SLICES.length}`,
        stoppedAt ? `Stopped at: ${stoppedAt}` : "Stopped at: nothing, the stack completed",
        "",
        "Per-slice result:",
        ...sliceReports,
        "",
        `Adversarial probe results against ${lastGoodBranch} (real command exits):`,
        probeSummary,
        "",
        `Merge into ${mergeTarget}: ${merged ? "done, gated on the merged tree" : mergeSummary}`,
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
