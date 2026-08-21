// .atomic/workflows/v004-milestone.ts
//
// OrkStack v0.0.4: container application logs in the live stream, version-
// scoped orkflow editing, and an Admin section.
//
// The problem this exists for: three gaps users hit in v0.0.3.
//
// 1. Log streaming shows the plugin's own lifecycle lines (task started,
//    command invoked), but an application running INSIDE the container plugin
//    writes its stdout/stderr only to capture files — nothing appears in the
//    live log stream until the task ends and the artifacts exist. Engineers
//    watching a long-running container see silence. The presentation side is
//    also mixed together: task logs and the orkflow's event stream share one
//    surface, where they are two different audiences — an engineer debugging
//    one task wants ONLY that task's output, and the event stream is an
//    inspection view that should appear on request, not occupy the page.
//
// 2. Editing is head-pinned. `orkctl orkflow edit` and the UI's Edit tab both
//    fetch the newest version by design ("editing is lineage"), so after
//    promoting an older version, Edit shows head's contents instead of the
//    promoted one. The owner's decision overrides that design: ANY stored
//    version is a valid editing base; applying the edit creates a NEW version
//    from it.
//
// 3. There is no Admin surface. Namespaces, pools and plugins have RPCs, but
//    nothing manages them. The architect decides what the Admin section
//    contains, on the floor of: namespace management, pool health, plugin
//    inventory and system information.
//
// Shape: seven STACKED implementation slices, each in its own worktree
// branched from the previous VERIFIED one, each executed by the repo's
// milestone-finish child (deterministic ctx.tool gates + dual skeptical
// review + bounded repair). Then a fresh-context adversarial pass proposes
// falsification probes which THIS workflow executes as real commands, a
// bounded repair loop, and a merge gate that honours AGENTS.md §7
// (merge-check on the MERGED tree).
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
// Models. Roles per docs/MODEL_SELECTION.md; every id and thinking level below
// was checked against the configured catalog before launch (2026-08-20).
//   design (ARCHITECT) -> contract-setting architecture              => max
//   adversarial probe  -> high cost of error (silent contract break) => max
//   repair             -> triage/repair                              => high
//   report             -> final reporting                            => medium
// Slice children pin their own cross-provider chains; they are not overridden.
//
// The design stage is the one place a single call is worth `max` twice over:
// it fixes the log-stream wire format, the editing semantics and the Admin
// surface that all seven slices inherit, and a slice cannot cheaply revise a
// decision it was handed. So the level does NOT decay down the chain
// (docs/MODEL_SELECTION.md rule 6). claude-fable-5 leads and claude-opus-5
// backs it because the first fallback should preserve reasoning depth, not
// provider spread; the chain still spans all three subscriptions from its
// third entry. Same chain v0.0.3 ran to completion.
// ---------------------------------------------------------------------------
const DESIGN_MODEL = "anthropic/claude-fable-5:max";
const DESIGN_FALLBACKS = [
  "anthropic/claude-opus-5:max",
  "openai-codex/gpt-5.6-sol:max",
  "opencode-go/kimi-k3:max",
];

const ADVERSARY_MODEL = "openai-codex/gpt-5.6-sol:max";
const ADVERSARY_FALLBACKS = [
  "anthropic/claude-opus-5:max",
  "openai-codex/gpt-5.6-terra:max",
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
// Order is forced by dependency: the container log stream (M11.1), the
// editing semantics (M11.2) and the Admin surface (M11.3) are backend
// contracts; the three UI slices (M11.4–M11.6) each render one of them; the
// consolidated proof (M11.7) needs all six.
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
    id: "M11.1",
    branch: "m11-1-container-logs",
    dir: "m11-1-container-logs",
    objective:
      "Stream what an application running inside the container plugin writes to stdout/stderr into the task's live log stream, not only into capture artifacts after it exits.",
    acceptance: [
      "1. The container plugin feeds the application's stdout and stderr into plugin.Logger WHILE the container runs. Today pkg/plugins/container writes both streams to capture files and head buffers only (streamSet in container.go), so the live log stream carries the plugin's own lifecycle lines and nothing the application said. After this slice, a task running a long-lived or chatty application shows the application's own output through StreamTaskLogs before the container exits.",
      "2. stdout and stderr remain distinguishable in the log stream. The design document fixes the wire shape (prefix, marker or field on the chunk line); whatever it picks, a reader of `orkctl run logs` and of the UI log pane can tell the two streams apart, and the ordering is total and gap-free per (run, task, attempt) exactly as M10.5 requires.",
      "3. Nothing the container plugin already does regresses: capture_stdout_as / capture_stderr_as still produce complete artifacts, the output payload keeps exit_code, stdout/stderr heads, image, duration_ms and friends, and a container that writes nothing produces an empty stream, not a hung one.",
      "4. A chatty application cannot turn the sink into a per-line or per-byte write storm and cannot grow Orker memory without bound. Lines are framed across chunk boundaries without drops or duplicates — a line split between two docker reads lands exactly once, and a final line without a trailing newline is flushed when the stream ends. State the buffering policy in a comment and cover it with tests.",
      "5. Log bytes still never enter a Temporal payload or the event stream (AGENTS.md §1.4). M10.5's history-absence test gains container coverage.",
      "6. Behavior tests: a docker-gated integration test (through the package's infraAbsent helper, never a bare t.Skip) asserts application lines arrive through StreamTaskLogs BEFORE the run reaches a terminal state, from a second replica if the harness has one; a docker-free unit test covers line framing, partial lines and the stdout/stderr marker.",
      "7. `orkctl run logs <run> --task <id> -f` shows the container application's output live; the existing --attempt/--from-seq/--from-attempt resume semantics are unchanged.",
      "8. proof/M11.1/ with README.md, test.log, a live-follow transcript whose timestamps show container output arriving mid-run, and a reproduce.sh that regenerates it (exit 77 when docker or the cluster is absent, AGENTS.md §6).",
    ].join("\n"),
    scopeLock:
      "pkg/plugins/container/, pkg/plugin/, internal/orker/, cmd/orkctl/ (tests only), docs/PLUGINS.md, proof/M11.1/",
    checks: GO_CHECKS,
  },
  {
    id: "M11.2",
    branch: "m11-2-version-scoped-editing",
    dir: "m11-2-version-scoped-editing",
    objective:
      "Make any stored orkflow version an editing base: read it, edit it, and apply the edit as a new version — in the API contract and the CLI.",
    acceptance: [
      "1. The owner's decision overrides the old design comment in cmd/orkctl/orkflow_versions.go ('The HEAD version, not the current one: editing is lineage'). `orkctl orkflow edit <name> --version <v>` fetches version v's source_yaml and applies the saved edit with BaseVersion=v. Without --version the command keeps today's behavior (head). Update the help text and the comment to state the new rule: ANY stored version is a valid base, and editing an old one is how a user forks forward from it.",
      "2. Applying an edit whose BaseVersion is NOT the head succeeds and creates the new head version. head moves to it; the `current` pointer is untouched unless the user promotes. Prove with a behavior test over the stored versions, not over the client's claim.",
      "3. Optimistic concurrency still bites on the chosen base: if another apply lands between the fetch and the apply, the apply is refused naming both versions, whatever base was chosen — head or not. The refusal path keeps the rejected edit on disk the way today's edit does.",
      "4. The read path the UI will use in M11.5 is settled and proven here: GetOrkflow(name, version, namespace) returns any version's source_yaml, and ListOrkflowVersions marks head and current. If anything the UI needs is missing from those responses, add it in this slice with `make proto` + `make tsgen`; never hand-edit gen/.",
      "5. Golden CLI tests cover the --version path in human and --json output, following cmd/orkctl/golden_test.go, including the refusal when the base moved mid-edit.",
      "6. proof/M11.2/ with README.md, test.log, and a transcript of the owner's exact scenario: create two new versions, promote the older one (head stays, current moves), then edit the CURRENT version — not head — and apply it as a new version. reproduce.sh regenerates it.",
    ].join("\n"),
    scopeLock:
      "proto/ (only if a field is missing), internal/api/, pkg/artifacts/, cmd/orkctl/, docs/, proof/M11.2/",
    checks: GO_CHECKS,
  },
  {
    id: "M11.3",
    branch: "m11-3-admin-surface",
    dir: "m11-3-admin-surface",
    objective:
      "Complete the API surface an Admin section needs, adding what the design document decides is missing and settling whether admin operations get CLI parity.",
    acceptance: [
      "1. The design document decides the Admin feature list; this slice implements its backend. Floor, all already partly present and to be verified, not assumed: namespace CRUD with default_pool and allowed_pools (NamespaceService), pool health with live Orker counts and their plugins (ListPools), plugin inventory (ListPlugins).",
      "2. System information an operator needs is exposed by an RPC the design names: at minimum engine version/build identity, deployment name and build id, whether Worker Deployment Versioning is active, the artifact backend in use, and the Temporal address. Values that come from process configuration are labeled configuration-derived; nothing is fabricated to look observed.",
      "3. New or changed RPCs go proto-first: edit proto/, run `make proto` and `make tsgen`, never hand-edit gen/ or ui/src/lib/gen (AGENTS.md §1.1).",
      "4. Every admin read obeys AGENTS.md §1.7: live state comes from Temporal (pool liveness from task-queue poller information), stored state from the archive/store; nothing is answered from process memory behind a wired reader, so any replica returns the same admin view.",
      "5. Write safety holds on the admin paths: the namespace slug rule is validated on every write (never sanitized), the `default` namespace cannot be deleted or made to vanish, UpdateNamespace honours the etag semantics the proto documents, and a delete refuses while the namespace holds orkflows, saying how many.",
      "6. The design states whether admin gets a CLI surface (for example `orkctl admin info` / `orkctl namespace ...` extensions). If yes, implement it with golden tests; if no, say why in the design document and in this proof's README.",
      "7. proof/M11.3/ with README.md, test.log, a transcript exercising the new surface from a replica that started nothing, and reproduce.sh.",
    ].join("\n"),
    scopeLock:
      "proto/, gen/ (regenerated only), internal/api/, pkg/artifacts/, cmd/orkctl/, docs/, proof/M11.3/",
    checks: GO_CHECKS,
  },
  {
    id: "M11.4",
    branch: "m11-4-run-page-logs-events",
    dir: "m11-4-run-page-logs-events",
    objective:
      "Rework the run detail page: task logs live in their own scoped panels, and the event stream becomes a visual, on-demand view.",
    acceptance: [
      "1. Task logs are isolated from events and scoped per task: each task keeps its own log panel (M10.5's TaskLogPane is the base), a task with retries scopes by attempt with a selector, and the container application's own stdout/stderr — streamed since M11.1 — is visible live, with the stdout/stderr distinction M11.1's wire shape provides.",
      "2. The event stream is shown on request: the page loads WITHOUT the event log occupying it, and an explicit control (expand, tab or URL state — the design picks one and says why) opens a redesigned event view. Closing it does not break live task updates: the socket still drives task state; only the event LIST rendering is on demand. State that split in a comment where the page wires the socket.",
      "3. The redesigned event view is legible at a glance: events grouped or styled by kind (run lifecycle vs task lifecycle vs failures), timestamps and sequence aligned, failures visually distinct. Keep the mono-font, dark, zinc-based conventions the UI already uses; no new framework or component library.",
      "4. Live behaviors from v0.0.3 survive: a log pane follows while the task runs, pauses following on scroll-up, resumes with from_sequence after a dropped socket, and a chatty task does not freeze the tab — bounded rendering (windowing or a capped buffer), not hope.",
      "5. Svelte 5 runes and the existing Tailwind conventions; clients regenerated with `make tsgen` if the proto moved, never hand-edited. Every state — loading, empty, error, stream-broken — is still handled; nothing spins forever.",
      "6. Playwright coverage (the harness v0.0.3's M10.7/M10.8 proofs used): scoped log view, attempt selection on a retried task, container output arriving live, opening and closing the event view while task state keeps updating. `cd ui && bun run check && bun run build` is clean.",
      "7. proof/M11.4/ with README.md, Playwright output, before/after screenshots of the run page, and reproduce.sh.",
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
      "1. The version list becomes the entry point to editing. Selecting a version shows THAT version's source — the page never loads head's contents when another version is selected, which is the exact bug the owner reported. A per-version action starts an edit based on the selected version, pre-filled with its source.",
      "2. Applying uses BaseVersion = the selected version (M11.2's contract). Success shows the new version and updates the markers: head moves to the new version, current stays where it was promoted to. Applying an unchanged document is a visible no-op, not a new version.",
      "3. A stale base is reported, never clobbered: if head moved from the chosen base while the edit was open, the apply refusal names both versions and offers a way forward (reload the newest and keep the edit) instead of dropping the user's work.",
      "4. Loading, empty and error states for every version action; the editor's validation errors still come from the real ApplyOrkflow response before anything is stored.",
      "5. Playwright coverage of the owner's scenario verbatim: apply two versions, promote the older one, open Edit on the CURRENT version and assert the editor shows current's contents (not head's), edit, apply, and assert the new version exists with head moved and current untouched.",
      "6. proof/M11.5/ with README.md, Playwright output, screenshots of the version-scoped editor, and reproduce.sh.",
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
      "1. A new Admin area, reachable from the main navigation and deep-linkable (its own route or routes — the design fixes the information architecture). It implements the design document's Admin feature list using M11.3's RPCs, at minimum: namespace create/update/delete with default_pool and allowed_pools, pool health with live Orker counts and per-pool plugins, plugin inventory, and system information.",
      "2. Writes are honest: an update sends the etag it read and a conflict is shown as one, a delete that is refused names the orkflow count it was refused with, the `default` namespace is visibly undeletable, and every form validates locally BEFORE the server has to repeat itself.",
      "3. The Admin area and the namespace switcher coexist without confusion: the design states which Admin views are global and which respect the selected namespace, and the UI says so where it matters (for example, 'all namespaces' on a global view).",
      "4. Loading, empty and error states everywhere; pool health that cannot be read is shown as unavailable, never as zero Orkers.",
      "5. Playwright coverage against the live stack: create a namespace with a pool policy, update it, attempt a delete that is refused, read pool health and system info. `cd ui && bun run check && bun run build` is clean.",
      "6. proof/M11.6/ with README.md, Playwright output, screenshots of the Admin section, and reproduce.sh.",
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
      "1. Docs match the tree: docs/PLUGINS.md documents container log streaming (what reaches the live stream, what stays an artifact); docs/DEPLOY.md documents the Admin surface and anything operational about log volume; docs/DSL.md changes only if the DSL changed. AGENTS.md gains a clause ONLY if the architect identified a new load-bearing invariant — state in the proof README which, if any.",
      "2. M10.1's naming gate (proof/M10.1/naming-sweep.sh) is re-run against this tree and passes; the M8.5 docs-claims sweep agrees with the new docs.",
      "3. One consolidated proof runs the real stack in ONE command: submit an orkflow whose container task runs a chatty application and follow its application-level logs live (CLI and UI); promote an older version, edit the promoted one in the UI and apply the new version; in the Admin section create a namespace, set its pool policy and read pool health — with the reads served by a replica that started nothing.",
      "4. proof/M11.7/ with README.md, test.log, the live transcript, UI screenshots, and a reproduce.sh that performs all of it in ONE command. It exits 77 — not 0 — when a prerequisite is absent, decides its prerequisites BEFORE opening its transcript, and never overwrites the previous transcript when it declines to run (AGENTS.md §6).",
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
        name: Type.String({
          description: "Short probe id, e.g. edit-current-shows-current-not-head.",
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
  name: "v004-milestone",
  description:
    "OrkStack v0.0.4: container application logs in the live stream with scoped log panels and on-demand events, version-scoped orkflow editing, and an Admin section, as stacked verified slices with adversarial probes and a merge gate.",
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
    // M11.1's proof needs the docker daemon itself (container plugin), not
    // just the compose services, so `docker info` is a blocking check too.
    // -----------------------------------------------------------------------
    const preflight = (await ctx.tool("preflight", { repo }, async () => {
      const checks: Probe[] = [
        probe(["git", "status", "--porcelain"], repo, "clean-tree"),
        probe(["git", "rev-parse", "--abbrev-ref", "HEAD"], repo, "branch"),
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
    // Design. One pass that fixes the shape of all seven slices and writes it
    // to a file the slice children READ, so the contract is decided once
    // instead of drifting seven times. The log wire shape, the editing
    // semantics and the Admin surface each touch several slices, so none of
    // them can be re-litigated per slice.
    // -----------------------------------------------------------------------
    const designPath = join(repo, ".atomic", "v004-design.md");
    const design = await ctx.task("design", {
      context: "fresh",
      model: DESIGN_MODEL,
      fallbackModels: DESIGN_FALLBACKS,
      prompt: [
        "<keepContext>",
        "You are the ARCHITECT for OrkStack v0.0.4. You are designing, NOT implementing.",
        "Produce one design document and write it to:",
        designPath,
        "",
        "Decisions already made by the repository owner. They are INPUTS, not open questions:",
        "- An application running in the container plugin must show its stdout/stderr in the live log stream — UI and CLI — while it runs, not only as artifacts afterwards.",
        "- Task logs and the orkflow event stream are separate audiences. Logs are scoped per task (and per attempt). The event stream is an inspection view shown ON REQUEST, redesigned to be visually legible.",
        "- Editing is version-scoped now: ANY stored version can be selected, its contents shown, and an edit applied as a NEW version from it. This OVERRIDES the old head-pinned design comment in cmd/orkctl/orkflow_versions.go. head and current keep their meanings.",
        "- The UI gains an Admin section. YOU decide its feature list, on the floor of: namespace management, Orker pool health, plugin inventory, system information. You also decide whether admin operations get CLI parity.",
        "",
        "Constraints that must hold in every recommendation:",
        "- Temporal workflow code must stay deterministic (AGENTS.md §1.3). No clock, network, randomness or IO inside workflow functions.",
        "- Proto is the source of truth for cross-process types; gen/ and ui/src/lib/gen are never hand-edited (AGENTS.md §1.1).",
        "- Artifacts and log bytes stay out of Temporal payloads and the event stream (AGENTS.md §1.4). Logs travel as M10.5's numbered chunks in the artifact store.",
        "- Temporal is the read model for live runs and the archive for finished ones; never answer a read from process memory (AGENTS.md §1.7).",
        "- `pool` is the user-facing word for where a task runs; `queue` and Temporal namespaces never surface in proto, the DSL, orkctl or the UI (AGENTS.md §1.9).",
        "- The repo pins go.temporal.io/sdk v1.47.0 against server 1.31.2. Only recommend APIs that exist in THAT SDK version — verify against the module cache, do not recall from memory.",
        "</keepContext>",
        "",
        "Repository: " + repo,
        "",
        "Design the following seven stacked slices as one coherent whole:",
        ...SLICES.map((s) => `\n## ${s.id} — ${s.objective}\n${s.acceptance}`),
        "",
        "Research first, with evidence:",
        "1. Read pkg/plugins/container/{container,docker,config}.go, pkg/plugin/plugin.go, internal/orker/activity.go (the log sink path), internal/api/logs.go, proto/orkstack/v1/{service,run,orkflow,namespace}.proto, internal/api/{orkflow_versions,namespaces,pools,stats}.go, cmd/orkctl/{logs,watch,orkflow_versions}.go, and ui/src/routes/runs/[id]/+page.svelte, ui/src/routes/orkflows/[name]/+page.svelte, ui/src/lib/components/TaskLogPane.svelte and OrkflowEditor.svelte.",
        "2. Trace exactly how the shell plugin's stdout/stderr reaches the chunk sink today, and name the seam where the container plugin diverges (its streamSet writes files and head buffers, never the logger).",
        "3. Read the Playwright setup v0.0.3's proofs used (proof/M10.7, proof/M10.8, ui/ test config) so the UI slices design against the harness that exists.",
        "",
        "Then decide and write down, concretely:",
        "- The log wire shape for container output: how stdout and stderr are distinguished on a chunk line (prefix/marker), how partial lines and carriage returns are framed, and why the choice keeps M10.5's ordering and resume intact.",
        "- The backpressure story for a chatty container: what bounds memory and chunk rate, and what a reader sees when output is truncated.",
        "- The run page's information architecture: scoped log panels per task and attempt, and the exact mechanism that makes the event view on-demand WITHOUT breaking the socket-driven task state updates.",
        "- The event view's visual design within the existing zinc/dark Tailwind conventions.",
        "- Version-scoped editing end to end: CLI --version semantics, BaseVersion behavior on apply from a non-head base, the concurrency-refusal UX, and the exact fields the UI reads.",
        "- The Admin section's feature list — your call, justified briefly — its routes, which views are global vs namespace-scoped, and whether there is CLI parity.",
        "- The system-information RPC: exact fields, and which are configuration-derived vs observed.",
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
            "- Integration tests must genuinely exercise real Temporal and MinIO (and docker, for the container plugin), and skip cleanly through the package's infraAbsent helper when absent. Never fake a pass, and never weaken a test to make it green.",
            `- Isolate every integration run you start: ORK_TEMPORAL_NAMESPACE=ork-${slice.dir} (AGENTS.md §6). Kill every engine and orker you start; anchor the patterns as '/bin/ork$' and '/bin/orker$'.`,
            "- Proto changes go through `make proto` and `make tsgen`. Never hand-edit anything under gen/ or ui/src/lib/gen.",
            "- DONE means proven: the proof directory must be regenerable by its own reproduce.sh, and its README must state what was proven and how. A skipped prerequisite is exit 77, never a PASS (AGENTS.md §6).",
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
          // it and carry on; stalling a seven-slice stack on a text file the
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
    if (verified > 0) {
      for (let round = 1; round <= maxRepairs + 1; round += 1) {
        const adversary = await ctx.task(`adversary-${round}`, {
          context: "fresh",
          schema: probePlan,
          model: ADVERSARY_MODEL,
          fallbackModels: ADVERSARY_FALLBACKS,
          prompt: [
            "<keepContext>",
            "You are a grumpy but fair distributed-systems and frontend reviewer with fresh context.",
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
            "- Container logs CALLED live but appearing only after the container exits; or a line split across two reads landing twice, or never; or stderr indistinguishable from stdout.",
            "- A chatty container that turns the sink into a per-line write storm, or that buffers without bound; truncation that is silent instead of visible.",
            "- The container streaming change breaking the capture artifacts (missing files, truncated heads) or the output payload fields.",
            "- Log bytes reaching Temporal history or the event stream despite the claim that they do not.",
            "- Edit with --version (or a selected version in the UI) that STILL loads head's contents — the exact bug this milestone exists to kill; or BaseVersion ignored on apply, so the edit lands on the wrong lineage.",
            "- A concurrency refusal that fires for a non-head base when it should not, or that does not fire when head moves under the chosen base mid-edit.",
            "- The event view still rendered on page load, or closing it breaking the socket-driven task state updates.",
            "- Admin writes that skip the slug validation, delete or hide the `default` namespace, bypass the etag on UpdateNamespace, or report zero Orkers when pool health could not be read.",
            "- A UI that polls on a timer and calls it live, a log pane that grows without bound, or a view that spins forever on a failed request.",
            "- A test that passed because it SKIPPED (missing docker or cluster proves nothing), or a proof artifact left over from an earlier run.",
            "- Temporal vocabulary leaking into proto, the DSL, orkctl flags or the UI.",
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
                `Merge ${lastGoodBranch}: v0.0.4 — container logs live, version-scoped editing, Admin\n\nGated on the merged tree with make merge-check and the UI checks (AGENTS.md §7).`,
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
