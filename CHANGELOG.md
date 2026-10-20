# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
will follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from the
first release. OrkStack has not reached 1.0: until it does, a release may
include breaking changes, and each is marked **BREAKING**.

## [Unreleased]

### Added

- The proof runs sharded in CI: `proofrun integration -plan-only` writes one
  plan, `-plan … -shard K` runs one shard of it, and `proofrun merge` merges
  every part into one `proof.json` with the same eligibility rules a local
  proof uses. A merge-queue or pull-request run of `.github/workflows/proof.yml`
  is the merge gate; `proof` is its single required check (docs/PROOFS.md).
- A nightly tier and a pre-release gate: `make proof-nightly` runs the
  deferred scale and soak tests, `make proof-release` runs every gate with
  observability, and `release.yml` tags only after `proofrun verify -release`
  accepts the proof for the exact commit. `make release-tag` is the local
  equivalent.
- `make check`, the fast contributor path: build, vet, lint and the unit
  suites with `-short`, with no Docker.
- `scripts/github-merge-gate.sh` and `make github-merge-gate`: the
  one-command enablement of the required `proof` check, in queue-less mode by
  default; it refuses until the repository is public.
- `docs/PROOFS.md`, the reference for the tiers, the plan and sharding, the CI
  job graph, the artifacts and the recovery procedures.
- `proofrun flakes` and `make proof-flakes`, which derive per-test flake rates
  from the proof ledgers, and `scripts/proof-quarantine.txt`, a dated,
  owned list of known flakes. A quarantined test still runs; its failure is
  recorded as `quarantined-fail` and never as a pass.
- `docs/CLI.md`, the `orkctl` command and flag reference generated from the
  Cobra tree, and `make cli-reference` to regenerate it.
- Documentation drift checks in the unit suites: every reason constant, DSL
  key, DSL rule number, `orkctl` invocation, `make` target, repo-relative
  link, workflow job and `proofrun` flag the docs use must exist, and every
  workflow job, gate list and `proofrun` flag must be named in
  `docs/PROOFS.md`.
- `.github/dependabot.yml`, grouped weekly updates for Go modules, Docker,
  GitHub Actions and the UI's bun dependencies; every update is a pull request
  that runs the proof.

- The feature proof warns when a user-facing source change has no
  documentation change and when there is no CHANGELOG.md `[Unreleased]`
  entry; `.github/pull_request_template.md` asks for both.
- JSON Schemas for the authored documents, generated from the parsers:
  `docs/schemas/orkflow.v1.json`, `docs/schemas/automation.v1.json`, and
  `docs/schemas/orkstack.v1.json`, which selects one by `apiVersion` and
  `kind`. First-party `with:` blocks complete from each image's interface
  label (docs/DSL.md, "Editor support").
- The `http` activity's `body:` accepts a YAML mapping or list and sends it as
  compact JSON, with `Content-Type: application/json` unless `headers` sets a
  content type. A string body is still sent as written.
- An activity interface label's `type` may name several types joined by `|`
  (`string|object|array`).

### Changed

- A local full, release or nightly proof now writes one `local` part and
  merges it with `proofrun merge`, the same verdict path the CI proof job
  runs over many parts.
- The development object store is **SeaweedFS**, pinned by digest in
  `deploy/docker-compose.yml`, because MinIO stopped publishing images and
  moved to a closed product. SeaweedFS performs the atomic conditional
  `PutObject` the commit protocol needs; the S3 integration suite proves it.
  MinIO is no longer the recommended or the development store.
- The six scale and soak tests the user approved (the 4,000-task soak, the
  3,000-task by-reference definition, the 1,000-task overlay bytes, the
  item-ceiling soak and the two 10,000-item fan-outs) now run in the nightly
  tier and the pre-release gate instead of every merge proof. The merge gate
  runs reduced-ceiling versions through the same helpers, and a failure of a
  deferred test is a failing `main`.
- `make check` skips the unit suite's scale tests under `-short`, so the fast
  path measures 34 s warm on the development host (59 s at 2 CPUs).
- `AGENTS.md` §11 is rules again, with the procedure, artifacts and recovery
  steps in `docs/PROOFS.md`; a change that alters behaviour updates the
  reference doc that describes it, and a user-visible change adds its
  CHANGELOG entry, in the same change.

- The UI is redrawn as a drawing office (stage 1 of 4: the shell). A slim
  top bar replaces the sidebar, with two modes — **Build** (Runs, Orkflows,
  Apply) and **Operate** (the Admin pages) — and a row along the bottom edge
  states what each page is. **Ctrl+K** (**⌘K** on macOS) opens a command
  palette that jumps to any page, run or Orkflow and switches the namespace
  or the appearance; it never writes (docs/UI_CLI_PARITY.md, "Navigation and
  the command palette"). Light and dark are designed together and follow the
  system unless one is chosen; a browser that refuses storage now follows
  the system too instead of painting light. Statuses are stamps that differ
  by drawn mark as well as colour, every focus ring is one solid 2 px line,
  and the faces are the self-hosted Archivo Narrow and Spline Sans Mono
  (55 KB preloaded, down from 89 KB). Every colour, size, radius, shadow and
  motion is a token in `ui/src/tokens.css`; a test refuses a raw value
  anywhere else.
- The run page, the Runs page and compare are redrawn (stage 2 of 4). A run
  opens on its drawing: the DAG on a grid, each node drawn by its status and a
  link that selects its task, or — past 80 tasks — a task map with one cell
  per task and per fan-out item, readable and keyboard-operable at 4,000
  tasks (arrows, Home/End, PageUp/Down, F for the next failure, Enter to
  select). The DAG is drawn up to 80 tasks, on request up to 300, and never
  above, where a selected task's neighbourhood is drawn instead; the Orkflow
  page's DAG follows the same bound. Beside it, the versions of the run's
  Orkflow (this run's, the current one and the newest), and a failed run's
  reason in a dashed red outline with the commands that reproduce it. A run
  whose definition or versions cannot be read still shows everything else,
  with the reason. The Runs page updates the statuses it shows every 15 s
  without reordering, and marks a run whose status changed since this
  browser last saw it until it is opened (docs/UI_CLI_PARITY.md,
  "Changed-run marks"); the command palette lists the changed runs, selects
  a task by id on a run page, and opens the run's actions behind their
  confirmations. A closed popover no longer listens to the page, so a run
  page of 4,000 tasks becomes interactive about 0.4 s sooner.
- The dashboard, the Orkflows pages and Apply are redrawn (stage 3 of 4).
  The dashboard's numbers are one ruled line of counts — each status count
  beside its drawn mark, never a coloured figure — above its registers:
  running now, recent failures, pool health, stability and usage. The
  Orkflows list adds each Orkflow's definition digest and labels, and its
  only link is the name. An Orkflow's page opens with **Submit a run** and
  **View runs** in its header, its DAG beside the versions it has (the one
  shown, the current one and the newest), and a bottom row naming the
  version shown, its digest and whether it is current; the command palette
  on that page opens Submit a run…, switches tabs and jumps to a task's row.
  Apply puts Validate and Apply in its header and lists the examples as a
  ruled list. Validation errors and warnings are told apart by a filled or an
  outlined mark as well as by their words.
- The Operate pages are redrawn (stage 4 of 4), and every page of the UI is
  now drawn only in the shared tokens. Each Admin page opens with its own
  heading under the Operate tabs and says once, as a note, what it covers:
  the deployment, the namespace selected or every namespace (Cache, like
  Pools and Automation, is the selected namespace's). Lists are ruled
  registers, forms are ruled field blocks with each label above its control,
  and destructive actions are quiet red behind their confirmations. The
  overview's facts read like the bottom row, and its links are an index of
  every Operate page with what it governs and how far it reaches. A held
  namespace sits on a red tint with its reason in red; budgets, counts and
  digests are set as figures; failed webhook deliveries and refused audit
  records are tinted red; the audit trail says each principal was asserted.
  On a narrow screen the Operate tabs scroll inside their own row and bring
  the page you are on into view.
- The redrawn UI's last pass: a DAG node's second line now fits its node — a
  long error code keeps its whole name and drops the duration beside it (the
  task's card still has it), and anything longer ends in an ellipsis with the
  full line on hover; on a narrow screen the bottom row's second cell is as
  wide as its value, so a run's status reads in full; and the Runs and
  Orkflows pages label their page size the way every other form field is
  labelled.
- **BREAKING:** every Orkflow begins with `apiVersion: orkstack/v1` and
  `kind: Orkflow`. A document without them is refused at validate, apply and
  plan, naming the two lines to add. The header is not part of the definition:
  digests, derived versions and memo keys are unchanged, a version stored
  before this release still reads, runs and diffs, and applying it again with
  the two lines added as its first YAML lines, above or below its comments, is
  a no-op (docs/DSL.md, "The document header").
- **BREAKING:** `orkctl apply` decides what a file is by its `kind:`
  (`Orkflow` or `Automation`) and refuses a file with neither, before anything
  is sent.
- **BREAKING:** an automation document's schedule `input:` is a YAML mapping.
  A JSON object written as a string is refused, naming the YAML form: remove
  the quotes. `orkctl export` writes `input:` as a mapping.
- **BREAKING:** data written in an Orkflow — `inputs:`, `with:`,
  `await.schema` — becomes JSON through one strict mapping: a plain value YAML
  readers resolve differently (`yes`, `on`, `0755`, `1:30`, `1e3`, a date), a
  number JSON cannot hold exactly, and tags, anchors, aliases and merge keys
  are refused, naming the path (docs/JSON.md, "Data written in an authored
  document"). A document that compiled before either compiles to the same
  definition or is refused; none changes silently.
- A stored Orkflow version is shown with its header wherever OrkStack shows its
  YAML (`orkctl orkflow get --json`, `edit`, `diff`, the UI). Evidence bundles
  keep the source verbatim.
- The `custom-image` (0.2.0) and `each-fan-out` (0.3.0) examples write their
  fixed lists as jq `vars:` in YAML instead of jq literals.

### Fixed

- `examples/http-post.ork.yaml` built its JSON body by splicing the message
  into a string, so a quote or a backslash in it posted malformed JSON. It now
  writes the body as a YAML mapping (version 0.3.0).
- The `http` activity no longer appends a newline to a string body.
