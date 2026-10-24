# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
will follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from the
first release. OrkStack has not reached 1.0: until it does, a release may
include breaking changes, and each is marked **BREAKING**.

## [Unreleased]

### Fixed

- A local full proof records each nightly-listed test its base also lists as
  `deferred`, as a sharded proof's merger does. It built its ledger over the
  plan's whole expected set, which includes the deferred tests, so the first
  full proof on a tree whose base carried a nightly list failed with every
  deferred test reported as having no outcome.
- The merger accepts those `deferred` entries for exactly the tests the plan
  defers, and records each once from the plan. It refused every `deferred`
  entry a part wrote ("only the merger writes deferred entries") and then
  reported the test as an outcome the plan had deferred, so a local full proof
  with a non-empty nightly list could not be merge-eligible. A `deferred` entry
  for a test the plan runs is still a problem.

### Changed

- The proof is faster on one host, running the same tests: integration tests
  keep `PROOF_JOBS=4` (6 was measured: clean but for one 50 s engine-boot
  stall on a fresh bucket, so it stays an operator's choice), the activity contract
  fixture starts after the heavy tests rather than beside them and runs its
  four images in parallel, and the Playwright suite runs spec files
  `PROOF_UI_WORKERS=2` at a time (was 1); the two spec files that assert a render
  budget run alone, after the others. The browser runs in a network
  namespace of its own — Playwright's browser server in the image
  `ui/e2e/browser-image.txt` pins by digest, pulled by the proof when the host
  lacks it — so a container starting or stopping on the host no longer aborts
  a page load, and the navigation guard's ceiling is zero repeats; every spec
  file creates and removes a bucket of its own instead of sharing
  `orkstack-artifacts`, and names a Temporal namespace of its own run; the UI
  build is reused when the hash of its inputs matches the last build's, the
  browser server is started once per gate, a trace is kept only for a failing
  test in every run (`--trace on` records one by hand); the
  helper tools specs ran with `go run` are built once
  per run. `ORK_E2E_BROWSER=host` drives the host's own Chromium instead
  (docs/PROOFS.md, AGENTS.md §11.1).

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
- `make check`, the fast contributor path with no Docker: `make lint`,
  `go vet -tags=integration ./...` and the unit suites with `-short`, keeping
  the test cache.
- `scripts/github-merge-gate.sh` and `make github-merge-gate`: the
  one-command enablement of the required `proof` check, in queue-less mode by
  default; it refuses until the repository is public.
- The CI workflows are held by tests to fail closed: no job or step sets
  `continue-on-error`, every `proof.yml` upload runs `if: always()`, the
  verdict is judged per tier, `release.yml` refuses a tag off `main` and keeps
  no credential on its checkout, and `scripts/github-merge-gate.sh` treats an
  API error as an error rather than as an absent ruleset or variable;
  `--verify-landed` reads the proof on a landed merge commit's pull request
  head.
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
- `scripts/proof-section11-coverage.tsv` and `TestSection11Coverage`: every
  claim of `AGENTS.md` §11's required-coverage list is mapped to the tests or
  fixture check that prove it, and the test fails when a claim loses its
  merge-gate evidence, a named test is not declared with its claimed kind, or
  a known gap is not named in `docs/PROOFS.md`.
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

- The UI's finish pass (`ui/DESIGN.md`): a failed run tells one story — the
  revision cloud and the task's panel read one retry phrase off the failing
  task, and a task the run never started is stamped CANCELED with "never
  started" on its row, its DAG node and the task map's key; the run header
  is one line of facts with a **More** menu below 1024 px, so the drawing
  starts within 220 px on a phone; timing is a register beside the drawing,
  task rows share one run timeline and name their commands; Admin →
  Namespaces leads with its registry and **Create namespace** opens the form;
  "Orkflow" is capitalised on every surface; and every CLI equivalent quotes
  only the words a shell would read as syntax.
- A RUNNING task is drawn in a third accent, survey green, and breathes: a
  soft green DAG node whose ground fades every 2.4 s, its task-map cell and
  fan-out items with it, still under reduced motion (DESIGN.md's Three
  Accents Rule).
- The DAG is a layered drawing: an edge that skips layers runs in a lane of
  its own and never through a box, each layer is ordered to cross as few
  edges as it can, and every split or join has a track of its own. It reads
  left to right unless top to bottom clearly costs its reader less in the
  frame it is drawn in — a long chain, or a small DAG on a phone; `?dag=lr`
  and `?dag=tb` override it (DESIGN.md's Direction Rule).

- A local proof now fails fast. `scripts/proof.sh` runs lint, build, unit,
  vet and the UI type-check and unit suites first, one after another, and
  starts the long gates only if they pass; when a gate fails it stops the
  gates still running (`proofrun integration -fail-fast`, `SIGTERM` to the
  recorded pids), starts no other, and writes the failing verdict, recording
  each gate it stopped or never started. `PROOF_FAIL_FAST=0` runs every gate
  as before (docs/PROOFS.md §2.2).
- **BREAKING**: an unpinned Orkflow's derived version, and so its digest, now
  covers the whole source, header included, and applying a pinned version
  again with only its header moved or reformatted is refused like any other
  edit. Versions stored without the header are no longer shown with one.
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
- The reduced-ceiling copies of the moved soaks run alone as heavy tests,
  like the soaks they replace: they assert the same absolute GetRun latency
  and replay bounds, and one run four-wide beside ordinary work missed its
  p95 bound. A full proof plans them as heavy from
  `test/tools/proofrun/plan.go`; a test that merely waits on a deadline for
  progress stays light (docs/PROOFS.md, "The gates").
- `make check` skips the unit suite's scale tests under `-short`, so the fast
  path measures 34 s warm on the development host (59 s at 2 CPUs).
- `AGENTS.md` §11 is rules again, with the procedure, artifacts and recovery
  steps in `docs/PROOFS.md`; a change that alters behaviour updates the
  reference doc that describes it, and a user-visible change adds its
  CHANGELOG entry, in the same change.
- A proof's `proof.json` now carries `lists_changed`: every gate file the
  change edits, with the rows its nightly, quarantine and allowed-skips lists
  add or remove. It is printed by the proof README, the proof job's summary
  and `make merge-check PROOF=`. Nothing is refused for it; it makes a change
  to the gate's own files impossible to miss.
- A local full proof is judged by the same identity rules as CI: the Temporal
  it ran, whether the activity-image tag was its own, and whether
  `PROOF_GATES` chose the gates are read from its part, so
  `PROOF_TEMPORAL=shared`, an outside `ORK_TEST_IMAGE_TAG` and a
  `PROOF_GATES` override all make it ineligible locally too.

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
  plan, naming the two lines to add (docs/DSL.md, "The document header").
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
- An Orkflow version is shown exactly as stored wherever OrkStack shows its
  YAML (`orkctl orkflow get --json`, `edit`, `diff`, the UI). Evidence bundles
  keep the source verbatim.
- The `custom-image` (0.2.0) and `each-fan-out` (0.3.0) examples write their
  fixed lists as jq `vars:` in YAML instead of jq literals.

### Fixed

- The CI proof passes each part its run attempt, so a lost-runner rerun can
  produce an eligible proof, and runs the observability part whenever the plan
  requires it.
- The proof merger no longer trusts a shard ledger's problem lines: a UI, integration or unit entry that is not a pass fails unless a quarantine, an allowed skip or a runner-size excusal that the merger itself derives explains it; the plan must be complete and of the tier's nightly mode; unexpected parts, duplicate part attempts, missing plan or image identity and corrupt part records fail closed; `merge-check` refuses a proof whose quarantine check is over 30 days old.
- The CI proof's `build`, `ui-e2e`, `activity-fixture` and `observability` parts now read the plan artifact and pass `--plan`, so they record the plan hash the merger requires; `release.yml` sets an explicit committer identity on the tag.
- A UI runner-size miss is excused only on a CI `ui-e2e` part, only for a
  failure of the bound's own timing assertion, and only when the base's copy
  of `scripts/proof-host-speed.tsv` already carries the row; a skip, a missing
  outcome or any other failure of those specs fails the proof.
- `proofrun verify -release` and the nightly check behind
  `make merge-check NIGHTLY=` refuse a proof that broke an identity rule (a
  shared Temporal, an outside image tag, `PROOF_GATES`, a digest mismatch):
  `proof.json` records `eligible_but_for_tier`, and both require it. The CI
  `proof` job judges release and nightly proofs the same way.
- `make github-merge-gate`: `APPLY=1 QUEUE=1` works from scratch and `APPLY=1`
  switches back, with `MERGE_GATE_MODE` checked against the applied ruleset
  rather than the requested mode, and the variable now reaches `proof.yml`.
  The "a proof check has passed" precondition is a refusal that asks GitHub
  about a real commit, CODEOWNERS must name an account with write access, and
  `--verify-landed` reads the proof the pattern download wrote.
- The workflows pin every action to an exact release of its latest major, Go
  (1.27.3), Bun (1.4.3) and cosign (v2.6.5) to exact versions, and pass
  workflow inputs through `env:` instead of splicing them into a shell.
  `release.yml` validates the version in a first `validate` job, on every
  dispatch. `ci.yml`'s `check` runs on every push, as documented, and the
  nightly `flakes` job waits for that night's proof.
- The CI `proof` job records each part job's steps, check-run annotations and
  log, so the merger recognises a lost runner, and its summary carries the
  seven-day quarantine warnings.
- A feature selection names the slow list's tests it leaves out of the
  regression set `slow_omitted` (formerly `deferred_slow`), so "deferred"
  means only a nightly-listed test.
- `orkctl activity build`'s help named a `--no-push` flag that does not exist;
  it is `--push=false`.
- A task whose pool the run could not route (`INVALID_POOL`,
  `RESERVED_QUEUE`) is recorded `FAILED` with that code as its own error, and
  its stream carries a `TASK_FAILED` event, so the run's failure handlers and
  cancellation no longer record it a second time; before, it stayed `RUNNING`
  with no error while its run failed. Runs already in flight replay unchanged.
- The generations test no longer depends on the order the scheduler finishes
  a stage's parallel tasks in, so its `flat stages` and pin subtests stop
  flaking at 2 CPUs (0 failures in 500 runs); the pin subtest's quarantine
  entry is removed.
- `examples/http-post.ork.yaml` built its JSON body by splicing the message
  into a string, so a quote or a backslash in it posted malformed JSON. It now
  writes the body as a YAML mapping (version 0.3.0).
- The `http` activity no longer appends a newline to a string body.
- A run archive's newest-first listing could skip runs that shared the
  boundary instant of a page. Each page now includes the whole instant, so
  no run is missed between pages.
