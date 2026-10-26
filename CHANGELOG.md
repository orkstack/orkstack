# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
will follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from the
first release. OrkStack has not reached 1.0: until it does, a release may
include breaking changes, and each is marked **BREAKING**.

## [Unreleased]

### Fixed

- `proofrun clean`, and so every proof's and `scripts/integration.sh`'s first
  cleanup and `make proof-clean REMOVE=1`, now removes a dead process's work
  root that holds root-owned files. An activity container runs as root and
  writes them into its bind-mounted work root, which an unprivileged process
  cannot unlink, so each such root was only listed (462 on the proof host).
  When the host's removal is refused for permission, the root is emptied
  through a throwaway container of the first-party images' pinned alpine base:
  `--rm`, no network, the root as its only mount, no privilege or host
  namespace, a two-minute limit after which the container is force-removed,
  and only for a root this run found whose process is gone. It is logged
  `removed through a container`; a failure is listed and not retried.
- A proof's integration phase removes each passing test process's bucket
  while the phase runs, two at a time, off the process's slot, instead of
  keeping every one until the phase's end. A bucket holds store volumes until
  it is removed, and a phase that kept its 343 passing buckets exhausted the
  development store's 290 (`No writable volumes`), failing the merge gate on
  writes no test made wrong. A slot waits to start its next process while
  four removals are pending, so the buckets alive at once stay within the
  slots, that backlog and the removals in flight.
- Every local proof and every `scripts/integration.sh` run first removes what
  proofs and test processes that are gone left on the host (`proofrun clean
  -remove`), before its own directory exists, and `setup.log` opens with
  `cleanup: removed N buckets, N work roots, N containers of dead proofs`. A
  proof stopped by `make proof-stop` or a signal left every passing job's
  bucket, each holding store volumes, and the next proof on the host ran out
  of them. It reads the proof and run directories of every worktree, removes a
  bucket, work root or activity container only when the process its name
  records is gone, and never one a still-running proof recorded; a thing
  whose owner it cannot tell is listed and left.
- `make proof-clean` is the same cleanup by hand, and now finds buckets at
  all: the development store (SeaweedFS 4.48, auth off) lists none, so the
  target listed and removed nothing. It reads the names proofs recorded
  instead, and it also lists the activity containers of dead test processes.
- `proofrun overhead` counts a concurrent setup group once, through its
  `setup` row, and puts the site gate among the fast gates: it summed the
  group's members as if serial and reported 88.3 minutes for a proof whose wall
  was 87.5. A lane's utilisation is never above 100%; it read 115% for the solo
  projects.
- The Playwright ledger keeps each invocation's worker lanes apart, so the time
  between the parallel and the solo invocation (the second Playwright start
  and its global setup) is no longer charged to the solo invocation's first
  spec file's wall, which the UI costs and plan read.
- A proof's split Playwright invocations pass their spec files before
  `--project`, which took every word after it as a project name: a feature
  proof naming specs, and every CI `ui-e2e` part, ran with more than one UI
  worker refused its files as unknown projects. They also pass
  `--pass-with-no-tests`, since a selection may hold no file of one project.
- An Orker whose identity lock cannot be taken at all, on a filesystem
  without file locks, fails boot with `WORKDIR_FAILED` naming the lock and the
  error. It reported another live Orker holding its identity, which sent the
  operator looking for a process that did not exist.
- `orkctl task run` calls the directory it runs a task in its work directory
  in every message; three still named it a session directory.
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
- An engine boot that stalled 50 s behind object-store bucket churn at a
  higher `PROOF_JOBS` left the integration tests' 60 s boot waits no margin on
  a merge gate that is never rerun. Every wait for a process a test starts
  now allows a boot 180 s (the test binary's own bucket create, 120 s), ends
  at once when the process exits, and records a boot over 20 s as a `WARN`
  line with its duration, which `proofrun overhead` lists, instead of
  absorbing it. The engine's boot log lines for the Orkflow store and the
  namespace registry carry a `list_took` attribute, so a slow boot names the
  read that stalled.
- A test process removed its bucket on its slot as it ended, and that
  removal's churn was the stall above. A process whose tests passed now names
  its bucket in a file (`ORK_TEST_BUCKET_LEFT_FILE`), and `proofrun
  integration` removes those buckets once, after the integration phase, four
  at a time; a failing process still keeps its bucket, and a removal that
  fails is logged and never fails the proof. A bucket is removed with one
  request when the store deletes a non-empty bucket, and is emptied object by
  object only when the store refuses, here and in `test/tools/bucket-ensure`.

### Added

- `proofrun overhead` and `make proof-overhead FROM=proof/<name>`, which read
  an existing proof directory, run nothing and change nothing, and report as
  Markdown where its wall-clock went: each gate's share of the serial
  critical path, each integration process's wall against its tests' ledger
  seconds (the per-process overhead, with its distribution and its cost in
  wall minutes at the proof's `PROOF_JOBS`), the heavy and ordinary phases'
  walls and slot utilisation, the two Playwright invocations' walls and lane
  packing, any slow-boot warnings, and the top three time sinks. A partial
  proof is reported as partial, never a crash.
- `engine.pin_creation_timeout` (`ORK_PIN_CREATION_TIMEOUT`, `5m`): how long a
  pin waits for some pin host of its pool to have a free slot before the task
  that asked for it fails `PIN_UNAVAILABLE`. It was a fixed five minutes. A
  duration of whole seconds, 1 second to 24 hours, refused at boot otherwise.
  A run carries the value in force when it was submitted, so a change moves
  only new runs, and a child run waits as long as the run that started it. The
  failure message says what ended the wait: "within" the timeout when no pin
  host picked the pin up in time, or that every pin host refused it because it
  is cordoned. A pool whose pin hosts are all cordoned still fails a pin after
  about five minutes, whatever the setting says. The engine
  asks a `pin-creation-timeout` version where a run first opens a pin: a run
  recorded before it keeps the five minutes it was recorded with
  (`docs/DEPLOY.md` §6).

### Changed

- A local proof and `scripts/integration.sh` run six integration test
  processes at a time by default (`PROOF_JOBS`, was 4). Six had failed once on
  an engine boot that stalled 50 s behind the object store's bucket churn;
  with slow boots tolerated and passing jobs' buckets removed while the phase
  runs, two full proofs at six ran clean on the proof host (79.0 and 71.2 min).
- The run page at the 4,000-task default ceiling moves to the nightly tier
  and the pre-release gate, through a new Playwright twin of the nightly list,
  `scripts/proof-nightly-specs.txt` (approved 2026-10-26): about 7.5 minutes of
  every full proof's Playwright solo lane. The merge gate keeps the same
  measurement in `ui/e2e/run-page-reduced-ceiling.spec.ts`, with
  `engine.orkflow_max_tasks` lowered to 1,000. A full proof's UI plan defers
  the spec files both the tree's and the base's list carry, and its Playwright
  ledger records their tests as `deferred`; the nightly tier, locally and in
  CI, runs the listed specs and needs its `ui-e2e` gate; `merge-check` asks for
  a nightly proof while either list is non-empty.
- A feature proof holds evidence that is there only because it reaches a
  changed helper, type or variable to the slow list, as it holds the
  regression set: on a shared test helper that is about 35 minutes. A test
  whose own source changed and every `--tests` name always run; when every
  test such a change reaches in a package is slow, the cheapest stays; a
  changed `TestMain` still runs the whole package. The merge proof runs every
  test as before.
- The merge contract names a CI full proof as merge evidence on the local
  path: a `validate/full/<name>` run's `proof-<run>-a<n>` artifact, downloaded
  and passed to `make merge-check PROOF=`, gates a fast-forward when it is a
  full, passing, merge-eligible proof of exactly the candidate's tree over
  `main`'s tip; a GitHub nightly proof of `main`'s tip, downloaded the same
  way, is the `NIGHTLY=` evidence; and `main` is pushed to `origin` after
  every merge (AGENTS.md §11.2, §11.3; docs/PROOFS.md §6, §10). The checks
  `merge-check` applies are unchanged.
- The Playwright suite runs less of itself alone, measuring the same things.
  The fan-out's render-budget test (E4, "the run page and the item list at the
  gated size are interactive within 5 s") moved from `each-fan-out.spec.ts`
  to `ui/e2e/each-fan-out-ceiling.spec.ts`, with ports, a bucket, a Temporal
  namespace and an Orker work root of its own; that file is now the one the
  `chromium-solo-each-fan-out` project runs alone, and the fan-out's six
  functional tests run in the parallel `chromium` project. The new file costs
  the UI planner's unmeasured 160 s until a full proof measures it. Its
  `scripts/proof-host-speed.tsv` row moved with it, and because a row excuses
  only once the base carries it identically, the moved row excuses nothing on
  the change that moves it: a CI `ui-e2e` part on a runner below 16 CPUs that
  misses the 5 s budget fails that proof, until `main` carries the row.
- `ui/e2e/run-page-ceiling.spec.ts` does less work of its own around what it
  measures. It polls GetRun every 1–2 s instead of every 250 ms
  while it waits for the run (each GetRun re-reads the run's whole history),
  and runs `test/tools/archive-sized-run` as a binary the suite builds once
  (`ui/e2e/global-setup.ts`, `scripts/proof.sh`, `scripts/proof-part.sh`)
  rather than through `go run`. No budget, size or timed open changed.
- A proof's setup — its own Temporal, the activity images, the binaries and
  the integration test binaries — runs as one concurrent group once the fast
  gates pass, instead of one step after another, with each member still its
  own `gates.tsv` row and a `setup` row for the group's wall; a failure of any
  member stops the others. `proofrun compile -out DIR` builds the integration
  test binaries ahead, and `proofrun integration -bin DIR` adopts them only
  when their manifest names the same working tree, packages, binary SHA-256
  and listing (docs/PROOFS.md §2.1, §4).
- `make -C images images`, `push` and `oci` build the six first-party images
  `IMAGE_JOBS` at a time (default 6; `IMAGE_JOBS=1` is the old serial order),
  and `push` pushes each as soon as it is built, with the same registry
  digests recorded. A proof links its nine stack binaries with one `go build`
  rather than nine.
- A proof that runs integration and Playwright builds the UI and lists the
  specs while its ordinary integration jobs run (after the heavy ones, never
  beside them), through `ui/e2e/build-ui.ts`, which the Playwright global
  setup now calls too, so the Playwright gate starts with the build in place.
  A failed or stale pre-build is rebuilt by the gate as before
  (docs/PROOFS.md §2.1).
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
- The README is OrkStack's front page: what it is, diagrams of the
  architecture, a run and the commit order drawn from the UI's tokens in both
  themes (`site/scripts/readme-diagrams.mjs`), one real Orkflow (held to
  `examples/each-fan-out.ork.yaml` by a test), the CLI and the UI in motion,
  and a Docker-only quickstart.
- Getting started needs only Docker: it runs the quickstart bundle of a
  published release and uses `orkctl` from its image or a release binary.
  Go and Bun are needed only to develop OrkStack: the development stack
  (`make infra-up`, `make up`, …) is in `CONTRIBUTING.md`, "The local
  development stack", beside the Linux host setup ("Linux hosts").
- The development stack's Temporal, and so the Temporal every proof starts for
  itself, detects a drained release in seconds instead of minutes. Its
  `deploy/docker-compose.yml` command passes
  `matching.wv.VersionDrainageStatusVisibilityGracePeriod` (`10s`) and
  `matching.wv.VersionDrainageStatusRefreshInterval` (`3s`) with
  `--dynamic-config-value`; the cluster's defaults are three minutes each, which
  every test that waits for a release to drain (`orkctl release wait`, the
  continuation chain's rollout) waited out. They are development and proof
  settings, not a recommendation for a deployment, and the quickstart keeps the
  defaults (docs/DEPLOY.md §4, "Underneath"; docs/PROOFS.md §2.1).

### Added

- Every release publishes what it takes to run OrkStack without building it
  (`release.yml`, after its pre-release proof and an end-to-end smoke of the
  very images it publishes): `orkctl` for Linux, macOS and Windows on amd64
  and arm64, `ork` and `orker` for Linux, as archives with `SHA256SUMS`; the
  images `ork`, `orker`, `orkctl`, `ui` and the six first-party activities on
  `ghcr.io/orkstack`, for linux/amd64 and linux/arm64, tagged `X.Y.Z` and
  `X.Y`; and `orkstack-quickstart.tar.gz`, a compose file that runs the whole
  stack from those images (docs/UPGRADING.md, "Release artifacts"). The
  images are not signed yet. One commit builds the same binaries whatever the
  state of the checkout or the caller's Go environment. A release below an
  existing release of its own `X.Y` is refused, and a backport to an older
  series is published but never marked the latest, so
  `releases/latest/download/` always serves the newest release. The
  quickstart keeps its work directory under `~/.cache/orkstack-quickstart`,
  never a shared `/tmp`. `make dist`, `make release-images` and
  `make release-smoke` build and check the same artifacts on a developer's
  machine.
- `orkctl` builds and runs on Windows; `orkctl task run` and
  `orkctl activity test`, which run containers locally, refuse there with a
  reason (docs/LIMITATIONS.md).
- `ork --healthcheck` and `orker --healthcheck` ask the process's own admin
  listener (`ORK_ADMIN_ADDR`, `/readyz`) whether it is ready, and exit 0, or 1
  with a one-line reason, for a container health check in an image with no
  shell or `curl`. They read the configuration file and environment as a boot
  does, ask loopback for a listener bound to every interface, and are refused
  together with `--check` (docs/DEPLOY.md, "Health checks").
- A landing page and documentation site in `site/` (Astro and Starlight),
  deployed to GitHub Pages at https://orkstack.github.io/orkstack/ on every
  push to `main` (`.github/workflows/site.yml`); a pull request uploads the
  built site as an artifact. Its docs are written for readers, by need:
  tutorials, one-task how-to guides, reference tables and short concepts,
  each page citing the repository docs it summarises, which stay the full
  reference (`site/content/`, `site/docs.manifest.json`); the orkctl,
  configuration, error-reason and API references are generated. The site
  takes its whole style from `ui/src/tokens.css`: `make site` checks and builds it,
  failing on a colour or face outside the tokens and on a broken link, and
  `make site-media` regenerates its screenshots and recordings from the real
  stack. Proofs run it as the `site` gate.
- `docs/API.md`, the client-facing API reference, generated from the proto
  sources' comments (`make api-reference`, and `make proto` regenerates it).
- `docs/REASONS.md`, every machine-readable failure reason with what it means
  and what to do, held complete against the reason constants.
- New reference pages: `docs/GETTING_STARTED.md`, `docs/WEBHOOKS.md`,
  `docs/SECURITY_MODEL.md`, `docs/LIMITATIONS.md`, `docs/UPGRADING.md`,
  `docs/PRODUCTION_CHECKLIST.md`, `docs/TROUBLESHOOTING.md` and
  `docs/PROVEN.md` (how OrkStack is proven).
- Issue forms for bugs, feature requests and questions, and
  `.github/SUPPORT.md`: questions go to GitHub Issues.

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
