# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
will follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from the
first release. OrkStack has not reached 1.0: until it does, a release may
include breaking changes, and each is marked **BREAKING**.

## [Unreleased]

### Added

- An activity image's `with:` interface can be written as a readable YAML
  document, `kind: ActivityInterface` (by convention `interface.ork.yaml`
  beside the Dockerfile), instead of one line of JSON. `orkctl activity
  interface pack FILE` prints the `org.orkstack.activity.interface` label
  value it packs into, for any toolchain's `--label`, and `orkctl activity
  build --interface FILE` packs it into the image it builds. The label stays
  the contract the engine reads, unchanged, so every existing labelled image
  keeps working. The packed value is canonical: one declaration always packs
  to the same label value. `docs/schemas/activityinterface.v1.json`
  gives editors completion, selected by `kind` in `orkstack.v1.json`.
  `orkctl apply` refuses an activity interface named on its command line and
  skips one in a directory, naming it on stderr (docs/ACTIVITY_IMAGES.md,
  "Declaring the interface").
- A YAML editor in the UI for the Apply page, an Orkflow's Edit tab and
  Admin → Automation. It checks the document as you type, against YAML syntax
  and the generated JSON Schema of its kind. It asks the engine
  (`ValidateOrkflow`, or a plan for an automation document) once typing
  pauses. That covers what only the engine can check: the strict YAML-to-JSON
  rules, `needs:` references and cycles, interfaces, pools and policy. Each
  finding is underlined on its line, marked in the gutter, explained on hover
  and listed under the editor with its line and column. The document reads as
  having no problems only when the engine said so about exactly that text.
  The editor also offers schema completion, and key docs on hover or with
  Ctrl-Shift-Space. Tab indents; Escape, then Tab, leaves the editor.
- Syntax colours as design tokens (`--color-syntax-*` in `ui/src/tokens.css`,
  light and dark): keys in ink, strings teal, numbers ochre, `true`/`false`/
  `null` olive, `{{ … }}` templates violet, anchors and tags slate. The editor,
  the automation export, the version diff and every code block on the site
  (landing page, examples, docs) draw YAML with them.
- `go run ./internal/schemas/cmd/genschemas` also writes the Orkflow and
  automation schemas to `ui/src/lib/schemas/`, the UI's copy of
  `docs/schemas/`.
- Fair dispatch across namespaces: when tasks of several namespaces wait for
  one pool, each namespace is served in proportion to its new
  `fair_share_weight` (`orkctl namespace update --fair-share-weight`, the
  `fair_share_weight` setting of an automation document; 0 is the default
  weight of 1, otherwise from 0.001 to 1000), pinned in each run at submit. It
  takes effect where the operator enables fair dispatch on the cluster;
  without it dispatch stays first in, first out (docs/DEPLOY.md, "Fair
  dispatch across namespaces").
- An Orker can declare a capacity, `orker.capacity.cpu` and
  `orker.capacity.memory` (`ORK_ORKER_CAPACITY_CPU`, `_MEMORY`), and then
  admits no task that would take its reservations above it: every poll
  reserves the per-task cap, the reservation is trimmed to the task's
  effective request once it arrives, and it is freed when the container is
  removed. A pinned member is counted but never made to wait, so a pin host
  can exceed its capacity by what its pins run at once, and polls for no new
  task until the overshoot drains. A container that could not be removed,
  including an orphan a restart could not reap, keeps its reservation until a
  later removal succeeds. Unset keeps today's behaviour. `DescribeOrkerResult`
  gains `capacity`, `reserved`, `max_request` and `default_request`, the
  `Orker` row `capacity` and `reserved`, and `orkctl orker list` the
  `CAPACITY` and `RESERVED` columns. New reasons: `RESOURCE_ADMISSION_MISMATCH`
  (a guard that cannot fire while caps are honoured) and the boot refusal
  `ORKER_CAPACITY_INVALID` (docs/DEPLOY.md, "Declaring an Orker's capacity").
- **BREAKING:** A submit refuses a task whose `resources:` request is above
  the per-task cap of every answering Orker of its pool, with
  `RESOURCES_UNSATISFIABLE` (`failed_precondition`), before a run id is
  minted. It used to be accepted and fail when that task was dispatched,
  `RESOURCE_REQUEST_REFUSED`, so a run whose oversized task never ran (a
  failure handler of a run that succeeded) used to succeed and is now
  refused. Such a submit reads the Orker inventory, one describe per live
  Orker.
- Podman as a second engine of the Orker's one runtime: the podman CLI
  (`orker.docker.cli: podman`, `ORK_DOCKER_CLI`, read by `orkctl activity
  test` and `orkctl task run` too), or the docker CLI against Podman's
  Docker-compatible socket, each with the Orker as root inside the runtime
  (docs/DEPLOY.md, "Running on Podman"). The engine is asked at boot, logged,
  and verified by the boot probe. On the Podman engine the default seccomp
  profile is spelled by omitting `seccomp=builtin`, which that engine reads as
  a file, and DescribeOrker's security profile says `seccomp=engine-default`;
  the podman CLI is told `--http-proxy=false`, so its own proxy variables
  never reach a container; the escape hatch uses `--userns=keep-id` on a
  rootless podman CLI; and `/run` and `/var/tmp` get a read-only tmpfs, which
  that engine's `--read-only` leaves writable. `podman.yml` runs the
  runtime-facing suites on Podman, rootless and rootful.
- The Orker counts an attempt whose own directory, or its secrets, could not
  be removed when it ended, on `ork.orker.workspace_cleanup_failures`, and
  logs the path and the error. The error was discarded, so a workload that
  left a file or directory as a uid the Orker cannot remove filled the disk
  with no sign of why.
- `RUNTIME_POSTURE_REFUSED` and `RUN_AS_ORKER_UNAVAILABLE`, two new reasons
  (docs/REASONS.md). The first is a boot refusal: the Orker's boot probe found
  that its runtime does not give a task's container the §9.2 posture. The
  second fails a task that runs as the Orker's own user (`run_as_orker_user`)
  on an Orker whose boot probe found that user does not land as the Orker,
  as on a rootless or user-namespaced daemon; it is not retried, and with
  `ORK_ORKER_FORCE_USER` it refuses boot instead.
- Fuzz targets for the inputs OrkStack accepts from outside, each stating the
  invariants the code or its reference doc claims and not only that nothing
  panics: the list-filter grammar, compiler and matcher (a compiled set is a
  fixed point of compiling and independent of filter order; a row matches as
  the documented rules say; every pushdown is sound), the enum dialect, the
  tolerant JSON codec against protojson, the trigger, webhook and await
  doors, page tokens, webhook signing and delivery against a real receiver,
  the Orkflow, automation and reference parsers, the `sh` template helper and
  `orkctl`'s printed quoting against a real shell, a container's
  `values.json`, the container argument vector, the Orker's cpu and memory
  quantities (an accepted request renders to a positive limit within the cap),
  the `allowed_registries` boundary rule, the secret-source matcher against a
  regular expression, the evidence verifier against a count made without it,
  the continuation object decoders, the run archive and saved-object decoders,
  the stored-record decoders against their encoders, the live-log framing and
  the object-key parsers, the WebSocket origin check against the library's own,
  schedule specs, configuration files and arguments, telemetry sinks, a
  registry's bearer challenge, the activity catalog, and `orkctl`'s group-by,
  category, time and plan-output flags. Their seeds run in every unit gate and
  a crasher a run finds is kept as a seed under `testdata/fuzz/`
  (CONTRIBUTING.md, "Fuzzing"; AGENTS.md §11.1). Each fix below says how its
  defect was found: by a failing input the fuzz engine wrote, or by reading the
  code while stating a target's property, with the target's seed failing until
  the fix.

- A runner-size miss of a Go test's wall-clock budget is a recorded known gap,
  like a UI spec's. A `kind=test` row of `scripts/proof-host-speed.tsv` may
  declare `timing ID`, one check the test reports through
  `internal/testsupport/proof` (`TimingBounds`, `Timing.Check`) instead of
  `t.Errorf`. A miss fails nothing where it is measured; the test's last
  cleanup writes `timing/<test>.json` under the part's artifacts and then
  reports each miss as a line of a fixed form. The merger excuses the miss on
  a CI `integration:K` part whose runner has fewer CPUs than the row's
  reference, under a row the base carries identically, only when it derives
  from the part itself that the test failed nothing else: the ledger shows it
  as the only test of its job not to pass, with no crash and no failing
  subtest; its own record says it failed no other assertion; the failure
  output the ledger entry now keeps (the last lines a failing test printed)
  ends in exactly the helper's lines; every recorded bound equals the
  row's; and no miss took longer than the bound times the reference's CPUs
  over the runner's. The record the excusal rests on is kept in the
  `host_speed_failures` entry (`timing_record`) of `proof.json`, because a
  part's artifacts are not kept in the merged proof. The helper's package,
  `internal/testsupport/proof`, is now a gate file with a code owner, because
  the excusal trusts the record it writes and the lines it prints. A failing test with no record, a local proof, a runner as large as
  the reference and any other failure still fail the proof. The rows are the
  live `GetRun` p95 (2 s on 16 CPUs) of the three reduced-ceiling soak tests,
  which missed at 2.014 s on 4 vCPUs in run 40593609129, and the run page at
  the reduced ceiling (5 s on 16 CPUs), which missed at 9,485 ms in run
  40571748126 with no row. The archive p95, `Events(0)` and replay checks go
  through the same helper and have no row. The soaks' live `GetRun` also
  keeps a bound on what it reads, which holds on every host: at most 110% of
  the history events and of the history bytes of the generation it read (one
  pass over it), and at most 4 saved-state objects. It bounds what is read,
  not what reading costs: the evidence records the cost per thousand events
  (`ms_per_1000_events`), which only the wall-clock numbers enforce, and the
  run page's 5 s has no such counterpart. No threshold changed.
- A race gate in the full and release proofs, and two lanes in the nightly
  one. The `race` gate runs the unit suites under the race detector with
  `-short` (`proofrun unit -race -short`, ledger `race-tests.json`): a data
  race fails it whatever the tests reported, a test skipped in its `-short`
  run is recorded as a skip, from a ledger that says its run had `-short`, and
  must have passed the unit gate, which runs it without `-short`, and no
  quarantine excuses a failure. A local proof runs it last of
  the fast gates, alone; CI runs it as the `race` part beside `fast`; a
  feature proof runs it over the packages its change touches
  (`race_packages` in the selection). The nightly tier adds the `race-repeat`
  lane (the engine, Orker, API and `pkg` packages' unit suites under the
  detector, `-count 3`, with a `-timeout` of 30 minutes for each repetition; a
  failure in any repetition stands, and a repetition that never reported
  leaves its test with no outcome) and the `fuzz`
  lane (`proofrun fuzz`: every `Fuzz` target, sharing a budget for the whole
  lane, `FUZZ_BUDGET`, 105 minutes in CI, each target an equal part of what
  remains less the overhead of starting it, never under 15 seconds, so the
  lane ends inside its CI step however many targets there are, and a unit test
  fails when the budget or the target count no longer fits the step; its
  ledger records the fuzz time each target was given; a
  crasher fails the lane and its input is kept under the part's `crashers/`,
  leaving the tree as it was found). `make test-race` runs the race gate's
  `go test` without its ledger, its `-timeout` included, and `make fuzz` runs
  the lane (`FUZZ_BUDGET`, or `FUZZTIME` for each target). The detector needs
  cgo and a C compiler, and a gate that runs it, the unit gate included (its
  tests run `go test -race`), refuses to start without. The gate may fail on a
  tree with a race in it until that race is fixed; that is its purpose.
- A runner-health floor for the CI proof parts (`scripts/proof-runner-floors.tsv`,
  `proofrun runner-health`): every part measures its runner for about fourteen
  seconds before any gate — sequential write and read throughput and fsync
  latency on its work disk, single-core and all-core CPU throughput, and
  available memory — into `runner-health.json`, with the floors of its
  runner class and a verdict. A stack part below a floor stops before any
  gate, and the merger classifies it `runner-degraded`, exactly as a part
  whose runner GitHub lost: its tests have no outcome of the tree, the proof
  is not a proof of the tree, `proof.json` records the part, the metric, the
  measured value and the floor (`degraded_runners`, `runner_health`), and
  that one job may run again. A part above the floor whose tests fail stays
  a test failure, and a part with no health file keeps its meaning. A local
  proof measures the same numbers for information only, into `setup.log`
  and `proof.json`. The nightly of 2026-10-26 (run 40371833169) failed two
  soaks on one shard at 2.6× and over 3× their measured times while the same
  tree passed both the next day; the contract had no honest way to record
  that the runner was degraded. The floors are derived from the 21 parts of
  run 40411125174, a third of the least measurement or three times the
  largest latency.
- Informational runner-health readings: besides the judged disk write, disk
  read and fsync, the measurement writes the probe a second time, reads it in
  two more bursts (one right after that write, one after the all-core CPU
  measurement) and takes one more fsync reading, last. Each is recorded in
  the metric's `samples` with the offset at which it began and, for a write
  or a read, the region of the probe it covered; the second burst starts
  where the first stopped, so bursts read different blocks where the file is
  large enough. The judged measurements are unchanged, and so is the floor: it
  judges only each metric's first reading, a later reading never degrades a
  runner, and `scripts/proof-runner-floors.tsv` has no row for a later
  reading. The readings appear in `proof.json`'s `runner_health`, in a
  "Runner health" section of every README and in the part's `runner-health:`
  log lines, and feed the next derivation of the floors
  (`docs/PROOFS.md` §10, which gives a `jq` recipe). The measurement takes
  about four seconds longer.
- A third kind of part attempt that may run again, **setup-failed**: a CI job
  that failed, whose proof step never started, in a fetch or install step on a
  per-kind allowlist (`setupStepNames`: `proof host`, `fetch plan`,
  `fetch build`, `fetch browser image`, `Chromium (host)`, `bun install`,
  `observability stack`). It ran no test and left no result, so the merger
  treats it exactly as a lost or a degraded part: `proof.json` lists it under
  `setup_failures` and records the rerun with reason `setup-failed`, and the
  README names it. The plan and the build have no entry; a failure in any
  other step, or any attempt that recorded a result, is still not
  rerun-eligible. The three `proof-host` steps of `proof.yml` are named, since
  the jobs API reports a composite action only by its step's name, and the
  merger's step names (`proofStepNames`, `setupStepNames`) are pinned to the
  workflow in both directions, with rename mutations.
- `proof-rerun.yml` and `proofrun rerun-plan`: automatic reruns of a failed
  candidate proof. Off unless the repository variable `PROOF_AUTO_RERUN` is
  `true`; it acts on a push to `validate/full/**` of this repository, while the
  run attempt is below 3 (at most two automatic reruns), and runs
  `gh run rerun --failed` only when every failed job is the `proof` job or a
  part the merger's own classification calls lost, setup-failed or degraded.
  The planner reads the attempt's jobs, annotations and logs through
  `gh api`, prints only words it validated, and runs from the default branch;
  the merger stays the only authority, so a wrong rerun can only make a proof
  ineligible. The workflow takes `checks: read` beside `actions: write` and
  `contents: read`, because the evidence is a check-run annotation. A stack
  part that stops on a runner below the floor now prints one fixed-word
  `::error::` annotation under `GITHUB_ACTIONS=true`, which the planner reads
  and the merger does not.
- Static analysis. `make lint` runs staticcheck, pinned in `go.mod` as a `tool`
  directive (`honnef.co/go/tools` v0.8.2), with its default checks over the
  module as built and again with `-tags=integration`, so `make check`,
  `make merge-check` and the lint gate of every proof run it. A finding is fixed
  in the code; the few that are not defects carry a line-level `//lint:ignore`
  saying why (the Temporal SDK's `VersionsInfo`, its only field for a queue's
  poller records, and the S3 multipart upload manager, whose v0 successor is a
  change of its own). `make vuln` runs the pinned `govulncheck`
  (`golang.org/x/vuln` v1.9.0) and reports the vulnerabilities whose vulnerable
  code the module reaches. It needs the network, so it is in no lint and no
  proof; `ci.yml` › `vuln` and `nightly.yml` › `vuln` run it, and a reachable
  advisory fails the run, which for the nightly is a regression on `main`. Adding the
  two tools raised the indirect modules `golang.org/x/net`, `x/sys`, `x/sync`
  and `x/text` to what their requirements need. The `ci.yml` › `check` and
  `feature` proof steps and `proof.yml` › `fast` have five minutes more for the
  lint time (docs/PROOFS.md §11.5). Its first run found three advisories no
  OrkStack code reaches, in `google.golang.org/grpc` v1.83.1 and
  `golang.org/x/net` v0.61.0; both are raised to the patch releases that fix
  them (v1.83.2 and v0.62.0).
- `nightly.yml` › `quickstart` runs the README's Quickstart as written on a
  clone of `main` every night (`scripts/quickstart-smoke.sh`, as
  `make quickstart-smoke` does), beside the nightly tier and whatever its
  result. No proof runs the getting-started path, and `release.yml` runs it
  only before a release, so a directory `deploy/source/Dockerfile.dockerignore`
  does not admit, a build break in `deploy/source/Dockerfile`, a
  `compose.yaml` key the runner's Compose refuses or a stack that never turns
  healthy now fails the next nightly, a regression on `main`, instead of
  waiting for the next release attempt (docs/PROOFS.md §5, §10).

### Changed

- The first-party images and the example images declare their interfaces in
  an `interface.ork.yaml` each, and their Dockerfiles' interface `LABEL` line
  is generated from it by `go run ./internal/schemas/cmd/genschemas`. The
  declarations are unchanged; the regenerated lines write them with sorted
  keys, so each image's config digest changes once, at its next build. Each
  image's README links its interface document instead of copying the label.

- DescribeOrker's runtime name is the engine that runs the containers
  (`docker` or `podman`), not the CLI, and its rootless flag is read from the
  CLI's own field: the podman CLI has no `SecurityOptions`, and a failed
  detection was reported as not rootless. A detection that fails is now an
  error.
- The Orker reads a container as started only from a state it names as
  started, and keeps waiting on one it does not know (Podman's `configured`
  and `initialized` are not started), and it reads the runtime's refusals in
  any case, including Podman's own words for a container that already
  stopped and for a bind source it cannot see.
- **BREAKING:** The Orker's boot probe runs one container built exactly as a
  task's is, and judges it. It used to run a weaker argument vector (no
  seccomp option, no `--init`, no limits, no tmpfs), so a runtime that refused
  or weakened the posture was found by every task and never at boot. It now
  runs a task's argument vector with a task's mounts and checks from inside
  the container that `NoNewPrivs` is 1, `Seccomp` is 2, no capability is held,
  the root, `/run`, `/var/tmp` and input are read-only, `/tmp` is `noexec`, no
  proxy variable was added to the environment, and on cgroup v2 the cpu,
  memory and pids limits are the ones it asked for; it proves the bind is the
  Orker's own directory with a nonce written each way. An Orker whose runtime
  fails any of these refuses to start, `RUNTIME_POSTURE_REFUSED` naming each
  fact, or `WORKDIR_NOT_BINDABLE` for a bind that is another directory (as
  Podman's Docker-compatible socket makes of a missing source). A deployment
  whose docker CLI configuration adds `proxies` to every container, or whose
  rootless daemon silently drops the cpu or memory limit, now refuses to
  start where it started before. A probe refusal that is neither a bind nor a
  deadline, which was `WORKDIR_NOT_BINDABLE`, is `RUNTIME_POSTURE_REFUSED`.
- **BREAKING:** An Orker that runs every task as its own user
  (`ORK_ORKER_FORCE_USER`) on a rootless or user-namespaced daemon refuses to
  start as `RUN_AS_ORKER_UNAVAILABLE`, and on such a daemon a task that sets
  `run_as_orker_user: true` fails with it, not retried, even where its image
  writes files the Orker could read. That user lands as another host user
  there, so the escape hatch did not do what it says.
- `/tmp` is mounted `noexec` explicitly. Docker adds `noexec` to a `--tmpfs`
  by itself and Podman does not. `/run` and `/var/tmp` get no tmpfs of their
  own: Docker's `--read-only` already keeps them read-only, and a mount there
  follows an image's symlink (an image whose `/var/tmp` links to `/tmp` lost
  its writable `/tmp`); the boot probe refuses a runtime that leaves either
  writable.
- The boot probe's image is
  `docker.io/library/alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc`,
  qualified and pinned, rather than `alpine:3.20`: a short name is resolved by
  the runtime's own search list, and Podman's enforcing short-name mode with
  more than one search registry refused it.
- `ContainerSpec` says "run as the Orker" (`RunAsOrker`) instead of carrying
  a `--user` value, and the runtime renders it; the docker CLI renders it as
  `--user <orker-uid>:<orker-gid>` as before.
- The pre-release gate excuses a runner-size miss exactly as the merge gate
  and the nightly do: on a CI runner below a row's reference CPUs, under a row
  of `scripts/proof-host-speed.tsv` the base carries, a wall-clock miss is a
  recorded known gap, never a pass. So `release.yml`'s `gate` can pass on the
  hosted 4-vCPU runners, and a release no longer waits for a runner as large
  as the 16-CPU reference host; only a proof on that host enforces the
  budgets now. Every other failure still fails it. Its `tag` and `publish`
  jobs follow the release proof, which `proofrun verify -release` checks,
  and the named results of what they need, never the gate call's colour: an
  excused Go timing miss leaves its integration part red while the proof
  passes, and the implicit `success()` would have skipped them.
- A red nightly is a regression on `main`, not a merge freeze: fixing it, or
  reverting what broke it, is the next work, and other changes keep merging
  through the merge gate meanwhile. It still holds the release: a tag requires
  a passing pre-release proof, which runs every deferred test, and a release
  is not tagged while the nightly's own lanes or its vulnerability scan are
  red. A nightly's verdict is its `proof.json`, not the run's colour.
- The feature proof is optional: a scoped check of a change on its branch that
  gates nothing. `ci.yml`'s `feature` job runs on a pull request only with the
  `ci:feature` label (it ran on every pull-request push of a public
  repository), and on a `validate/feature/**` push or by dispatch as before.
  The full proof remains the only gate.
- The default merge path is a CI full proof of the candidate: push it to
  `validate/full/<name>`, where the proof runs sharded (14.6 minutes measured,
  against 78 for one local host), and pass the downloaded artifact to
  `make merge-check PROOF=` (`docs/PROOFS.md` §10). A local `make proof` stays
  an equivalent path.
- `.github/actions/proof-host` no longer leaves the runner except through
  `proof-fetch`: cosign v2.6.5 is downloaded as the release's linux/amd64
  binary and kept only if it hashes to the pinned SHA-256 (replacing the
  installer action, which could not retry), the stack images the cache did not
  load are pulled through it, the stack starts with `--pull never`, and the one
  container pulled by tag is bounded with `timeout`. `proof-fetch` takes a `url`
  with a `sha256`, and a list of images, each of which must be pinned by digest.
  The registry image of `deploy/docker-compose.yml` was the floating tag
  `registry:3`; it is pinned by the index digest of 3.1.2.
- `proof-fetch`'s give-up line no longer says a setup failure needs a new run
  of the same tree: a fetch that fails both attempts is a setup-failed part
  the merger lets run again (`docs/PROOFS.md` §8).
- The site reads larger: it sets its root at 112.5%, so every token of
  `ui/src/tokens.css` it draws with steps up together, Starlight's own sizes
  included — docs body 18 px (was 16), landing body 16.9 px (was 15). On top
  of that, the docs' code blocks, tables and sidebar step up to `--text-base`,
  their headings by one token, and the hero lede keeps `--text-lg` on wide
  screens. The docs column is 49rem; prose still stops at 72ch.
- The site's screenshots are drawn at their own size and never re-encoded or
  enlarged. `make site-media` captures each one natively at one and at two
  device pixels per CSS pixel, saves both as lossless WebP in
  `site/public/media`, and records each one's size in the manifest; the
  landing page and the docs serve the pair as a `srcset`. The landing page's
  are the run page at 1616 px, the width of its frames; the docs' are cuts of
  the UI's main column at its 1280 px layout (the graph, a task inspected,
  a fan-out item, the resolve dialog), which fit the docs column one to one.
  Before, a capture was resized, re-encoded twice (WebP, then AVIF at
  Astro's default quality) and shown enlarged on high-density screens, and a
  docs screenshot was squeezed into the 72ch measure. The terminal recording
  is drawn at twice its size, with a lossless poster from the tape itself;
  the README's GIF is scaled back to its old size. The unused `ui-orkflow`
  screenshot is no longer taken.
- Every YAML example — `examples/`, `deploy/orkstack.example.yaml`, the README,
  `docs/`, the activity images' READMEs and the docs site's pages — writes
  its sequences and mappings in block style (`needs:` then `- list`), never
  as a JSON-looking flow collection (`needs: [list]`, `retry: { … }`), and so
  does the configuration reference `make config-defaults` generates
  (`deploy/orkstack.defaults.yaml`, which `ork --defaults` and
  `orker --defaults` print). The examples' definitions are unchanged
  (`TestExamples_KeepTheirDefinitionDigests` holds every digest), but their
  bytes are not: a store that applied an example before refuses the new
  file under the same version (`examples/README.md`). `TestDocs_YAMLExamplesUseBlockStyle`
  refuses a flow collection in any of them; an empty `[]` or `{}`, which has
  no block form, is allowed.

### Fixed

- A pool whose every Orker is busy is no longer refused at submit as empty,
  and submit's capability probes no longer skip it. An Orker polls its pool's
  channel only while it has a free task slot, so once every Orker of a pool
  had run `ORK_ORKER_MAX_CONCURRENT` tasks for 90 seconds the channel showed
  no poller: a submit was refused `has no live Orkers` while the Orkers were
  doing the pool's work, and the pin-host, fan-out, caching and failure
  handler probes, sent as activities on that same channel, waited behind the
  busy slots and were skipped with a warning. When a pool's channel shows no
  poller, submit now asks the Orker inventory (the presence channel, and each
  Orker on its own control channel, the read `orkctl orker list` makes), and
  every probe asks each uncordoned Orker of the pool on its own control
  channel. A probe now judges every such Orker, so one Orker of an older
  release refuses its pool rather than only when it happened to answer, and
  one pin host is enough to open a pin. `ListPools` counts a busy pool's
  Orkers the same way. Found by reading the code; reproduced by
  `TestIntegration_ASaturatedPoolIsLiveAtSubmitAndAnswersItsProbes`, which
  failed on the empty-pool refusal and, with only presence fixed, on the
  skipped pin-host probe.
- **BREAKING:** A task's `resources.cpu` and `resources.memory` are refused
  at validate and apply, with their path, when they do not read as a limit in the
  Kubernetes spelling (`docs/DSL.md` rule 6d): `cpu: "banana"` or
  `memory: 4GB` used to be accepted and stored, then fail every attempt
  `RESOURCE_REQUEST_REFUSED` at dispatch. The DSL and the Orker now read a
  request with one grammar (`internal/quantity`).
- The Orker's task worker sets its poller count explicitly (2, the SDK's
  default). Left at the default, a Temporal namespace that advertises poller
  autoscaling auto-enrolled it, and a pin host's session worker with it, so
  how many polls an Orker held was a server setting.
- `docs/TROUBLESHOOTING.md` no longer says a task waiting for a busy pool
  stays `PENDING`. It is `RUNNING` from dispatch, with `waiting for an orker`
  in `orkctl run inspect` and on the run page; `PENDING` is only the engine's
  own hold (the in-flight cap, the history hold line, a pin opening).
- `orkctl task run` gives each run an identity of its own,
  `ork.orker=orkctl-task-run-<hex>` from its local run id, in place of the one
  `orkctl-task-run` every run shared, so one run's label never names another
  run's container. Each run holds a lock file for its identity in its work
  directory while it lives, naming its host and container engine, and deletes
  it once its container is removed. The next task run in the same work
  directory, on the same host and engine, removes the containers of every
  identity whose lock is free: a container a run abandoned on a second
  interrupt, or left when it was killed. A live run's container is never
  touched, and `--render-only` removes none. Before, nothing removed such a
  container; one an earlier release left under the shared
  `ork.orker=orkctl-task-run` label is not collected and is removed by that
  label by hand.

- The Orker reads a docker CLI's answer from its stdout alone and quotes its
  stderr in errors. Both streams were read as one, so a CLI that wrote a line
  of its own on stderr (the `podman-docker` shim's "Emulate Docker CLI using
  podman" on every command, a rootless runtime's warnings) broke every parse:
  the image inspect no longer decoded and every task failed
  `IMAGE_PULL_FAILED`, and a created container's id carried the warning.
- The Orker kills a container before it removes it. Podman's `rm --force`,
  behind its CLI and its Docker-compatible socket, waits out a running
  container's stop timeout first: 30 s for a workload that ignores SIGTERM,
  the whole of the removal's own time limit, so a removal could time out and
  a boot reap cost 30 s per orphan.
- The unit gate gives each package's test binary a 15-minute `-timeout`
  instead of `go test`'s default ten. `internal/engine`'s unit suite took
  460 s on a 4-vCPU CI runner and passed ten minutes on a slower one (CI run
  40749987883), which failed the gate with every engine test after it left
  without an outcome. The race gate already had its own (30 minutes per run).
- The Orker no longer reports a docker CLI that exited 0 as a failure when
  its output pipe drains late. It allowed 100 ms for that drain, so on a
  loaded host a successful `docker create` became `RUNTIME_REFUSED`
  ("exec: WaitDelay expired before I/O complete") and a task with one attempt
  failed its run (CI run 40740645720). It now allows 5 s, a bound on a pipe a
  helper inherited, never on the command itself.
- The run, newest-first and origin index parsers read only a status segment
  the writer can spell (lower case, no trailing space), and the newest-first
  cursor renders a scanned entry's stored status verbatim, as the run index's
  cursor already did. A key with status `A` read back as a run whose cursor
  was the key spelled `a`, and a stored `status_succeeded` (what a raw
  `STATUS_STATUS_SUCCEEDED` stores) rebuilt as `succeeded`, a key after it, so
  a newest-first page could skip runs. The nightly fuzz lane found it in its
  first run on GitHub (nightly 40706217000); the input is kept under
  `pkg/artifacts/testdata/fuzz/`.
- The documentation drift checks (`internal/docscheck`) read only the
  markdown a commit of the work tree would hold: tracked files, and untracked
  ones git does not ignore. They read every `*.md` on disk outside a fixed
  list of directory names, so they also judged files that are not the tree's:
  the pages the site's docs sync writes into `site/src/content/docs/docs/`,
  which git ignores and which stay behind when a checkout switches branches,
  a design tool's notes under `site/.impeccable/`, a hand-run Playwright
  failure's output under `ui/test-results/` and, from a checkout holding
  worktrees under `.claude/worktrees/`, every other worktree's documents. A
  worktree that synced the site on a branch with `make test-race` and
  `make vuln`, then switched to one without them, failed
  `TestDocs_EveryMakeTargetExists` in its unit gate until the site was
  synced again. A tracked document is now read wherever it lies, even in a
  directory named like a build output. The checks need git and a git
  checkout, as `go test ./...` already did.
- `make check` runs the packages whose unit tests run git
  (`GIT_STATE_PACKAGES` in the Makefile: `internal/docscheck`,
  `internal/engine` and `test/tools/proofrun`) with `-count=1`, and keeps the
  test cache for the rest. Go's cache records only the files the test process
  itself opens inside the module, never what git reads in a subprocess (HEAD,
  the index, `info/exclude`, a global ignore file), so a change to what git
  tracks or ignores alone left a cached pass standing: the documentation
  checks, the scope table's check and the engine's golden-fixture check all
  read that state. `TestMakeCheck_RunsGitStatePackagesUncached` keeps the list
  whole.
- The feature proof's evidence selector treats a native fuzz target
  (`func FuzzXxx(f *testing.F)` in a `_test.go` file) as a test. A diff that
  changed only a fuzz target, or only a helper one reaches, selected no
  evidence, and `--tests PKG:FuzzXxx` was refused as matching no test. A
  changed target, one that reaches a changed helper and a named one are now
  evidence like any other, a deleted one is removed, and the unit gate's
  ledger, which already listed fuzz targets, records the target's seeds once
  (docs/PROOFS.md §3).
- Unit tests no longer bound code cost by a sub-second wall-clock window, which
  measured the runner and not the code: the CI unit gate failed
  `TestLogChunkSink_ABurstGrowsTheChunkInsteadOfThrottlingTheWriter` at 81 ms
  against a 75 ms bound (run 40432725750) with nothing wrong in the sink. The
  sink's pace hold and upload back-off are now counted and sized on a clock
  the test moves; the history-guard and run-state cost bounds read the
  workflow thread's CPU time; the audit prefetch, sink shutdown and
  unknown-Orker cordon tests observe concurrency and dispatch instead of
  elapsed time; and the UI's DAG bounds read the thread's CPU time.
  AGENTS.md §11.1 states the rule.
- The Orker's boot check of its work directory no longer mistakes a slow
  container daemon for an unbindable directory. The check runs a container,
  and it ran under the 30-second timeout meant for bookkeeping commands: a
  daemon under load took 32 seconds to start a container that exits at once,
  the check was killed, and the Orker refused to start with
  `WORKDIR_NOT_BINDABLE` ("docker exited -1"). Several proofs failed that way,
  the last the local proof of `pin-close-full` (job `orkctl-171`). The check
  now has a timeout of its own, `orker.docker.bind_probe_timeout`
  (`ORK_DOCKER_BIND_PROBE_TIMEOUT`, 2 minutes), for each step that waits on
  the daemon: the question whether the check's image is local, and each of two
  attempts to run its container. An attempt that overruns it, or that the
  daemon ends with a deadline error of its own, is tried once more after the
  container it named is removed. A daemon that is still too slow refuses boot
  as the new reason `BIND_PROBE_TIMED_OUT`, whose message says the daemon was
  slow, quotes what it answered, and names the setting; a daemon that was slow
  to say whether the check's image is local is that reason too, no longer
  `BIND_PROBE_IMAGE_UNAVAILABLE`. `WORKDIR_NOT_BINDABLE` is kept for a
  daemon that refuses the bind within the budget, in its own words or
  otherwise, a check container that exits non-zero, and a check directory that
  cannot be made, and none of those is tried again. An Orker stopped during
  the check reports the interruption rather than any of them. A daemon that
  never answers can now hold boot for up to three times the timeout plus a
  minute, 7 minutes at the default, before it is refused.
- **BREAKING:** The Orker accepted a memory request of zero and passed it to the
  container runtime as no limit. A cpu request of zero was already refused
  because `--cpus 0` is unlimited, but memory had no such check: `memory: "0"`,
  `"0Gi"`, `"0.5"` (rendered as 0 bytes) and `"-1"` passed the cap, reached the
  runtime as `--memory 0` or an unparsable value, and an attempt's usage record
  reserved 0 bytes. A memory request is now refused unless it is at least one
  byte and below what an int64 holds, as `RESOURCE_REQUEST_REFUSED`; the Orker
  refuses at boot a cap or default (`ORK_ORKER_MAX_CPU`, `_MAX_MEMORY`,
  `_DEFAULT_CPU`, `_DEFAULT_MEMORY`) that parses but is not a positive finite
  quantity, since a NaN cap compared above and below nothing and removed the
  ceiling; and the container argument builder refuses a cpu or memory that
  parses to no limit. Found by reading the code for the property an accepted
  request renders to a positive limit within the cap
  (`FuzzResolveResources_AnAcceptedRequestIsALimitWithinTheCap`).
- The Orker refuses at boot a work root (`ORK_ORKER_WORKDIR`) whose path holds a
  comma or a control character, as `WORKDIR_NOT_BINDABLE` naming the setting. A
  mount is a comma-separated option list, so every task of such an Orker failed
  at dispatch, one attempt at a time, with an error that did not name the
  setting.
- **BREAKING:** The JSON codec, the trigger invoke route, the webhook
  registration route, the await completion and rejection routes and `orkctl
  --input-json`, `--output` and `--outputs-file` no longer repair what a caller
  sent. Go's JSON decoder replaces bytes that are not UTF-8, and a string's lone
  surrogate escape, with U+FFFD, so a run's input, a caller-chosen webhook
  signing secret, an await's values or a name was stored as something other than
  what was posted while protojson would have refused it; they are now refused,
  as protojson refuses them. The webhook registration route also ignored
  everything after its first JSON value (`{…}{…}` registered the first) and now
  refuses a second value as the trigger route does, and the trigger and callback
  routes trimmed any Unicode space around a body, so a body wrapped in a
  no-break space started a run; only JSON's four whitespace characters are
  trimmed now. The tolerant codec behind every Connect JSON request also dropped
  whatever followed the first JSON value (`{}0` was read as `{}`); that is
  refused too. The `{}0` input was written by the fuzz engine
  (`FuzzUnmarshalJSON_IsProtojsonPlusEnumSpellings`); the others were found by
  reading the doors while stating the targets' properties (`FuzzTriggerInput`,
  `FuzzRegisterWebhook`, `FuzzAwaitRejectReason`, `FuzzCheckJSONText`).
- An enum value, a filter's enum value, a webhook event name and the
  `orkctl stats` `--group-by` and `--category` spellings are matched with ASCII
  case only. They were upper-cased with Unicode rules, so `ſucceeded` (U+017F)
  read as `SUCCEEDED`, `runnıng` (U+0131) as `RUNNING` and `taſk` as `task`; no
  declared name is spelled with those letters. Found by reading the code while
  stating `FuzzProto_ReadsOnlyASCIICaseAndDashAsTheSameValue` and
  `FuzzParseGroupBy`.
- A list filter on an instant whose UTC spelling falls outside the years 0000
  to 9999 (`created_at<9999-12-31T23:59:59-23:59` is year 10000 in UTC) was
  compiled to a canonical text, bound into the page token, that a following
  request could not send (`10000-01-01T…` is not RFC 3339). It is refused by
  name instead: `"…" is outside 0000-01-01T00:00:00Z to
  9999-12-31T23:59:59.999999Z, the instants a filter can name`. Found by
  reading the compiler while stating the canonical-set property
  (`FuzzCompile_ACompiledSetIsCanonicalAndTheOrderOfItsFiltersDoesNotMatter`).
- `orkctl`'s `--since`, `--until` and the run list's RFC 3339 bounds accepted an
  instant a request cannot carry (`0000-01-01T00:00:00Z`, or a local time whose
  UTC instant falls before 0001 or after 9999) and the request then failed to
  encode with a message that did not name the flag; they are refused by name,
  judged on the UTC instant. Found by `FuzzParseInstantFlags`.
- **BREAKING:** An image reference is held to the OCI distribution grammar when
  an Orkflow is applied and when the activity catalog is read at boot: a
  registry host name (or bracketed IP address) with an optional port from 1 to
  65535, lower-case repository path components, a tag of word characters, dots
  and dashes that does not start with a dot or a dash, and a `sha256:` digest.
  `ParseReference` accepted any text, so `uses: --cap-add=ALL.example/x:1` was a
  registry named `--cap-add=ALL.example` (pinned as
  `--cap-add=ALL.example/x@sha256:…`, an argument the container runtime reads as
  a flag, ahead of the image the Orker meant), and a repository could carry `?`,
  `#`, `%`, a space or a newline into the manifest URL. A reference outside the
  grammar is refused at apply as `IMAGE_RESOLVE_UNSUPPORTED`, which Docker
  refuses at pull anyway, and an `ORK_ACTIVITY_CATALOG` entry outside it stops
  the engine at boot instead of failing at the first apply that names the
  activity. The Orker's container argument builder also refuses, as defence in
  depth, an image that begins with `-` or holds a space or control character,
  and a mount path with a comma or control character, which would add an option
  to the `--mount` value. Found by reading `dsl.ParseReference` and
  `dockerCreateArgs` while stating `FuzzParseReference` and
  `FuzzDockerCreateArgs`.
- A template renders data that spells `<no value>` as itself, and refuses only
  a nil value that it prints. The Orker's `with:` renderer and the engine's
  output renderer refused any rendered string containing those characters, to
  catch a nil value, so an input, a task output or an author's text that merely
  contained them failed `RENDER_FAILED` or `OUTPUTS_RENDER_FAILED`. A nil value
  is now noted where the template prints it, by one stage appended to each
  printing action as `html/template` appends its escapers, and the template
  runs once on the real data: the characters of the marker put together across
  text and a value, across two values or inside one `printf` render as they
  spell, `printf "%q"` and a comparison against a string the template builds see
  the real value, and a nil that is only tested (`{{ if .x }}`) is not refused.
  Found by reading the renderer while stating `FuzzRenderer_ARenderedValueIsTheValue`
  and `FuzzRenderValues_OnlyAPrintedNilIsRefused`.
- An await's completion values are refused when a string in them holds half of a
  surrogate pair (`{"a":"\ud800"}`). The completion routes stored the values byte
  for byte after checking only UTF-8 and JSON validity, so a later Go decode of
  the stored object read the half as U+FFFD; the rejection route already
  refused it. Found by reading the validation while stating
  `FuzzValidateAwaitValues`.
- `orkctl run submit --input-json null --input key=value` (and `run again`,
  `schedule create`, `run plan`, `task run`) no longer panics. JSON `null` read
  as an empty object, and the first `--input` then wrote into a nil map; `null`
  is now refused as not a JSON object, like every other non-object. `--input`
  and `--input-json` text that Go's JSON decoder would change on the way
  through (bytes that are not UTF-8, a lone surrogate escape) is no longer
  replaced with U+FFFD: `--input-json` refuses it and a `--input` value stays
  the string you typed. A task id given to `orkctl run plan --output` or keyed
  in its `--outputs-file` is held to the name rule (`^[a-z][a-z0-9-]*$`, at most
  200 bytes), naming the flag: an empty key in the file, and an id that is not
  text in either, were accepted and failed to encode with no word about the
  flag. Found by reading `buildInputPayload` while stating
  `FuzzBuildInputPayload`; the empty and non-text ids were written by the fuzz
  engine (`FuzzBuildAssumedOutputs`).
- A task's live log is framed the same way wherever the container's reads
  split its output. A line of exactly 64 KiB (`MaxStreamedLine`) whose
  terminator arrived in a later read streamed an extra empty line after it,
  and a line longer than 64 KiB whose terminator arrived in the same read as
  its end streamed as one line past the limit instead of in 64 KiB pieces. A
  line is now cut into pieces of at most 64 KiB however its bytes arrive, and a
  terminator right after a piece ends no empty line. Found by reading
  `LineWriter` while stating `FuzzLineWriter_FramesAnyStreamAtAnyWriteBoundary`
  against a framing written without it.
- `ORK_WS_ALLOWED_ORIGINS` is read as one list by both origin checks. It was
  split at commas and kept as written, so `a, b` left ` b` with its space; the
  socket's early check trimmed each entry and let the origin in, and the
  WebSocket library's own check, which does not trim, then refused the upgrade
  with a 403 the early check had approved. Entries are trimmed and blanks
  dropped once, a blank pattern no longer matches an Origin with no host, and
  the early check mirrors the library for a pattern that does not parse and for
  Unicode case folding of the host. Found by `FuzzOriginAllowed`, which takes
  the library as its oracle.
- **BREAKING:** A schedule's `timezone` of `Local` is refused, in the API and in
  an automation document: Go reads it as the zone of the machine that reads it,
  not a zone of the IANA database the reference promises, so the schedule fired
  at a different instant on every host. Found by reading `ValidateScheduleSpec`
  while stating `FuzzValidateScheduleSpec`.
- A configuration file's integer, boolean or duration is applied as its bare
  value. It was checked with its surrounding space trimmed and applied
  untrimmed, and not every reader trims: `engine.idempotency_ttl: ' 5m'` passed
  the file and was then silently replaced by the reader's default, and a padded
  integer stopped the daemon at its reader. A double-quoted value that spells a NUL byte
  (`"\0"`) is refused at load too, naming the key, instead of failing partway
  through applying the file. Found by reading `Load` while stating
  `FuzzLoad_AValueTheFileSetsIsOneItsKindReads`.
- A key is read only in the spelling the writer uses. The run, recent and origin
  index parsers used `time.Parse` for their stamp, which takes a comma as the
  decimal separator, the attempt and task-log key parsers used
  `strconv.ParseInt` for their zero-padded counters, which takes a leading plus
  sign, and the escaped form of a name (`~` and base64) was decoded without
  checking its padding bits or that the name was not clean enough to be written
  as itself, so two keys named one record and the cursor a paged scan resumes
  from was a key the scan had never listed. All three were written by the fuzz
  engine (`FuzzKeys_AKeyTheParserAcceptsIsTheKeyTheBuilderWrites`).
- A registry's bearer challenge is read as bytes. Splitting it ranged over
  runes, which rewrites a byte that is not UTF-8 as U+FFFD, so the token realm
  the resolver requested was not the one the registry named. Found by the fuzz
  engine (`FuzzParseBearerChallenge`).
- An Orker id that is not UTF-8 is refused at boot, naming `ORK_ORKER_ID`. It
  becomes part of a task queue name, a protobuf string, and was refused by the
  transport at the first poll instead. Found by reading `ValidOrkerID` while
  stating `FuzzValidOrkerID`.
- The feature proof's evidence selector no longer matches a changed method by
  its name alone. It reached a changed `T.m` through every test of the
  package that mentioned any identifier called `m` (a local variable, a
  function, a field, another type's method), so a test-helper method named
  `setup` or `count` made 832 unit tests and 1,040 integration tests
  evidence in one measured change, or pulled in always-skipping opt-in
  recorders and failed the proof with `evidence_without_a_pass`, and helpers
  were renamed to avoid it. A method is now matched by its receiver type and
  its name (docs/PROOFS.md §3): an exported one also by its type alone,
  since another package can call it through an interface, except a hook a
  `_test.go` file adds to a production type: only test code calls it, and it
  is matched by its selector alone. The tests that read a changed tool or
  example are found the same way. Functions, variables, constants and types
  are still matched by name, and an `init` function of a test file now
  counts as part of every test's reach.
- `proof-fetch`'s retry never ran on GitHub. The runner starts a composite
  step with `bash -e`, which ended the script at the first failed attempt,
  before the second, before the log line and before the give-up line the merger
  reads. The script turns errexit off, and a test runs it under the runner's
  shell against fakes.
- A feature proof never makes a test `scripts/proof-allowed-skips.txt` names
  into evidence. Such a test (an opt-in recorder, a helper subprocess) skips
  in every proof by design, and the opt-in history recorders read the shipped
  examples, so a change to an example they read ended `FEATURE FAIL` with
  every gate passed: `evidence_without_a_pass` named the recorders. The
  selection now lists each one a rule reaches as not evidence, with why it was
  reached and the list's reason (`not_evidence` in the selection, "Not
  evidence: allowed skips" in the README); a package whose regression set
  holds one still runs it. A `--tests` pattern that names only allowed skips
  is an invalid invocation, and a changed `test/tools/` tool that only allowed
  skips read is refused as one no test or spec runs. `proofrun select` reads
  the list with `-allow`.
- `make site-media` no longer stops at its libwebp check with an ffmpeg that
  has the encoder: the check piped `ffmpeg -encoders` into `grep -q`, and
  under `pipefail` the SIGPIPE ffmpeg 9 takes when grep stops early failed
  the pipeline. It reads the whole list first.

- `proofrun clean` now tells which test process left an activity container
  by the work root the container mounts, whose name carries that process's
  pid, rather than by the container's Orker id. Tests that name their own
  Orkers (`orker-cli-…`, `orker-pin-chaos-…`, `orker-rel-v1-…`) and every
  Playwright spec's Orker, on a fixed root under `.data/work/e2e/`, left
  containers it could not attribute: 131 had piled up on the proof host,
  about 50 of them from the day's two full proofs. A spec's Orkers now work
  in roots named like an integration test's, `e2e-<name>-p<pid>-<suffix>`,
  and a repository test refuses a spec that sets its own.
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
- The CI proof job copies the parts' records, and a failing part's events
  and logs, into the merged proof whatever the merge decided: under `set -e`
  a failing `proofrun merge` ended the step before the copy, so the proof
  that most needed its parts' details (run 40411125174) uploaded five files.
- The CI `ui-e2e` shard that drives the host's Chromium installs the browser
  without `--with-deps`, and apt-installs its system libraries only when a
  launch probe finds the installed browser does not start, each install in
  two bounded attempts: the runner image already carries those libraries
  (the apt install's only new packages were fonts), and that apt install
  from the runner's mirror at 50–100 KB/s took the shard past its 5-minute
  limit three times (runs 40346476240, 40411125174 and nightly 40413305751).
- `proofrun overhead` counts a concurrent setup group once, through its
  `setup` row, and puts the site gate among the fast gates: it summed the
  group's members as if serial and reported 88.3 minutes for a proof whose wall
  was 87.5. A lane's utilisation is never above 100%; it read 115% for the solo
  projects.
- The Playwright ledger keeps each invocation's worker lanes apart, so the time
  between the parallel and the solo invocation (the second Playwright start
  and its global setup) is no longer charged to the solo invocation's first
  spec file's wall, which the UI costs and plan read.
- A CI proof part fetches the run's own artifacts, and a `ui-e2e` part the
  browser image, through `.github/actions/proof-fetch`: a fetch that makes no
  progress is retried once with twice the limit (3 minutes, then 6, under a
  step limit of 10), every attempt is recorded in the part's `setup.log`,
  its `part.json` (`setup`) and the merged `proof.json` (`setup_retries`),
  and a fetch that fails both attempts fails the part before it starts,
  never a job re-run. An artifact download that stalled for its 3-minute
  limit (GitHub Actions run 40346476240, `activity-fixture (json)`) lost the
  whole proof to a new run.
- The CI `ui-e2e` parts drive the browser in the pinned container
  (`ui/e2e/browser-image.txt`), as a local proof does, and install the
  host's Chromium only in the shard whose files drive one
  (`run-page-ceiling.spec.ts`, `run-page-reduced-ceiling.spec.ts`,
  `network-change.spec.ts`), decided from the UI plan and required by
  `scripts/proof-part.sh` as a prerequisite of that part. Every part apt-installed the host browser's system libraries, and
  one mirror at 50 KB/s took `ui-e2e (3)` past its 5-minute limit (the same
  run) with no test started.
- The merged CI proof copies each part's records — `part.json`, gates,
  ledgers, logs, events — and GitHub's job records, never a part's
  binaries, image layouts or test artifacts, and every part upload but the
  plan's leaves its `bin/` out (the plan's holds the compiled test binaries
  the shards adopt). The proof job copied every part whole, 4.6 GB of
  mostly binaries, and its upload timed out after every part had passed
  (run 40365261654), so no CI full proof could yet produce its artifact.
- The merged CI proof carries a part's events and per-job logs only when
  that part did not pass, and the fast part keeps its unit log and events
  only when its unit gate failed, as docs/PROOFS.md §7 already said; the
  ledgers hold every test's outcome either way. With the binaries out, a
  green run's merged proof was still 921 MB of passing parts' events and
  logs (run 40388322907), against a five-minute upload limit.
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
- Findings of the new static analysis, fixed rather than silenced. `orkctl
  cache purge` assigned its namespace before a loop that always overwrote it.
  The interpreter's main loop and its failure-handler loop compared with nil
  the error `expire` always returns; the contract is now stated and the dead
  branch gone, with no change to a command or an event. The memo key copied
  the fields of task.json's each-upstream entry by name, so a field added to
  the entry would have been left out of the key silently; it now converts the
  struct, and the build stops until the key has been decided. Tests and
  tooling moved off deprecated APIs, and code nothing used was deleted.
  Two validation messages no longer end in a colon: a join with no needs ends
  `or remove the join: key`, and `.each` outside a fan-out ends `that declares
  each: (a fan-out)`.
- The image-policy integration suites no longer pull `registry:3` from Docker
  Hub at test time. Their private registry ran the floating tag, but the
  stack pins the registry image by digest and the CI proof host pulls it by
  that reference, which Docker stores without the tag; a fresh runner had no
  `registry:3`, so a Hub stall during the suite failed it outside every setup
  retry. `cosigntest.StartRegistry` now runs the exact reference
  `deploy/docker-compose.yml` pins, with `--pull never`, and takes the
  suite's infrastructure-absent path (a failure under `ORK_REQUIRE_INFRA=1`),
  naming `make up` and the proof host, when it is not on the host. The
  Temporal dev server of the continuation contract test also runs with
  `--pull never`. The boot bind probe's integration test has its docker shim
  answer the probe image's inspect in every short-budget case, so a busy
  daemon cannot fail them for a step they do not test, and the history-guard
  CPU bound is scaled under the race detector like its siblings.

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
  CI, runs the listed specs and needs its `ui-e2e` gate.
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
  `main`'s tip; and `main` is pushed to `origin` after every merge (AGENTS.md
  §11.2, §11.3; docs/PROOFS.md §6, §10). The checks `merge-check` applies to
  a full proof are unchanged.
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
  and a quickstart that needs only Docker and git.
- Getting started is a clone and one command:
  `git clone https://github.com/orkstack/orkstack.git && cd orkstack`, then
  `docker compose up --build -d --wait`. The repository's root `compose.yaml`
  builds the engine, the Orker, the UI, `orkctl` and the six first-party
  activities from the clone (`deploy/source/Dockerfile`, on the release
  images' own pinned stages), pushes the activities to a registry of the
  stack's own so that an apply pins them by digest, and starts the stack on
  loopback, with the Orker's work directory under
  `~/.cache/orkstack-quickstart`, never a shared `/tmp`; `orkctl` runs from
  its image (`alias orkctl='docker compose run --rm orkctl'`). Docker and git
  are the only prerequisites, and no release is needed. The README,
  `docs/GETTING_STARTED.md`, the site's landing page and its first tutorial
  show the same commands: the landing page reads them from the README,
  `internal/docscheck` holds the other pages to them, and
  `make quickstart-smoke` runs them as written on a fresh clone of the
  working tree, as `release.yml` does before every release. Go and Bun are
  needed only to develop OrkStack: the development stack (`make infra-up`,
  `make up`, …) is in `CONTRIBUTING.md`, "The local development stack",
  beside the Linux host setup ("Linux hosts").
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
- The interim nightly rule is retired (2026-10-27). While the nightly workflow
  did not run on GitHub, `make proof-nightly` had to run on `main`'s tip after
  every merge and pass before the next, and `make merge-check` refused a merge
  without a passing nightly proof of `main`'s tip (`NIGHTLY=`) while either
  nightly list was non-empty. Its exit condition is met: the last five runs of
  `nightly.yml` on `main` passed. `merge-check` now asks for no nightly proof,
  reads neither list, and still verifies a `NIGHTLY=` proof when it is given
  one (an empty `-nightly` value is refused, not read as no proof). The nightly
  of `main` is GitHub's: a run speaks for the commit it measured, a merge is
  not held for one of its own, and a failing one is still a failing `main`,
  fixed or reverted before anything else merges, which clears when a later run
  of the fix passes (AGENTS.md §11, §11.2; docs/PROOFS.md §6, §10).
  `docs/PROOFS.md` §12's known gaps now say what is true: the repository is
  public and organization-owned, the gate's apply has not been run, `main` is
  under a ruleset made by hand that does not require the `proof` check, the
  full proof has passed on GitHub only on `validate/full/` pushes, and
  `release.yml` and `proof-rerun.yml` have not run there, nor have the `vuln`
  jobs of `ci.yml` and `nightly.yml` or the proof's `race`, `race-repeat` and
  `fuzz` parts.

### Added

- Every release publishes what it takes to run OrkStack without building it
  (`release.yml`, after its pre-release proof and an end-to-end smoke of the
  very images it publishes): `orkctl` for Linux, macOS and Windows on amd64
  and arm64, `ork` and `orker` for Linux, as archives with `SHA256SUMS`; the
  images `ork`, `orker`, `orkctl`, `ui` and the six first-party activities on
  `ghcr.io/orkstack`, for linux/amd64 and linux/arm64, tagged `X.Y.Z` and
  `X.Y` (docs/UPGRADING.md, "Release artifacts"). The smoke runs the root
  `compose.yaml` on those images, by digest. The images are not signed yet. One commit builds the same binaries whatever the
  state of the checkout or the caller's Go environment. A release below an
  existing release of its own `X.Y` is refused, and a backport to an older
  series is published but never marked the latest, so
  `releases/latest/download/` always serves the newest release.
  `make dist`, `make release-images` and `make release-smoke` build and check
  the same artifacts on a developer's machine.
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
