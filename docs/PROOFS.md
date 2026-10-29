# Proofs and CI

`AGENTS.md` §11 states the rules: no proof, no done, and what a proof must
establish. This file is the reference for how the machinery works: the tiers,
the plan and its shards, the CI job graph, the artifacts, the procedures and
the known gaps. It is not a second statement of the rules — when the two
disagree, `AGENTS.md` wins.

## 1. What a proof is

A proof is observed evidence, never a reading of the code. A suite lists what
it will run from the compiled binary's own listing, runs it, and records one
outcome per listed test with the job that ran it. A skip is never a pass. The
ledgers are checked against those listings, so a test that quietly disappeared
fails the proof. A proof is of a **tree**: the hash of the working tree,
staged and untracked files included, must be the same when it starts and when
it ends, and a merge lands exactly the tree the gate tested.

Every proof names its tier in its directory name, the first line of its
README, its result line and `proof.json`. Evidence is never committed:
`proof/` is gitignored, and the commit or pull request quotes the proof
directory, the tree and the gates that ran. Read `proof.json`, never the
README, for a verdict.

Exit status: `0` pass, `1` a gate failed, `2` an invalid invocation, `77` a
missing prerequisite.

## 2. The tiers

| Tier | Command | Merge evidence | Runs |
|---|---|---|---|
| **feature** | `make proof-feature TESTS=… SPECS=…`; optional | no | the fast gates, the change's evidence, its regression set |
| **full** | `make proof` | yes, on a local host; in CI, the `proof` job | every gate, every test and every spec, except the nightly lists' entries the base also carries |
| **nightly** | `make proof-nightly` | no | only the tests `scripts/proof-nightly-tests.txt` names and the spec files `scripts/proof-nightly-specs.txt` names, and two lanes of its own: the race repeat and the fuzz lane (§2.3) |
| **release** | `make proof-release` | no; it is a tag precondition | every gate, the nightly tests and specs included, observability always; not the nightly lanes |

A full proof records each nightly-listed test as `deferred`, never as passed,
and only when its base's list also carries it; an entry the change adds runs.
The spec list works the same way for Playwright: a row names a spec file under
`ui/e2e/`, the merge-gate spec files that cover it at a smaller size, the
approval and the reason. The full tier's UI plan leaves out of its files and
shards each spec file both the tree's list and the base's carry, keeps its
tests planned, and the Playwright ledger records each as `deferred` with the
row; a test of such a file that ran, a deferred entry for a file the plan
runs, and a deferral the two lists do not both carry each fail the proof. The
nightly tier's plan holds the listed spec files alone; its merge reads the
tree's list itself (`proofrun merge -nightly-specs`), and while the list has
a row the nightly proof needs a UI plan read from that very list, planning
exactly its rows, and its `ui-e2e` gate — in CI one `ui-e2e` part per listed
spec file, up to the requested UI shards. A feature proof's listing holds
only its selected specs, so it plans with no spec list. Today the list holds
`run-page-ceiling.spec.ts`, the run page at the 4,000-task default ceiling;
the merge gate runs `run-page-reduced-ceiling.spec.ts`, the same measurement
with `engine.orkflow_max_tasks` lowered to 1,000.
The pre-release gate is the full tier with nothing deferred, the race gate
included. A tag requires a passing release proof of the exact commit: `make
release-tag` and the workflow both refuse without one.

The feature tier is never merge evidence, and it is optional: a scoped check
of a change on its branch that gates nothing (AGENTS.md §11.1). A local full, release or nightly
proof runs its gates on one host and writes one part, `local`, then merges it
with `proofrun merge`. The CI `proof` job runs the same merger over many
parts. There is one verdict implementation.

Every local proof of any tier, and every `scripts/integration.sh` run, first
removes what proofs and test processes that are gone left on the host — their
buckets, Orker work roots and containers — with `proofrun clean -remove`, once
its prerequisites are checked and before its own directory exists; its
`setup.log` opens with the count (§10). It never touches what a live process
owns or a running proof recorded, and the dev stack stays up. A by-hand
`go test -tags=integration` does not clean: run `make proof-clean REMOVE=1`
before it (AGENTS.md §11.1). A CI part runs on a fresh runner and does not.

### 2.1 The gates

A local proof runs its gates in this order: the **fast gates**, alone and one
after another — lint (`make lint`: `go vet`, gofmt, and staticcheck as built
and with `-tags=integration`; the vulnerability scan is not part of it, §5),
build, unit, `go vet -tags=integration`, then the UI
type-check and unit suites (after one `svelte-kit sync`, which both read), then
the site gate, then the race gate (§2.3); then the **setup**, as one
concurrent group — the proof's own Temporal, the activity images, the binaries
and the integration test binaries (`proofrun compile`), each its own row of
`gates.tsv` (`temporal`, `images`, `binaries`, `integration-compile`) and the
group's wall a `setup` row; nothing in it is load-sensitive, no test runs until
it has ended, and a failure of any member stops the others. Within it, `make -C
images push` builds and pushes the six images `IMAGE_JOBS` (default 6) at a
time (`make -C images oci`, a CI build part's, does the same), and one `go
build -o bin/` links the nine stack binaries; then the integration suite with the activity contract
fixture beside it — the only two gates that overlap, and the fixture starts
only once the integration runner has finished its heavy jobs
(`integration-heavy-done`), so a heavy test's quiet host never carries the
fixture's containers; then Playwright, spec files `PROOF_UI_WORKERS` at a
time; then the observability gate. A full proof runs: lint; build; unit; `go vet -tags=integration`; the activity
images, built once from this tree; every integration test whose package's
integration build differs from its unit build, against real Temporal and the
S3-compatible store; the activity contract fixture; the UI type-check, unit
and every Playwright spec; the site's check and build; the race gate; and the
observability gate when the tree changes a telemetry path. A release proof runs
the observability gate unconditionally.

The proof's own Temporal is the `temporal` service of
`deploy/docker-compose.yml`: its image and its command, read by `proofrun
temporal-args` and started on a port of its own, never a second copy of them
in a script. The command carries two `--dynamic-config-value` settings that
make a worker-deployment version report `Drained` in seconds rather than in
the cluster's default minutes, so the integration tests that wait for a
release to drain (`orkctl release wait`, the continuation chain's rollout) wait
seconds (docs/DEPLOY.md §4, "Underneath"). They are development and proof
settings, not a deployment's.

The **site gate** runs `bun run check && bun run build` in `site/`, logged to
`site.log`: the docs sync, the design-token drift check and `astro check`, then
`astro build` with link validation and a post-build link check. The site
renders the repository's markdown (`docs/`, every `*.md`), the example
Orkflows, `deploy/orkstack.defaults.yaml` and `docs/API.md` (generated from
`proto/`), and takes its whole style from `ui/src/tokens.css`, so a change to
any of them can break it. It needs `bun` and `node` on PATH and
`site/node_modules` (`cd site && bun install --frozen-lockfile`), as the UI
gates need `ui/node_modules`; without them the proof exits `77` before it
writes anything. The full and release tiers always run it, and `proofrun
merge` requires it, so a full proof without a passing `site` gate is not
merge-eligible. The nightly tier does not run it. The feature tier runs it
when a changed path's scope row names `site` (§3). In CI it is part of the
`fast` job, recorded beside the UI gates; that job and `ci.yml` › `feature`
install the pinned Node and `site/node_modules` for it.

Integration tests run as separate processes, `PROOF_JOBS` at a time (default
6; the ordinary jobs keep every slot busy, so the phase's wall is their total
work divided by the slots). Six was first measured with one engine boot in
277 jobs stalled 50 s on its fresh bucket's first read while SeaweedFS grew
and removed per-bucket volumes, and a merge gate is never rerun, so the
default stayed 4 until that stall was tolerated (below) and passing jobs'
buckets were removed while the phase runs. At 6 the proof host, a 2019
MacBook Pro, then ran two full proofs clean: train-a-full in 79.0 min
(integration 2805 s) and train-b2-full in 71.2 min (integration 2760 s).

That stall class is tolerated, and still recorded. Every wait for a process a
test started (`internal/testsupport/bootwait`) allows the boot 180 s, ends at
once when the process has exited instead of polling it, and logs how long the
boot took, with a `WARN` line past 20 s that `proofrun overhead` lists under
"Slow boots". A test binary's bucket create (`s3bucket.Main`) is allowed 120 s
and warns the same way, and the engine's two boot reads log `list_took`, so a
slow boot says which read stalled.

A test process's bucket is removed by the runner, off the process's slot,
because removing a bucket is the store's heaviest per-bucket operation and its
churn was the stall. `proofrun integration` (a local proof, and each CI shard)
gives every test process a file to name its bucket in
(`ORK_TEST_BUCKET_LEFT_FILE`, `integration/<job>.bucket`). A process whose
tests passed writes its bucket's name there and exits (`s3bucket.Finish`); one
with a failure keeps its bucket and writes nothing, as before. The runner's two
sweepers remove those buckets while the phase runs, two minutes each at most,
asking the store to delete the bucket whole and emptying it object by object
only when the store refuses a non-empty one. They do not wait for the phase to
end, because a bucket holds at least one of the store's volumes until it is
removed and a store has a bounded number of them: a SeaweedFS mini server
sizes it from its free disk (290 on the 2019 MacBook Pro host), so a phase that
kept its 343 passing buckets to its end ran out, and every write failed with
`No writable volumes`. A slot hands its process's bucket over before it starts
the next process and waits while four already wait, so the buckets alive at
once never exceed the slots, that backlog and the two removals in flight. When
the phase's last process has ended, still inside the gate's wall, the runner
removes what the sweepers had not reached. It removes only a name of the shape
`s3bucket.Name` makes, never the development bucket. The `bucket sweep:` lines
of `integration.log` say what it kept, what it left (`left s3://… : <error>`),
how many went while the phase ran, and end with
`removed N of M passing job buckets`; a removal that fails costs disk and never
the verdict, and a run a signal stopped removes no more and leaves the rest
named in its hand-off files (§10).

Playwright spec files run on
`PROOF_UI_WORKERS` workers (default 2 — measured clean on a 16-core host,
while at 3 one load-sensitive spec failed in each of three runs): the browser
runs in a network
namespace of its own (`ui/e2e/browser-server.ts`, from the image
`ui/e2e/browser-image.txt` pins; a proof pulls it when the host lacks it), and
every spec file owns its bucket. The spec files that assert a render budget
(`each-fan-out-ceiling.spec.ts`, `run-page-reduced-ceiling.spec.ts` and, where
it is not deferred, `run-page-ceiling.spec.ts`) hold nothing but their budget
test and run alone after the others, as `--project` runs on one worker; the
fan-out's functional tests are `each-fan-out.spec.ts`, which runs with the
others. The run page at the task ceiling runs on the host's Chromium
(`ui/playwright.config.ts`); its two spec files share their steps in
`ui/e2e/runPageCeiling.ts`. `network-change.spec.ts` launches a host-namespace
Chromium of its own as its positive control; every other spec drives the
container. A CI `ui-e2e` part installs the host's Chromium only when its
shard holds one of those three files, decided from the UI plan by the same
rule `scripts/proof-part.sh` applies as a prerequisite of the part:
`playwright install chromium`, the browser from Playwright's own store and
nothing from an apt mirror, because the runner image already carries its
system libraries and `--with-deps`'s apt install from that mirror stalled
the shard past its limit three times (runs 40346476240, 40411125174 and
nightly 40413305751). A launch probe then decides: only a Chromium that
does not launch falls back to `--with-deps`, each install in the two
bounded attempts `proof-fetch` makes (180 s, then 360 s) under a 10-minute
step, and a browser that still does not launch fails the step before the
part.
The gate starts one browser server for both invocations, and the second reuses
the first's UI build when the hash of the build's inputs
(`ui/e2e/build-hash.ts`, written beside `ui/build`) is unchanged. A proof that
runs both integration and Playwright makes that build earlier, while its
ordinary integration jobs run: once `integration-heavy-done` exists (the
fixture's policy, so never beside a heavy test), or once the integration gate
has passed without one, it runs `bun e2e/build-ui.ts` — the same
`ensureUIBuild` the global setup calls — and `playwright test --list` into
`ui-e2e-list.json`, logging both to `ui-prebuild.log`. The Playwright gate
waits for it to end, then uses that listing and finds the build's hash
matching (its log reads `e2e: ui/build is this tree's own (inputs …): not
rebuilt`). It is a head start, never a gate: a failed or interrupted build
leaves no matching hash and a failed listing leaves no file, so the gate
builds and lists for itself as before. A **heavy** test — one whose assertions are
bounds — runs alone, in its own CI shard or first on a local host. A test is
heavy by what it asserts, not by how long it takes: the reduced-ceiling copies
of a moved soak run the same E4/E7 thresholds and are heavy, while a test that
merely waits on a deadline for progress is not. A new test that asserts such a
bound is added to `heavyTests` in `test/tools/proofrun/plan.go`.

### 2.2 Fail fast

A proof is never rerun, so a local proof that has failed a gate can never
become eligible, and everything after that failure only costs time. By default
(`PROOF_FAIL_FAST=1`) `scripts/proof.sh` therefore stops at the first failing
gate, in every tier it runs on one host (feature, full, nightly and release):

- a failure in a fast gate starts no other gate, and nothing long has begun;
- a failure of a setup member (Temporal, images, binaries, the integration
  test binaries) stops the members still running, as integration and the
  fixture stop each other below, and starts no other gate. A member that is
  a shell function ends at once on `SIGTERM`, so the script also signals the
  processes it found under that member's pid before stopping it;
- while the integration suite and the activity fixture run, a failure of
  either stops the other: the script sends `SIGTERM` to the command pid it
  recorded for that gate (never a name or a pattern), waits up to a minute, and
  only then kills what is left of that pid's process tree;
- `proofrun integration -fail-fast` (passed by the script) stops at the first
  failed test no quarantine entry excuses: it ends the running test processes,
  starts no more, and still writes the whole ledger. A test it ended is
  recorded `none` with "not finished", a test it never started with "not
  run", each naming the failure; neither is a pass, and the ledger fails the
  proof. `proofrun integration` ends the same way on `SIGTERM` or `SIGINT`,
  which is how the script stops it;
- Playwright and the observability gate start only while nothing has failed.

The verdict is still written, and it is a failure: `proof.json` has `result:
fail` and `merge_eligible: false`, the README names the failed gate and has a
"Stopped early" section, and the script exits `1`. A gate that was running
when the proof stopped is recorded `stopped` (`stopped: <gate> failed`); a gate
that never began is recorded `not-started` (`not started: <gate> failed`).
Neither is a pass, and `proofrun merge` lists each in `ineligible_reasons`.
When the proof stopped before `proofrun integration` wrote its plan, there is
no plan to merge against: the script runs `proofrun merge -stopped`, which
writes the failing `proof.json` and README from the `local` part alone.

`PROOF_FAIL_FAST=0` runs every gate to its end, in the same order. It changes
no eligibility rule: a proof whose gates all pass is identical either way.
Unit tests are not part of this: the unit suite runs to its end whatever fails
in it. CI parts (`scripts/proof-part.sh`) are unchanged, each part being one
job.

### 2.3 The race gate and the nightly lanes

**The race gate** (`race`) is the unit suites run under the race detector:
`proofrun unit -race -short`, over every package the unit gate lists, once,
into `race-tests.json`. The full and release tiers run it, in CI as the
`race` part on a runner of its own beside `fast` (§5); a feature proof runs it
over the packages its change touches, and not at all when the change touches
none (`proofrun gates -select` prints them as `race_packages`). It is the unit
gate's ledger, a listed test without an outcome failing it, with these
differences:

- **A data race fails it, whatever the tests reported.** The ledger counts
  every `WARNING: DATA RACE` the detector printed — inside a test, or outside
  every test, where a goroutine that outlived them printed it — and records a
  problem naming the test or package for each. The detector also marks the
  running test failed, but a gate that read only test outcomes would pass the
  second kind.
- **`-short` is on.** The scale tests the unit gate runs without it (the
  history-guard, ten-thousand-item and closure-cost tests) are single-thread
  CPU work whose assertion is their own cost, and the detector multiplies that
  several times over. The ledger records that its run had `-short`
  (`"short": true`), and only then is a skip no allowed-skip entry names
  recorded, with the reason "skipped in a run with `-short`", rather than
  refused: the ledger does not know why a test skipped (the test's own reason
  is in the gate's log), and a run without `-short` follows the unit gate's
  rule, a skip being a problem. A recorded skip is not a problem of the race
  ledger and never a pass, and `proofrun merge` holds each one to the unit
  ledger: it accepts the skip only from a ledger that says its run had
  `-short`, and the unit gate, which runs the test without `-short`, must have
  passed it. A test the unit ledger lists and the race ledger does not, a
  skipped test the unit ledger did not pass, and a skipped test it does not
  list each fail the proof, so no test is run by neither lane. A skip an
  allowed-skip entry names (an `any` entry, or a `unit` entry, which covers
  the race suites too) stands on that entry, as in the unit gate. A subtest
  skipped under `-short` is counted and not refused. A test the detector cannot carry stays in both ledgers: it
  skips itself under the detector, or scales its bound by a stated factor (the
  engine's `raceBuild` and `raceSlowdown`), never drops it; a build tag that
  removes it from the race build is refused, because the ledger then lacks it.
- **No quarantine excuses it.** The race ledger is built with none, and the
  merger refuses a `quarantined-fail` entry in it.
- **`-timeout` is 30 minutes for each run of a package's tests**, where
  `go test` gives ten minutes in all. `internal/engine` under the detector
  can exceed ten minutes on a smaller runner, and the timeout covers a test
  binary from its start to its end across every `-count` repeat, so the
  race repeat's three runs get 90 minutes (`raceTimeoutFor` in `run.go`).
  It is a hang guard and not a budget. `make test-race` gives `go test` the
  same figure for its `RACE_COUNT` (`RACE_MINUTES_PER_RUN`, which a test holds
  equal to the gate's). The plain unit gate gives each package's binary 15
  minutes (`unitTimeout`) for the same reason: `internal/engine`'s unit suite
  took 460 s on a 4-vCPU runner and passed ten minutes on a slower one (CI run
  40749987883). Fifteen stays well under the `fast` step's 30 (raised from 20
  on 2026-10-28: that run's unit gate alone took 689 s), so a hang ends in
  `go test`'s own trace naming the test.
- **It sets no `ORK_PROOF_DIR`.** The measurement files the unit gate's tests
  write there are the unit gate's evidence; a measurement taken under the
  detector is evidence of nothing, and must not overwrite them.
- **It needs cgo.** The detector links C code, so the gate refuses to start
  (exit `77`, before anything is written) without `CGO_ENABLED=1` and a C
  compiler on `PATH` (`go env CC`). The unit gate needs them too, in a local
  proof of any tier that runs it and in the CI `fast` part, because
  `proofrun`'s own tests run `go test -race`; a feature proof whose change
  touches no Go package races nothing and still runs the unit gate. The
  `ubuntu-24.04` runners and a developer host have one.

*Where it runs.* A local proof runs it last of the fast gates, alone: after the
site gate and before the setup, never beside another gate. The detector
multiplies every test's time, so beside the integration phase it would put
about 1,700 CPU-seconds on top of six load-sensitive integration jobs, and a
race found there would end the proof only after the long gates had run; alone
and early it costs wall time and ends a racing proof in the first minutes
(§2.2). The cost is the longest package, `internal/engine`, plus the build, and
the only measurements are two runs on a loaded 16-core host, 573 s and 632 s
of wall (§11.5): plan about ten minutes on a local proof of 70–80, that is
about one eighth of it. A CI proof pays nothing on its critical path, the
`race` part running beside `fast`. Nobody has measured it on a quiet host; the
first is the `race` row of a full proof's `gates.tsv`, and a placement beside
the integration phase is worth another look only if that row is far above ten
minutes.

**The nightly lanes** belong to the nightly tier alone; the pre-release gate
runs the race gate and not these. Each runs alone, before the integration
phase, and is one CI part (§5):

- **`race-repeat`** is the same run over the concurrency-heavy packages with
  `-count 3`: `./internal/engine/...`, `./internal/orker/...`,
  `./internal/api/...` and `./pkg/...` (`RACE_REPEAT_PACKAGES` in
  `scripts/proof.sh`), into `race-repeat-tests.json`. A race that one run in
  several exposes is what it looks for. One test name reports once per
  repetition, so the ledger keeps the worst outcome of a test's repetitions
  (fail, then skip, then pass) and adds up its seconds: a failure in any
  repetition stands, and a repetition that started and never reported (the
  binary ended, hung or timed out inside it) leaves its test with no outcome,
  whatever an earlier repetition reported. The run is `-short`, as the gate
  is, and records its `-short` skips the same way. The nightly proof has no
  unit ledger, so nothing holds those skips to a pass: a full proof of the
  same tree holds the same skips to its unit gate, and here they only narrow
  what the lane hunts, are counted in its ledger summary, and are refused
  from a ledger that does not say its run had `-short`.
- **`fuzz`** runs `proofrun fuzz -budget $FUZZ_BUDGET` (default `105m`; a
  whole number of `s`, `m` or `h`; the `nightly.yml` dispatch has a
  `fuzz_budget` input): every `Fuzz*` target `go test -list '^Fuzz' ./...`
  finds, one after another, and the lane's whole wall-clock time is the
  budget, shared between them, so the lane ends inside its CI step however
  many targets the module has. Before each target the lane takes an equal part
  of the budget that remains, less the overhead of starting a target (the
  build and link of its instrumented test binary, which `go test -fuzztime`
  does not count), in whole seconds and never under the 15-second minimum.
  The overhead is assumed to be 45 seconds until three targets have been
  measured, and is then the median of what the targets measured (elapsed less
  fuzz time), so one cold build does not starve the rest; what an early
  estimate over- or under-spent is shared out among the targets after it,
  which is why the first targets get the shortest turns. The clock starts when
  the lane does, so finding the targets counts against the budget. A target
  starts only if the budget left covers the minimum and the overhead; a lane
  that spends its budget with targets unstarted fails, naming each one as "not
  fuzzed", and never skips a target quietly. A target that has started may
  run ten minutes past its fuzz time (its build, and the minimization of a
  crasher) before the lane stops it, so the lane ends at most that long past
  its budget; the fuzz step's limit is the budget plus that and five minutes
  for the part's own work and the ledger write, and
  `TestFuzzLane_TheBudgetAndTheModulesTargetsFitTheFuzzStep` fails when the
  budget and that margin exceed the step's `timeout-minutes` in `proof.yml`,
  when the places that set a budget (`proof.yml`, `nightly.yml`,
  `scripts/proof.sh`) disagree, or when the module's `Fuzz` target count times
  the minimum plus the assumed overhead exceeds the budget. A new target
  therefore fails a unit test before it can make a nightly red; the answer is
  a larger budget with a longer step and job limit, or fewer targets. The
  unit test reads the budgets the repository sets, not the one a dispatch
  supplies: a dispatched `fuzz_budget` above 120 minutes ends in the step's
  timeout, with no ledger.
  The CI budget is 105 minutes for the 82 targets (`go test -list '^Fuzz'
  ./...`) of this tree: a turn of about 77 seconds, of which the first
  targets fuzz 31 beside the assumed 45 seconds of overhead, and later ones
  more, since a measured overhead is usually well under the assumed one. The
  fuzz step is limited to 135 minutes and its job to 155. Its ledger,
  `fuzz-tests.json`, has one entry per target with the `seconds` it took and
  the `fuzz_seconds` it was given, and each target keeps its log under
  `fuzz/`; the lane's own log line for a target says the fuzz time it was
  given, how long it took, and what is left of the budget. A target the engine
  breaks fails the lane, whatever its share. The engine writes the failing
  input into the package's `testdata/fuzz/<Target>/`, a file that would turn
  the proof's tree into another; the lane moves it to
  `crashers/<package>/<Target>/` in the part's directory, which the part's
  artifact carries (§7), and leaves the tree as it found it. To reproduce,
  copy the file back into the package's `testdata/fuzz/<Target>/` and run
  `go test -run '^<Target>$' <package>`. A lane that finds no `Fuzz` target
  fails: fuzzing nothing proves nothing. `proofrun fuzz -fuzztime DURATION`
  (or `Nx` iterations) instead gives every target that time and no budget,
  for a local run or a smoke; the proofs never use it.

`make test-race` runs the race gate's `go test` without its ledger
(`RACE_COUNT` and `RACE_PACKAGES` give it the repeat's shape), and `make fuzz`
runs the lane with `FUZZ_BUDGET` (default `45m`; `FUZZTIME=30s` gives every
target that time instead), keeping its logs and crashers under `FUZZ_OUT`.

## 3. How `proofrun select` picks evidence

`proofrun select -base REF -out FILE` reads the diff from the merge base and
`scripts/proof-scope.tsv`, and writes the selection the feature proof runs. A
path no row matches, or a row whose action is `full`, refuses the feature tier
(exit `2`): `scripts/`, `.github/`, the Makefiles, `internal/testsupport/` and
`test/tools/proofrun/` change how every suite runs and need the full proof.

The scope table's actions: `go`, `proto`, `ui`, `spec`, `ui-unit`,
`all-specs`, `images`, `examples`, `tool`, `observability`, `site`, `none` and
`full`. A row may name several: `docs/**` is `none,site`, `ui/src/tokens.css`
is `ui,site`. The `temporal` flag marks a package whose evidence must include an
integration test. A `docs` flag marks a row whose change should come with a
documentation change; the proof prints a WARNING for a missing one and never
fails on it.

The selection is:

- **the change's evidence**: every test whose source the diff changes,
  including tests that reach a changed or deleted helper, type, variable or
  `TestMain` through their package's declarations; every test named with
  `-tests` (the pattern must match whole test names). A test is a `Test`
  function or a native fuzz target (`func FuzzXxx(f *testing.F)` in a
  `_test.go` file, AGENTS.md §11.1): go test runs a target's seeds, `f.Add` and
  `testdata/fuzz/<Target>/` alike, as an ordinary test, so a changed target,
  one that reaches a changed helper, and `-tests PKG:FuzzXxx` are evidence like
  any other, a deleted target is removed, and the unit gate's ledger lists the
  target once and records its outcome; every spec the diff
  changes or `-specs` names; every spec when the Playwright harness or the UI's
  dependencies changed; the activity fixture when `images/` or `test/fixtures/`
  changed; the tests that read a changed example, and the examples spec; the
  tests and specs that run a changed `test/tools/` tool; the observability
  gate when a telemetry path changed; and the site gate when a changed path's
  row names `site` — `site/`, `docs/` and every `*.md`, `examples/`,
  `deploy/orkstack.defaults.yaml`, `proto/`, `ui/src/tokens.css`,
  `ui/src/lib/icons.ts` and `ui/package.json`, which the site renders, imports
  or takes its fonts and style from. A deleted path counts. The gate is that
  path's whole evidence on that account: it asks for no Go test or spec, and a
  path whose row also names `go` or `ui` keeps those requirements. The README
  names the paths each gate ran for, under "Evidence for each changed package
  and path". `proofrun gates -select` prints `site=yes` or `site=no` for the
  script;
- **the regression set**: in every integration package whose test binary
  depends on a changed package — through its imports or a binary its suite
  builds — every test declared in an integration-tagged file, except the slow
  list's (`scripts/proof-slow-tests.txt`). Those are recorded in the
  selection's `slow_omitted` and the README says "left out: on the slow list";
  they are never called deferred, a word that belongs to the nightly tier.

A test reaches a changed helper through the package's top-level declarations
in the build configuration the test compiles in, by syntax alone (no type
checking), and over-selects where it cannot tell. A function, variable,
constant or type is matched by its identifier: any mention of the name, a
selector's included, reaches it. A method is matched by its receiver type and
its name, never by its name alone: a function, a local variable, a field or
another type's method called `m` is not a reference to `T.m`. A changed `T.m`
is reached by a test whose reach mentions `T` and selects `m` (`x.m`, `T.m`,
`(*T).m`); when `m` is exported, by a test whose reach mentions `T` at all,
because another package's code can call an exported method through an
interface the type satisfies and no declaration of the package names that
call; and, when `T` is an alias or is not declared in the package's files of
that configuration, by the selector alone. A method a `_test.go` file adds to
a type a non-test file declares (an `export_test.go` hook) is reached by its
selector alone, whether or not the test names the type: it exists in the test
build only, so production code does not call it through an interface, and the
test's value of the type may come from another package. A deleted method is
matched the same way. Every `init` function of a test file is part of every
test's reach, since it runs before each and can register a type nothing else
mentions; one of a non-test file is not, because it cannot mention a
declaration of a test file.


The selection also names the Go packages the change touches
(`race_packages`, each as a `./dir` pattern): the packages the feature proof's
race gate runs (§2.3). A directory under `testdata` and a package the change
deleted are not among them, and a change that touches no package leaves the
list, and the gate, out.

The slow list also holds back evidence that is there only because it reaches
a changed helper, type or variable: such a test's own source did not change,
so the merge proof runs it. Its `slow_omitted` entry carries `reached`, the
reach that selected it, and the README line ends "evidence only because it
reaches changed <name>; the merge proof runs it". When every test such a
change reaches in a package is slow, the cheapest stays as evidence, marked
"kept: the cheapest of the slow tests the change reaches", so the change
still has a test that exercises it. A test whose own source changed, and
every `-tests` name, always runs; a changed `TestMain` still runs the whole
package, slow tests included. On a shared helper such as `newChaosStack`
this is about 35 minutes of a feature proof.

A test `scripts/proof-allowed-skips.txt` names is never evidence, by any of
the rules above: it skips in every proof by design (an opt-in recorder, a
helper subprocess), so it could never pass as evidence. Each one a rule
reaches goes in the selection's `not_evidence` with the reason it was reached
and the list's reason, and the README lists it under "Not evidence: allowed
skips"; a package whose regression set holds it still runs it, and its skip
is recorded as allowed. The opt-in recorders read the shipped examples, so
without this an example change could never pass a feature proof. A `-tests`
pattern that names only allowed skips is an invalid invocation, and a
changed `test/tools/` tool that only allowed skips read is refused as one no
test or spec runs.

The regression set is never evidence. Every changed package with a change
outside its tests and its comments needs at least one evidence test whose
binary depends on it, and it must be an integration test when the package is
Temporal- or storage-facing. A changed `ui/src/` file needs an evidence spec,
except that `ui/src/lib/**/*.ts` may be proven by a changed or named UI unit
test. Missing evidence, a pattern that matches no test, and a spec file that
does not exist are invalid invocations.

## 4. The plan and the sharding

`proofrun integration -plan-only` compiles and lists the integration packages
once and writes `integration-plan.json`, format 2. A local proof compiles them
earlier, while its Temporal starts and its images push: `proofrun compile -out
DIR` (`-select FILE` for a feature proof's packages) compiles and lists the
same package set into `DIR` and writes `compiled.json` last — the working tree
it compiled, and each package's binary, binary SHA-256 and listing SHA-256.
`proofrun integration -bin DIR` adopts those binaries only when the manifest
names this working tree, exactly the packages it plans, and binaries whose
bytes and listings still hash as recorded, and fails on any mismatch; a
directory with no manifest is compiled into as before. The plan records the
adopted binaries' SHA-256 exactly as it records its own compile's:

- `tree`, `commit`, `tier`, `proof_id`, `base_ref`, `base_commit`,
  `merge_base`, `observability_required`;
- `costs`, `nightly`, `quarantine` and `allowed_skips`, each with the file it
  read and the SHA-256 it hashed;
- `parts_expected`: the job graph a CI proof expects, one entry per part;
- `packages`: each package's import path, directory, test binary, binary
  digest and listing;
- `expected`: the tests the proof runs plus the deferred ones, each once;
- `deferred`: the nightly entries both the tree's and the base's list carry;
- `unmeasured`: tests with no costs row;
- `packages[].listing_sha256`: the hash of each package's `-test.list` output,
  which every shard's own listing must equal;
- `jobs`: one job per test, with its package, estimated seconds, timeout and
  heavy flag;
- `shards`: the jobs of each shard and its makespan;
- `problems`: anything the plan already knows is wrong.

The sets, named once: **listed** is what a package's integration binary
reports from `-test.list`; **run** is the tests in `jobs`; **deferred** is the
nightly-listed tests the base also lists; **expected** is `run ∪ deferred`;
**out of tier** is, in the nightly tier, every listed test the list does not
name.

`splitShards` sorts jobs longest first and puts each into the least-loaded
shard, a tie going to the lowest index. The floor is the longest single job;
no number of shards goes below it. The same inputs always give the same plan.
A heavy job goes into a heavy-only shard, at most half the requested count,
one heavy test at a time.

`costs` reads `scripts/proof-test-costs.tsv`. An unmeasured test costs 300 s
when it is declared in an integration-tagged file and 30 s otherwise. The
costs file is regenerated from a proof's ledger; the plan reproduces from the
commit.

`proofrun ui-plan -list FILE -shards M -out FILE` splits Playwright spec files
by their measured wall — their tests' seconds plus the gap before each test in
its own worker lane — and passes each shard a file list. Playwright's own
`--shard` is never used. `-nightly-mode` is the tier's (`defer` for the full
tier, `only` for the nightly tier, `include` otherwise), `-nightly-specs` the
tree's spec list and `-nightly-specs-base` the base's copy beside the plan,
which `defer` needs; the plan records both lists' hashes and rows and its
`deferred` files, and exits 1 on a stale row. `-run-files-out FILE` writes the
spec files a local run names on Playwright's command line: the plan's files
when it leaves a listed one out, and nothing otherwise. Each invocation's worker lanes are its own: when a
proof runs the parallel project and then the solo projects, the time between
the two invocations belongs to no file.

## 5. The CI job graph

`.github/workflows/proof.yml` is one graph behind the full, nightly and release
tiers. It triggers on `pull_request` (opened, synchronize, reopened,
ready_for_review — never `labeled`), `merge_group`, a push to
`validate/full/**`, `workflow_dispatch` and `workflow_call`.

```mermaid
flowchart LR
  base --> plan & build & fast & race & race_repeat["race-repeat"] & fuzz
  plan --> integration & ui_e2e["ui-e2e"] & observability
  build --> integration & ui_e2e & activity_fixture["activity-fixture"] & observability
  base & plan & build & fast & race & race_repeat & fuzz & integration & ui_e2e & activity_fixture & observability --> proof
  nightly["nightly.yml › nightly"] -. calls .-> callee["proof.yml"]
  gate["release.yml › gate"] -. calls .-> callee
  validate["release.yml › validate"] --> gate --> tag["release.yml › tag"]
  validate --> artifacts["release.yml › artifacts"] --> tag --> publish["release.yml › publish"]
  artifacts --> publish
  nightly --> flakes["nightly.yml › flakes"]
```

Every job checks out `github.sha` — the merge commit on a pull request, the
group's head on a merge group, the pushed commit, or the chosen branch's tip
on a dispatch or a call — so every part tests one commit, and the merger
checks the tree anyway. Every job but `proof` carries the same guard: on a
pull request it runs only when the pull request is not a draft and, while the
repository is private, carries the `ci:proof` label.

| Job | Runs | Uploads |
|---|---|---|
| `proof.yml` › `base` | decides the base: a pull request's first parent, which must be an ancestor of its head ("update this branch") unless `MERGE_GATE_MODE` is `queue`; a merge group's base; a validate push's first parent; a dispatch's input | nothing; job outputs |
| `proof.yml` › `plan` | compiles and lists the packages; writes both plans; decides observability | the plans, the listing, the base lists |
| `proof.yml` › `build` | builds every activity image once as an OCI layout and records its digests | the layouts and `images.tsv` |
| `proof.yml` › `fast` | lint, build, unit (no `-short`), vet-integration, UI type-check and unit, the site's check and build | one part |
| `proof.yml` › `race` | the race gate (§2.3), on a runner of its own; the full and release tiers | one part |
| `proof.yml` › `race-repeat` | the nightly race repeat (§2.3); the nightly tier alone | one part |
| `proof.yml` › `fuzz` | the nightly fuzz lane (§2.3), `FUZZ_BUDGET` shared by its targets; the nightly tier alone | one part, with the crashers it found |
| `proof.yml` › `integration` | one matrix entry per shard: its own Temporal, store, registry and images | one part per shard |
| `proof.yml` › `ui-e2e` | one matrix entry per UI shard: its own stack and file list; the pinned browser image pulled, the host's Chromium installed only where the shard's files drive it (§2.1) | one part per shard |
| `proof.yml` › `activity-fixture` | one matrix entry per activity: `make -C images contract` | one part per activity |
| `proof.yml` › `observability` | when the plan says it is due, or always for release; its `observability stack` step starts the profile and waits up to 3 minutes until the Collector, Loki, Tempo and Prometheus answer ready, the readiness `proof.sh` requires | one part |
| `proof.yml` › `proof` | `if: always()`; downloads every part and GitHub's job records; runs `proofrun merge`; writes the summary | the merged `proof.json` and ledgers |

`proof` is the single required check. Its last step judges each tier by its
own rule: a full proof must say `result: pass` and `merge_eligible: true`; a
release proof must pass `proofrun verify -release -proof DIR` against the
commit the job checked out, the rule the `tag` job applies; a nightly proof
must say `result: pass` and `eligible_but_for_tier: true`, a pass with every
identity rule held and its tier the only thing it is not. The last step reads
the tier from `proof.json` and dispatches on it, so the full branch alone
requires `merge_eligible`, and any other tier fails. No job or step of these
workflows sets `continue-on-error`, and every `upload-artifact` step of
`proof.yml` runs `if: always()`, so a stuck shard still leaves its part. A
skipped required job reports success, which is why
`proof` runs on `always()` and nothing else, and why on a draft, or without
`ci:proof` while private, it fails with the reason instead of skipping.

`ci.yml` holds no required check:

| Job | Runs |
|---|---|
| `ci.yml` › `check` | `make check` on every push and every pull request: no Docker, no Temporal |
| `ci.yml` › `vuln` | `make vuln` (the pinned `govulncheck`) on every push and every pull request. It reads the vulnerability database over the network, so it is a job of its own and in no proof gate: a network failure cannot fail a proof |
| `ci.yml` › `feature` | the optional feature tier: on a pull request only with the `ci:feature` label, on a push to `validate/feature/**`, and by dispatch; it gates nothing |

`site.yml` is not part of the proof and holds no required check; it builds and
publishes the site:

| Job | Runs |
|---|---|
| `site.yml` › `build` | `bun run check && bun run build` in `site/` on every pull request that touches a path the site reads, and on a push to `main`; a pull request uploads the built site as its preview, a push to `main` uploads the Pages artifact |
| `site.yml` › `deploy` | on `main` only (a push, or a dispatch from `main`), after `build`: publishes the Pages artifact. It holds the workflow's only write permissions (`pages`, `id-token`) and checks out and runs no repository code |

The proof's site gate (§2.1) runs the same command, so a change that breaks the
build is stopped before it merges; the workflow publishes only a build that
passed.

`proof-rerun.yml` is not part of the proof and holds no required check; it
asks GitHub for a new attempt of a failed candidate proof, when nothing in it
recorded a result (§10):

| Job | Runs |
|---|---|
| `proof-rerun.yml` › `rerun` | on the completion of a failed `proof` run, only when the repository variable `PROOF_AUTO_RERUN` is `true`, the run was a push to `validate/full/**` of this repository, and its run attempt is below 3 (so at most two automatic reruns). `proofrun rerun-plan` reads that attempt's jobs, annotations and logs through `gh api`; the job runs `gh run rerun --failed` only when every failed job is the `proof` job or a part the merger's own classification calls lost, setup-failed or degraded. It holds `actions: write` and `contents: read`, and `checks: read`, because the evidence is a check-run annotation (the `proof` job takes the same permission); it checks out the default branch, never the candidate, and runs none of the candidate's code |

`podman.yml` is not part of the proof and holds no required check; it runs the
runtime-facing suites on Podman, OrkStack's second engine
([DEPLOY.md](DEPLOY.md), "Running on Podman"), on `ubuntu-26.04`, whose image
ships Podman 5:

| Job | Runs |
|---|---|
| `podman.yml` › `podman` | on a push or pull request that touches `internal/orker/`, `cmd/orker/`, `orkctl task run`, `test/fixtures/` or the workflow, at 05:41 UTC every day, and by dispatch; one job per shape: `rootless-cli` (the podman CLI as the runner's user, `ORK_DOCKER_CLI=podman`), `rootful-cli` (the podman CLI as root through `sudo`) and `rootful-socket` (the docker CLI against the rootful Podman socket, the release image's shape). Each runs `scripts/integration.sh ./internal/orker` and the Bash activity contract fixture; the CLI shapes also run `./cmd/orker` and `orkctl task run`'s tests. The socket shape runs `internal/orker`'s runtime tests only: its image-policy tests start a plain-HTTP registry on a port chosen at run time, which the docker CLI cannot tell Podman to trust. A rootless runner that cannot start a container (AppArmor's restriction on unprivileged user namespaces) fails that job and says so in its summary; a rootful job names its `sudo` in its summary, boots the real `orker` on the runner's AppArmor profile and fails unless its boot probe refuses it (a container's init cannot signal its workload there), applies the fix of DEPLOY.md "Rootful Podman under AppArmor" and fails unless the `orker` then boots, and, when it fails, prints the kernel's AppArmor denials. Docker hosts the stack the suites use; Podman is only the engine under test |

`kind-orker.yml` is not part of the proof and holds no required check; it runs
`scripts/kind-orker.sh` on `ubuntu-24.04`, which makes a throwaway kind
cluster and runs `scripts/k8s-orker-check.sh` against it: the check an
operator runs against a cluster of their own before trusting the Kubernetes
Orker bases ([DEPLOY.md](DEPLOY.md), "Orkers on Kubernetes"):

| Job | Runs |
|---|---|
| `kind-orker.yml` › `kind-orker` | on a push or pull request that touches `deploy/kubernetes/`, either script, `internal/orker/`, `cmd/orker/`, the source build (`deploy/source/`), the release `deploy/Dockerfile`, the shell or file activity, or the workflow, at 06:29 UTC every day, and by dispatch. It first lifts the runner's AppArmor restriction on unprivileged user namespaces, which the rootless engine the script measures needs, and records the kernel and that setting in its summary. The script pins every tool: kind's and kubectl's release binaries by SHA-256, the node image and crane's image by digest. The check refuses a missing prerequisite with exit 77 before it deploys anything, so on kind a refusal fails the lane like a failed check. Both shapes run artifacts, both network modes, a cancel, the Orker's declared capacity, the per-task cap (a task above it refused at submit, one at it run with it as its limit), an out-of-memory task and a pod deletion; the Orker must refuse a mismatched work root, the user-namespaced engine must refuse to start without its RuntimeClass, and Docker's rootless engine must be seen to drop a container's limits. Its summary names the check that failed and the last of the namespace's and the cluster's state then; the artifact `kind-orker-<run>-a<attempt>` holds both scripts' logs, the check's evidence and that state in full, without the throwaway cluster's kubeconfig or the registry's keys |

`nightly.yml` holds four jobs, scheduled at 05:17 UTC on the default branch:
`nightly.yml` › `nightly` runs the nightly tier, `nightly.yml` › `vuln` runs
`make vuln`, `nightly.yml` › `quickstart` runs `scripts/quickstart-smoke.sh`,
and `nightly.yml` › `flakes`
downloads the last 30 days of proof artifacts and reports same-tree flips and
suspect failures (`proofrun flakes`); it edits no list. `flakes` waits for
`nightly`, whatever its result, so its window includes that night's proof.
`vuln` is independent of the proof: it reports only the known vulnerabilities
whose vulnerable code the module can reach, and a new one fails the run, which
is a regression on `main` like any red nightly (§6). The fix is to bump the module it names to the fixed
version (an exact one, §11.1 of `AGENTS.md`) and prove that change like any
other; `go tool govulncheck -show verbose ./...` lists the ones the code cannot
reach, which fail nothing.
`quickstart` is independent of the proof too: it runs the README's Quickstart
as written on a clone of the commit, where the root `compose.yaml` builds the
stack from source, and checks that a run of `each-fan-out` succeeds and that
the UI answers (`make quickstart-smoke`). No proof gate runs that path, and
`release.yml` › `artifacts` runs it only before a release, so this job is what
finds a break on `main` within a day: a directory the source Dockerfile copies
that `deploy/source/Dockerfile.dockerignore` does not admit, a Go or UI build
that fails in `deploy/source/Dockerfile`, a `compose.yaml` key the runner's
Compose refuses, or a stack that never turns healthy. It needs only what the
runner image has (Docker with Compose, git, curl and python3), and a failure
is a regression on `main` like any red nightly (§6).
`release.yml` holds five: `release.yml` › `validate` runs first on every
dispatch, refuses a version that is not `vMAJOR.MINOR.PATCH`, refuses
`tag: true` from any ref but `main`, and refuses a version whose `X.Y` the
built-in activity catalog does not name, which CHANGELOG.md has no
`## [X.Y.Z]` section for, or whose CHANGELOG.md still holds entries under
`## [Unreleased]` (`dist check`, `dist notes`), and decides from the
repository's tags whether the release is the latest (`dist order`): a release
below a released tag of its own `X.Y` is refused, because it would move the
`X.Y` image tag back, and a backport to an older series is released but never
marked the latest;
`release.yml` › `gate` runs the release tier, on hosted 4-vCPU runners where
its runner-size budgets cannot hold (§10, "Run the release gate"); `release.yml`
› `artifacts`, beside it and read-only, builds what the release publishes — `make dist` (the
archives and `SHA256SUMS`) and `make release-images` for linux/amd64 and
linux/arm64 as OCI layouts — runs `scripts/quickstart-smoke.sh --release
--layouts` on them (the root `compose.yaml` on those images, by digest), and
runs `scripts/quickstart-smoke.sh` (the README's Quickstart as written, on a
clone of the commit, built from source) before it uploads anything.
It restores no cache, and runs QEMU's installer and BuildKit from images
pinned by digest in `with:`, which Dependabot does not see, so those two pins
are moved by hand;
`release.yml` › `tag` waits for both and runs only with `tag: true`, and
creates and pushes the tag only after `proofrun verify -release` accepts a
passing release proof of the exact commit; and `release.yml` › `publish`, also
only with `tag: true` on `main`, verifies before anything is pushed that the
assets are exactly what the artifact's `SHA256SUMS` sums and match it, that
the image list names each of the ten images once, that each layout's index
names that digest, and that its bytes hash to it and list linux/amd64 and
linux/arm64; it signs `SHA256SUMS` with the release key and verifies that
signature with the committed `deploy/cosign.pub`, so a missing key, a wrong
password or a key that is not that file's pair stops it before anything is
pushed; it then pushes each layout to `ghcr.io/orkstack/<image>` byte for byte
(`skopeo copy --all --preserve-digests`, from the image
`deploy/skopeo-image.txt` pins) under `X.Y.Z` and `X.Y`, checks each digest
after the push, refuses to move an `X.Y.Z` that holds another digest, signs
each image by its digest (cosign, from the image `deploy/cosign-image.txt`
pins, with `--new-bundle-format=false --use-signing-config=false
--tlog-upload=false`, so it writes the `sha256-<hex>.sig` layout OrkStack's
verifier reads and nothing in a public transparency log), verifies each
signature by digest with `deploy/cosign.pub` and reads the `.sig` manifest
back, and only then creates the GitHub release (`gh release create
--verify-tag`, marked the latest when `validate` said so, never a draft or a
pre-release, so `releases/latest/download/` names the newest release), with
`SHA256SUMS.sig` and `cosign.pub` beside the archives. `validate` refuses a
dispatch whose `deploy/cosign.pub` is not committed, and the `--release`
smoke signs every image with a throwaway key by the same image and flags and
runs an Orkflow in a namespace that requires that key. `tag` and `publish`
download what `gate` and `artifacts` uploaded by the names those jobs output,
so a re-run of either finds the first attempt's artifact.
`publish` checks out nothing and runs no repository code; it holds the only
`packages: write`, and runs in the `release` environment, which alone holds
the signing key. The tag job's checkout keeps no credential, only the steps
that push, sign images and release are given the token, and only the two
signing steps the key, which cosign reads from the environment
(`env://COSIGN_PRIVATE_KEY`), never from a file. Every one takes a concurrency group named
from the validated version, so two dispatches of one version run one after the
other. A dispatch with `tag: false` builds, smokes and proves, and publishes
nothing.

No `run:` step splices a workflow input or an event field into its shell; each
reaches the step through `env:`. Every action is pinned to an exact release of
its latest major, and Go, Bun, Node and cosign to exact versions; Dependabot
moves them, one pull request and one proof at a time.

The composite action `.github/actions/proof-host` installs the host setup a
stack part needs: Docker images cached, the Temporal CLI, cosign, and the
Linux `host.docker.internal` name. It is proof-host setup, never Orker
behaviour. Nothing in it leaves the runner except through `proof-fetch`: the
stack images the cache did not load are pulled through it (the compose file
pins each by digest, and the stack then starts with `--pull never`, so a
missing image is an explicit failure and never a stall), cosign v2.6.5 is
downloaded through it as the release's linux/amd64 binary and kept only if it
hashes to the SHA-256 the release's `cosign_checksums.txt` lists, and the one
container the action runs from Docker Hub by tag is bounded with `timeout`.
Its fetches run inside the caller's `proof host` step, which has a limit of
10 minutes of its own.

The composite action `.github/actions/proof-fetch` is how every part job
fetches a run's own artifacts (the plan, the build, the proof job's parts),
how a `ui-e2e` job pulls the browser image, and how the proof host pulls its
images and downloads its one file: at most two attempts, the first bounded to
3 minutes and the second to 6 (`timeout` kills a fetch that makes no
progress), under a step limit of 10, so a stalled blob store or registry
costs a retry rather than the part, and a lost runner still fails within the
step. An artifact is fetched with `gh run download` on the job's own token
(`actions: read`), because `actions/download-artifact` has no limit of its
own and nothing to retry. An image is pulled only by digest, and a file is
fetched only over https to a path and kept only if it hashes to the SHA-256
the caller pinned: other bytes are a failed attempt. Every attempt is one
line in the runner's setup log, which the part step hands to
`scripts/proof.sh --part` as `--setup-log`; the part copies it to its
`setup.log`, `proofrun record` carries its lines (`-setup-log`) into
`part.json` (`setup`), and the merger lists every fetch that needed its second
attempt in `proof.json` (`setup_retries`) and under the README's Notes. A
retry is inside one attempt of a part: it is never a rerun and costs no
eligibility. A fetch that fails both attempts fails the step, before the
part: the job is then a **setup-failed** part (§7.1), which the merger lets
run again as a new part of the same tree (§8, §10). The action re-runs no job
and nothing continues on error. The runner starts the action's script under
`bash -e`, which would end it at the first failed command, so the script turns
errexit off and a test (`TestProofFetch_...`) runs it under the runner's shell
against fakes.

**The runner-health floor.** The first thing every part does, before any
gate, is measure its runner for about fourteen seconds (`proofrun runner-health`,
run by `scripts/proof-part.sh`): sequential write and read throughput and
the 95th-percentile fsync latency on its work disk, where the object store's
and Temporal's volumes live (the probe is written and read back through
`O_DIRECT`, so the read measures the disk and not the page cache),
single-core and all-core CPU throughput (SHA-256 over a fixed buffer), and
available memory. It writes `runner-health.json` beside the part's records:
each value, the floor `scripts/proof-runner-floors.tsv` declares for the
runner's class — `github-hosted/<ImageOS>/<N>cpu` on a GitHub-hosted runner,
the image and its vCPU count — and a verdict, `above-floor`, `below-floor`
or `unfloored` (a class the table does not name). A stack part
(`integration`, `ui-e2e`, `activity-fixture`, `observability`) below a floor
records itself and stops there, with no gate run and its job red; the
merger classifies that attempt **runner-degraded**, exactly as one whose
runner GitHub lost: nothing it recorded is an outcome of the tree, the proof
is not a proof of the tree, `proof.json` names the part, the metric, the
measured value and the floor (`degraded_runners`), and that one job may run
again (§8, §10). On GitHub (`GITHUB_ACTIONS=true`) the stopping part also
prints one annotation of fixed words, `proof: runner below the floor, no gate
ran`, which is how the rerun planner (§5, `proof-rerun.yml`) tells that job
from a failed one without downloading its artifact; it is evidence for
running the job again and never for a verdict, which the merger re-derives
from `runner-health.json`. The `plan`, `build`, `fast`, `race`, `race-repeat` and
`fuzz` parts, and a local proof's `local` part, measure for information: their
verdict is recorded and changes nothing, because a local host's floors are its
measured costs themselves. The merger re-derives every verdict from the file's values and
floors, never from the file's own word, refuses a part that measured against
floors that are not the tree's, and fails closed on a file it cannot read.
A measurement that cannot run (a filesystem that refuses `O_DIRECT`, say)
leaves that metric unmeasured and never below the floor: the floor only
ever turns a result into "not a proof of the tree", never a failure into a
pass, which is also why a feature proof may change the table and why no
base anchor is needed for it. The floor is measured once, at the start: a
runner that degrades later is not seen, and its failures stay test failures
(§12).

**Informational readings.** A disk read is one burst of one to two seconds and
an fsync reading one series: thin evidence about a disk that may be throttled
in bursts. So the measurement also writes the probe a second time, after the
single-core CPU measurement, reads it in two more bursts, one right after
that write and one after the all-core CPU measurement, and takes one more
fsync reading, last. Each is recorded in its metric's `samples` array with
the `offset_seconds` at which it began, how long it took (`seconds`) and,
when it could not be taken, its `error`; the array's first entry is the
judged reading itself. A write or a read also records the `region` of the
probe it covered (`from_bytes`, `bytes`): the second burst starts where the
first stopped and wraps to the start of the file at its end, so a file larger
than a burst reads gives the bursts different blocks, and a file a burst
reads whole (a fast disk) shows in the regions as blocks read again. An
`O_DIRECT` read bypasses the runner's page cache and nothing beyond it, so a
burst over blocks an earlier one read may have been served by a cache the
runner cannot see. The judged measurements are exactly what they were: the
same order, the same share of the budget each, and the judged fsync opens the
file the write left, which truncates it, as it always did. The extra readings
add about four seconds to the ten of the judged measurements. They are
informational: the floor judges only the first reading of each metric, a
later reading, however slow, never makes a part runner-degraded, one that
cannot be taken changes nothing, and `scripts/proof-runner-floors.tsv` does
not read them. They appear in `proof.json`'s `runner_health`, in the
README's "Runner health" section and in the part's `runner-health:` log
lines, and they feed the next derivation of the floors (§10).

## 6. The merge queue, the nightly tier and the pre-release gate

The required `proof` check has two modes:

- **queue-less (the default):** `proof` runs the real proof on the pull
  request, whose branch must contain `main`'s tip, under the strict policy.
  GitHub lands exactly the tree it tested.
- **merge queue (optional, organization-owned repositories):** `proof` runs on
  the merge group, and the queue lands that tree. The repository variable
  `MERGE_GATE_MODE=queue`, which only the enablement script sets, lifts the
  pull request's containment check, because there the merge group is what
  lands. It never makes `proof` trivial.

A proof binds a **tree, not a commit**. GitHub does not document which commit
a merge queue lands, and a merge commit made at merge time is not the commit a
pull request's proof tested; what both modes guarantee is that the landed
commit's tree is the tree the proof measured. `proof.json` records the tree,
and `scripts/github-merge-gate.sh --verify-landed SHA` checks the first merge
through either mode by downloading that run's `proof-<run_id>-a<n>` and
comparing its `tree` with `SHA^{tree}`. Pass the commit that landed on `main`.
In the queue-less mode that is a merge commit GitHub made, and it carries no
check run: the `proof` run is on the pull request's head, its second parent,
which the script reads when the landed commit has none. The merge commit's tree
equals the head's merged tree, because the branch contains `main`'s tip.

A required check and the merge queue are GitHub settings, not repository
files. `scripts/github-merge-gate.sh` (`make github-merge-gate`) is the
one-command enablement; it refuses a private repository, and this one is
public and organization-owned. **Until the apply is run the gate is not
enforced**: no `main-proof-gate` ruleset requires the `proof` check, so `main`
moves only by `AGENTS.md` §11.3's local path. An organization owner is needed
only for the optional queue.

The nightly job runs the deferred tests, and the race repeat and the fuzz lane
(§2.3), on a schedule (05:17 UTC) and by dispatch. A failed scheduled run is noticed through GitHub's standard
workflow-failure notification; nothing opens an issue. The latest completed run of
`nightly.yml` on `main` is `main`'s nightly. It speaks for the commit it
measured (`headSha`), which is `main`'s tip only while nothing has merged
since, and a red one is a regression on `main` (below). Dispatching one after a
merge that moves `main` has its verdict sooner (§10).

Interim rule A is retired (2026-10-27). While the nightly workflow did not run
on GitHub it required `make proof-nightly` on `main`'s tip after every merge
that moved `main`, passing before the next merge started and before every tag,
and it made `make merge-check` refuse a merge when either of `main`'s nightly
lists was non-empty and no passing nightly proof of `main`'s tree was given
(`NIGHTLY=`). Its exit condition, three consecutive public nightlies passing,
was met: the five most recent runs of `nightly.yml` on `main` (40414822382,
40431603801, 40443319443, 40489432056 and 40555387388) all passed. Now
`make merge-check` asks for no nightly proof and reads neither of `main`'s
lists. `NIGHTLY=proof/<name>` stays as an option: a nightly proof it is given
(a GitHub run's `proof-<run>-a<n>` artifact, or a local `make proof-nightly`,
§10) must be a passing nightly proof of the tree `main` points at, and `verify`
refuses it as a tree mismatch once a merge has moved `main`. A release tag has
its own gate, the pre-release proof. A merge is no longer held for a nightly
of its own, so a regression only a deferred test can see is found by the next
run, the cost `AGENTS.md` §11 accepts (2026-10-15). A red nightly is a
regression on `main`, not a merge freeze (2026-10-28): fixing it, or reverting
the change that broke it, is the next work, and other changes keep merging
through the merge gate meanwhile; never edit a list, a threshold or a
measurement file to make it pass. A red nightly clears only when a later run,
of the fix or of a commit after it, passes. It holds the release: a tag
requires a passing pre-release proof, which runs every deferred test, and a
red release proof holds its release the same way. Nothing mechanical reads GitHub's nightly before a
merge (§12): read the latest run on `main` first (§10).

**Calibration.** The soaks' thresholds were measured on a 16-core host, and a
public runner has 4 vCPU, so the first public nightlies were calibration runs:
a threshold missed on runner size alone is reported and the nightly's layout
changes (one shape per job, or a larger runner); a threshold or a measured
default never changes without the user's approval. Three consecutive public
nightlies have passed, which retired rule A.

**The 60-day rule.** GitHub disables a public repository's scheduled
workflows after 60 days with no repository activity. `make github-merge-gate`
reports `nightly.yml`'s state, and `gh workflow enable nightly.yml` is the
fix.

The pre-release gate is the release tier. A tag pushed by hand is outside
both paths; `make release-tag` and the `tag` job are the only sanctioned
paths, and no tag ruleset backs them: none has been applied. One would block
`release.yml`'s own push unless it named a bypass actor for it, and which actor
the Actions app's token can be on this organization-owned repository is not
verified.

## 7. Artifacts, `proof.json` and the ledgers

Retention is chosen from `github.event.repository.private`:

| Artifact | Contents | Retention (private / public) |
|---|---|---|
| `plan-a<n>` | `integration-plan.json`, `ui-plan.json`, the Playwright listing, the base lists, `part.json`, and `bin/`: the compiled integration test binaries every shard adopts (`proofrun compile`) | 1 d / 7 d |
| `part-build-a<n>` | the OCI layouts, `images.tsv`, `part.json` | 1 d / 7 d |
| `part-fast-a<n>` | `unit-tests.json`, the gates' logs; the unit log and events only when the unit gate failed (`scripts/proof-part.sh` removes them from a part whose unit gate passed) | 1 d / 7 d |
| `part-race-a<n>` | `race-tests.json`, `part.json`; the log and events only when the race gate failed (removed from a part whose gate passed, as the fast part's are) | 1 d / 7 d |
| `part-race-repeat-a<n>` | `race-repeat-tests.json`, `part.json`; the log and events only when the lane failed | 1 d / 14 d |
| `part-fuzz-a<n>` | `fuzz-tests.json`, `part.json`, each target's log under `fuzz/`, and `crashers/`: the input of every target the lane broke | 1 d / 14 d |
| `part-integration-<k>-a<n>` | the shard's ledger, `part.json`, gates, `integration/*.log`, events, its `artifacts/` | 1 d / 14 d |
| `part-ui-e2e-<k>-a<n>` | ledger, logs, screenshots, the traces of failed tests | 1 d / 14 d |
| `part-activity-fixture-<A>-a<n>`, `part-observability-a<n>` | logs, `part.json` | 1 d / 14 d |
| `proof-<run_id>-a<n>` | the merged proof (below) | 30 d / 90 d |
| `feature-proof-<run_id>-a<n>` (`ci.yml` › `feature`) | the feature proof directory | 14 d |
| `flakes-<run_id>-a<n>` (`nightly.yml` › `flakes`) | `flakes.tsv`, `flakes.md` | 30 d |

Every other part's upload leaves its `bin/` out — the stack binaries,
proofrun and the adopted test binaries, about 500 MB a shard beside 100 KB
of records — because nothing downstream reads them: the merger compares the
digests `part.json` records, never the files.

`plan` and `build` publish their artifact names as job outputs, and every job
that downloads them reads the names from `needs.*.outputs`, so a "Re-run job"
of one shard finds what attempt 1 uploaded — while it exists. Past the
retention above, a re-run fails at its fetch step, and the remedy is a new
run.

The merged artifact `proof-<run_id>-a<n>` carries `proof.json`, the merged
`unit-tests.json`, `integration-tests.json` and `ui-e2e-tests.json`, the
`race-tests.json` of a full or release proof or the `race-repeat-tests.json`
and `fuzz-tests.json` of a nightly one, both plans, every part's records under `parts/` — its `part.json`, gates, ledgers,
setup log and gate logs — and GitHub's record of every attempt's jobs under
`jobs/`, with the logs of failed jobs. A part that did not pass (a gate
recorded as anything but `pass`, or no `part.json` because its process died)
also has its events and per-job logs there (`*.events.jsonl`, `integration/`,
`unit.log`, `race.log`, `race-repeat.log`, `fuzz/`), the files §8 reads a
failure from; a passing part's are left out, and no part's `bin/`, `layouts/` or `artifacts/` is copied: all of it
stays in the part's own artifact. A failing fuzz part's `crashers/` is none of
those: it is copied with the part's other details. `scripts/proof-collect.sh`
makes the copy, after the merger has read the parts and written `proof.json`,
so what it leaves out changes no verdict. The merged proof is self-sufficient for a costs
refresh: the merged UI ledger carries each spec file's `wall_seconds`. Its
upload is bounded to five minutes, which the records fit: copied in whole
the parts were 4.6 GB and the upload timed out after every part had passed
(run 40365261654), and with the binaries out a green run's events and logs
were still 921 MB (run 40388322907).

`proof.json` carries the fields a proof always had (`tier`, `name`, `commit`,
`branch`, `tree`, `tree_end`, `clean_start`, `clean_end`, `base_ref`,
`base_commit`, `merge_base`, `gates`, `ledgers`, `image_tag`, `image_tag_own`,
`image_digests`, `temporal`, `proof_jobs`, `proof_ui_workers`, `result`,
`merge_eligible`, `ineligible_reasons`), and adds:

- `parts`, `reruns`;
- `plan` (its SHA-256, shards, jobs, costs source, unmeasured count, nightly
  mode and hash, deferred entries) and `ui_plan`;
- `quarantine` (`checked_on`, `base_sha256`) and `quarantined` entries;
- `nightly` (hash, base commit, base hash);
- `host_speed_failures` (an excused Go test's entry carries its `timing_record`,
  because no part's `artifacts/` reaches the merged proof), `known_gaps`,
  `binary_digest_notes`;
- `lists_changed`, the gate files the change edits and the rows it adds or
  removes;
- `setup_retries`, every fetch a part's runner needed a second attempt for
  (§5), the action's own lines prefixed with the part;
- `runner_health`, every part attempt's runner-health measurement (§5) with
  the verdict the merger re-derived and the informational `samples` of the
  disk write, the disk read and the fsync, and `degraded_runners`, every
  attempt that measured below the floor, one entry per metric with its value
  and the floor; a `reruns` entry whose `reason` is `runner-degraded` carries
  the earlier attempt's entries as `degraded`;
- `setup_failures`, every part attempt whose job failed in a fetch or install
  step before its proof step started (§7.1), with the part, the attempt, the
  step and the step's conclusion, whether or not the part ran again; a
  `reruns` entry has `reason` `lost-runner`, `runner-degraded` or
  `setup-failed`, and its `lost_attempt` describes the earlier attempt
  whichever it was (conclusion, annotations, failed step);
- `carried_forward`, every part attempt whose job "Re-run failed jobs" did
  not run again (§7.1), with the part, the attempt and the attempt that ran
  the job (`from`);
- `pr` and `ci`.

A ledger entry has `package`, `test`, `job`, `outcome`, `seconds`, an optional
`fuzz_seconds` (the fuzz lane's: the fuzz time the target was given, §2.3), `reason`, `allowed_skip`, `quarantine`, `failures` and, for a test that
failed, `failure_output` (the last 40 lines it printed, without the testing
package's own framing, each cut at 400 bytes: what the merger reads a
timing-only failure from, §12). The outcomes are
`pass`, `fail`, `skip`, `none`, `deferred` and `quarantined-fail`. A race
suite's ledger (§2.3) also has `short`, true when its run had `-short`. The words,
defined once: **listed** is what the binary reports; **run** is asked for;
**deferred** is a nightly-listed test the full tier did not run; **expected**
is every test with one required outcome; **out of tier** is a listed test the
tier does not expect.

A part's `part.json` holds its identity (`part`, `attempt`, `tier`, `tree`,
`tree_end`, `clean_start`, `clean_end`, `base_commit`, `image_tag`), its
gates, the plan hashes it read, its runner, its image digests, its ledger
names, its runner-health measurement (`runner_health`, the
`runner-health.json` beside it, §5) and, for a CI part, its runner's setup
log (`setup`: every fetch attempt made before it). It carries no verdict.

### 7.1 What the merger re-derives

`proofrun merge` trusts nothing a shard wrote as a verdict:

- **Ledger entries.** It re-derives each entry's verdict. An entry that is not
  a pass needs a problem naming it, an excusal the merger derived itself, a
  quarantine it re-checked, or an allowed skip found in the allow-list whose
  hash is the plan's. A shard's own problem lines excuse nothing.
- **The plan.** It must be complete (no `-select`, `-run` or package patterns)
  and its tier and nightly mode must be the pair `full`/`defer`,
  `release`/`include` or `nightly`/`only`.
- **The parts.** The merger recomputes the expected parts and flags and
  ignores unexpected ones. It requires the UI plan when `ui-e2e` parts are
  expected. A corrupt `part.json` or unreadable events count as a result, so
  the part fails closed. Two directories for one part attempt are refused.
  Reusable-workflow `X / job` prefixes are stripped when GitHub's job names are
  matched to parts.
- **Identity.** Every image-using part (`build`, `integration`, `ui-e2e`,
  `activity-fixture`, `observability`, `local`) must record the plan hash, the
  image tag and the image digests, so each CI step that runs one passes
  `--plan`; `ui-e2e` and `local` parts also record the UI plan hash
  (`--ui-plan`). A `build` part with no digests is ineligible.
- **Host speed.** An excused `visibleWithin` miss must be that locator's own
  timeout with the element not found or hidden, never a strict-mode violation,
  another error, or a test timeout. An excused Go `timing` miss is derived
  from the part alone (§12): the ledger entry is a plain failure of a test
  that is the only one of its job not to pass, the test's own timing record
  says it failed nothing else, the output the entry kept ends in exactly the
  helper's timing-miss lines, each recorded bound equals the base row's, and
  no miss took longer than the runner's share of the reference allows.
  A shard's own excusal, and a failing test with no record, excuse nothing.
  The release tier excuses exactly what the full tier does (2026-10-28), so a
  release proof on hosted runners can pass.
- **Unit gate.** A quarantined-only unit exit is dropped only when the
  package's own output is a plain test failure.
- **Race and lane ledgers.** The race gate's ledger (full and release), and the
  race repeat's and the fuzz lane's (nightly), are read from the part of the
  gate's name or the local part and held to the unit ledger's rules: an entry
  that is not a pass needs a problem naming it or an allowed-skip entry in the
  plan's list, or is a skip from a ledger that says its run had `-short`;
  `quarantined-fail` is refused; a gate without its ledger fails its proof.
  The race ledger must list every test the unit ledger lists, and each test
  it skipped under `-short` must be one the unit ledger passed. The nightly
  race repeat has no unit ledger, so its `-short` skips are accepted and
  counted and held to nothing further (§2.3).
- **Lost runners.** The lost-runner match reads every check-run annotation of
  the job; GitHub's system annotations cannot be told apart by path. A failure
  seen in the attempt's events or log always wins over it.
- **Degraded runners.** An attempt's `runner-health.json` is judged by its
  values against its floors, by each metric's direction, never by its
  `verdict` or `below_floor` fields; a below-floor measurement wins over
  everything the attempt recorded, a part.json and passing tests included,
  because nothing a degraded runner measured is an outcome of the tree. The
  file's `floors_sha256` must be the tree's table; a file that cannot be
  read fails closed; an absent file is a part measured before the step
  existed. The floor binds only the CI stack parts (`floorBinds` in
  `merge.go`, the same parts `scripts/proof-part.sh` stops).
- **Setup failures.** A job that failed, whose proof step never started (the
  jobs API records it skipped), and whose failed step is on the allowlist of
  fetch and install steps for its kind of part (`setupStepNames` in
  `merge.go`), is **setup-failed**: it ran no test and left no result, and it
  may run again exactly as a lost or a degraded part. The allowlist is by the
  steps' own names, because the jobs API reports a composite action only as the
  step that uses it: `proof host`, `fetch plan`, `fetch build` for
  `integration` and `activity-fixture`; those, `bun install`,
  `fetch browser image` and `Chromium (host)` for `ui-e2e`;
  `observability stack`, `fetch plan` and `fetch build` for `observability`;
  `bun install` and `bun install (site)` for `fast`. The plan and the build
  have no entry: every other part reads what they make. Every row of the
  classification above ranks over it: a part.json, a failing test in the
  events or the log, a read error, a cancel, a lost runner and a degraded
  runner are all decided before a setup step is looked at, and a job whose
  proof step ran is a result whatever else failed. A failure in any other
  step before the proof step (a checkout, a toolchain's setup) is not
  rerun-eligible. `proof.json` lists the attempt in `setup_failures` and the
  README under "Setup failures"; a part whose last attempt is setup-failed has
  no result, and the proof fails naming it. Two tests pin the names to
  `proof.yml` in both directions, with rename mutations: the proof step names
  (`proofStepNames`) and the allowlist (`setupStepNames`).
- **Carried-forward jobs.** "Re-run failed jobs" lists every job of the run
  again in the new attempt, each under a new id and that attempt's number,
  and a job it did not run keeps the earlier attempt's start, end,
  conclusion, runner and steps, to the second, while its artifact exists only
  as `part-<name>-a<earlier>` (run 40934338667). The proof job's job records
  keep those fields, and the merger reads such a job as the attempt that ran
  it, not as an attempt of the part (`carriedForward` in `merge.go`): only
  when its record and the one of the attempt just before it each name their
  own attempt, were read whole, have a start, an end, a conclusion and steps,
  and agree in all of them. A job that ran again started later, so it stays an
  attempt of its own, and every rule above judges it as before: a part that
  recorded a result and ran again is still ineligible. A record missing any of
  those fields (one a proof job wrote before it kept them, or one it could not
  read) is an attempt of its own too, so it fails closed. A part that left an
  artifact of an attempt its record says was carried forward is ineligible.
  `proof.json` lists every carried-forward attempt in `carried_forward`
  (`part`, `attempt`, `from`), and the README under "Carried forward".

`make merge-check PROOF=…` refuses a proof whose quarantine `checked_on` is
more than 30 days old (the merged artifact's private retention). A `NIGHTLY=` proof it is given is
judged against `main`'s tip tree (§6), and an empty `-nightly` value or a tip
git cannot read refuses it; with none given, nothing of the nightly is read.

## 8. Reading a failed proof

1. Read `proof.json`: `result`, `merge_eligible`, `ineligible_reasons` and
   `reruns`.
2. For a missing or duplicate test, the merged integration ledger names it and
   its shard; that shard's `part.json` names its artifact.
3. For a failed test, the shard's `integration/<job>.log` and
   `integration.events.jsonl` hold the test's own output; the ledger row's
   `job` names the log.
4. For a part with no result, `ineligible_reasons` says whether GitHub lost the
   runner (that one job may be re-run), the part ran on a degraded runner
   (`degraded_runners` names the metric, its value and the floor; that one
   job may be re-run), the job failed in a fetch or install step before its
   proof step started (`setup_failures` names the step; that one job may be
   re-run), or the attempt is not rerun-eligible (start a new run).
5. For a gate, the merged README's gate line names the part and log; download
   that part's artifact. A failed race gate names the test or package in the
   `race` ledger's problems, and the detector's report is in the failed part's
   `race.log`; a failed fuzz lane names the target, and the input that broke it
   is the file under the part's `crashers/` (§2.3).
6. A local proof that stopped early (§2.2) lists a **Stopped early** section:
   the gate that **failed** is the cause, so read its log; the gates marked
   `stopped` or `not-started` ended or never began because of it and say
   nothing about the tree. Fix the failure and run a new proof; this one can
   not become eligible. Their integration ledger rows read `not finished` or
   `not run`, never `pass`.

Only a part GitHub annotated as a **lost runner**, whose own measurement at
its start put its runner **below the floor** (`runner-degraded`, §5), or whose
job failed in a fetch or install step before its proof step started
(**setup-failed**, §7.1), may run again. "Re-run job" on that one job is the
way, and "Re-run failed jobs" is permitted when every failed part is one of
these three, because it then gives no part that recorded a result a second
attempt (the `proof` job runs again with them, and merges every attempt). The
jobs it does not run appear in the new attempt's record as carried forward,
and the merger takes their result from the attempt that ran them (§7.1), so
the proof is merge-eligible once the parts it ran again pass.
"Re-run all jobs" gives every part a second attempt after a result, so its
proof is ineligible. A test failure is never answered by a rerun, and a part
that runs again runs whole, as a new part of the same tree, never a failed
test in isolation. `proof-rerun.yml` issues "Re-run failed jobs" itself where
the repository enables it (§10).

## 9. Reproducing one CI shard locally

```bash
make infra-up
make -C images oci TAG=<plan.proof_id> OUT=/tmp/layouts
proofrun push-layout -layouts /tmp/layouts -registry localhost:5001 -tag <plan.proof_id> -out /tmp/images.tsv
proofrun integration -plan plan.json -shard K -out /tmp/shard -jobs 3 -namespace-prefix local-sK
```

It refuses a checkout whose tree hash differs from the plan's and a compiled
binary whose listing differs from the plan's. A binary digest that differs is
a note, never a failure, because separate compiles need not be identical.

By hand, `go test -tags=integration -p 1 ./internal/engine -run '^TestX$'`
works against `make infra-up`, after `make proof-clean REMOVE=1` (§10); use a
unique `ORK_TEMPORAL_NAMESPACE` whenever anything else may run.

## 10. Procedures

**Add a test and get it measured.** Land it; the next proof plans it
unmeasured (its own job, conservative cost), and its ledger row measures it.
Then refresh the costs and commit the two files.

**Refresh the costs from CI ledgers.**

```bash
gh run view <run_id> --json attempt -q .attempt
gh run download <run_id> -n proof-<run_id>-a<attempt> -D /tmp/p
make proof-slow-list FROM=/tmp/p
```

It rewrites `scripts/proof-test-costs.tsv`, `scripts/proof-slow-tests.txt` and,
from the merged UI ledger's file walls, `scripts/proof-ui-costs.tsv`. Commit
with the run URL as the source.

**Measure a proof's overhead.** `make proof-overhead FROM=proof/<name>
[OUT=/tmp/overhead.md]` (`proofrun overhead -from DIR [-out FILE]`) reads an
existing proof directory, runs nothing, and never writes inside it (an `-out`
there is refused). Its Markdown report has:

- **Gates**, from `gates.tsv`: each gate's seconds and share of the critical
  path, which is the serial chain fast gates (the site gate among them) →
  setup (Temporal, images, binaries, the integration test binaries) →
  integration beside the activity fixture (the longer of the two) → `ui-e2e`
  → observability. A proof that ran its setup as one concurrent group (§2.1)
  has a `setup` row holding the group's wall: the setup phase costs that row
  once and its members nothing more; an older proof's setup is their sum.
- **Integration processes**, from the runner's `start <job> slot k:` and
  `end <job> after X s` lines in `integration.log` (each part's log in a
  merged proof) joined with `integration-tests.json` by job: each process's
  wall, its top-level tests' ledger seconds, and the difference. That
  difference is the per-process overhead: bucket create, binary init and
  Temporal namespace registration, together (a bucket's removal was part of it
  until the runner's bucket sweep, §2.1, took it off the slot). The report gives its
  min, median, p90, max and total, and the total in slot-seconds and in wall
  at `proof.json`'s `proof_jobs`. It also gives the heavy phase's wall (the
  plan's heavy jobs, run alone) and the ordinary phase's wall and slot
  utilisation (busy slot-seconds over slots × wall). The log has no
  timestamps, so a phase's wall is its busiest slot's summed process walls.
- **Playwright**, from `ui-e2e-tests.json`: the parallel and solo
  invocations' walls (`SOLO_SPECS` in `ui/playwright.config.ts`, `-config`,
  names the solo files), their summed file walls against an ideal packing on
  the proof's `proof_ui_workers`, each lane's utilisation, and the gate's
  seconds outside the tests. A utilisation is at most 100%: when an older
  ledger's file walls exceed what the lanes held (it charged the gap between
  two invocations to the next one's first file), the report says so and reads
  100%.
- **Slow boots**: every `WARN` line about a boot that took long, from the
  integration logs, each job's log and `ui-e2e.log`.
- A summary naming the top three time sinks.

A missing ledger, plan or log is listed under "Partial proof" and its section
says so.

**Derive flake rates.** `make proof-flakes FROM='proof/*' REPO=.` reads every
proof's `unit-tests.json`, `integration-tests.json` and `ui-e2e-tests.json`,
and reports flips (a fail and a pass on one tree) and suspect failures (a
failure the proof's change could not reach). The newest 50 proofs of each test
are its window.

**Quarantine a test.** It needs the user's approval. Add a row to
`scripts/proof-quarantine.txt`: suite, package, test, owner, since,
review-by (at most 30 days later) and reason. At most five entries exist. An
entry names a listed test, is not also an allowed skip, and excuses nothing
until the base carries it. A quarantined test still runs; its failure is
recorded as `quarantined-fail`, never as a pass. An entry past its review date
stops excusing, and every proof warns from seven days before.

**Renew or remove a quarantine entry.** The user's approval; an expired entry
excuses nothing. Remove it once `proofrun flakes` shows no failure in the
window, or when the fix lands with its evidence.

**Move a test to the nightly tier.** It needs the user's approval, a
merge-gate test that covers the same behaviour at a smaller size or by another
route, and a row in `scripts/proof-nightly-tests.txt` with
`<date> <name>` in the approved column. A stale entry, a missing covering test
or a covering test that is itself on the list fails the plan. The §11 coverage
map stays green. A Playwright spec file moves the same way, through
`scripts/proof-nightly-specs.txt`, with a merge-gate spec file that covers it;
a spec file that holds only the scale case is the unit that moves.

**Keep §11's coverage map current.** `scripts/proof-section11-coverage.tsv`
maps every claim of `AGENTS.md` §11's required-coverage list to the tests or
fixture check that prove it, with a `known-gap:` row where nothing end to end
does. `TestSection11Coverage` (in `test/tools/proofrun`) checks that the
bullets match, that every named test is declared with the kind the map claims,
that every claim has non-unit evidence running in the merge gate or a known
gap, and that every known gap is named in §12. A new §11 bullet, a moved test
or a closed gap fails it.

**Add a scope row.** A new top-level path needs a row in
`scripts/proof-scope.tsv`: `TestRepositoryScopeTable_MapsEveryTrackedPath`
fails for any tracked path no row matches, and a case in
`TestRepositoryScopeTable` pins the row the new path must land on.

**Recover from a lost runner.** Confirm GitHub annotated the runner lost, then
"Re-run job" on that job, on the same day, while the plan and build artifacts
exist. The merger records the rerun and the unfinished tests. Any other
missing part needs a new run of the same tree, except a degraded runner and a
setup failure, below.

**Recover from a degraded runner.** `proof.json`'s `degraded_runners` (and
the step summary's "degraded runner" line) names the part, the metric, its
value and the floor, and the part's own log shows the `runner-health:`
lines. "Re-run job" on that job alone, while the plan and build artifacts
exist; the merger records the rerun with reason `runner-degraded`. Never
edit `scripts/proof-runner-floors.tsv` to make the next attempt read as
healthy, and never answer a failing test on a runner above the floor with a
rerun: the floor is a measurement, and a part above it that failed is a
test failure.

**Re-derive the runner-health floors.** The table is a measurement file,
like the costs: a feature proof may change it, and it is never edited to
get a proof through. Each class's floor comes from the `runner-health.json`
of every part of healthy CI runs (under `parts/<part>/` in each merged
`proof-<run_id>-a<n>` artifact; `runner_health` in `proof.json` has the same
values): for a throughput or a capacity, well under the least healthy
measurement; for a latency (`fsync_p95_ms`), well over the largest. Name
the runs and the margin in the row's `derived` column. The first rows came
from run 40411125174's 21 parts (2026-10-26), each a third of the least
measurement or three times the largest latency, before the informational
readings existed; a class with no row (`unfloored`) is measured for
information until its runs give it rows. The `samples` of the disk write, the
disk read and the fsync (§5) judge nothing, and the next derivation reads
them first, from the same `runner-health.json` files, or across the proofs in
one table:

```sh
jq -r '.runner_health[] | .part as $part | .attempt as $try
  | .metrics[] | .metric as $metric | (.samples // [])[]
  | [$part, $try, $metric, .offset_seconds, .value,
     (.region.from_bytes // "-"), (.region.bytes // "-"), (.error // "-")]
  | @tsv' proof/<name>/proof.json
```

A slow first burst that the later bursts repeat is a disk that stayed slow,
and one they do not repeat is a transient, but only where the later bursts
read blocks the first did not: compare the regions, because a burst over
blocks already read may have been served by a cache beyond the runner and
shows less. Gather the readings from several proofs before that derivation,
and name the runs in `derived` as for any row. A floor that no healthy run
would cross is the aim: a floor can only turn a part into "not a proof of the
tree", and a floor set too high costs reruns, never a false pass.

**Recover from a setup failure.** A job that failed in a fetch or install step
before its proof step started (§7.1) ran no test: `proof.json`'s
`setup_failures` (and the step summary's "setup failure" line) names the part,
the attempt and the step, and the job's log names every attempt of a fetch that
gave up (§5). "Re-run job" on that job alone, while the plan and build
artifacts exist; the merger records the rerun with reason `setup-failed`. A
failure in any other step before the proof step (a checkout, a toolchain's
setup), and any failure of the plan or the build, is not rerun-eligible: start
a new run, below.

**Run failed candidates again automatically.** `proof-rerun.yml` does what the
three recoveries above do by hand, and only that: when a `proof` run for a push
to `validate/full/**` of this repository fails, and the run attempt is below 3,
`proofrun rerun-plan` classifies every failed job with the merger's own
`classify` from GitHub's record of the attempt (its jobs, its check-run
annotations, the logs of failed jobs), and `gh run rerun --failed` runs only if
every failed job is the `proof` job or a part that is lost, setup-failed or
degraded. A part with a result, a cancel, a failed `base`, `plan` or `build`,
a job it cannot read and a job whose name is not a part of the workflow all
mean no rerun, so a run that can never become merge-eligible is not rerun. The
planner reads only the failed jobs of the attempt that just completed: a job
"Re-run failed jobs" carried forward (§7.1) succeeded, so it never decides a
rerun, and a failed job a single "Re-run job" carried forward is classified
from its record in that attempt, while the merger still judges it by the
attempt that ran it. At most two automatic reruns follow the first attempt. It is off until the
repository variable `PROOF_AUTO_RERUN` is `true`, a setting only the
maintainer makes. Its planner prints three words it validated (`rerun`,
`reason`, `parts`) and never an annotation or a log line. The merger remains
the only authority, reading every attempt again: an automatic rerun that was
wrong, or forged by a branch that prints the annotation a degraded part
prints, costs one attempt and can only make the proof ineligible, never pass
it. The workflow takes `checks: read` beside the `actions: write` and
`contents: read` a rerun needs, because the evidence is a check-run
annotation; the `proof` job takes the same permission for the same reason.

**After a setup failure the merger cannot excuse, or a "Re-run job" past the
artifact's retention.** Start a new run of the same tree (push again, or close
and reopen the pull request). A part that did run after a retried fetch shows
it in `proof.json`'s `setup_retries`. After a test failure, never: fix it,
with evidence.

**On a private repository, start the proof** after adding the `ci:proof`
label: push again, or close and reopen.

**Get a CI proof of a candidate before the gate is enforced.** Push it to
`validate/full/<name>` with its base as the first parent; the proof compares
by tree, never by commit. A feature proof uses `validate/feature/<name>`.

**Merge on a CI full proof of the candidate** (`AGENTS.md` §11.3(b)), the
default merge path since 2026-10-28: the sharded run takes about a fifth of
one host's time (§11.5). The `validate/full/<name>` run's base is the
candidate's first parent, so build the candidate on `main`'s tip, push it,
and download its passing run's artifact into `proof/` (gitignored, so the
worktree stays clean). The artifact's `proof.json` is the evidence: download
it, never retype it. Amending the merge commit's message to quote it leaves
the tree, which the proof is bound to, unchanged.

```bash
git push origin HEAD:validate/full/<name>
gh run list --workflow proof.yml --branch validate/full/<name> -L 1   # <id>
gh run watch <id> --exit-status
gh run view <id> --json attempt -q .attempt                          # <attempt>
gh run download <id> -n proof-<id>-a<attempt> -D proof/ci-<id>
git commit --amend                    # quote proof/ci-<id>/proof.json
make merge-check PROOF=proof/ci-<id>
git checkout main && git merge --ff-only <candidate>
git push origin main
```

`merge-check PROOF=` applies the same rules as to a local proof: tier `full`,
result `pass`, `merge_eligible`, `tree` equal to `HEAD`'s tree, `base_commit`
equal to `main`'s tip, the candidate's first parent equal to `main`'s tip, a
clean worktree, and a quarantine check date at most 30 days old. If `main`
moved since the push, rebuild the candidate and push it again.

**Read the nightly after a merge.** `main`'s nightly is `nightly.yml`'s latest
completed run on `main` (§6), and it speaks for the commit it measured, not for
`main`'s tip: compare its `headSha` with `main`. Its verdict is its proof's
`proof.json`, not the run's colour: an excused Go timing miss leaves its
integration part red while the proof passes (§12).

```bash
gh run list --workflow nightly.yml --branch main -L 3 --json databaseId,status,conclusion,event,headSha
git rev-parse main
```

- Green: nothing a deferred test sees is wrong at `headSha`. The commits after
  it are not measured yet, and a merge is not held for them (`AGENTS.md` §11
  accepts that cost).
- Red: a regression on `main`. Fixing it, or reverting the change that broke
  it, is the next work, but other merges are not held for it (`AGENTS.md` §11,
  2026-10-28); no release is tagged until it clears. Once the fix is pushed,
  dispatch a run of it and watch it:

  ```bash
  gh workflow run nightly.yml --ref main
  gh run list --workflow nightly.yml --branch main -L 1 --json databaseId   # <id>
  gh run watch <id> --exit-status
  ```

- Only `nightly.yml` › `vuln` failed: a known vulnerability the module can
  reach, which is a regression on `main` too. `go tool govulncheck ./...` names the
  module and the version that fixes it; bump the module to exactly that
  version, run `make check`, and merge that change on its own proof. A
  vulnerability the code cannot reach is not reported and fails nothing.
- Only `nightly.yml` › `quickstart` failed: the README's Quickstart no longer
  works on a clone of `headSha`, which is a regression on `main` too. The
  job's log names the Quickstart command that failed, or the check, and on a
  failure ends with the last 80 lines of the stack's `activities`, `ork`,
  `orker` and `ui` logs. `make quickstart-smoke` on that commit reproduces it.
- Only `runner-degraded` parts failed: a runner failure, not a regression
  (§5, `AGENTS.md` §11). That part runs again as a new part of the same tree.

After any merge that moves `main`, once it is pushed, a dispatched run has its
verdict sooner than the schedule's.

`make merge-check` asks for no nightly proof. To have it verify one anyway,
pass `NIGHTLY=`: a GitHub run's artifact, or a local `make proof-nightly`.
It is evidence only for the tree `main` pointed at when it started (§6).

```bash
gh run view <nid> --json attempt -q .attempt                         # <attempt>
gh run download <nid> -n proof-<nid>-a<attempt> -D proof/nightly-gh-<nid>
make merge-check PROOF=proof/ci-<id> NIGHTLY=proof/nightly-gh-<nid>
```

**Write the release notes.** A release's notes on its GitHub release page
are its CHANGELOG.md section, word for word. Each user-visible change lands
with its entry under `## [Unreleased]` (AGENTS.md §11.1, step 7; CONTRIBUTING.md,
"The changelog"). Before a release is dispatched, a change to `main`, proved
and merged like any other, cuts the version's section: it renames
`## [Unreleased]` to `## [X.Y.Z] - YYYY-MM-DD`, the version without its `v`
and the day the section is cut, puts an empty `## [Unreleased]` above it, and
edits the entries into what the release page should say, under Keep a
Changelog's `### Added`, `### Changed`, `### Deprecated`, `### Removed`,
`### Fixed` and `### Security`, each breaking change marked **BREAKING**.
Nothing writes the date at tag time: the tag names a commit a proof passed
on, so the heading is already in that commit. `dist notes -version vX.Y.Z`
prints exactly what the page will show: the section without its heading,
because the release has its own title, `OrkStack vX.Y.Z`. `release.yml` ›
`artifacts` writes that to `meta/notes.md`, and `release.yml` › `publish`
hands it, and nothing else, to `gh release create --notes-file`; the
checksums, `SHA256SUMS.sig` and `cosign.pub` are the release's assets, and
[UPGRADING.md](./UPGRADING.md), "Release artifacts", says how to verify
them. `release.yml` › `validate` refuses a version with no section or an
empty one, and one whose `## [Unreleased]` still holds entries, because they
would ship in the release without being in its notes: an entry merged after
the cut moves into the section before the dispatch. The first release, 0.1.0,
says only "First release.": everything built before it is that release.

**Run the release gate.** Dispatch `release.yml` from `main` with `version`
set and `tag: false`; `release.yml` › `validate` refuses a malformed version
before any job starts, and `release.yml` › `gate` runs the release tier on the
chosen branch's tip. Locally, `make proof-release` on a clean tree.

`release.yml` › `gate` runs on `ubuntu-24.04` (4 vCPUs) and excuses a
runner-size miss exactly as the merge gate does (§12, "Runner-size known
gaps"; decided 2026-10-28): the run page at the reduced task ceiling, which
measured 9,485 ms against its 5 s there (run 40571748126), and the soaks' live
`GetRun` p95, which measured 2.014 s against its 2 s (run 40593609129), are
recorded known gaps under their approved rows, never passes. Before that
decision the release tier excused none and could not pass on hosted runners.
A release no longer waits for a runner as large as the 16-CPU reference, so
its tag does not rest on budgets that held on the reference class; a local
`make proof-release` on the reference host still enforces them.

**Tag a release.** Either dispatch `release.yml` with `tag: true` — the `tag`
job checks out the proved commit, runs `proofrun verify -release -tag vX.Y.Z
-proof DIR` on the gate's artifact, and only then creates the annotated tag
and pushes it, and `publish` then pushes the images and creates the GitHub
release — or, locally, `make release-tag VERSION=vX.Y.Z
PROOF=proof/<release>`, which applies the same check to `HEAD`, tags it and
does not push; a tag made that way publishes nothing, because `release.yml` is
the only path that publishes. Only a `release.yml` run whose `gate` passed
publishes. Neither path stops a
person with write access from pushing a `v*` tag by hand (§12). Before
dispatching, write the version's section ("Write the release notes", above):
it becomes the release notes, and `validate` refuses a version without one.

**Set up release signing.** Once, before the first release (done on
2026-10-29: `deploy/cosign.pub` is committed, and the environment holds both
secrets, deploys from `main` only and has one required reviewer): a release signs
what it publishes, so `validate` refuses every dispatch until
`deploy/cosign.pub` is committed, and `publish` pushes nothing until the
`release` environment holds the private half. A repository administrator runs,
on their own machine, with `cosign` (2.6 or later) and a signed-in `gh`:

```bash
scripts/release-signing-setup.sh --dry-run    # every action, nothing run
scripts/release-signing-setup.sh              # asks for the key's password twice
```

It refuses a key directory (`--dir`, default `~/orkstack-release-key`) inside
any git work tree, an existing key file, and an environment that already holds
`COSIGN_PRIVATE_KEY` or a committed `deploy/cosign.pub` unless `--rotate`, all
before it writes anything. It then generates the pair with `cosign
generate-key-pair`, proves it by signing and verifying a file with
`release.yml`'s flags, creates or updates the `release` environment
(deployments from `main` only, the caller a required reviewer unless
`--no-reviewer`, so `publish` waits for an approval), sets
`COSIGN_PRIVATE_KEY` (the whole `.key` file) and `COSIGN_PASSWORD`, and
copies the public key to `deploy/cosign.pub`. The password reaches cosign
through its environment and `gh` on stdin, never an argument or a file.
Commit `deploy/cosign.pub` through a pull request, and keep the `.key` file
and its password offline, apart, and never in a repository. Without the
script, the same by hand: `cosign generate-key-pair`; create the environment
`release` in the repository's settings, limited to the `main` branch
(Deployment branches and tags → Selected branches); `gh secret set
COSIGN_PRIVATE_KEY --env release < cosign.key`; `gh secret set
COSIGN_PASSWORD --env release`; and commit the `.pub` file as
`deploy/cosign.pub`. To rotate, run the script with `--rotate` and a new
`--dir`, or replace both secrets and `deploy/cosign.pub` together; a release
already published stays signed by the key its own tag carries, and each
deployment that trusts OrkStack's releases registers the new key before it
runs a release signed with it.

**Recover a failed publish.** `publish` is safe to re-run: an image tag that
already holds this release's digest is left alone, an image whose signature
already verifies is not signed again, and the release itself is created
last. Re-run the failed job from the run's page while the
`artifacts` job's upload is kept (7 days); after that, dispatch again with a
new patch version.

**After the first publish,** make the ten packages public: each new
`ghcr.io/orkstack/<image>` starts private, and an anonymous pull fails until
it is public (on each package's settings page).

**Enable the merge gate.** The repository is public, so run
`make github-merge-gate` (check mode, read-only) and then
`make github-merge-gate APPLY=1`. Check mode refuses unless: you are an
administrator; the repository is public; for `QUEUE=1`, it is owned by an
organization; `origin/main` is local `main`'s tree and holds `proof.yml`;
`TestWorkflows_TheProofCheckRunsTheRealProofOnPullRequests` passes; the
GitHub Actions app has reported a successful `proof` check on `main`'s tip or
on the head of the newest successful `proof.yml` run (a `validate/full/**`
push does it); merge commits are allowed; Actions are enabled;
`.github/CODEOWNERS` names an account with write access (a warning with
`CODEOWNER_REVIEW=0`); and `MERGE_GATE_MODE` agrees with the applied
`main-proof-gate` ruleset's `merge_queue` rule. A disabled `nightly.yml` is a
warning. The apply creates or updates the rulesets by name; every body is
unverified against the API until then, so read the API's answer before
anything else. The script fails closed on an API error: only a 404 reads as
"no such ruleset" or "variable unset", and a failed read or name lookup stops
the apply instead of posting a duplicate. `APPLY=1 QUEUE=1` adds the merge queue and then sets
`MERGE_GATE_MODE=queue` (it also finishes a half-applied queue, the rule
present and the variable unset); `APPLY=1` alone, later, unsets the variable first
and then removes the queue rule, so neither switch is ever looser than either
mode. After the first merge through a mode, run
`scripts/github-merge-gate.sh --verify-landed SHA` once. It is not run from a
private repository.

**Stop or clean up a proof.** `make proof-stop PROOF=proof/<name>` stops the
proof you started and the containers it labelled. A proof stopped this way (or
by a signal, or killed) leaves what its test processes owned: the passing
jobs' buckets its sweep did not reach (§2.1, its `integration/*.bucket`
files), each failing or killed process's bucket, Orker work roots and activity
containers. A bucket holds store volumes until it is removed, so a host that
kept them would run the next proof out of volumes.

`proofrun clean` removes them, and nothing else. Every local proof runs
`proofrun clean -remove` after its prerequisites are checked and before its
directory exists, keeps the listing as `cleanup.log`, and opens `setup.log`
with its summary, `cleanup: removed N buckets, N work roots, N containers of
dead proofs`; `scripts/integration.sh` runs it before each run, and
`make proof-clean` lists the same things (`REMOVE=1` removes them) by hand. It
reads the proof directories and `scripts/integration.sh` run directories of
every worktree of this repository, and removes:

- a bucket named `orkstack-<label>-<pid>-<suffix>` that a run recorded (its
  hand-off file, or the head of a process's log, where `s3bucket.Main` names
  the bucket it made) and the store still holds, or that the store lists;
- a work root under a worktree's `.data/work` (or `ORK_ORKER_WORKDIR`) named
  `<label>-p<pid>-<suffix>`. A dead root holding root-owned files (an
  activity container wrote them) is refused by the host's removal; it is
  emptied through a throwaway container of the first-party images' pinned
  alpine base instead, mounting only that root, with no privileged or host
  flags and a two-minute limit, and logged `removed through a container`. A
  failure there is listed and not retried;
- an activity container whose Orker id ends in a proof id and a pid
  (`proof.OrkerID`), or whose workspace lies in a work root that names a pid,
  whatever its Orker is called: a directory named `<label>-p<pid>-<suffix>`
  right under a worktree's `.data/work` (one since removed included) or
  `ORK_ORKER_WORKDIR`, read from the container's own mounts. A Playwright
  spec's Orkers work in such roots too, `e2e-<name>-p<pid>-<suffix>`
  (`orkerEnv` in `ui/e2e/harness.ts`);
- a proof Temporal container whose proof process is gone;

each only when the process its name records is gone, and never one whose pid
a still-running proof or `integration.sh` run recorded. A work root or a
container whose owner cannot be told is listed and left alone, and the
development bucket, the dev stack's work root and its Orker's containers are
never touched. The recorded names are the source of truth for buckets: the
development store, SeaweedFS 4.48 with its auth off, answers the S3 bucket
listing with no bucket at all.

## 11. Reference

### 11.1 Make targets

| Target | Does |
|---|---|
| `make check` | `make lint` (`go vet ./...`, gofmt, and `go tool staticcheck` with its default checks as built and with `-tags=integration`), `go vet -tags=integration ./...`, and `go test -short ./...` with the test cache kept, except for `GIT_STATE_PACKAGES` (the packages whose unit tests run git, whose reads the cache cannot see), run with `-count=1`; no Docker |
| `make vuln` | `go tool govulncheck ./...`: the known vulnerabilities whose vulnerable code the module reaches. It needs the network, so it is in no proof gate; `ci.yml` and `nightly.yml` run it (§5) |
| `make proof` | the full proof, the merge gate |
| `make proof-ui` | the same; the full proof already runs the UI suites |
| `make test-race` | the race gate's `go test`, without its ledger: `-race -short` over `RACE_PACKAGES` (default `./...`), `RACE_COUNT` times (default 1) |
| `make fuzz` | the fuzz lane: every Fuzz target, sharing `FUZZ_BUDGET` (default 45m) or given `FUZZTIME` each, logs and crashers under `FUZZ_OUT` |
| `make proof-feature` | an optional feature proof of this branch |
| `make proof-nightly` | the nightly tier |
| `make proof-release` | the pre-release gate |
| `make release-tag` | tag HEAD after a passing release proof of it |
| `make dist` | the release's binaries, archives and `SHA256SUMS` for `VERSION=vX.Y.Z` |
| `make release-images` | the release images as OCI layouts under `OUT`, for `PLATFORMS` |
| `make release-smoke` | build both for this machine and run `compose.yaml` on those images |
| `make quickstart-smoke` | run the README's Quickstart as written on a fresh clone of the working tree; `nightly.yml` › `quickstart` and `release.yml` › `artifacts` run it (§5) |
| `make proof-flakes` | derive per-test flake rates from the ledgers |
| `make proof-slow-list` | regenerate the costs and the slow list from a proof |
| `make proof-overhead` | report where a proof's wall-clock went, from its directory, running nothing |
| `make proof-observability` | only the observability gate; never merge-eligible |
| `make proof-stop` | stop one proof and everything it started |
| `make proof-clean` | list, or with `REMOVE=1` remove, what test processes that are gone left (`proofrun clean`) |
| `make github-merge-gate` | check or apply the required-check settings |

### 11.2 The lists

| File | Format | A bad row |
|---|---|---|
| `scripts/proof-scope.tsv` | pattern, actions, flags | refuses the feature tier |
| `scripts/proof-allowed-skips.txt` | suite, package, test, reason | a skip with no entry, or an entry naming no listed test, fails |
| `scripts/proof-nightly-tests.txt` | package, test, covering tests, approved, reason | a stale name or a missing covering test fails the plan |
| `scripts/proof-nightly-specs.txt` | spec, covering specs, approved, reason | a stale spec or a missing covering spec fails the UI plan; a full proof defers the rows its base also carries |
| `scripts/proof-quarantine.txt` | suite, package, test, owner, since, review-by, reason | structure, budget, a stale name, or an allowed-skip overlap fails; an expired entry stops excusing |
| `scripts/proof-test-costs.tsv` | package, test, seconds | an unmeasured test gets the conservative default |
| `scripts/proof-slow-tests.txt` | package, test | a slow test is left out of a feature proof's regression set, and of its evidence when it is there only by reaching a changed helper, type or variable |
| `scripts/proof-ui-costs.tsv` | spec file, wall seconds | an unmeasured spec gets 160 s |
| `scripts/proof-host-speed.tsv` | kind, spec or package, test title or name, assertion, bound, reference CPUs, note | a malformed row fails the merge; only a row the base carries identically excuses, and only its declared assertion's miss on a CI UI part, or a Go test's `timing` check on a CI integration part (§12) |
| `scripts/proof-runner-floors.tsv` | class, metric, floor, derived | a malformed row fails the merge and makes `runner-health` exit 2 (the part runs on, unjudged); a class with no row is `unfloored`; a floor only ever makes a part "not a proof of the tree" (§5) |

### 11.3 `proofrun`

| Subcommand | Flags |
|---|---|
| `unit` | `-out DIR`, `-allow FILE`, `-quarantine FILE`, `-quarantine-base FILE`, `-race`, `-short`, `-count N` |
| `fuzz` | `-out DIR`, `-budget DURATION` or `-fuzztime DURATION`, one of the two |
| `integration` | `-out DIR`, `-bin DIR`, `-jobs N`, `-namespace-prefix S`, `-costs FILE`, `-allow FILE`, `-quarantine FILE`, `-quarantine-base FILE`, `-select FILE`, `-run REGEX`, `-plan-only`, `-plan FILE`, `-shard K`, `-shards N`, `-matrix-out FILE`, `-nightly FILE`, `-nightly-mode MODE`, `-nightly-base FILE`, `-tier TIER`, `-proof-id S`, `-base-commit SHA`, `-merge-base SHA`, `-observability-required`, `-print-progress`, `-fail-fast`, `-ci`, `-lists-changed FILE`, `-pr-number N`, `-pr-head-sha SHA`, `-pr-base-sha SHA`, `-pr-contains-base BOOL`, `-ui-shards N`, `-nightly-specs FILE`, `-host-speed FILE`, `-host-speed-base FILE` |
| `compile` | `-out DIR`, `-select FILE` |
| `select` | `-base REF`, `-out FILE`, `-scope FILE`, `-slow FILE`, `-allow FILE`, `-tests PKG:REGEX`, `-specs FILE` |
| `feature-report` | `-dir DIR`, `-select FILE` |
| `ui-ledger` | `-report FILE`, `-list FILE`, `-plan FILE`, `-out DIR`, `-config FILE`, `-log FILE`, `-shard K`, `-part NAME`, `-host-speed FILE`, `-host-speed-base FILE` |
| `ui-plan` | `-list FILE`, `-costs FILE`, `-shards N`, `-out FILE`, `-matrix-out FILE`, `-nightly-mode MODE`, `-nightly-specs FILE`, `-nightly-specs-base FILE`, `-run-files-out FILE` |
| `record` | `-dir DIR`, `-tier TIER`, `-name S`, `-commit SHA`, `-branch S`, `-tree-start SHA`, `-tree-end SHA`, `-clean-start BOOL`, `-clean-end BOOL`, `-base-ref REF`, `-base-commit SHA`, `-merge-base SHA`, `-jobs N`, `-ui-workers N`, `-temporal MODE`, `-temporal-image IMAGE`, `-temporal-address ADDR`, `-image-tag TAG`, `-images FILE`, `-image-tag-own`, `-gates-override S`, `-observability-required`, `-part NAME`, `-attempt N`, `-out-dir DIR`, `-plan FILE`, `-ui-plan FILE`, `-setup-log FILE`, `-runner-health FILE`, `-select FILE`, `-status N` |
| `merge` | `-plan FILE`, `-ui-plan FILE`, `-parts DIR`, `-jobs DIR`, `-host-speed FILE`, `-runner-floors FILE`, `-nightly-specs FILE`, `-out DIR`, `-name S`, `-ci-run-id S`, `-ci-attempt N`, `-ci-event S`, `-ci-url URL`, `-stopped` |
| `flakes` | `-from GLOB`, `-repo DIR`, `-window N`, `-out FILE`, `-md FILE`, `-min-runs N` |
| `verify` | `-proof DIR`, `-nightly DIR`, `-release`, `-tag S` |
| `temporal-args` | `-compose FILE` |
| `costs` | `-from DIR`, `-costs-out FILE`, `-slow-out FILE`, `-ui-costs-out FILE`, `-threshold SECONDS` |
| `overhead` | `-from DIR`, `-out FILE`, `-config FILE` |
| `push-layout` | `-layouts DIR`, `-registry HOST`, `-namespace S`, `-tag TAG`, `-out FILE` |
| `layout-digests` | `-layouts DIR`, `-registry HOST`, `-namespace S`, `-tag TAG`, `-out FILE` |
| `evidence-args` | `-body FILE` |
| `free-port` | none |
| `touches` | `-base REF`, `-action NAME`, `-gate-files`, `-scope FILE` |
| `gates` | `-select FILE` |
| `clean` | `-remove` |
| `runner-health` | `-out FILE`, `-class NAME`, `-floors FILE`, `-dir DIR`, `-seconds N` |
| `rerun-plan` | `-repo OWNER/NAME`, `-run ID`, `-attempt N`, `-max-attempts N` |

### 11.4 The scripts

`scripts/proof-observability.sh` runs the observability gate.
`scripts/proof-part.sh` runs one CI part. `scripts/proof-stop.sh` stops a
proof. `scripts/proof-clean.sh` lists and removes what test processes that are gone left (`proofrun clean`; every proof and `scripts/integration.sh` run it first).
`scripts/proof.sh` is the entry point behind every proof target.

### 11.5 Measured numbers

- The full proof's baseline on a 16-core host: about 90 minutes before
  sharding, with the integration suite at 3,693 s and Playwright at 1,598 s.
- The sharded CI graph's reference: ten integration shards, five UI shards and
  four activity-fixture shards, about 306–426 runner-minutes.
- Wall time of one full proof: 14.6 minutes sharded in CI (run 40438152385, a
  `validate/full/**` push, 2026-10-27, before the race gate joined the full
  tier), and 78.0 minutes as one local part on the 16-core host (2026-10-28,
  race gate included). That gap is why a CI proof is the default merge path
  (§10).
- `make check` (lint, `go vet -tags=integration ./...`, `go test -short ./...`),
  measured 2026-10-21 on a quiet 16-core host: with a warm build and test
  cache, 11 s at 16 CPUs and 7 s at 2 CPUs (`taskset -c 0-1`), since unchanged
  packages come from the test cache; with a cold build cache, 120 s at 16 CPUs
  and 377 s at 4 CPUs (`taskset -c 0-3`). A cold cache recompiles the suites
  and is the number the `ci.yml` › `check` job records on a runner, so a
  2-vCPU runner's cold `check` takes longer than the two-minute warm target.
  Those figures are from before staticcheck joined `make lint`.
- staticcheck in `make lint` (`go tool staticcheck ./...`, then the same with
  `-tags=integration`), measured 2026-10-28 on a 16-core host whose load
  average was 7–9. It keeps each package's results, so with nothing changed a
  run took 1.7 s at 2 CPUs (`taskset -c 0-1`). After edits to about thirty
  packages the two runs took 37 s and 14 s at 16 CPUs (5 m 19 s and 1 m 58 s of
  CPU). With a cold build cache and a cold staticcheck cache they took 69 s and
  14 s (9 m 48 s and 2 m 0 s of CPU), most of the first run being the
  dependencies' export data that `go vet` otherwise warms; after a cold
  `go vet ./...` (41 s, 6 m 22 s of CPU) they took 30 s and 14 s (4 m 37 s and
  2 m 1 s of CPU). So lint adds about 6.6 CPU-minutes on a cold runner, about
  four minutes on two vCPUs, if it parallelises as it did here; that is why
  the `ci.yml` › `check` step, the `ci.yml` › `feature` proof step and the
  `proof.yml` › `fast` step each have five minutes more than they had. The
  runner's own figure replaces this estimate once a run has recorded it.
- The race gate (§2.3): `go test -race -short -count=1 ./...` took 573 s on
  2026-10-27 on a 16-core host whose load average was 15–29 for the whole run,
  another agent's race run beside it, `internal/engine` the long pole at 515 s
  and `internal/api` at 199 s; the unit gate, without `-short` and without the
  detector, takes 160–360 s alone. A second run took 632 s of wall under a load
  average of 30–70 and 1,744 CPU-seconds (user 1,664, sys 80). Dividing the
  CPU-seconds by 16 cores gives 110 s, which is not a floor: `internal/engine`
  is one long package, and its own path bounds the gate. Neither run was
  quiet, so neither is the gate's cost on the proof host; the first full proof
  that runs it is. A third run, of the `internal/engine` test binary alone
  (`-short`, one run) pinned to four CPUs with `taskset -c 12-15` under a load
  average of 15–30, took 545 s: the package is bound by a few long
  single-threaded tests (one took 172 s), not by cores, so four CPUs did not
  lengthen it. That is a data point for a 4-vCPU runner on this host's cores,
  not a measurement on one, and it leaves the 30-minute timeout per run (§2.3)
  about 3.3 times the longest package's time.
- A CI runner, as `runner_health` records it (run 40411125174, `ubuntu-24.04`
  on a private repository): 4 CPUs and 16 GB, with 14.3–14.8 GiB available
  before a part, sequential writes of 59–457 MB/s and reads of 235–534 MB/s
  on its work disk, and a p95 fsync of 0.2–10 ms; `PROOF_JOBS=2`, ten
  integration shards and five UI shards fit the budget.

## 12. Known gaps

- **The gate is not enforced yet.** The repository is public, so rulesets are
  available, but `scripts/github-merge-gate.sh`'s apply has not been run: no
  `main-proof-gate` ruleset requires the `proof` check. Until it is, `main`
  moves only by `AGENTS.md` §11.3's local path.
- **`main` is under a ruleset the script did not apply.** A ruleset named
  `main`, active since 2026-10-27 and made by hand, covers the default branch:
  it refuses deletion, force pushes, creating and updating the ref, and
  unsigned commits, and its pull-request rule asks for one approval and a
  code-owner review, with the repository's admin role able to bypass it
  always. It does not require the `proof` check, so until the gate's apply is
  run, `AGENTS.md` §11.3's push of `main` goes through that bypass. The apply
  adds `main-proof-gate` beside it (the script finds its own rulesets by name),
  which has no bypass actor, and `main` then moves through a pull request or
  the queue. What the `main` ruleset holds is read from the API (`gh api
  repos/orkstack/orkstack/rulesets`), not from the repository.
- **The merge queue is optional and not applied.** The repository is
  organization-owned, so the queue could be (`APPLY=1 QUEUE=1`). Queue-less
  mode is the default: `proof` runs the real proof on the pull request, and
  `strict_required_status_checks_policy` makes the tested tree the landed
  tree.
- **The full proof has passed on GitHub only on `validate/full/` pushes.**
  Runs 40388322907 and 40438152385 passed, but never as the required check of
  a pull request: the one `pull_request` run, a Dependabot pull request,
  failed, and no `merge_group` run exists. The pull-request and queue paths
  are structurally validated and unexercised until the gate is applied.
- **The planner reads the default branch's allowlist; the merger reads the
  candidate's.** `proof-rerun.yml` runs from the default branch, so
  `rerun-plan` classifies with the default branch's `setupStepNames`, while the
  merger of the candidate's own `proof` job uses the candidate's. A change that
  renames a step of `proof.yml` (and moves the allowlist with it) can therefore
  cost one wasted automatic attempt, or one that is not issued, until the
  default branch carries the rename. It can never produce a false pass: the
  merger decides, from the candidate's records, and refuses any attempt after a
  result.
- **A runner that degrades after its measurement.** The runner-health floor
  (§5) is measured once, before any gate; a runner that slows down later —
  as the one of nightly run 40371833169 did, in the third generation of a
  soak that had run two at full speed — is not seen by it, and its failures
  are recorded as test failures, with a new run of the same tree the only
  honest answer. A measurement after a failure would be confounded by what
  the failure left running, so none is taken.
- **Runner-size known gaps.** A UI spec's 5 s wall-clock budget cannot hold on
  a 2-vCPU runner, nor the soaks' 2 s `GetRun` budget on a 4-vCPU one: both
  were calibrated on the 16-CPU reference host and are enforced in every
  local proof of any tier. In CI, the merge gate, the nightly and the
  pre-release gate excuse a runner-size miss alike (the release tier since
  2026-10-28), so a budget is enforced only by a proof on the reference host.
  `scripts/proof-host-speed.tsv` names each such test, with the one assertion
  whose failure is its runner-size miss, its bound and its reference CPU
  count. A UI row, named by its spec file and title, declares either
  `lessThan MS` (Playwright's `toBeLessThan` or `toBeLessThanOrEqual`
  with `Expected` exactly MS: the bound's own timing miss) or `visibleWithin
  MS LOCATOR` (a `toBeVisible` on exactly that locator that timed out after
  exactly MS: the page never drew within the spec's own wait). A UI miss is a
  recorded known gap (`host_speed_failures`, `known_gaps`, the README's
  "Known gaps: runner size"), not a merge-blocking failure, only when all of
  these hold: the outcome is `failed`; the failure is exactly the row's
  assertion; the part is a CI `ui-e2e:K` shard whose runner has fewer CPUs
  than the row's reference, in any tier; and the base's
  copy of the table carries the same row (a row a change adds, or a bound it
  loosens, excuses nothing). A skip, a
  missing outcome, any other failure of the same test (another locator's
  visibility timeout included), and every miss on a local proof still fail
  the proof; the merger re-derives each excusal and never takes a shard's
  word for one. A miss is never turned into a wider local bound. Both
  class-B misses below are declared rows.

  A Go test's wall-clock budget is a row of kind `test`, whose assertion is
  `timing ID`: one check the top-level test reports under that id through
  `internal/testsupport/proof` (`TimingBounds`, `Timing.Check`), never through
  `t.Errorf`. A miss fails nothing where it is measured. The test's last
  cleanup writes `timing/<test>.json` under the part's artifacts (every
  check, its measured value and bound, and whether the test failed anything
  that is not a timing check) and then reports each miss as one line of a
  fixed form (`timing miss: id=… measured=…s bound=…s`), so the output of a
  test that failed only a timing check ends with exactly those lines. The
  merger excuses the miss, as a recorded known gap, only when all of these
  hold: the part is a CI `integration:K` shard whose runner has fewer CPUs
  than the row's reference, in any tier; the base's
  copy of the table carries the same row; the ledger shows the test as a
  plain failure (no crash, no failing subtest) and no other test of its job
  did not pass; the test's own record says it failed nothing but its timing
  checks; the output the ledger entry kept (its last 40 lines, without the
  testing package's framing) holds, from the first timing-miss line to the
  end, nothing but the record's misses, one line each and in order; and every
  missed check is declared by a row, is over its bound, was bounded at the
  row's bound, and took no longer than the bound times the reference's CPUs
  over the runner's (a runner with a k-th of the reference's CPUs may take k
  times as long, no more: 8 s for the 2 s `GetRun` budget on 4 CPUs; a UI row
  has no such ceiling). A test that failed with no record has no sample to
  excuse and fails the proof; so does every miss on a local proof, on a
  runner with as many CPUs as the reference, beyond that ceiling, and by any
  check no row declares. The shard's gate is excused only when the excused
  miss is the part's only defect. A row with the assertion `-` is recorded on
  a miss and never excused. Counts, bytes, history shares, deadlock-detector
  trips and correctness assertions get no row.

  The rows are `getrun-live-p95` of the three reduced-ceiling soak tests
  (`TestIntegration_AFanOutAtAReducedItemCeilingMeetsItsThresholds`,
  `TestIntegration_AnItemCeilingRunCarriesOnAtAReducedCeiling` and
  `TestIntegration_ATaskCeilingRunCarriesOnAtAReducedCeiling`), 2 s on 16
  CPUs, and the run page at the reduced ceiling
  (`e2e/run-page-reduced-ceiling.spec.ts`), 5 s on 16 CPUs; the user approved
  them on 2026-10-27. The archive p95, `Events(0)` and replay checks of the
  same tests go through the same helper and have no row: none of them missed
  in the CI full proofs and nightlies read when the rows were chosen (runs
  39337036208 to 40593609129), so a miss of one fails the proof, and a row
  for one is proposed when one does.

  What stays visible where the live `GetRun` budget is excused is a bound on
  what that read reads, which holds on every host: the soaks' live `GetRun`s
  go through a counting reader, and each must read at least one history
  event, at most 110% of the events and at most 110% of the bytes the
  generation it read holds (the history is scanned once, in events and in
  bytes; the margin is for the instant between the read and the count), and
  at most 4 saved-state objects (a read takes 1 or 2). They fail the test on
  every runner, being counts and sizes, and were calibrated on 160 reads of
  the three tests on the reference host, the bytes on the last 40
  (`internal/engine/getrun_structure_test.go` holds the calibration). They
  bound what is read, not what reading it costs: a slower decode or a larger
  saved state shows only in the wall-clock numbers, which a proof on the
  reference host enforces, and the evidence records the cost per thousand events
  (`ms_per_1000_events`) and the saved-state bytes to read it by. The run
  page's 5 s has no structural counterpart at all.
- **The validation run's class-B results** (run 39345074409, recorded in
  `host_speed_failures` and `known_gaps`): `each-fan-out.spec.ts` (whose
  gated test is now `each-fan-out-ceiling.spec.ts`) measured
  7,006 ms against its declared 5,000 ms budget at the gated size on a 2-vCPU
  runner; `run-page-ceiling.spec.ts` never reached its 5 s budget, because
  the run page did not become visible within 60 s there.
- **The scale tests are never raced.** The race gate runs `-short`, which
  skips the history-guard, ten-thousand-item and closure-cost tests, so those
  run under the race detector nowhere; the unit gate passes them without it
  (§2.3). A test that asserts an absolute cost and runs under `-short` must
  scale its bound under the detector (`raceBuild` in `internal/engine`), as the
  continuation and definition-reference tests do.
- **A race part's runner is not floored.** Like `fast`, the `race`,
  `race-repeat` and `fuzz` parts measure their runner for information (§5):
  the detector slows every test several times over, so a runner below the
  floors that bind the stack parts fails a bound test explicitly rather than
  being set aside as degraded. `AGENTS.md` §11 says so: a red `race-repeat`
  or `fuzz` part is a regression on `main`, never `runner-degraded`. Flooring them
  needs floor rows derived from measured runs in
  `scripts/proof-runner-floors.tsv`, which changes only with the user's
  approval.
- **The nightly race repeat's skips are held to nothing.** The repeat is
  `-short`, and the nightly proof has no unit ledger to hold a skip to (the
  full proof's merger does, §7.1), so a test that skips there narrows what the
  lane hunts without failing it; the ledger summary counts the skips.
- **The nightly lanes' time grows with the code.** The race repeat runs three
  runs of its packages, so a new package lengthens a nightly; the CI job's
  limit (`proof.yml` › `race-repeat`) is its bound. The fuzz lane is bounded
  by construction: its budget is its whole time, shared by the targets, so a
  new target shortens every turn instead of lengthening the lane (§2.3), and
  a unit test fails when the targets no longer fit the budget at the minimum
  turn. What that leaves unmeasured: the lane's real overhead per target on a
  runner (the assumed 45 seconds is conservative, not measured there), and
  the fuzzing a turn of a minute or so finds, which is shorter than the two
  minutes each target had when the lane was a fixed time.
- **Nothing mechanical reads GitHub's nightly before a merge.** `make
  merge-check` asks for no nightly proof (rule A is retired, §6), and the
  `proof` check does not read `nightly.yml`, and since 2026-10-28 a red
  nightly holds no merge (`AGENTS.md` §11): it is read after a merge, by the
  latest run on `main` (§10), and acted on as the next work. Between a merge
  and the next run the deferred tests have not seen `main`'s tip, and a
  regression only they can see is found by that run, not at merge; the
  pre-release gate runs them all before a tag.
- **An excused Go timing miss leaves its integration part red.** The merger
  excuses a Go `timing` miss (above) and the proof passes, but the CI
  `integration (K)` shard that ran the test still exits non-zero, because the
  shard itself excuses nothing (a UI shard does: `proofrun ui-ledger` exits
  cleanly on a miss it re-derives as excused). A run with such a miss
  therefore concludes `failure` while its `proof.json` passes. The verdict is
  the proof: the required check is the `proof` job, `release.yml`'s `tag` and
  `publish` read the release proof and the named results of what they need
  rather than the gate's colour (2026-10-28), and a nightly is read by its
  `proof.json` (§10). Excusing the miss in the shard, as the UI ledger does,
  would make the colours agree; it is not done.
- **The observability gate has not run on GitHub.** The first release run,
  which always runs it, is its calibration.
- **`release.yml` has not run on GitHub yet.**
  `nightly.yml` has four jobs, `nightly`, `vuln`, `quickstart` and `flakes`;
  `nightly` and `flakes` have run, and so has `proof.yml` as a reusable
  workflow through them (`workflow_call`; the flakes job passed in run
  40555387388). `quickstart` has not run on GitHub yet, and neither has the
  same smoke in `release.yml`: the first scheduled or dispatched nightly after
  it lands is the smoke's first run on a hosted runner, and its time there,
  for which the job allows 40 minutes, is not measured (5 minutes cold on the
  16-CPU reference host). `release.yml`'s
  `artifacts` and `publish` jobs were added after the last `actionlint` pass;
  the workflow rules in `test/tools/proofrun` hold their shape, and `make
  release-smoke` and `make quickstart-smoke` run their build and both smokes
  on a developer's host. The first
  `tag: false` dispatch is their first run on GitHub. Its `gate` excuses a
  runner-size miss as the merge gate does (§10, "Run the release gate"), so
  the hosted runners' size alone no longer fails it.
- **The proof's `race` part has not run on GitHub.** The `race` part of the
  full and release tiers is on the same footing as the observability gate:
  it is proven on a developer's host and by the workflow rules in
  `test/tools/proofrun`, not on a hosted runner, so the first CI full proof
  that carries it is its calibration. The other lanes have run there: the
  `vuln` jobs of `ci.yml` (run 40706033375) and `nightly.yml`, and the
  nightly's `race-repeat` part, passed in their first runs on 2026-10-28, and
  its `fuzz` part ran its whole 105-minute budget in nightly 40706217000 and
  found a real crasher (`pkg/artifacts`, a run-index status the writer never
  spells), kept as a seed.
- **`kind-orker.yml` has not passed on GitHub yet.** `scripts/kind-orker.sh`,
  and through it `scripts/k8s-orker-check.sh`, passed on a developer's host
  (kind v0.33.0, Kubernetes 1.37.0, containerd 2.3.4, Linux 7.2), and the
  workflow rules in `test/tools/proofrun` hold the lane's shape. Its first run
  on GitHub (40965501413, of `2d95c0f3` on 2026-10-29) built the cluster and
  the images, and on the hosted runner's kernel (7.0, inside kind) every
  prerequisite held and the user-namespaced shape passed its run with
  artifacts and both network modes, its cgroups and its cancel, in under four
  minutes. It
  then failed at the limits check, which still expected a task above the
  per-task cap to fail at dispatch after resource placement had moved that
  refusal to submit. The rest of the check, the privileged shape and the
  rootless measurement, after lifting the runner's AppArmor restriction, have
  not run there. The check has run against kind clusters only; a cluster of
  another make is checked by its operator.
- **`proof-rerun.yml` has not run on GitHub yet, and is off.** The
  `PROOF_AUTO_RERUN` variable is unset, so nothing has exercised it; its
  planner is proven against fakes of `gh api`, and its workflow shape by the
  rules in `test/tools/proofrun`. Two things only a run can show: that the
  `proof` workflow's completion fires `workflow_run` for an attempt that this
  workflow's own `GITHUB_TOKEN` asked for (if it does not, one automatic rerun
  follows a run, and a person's rerun arms the next), and that a failed
  job's log, annotations and `check_run_url` read as the planner expects. Both
  fail toward fewer reruns, never toward a false pass.
- **The nightly soaks' thresholds were measured on a 16-core host.** A
  threshold missed on a 4-vCPU public runner is reported as a calibration
  result, never silently changed. The soaks themselves carry no row of
  `scripts/proof-host-speed.tsv`: only the merge gate's reduced-ceiling copies
  do, for the live `GetRun` p95 (above), so a nightly's wall-clock miss on a
  CI runner fails that nightly.
- **Base-cache sizing is unmeasured at concurrency.** The 8-entry, 128 MiB
  cache is documented arithmetic, not a measured miss rate; a setting needs a
  measurement first, with the user's approval.
- **A tag pushed by hand is outside both paths.** `make release-tag` and the
  `tag` job are the only paths that check a release proof; GitHub itself does
  not refuse a hand-pushed tag.
- **§11 3j**: no run moves an image tag between two attempts. The digest is
  pinned at apply, so a moved tag never reaches a stored version; the claim is
  a known gap rather than a test.
- **An Orker-only Temporal cut is a named follow-up.** C28, C29 and C35 cut
  every worker's connection, the Orkers' included; a cut of one Orker's alone
  is not yet a dedicated test.
