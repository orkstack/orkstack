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
| **feature** | `make proof-feature TESTS=… SPECS=…` | no | the fast gates, the change's evidence, its regression set |
| **full** | `make proof` | yes, on a local host; in CI, the `proof` job | every gate and every test, except the nightly list's entries the base also carries |
| **nightly** | `make proof-nightly` | no | only the tests `scripts/proof-nightly-tests.txt` names |
| **release** | `make proof-release` | no; it is a tag precondition | every gate, the nightly tests included, observability always |

A full proof records each nightly-listed test as `deferred`, never as passed,
and only when its base's list also carries it; an entry the change adds runs.
The pre-release gate is the full tier with nothing deferred. A tag requires a
passing release proof of the exact commit: `make release-tag` and the workflow
both refuse without one.

The feature tier is never merge evidence. A local full, release or nightly
proof runs its gates on one host and writes one part, `local`, then merges it
with `proofrun merge`. The CI `proof` job runs the same merger over many
parts. There is one verdict implementation.

### 2.1 The gates

A full proof runs: lint; build; unit; `go vet -tags=integration`; the activity
images, built once from this tree; every integration test whose package's
integration build differs from its unit build, against real Temporal and the
S3-compatible store; the activity contract fixture; the UI type-check, unit
and every Playwright spec; and the observability gate when the tree changes a
telemetry path. A release proof runs the observability gate unconditionally.

Integration tests run as separate processes, `PROOF_JOBS` at a time (default
4). Playwright spec files run on `PROOF_UI_WORKERS` workers (default and
today's only safe value: 1). A **heavy** test — one whose assertions are
bounds — runs alone, in its own CI shard or first on a local host. A test is
heavy by what it asserts, not by how long it takes: the reduced-ceiling copies
of a moved soak run the same E4/E7 thresholds and are heavy, while a test that
merely waits on a deadline for progress is not. A new test that asserts such a
bound is added to `heavyTests` in `test/tools/proofrun/plan.go`.

## 3. How `proofrun select` picks evidence

`proofrun select -base REF -out FILE` reads the diff from the merge base and
`scripts/proof-scope.tsv`, and writes the selection the feature proof runs. A
path no row matches, or a row whose action is `full`, refuses the feature tier
(exit `2`): `scripts/`, `.github/`, the Makefiles, `internal/testsupport/` and
`test/tools/proofrun/` change how every suite runs and need the full proof.

The scope table's actions: `go`, `proto`, `ui`, `spec`, `ui-unit`,
`all-specs`, `images`, `examples`, `tool`, `observability`, `none` and
`full`. The `temporal` flag marks a package whose evidence must include an
integration test. A `docs` flag marks a row whose change should come with a
documentation change; the proof prints a WARNING for a missing one and never
fails on it.

The selection is:

- **the change's evidence**: every test whose source the diff changes,
  including tests that reach a changed or deleted helper, type, variable or
  `TestMain` through their package's declarations; every test named with
  `-tests` (the pattern must match whole test names); every spec the diff
  changes or `-specs` names; every spec when the Playwright harness or the UI's
  dependencies changed; the activity fixture when `images/` or `test/fixtures/`
  changed; the tests that read a changed example, and the examples spec; the
  tests and specs that run a changed `test/tools/` tool; and the observability
  gate when a telemetry path changed;
- **the regression set**: in every integration package whose test binary
  depends on a changed package — through its imports or a binary its suite
  builds — every test declared in an integration-tagged file, except the slow
  list's (`scripts/proof-slow-tests.txt`).

The regression set is never evidence. Every changed package with a change
outside its tests and its comments needs at least one evidence test whose
binary depends on it, and it must be an integration test when the package is
Temporal- or storage-facing. A changed `ui/src/` file needs an evidence spec,
except that `ui/src/lib/**/*.ts` may be proven by a changed or named UI unit
test. Missing evidence, a pattern that matches no test, and a spec file that
does not exist are invalid invocations.

## 4. The plan and the sharding

`proofrun integration -plan-only` compiles and lists the integration packages
once and writes `integration-plan.json`, format 2:

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
by their measured wall — their tests' seconds plus the gap before each test —
and passes each shard a file list. Playwright's own `--shard` is never used.

## 5. The CI job graph

`.github/workflows/proof.yml` is one graph behind the full, nightly and release
tiers. It triggers on `pull_request`, `merge_group`, a push to
`validate/full/**`, `workflow_dispatch` and `workflow_call`.

| Job | Runs | Uploads |
|---|---|---|
| `proof.yml` › `base` | decides the base, the tree and the feature evidence a pull request names | the base files |
| `proof.yml` › `plan` | compiles and lists the packages; writes both plans; decides observability | the plans, the listing, the base lists |
| `proof.yml` › `build` | builds every activity image once as an OCI layout and records its digests | the layouts and `images.tsv` |
| `proof.yml` › `fast` | lint, build, unit (no `-short`), vet-integration, UI type-check and unit | one part |
| `proof.yml` › `integration` | one matrix entry per shard: its own Temporal, store, registry and images | one part per shard |
| `proof.yml` › `ui-e2e` | one matrix entry per UI shard: its own stack and file list | one part per shard |
| `proof.yml` › `activity-fixture` | one matrix entry per activity: `make -C images contract` | one part per activity |
| `proof.yml` › `observability` | when the plan says it is due, or always for release | one part |
| `proof.yml` › `proof` | `if: always()`; downloads every part and GitHub's job records; runs `proofrun merge`; writes the summary | the merged `proof.json` and ledgers |

`proof` is the single required check. It passes only when the merged
`proof.json` says `result: pass` and, for a full proof, `merge_eligible:
true`. A skipped required job reports success, which is why `proof` runs on
`always()` and nothing else.

`ci.yml` holds no required check:

| Job | Runs |
|---|---|
| `ci.yml` › `check` | `make check` on every push: no Docker, no Temporal |
| `ci.yml` › `feature` | the feature tier on every pull-request push once the repository is public; with the `ci:feature` label while it is private, and by dispatch |

`nightly.yml` holds one job, `nightly.yml` › `nightly`, scheduled at 05:17
UTC on the default branch. `release.yml` holds two: `release.yml` › `gate`
runs the release tier, and `release.yml` › `tag` runs only with `tag: true`
and only after `proofrun verify -release` accepts a passing release proof of
the exact commit.

The composite action `.github/actions/proof-host` installs the host setup a
stack part needs: Docker images cached, the Temporal CLI, cosign, and the
Linux `host.docker.internal` name. It is proof-host setup, never Orker
behaviour.

## 6. The merge queue, the nightly tier and the pre-release gate

The required `proof` check has two modes:

- **queue-less (the default):** `proof` runs the real proof on the pull
  request, whose branch must contain `main`'s tip, under the strict policy.
  GitHub lands exactly the tree it tested.
- **merge queue (optional, organization-owned repositories):** `proof` runs on
  the merge group, and the queue lands that tree.

A required check and the merge queue are GitHub settings, not repository
files. `scripts/github-merge-gate.sh` (`make github-merge-gate`) is the
one-command enablement; it refuses until the repository is public and its
owner can carry the rulesets. **Until then the gate is not enforced**: `main`
moves only by `AGENTS.md` §11.3's local path. An organization owner is needed
only for the optional queue.

The nightly job runs the deferred tests on a schedule. While the nightly
workflow does not run on GitHub, `make proof-nightly` runs on `main`'s tip
after every merge that moves `main`, and passes before the next merge starts
and before every tag. `make merge-check PROOF=… NIGHTLY=…` refuses a merge
when `main`'s list is non-empty and no passing nightly proof of `main`'s tree
is given. A red nightly or release proof is a failing `main`: fix or revert
the change that broke it, and never edit a list, a threshold or a measurement
file to make it pass.

The pre-release gate is the release tier. A tag pushed by hand is outside
both paths; `make release-tag` and the `tag` job are the only sanctioned
paths.

## 7. Artifacts, `proof.json` and the ledgers

The merged artifact `proof-<run_id>-a<n>` carries `proof.json`, the merged
`unit-tests.json`, `integration-tests.json` and `ui-e2e-tests.json`, both
plans, every part's `part.json`, and the logs of failed jobs. It is
self-sufficient for a costs refresh: the merged UI ledger carries each spec
file's `wall_seconds`.

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
- `host_speed_failures`, `known_gaps`, `binary_digest_notes`;
- `lists_changed`, the gate files the change edits and the rows it adds or
  removes;
- `pr` and `ci`.

A ledger entry has `package`, `test`, `job`, `outcome`, `seconds`, an optional
`reason`, `allowed_skip`, `quarantine` and `failures`. The outcomes are
`pass`, `fail`, `skip`, `none`, `deferred` and `quarantined-fail`. The words,
defined once: **listed** is what the binary reports; **run** is asked for;
**deferred** is a nightly-listed test the full tier did not run; **expected**
is every test with one required outcome; **out of tier** is a listed test the
tier does not expect.

A part's `part.json` holds its identity (`part`, `attempt`, `tier`, `tree`,
`tree_end`, `clean_start`, `clean_end`, `base_commit`, `image_tag`), its
gates, the plan hashes it read, its runner, its image digests and its ledger
names. It carries no verdict.

## 8. Reading a failed proof

1. Read `proof.json`: `result`, `merge_eligible`, `ineligible_reasons` and
   `reruns`.
2. For a missing or duplicate test, the merged integration ledger names it and
   its shard; that shard's `part.json` names its artifact.
3. For a failed test, the shard's `integration/<job>.log` and
   `integration.events.jsonl` hold the test's own output; the ledger row's
   `job` names the log.
4. For a part with no result, `ineligible_reasons` says whether GitHub lost the
   runner (that one job may be re-run) or the attempt is not rerun-eligible
   (start a new run).
5. For a gate, the merged README's gate line names the part and log; download
   that part's artifact.

Only a part GitHub annotated as a **lost runner** may run again, and only
"Re-run job" on that job. "Re-run all jobs" and "Re-run failed jobs" give some
or every part a second attempt after a result, so their proof is ineligible. A
test failure is never answered by a rerun.

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
works against `make infra-up`; use a unique `ORK_TEMPORAL_NAMESPACE` whenever
anything else may run.

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
map stays green.

**Add a scope row.** A new top-level path needs a row in
`scripts/proof-scope.tsv`; a test refuses an unmapped path.

**Recover from a lost runner.** Confirm GitHub annotated the runner lost, then
"Re-run job" on that job, on the same day, while the plan and build artifacts
exist. The merger records the rerun and the unfinished tests. Any other
missing part needs a new run of the same tree.

**After a setup failure, or a "Re-run job" past the artifact's retention.**
Start a new run of the same tree (push again, or close and reopen the pull
request). After a test failure, never: fix it, with evidence.

**On a private repository, start the proof** after adding the `ci:proof`
label: push again, or close and reopen.

**Get a CI proof of a candidate before the gate is enforced.** Push it to
`validate/full/<name>` with its base as the first parent; the proof compares
by tree, never by commit. A feature proof uses `validate/feature/<name>`.

**Run the nightly proof after a merge** and pass it to the next
`make merge-check PROOF=… NIGHTLY=…`.

**Enable the merge gate.** Once the repository is public:
`make github-merge-gate APPLY=1`. It checks the owner, the default branch and
the workflow's presence, and refuses loudly otherwise. It is not run from a
private repository.

**Stop or clean up a proof.** `make proof-stop PROOF=proof/<name>` stops the
proof you started and the containers it labelled. `make proof-clean` lists the
buckets and work roots failed tests kept; `make proof-clean REMOVE=1` removes
one only once the process its name records is gone.

## 11. Reference

### 11.1 Make targets

| Target | Does |
|---|---|
| `make check` | build, vet, lint and the unit suites with `-short`; no Docker |
| `make proof` | the full proof, the merge gate |
| `make proof-ui` | the same; the full proof already runs the UI suites |
| `make proof-feature` | a feature proof of this branch |
| `make proof-nightly` | the nightly tier |
| `make proof-release` | the pre-release gate |
| `make release-tag` | tag HEAD after a passing release proof of it |
| `make proof-flakes` | derive per-test flake rates from the ledgers |
| `make proof-slow-list` | regenerate the costs and the slow list from a proof |
| `make proof-observability` | only the observability gate; never merge-eligible |
| `make proof-stop` | stop one proof and everything it started |
| `make proof-clean` | list or remove what failed tests kept |
| `make github-merge-gate` | check or apply the required-check settings |

### 11.2 The lists

| File | Format | A bad row |
|---|---|---|
| `scripts/proof-scope.tsv` | pattern, actions, flags | refuses the feature tier |
| `scripts/proof-allowed-skips.txt` | suite, package, test, reason | a skip with no entry, or an entry naming no listed test, fails |
| `scripts/proof-nightly-tests.txt` | package, test, covering tests, approved, reason | a stale name or a missing covering test fails the plan |
| `scripts/proof-quarantine.txt` | suite, package, test, owner, since, review-by, reason | structure, budget, a stale name, or an allowed-skip overlap fails; an expired entry stops excusing |
| `scripts/proof-test-costs.tsv` | package, test, seconds | an unmeasured test gets the conservative default |
| `scripts/proof-slow-tests.txt` | package, test | a slow test is left out of a feature proof's regression set |
| `scripts/proof-ui-costs.tsv` | spec file, wall seconds | an unmeasured spec gets 160 s |
| `scripts/proof-host-speed.tsv` | spec, bound, reference CPUs | a miss below the reference is recorded, never a wider bound |

### 11.3 `proofrun`

| Subcommand | Flags |
|---|---|
| `unit` | `-out FILE`, `-allow FILE`, `-quarantine FILE`, `-quarantine-base FILE` |
| `integration` | `-out DIR`, `-bin DIR`, `-jobs N`, `-namespace-prefix S`, `-costs FILE`, `-allow FILE`, `-quarantine FILE`, `-quarantine-base FILE`, `-select FILE`, `-run REGEX`, `-plan-only`, `-plan FILE`, `-shard K`, `-shards N`, `-matrix-out FILE`, `-nightly FILE`, `-nightly-mode MODE`, `-nightly-base FILE`, `-tier TIER`, `-proof-id S`, `-base-commit SHA`, `-merge-base SHA`, `-observability-required`, `-print-progress`, `-ci`, `-lists-changed FILE`, `-pr-number N`, `-pr-head-sha SHA`, `-pr-base-sha SHA`, `-pr-contains-base BOOL`, `-ui-shards N` |
| `select` | `-base REF`, `-out FILE`, `-scope FILE`, `-slow FILE`, `-tests PKG:REGEX`, `-specs FILE` |
| `feature-report` | `-dir DIR`, `-select FILE` |
| `ui-ledger` | `-report FILE`, `-list FILE`, `-out DIR`, `-config FILE`, `-log FILE`, `-shard K`, `-host-speed FILE` |
| `ui-plan` | `-list FILE`, `-costs FILE`, `-shards N`, `-out FILE`, `-matrix-out FILE` |
| `record` | `-dir DIR`, `-tier TIER`, `-name S`, `-commit SHA`, `-branch S`, `-tree-start SHA`, `-tree-end SHA`, `-clean-start BOOL`, `-clean-end BOOL`, `-base-ref REF`, `-base-commit SHA`, `-merge-base SHA`, `-jobs N`, `-ui-workers N`, `-temporal MODE`, `-temporal-image IMAGE`, `-temporal-address ADDR`, `-image-tag TAG`, `-images FILE`, `-image-tag-own`, `-gates-override S`, `-observability-required`, `-part NAME`, `-attempt N`, `-out-dir DIR`, `-plan FILE`, `-ui-plan FILE`, `-select FILE`, `-status N` |
| `merge` | `-plan FILE`, `-ui-plan FILE`, `-parts DIR`, `-jobs DIR`, `-host-speed FILE`, `-out DIR`, `-name S`, `-ci-run-id S`, `-ci-attempt N`, `-ci-event S`, `-ci-url URL` |
| `flakes` | `-from GLOB`, `-repo DIR`, `-window N`, `-out FILE`, `-md FILE`, `-min-runs N` |
| `verify` | `-proof DIR`, `-nightly DIR`, `-release`, `-tag S` |
| `temporal-args` | `-compose FILE` |
| `costs` | `-from DIR`, `-costs-out FILE`, `-slow-out FILE`, `-ui-costs-out FILE`, `-threshold SECONDS` |
| `push-layout` | `-layouts DIR`, `-registry HOST`, `-namespace S`, `-tag TAG`, `-out FILE` |
| `layout-digests` | `-layouts DIR`, `-registry HOST`, `-namespace S`, `-tag TAG`, `-out FILE` |
| `evidence-args` | `-body FILE` |
| `free-port` | none |
| `touches` | `-base REF`, `-action NAME`, `-scope FILE` |
| `gates` | `-select FILE` |

### 11.4 The scripts

`scripts/proof-observability.sh` runs the observability gate.
`scripts/proof-part.sh` runs one CI part. `scripts/proof-stop.sh` stops a
proof. `scripts/proof-clean.sh` lists and removes what failed tests kept.
`scripts/proof.sh` is the entry point behind every proof target.

### 11.5 Measured numbers

- The full proof's baseline on a 16-core host: about 90 minutes before
  sharding, with the integration suite at 3,693 s and Playwright at 1,598 s.
- The sharded CI graph's reference: ten integration shards, five UI shards and
  four activity-fixture shards, about 306–426 runner-minutes.
- `make check` measured on the 16-core development host: **34 s warm** with a
  full module and test cache, and **59 s** pinned to 2 CPUs (`taskset -c 0-1`)
  when the known quarantined pin subtest failed; the check itself completed.
  A cold cache recompiles the suites and is the number the `ci.yml` › `check`
  job records on a runner.
- A CI runner is 2 vCPU / 8 GB; `PROOF_JOBS=2`, ten integration shards and
  five UI shards fit the budget.

## 12. Known gaps

- **The gate is not enforced yet.** Rulesets and branch protection are
  plan-gated on a private repository; `scripts/github-merge-gate.sh` is the
  one-command enablement, and it refuses until the repository is public.
  Until then `main` moves only by `AGENTS.md` §11.3's local path.
- **The merge queue needs an organization-owned repository.** Queue-less mode
  is the default: `proof` runs the real proof on the pull request, and
  `strict_required_status_checks_policy` makes the tested tree the landed
  tree.
- **The gate has not had a full green run on GitHub.** Its structure was
  validated end to end; the first green run is planned for the public
  repository. Until then the gate is built, structurally validated and not
  enforced.
- **Runner-size known gaps.** A UI spec's 5 s wall-clock budget cannot hold on
  a 2-vCPU runner. `scripts/proof-host-speed.tsv` names those specs and their
  reference CPU count; a miss below it is recorded with its measured message,
  never turned into a wider local bound.
- **The observability gate has not run on GitHub.** The first release run,
  which always runs it, is its calibration.
- **`nightly.yml`, `release.yml`, `workflow_call` and the flakes job have
  not run on GitHub yet.** Their YAML passes `actionlint`.
- **The nightly soaks' thresholds were measured on a 16-core host.** A
  threshold missed on a 4-vCPU public runner is reported as a calibration
  result, never silently changed.
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
