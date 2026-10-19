# Proofs and CI

`AGENTS.md` §11 states the rules: no proof, no done, and what a proof must
establish. This file is the reference for how the machinery works: the tiers,
the plan and its shards, the CI job graph, the artifacts, and the procedures.
It is not a second statement of the rules — when the two disagree, `AGENTS.md`
wins.

## 1. The tiers

| Tier | Command | Merge evidence | Runs |
|---|---|---|---|
| **full** | `make proof` | yes, on a local host; in CI, `proof.yml`'s `proof` job | every gate, every integration test, every spec, the activity contract fixture, observability when the change touches a telemetry path |
| **feature** | `make proof-feature TESTS=… SPECS=…` | no | the fast gates, the change's evidence, its regression set |
| **nightly** | `make proof-nightly` | no | only the integration tests `scripts/proof-nightly-tests.txt` names |
| **release** | `make proof-release` | no (a tag precondition) | every gate, the nightly tests included, observability always |

A full proof defers a nightly-listed test only when the proof's **base** also
lists it. An entry the same change adds runs. This is what makes a change's
own new deferral impossible.

A local full, release or nightly proof runs its gates on one host and then
writes one part, `local`, and merges it with `proofrun merge`. The CI proof
job runs the same merger over many parts. There is one verdict implementation.

The feature tier writes `proof.json` itself; it is never merge evidence.

## 2. `proofrun`, the engine

Built from `test/tools/proofrun` (standard library only in non-test files).
Exit status: `0` pass, `1` a gate failed, `2` an invalid invocation.

| Subcommand | What it does |
|---|---|
| `integration` | compile, list, plan and run; `-plan-only` writes the plan; `-plan FILE -shard K` runs one shard of it |
| `unit` | run the unit suites and write their ledger |
| `ui-plan` | split the Playwright spec files by measured wall |
| `ui-ledger` | check the Playwright report against its listing |
| `record` | write a part (`part.json`), or a feature proof's `proof.json` |
| `merge` | merge a proof's parts into one `proof.json`, one set of ledgers and a README |
| `select` | decide what a feature proof runs, or refuse |
| `verify` | refuse a merge, or a tag, whose proof does not cover it |
| `costs` | regenerate the cost ledgers and the slow list |
| `push-layout` | push OCI image layouts byte for byte and check their digests |
| `layout-digests` | the `images.tsv` a layout push would produce |
| `evidence-args` | turn a PR body's `Proof-tests:`/`Proof-specs:` lines into flags |
| `touches` | whether a change touches paths of one scope action |
| `gates` | the gates and specs a feature selection runs |
| `temporal-args` | the compose Temporal service's image and arguments |
| `feature-report` | the feature README's scope sections |
| `free-port` | a port nothing listens on |

### 2.1 The plan

`integration-plan.json`, format 2, is the single description of what a proof
runs. A local proof is a one-shard, one-part plan; CI plans once and shards
the result.

```text
{
  "format": 2, "tree": "…", "commit": "…", "tier": "full",
  "proof_id": "pr-ci-…", "base_ref": "main", "base_commit": "…", "merge_base": "…",
  "observability_required": false,
  "costs": {"file": "scripts/proof-test-costs.tsv", "sha256": "…"},
  "nightly": {"file": "…", "sha256": "…", "mode": "defer|include|only",
              "entries": ["<package>\t<test>"], "base_commit": "…",
              "base_sha256": "…", "base_entries": ["…"]},
  "quarantine": {"file": "…", "sha256": "…", "base_sha256": "…"},
  "allowed_skips": {"file": "scripts/proof-allowed-skips.txt", "sha256": "…"},
  "parts_expected": ["plan", "build", "fast", "integration:1", "…"],
  "packages": [{"import_path": "…", "dir": "…", "binary": "…",
                "binary_sha256": "…", "listed": ["Test…"]}],
  "expected":  [{"package": "…", "test": "…"}],
  "deferred":  [{"package": "…", "test": "…", "entry": "<list line>"}],
  "unmeasured":[{"package": "…", "test": "…", "kind": "integration|unit"}],
  "jobs":      [{"id": "…", "package": "…", "tests": ["…"],
                 "estimated_seconds": 1.0, "measured": true,
                 "timeout_seconds": 600, "shard": 1}],
  "shards":    [{"index": 1, "jobs": ["…"], "estimated_seconds": 1.0,
                 "estimated_makespan_seconds": 1.0}],
  "requested_shards": 10, "slots_per_shard": 3, "problems": []
}
```

The sets, named once:

- **listed** — what a package's integration binary reports for `-test.list`.
- **run** — the tests in `jobs`; each is in exactly one job and one shard.
- **deferred** — the listed tests both the tree's and the base's nightly list
  carry; the full tier does not run them.
- **expected** — `run ∪ deferred`. Each has exactly one outcome, from one
  shard, or the deferred entry the merger writes.
- **out of tier** — in the nightly tier, every listed test the nightly list
  does not name. The ledger counts them and gives them no entry.

An unmeasured test is a job of its own: 300 s when it is declared in an
integration-tagged file, 30 s when it is declared in a file the unit build
compiles too. Both get the 30-minute timeout.

### 2.2 Sharding

`splitShards` sorts jobs longest first (package, then first test as tie
breaks), then puts each into the least-loaded shard, a tie going to the
lowest index. The floor is the longest single job; no number of shards goes
below it. The same inputs always give the same plan.

`ui-plan` splits Playwright spec files by their measured **wall** — their
tests' seconds plus the gap before each test, which is where page start,
per-file fixtures and `beforeAll` land. It passes each shard a file list;
Playwright's own `--shard` is never used.

### 2.3 Heavy tests

A job whose measured cost is at least 120 s, or an unmeasured
`TestIntegration_Soak*` test, is **heavy**. A heavy job runs alone:

- in CI, `splitShards` gives each heavy job a shard of its own, so it has its
  own Temporal and object store;
- locally, the runner runs every heavy job first, one at a time, and starts no
  ordinary job until the heavy phase is over — the local equivalent of a CI
  heavy shard. The soaks' GetRun latency thresholds are absolute wall-clock
  bounds, and measuring them with three other integration jobs on the machine
  is meaningless.

The cost is real: a local full proof's wall clock is the heavy tests one
after another plus the ordinary tests. That is the price of measuring a soak
on a quiet machine, and the merge gate does not trade a soak threshold for
speed. `proof.json`'s plan records each job's `heavy` flag.

### 2.4 Running a shard

```bash
proofrun integration -plan plan.json -shard K -out DIR -jobs 3 \
  -namespace-prefix pr-ci-123-sK
```

It refuses (exit 2) a checkout whose working tree hash differs from the
plan's, and a compiled binary whose listing differs from the plan's. The
listing is the identity that matters: it makes the plan's expected set this
binary's tests. A binary digest that differs from the plan's compile is a
README note, never a failure, because separate compiles need not be identical.

Reproduce one CI shard locally:

```bash
make infra-up
make -C images oci TAG=<plan.proof_id> OUT=/tmp/layouts
go run ./test/tools/proofrun push-layout -layouts /tmp/layouts \
  -registry localhost:5001 -tag <plan.proof_id> -out /tmp/images.tsv
scripts/proof.sh --part integration:K --plan plan.json --layouts /tmp/layouts \
  --proof-id <plan.proof_id> pr-ci-local
```

## 3. The CI job graph

`.github/workflows/proof.yml` is one graph behind the full, nightly and release
tiers. It triggers on `pull_request`, `merge_group`, a push to
`validate/full/**`, `workflow_dispatch`, and `workflow_call`.

| Job | Runs | Uploads |
|---|---|---|
| `plan` | compile and list the packages; write `integration-plan.json` and `ui-plan.json`; decide observability | `plan-a<n>` (both plans, the Playwright listing, the base lists, `part.json`) |
| `build` | build every activity image once as an OCI layout; record its digests | `part-build-a<n>` (layouts, `images.tsv`) |
| `fast` | lint, build, unit (no `-short`), vet-integration, UI type-check and unit | `part-fast-a<n>` |
| `integration` | one matrix entry per shard: its own Temporal, SeaweedFS, registry, images pushed from the layouts | `part-integration-<k>-a<n>` |
| `ui-e2e` | one matrix entry per UI shard: its own stack, its file list | `part-ui-e2e-<k>-a<n>` |
| `activity-fixture` | one matrix entry per activity: `make -C images contract` | `part-activity-fixture-<a>-a<n>` |
| `observability` | when the plan says it is due, or always for release | `part-observability-a<n>` |
| `proof` | `if: always()`; downloads every part and GitHub's job records; runs `proofrun merge`; writes the summary from `proof.json` | `proof-<run_id>-a<n>` |

`proof` is the single required check. It passes only when the merged
`proof.json` says `result: pass` and (for a full proof) `merge_eligible:
true`. A skipped required job reports success, which is why `proof` runs on
`always()` and nothing else.

On a draft pull request `proof` fails with "mark it ready for review". While
the repository is private, a pull request's heavy jobs also need the
`ci:proof` label; adding the label starts nothing until the next push or a
close/reopen, because `proof.yml` deliberately does not trigger on `labeled`.

`ci.yml` holds no required check: `check` is the fast path (no Docker), and
`feature` is the feature tier, on a public repository or with `ci:feature`.

### 3.1 The merger's rules

`proofrun merge` checks, and each failure is an `ineligible_reasons` line, a
ledger `problems` line, or both:

1. every expected part left a result, and every part that was not expected is
   ignored;
2. a part may run again only when GitHub annotated its earlier attempt as a
   **lost runner**. A later attempt after an attempt that produced a result
   makes the proof ineligible, and is never recorded as a rerun. A lost
   runner's rerun records how many tests had passed and which had started and
   not finished;
3. one tree, one base, clean at every part's start and end;
4. every stack part ran its own Temporal;
5. the image tag is the plan's own `proof_id`;
6. every part resolved the build's layout digests;
7. every shard's binary listings equal the plan's;
8. the integration ledger is complete: one outcome per expected test, no
   duplicates, no unplanned entry, each shard's problems carried verbatim;
   deferred entries are written by the merger, and the base anchor is
   recomputed from both lists' hashes;
9. the unit ledger is complete and problem-free;
10. the UI ledger covers every listed test exactly once, carries every spec
    file's wall, and stays at or under the network-change ceiling per host;
11. every gate of the tier ran and passed;
12. `verdict()` decides the result and merge eligibility;
13. a UI spec named in `scripts/proof-host-speed.tsv` that misses its bound on
    a runner below the reference CPU count is recorded in
    `host_speed_failures[]` and `known_gaps[]`, printed under the README's
    "Known gaps: runner size", and does not fail the proof; on the reference
    host the same miss is a failure.

### 3.2 Reruns

Only GitHub's runner-lost annotation makes an attempt rerun-eligible, and only
"Re-run job" on that job. "Re-run all jobs" and "Re-run failed jobs" give every
or some parts a second attempt after a result, so their proof is always
ineligible. After the plan and build artifacts expire (1 day on a private
repository, 7 on a public one), a re-run cannot find its inputs: start a new
run. A test failure is never answered by a rerun; fix it with evidence.

## 4. Artifacts and `proof.json`

The merged artifact `proof-<run_id>-a<n>` carries `proof.json`, the merged
ledgers (`unit-tests.json`, `integration-tests.json`, `ui-e2e-tests.json`),
both plans, every part's `part.json`, and the logs of failed jobs. It is
self-sufficient for a costs refresh: the merged UI ledger carries each spec
file's `wall_seconds`.

`proof.json` adds to the fields it always had: `parts[]`, `reruns[]`,
`plan{sha256, shards, jobs, costs_source, unmeasured, nightly_mode,
nightly_sha256, deferred}`, `ui_plan{sha256, shards, files}`,
`quarantine{checked_on, base_sha256}`, `nightly{sha256, base_commit,
base_sha256}`, `host_speed_failures[]`, `binary_digest_notes[]`,
`lists_changed[]`, `pr{number, head_sha, base_sha, contains_base}`, and
`ci{run_id, run_attempt, event, sha, base_sha, url}`.

A part's `part.json` holds its identity (`part`, `attempt`, `tier`, `tree`,
`tree_end`, `clean_start`, `clean_end`, `base_commit`, `image_tag`), its
gates, the plan hashes it read, its runner, its image digests and its ledger
names. It carries no verdict.

Read `proof.json`, never the README, for a verdict.

## 5. Nightly and pre-release

- `nightly.yml` runs at 05:17 UTC on the default branch, and by dispatch. A
  private repository runs it only by dispatch. A red nightly is a failing
  `main`: fix or revert the change that broke it before anything else merges.
  GitHub disables a public repository's schedules after 60 days without
  activity; `make github-merge-gate` reports that and
  `gh workflow enable nightly.yml` fixes it.
- `release.yml` is dispatchable. `gate` runs the release tier (14 integration
  shards, every gate, observability always); `tag` runs only with `tag: true`
  and only after `proofrun verify -release` accepts the proof for the exact
  commit. `make release-tag VERSION=vX.Y.Z PROOF=proof/<release>` is the local
  equivalent. Neither pushes without the tag input.
- While the repository is private, or no `main` holding this workflow has been
  pushed, `make proof` on a branch whose base lacks the nightly list runs
  every test; local `make proof-nightly` on `main`'s tip and
  `make merge-check PROOF=… NIGHTLY=…` stand in.

## 6. Reading a failed proof

1. Read `proof.json`: `result`, `merge_eligible`, `ineligible_reasons`, and
   `reruns`.
2. For a missing or duplicate test, the merged `integration-tests.json` names
   it and the shard; the shard's `part.json` names its artifact.
3. For a failed test, the shard's `integration/<job>.log` and
   `integration.events.jsonl` hold the test's own output; the job that ran it
   is the entry's `job`.
4. For a part with no result, `ineligible_reasons` says whether GitHub lost
   the runner (re-run that job) or the attempt is not rerun-eligible (start a
   new run).
5. For a gate, the merged README's gate line names the part and log; download
   that part artifact.

## 7. Procedures

**Add a test and get it measured.** Land it; the next proof plans it as
unmeasured (its own job, conservative cost) and its ledger row measures it.
Then refresh the costs (below) and commit the two files.

**Refresh the costs from CI ledgers.**

```bash
make proof-slow-list FROM=<downloaded proof-<run_id>-a<n> directory>
```

It rewrites `scripts/proof-test-costs.tsv`, `scripts/proof-slow-tests.txt` and,
from the merged UI ledger's file walls, `scripts/proof-ui-costs.tsv`. A source
that measured only part of the suite keeps every row another source's plan
listed.

**Move a test to nightly.** It needs the user's approval. Add its row to
`scripts/proof-nightly-tests.txt` with the covering merge-gate test named in
the third column and `<date> <name>` in the approved column; the plan refuses a
stale entry, a missing covering test, or a covering test that is itself on the
list.

**Quarantine a test.** It needs the user's approval. Add it to
`scripts/proof-quarantine.txt` with an owner, a since date, a review-by date
and a reason; an expired entry stops excusing. Never rerun until green.

**Add a scope row.** A new top-level path needs a row in
`scripts/proof-scope.tsv`; a test refuses an unmapped path.

**Recover from a lost runner.** Confirm GitHub's annotation says the runner
was lost, then "Re-run job" on that job, on the same day, while the plan and
build artifacts exist. The merger records the rerun and the unfinished tests.
Any other missing part needs a new run of the same tree.

## 8. Known gaps

- **The gate is not enforced yet.** Rulesets and branch protection are
  plan-gated on a private repository; `scripts/github-merge-gate.sh` is the
  one-command enablement, and it refuses until the repository is public. Until
  then `main` moves only by AGENTS.md §11.3's local path.
- **The merge queue needs an organization-owned repository.** The queue-less
  mode is the default: `proof` runs the real proof on the pull request, and
  `strict_required_status_checks_policy` makes the tested tree the landed tree.
- **Runner-size known gaps.** A UI spec's wall-clock budget (5 s at the gated
  sizes) cannot hold on a 2-vCPU runner. `scripts/proof-host-speed.tsv` names
  those specs and their reference CPU count; a miss below it is recorded with
  its measured message, never turned into a wider local bound.
- **The dev object store is SeaweedFS, pinned by digest.** MinIO stopped
  publishing images and moved to a closed product; Garage cannot do the
  conditional write the commit protocol needs. SeaweedFS documents
  `If-None-Match` creates as atomic cluster-wide, and the S3 commit probe runs
  against it in the integration suite. An image update is a dependabot PR and
  runs the proof like any other change.
- **Dependency updates are automated.** `.github/dependabot.yml` covers Go
  modules, Docker, GitHub Actions and the UI's bun dependencies, grouped and
  weekly with the `chore(deps):` prefix. Dependabot version updates may be
  refused on a private repository; the config stays in place, and an update
  never merges on a green unit run alone — it must pass `proof`.
- **The observability gate has not run on GitHub.** A runner's Prometheus
  reaches the admin listeners over the default bridge, as the spike's setup
  does locally; the first release run, which always runs the gate, is its
  calibration.
- `nightly.yml`, `release.yml`, `workflow_call` and the `flakes` job (a later
  change) have not run on GitHub yet. Their YAML passes `actionlint`.
- The nightly soaks' thresholds were measured on a 16-core host. A threshold
  missed on a 4-vCPU public runner is reported as a calibration result, never
  silently changed.
