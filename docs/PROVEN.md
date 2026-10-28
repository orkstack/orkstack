# How OrkStack is proven

This page is for engineers deciding whether to trust OrkStack with
consequential work. It says what is tested, how, and why the results can be
believed, and it links the tests and rules rather than restating them:
[AGENTS.md](../AGENTS.md) §11 states the rules, and
[PROOFS.md](./PROOFS.md) is the reference for the machinery.

OrkStack is pre-release. AGENTS.md §11 forbids calling it production-ready
until the required failure and recovery scenarios are proven, and this page
makes no such claim. Activities run at least once, never exactly once
(AGENTS.md §4): a durably committed result is not run again, and a stable
idempotency key lets the systems an activity calls recognise a repeat.

## The principle: no proof, no done

A change is finished when its behaviour has been observed, not when its code
reads right. It is done only when it is on `main` and a full proof passed on
the exact tree `main` then points at, and every reference doc that describes
it says the new thing (AGENTS.md §11.2). `main` moves only to a commit whose
tree a full proof passed on (§11.3).

## What is proven

AGENTS.md §11 lists the failure and recovery scenarios a merge must prove.
[`scripts/proof-section11-coverage.tsv`](../scripts/proof-section11-coverage.tsv)
maps every one of them to the tests that prove it, and a test of its own
(`TestSection11Coverage` in [`test/tools/proofrun`](../test/tools/proofrun/))
holds that map true: every test it names must be declared in its package with
the build tag its kind says, and every claim needs evidence beyond a unit test
that runs in the merge gate, or a row that names its known gap.

In plain language, the scenarios are:

- **An Orker lost at every step of an attempt**: while its container runs,
  after the container exits, during the artifact upload, and after
  `commit.json` is written but before the result is reported. Each ends in
  the right durable state, and an attempt whose result was committed is not
  run again.
- **Outages and restarts**: an Orker restart, object storage failing for a
  while (in the engine and at the Orker's own upload and commit), and the
  Temporal cluster unreachable for a while.
- **What a container can do wrong**: exit 1, exit 0 without `values.json`,
  malformed JSON, outputs over their limit, a timeout, a cancellation, an
  ignored SIGTERM, an input artifact that is gone, and an image that cannot be
  pulled, each ending with its reason and retried only as its category
  allows.
- **Pins and the loss of their host**: every task of a pin landing on one
  Orker among three, no failover when that host dies, a host lost between two
  members, a committed pinned attempt adopted on retry, a run terminated with
  a pin open, and every pin host saturated.
- **Release rollouts**: a new release rolled out while a pin is open, and
  while a run carries on as new.
- **Failure handlers**: a failing run's handlers run to their own outcomes
  and the run still ends `FAILED`; an operator's cancel starts none and stops
  running ones; an Orker killed under a handler retries it per its policy.
- **A run carrying on as new**: with tasks running, with awaits and pins in
  flight, with engine replicas and Orkers killed and the cluster unreachable
  at the switch, with its saved state corrupted or missing, with history
  deleted or the event projection down, and at the task and item ceilings. In
  every case the run ends in a correct durable state with one run id and a
  contiguous event stream, or fails explicitly.

<!-- site-generated: section11-coverage -->

A claim with no end-to-end test is listed as a known gap, never left out:
[PROOFS.md](./PROOFS.md) §12 names every one.

## The proof tiers

| Tier | What it runs | Merge evidence |
|---|---|---|
| Feature proof (`make proof-feature`) | the fast gates, the tests and specs that prove the change (its **evidence**), and the integration tests of every package the change reaches (its **regression set**) | never |
| Full proof (`make proof`, or CI's `proof` check) | every gate: lint, build, unit, vet, the activity images, every integration test against a real Temporal cluster and S3-compatible store, the activity contract fixture, the UI's type-check and unit suites, the site's check and build (which also renders every doc), the race gate (the unit tests under the race detector), every Playwright spec, and the observability gate when telemetry changed | yes |
| Nightly (`make proof-nightly`) | only the scale and soak tests [`scripts/proof-nightly-tests.txt`](../scripts/proof-nightly-tests.txt) lists and the Playwright specs [`scripts/proof-nightly-specs.txt`](../scripts/proof-nightly-specs.txt) lists, each beside the merge-gate tests or specs that cover the same behaviour at a smaller size, and two lanes of its own: the unit tests of the concurrency-heavy packages raced three times over, and every fuzz target for a bounded time | no |
| Pre-release (`make proof-release`) | the full proof with nothing deferred; a release tag requires one for its exact commit | a tag precondition |

A feature proof is scoped by the diff and is never merge evidence. A full
proof records each nightly-listed test as `deferred`, never as passed, and a
failing nightly is a failing `main` that is fixed or reverted before anything
else merges (AGENTS.md §11). [PROOFS.md](./PROOFS.md) §2 and §6 describe each
tier as it runs today.

The full proof is built to run sharded in CI as the required `proof` check.
Until that gate is enforced on GitHub ([PROOFS.md](./PROOFS.md) §12), `main`
moves only by AGENTS.md §11.3's local path: a full proof of the merge
candidate, quoted in the merge commit, checked by
`make merge-check PROOF=…`.

## Why the evidence can be believed

- **Bound to a tree.** A proof is of the working tree's hash, which must be
  the same when it starts and when it ends; a merge lands exactly the tree
  the gate tested, and `make merge-check PROOF=…` refuses anything else
  ([PROOFS.md](./PROOFS.md) §1, §6).
- **A skip is never a pass.** The only tests a proof may see skip are listed
  in [`scripts/proof-allowed-skips.txt`](../scripts/proof-allowed-skips.txt),
  each with its reason; any other skip fails its gate. The one exception is
  the race gate, which runs `-short`: a test it skips counts only because the
  unit gate, which runs it without `-short`, passed it
  ([PROOFS.md](./PROOFS.md) §2.3).
- **Nothing goes missing.** The ledgers list every test the compiled
  binaries report; a listed test without an outcome fails the proof, and so
  does an allowance that names no test ([PROOFS.md](./PROOFS.md) §7).
- **Negative results carry their positive control.** A test that proves
  something did not happen within a window also observes, in that window,
  that the system was making progress, so a slow host cannot make it pass
  (AGENTS.md §11.1).
- **Flakes are owned, never rerun away.** A failing test is never rerun until
  it passes. A known flake is quarantined in
  [`scripts/proof-quarantine.txt`](../scripts/proof-quarantine.txt) with an
  owner, a reason and a review date; it still runs, and its failure is
  recorded as `quarantined-fail`, never as a pass. The list changes only with
  the maintainer's approval.
- **The verdict is re-derived.** `proofrun merge` trusts nothing a shard
  wrote as a verdict: it re-derives every ledger entry from the evidence
  ([PROOFS.md](./PROOFS.md) §7.1).
- **The evidence is kept.** Proofs are never committed; the commit or pull
  request quotes the proof directory, the tree and the gates that ran, and a
  CI proof's `proof.json` and ledgers are uploaded as the run's artifacts
  ([PROOFS.md](./PROOFS.md) §7).

## The product invariants

These are the guarantees every test above defends. From
[AGENTS.md](../AGENTS.md) §15, verbatim:

- an accepted execution cannot disappear;
- a durably committed activity result cannot become pending again;
- an Orker crash or activity crash cannot corrupt Temporal workflow state;
- object-storage failure cannot silently produce successful execution;
- no activity needs an OrkStack SDK or control-plane network access;
- no Temporal workflow state exists exclusively inside an OrkStack process;
- every terminal failure has machine-readable and human-readable reasons;
- an engineer must never edit a database record to unstick an execution.
