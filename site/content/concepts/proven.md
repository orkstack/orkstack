# How OrkStack is proven

Learn what is tested, how a change is checked before it lands, and what is still open. Read this before you trust OrkStack with work that matters.

## What is tested

Each case below has tests that run against a real Temporal cluster and a real S3-compatible object store. Where a test exists, the run ends in a correct stored state, or in a failure with a reason.

- **An Orker is lost during a task.** This is tested at each step: while the container runs, after it exits, during the upload, and after the commit is written. A committed result is not run again.
- **Restarts and outages.** An Orker restarts, object storage is unreachable for a while, or Temporal is unreachable for a while.
- **A container misbehaves.** It exits `1`, exits `0` without `values.json`, writes malformed or oversized output, times out, is cancelled, ignores `SIGTERM`, is missing an input, or its image cannot be pulled. Each one ends with a reason.
- **Pins.** Every task of a pin runs on one Orker. If that host is lost, the run fails with a reason, and the pin is not moved to another Orker.
- **Release rollouts** while a pin is open, or while a run carries on.
- **Failure handlers** run to their own outcomes. The run still ends `FAILED` with its original reason.
- **Runs that carry on.** A run keeps one run ID and one event stream while its work is in flight, and when its saved state is damaged or missing. It ends in a correct state, or in an explicit failure.

## How a change is checked

A change lands only after a full proof passes on the exact tree that lands. The full proof runs every test against a real cluster and object store, and the unit tests under the race detector. A result belongs to one exact tree.

Two rules keep the results honest. A test that skips does not pass. And every test the binaries report must have a recorded result, so none goes missing. A failing test is not rerun until it passes. A known flaky test is quarantined, with an owner, a reason and a review date. It still runs, and its failure still counts.

The scale and soak tests run every night on `main`, not on each change. The checks from a branch to a release are listed in [Contribute](/docs/contributing/).

## What is still a known gap

- **A tag that moves between two attempts** of one task has no end-to-end test yet.
- **The GitHub rule that requires the full proof** before a pull request lands is not switched on yet. Until it is, a maintainer runs the local merge check before `main` moves.
- **Some timing budgets** hold exactly only on the reference host. On a smaller CI runner, the run page's 5 s and the live run read's 2 s are held to that bound scaled to the runner's CPUs, so a release proved there rests on the wider bound.

Read the gaps before you rely on a case.

## See also

- [docs/PROVEN.md, What is proven](https://github.com/orkstack/orkstack/blob/main/docs/PROVEN.md#what-is-proven): the scenarios, and the map from each one to its tests.
- [docs/PROVEN.md, The proof tiers](https://github.com/orkstack/orkstack/blob/main/docs/PROVEN.md#the-proof-tiers): what each check runs.
- [docs/PROVEN.md, Why the evidence can be believed](https://github.com/orkstack/orkstack/blob/main/docs/PROVEN.md#why-the-evidence-can-be-believed): binding, skips, quarantine and verdicts.
- [docs/PROOFS.md, Known gaps](https://github.com/orkstack/orkstack/blob/main/docs/PROOFS.md#12-known-gaps): every claim with no end-to-end test.
