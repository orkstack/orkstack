# How OrkStack is proven

Learn what the test suite covers, how each change is checked before it lands, and why you can trust a result. Read this before you trust OrkStack with work that matters.

OrkStack has not reached 1.0, and until it does any release may include breaking changes. The repository calls its checks **proofs**.

## What the tests cover

Each failure case below needs a test beyond the unit tests, or it is listed as a known gap. Where a test exists, it ends in a correct stored state, or in an explicit failure with a reason.

| Scenario | What the tests check |
|---|---|
| An Orker is lost during a task | At each step: while the container runs, after it exits, during the upload, and after the commit is written. A committed result is not run again. |
| Outages | An Orker restarts. Object storage or Temporal is unreachable for a while. The run ends in a correct state. |
| A container misbehaves | Exit `1`, exit `0` without `values.json`, malformed JSON, oversized outputs, a timeout, a cancel, a `SIGTERM` the container ignores, a missing input, and an image that cannot be pulled. Each one ends with a reason. |
| Pins | Every task of a pin lands on one Orker. A lost pin host fails the run, and the pin is not reopened elsewhere. |
| Release rollouts | A rollout while a pin is open, or while a run carries on. |
| Failure handlers | They run to their own outcomes, and the run ends `FAILED` with its original reason. |
| Runs that carry on | A run carries on with work in flight, and with saved state that is corrupted or missing. It ends with an explicit reason. |

## How changes are checked

Each change runs through the same checks, at different scopes.

| Check | When it runs | Counts as merge evidence |
|---|---|---|
| Feature proof | On a branch, for the tests that cover the change | No |
| Full proof | Before a change lands on `main`: every test against a real cluster and object store, and the unit tests under the race detector | Yes |
| Nightly proof | On `main`: the largest scale and soak tests, the busiest packages' unit tests raced three times over, and every fuzz target for a bounded time | No |
| Pre-release proof | Before a release tag, on the exact commit | Required for a release |

A change is done only after a full proof passes on the exact tree that lands.

## Why you can trust a result

A result is bound to one exact tree. A test that skips does not pass. Every test the binaries report must have a recorded outcome, so none goes missing.

A failing test is not rerun to make it pass. A known flake is quarantined, with an owner, a reason and a review date. It still runs, and its failure is recorded as a failure.

A test that checks something did not happen also checks that the system kept making progress in the same window. A slow host cannot make it pass.

## What is not yet proven

Some claims have no end-to-end test yet. The repository lists each one as a known gap, so you can see what is still open.

## See also

- [docs/PROVEN.md, What is proven](https://github.com/orkstack/orkstack/blob/main/docs/PROVEN.md#what-is-proven): the scenarios, and the map from each one to its tests.
- [docs/PROVEN.md, The proof tiers](https://github.com/orkstack/orkstack/blob/main/docs/PROVEN.md#the-proof-tiers): what each tier runs.
- [docs/PROVEN.md, Why the evidence can be believed](https://github.com/orkstack/orkstack/blob/main/docs/PROVEN.md#why-the-evidence-can-be-believed): binding, skips, quarantine and verdicts.
- [docs/PROOFS.md, Known gaps](https://github.com/orkstack/orkstack/blob/main/docs/PROOFS.md#12-known-gaps): every claim with no end-to-end test.
