# Contributing to OrkStack

Read [AGENTS.md](./AGENTS.md) first — it is the authoritative engineering
contract. This file is the quick checklist.

## Fast checks (no Docker, under two minutes warm)

`make check` runs `make lint` (`go vet` and gofmt), `go vet -tags=integration`
over the integration suites, and the unit suites with `-short`, which skips
the scale tests and the tests that compile binaries; it keeps the test cache.
Every proof runs those, without `-short`. Run it before every push. For the UI:
`cd ui && bun run check && bun run test:unit`.

When a test is listed in `scripts/proof-quarantine.txt`, its failure is that
known flake: tell its owner if it is new to you. Never re-run a failing test until it
passes; a second run is the flip the list exists to count, not a remedy.

Start infra only when touching Temporal or storage: `make infra-up`. On a
Linux host, do the one-time `host.docker.internal` setup in the README's
"Linux hosts" first.

## What CI runs on your pull request

- **`check`** — `make check`, on every push and every pull request, with
  `DOCKER_HOST` pointing at nothing to prove the fast path needs no Docker.
- **`feature`** — the feature tier: the fast gates, the tests and specs that
  prove your change, and its regression set. Name evidence the selection
  cannot find in your PR description:

  ```text
  Proof-tests: internal/engine:TestIntegration_X
  Proof-specs: ui/e2e/x.spec.ts
  ```

  These are the same values as `make proof-feature TESTS=… SPECS=…`. A change
  to a path only the full proof covers (`scripts/`, `.github/`, the Makefiles,
  `test/tools/proofrun/`, …) skips this job; the `proof` check proves it.
  While the repository is private, `feature` runs only with the `ci:feature`
  label, on a push to `validate/feature/**`, or by dispatch, to keep metered
  minutes for the merge gate; it runs on every pull-request push once the
  repository is public.
- **`proof`** — the full proof, split across runners, the only required
  check. Read the result from the `proof-<run>-a<attempt>` artifact's
  `proof.json` (`result`, `merge_eligible`, `ineligible_reasons`); the job
  summary is written from it. It proves exactly the tree that lands, so a
  branch that does not contain `main`'s tip is asked to update.

Once the gate is enforced, a change to the gate's own files (the lists under
`scripts/proof-*`, `.github/`, `test/tools/proofrun/`, …) needs the
maintainer's approval. That is GitHub's code-owner ruleset, not a CI job.

**You never need to run the full proof locally.** CI runs it. `make proof`
keeps working, and is equivalent, for maintainers who merge locally. A local
proof runs the fast gates first and stops at its first failing gate, still
writing a failing verdict (`PROOF_FAIL_FAST=0` runs every gate to its end).

Everything else — tiers, sharding, artifacts, reading a failed proof,
procedures — is in [docs/PROOFS.md](./docs/PROOFS.md).

## The loop

1. Cross-process type change? Edit `proto/`, run `make proto`, commit generated
   code in the same change.
2. New configuration setting? Add it to `internal/config`'s `Schema`, to
   `docs/DEPLOY.md`'s configuration table, and run `make config-defaults`
   (AGENTS.md §11.1). Changed a DSL or automation key, or a first-party
   image's interface label? Run `go run ./internal/schemas/cmd/genschemas`.
3. Write a behavior test first (AGENTS.md §11.1).
4. Implement until it passes.
5. Run `make check`; for Temporal or storage changes, run the affected
   integration tests against `make infra-up`
   (`scripts/integration.sh ./internal/engine -run '^TestIntegration_X$'`, or
   `make test-integration` for all of them).
6. Update the docs the change makes false, and add the CHANGELOG.md
   `[Unreleased]` entry a user-visible change needs (AGENTS.md §11.1).
7. Produce the change's evidence with a feature proof that names the tests and
   specs proving it:
   `make proof-feature TESTS='internal/engine:TestIntegration_X' SPECS='ui/e2e/x.spec.ts'`.
   It writes `proof/<name>-feature/`, gitignored, and refuses (exit 2) a
   change it cannot account for.
8. Commit with a Conventional Commit that quotes the feature proof; keep
   `main` buildable.
9. Merge through AGENTS.md §11.3. CI's `proof` check is the merge gate; a
   local `make proof` is the equivalent path.

## Definition of DONE

A change is done when it is on `main` and a full proof passed on the exact
tree `main` then points at — the `proof` check of the pull request (or merge
group) that landed it, or a local `make proof` — and when every reference doc
that describes what changed says the new thing, with a CHANGELOG.md
`[Unreleased]` entry for a user-visible change. Done is judged by the merge
gate: the few scale tests listed in `scripts/proof-nightly-tests.txt` run
nightly and before a release, and a failure there is a failing `main`. A
feature proof means "ready for the merge proof", never "done". No proof, not
done.

## Style

- Go: `gofmt`, wrapped errors, `context.Context` first, no panics in libraries.
- Keep the Temporal workflow layer deterministic (AGENTS.md §2.1). A change to
  what `RunWorkflow` commands or sends must keep every recorded history
  replaying; see
  [internal/engine/testdata/history/README.md](./internal/engine/testdata/history/README.md).
- Small, focused commits and PRs.
