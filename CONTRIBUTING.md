# Contributing to OrkStack

Read [AGENTS.md](./AGENTS.md) first — it is the authoritative engineering
contract. This file is the quick checklist.

## Before you code
- `make tools && make proto && make build && make test` should pass on a clean
  checkout.
- Start infra when touching Temporal or storage: `make infra-up`. On a Linux
  host, do the one-time `host.docker.internal` setup in the README's "Linux
  hosts" first.

## The fast path
`make check` runs the build, vet, lint and the unit suites with `-short`; it
needs no Docker and no Temporal, and takes about two minutes. Run it on every
change. `make proof-feature` adds your change's evidence; `make proof` is the
full merge gate and is never needed locally before you open a pull request —
CI runs it. See [docs/PROOFS.md](./docs/PROOFS.md) for the tiers, what CI
runs, and how to read its result.

## The loop
1. Cross-process type change? Edit `proto/`, run `make proto`, commit generated
   code in the same change.
2. New configuration setting? Add it to `internal/config`'s `Schema`, to
   `docs/DEPLOY.md`'s configuration table, and run `make config-defaults`
   (AGENTS.md §11.1). Changed a DSL or automation key, or a first-party
   image's interface label? Run `go run ./internal/schemas/cmd/genschemas`.
3. Write a behavior test first (AGENTS.md §11.1).
4. Implement until green.
5. `make lint test` must pass; run the affected integration tests against
   `make infra-up` when Temporal or storage is involved
   (`scripts/integration.sh ./internal/engine -run '^TestIntegration_X$'`, or
   `make test-integration` for all of them); for the UI,
   `cd ui && bun run check && bun run test:unit`.
6. Produce the change's evidence with a feature proof that names the tests and
   specs proving it:
   `make proof-feature TESTS='internal/engine:TestIntegration_X' SPECS='ui/e2e/x.spec.ts'`.
   It writes `proof/<name>-feature/`, gitignored, and refuses (exit 2) a
   change it cannot account for: a path `scripts/proof-scope.tsv` does not
   map, or a changed package no evidence test depends on (AGENTS.md §11.2).
7. Conventional Commit that quotes the feature proof; keep `main` buildable.
8. Merge through AGENTS.md §11.3: on a candidate branched from `main`'s tip,
   `make merge-check`, then `make proof` (the full proof), amend the merge
   commit to quote it, `make merge-check PROOF=proof/<name>`, and
   `git merge --ff-only`.

## Definition of DONE
A change is done when it is on `main` and a full proof (`make proof`) passed
on the exact tree `main` then points at, with the commit quoting the lines of
that evidence that support each claim. A feature proof
(`SCOPED FEATURE PROOF — NOT MERGE EVIDENCE`) means "ready for the merge
proof", never "done". The evidence itself is never committed. No proof, not
done.

## Style
- Go: `gofmt`, wrapped errors, `context.Context` first, no panics in libraries.
- Keep the Temporal workflow layer deterministic (AGENTS.md §2.1). A change to
  what `RunWorkflow` commands or sends must keep every recorded history
  replaying; see
  [internal/engine/testdata/history/README.md](./internal/engine/testdata/history/README.md).
- Small, focused commits and PRs.
