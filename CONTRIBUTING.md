# Contributing to OrkStack

Read [AGENTS.md](./AGENTS.md) first — it is the authoritative engineering
contract. This file is the quick checklist.

## Before you code
- `make tools && make proto && make build && make test` should pass on a clean
  checkout.
- Start infra when touching Temporal or storage: `make infra-up`.

## The loop
1. Cross-process type change? Edit `proto/`, run `make proto`, commit generated
   code in the same change.
2. Write a behavior test first (see AGENTS.md §11.1).
3. Implement until green.
4. `make lint test` must pass; run integration tests where relevant.
5. Produce the evidence: `make proof` (or `make proof-ui` for a UI change)
   writes it under `proof/<name>/`, gitignored (AGENTS.md §11.1).
6. Conventional Commit that quotes the proof; keep `main` buildable. Before
   merging, run `make merge-check` on the merged tree.

## Definition of DONE
Behavior tests pass **and** `make proof` is green for the gates the change
touches, with the commit or pull request quoting the lines of that evidence
that support each claim. The evidence itself is never committed. No proof, not
done.

## Style
- Go: `gofmt`, wrapped errors, `context.Context` first, no panics in libraries.
- Keep the Temporal workflow layer deterministic (AGENTS.md §2.1).
- Small, focused commits and PRs.
