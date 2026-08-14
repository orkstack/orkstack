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
2. Write a behavior test first (see AGENTS.md §5).
3. Implement until green.
4. `make lint test` must pass; run integration tests where relevant.
5. Write/refresh the proof artifact under `proof/<area>/` (AGENTS.md §6).
6. Conventional Commit; keep `main` buildable.

## Definition of DONE
Behavior tests pass **and** a reproducible proof artifact exists. No proof, not
done.

## Style
- Go: `gofmt`, wrapped errors, `context.Context` first, no panics in libraries.
- Keep the Temporal workflow layer deterministic (AGENTS.md §1.3).
- Small, focused commits and PRs.
