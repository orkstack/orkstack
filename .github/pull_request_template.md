<!--
Thanks for the change. CI's `proof` check is the merge gate; docs/PROOFS.md
says what it runs and how to read it. You never need the full proof locally.
-->

## What changed

<!-- One paragraph: the behaviour, not the diff. -->

## Evidence

<!--
Name tests and specs the selection cannot find. The same values as
`make proof-feature TESTS=… SPECS=…`.
-->

Proof-tests:
Proof-specs:

## Documentation

- Which reference doc describes this change, and was it updated?
  (DSL keys and rules, CLI commands and flags, API fields, error reasons,
  configuration, the activity contract, storage layout, proof/CI behaviour.)
- Did you add the CHANGELOG.md `[Unreleased]` entry a user-visible change
  needs? A breaking change is marked **BREAKING**.

## Checklist

- [ ] `make check` passes (no Docker, under two minutes).
- [ ] The evidence above runs and passes.
- [ ] Docs and CHANGELOG are current.
- [ ] The branch contains `main`'s tip.
