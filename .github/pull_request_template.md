<!--
Thanks for the change. CONTRIBUTING.md is the checklist. CI's `proof` check
is the merge gate; docs/PROOFS.md says what each proof tier runs and how to
read it. You never need the full proof locally.
-->

## What changed

<!-- One paragraph: the behaviour, not the diff. -->

## Evidence

<!--
Name the tests and specs that prove the change. The same values as
`make proof-feature TESTS=… SPECS=…`, which is optional; the `proof` check
is the gate.
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
