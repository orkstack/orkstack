<!--
Thanks for the change. CONTRIBUTING.md is the checklist. CI's `proof` check
is the merge gate; docs/PROOFS.md says what each proof tier runs and how to
read it. You never need the full proof locally.

The title must be a Conventional Commit, such as
`fix(orker): keep the shared directory on a retry`: this pull request lands as
one squash commit whose subject is its title and whose body is this description.
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
- Can this change affect the people who run or use OrkStack (behaviour, the
  DSL, the CLI, the API and its error reasons, configuration, the activity
  contract, storage layout, what a release publishes and any security fix to
  it, including a vulnerable dependency's bump, a deprecation)? Then add its
  CHANGELOG.md `[Unreleased]` entry: a sentence or two on what changed and what
  they must do, if anything, an absolute link to the doc where one holds more
  detail, and a link to this pull request; a breaking change is marked
  **BREAKING**. A change that cannot affect them gets none: documentation,
  tests, refactors, a cosmetic change to the UI, internal tooling, a dependency
  bump that cannot affect them, and CI or proof changes that leave what a
  release publishes as it was.

## Checklist

- [ ] `make check` passes (no Docker, under two minutes).
- [ ] The evidence above runs and passes.
- [ ] Docs are current; CHANGELOG.md has an entry if the change needs one.
- [ ] The branch contains `main`'s tip.
