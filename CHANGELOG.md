# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Each
breaking change is marked **BREAKING**.

A change lands with its entry under `[Unreleased]`. At a release that section
becomes the version's own, and its text is the release's notes on GitHub,
word for word.

## [Unreleased]

### Added

- Each proof part that starts its own Temporal, and a local `make proof`,
  keeps that Temporal server's log as `temporal-server.log.gz` (its last
  16 MiB, compressed), so an error a test read from Temporal can be traced.
- `docs/CI.md`: every CI workflow, its trigger, whether a merge waits for
  it, how long it takes and what to do when it is red, with the site page
  "Read a red CI run".

### Changed

- The full proof's `integration`, `ui-e2e` and `activity-fixture` shards
  start only once its `fast` part has passed, so a deterministic lint, build
  or unit failure no longer spends the shards' runner time. A `fast` that
  failed in a setup step runs again together with the shards. A green run
  is expected to take about 8 to 11 minutes longer.
- `proof-rerun.yml` also runs a failed nightly of `main` again when its only
  failures are parts whose runner was lost or measured below the floor, or
  that failed in a setup step.

### Fixed

- The proof's automatic rerun and its job records read GitHub's job logs
  again: gh 2.103 refuses to print a log that carries terminal escape
  sequences unless it is given `--allow-escape-sequences`, so every log read
  came back empty. The job records now keep a bounded log of each failed job:
  every line the merger reads and its last 200 lines. A log that cannot be
  read or kept, other than one GitHub does not have, fails its job closed.
- A proof shard cancelled, or whose runner was lost, before its artifact was
  uploaded is read by the test it was running: the integration gate now also
  prints its `proofrun` progress lines to the job's log, where none reached
  before.

## [0.1.0] - 2026-10-30

First release.
