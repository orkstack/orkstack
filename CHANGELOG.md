# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Each
breaking change is marked **BREAKING**.

A change lands with its entry under `[Unreleased]`. At a release that section
becomes the version's own, and its text is the release's notes on GitHub,
word for word.

## [Unreleased]

### Changed

- A task's `retry:` that sets no `maximum_interval_seconds` now waits at most
  60 seconds between attempts, the contract's suggested maximum, instead of
  100. The number is decided when a run is submitted and kept for the whole
  run, so runs submitted before the upgrade keep 100 seconds. Set
  `maximum_interval_seconds: 100` to keep the old spacing.
- Fair dispatch across namespaces now also keys each pin's request for a pin
  host, so where the cluster has fair dispatch enabled, pin requests are
  ordered by namespace as tasks are, rather than first in, first out. It
  applies to runs submitted from this release on. Fan-out items,
  pinned tasks, runs that carry on and composed child runs already carried
  the key; this is now stated and tested.

### Fixed

- A task whose `retry:` sets an `initial_interval_seconds` above the default
  maximum interval, and no `maximum_interval_seconds`, no longer leaves its run
  stuck: Temporal refused the retry policy and retried the refusal forever.
  An unset maximum interval is now never below the initial interval, in every
  run this release's engine executes, a run submitted before the upgrade
  included.

## [0.1.0] - 2026-10-30

First release.
