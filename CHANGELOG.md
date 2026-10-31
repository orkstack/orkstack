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
  60 seconds between attempts, instead of 100: the maximum interval
  OrkStack's engineering contract suggests (`maxInterval: 1m`, AGENTS.md
  §4). The number is decided when a run is submitted and kept for the whole
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
  included. On a deployment with versioning, a run v0.1.0 already left stuck
  stays on v0.1.0 and never gets the fix: cancel it, and terminate it if it
  stays running (UPGRADING.md, "Upgrading Orkflows with the release").
- **BREAKING**: An Orkflow whose `retry:` sets a negative
  `initial_interval_seconds` or `maximum_interval_seconds`, a
  `maximum_interval_seconds` below its `initial_interval_seconds`, or a
  `backoff_coefficient` below 1 or not finite, is now refused: validate
  reports an error at the field, and apply and plan refuse the document as
  an invalid argument, each naming the field and its value. Such a document
  applied and ran before. Temporal refused a maximum below the initial
  interval and a coefficient between 0 and 1, and retried the refusal
  forever, so every run that retried the task was stuck; the other values
  have no meaning as a wait. The refusal holds whatever `maximum_attempts`
  says, so a document that pairs such a policy with one attempt is refused
  too: fix the document before you next apply it. A version applied before
  this release keeps starting, and such a task is scheduled with the nearest
  policy Temporal accepts: the maximum raised to the initial interval, a
  coefficient between 0 and 1 raised to 1, and, in a run submitted from this
  release on, a coefficient that is not a number read as the default 2.

## [0.1.0] - 2026-10-30

First release.
