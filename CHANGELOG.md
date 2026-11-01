# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). Each
breaking change is marked **BREAKING**.

A change lands with its entry under `[Unreleased]`. At a release that section
becomes the version's own, and its text is the release's notes on GitHub,
word for word.

## [Unreleased]

### Fixed

- A run whose definition, passed by reference, could not be used no longer
  serves a stream that silently begins at sequence 2 when object storage
  refused its `RUN_CREATED`. `RUN_CREATED` is now written again under the same
  number, so a store that is back for that write leaves both events, from 1.
  When it is refused again, the terminal event is still published at 2, and
  the missing 1 is said, not skipped: a read of the run's stream from its
  start now fails `DataLoss`, naming 1 as never published, live and from its
  archive, which records it as a gap line; a read from 1 serves the terminal
  event; and the run's evidence bundle is `schema_version` 2, with the gap in
  `event_gaps`, as every bundle of a gapped archive already was. A run a
  release before this left the same way, after one refused write, is read the
  same way (`docs/DEPLOY.md` §6, "An ending before start writes `RUN_CREATED`
  first").
- The notice of an archive gap names its cause: events "lost from object
  storage before the run was archived", or "never published".

## [0.1.0] - 2026-10-30

First release.
