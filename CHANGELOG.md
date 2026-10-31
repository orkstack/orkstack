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
  publishes its terminal event at sequence 2 when object storage refused its
  `RUN_CREATED`: the stream and the archive held that event alone, with
  nothing at 1, for good. `RUN_CREATED` is now written again under the same
  number, and when that write fails too the run publishes nothing, so its
  close reads as its one event, at 1; such a run has no archive body, like
  every run that publishes no terminal event (`docs/DEPLOY.md` §6, "An ending
  before start writes `RUN_CREATED` first").

## [0.1.0] - 2026-10-30

First release.
