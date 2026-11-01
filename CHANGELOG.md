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
  loses its `RUN_CREATED` without a word when object storage refuses it:
  before, its terminal event was published at sequence 2 with nothing at 1,
  and the stream and the archive kept it that way. `RUN_CREATED` is now
  written again under the same number, so a store that is back for that
  write leaves both events, from 1. When that write is refused too, the
  terminal event is still published at 2, and the run's archive records 1 as
  a gap that a read of the archive names (`docs/DEPLOY.md` §6, "An ending
  before start writes `RUN_CREATED` first").

## [0.1.0] - 2026-10-30

First release.
