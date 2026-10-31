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

- An Orker that declares no capacity says at boot that its admission is best
  effort, bounded only by `max_concurrent`
  (`capacity not declared: admission is best effort, bounded only by max_concurrent`).
- `docs/DEPLOY.md`, "Every Orker of a pool has the same settings": the settings
  every Orker of a pool shares, and the order in which to change one with runs
  in flight — widen every Orker before any task needs more, narrow only once no
  run does.

### Fixed

- An Orker's own policy checks — its cpu and memory caps, secret schemes and
  `ORK_ORKER_SECRET_ALLOW` floor — now run after commit adoption and the memo
  step. A retry whose result another Orker of the pool already committed, or a
  cached task whose memo hits, no longer fails its run with
  `RESOURCE_REQUEST_REFUSED`, `SECRET_SCHEME_FORBIDDEN` or
  `SECRET_NOT_PERMITTED` when it lands on an Orker with narrower settings. A
  task with neither is still refused, before any secret is resolved or any
  image pulled. `RUN_AS_ORKER_UNAVAILABLE`, already raised after the commit
  check and the memo step, is now raised with them, before secrets are staged
  and the image is pulled.

## [0.1.0] - 2026-10-30

First release.
