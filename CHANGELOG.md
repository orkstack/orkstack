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

- A canceled run shows who canceled it: the name your gateway asserted in
  `Ork-Principal` on the cancel request (`ORK_PRINCIPAL` for `orkctl`),
  marked as asserted and never authenticated. The API returns it as the
  run's `canceled_by`, `orkctl run get` prints `Canceled by:` and
  `orkctl run list` shows `CANCELED by … (asserted)`. It stays after the run
  is retired. A cancel without the header, and runs canceled before this
  release, show no name.

## [0.1.0] - 2026-10-30

First release.
