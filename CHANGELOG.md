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

- A task waiting on a pool whose every Orker is at its declared capacity now
  says so: the run page captions it `Waiting for capacity`, and
  `orkctl run inspect` prints `waiting for capacity … every orker of pool
  <pool> is full (reserved 4/4 cpu)`, both read from the Orker inventory
  (`InspectRun`'s new `LiveAttempt.capacity_wait`). Any other waiting task
  still reads `Waiting for an orker`.
- Capacity metrics on an Orker that declares a capacity, when metrics are on:
  `ork.orker.capacity.cpu` and `.memory`, `ork.orker.capacity.reserved.cpu`
  and `.memory`, `ork.orker.admission.wait.duration` and
  `ork.orker.admission.refusals` (docs/DEPLOY.md, "Declaring an Orker's
  capacity").

## [0.1.0] - 2026-10-30

First release.
