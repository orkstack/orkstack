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

- The Kubernetes Orker bases (`deploy/kubernetes/orker-userns/`,
  `deploy/kubernetes/orker-privileged/`) declare the Orker's capacity, 4 cpu
  and 8Gi, which their engine container holds with room for its daemon. An
  overlay that raises `ORK_ORKER_MAX_CONCURRENT` can no longer reserve more
  than the pod's limits; change the capacity and the engine's resources
  together ([DEPLOY.md](docs/DEPLOY.md), "Orkers on Kubernetes").

### Fixed

- `scripts/k8s-orker-check.sh` expects a task above the per-task cap to be
  refused at submit with `RESOURCES_UNSATISFIABLE`, as resource placement
  does, instead of failing at dispatch, and checks that the Orker reports the
  capacity its pod declares. A command that fails where no check expected it
  now fails the check with its line and command, and the namespace's state,
  instead of ending it with no reason.

## [0.1.0] - 2026-10-29

First release.
