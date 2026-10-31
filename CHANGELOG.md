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

- `make reset` no longer fails on a workspace an Orker left read-only under
  `.data/work`, and takes the observability profile's containers and volumes
  down with the rest instead of reporting them as survivors.
- The Orker's boot probe no longer leaves `ork-bindprobe-*` or
  `ork-sigprobe-*` containers behind when its boot is stopped or refused
  between a container's create and its start. Both probe containers carry
  the Orker's `ork.orker` label, the Orker removes each itself however the
  probe ends, and one it could not remove (it was killed first) is reaped by
  the next boot under the same identity, or listed by that label.

### Security

- The development stack (`deploy/docker-compose.yml`, `make infra-up`)
  publishes every port on `127.0.0.1` only. Temporal's frontend, the object
  store with its development key, the unauthenticated registry and, in the
  observability profile, Grafana with anonymous admin were reachable from
  any host that could reach the contributor's machine. Reach it from another
  machine through an SSH tunnel. The next `make infra-up` on a running stack
  recreates its containers to apply this: the object store and the registry
  keep their volumes, and Temporal's development server, which keeps its
  history in memory, starts empty.

## [0.1.0] - 2026-10-30

First release.
