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

- Every Go dependency is on its latest release: the Temporal Go SDK 1.50.0
  (API 1.64.0), Connect 1.21.1, gRPC 1.85.0, OpenTelemetry Go 1.47.0 (its logs
  API and SDK are now stable, 1.47.0) with otelhttp 0.72.0, the AWS SDK for Go
  v2 1.47.5 (S3 1.114.6) and the Prometheus client 1.25.0. The Temporal SDK's
  `temporal_activity_execution_failed` and
  `temporal_local_activity_execution_failed` metrics now carry a
  `failure_reason` attribute, one series per reason, and
  `temporal_sticky_cache_total_forced_eviction` no longer counts a worker's
  shutdown, so a dashboard built on them may need adjusting.
- Code generation is pinned exactly: buf 1.75.0, protobuf-es and
  protoc-gen-es 2.16.0, and connect-es 2.3.0 in `ui/package.json`. The Go
  protoc plugins are `go.mod` tool directives, so each generates with the
  version of the runtime it links against, and `make tools` installs only buf,
  at the exact version `ui/package.json` pins.

## [0.1.0] - 2026-10-30

First release.
