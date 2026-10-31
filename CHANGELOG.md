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

- **BREAKING** Every first-party activity image ships jq 1.8 (was 1.7) and BusyBox 1.37 (was 1.36), the `git` image git 2.54 (was 2.45), and the `http` image curl 8.23 (was 8.14). jq 1.8 changes what some filters compute, both a `json` task's filter and any `jq` a `shell` task's command runs: `indices`, `index` and `rindex` count code points rather than bytes; `tonumber` refuses leading or trailing white space; `ltrimstr` and `rtrimstr` fail on a non-string input or argument; `last(empty)` yields nothing; `limit` fails on a negative count; `pow10` is gone (use `exp10`); an `as` binding takes the whole expression before it, operators included (`[-1 as $x | 1, $x]` gives `[1,-1]`, was `[-1,-1]`); and a `#` comment that ends in a backslash continues on the next line. Check those filters and commands against jq 1.8 before you upgrade. A `template` task's own `jq` (gomplate's) is unchanged.
- **BREAKING** The Orker's boot probe runs `docker.io/library/alpine:3.24` by digest (was alpine 3.20). An Orker that cannot pull it refuses to start with `BIND_PROBE_IMAGE_UNAVAILABLE`: on an air-gapped or rate-limited host, mirror or load it before you upgrade (UPGRADING.md, "Upgrading the deployment").
- The development stack and the quickstart run Temporal's development server 1.9.1 (Temporal Server 1.32.0) and SeaweedFS 4.51.

### Security

- The first-party activity images move to Alpine 3.24; Alpine 3.20, their base until now, is past its support window.

## [0.1.0] - 2026-10-30

First release.
