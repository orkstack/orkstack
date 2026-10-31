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

### Changed

- **BREAKING:** An Orker's secret floor never admits the Orker's own sources,
  whatever `ORK_ORKER_SECRET_ALLOW` says, empty or not: its OrkStack and
  telemetry settings (`env:ORK_*`, `env:OTEL_*`), its proxy settings, the
  container CLI's settings (`env:DOCKER_*`, `env:CONTAINER_HOST` and the like)
  and its registry credential helpers' (`env:AWS_*`, `env:AZURE_*`,
  `env:CLOUDSDK_*`, `env:GOOGLE_APPLICATION_CREDENTIALS`), `/proc`, `/dev`,
  its container's credentials under `/run/secrets` and `/var/run/secrets`, its
  work root, its configuration file and each file that reads, its telemetry
  sinks' header sources, the container engine's state, and the credential
  files its settings name. A `file:` source is the Orker's own when the way
  the kernel takes to open it passes through or ends at one of these, by name
  or as the file is when the source is judged, so a link, a hard link to an
  own file and a Kubernetes mount updated after boot reach none of them. The
  work root is the temp directory when
  `ORK_ORKER_WORKDIR` is unset, so there a `file:/tmp/…` source is refused and
  a floor entry under `/tmp` stops the Orker at boot. A task that names one
  fails `SECRET_NOT_PERMITTED` where the floor is checked, after the commit
  check and a cached task's memo step and before its image is pulled, under
  `orkctl task run` as on an Orker; give the task the value under a source of
  its own. A floor entry that can match only such sources stops the Orker at
  boot, and an Orker whose floor is empty logs a
  warning at boot that names `ORK_ORKER_SECRET_ALLOW`. The refusal says only
  that the source is one of the Orker's variables or one of its files, never
  which ([SECURITY_MODEL.md](docs/SECURITY_MODEL.md), "The Orker's own
  sources").
- A secret refusal, the namespace's, the Orker's and `orkctl task run`'s
  alike, quotes a source, a scheme, a variable or a secret name longer than
  4101 bytes by its first 64 bytes and its length.
- **BREAKING:** A `file:` secret source must be a plain absolute path, with
  no `..`, `.` or empty segment, of at most 4096 bytes, under every secret
  floor, the empty one included. Any other name fails `SECRET_NOT_PERMITTED`,
  as a non-empty floor already refused it, except a name longer than 4096
  bytes, which no floor checked before and the kernel never opens.
- `scripts/k8s-orker-check.sh` runs a task's secrets through the Kubernetes
  bases' floor on a live cluster: a key of `orkstack-task-secrets` is read, and
  `ORK_S3_SECRET_KEY` and a variable the floor does not list are refused. It
  makes the `orkstack-task-secrets` Secret in its own namespace, and leaves a
  fifth Orkflow, `k8s-orker-secrets`.

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
- `orkctl task run --secret NAME=file:PATH` says why a file cannot be used, as
  the file system gave it, rather than calling every such file missing.

## [0.1.0] - 2026-10-30

First release.
