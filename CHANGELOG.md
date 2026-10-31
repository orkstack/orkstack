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

- **BREAKING** Every first-party activity image ships jq 1.8 (was 1.7) and BusyBox 1.37 (was 1.36), the `git` image git 2.54 (was 2.45), and the `http` image curl 8.23 (was 8.14). jq 1.8 changes what some filters compute, both a `json` task's filter and any `jq` a `shell` task's command runs: `indices`, `index` and `rindex` count code points rather than bytes; `tonumber` refuses leading or trailing white space; `ltrimstr` and `rtrimstr` fail on a non-string input or argument; `last(empty)` yields nothing; `limit` fails on a negative count; `pow10` is gone (use `exp10`); an `as` binding takes the whole expression before it, operators included (`[-1 as $x | 1, $x]` gives `[1,-1]`, was `[-1,-1]`); and a `#` comment that ends in a backslash continues on the next line. Check those filters and commands against jq 1.8 before you upgrade. A `template` task's `jq` is gomplate's own (gojq 0.12.19, was 0.12.17), where such a binding, a parse error before, now binds the whole expression too.
- **BREAKING** The `template` activity image renders with gomplate 5.3.1 (was 4.3.3). gomplate 5 removed most of the names gomplate 4 still served with a deprecation warning, so a template that calls one fails to render: use `strings.Contains`, `strings.HasPrefix`, `strings.HasSuffix`, `strings.Split`, `strings.SplitN` and `strings.Trim` for the bare `contains`, `hasPrefix`, `hasSuffix`, `split`, `splitN` and `trim`, with the input last (`{{ contains .s "x" }}` becomes `{{ strings.Contains "x" .s }}`); `conv.ToBool` for `bool` and `conv.Bool`; `coll.Sort` for `strings.Sort`; and `net.ParseAddr`, `net.ParsePrefix` and `net.ParseRange` for `net.ParseIP`, `net.ParseIPPrefix` and `net.ParseIPRange`. Bare `slice` does not fail but now slices (Go's builtin): build a list with `coll.Slice`. A `GOMPLATE_CONFIG` file that lists `templates` as a sequence no longer parses (gomplate 5 reads a map), and the image's `--missing-key error` now beats the file's `missingKey`. The `aws` functions run on AWS SDK v2 and no longer read `AWS_META_ENDPOINT`: set `AWS_EC2_METADATA_SERVICE_ENDPOINT` instead. The key generators and the RSA and AES functions no longer need `GOMPLATE_EXPERIMENTAL`, and the image's README lists which of them make a cached render wrong.
- **BREAKING** The Orker's boot probe runs `docker.io/library/alpine:3.24` by digest (was alpine 3.20). An Orker that cannot pull it refuses to start with `BIND_PROBE_IMAGE_UNAVAILABLE`: on an air-gapped or rate-limited host, mirror or load it before you upgrade (UPGRADING.md, "Upgrading the deployment").
- The development stack and the quickstart run Temporal's development server 1.9.1 (Temporal Server 1.32.0) and SeaweedFS 4.51.
- The development stack's observability profile runs OpenTelemetry Collector 0.164.0, Loki 3.7.9, Tempo 3.1.1, Prometheus 3.15.1 and Grafana 13.3.0.

### Security

- The first-party activity images move to Alpine 3.24; Alpine 3.20, their base until now, is past its support window.

## [0.1.0] - 2026-10-30

First release.
