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

- The nightly workflow also audits the UI's and the site's JavaScript
  dependencies (`make vuln-js`, `bun audit`) and scans the images a release
  publishes (`make vuln-images`, a pinned Grype) for known vulnerabilities; a
  release is not tagged while either is red.

### Security

- Every workflow and composite action pins every action by commit SHA, and
  Dependabot now also bumps the composite actions.
- The nightly image scan waives GHSA-mgcc-cww5-fp2q (go-jose/v4 v4.1.4, high,
  fixed in v4.1.6) in the template activity's gomplate alone: gomplate's
  latest release, v5.3.1, still carries v4.1.4, and gomplate's `main` already
  requires v4.1.6. The waiver is reviewed by 2026-11-30 and is removed when
  gomplate releases with go-jose v4.1.6 or later.

## [0.1.0] - 2026-10-30

First release.
