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

- Release workflows pin every action by commit SHA.

## [0.1.0] - 2026-10-30

First release.
