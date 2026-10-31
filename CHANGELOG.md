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

- An unknown key in an Orkflow or an activity interface is refused in the
  document's own words: the key, where it stands, the document's kind and the
  keys that position takes, at the key's field path
  (`line 10: unknown key "need" in tasks[1] of the Orkflow; allowed: …`),
  followed by every other unknown key and its line. It used to repeat the
  YAML decoder's sentence, which named an internal type and only one key.
- `orkctl activity test` reports the `with:` interface the image declares in
  its `org.orkstack.activity.interface` label (declared, none or malformed),
  in its table and as `interface` in `--json`. The label is reported, never
  asserted, so it never changes the verdict.

## [0.1.0] - 2026-10-30

First release.
