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

- The web UI is built with SvelteKit 3, Vite 8 and Svelte 5.57. An open UI
  tab now also asks the engine for the UI's build version
  (`/_app/version.json`) every hour and when the tab regains focus, as
  SvelteKit 3 does by default.

## [0.1.0] - 2026-10-30

First release.
