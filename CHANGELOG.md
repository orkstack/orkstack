# Changelog

All notable changes to OrkStack are recorded here. The format follows
[Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and versions
will follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html) from the
first release. OrkStack has not reached 1.0: until it does, a release may
include breaking changes, and each is marked **BREAKING**.

## [Unreleased]

### Added

- JSON Schemas for the authored documents, generated from the parsers:
  `docs/schemas/orkflow.v1.json`, `docs/schemas/automation.v1.json`, and
  `docs/schemas/orkstack.v1.json`, which selects one by `apiVersion` and
  `kind`. First-party `with:` blocks complete from each image's interface
  label (docs/DSL.md, "Editor support").
- The `http` activity's `body:` accepts a YAML mapping or list and sends it as
  compact JSON, with `Content-Type: application/json` unless `headers` sets a
  content type. A string body is still sent as written.
- An activity interface label's `type` may name several types joined by `|`
  (`string|object|array`).

### Changed

- The UI is redrawn as a drawing office (stage 1 of 4: the shell). A slim
  top bar replaces the sidebar, with two modes — **Build** (Runs, Orkflows,
  Apply) and **Operate** (the Admin pages) — and a row along the bottom edge
  states what each page is. **Ctrl+K** (**⌘K** on macOS) opens a command
  palette that jumps to any page, run or Orkflow and switches the namespace
  or the appearance; it never writes (docs/UI_CLI_PARITY.md, "Navigation and
  the command palette"). Light and dark are designed together and follow the
  system unless one is chosen; a browser that refuses storage now follows
  the system too instead of painting light. Statuses are stamps that differ
  by drawn mark as well as colour, every focus ring is one solid 2 px line,
  and the faces are the self-hosted Archivo Narrow and Spline Sans Mono
  (55 KB preloaded, down from 89 KB). Every colour, size, radius, shadow and
  motion is a token in `ui/src/tokens.css`; a test refuses a raw value
  anywhere else.
- The run page, the Runs page and compare are redrawn (stage 2 of 4). A run
  opens on its drawing: the DAG on a grid, each node drawn by its status and a
  link that selects its task, or — past 80 tasks — a task map with one cell
  per task and per fan-out item, readable and keyboard-operable at 4,000
  tasks (arrows, Home/End, PageUp/Down, F for the next failure, Enter to
  select). The DAG is drawn up to 80 tasks, on request up to 300, and never
  above, where a selected task's neighbourhood is drawn instead; the Orkflow
  page's DAG follows the same bound. Beside it, the versions of the run's
  Orkflow (this run's, the current one and the newest), and a failed run's
  reason in a dashed red outline with the commands that reproduce it. A run
  whose definition or versions cannot be read still shows everything else,
  with the reason. The Runs page updates the statuses it shows every 15 s
  without reordering, and marks a run whose status changed since this
  browser last saw it until it is opened (docs/UI_CLI_PARITY.md,
  "Changed-run marks"); the command palette lists the changed runs, selects
  a task by id on a run page, and opens the run's actions behind their
  confirmations. A closed popover no longer listens to the page, so a run
  page of 4,000 tasks becomes interactive about 0.4 s sooner.
- **BREAKING:** every Orkflow begins with `apiVersion: orkstack/v1` and
  `kind: Orkflow`. A document without them is refused at validate, apply and
  plan, naming the two lines to add. The header is not part of the definition:
  digests, derived versions and memo keys are unchanged, a version stored
  before this release still reads, runs and diffs, and applying it again with
  the two lines added as its first YAML lines, above or below its comments, is
  a no-op (docs/DSL.md, "The document header").
- **BREAKING:** `orkctl apply` decides what a file is by its `kind:`
  (`Orkflow` or `Automation`) and refuses a file with neither, before anything
  is sent.
- **BREAKING:** an automation document's schedule `input:` is a YAML mapping.
  A JSON object written as a string is refused, naming the YAML form: remove
  the quotes. `orkctl export` writes `input:` as a mapping.
- **BREAKING:** data written in an Orkflow — `inputs:`, `with:`,
  `await.schema` — becomes JSON through one strict mapping: a plain value YAML
  readers resolve differently (`yes`, `on`, `0755`, `1:30`, `1e3`, a date), a
  number JSON cannot hold exactly, and tags, anchors, aliases and merge keys
  are refused, naming the path (docs/JSON.md, "Data written in an authored
  document"). A document that compiled before either compiles to the same
  definition or is refused; none changes silently.
- A stored Orkflow version is shown with its header wherever OrkStack shows its
  YAML (`orkctl orkflow get --json`, `edit`, `diff`, the UI). Evidence bundles
  keep the source verbatim.
- The `custom-image` (0.2.0) and `each-fan-out` (0.3.0) examples write their
  fixed lists as jq `vars:` in YAML instead of jq literals.

### Fixed

- `examples/http-post.ork.yaml` built its JSON body by splicing the message
  into a string, so a quote or a backslash in it posted malformed JSON. It now
  writes the body as a YAML mapping (version 0.3.0).
- The `http` activity no longer appends a newline to a string body.
