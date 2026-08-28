# Retired Go plugin system

> **Retired in v0.0.5.** OrkStack no longer has `pkg/plugin`,
> `pkg/plugins/*`, a Go `Plugin` interface, a plugin registry, or the `plugin:`
> DSL key. This file keeps its historical path so existing links explain the
> removal instead of returning 404.

Tasks now name an OCI activity image with `uses:`. The value is either a catalog
activity name such as `http`, `shell`, or `file`, or a full image reference.
The Orker resolves the image to a digest and runs it through the filesystem ABI;
the workload links no OrkStack SDK and receives no control-plane credentials.

Current references:

- [OCI Activity Contract](OCI_ACTIVITY_CONTRACT.md) — authoritative execution,
  workspace, completion, retry, security, and image-resolution contract;
- [DSL reference](DSL.md) — current Orkflow task shape;
- [activity-image `op` convention](PLUGIN_DESIGN.md) — an optional convention
  used by the first-party reference images;
- [`activity-http`](../images/activity-http/README.md),
  [`activity-shell`](../images/activity-shell/README.md), and
  [`activity-file`](../images/activity-file/README.md) — current first-party
  activity documentation.

There is no extension interface to implement inside the Orker. To add an
activity, build an OCI image that reads `/orkstack/input`, writes
`/orkstack/output`, logs to stdout/stderr, and exits normally.
