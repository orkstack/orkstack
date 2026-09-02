# Activity images and the `op` convention

An activity is an OCI image named by a task's `uses:` — a catalog name such
as `http`, `shell`, or `file`, or a full registry reference. Every image
implements the public filesystem ABI of the
[OCI Activity Contract](OCI_ACTIVITY_CONTRACT.md); there is no extension
interface inside the Orker, no OrkStack SDK, and no plugin registry. To add
an activity, build an image that reads `/orkstack/input`, writes
`/orkstack/output`, logs to stdout/stderr, and exits normally.

This page documents one convention the first-party reference images share.
It is not part of the platform contract.

An activity image may expose one operation selector, spelled `op`, inside its
`with:` configuration. `op` names what that image does; the other keys are the
arguments for that operation. This is an image-level convention, not part of
the OCI Activity Contract: third-party images may choose any configuration
shape and need no OrkStack SDK.

```yaml
name: notify
inputs:
  url:
    type: string
    required: true
tasks:
  - id: publish
    uses: http
    with:
      op: post
      url: "{{ .inputs.url }}"
      headers:
        content-type: application/json
      body: '{"status":"ok"}'
```

The catalog name `http` resolves to the `activity-http` OCI image at apply time.
A task may instead put a registry reference directly in `uses:`. There is no
`container` activity: every activity already is a container, so a custom
workload names its own image directly.

## Rules for reference activity authors

1. Use `op` only when one image genuinely offers several related operations.
2. Use lowercase verbs and reject unknown values before side effects.
3. Document whether an op is the default; do not silently guess when no choice
   is clearly safest.
4. Keep arguments minimal and document output values and artifact files.
5. Use ordinary exit status: zero succeeds and non-zero fails. An image such as
   `activity-shell` may explicitly define an operation whose child exit status
   is returned as data, but that is the image's contract, not a platform magic
   exit code.

## First-party reference images

The current configuration and output contract for each image lives beside its
Dockerfile:

- [`activity-http`](../images/activity-http/README.md) — HTTP requests;
- [`activity-shell`](../images/activity-shell/README.md) — argv, scripts, and
  evaluations;
- [`activity-file`](../images/activity-file/README.md) — small Unix-like file
  operations.

All three read rendered configuration from
`/orkstack/input/config.json`, write compact values to
`/orkstack/output/values.json`, and write regular-file artifacts beneath
`/orkstack/output/artifacts/`. They use the same ABI as any third-party image.
See the [OCI Activity Contract](OCI_ACTIVITY_CONTRACT.md) for the authoritative
platform contract and [the DSL reference](DSL.md) for task syntax.
