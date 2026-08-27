---
title: "POSIX subshell signals, container lifetime, image-import verification, registry auth, protobuf UTF-8, and Temporal error details"
date: 2026-08-26
updated: 2026-08-26
sources_checked: 2026-08-26
scope: "POSIX.1-2024; current Moby/containerd/protobuf-go/Temporal Go SDK source"
---

## Summary

1. POSIX requires `$(...)` to run in a subshell environment and says every command in a multi-command pipeline is in a subshell environment, although it explicitly permits an extension that runs any/all pipeline commands in the current environment. Subshell trap changes cannot affect the parent. Redirection itself does not create a subshell.
2. The Docker/container-lifetime premise is right only with qualifications: loss or hard death of the client is not a container-lifetime event, but an ordinary signal received by an attached `docker run` is proxied by default and may stop the container. The claim that modern `--rm` is client-side and skipped when the client dies is **false**: Moby stores `AutoRemove` in daemon state and the daemon removes the container on exit.
3. Current Moby's containerd image store calls containerd's archive importer. It hashes every tar entry while ingesting, but the OCI importer does **not** compare `blobs/<alg>/<encoded>`'s path to those bytes; it records the computed digest. Descriptor-following later enforces expected digest lookup. Neither OCI `index.json` nor Docker `manifest.json` has an authenticated root supplied by the archive itself. If authenticity against a trusted root digest matters, the caller must hash/verify the root bytes (or use an API that accepts/verifies a trusted root descriptor) before import.
4. Registry manifest `GET` and `HEAD` are both read access to the same repository resource, hence both require `repository:<name>:pull`; there is no separate “HEAD credential.”
5. Proto3 `string` is UTF-8 text; `bytes` is arbitrary octets. Current Go protobuf returns an error on invalid UTF-8 from both binary `proto.Marshal` and `protojson.Marshal`.
6. Temporal `ApplicationError` details go through the configured data converter; protobuf messages are supported details. A workflow uses `errors.As` then `Details(&out)`. An activity cancellation outcome has no success-result slot: return cancellation data as `temporal.NewCanceledError(details...)` details.

## Detailed findings

### 1. POSIX command substitution, pipelines, traps, and redirection

**Command substitution.** POSIX.1-2024 [§2.6.3 Command Substitution](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_06_03) says exactly:

> “The shell shall expand the command substitution by executing *commands* in a subshell environment … and replacing the command substitution … with the standard output of the command(s)”.

Thus `$(command)` is unambiguous.

**Pipeline syntax and environment.** [§2.9.2 Pipelines](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_09_02) defines a pipeline as one or more commands separated by `|`/`|&` and specifies that, for a foreground pipeline, “the shell shall wait for the last command specified in the pipeline to complete, and may also wait for all commands to complete.” The environment rule is in [§2.13 Shell Execution Environment](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_13):

> “Changes made to the subshell environment shall not affect the shell environment. Command substitution, commands that are grouped with parentheses, and asynchronous AND-OR lists shall be executed in a subshell environment. Additionally, each command of a multi-command pipeline is in a subshell environment; as an extension, however, any or all commands in a pipeline may be executed in the current environment. Except where otherwise stated, all other commands shall be executed in the current shell environment.”

The extension sentence matters: portable code cannot rely on a pipeline component changing the parent, but a conforming shell extension may run (commonly) the last component in the current shell.

**Traps and the parent.** The same [§2.13](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_13) says:

> “A subshell environment shall be created as a duplicate of the shell environment, except that … traps that are not being ignored shall be set to the default action.”

and, decisively, “Changes made to the subshell environment shall not affect the shell environment.” [The `trap` special built-in](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_29) adds:

> “When a subshell is entered, traps that are not being ignored shall be set to the default actions, except in the case of a command substitution containing only a single trap command, when the traps need not be altered.”

It immediately clarifies: “This does not imply that the trap command cannot be used within the subshell to set new traps.” Those new settings belong to the subshell environment; by §2.13 they do not alter the parent shell's trap setting/signal disposition. The parent retains its own setting.

**Plain redirection.** [§2.7 Redirection](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_07) says:

> “Redirection is used to open and close files for the current shell execution environment … or for any command.”

Together with §2.13's “Except where otherwise stated, all other commands shall be executed in the current shell environment,” this confirms that `cmd >file` introduces no subshell merely because of `>file`. An external `cmd` still runs in its ordinary separate **utility execution environment**; that is command execution, not a redirection-created shell subshell. Special case: `exec >file` intentionally changes the current shell's descriptors.

### 2. Docker/containerd lifetime and reaping

**What owns lifetime.** Docker Engine is a daemon API. Docker's [`run` reference](https://docs.docker.com/reference/cli/docker/container/run/) distinguishes attached and detached operation and documents `--sig-proxy` (default `true`) as “Proxy received signals to the process.” Therefore:

- Closing a connection, losing SSH, or `SIGKILL`ing the CLI cannot by itself be relied on to stop the container; the daemon/task remains.
- But “killing `docker run` does not stop the container” is too broad. If the attached CLI receives a catchable signal, it proxies that signal by default, and the container process may exit. Use detached mode or `--sig-proxy=false` if that behavior is unwanted; neither creates a disconnect-lifetime guarantee.

containerd describes itself as a daemon that “can manage the complete container lifecycle” in its [official README](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/README.md#L34-L39). Runtime v2 separates control from process ownership: the [shim API](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/api/runtime/task/v2/shim.proto#L29-L32) says:

```proto
// Shim service is launched for each container and is responsible for owning the IO
// ... The shim is also the parent of each container and allows reattaching ...
// and receiving the exit status for the container processes.
```

The [runtime-v2 architecture](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/docs/runtime-v2.md#L38-L47) explains that containerd invokes a runtime/shim socket service, not a client-owned child tied to the launching CLI.

**`--rm` correction.** Docker documents `--rm` as “Automatically remove the container and its associated anonymous volumes when it exits,” and specifically says detached `--rm` containers are removed “when it exits or when the daemon exits, whichever happens first” ([`docker container run`](https://docs.docker.com/reference/cli/docker/container/run/#clean-up---rm)). Current Moby implements it daemon-side. On exit the monitor reads persisted `HostConfig.AutoRemove` and calls daemon removal ([Moby source](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/daemon/monitor.go#L340-L354)):

```go
ar := c.HostConfig.AutoRemove
if !ar { return }
err := daemon.containerRm(cfg, c.ID,
    &backend.ContainerRmConfig{ForceRemove: true, RemoveVolume: true})
```

Daemon restoration also queues auto-remove containers for cleanup ([Moby source](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/daemon/daemon.go#L557-L567)). Consequently, modern `--rm` is **not** cleanup performed only by a waiting CLI and is not skipped merely because that CLI dies. It does not stop a still-running container; it removes it after exit (or per documented daemon-exit behavior).

**Available mechanisms and what they do.** 

- [`docker ps --filter label=...`](https://docs.docker.com/reference/cli/docker/container/ls/#filtering---filter) is supported inventory/polling. Labels are documented as filterable metadata ([labels docs](https://docs.docker.com/engine/manage-resources/labels/#manage-labels-on-objects)); an external reaper can select, inspect age/state, then `docker stop`/`docker rm`.
- [`docker events`](https://docs.docker.com/reference/cli/docker/system/events/) is a real-time **server event stream**, filterable by object/event/label. It is a trigger source for an external controller, not itself a reaper; the docs warn only the last 256 past events are returned, so reconcile with `docker ps` after gaps/restarts.
- [Restart policies](https://docs.docker.com/engine/containers/start-containers-automatically/) control whether the daemon restarts a container **after it exits**. They do not expire/reap it and conflict with `--rm` in Moby ([validation source](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/daemon/container.go#L256-L259)).
- [`--stop-timeout`](https://docs.docker.com/reference/cli/docker/container/run/) is only the timeout used **after a stop has been initiated** before force-killing; it is not a maximum run time or stop trigger. [`docker stop`](https://docs.docker.com/reference/cli/docker/container/stop/) documents the TERM-then-KILL grace sequence.

There is no documented Docker Engine `run`/create option that means “kill after N seconds” or “kill when this API client disconnects.” Thus there is no such daemon-side guarantee. Implement an external durable lease/TTL supervisor (labels + periodic reconciliation, optionally events for low latency) that explicitly calls stop/kill/removal. A client-side `timeout docker run ...` is not equivalent, because signal proxying and hard client failure have different outcomes.

### 3. `docker load`, OCI layout, and digest verification

**Current Moby path.** With the containerd image store, Moby decompresses the archive and calls `i.client.Import(...)` ([Moby `LoadImage`](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/daemon/containerd/image_exporter.go#L240-L248), [call site](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/daemon/containerd/image_exporter.go#L285-L291)). containerd then calls `archive.ImportIndex` ([client source](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/client/import.go#L148-L169)).

**What import actually verifies.** `ImportIndex` supports OCI v1 and Docker v1.1/v1.2 ([source](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/core/images/archive/importer.go#L58-L65)). For every ordinary tar entry it calls `onUntarBlob`, then records the **computed** digest ([source](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/core/images/archive/importer.go#L106-L125)):

```go
dgst, err := onUntarBlob(ctx, tr, store, hdr.Size, "tar-"+hdrName)
...
blobs[hdrName] = ocispec.Descriptor{Digest: dgst, Size: hdr.Size}
```

`onUntarBlob` tees bytes through a canonical digester but passes an empty expected digest to `WriteBlob` ([source](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/core/images/archive/importer.go#L248-L253)). Thus it establishes content addressing in the store, but does **not** compare an OCI blob filename's `<alg>/<encoded>` with the actual digest. For OCI, it simply returns the descriptor it recorded for the `index.json` path ([source](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/core/images/archive/importer.go#L129-L143)).

When an expected descriptor is supplied, containerd's generic content writer does enforce it at commit: `WriteBlob` passes `desc.Digest` to `Copy`, and `Copy` calls `Commit(..., expected)` ([source](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/core/content/helpers.go#L126-L145), [commit](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/core/content/helpers.go#L184-L228)). During archive ingestion here, however, the path-derived expected digest is never supplied. A mismatched referenced blob normally causes later descriptor traversal/content lookup to fail because the expected digest is absent, but that is not filename validation during untar.

For a Docker archive, there are no OCI content-addressed blob path semantics to verify. The importer hashes entries, then synthesizes OCI descriptors/manifests from the actual config/layer content. `manifest.json` is archive metadata, not an authenticated root.

**OCI spec responsibility.** OCI Image Layout requires: “The content of `blobs/<alg>/<encoded>` MUST match the digest `<alg>:<encoded>`” ([OCI Image Layout](https://github.com/opencontainers/image-spec/blob/af26a05fba5ee648512f4ea3c9fda1fcc1b6d6dc/image-layout.md#L58-L64)). For descriptors it says: “Retrieved content SHOULD be verified against this digest when consumed via untrusted sources” and “Before consuming content targeted by a descriptor from untrusted sources, the byte content SHOULD be verified against the digest string” ([OCI descriptor spec](https://github.com/opencontainers/image-spec/blob/af26a05fba5ee648512f4ea3c9fda1fcc1b6d6dc/descriptor.md#L27-L33), [verification section](https://github.com/opencontainers/image-spec/blob/af26a05fba5ee648512f4ea3c9fda1fcc1b6d6dc/descriptor.md#L105-L112)). These are requirements/recommendations on a conforming layout and its consumer; they do not make `index.json` self-authenticating.

**Conclusion.** If the caller has a trusted expected digest for the root index/manifest and security depends on it, **yes: verify/hash the exact root bytes before import** (and reject mismatch), or use a lower-level ingestion API with that digest as the expected descriptor. `docker load` alone is not a trust/authenticity check. Hashing without comparing to a trusted value only detects accidental changes if the expected hash is independently protected; it does not establish provenance.

### 4. Private registry manifest `GET` versus `HEAD`

The Distribution [HTTP API V2 “Pulling an Image Manifest” section](https://distribution.github.io/distribution/spec/api/#pulling-an-image-manifest) specifies `GET /v2/<name>/manifests/<reference>` and, in the same pull operation, `HEAD /v2/<name>/manifests/<reference>` for existence/digest. The [token-scope specification](https://distribution.github.io/distribution/spec/auth/scope/#example-resource-types) says a `repository` represents the collection of manifests/blobs and its actions are “`pull` for accessing the collection and `push` for adding to it”; it further defines `pull` as read access ([Resource Actions](https://distribution.github.io/distribution/spec/auth/scope/#resource-actions)). The bearer challenge format is `scope="repository:samalba/my-app:pull,push"` in the [token-auth specification](https://distribution.github.io/distribution/spec/auth/token/#how-to-authenticate).

Therefore both manifest GET and HEAD need repository read access: `repository:<name>:pull`. HTTP HEAD suppresses the response body; it is not a distinct authorization action. A token usable for a private manifest HEAD is not intrinsically narrower than a token usable for GET of that same repository. A particular registry may impose additional nonstandard policy, but that is outside the Distribution scope model.

### 5. Protobuf strings and invalid UTF-8 in Go

The official proto3 [scalar-type table](https://protobuf.dev/programming-guides/proto3/#scalar) states exactly:

> `string`: “A string must always contain UTF-8 encoded or 7-bit ASCII text…”
>
> `bytes`: “May contain any arbitrary sequence of bytes…”

Thus use `bytes`, not `string`, for arbitrary octets.

Current `google.golang.org/protobuf` binary marshaling checks UTF-8 for string fields and returns `errors.InvalidUTF8` ([`proto/encode_gen.go`](https://github.com/protocolbuffers/protobuf-go/blob/f4a54025970a405fb35cada3b865101d8fa8ab9d/proto/encode_gen.go#L68-L75)):

```go
case protoreflect.StringKind:
    if strs.EnforceUTF8(fd) && !utf8.ValidString(v.String()) {
        return b, errors.InvalidUTF8(string(fd.FullName()))
    }
```

The generated fast path likewise returns `errInvalidUTF8`, whose error text is “string field contains invalid UTF-8” ([source](https://github.com/protocolbuffers/protobuf-go/blob/f4a54025970a405fb35cada3b865101d8fa8ab9d/internal/impl/codec_field.go#L16-L22)). ProtoJSON explicitly turns a failed JSON `WriteString` into `errors.InvalidUTF8` ([`protojson` source](https://github.com/protocolbuffers/protobuf-go/blob/f4a54025970a405fb35cada3b865101d8fa8ab9d/encoding/protojson/encode.go#L296-L303)); its JSON encoder documents “Returns error if input string contains invalid UTF-8” ([source](https://github.com/protocolbuffers/protobuf-go/blob/f4a54025970a405fb35cada3b865101d8fa8ab9d/internal/encoding/json/encode.go#L78-L90)). So both `proto.Marshal` and `protojson.Marshal` error for an invalid proto3 `string` in current Go protobuf. (Editions/proto2 can expose explicit UTF-8-validation feature differences; this conclusion is for proto3 as asked.)

### 6. Temporal `ApplicationError` and cancellation payloads

The public Go SDK API is exactly `temporal.NewApplicationError(message, errType string, details ...any)` and constructs an `ApplicationError` with those details ([SDK source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/temporal/error.go#L165-L171)). The SDK documentation/source says details are encoded payloads and the workflow must know their types before extracting them; its example uses `errors.As` and `.Details(&detailMsg)` ([error documentation](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/error.go#L18-L27), [example](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/error.go#L41-L53)). `Details` delegates decoding to the encoded values ([source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/error.go#L735-L745)), and the failure converter serializes details with its configured data converter ([source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/failure_converter.go#L87-L105)).

A protobuf message is a supported detail: the default converter tries `ProtoJSONPayloadConverter` before binary proto and JSON ([default converter](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/converter/default_data_converter.go#L3-L16)); that converter recognizes `proto.Message` and calls `protojson.Marshal` ([source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/converter/proto_json_payload_converter.go#L92-L115)). “Arbitrary” therefore means any values the configured converter can serialize, not literally every Go value.

Workflow pattern:

```go
err := workflow.ExecuteActivity(ctx, A).Get(ctx, nil)
var appErr *temporal.ApplicationError
if errors.As(err, &appErr) {
    var out mypb.Detail
    if err := appErr.Details(&out); err != nil { /* handle decode error */ }
}
```

**Canceled path.** The wire response is a sum of completed, canceled, or failed outcomes. SDK code sends `result` only when `err == nil`; for `CanceledError` it creates `RespondActivityTaskCanceledRequest` containing only converted cancellation `Details` ([source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/internal_task_pollers.go#L1618-L1653)). Therefore an activity cannot return a normal result payload *and* have that execution recorded as canceled; a Go `(result, error)` result is discarded on the canceled/error branch. Put cancellation data in `temporal.NewCanceledError(details...)`, whose API accepts details ([source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/error.go#L494-L500)); the failure converter maps them to `CanceledFailureInfo.Details` ([source](https://github.com/temporalio/sdk-go/blob/9e67f43b78c6f0174e06009a2eded936829de59c/internal/failure_converter.go#L101-L105)).

## Conflicts, gaps, and limitations

- The requested Docker premise that `--rm` is client-side and skipped on client death conflicts with current Docker docs and current Moby source; it describes historical behavior, not modern Engine behavior.
- “Killing `docker run`” is signal/mode dependent because attached clients proxy catchable signals by default. Only client loss/hard death is clearly independent of task lifetime.
- OCI import behavior above is pinned to current containerd commit `a9eb76f…` and current Moby commit `40baece…`; Docker installations using the classic (non-containerd) image store take a different legacy loader path. Neither loader should be treated as an authenticity verifier without a trusted root.
- POSIX permits a pipeline extension that executes components in the current environment. The portable semantic remains that code must not depend on pipeline-side state reaching the parent.
