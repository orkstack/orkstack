---
source_url:
  - https://docs.docker.com/engine/containers/run/#exit-status
  - https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_08_02
  - https://github.com/krallin/tini
  - https://github.com/moby/moby
  - https://specs.opencontainers.org/distribution-spec/
  - https://specs.opencontainers.org/image-spec/image-index/
  - https://docs.temporal.io/encyclopedia/detecting-activity-failures
fetched_at: 2026-08-26T00:00:00Z
fetch_method: fetch_content, web_search, git clone, curl
topic: Docker exit-code ambiguity; Tini signal forwarding; POSIX shell wait/exec; OCI registry HEAD/GET and media types; Temporal long-transfer timeouts
---

# Exit codes, init/signals, OCI manifest retrieval, and Temporal transfer timeouts

## 1. Docker/container exit codes 125, 126, and 127

### Conclusion

**No cross-layer guarantee makes 125/126/127 unambiguous.** Docker documents these as `docker run` command outcomes, but an actually started container process can itself exit 125, 126, or 127. The current Docker CLI passes an attached container's post-start status through unchanged, including those three values. Therefore:

* A bare **`docker run` process exit code** of 125 is ambiguous: it can be a Docker create/start/wait failure or the started container process's own 125.
* If independent evidence proves the container **successfully started**, a recorded **`.State.ExitCode == 125`** is the last container-process exit code; it is not Docker's pre-start CLI diagnostic. It still does not reveal whether application code, a shell, or an init wrapper chose that value.
* 126 and 127 are conventions, not reserved workload-inaccessible values. A started workload may deliberately return either.

### Exact Docker documentation

Docker's [`docker run` exit-status section](https://docs.docker.com/engine/containers/run/#exit-status) says:

> “The exit code from `docker run` gives information about why the container failed to run or why it exited.”

It then defines:

> “Exit code `125` indicates that the error is with Docker daemon itself.”
>
> “Exit code `126` indicates that the specified contained command can't be invoked.”
>
> “Exit code `127` indicates that the contained command can't be found.”

It also claims:

> “Any exit code other than `125`, `126`, and `127` represent the exit code of the provided container command.”

That last sentence describes the CLI convention, but it must not be read as reserving those byte values against the workload.

### Implementation proof and start boundary

The current Docker CLI first creates, then starts, then waits. A create failure and a start failure go through `toStatusError`; after a successful start, the wait result is returned directly as `cli.StatusError{StatusCode: status}`. See [`run.go` at Docker CLI commit `3f096b4…`](https://github.com/docker/cli/blob/3f096b4ffd8e30329221c583db6dfc9279673b96/cli/command/container/run.go#L151-L162) and [the start/wait path at lines 203–262](https://github.com/docker/cli/blob/3f096b4ffd8e30329221c583db6dfc9279673b96/cli/command/container/run.go#L203-L262):

```go
containerID, err := createContainer(...)
if err != nil { return toStatusError(err) }
...
if _, err := apiClient.ContainerStart(...); err != nil {
    return toStatusError(err)
}
...
if status != 0 {
    return cli.StatusError{StatusCode: status}
}
```

The classifier is explicitly pre-start/error-path logic: generic daemon errors become 125, start “permission denied” becomes 126, and start “not found” becomes 127 ([same commit, `toStatusError`](https://github.com/docker/cli/blob/3f096b4ffd8e30329221c583db6dfc9279673b96/cli/command/container/run.go#L322-L355)). But the wait helper also synthesizes 125 if waiting itself fails ([`utils.go` lines 23–45](https://github.com/docker/cli/blob/3f096b4ffd8e30329221c583db6dfc9279673b96/cli/command/container/utils.go#L23-L45)). Thus **125 is not only a “failed to start” result**, even at the CLI layer.

`docker wait` does no origin classification: it prints `result.StatusCode` verbatim ([`wait.go` lines 42–56](https://github.com/docker/cli/blob/3f096b4ffd8e30329221c583db6dfc9279673b96/cli/command/container/wait.go#L42-L56)). Docker's Engine API describes inspect state's `ExitCode` only as **“The last exit code of this container”** ([Moby Swagger at `40baece…`](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/api/swagger.yaml#L6120-L6127)); it has no origin/discriminator field.

### Why every byte value is available to a workload

POSIX Shell Command Language [§2.8.2](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_08_02) says that an ordinary exit status is `WEXITSTATUS`, and for C programs it equals the value passed to `_Exit()`, `_exit()`, or `exit()` (or returned from `main`) **modulo 256**. Hence the observable normal-exit range is 0–255, and `exit(125)`, `exit(126)`, and `exit(127)` are valid workload choices.

The same POSIX section defines the shell conventions exactly:

> “If the command is not found, the exit status shall be 127.”
>
> “Otherwise, if the command name is found, but it is not an executable utility, the exit status shall be 126.”

These are shell meanings, not globally reserved process statuses. POSIX `wait` additionally warns that a status greater than 128 can be indistinguishable from a process that explicitly exited with that value ([`wait`, Application Usage](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/wait.html)).

### containerd/runc corroboration and another collision

containerd's status has one undifferentiated `ExitStatus uint32` described as “ExitStatus returned by the process” ([containerd commit `c2171cb…`, `client/task.go`](https://github.com/containerd/containerd/blob/c2171cb91834a0461022df40ad93f877fbd816d3/client/task.go#L53-L69)). Its `Wait` copies the service's `r.ExitStatus` unchanged; on a wait error it uses **255** as `UnknownExitStatus` plus an error ([lines 342–364](https://github.com/containerd/containerd/blob/c2171cb91834a0461022df40ad93f877fbd816d3/client/task.go#L342-L364)). This is further evidence that numeric status alone is not an origin tag.

runc/libcontainer converts a signaled process to `128 + signal`, otherwise returns the kernel wait status directly ([runc commit `9674194…`](https://github.com/opencontainers/runc/blob/9674194cc2b944f44407f87f3c5e3f2848aea0b8/libcontainer/utils/utils.go#L12-L23)):

```go
if status.Signaled() {
    return 128 + int(status.Signal())
}
return status.ExitStatus()
```

The OCI Runtime Specification does not reserve 125/126/127 as application-inaccessible statuses.

## 2. Docker `--init` and Tini signal forwarding

### Conclusion

**Tini defaults to forwarding signals only to its immediate child.** Whole-process-group forwarding is opt-in with `-g` or `TINI_KILL_PROCESS_GROUP`. Docker's Linux `--init` integration invokes `docker-init -- <command>`—it does **not** pass `-g`. Therefore Docker `--init` alone does not guarantee SIGTERM reaches a grandchild behind a direct child such as `sh -c`; it reaches that shell, which must forward/`exec`, unless a different configured init binary behaves differently.

Tini's README says exactly ([commit `369448a…`](https://github.com/krallin/tini/blob/369448a167e8b3da4ca5bca0b3307500c3371828/README.md#L193-L215)):

> “By default, Tini only kills its immediate child process.”
>
> “You can configure Tini to kill the child process group, so that every process in the group gets the signal”

by `-g` or `TINI_KILL_PROCESS_GROUP`. The source implements exactly `kill(kill_process_group ? -child_pid : child_pid, signal)` ([`src/tini.c` lines 522–540](https://github.com/krallin/tini/blob/369448a167e8b3da4ca5bca0b3307500c3371828/src/tini.c#L522-L540)).

Docker identifies its built `docker-init` as Tini: Moby's Dockerfile says `TINI_VERSION` specifies “the version of tini (docker-init) to build” and renames `tini-static` to `docker-init` ([Moby commit `40baece…`](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/Dockerfile#L291-L317)). The OCI process arguments inserted for `--init` are:

```go
s.Process.Args = append([]string{inContainerInitPath, "--", c.Path}, c.Args...)
```

([`daemon/oci_linux.go` lines 726–744](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/daemon/oci_linux.go#L726-L744)). There is no `-g`.

Caveat: Docker supports a configurable `init-path`; the statement above is for the stock `docker-init`/Tini shipped by Moby, not every custom binary an operator could configure.

## 3. POSIX-sh forwarding while retaining post-child cleanup

### Recommended shape

```sh
child_command &
child=$!
trap 'kill -TERM "$child" 2>/dev/null || :' TERM

wait "$child"       # may be interrupted by trapped TERM
wait "$child"       # obtain the child's eventual status
status=$?

cleanup
exit "$status"
```

The second `wait` is not redundant. POSIX [§2.12 Signals and Error Handling](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_12) says:

> “When the shell is waiting, by means of the `wait` utility, for asynchronous commands to complete, the reception of a signal for which a trap has been set shall cause the `wait` utility to return immediately with an exit status >128, immediately after which the trap associated with that signal shall be taken.”

The first `wait` therefore returns because the shell itself received trapped TERM, not necessarily because the child has terminated. The trap forwards TERM; the second `wait` then waits for (or retrieves the retained status of) that child. POSIX [`wait`](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/wait.html) requires `wait <pid>` to wait until that known child terminates and return its status. The [Bash signal manual](https://www.gnu.org/software/bash/manual/html_node/Signals.html) independently states the same immediate `>128` behavior for a trap-interrupted `wait`.

Practical qualifications:

* Quote `"$child"`; tolerate `kill` racing a child that has already exited.
* Under `set -e`, protect the first nonzero `wait` (for example `wait "$child" || :`) or the shell can exit before the second wait and cleanup.
* If no TERM interrupts the first wait, it consumes the child's known PID/status; in strict POSIX Issue 8, a second wait of that same PID is then “unknown” and returns 127. Thus an unconditional literal double-wait loses the original success/failure status in the no-signal path. Production code should record the first result and only re-wait when the first wait was trap-interrupted, commonly by setting a flag in the trap. The double-wait idiom explains the signal path, but is not by itself a complete status-preserving script for both paths.

A robust form is:

```sh
terminated=
child_command & child=$!
trap 'terminated=1; kill -TERM "$child" 2>/dev/null || :' TERM
wait "$child"; status=$?
if [ -n "$terminated" ]; then
  wait "$child"; status=$?
fi
cleanup
exit "$status"
```

(`set -e` still requires guarding the waits.)

### Why `exec` cannot coexist with later shell cleanup

The Bash reference says of [`exec`](https://www.gnu.org/software/bash/manual/html_node/Bourne-Shell-Builtins.html#index-exec):

> “If command is supplied, it replaces the shell without creating a new process.”

POSIX [`exec`](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#exec) likewise executes the command “without creating a new process.” On success, the shell no longer exists, so no statement after `exec child` can run. Use `exec` when direct signal delivery/status is more important and shell cleanup is unnecessary; use background + trap + wait when cleanup must execute afterward.

## 4. OCI registry `HEAD` versus `GET`

### Conclusion

**HEAD cannot provide `manifests[]`; GET is required to inspect index platform entries.** A successful manifest HEAD carries representation metadata but no response content. More precisely, OCI Distribution normatively requires `Docker-Content-Digest` and `Content-Length` for successful HEAD. `Content-Type` is expected representation metadata under HTTP and is defined for successful manifest responses/content negotiation, but the OCI HEAD subsection does not separately list it as a MUST.

OCI Distribution's manifest GET section says a GET to `/v2/<name>/manifests/<tag-or-digest>` **“MUST provide the expected manifest”** and the response `Content-Type` indicates its type ([distribution-spec commit `fee2119…`, lines 156–177](https://github.com/opencontainers/distribution-spec/blob/fee21197eb94360ddfa6dda0b7edabcd12456809/spec.md#L156-L177)). Its HEAD section says HEAD verifies presence and requires:

> “the digest … in the header `Docker-Content-Digest`”
>
> “the size in bytes … in the header `Content-Length`”

([lines 199–214](https://github.com/opencontainers/distribution-spec/blob/fee21197eb94360ddfa6dda0b7edabcd12456809/spec.md#L199-L214)). HTTP Semantics [RFC 9110 §9.3.2](https://www.rfc-editor.org/rfc/rfc9110.html#name-head) is decisive about content: a server **“MUST NOT send content in the response”** to HEAD and should send the same header fields as GET. Thus there is no JSON body and no `manifests[]` array to parse from HEAD.

### Index platform fields and media-type discrimination

OCI Image Index defines `application/vnd.oci.image.index.v1+json`, requires a `manifests` list, and defines each descriptor's optional `platform`; when present, `architecture` and `os` are required within it, while `variant` is optional ([image-spec commit `af26a05…`, lines 3–6 and 18–85](https://github.com/opencontainers/image-spec/blob/af26a05fba5ee648512f4ea3c9fda1fcc1b6d6dc/image-index.md#L3-L6), [property details](https://github.com/opencontainers/image-spec/blob/af26a05fba5ee648512f4ea3c9fda1fcc1b6d6dc/image-index.md#L18-L85)).

Use negotiated response `Content-Type` and verify it against the body's top-level `mediaType` when present: Distribution says clients SHOULD reject a mismatch ([distribution spec lines 162–167](https://github.com/opencontainers/distribution-spec/blob/fee21197eb94360ddfa6dda0b7edabcd12456809/spec.md#L162-L167)). Relevant types are:

* OCI index: `application/vnd.oci.image.index.v1+json`
* Docker manifest list: `application/vnd.docker.distribution.manifest.list.v2+json`
* OCI single manifest: `application/vnd.oci.image.manifest.v1+json`
* Docker schema-2 single manifest: `application/vnd.docker.distribution.manifest.v2+json`

The OCI media-type list and compatibility matrix identify these pairs ([image-spec `media-types.md` lines 3–10, 41–58](https://github.com/opencontainers/image-spec/blob/af26a05fba5ee648512f4ea3c9fda1fcc1b6d6dc/media-types.md#L3-L10), [lines 41–58](https://github.com/opencontainers/image-spec/blob/af26a05fba5ee648512f4ea3c9fda1fcc1b6d6dc/media-types.md#L41-L58)). A GET body is still required to read children even when HEAD's Content-Type tells you it is an index/list.

### Must clients verify `sha256(body) == Docker-Content-Digest`?

**Not unconditionally, and not necessarily SHA-256.** Current distribution-spec text says `Docker-Content-Digest` MAY use a different algorithm, **“Most clients MAY ignore the value, but if it is used, the client MUST verify the value matches the returned manifest.”** If the URL reference itself is a digest, clients **SHOULD** verify the body matches that requested digest ([lines 170–177](https://github.com/opencontainers/distribution-spec/blob/fee21197eb94360ddfa6dda0b7edabcd12456809/spec.md#L170-L177)). Therefore:

* the header is required on successful GET in the current main text, but using it is optional;
* if used, verification is mandatory using the algorithm named by the digest, not hard-coded SHA-256;
* verification against a digest in the requested URL is SHOULD, not MUST.

Do not treat a tag's digest header as trustworthy content identity without hashing when security/correctness depends on it.

## 5. Temporal Start-to-Close and Heartbeat for long transfers

### Conclusion

**Yes: use a Start-to-Close timeout longer than the maximum duration of one transfer attempt, plus a much shorter Heartbeat timeout and frequent progress heartbeats. A heartbeat does not extend or reset Start-to-Close.** For genuinely unbounded/data-dependent work, choose a defensible upper bound for one attempt (possibly computed from size plus margin), checkpoint offsets in heartbeat details, make retry/resume idempotent, and split work into bounded chunks/Activities if no credible upper bound exists.

Temporal's official failure-detection documentation defines Start-to-Close as:

> “the maximum time allowed for a single Activity Task Execution”

and recommends it be **“longer than the maximum possible time for the Activity Execution to complete.”** It immediately adds: **“For long running Activity Executions, we recommend also using Activity Heartbeats and Heartbeat Timeouts.”** ([Start-to-Close section](https://docs.temporal.io/encyclopedia/detecting-activity-failures#start-to-close-timeout)).

The same page says:

> “For long-running Activities, we recommend using a relatively short Heartbeat Timeout and a frequent Heartbeat.”

and defines Heartbeat Timeout as **“the maximum time between Activity Heartbeats”** ([Activity Heartbeat and Heartbeat Timeout](https://docs.temporal.io/encyclopedia/detecting-activity-failures#activity-heartbeat)). These are independent clocks: only a Start-to-Close timeout/retry resets the per-attempt Start-to-Close timer; heartbeat only satisfies the maximum-between-heartbeats timer. The docs explicitly say the Start-to-Close timer resets when a **new retry attempt** is scheduled, not on heartbeat.

Temporal's official long-running-Activity pattern specifically lists **“file uploads and downloads with progress tracking”** as a fit, recommends Heartbeat timeout at 2–3× expected heartbeat interval, meaningful checkpoints, idempotency, and resume from heartbeat details ([long-running Activity pattern](https://docs.temporal.io/design-patterns/long-running-activity)). It warns:

> “Always set HeartbeatTimeout shorter than StartToCloseTimeout.”

This provides quick crash/stall detection without misusing Start-to-Close as a sliding lease. Heartbeat progress should contain a compact offset/chunk identifier so a retry resumes rather than restarts the entire transfer. If business policy needs a cap across all attempts, additionally set Schedule-to-Close, which is the overall Activity Execution limit ([official Schedule-to-Close definition](https://docs.temporal.io/encyclopedia/detecting-activity-failures#schedule-to-close-timeout)).

## Conflicts, gaps, and version notes

* Docker's prose reserves 125/126/127 for `docker run` diagnostics, while its current CLI implementation passes a started workload's same numeric status through. Code and process-status semantics establish the collision; the documentation does not warn about it.
* `docker run`'s bare exit 125 cannot prove whether start succeeded. Container inspection plus lifecycle evidence is necessary. A persisted `.State.ExitCode` belongs to the container state rather than the CLI's create/start diagnostic, but it does not identify which in-container layer selected it.
* OCI Distribution explicitly mandates digest and length on HEAD, not a separate Content-Type MUST in that subsection. HTTP HEAD semantics and the manifest content-negotiation rules support Content-Type, but implementations should not pretend HEAD contains platform JSON.
* Temporal documentation does not use the exact sentence “heartbeats do not reset Start-to-Close”; that conclusion follows directly from its two independent timeout definitions and its explicit statement that Start-to-Close resets on a retry attempt.
