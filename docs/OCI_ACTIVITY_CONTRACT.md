# OCI Activity Contract — architecture

Status: **accepted design, not yet implemented.** This document is the design
authority for replacing the in-process Go plugin registry with pure OCI
execution. It is normative: where it says MUST, an implementation that does
otherwise is wrong. Where it says SHOULD, a deviation needs a recorded reason.

Scope: the engine↔Orker wire, the container-facing filesystem ABI, the durable
commit protocol, image resolution and delivery, Orker dispatch, failure
categories, the security profile, and the first-party activity images. It does
not design a plugin marketplace, a policy engine, or a second runtime.

Grounding: `AGENTS.md` §§1–15, and
`research/2026-08-26-keepcontext-task-design-the-architectural-refactoring-for-orkstack-to-migrate-fr.md`
(read at commit `55b7367`). Empirical evidence for the load-bearing claims is in
§14.

---

## 0. The one-paragraph version

An Orkflow task names an **activity** (`uses:`). At apply time the engine
resolves that name through an **activity catalog** to a registry reference and
then to an immutable **index digest**, and stores both inside the immutable
Orkflow version. At run time the engine schedules one Temporal activity,
`ExecuteTask`, whose input is a proto message carrying coordinates, the pinned
image, the raw `with:` block, and **references** to upstream results — never
their bytes. An Orker receives it, checks object storage for a durable
`commit.json` under a deterministic key, and if one exists returns it without
running anything. Otherwise it builds a `/orkstack` workspace on local disk,
bind-mounts it into a locked-down container, streams stdout and stderr,
heartbeats for the whole run, and on exit `0` validates
`/orkstack/output/values.json`, uploads artifacts, writes `commit.json` with a
create-if-absent conditional write, and only then reports success to Temporal.
Any non-zero exit fails the activity with a category that decides whether
Temporal retries.

---

## 1. Ownership: who knows what

This table is the contract's spine. Every later section is consistent with it.

| Fact | Engine (`cmd/ork`, `internal/engine`, `internal/api`) | Orker (`cmd/orker`, `internal/orker`) | Container | Object store |
|---|---|---|---|---|
| Orkflow YAML, compiled IR, definition digest | **owns** — compiles, resolves images, stamps digest | never sees YAML | never | holds the immutable version document |
| Activity name → image reference | **owns** — the activity catalog is engine config | never resolves a name | never | — |
| Image reference → index digest | **owns** — resolved once, at apply | verifies the pin on pull | never | — |
| Which version a run uses | **owns** — `@current.json` at submit; the run pins it | receives the pin as a coordinate | never | holds the pointers |
| Pool → Temporal task queue | **owns** (`internal/task/pool.go`) | binds exactly one queue at boot | never | — |
| Template rendering | **never** — determinism (`internal/task/contract.go:7-13`) | **owns** — renders `with:` after staging artifacts | sees only the rendered result | — |
| Registry credentials | resolve-only (HEAD manifests), read-only | pull credentials | **never** | — |
| Temporal credentials | yes | yes | **never** | — |
| Object-store credentials | yes | yes | **never** | — |
| Upstream artifact bytes | **never** — references only | materializes them into the workspace | reads the files it was given | holds them |
| Container runtime | never | **owns** — the only Docker/OCI caller in the repo | is the workload | — |
| Exit code | receives it in the result | captures it | produces it | recorded in `commit.json` |
| `values.json` | receives it inline only when ≤ 4 KiB, for display | validates, uploads | writes it | holds the authoritative copy |
| Live logs | reads chunks | writes chunks | writes stdout/stderr | holds the chunks |
| Durable commit record | reads it for archive | writes it, exactly once | never sees it | **holds it — it is the record** |
| Failure category | projects it into `Error.category` | assigns it | expresses only an exit code | recorded on failure paths |
| Secrets | resolves references to a source | materializes files, deletes them after | reads files it was told about | never (secret bytes never stored) |

Three rules follow and are enforced everywhere below:

1. **The container never holds a platform credential.** The Orker is the
   security boundary (AGENTS.md §2.3).
2. **Bytes never travel through Temporal.** The engine's inputs and the Orker's
   results carry references. The only exception is a `values.json` of at most
   4 KiB, copied inline purely so the engine's read model can display it.
3. **The engine never touches a filesystem or a registry inside workflow code.**
   Image resolution happens at *apply*, in `internal/api`, not in
   `internal/engine`'s deterministic workflow.

---

## 2. Proto contract

### 2.1 Compatibility rule

The launch objective says two things that cannot both hold literally: "*This is
NOT a backward-compatible migration*" and "*Proto contract is forward-compatible
(no breaking changes to existing code)*". The narrowest reading satisfying both,
and the one this design uses:

> **Field numbers are never reused or renumbered.** A removed field is
> `reserved`. Go APIs, the DSL surface, and the `internal/task` structs may be
> replaced outright.

The repo already works this way: `proto/orkstack/v1/orkflow.proto:37-38` carries
`reserved 8; reserved "queue";` with a matching compiler rejection in
`internal/dsl/removed.go:14-39`. One artifact still breaks regardless — the
committed Temporal replay fixture (§11.4).

Measured, not asserted (§14.6): the full proto below compiles, passes `buf
lint`, and passes `buf breaking --against` the current tree under both
`WIRE_JSON` and `FILE` minus `FIELD_NO_DELETE`. The single complaint under the
repo's literal `breaking: use: [FILE]` is that `Task.plugin` was deleted — and
the same command reports the identical complaint for the `queue` field the repo
already reserved, so this design is no less compatible than the tree it edits.
`make proto` runs `buf lint` and `buf generate`, not `buf breaking`.

### 2.2 New file: `proto/orkstack/v1/activity.proto`

```proto
syntax = "proto3";

package orkstack.v1;

import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
import "orkstack/v1/common.proto";

option go_package = "github.com/orkstack/orkstack/gen/go/orkstack/v1;orkstackv1";

// ---------------------------------------------------------------- images

// ImageRef is an OCI image resolved to an immutable reference. The engine
// produces it at apply time and it never changes afterwards: a run executes
// what `pinned` names, whatever a tag does later.
message ImageRef {
  // reference is what the author wrote, verbatim: "ghcr.io/acme/migrate:v3",
  // "shell", or an already-pinned "repo@sha256:...". Never used to pull.
  string reference = 1;
  // registry is the resolved host, e.g. "ghcr.io" or "docker.io".
  string registry = 2;
  // repository is the resolved path, e.g. "acme/migrate".
  string repository = 3;
  // index_digest is the digest of the object the tag named at resolve time:
  // an OCI image index for a multi-platform tag, an image manifest otherwise.
  // Spelled "sha256:<64 hex>".
  string index_digest = 4;
  // pinned is the only string that may be pulled or run:
  //   "{registry}/{repository}@{index_digest}"
  string pinned = 5;
  // media_type is the media type index_digest addresses.
  string media_type = 6;
  // platforms lists the platforms the index advertises, "os/arch[/variant]".
  // Empty for a single-manifest reference.
  repeated string platforms = 7;
  // 8 held resolved_at, the wall-clock time of the resolution. It is REMOVED,
  // not merely excluded from the definition digest: ImageRef travels inside
  // Task, Task travels inside the immutable Orkflow version document, and
  // `pkg/artifacts.sameStoredOrkflow` (orkflow_store.go:400-408) decides
  // "this apply is a repeat" with proto.Equal after clearing ONLY CreatedAt.
  // A per-apply timestamp there turns every re-apply of an unchanged document
  // into a version conflict (§4.4). Resolution time is audit data, not
  // identity: the version document's created_at already records when the
  // apply happened, and ActivityCommit records when the image actually ran.
  reserved 8;
  reserved "resolved_at";
  // platform_digest is the manifest digest the Orker actually ran, and the
  // platform it ran on. The engine leaves both empty; only an Orker fills
  // them, and only into ExecuteTaskResult.image and ActivityCommit.image.
  string platform_digest = 9;
  string platform = 10;
}

// ---------------------------------------------------------------- execution shape

// ResourceRequest is what a task asks the Orker for. Orker policy may cap it
// (AGENTS.md §6); it may never raise it.
message ResourceRequest {
  string cpu = 1;     // Kubernetes spelling: "2", "500m"
  string memory = 2;  // Kubernetes spelling: "4Gi", "512Mi"
}

// NetworkMode is the complete set of network configurations an Orkflow may
// select. Named Orker-defined profiles are deliberately absent (AGENTS.md §6).
enum NetworkMode {
  NETWORK_MODE_UNSPECIFIED = 0;  // treated as NETWORK_MODE_DEFAULT
  NETWORK_MODE_DEFAULT = 1;
  NETWORK_MODE_NONE = 2;
}

// SecretRef names a secret and where the Orker gets it. The value never
// appears in a proto message, in Temporal history, or in the archive.
message SecretRef {
  // name is the file name the workload reads under /orkstack/input/secrets/.
  // Validated: ^[A-Za-z0-9._-]{1,128}$.
  string name = 1;
  // source is an Orker-resolvable locator, e.g. "env:DB_PASSWORD" or
  // "file:/etc/orkstack/secrets/db". Orker policy decides which schemes exist.
  string source = 2;
}

// WorkspaceLimits are the contract limits the Orker enforces after the
// container exits. They also appear verbatim in /orkstack/input/task.json so a
// workload can respect them instead of discovering them by failing.
message WorkspaceLimits {
  int64 values_max_bytes = 1;      // default 262144 (256 KiB)
  int32 artifact_max_count = 2;    // default 256
  int64 artifact_max_bytes = 3;    // per artifact; default 5368709120 (5 GiB)
  int64 log_max_bytes = 4;         // per stream per attempt; default 16777216
  // commit_max_bytes bounds the serialized ActivityCommit. Default 4194304
  // (4 MiB). See §5.3 for why it is not 64 KiB, and §8.2 for what happens
  // when a legal workload would produce a commit above it.
  int64 commit_max_bytes = 5;
}

// ---------------------------------------------------------------- upstream

// UpstreamRef is one completed ancestor's contribution to a downstream task.
// It carries references only: fan-in is O(ancestors) small records, never a
// transitive copy of every ancestor's payload.
message UpstreamRef {
  string task_id = 1;
  // commit_uri addresses the ancestor's ActivityCommit object.
  string commit_uri = 2;
  // values is the reference to the ancestor's committed values.json.
  ArtifactRef values = 3;
  // artifacts are the ancestor's output artifacts, references only.
  repeated ArtifactRef artifacts = 4;
}

// ---------------------------------------------------------------- the seam

// ExecuteTaskInput is the argument of the ExecuteTask Temporal activity. It
// replaces the Go struct at internal/task/contract.go:49-84 outright.
//
// Serialized size MUST NOT exceed 262144 bytes (AGENTS.md §2.2). The engine
// checks this deterministically before scheduling and fails the task with
// ContractViolation/INPUT_TOO_LARGE when it would be exceeded.
message ExecuteTaskInput {
  // --- coordinates
  string run_id = 1;
  string orkflow_name = 2;
  string orkflow_version = 3;
  // orkflow_definition_digest is the sha256 of the canonical compiled
  // definition this run pinned (AGENTS.md §2.2, §7.2).
  string orkflow_definition_digest = 4;
  string namespace = 5;   // empty means the built-in "default"
  string pool = 6;        // empty means the deployment's default pool
  string task_id = 7;

  // --- identity
  // task_idempotency_key is the stable key of one logical activity, unchanged
  // across every retry (AGENTS.md §3, §4). Its value is exactly
  //   "{namespace-or-default}/{run_id}/{task_id}"
  // It is deliberately NOT the run-submission idempotency key handled by
  // IdempotencyClaimWorkflow; the two are different things (§11.5).
  string task_idempotency_key = 8;
  // commit_uri is the deterministic object URI of this activity's commit
  // record. The engine composes it from the commit-store root carried in
  // RunPlan (decided at submit, outside workflow code) plus the run and task
  // coordinates, so composing it stays a pure function of workflow arguments
  // (§5.2, §7.2).
  string commit_uri = 9;

  // --- what to run
  ImageRef image = 10;
  // command overrides the image ENTRYPOINT/CMD. Empty means the image decides.
  repeated string command = 11;
  // with is the RAW, un-rendered `with:` block. Rendering stays in the Orker
  // because `artifact "<task>/<name>"` resolves to a path only the Orker knows
  // after staging (internal/task/contract.go:7-13). The rendered result is
  // materialized to /orkstack/input/config.json.
  google.protobuf.Struct with = 12;
  // env is workload-supplied environment. The platform puts NOTHING here:
  // required platform state travels as files (AGENTS.md §3).
  map<string, string> env = 13;
  repeated SecretRef secrets = 14;

  // --- how to run it
  int32 timeout_seconds = 15;                    // container wall clock
  int32 cancellation_grace_period_seconds = 16;  // default 30 (AGENTS.md §3.3)
  ResourceRequest resources = 17;
  NetworkMode network = 18;
  WorkspaceLimits limits = 19;

  // --- context
  Payload run_input = 20;               // exposed to templates as .inputs
  repeated UpstreamRef upstream = 21;   // exposed as .tasks.<id>
}

// CommitRef points at a written ActivityCommit object.
message CommitRef {
  string uri = 1;
  // sha256 is the hex sha256 of the exact bytes written to, or read from, the
  // store. It MUST NEVER be recomputed by re-marshalling a decoded
  // ActivityCommit: protojson deliberately varies its output across builds
  // (§14.10), so a rebuilt Orker would compute a different digest for an
  // unchanged commit.
  string sha256 = 2;
  string etag = 3;       // store etag observed at write or read
  google.protobuf.Timestamp created_at = 4;
}

// ExecuteTaskResult is what an Orker returns for a SUCCEEDED activity. A
// failure is an ApplicationError, not a result (§8).
message ExecuteTaskResult {
  string task_id = 1;
  // exit_code is the container's exit status. It is always 0 in a result: a
  // non-zero exit is a failure. It travels so the engine, the archive and the
  // UI record the fact rather than inferring it.
  int32 exit_code = 2;
  // commit references the durable completion record. Always set.
  CommitRef commit = 3;
  // artifacts are the workload's output artifacts, references only.
  repeated ArtifactRef artifacts = 4;
  // system_artifacts are platform-owned objects — stdout.log, stderr.log —
  // kept in a separate field so a workload artifact can never collide with a
  // platform name.
  repeated ArtifactRef system_artifacts = 5;
  // values_ref addresses the committed values.json. Always set.
  ArtifactRef values_ref = 6;
  // values_json is the inline copy of values.json — the workload's EXACT
  // bytes — present ONLY when values.json is at most 4096 bytes. It exists
  // for the engine's read model (TaskRun.output, the UI). Nothing renders a
  // downstream template from it; downstream tasks read the committed object.
  //
  // It is `bytes`, not Payload, because Payload wraps google.protobuf.Struct
  // and a Struct round trip is lossy in four measured ways: key order is not
  // preserved, number formatting is normalized, unicode escapes are decoded,
  // and an integer outside float64's exact range is SILENTLY CORRUPTED
  // (12345678901234567890 -> 12345678901234567000). See §14.9. A field that
  // promises byte identity cannot be a Struct.
  bytes values_json = 7;
  bool values_inlined = 8;
  // image is the ImageRef as executed, with platform_digest and platform set.
  ImageRef image = 9;
  // commit_adopted is true when this attempt found an existing commit and
  // returned it without running the container (§5.4).
  bool commit_adopted = 10;
  google.protobuf.Timestamp started_at = 11;
  google.protobuf.Timestamp finished_at = 12;
  string orker_id = 13;
}

// ---------------------------------------------------------------- the commit record

// ActivityCommit is the immutable completion manifest (AGENTS.md §4.1). It is
// serialized with protojson and stored as commit.json. Serialized size MUST
// NOT exceed WorkspaceLimits.commit_max_bytes (default 4 MiB, §5.3).
message ActivityCommit {
  // schema_version is 1. A reader that does not know the value refuses the
  // commit with ContractViolation/COMMIT_SCHEMA_UNSUPPORTED rather than
  // guessing (§5.4).
  int32 schema_version = 1;

  string namespace = 2;
  string run_id = 3;
  string task_id = 4;
  string orkflow_name = 5;
  string orkflow_version = 6;
  string orkflow_definition_digest = 7;
  string task_idempotency_key = 8;

  ImageRef image = 9;                        // with platform_digest set
  int32 exit_code = 10;                      // always 0 in schema_version 1
  ArtifactRef values = 11;
  repeated ArtifactRef artifacts = 12;
  repeated ArtifactRef system_artifacts = 13;

  google.protobuf.Timestamp started_at = 14;
  google.protobuf.Timestamp finished_at = 15;
  int32 attempt = 16;                        // the attempt that committed
  string orker_id = 17;
  string build_id = 18;                      // ORK_BUILD_ID of the committer
}

// ---------------------------------------------------------------- orker identity

// DescribeOrkerResult replaces ListPlugins as the pool-liveness probe. An Orker
// no longer has a plugin list, but a pool still has to prove it is alive
// (proof/M11.6), so the probe stays and only its payload changes.
message DescribeOrkerResult {
  string orker_id = 1;
  string build_id = 2;
  string pool = 3;
  string runtime_name = 4;      // "docker"
  string runtime_version = 5;
  string security_profile = 6;  // the profile id the Orker enforces (§9.2)
  int32 max_concurrent = 7;
  bool rootless = 8;
}
```

### 2.3 Changes to existing files

`proto/orkstack/v1/common.proto` — append one field to `Error`:

```proto
// FailureCategory is the AGENTS.md §4 category of a terminal failure. Its
// value names are also the exact Temporal ApplicationError type strings, so a
// RetryPolicy's non_retryable_error_types can name them (§7.2).
enum FailureCategory {
  FAILURE_CATEGORY_UNSPECIFIED = 0;
  FAILURE_CATEGORY_ACTIVITY_FAILED = 1;
  FAILURE_CATEGORY_INFRASTRUCTURE_ERROR = 2;
  FAILURE_CATEGORY_CONTRACT_VIOLATION = 3;
  FAILURE_CATEGORY_TIMED_OUT = 4;
  FAILURE_CATEGORY_CANCELLED = 5;
}

message Error {
  string code = 1;
  string message = 2;
  string details = 3;
  bool retryable = 4;
  FailureCategory category = 5;   // NEW
}
```

`proto/orkstack/v1/orkflow.proto` — `Task` gains the OCI fields and retires
`plugin`:

```proto
message Task {
  string id = 1;

  // 2 held the in-process plugin name. Under the OCI Activity Contract a task
  // names an activity, and an activity is an image. Reserved rather than
  // reused so a stored version document can never be re-read as an image.
  reserved 2;
  reserved "plugin";

  google.protobuf.Struct with = 3;
  repeated string needs = 4;
  string condition = 5;
  int32 timeout_seconds = 6;
  RetryPolicy retry = 7;
  reserved 8;
  reserved "queue";
  string pool = 9;

  // uses is what the author wrote: a catalog activity name ("shell") or a
  // registry reference ("ghcr.io/acme/migrate:v3", "repo@sha256:..."). It is
  // the only image-selecting field in the DSL.
  string uses = 10;
  // image is the engine's apply-time resolution of uses. It is stamped into
  // the immutable version document, so re-applying the same YAML after a tag
  // moves produces a different definition (§4.4).
  ImageRef image = 11;
  repeated string command = 12;
  map<string, string> env = 13;
  repeated SecretRef secrets = 14;
  ResourceRequest resources = 15;
  NetworkMode network = 16;
  int32 cancellation_grace_period_seconds = 17;
}
```

`orkflow.proto` must import `orkstack/v1/activity.proto` for `ImageRef`,
`SecretRef`, `ResourceRequest` and `NetworkMode`.

`proto/orkstack/v1/run.proto` — the read model must be able to say what the
commit protocol produced. Without these, Phase 2 and Phase 3 would invent
fields after the Phase 1 freeze, which is exactly what §11.1 exists to prevent.

```proto
message TaskRun {
  string task_id = 1;

  // 2 held the in-process plugin name, mirroring Task.plugin. Reserved for
  // the same reason and at the same time.
  reserved 2;
  reserved "plugin";

  Status status = 3;
  int32 attempt = 4;
  // output carries the inline values.json when one was inlined, decoded for
  // display only. values_ref is the authoritative reference.
  Payload output = 5;
  repeated ArtifactRef artifacts = 6;
  Error error = 7;                       // now carries FailureCategory
  google.protobuf.Timestamp started_at = 8;
  google.protobuf.Timestamp finished_at = 9;
  string pool = 10;

  // values_ref addresses the committed values.json for this attempt. Set
  // whenever the task succeeded, whether or not output was inlined.
  ArtifactRef values_ref = 11;
  // commit references the durable completion record (§5).
  CommitRef commit = 12;
  // exit_code is the container's exit status as reported by the Orker.
  int32 exit_code = 13;
  // system_artifacts are stdout.log and stderr.log for this attempt.
  repeated ArtifactRef system_artifacts = 14;
}

message Run {
  // … fields 1-12 unchanged …

  // orkflow_definition_digest is the canonical digest of the definition this
  // run pinned (§4.4). A run that started before a pointer moved is still
  // explainable by the exact bytes it ran.
  string orkflow_definition_digest = 13;
}
```

`run.proto` must import `orkstack/v1/activity.proto` for `CommitRef`.

`proto/orkstack/v1/service.proto` — `PluginInfo` and `ListPlugins`-shaped
surfaces are replaced by an activity catalog read (`ListActivities`) and
`DescribeOrker`. Existing field numbers in those messages stay reserved.

### 2.4 What dies in Go

`pkg/plugin.Plugin`, `pkg/plugin.Registry`, `internal/orker.BuiltinRegistry`,
`pkg/plugins/{http,shell,file,container}`, `task.ActivityListPlugins`, and the
`ExecuteTaskInput`/`ExecuteTaskResult`/`UpstreamResult` structs in
`internal/task/contract.go`. `pkg/plugin/linewriter.go` survives the deletion of
its package by moving to `pkg/logstream` unchanged — the UI already decodes its
framing (proof/M11.4).

### 2.5 Encoder note

Temporal's converter encodes a top-level protobuf message with `protojson` and
everything else with `encoding/json` (`internal/engine/plan.go:23-26`). Making
`ExecuteTaskInput` a proto message therefore flips the recorded payload
encoding. That is a deliberate, one-time change and is the reason §11.4 exists.

---

## 3. The `/orkstack` workspace

The filesystem is the activity API. A workload needs `cat`, a JSON reader, and
a redirect. If it needs anything else, this section is wrong.

### 3.1 Layout

```
/orkstack/
├── input/                          read-only bind, mode 0555
│   ├── task.json                   0444  the activity context (§3.2)
│   ├── config.json                 0444  the rendered `with:` block
│   ├── run.json                    0444  the run input payload
│   ├── secrets/                    0555  separate read-only bind
│   │   └── <secret-name>           0444  exact secret bytes, no trailing newline added
│   ├── artifacts/                  0555
│   │   └── <producer-task>/
│   │       └── <safe-name>         0444  one regular file per input artifact
│   └── upstream/                   0555
│       └── <task-id>/
│           └── values.json         0444  that ancestor's committed values.json
├── output/                         read-write bind, mode 1777
│   ├── values.json                 written by the workload — REQUIRED
│   └── artifacts/                  1777  one regular file per output artifact
└── tmp/                            read-write bind, mode 1777
```

The container's working directory is `/orkstack`. The container root filesystem
is read-only (§9), so `/orkstack/tmp` is the workload's scratch space; `/tmp`
inside the container is a separate small tmpfs the Orker also provides.

**Permissions rationale.** The host per-attempt directory is `0700`, owned by
the Orker's user, so no other host user can traverse into a `1777` child. Inside
the container the workload may run as any UID the image chooses — the Orker does
not force `--user` by default — so every directory the workload must write is
world-writable with the sticky bit, and every file it must read is `0444`. This
combination was executed and verified (§14.3): a container running as UID 65534
read `/orkstack/input/task.json`, wrote `/orkstack/output/values.json`, and was
refused a write to `/orkstack/input` with `Read-only file system`.

**Readability is NOT guaranteed by the mount, and the design must not pretend
otherwise.** `1777` guarantees the workload can *create* a file; it says
nothing about the mode that file lands with. A workload running under
`umask 077` writes `values.json` as `0600` owned by its own UID, and an Orker
running as a different, non-root user then cannot read the bytes it must hash
and upload. Both outcomes were reproduced this session on one machine
(§14.8): on a native filesystem the Orker got `Permission denied`; through a
VM-shared bind mount the same write was ownership-remapped to the host user
and read fine. The behavior depends on the host and the mount type, so no
mount configuration can be relied on.

The design therefore takes three measures together, and needs no exemption
from §9.1:

1. **Detect and fail terminally.** After the container exits, the Orker opens
   every file it must read. A file it cannot open is
   `ContractViolation/OUTPUT_NOT_READABLE` — non-retryable, because the image
   will do the same thing on every attempt. The error names the file, its
   mode, its owner, and the fix (`umask 022`). `stat` succeeds even when
   `open` fails, so the message can always be specific; this was verified
   (§14.8: `mode=600 owner=65534:65534`). Classifying it as
   `ARTIFACT_UPLOAD_FAILED` — infrastructure, retryable — would loop forever
   against deterministic image behavior, which AGENTS.md §11 forbids.
2. **Give an explicit escape hatch.** A task may set `runAsOrkerUser: true`,
   or an Orker may set `ORK_ORKER_FORCE_USER=1`, which adds
   `--user <orker-uid>:<orker-gid>`. The Orker then owns every written file
   and reads it whatever the umask — verified (§14.8). It is opt-in because
   it overrides the image's own `USER` and breaks images that need their
   baked-in identity, which would violate "any OCI image can become an
   activity".
3. **Require first-party images to set `umask 022`** in their entry scripts
   (§10.2), and make `orkstack activity test` assert output modes (§10.3).

Two mechanisms were considered and rejected. `--group-add` plus a setgid
output directory propagates group *ownership* but not group *permission*, so
`umask 077` still produces `0600` and still fails. An Orker-owned relabel
container running `chmod -R a+rX` works, but requires UID 0 plus `CAP_FOWNER`,
which §9.1 forbids; adopting it would have needed a security exemption, so it
is not adopted.

**The Orker MUST NOT assume ownership of files the workload wrote**, only that
it can open them — and it verifies even that rather than assuming it.

**Host placement.** The workspace root is a per-attempt directory under
`ORK_ORKER_WORKDIR` (`internal/orker/activity.go:252-268`). It MUST be on a path
the container daemon can bind-mount. On macOS with a VM-hosted daemon, `$HOME`
is shared and `/var/folders` is not (`examples/container-example.ork.yaml:5-14`),
which is why `make up` points `ORK_ORKER_WORKDIR` at `.data/work`. The Orker
MUST fail at boot with `InfrastructureError/WORKDIR_NOT_BINDABLE` if a probe
bind-mount of that root fails, rather than failing every task later.

**Secrets.** `/orkstack/input/secrets` is a *separate* bind from a per-attempt
`secrets/` directory that the Orker creates, populates, mounts read-only, and
removes on every exit path. Secret bytes never enter a proto message, Temporal
history, the archive, or a log chunk. Where the host supports it, the Orker
SHOULD place that directory on a tmpfs-backed path (`ORK_ORKER_SECRETS_DIR`).

### 3.2 `input/task.json`

The complete activity context, as one JSON object. Every field a workload could
otherwise want from an environment variable lives here, because environment
variables are not part of the ABI (AGENTS.md §3, user decision 1). Key casing is
lowerCamelCase, matching every other JSON surface OrkStack emits
(`docs/JSON.md`).

```jsonc
{
  "schemaVersion": 1,
  "idempotencyKey": "default/ork-1786837143070480000-1/migrate",

  "run": {
    "id": "ork-1786837143070480000-1",
    "namespace": "default",
    "orkflow": {
      "name": "deploy",
      "version": "1.4.0",
      "definitionDigest": "sha256:6f1c…"
    }
  },

  "task": {
    "id": "migrate",
    "attempt": 2,
    "pool": "main"
  },

  "image": {
    "reference": "ghcr.io/acme/migrate:v3",
    "pinned": "ghcr.io/acme/migrate@sha256:9ab2…",
    "indexDigest": "sha256:9ab2…",
    "platformDigest": "sha256:41de…",
    "platform": "linux/arm64"
  },

  "limits": {
    "timeoutSeconds": 300,
    "cancellationGracePeriodSeconds": 30,
    "valuesMaxBytes": 262144,
    "artifactMaxCount": 256,
    "artifactMaxBytes": 5368709120,
    "logMaxBytes": 16777216,
    "commitMaxBytes": 4194304
  },

  "resources": { "cpu": "2", "memory": "4Gi" },
  "network": "none",

  "paths": {
    "input": "/orkstack/input",
    "config": "/orkstack/input/config.json",
    "runInput": "/orkstack/input/run.json",
    "secrets": "/orkstack/input/secrets",
    "output": "/orkstack/output",
    "values": "/orkstack/output/values.json",
    "artifacts": "/orkstack/output/artifacts",
    "tmp": "/orkstack/tmp"
  },

  "inputs": {
    "artifacts": [
      {
        "producer": "build",
        "name": "dist.tar.gz",
        "nameB64": "ZGlzdC50YXIuZ3o",
        "path": "/orkstack/input/artifacts/build/dist.tar.gz",
        "sizeBytes": 10485760,
        "sha256": "3b1f…",
        "contentType": "application/gzip"
      }
    ],
    "upstream": [
      { "taskId": "build", "valuesPath": "/orkstack/input/upstream/build/values.json" }
    ]
  },

  "secrets": [
    { "name": "db_password", "path": "/orkstack/input/secrets/db_password" }
  ]
}
```

`paths` is redundant with the fixed layout on purpose: a workload that reads it
never hardcodes a path, and a future layout change stays a data change.

**Artifact names are opaque bytes; paths are derived.** `path` is produced by
the existing `safeComponent` rule (`internal/orker/resolver.go:127-151`) and is
lossy. `name` carries the exact logical name when it is valid UTF-8;
`nameB64` (RawURL base64 of the exact bytes) is **always** present and is
authoritative. This mirrors the existing S3 companion manifest, which already
solves "the safe key is not the name" (`pkg/artifacts/s3_manifest.go:144-187`).

#### The schema, field by field

`task.json` is a contract, so it is specified as one rather than left to an
example. Required means always present; a workload may rely on it. Optional
means present only under the stated condition; a workload MUST tolerate its
absence rather than treating it as an error.

| Path | JSON type | Req. | Empty form | Meaning |
|---|---|---|---|---|
| `schemaVersion` | integer | required | — | Major version of this file. `1` today. |
| `idempotencyKey` | string | required | — | `"{namespace}/{runId}/{taskId}"`, stable across retries |
| `run.id` | string | required | — | Opaque run id. Never parsed by anything. |
| `run.namespace` | string | required | `"default"` | Resolved, never empty on the wire into the container |
| `run.orkflow.name` | string | required | — | slug |
| `run.orkflow.version` | string | required | — | pinned version string |
| `run.orkflow.definitionDigest` | string | required | — | `sha256:<64 hex>` (§4.4) |
| `task.id` | string | required | — | slug |
| `task.attempt` | integer | required | — | 1-based Temporal attempt |
| `task.pool` | string | required | `"main"` | resolved pool name |
| `image.reference` | string | required | — | as authored |
| `image.pinned` | string | required | — | `{registry}/{repo}@sha256:…` — what ran |
| `image.indexDigest` | string | required | — | `sha256:<64 hex>` |
| `image.platformDigest` | string | optional | absent | present once the Orker resolved the platform manifest |
| `image.platform` | string | optional | absent | `os/arch[/variant]` |
| `limits.timeoutSeconds` | integer | required | — | container wall clock |
| `limits.cancellationGracePeriodSeconds` | integer | required | — | SIGTERM→SIGKILL window |
| `limits.valuesMaxBytes` | integer | required | — | §3.4 |
| `limits.artifactMaxCount` | integer | required | — | §3.5 |
| `limits.artifactMaxBytes` | integer | required | — | per artifact |
| `limits.logMaxBytes` | integer | required | — | per stream per attempt |
| `limits.commitMaxBytes` | integer | required | — | §5.3 |
| `resources.cpu` | string | optional | absent | as requested/capped; absent means Orker default |
| `resources.memory` | string | optional | absent | as requested/capped |
| `network` | string | required | `"default"` | `"default"` or `"none"` only |
| `paths.*` | string | required | — | all eight keys always present (§3.1) |
| `inputs.artifacts` | array | required | `[]` | never null |
| `inputs.artifacts[].producer` | string | required | — | upstream task id |
| `inputs.artifacts[].name` | string | optional | absent | present only when the exact name is valid UTF-8 |
| `inputs.artifacts[].nameB64` | string | required | — | RawURL base64 of the exact name bytes; authoritative |
| `inputs.artifacts[].path` | string | required | — | absolute container path |
| `inputs.artifacts[].sizeBytes` | integer | required | `0` | |
| `inputs.artifacts[].sha256` | string | required | — | hex |
| `inputs.artifacts[].contentType` | string | optional | absent | |
| `inputs.upstream` | array | required | `[]` | never null |
| `inputs.upstream[].taskId` | string | required | — | |
| `inputs.upstream[].valuesPath` | string | required | — | absolute container path |
| `secrets` | array | required | `[]` | never null; names and paths only, never values |
| `secrets[].name` | string | required | — | `^[A-Za-z0-9._-]{1,128}$` |
| `secrets[].path` | string | required | — | absolute container path |

An absent array is never written as `null`; the Orker emits `[]`. An absent
optional scalar is omitted, never written as `""` — a workload distinguishes
"no content type" from "empty content type" by presence.

#### Two policies the file itself cannot state

**Unknown fields.** A workload MUST ignore keys it does not recognize. The
Orker MUST NOT emit a key that is not in the table above for the declared
`schemaVersion`, so an unknown key always means the workload is older than the
platform, never that the platform invented something.

**Version handling.** Within one major `schemaVersion` the platform MUST NOT
remove a field, retype a field, or change a field's meaning; it may only add
optional fields. A workload that reads a `schemaVersion` it does not know MUST
fail loudly rather than proceed on guessed semantics — a wrong result committed
durably is worse than a clean failure.

### 3.3 `input/config.json` and `input/run.json`

`config.json` is the `with:` block after the Orker renders it: a JSON object,
templates resolved, `artifact "<task>/<name>"` already rewritten to the
container path under `/orkstack/input/artifacts/`, `env "<name>"` already
substituted. `run.json` is the run's input payload — the same data templates see
as `.inputs`.

Rendering rules are unchanged from `internal/orker/render.go:36-168`:
recursive over nested maps and lists, `missingkey=error`, a rendered
`<no value>` is rejected. A render failure is
`ContractViolation/RENDER_FAILED` and is never retried.

### 3.4 `output/values.json`

- **REQUIRED.** Exit `0` with no `values.json` is
  `ContractViolation/VALUES_MISSING`. A workload with no result writes `{}`.
  Retrying is harmless but pointless: the image is wrong, not the world.
- MUST be a JSON **object** at the top level. An array, a scalar, or `null` is
  `ContractViolation/VALUES_MALFORMED`.
- MUST be valid UTF-8 and MUST NOT exceed `limits.valuesMaxBytes` (256 KiB
  default) → `ContractViolation/VALUES_TOO_LARGE`.
- MUST be readable by the Orker after the container exits. A file the Orker
  cannot open is `ContractViolation/OUTPUT_NOT_READABLE` (§3.1).
- The bytes are uploaded verbatim. The Orker parses to validate and never
  re-serializes: key order, spacing and number formatting are preserved, and
  the committed `sha256` is the sha256 of exactly what the workload wrote.
  The same exact bytes are what travels inline as
  `ExecuteTaskResult.values_json` — which is why that field is `bytes` and not
  a `Struct`-backed `Payload` (§2.2, §14.9).

Why 256 KiB and not the 4 KiB of Tekton or Kubernetes: `values.json` is an
object-store artifact, so its budget is not the Temporal payload budget. The
Temporal budget is enforced separately — only a `values.json` of ≤ 4 KiB is
copied inline into `ExecuteTaskResult.values_json`.

### 3.5 `output/artifacts/`

- Each **regular file directly inside** the directory is one output artifact.
- The artifact's logical name is the file's name, exactly.
- The scan is **not recursive**. A directory, symlink, device, socket or FIFO
  inside `output/artifacts/` is `ContractViolation/ARTIFACT_NOT_A_FILE`. A
  directory hierarchy is archived by the workload, e.g. as `workspace.tar.gz`
  (AGENTS.md §3.1). The artifact layer has no directory semantics.
- More than `limits.artifactMaxCount` files, or any file larger than
  `limits.artifactMaxBytes`, is `ContractViolation/ARTIFACT_LIMIT_EXCEEDED`.
- Every file MUST be readable by the Orker; one that is not is
  `ContractViolation/OUTPUT_NOT_READABLE` (§3.1).
- Ordering is by name bytes ascending, so two attempts of the same computation
  produce the same commit ordering.

### 3.6 Streams

stdout and stderr are captured **independently**, byte-exact, and never merged
on disk. Two things happen to them:

1. **Live**, they go to the existing log sink
   (`internal/orker/logsink.go:54-521`) with the existing framing
   (`[stderr] ` prefix, `[stdout] ` escape) so the browser keeps rendering
   tinted stderr in real time (proof/M11.1, proof/M11.4). This path is
   unchanged and is a regression gate.
2. **Durably**, each stream is uploaded as its own object and recorded in
   `ExecuteTaskResult.system_artifacts` and `ActivityCommit.system_artifacts`
   under the names `stdout.log` and `stderr.log`. Because `system_artifacts` is
   a separate field from `artifacts`, a workload artifact named `stdout.log`
   cannot collide with the platform's.

Plain text works with no logging SDK. Structured JSON is not part of the ABI.

---

## 4. Image resolution and delivery

### 4.1 The two halves

```
APPLY TIME (engine, internal/api, once per version)
  uses: "shell"                      catalog lookup
     └─> ghcr.io/orkstack/shell:0.1   registry HEAD /v2/<repo>/manifests/<tag>
          └─> sha256:9ab2…            Docker-Content-Digest
               └─> ImageRef{pinned: "ghcr.io/orkstack/shell@sha256:9ab2…"}
                    └─> stamped into the immutable Orkflow version document

RUN TIME (Orker, every attempt)
  ImageRef.pinned
     └─> docker pull ghcr.io/orkstack/shell@sha256:9ab2…   (content-verified)
          └─> record platform_digest + platform into the commit
```

A tag is never pulled. A tag is never run. The only string that reaches the
runtime is `ImageRef.pinned`.

### 4.2 The activity catalog (engine config)

The catalog maps a short activity name to a registry reference. It is engine
configuration, not an Orker probe and not a marketplace.

```yaml
# ORK_ACTIVITY_CATALOG, default: the built-in table below
activities:
  shell: ghcr.io/orkstack/activity-shell:0.1
  http:  ghcr.io/orkstack/activity-http:0.1
  file:  ghcr.io/orkstack/activity-file:0.1
```

`container` is deliberately absent: under this contract every task already is a
container, so it is a DSL alias rather than an image (§10.2).

`uses:` resolution order, applied in `internal/dsl` validation and then in
`internal/api` before `OrkflowStore.Put`:

1. If `uses` contains a Go template action (`{{`), reject at compile time with
   `IMAGE_NOT_PINNED`. An image chosen at run time cannot be pinned to a digest
   at apply time, and AGENTS.md §5 forbids running an unresolved reference
   (§10.2).
2. Else if `uses` matches `^[a-z][a-z0-9-]*$` and is a catalog key → the
   catalog reference.
3. Else if `uses` parses as an OCI reference (contains `/`, `:` or `@`) →
   itself.
4. Else → compile error `UNKNOWN_ACTIVITY`, listing the catalog keys.

Rule 2 before rule 3 means `shell` can never accidentally mean Docker Hub's
`library/shell`.

### 4.3 Where resolution runs

`dsl.Compile` stays pure — no network, no clock — because
`TestCompile_AcceptsEveryShippedExample` and every hermetic compiler test depend
on it (`internal/dsl/compile.go:121-193`). Resolution sits in
`internal/api.ApplyOrkflow`, between `Compile` and `Put`, where `created_at` is
already stamped (`internal/api/service.go:342-393`).

The resolver:

1. Collects the distinct `uses` values of the compiled tasks.
2. For each, issues `HEAD /v2/<repository>/manifests/<tag-or-digest>` with the
   **exact** `Accept` header set the Orker will use at pull time, and reads
   `Docker-Content-Digest`. Sending a different `Accept` set at resolve and pull
   can select a different object (OCI distribution-spec content negotiation).
3. For an already-pinned `repo@sha256:…` it still issues the HEAD, and requires
   a `200` whose `Docker-Content-Digest` equals the pin. Apply MUST NOT succeed
   with an unpullable pin.
4. Stamps `Task.image`.

Failures are apply-time Connect errors, never silent: `IMAGE_NOT_FOUND` (404),
`IMAGE_RESOLVE_UNAUTHORIZED` (401/403), `IMAGE_RESOLVE_UNAVAILABLE` (5xx,
timeout, DNS). No fallback to a tag exists.

**The daemon is not the resolver.** `docker image inspect .RepoDigests` reports
whatever the daemon negotiated for the platform it pulled, and
`docker manifest inspect --verbose` does not reliably surface a parent index
digest. Resolution goes to the registry, over the distribution API, or through
`docker buildx imagetools inspect`, which does report the index digest (§14.4).

### 4.4 Where the digest lives, and what it does to versioning

`Task.image` is inside `Orkflow`, and `Orkflow` is the immutable version
document, so **the version document is the pin** — exactly what AGENTS.md §7.2
requires of a run.

This forces two deliberate changes, because the obvious formulation does not
survive contact with the code or with the protobuf runtime.

**(a) The canonical form is OrkStack's own, not `Deterministic:true`.**
The upstream doc comment for `proto.MarshalOptions.Deterministic` disqualifies
it for exactly this use, verbatim: *"It is not guaranteed to remain stable over
time… Users who need canonical serialization (e.g., persistent storage in a
canonical form, fingerprinting, etc.) must define their own canonicalization
specification and implement their own serializer rather than relying on this
API."* A definition digest stamped into an immutable document and compared
across engine releases is precisely that use. So OrkStack defines the
serializer:

> **`canonicalOrkflow(wf) []byte`** — JSON, UTF-8, no insignificant
> whitespace, object keys sorted by their UTF-8 byte sequence, integers
> rendered without exponent or fraction, and **only** the fields enumerated
> below, emitted whether or not they are zero. Fields not enumerated are not
> serialized, so adding a new proto field cannot silently change an existing
> digest.
>
> `Orkflow`: `namespace`, `name`, `version`, `description`, `labels`, `inputs`,
> `tasks`. **Not** `source_yaml`, **not** `created_at`.
> `Task`: `id`, `uses`, `image`, `with`, `needs`, `condition`,
> `timeout_seconds`, `retry`, `pool`, `command`, `env`, `secrets`, `resources`,
> `network`, `cancellation_grace_period_seconds`.
> `ImageRef`: `reference`, `registry`, `repository`, `index_digest`, `pinned`,
> `media_type`, `platforms`. (`resolved_at` no longer exists; `platform_digest`
> and `platform` are always empty in a definition and are not serialized.)
>
> `orkflow_definition_digest = "sha256:" + hex(sha256(canonicalOrkflow(wf)))`

This lives beside `internal/dsl/hash.go` with its own golden-vector test, so a
digest change is a test failure rather than a silent incompatibility.

**(b) The derived version is assigned after resolution, and covers the
resolution.** Today `internal/dsl/compile.go:190-192` sets
`wf.Version = sourceHash(source)` inside `Compile`, which runs *before* image
resolution. A moved tag would therefore mint the *same* version string for a
*different* definition, and `pkg/artifacts/orkflow_store.go:676-683` would
refuse the apply as an attempt to rewrite an immutable version — the author
would get a refusal, not a new version. So for a document that pins no
`version:`, the derivation moves to `ApplyOrkflow`, after resolution, and
becomes:

> `derivedVersion = hex(sha256(sourceYAML ‖ 0x1F ‖ canonicalImages(wf)))`
> where `canonicalImages` is the canonical JSON of `[{taskId, image}]` in task
> declaration order.

This deliberately does **not** re-open the question `internal/dsl/hash.go:8-27`
already settled. That comment records that hashing *normalized semantic YAML*
was tried and reverted, because a comment-only edit then compiled to the stored
version with different source and could not be applied at all — *"Two notions
of 'the same document' in one system is one too many."* The formulation above
keeps that ruling intact: the source bytes are still hashed in full, so a
comment-only edit still mints a new version, exactly as today. The only
addition is the resolution, which is not a second notion of the document but a
second *input to what runs*.

**Why `ImageRef.resolved_at` had to go.** `pkg/artifacts.sameStoredOrkflow`
(`pkg/artifacts/orkflow_store.go:400-408`) compares `source_yaml` and then
after clearing **only** `CreatedAt`. A per-apply timestamp inside `Task.image`
would make every re-apply of an unchanged document unequal, and the store would
refuse it with `ErrVersionExists`. Removing the field (§2.2) makes the no-op
case work with `sameStoredOrkflow` exactly as it is written today — no change
to that function, and no second clearing rule to keep in sync with the digest's.

Consequences, all correct and all intended:

- Same YAML, moved tag, no explicit `version:` → a **new** version, because a
  different thing now runs and the derivation now sees it.
- Same YAML, moved tag, explicit `version:` → an **`ALREADY_EXISTS` conflict**,
  because an immutable version cannot mean two definitions.
- Same YAML, same tag digest → byte-identical definition, `sameStoredOrkflow`
  is true, apply is a no-op.
- Comment-only edit, unpinned version → a new version, unchanged from today.

### 4.5 Registry credentials at the engine

`ork` gains one narrowly scoped credential surface: `ORK_REGISTRY_AUTH_FILE`, a
Docker `config.json`, used **only** for manifest HEADs. It grants no push and no
execution. It does not weaken AGENTS.md §2.3, which makes the Orker the boundary
for *execution* credentials; those stay on the Orker and are never sent to a
container.

### 4.6 Delivery model

| Source | How the image arrives | Digest |
|---|---|---|
| **Builtin** (`shell`, `http`, `file`) | Pulled from the catalog reference like any other image. Built locally by `make images`, which builds **and pushes** to a registry. | Real registry index digest |
| **Custom / community** | Pull from the registry named in `uses:`. This is the default and needs no configuration. | Real registry index digest |
| **User-provided source** (Dockerfile or git context) | `orkctl activity build <path\|git-url> --tag <ref> --push` builds and pushes, printing the pinned reference to paste into `uses:`. An explicit, out-of-band step. | Real registry index digest |
| **Embedded fallback** (builtins only) | An **OCI image layout** shipped with the Orker at `ORK_ORKER_IMAGE_LAYOUT` (default `/var/lib/orkstack/images`). Used only when a pull of an already-pinned digest fails. | Real registry index digest, carried inside the layout |

There is no *unpushed* local image, because a locally built, never-pushed image
has **no registry manifest digest** — `--iidfile` and `--quiet` give a local
config ID, which is not a manifest digest and does not transfer to a second
Orker. Making every build path push is what keeps AGENTS.md §5's immutable
reference literally true everywhere, including `orkstack dev`.

**The embedded fallback is not an exception to that.** An OCI image layout
(`oci-layout` + `index.json` + `blobs/sha256/…`) is content-addressed by
construction: the manifest digest *is* the blob name. So the Orker can serve a
pinned digest out of the layout and still satisfy §5, because it verifies the
same string it would have verified after a pull:

```
Runtime.Pull(pinned) fails (network, registry down, rate limit)
  → look up image.index_digest in <layout>/index.json
  → miss  → InfrastructureError/IMAGE_PULL_FAILED   (retryable, as before)
  → hit   → load the blob, verify sha256(blob) == index_digest
            mismatch → ContractViolation/IMAGE_DIGEST_MISMATCH
            match    → run it, and record source="embedded" in the commit
```

The layout is a cache with a proof, not a second source of truth: an image can
only be served from it if its digest was already resolved from a registry at
apply time. It exists so an air-gapped or rate-limited Orker can still run the
builtins, and it is populated by `make images` alongside the push.

The local stack therefore gains a registry:

```yaml
# deploy/docker-compose.yml
registry:
  image: registry:3
  ports: ["5001:5000"]

# and the floating tags get pinned, because the commit protocol depends on
# behavior that changed inside these images:
#   minio/minio:RELEASE.2025-09-07T16-13-09Z   conditional PutObject exists
#                                              since RELEASE.2023-02-17T17-52-43Z
#   temporalio/temporal:<pinned>
```

and `make images` builds each first-party activity, pushes it to
`localhost:5001/orkstack/activity-<name>:dev`, and exports the same manifests
into the Orker's OCI layout. `orkstack dev` points `ORK_ACTIVITY_CATALOG` at
that registry. A developer with no network still gets real digests.

Two operational notes carried from the research: Docker Hub anonymous pulls are
limited to 100 per 6 h per IPv4 or IPv6 `/64`, so a busy pool SHOULD sit behind
a pull-through mirror; and a content digest proves *content* identity, not
*publisher* identity — signature verification is a separate, later concern and
is deliberately not designed here (AGENTS.md §5: "do not build that policy
system unless current work needs it").

---

## 5. Durable commit protocol

### 5.1 What a commit means and does not mean

A commit means: **this activity's outputs are durably stored, and the container
must not run again for this logical activity.** It does not mean the container's
external side effects happened exactly once. Activities are at-least-once
(AGENTS.md §4); the commit only removes duplicate *container execution after
OrkStack has durably committed a result*.

Only success commits. A non-zero exit, a contract violation, a timeout, and a
cancellation all write **no** commit, because they must be free to retry.

### 5.2 The key

```
metadata/commits/{namespace}/{run-id}/{task-id}/commit.json
```

- Under `metadata/`, beside `metadata/logs`, `metadata/runs`,
  `metadata/orkflows`, `metadata/namespaces`. Workload artifacts live under
  `orkflows/` (S3) or `objects/` (local), so a workload cannot collide with a
  commit key by naming an artifact `commit.json`.
- Deterministic and attempt-independent: `namespace`, `run_id` and `task_id` do
  not change across retries, and a run pins exactly one definition, so
  run+task determines everything the commit describes.
- Namespace and task id are **validated, never sanitized**: both are slugs
  (`^[a-z][a-z0-9-]*$`, ≤ 200 bytes) enforced at
  `internal/dsl/validate.go:22-55`, so neither can escape the prefix.
- **The run id is encoded, never parsed.** AGENTS.md §7 is explicit —
  *"Nothing may parse a run ID or derive meaning from its parts"* — and
  `internal/engine/router.go:99-104` documents run-id opacity as a design
  principle. Requiring the key to match `ork-{timestamp}-{sequence}` would
  break every run id the tests already inject (`"run-001"`, `"run-stream"`,
  `"ghost"`, … in `internal/api/service_test.go`). The run id becomes **one**
  key segment through `encodeArchiveSegment`
  (`pkg/artifacts/archive.go:283-317`), the same reversible encoder
  `metadata/runs/{encoded-run-id}.jsonl` already uses: a clean component is
  used verbatim, anything else is base64url-escaped with a marker, and
  decoding returns the exact original bytes. No new rule is invented.
- The orkflow name is deliberately absent: a run id is already unique, and one
  fewer segment is one fewer name to sanitize.

**Where the URI comes from.** A URI needs the artifact backend and endpoint —
`ORK_ARTIFACT_BACKEND`, `ORK_S3_ENDPOINT` — which is process configuration that
workflow code may not read (AGENTS.md §2.1). So the commit-store root is
decided **at submit**, in `internal/api`, and travels on `RunPlan`, which is
already workflow argument 4 and already replay-pinned. `internal/engine/plan.go`
states the pattern for exactly this case: the decision "is made once, by the
process that accepts the submission, and travels as an argument". `RunPlan`
gains two fields:

```go
type RunPlan struct {
    Namespace string            `json:"namespace"`
    Routes    map[string]string `json:"routes"`

    // CommitRoot is the store root every commit URI in this run is composed
    // against, e.g. "s3://orkstack-artifacts" or "file:///var/lib/ork".
    CommitRoot string `json:"commit_root"`
    // Limits are the effective WorkspaceLimits for this run, after Orker
    // policy defaults and caps were applied at submit.
    Limits WorkspaceLimits `json:"limits"`
}
```

The workflow then composes `commit_uri` as
`plan.CommitRoot + "/" + CommitKey(namespace, runID, taskID)`, which is a pure
function of workflow arguments and replays identically. The Orker MUST use the
value it was sent rather than re-deriving one, so there is exactly one
convention with one owner. `artifacts.CommitKey` is the single shared renderer,
in the spirit of `internal/task/pool.go` centralizing the pool→queue mapping.

### 5.3 The store

The commit record does **not** go through `artifacts.Store`. Two reasons, both
verified in §14.2:

- `Store` has no conditional write, so a `commit.json` written through it is
  silently overwritable — the opposite of an immutable record.
- The local backend is content-addressed
  (`objects/n-<b64(name)>/<sha256-of-content>`), so a retry that does not yet
  know the content cannot address the object by name. A `Stat` by logical name
  fails with `malformed physical object key`, and a second `Put` of the same
  name with different bytes creates a *second* object instead of being refused.

Instead, a new store for a new concern — the established repo pattern
(`LogStore`, `NamespaceStore`, `OrkflowStore`, `RunArchive` are each their own
store):

```go
// pkg/artifacts/commit_store.go
type CommitStore interface {
    // Create writes the commit if and only if no object exists at uri.
    // An existing object returns an error matching ErrPreconditionFailed —
    // a fact about the object, not a request to retry verbatim.
    Create(ctx context.Context, uri string, c *orkstackv1.ActivityCommit) (CommitRecord, error)
    // Get reads the commit at uri. Missing returns ErrNotFound.
    Get(ctx context.Context, uri string) (CommitRecord, error)
}

type CommitRecord struct {
    Commit *orkstackv1.ActivityCommit
    ETag   string
    SHA256 string  // of the exact stored bytes
    Bytes  int64
}
```

- **S3 implementation**: `PutObject` with `IfNoneMatch: "*"`, mapping `412
  PreconditionFailed` to `ErrPreconditionFailed` and retrying `409
  ConditionalRequestConflict` a bounded number of times. This is the exact
  mechanism already used for immutable orkflow versions
  (`pkg/artifacts/orkflow_store.go:626-667`) and namespace registration
  (`pkg/artifacts/namespace_store.go:292-316`), and it is verified working
  against the pinned local MinIO in §14.1. The write MUST be a single
  `PutObject` and MUST NOT engage the multipart manager — which the repo
  already satisfies by construction, since no code path anywhere uses
  `feature/s3/manager`.
- **Local implementation**: write to `<dir>/.tmp/commit-<random>`, `fsync`,
  `close`, then **`link(2)`** the temp file to the canonical path and `unlink`
  the temp. `O_CREATE|O_EXCL` **directly at the canonical path is wrong** and
  the earlier draft of this document specified it: a crash mid-write leaves a
  truncated `commit.json` that `O_EXCL` then makes permanently unreplaceable.
  Reproduced in §14.7. `rename(2)` is also wrong — POSIX says it *"atomically
  replaces"* an existing destination, so it is not create-if-absent, which the
  same probe confirms. `link(2)` is the primitive that is both: POSIX.1-2024
  requires it to *"atomically create a new hard link"* and to fail `[EEXIST]`
  when the destination exists, and *"if link() fails, no link shall be
  created"*. This is Git's loose-object publication verbatim
  (`object-file.c`: `else if (link(tmpfile, filename))` … `if (ret && ret !=
  EEXIST)`). Caveat worth encoding: `link(2)`'s BUGS section notes the return
  code can be wrong on NFS, so an `EEXIST` MUST be confirmed by reading the
  object — which the adoption path (§5.4 step 0) already does.

**The size cap is 4 MiB, not 64 KiB.** The earlier 64 KiB figure was justified
by "conditional multipart completion is not portable" — a real constraint, but
one the repo already satisfies without any cap, because every S3 write in the
tree is a raw single-shot `client.PutObject` and a single `PutObject` accepts
up to 5 GiB. Meanwhile 64 KiB is unreachable for a *legal* workload:
`artifact_max_count` is 256, and a commit carrying 256 `ArtifactRef`s with
8-character names serializes to **67,699 bytes**, crossing the cap at 248 refs
(measured with the generated code and the specified encoder, §14.7). A limit a
conforming workload cannot respect is a bug in the limit. 4 MiB leaves ~60×
headroom over the worst legal case and is still ~1,250× below the single-PUT
ceiling.

**Conditional `PutObject` is a deployment requirement, not a property of
"S3-compatible".** AGENTS.md §2.2 says S3-compatible object storage; atomic
`If-None-Match: *` is narrower than that. AWS S3, MinIO (≥
`RELEASE.2023-02-17T17-52-43Z`), Cloudflare R2, LocalStack and Ceph RGW
(unversioned simple PUT) support it; **Google Cloud Storage's XML API,
Backblaze B2's S3 endpoint, and Garage do not.** The design therefore states
it plainly: a production OrkStack deployment requires an object store that
implements conditional `PutObject` with `If-None-Match: *`, returning 412 on
conflict, on non-multipart writes.

The Orker MUST verify this at boot with a **two-writer race**, not a sequential
`HEAD`-then-`PUT`: a sequential check proves ordering, not atomicity. Two
goroutines conditionally create the same probe key; exactly one must succeed
and the other must see `ErrPreconditionFailed`. Any other outcome is a boot
failure with `InfrastructureError/COMMIT_STORE_NOT_CONDITIONAL`, naming the
endpoint — refusing to start is the only honest response, because the
alternative is a deployment that silently loses the immutability guarantee.
`TestNamespaceStore_ConditionalWritesArbitrateBetweenTwoWriters` is the model.

### 5.4 The sequence

**Retry-start check** (before anything expensive, and before any container
starts):

```
0. GET commit_uri
   ├─ found, decodes, schema_version == 1, identity matches
   │        → adopt: return the committed result, commit_adopted=true,
   │          DO NOT run the container
   ├─ found, decodes, schema_version != 1     → ContractViolation /
   │        COMMIT_SCHEMA_UNSUPPORTED  (an Orker that cannot read a commit must
   │        neither overwrite it nor re-run past it)
   ├─ found, decodes, identity differs        → ContractViolation /
   │        COMMIT_IDENTITY_MISMATCH
   ├─ found, DOES NOT DECODE                  → see "the undecodable commit"
   ├─ not found                               → proceed
   └─ store unavailable                       → InfrastructureError /
            COMMIT_READ_FAILED (retryable — never proceed to execution blind)
```

**Identity is exactly these seven fields**, compared for byte equality:

```
schema_version            == 1
namespace                 == input.namespace
run_id                    == input.run_id
task_id                   == input.task_id
orkflow_definition_digest == input.orkflow_definition_digest
task_idempotency_key      == input.task_idempotency_key
image.index_digest        == input.image.index_digest
```

`image.platform_digest` and `image.platform` are **audit data recorded by the
winning attempt and are never compared.** The engine leaves both empty in
`ExecuteTaskInput` (§2.2), so comparing them would reject *every* adoption —
the input's is always empty and a stored commit's is always set. On a
mixed-architecture pool it would additionally reject two perfectly well-formed
attempts of the same activity for differing on a fact that is allowed to
differ. The index digest is the identity because it is what was pinned, what
was pulled, and what does not vary by host.

**The undecodable commit.** An object exists at `commit_uri` but is not valid
`ActivityCommit` JSON — a torn write from a pre-`link(2)` Orker, or corruption.
This is the one branch that is neither of the two obvious answers: treating it
as "no commit" would let a second container run after a possible durable
commit, and treating it as `COMMIT_IDENTITY_MISMATCH` would permanently wedge a
run on a transient read. The rule is **retry once, then terminal**: re-read the
object on the next attempt (a truncated read can succeed on a retry), and if it
still does not decode, fail with `ContractViolation/COMMIT_UNDECODABLE`, naming
the URI and its size. That is an operator-actionable terminal state — inspect
or delete one named object — and never a silent re-execution.

Adoption does not re-verify artifact bytes: they may have been expired by a
lifecycle rule, and a commit is a record of what happened, not a live guarantee
that every byte still exists.

**Success path**, in this exact order (AGENTS.md §4.1):

```
1. validate values.json           (exists, readable, object, UTF-8, ≤ limit)
2. scan + validate output/artifacts/  (regular files, readable, count/size limits)
3. hash every artifact and values.json (sha256, streaming)
4. bound the commit                (project ActivityCommit, assert ≤ commit_max_bytes)
5. upload artifacts               (content-addressed keys — see below)
6. upload values.json             (bytes verbatim)
7. upload stdout.log, stderr.log  (system artifacts)
8. CommitStore.Create(commit_uri) (conditional, create-if-absent)
9. return ExecuteTaskResult to Temporal
```

Steps 1–4 happen before any upload so a contract violation costs no bandwidth
and produces no orphans. Step 4 exists because the commit is the only thing a
conforming workload can overflow without breaking any workload-facing limit: it
projects the manifest, measures it, and fails with
`ContractViolation/COMMIT_TOO_LARGE` before uploading a byte, rather than
discovering the problem at step 8 where the only available code would be
retryable and would loop. Step 8 is the completion boundary; nothing after it
can change the outcome.

**Output objects MUST be immutable before step 8.** Today's S3 key is
`orkflows/{orkflow}/{run}/{task}/{name}` — five segments, **no attempt and no
content component** (`pkg/artifacts/s3_manifest.go:34-49`) — written with an
unconditional `PutObject` (`pkg/artifacts/s3.go:196-215`). Two attempts of one
task therefore write the same key, so a losing attempt can overwrite the exact
bytes a winning commit already hashed and referenced, turning a committed
`sha256` into a lie. The local backend is already immune because it is
content-addressed (`pkg/artifacts/local.go:121`).

The fix is to make S3 match: append the content digest as a sixth segment,
`orkflows/{orkflow}/{run}/{task}/{name}/{sha256}`. A key then names bytes, not
a slot, so a re-upload of identical content is idempotent, a re-upload of
different content cannot destroy the old, and step 5 becomes safely repeatable
across retries. The companion manifest that restores the exact logical name
(`pkg/artifacts/s3_manifest.go:144-187`) is unchanged.

**Concurrent committer at step 8.** A `412` means another Orker committed the
same logical activity while this one was working — possible whenever Temporal
delivers a retry to a second worker before the first finished. The response is:
`Get` the existing commit, apply the same identity checks as step 0, and return
**that** commit's result. The loser's uploaded artifacts are inert: with
content-addressed keys they are either byte-identical to the winner's (same
key, same bytes) or unreferenced by any commit, and in neither case can they
corrupt what the winner recorded. Being honest about the two-object boundary
matches the existing precedent at `pkg/artifacts/orkflow_store.go:497-500`,
which explicitly declines to claim a two-object transaction.

### 5.5 Crash coverage (AGENTS.md §11)

| Crash point | What survives | What happens next | Terminal state |
|---|---|---|---|
| Orker dies mid-container | nothing on that host | Heartbeat timeout (30 s) → Temporal retries. New Orker finds no commit, re-runs. | correct, container ran twice (at-least-once) |
| Orker dies after exit, before upload | nothing | Same as above. | correct, container ran twice |
| Orker dies mid artifact upload | some artifacts, no commit | Retry re-uploads. Orphans from the dead attempt are unreferenced. | correct |
| Orker dies after `commit.json`, before Temporal completion | the commit | Retry's step 0 finds it and **adopts** it: no second container run, result returned. | correct, container ran once |
| Orker restarts | image cache only (§6.4) | Temporal reschedules on any Orker in the pool. | correct |
| Object storage temporarily unavailable during upload | nothing committed | `InfrastructureError` → retried per policy. Storage failure can never look like success. | correct |
| Object storage unavailable at step 0 | — | `InfrastructureError/COMMIT_READ_FAILED`, retried. Execution never proceeds on an unknown commit state. | correct |
| Temporal temporarily unavailable | the commit, if written | Worker retries the completion; on adoption the result is returned again. | correct |
| Container exits 1 | logs + declared artifacts, no commit | `ActivityFailed` → retried per policy. | explicit failure |
| Exit 0, no `values.json` | logs, no commit | `ContractViolation/VALUES_MISSING` → never retried. | explicit failure |
| Malformed `values.json` | logs, no commit | `ContractViolation/VALUES_MALFORMED` → never retried. | explicit failure |
| Output too large | logs, no commit | `ContractViolation/VALUES_TOO_LARGE` or `ARTIFACT_LIMIT_EXCEEDED`. | explicit failure |
| A legal workload's manifest exceeds the commit cap | logs, no commit, **no uploads** | Step 4 measures before uploading → `ContractViolation/COMMIT_TOO_LARGE`. | explicit failure |
| Workload wrote outputs the Orker cannot read (`umask 077`) | logs, no commit | `ContractViolation/OUTPUT_NOT_READABLE`, naming file, mode and owner. Never a retry loop. | explicit failure |
| Torn `commit.json` from a crash mid-write | the torn object | `link(2)` publication makes it impossible to produce; an existing one is retried once then `ContractViolation/COMMIT_UNDECODABLE`. | explicit failure |
| A second attempt uploads different bytes for the same artifact name | both objects | Content-addressed keys: different bytes are a different key, so the committed `sha256` still resolves. | correct |
| Object store does not implement conditional PUT | — | Boot-time two-writer race fails → Orker refuses to start (`COMMIT_STORE_NOT_CONDITIONAL`). | refuses to run, never runs unsafely |
| Container exceeds its own timeout | logs, no commit | Orker kills it, `TimedOut/CONTAINER_TIMEOUT`. **Terminal by default** (§8.1). | explicit failure |
| Cancellation | logs, no commit | SIGTERM → grace → SIGKILL, Orker returns `ctx.Err()`. | canceled, never auto-retried |
| Container ignores SIGTERM | logs, no commit | SIGKILL after the grace period; verified in §14.5. | canceled |
| Input artifact unavailable | nothing | `InfrastructureError/INPUT_ARTIFACT_UNAVAILABLE` → retried. | explicit failure |
| Image pull fails | nothing | `InfrastructureError/IMAGE_PULL_FAILED` → retried. | explicit failure |
| Tag moved between two executions | — | Impossible: the run pins a digest at apply (§4). | correct |

There is no stuck, half-completed, unknown, or database-surgery state. Every row
ends in a correct durable state or an explicit, categorized, actionable failure.

---

## 6. Orker dispatch: the `OCIDispatcher`

### 6.1 Shape

`plugin.Registry` and `registry.Get(name)` disappear. One implementation runs
every activity:

```go
// internal/orker/dispatch.go
type OCIDispatcher struct {
    Runtime   ContainerRuntime   // the only Docker-aware seam in the repo
    Store     artifacts.Store    // artifacts + values + streams
    Commits   artifacts.CommitStore
    Logs      *LogSink
    Policy    SecurityPolicy     // caps, forbidden flags, defaults (§9)
    WorkRoot  string
    OrkerID   string
    BuildID   string
}

func (d *OCIDispatcher) Execute(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)
```

```go
// internal/orker/runtime.go — portability boundary, not a framework
type ContainerRuntime interface {
    Pull(ctx context.Context, pinned string) (PullInfo, error)   // PullInfo: platform digest, platform, cached bool
    Create(ctx context.Context, spec ContainerSpec) (ContainerID, error)
    Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error
    Signal(ctx context.Context, id ContainerID, sig Signal) error
    Wait(ctx context.Context, id ContainerID) (ExitStatus, error)
    Remove(ctx context.Context, id ContainerID) error
}
```

Exactly one implementation ships: `DockerRuntime`, shelling out to `docker` as
`pkg/plugins/container/docker.go:54-75` does today. The interface exists so
Podman, containerd, Kubernetes Jobs, ECS or Nomad remain *possible*, not because
any of them is planned. **No Docker-specific detail may appear outside
`internal/orker/runtime_docker.go`.**

### 6.2 What survives from today's Orker

| Component | Fate |
|---|---|
| `internal/orker/resolver.go:23-151` | Survives. Becomes the `/orkstack/input/artifacts/` stager; download logic and `safeComponent` unchanged. |
| `internal/orker/render.go:36-168` | Survives. Renders `with:` into `config.json`; `artifact` helper now returns a container path. |
| `internal/orker/logsink.go:54-521` | Survives unchanged. Keyed `(RunID, TaskID, Attempt)`, dense 1-based sequences, backpressure, truncation notice. |
| `pkg/plugin/linewriter.go` | Survives, moves to `pkg/logstream`. Framing is a UI contract (proof/M11.4). |
| `internal/orker/activity.go:252-268` | Survives. Per-attempt workdir plus `defer os.RemoveAll` on every exit path. |
| `internal/orker/activity.go:115-123` | **Load-bearing, must be preserved.** The log sink opens before anything else and closes before return, so every chunk is durable before Temporal records the terminal event. |
| `internal/orker/activity.go:155-171` | **Preserved in spirit.** A failed activity still publishes what it produced, and an upload error must never replace the workload's real error. |
| `pkg/plugins/*`, `pkg/plugin.Registry`, `BuiltinRegistry` | Deleted. |

### 6.3 Heartbeats and cancellation

This is the largest correctness gap in the current system and the part most
likely to be got wrong. Temporal delivers cancellation to an activity **only
through a heartbeat response**: an activity that never calls `RecordHeartbeat`
never observes `ctx.Done()`. Today the Orker heartbeats four fixed phase strings
and **none during the container run**, and no `HeartbeatTimeout` is set
anywhere, so cancellation cannot reach a running container.

Required, all three together:

1. **Engine** sets `HeartbeatTimeout: 30 * time.Second` and
   `WaitForCancellation: true` in `ActivityOptions`
   (`internal/engine/workflow.go:361-366`). Without the timeout the SDK
   throttles heartbeats to a 30 s default and a dead worker is only noticed at
   `StartToCloseTimeout`. Without `WaitForCancellation` the workflow may mark
   the task terminal before the Orker finishes killing and cleaning up.
2. **Orker** runs a heartbeat goroutine for the **entire** container lifetime,
   ticking every `HeartbeatTimeout/3` (10 s), carrying a small progress detail
   (`phase`, `container id`, `bytes logged`).
3. **Orker** reacts to `ctx.Done()` with the §3.3 sequence:

```
cancellation observed
  → Runtime.Signal(SIGTERM)
  → wait cancellation_grace_period_seconds (default 30)
  → Runtime.Signal(SIGKILL) if still running
  → Runtime.Remove on a DETACHED context (cleanup must survive cancellation)
  → close the log sink
  → return ctx.Err()
```

The Orker MUST return `ctx.Err()` (or `temporal.NewCanceledError`) only *after*
the server delivered cancellation; returning it spontaneously is an unexpected
cancel. Returning `nil` after cancellation would report success and MUST NOT
happen.

Timeouts, which must not be confused:

| Timeout | Owner | Value | Effect |
|---|---|---|---|
| `timeout_seconds` | Orker | task's `timeout:` | Kills the container (SIGTERM→grace→SIGKILL), returns `TimedOut/CONTAINER_TIMEOUT`. Fires first. |
| `HeartbeatTimeout` | Temporal | 30 s | Detects a dead Orker. Produces a Temporal `*TimeoutError` with `TIMEOUT_TYPE_HEARTBEAT`. |
| `StartToCloseTimeout` | Temporal | `timeout_seconds + grace + 120 s` | Backstop for an Orker wedged outside the container run. `TIMEOUT_TYPE_START_TO_CLOSE`. |

### 6.4 Orker state

The Orker stays stateless and disposable. Its only persistent local state is the
OCI image cache, per-attempt execution directories (removed on every exit path),
and an optional transient artifact download cache. If an Orker disappears,
Temporal retries elsewhere and the commit protocol decides whether the container
runs again.

### 6.5 Pool liveness

`ListPlugins` disappears, but it was never only a plugin list: a successful
`ListPlugins` probe is how a pool proves it is alive, and proof/M11.6 fixed a
bug in exactly that path. `DescribeOrker` replaces it one-for-one — same probe
call site, same 5 s and 10 s timeouts, same `MaximumAttempts: 2`, same
default-pool-failure→`Unavailable` mapping in `internal/api`. The Admin
"Plugins" page becomes an **Activities** page rendering the engine's activity
catalog (name → reference → digest), because that is engine configuration and no
longer something to ask an Orker.

---

## 7. Activity lifecycle

### 7.1 The sequence

```
 1. OPEN LOG SINK                     first, always; deferred Close before return
 2. HEARTBEAT LOOP START              10 s tick, runs until step 15
 3. COMMIT CHECK      GET commit_uri  → adopt and jump to 15, or proceed  (§5.4)
 4. WORKSPACE         mkdir per-attempt 0700; input/ 0555, output/ 1777, tmp/ 1777
 5. INPUT STAGE       download upstream artifacts   → /orkstack/input/artifacts/
                      download upstream values.json → /orkstack/input/upstream/
                      resolve secrets               → /orkstack/input/secrets/
 6. RENDER            with: → config.json ; run_input → run.json ; task.json
 7. IMAGE             Runtime.Pull(image.pinned); record platform digest
 8. CREATE            build ContainerSpec from the security profile (§9)
 9. RUN               Start; stream stdout/stderr to the sink and to disk
10. WAIT              Wait for exit, cancellation, or the task timeout
                      cancellation → SIGTERM → grace → SIGKILL  (§6.3)
11. EXIT CAPTURE      exit code recorded; non-zero → classify and go to 14
12. VALIDATE          values.json + output/artifacts/, INCLUDING readability
                      (§3.1, §3.4, §3.5), then bound the projected commit
                      (§5.4 step 4) before any byte is uploaded
13. UPLOAD + COMMIT   hash → artifacts → values.json → streams → commit.json (§5.4)
14. FAILURE PATH      upload streams and produced artifacts, attach refs to the
                      error, write NO commit, return the workload's error —
                      never a storage error in its place
15. CLEANUP           Remove container (detached ctx), RemoveAll workdir,
                      stop heartbeat, close log sink, return
```

Step 14's ordering is not cosmetic. Today's comment is explicit that replacing
the plugin's error with a storage error "would tell the author their build
failed because of the bucket" (`internal/orker/activity.go:155-171`, proven by
proof/M11.3). Under OCI the same rule holds: a workload's exit-1 is reported as
exit-1 even if the stderr upload also failed.

### 7.2 Data flow, end to end

```
ENGINE (deterministic workflow)
  Orkflow version doc ──> Task{uses, image(pinned), with, needs, retry, pool, …}
     │
     ├─ upstreamFor(id) → []UpstreamRef   (commit_uri, values ref, artifact refs)
     ├─ commit_uri      = plan.CommitRoot + "/" + CommitKey(ns, run, task)
     ├─ idempotency key = "{ns}/{run}/{task}"
     └─ ExecuteTaskInput  ── ≤ 256 KiB, checked ──> Temporal ──> pool queue

ORKER
  ExecuteTaskInput
     ├─ CommitStore.Get(commit_uri) ─── hit ──> ExecuteTaskResult (adopted)
     ├─ Store.Get(upstream artifacts)  ──> /orkstack/input/artifacts/<task>/<file>
     ├─ Store.Get(upstream values)     ──> /orkstack/input/upstream/<task>/values.json
     ├─ render(with)                   ──> /orkstack/input/config.json
     ├─ compose(input)                 ──> /orkstack/input/task.json
     └─ Runtime.Pull(pinned) ──> Runtime.Start(spec)

CONTAINER  (no credentials, no network unless asked, read-only rootfs)
  reads  /orkstack/input/**            writes /orkstack/output/values.json
  writes /orkstack/output/artifacts/*  writes stdout, stderr
  exits  0 = computed, non-zero = failed

ORKER (post-run)
  validate ──> hash ──> Store.Put(artifacts) ──> Store.Put(values.json)
           ──> Store.Put(stdout.log, stderr.log)
           ──> CommitStore.Create(commit_uri)  [conditional]
           ──> ExecuteTaskResult{exit_code, commit, artifacts, values_ref, …}

ENGINE
  TaskRun{status, output(inline ≤4 KiB), artifacts, error{category}}
  events ──> EventBus ──> archive + websocket
```

---

## 8. Failure categories and error handling

### 8.1 The five categories

The category names are also the exact Temporal `ApplicationError` type strings,
because `RetryPolicy.NonRetryableErrorTypes` matches the type string literally,
not with `errors.Is`.

| Category | Who produces it | Retry default |
|---|---|---|
| `ActivityFailed` | Orker: the container exited non-zero for its own reasons | per policy |
| `InfrastructureError` | Orker: storage, registry, runtime, or workspace failed | retry |
| `ContractViolation` | Orker: the image or the definition broke the ABI | **never** |
| `TimedOut` | Orker (container exceeded `timeout_seconds`) **or** Temporal (`*TimeoutError`) | **never, by default** — see below |
| `Cancelled` | Temporal, delivered through a heartbeat; the Orker returns `ctx.Err()` | never automatically |

`RetryPolicy.NonRetryableErrorTypes = ["ContractViolation", "TimedOut"]`. The
constructor's non-retryable flag always wins, so `ContractViolation` is also
constructed with `temporal.NewNonRetryableApplicationError`; the list is belt
and braces and is what makes the intent visible in the workflow.

**`TimedOut` is terminal by default, and this deviates from AGENTS.md §4.**
AGENTS.md §4's suggested-default table says `TimedOut → retry according to
policy`. Acceptance criterion 7 for this design says the opposite in as many
words: *"distinguishes retryable (infrastructure, transient) from permanent
(contract violation, **timeout**)"*. The two cannot both be honored, and the
acceptance criteria are this work's literal contract, so the criterion controls
and `TimedOut` is permanent by default. It is recorded here, in §13, and in
§17 rather than resolved quietly, because it is exactly the kind of ruling only
a user should make: **flipping it is a one-line change** — remove `"TimedOut"`
from `NonRetryableErrorTypes` and change one row in §8.2.

The behavior is overridable per task either way: a task whose `retry:` policy
explicitly lists retryable categories gets what it asked for. The default
chosen here is also the louder of the two — a workload that times out
repeatedly fails once and visibly, rather than burning its full retry budget on
the same wall clock.

`Cancelled` is never an `ApplicationError`. `TimedOut` is an `ApplicationError`
**only** when the Orker killed the container itself; a Temporal-produced timeout
arrives as `*TimeoutError` and the engine maps
`TIMEOUT_TYPE_{START_TO_CLOSE,HEARTBEAT}` to `TimedOut` on the reading side.
A Temporal `TIMEOUT_TYPE_HEARTBEAT` is the one timeout that stays **retryable**
regardless: it means the Orker died, which is an infrastructure fact about the
host and not a statement about the workload, and Temporal's own retry is what
moves the task to a live Orker.

### 8.2 The exhaustive table

| Reason code | Category | Retryable | Trigger |
|---|---|---|---|
| `CONTAINER_EXIT_NONZERO` | ActivityFailed | yes | any non-zero exit not otherwise classified |
| `CONTAINER_NOT_EXECUTABLE` | ContractViolation | no | exit 126 — the image cannot run the requested command |
| `CONTAINER_COMMAND_NOT_FOUND` | ContractViolation | no | exit 127 |
| `CONTAINER_TIMEOUT` | TimedOut | **no** (see §8.1) | Orker killed the container at `timeout_seconds` |
| `RUNTIME_REFUSED` | InfrastructureError | yes | exit 125 — the daemon refused to start the container |
| `WORKSPACE_NOT_BINDABLE` | InfrastructureError | no | daemon cannot bind the workspace path (`bind source path does not exist`) — a host misconfiguration retrying will not fix |
| `IMAGE_PULL_FAILED` | InfrastructureError | yes | pull failed or timed out |
| `IMAGE_DIGEST_MISMATCH` | ContractViolation | no | pulled content does not match `image.pinned` |
| `IMAGE_NOT_PINNED` | ContractViolation | no | `ImageRef.pinned` empty or not `…@sha256:…` |
| `VALUES_MISSING` | ContractViolation | no | exit 0 with no `/orkstack/output/values.json` |
| `VALUES_MALFORMED` | ContractViolation | no | not valid UTF-8 JSON, or not a top-level object |
| `VALUES_TOO_LARGE` | ContractViolation | no | over `limits.valuesMaxBytes` |
| `ARTIFACT_NOT_A_FILE` | ContractViolation | no | a directory, symlink, device, socket or FIFO in `output/artifacts/` |
| `ARTIFACT_LIMIT_EXCEEDED` | ContractViolation | no | too many artifacts, or one too large |
| `ARTIFACT_UPLOAD_FAILED` | InfrastructureError | yes | store `Put` failed |
| `INPUT_ARTIFACT_UNAVAILABLE` | InfrastructureError | yes | an upstream artifact could not be fetched |
| `INPUT_ARTIFACT_CORRUPT` | ContractViolation | no | fetched bytes do not match the recorded sha256 |
| `INPUT_TOO_LARGE` | ContractViolation | no | `ExecuteTaskInput` would exceed 256 KiB (raised by the engine) |
| `RENDER_FAILED` | ContractViolation | no | template error, missing key, or `<no value>` |
| `WORKDIR_FAILED` | InfrastructureError | yes | per-attempt directory could not be created |
| `SECRET_UNRESOLVED` | InfrastructureError | yes | a `SecretRef.source` could not be resolved |
| `SECRET_SCHEME_FORBIDDEN` | ContractViolation | no | Orker policy does not allow that source scheme |
| `COMMIT_READ_FAILED` | InfrastructureError | yes | commit store unreadable at step 0 |
| `COMMIT_WRITE_FAILED` | InfrastructureError | yes | conditional PUT failed for a non-precondition reason |
| `COMMIT_SCHEMA_UNSUPPORTED` | ContractViolation | no | existing commit has an unknown `schema_version` |
| `COMMIT_IDENTITY_MISMATCH` | ContractViolation | no | existing commit differs on one of the seven identity fields (§5.4) |
| `COMMIT_UNDECODABLE` | ContractViolation | no | an object exists at `commit_uri` but is not valid `ActivityCommit` JSON, on the second consecutive attempt (§5.4) |
| `COMMIT_TOO_LARGE` | ContractViolation | no | the projected `ActivityCommit` exceeds `limits.commitMaxBytes`, measured at step 4 before any upload |
| `COMMIT_STORE_NOT_CONDITIONAL` | InfrastructureError | no | the boot-time two-writer race proved the object store does not implement conditional create; the Orker refuses to start (§5.3) |
| `OUTPUT_NOT_READABLE` | ContractViolation | no | the Orker cannot open a file the workload wrote; the message names path, mode and owner, and the fix is `umask 022` (§3.1) |
| `RESOURCE_REQUEST_REFUSED` | ContractViolation | no | request exceeds the Orker's policy cap |
| `NETWORK_MODE_REFUSED` | ContractViolation | no | requested mode is not `default` or `none` |

Note `RUNTIME_REFUSED` versus `WORKSPACE_NOT_BINDABLE`: today's container plugin
already special-cases the bind-source message inside exit 125 and treats it as
permanent, and that distinction is preserved.

### 8.3 Engine side

The engine stops flattening everything into `TASK_FAILED` with a stringified
error (`internal/engine/workflow.go:384-402`). It reads the
`ApplicationError.Type()` and details and populates
`Error{code: <reason>, message, details, retryable, category}`. The event type
stays `TASK_FAILED`; the category is data on the error, so the UI can say
"contract violation — this image did not write values.json" instead of a Go
error string. Cancellation keeps its existing special case
(`temporal.IsCanceledError` → `STATUS_CANCELED`, projected on a disconnected
context).

---

## 9. Security

### 9.1 Forbidden, without exception

An Orkflow cannot express any of these, and the Orker refuses to construct them.
No configuration key exists for any of them, which is a stronger guarantee than
validation.

```
--privileged
--pid=host          (any host namespace: pid, ipc, uts, net)
--network=host      (and any mode other than default | none)
--userns=host
--cap-add           (of any capability)
--device            (any device passthrough)
--security-opt seccomp=unconfined
--security-opt apparmor=unconfined
--security-opt label=disable
--ipc=host
arbitrary host bind mounts       (mount sources are Orker-created paths only)
/var/run/docker.sock             (as a source OR as a target)
any mount of the Orker's own config, credentials, or workspace root
--volumes-from
--cgroup-parent, --cgroupns=host
--sysctl
--restart
--uts=host
```

The `/var/run/docker.sock` prohibition is listed as a **target** rule as well as
a source rule, because today's target validation accepts any clean absolute
path; nothing may mount anything at a container path that would shadow a runtime
socket.

### 9.2 Enforced on every container

```
--read-only                                   root filesystem
--cap-drop=ALL
--security-opt no-new-privileges
--security-opt seccomp=builtin                default profile, named explicitly
--pids-limit <policy>                         default 512
--memory <request or policy cap>              always set; never unlimited
--cpus <request or policy cap>                always set; never unlimited
--network none|default                        always explicit, never implicit
--init                                        PID 1 reaps zombies, forwards signals
--workdir /orkstack
--label ork.run=<run-id> --label ork.task=<task-id> --label ork.attempt=<n>
--mount type=bind,source=<attempt>/input,destination=/orkstack/input,readonly
--mount type=bind,source=<attempt>/secrets,destination=/orkstack/input/secrets,readonly
--mount type=bind,source=<attempt>/output,destination=/orkstack/output
--mount type=bind,source=<attempt>/tmp,destination=/orkstack/tmp
--tmpfs /tmp:rw,nosuid,nodev,size=<policy>
```

Rootless execution is used when the runtime supports it. Per-container user
namespace remapping is **not** implementable: `--userns=host` is the only valid
`docker run` value and it *reduces* isolation; remapping is a daemon-level
setting. Deployments wanting it configure the daemon.

The complete flag set above was executed and verified to compose (§14.3),
including `NoNewPrivs: 1` observed inside the container.

### 9.3 Resource policy

A task may request:

```yaml
resources:
  cpu: "2"
  memory: 4Gi
```

Orker policy (`ORK_ORKER_MAX_CPU`, `ORK_ORKER_MAX_MEMORY`,
`ORK_ORKER_DEFAULT_CPU`, `ORK_ORKER_DEFAULT_MEMORY`, `ORK_ORKER_PIDS_LIMIT`)
supplies defaults and caps. A request above the cap is **refused**
(`ContractViolation/RESOURCE_REQUEST_REFUSED`), not silently clamped: a workload
that asked for 64 GiB and got 4 GiB will fail confusingly later. **No workload
ever receives unlimited resources**, including one that requested nothing.

### 9.4 Secrets

Secrets reach a container as files under `/orkstack/input/secrets/`, resolved
from a `SecretRef.source` the Orker understands. Orker policy declares which
schemes exist; an unknown scheme is refused rather than ignored. Secret bytes
never appear in a proto message, Temporal history, the run archive, a log chunk,
or `task.json` — `task.json` carries only names and paths. The per-attempt
secrets directory is removed on every exit path, including panic and
cancellation.

---

## 10. First-party activities

First-party activities use **exactly** the public OCI contract. There is no
privileged internal plugin path, no shared Go SDK, and nothing an external image
cannot do. They are reference implementations.

### 10.1 The Dockerfile contract

```dockerfile
# images/activity-shell/Dockerfile
FROM alpine:3.20@sha256:…            # first-party images pin their own bases
RUN apk add --no-cache jq            # jq is the only "SDK": a JSON reader
COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh
USER 65532:65532                     # never root
ENTRYPOINT ["/usr/local/bin/entry.sh"]   # exec form, mandatory
```

Rules, all mandatory for first-party images and recommended for everyone:

1. **`ENTRYPOINT` in exec form.** Shell form runs the process as a child of a
   shell that does not forward signals, so SIGTERM never arrives and every
   cancellation degrades to SIGKILL.
2. **The entry script ends in `exec`** for the same reason, whenever it hands
   off to another program.
3. **`USER` is not root.**
4. **No `VOLUME`** — it would shadow the workspace mounts.
5. **The base image is digest-pinned** inside the Dockerfile.
6. **No writes outside `/orkstack/output`, `/orkstack/tmp`, `/tmp`** — the root
   filesystem is read-only at run time.
7. **The entry script sets `umask 022` before writing anything.** A first-party
   image inherits whatever umask the runtime gives it, and `USER 65532:65532`
   plus an inherited `umask 077` produces a `0600` `values.json` the Orker
   cannot read — a `ContractViolation/OUTPUT_NOT_READABLE` caused by the
   platform's own image (§3.1, measured in §14.8). Setting the umask
   explicitly is one line and removes the whole class.

### 10.2 The entry-script contract

```sh
#!/bin/sh
# images/activity-shell/entry.sh — the whole SDK is jq and a redirect.
set -eu
umask 022        # rule 7: outputs must be readable by the Orker (§3.1)
TASK=/orkstack/input/task.json
CONFIG=/orkstack/input/config.json
OUT=/orkstack/output
VALUES="$OUT/values.json"

op=$(jq -r '.op // "exec"' "$CONFIG")
cmd=$(jq -r '.command // empty' "$CONFIG")

case "$op" in
  exec)
    # The child's exit status IS the activity's exit status. No magic codes.
    sh -c "$cmd"
    rc=$?
    printf '{"exitCode":%d}\n' "$rc" > "$VALUES"
    exit $rc
    ;;
  eval)
    # `eval` is documented to treat a non-zero exit as DATA: the evaluation
    # ran, so the activity succeeded. This is one image's contract, not a
    # platform rule, and it is the only way to express it under exit-code
    # semantics without the platform inventing a magic code.
    value=$(sh -c "$cmd") || rc=$?
    rc=${rc:-0}
    jq -n --arg v "$value" --argjson rc "$rc" '{value:$v, exitCode:$rc}' > "$VALUES"
    exit 0
    ;;
  *)
    echo "activity-shell: unknown op '$op'" >&2
    exit 2
    ;;
esac
```

The three first-party images:

| Image | `op` values | `values.json` | Artifacts |
|---|---|---|---|
| `activity-shell` | `exec`, `eval`, `script` | `exitCode`; `eval` adds `value`; `script` adds `scriptPath` | files the script writes to `output/artifacts/` |
| `activity-http` | `get`, `post`, `put`, `patch`, `delete`, `head` | `status`, `headers`, `durationMs`, and `json` when the body parses and is small | the response body when `captureBodyAs` is set |
| `activity-file` | `copy`, `checksum`, `gzip`, `gunzip`, `tar`, `untar`, `concat` | `path`, `sizeBytes`, `sha256` | exactly one per op |

**There is no `activity-container` image, and there cannot be one.** Today's
container plugin works by reaching an ambient Docker daemon: it resolves a
`docker` CLI on `PATH` and connects through `DOCKER_HOST` or the default socket
(`pkg/plugins/container/docker.go:54-75`). Inside an activity container, §9.1
forbids every mechanism that could provide that — the Docker socket as a mount
source *or* target, any credential mount, `--privileged`, `--device`, and host
namespaces. No compliant implementation exists, and inventing a privileged
exception for one first-party image is exactly what AGENTS.md §9 forbids
("first-party activities must use exactly the same OCI contract… do not create
a privileged internal plugin system").

It is not needed either, because under this contract **every task already is a
container**. What `uses: container` meant is now spelled directly:

```yaml
# before (in-process container plugin)
- id: migrate
  plugin: container
  with: { op: run, image: ghcr.io/acme/migrate:v3, command: ["/bin/migrate"] }

# after (the task IS the container)
- id: migrate
  uses: ghcr.io/acme/migrate:v3
  command: ["/bin/migrate"]
```

The compiler accepts `uses: container` for one release as an alias that
rewrites to `uses: <with.image>` and lifts `with.command`/`with.env`, so an
existing document keeps compiling; anything else under `with:` that the old
plugin interpreted is a compile error naming the replacement.

**One capability genuinely goes away: a templated image.**
`examples/container-example.ork.yaml` selects its image with
`{{ .tasks.build.output.image }}`, which cannot be resolved to a digest at
apply time because the value does not exist until the run. AGENTS.md §5 forbids
running an unresolved reference, so a templated `uses:` is a compile error
(`ContractViolation/IMAGE_NOT_PINNED` at validation, not at run time). The
example is rewritten in Phase 3. This is a real, deliberate loss of expressive
power, listed in §13 and §17 rather than buried.

Two carried-over semantics are documented image contracts, not platform rules:

- **`activity-http` exits 0 on a non-2xx response** and reports `status` in
  `values.json`. An HTTP status is data; an activity failure is a failure to
  perform the request. This preserves today's behavior
  (`pkg/plugins/http/http.go:118-182`).
- **`activity-shell`'s `eval`** treats a non-zero child exit as data, as above.
  `exec` and `script` propagate it.

`activity-file`'s operation set changes from today's `write/read/copy/delete` to
AGENTS.md §9's `copy/checksum/gzip/gunzip/tar/untar/concat`. That is a
deliberate contract change. Its paths become workspace paths: reads resolve
under `/orkstack/input/`, writes land in `/orkstack/output/artifacts/`. The
shipped examples are rewritten accordingly, which also updates
`TestCompile_AcceptsEveryShippedExample`.

### 10.3 The contract test suite

```bash
orkstack activity test <image>
```

runs any image — first-party, community, or a user's — against the ABI, with no
Orkflow and no Temporal. It asserts: the container starts; `/orkstack/input` is
present and read-only; `task.json` is valid and complete against the §3.2
table; `values.json` is written, is an object, and is within the limit;
`output/artifacts/` holds only regular files; artifact names round-trip;
oversize outputs are rejected; SIGTERM is honored within the grace period; a
non-zero exit is reported as a non-zero exit; and the task timeout kills a
hung container.

Two assertions exist because probes found real failures they would have caught:

- **Output modes.** Every file the image writes must be readable by a user
  other than the one that wrote it. A dedicated fixture runs the image under
  `umask 077` and asserts the suite reports `OUTPUT_NOT_READABLE` rather than
  hanging, looping, or silently succeeding (§3.1, §14.8). This is a two-line
  falsification of the whole permission model and belongs in the suite that
  exists for exactly that.
- **Commit bound.** A fixture that emits `artifactMaxCount` artifacts must
  produce a commit within `commitMaxBytes`. The equivalent unit test — build a
  maximal legal `ActivityCommit`, assert it serializes under the cap — fails
  against the earlier 64 KiB figure at 248 refs (§14.7) and is the regression
  gate for §5.3's cap.

Fixtures in Bash, Go and Python ship with it; **the Bash fixture is mandatory
and is the design's own falsification test**. If the Bash fixture needs anything
beyond `cat`, `jq` and a redirect, this design is wrong (AGENTS.md §10).

---

## 11. Implementation phases

The complete proto lands in Phase 1 so Phases 2 and 3 never renegotiate the
wire. After Phase 1's freeze, the three phases touch disjoint packages and can
proceed in parallel across teams.

### 11.1 Phase 1 — the wire and the workspace

Owns `proto/`, `internal/task`, `internal/engine`, `internal/orker`.

The wire freeze is the point of this phase, so it includes every item that
would otherwise be a *second* wire change later. Four of them exist only
because a review round found them:

- `proto/orkstack/v1/activity.proto`; `Error.category`; `Task` reserving
  `plugin` and gaining `uses`/`image`/`env`/`secrets`/`resources`/`network`;
  `make proto`.
- **`run.proto`**: `TaskRun` reserving `plugin` and gaining `values_ref`,
  `commit`, `exit_code`, `system_artifacts`; `Run.orkflow_definition_digest`
  (§2.3). Without this Phase 2 has nowhere to record what it commits.
- **`ExecuteTaskResult.values_json` as `bytes`** (§2.2). Changing it from
  `Payload` after the freeze is a wire break, and `Payload` cannot carry the
  byte identity the field promises (§14.9).
- **`RunPlan.CommitRoot` and `RunPlan.Limits`** (§5.2). `RunPlan` is workflow
  argument 4; changing its shape after the fixture reset would force a *second*
  reset, which §11.4 calls a process failure.
- **`WorkspaceLimits.commit_max_bytes`** and the `COMMIT_*` / `OUTPUT_*` reason
  codes. Reason codes are `Error.code` strings and are cheap to add later, but
  the *categories* drive `NonRetryableErrorTypes` in `ActivityOptions`, which
  is replay-visible.
- `OCIDispatcher` + `ContainerRuntime` + `DockerRuntime`.
- `/orkstack` workspace construction, permissions, `task.json` (to the §3.2
  table), `config.json`, `run.json`, input staging, output scan, validation,
  and the readability check.
- Heartbeat loop, `HeartbeatTimeout`, `WaitForCancellation`, SIGTERM→grace→
  SIGKILL.
- The security profile of §9.2, and the refusals of §9.1.
- Delete `pkg/plugins/*` and `pkg/plugin.Registry`; move `linewriter` to
  `pkg/logstream`.
- **Exit gate: one replay-fixture reset** (§11.4), then `make lint test`.

### 11.2 Phase 2 — durability

Owns `pkg/artifacts`, and the failure-category read side in `internal/engine`.

- `artifacts.CommitStore`; S3 conditional implementation; local temp-file +
  `fsync` + **`link(2)`** implementation; `artifacts.CommitKey` built on
  `encodeArchiveSegment`; the boot-time two-writer conditional-write probe.
- **Content-addressed S3 output keys** (§5.4) — the sixth key segment that
  makes an uploaded artifact immutable before the commit references it.
- The §5.4 sequence, retry adoption, the seven-field identity check, the
  undecodable branch, the step-4 commit bound, and 412 handling.
- Durable `stdout.log` / `stderr.log` as system artifacts.
- Failure categories end to end into `Error.category`, the UI, and the archive.
- The §5.5 chaos matrix as integration tests under `-tags=integration`,
  including a kill between the temp write and the `link(2)`.

### 11.3 Phase 3 — images and first-party activities

Owns `internal/dsl`, `internal/api`, `images/`, `cmd/orkctl`, `ui/`, `examples/`.

- The activity catalog, `uses:` resolution, apply-time registry HEAD resolution,
  the canonical definition digest.
- `registry:3` in `deploy/docker-compose.yml`; `make images`;
  `orkctl activity build`.
- The three first-party images, their Dockerfiles and entry scripts (including
  `umask 022`).
- `orkstack activity test`, the Bash/Go/Python fixtures, and the umask and
  commit-bound assertions (§10.3).
- `DescribeOrker` replacing `ListPlugins`; the Admin Activities page.
- Rewritten `examples/*.ork.yaml`, including retiring the templated image in
  `container-example.ork.yaml` (§10.2).

### 11.4 The replay-fixture reset

Two guards protect `internal/engine/testdata/history/run-succeeded-v1.json`:
`TestRunWorkflow_ReplaysCommittedV1History` (command stream) and
`TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends` (payload bytes).
Any encoded field added to `ExecuteTaskInput` fails the second, and switching to
a top-level proto message flips the encoder from `encoding/json` to `protojson`,
which fails it as well. `RunWorkflow` is registered `VersioningBehaviorPinned`
precisely because it is replay-sensitive.

Therefore: **one reset, at the end of Phase 1, after the entire new wire shape
has landed** — the same discipline `internal/task/contract.go:55-63` records for
the M10.1 addition. Two resets is a process failure, not an accident.

### 11.5 The name collision

"Idempotency key" already means the *run submission* key claimed through
`IdempotencyClaimWorkflow`. AGENTS.md §3's activity-level key is a different
thing with a different lifetime. Spellings, enforced everywhere:

| Concept | Wire | Go | User-facing |
|---|---|---|---|
| Run submission dedupe | `run_idempotency_key` | `RunIdempotencyKey` | `orkctl run submit --idempotency-key` |
| Activity identity | `task_idempotency_key` | `TaskIdempotencyKey` | `idempotencyKey` in `task.json` (the ABI spelling AGENTS.md §3 fixes) |

---

## 12. Resolved design questions

Every open question the research raised, with its decision.

| # | Question | Decision |
|---|---|---|
| OQ-1 | Rendered vs raw `with:` on the wire | **Raw.** Rendering stays in the Orker; the rendered result is materialized to `/orkstack/input/config.json` (§3.3). |
| OQ-2 | Bounding upstream fan-in | `UpstreamRef` carries **references only**; the Orker fetches each ancestor's `values.json` into `/orkstack/input/upstream/<id>/`. Payload is O(ancestors) small records (§2.2). |
| OQ-3 | Does a structured output survive in the result | The committed object is authoritative. `ExecuteTaskResult.values_json` is the workload's exact **bytes**, inline only at ≤ 4 KiB, used only for the engine's read model. It is `bytes` and not `Payload` because a `Struct` round trip reorders keys and corrupts int64 (§2.2, §14.9). |
| OQ-4 | Workspace permissions and container UID | Attempt dir 0700; `input` 0555/0444 read-only bind; `output` and `tmp` 1777. No forced `--user` by default, plus a readability check that fails terminally and an opt-in `runAsOrkerUser` (§3.1, §14.8). |
| OQ-5 | Input artifact naming | Both: derived `safeComponent` path **and** exact `name`/`nameB64` in `task.json` (§3.2). |
| OQ-6 | `values.json` size limit | 256 KiB on disk (`ContractViolation` above it); 4 KiB for the inline copy (§3.4). |
| OQ-7 | Separate durable stdout/stderr | Yes — two objects in `system_artifacts`; the merged live stream and its framing are unchanged (§3.6). |
| OQ-8 | Commit key and collisions | `metadata/commits/{ns}/{encoded-run-id}/{task}/commit.json`, under `metadata/`, where a workload artifact cannot reach. The run id is encoded through `encodeArchiveSegment`, never parsed (§5.2). |
| OQ-9 | `Store` methods vs a separate store | Separate `artifacts.CommitStore`; `Store` untouched (§5.3). |
| OQ-10 | 412 from an unknown committer | Read it, check `schema_version` and identity, adopt or fail `COMMIT_SCHEMA_UNSUPPORTED` / `COMMIT_IDENTITY_MISMATCH` (§5.4). |
| OQ-11 | Local-backend commit path | Temp file + `fsync` + **`link(2)`** to a **name-addressed** path. `O_EXCL` at the canonical path was the earlier answer and is wrong: it makes a torn write permanent (§5.3, §14.7). |
| OQ-12 | Index vs platform digest | Pin and pull the **index** digest; record the **platform** digest and platform in the result and the commit (§2.2, §4.1). |
| OQ-13 | Registry credentials at the engine | `ORK_REGISTRY_AUTH_FILE`, HEAD-only, no push, no execution (§4.5). |
| OQ-14 | Re-verify an already-pinned digest at apply | Yes. Apply must not succeed with an unpullable pin (§4.3). |
| OQ-15 | Where the digest lives | In `Task.image` inside the immutable version document; the definition digest is an OrkStack-defined canonical JSON hash — not `Deterministic:true`, which upstream disqualifies for fingerprinting — and the derived version moves post-resolution (§4.4). |
| OQ-16 | `ListPlugins` replacement | `DescribeOrker`, same probe call sites and timeouts; the catalog moves to the engine (§6.5). |
| OQ-17 | Exhaustive failure table | §8.2. |
| OQ-18 | `http` non-2xx is success | Preserved as the **image's** documented contract: exit 0, `status` in `values.json` (§10.2). |
| OQ-19 | `shell eval`'s non-zero exit as data | `eval` documents it; `exec` and `script` propagate the exit code (§10.2). |
| OQ-20 | Rewriting workdir-relative examples | Phase 3; paths become workspace paths and `activity-file` adopts AGENTS.md §9's op set (§10.2, §11). |
| OQ-21 | Immutable reference for a local build | Every build path pushes, including `make images`. The only non-registry path is an OCI image **layout** whose blob name *is* the manifest digest, used solely as a verified fallback for an already-pinned digest (§4.6). |
| OQ-22 | Idempotency-key spelling | `run_idempotency_key` vs `task_idempotency_key`; `idempotencyKey` only in `task.json` (§11.5). |
| OQ-23 | One fixture reset or two | **One**, at the end of Phase 1 (§11.4). |

---

## 13. Deliberate contract changes

Changes this design makes on purpose, each with its reason. None is a silent
reconciliation.

1. **`plugin:` → `uses:`.** AGENTS.md §§3, 9 speak of activities, not plugins,
   and an activity is an image. Field 2 is reserved, not reused, and the DSL
   rejects `plugin:` through the existing `internal/dsl/removed.go` mechanism.
2. **Definition digest becomes a canonical-IR hash.** Required so a moved tag
   changes the definition (§4.4).
3. **`activity-file`'s operations change** to AGENTS.md §9's set (§10.2).
4. **`ExecuteTaskInput`/`ExecuteTaskResult` become proto messages**, per
   AGENTS.md §§11.1, 12. This flips the Temporal payload encoder and forces the
   fixture reset (§11.4).
5. **Task output no longer travels in full.** `TaskRun.output` is populated from
   the inline copy when it exists and is otherwise empty with `values_ref` set.
   The UI reads the artifact for larger results.
6. **`ListPlugins` is removed** and `DescribeOrker` takes over pool liveness
   (§6.5).
7. **`activity-container` is retired as an image** and becomes a DSL alias for a
   direct image reference (§10.2). No compliant implementation of it exists
   under §9.1, and every task is already a container.
8. **A templated `uses:` is a compile error.** Dynamic image selection cannot be
   digest-pinned at apply time, which AGENTS.md §5 requires. This is a real
   loss of expressive power (§10.2, §17).
9. **`TimedOut` is non-retryable by default**, following acceptance criterion 7
   rather than AGENTS.md §4's suggested-default table (§8.1, §17).
10. **`ImageRef` has no `resolved_at`.** A per-apply timestamp inside the
    immutable version document breaks `sameStoredOrkflow`'s repeat detection
    (§4.4).
11. **The derived version is computed after image resolution**, over the source
    bytes *and* the resolved images, so a moved tag mints a new version instead
    of being refused (§4.4).
12. **A production deployment requires conditional `PutObject`**, which is
    narrower than "S3-compatible" and excludes GCS's XML API, Backblaze B2's S3
    endpoint, and Garage (§5.3, §17).
13. **S3 output keys become content-addressed.** A sixth key segment makes an
    uploaded artifact immutable, so a losing attempt cannot overwrite bytes a
    winning commit already hashed (§5.4).

---

## 14. Evidence

Every claim below was executed in this repository's environment. Everything else
in this document is design, or is cited to a file and line read at commit
`55b7367`.

### 14.1 Conditional create-if-absent works on the deployed object store

```
$ docker exec orkstack-minio-1 minio --version
minio version RELEASE.2025-09-07T16-13-09Z (commit-id=07c3a429bfed433e49018cb0f78a52145d4bedeb)

$ ORK_REQUIRE_INFRA=1 go test -tags=integration ./pkg/artifacts/ \
    -run TestNamespaceStore_ConditionalWritesArbitrateBetweenTwoWriters -count=1 -v
=== RUN   TestNamespaceStore_ConditionalWritesArbitrateBetweenTwoWriters
    MinIO health http://localhost:9000/minio/health/live -> 200
--- PASS (0.04s)
```

The primitive the commit protocol rests on — `IfNoneMatch: "*"` mapping to
`ErrPreconditionFailed` — is proven against the exact MinIO the local stack
runs. Note that this probe is **sequential** and therefore proves the mapping,
not atomicity; §5.3's boot-time check races two writers for that reason, and
`deploy/docker-compose.yml`'s floating `minio/minio:latest` must be pinned so
the conditional-write boundary cannot silently move underneath it (§4.6).

### 14.2 The local artifact store cannot hold the commit record

```
PUT  uri="file:///…/objects/n-Y29tbWl0Lmpzb24/afbf9d0f…"  sha="afbf9d0f…"
STAT by-name err=artifacts: local "file:///…/objects/commit.json":
       invalid artifact URI: malformed physical object key
LIST prefix=commit.json n=1
PUT2 uri="file:///…/objects/n-Y29tbWl0Lmpzb24/2b544279…"   ← a SECOND object
LIST after second put n=2
```

A retry cannot address the commit by name, and a second write of the same
logical name is not refused — it creates another object. This is why
`CommitStore` exists and why its local implementation is name-addressed
(§5.3). The *publication* primitive it uses is `link(2)`, not `O_EXCL` at the
canonical path — §14.7 shows why that distinction matters.

### 14.3 The workspace and the security profile compose and are enforced

```
$ docker run --rm --network=none --read-only --cap-drop=ALL \
    --security-opt no-new-privileges --pids-limit 256 --memory 256m --cpus 1 \
    --user 65534:65534 --init --workdir /orkstack \
    --mount type=bind,source=$W/input,destination=/orkstack/input,readonly \
    --mount type=bind,source=$W/output,destination=/orkstack/output \
    --mount type=bind,source=$W/tmp,destination=/orkstack/tmp \
    alpine:3.20 sh -c '…'
65534
{"task_id":"t1"}
WROTE-OUTPUT
sh: can't create /orkstack/input/nope: Read-only file system
rc=0
$ cat $W/output/values.json
{"ok":true}
```

An unprivileged, unknown UID read the read-only input, wrote `values.json` into
a `1777` output directory, and was refused a write to input. A separate run with
`--security-opt seccomp=builtin` added confirmed `NoNewPrivs: 1` inside the
container and `Read-only file system` for the container root.

The first attempt of this probe used a tmpfs at `/orkstack` and **failed**: the
tmpfs is root-owned and UID 65534 could not write it. That is why §3.1 mandates
explicit modes on Orker-created bind sources rather than relying on mount
defaults.

**This probe does not prove readability in general.** It ran through a
VM-shared bind mount, where ownership is remapped to the host user. §14.8 runs
the same workload on a native filesystem and gets `Permission denied`, which is
why §3.1 requires a readability check rather than trusting this result.

### 14.4 Index digest resolution

```
$ docker buildx imagetools inspect alpine:3.20
Name:      docker.io/library/alpine:3.20
MediaType: application/vnd.oci.image.index.v1+json
Digest:    sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
```

`docker image inspect --format '{{json .RepoDigests}}'` returned the same digest
*for this pull on this host*, which is exactly the problem: the value depends on
what the daemon negotiated. Resolution goes to the registry (§4.3), never to
daemon introspection.

### 14.5 The cancellation grace period is a real mechanism

```
container traps SIGTERM:   docker stop --timeout 30 → elapsed 0s, exit 0, logs "GOT-SIGTERM"
container ignores SIGTERM: docker stop --timeout  5 → elapsed 5s, exit 137
```

SIGTERM → grace → SIGKILL is exactly `docker stop --timeout <grace>`, and an
image that ignores SIGTERM is killed at the boundary with exit 137. What is
missing today is not this mechanism but the heartbeat that lets Temporal
cancellation reach the Orker at all (§6.3).

### 14.6 The proto in §2 compiles, lints, and does not break the wire

The three proto blocks in §2 were extracted from this document, applied to a
copy of `proto/` (new `activity.proto`, patched `common.proto`, patched
`orkflow.proto` with the added `orkstack/v1/activity.proto` import), and run
through the repo's own `buf.yaml`:

```
$ buf build                                        # (no output — compiles)
$ buf lint                                         # (no output — passes)
$ buf breaking --against <repo>                    # breaking: use: [FILE]
proto/orkstack/v1/orkflow.proto:21:1:Previously present field "2" with name
  "plugin" on message "Task" was deleted.
$ buf breaking --against <repo>                    # breaking: use: [WIRE_JSON]
                                                   # (no output — passes)
$ buf breaking --against <repo>                    # FILE except FIELD_NO_DELETE
                                                   # (no output — passes)
```

And the control, proving the complaint is pre-existing practice rather than a
regression this design introduces — the current tree checked against a baseline
where `Task.queue` still exists, under the repo's unmodified `buf.yaml`:

```
$ buf breaking --against /tmp/queuebase
proto/orkstack/v1/orkflow.proto:20:1:Previously present field "8" with name
  "queue" on message "Task" was deleted.
```

Identical shape, identical rule, already shipped. The design is wire-compatible
and JSON-compatible; only buf's strictest `FIELD_NO_DELETE` objects, and it
objects to `reserved` itself.

### 14.7 The commit primitives, measured against the specified encoder

A Go probe built against **the generated code for §2.2's own proto** (the
scratch tree of §14.6, `buf generate` → `protoc-gen-go`), so these are the real
messages this design specifies, not an approximation.

**Atomic publication.** Three primitives, one question — which one is *both*
atomic and create-if-absent:

```
link -> fresh path        err=<nil>, dst = {"winner":1}
link -> existing path     err=file exists, isEEXIST=true, dst UNCHANGED   ← correct
rename -> existing path   err=<nil>, dst now {"loser":2}                  ← REPLACES
O_EXCL at final path, torn write, then crash:
                          file = "{\"schemaVersion\":"
                          second O_EXCL -> isEEXIST=true                  ← torn PERMANENTLY
```

`link(2)` is the primitive; `rename(2)` silently replaces, and `O_EXCL` at the
canonical path — which an earlier draft of this document specified — makes a
torn commit permanently unreplaceable. §5.3 is written to the first row.

**Commit size at the documented limits.** A maximal *legal* commit: 256
`ArtifactRef`s (the `artifact_max_count` default), realistic S3 URIs, a full
`ImageRef`, all 18 other fields set, encoded with `protojson`:

```
256 refs, name=  8 chars ->  67,699 bytes   64KiB cap: EXCEEDED 1.03x   4MiB cap: ok
256 refs, name= 32 chars ->  79,987 bytes   64KiB cap: EXCEEDED 1.22x   4MiB cap: ok
256 refs, name= 64 chars ->  96,371 bytes   64KiB cap: EXCEEDED 1.47x   4MiB cap: ok
256 refs, name=128 chars -> 129,139 bytes   64KiB cap: EXCEEDED 1.97x   4MiB cap: ok
64KiB cap crossed at n=248 refs (8-char names)
```

A workload that respects every documented limit produces a commit the earlier
64 KiB cap forbids. Hence §5.3's 4 MiB, and hence step 4's pre-upload bound
with a terminal `COMMIT_TOO_LARGE`.

### 14.8 Output readability is host-dependent, and both outcomes reproduce

Same machine, same daemon, one variable: the mount type.

**Native filesystem** (Docker volume, no sharing layer), output dir `1777`,
workload as UID 65534:

```
umask 077 -> -rw-------  1 65534 65534  values.json
umask 022 -> -rw-r--r--  1 65534 65534  control.json

Orker as uid 1000:   values.json:  PERMISSION DENIED
                     control.json: READ-OK
                     stat /w/values.json -> mode=600 owner=65534:65534   ← detection works
```

**VM-shared bind mount** (`$HOME` on the same daemon), identical workload:

```
in container: -rw-------  1 65534 65534   values.json
on host:      -rw-------  1   501    20   values.json   ← ownership REMAPPED
host read:    {"ok":true}  READ OK
```

So the same image, the same umask and the same mode produce a failure on one
mount type and a success on the other. No mount configuration fixes this, which
is why §3.1 detects rather than assumes. The `stat` line matters: it succeeds
where `open` fails, so `OUTPUT_NOT_READABLE` can always name the mode and owner.

**The opt-in escape hatch works.** Forcing `--user 1000:1000` (the Orker's own
uid) with the same `umask 077`:

```
-rw-------  1 1000 1000  values.json     →  Orker read: {"ok":true}  READ-OK
```

The owner reads its own `0600` file whatever the umask. That is §3.1's
`runAsOrkerUser`, opt-in because it overrides the image's `USER`.

### 14.9 `Payload` cannot carry byte-identical JSON

`structpb.Struct` + `protojson` round trip, four inputs:

```
IN  {"n":1.00,"z":0}              OUT {"n":1,"z":0}                identical=false
IN  {"b":  true ,"a":1}           OUT {"a":1,"b":true}             identical=false  ← keys REORDERED
IN  {"big":12345678901234567890}  OUT {"big":12345678901234567000} identical=false  ← int64 CORRUPTED
IN  {"s":"caf\u00e9"}             OUT {"s":"café"}                 identical=false
```

The third row is the serious one: `Struct` values are `double`, so an integer
outside float64's exact range is silently changed, and a UI reading
`TaskRun.output` would display a wrong number with no indication. This is why
`ExecuteTaskResult.values_json` is `bytes` (§2.2) and why §3.4's byte-identity
promise is now true rather than aspirational.

### 14.10 `protojson` output is deliberately unstable across builds

Not reproduced locally — 200 marshals in one build gave one encoding, which is
consistent with the mechanism rather than contrary to it. The mechanism is
documented upstream: *"Do not depend on the output being stable. Its output
will change across different builds of your program, even when using the same
version of the protobuf module."* The implementation inserts random whitespace
on purpose (`internal/encoding/json/encode.go`: `if detrand.Bool() { e.out =
append(e.out, ' ') }`), added in commit `582ab3d` explicitly *"to deter users
from doing byte for byte comparison"*.

Consequence encoded in §2.2: `CommitRef.sha256` is computed from the bytes
written to or read from the store, never by re-marshalling a decoded
`ActivityCommit`.

### 14.11 Not verified

- That cancellation cannot currently reach a running container is an inference
  from Temporal's heartbeat-gated cancellation delivery plus the four discrete
  heartbeat call sites in `internal/orker/activity.go`. It was **not**
  reproduced end to end. §6.3 is written so the design is correct whether or not
  the current behavior is as inferred.
- The quadratic fan-in cost was reasoned from `transitiveNeeds` + `upstreamFor`,
  not measured.
- That `protojson` changes recorded bytes for a given payload was reasoned from
  `internal/engine/plan.go:23-26`, not executed. §14.10 documents the upstream
  mechanism; the cross-build variation itself was not reproduced.
- The S3 output-key overwrite race (§5.4) was confirmed by **reading**
  `pkg/artifacts/s3_manifest.go:34-49` and `pkg/artifacts/s3.go:196-215`, not by
  executing two competing uploads.
- The conditional-`PutObject` backend survey (§5.3) rests on vendor
  documentation and third-party reports; only MinIO was exercised (§14.1).
- `proto.MarshalOptions{Deterministic:true}` instability across schema changes
  (§4.4) is quoted from the upstream doc comment, not executed.

---

## 15. Non-goals

This design does not add, and Phases 1–3 must not add: a visual designer, a
plugin marketplace, an expression language, an embedded scripting runtime, a
custom scheduler or event bus or state machine, an OrkStack-owned execution
database, a Kubernetes operator, image signature policy, named network profiles,
a second container runtime implementation, or a required workload SDK in any
language. The activity ABI is a directory of files and an exit code, and it stays
that way.

---

## 16. Where each required decision is specified

| Required decision | Section |
|---|---|
| `ExecuteTaskInput` with image and idempotency key | §2.2 (fields 8, 10) |
| `ExecuteTaskResult` with exit code and commit reference | §2.2 (fields 2, 3) |
| `/orkstack` layout: directories, files, JSON schemas, permissions | §3.1–§3.6 |
| Durable commit: pre-commit state, upload, `commit.json`, Temporal notify, retry detection | §5.1–§5.5 |
| Image resolution: apply → digest, Orker → pull and verify | §4.1–§4.5 |
| `OCIDispatcher` replacing `plugin.Registry` | §6.1–§6.5 |
| Activity lifecycle, sequenced | §7.1, §7.2 |
| Retryable vs permanent, mapped to AGENTS.md §4 categories | §8.1–§8.3 |
| Forbidden flags and enforced constraints | §9.1–§9.4 |
| First-party Dockerfile and entry-script contract | §10.1–§10.3 |
| Image delivery: registry, pull-by-default, local build | §4.6 |
| Data ownership: engine / Orker / container / object store | §1 |
| Phase 1–3 independence | §11 |
| Every research open question, resolved | §12 |
| Deliberate contract changes, listed rather than buried | §13 |
| Conflicts a user must rule on | §17 |

---

## 17. Conflicts requiring a user ruling

Five places where the launch contract, AGENTS.md, and the code cannot all be
satisfied at once. Each is implemented here under the narrowest reading
consistent with the acceptance criteria, and each is listed so a user can
overrule it cheaply. None was resolved silently.

| # | Conflict | Implemented as | Cost to flip |
|---|---|---|---|
| **C1** | Acceptance criterion 7 groups timeout with *permanent* failures; AGENTS.md §4 says `TimedOut` → "retry according to policy". | The criterion controls: `TimedOut` is non-retryable by default (§8.1). | One line: drop `"TimedOut"` from `NonRetryableErrorTypes`, flip one row in §8.2. |
| **C2** | Objective deliverable 10 names a "fallback to embedded" image path; the previous draft said there is none. | An **OCI image layout** fallback, verified against the pinned digest (§4.6). The deliverable is met without weakening AGENTS.md §5. | None — this is believed to satisfy both. Flagged because the earlier draft contradicted the deliverable outright. |
| **C3** | "Proto contract is forward-compatible (no breaking changes to existing code)" vs. retiring `Task.plugin` in a system the same objective says needs no compatibility shims. | Field numbers reserved, never reused (§2.1). Wire- and JSON-compatible; only buf's `FIELD_NO_DELETE` objects, and it objects to the `queue` removal the repo already shipped (§14.6). | Keep `plugin = 2` in place with new semantics: no `reserved`, but the word survives in the contract. |
| **C4** | AGENTS.md §2.2 says "S3-compatible object storage"; the commit protocol needs atomic `If-None-Match: *`, which GCS's XML API, Backblaze B2's S3 endpoint and Garage do not provide. | Stated as an explicit deployment requirement with a boot-time two-writer race (§5.3). | Add a per-backend commit strategy — substantially more design, and a second durability mechanism to prove. |
| **C5** | AGENTS.md §9 forbids a privileged internal path for first-party activities; the only mechanism that *guarantees* the Orker can read workload output is a relabel container running as UID 0 with `CAP_FOWNER`, which §9.1 forbids. | **Avoided rather than resolved.** Detection + terminal error + opt-in `runAsOrkerUser` + mandatory `umask 022` in first-party images (§3.1). No exemption is requested. | Grant a §9.1 exemption for an Orker-owned relabel container, which guarantees readability at the cost of one privileged container per task. |

A sixth item is a capability loss rather than a conflict, and is called out
here for the same reason: **a templated `uses:` no longer compiles** (§10.2).
`examples/container-example.ork.yaml` uses one today. If run-time image
selection must survive, this design cannot pin digests at apply time and
AGENTS.md §5 has to change instead.
