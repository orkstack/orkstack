# OCI Activity Contract

This document is the contract under which every OrkStack task executes. It
is normative: where it says MUST, an implementation that
does otherwise is wrong. Where it says SHOULD, a deviation needs a recorded
reason.

Scope: the engine↔Orker wire, the container-facing filesystem ABI, the durable
commit protocol, image resolution and delivery, Orker dispatch, failure
categories, the security profile, and the first-party activity images. It does
not design a plugin marketplace, a policy engine, or a second runtime.

Grounding: `AGENTS.md` §§1–15. The empirical findings behind the load-bearing
choices are in §14; the tests named throughout hold the tree to them.

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
| Registry credentials | resolve-only (manifest and config-blob GETs), read-only, no push, no execution | pull credentials | **never** | — |
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

> **Field numbers are never reused or renumbered.** A removed field is
> `reserved`. Go APIs, the DSL surface, and the `internal/task` structs may be
> replaced outright.

`proto/orkstack/v1/orkflow.proto` carries `reserved 8; reserved "queue";` and
`reserved 2; reserved "plugin";`, and `namespace.proto` reserves
`Pool.plugins` the same way. The DSL compiler decodes strictly, so a document
carrying any key the schema does not define is refused. `make proto` runs `buf lint` and `buf generate`, not `buf breaking`.

### 2.2 `proto/orkstack/v1/activity.proto`

The shipped file, verbatim.

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
  // delivery records how the Orker obtained the image for this execution:
  // "registry" (pulled) or "embedded" (loaded from the OCI image layout,
  // §4.6). The engine leaves it empty; only an Orker fills it, beside
  // platform_digest. It exists because objective deliverable 10's embedded
  // fallback is otherwise invisible after the fact: an operator debugging a
  // stale builtin needs to know the pull did not happen.
  string delivery = 11;
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
// WorkspaceLimits are the deployment-wide CONTRACT limits the Orker enforces
// after the container exits. They also appear verbatim in
// /orkstack/input/task.json so a workload can respect them instead of
// discovering them by failing.
//
// These are engine configuration and are uniform across a deployment, which is
// what lets them travel on RunPlan and lets the engine run its own
// INPUT_TOO_LARGE check. Orker-local RESOURCE policy — cpu, memory, pids caps
// — is deliberately NOT here: it varies per pool, the engine does not own it,
// and it reaches the workload as the effective values in task.json instead
// (§5.2, §9.3).
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
  // idempotency_key is the stable key of one logical activity, unchanged
  // across every retry (AGENTS.md §3, §4). Its value is exactly
  //   "{namespace-or-default}/{run_id}/{task_id}"
  //
  // The name is plain, not "task_idempotency_key", for two reasons: its wire
  // JSON (`idempotencyKey`) is then byte-identical to the key the workspace
  // already writes into /orkstack/input/task.json, and there is nothing to
  // collide with — proto scopes field names per message, and the run
  // submission key lives on a different message entirely
  // (SubmitRunRequest.idempotency_key = 4, service.proto). The Go
  // identifiers stay distinct.
  string idempotency_key = 8;
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

  // run_as_orker_user makes the Orker add --user <orker-uid>:<orker-gid>, so
  // it owns every file the workload writes and can read them whatever umask
  // the image uses (§3.1). Opt-in, because it overrides the image's own USER.
  bool run_as_orker_user = 22;
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
  // attempt is the Temporal attempt that produced this result: from
  // activity.GetInfo(ctx).Attempt on a fresh run, and from commit.attempt on
  // an adopted one (§5.4). Workflow code cannot call GetInfo, and an adopted
  // result carries a CommitRef rather than the commit body, so without this
  // field the engine cannot populate TaskRun.attempt identically for fresh and
  // adopted results.
  int32 attempt = 14;
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
  string idempotency_key = 8;

  ImageRef image = 9;                        // with platform_digest and delivery set
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

// ---------------------------------------------------------------- failure transport

// ActivityFailureDetail is what a FAILED activity returns. A failure produces
// no ExecuteTaskResult, and orkstack.v1.Error.details is a bare string, so
// without this message everything a failing task produced is unreachable: an
// exit-1 task can upload its test report and its stderr and leave the engine
// with nowhere to put the references.
//
// It travels as the single `details` argument of
// temporal.NewApplicationError(message, category, detail); the Go SDK carries
// details through the configured data converter, and the workflow retrieves
// them with errors.As to *ApplicationError followed by .Details(&out).
//
// The engine then populates TaskRun.artifacts, .system_artifacts, .exit_code
// and .attempt on the failure path exactly as it does on the success path, so
// a failed attempt is as inspectable as a successful one.
message ActivityFailureDetail {
  string reason_code = 1;              // the §8.2 code, e.g. "VALUES_MISSING"
  FailureCategory category = 2;
  int32 exit_code = 3;                 // 0 when the container never ran
  repeated ArtifactRef artifacts = 4;  // what the failure path managed to upload
  repeated ArtifactRef system_artifacts = 5;
  // scan_notes carries §3.5 violations found on the failure path. They are
  // notes ON the workload's error, never a replacement for it (§7.1 step 14).
  repeated string scan_notes = 6;
  ImageRef image = 7;
  int32 attempt = 8;
  string orker_id = 9;
}

// ---------------------------------------------------------------- orker identity

// DescribeOrkerResult is the pool-liveness probe's payload: a pool proves it
// is alive by answering DescribeOrker (§6.5).
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

**Task caching** (`docs/DSL.md`, "Caching") adds, all Orker-side and none of
it visible to a container: `CacheScope`, `CacheOutcome`, `CacheContext`
(`ExecuteTaskInput.cache = 27`: scope, the effective TTL in seconds, and
`refresh` for a `no_cache` run — set ONLY for a cached task, so every other
payload is byte-identical), `CacheResult` (`ExecuteTaskResult.cache = 15`,
`ActivityCommit.cache = 19`, `TaskAttempt.cache = 20`: outcome, key, a miss's
reason and sentence, a hit's `CacheSource`, the memo write's status, and — on
the attempt record only — the key's components), `CacheSource` (the commit a
hit reused), `CacheMemo` (the stored pointer, §5.4 "Memoized completion") and
`DescribeOrkerResult.cache_supported = 17`, which submit reads to refuse a
cached task on a pool whose Orker predates caching (it could not decode the
input).

### 2.3 Other proto files

The blocks below are excerpts of the files as they stand: field numbers, names
and types are verbatim; long field comments that other sections cover are
elided. The files are authoritative.

`proto/orkstack/v1/common.proto` carries the failure category and the `Error`
that reports it:

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

// Error captures a structured failure for a run or task run.
message Error {
  string code = 1;      // stable machine code, e.g. "VALUES_MISSING"
  string message = 2;   // human readable
  string details = 3;   // optional stack/context
  bool retryable = 4;
  // category is the AGENTS.md §4 failure category this error belongs to. It
  // is what drives NonRetryableErrorTypes in ActivityOptions, so it is part
  // of the replay-visible contract and not merely display data.
  FailureCategory category = 5;
}
```

`proto/orkstack/v1/orkflow.proto` — a `Task` names an activity, and an
activity is an image. The file imports `orkstack/v1/activity.proto` for
`ImageRef`, `SecretRef`, `ResourceRequest` and `NetworkMode`:

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

  // 8 held a removed execution-routing field. It is reserved rather than
  // reused, so a stale document cannot silently become a pool selection.
  reserved 8;
  reserved "queue";

  // pool names the set of Orkers this task runs on. Empty means the
  // deployment's default pool, which answers to the name "main".
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
  // run_as_orker_user is the §3.1 escape hatch, per task. It changes what
  // runs, so it is part of canonicalOrkflow(Task) (§4.4).
  bool run_as_orker_user = 18;
}
```

`proto/orkstack/v1/run.proto` — the read model says what the commit protocol
produced. The file imports `orkstack/v1/activity.proto` for `CommitRef`:

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
  Error error = 7;                       // carries FailureCategory (§8)
  google.protobuf.Timestamp started_at = 8;
  google.protobuf.Timestamp finished_at = 9;
  // pool names the set of Orkers this attempt was dispatched to, in the word
  // its author typed. Empty means the task never reached an Orker.
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
  string id = 1;                         // ork run id, ork-{timestamp}-{sequence} (also Temporal workflow id)
  string orkflow_name = 2;
  string orkflow_version = 3;
  Status status = 4;
  Payload input = 5;
  Payload output = 6;                    // aggregated terminal outputs
  Error error = 7;
  repeated TaskRun task_runs = 8;
  google.protobuf.Timestamp created_at = 9;
  google.protobuf.Timestamp started_at = 10;
  google.protobuf.Timestamp finished_at = 11;
  // namespace is the namespace of the orkflow this run executed. Empty means
  // the built-in "default" namespace.
  string namespace = 12;

  // orkflow_definition_digest is the canonical digest of the definition this
  // run pinned (§4.4). A run that started before a pointer moved is still
  // explainable by the exact bytes it ran.
  string orkflow_definition_digest = 13;
}
```

`proto/orkstack/v1/service.proto` — the activity catalog is a cross-process
contract and is frozen at the same fidelity as `run.proto`. The file imports
`orkstack/v1/activity.proto` for `DescribeOrkerResult`:

```proto
// ActivityService reports what this deployment can run: an activity is an
// image, and which images exist is engine configuration (§4.2), not something
// to ask an Orker.
service ActivityService {
  rpc ListActivities(ListActivitiesRequest) returns (ListActivitiesResponse);
  // DescribeOrker is the pool-liveness probe (§6.5). It is dispatched as a
  // Temporal activity on a pool's own channel, so it answers only when an
  // Orker is actually polling that pool.
  rpc DescribeOrker(DescribeOrkerRequest) returns (DescribeOrkerResponse);
}

message ListActivitiesRequest {}

// ActivityInfo is one catalog entry, resolved.
//
// resolved_at is a READ-MODEL field on a live catalog read, and is unrelated
// to the ImageRef.resolved_at that activity.proto reserves: nothing hashes it,
// and it is never stored in a version document.
message ActivityInfo {
  string name = 1;          // catalog key, e.g. "shell"
  string reference = 2;     // as configured, e.g. ghcr.io/orkstack/activity-shell:0.1
  string index_digest = 3;  // resolved, "sha256:<64 hex>"
  string media_type = 4;
  repeated string platforms = 5;
  google.protobuf.Timestamp resolved_at = 6;
}

message ListActivitiesResponse {
  // activities is ordered by name, matching ListPoolsResponse.pools
  // ("ordered by name", above). A stable order is part of the contract, not an
  // implementation detail: the Admin page renders it.
  repeated ActivityInfo activities = 1;
}

message DescribeOrkerRequest {}
message DescribeOrkerResponse { DescribeOrkerResult orker = 1; }
```

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
│   │       ├── <safe-name>         0444  one regular file per input artifact
│   │       └── <item>/             0555  FAN-OUT PRODUCER ONLY: one per item, 00000…
│   │           └── <safe-name>     0444  that item's artifact
│   └── upstream/                   0555
│       └── <task-id>/
│           ├── values.json         0444  that ancestor's committed values.json
│           └── <item>/             0555  FAN-OUT PRODUCER ONLY: one per item, 00000…
│               └── values.json     0444  that item's committed values.json
├── output/                         read-write bind, mode 1777
│   ├── values.json                 written by the workload — REQUIRED
│   └── artifacts/                  1777  one regular file per output artifact
├── shared/                         read-write bind, mode 1777 — PINNED TASKS ONLY
└── tmp/                            read-write bind, mode 1777
```

**A fan-out producer's items** (`docs/DSL.md`, "Fan-out") are staged one
directory per item, `<item>` the 0-based index zero-padded to five digits so a
sorted listing is item order: `upstream/<task>/<item>/values.json` and
`artifacts/<task>/<item>/<safe-name>`. A fan-out's own task has no single
`values.json`, so the item directories are all there is under
`upstream/<task>/`. Both directories exist even for a fan-out of zero items.
They are staged only for a **direct** dependent — a task that names the
fan-out in its own `needs:`; a transitive descendant gets nothing for it.
Each item's commit is read, its identity checked (namespace, run, task, the
item's idempotency key, definition digest) and every byte digest-verified,
exactly as an adopted commit is (§5.4).

The container's working directory is `/orkstack`. The container root filesystem
is read-only (§9), so `/orkstack/tmp` is the workload's scratch space; `/tmp`
inside the container is a separate small tmpfs the Orker also provides.

**`/orkstack/shared` is present only for a pinned task**, and it is the one
mount whose lifetime is not the attempt's. Every task of one pin, and every
attempt of every such task, sees the same directory with the same contents for
the life of the pin (`docs/DSL.md`, "Pins"). It is bind-mounted read-write,
`1777` with the sticky bit like `output/`, from the pin host's own
`ork-pin-<run>-<pin>-<instance>/shared/` directory under `ORK_ORKER_WORKDIR`;
that parent is `0700` and owned by the Orker's user, which is what makes the
`1777` child safe, exactly as it is for a per-attempt directory. Every
component of the path is Orker-constructed — a run id, a slug, and an instance
id validated before it becomes a path segment — so no definition can name a
host path and §9.1 is intact. For an unpinned task the mount does not exist and
`paths.shared` is absent from `task.json` (§3.2). The directory is neither an
input tree (it is never sealed) nor an output tree (nothing in it is scanned,
hashed, uploaded or committed), and no secret is ever written under it; a file
that must outlive the pin is copied to `/orkstack/output/artifacts/` and
becomes an artifact like any other.

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
2. **Give an explicit escape hatch.** A task may set `runAsOrkerUser: true`
   (`Task.run_as_orker_user = 18`, `ExecuteTaskInput.run_as_orker_user = 22`,
   surfaced to the workload as `task.runAsOrkerUser` in `task.json`), or an
   Orker may set `ORK_ORKER_FORCE_USER=1`, which adds
   `--user <orker-uid>:<orker-gid>`. The Orker then owns every written file
   and reads it whatever the umask — verified (§14.8). It is opt-in because
   it overrides the image's own `USER` and breaks images that need their
   baked-in identity, which would violate "any OCI image can become an
   activity".
3. **Require first-party images to set `umask 022`** in their entry scripts
   (§10.2), and make `orkctl activity test` assert output modes (§10.3).

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
is shared and `/var/folders` is not, which is why `make up` points `ORK_ORKER_WORKDIR` at `.data/work`. The Orker
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

**A pinned task gets two more keys, and only a pinned task.** A task that
belongs to a pin (`docs/DSL.md`, "Pins") carries `paths.shared` and a
`task.pin` object:

```jsonc
  "task": {
    "id": "compile",
    "attempt": 2,
    "pool": "main",
    "pin": { "name": "build", "instanceId": "3f9c1a2e-7b40-4d51-9a6f-2c8e5b71d0aa" }
  },

  "paths": {
    "…": "…",
    "shared": "/orkstack/shared"
  }
```

Both are **omitted** — absent as keys, not written empty — for an unpinned
task, which is what makes this addition additive: `schemaVersion` stays `1`
under the rule below that the platform may only add optional fields within one
major version, and a workload that has never heard of pins loses nothing by
ignoring them. `task.pin.name` is the pin as its author named it.
`task.pin.instanceId` identifies one opening of that pin within one run; it is
what the Orker keys the shared directory by, and it is informational to the
workload.

**Artifact names are UTF-8; paths are derived and lossy.** `path` is produced
by the existing `safeComponent` rule (`internal/orker/resolver.go:127-151`),
which rewrites separators, dot segments and long names, so it cannot carry the
logical name. `name` is the exact logical name and `nameB64` (RawURL base64 of
its bytes) is **always** present, so a workload never has to reverse
`safeComponent`. This mirrors the S3 companion manifest, which already solves
"the safe key is not the name" (`pkg/artifacts/s3_manifest.go:144-187`).

`nameB64` exists for names containing `/`, `..`, spaces or 200 bytes of
Unicode — all valid UTF-8, all unrepresentable as a path segment. It does
**not** license arbitrary octets: an earlier draft called names "opaque bytes",
which the wire cannot honor. Measured (§14.16): `ArtifactRef.name` is a proto
`string`, and **both** `protojson.Marshal` and binary `proto.Marshal` reject
invalid UTF-8, inside `ArtifactRef`, `TaskRun` and `ActivityCommit` alike. A
name the wire cannot carry could be staged and stored but never committed, so
§3.5 rejects it at the output boundary instead.

#### The schema, field by field

`task.json` is a contract, so it is specified as one rather than left to an
example. Required means always present; a workload may rely on it. Optional
means present only under the stated condition; a workload MUST tolerate its
absence rather than treating it as an error.

| Path | JSON type | Req. | Empty form | Meaning |
|---|---|---|---|---|
| `schemaVersion` | integer | required | — | Major version of this file. `1` today. |
| `idempotencyKey` | string | required | — | `"{namespace}/{runId}/{taskId}"`, stable across retries; for an item of a fan-out `"{namespace}/{runId}/{taskId}/{index}"`, stable across that item's retries |
| `run.id` | string | required | — | Opaque run id. Never parsed by anything. |
| `run.namespace` | string | required | `"default"` | Resolved, never empty on the wire into the container |
| `run.orkflow.name` | string | required | — | slug |
| `run.orkflow.version` | string | required | — | pinned version string |
| `run.orkflow.definitionDigest` | string | required | — | `sha256:<64 hex>` (§4.4) |
| `task.id` | string | required | — | slug |
| `task.attempt` | integer | required | — | 1-based Temporal attempt |
| `task.pool` | string | required | `"main"` | resolved pool name |
| `task.runAsOrkerUser` | boolean | optional | absent = false | present and true when the Orker overrode the image's `USER` (§3.1), so a workload can see why its UID is not the one its image declared |
| `task.pin` | object | optional | absent | present only for a pinned task; absent means unpinned |
| `task.pin.name` | string | required in `task.pin` | — | slug; the pin as its author named it |
| `task.pin.instanceId` | string | required in `task.pin` | — | one opening of that pin within one run; what the Orker keys the shared directory by |
| `each` | object | optional | absent | present only for an item of a fan-out task; `task.id` stays the fan-out's |
| `each.index` | integer | required in `each` | — | 0-based position of this item in the array |
| `each.count` | integer | required in `each` | — | the number of items, counted once by the engine |
| `each.as` | string | required in `each` | — | the name the item is bound to under `.each` in `with:` (default `"item"`) |
| `each.item` | any JSON | required in `each` | — | the element itself: its exact JSON text for an upstream-task source (key order and number spelling preserved), the protojson rendering of the value for a run-input source |
| `image.reference` | string | required | — | as authored |
| `image.pinned` | string | required | — | `{registry}/{repo}@sha256:…` — what ran |
| `image.indexDigest` | string | required | — | `sha256:<64 hex>` |
| `image.platformDigest` | string | optional | absent | present once the Orker resolved the platform manifest |
| `image.platform` | string | optional | absent | `os/arch[/variant]` |
| `image.delivery` | string | optional | absent | `"registry"` or `"embedded"` (§4.6) |
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
| `paths.*` | string | required | — | all eight fixed keys always present (§3.1) |
| `paths.shared` | string | optional | absent | `/orkstack/shared`, present only for a pinned task (§3.1) |
| `inputs.artifacts` | array | required | `[]` | never null |
| `inputs.artifacts[].producer` | string | required | — | upstream task id |
| `inputs.artifacts[].name` | string | optional | absent | present only when the exact name is valid UTF-8 |
| `inputs.artifacts[].nameB64` | string | required | — | RawURL base64 of the exact name bytes; authoritative |
| `inputs.artifacts[].path` | string | required | — | absolute container path |
| `inputs.artifacts[].sizeBytes` | integer | required | `0` | |
| `inputs.artifacts[].sha256` | string | required | — | hex |
| `inputs.artifacts[].contentType` | string | optional | absent | |
| `inputs.artifacts[].item` | integer | optional | absent | present only for an artifact of a fan-out producer's item (§3.1) |
| `inputs.upstream` | array | required | `[]` | never null |
| `inputs.upstream[].taskId` | string | required | — | |
| `inputs.upstream[].valuesPath` | string | required | — | absolute container path |
| `inputs.upstream[].item` | integer | optional | absent | present only for one item of a fan-out producer (§3.1) |
| `inputs.eachUpstream` | array | optional | absent | present only when a direct upstream is a fan-out, so a workload can tell "zero items" from "no such upstream" |
| `inputs.eachUpstream[].taskId` | string | required | — | the fan-out's task id |
| `inputs.eachUpstream[].count` | integer | required | `0` | how many items were staged |
| `secrets` | array | required | `[]` | never null; names and paths only, never values |
| `secrets[].name` | string | required | — | `^[A-Za-z0-9._-]{1,128}$` |
| `secrets[].path` | string | required | — | absolute container path |
| `trace.traceparent` | string | optional | absent | present only when this deployment exports traces (§ below) |
| `trace.tracestate` | string | optional | absent | present only alongside `traceparent`, and only when the propagator set one |

An absent array is never written as `null`; the Orker emits `[]`. An absent
optional scalar is omitted, never written as `""` — a workload distinguishes
"no content type" from "empty content type" by presence.

An optional `trace` object (`traceparent`, and `tracestate` when the
propagator sets one, both in W3C header form) is present only when this
deployment exports traces (`internal/telemetry`, off by default). It is
purely informational, never ABI: it does not move `schemaVersion`, and a
workload that has never heard of tracing loses nothing by ignoring it. One
that wants to join the trace its own activity ran under propagates the two
strings verbatim to whatever it calls next.

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

The FuncMap exposes three helpers: `artifact`, `env`, and **`sh`**, which wraps
a value in POSIX single quotes, escaping any embedded `'` as `'\''`
(`internal/orker/render.go`; the DSL registers parse-time stubs of the same
names in `internal/dsl/template.go`). AGENTS.md §6 requires that *"shell
commands must not interpolate untrusted input without the templating layer's
escaping"*, and `sh` is the escaping that sentence names. It is the
*secondary* mitigation: the primary one is that a first-party `command` is an
argv array rather than a shell string (§10.2), so nothing rendered becomes code
in the first place.

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
- Every file name MUST be **valid UTF-8**. One that is not is
  `ContractViolation/ARTIFACT_NAME_NOT_UTF8`, naming the offending entry's
  bytes in hex. This is a real restriction on a POSIX filesystem, and it is
  imposed because the alternative is worse: `ArtifactRef.name` is a proto
  `string`, and every protobuf encoding — protojson *and* binary — refuses
  invalid UTF-8 (§14.16), so such a name could be staged, hashed and uploaded
  and then fail at the commit, after the bandwidth was spent. Rejecting at the
  scan makes it a fast, terminal, actionable failure instead.
- Ordering is by name bytes ascending, so two attempts of the same computation
  produce the same commit ordering.

### 3.6 Streams

stdout and stderr are captured **independently**, byte-exact, and never merged
on disk. Two things happen to them:

1. **Live**, they go to the existing log sink
   (`internal/orker/logsink.go:54-521`) with the existing framing
   (`[stderr] ` prefix, `[stdout] ` escape) so the browser keeps rendering
   tinted stderr in real time (`ui/e2e/run-page.spec.ts`). This path is a
   regression gate.
2. **Durably**, on the success and failure paths each stream is uploaded as
   its own object and recorded in `ExecuteTaskResult.system_artifacts` /
   `ActivityFailureDetail.system_artifacts` and, on success,
   `ActivityCommit.system_artifacts`, under the names `stdout.log` and
   `stderr.log`. Because `system_artifacts` is a separate field from
   `artifacts`, a workload artifact named `stdout.log` cannot collide with the
   platform's.

**On the cancellation path there is no durable object, and that is stated
rather than promised away.** §6.3 signals, waits, kills, removes on a detached
context, closes the log sink and returns `ctx.Err()`. A canceled activity
produces neither an `ExecuteTaskResult` nor an `ActivityCommit`, so both
declared carriers are absent; Temporal permits only `CanceledError` details on
that path. What survives is path 1, which is already durable and already
readable: the log sink has written numbered chunks to
`metadata/logs/{run}/{task}/{attempt}/{seq}` throughout the run, and
`internal/api/logs.go:172-276` serves them by `(run, task, attempt)` with no
dependence on a result. The CLI and UI already consume exactly that. So a
canceled attempt's output is retained and reachable — it simply is not a
`system_artifacts` reference, and §3.6 no longer implies otherwise.

Plain text works with no logging SDK. Structured JSON is not part of the ABI.

Independently of the two uses above, an operator may have the Orker export
each stdout/stderr line a third time, as an OpenTelemetry log record carrying
that line's run id, task id, attempt, namespace and orkflow coordinates
(`ORK_ORKER_TASK_LOG_EXPORT`, `docs/DEPLOY.md`, "Telemetry"). This changes
nothing about framing, exit semantics or the ABI above — it is a copy of what
already left the container. An image needs no agent, SDK, environment
variable or file to be exported this way; the Orker does the whole thing from
outside.

---

## 4. Image resolution and delivery

### 4.1 The two halves

```
APPLY TIME (engine, internal/api, once per version)
  uses: "shell"                      catalog lookup
     └─> ghcr.io/orkstack/shell:0.1   registry GET /v2/<repo>/manifests/<tag>
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
2. For each, issues a **`GET /v2/<repository>/manifests/<tag-or-digest>`** with
   the **exact** `Accept` header set the Orker will use at pull time. A
   different `Accept` set at resolve and pull can select a different object
   (OCI distribution-spec content negotiation).

   It must be a GET, not a HEAD, and this is not a preference. §4.4 hashes
   `ImageRef.platforms` into the definition digest, and `platforms` comes from
   the index's `manifests[].platform` array — which lives in the response
   **body**. HTTP forbids content on a HEAD response, and the OCI spec's HEAD
   requires only `Docker-Content-Digest` and `Content-Length`. A HEAD-only
   resolver would leave `platforms` empty where a GET-based one fills it, and
   the two would mint **different definition digests for the same index** —
   precisely the non-determinism §4.4 exists to remove.
3. Computes `sha256` over the **exact response bytes** and requires equality
   with `Docker-Content-Digest` when the header is present; the spec makes
   that verification mandatory. A mismatch is `IMAGE_DIGEST_MISMATCH` at apply.
4. Branches on the response `Content-Type`:
   - `application/vnd.oci.image.index.v1+json` or
     `application/vnd.docker.distribution.manifest.list.v2+json` → read
     `manifests[].platform{os,architecture,variant}` and render each as
     `os/arch[/variant]`.
   - any single-manifest media type → `platforms` stays **empty**, as §2.2
     already specifies.

   `platforms` is recorded in the **index's own order**, not sorted. The digest
   hashes it, so the order is part of the contract; index order is what the
   registry returned and needs no second rule to agree on.
5. For an already-pinned `repo@sha256:…` it still issues the GET, and requires
   a `200` whose computed body digest equals the pin. Apply MUST NOT succeed
   with an unpullable pin.
6. Stamps `Task.image`.
7. Reads the interface label (`docs/ACTIVITY_IMAGES.md`, "Declaring the
   interface"):
   a. Locates the image manifest. For a single manifest it is the body of
      step 2. For an index it selects the **first entry in index order whose
      `platform.os` is present and is not `unknown`** (buildx attestation
      manifests are skipped) and GETs `/v2/<repository>/manifests/<digest>`
      with the same `Accept` set, requiring `sha256(body)` to equal the
      entry's digest (`IMAGE_DIGEST_MISMATCH` otherwise). An index with no
      such entry — or whose selected entry is itself an index, or names no
      sha256 digest — reports "index advertises no platform manifest to read
      a config from" as the interface error; resolution still succeeds. One
      interface is read per image.
   b. Reads `config.digest`; a manifest without a readable config descriptor
      is `IMAGE_RESOLVE_UNSUPPORTED`.
   c. GETs `/v2/<repository>/blobs/<config.digest>` with the credential of
      step 2 (one token handshake per image, repeated once on a `401`),
      following redirects, bounded at 4 MiB (`IMAGE_RESOLVE_UNSUPPORTED`
      past it), and requires `sha256(body)` to equal `config.digest`.
      `404` is `IMAGE_NOT_FOUND`, `401`/`403` `IMAGE_RESOLVE_UNAUTHORIZED`,
      `5xx`/timeout/DNS `IMAGE_RESOLVE_UNAVAILABLE`.
   d. Decodes `config.Labels`; a blob that is not JSON is
      `IMAGE_RESOLVE_UNSUPPORTED`. No `org.orkstack.activity.interface`
      label means the image declares nothing.
   e. Parses the label. A malformed label **never** fails resolution: the
      image is still pinned and runnable, and the reason is reported as the
      interface error.

   A failure in 7a–7d fails the apply with the reason codes above: a registry
   that serves a manifest but not its config cannot serve a pull either, and
   step 5 already refuses an unpullable pin. The interface is not an
   `ImageRef` field, so it never reaches the definition digest (§4.4).

Failures are apply-time Connect errors, never silent: `IMAGE_NOT_FOUND` (404),
`IMAGE_RESOLVE_UNAUTHORIZED` (401/403), `IMAGE_RESOLVE_UNAVAILABLE` (5xx,
timeout, DNS). No fallback to a tag exists.

**There is exactly one resolver.** `docker image inspect .RepoDigests` reports
whatever the daemon negotiated for the platform it pulled, and
`docker manifest inspect --verbose` does not reliably surface a parent index
digest — so the daemon is not the resolver. Neither is
`docker buildx imagetools inspect`, which an earlier draft offered as an
alternative: a second resolver can disagree with the first about `platforms`
and therefore about the definition digest, and §4.4's whole argument is that
one definition has one digest. Resolution goes to the registry over the
distribution API, and nowhere else. (`imagetools inspect` remains fine as a
human diagnostic, which is all §14.4 uses it for.)

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
> `pins` (only when non-empty — the exception below), `tasks`. **Not**
> `source_yaml`, **not** `created_at`.
> `Task`: `id`, `uses`, `image`, `with`, `needs`, `condition`,
> `timeout_seconds`, `retry`, `pool`, `command`, `env`, `secrets`, `resources`,
> `network`, `cancellation_grace_period_seconds`, `run_as_orker_user`,
> `pin` (only when non-empty — the exception below).
> `ImageRef`: `reference`, `registry`, `repository`, `index_digest`, `pinned`,
> `media_type`, `platforms`, in the index's own order (§4.3 step 4).
> (`resolved_at` no longer exists; `platform_digest`, `platform` and `delivery`
> are always empty in a definition — they are per-execution facts an Orker
> records — and are not serialized.)
>
> `orkflow_definition_digest = "sha256:" + hex(sha256(canonicalOrkflow(wf)))`

This lives beside `internal/dsl/hash.go` with its own golden-vector test, so a
digest change is a test failure rather than a silent incompatibility.

**The one documented exception to "emitted whether or not they are zero" is
`pin` and `pins`.** Both change what runs and where, so both are in the digest,
but each is emitted **only when non-empty**. Emitting `"pin": ""` on every task
unconditionally would move the digest of every existing definition on its next
apply, and that is not cosmetic: `pkg/artifacts.sameStoredOrkflow` decides
"unchanged document" by comparing after clearing only `created_at`, so an
unchanged, source-hash-versioned document stamped with a moved digest becomes a
*changed* document under an *existing* version and is refused with
`ALREADY_EXISTS`. The exception is deliberate and named here so a later reader
does not read it as an oversight; the existing golden vector passing unchanged
is the proof that no unpinned digest moved, and a second golden vector with a
pin covers the emitted form.

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
Docker `config.json`, used **only** for manifest and config-blob **GET**s
during apply-time resolution (§4.3), always with a pull scope. It grants no push and no
execution. It does not weaken AGENTS.md §2.3, which makes the Orker the boundary
for *execution* credentials; those stay on the Orker and are never sent to a
container.

### 4.6 Delivery model

| Source | How the image arrives | Digest |
|---|---|---|
| **Builtin** (`shell`, `http`, `file`) | Pulled from the catalog reference like any other image. Built and pushed by `make -C images push`. | Real registry index digest |
| **Custom / community** | Pull from the registry named in `uses:`. This is the default and needs no configuration. | Real registry index digest |
| **User-provided source** (Dockerfile or git context) | `orkctl activity build <path\|git-url> --tag <ref> --push` builds and pushes, printing the pinned reference to paste into `uses:`. An explicit, out-of-band step. | Real registry index digest |
| **Embedded fallback** (builtins only) | An **OCI image layout** shipped with the Orker at `ORK_ORKER_IMAGE_LAYOUT` (default `/var/lib/orkstack/images`). Used only when a pull of an already-pinned digest fails. | Real registry index digest, carried inside the layout |

There is no *unpushed* local image, because a locally built, never-pushed image
has **no registry manifest digest** — `--iidfile` and `--quiet` give a local
config ID, which is not a manifest digest and does not transfer to a second
Orker. Making every build path push is what keeps AGENTS.md §5's immutable
reference literally true everywhere, including `make up`.

**The embedded fallback is not an exception to that.** An OCI image layout
(`oci-layout` + `index.json` + `blobs/sha256/…`) is content-addressed by
construction: the manifest digest *is* the blob name. So the Orker can serve a
pinned digest out of the layout and still satisfy §5, because it verifies the
same string it would have verified after a pull:

```
Runtime.Pull(pinned) fails (network, registry down, rate limit)
  → OrkStack reads <layout>/index.json, finds the descriptor whose
    digest == image.index_digest
       absent → InfrastructureError/IMAGE_PULL_FAILED   (retryable, as before)
  → OrkStack reads blobs/sha256/<hex>, computes sha256 over those exact bytes,
    and compares to image.index_digest
       unequal → ContractViolation/IMAGE_DIGEST_MISMATCH  (never imported)
  → only then Runtime.LoadLayout(layoutPath, image.index_digest)
  → run, and set ImageRef.delivery = "embedded"
```

**The hash is computed by OrkStack, before import, and this is not belt and
braces.** An earlier draft said the layout is "content-addressed by
construction: the manifest digest *is* the blob name", and treated the filename
as the proof. It is not. Measured (§14.15): a root blob whose filename claims
`sha256:d9e853e8…` while its bytes hash to `sha256:446d9649…` was accepted by
`docker load` with exit 0 and the resulting image **ran**. Nothing in the load
path compares `blobs/<alg>/<encoded>` against the bytes stored there, and
neither OCI `index.json` nor Docker `manifest.json` supplies an authenticated
root. A filename is a claim, not a digest.

So §4.6's guarantee rests on OrkStack hashing the root index bytes itself.
Without that step the embedded fallback would import an arbitrary image under a
pinned digest, silently — which is exactly the failure AGENTS.md §5 exists to
prevent, arriving through the door meant to satisfy it.

**The runtime seam.** `ContainerRuntime` (§6.1) gains one method, because
§6.1's own rule forbids Docker-specific detail anywhere else:

```go
// LoadLayout imports the image addressed by digest from an OCI image layout
// on local disk and returns the same PullInfo a Pull would have. A digest the
// layout does not contain returns an error matching ErrNotInLayout.
//
// The CALLER has already verified that the layout's root blob hashes to
// digest. LoadLayout does not re-verify and MUST NOT be relied on to: the
// Docker implementation delegates to `docker load`, which verifies nothing
// about the root (§14.15).
LoadLayout(ctx context.Context, layoutPath, digest string) (PullInfo, error)
```

`DockerRuntime` implements it with `docker load`, which accepts an OCI image
layout tar — verified, §14.12. No Docker vocabulary escapes
`runtime_docker.go`.

**Which layer verifies what**, stated rather than implied:

| Object | Verified by | How |
|---|---|---|
| the root index blob | **OrkStack**, before import | reads `blobs/sha256/<hex>`, computes `sha256`, compares to `image.index_digest`. Not the filename — the bytes (§14.15). |
| the child platform manifest | the container runtime, on load | referenced by digest from the index OrkStack just verified |
| the layer blobs | the container runtime, on load | referenced by digest from the child manifest |

OrkStack verifies the one link that ties the layout to what apply pinned,
because nothing else does; the runtime follows descriptors from there, which is
what a registry pull also delegates to it. OrkStack does not re-implement layer
verification.

The layout is a cache with a proof, not a second source of truth: an image can
only be served from it if its digest was already resolved from a registry at
apply time. It exists so an air-gapped or rate-limited Orker can still run the
builtins, and it is populated by `make -C images layout` after the push.

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

and `make -C images push` builds each first-party activity, pushes it to
`localhost:5001/orkstack/activity-<name>:dev`, and exports the same manifests
into the Orker's OCI layout. `make up` points `ORK_ACTIVITY_CATALOG` at
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

**An item of a fan-out** commits one segment deeper:

```
metadata/commits/{namespace}/{run-id}/{task-id}/{item:05}/commit.json
```

(`artifacts.ItemCommitKey`). It is still deterministic and
attempt-independent: an item's index is its position in an array the engine
counted once and recorded in the run's history, so it never moves across that
item's retries, and the adoption of §5.4 applies per item with no logic of the
Orker's own — the Orker writes and adopts at the `commit_uri` it was sent. The
five-digit segment can never be mistaken for a task's `commit.json`, and an
item's commit carries the item's own `idempotency_key`
(`{ns}/{run}/{task}/{index}`), which the identity check compares.

**Where the URI comes from.** A URI needs the artifact backend and endpoint —
`ORK_ARTIFACT_BACKEND`, `ORK_S3_ENDPOINT` — which is process configuration that
workflow code may not read (AGENTS.md §2.1). So the commit-store root is
decided **at submit**, in `internal/api`, and travels on `RunPlan`, which is
already workflow argument 4 and already replay-pinned. `internal/engine/plan.go`
states the pattern for exactly this case: the decision "is made once, by the
process that accepts the submission, and travels as an argument". `RunPlan`
gains three fields:

```go
type RunPlan struct {
    Namespace string            `json:"namespace"`
    Routes    map[string]string `json:"routes"`

    // CommitRoot is the store root every commit URI in this run is composed
    // against, e.g. "s3://orkstack-artifacts" or "file:///var/lib/ork".
    CommitRoot string `json:"commit_root"`
    // Limits are the deployment-wide CONTRACT limits (§2.2 WorkspaceLimits):
    // values, artifact count and size, log and commit bounds. They are engine
    // configuration, uniform across the deployment, and the engine needs them
    // itself for its own INPUT_TOO_LARGE check.
    //
    // They are NOT "effective limits after Orker policy" — an earlier draft
    // said that and it cannot be true. `internal/engine/pools.go:410-436`
    // builds `routes` as a map of MANY pools to MANY queues plus a
    // `tasksByPool` index, so one run genuinely spans pools whose Orkers hold
    // different caps; and `internal/api` owns no worker-local policy at all —
    // §9.3 puts cpu/memory/pids caps in ORK_ORKER_* on the Orker. A single
    // submit-time value cannot represent per-pool policy, so it does not try.
    Limits WorkspaceLimits `json:"limits"`

    // TransferBudgetSeconds is the data-transfer slack in this run's
    // StartToCloseTimeout (§6.3). It is resolved at submit for the same reason
    // CommitRoot is: the derivation reads process configuration, and
    // ActivityOptions is set in workflow code, which may not.
    //
    // Without this field §6.3's formula is unimplementable — it named two
    // ORK_ORKER_-prefixed variables that the ENGINE would have to read, which
    // is precisely the contradiction Limits' own comment rejects.
    TransferBudgetSeconds int32 `json:"transfer_budget_seconds"`
}
```

**Resource policy stays where it is enforced.** The Orker applies its own
`ORK_ORKER_MAX_CPU` / `ORK_ORKER_MAX_MEMORY` / `ORK_ORKER_PIDS_LIMIT`, refuses
an over-cap request with `ContractViolation/RESOURCE_REQUEST_REFUSED` (§9.3),
and writes the **effective** values into `task.json` — which is already exactly
how §3.2 documents `resources.*` ("as requested/capped; absent means Orker
default"). Neither half claims knowledge it does not have: the engine owns
contract limits, the Orker owns resource policy.

The workflow composes `commit_uri` as
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

The Orker MUST verify this at boot with concurrent writers, not a sequential
`HEAD`-then-`PUT`: a sequential check proves ordering, not atomicity. The probe
uses one fixed object **per commit schema version** under
`metadata/probes/commit-store/schema-v<version>/`, outside the
activity-completion prefix. Versioning the key means a newer build never
strands every Orker on the durable winner written by an older schema; the old
capability record is harmless and no operator must delete storage to recover.
On the first boot for a schema version, two goroutines conditionally create
that absent key; exactly one must succeed and the other must see
`ErrPreconditionFailed`, proving full fresh-key arbitration. The winner remains
as a bounded capability record. Later boots on that version race the same two
writes, require both to see `ErrPreconditionFailed`, and read back the exact
fixed candidate. Those later checks prove refusal and readback rather than
repeating fresh-key arbitration, while remaining capable of rejecting an
unconditional backend. Any other outcome is a boot failure with
`InfrastructureError/COMMIT_STORE_NOT_CONDITIONAL`, naming the endpoint —
refusing to start is the only honest response, because the alternative is a
deployment that silently loses the immutability guarantee.
`TestS3CommitStoreConditionalTwoWriterProbe` measures both first and later
calls against the deployed MinIO backend.

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
idempotency_key           == input.idempotency_key
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
Treating it as "no commit" would let a second container run after a possibly
durable commit; treating it as `COMMIT_IDENTITY_MISMATCH` would wedge a run on
a transient read.

The rule is an **in-attempt reread**, and it needs no cross-attempt state:
re-read the object once after a short backoff, **within the same attempt**. If
it decodes, continue at the matching branch above. If it does not, the stored
bytes are corrupt — not in flight — and the attempt fails with
`ContractViolation/COMMIT_UNDECODABLE`, naming the URI and its size.

An earlier draft said "retry once on the *next* attempt", which is not
implementable as written: §6.4 makes the Orker stateless and disposable, a
retry may land on a different host, Temporal's attempt counter counts every
failure cause rather than consecutive undecodable reads, and §8.2 listed no
retryable code for the first occurrence — so an implementer following the
table literally failed terminally on read one, the outcome the branch exists
to avoid. The only durable cross-attempt carrier available would be a second
marker object, i.e. a second durability mechanism to prove.

A truncated read is a read-time artifact, so one immediate reread separates it
from genuine corruption without any carrier. And under the local store's
`link(2)` publication a torn commit cannot be created at all, so this branch
only ever fires for real corruption.

**Adoption reconstructs the full result, including the inline copy.** The
commit stores `values` as an `ArtifactRef`, while `ExecuteTaskResult` carries
`values_json` inline at ≤ 4096 bytes. If adoption simply returned the commit,
the same task would produce a populated `TaskRun.output` on a fresh attempt and
an empty one on an adopted attempt — a read-model difference decided by nothing
but which Orker happened to crash. So an adopting Orker:

```
commit.values.size_bytes <= 4096
  → GET commit.values.uri
  → verify sha256 == commit.values.sha256
       mismatch → ContractViolation/VALUES_CORRUPT     (the store lied; terminal)
       fetch failed → InfrastructureError/VALUES_REFETCH_FAILED  (retryable)
  → set values_json, values_inlined = true
commit.values.size_bytes > 4096
  → values_json stays empty, values_inlined = false, exactly as a fresh run
```

The cost is one GET on a rare path, and the benefit is that `commit_adopted`
changes nothing an API consumer can observe except the flag itself.

Adoption does **not** re-verify artifact bytes: they may have been expired by a
lifecycle rule, and a commit is a record of what happened, not a live guarantee
that every byte still exists. `values` is the one exception, because it is
being copied into the result rather than merely referenced.

**Success path**, in this exact order (AGENTS.md §4.1):

```
1. validate values.json           (exists, readable, object, UTF-8, ≤ limit)
2. scan + validate output/artifacts/  (regular files, readable, count/size limits)
3. hash every artifact and values.json (sha256, streaming)
4. bound the commit                (project ActivityCommit via RefFor, assert ≤ commit_max_bytes)
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

**Step 4 needs a ref projector, and the frozen `Store` cannot supply one.**
A projected `ActivityCommit` must contain the same `ArtifactRef`s step 5 will
produce, or measuring it proves nothing — and a ref's `uri` is the longest
field in it. But `artifacts.Store` is `Put/Get/Stat/List/Presign/Delete`
(`pkg/artifacts/artifacts.go:40-61`): nothing predicts a ref, `Stat` needs a
URI the Orker does not yet have, and both key builders are unexported
(`objectKey` at `pkg/artifacts/artifacts.go:89`, `s3ObjectKey` at
`pkg/artifacts/s3_manifest.go:64`).

The keys are already pure functions of data the Orker holds after step 3 — the
logical name plus the content digest for local, provenance plus name plus
digest for S3 once §5.4's sixth content segment lands. So the answer is to
export the projection, not to add I/O:

```go
// pkg/artifacts — implemented by both backends, taken by the dispatcher.
type RefPredictor interface {
    // RefFor returns exactly the ArtifactRef a Put of these bytes under this
    // name and provenance would return. It performs no I/O and must agree
    // byte-for-byte with Put's result.
    RefFor(p Provenance, name, sha256 string, sizeBytes int64, contentType string) ArtifactRef
}
```

Step 4 then measures a manifest whose refs are byte-identical to the ones step
8 commits, and §10.3's commit-bound fixture can assert that equality directly
(`pkg/artifacts/ref_predictor.go`).

**Output objects MUST be immutable before step 8.** A five-segment S3 key,
`orkflows/{orkflow}/{run}/{task}/{name}` — no attempt and no content
component — written with an unconditional `PutObject` would let two attempts
of one task write the same key, so a losing attempt could overwrite the exact
bytes a winning commit already hashed and referenced, turning a committed
`sha256` into a lie. The local backend is immune because it is
content-addressed (`pkg/artifacts/local.go`).

The S3 key therefore carries the content digest as a sixth segment,
`orkflows/{orkflow}/{run}/{task}/{name}/{sha256}` (`s3ObjectKey`,
`pkg/artifacts/s3_manifest.go`). A key names bytes, not a slot, so a re-upload
of identical content is idempotent, a re-upload of different content cannot
destroy the old, and step 5 is safely repeatable across retries. The companion
manifest under `manifests/` restores the exact logical name.

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

### 5.4.1 Memoized completion (`cache:`)

A task that declares `cache:` (`docs/DSL.md`, "Caching") carries a
`CacheContext`, and its attempt takes one more step between staging and the
image — the **memo step**. It is Orker-side behaviour and changes nothing a
container sees: no file, `task.json` key, environment variable or exit code.

1. **Deferred staging.** The upstream `values.json` objects are fetched and
   digest-verified (the render reads them) and a fan-in's item commits are
   read, but every artifact download, and the secrets, are recorded instead of
   performed.
2. **The key.** `config.json` is rendered in memory with the same renderer,
   `run.json` encoded, and the memo key computed: the SHA-256 of a document
   naming the namespace, the scope, the Orkflow (orkflow scope only), the task
   and the components of `docs/DSL.md`'s table. An input reference with no
   digest cannot be keyed: a MISS `INPUT_UNHASHED`, and no memo is written.
3. **The lookup** (skipped, and recorded `BYPASSED`, when `refresh` is set),
   bounded as a whole by 30 s; the first failing check is the recorded miss
   reason, and **every** failing check means *run the task*, never a failure:

   | # | Check | Otherwise |
   |---|---|---|
   | 1 | the Orker has a memo store, a commit store and a commit URI | `MEMO_STORE_ABSENT` |
   | 2 | the memo reads | absent → `MEMO_ABSENT`; corrupt, store error or timeout → `MEMO_UNREADABLE` |
   | 3 | schema 1; key, namespace, scope, task (and Orkflow under orkflow scope) are ours | `MEMO_UNREADABLE` / `MEMO_MISMATCH` |
   | 4 | the source finished less than the effective TTL ago | `MEMO_EXPIRED` |
   | 5 | the source commit reads, schema 1 | `SOURCE_COMMIT_GONE` / `SOURCE_COMMIT_UNREADABLE` |
   | 6 | its stored bytes hash to the memo's `commit_sha256` | `SOURCE_COMMIT_CHANGED` |
   | 7 | its namespace, task, run, `finished_at`, index digest and Orkflow are the memo's and ours; exit 0 | `SOURCE_COMMIT_MISMATCH` |
   | 8 | it fits this run's value, artifact-count and artifact-size limits | `SOURCE_OVER_LIMITS` |
   | 9 | its `values.json` refetches and hashes to the recorded digest; every artifact object exists | `SOURCE_OBJECT_UNAVAILABLE` |
   | 10 | the hit's own commit fits `commit_max_bytes` | `SOURCE_OVER_LIMITS` |

4. **A hit writes this run's own commit** — §5.4's sequence with the upload
   replaced by a check: identity fields this activity's, `values`,
   `artifacts`, `system_artifacts` and `image` the source's references, `cache`
   naming the source — through the same create-if-absent, before success is
   reported. No image is pulled, no secret resolved, no container created. A
   conflict adopts the winner, as step 8 does; any other write error is
   `COMMIT_WRITE_FAILED`, the only way the memo step can fail an attempt. A
   memo that points at a HIT commit is followed once, so `cache.source` always
   names the run whose container produced the bytes.
5. **A miss or a bypass** completes staging, runs the container as always, and
   records the decision on the commit it writes. After that commit is
   **created** — never after a conflict's adoption — it writes the memo, a
   plain put that replaces whatever was at the key, best-effort on a detached
   10 s context: a failure is logged and reported (`memo_write_error`), never
   an attempt failure.
6. **Adoption on retry** (step 0) returns the adopted commit's `cache`, so a
   retry after a hit's commit returns the same hit with `commit_adopted`.

A memo is a pointer, never state: deleting any memo, or the whole
`metadata/memo/` prefix, changes no result — only whether the next run's
containers run.

### 5.5 Crash coverage (AGENTS.md §11)

| Crash point | What survives | What happens next | Terminal state |
|---|---|---|---|
| Orker dies mid-container | **the container keeps running** (§14.17) | Heartbeat timeout (30 s) → Temporal retries. New Orker finds no commit, re-runs. The abandoned container is reaped by the measures below. | correct, container ran twice (at-least-once), plus duplicate resource use until reaped |
| Orker dies after exit, before upload | nothing | Same as above. | correct, container ran twice |
| Orker dies mid artifact upload | some artifacts, no commit | Retry re-uploads. Orphans from the dead attempt are unreferenced. | correct |
| Orker dies after `commit.json`, before Temporal completion | the commit | Retry's step 0 finds it and **adopts** it: no second container run, result returned. | correct, container ran once |
| Orker dies after a cache hit's `commit.json`, before completion | this run's hit commit | Retry's step 0 adopts it: `cache` HIT with the same source, `commit_adopted`; no container in either attempt. | correct, container ran zero times |
| Orker dies after a fresh commit, before its memo write | the commit, no memo | The task completes on retry by adoption; the next run misses (`MEMO_ABSENT`) and writes the memo. | correct, one extra container run later |
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

**One row deserves more than a cell: an Orker crash does not stop the
container.** An earlier draft said "nothing on that host" survives. Measured
(§14.17): after `SIGKILL` of the managing process the labelled container was
still `running=true` three seconds later, and the `--rm` control was *also*
still running. `--rm` is daemon-side but fires on container **exit**, and a
container whose client was killed has not exited. Docker's `--sig-proxy`
forwards only catchable signals, so it cannot help in exactly the crash case
this row describes. There is no daemon-side "kill when the client disconnects".

Three composable measures, all built on seams the repo already has:

1. **Label every container** with `ork.run`, `ork.task`, `ork.attempt` and
   `ork.orker`. §9.2 already specified the first three; `ork.orker` is added so
   an Orker can recognize its *own* previous life's containers.
2. **A startup reaper.** On boot the Orker lists
   `--filter label=ork.orker=<its own id>` and force-removes survivors
   (`internal/orker/runtime_docker.go`; `dispatcher_integration_test.go`
   asserts no leftovers by label).
3. **A runtime-owned lifetime bound**, because a reaper cannot reach the
   containers of an Orker that never comes back. The container's own
   `timeout_seconds` must be enforced by something inside the container's own
   lifetime — a daemon-side stop timeout, or a supervisor in the container's
   namespace — so an abandoned container terminates on its own budget rather
   than running forever on a host nobody is watching.

Duplicate *execution* is inside the at-least-once contract (AGENTS.md §4).
Duplicate *resource consumption* is not a correctness failure but it is a real
operational cost, and the design says so rather than claiming the host is clean.

---

## 6. Orker dispatch: the `OCIDispatcher`

### 6.1 Shape

One implementation runs every activity:

```go
// internal/orker/oci_dispatcher.go
type OCIDispatcher struct {
    Runtime   ContainerRuntime   // the only Docker-aware seam in the repo
    Store     artifacts.Store    // artifacts + values + streams
    Commits   artifacts.CommitStore
    Refs      artifacts.RefPredictor  // step-4 commit bound, no I/O (§5.4)
    Logs      *LogSink
    Policy    SecurityPolicy     // caps, forbidden flags, defaults (§9)
    WorkRoot  string
    OrkerID   string
    BuildID   string
}

func (d *OCIDispatcher) Execute(ctx context.Context, in *orkstackv1.ExecuteTaskInput) (*orkstackv1.ExecuteTaskResult, error)
```

```go
// internal/orker/container_runtime.go — portability boundary, not a framework
type ContainerRuntime interface {
    Pull(ctx context.Context, pinned string) (PullInfo, error)   // PullInfo: platform digest, platform, cached bool
    // LoadLayout imports the image addressed by digest from a local OCI image
    // layout — the embedded fallback of §4.6 — and returns the PullInfo a
    // Pull would have. A digest the layout does not hold returns an error
    // matching ErrNotInLayout.
    LoadLayout(ctx context.Context, layoutPath, digest string) (PullInfo, error)
    Create(ctx context.Context, spec ContainerSpec) (ContainerID, error)
    Start(ctx context.Context, id ContainerID, stdout, stderr io.Writer) error
    Signal(ctx context.Context, id ContainerID, sig Signal) error
    Wait(ctx context.Context, id ContainerID) (ExitStatus, error)
    Remove(ctx context.Context, id ContainerID) error
}
```

Exactly one implementation ships: `DockerRuntime`, shelling out to `docker`
(`internal/orker/runtime_docker.go`). The interface exists so
Podman, containerd, Kubernetes Jobs, ECS or Nomad remain *possible*, not because
any of them is planned. **No Docker-specific detail may appear outside
`internal/orker/runtime_docker.go`.**

**The interface split is also the failure classification.** `Create` and
`Start` return `error`; `Wait` returns `(ExitStatus, error)`. A container that
never started never reaches `Wait`. So "the runtime refused" and "the workload
failed" are distinguished by *which call failed*, not by inspecting a number —
which matters because exit codes 125, 126 and 127 carry no runtime guarantee
(§8.2, measured in §14.13).

### 6.3 Heartbeats and cancellation

This is the part most likely to be got wrong. Temporal delivers cancellation
to an activity **only through a heartbeat response**: an activity that never
calls `RecordHeartbeat` never observes `ctx.Done()`, so an Orker that stopped
heartbeating during the container run could never cancel it.

Required, all four together (`internal/orker/heartbeat.go`):

1. **Engine** sets `HeartbeatTimeout: 30 * time.Second` and
   `WaitForCancellation: true` in `ActivityOptions`
   (`internal/engine/workflow.go`, `schedule()`). Without the timeout a dead
   worker is only noticed at `StartToCloseTimeout`. Without
   `WaitForCancellation` the workflow may mark the task terminal before the
   Orker finishes killing and cleaning up.
2. **Orker** runs a heartbeat goroutine for the **entire** container lifetime,
   ticking every `ORK_ORKER_HEARTBEAT_INTERVAL` (default 5 s, bounded to
   `1s`–`10s`; docs/DEPLOY.md §1), carrying a small progress detail
   (`phase`, `container id`, `bytes logged`, and the Orker's own id —
   `ORK_ORKER_ID`, never the SDK's `pid@hostname` worker identity). The
   detail's shape is `task.HeartbeatProgress` (`internal/task/contract.go`),
   because the engine reads it back: `RunService.InspectRun` reports, for
   every pending attempt, the detail Temporal holds for it, refusing a
   previous attempt's carried-over detail and flagging a beat older than
   `task.HeartbeatStaleAfter` (two ceiling ticks) stale. The container sees
   none of this.
3. **Orker** caps its task worker's `worker.Options.MaxHeartbeatThrottleInterval`
   to that same interval (`cmd/orker/main.go`, `workerOptions`). This is the
   half that is invisible until somebody cancels. The Go SDK does not send
   every `RecordHeartbeat`: it batches them into windows of
   `min(0.8 × HeartbeatTimeout, MaxHeartbeatThrottleInterval)` — inside a window
   a heartbeat only overwrites the pending detail, and no RPC leaves the
   process. Since cancellation rides the RPC's *response*, that window **is**
   the cancellation latency: with the cap unset it is 24 s no matter how fast
   the loop above ticks. Measured on a real cluster before the cap existed: a
   failing task's still-running sibling was asked to stop 52 ms after the
   failure and learned of it 23.86 s later; SIGTERM to cleanup then took
   296 ms, and the task's 10 s grace period never engaged. The same window is
   also the RPC's own deadline, which makes the interval the longest Temporal
   frontend outage a running container survives — the tradeoff that makes it an
   operator setting rather than a constant. A test harness that runs the real
   dispatcher against a real Temporal must set both, or it measures the SDK's
   default.
4. **Orker** reacts to `ctx.Done()` with the §3.3 sequence:

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
| `StartToCloseTimeout` | Temporal | `timeout_seconds + grace + transfer_budget` | Backstop for an Orker wedged outside the container run. `TIMEOUT_TYPE_START_TO_CLOSE`. |

**`transfer_budget` is derived, not a constant.** An earlier draft used a fixed
120 s, which is the one number in this section with no measurement behind it,
and it is not survivable: inside that window sit input staging (step 5), the
image pull (step 7), hashing (step 12) and uploading up to
`artifact_max_count` × `artifact_max_bytes` — 256 × 5 GiB at the documented
defaults. Worse, `retryPolicyFor` returns `MaximumAttempts: 1` whenever
`retry:` is absent or `<= 1` (`internal/engine/workflow.go:632-637`), so a
legal large upload would be **terminal on its first timeout, with no retry at
all**.

Temporal's own guidance for a long-running activity is a generous
Start-to-Close paired with a short Heartbeat timeout, because heartbeats do not
reset Start-to-Close. §6.3's heartbeat loop already supplies dead-worker
detection, so Start-to-Close only has to be large enough not to kill honest
work:

```
# Resolved once, at submit, in internal/api. Carried as
# RunPlan.TransferBudgetSeconds (§5.2). The workflow only adds it up.

transfer_budget_seconds =
    ORK_TRANSFER_BUDGET_SECONDS                          if set
    else max(300,
             limits.artifact_max_count × limits.artifact_max_bytes
             ÷ ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC)       default 10_485_760 (10 MiB/s)

StartToCloseTimeout = timeout_seconds + cancellation_grace + plan.TransferBudgetSeconds
```

Three details that an earlier draft got wrong and that an implementer needs.
**The variables are engine-scoped**, `ORK_TRANSFER_BUDGET_SECONDS` and
`ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC`, not `ORK_ORKER_`-prefixed: they are read
by `internal/api` at submit, and §5.2's own argument is that `ORK_ORKER_*` is
Orker-local and cannot travel. **The floor throughput has a stated default**
(10 MiB/s) — a formula with an unspecified denominator is not a formula.
**And the result needs a carrier**: `ActivityOptions` is set in workflow code,
which may not read process configuration, so the resolved integer travels on
`RunPlan` exactly as `CommitRoot` does. The workflow then computes the timeout
as a pure function of its arguments.

At the documented defaults this yields `max(300, 256 × 5 GiB ÷ 10 MiB/s)` ≈ 36
hours, which is deliberately generous: the heartbeat timeout, not this one,
detects a dead worker. An operator who knows their storage sets the seconds
directly.

### 6.4 Orker state

The Orker stays stateless and disposable. Its only persistent local state is the
OCI image cache, per-attempt execution directories (removed on every exit path),
and an optional transient artifact download cache. If an Orker disappears,
Temporal retries elsewhere and the commit protocol decides whether the container
runs again.

A pin's shared directory (§3.1) is a second, longer-lived kind of ephemeral
execution directory: its lifetime is the pin's rather than one attempt's, so it
survives between two tasks and between two attempts of one task. It is still
ephemeral, local, disposable and reconstructible by re-running. It is bounded by
a lease written beside it, removed explicitly once every member task is
terminal, and reaped at Orker boot — a pin can never outlive the worker process
that opened its Temporal session, because a fresh session-resource id is minted
per worker start, so every pin directory found at boot is dead by construction
rather than by heuristic. The pinned task is also the one place where "Temporal
retries elsewhere" does not apply: the pin and its run fail explicitly
(`PIN_HOST_LOST`) instead (AGENTS.md §5).

### 6.5 Pool liveness

`DescribeOrker` is the pool-liveness probe: the engine dispatches it as a
Temporal activity on the pool's own channel with 5 s / 10 s timeouts and
`MaximumAttempts: 2`, so a pool proves it is alive only when an Orker is
actually polling it, and a default-pool failure maps to `Unavailable` in
`internal/api`. The Admin **Activities** page renders the engine's activity
catalog (name → reference → digest), because that is engine configuration, not
something to ask an Orker.

---

## 7. Activity lifecycle

### 7.1 The sequence

```
 1. OPEN LOG SINK                     first, always; deferred Close before return
 2. HEARTBEAT LOOP START              5 s tick (configurable), until step 15
 3. COMMIT CHECK      GET commit_uri  → adopt and jump to 15, or proceed  (§5.4)
 4. WORKSPACE         mkdir per-attempt 0700; input/ 0555, output/ 1777, tmp/ 1777
 5. INPUT STAGE       download upstream artifacts   → /orkstack/input/artifacts/
                      download upstream values.json → /orkstack/input/upstream/
                      resolve secrets               → /orkstack/input/secrets/
                      (a cached task: values.json only; artifacts and secrets deferred)
 5b. MEMO             a cached task only: render config.json, compute the key,
                      look up the memo (§5.4.1) → a HIT writes this run's
                      commit and jumps to 15; anything else continues
 5c. STAGE ARTIFACTS  a cached task only: the deferred downloads, then secrets
 6. IMAGE             Runtime.Pull(image.pinned), or the verified embedded
                      layout (§4.6); record platform_digest and delivery
 7. RENDER            with: → config.json ; run_input → run.json ; task.json
 8. CREATE            build ContainerSpec from the security profile (§9)
 9. RUN               Start; stream stdout/stderr to the sink and to disk
10. WAIT              Wait for exit, cancellation, or the task timeout
                      cancellation → SIGTERM → grace → SIGKILL  (§6.3)
11. EXIT CAPTURE      exit code recorded; non-zero → classify and go to 14
12. VALIDATE          values.json + output/artifacts/, INCLUDING readability
                      (§3.1, §3.4, §3.5), then bound the projected commit
                      (§5.4 step 4) before any byte is uploaded
13. UPLOAD + COMMIT   hash → artifacts → values.json → streams → commit.json (§5.4)
14. FAILURE PATH      SCAN output/artifacts/ under the same §3.5 + readability
                      rules, then upload streams and the artifacts that passed;
                      attach refs and any scan note to the error; write NO
                      commit; return the workload's error — never a storage
                      error, and never a scan error, in its place
                      (`ActivityFailureDetail`, §2.2)
15. CLEANUP           Remove container (detached ctx), RemoveAll workdir,
                      stop heartbeat, close log sink, return
16. ATTEMPT RECORD    write one TaskAttempt to
                      metadata/attempts/{run}/{task}/{attempt}.json — outcome,
                      error, exit code, the phase this attempt ended in and
                      the phases it passed through (each with its own start
                      and end, contiguous, the last closing at finished_at),
                      the image as executed, orker/pool/build; detached ctx,
                      BEST-EFFORT (a write failure is a Warn and nothing else)
```

**Step 16 changes nothing for a container.** It is an Orker-side projection:
`task.json`, the environment, the exit-code semantics and every other part of
this contract are untouched, and an activity image needs to do nothing to
participate. It exists because Temporal owns retries, so the workflow only ever
sees a task's FINAL outcome — when attempt 1 fails, its error is consumed by the
scheduler for attempt 2 and nothing durable is left behind. The Orker is the
only process that observes every attempt, and `end()` in `Execute` is its
exactly-once-per-attempt seam, so that is where the record is written
(`internal/orker/oci_dispatcher.go`, `recordAttempt`; the store is
`pkg/artifacts/attempts.go`, and `docs/STORAGE.md` carries the key family).

It runs on a DETACHED context, like the container removal above it: a CANCELED
attempt is exactly the attempt somebody most wants the record of, and its own
context is already done. Succeeded and adopted attempts get a record too, so a
reader never has to synthesize the last row of a timeline from `TaskRun` and
`commit.json` — two producers of one row is how they come to disagree.

**A record is never a failure.** `Attempts` unset writes nothing, and a write
that fails is logged at Warn with the attempt's coordinates. The attempt's own
outcome — the `ExecuteTaskResult` or the `ApplicationError` — is what the
engine reports, unchanged. Retry *visibility* must not become a new way for
work to fail, which is the same projection-not-orchestration rule the engine's
`emit()` already applies to `PublishEvent`.

§5.4's "no cross-attempt state" rule still holds: an Orker WRITES this record
and never reads one. Nothing an attempt does depends on what an earlier attempt
recorded.

**IMAGE comes before RENDER, and the order is load-bearing.** An earlier draft
rendered at step 6 and pulled at step 7. But `task.json` carries
`image.platformDigest`, `image.platform` and `image.delivery` (§3.2), and none
of those exists until the image is resolved — so under that order all three
keys were *always* absent, and the §3.2 example showing them populated was
unreachable. The pull has no dependency on the workspace: `Runtime.Pull` needs
only `ImageRef.pinned` from the input. Pulling first also fails faster, since
`IMAGE_PULL_FAILED` is then raised before any staging bandwidth is spent.

The alternative — render, then rewrite `task.json` after the pull — is worse:
it makes an input file mutable during setup, and §3.1's permission model
depends on `input/` being sealed `0555`/`0444` before the container starts.

Step 14's ordering is not cosmetic: a workload's exit-1 is reported as exit-1
even if the stderr upload also failed — replacing it with a storage error would
tell the author their build failed because of the bucket
(`internal/orker/oci_dispatcher.go`, the cleanup path; held by
`cmd/orkctl/admin_surface_integration_test.go`).

**The failure path scans too.** An earlier draft sent a non-zero exit from step
11 straight to step 14, so the regular-file, symlink, device, FIFO,
readability, count and size rules of §3.5 applied *only* on the success path —
leaving them unenforced on exactly the path a broken or hostile image is most
likely to take. Step 14 therefore runs the same scan. What differs is the
consequence: a violation here is **a note attached to the workload's error**,
never a replacement for it and never a separate failure. A task that exited 1
*and* wrote a symlink into `output/artifacts/` is reported as exit 1, with the
symlink named in the error details and not uploaded.

Discarding everything but `stdout.log`/`stderr.log` on failure would be simpler
and is rejected: the compiler errors, test reports and core dumps a failing
task produces are what that test exists to hold.

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
and `TimedOut` is permanent by default. It is recorded here rather than
resolved quietly, because **flipping it is a one-line change** — remove
`"TimedOut"` from `NonRetryableErrorTypes` and change one row in §8.2.

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
| `CONTAINER_EXIT_NONZERO` | ActivityFailed | yes | **every** non-zero status returned by `Wait`, whatever the number |
| `CONTAINER_TIMEOUT` | TimedOut | **no** (see §8.1) | Orker killed the container at `timeout_seconds` |
| `RUNTIME_REFUSED` | InfrastructureError | yes | `Create` or `Start` returned an error — the container never ran. Sourced from the runtime error, never from an exit code. |
| `WORKSPACE_NOT_BINDABLE` | InfrastructureError | no | daemon cannot bind the workspace path (`bind source path does not exist`) — a host misconfiguration retrying will not fix |
| `WORKDIR_NOT_BINDABLE` | InfrastructureError | no | the Orker's boot probe found `ORK_ORKER_WORKDIR` itself not bind-mountable (§3.1); the Orker refuses to start |
| `IMAGE_PULL_FAILED` | InfrastructureError | yes | pull failed or timed out |
| `IMAGE_DIGEST_MISMATCH` | ContractViolation | no | pulled content does not match `image.pinned` |
| `IMAGE_NOT_PINNED` | ContractViolation | no | `ImageRef.pinned` empty or not `…@sha256:…` |
| `VALUES_MISSING` | ContractViolation | no | exit 0 with no `/orkstack/output/values.json` |
| `VALUES_MALFORMED` | ContractViolation | no | not valid UTF-8 JSON, or not a top-level object |
| `VALUES_TOO_LARGE` | ContractViolation | no | over `limits.valuesMaxBytes` |
| `ARTIFACT_NOT_A_FILE` | ContractViolation | no | a directory, symlink, device, socket or FIFO in `output/artifacts/` |
| `ARTIFACT_LIMIT_EXCEEDED` | ContractViolation | no | too many artifacts, or one too large |
| `ARTIFACT_NAME_NOT_UTF8` | ContractViolation | no | an `output/artifacts/` file name is not valid UTF-8; no protobuf encoding can carry it (§3.5, §14.16) |
| `ARTIFACT_UPLOAD_FAILED` | InfrastructureError | yes | store `Put` failed |
| `INPUT_ARTIFACT_UNAVAILABLE` | InfrastructureError | yes | an upstream artifact could not be fetched |
| `INPUT_ARTIFACT_CORRUPT` | ContractViolation | no | fetched bytes do not match the recorded sha256 |
| `INPUT_TOO_LARGE` | ContractViolation | no | `ExecuteTaskInput` would exceed 256 KiB (raised by the engine) |
| `RENDER_FAILED` | ContractViolation | no | template error, missing key, or `<no value>` |
| `WORKDIR_FAILED` | InfrastructureError | yes | per-attempt directory could not be created |
| `SECRET_UNRESOLVED` | InfrastructureError | yes | a `SecretRef.source` could not be resolved |
| `SECRET_SCHEME_FORBIDDEN` | ContractViolation | no | Orker policy does not allow that source scheme |
| `SECRET_NOT_PERMITTED` | ContractViolation | no | the source is outside this Orker's `ORK_ORKER_SECRET_ALLOW` floor |
| `COMMIT_READ_FAILED` | InfrastructureError | yes | commit store unreadable at step 0 |
| `COMMIT_WRITE_FAILED` | InfrastructureError | yes | conditional PUT failed for a non-precondition reason |
| `COMMIT_SCHEMA_UNSUPPORTED` | ContractViolation | no | existing commit has an unknown `schema_version` |
| `COMMIT_IDENTITY_MISMATCH` | ContractViolation | no | existing commit differs on one of the seven identity fields (§5.4) |
| `COMMIT_UNDECODABLE` | ContractViolation | no | an object exists at `commit_uri` but is still not valid `ActivityCommit` JSON after the in-attempt reread (§5.4) |
| `COMMIT_TOO_LARGE` | ContractViolation | no | the projected `ActivityCommit` exceeds `limits.commitMaxBytes`, measured at step 4 before any upload |
| `COMMIT_STORE_NOT_CONDITIONAL` | InfrastructureError | no | the boot-time two-writer race proved the object store does not implement conditional create; the Orker refuses to start (§5.3) |
| `OUTPUT_NOT_READABLE` | ContractViolation | no | the Orker cannot open a file the workload wrote; the message names path, mode and owner, and the fix is `umask 022` (§3.1) |
| `VALUES_CORRUPT` | ContractViolation | no | on adoption, the refetched `values.json` does not match `commit.values.sha256` (§5.4) |
| `VALUES_REFETCH_FAILED` | InfrastructureError | yes | on adoption, `commit.values.uri` could not be fetched (§5.4) |
| `RESOURCE_REQUEST_REFUSED` | ContractViolation | no | request exceeds the Orker's policy cap |
| `NETWORK_MODE_REFUSED` | ContractViolation | no | requested mode is not `default` or `none` |
| `EACH_ITEM_UNAVAILABLE` | ContractViolation | no | an item's `each.over` no longer resolves to the array, of the length, the engine counted — unreachable while the store honours its digests, stated rather than guessed at |

**There is no exit-code table, deliberately.** An earlier draft classified exit
126 as `CONTAINER_NOT_EXECUTABLE` and 127 as `CONTAINER_COMMAND_NOT_FOUND`,
both `ContractViolation`, both never retried. Measurement kills that: a
workload choosing `exit 126` produces `.State.ExitCode == 126`, byte-identical
to the CLI's own diagnostic for a missing entrypoint (§14.13). Since POSIX
makes 127 the shell's ordinary "command not found" and 126 "not executable", a
*routine* `activity-shell` failure lands on 127 — and the draft would have
permanently refused to retry it. AGENTS.md §3.2 says exactly this: *"Any
non-zero exit means the activity failed. Retry behavior belongs to Temporal
workflow policy, not magic exit codes such as 42, 43, or 44."*

So the two codes are deleted rather than kept with a caveat. The signal that
genuinely distinguishes "the runtime refused" from "the workload failed" lives
at the interface boundary, not in a number: `Create`/`Start` returning an error
means the container never ran (`RUNTIME_REFUSED`, or `WORKSPACE_NOT_BINDABLE`
when the runtime error names an unbindable source); every status `Wait`
returns is the workload's own and is `ActivityFailed`.

### 8.3 Engine side

The engine (`internal/engine/workflow.go`) reads the
`ApplicationError.Type()` and details and populates
`Error{code: <reason>, message, details, retryable, category}`. The event type
stays `TASK_FAILED`; the category is data on the error, so the UI can say
"contract violation — this image did not write values.json" instead of a Go
error string. Cancellation keeps its existing special case
(`temporal.IsCanceledError` → `STATUS_CANCELED`, projected on a disconnected
context).

It also records **why retrying stopped**: `*temporal.ActivityError` carries a
`RetryState()`, and `recordFailure` maps it onto `TaskRun.retry_state`
(`RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED`, `_NON_RETRYABLE_FAILURE`, `_TIMEOUT`,
`_CANCEL_REQUESTED`, …). Three attempts and a terminal failure otherwise look
identical on the snapshot to one non-retryable attempt. It is derived, data-only
and unversioned: it issues no command, changes no decision, and reaches
Temporal history through nothing.

The Orker's own §8.2 reason for each individual attempt — including the ones
Temporal discarded — is in that attempt's record (step 16), spelled in the same
`Error{code, message, details, retryable, category}` shape this section
describes, so a timeline's intermediate rows and its final row use one
vocabulary (`Failure.ErrorProto`, `internal/orker/failure.go`).

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
--label ork.orker=<orker-id>                  the startup reaper's key (§5.5)
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
# first-party images pin their own bases
FROM alpine:3.20@sha256:…
# jq is the only "SDK": a JSON reader
RUN apk add --no-cache jq
COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh
# never root
USER 65532:65532
# exec form, mandatory
ENTRYPOINT ["/usr/local/bin/entry.sh"]
```

**The comments are on their own lines, and that is load-bearing rather than a
style choice.** A Dockerfile has no inline comment syntax: `#` introduces a
comment only as the first non-whitespace character of a line, and anywhere
else it is ordinary argument text. An annotation trailing its instruction —
`USER 65532:65532   # never root` — makes the group `65532   # never root`,
and **every container is refused before it starts**.

Rules, all mandatory for first-party images and recommended for everyone:

1. **`ENTRYPOINT` in exec form.** Shell form runs the process as a child of a
   shell that does not forward signals, so SIGTERM never arrives and every
   cancellation degrades to SIGKILL.
2. **`exec` only when the script produces nothing itself.** A script that hands
   off and is finished with — a thin wrapper — ends in `exec "$@"`, so signals
   reach the workload directly. **A script that must write `values.json` after
   the workload exits MUST NOT use `exec`**, because `exec` replaces the shell
   and nothing after it can run; every successful run would then end in
   `ContractViolation/VALUES_MISSING`. Such a script MUST instead run the
   workload in the background, install a TERM trap that forwards to `$!`, and
   `wait`. See rule 2a. This exception exists because measurement showed the
   unqualified rule to be unfollowable (§14.14).
2a. **The background-and-trap shape, exactly.** Three shapes were measured
   (§14.14) and only one satisfies the contract:

   | Shape | Workload sees SIGTERM? | `values.json` written? | Exit |
   |---|---|---|---|
   | `sh -c "$cmd"; rc=$?; …` (naive) | **no** | **no** | 143 |
   | `exec sh -c "$cmd"` | yes | **no** | 0 |
   | background + trap + `wait` | yes | yes | 0 |

   The naive shape fails because `--init` forwards SIGTERM to its **direct
   child only**: tini signals one process and Docker injects `docker-init --`
   without `-g`, so `entry.sh` — a shell with no trap — dies on the default
   disposition and takes the grandchild with it. It is not the "degrades to
   SIGKILL at the grace boundary" one might assume; it is faster and more
   total, and §10.3's own promise that "SIGTERM is honored within the grace
   period" would fail for the platform's own reference image.
2b. **`run_supervised` must never be called inside `$( )` or on either side of
   a pipeline.** This is the same bug one level deeper, and the specified
   script had it: `value=$(run_supervised "$cmd")` put the trap in a
   command-substitution subshell, so `op=eval` failed at **exit 143 with no
   `values.json` and empty logs** — twice in a row, on the image built verbatim
   from this section (§14.18). POSIX.1-2024 §2.6.3 makes a command
   substitution a subshell, and POSIX §2.13 says a subshell resets *"traps that are
   not being ignored … to the default action"*, so the trap never protects
   `entry.sh`. A pipeline fails identically — POSIX §2.13: *"each command of a
   multi-command pipeline is in a subshell environment"*. **Plain invocation
   and redirection are the only safe forms**; POSIX §2.7 confirms redirection
   alone creates no subshell. The verified shape is in §10.2.
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
8. **`with.command` is a JSON array, executed as argv** — never a shell string
   the image passes to `sh -c`. A rendered value then cannot become code
   whatever it contains (§10.2, verified §14.18). An image that genuinely needs
   a shell takes `["sh", "-c", "…"]`, which puts the boundary in the document.

### 10.2 The entry-script contract

This script is not illustrative. It was built into an image exactly as written
and exercised on every op, cancelled and successful (§14.18); the previous
draft of it failed two of those cases.

```sh
#!/bin/sh
# images/activity-shell/entry.sh — the whole SDK is jq and a redirect.
set -u
umask 022        # rule 7: outputs must be readable by the Orker (§3.1)

CONFIG=/orkstack/input/config.json
VALUES=/orkstack/output/values.json
TMP=/orkstack/tmp

op=$(jq -r '.op // "exec"' "$CONFIG")

# `command` is a JSON ARRAY — argv, not a shell string (rule 8). jq's @sh
# quotes each element, so `eval set --` cannot be escaped by any value in it;
# the substitution runs before any workload starts, so no trap is at risk here.
eval "set -- $(jq -r '(.command // []) | @sh' "$CONFIG")"

# run_supervised runs its ARGUMENTS as the workload, in the background, with a
# TERM trap that forwards to the child, and returns the child's real status.
#
# MUST NOT be called inside $( ) or on either side of a pipeline (rule 2b):
# both create a subshell, which resets the trap and loses cancellation.
run_supervised() {
    "$@" &
    child=$!
    trap 'kill -TERM "$child" 2>/dev/null' TERM INT
    wait "$child"
    rc=$?
    # POSIX: a trapped signal makes `wait` return >128 IMMEDIATELY, before the
    # child has actually finished. Wait again on that path only — a second
    # unconditional wait would discard the status when no signal arrived.
    if [ "$rc" -gt 128 ]; then
        wait "$child"
        rc=$?
    fi
    trap - TERM INT
    return "$rc"
}

case "$op" in
  exec)
    # The child's exit status IS the activity's exit status. No magic codes.
    run_supervised "$@"
    rc=$?
    printf '{"exitCode":%d}\n' "$rc" > "$VALUES"
    exit "$rc"
    ;;
  script)
    # `.script` is the program text; `.interpreter` overrides the shebang.
    jq -r '.script' "$CONFIG" > "$TMP/script"
    chmod 0755 "$TMP/script"
    interp=$(jq -r '.interpreter // empty' "$CONFIG")
    if [ -n "$interp" ]; then
        run_supervised "$interp" "$TMP/script" "$@"
    elif head -c 2 "$TMP/script" | grep -q '#!'; then
        run_supervised "$TMP/script" "$@"
    else
        run_supervised sh "$TMP/script" "$@"
    fi
    rc=$?
    jq -n --arg p "$TMP/script" --argjson rc "$rc" \
       '{scriptPath:$p, exitCode:$rc}' > "$VALUES"
    exit "$rc"
    ;;
  eval)
    # `eval` is documented to treat a non-zero exit as DATA: the evaluation
    # ran, so the activity succeeded. This is one image's contract, not a
    # platform rule, and it is the only way to express it under exit-code
    # semantics without the platform inventing a magic code.
    #
    # REDIRECTION, not $( ): a substitution would put run_supervised in a
    # subshell and lose the trap (rule 2b), and it would also swallow the
    # workload's stdout so §3.6's durable stdout.log came out empty.
    run_supervised "$@" > "$TMP/stdout"
    rc=$?
    cat "$TMP/stdout"          # stdout still reaches the container log stream
    value=$(cat "$TMP/stdout") # safe: no supervised child is running now
    jq -n --arg v "$value" --argjson rc "$rc" '{value:$v, exitCode:$rc}' > "$VALUES"
    exit 0
    ;;
  *)
    echo "activity-shell: unknown op '$op'" >&2
    exit 2
    ;;
esac
```

Note `set -u` rather than `set -eu`: `-e` would abort the script on the
workload's first non-zero status, before `values.json` is written, which is the
same failure rule 2a exists to prevent.

**`with.command` is an argv array, and that is the shell-injection fix.**
AGENTS.md §6 requires that *"shell commands must not interpolate untrusted
input without the templating layer's escaping"*. Rather than rely on every
author remembering the `sh` quote helper, the shell boundary is removed from
the template: `command` is a JSON array, each element becomes one argv
element, and a rendered value can never become code. Verified (§14.18):
`command: ["echo", "hello; touch /orkstack/output/artifacts/PWNED"]` printed
the payload as data and created no file.

An author who genuinely wants a shell writes `["sh", "-c", "…"]`, which makes
the shell boundary visible in the document rather than implicit in the image.
For that case the `sh` helper (POSIX single-quote wrapping with `'\''`
escaping, §3.3) makes `{{ sh .inputs.name }}` safe and satisfies AGENTS.md §6's
sentence literally; the array form remains the documented default because it
needs no author vigilance.

The three first-party images:

| Image | `op` values | `values.json` | Artifacts |
|---|---|---|---|
| `activity-shell` | `exec`, `eval`, `script` | `exitCode`; `eval` adds `value`; `script` adds `scriptPath` | files the script writes to `output/artifacts/` |
| `activity-http` | `get`, `post`, `put`, `patch`, `delete`, `head` | `status`, `headers`, `durationMs`, and `json` when the body parses and is small | the response body when `captureBodyAs` is set |
| `activity-file` | `copy`, `checksum`, `gzip`, `gunzip`, `tar`, `untar`, `concat` | `path`, `sizeBytes`, `sha256` | exactly one per op |

**There is no `activity-container` image, and there cannot be one.** Inside
an activity container, §9.1 forbids every mechanism that could reach a Docker
daemon — the Docker socket as a mount source *or* target, any credential mount,
`--privileged`, `--device`, and host namespaces — so no compliant image could
start containers, and inventing a privileged exception for one first-party
image is exactly what AGENTS.md §9 forbids ("first-party activities must use
exactly the same OCI contract… do not create a privileged internal plugin
system").

None is needed either, because under this contract **every task already is a
container**: name the image in `uses:` and give it a command.

```yaml
- id: migrate
  uses: ghcr.io/acme/migrate:v3
  command: ["/bin/migrate"]
```

**A templated image is deliberately unsupported.**
An image selected with `image: "{{ .inputs.image }}"` cannot be resolved to a
digest at apply time, because a run input does not exist until submit — later
than apply, and later still than the version document that must carry the
pin. AGENTS.md §5 forbids running an unresolved reference, so a templated
`uses:` is a compile error (`IMAGE_NOT_PINNED` at validation, not at run time).

Two semantics are documented image contracts, not platform rules:

- **`activity-http` exits 0 on a non-2xx response** and reports `status` in
  `values.json`. An HTTP status is data; an activity failure is a failure to
  perform the request.
- **`activity-shell`'s `eval`** treats a non-zero child exit as data, as above.
  `exec` and `script` propagate it.

`activity-file`'s operation set is AGENTS.md §9's
`copy/checksum/gzip/gunzip/tar/untar/concat`. Its paths are workspace paths:
reads resolve under `/orkstack/input/`, writes land in
`/orkstack/output/artifacts/`. Every shipped example is compiled by
`TestCompile_AcceptsEveryShippedExample`.

### 10.3 The contract test suite

```bash
orkctl activity test <image>
```

runs any image — first-party, community, or a user's — against the ABI, with no
Orkflow and no Temporal. It asserts: the container starts; `/orkstack/input` is
present and read-only; `task.json` is valid and complete against the §3.2
table; `values.json` is written, is an object, and is within the limit;
`output/artifacts/` holds only regular files; artifact names round-trip;
oversize outputs are rejected; SIGTERM is honored within the grace period; a
non-zero exit is reported as a non-zero exit; and the task timeout kills a
hung container.

Six assertions exist because probes found real failures they would have caught:

- **SIGTERM actually reaches the workload — on every op.** Assert both halves:
  the workload observes SIGTERM *and* `values.json` exists afterwards. Run it
  against **each** op the image declares, not one representative shape: the
  script in §10.2's previous draft passed on `exec` and failed on `eval` at
  exit 143 with no `values.json` and empty logs (§14.18). A per-shape test
  would not have caught that; a per-op test does, in about a second.
- **Workload stdout reaches the log stream.** Assert the container's stdout
  contains what the workload printed, not just that `values.json` is correct.
  The `eval` command-substitution defect was invisible to any values-only
  check: `values.json` was right while the durable `stdout.log` would have been
  empty for every `eval` task (§14.18), silently voiding §3.6's regression gate.
- **Exit codes are not interpreted.** A table fixture over 0, 1, 42, 125, 126
  and 127 asserting every non-zero maps to `ActivityFailed`, plus a separate
  case proving a `Create`/`Start` failure maps to `InfrastructureError`. These
  are the two halves §8.2 distinguishes, and the measurement that forced them
  is §14.13.
- **Output modes.** Every file the image writes must be readable by a user
  other than the one that wrote it. A dedicated fixture runs the image under
  `umask 077` and asserts the suite reports `OUTPUT_NOT_READABLE` rather than
  hanging, looping, or silently succeeding (§3.1, §14.8). This is a two-line
  falsification of the whole permission model and belongs in the suite that
  exists for exactly that.
- **Commit bound, and projection equality.** A fixture that emits
  `artifactMaxCount` artifacts must produce a commit within `commitMaxBytes`;
  the equivalent unit test fails against the earlier 64 KiB figure at 248 refs
  (§14.7). Additionally assert that the step-4 projection is **byte-identical**
  to the manifest step 8 commits — that is the property `RefFor` exists to
  guarantee (§5.4), and nothing else checks it.
- **Artifact-name rejection.** A fixture writing an output file whose name
  contains `0xff` must fail with `ARTIFACT_NAME_NOT_UTF8` at the scan, not at
  the commit. Without it the failure surfaces only after upload (§14.16).

Two more belong to the chaos matrix rather than the image suite, and §11.2
carries them: **a tampered embedded layout** must fail
`IMAGE_DIGEST_MISMATCH` in OrkStack code and never reach the runtime (§14.15
shows the runtime will not catch it), and **an orphan check** — SIGKILL the
Orker mid-container, then assert no container carrying `ork.run=<run>` survives
once the replacement Orker boots (§14.17).

Fixtures in Bash, Go and Python ship with it; **the Bash fixture is mandatory
and is the design's own falsification test**. If the Bash fixture needs anything
beyond `cat`, `jq` and a redirect, this design is wrong (AGENTS.md §10).

### 10.4 Running one task locally

```bash
orkctl task run --from-run RUN --task TASK [--file FILE.ork.yaml] [--input k=v]…
orkctl task run --file FILE.ork.yaml --task TASK [--output TASK=JSON]… [--artifact TASK/NAME=PATH]…
```

answers the question §10.3 does not: *why did this task, in this document, with
those inputs, fail?* It runs **one** task of an Orkflow on the caller's machine
and prints the rendered `config.json` before the container starts.

**What it shares with an Orker.** Nothing about the task's execution is
reimplemented:

- the dispatcher is the Orker's `OCIDispatcher`, built by the Orker's own
  constructor (`orker.NewActivitiesWithOptions`), on this machine's container
  runtime;
- the renderer is the Orker's (`missingkey=error`, the `artifact`, `env` and
  `sh` helpers), so a `with:` that fails here fails `RENDER_FAILED` exactly as
  it would on an Orker;
- the workspace is the Orker's: the same `/orkstack` tree, the same §3.1
  modes, the same §9.2 container constants, and resources capped by this
  machine's `ORK_ORKER_*` policy, read exactly as an Orker reads it;
- the `ExecuteTaskInput` is composed by `engine.LocalTaskInput`, which a
  parity test holds equal to every input `RunWorkflow` schedules.

With `--from-run`, every upstream value and artifact is the run's committed
bytes, read through the engine's namespace-checked, integrity-verified
`ReadArtifact` and re-verified against their sha256 when staged. A FAILED,
SKIPPED or never-run ancestor contributes nothing, as in a run. With `--file`,
the edited document is prepared by `ValidateOrkflow` — compiled against the
deployment's catalog, images resolved to digests — and an ERROR refuses.
`--output` and `--artifact` supply an upstream task entirely from local files.

**What differs, deliberately.** The local input carries no `commit_uri` (a
local run has no durable result), no `pin` (the CLI mints a fresh pin context
for a pinned task, whose `/orkstack/shared` starts empty unless `--shared`
seeds it), no `telemetry`, no `image_policy` (a local run does not enter the
dispatcher's image-policy step, and a `--file` document already met the
policy at `ValidateOrkflow`), and the contract-default limits rather than the
deployment's configured ones, which no client can read. `condition:` is not
evaluated (`orkctl run plan` answers that) and `retry:` is not applied: a local
run is one attempt.

**Awaited tasks.** An awaited task (`await:`, `docs/DSL.md`) starts no
container, so there is nothing to run locally: naming one refuses with exit `1`
and points at `orkctl run resolve`. A task downstream of an await is an
ordinary local run; with `--from-run` a COMPLETED await's values are staged like
any upstream's, and otherwise `--output AWAIT=JSON` supplies them.

**Identity.** A local run is a **new logical activity**. Its run id is
`local-` and 16 hex digits, and its idempotency key is
`{namespace}/local-…/{task}`, fresh per invocation. The source run's id and key
never reach `task.json`, a container label, or anything the workload can read:
reusing the production key would let an external system return the production
call's cached result, or record the local run's side effect under the
production key. Any external side effect the task performs is real, and the
transcript says so before the container starts.

**Secrets.** No secret is resolved implicitly. Every secret the task declares
must be supplied (`--secret NAME=file:PATH` or `--secret NAME=env:VAR`) or
explicitly skipped (`--omit-secret NAME`); an unaccounted one refuses before
any byte is fetched. The dispatcher reads the value, stages it in the
per-attempt secrets directory and removes it on every exit path; no value is
printed, reported, or kept with `--keep`.

**What it never does.** It calls only `GetRun`, `GetOrkflow`,
`ValidateOrkflow` and `ReadArtifact`, and never dials Temporal or object
storage. Its dispatcher has no commit store, attempt store or log store, so
the deployment gains no run, commit, attempt record, log chunk, event, audit
record or metric — the transcript's last line states it. It never passes the
container a credential, server address or token the definition does not
declare, never evaluates `condition:`, never applies `retry:`, and never runs
a second task.

Exit status: `0` succeeded (or `--render-only` rendered), `2` the task failed
(`ActivityFailed`, `ContractViolation`, `TimedOut`), `3` interrupted, `1` the
command could not run the task — including an `InfrastructureError`, which is
this machine's problem rather than the task's verdict.

---

## 14. Evidence

Every claim below was executed in this repository's environment while the
contract was designed; the integration and contract tests named elsewhere in
this document hold the tree to them now. File and line citations in this
section are as of that time and may have drifted.

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

### 14.7 The commit primitives, measured against the specified encoder

A Go probe built against **the generated code for §2.2's own proto**
(`buf generate` → `protoc-gen-go`), so these are the real messages this design
specifies, not an approximation.

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

### 14.12 The embedded fallback is implementable: `docker load` takes an OCI layout

§4.6 requires `LoadLayout`, and the previous round listed "`docker load`
accepting an OCI layout tar" as an inference. It is not:

```
$ docker save alpine:3.20 -o save.tar && tar tf save.tar
blobs/  blobs/sha256/…  index.json  oci-layout          ← already an OCI layout
$ cat oci-layout
{"imageLayoutVersion":"1.0.0"}
$ python3 -c 'print(json.load(open("index.json"))["manifests"][0])'
mediaType: application/vnd.oci.image.index.v1+json
digest:    sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc

$ docker buildx build --output type=oci,dest=oci.tar .   # an OCI-layout export
$ docker load -i oci.tar
Loaded image ID: sha256:fa6ded5f…
```

Three things this settles. `docker load` **accepts** an OCI image layout, so
`DockerRuntime.LoadLayout` needs no exotic tooling. The layout's `index.json`
names the **index digest** — `sha256:d9e853e8…`, the same digest §14.4 resolved
for `alpine:3.20` — so the lookup §4.6 specifies is a direct key read. And the
blob's file name *is* its digest (`blobs/sha256/<hex>`, verified to match), so
verifying the index blob is a path comparison rather than a re-hash, which is
why §4.6 can assign the remaining chain to the runtime.

### 14.13 Exit codes 125/126/127 carry no runtime guarantee

```
workload exit   0 -> docker reports   0
workload exit   1 -> docker reports   1
workload exit  42 -> docker reports  42
workload exit 125 -> docker reports 125     logs=WORKLOAD-CHOSE-125
workload exit 126 -> docker reports 126     logs=WORKLOAD-CHOSE-126
workload exit 127 -> docker reports 127     logs=WORKLOAD-CHOSE-127

contrast, a container that never started:
  docker run alpine /no/such/binary   -> CLI rc=127
  docker run --cgroupns=nope alpine   -> CLI rc=125
```

A started container's status passes through verbatim, and a workload can choose
the exact numbers the CLI uses for its own diagnostics. Since POSIX makes 127
the shell's ordinary "command not found" and 126 "not executable", dedicated
`CONTAINER_COMMAND_NOT_FOUND` / `CONTAINER_NOT_EXECUTABLE` rows would
permanently refuse to retry a routine shell failure, so §8.2 has none.

### 14.14 SIGTERM does not reach the workload under the naive entry script

`--init`, a workload that traps TERM and exits 0, `docker stop --timeout 5`:

```
(a) sh -c "$cmd"; rc=$?; write values; exit $rc
      stop=192ms  exit=143  logs: WORKLOAD-UP
(b) sh -c "$cmd" & child=$!; trap 'kill -TERM $child' TERM; wait; [double-wait]
      stop=321ms  exit=  0  logs: WORKLOAD-UP | WORKLOAD-GOT-SIGTERM | ENTRY-WROTE-VALUES rc=0
(c) exec sh -c "$cmd"
      stop=317ms  exit=  0  logs: WORKLOAD-UP | WORKLOAD-GOT-SIGTERM
```

**(a) fails both halves of the contract.** The workload's handler never ran and
`values.json` was never written; exit 143 is 128+15, the default disposition.
`--init` forwards SIGTERM to its *direct child only* — tini signals one process
and Docker injects `docker-init --` without `-g` — so `entry.sh`, a shell with
no trap, dies and takes the grandchild with it. This is not the "degrades to
SIGKILL at the grace boundary" one might assume; it is faster and more total,
and §10.3's promise that "SIGTERM is honored within the grace period" would
fail for the platform's own reference image.

**(c) shows rule 2 was unfollowable as written.** `exec` replaces the shell, so
the signal arrives but nothing can write `values.json` — every successful run
would end in `ContractViolation/VALUES_MISSING`.

**(b) is the only shape that satisfies both**, and it needs the double-`wait`
caveat §10.2's script encodes: POSIX makes a trapped signal return from `wait`
immediately with a status >128, before the child has finished, so the real
status needs a second `wait` **on that path only** — an unconditional second
`wait` would discard the status when no signal arrived.

### 14.15 `docker load` verifies nothing about the root object

§4.6's earlier draft treated the layout's content-addressed filenames as the
proof. Tampered a root blob's bytes while keeping its digest-shaped filename:

```
filename claims:  sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
actual content:   sha256:446d9649e45d53ff53efbacf59a1e4308c4b56e3218e5dd615e542779cc4f94c
>>> MISMATCH: filename lies about content

$ docker load -i tampered.tar
Loaded image: alpine:3.20        load rc=0
$ docker run --rm alpine:3.20 echo RAN
RAN                              run rc=0
```

The fault did not surface at load, and did not surface at run. Nothing in the
path compares `blobs/<alg>/<encoded>` to the bytes stored there. So §4.6 now
requires OrkStack to hash the root index blob itself, before import.

Caveat, stated because it bounds the claim: the tamper added an annotation
inside the index while the child descriptors still matched, which is why the
image ran. A child-digest swap would produce a different image. What is
demonstrated is that **nothing verified the root**, not an end-to-end image
substitution.

### 14.16 Invalid UTF-8 in `ArtifactRef.name` is rejected by every encoding

Against the generated Go for §2.2's own proto:

```
name bytes: "report\xff.bin"
protojson.Marshal(ArtifactRef)   -> proto: field orkstack.v1.ArtifactRef.name contains invalid UTF-8
proto.Marshal(ArtifactRef, bin)  -> string field contains invalid UTF-8
protojson.Marshal(TaskRun)       -> proto: field orkstack.v1.ArtifactRef.name contains invalid UTF-8
protojson.Marshal(ActivityCommit)-> proto: field orkstack.v1.ArtifactRef.name contains invalid UTF-8
control, valid UTF-8             -> {"name":"report.bin","uri":"s3://b/k","sha256":"abc"}  err=<nil>
```

Binary refuses too, so there is no encoding escape and no `bytes`-typed
workaround short of adding a field. Storage is not the problem — the local
backend base64-encodes opaque names and S3 keeps a companion manifest — only
the wire. §3.5 therefore rejects such a name at the output scan, before any
bandwidth is spent, rather than letting it fail at the commit.

### 14.17 An Orker crash leaves the container running

```
container before kill:               running=true
SIGKILL the managing process
container 3s later:                  running=true  status=running
label filter finds:                  1 container

control, same test with --rm:        orphan-rm-…  Up 4 seconds  → STILL RUNNING
```

`--rm` is daemon-side but fires on container **exit**; a container whose client
was killed has not exited. `--sig-proxy` forwards only catchable signals, so it
cannot cover a SIGKILLed Orker — which is exactly the crash §5.5 row 1
describes. §5.5 is corrected and the three reaping measures are specified there.

### 14.18 The specified entry script, built and exercised on every op

Built §10.1's Dockerfile and the **previous** draft of §10.2's `entry.sh`
verbatim (`alpine:3.20` + `jq`, `USER 65532:65532`, exec-form `ENTRYPOINT`),
workspace mounted per §3.1, workload traps TERM and exits 0, `docker stop
--timeout 5`:

```
op=exec  + cancel      exit=  0   values={"exitCode":0}   logs: WORKLOAD-UP|WORKLOAD-GOT-SIGTERM
op=eval  + cancel (1)  exit=143   values=ABSENT           logs: (empty)
op=eval  + cancel (2)  exit=143   values=ABSENT           logs: (empty)

op=eval  + success     exit=  0   logs: DIAG                       ← stdout SWALLOWED
op=exec  + success     exit=  0   logs: HELLO-ON-STDOUT|DIAG
```

Two independent defects in one construct. `value=$(run_supervised "$cmd")` put
the trap in a command-substitution subshell, so cancellation was lost entirely;
and the same substitution captured the workload's stdout, so §3.6's durable
`stdout.log` would have been empty for every `eval` task while `values.json`
looked perfectly correct.

The corrected script in §10.2 was then built and re-run on the same harness:

```
eval  + cancel   exit=0  logs: WORKLOAD-UP|WORKLOAD-GOT-SIGTERM
                         values={"value":"WORKLOAD-UP\nWORKLOAD-GOT-SIGTERM","exitCode":0}
eval  + success  exit=0  logs: DIAG|HELLO-ON-STDOUT
                         values={"value":"HELLO-ON-STDOUT","exitCode":0}
exec  + cancel   exit=0  logs: WORKLOAD-UP|WORKLOAD-GOT-SIGTERM   values={"exitCode":0}
script           exit=3  logs: FROM-SCRIPT
                         values={"scriptPath":"/orkstack/tmp/script","exitCode":3}
argv-injection   exit=0  logs: hello; touch /orkstack/output/artifacts/PWNED
                         artifacts=[]                              ← payload inert
```

All five pass. The last row is the shell-injection check: with
`command: ["echo", "hello; touch /orkstack/output/artifacts/PWNED"]` the
metacharacters were printed as data and **no file was created**, which is what
makes the argv-array form (rule 8) a real mitigation rather than advice.

`script` is included because the previous draft routed `exec|script` through
the same branch and never read `.script`, `.interpreter` or emitted
`scriptPath` — the op table promised a feature the script did not have.


### 14.19 Not verified

- That cancellation cannot currently reach a running container is an inference
  from Temporal's heartbeat-gated cancellation delivery plus the four discrete
  heartbeat call sites in `internal/orker/activity.go`. It was **not**
  reproduced end to end. §14.14 proves the container-side half — that SIGTERM
  arrives and is handled — but the Temporal-side half, a heartbeat-delivered
  cancellation reaching the Orker, remains unexercised. §6.3 is written so the
  design is correct whether or not the current behavior is as inferred.
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
- §14.13 and §14.14 used inline `sh -c` scripts, not built images. The process
  topology under test — `docker-init` → `entry.sh` → `sh -c "$cmd"` — is the
  same one the specified image produces, but exec-form `ENTRYPOINT` was not
  separately exercised.
- The §4.3 GET-based resolver was **not** exercised against a live registry;
  the HEAD-cannot-carry-a-body argument rests on RFC 9110 and the OCI
  distribution spec, and §14.4 observed the index digest through a diagnostic
  tool rather than the specified code path.
- No Start-to-Close transfer budget was measured. §6.3 replaces an unsupported
  constant with a derived formula; the formula's constants are themselves
  unmeasured and are operator-configurable for that reason.
- §14.18 built and ran the `activity-shell` image only; `activity-http` and
  `activity-file` are not specified concretely enough to build and were not
  exercised.
- §14.15's tamper altered an annotation inside the root index while the child
  descriptors still matched. "Nothing verified the root" is demonstrated; "an
  arbitrary image can be substituted" is not.
- `RefFor` (§5.4) and `ActivityFailureDetail` (§2.2) are specified against the
  interfaces and SDK behavior read in this repository; neither was compiled.
- §14.18 used Docker named volumes with `volume-subpath` rather than host bind
  mounts, because the daemon cannot see this host's filesystem. Process and
  signal semantics are identical; the bind-specific ownership question is
  covered separately by §14.8.

---

## 15. Non-goals

This contract does not add, and must not grow: a visual designer, a
plugin marketplace, an expression language, an embedded scripting runtime, a
custom scheduler or event bus or state machine, an OrkStack-owned execution
database, a Kubernetes operator, image signature policy, named network profiles,
a second container runtime implementation, or a required workload SDK in any
language. The activity ABI is a directory of files and an exit code, and it stays
that way.
