# OCI Activity Contract

This document is the contract under which every OrkStack task executes. It
is normative: where it says MUST, an implementation that does otherwise is
wrong. Where it says SHOULD, a deviation needs a recorded reason.

Scope: the engine↔Orker wire, the container-facing filesystem ABI, the durable
commit protocol, image resolution, delivery and policy, Orker dispatch,
failure categories, the security profile, and the first-party activity images.
It does not design a plugin marketplace, a general policy engine, or a second
runtime.

An image author needs §0, §3, §8.2 and §10. Everything else is how the
platform keeps the promises §3 makes. The rationale behind the rules that look
arbitrary — and were each found by a failing probe — is collected in §14;
the tests named throughout hold the tree to them. Grounding: `AGENTS.md`
§§1–15.

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
running anything. Otherwise it checks the image against the run's image
policy, builds a `/orkstack` workspace on local disk, bind-mounts it into a
locked-down container, streams stdout and stderr, heartbeats for the whole
run, and on exit `0` validates `/orkstack/output/values.json`, uploads
artifacts, writes `commit.json` with a create-if-absent conditional write, and
only then reports success to Temporal. Any non-zero exit fails the activity
with a category that decides whether Temporal retries.

---

## 1. Ownership: who knows what

This table is the contract's spine. Every later section is consistent with it.

| Fact | Engine (`cmd/ork`, `internal/engine`, `internal/api`) | Orker (`cmd/orker`, `internal/orker`) | Container | Object store |
|---|---|---|---|---|
| Orkflow YAML, compiled IR, definition digest | **owns** — compiles, resolves images, stamps digest | never sees YAML | never | holds the immutable version document |
| Activity name → image reference | **owns** — the activity catalog is engine config | never resolves a name | never | — |
| Image reference → index digest | **owns** — resolved once, at apply | verifies the pin on pull | never | — |
| Image policy | **owns** — composes the policy in force at submit, checks it at validate, apply and submit | re-checks it at dispatch and verifies signatures itself | never | holds the deployment floor |
| Which version a run uses | **owns** — `@current.json` at submit; the run pins it | receives the pin as a coordinate | never | holds the pointers |
| Pool → Temporal task queue | **owns** (`internal/task/pool.go`) | binds exactly one queue at boot | never | — |
| Template rendering | **never** — determinism | **owns** — renders `with:` after staging artifacts | sees only the rendered result | — |
| Registry credentials | resolve-only (manifest, config-blob and signature GETs), read-only, no push, no execution | pull and signature-read credentials | **never** | — |
| Temporal credentials | yes | yes | **never** | — |
| Object-store credentials | yes | yes | **never** | — |
| Upstream artifact bytes | **never** — references only | materializes them into the workspace | reads the files it was given | holds them |
| Container runtime | never | **owns** — the only Docker/OCI caller in the repo | is the workload | — |
| Exit code | receives it in the result | captures it | produces it | recorded in `commit.json` |
| `values.json` | receives it inline only when ≤ 4 KiB, for display | validates, uploads | writes it | holds the authoritative copy |
| Live logs | reads chunks | writes chunks | writes stdout/stderr | holds the chunks |
| Durable commit record | reads it for archive | writes it, exactly once | never sees it | **holds it — it is the record** |
| Per-attempt record (phases, outcome) | reads it | writes it, best-effort | never sees it | holds it |
| Failure category | projects it into `Error.category` | assigns it | expresses only an exit code | recorded on failure paths |
| Secrets | resolves references to a source | materializes files, deletes them after | reads files it was told about | never (secret bytes never stored) |

Three rules follow and are enforced everywhere below:

1. **The container never holds a platform credential.** The Orker is the
   security boundary (AGENTS.md §2.3).
2. **Bytes never travel through Temporal.** The engine's inputs and the Orker's
   results carry references. The only exception is a `values.json` of at most
   4 KiB, copied inline purely so the engine's read model can display it.
3. **The engine never touches a filesystem or a registry inside workflow code.**
   Image resolution happens at *apply*, and the policy and every
   configuration-derived value at *submit*, in `internal/api` — never in
   `internal/engine`'s deterministic workflow.

---

## 2. Proto contract

### 2.1 Compatibility rule

> **Field numbers are never reused or renumbered.** A removed field is
> `reserved`. Go APIs, the DSL surface, and the `internal/task` structs may be
> replaced outright.

`orkflow.proto` carries `reserved 8; reserved "queue";` and
`reserved 2; reserved "plugin";` on `Task` (and `run.proto` the same `2` on
`TaskRun`), and `namespace.proto` reserves `Pool.plugins` the same way, so a
stale stored document can never be re-read as a pool selection or an image.
The DSL compiler decodes strictly: a document carrying any key the schema does
not define is refused. `make proto` runs `buf lint` and `buf generate`, not
`buf breaking`.

**Additive contexts are set only when used.** Every optional context added to
`ExecuteTaskInput` after the first release — `telemetry = 23`, `pin = 24`,
`image_policy = 25`, `each = 26`, `cache = 27`, `join = 28` — is set ONLY for
a task that uses the feature, so every other payload stays byte-identical and
old histories replay unchanged. Submit asks the task's pool what it can run
(`DescribeOrker`, §6.5) and refuses a pinned task on a pool whose Orker hosts
no pins (`max_concurrent_pins` 0), and a fan-out, a cached task or a container
failure handler on a pool whose Orker predates the feature (`each_supported`,
`cache_supported`, `join_supported` false) and could not decode its context.
The probe reaches one Orker, so every Orker of a pool must run a release that
supports what the pool is sent (`docs/DEPLOY.md`, §1).

### 2.2 `proto/orkstack/v1/activity.proto`

The file is authoritative; its field comments carry the per-field detail.
What each message is for, and the rules that are easy to break:

| Message | Role |
|---|---|
| `ImageRef` | an image resolved to an immutable reference (§4) |
| `ResourceRequest`, `NetworkMode`, `SecretRef` | what a task asks the Orker for (§9) |
| `WorkspaceLimits` | the deployment-wide contract limits, below |
| `UpstreamRef` | one completed ancestor's contribution: `commit_uri`, the `values` ref, artifact refs — references only, so fan-in is O(ancestors) small records |
| `PinContext`, `ReleasePinInput` | a pinned task's pin name, the `instance_id` of this opening, and its lifetime (§3.1, §6.4) |
| `EachContext`, `EachUpstream`, `EachSource` | one item of a fan-out, and a fan-out upstream's item count (§3.1, §3.2) |
| `JoinPolicy`, `NeedOutcome`, `JoinContext` | a failure handler's policy and how each of its needs ended (§3.2) |
| `CacheScope`, `CacheContext`, `CacheResult`, `CacheSource`, `CacheMemo`, `CacheOutcome` | memoized completion (§5.4.1); none of it reaches a container |
| `ExecuteTaskInput` | the argument of `ExecuteTask` — the seam |
| `ExecuteTaskResult` | what an Orker returns for a SUCCEEDED activity |
| `ActivityCommit`, `CommitRef` | the immutable completion manifest and a pointer to its stored bytes (§5) |
| `ActivityFailureDetail` | what a FAILED activity carries as its `ApplicationError` details |
| `DescribeOrkerResult`, `OpenPin` | the pool-liveness probe's payload and the Orker's capabilities (§6.5) |

`WorkspaceLimits` are engine configuration, uniform across a deployment —
which is what lets them travel on the run plan and lets the engine run its own
`INPUT_TOO_LARGE` check. They also appear verbatim in `task.json` so a workload
can respect them instead of discovering them by failing. Orker-local
*resource* policy (cpu, memory, pids) is deliberately not here: it varies per
pool, and reaches the workload as effective values in `task.json` instead
(§9.3).

| Limit | Default | Setting |
|---|---|---|
| `values_max_bytes` | 262144 (256 KiB) | `ORK_VALUES_MAX_BYTES` |
| `artifact_max_count` | 256 | `ORK_ARTIFACT_MAX_COUNT` |
| `artifact_max_bytes` (per artifact) | 5368709120 (5 GiB) | `ORK_ARTIFACT_MAX_BYTES` |
| `log_max_bytes` (durable log, per stream per attempt) | 16777216 (16 MiB) | `ORK_LOG_MAX_BYTES` |
| `commit_max_bytes` (serialized `ActivityCommit`) | 4194304 (4 MiB, §5.3) | `ORK_COMMIT_MAX_BYTES` |

Rules the field comments state and every change must keep:

- **`ImageRef.pinned` is the only string that may be pulled or run**:
  `{registry}/{repository}@{index_digest}`. `reference` is what the author
  wrote and is never pulled. `platforms` is in the index's own order.
  `platform_digest`, `platform` and `delivery` (`"registry"` or `"embedded"`)
  are per-execution facts only an Orker fills, and only into results, commits
  and attempt records. Field 8 (`resolved_at`) is **removed**, not merely
  excluded from the digest: `ImageRef` lives inside the immutable version
  document, and a per-apply timestamp there would turn every re-apply of an
  unchanged document into a version conflict (§4.4).
- **`ExecuteTaskInput` MUST NOT exceed 262144 serialized bytes** (AGENTS.md
  §2.2). The engine checks this deterministically before scheduling and fails
  the task `ContractViolation/INPUT_TOO_LARGE`.
- **`idempotency_key` is exactly `"{namespace-or-default}/{run_id}/{task_id}"`**
  (an item of a fan-out appends `/{index}`), unchanged across every retry. Its
  JSON name equals the `task.json` key on purpose.
- **`commit_uri` is composed by the engine** from the commit-store root the
  run plan carries plus the run and task coordinates (§5.2). The Orker uses the
  value it was sent and never re-derives one.
- **`with` is the RAW, un-rendered block.** Rendering stays in the Orker,
  because `artifact "<task>/<name>"` resolves to a path only the Orker knows
  after staging, and the workflow must stay deterministic.
- **`env` is the task's own `env:`. The platform puts nothing there**:
  required platform state travels as files (§3.7).
- **`ExecuteTaskResult.values_json` is `bytes`, not a `Payload`.** It is the
  workload's exact `values.json` bytes, present only when the file is at most
  4096 bytes, for the read model only; nothing downstream renders from it. A
  `google.protobuf.Struct` round trip reorders keys, normalizes numbers,
  decodes escapes and silently corrupts integers beyond float64's exact range
  (§14.9), so a field that promises byte identity cannot be one.
- **`CommitRef.sha256` is the sha256 of the exact bytes written to, or read
  from, the store.** It MUST NEVER be recomputed by re-marshalling a decoded
  `ActivityCommit`: `protojson` deliberately varies its output across builds
  (§14.10).
- **`ExecuteTaskResult.attempt`** is the Temporal attempt that produced the
  result — from the activity info on a fresh run, from `commit.attempt` on an
  adopted one — because workflow code cannot read activity info and an adopted
  result carries a reference, not the commit body.
- **A failure produces no `ExecuteTaskResult`.** It is an `ApplicationError`
  whose single details argument is an `ActivityFailureDetail` (reason code,
  category, exit code, the artifacts and logs the failure path uploaded, scan
  notes, image, attempt, Orker, start time). The engine populates the failed
  `TaskRun` from it exactly as it does from a result, so a failed attempt is
  as inspectable as a successful one.
- **`ActivityCommit.schema_version` is 1.** A reader that does not know the
  value refuses the commit (`COMMIT_SCHEMA_UNSUPPORTED`) rather than guessing.

### 2.3 Other proto files

- `common.proto`: `FailureCategory` (§8.1) — its value names are also the
  exact Temporal `ApplicationError` type strings — and `Error{code, message,
  details, retryable, category}`.
- `orkflow.proto`: `Task.uses` is the only image-selecting field in the DSL;
  `Task.image` is the engine's apply-time resolution of it, stamped into the
  version document; `Task.run_as_orker_user` is the §3.1 escape hatch, part of
  the canonical form.
- `run.proto`: `TaskRun` says what the commit protocol produced (`values_ref`,
  `commit`, `exit_code`, `system_artifacts`, `retry_state`) and is the read
  model, never the authority. `TaskAttempt` is the per-attempt record of
  §7.1 step 16; `PhaseSpan` is one phase of it; `ReservedResources` is what
  the Orker reserved for the attempt.
- `image_policy.proto`: `ImagePolicy` (the stored deployment floor) and
  `ImagePolicySelection` (the policy in force for one run, carried on
  `ExecuteTaskInput.image_policy`, §4.7).
- `activity_service.proto`: `ActivityService.ListActivities` (the catalog,
  resolved, ordered by name, with each image's declared interface) and
  `DescribeOrker` (the pool-liveness probe, §6.5). `run_service.proto`:
  `RunService.InspectRun` and its `LiveAttempt` (§6.6). Each service has its
  own file, so a client loads only the schemas of the service it calls.

---

## 3. The `/orkstack` workspace

The filesystem is the activity API. A workload needs `cat`, a JSON reader, and
a redirect. If it needs anything else, this section is wrong.

### 3.1 Layout

```
/orkstack/
├── input/                          read-only bind, mode 0555
│   ├── task.json                   0444  the activity context (§3.2)
│   ├── config.json                 0444  the rendered `with:` block (§3.3)
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

The container's working directory is `/orkstack`. The root filesystem is
read-only (§9.2), so `/orkstack/tmp` is the workload's scratch space; `/tmp`
inside the container is a separate small tmpfs (`ORK_ORKER_TMPFS_BYTES`,
default 64 MiB).

**Upstreams.** A task is staged the committed `values.json` and artifacts of
every succeeded ancestor in its `needs` closure, each re-verified against its
recorded sha256 (`INPUT_ARTIFACT_CORRUPT` otherwise). A **fan-out producer's
items** are staged one directory per item, `<item>` the 0-based index
zero-padded to five digits so a sorted listing is item order. A fan-out has no
single `values.json`, so the item directories are all there is under
`upstream/<task>/`; both directories exist even for zero items. They are staged
only for a **direct** dependent — a task that names the fan-out in its own
`needs:` — and each item's commit is identity-checked and digest-verified
exactly as an adopted commit is (§5.4).

**`/orkstack/shared` exists only for a pinned task**, and it is the one mount
whose lifetime is not the attempt's: every task of one pin, and every attempt
of each, sees the same directory for the life of the pin (`docs/DSL.md`,
"Pins"). It is bound from the pin host's
`ork-pin-<run>-<pin>-<instance>/shared/` under `ORK_ORKER_WORKDIR`, whose
parent is `0700` and Orker-owned, exactly as a per-attempt directory is. Every
path component is Orker-constructed and validated, so no definition can name a
host path (§9.1). It is neither an input tree (never sealed) nor an output tree
(nothing in it is scanned, hashed, uploaded or committed), and no secret is
written under it; a file that must outlive the pin is copied to
`output/artifacts/`. For an unpinned task the mount and `paths.shared` are
absent.

**Permissions.** The host per-attempt directory is `0700`, owned by the
Orker's user, so no other host user can traverse into a `1777` child. Inside,
the workload runs as whatever UID its image chooses, so every directory it
must write is world-writable with the sticky bit and every file it must read
is `0444` (§14.3).

**Readability is not guaranteed by the mount.** `1777` guarantees the workload
can *create* a file, not the mode it lands with: under `umask 077` a workload
writes a `0600` `values.json` that a differently-UID'd Orker cannot read on a
native filesystem, while a VM-shared bind mount remaps ownership and reads
fine (§14.8). No mount configuration fixes this, so three measures apply:

1. **Detect and fail terminally.** After the container exits the Orker opens
   every file it must read. One it cannot open is
   `ContractViolation/OUTPUT_NOT_READABLE`, naming the file, its mode, its
   owner and the fix (`umask 022`). It is non-retryable because the image will
   do the same thing on every attempt; classifying it as a retryable upload
   failure would loop forever against deterministic behaviour.
2. **An explicit escape hatch.** A task may set `runAsOrkerUser: true`, or an
   Orker `ORK_ORKER_FORCE_USER=1`, which adds `--user <orker-uid>:<orker-gid>`
   so the Orker owns every written file whatever the umask; `task.json` then
   reports `task.runAsOrkerUser: true`. It is opt-in because it overrides the
   image's own `USER` and breaks images that need their baked-in identity.
3. **First-party images set `umask 022`** before writing anything (§10.1).

Rejected alternatives: `--group-add` plus a setgid directory propagates group
*ownership*, not group *permission*; an Orker-run `chmod -R` relabel container
needs UID 0 and `CAP_FOWNER`, which §9.1 forbids. The Orker MUST NOT assume it
owns files the workload wrote, only that it can open them — and it verifies
even that.

**Host placement.** The workspace root is a per-attempt directory under
`ORK_ORKER_WORKDIR`, which MUST be a path the container daemon can bind-mount
(on macOS with a VM-hosted daemon `$HOME` is shared and `/var/folders` is not,
which is why `make up` uses `.data/work`) and whose path holds no comma or
control character, which a container mount cannot carry. The Orker probes this
at boot and refuses to start with `InfrastructureError/WORKDIR_NOT_BINDABLE`
rather than failing every task later.

**Secrets.** `/orkstack/input/secrets` is a separate bind of a per-attempt
directory the Orker creates, populates, mounts read-only, and removes on every
exit path. Secret bytes never enter a proto message, Temporal history, the
archive, a log chunk or `task.json` (§9.4).

### 3.2 `input/task.json`

The complete activity context, as one JSON object. Every field a workload
could otherwise want from an environment variable lives here, because
environment variables are not part of the ABI (§3.7). Key casing is
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
    "platform": "linux/arm64",
    "delivery": "registry"
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

**Feature keys appear only for the task that uses the feature** — absent as
keys, never written empty — so each addition is additive under the version
rule below and an image that has never heard of the feature loses nothing:

- **A pinned task** carries `task.pin` (`name`, the pin as its author named
  it; `instanceId`, one opening of it within the run, which keys the shared
  directory and is informational to the workload) and `paths.shared`.
- **An item of a fan-out** carries `each` (`index`, `count`, `as`, and `item`,
  the element itself as exact JSON). `task.id` stays the fan-out's.
- **A task with a fan-out among its direct upstreams** carries
  `inputs.eachUpstream`, so it can tell "zero items" from "no such upstream".
- **A failure handler** (`join:` `any_failed` or `all_done`, `docs/DSL.md`,
  "Joins and failure handlers") carries `join`: the policy and how each need
  ended, in `needs:` order:

  ```jsonc
  "join": {
    "policy": "any_failed",
    "needs": [
      { "taskId": "deploy", "status": "FAILED",
        "error": { "code": "TASK_FAILED", "category": "ActivityFailed",
                   "message": "container exited with status 1", "details": "task \"deploy\"" } },
      { "taskId": "smoke", "status": "CANCELED",
        "error": { "code": "TASK_CANCELED", "category": "Cancelled", "message": "canceled" } }
    ]
  }
  ```

  Submit refuses a container failure handler on a pool whose Orker cannot
  write `join` (§2.1), so a handler can rely on the key. A need fail-fast
  stopped while it ran is `CANCELED` with an `error`, never `FAILED`; a need
  that never started is `CANCELED` or `SKIPPED` with no `error`. A direct need
  that `FAILED` after writing output artifacts has them staged exactly like a
  succeeded upstream's — under `input/artifacts/<need>/`, listed in
  `inputs.artifacts` — with no `inputs.upstream` entry, because a failed need
  has no values.
- **A deployment that exports traces** adds `trace` (`traceparent`, and
  `tracestate` when the propagator set one, W3C header form). It is purely
  informational: a workload that wants to join the trace propagates the two
  strings verbatim to whatever it calls next.

**Artifact names are UTF-8; paths are derived and lossy.** `path` is produced
by the Orker's `safeComponent` rule, which rewrites separators, dot segments
and long names, so it cannot carry the logical name. `name` is the exact
logical name and `nameB64` (RawURL base64 of its bytes) is **always**
present, so a workload never has to reverse `safeComponent`. Names are valid
UTF-8 by construction, because the wire cannot carry anything else (§3.5).

#### The schema, field by field

Required means always present; a workload may rely on it. Optional means
present only under the stated condition; a workload MUST tolerate its absence.

| Path | JSON type | Req. | Empty form | Meaning |
|---|---|---|---|---|
| `schemaVersion` | integer | required | — | Major version of this file. `1` today. |
| `idempotencyKey` | string | required | — | `"{namespace}/{runId}/{taskId}"`, stable across retries; for an item of a fan-out `"{namespace}/{runId}/{taskId}/{index}"`, stable across that item's retries |
| `run.id` | string | required | — | Opaque run id. Never parsed by anything. |
| `run.namespace` | string | required | `"default"` | Resolved, never empty |
| `run.orkflow.name` | string | required | — | slug |
| `run.orkflow.version` | string | required | — | pinned version string |
| `run.orkflow.definitionDigest` | string | required | — | `sha256:<64 hex>` (§4.4) |
| `task.id` | string | required | — | slug |
| `task.attempt` | integer | required | — | 1-based Temporal attempt |
| `task.pool` | string | required | `"main"` | resolved pool name |
| `task.runAsOrkerUser` | boolean | optional | absent = false | present and true when the Orker overrode the image's `USER` (§3.1) |
| `task.pin` | object | optional | absent | present only for a pinned task |
| `task.pin.name` | string | required in `task.pin` | — | slug |
| `task.pin.instanceId` | string | required in `task.pin` | — | one opening of that pin within one run |
| `each` | object | optional | absent | present only for an item of a fan-out task |
| `each.index` | integer | required in `each` | — | 0-based position of this item |
| `each.count` | integer | required in `each` | — | the number of items, counted once by the engine |
| `each.as` | string | required in `each` | — | the name the item is bound to under `.each` in `with:` (default `"item"`) |
| `each.item` | any JSON | required in `each` | — | the element: its exact JSON text for an upstream-task source (key order and number spelling preserved), the protojson rendering of the value for a run-input source |
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
| `resources.cpu` | string | optional | absent | the effective value after this Orker's defaults and caps |
| `resources.memory` | string | optional | absent | the effective value after this Orker's defaults and caps |
| `network` | string | required | `"default"` | `"default"` or `"none"` only |
| `paths.*` | string | required | — | all eight fixed keys always present (§3.1) |
| `paths.shared` | string | optional | absent | `/orkstack/shared`, present only for a pinned task |
| `inputs.artifacts` | array | required | `[]` | never null |
| `inputs.artifacts[].producer` | string | required | — | upstream task id |
| `inputs.artifacts[].name` | string | optional | absent | the exact logical name |
| `inputs.artifacts[].nameB64` | string | required | — | RawURL base64 of the exact name bytes; authoritative |
| `inputs.artifacts[].path` | string | required | — | absolute container path |
| `inputs.artifacts[].sizeBytes` | integer | required | `0` | |
| `inputs.artifacts[].sha256` | string | required | — | hex |
| `inputs.artifacts[].contentType` | string | optional | absent | |
| `inputs.artifacts[].item` | integer | optional | absent | present only for an artifact of a fan-out producer's item |
| `inputs.upstream` | array | required | `[]` | never null |
| `inputs.upstream[].taskId` | string | required | — | |
| `inputs.upstream[].valuesPath` | string | required | — | absolute container path |
| `inputs.upstream[].item` | integer | optional | absent | present only for one item of a fan-out producer |
| `inputs.eachUpstream` | array | optional | absent | present only when a direct upstream is a fan-out |
| `inputs.eachUpstream[].taskId` | string | required | — | the fan-out's task id |
| `inputs.eachUpstream[].count` | integer | required | `0` | how many items were staged |
| `join` | object | optional | absent | present only for a failure handler — and always present for one |
| `join.policy` | string | required in `join` | — | `"any_failed"` or `"all_done"` |
| `join.needs` | array | required in `join` | — | one entry per need, in `needs:` order; at most 32 |
| `join.needs[].taskId` | string | required | — | the need's task id |
| `join.needs[].status` | string | required | — | `"SUCCEEDED"`, `"FAILED"`, `"SKIPPED"` or `"CANCELED"` |
| `join.needs[].error` | object | optional | absent | present only when the need recorded an error |
| `join.needs[].error.code` | string | required in `error` | — | machine-readable reason, at most 128 bytes |
| `join.needs[].error.category` | string | optional | absent | one of the five categories (§8.1) |
| `join.needs[].error.message` | string | required in `error` | — | human-readable explanation, at most 1024 bytes |
| `join.needs[].error.details` | string | optional | absent | at most 256 bytes |
| `secrets` | array | required | `[]` | never null; names and paths only, never values |
| `secrets[].name` | string | required | — | `^[A-Za-z0-9._-]{1,128}$` |
| `secrets[].path` | string | required | — | absolute container path |
| `trace.traceparent` | string | optional | absent | present only when this deployment exports traces |
| `trace.tracestate` | string | optional | absent | present only alongside `traceparent`, and only when the propagator set one |

Each bound on a `join.needs[].error` text is measured on its JSON-escaped
encoding; a cut text ends at a UTF-8 boundary with `…`. The need's full error
stays on its own row of the run.

An absent array is never written as `null`; the Orker emits `[]`. An absent
optional scalar is omitted, never written as `""`.

#### Two policies the file itself cannot state

**Unknown fields.** A workload MUST ignore keys it does not recognize. The
Orker MUST NOT emit a key that is not in the table above for the declared
`schemaVersion`, so an unknown key always means the workload is older than the
platform, never that the platform invented something.

**Version handling.** Within one major `schemaVersion` the platform MUST NOT
remove a field, retype a field, or change a field's meaning; it may only add
optional fields. A workload that reads a `schemaVersion` it does not know MUST
fail loudly rather than proceed on guessed semantics — a wrong result
committed durably is worse than a clean failure.

### 3.3 `input/config.json` and `input/run.json`

`config.json` is the `with:` block after the Orker renders it: a JSON object,
templates resolved, `artifact "<task>/<name>"` already rewritten to the
container path under `/orkstack/input/artifacts/`. `run.json` is the run's
input payload — the same data templates see as `.inputs`. The template context
is `docs/DSL.md`, "Templating".

Rendering is recursive over nested maps and lists, with `missingkey=error`, and
a nil value, which Go prints as `<no value>`, is rejected. Data or text that
merely spells those characters (an input of `see <no value> here`) renders as
itself: the Orker tells the two apart by running the template again with the
spelling masked, and refuses a template whose text or data holds the mask
(`<no`, U+0001, `value>`) itself. A render failure is
`ContractViolation/RENDER_FAILED` and is never retried. A templated value
renders to a **string**: a number arrives in Go's spelling (`1000000` as
`1e+06`), so numbers and structures a workload computes with travel as files,
not through `with:`.

The template helpers are exactly three:

- `artifact "<task>/<name>"` — the staged path of an upstream artifact;
- `env "<NAME>"` — a variable of the **Orker's** environment, readable only
  when the Orker lists it in `ORK_ORKER_ENV_ALLOWLIST` (an unlisted name is a
  render failure). It is not the task's `env:`;
- `sh` — wraps a value in POSIX single quotes, escaping an embedded `'` as
  `'\''`. AGENTS.md §6 requires that shell commands not interpolate untrusted
  input without the templating layer's escaping, and `sh` is that escaping. It
  is the *secondary* mitigation: the primary one is that a first-party
  `command` is an argv array (§10.2), so nothing rendered becomes code.

### 3.4 `output/values.json`

- **REQUIRED.** Exit `0` with no `values.json` is
  `ContractViolation/VALUES_MISSING`. A workload with no result writes `{}`.
- MUST be a JSON **object** at the top level. An array, a scalar, or `null` is
  `ContractViolation/VALUES_MALFORMED`.
- MUST be valid UTF-8 and MUST NOT exceed `limits.valuesMaxBytes` (256 KiB
  default) → `ContractViolation/VALUES_TOO_LARGE`. A larger result is an
  artifact.
- MUST be readable by the Orker after the container exits
  (`OUTPUT_NOT_READABLE`, §3.1).
- The bytes are uploaded verbatim. The Orker parses to validate and never
  re-serializes: key order, spacing and number formatting are preserved, and
  the committed `sha256` is the sha256 of exactly what the workload wrote.

The bound is 256 KiB rather than a few KiB because `values.json` is an
object-store artifact, not a Temporal payload. The Temporal budget is enforced
separately: only a `values.json` of ≤ 4 KiB is copied inline into the result.

### 3.5 `output/artifacts/`

- Each **regular file directly inside** the directory is one output artifact,
  named exactly as the file.
- The scan is **not recursive**. A directory, symlink, device, socket or FIFO
  is `ContractViolation/ARTIFACT_NOT_A_FILE`. A hierarchy is archived by the
  workload, e.g. as `workspace.tar.gz` (AGENTS.md §3.1).
- More than `limits.artifactMaxCount` files, or any file larger than
  `limits.artifactMaxBytes`, is `ContractViolation/ARTIFACT_LIMIT_EXCEEDED`.
- Every file MUST be readable by the Orker (`OUTPUT_NOT_READABLE`, §3.1).
- Every file name MUST be **valid UTF-8**, else
  `ContractViolation/ARTIFACT_NAME_NOT_UTF8`, naming the entry's bytes in hex.
  `ArtifactRef.name` is a proto `string` and every protobuf encoding refuses
  invalid UTF-8 (§14.16), so such a name would be staged, hashed and uploaded
  and then fail at the commit; rejecting it at the scan makes it fast,
  terminal and actionable.
- Ordering is by name bytes ascending, so two attempts of the same computation
  produce the same commit ordering.

### 3.6 Streams

stdout and stderr are captured **independently**, byte-exact, and never merged
on disk. Plain text works with no logging SDK; structured JSON is not part of
the ABI. Three things happen to them:

1. **Live**, they go to the log sink with its framing (`[stderr] ` prefix,
   `[stdout] ` escape), written as numbered chunks under
   `metadata/logs/{run}/{task}/{attempt}/` throughout the run and served by
   `(run, task, attempt)` to the CLI and UI. The live cap is
   `ORK_ORKER_LOG_MAX_BYTES` per attempt (`docs/DEPLOY.md`, "Live task logs").
2. **Durably**, on the success and failure paths each stream is uploaded as its
   own object named `stdout.log` / `stderr.log` (bounded by
   `limits.logMaxBytes`) and recorded in `system_artifacts` — a field separate
   from `artifacts`, so a workload artifact named `stdout.log` cannot collide
   with the platform's.
3. **Optionally exported**, one OpenTelemetry log record per line carrying the
   run, task, attempt, namespace and Orkflow coordinates
   (`ORK_ORKER_TASK_LOG_EXPORT`, `docs/DEPLOY.md`, "Telemetry"). The image
   needs no agent, SDK, variable or file for it; the Orker does it from
   outside.

**A cancelled attempt has no `stdout.log`/`stderr.log` object**: it produces
neither a result nor a commit, and Temporal permits only `CanceledError`
details on that path. Its output is still retained and readable through the
live chunks of item 1, which depend on no result.

### 3.7 Environment

The container's environment is the image's own `ENV`, overridden by the
task's `env:` (`ExecuteTaskInput.env`), plus what the container runtime adds
(under Docker, `HOSTNAME` — each container's own id). The platform adds
nothing: no `ORK_*` variable, no credential, no coordinate. Required platform
state travels as files (§3.2), because a variable is invisible to review,
leaks into every child process, and cannot carry structure — AGENTS.md §3
makes environment variables no part of the ABI.

A task's `env:` is a legitimate interface — `HTTPS_PROXY`, `TZ`, `LANG`,
`GIT_TRACE` — and an image SHOULD honour it like any Unix tool rather than
clearing or overriding it. The security boundary is the Orker versus the
workload (credentials, privileges, host access), not an author versus their
own task. Every first-party image honours `env:` this way and documents its
defaults (`docs/ACTIVITY_IMAGES.md`). `env:` is part of a cached task's memo
key; `HOSTNAME` is not (`docs/DSL.md`, "Caching").

---

## 4. Image resolution, delivery and policy

### 4.1 The two halves

```
APPLY TIME (engine, internal/api, once per version)
  uses: "shell"                      catalog lookup
     └─> ghcr.io/orkstack/activity-shell:0.1   registry GET /v2/<repo>/manifests/<tag>
          └─> sha256:9ab2…            sha256 of the response body
               └─> ImageRef{pinned: "ghcr.io/orkstack/activity-shell@sha256:9ab2…"}
                    └─> stamped into the immutable Orkflow version document

RUN TIME (Orker, every attempt)
  ImageRef.pinned
     └─> image policy check (§4.7), when the run carries one
     └─> docker pull ghcr.io/orkstack/activity-shell@sha256:9ab2…   (content-verified)
          └─> record platform_digest, platform and delivery
```

A tag is never pulled. A tag is never run. The only string that reaches the
runtime is `ImageRef.pinned`, so a running execution can never switch images
because a tag moved (AGENTS.md §5).

### 4.2 The activity catalog (engine config)

The catalog maps a short activity name to a registry reference. It is engine
configuration (`ORK_ACTIVITY_CATALOG`, a YAML `activities:` map), not an Orker
probe and not a marketplace. The built-in table:

```yaml
activities:
  shell:    ghcr.io/orkstack/activity-shell:0.1
  http:     ghcr.io/orkstack/activity-http:0.1
  file:     ghcr.io/orkstack/activity-file:0.1
  json:     ghcr.io/orkstack/activity-json:0.1
  git:      ghcr.io/orkstack/activity-git:0.1
  template: ghcr.io/orkstack/activity-template:0.1
```

`uses:` resolution order, applied in `internal/dsl` validation and then in
`internal/api` before the version is stored:

1. `uses` containing a Go template action (`{{`) is a compile error,
   `IMAGE_NOT_PINNED`: an image chosen at run time cannot be pinned at apply.
2. Else, `uses` matching `^[a-z][a-z0-9-]*$` that is a catalog key → the
   catalog reference.
3. Else, `uses` that parses as an OCI reference (contains `/`, `:` or `@`) →
   itself. A reference is held to the distribution grammar and refused by name
   outside it, never repaired: a registry that is a host name (or a bracketed IP
   address) with an optional port from 1 to 65535, repository path components
   of lower-case letters and digits joined by single `.`, `_`, `__` or dash
   separators, a tag of at most 128 word characters, dots and dashes that does
   not start with a dot or a dash, and a `sha256:` digest. The reference becomes
   a registry URL at apply and a runtime argument at dispatch, so nothing it
   accepts can begin like a runtime option, hold a space or a control character,
   or change a URL's path, query or fragment.
4. Else → compile error `UNKNOWN_ACTIVITY`, listing the catalog keys.

Rule 2 before rule 3 means `shell` can never accidentally mean Docker Hub's
`library/shell`. (`uses: orkflow:<name>` names a child Orkflow, not an image —
`docs/DSL.md`, "Composition".)

### 4.3 Where resolution runs

`dsl.Compile` stays pure — no network, no clock — so every hermetic compiler
test holds. Resolution runs in `internal/api` (apply and validate), between
compile and store. For each distinct `uses` the resolver:

1. Issues **`GET /v2/<repository>/manifests/<tag-or-digest>`** with the exact
   `Accept` set the Orker uses at pull time (`dsl.ManifestAccept`), because
   OCI content negotiation can select a different object for a different
   `Accept` set. It MUST be a GET, not a HEAD: `platforms` comes from the
   index body, which a HEAD response cannot carry, and a HEAD-based resolver
   would mint a different definition digest for the same index.
2. Computes `sha256` over the **exact response bytes** and requires equality
   with `Docker-Content-Digest` when present (`IMAGE_DIGEST_MISMATCH`).
3. For an index (`application/vnd.oci.image.index.v1+json` or a Docker
   manifest list) records each `manifests[].platform` as `os/arch[/variant]`,
   in the **index's own order** (the digest hashes it, so order is contract);
   for a single manifest `platforms` stays empty.
4. For an already-pinned `repo@sha256:…` still issues the GET and requires a
   `200` whose body digest equals the pin. Apply MUST NOT succeed with an
   unpullable pin.
5. Stamps `Task.image`.
6. Reads the image's interface label (`docs/ACTIVITY_IMAGES.md`, "Declaring
   the interface"): for an index, the **first entry in index order whose
   `platform.os` is present and not `unknown`** (buildx attestation manifests
   are skipped), fetched and digest-checked; then its config blob, bounded at
   4 MiB and digest-checked. A registry that serves a manifest but not its
   config cannot serve a pull either, so a failure here fails the apply with
   the codes below. A malformed **label** never does: the image is still
   pinned and runnable, and the reason is reported as an interface warning.
   The interface is not an `ImageRef` field and never reaches the digest; it
   is recomputed from the digest-addressed image on every read.
7. Checks the image against the image policy in force (§4.7).

Failures are apply-time errors, never silent and never a fallback to a tag:
`IMAGE_NOT_FOUND` (404), `IMAGE_RESOLVE_UNAUTHORIZED` (401/403),
`IMAGE_RESOLVE_UNAVAILABLE` (5xx, timeout, DNS), `IMAGE_RESOLVE_UNSUPPORTED`
(an unreadable manifest or config), `IMAGE_DIGEST_MISMATCH`.

**There is exactly one resolver** (`internal/dsl/registry_resolver.go`), and it
goes to the registry over the distribution API. The daemon is not a resolver
(`docker image inspect` reports whatever it negotiated for the platform it
pulled, §14.4), and neither is `docker buildx imagetools inspect`: a second
resolver can disagree about `platforms` and therefore about the definition
digest. The signature verifier (§4.7) is built on the same resolver.

### 4.4 Where the digest lives, and what it does to versioning

`Task.image` is inside the immutable version document, so **the version
document is the pin** — what AGENTS.md §7.2 requires of a run.

**The canonical form is OrkStack's own, not `Deterministic: true`.**
`proto.MarshalOptions.Deterministic` is documented upstream as "not
guaranteed to remain stable over time", and tells users who need canonical
serialization for fingerprinting to define their own. A digest stamped into
an immutable document and compared across releases is that use. So
(`internal/dsl/canonical.go`):

> **`canonicalOrkflow(wf)`** — JSON, UTF-8, no insignificant whitespace,
> object keys sorted by their UTF-8 byte sequence, integers rendered without
> exponent or fraction, and **only** the fields the serializer enumerates. A
> field it does not name is not serialized, so adding a proto field cannot
> silently change an existing digest.
>
> `orkflow_definition_digest = "sha256:" + hex(sha256(canonicalOrkflow(wf)))`

The base fields — on `Orkflow` the namespace, name, version, description,
labels, inputs and tasks; on `Task` everything that decides what runs (`uses`,
`image`, `with`, `needs`, `condition`, timeouts, `retry`, `pool`, `command`,
`env`, `secrets`, `resources`, `network`, `run_as_orker_user`); on `ImageRef`
the seven resolved fields — are emitted whether or not they are zero. Never
`source_yaml`, never `created_at`, never the per-execution `ImageRef` fields.

**Every field added after the first release is emitted only when set**:
`pins` and `outputs` on the Orkflow; `pin`, `join` (when not the default),
`await`, `orkflow`, `each` and `cache` on a task. Each changes what runs, so
each is in the digest — but emitting an empty one unconditionally would move
the digest of every existing definition on its next apply, and the store
decides "unchanged document" by comparing after clearing only `created_at`,
so an unchanged, source-hash-versioned document stamped with a moved digest
would be refused with `ALREADY_EXISTS`. Golden vectors beside the serializer
prove no unpinned digest moves; a digest change is a test failure, never a
silent incompatibility.

**The derived version covers the resolution.** For a document that pins no
`version:`, the version is assigned after resolution:

> `derivedVersion = hex(sha256(sourceYAML ‖ 0x1F ‖ canonicalImages(wf)))`,
> `canonicalImages` the canonical JSON of `[{taskId, image}]` in declaration
> order.

The source bytes are hashed in full, so a comment-only edit still mints a new
version. Do not hash normalized YAML instead: a comment-only edit would then
compile to the stored version with different source and could not be applied
at all (`internal/dsl/hash.go`). The resolution is a second *input to what
runs*, not a second notion of the document. Consequences:

| Change | Result |
|---|---|
| Same YAML, moved tag, no explicit `version:` | a **new** version, because a different thing now runs |
| Same YAML, moved tag, explicit `version:` | **`ALREADY_EXISTS`**: an immutable version cannot mean two definitions |
| Same YAML, same digest | byte-identical definition; apply is a no-op |
| Comment-only edit, no explicit `version:` | a new version |

### 4.5 Registry credentials

`ork` has one narrowly scoped credential surface: `ORK_REGISTRY_AUTH_FILE`, a
Docker `config.json`, used **only** for manifest, config-blob and signature
**GET**s at validate, apply and submit, always with a pull scope. The Orker's
`ORK_ORKER_REGISTRY_AUTH_FILE` is used only for the signature GETs of §4.7;
pulls use the container runtime's own credentials. None of them grants push or
execution, and none ever reaches a container.

### 4.6 Delivery model

| Source | How the image arrives | Digest |
|---|---|---|
| **Builtin** (`shell`, `http`, `file`, `json`, `git`, `template`) | Pulled from the catalog reference like any other image. Each release publishes them to `ghcr.io/orkstack/activity-<name>` for linux/amd64 and linux/arm64, tagged `X.Y.Z` and `X.Y`, the tag the built-in catalog names (§4.2; [UPGRADING.md](./UPGRADING.md), "Release artifacts"); a checkout builds and pushes them to its local registry with `make -C images push`. | Real registry index digest |
| **Custom / community** | Pulled from the registry named in `uses:`. The default; needs no configuration. | Real registry index digest |
| **User-provided source** | `orkctl activity build <path\|git-url> --tag <ref>` builds and pushes, printing the pinned reference to paste into `uses:`. | Real registry index digest |
| **Embedded fallback** (builtins only) | An **OCI image layout** shipped with the Orker at `ORK_ORKER_IMAGE_LAYOUT` (default `/var/lib/orkstack/images`), used only when a pull of an already-pinned digest fails. | Real registry index digest, carried inside the layout |

`orkctl activity build` pushes by default (`--push`); `--push=false` builds
only, an image you can smoke-test but cannot pin. `--platform
linux/amd64,linux/arm64` builds for each platform named, and `--file` names a
Dockerfile other than the context's own.

There is no *unpushed* local image: a never-pushed build has a local config ID,
not a registry manifest digest, and it does not transfer to a second Orker.
Making every build path push keeps AGENTS.md §5's immutable reference
literally true everywhere, including `make up`, which runs a local registry
(`localhost:5001`) and points `ORK_ACTIVITY_CATALOG` at it.

**The embedded fallback verifies the root itself.** `docker load` verifies
nothing about the root object of a layout — a blob whose filename claims one
digest and whose bytes hash to another loads and runs (§14.15). A filename is
a claim, not a digest. So:

```
Runtime.Pull(pinned) fails (network, registry down, rate limit)
  → read <layout>/index.json, find the descriptor whose digest == image.index_digest
       absent  → InfrastructureError/IMAGE_PULL_FAILED   (retryable)
  → read blobs/sha256/<hex>, compute sha256 over those exact bytes
       unequal → ContractViolation/IMAGE_DIGEST_MISMATCH (never imported)
  → only then Runtime.LoadLayout(layout, image.index_digest)
  → run, and record ImageRef.delivery = "embedded"
```

OrkStack verifies the root index blob, the one link that ties the layout to
what apply pinned; the runtime verifies the child manifest and layers it
reaches by digest from there, as it does for a registry pull. The layout is a
cache with a proof, not a second source of truth: it serves only a digest
already resolved from a registry at apply. `make -C images layout` populates
it.

Operational notes: Docker Hub limits anonymous pulls (100 per 6 h per IPv4 or
IPv6 `/64`), so a busy pool SHOULD sit behind a pull-through mirror; and a
content digest proves *content* identity, not *publisher* identity — that is
what §4.7's signature requirement is for.

### 4.7 Image policy at the Orker

A deployment may bound which images run (`require_digest`,
`allowed_registries`) and a namespace may require a cosign signature
(`require_signed_by`). The operator surface — setting the floor, keys,
rotation, and the validate/apply/submit gates — is `docs/DEPLOY.md`, "Image
supply-chain policy". This is the Orker's half.

- **The policy travels on the input.** The engine composes the policy in
  force once, at submit, into an `ImagePolicySelection` — including the
  signing key's PEM — and carries it on the run plan and every
  `ExecuteTaskInput.image_policy`, exactly as `WorkspaceLimits` are. So every
  attempt of one task is judged by one rule, the Orker makes no control-plane
  read at dispatch, and an accepted run is never re-judged by a policy that
  changed after it was accepted. Unset (no floor and no signer) means no check
  and a byte-identical payload.
- **Where it runs:** after the retry-start commit check and before the
  workspace, the pin directory, staged secrets and the pull (phase
  `image-policy`, §7.1). A refused image leaves nothing on disk and is never
  fetched. An attempt whose `commit.json` exists is adopted and never judged
  again: a signature removed after the commit must not turn a durably
  committed result back into work (AGENTS.md §15).
- **What it checks:** the pure rule (`internal/task/image_policy.go`, the one
  place it is stated) against `ImageRef`, then — when a signer is named — the
  signature, verified by the Orker itself with its own registry client and the
  key the selection carries. The verifier accepts cosign's key-based layout
  (an OCI manifest at tag `sha256-<hex>.sig` in the same repository) when at
  least one signature layer verifies over this exact index digest with that
  key; it checks the digest claim, not the payload's docker-reference, as
  cosign's own key-based verify does.
- **Outcomes:** `IMAGE_NOT_PERMITTED` (a tag under `require_digest`, a
  registry outside the list), `IMAGE_NOT_SIGNED` (no signature object),
  `IMAGE_SIGNATURE_INVALID` (signatures exist, none verifies) — all
  `ContractViolation`, never retried; `IMAGE_SIGNATURE_UNAVAILABLE` (the
  registry could not be asked, or this Orker has no registry client) —
  `InfrastructureError`, retried. An unreachable registry, or any answer that
  is not a verdict, is never a pass and never "unsigned".
- **Rolling upgrade:** a run submitted through an engine replica of a release
  without the policy carries no selection, and its tasks are not checked by
  the Orker. The window is the rollout.

---

## 5. Durable commit protocol

### 5.1 What a commit means and does not mean

A commit means: **this activity's outputs are durably stored, and the container
must not run again for this logical activity.** It does not mean the
container's external side effects happened exactly once. Activities are
at-least-once (AGENTS.md §4); the commit only removes duplicate *container
execution after OrkStack has durably committed a result*.

Only success commits. A non-zero exit, a contract violation, a timeout, and a
cancellation all write **no** commit, because they must be free to retry.

### 5.2 The key

```
metadata/commits/{namespace}/{run-id}/{task-id}/commit.json
metadata/commits/{namespace}/{run-id}/{task-id}/{item:05}/commit.json   (an item of a fan-out)
```

- Under `metadata/`, beside the other control records (`docs/STORAGE.md`).
  Workload artifacts live under `orkflows/` (S3) or `objects/` (local), so a
  workload cannot collide with a commit key by naming an artifact
  `commit.json`.
- Deterministic and attempt-independent: namespace, run and task do not change
  across retries, a run pins exactly one definition, and an item's index is
  its position in an array the engine counted once and recorded. Adoption
  (§5.4) therefore applies per task and per item with no logic of the Orker's
  own. An item's commit carries the item's own `idempotency_key`.
- Namespace and task id are **validated, never sanitized** slugs, so neither
  can escape the prefix. **The run id is encoded, never parsed** (AGENTS.md §7):
  it becomes one key segment through the archive's reversible segment encoder
  — a clean component verbatim, anything else base64url-escaped with a marker.
- The Orkflow name is deliberately absent: a run id is already unique.

**Where the URI comes from.** A URI needs the artifact backend and endpoint,
which is process configuration workflow code may not read (AGENTS.md §2.1).
So the commit-store root is decided **at submit**, in `internal/api`, and
travels on the run plan (a replay-pinned workflow argument) beside the
contract limits (`WorkspaceLimits`), the transfer budget (§6.3), and the image
policy (§4.7). The workflow composes
`commit_uri = plan.CommitRoot + "/" + CommitKey(namespace, run, task)`, a pure
function of its arguments. `artifacts.CommitKey` / `ItemCommitKey` are the
single renderers.

**Resource policy stays where it is enforced.** The engine owns contract
limits; the Orker owns resource policy (`ORK_ORKER_MAX_CPU`,
`ORK_ORKER_MAX_MEMORY`, `ORK_ORKER_PIDS_LIMIT`, §9.3) and writes the
*effective* values into `task.json`. One run spans pools whose Orkers hold
different caps, so no submit-time value could represent them.

### 5.3 The store

The commit record does **not** go through `artifacts.Store`: that interface has
no conditional write, so a commit through it would be silently overwritable,
and the local backend is content-addressed, so a retry that does not yet know
the content cannot address the object by name (§14.2). `CommitStore`
(`pkg/artifacts/commit_store.go`) is its own store with two operations:
`Create` writes the commit **if and only if** no object exists at the URI
(an existing object returns `ErrPreconditionFailed`), and `Get` reads it,
returning the exact stored bytes' sha256 and etag.

- **S3:** a single `PutObject` with `If-None-Match: *`, mapping `412` to
  `ErrPreconditionFailed` and retrying `409 ConditionalRequestConflict` a
  bounded number of times — the mechanism immutable Orkflow versions and
  namespaces already use. It MUST NOT engage multipart upload, where
  conditional completion is not portable.
- **Local:** write `<dir>/.tmp/commit-<random>`, `fsync`, close, then
  **`link(2)`** it to the canonical path and unlink the temp. `link(2)` is the
  one primitive that is both atomic and create-if-absent (POSIX: it fails
  `EEXIST`, and on failure creates no link) — Git's loose-object publication.
  `O_CREAT|O_EXCL` at the canonical path is wrong (a crash mid-write leaves a
  torn commit that is then permanently unreplaceable), and `rename(2)` is
  wrong (it atomically *replaces*) (§14.7). Because `link(2)`'s return code
  can be wrong on NFS, an `EEXIST` is confirmed by reading the object, which
  the adoption path does anyway.

**The size cap is 4 MiB.** Every S3 write is a single `PutObject`, which
accepts up to 5 GiB, so nothing forces a small cap — and a 64 KiB cap is
unreachable for a legal workload: 256 `ArtifactRef`s with 8-character names
already serialize to ~67 KB (§14.7). A limit a conforming workload cannot
respect is a bug in the limit. 4 MiB leaves ~60× headroom over the worst
legal case.

**Conditional `PutObject` is a deployment requirement, not a property of
"S3-compatible".** AWS S3, SeaweedFS (documented as atomic cluster-wide),
Cloudflare R2, LocalStack and Ceph RGW (unversioned simple PUT) support
`If-None-Match: *`; Google Cloud Storage's XML API, Backblaze B2's S3 endpoint
and Garage do not (vendor documentation; SeaweedFS is exercised by the test
suite). A production deployment requires a store that implements it, returning
412 on conflict, on non-multipart writes.

The Orker verifies this at boot with **concurrent** writers — a sequential
`HEAD`-then-`PUT` proves ordering, not atomicity — against one fixed key per
commit schema version under `metadata/probes/commit-store/schema-v<N>/`. On
the first boot for a schema version two goroutines race to create the absent
key; exactly one must win and the other must see `ErrPreconditionFailed`. The
winner stays as a bounded capability record; later boots race the same two
writes, require both to be refused, and read back the exact candidate.
Versioning the key means a newer build never strands every Orker on an older
schema's winner. Any other outcome refuses to start with
`InfrastructureError/COMMIT_STORE_NOT_CONDITIONAL`, naming the endpoint — the
alternative is a deployment that silently loses immutability.

### 5.4 The sequence

**Retry-start check** (phase `commit-check`, before anything expensive and
before any container starts):

```
0. GET commit_uri
   ├─ found, decodes, schema_version == 1, identity matches
   │        → adopt: return the committed result, commit_adopted=true,
   │          DO NOT run the container
   ├─ found, decodes, schema_version != 1     → ContractViolation/COMMIT_SCHEMA_UNSUPPORTED
   │        (an Orker that cannot read a commit must neither overwrite it nor re-run past it)
   ├─ found, decodes, identity differs        → ContractViolation/COMMIT_IDENTITY_MISMATCH
   ├─ found, DOES NOT DECODE                  → reread once within this attempt; still
   │        undecodable → ContractViolation/COMMIT_UNDECODABLE (naming URI and size)
   ├─ not found                               → proceed
   └─ store unavailable                       → InfrastructureError/COMMIT_READ_FAILED
            (retryable — never proceed to execution blind)
```

**Identity is exactly these seven fields**, compared for byte equality:
`schema_version == 1`, `namespace`, `run_id`, `task_id`,
`orkflow_definition_digest`, `idempotency_key`, `image.index_digest`.
`image.platform_digest` and `image.platform` are audit data recorded by the
winning attempt and are never compared: the input's are always empty, and on a
mixed-architecture pool two well-formed attempts legitimately differ. The index
digest is what was pinned and does not vary by host.

**The undecodable commit** is reread once, after a short backoff, inside the
same attempt. Treating it as "no commit" would let a second container run after
a possibly durable commit; treating it as a mismatch would wedge a run on a
transient read. One reread separates a truncated read from corruption without
any cross-attempt state (the Orker is stateless, and a retry may land on
another host). Under `link(2)` publication a torn commit cannot be created, so
this branch fires only for real corruption.

**Adoption reconstructs the full result, including the inline copy**, so that
`commit_adopted` changes nothing an API consumer can observe except the flag:

```
commit.values.size_bytes <= 4096
  → GET commit.values.uri, verify sha256 == commit.values.sha256
       mismatch     → ContractViolation/VALUES_CORRUPT          (the store lied; terminal)
       fetch failed → InfrastructureError/VALUES_REFETCH_FAILED (retryable)
  → set values_json, values_inlined = true
commit.values.size_bytes > 4096
  → values_json stays empty, exactly as on a fresh run
```

Adoption does **not** re-verify artifact bytes: they may have been expired by a
lifecycle rule, and a commit is a record of what happened, not a live
guarantee that every byte still exists.

**Success path**, in this exact order (AGENTS.md §4.1):

```
1. validate values.json                (exists, readable, object, UTF-8, ≤ limit)
2. scan + validate output/artifacts/   (regular files, UTF-8 names, readable, count/size)
3. hash every artifact and values.json (sha256, streaming)
4. bound the commit                    (project ActivityCommit via RefFor, assert ≤ commit_max_bytes)
5. upload artifacts                    (content-addressed keys)
6. upload values.json                  (bytes verbatim)
7. upload stdout.log, stderr.log       (system artifacts)
8. CommitStore.Create(commit_uri)      (conditional, create-if-absent)
9. return ExecuteTaskResult to Temporal
```

Steps 1–4 happen before any upload, so a contract violation costs no bandwidth
and produces no orphans. Step 4 exists because the commit is the one thing a
conforming workload can overflow without breaking a workload-facing limit; it
fails `ContractViolation/COMMIT_TOO_LARGE` before a byte is uploaded rather
than at step 8, where the only available code would be retryable and would
loop. It measures a projection whose refs are byte-identical to the ones step 8
commits: `artifacts.RefPredictor.RefFor` returns exactly the `ArtifactRef` a
`Put` would, with no I/O. Step 8 is the completion boundary; nothing after it
can change the outcome.

**Output objects are immutable before step 8.** The S3 artifact key carries the
content digest as its last segment,
`orkflows/{orkflow}/{run}/{task}/{name}/{sha256}`, and the local backend is
content-addressed. A key names bytes, not a slot, so a re-upload of identical
content is idempotent, a losing attempt can never overwrite bytes a winning
commit hashed, and step 5 is safely repeatable across retries. The companion
manifest restores the exact logical name.

**Concurrent committer at step 8.** A `412` means another Orker committed the
same logical activity while this one worked — possible whenever Temporal
delivers a retry to a second worker before the first finished. The Orker reads
the existing commit, applies step 0's identity checks, and returns **that**
commit's result. The loser's uploads are inert: with content-addressed keys
they are byte-identical to the winner's or referenced by no commit.

### 5.4.1 Memoized completion (`cache:`)

A task that declares `cache:` (`docs/DSL.md`, "Caching" — the key, the scopes
and the TTLs are specified there) carries a `CacheContext`, and its attempt
takes one more step between staging and the image — the **memo step**. It is
Orker-side behaviour and changes nothing a container sees: no file,
`task.json` key, environment variable or exit code.

1. **Deferred staging** (phase `stage-inputs`). The upstream `values.json`
   objects are fetched and digest-verified (the render reads them) and a
   fan-in's item commits are read, but every artifact download, and the
   secrets, are recorded instead of performed.
2. **The key** (phase `memo`). `config.json` is rendered in memory with the
   same renderer, `run.json` encoded, and the memo key computed. An input
   reference with no digest cannot be keyed: a MISS `INPUT_UNHASHED`, and no
   memo is written.
3. **The lookup** (skipped, and recorded `BYPASSED`, when the run was
   submitted with `no_cache`), bounded as a whole by 30 s. The first failing
   check is the recorded miss reason, and **every** failing check means *run
   the task*, never a failure:

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
   replaced by a check: identity fields this activity's; `values`,
   `artifacts`, `system_artifacts` and `image` the source's references;
   `cache` naming the source — through the same create-if-absent, before
   success is reported. No image is pulled, no secret resolved, no container
   created. A conflict adopts the winner, as step 8 does; any other write
   error is `COMMIT_WRITE_FAILED`, the only way the memo step can fail an
   attempt. A memo that points at a HIT commit is followed once, so
   `cache.source` always names the run whose container produced the bytes.
5. **A miss or a bypass** completes staging (phase `stage-artifacts`), runs
   the container as always, and records the decision on the commit it writes.
   After that commit is **created** — never after a conflict's adoption — it
   writes the memo, a plain put that replaces whatever was at the key,
   best-effort on a detached 10 s context: a failure is logged and reported
   (`memo_write_error`), never an attempt failure.
6. **Adoption on retry** (step 0) returns the adopted commit's `cache`, so a
   retry after a hit's commit returns the same hit with `commit_adopted`.

A memo is a pointer, never state: deleting any memo, or the whole
`metadata/memo/` prefix, changes no result — only whether the next run's
containers run.

### 5.5 Crash coverage (AGENTS.md §11)

| Crash point | What survives | What happens next | Terminal state |
|---|---|---|---|
| Orker dies mid-container | **the container keeps running** (§14.17) | Heartbeat timeout (30 s) → Temporal retries per the task's policy. The new Orker finds no commit and re-runs. The abandoned container is reaped as below. | correct, container ran twice (at-least-once) |
| Orker dies after exit, before upload | nothing | Same as above. | correct, container ran twice |
| Orker dies mid artifact upload | some artifacts, no commit | Retry re-uploads. Orphans from the dead attempt are unreferenced. | correct |
| Orker dies after `commit.json`, before Temporal completion | the commit | Retry's step 0 **adopts** it: no second container run. | correct, container ran once |
| Orker dies after a cache hit's `commit.json`, before completion | this run's hit commit | Retry's step 0 adopts it: `cache` HIT with the same source, `commit_adopted`. | correct, container ran zero times |
| Orker dies after a fresh commit, before its memo write | the commit, no memo | The task completes on retry by adoption; the next run misses (`MEMO_ABSENT`) and writes the memo. | correct, one extra container run later |
| Orker restarts | image cache only (§6.4) | Temporal reschedules on any Orker in the pool (a pinned task fails `PIN_HOST_LOST` instead, `docs/DSL.md`, "Pins"). | correct |
| Object storage unavailable during upload | nothing committed | `InfrastructureError` → retried per policy. Storage failure can never look like success. | correct |
| Object storage unavailable at step 0 | — | `COMMIT_READ_FAILED`, retried. Execution never proceeds on an unknown commit state. | correct |
| Temporal temporarily unavailable | the commit, if written | Worker retries the completion; on adoption the result is returned again. | correct |
| Container exits 1 | logs + declared artifacts, no commit | `ActivityFailed` → retried per policy. | explicit failure |
| Exit 0, no / malformed / oversize `values.json` | logs, no commit | `VALUES_MISSING` / `VALUES_MALFORMED` / `VALUES_TOO_LARGE` → never retried. | explicit failure |
| Output limits exceeded | logs, no commit | `ARTIFACT_LIMIT_EXCEEDED`, or `COMMIT_TOO_LARGE` measured before any upload. | explicit failure |
| Outputs the Orker cannot read (`umask 077`) | logs, no commit | `OUTPUT_NOT_READABLE`, naming file, mode and owner. | explicit failure |
| Torn `commit.json` | the torn object | `link(2)` publication cannot produce one; an existing one is reread once, then `COMMIT_UNDECODABLE`. | explicit failure |
| Two attempts upload different bytes under one name | both objects | Different bytes are a different key, so the committed `sha256` still resolves. | correct |
| Object store without conditional PUT | — | Boot-time two-writer race fails → Orker refuses to start (`COMMIT_STORE_NOT_CONDITIONAL`). | refuses to run, never runs unsafely |
| Container exceeds its own timeout | logs, no commit | SIGTERM → grace → SIGKILL, `TimedOut/CONTAINER_TIMEOUT`, terminal by default (§8.1). | explicit failure |
| Cancellation, or SIGTERM ignored | logs, no commit | SIGTERM → grace → SIGKILL (§14.5); the Orker returns `ctx.Err()`. | canceled, never auto-retried |
| Input artifact unavailable / image pull fails | nothing | `INPUT_ARTIFACT_UNAVAILABLE` / `IMAGE_PULL_FAILED` → retried. | explicit failure |
| Tag moved between two executions | — | Impossible: the run pins a digest at apply (§4). | correct |

There is no stuck, half-completed, unknown, or database-surgery state. Every
row ends in a correct durable state or an explicit, categorized, actionable
failure.

**An Orker crash does not stop its container** (§14.17): `--rm` fires on
container *exit*, and there is no daemon-side "kill when the client
disconnects". So every container is labelled `ork.run`, `ork.task`,
`ork.attempt` and `ork.orker` (plus `ork.pin` / `ork.item` when they apply),
and every Orker at boot claims its stable `ORK_ORKER_ID` and force-removes
every container labelled with it — its own previous life's. Two live Orkers
must never share an id, or they would reap each other's work; the Orker holds
a process-lifetime lock on its identity before reaping. Duplicate *execution*
is inside the at-least-once contract; duplicate *resource consumption* until
the reap is a real operational cost, stated rather than hidden.

---

## 6. Orker dispatch: the `OCIDispatcher`

### 6.1 Shape

One implementation runs every activity: `internal/orker.OCIDispatcher`, over a
`ContainerRuntime` (`Pull`, `LoadLayout`, `Create`, `Start`, `Signal`, `Wait`,
`Remove`, `Reap`, `Describe`), the artifact, commit, log, attempt and memo
stores, the security policy, and the work root. `orkctl activity test` and
`orkctl task run` drive the same dispatcher, built by the same constructor.

Exactly one runtime ships: `DockerRuntime`, shelling out to `docker`. The
interface exists so Podman, containerd, Kubernetes Jobs, ECS or Nomad remain
*possible*, not because any is planned. **No Docker-specific detail may
appear outside `internal/orker/runtime_docker.go`.**

**The interface split is also the failure classification.** `Create` and
`Start` return `error`; `Wait` returns `(ExitStatus, error)`. A container that
never started never reaches `Wait`. So "the runtime refused" and "the workload
failed" are distinguished by *which call failed*, never by inspecting a number
— exit codes 125, 126 and 127 carry no runtime guarantee (§14.13).

### 6.3 Heartbeats and cancellation

Temporal delivers cancellation to an activity **only through a heartbeat
response**, so an Orker that stopped heartbeating during the container run
could never cancel it. Required, all four together:

1. **Engine** sets `HeartbeatTimeout: 30s` and `WaitForCancellation: true`.
   Without the timeout a dead worker is noticed only at `StartToCloseTimeout`;
   without `WaitForCancellation` the workflow may mark the task terminal
   before the Orker finishes killing and cleaning up.
2. **Orker** runs a heartbeat loop for the **entire** attempt, ticking every
   `ORK_ORKER_HEARTBEAT_INTERVAL` (default 5 s, bounded to 1 s–10 s: below
   1 s the SDK clamps the RPC deadline anyway, above a third of the heartbeat
   timeout two missed ticks would declare live activities dead). The detail is
   `task.HeartbeatProgress` — the phase, the container id, bytes logged, and
   the Orker's own `ORK_ORKER_ID` — and the engine reads it back for live
   introspection (§6.6). The container sees none of it.
3. **Orker** caps its worker's `MaxHeartbeatThrottleInterval` to that same
   interval. The Go SDK batches heartbeats into windows of
   `min(0.8 × HeartbeatTimeout, MaxHeartbeatThrottleInterval)`; inside a
   window no RPC leaves the process, and cancellation rides the RPC's
   *response* — so that window **is** the cancellation latency, 24 s with the
   cap unset however fast the loop ticks. The same window is the RPC's
   deadline, which makes the interval the longest Temporal frontend outage a
   running container survives — the tradeoff that makes it an operator
   setting. A harness that runs the real dispatcher against a real Temporal
   must set both, or it measures the SDK's default.
4. **Orker** reacts to `ctx.Done()` with the AGENTS.md §3.3 sequence:

```
cancellation observed                     (phase "cancel")
  → Runtime.Signal(SIGTERM)
  → wait cancellation_grace_period_seconds (default 30)
  → Runtime.Signal(SIGKILL) if still running
  → Runtime.Remove on a DETACHED context (cleanup must survive cancellation)
  → close the log sink
  → return ctx.Err()
```

The Orker MUST return `ctx.Err()` only *after* the server delivered
cancellation; returning `nil` after cancellation would report success and MUST
NOT happen. Containers are also created with `--stop-timeout` equal to the
grace period, so a daemon-side stop honours the same window.

Timeouts, which must not be confused:

| Timeout | Owner | Value | Effect |
|---|---|---|---|
| `timeout_seconds` | Orker | task's `timeout:` | Armed when the container starts, after staging and the pull. Kills the container (SIGTERM→grace→SIGKILL), returns `TimedOut/CONTAINER_TIMEOUT`. Fires first. |
| `HeartbeatTimeout` | Temporal | 30 s | Detects a dead Orker (`TIMEOUT_TYPE_HEARTBEAT`). |
| `StartToCloseTimeout` | Temporal | `timeout_seconds + grace + transfer_budget` | Backstop for an Orker wedged outside the container run (`TIMEOUT_TYPE_START_TO_CLOSE`). |

**`transfer_budget` is derived, not a constant.** Inside Start-to-Close sit
input staging, the image pull, hashing and uploading up to
`artifact_max_count × artifact_max_bytes`, and a task with no `retry:` has one
attempt — so an undersized constant would make a legal large upload terminal
on its first timeout. Heartbeats do not reset Start-to-Close and already
detect a dead worker, so Start-to-Close only has to be large enough not to
kill honest work:

```
transfer_budget_seconds =
    ORK_TRANSFER_BUDGET_SECONDS                                   if set
    else max(300, artifact_max_count × artifact_max_bytes
                  ÷ ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC)            default 10 MiB/s
```

Both settings are engine-scoped: `internal/api` resolves the integer at submit
and carries it on the run plan, because `ActivityOptions` is set in workflow
code, which may not read process configuration. At the defaults this is about
36 hours — deliberately generous; an operator who knows their storage sets the
seconds directly.

### 6.4 Orker state

The Orker stays stateless and disposable. Its only persistent local state is
the OCI image cache, per-attempt execution directories (removed on every exit
path), and an optional transient download cache. If an Orker disappears,
Temporal retries elsewhere and the commit protocol decides whether the
container runs again.

A pin's shared directory (§3.1) is the one longer-lived kind of ephemeral
execution directory: its lifetime is the pin's, so it survives between tasks
and between attempts. It is still local, disposable and reconstructible by
re-running, and the pinned task is the one place "Temporal retries elsewhere"
does not apply: the pin and its run fail `PIN_HOST_LOST` instead (AGENTS.md
§5).

`ork-pin-<run>-<pin>-<instance>/` is created under the work root by the first
member attempt that arrives (the Orker has no hook at session creation) and
re-asserted to `0700`/`1777` by every later one, so a workload that chmod'ed
its view of `shared/` cannot lock the next member out. The instance id must
match `^[0-9a-f-]{36}$` before it becomes a path segment — refused, never
rewritten, because a rewritten id would name a different pin's directory.
Removal walks the tree chmod-first (a workload may have left `0500`
directories). It ends by whichever of three paths comes first:

1. **`ReleasePin`** — the normal path. Once every member is terminal the
   engine schedules this activity *on the pin's own session*, so it can only
   run on the host that owns the directory. It is idempotent and best effort:
   a failure is logged and never fails the run, because cleanup is not
   orchestration.
2. **Boot reap** — at every Orker start, unconditionally (also when
   `ORK_ORKER_MAX_PINS` is now `0`), every `ork-pin-*` is removed. A pin can
   never outlive the process that opened its session — a fresh session
   resource id is minted per worker start — so every pin directory found at
   boot is dead by construction, not by heuristic.
3. **Lease sweep** — `lease.json`, `0600`, sits *beside* `shared/`, never
   inside it, so nothing a workload can reach can forge or extend it. It is
   written once, on first arrival, as first arrival + the pin's lifetime +
   6 minutes of slack, and never re-armed. A sweep every minute removes any
   pin directory whose lease expired — or, with the lease missing or
   undecodable, whose directory is older than the slack. It is the only
   collector when the run is terminated from outside the workflow while the
   Orker stays up (no workflow code runs, so no `ReleasePin`). It can never
   remove a live pin's directory: the session's lifetime is measured from
   session creation, which precedes first arrival.

A pin directory has no per-pin size quota; it is bounded by the lease and the
host's disk, and `ORK_ORKER_TMPFS_BYTES` does not apply (it is a bind mount).

### 6.5 Pool liveness

`DescribeOrker` is the pool-liveness probe: the engine dispatches it as a
Temporal activity on the pool's own channel (5 s / 10 s timeouts,
`MaximumAttempts: 2`), so a pool proves it is alive only when an Orker is
actually polling it. Its result also carries the Orker's runtime, security
profile, concurrency, pin capacity, cordon state, and the capability flags
submit reads (§2.1). The Admin **Activities** page and `orkctl activity list`
render the engine's catalog, because that is engine configuration, not
something to ask an Orker.

### 6.6 Live introspection and timing

Neither changes anything a container sees.

- **Live.** `RunService.InspectRun` (`orkctl run inspect`, the run page's live
  line) reports each pending attempt's state from Temporal and the heartbeat
  detail of §6.3: phase, container id, bytes logged (stdout and stderr,
  counted before any retention cap) and Orker id. A detail carried over from a
  previous attempt is refused, never shown as the current one; a heartbeat
  older than two ceiling ticks (`task.HeartbeatStaleAfter`, 20 s) is
  **stale**, reported with the instant Temporal will time the attempt out.
  Absent evidence is reported as unavailable with its reason, never as zero.
- **After the fact.** Every attempt record (§7.1 step 16) carries the phases
  the attempt passed through, contiguous, each on the Orker's clock, the last
  ending at the attempt's finish. `orkctl run timing` splits each task into
  wait (scheduled → the producing attempt's start) and run, and lists every
  attempt's phases; a value that cannot be known prints `unavailable`, never
  `0s`.

---

## 7. Activity lifecycle

### 7.1 The sequence

The **phase** column is the name the Orker reports in its heartbeat and
records on the attempt (`TaskAttempt.phase`/`phases`). Phase names are
strings, not an enum, so a reader of an older build can read one it has never
heard of.

| # | Phase | Step |
|---|---|---|
| 1 | — | **Open the log sink**, first, always; closed before return |
| 2 | `start` | **Heartbeat loop start**, until step 15; input validated |
| 3 | `commit-check` | **Commit check**: GET `commit_uri` → adopt and jump to 15, or proceed (§5.4) |
| 3b | `image-policy` | **Image policy**, only when the input carries one (§4.7) |
| 4 | `workspace` | **Workspace**: per-attempt dir `0700`; `input/` `0555`, `output/` `1777`, `tmp/` `1777`; a pinned task's shared directory |
| 5 | `stage-inputs` | **Input stage**: upstream artifacts → `input/artifacts/`, upstream `values.json` → `input/upstream/`, secrets → `input/secrets/` (a cached task: `values.json` only) |
| 5b | `memo` | **Memo**, a cached task only: render, key, lookup (§5.4.1); a HIT writes this run's commit and jumps to 15 |
| 5c | `stage-artifacts` | **Deferred staging**, a cached task only: the recorded downloads, then secrets |
| 6 | `image` | **Image**: `Runtime.Pull(image.pinned)`, or the verified embedded layout (§4.6); record platform digest and delivery |
| 7 | `render` | **Render**: resources and network resolved against policy; `with:` → `config.json`; `run.json`; `task.json`; input tree sealed |
| 8 | `create` | **Create** from the security profile (§9.2) |
| 9–10 | `run` | **Run and wait**: stream stdout/stderr to the sink and to disk; wait for exit, cancellation (`cancel`) or the task timeout (`timeout`) |
| 11 | `exit` | **Exit capture**; non-zero → classify and go to 14 |
| 12 | `validate` | **Validate** `values.json` and `output/artifacts/`, including readability, then bound the projected commit (§5.4 steps 1–4) |
| 13 | `commit` | **Upload and commit**: artifacts → `values.json` → streams → `commit.json` (§5.4) |
| 14 | — | **Failure path**: scan `output/artifacts/` under the same §3.5 and readability rules; upload streams and the artifacts that passed; attach refs and any scan note to the error; write NO commit; return the workload's error |
| 15 | — | **Cleanup**: remove the container (detached context), remove the workdir and secrets, stop the heartbeat, close the log sink |
| 16 | — | **Attempt record**: one `TaskAttempt`, best-effort (below) |

**IMAGE comes before RENDER, and the order is load-bearing.** `task.json`
carries `image.platformDigest`, `image.platform` and `image.delivery`, which do
not exist until the image is resolved. Rewriting `task.json` after the pull
instead would make an input file mutable during setup, and §3.1 depends on
`input/` being sealed `0555`/`0444` before the container starts. Pulling first
also fails `IMAGE_PULL_FAILED` before any staging bandwidth is spent.

**The failure path scans too**, so the regular-file, symlink, device, FIFO,
readability, count and size rules apply on exactly the path a broken or
hostile image is most likely to take. A violation there is a **note attached
to the workload's error** (`ActivityFailureDetail.scan_notes`), never a
replacement for it: a task that exited 1 *and* wrote a symlink is reported as
exit 1 with the symlink named and not uploaded. Likewise a workload's exit 1
stays exit 1 even if the stderr upload also failed — replacing it with a
storage error would tell the author their build failed because of the bucket.
A failing task's compiler errors, test reports and core dumps are uploaded,
because they are what the failure is for.

**Step 16, the attempt record.** Temporal owns retries, so the workflow only
ever sees a task's *final* outcome; an intermediate attempt's error is
consumed by the scheduler. The Orker is the only process that observes every
attempt, so at the end of each one it writes a `TaskAttempt` to
`metadata/attempts/{run}/{task}/{attempt}.json` (an item: one segment deeper,
`docs/STORAGE.md`): outcome, error in the engine's `Error` shape, exit code,
the phase it ended in and every phase it passed through, the image as
executed, Orker, pool and build, the resources it reserved, and the cache
decision. Succeeded and adopted attempts get one too, so a timeline's last row
is never synthesized from two producers. It runs on a **detached** context —
a cancelled attempt is the one most wanted — and is **never a failure**: a
write error is logged at Warn and the attempt's own outcome is reported
unchanged. An Orker writes this record and never reads one; nothing an
attempt does depends on an earlier attempt's record. `orkctl run attempts`
lists them.

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
     ├─ image policy (if carried)
     ├─ Store.Get(upstream artifacts)  ──> /orkstack/input/artifacts/<task>/<file>
     ├─ Store.Get(upstream values)     ──> /orkstack/input/upstream/<task>/values.json
     ├─ Runtime.Pull(pinned)
     ├─ render(with)                   ──> /orkstack/input/config.json
     ├─ compose(input)                 ──> /orkstack/input/task.json
     └─ Runtime.Create/Start(spec)

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

The category names are also the exact Temporal `ApplicationError` type
strings (`internal/task/contract.go`), spelled once and shared by the engine
and the Orker.

| Category | Who produces it | Retried |
|---|---|---|
| `ActivityFailed` | Orker: the container exited non-zero for its own reasons | per the task's `retry:` |
| `InfrastructureError` | Orker: storage, registry, runtime, or workspace failed | per the task's `retry:`, except three host misconfigurations (§8.2) |
| `ContractViolation` | Orker or engine: the image or the definition broke the ABI | **never** |
| `TimedOut` | Orker (container exceeded `timeout_seconds`) **or** Temporal (`*TimeoutError`) | **never, by default** — see below |
| `Cancelled` | Temporal, delivered through a heartbeat; the Orker returns `ctx.Err()` | never automatically |

A task with no `retry:` (or `maximum_attempts` ≤ 1) has exactly one attempt
whatever the category; "retried" means "retryable when the task's policy
allows another attempt". Non-retryability is carried by the error itself: the
Orker constructs every non-retryable reason with
`temporal.NewNonRetryableApplicationError`, whose flag wins over any policy
(`RetryableReason`, `internal/orker/failure.go`).

**`TimedOut` is terminal by default.** This differs from AGENTS.md §4's
suggested default ("retry according to policy"): a workload that times out
repeatedly fails once and visibly rather than burning its retry budget on the
same wall clock. Changing it is one line in `RetryableReason`.

`Cancelled` is never an `ApplicationError`. `TimedOut` is an
`ApplicationError` **only** when the Orker killed the container itself; a
Temporal-produced timeout arrives as `*TimeoutError` and the engine maps
`TIMEOUT_TYPE_{START_TO_CLOSE,HEARTBEAT}` to `TimedOut` on the reading side. A
heartbeat timeout means the Orker died — a fact about the host, not the
workload — so it stays retryable under the task's policy, which is what moves
the task to a live Orker.

### 8.2 Reason codes

Every reason the Orker reports for a task attempt (`internal/orker/failure.go`):

| Reason code | Category | Retryable | Trigger |
|---|---|---|---|
| `CONTAINER_EXIT_NONZERO` | ActivityFailed | yes | **every** non-zero status returned by `Wait`, whatever the number; the message says when the kernel OOM-killed the container at its memory limit |
| `CONTAINER_TIMEOUT` | TimedOut | **no** (§8.1) | Orker killed the container at `timeout_seconds` |
| `RUNTIME_REFUSED` | InfrastructureError | yes | `Create` or `Start` returned an error — the container never ran |
| `WORKSPACE_NOT_BINDABLE` | InfrastructureError | **no** | the daemon cannot bind the workspace path — a host misconfiguration retrying will not fix |
| `WORKDIR_NOT_BINDABLE` | InfrastructureError | **no** | the boot probe found `ORK_ORKER_WORKDIR` not bind-mountable, or its path holds a comma or control character; the Orker refuses to start |
| `BIND_PROBE_IMAGE_UNAVAILABLE` | InfrastructureError | yes | the boot probe's own image (`alpine:3.20`) is not local and could not be pulled within `ORK_DOCKER_PULL_TIMEOUT`; the Orker refuses to start, naming the image rather than the work root |
| `WORKDIR_FAILED` | InfrastructureError | yes | the per-attempt directory, a pin's directory, or an input file could not be created |
| `IMAGE_PULL_FAILED` | InfrastructureError | yes | pull failed or timed out (`ORK_DOCKER_PULL_TIMEOUT`), and no verified embedded copy exists |
| `IMAGE_DIGEST_MISMATCH` | ContractViolation | no | pulled or embedded content does not match `image.pinned` |
| `IMAGE_NOT_PINNED` | ContractViolation | no | `ImageRef.pinned` empty or not `…@sha256:…` |
| `IMAGE_NOT_PERMITTED` | ContractViolation | no | the image is outside the run's image policy (§4.7) |
| `IMAGE_NOT_SIGNED` | ContractViolation | no | the namespace requires a signature and the registry has none for this digest |
| `IMAGE_SIGNATURE_INVALID` | ContractViolation | no | signatures exist and none verifies with the named key over this digest |
| `IMAGE_SIGNATURE_UNAVAILABLE` | InfrastructureError | yes | the registry could not be asked for a signature, or this Orker has no registry client |
| `VALUES_MISSING` | ContractViolation | no | exit 0 with no `/orkstack/output/values.json` |
| `VALUES_MALFORMED` | ContractViolation | no | not valid UTF-8 JSON, or not a top-level object |
| `VALUES_TOO_LARGE` | ContractViolation | no | over `limits.valuesMaxBytes` |
| `ARTIFACT_NOT_A_FILE` | ContractViolation | no | a directory, symlink, device, socket or FIFO in `output/artifacts/` |
| `ARTIFACT_LIMIT_EXCEEDED` | ContractViolation | no | too many artifacts, or one too large |
| `ARTIFACT_NAME_NOT_UTF8` | ContractViolation | no | an `output/artifacts/` file name is not valid UTF-8 (§3.5) |
| `ARTIFACT_UPLOAD_FAILED` | InfrastructureError | yes | store `Put` failed |
| `OUTPUT_NOT_READABLE` | ContractViolation | no | the Orker cannot open a file the workload wrote; names path, mode and owner; the fix is `umask 022` (§3.1) |
| `INPUT_ARTIFACT_UNAVAILABLE` | InfrastructureError | yes | an upstream artifact could not be fetched |
| `INPUT_ARTIFACT_CORRUPT` | ContractViolation | no | fetched bytes do not match the recorded sha256 |
| `INPUT_TOO_LARGE` | ContractViolation | no | `ExecuteTaskInput` would exceed 256 KiB (raised by the engine); the same code refuses a run input over 256 KiB at submit, before any run starts (`docs/DSL.md`, "Inputs") |
| `EACH_ITEM_UNAVAILABLE` | ContractViolation | no | an item's `each.over` no longer resolves to the array, of the length, the engine counted — unreachable while the store honours its digests |
| `RENDER_FAILED` | ContractViolation | no | template error, missing key, `<no value>`, or an `env` name not allowlisted |
| `SECRET_UNRESOLVED` | InfrastructureError | yes | a `SecretRef.source` could not be resolved |
| `SECRET_SCHEME_FORBIDDEN` | ContractViolation | no | the Orker does not resolve that source scheme (`ORK_ORKER_SECRET_SCHEMES`), or the name is malformed |
| `SECRET_NOT_PERMITTED` | ContractViolation | no | the source is outside this Orker's `ORK_ORKER_SECRET_ALLOW` floor (`docs/DEPLOY.md`, "Secrets on a shared pool") |
| `RESOURCE_REQUEST_REFUSED` | ContractViolation | no | the request exceeds the Orker's cap, or is not a positive finite quantity |
| `NETWORK_MODE_REFUSED` | ContractViolation | no | the requested mode is not `default` or `none` |
| `COMMIT_READ_FAILED` | InfrastructureError | yes | commit store unreadable at step 0 |
| `COMMIT_WRITE_FAILED` | InfrastructureError | yes | conditional PUT failed for a non-precondition reason |
| `COMMIT_SCHEMA_UNSUPPORTED` | ContractViolation | no | existing commit has an unknown `schema_version` |
| `COMMIT_IDENTITY_MISMATCH` | ContractViolation | no | existing commit differs on one of the seven identity fields (§5.4) |
| `COMMIT_UNDECODABLE` | ContractViolation | no | an object at `commit_uri` is still not valid `ActivityCommit` JSON after the in-attempt reread |
| `COMMIT_TOO_LARGE` | ContractViolation | no | the projected `ActivityCommit` exceeds `limits.commitMaxBytes`, measured before any upload |
| `COMMIT_STORE_NOT_CONDITIONAL` | InfrastructureError | **no** | the boot-time two-writer race proved the store lacks conditional create; the Orker refuses to start (§5.3) |
| `VALUES_CORRUPT` | ContractViolation | no | on adoption, the refetched `values.json` does not match `commit.values.sha256` |
| `VALUES_REFETCH_FAILED` | InfrastructureError | yes | on adoption, `commit.values.uri` could not be fetched |

Two Orker codes are not task-attempt reasons: `ORKER_CORDON_INVALID`
(ContractViolation) refuses a malformed cordon control request, and `PANIC`
is only what the Orker's attempt observer and per-reason metrics record when
an attempt unwinds by panic — Temporal's activity worker turns the panic into
its own error, never into a reported reason.

The engine's own codes, independent of any one DSL feature:

| Code | Where it lands | Meaning |
|---|---|---|
| `TASK_FAILED` | task `Error.code` | the default when a failure carries no Orker reason code; the message holds the cause |
| `TASK_CANCELED` | task, Cancelled | the task was canceled (status `CANCELED`) |
| `TASK_OUTPUT_INVALID` | task, ContractViolation | a committed or await-delivered `values.json` could not be converted to the task's output payload |
| `ACTIVITY_TIMEOUT` | task, TimedOut, not retryable | Temporal's start-to-close timeout ended the activity (§8.1) |
| `ORKER_HEARTBEAT_TIMEOUT` | task, InfrastructureError, retryable | the Orker stopped heartbeating — it died (§8.1) |
| `INVALID_POOL`, `RESERVED_QUEUE` | non-retryable refusal to dispatch; a fan-out node's own code | the task's pool has no route in the run's plan, or resolves to a channel the engine reserves for itself |
| `INVALID_COMMIT_KEY` | non-retryable refusal to dispatch | the namespace, run ID and task ID cannot compose a commit key (§5.2) |
| `INVALID_RUN`, `INVALID_ORKFLOW` | the run's workflow failure | input guards: no run ID, or an Orkflow with no tasks |
| `RUN_FAILED`, `RUN_CANCELED`, `RUN_TERMINATED`, `RUN_TIMED_OUT` | run `Error.code`, read from the history event that closed the execution | failed; canceled; terminated from outside the workflow (status `CANCELED`, message is the termination reason); exceeded a run timeout |
| `RUN_START_FAILED` | run `Error.code` | the run's start failed after its ID was minted (`docs/STATS.md`) |
| `RUN_HISTORY_LIMIT` | run `Error.code`, InfrastructureError, not retryable | the run would have exceeded the cluster's per-execution history limit; it stopped explicitly instead of being terminated. The message names how much history was recorded and kept back; `details` names what was still running, the pins open and the awaits open. Failure handlers still run, within the history kept back for them; a handler there was no room to start is `CANCELED`. A composed task whose child ends this way fails `CHILD_RUN_FAILED`, InfrastructureError (`docs/DEPLOY.md` §1 `engine.history.*`). A run the cluster itself terminated at its history limit (the one gap: a failure path landing many large results at once) is reported with this code too, never as `RUN_TERMINATED` |
| `RUN_STATE_UNSUPPORTED` | run `Error.code`, ContractViolation, not retryable | the run carried on into a fresh history under a newer release, and this release cannot carry on its saved state (its format is newer, or an older release picked it up and refused it before any side effect). The run stops explicitly and still publishes its terminal event: tasks that finished keep their outcome, the rest are `CANCELED`. Roll the engines forward (`docs/DEPLOY.md` §4). A composed task whose child ends this way fails `CHILD_RUN_FAILED`, ContractViolation |
| `RUN_STATE_CORRUPT` | run `Error.code`, ContractViolation, not retryable | the run's saved state failed verification (a hash, size, location, structure or row check) and was not used; the run has stopped, with its terminal event. A composed task whose child ends this way fails `CHILD_RUN_FAILED`, ContractViolation |
| `RUN_STATE_UNAVAILABLE` | run `Error.code`, InfrastructureError, not retryable | the run's saved state could not be read from object storage within its restore timeout; the run has stopped, with its terminal event. A composed task whose child ends this way fails `CHILD_RUN_FAILED`, InfrastructureError |
| `RUN_DEFINITION_UNAVAILABLE` | run `Error.code`, InfrastructureError, not retryable | the run's definition, too large to travel with its start and passed by reference (`docs/DEPLOY.md` §1, `engine.definition_by_reference`), could not be read from object storage before the restore timeout; the run has stopped, with its terminal event (a run that stops before its first task publishes `RUN_CREATED` and `RUN_FAILED`) |
| `RUN_DEFINITION_CORRUPT` | run `Error.code`, ContractViolation, not retryable | the run's definition passed by reference failed verification (a hash, size or structure check, or it is not the definition the run started with) and was not used; the run has stopped, with its terminal event |

Codes that belong to one construct — pins (`PIN_HOST_LOST`,
`PIN_UNAVAILABLE`, `PIN_TIMEOUT`, …), joins (`CONDITION_FAILED`), awaits,
fan-out and composition — are documented with that construct in
`docs/DSL.md`.

**There is no exit-code table, deliberately.** A workload choosing `exit 126`
produces a status byte-identical to the CLI's own diagnostic for a missing
entrypoint (§14.13), and POSIX makes 127 the shell's ordinary "command not
found" — so a "not executable"/"not found" row would permanently refuse to
retry a routine `activity-shell` failure. AGENTS.md §3.2: any non-zero exit
means the activity failed, and retry behaviour belongs to policy, not magic
exit codes. The signal that genuinely distinguishes "the runtime refused" from
"the workload failed" lives at the interface boundary (§6.1).

### 8.3 Engine side

The engine reads the `ApplicationError` type and details and populates
`Error{code: <reason>, message, details, retryable, category}`. The event type
stays `TASK_FAILED`; the category is data on the error, so the UI can say
"contract violation — this image did not write values.json" instead of a Go
error string. Cancellation keeps its special case (`STATUS_CANCELED`,
projected on a disconnected context).

It also records **why retrying stopped**: Temporal's `RetryState` is mapped
onto `TaskRun.retry_state` (`MAXIMUM_ATTEMPTS_REACHED`,
`NON_RETRYABLE_FAILURE`, `TIMEOUT`, `CANCEL_REQUESTED`, …), because three
attempts and a terminal failure otherwise look identical on the snapshot to
one non-retryable attempt. It is derived and data-only: it issues no command
and changes no decision. Each individual attempt's reason — including the
ones Temporal discarded — is in that attempt's record (§7.1 step 16), in the
same `Error` shape.

---

## 9. Security

### 9.1 Forbidden, without exception

An Orkflow cannot express any of these, and the Orker refuses to construct
them. No configuration key exists for any of them, which is a stronger
guarantee than validation.

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

The Docker-socket prohibition is a **target** rule as well as a source rule:
nothing may mount anything at a container path that would shadow a runtime
socket.

### 9.2 Enforced on every container

```
--read-only                                   root filesystem
--cap-drop=ALL
--security-opt no-new-privileges
--security-opt seccomp=builtin                default profile, named explicitly
--pids-limit <policy>                         default 512
--memory <request or policy default>          always set; never unlimited
--cpus <request or policy default>            always set; never unlimited
--network none|default                        always explicit, never implicit
--init                                        PID 1 reaps zombies, forwards signals
--workdir /orkstack
--stop-timeout <grace>
--label ork.run=… --label ork.task=… --label ork.attempt=… --label ork.orker=…
--label ork.pin=<instance> / ork.item=<index>   when they apply
--mount type=bind,source=<attempt>/input,destination=/orkstack/input,readonly
--mount type=bind,source=<attempt>/secrets,destination=/orkstack/input/secrets,readonly
--mount type=bind,source=<attempt>/output,destination=/orkstack/output
--mount type=bind,source=<attempt>/tmp,destination=/orkstack/tmp
--mount type=bind,source=<pin>/shared,destination=/orkstack/shared   pinned tasks only
--tmpfs /tmp:rw,nosuid,nodev,size=<policy>    default 64 MiB
--user <orker-uid>:<orker-gid>                only with runAsOrkerUser / ORK_ORKER_FORCE_USER
--env <task env:>                             the task's own, nothing else
```

Rootless execution is used when the runtime supports it. Per-container user
namespace remapping is not implementable (`--userns=host` is the only valid
`docker run` value and it *reduces* isolation); deployments wanting it
configure the daemon.

### 9.3 Resource policy

A task may request `resources: {cpu: "2", memory: 4Gi}` (Kubernetes
spelling). Orker policy supplies defaults and caps:
`ORK_ORKER_DEFAULT_CPU` (`1`), `ORK_ORKER_DEFAULT_MEMORY` (`1Gi`),
`ORK_ORKER_MAX_CPU` (`8`), `ORK_ORKER_MAX_MEMORY` (`16Gi`),
`ORK_ORKER_PIDS_LIMIT` (`512`). A request above the cap is **refused**
(`RESOURCE_REQUEST_REFUSED`), not silently clamped: a workload that asked for
64 GiB and got 4 GiB fails confusingly later. A zero, negative or non-finite
request is refused too (for memory, one under a single byte, which renders as
zero), because the runtime reads a zero limit as unlimited. An unreadable cap
refuses rather than running uncapped, and a cap or default that is not a
positive, finite quantity stops the Orker at boot. **No workload ever
receives unlimited resources**, including one that requested nothing. The
effective values reach the workload in `task.json` and the attempt record.

### 9.4 Secrets

Secrets reach a container only as files under `/orkstack/input/secrets/`,
resolved from a `SecretRef.source` the Orker understands (`env:NAME` from the
Orker's environment, `file:/path` from its filesystem;
`ORK_ORKER_SECRET_SCHEMES`). An unknown scheme is refused rather than ignored,
and a source outside the Orker's `ORK_ORKER_SECRET_ALLOW` floor or the
namespace's allowed list is refused (`docs/DEPLOY.md`, "Secrets on a shared
pool"). Secret bytes never appear in a proto message, Temporal history, the
run archive, a log chunk, or `task.json` — which carries names and paths only.
The per-attempt secrets directory is removed on every exit path, including
panic and cancellation. A cached task resolves no secret on a hit, which is why
a task that mounts a secret should not be cached (`docs/ACTIVITY_IMAGES.md`).

---

## 10. First-party activities

First-party activities use **exactly** the public OCI contract. There is no
privileged internal plugin path, no shared Go SDK, and nothing an external
image cannot do. They are reference implementations; their per-image
contracts are `docs/ACTIVITY_IMAGES.md` and each `images/activity-*/README.md`.

### 10.1 The Dockerfile and entry-script rules

Mandatory for first-party images and recommended for everyone. "No SDK" means
no library that knows about OrkStack; an image installs whatever tools its
work needs (`curl`, `git`, `gomplate`) and needs only a JSON reader to speak
the ABI.

```dockerfile
# first-party images pin their own bases
FROM alpine:3.20@sha256:…
RUN apk add --no-cache jq
COPY entry.sh /usr/local/bin/entry.sh
RUN chmod 0555 /usr/local/bin/entry.sh
# never root
USER 65532:65532
# exec form, mandatory
ENTRYPOINT ["/usr/local/bin/entry.sh"]
```

**Comments go on their own lines.** A Dockerfile has no inline comments: `#`
starts a comment only as a line's first non-whitespace character.
`USER 65532:65532   # never root` makes the group `65532   # never root`, and
every container is refused before it starts.

1. **`ENTRYPOINT` in exec form.** Shell form runs the process as a child of a
   shell that does not forward signals, so SIGTERM never arrives.
2. **`exec` only when the script produces nothing itself.** A thin wrapper
   ends in `exec "$@"`. **A script that must write `values.json` after the
   workload exits MUST NOT `exec`** — nothing after `exec` runs, so every
   success would end `VALUES_MISSING` — and MUST use rule 2a instead.
   - **2a. Background, trap, `wait`.** Run the workload in the background,
     install a TERM trap that forwards to the child, and `wait`. `--init`
     forwards SIGTERM to its *direct child only*, so a trapless entry script
     dies on the default disposition and takes the workload with it — exit
     143, no `values.json` (§14.14). The shape is `run_supervised` in §10.2.
   - **2b. Never call the supervisor inside `$( )` or on either side of a
     pipeline.** Both create a subshell, which resets traps to the default
     action (POSIX §2.13), so cancellation is lost; a substitution also
     swallows the workload's stdout, emptying the durable `stdout.log`
     (§14.18). Plain invocation and redirection are the only safe forms.
3. **`USER` is not root.**
4. **No `VOLUME`** — it would shadow the workspace mounts.
5. **The base image is digest-pinned**, and so is every downloaded tool
   (activity-template verifies gomplate's release binary against a pinned
   SHA-256 at build time).
6. **No writes outside `/orkstack/output`, `/orkstack/tmp`, `/tmp`** — the
   root filesystem is read-only. Images whose tools want a home set
   `HOME=/orkstack/tmp`.
7. **`umask 022` before writing anything**, so the Orker can read every output
   whatever umask the runtime supplies (§3.1).
8. **`with.command` is a JSON array, executed as argv** — never a shell string
   passed to `sh -c` — so a rendered value cannot become code whatever it
   contains (§14.18). An image that genuinely needs a shell takes
   `["sh", "-c", "…"]`, which puts the boundary in the document.
9. **Honour the task's `env:`** (§3.7): never clear, filter or re-exec the
   environment. Defaults go in the Dockerfile as `ENV`, overridable by `env:`,
   and are documented in the image's README.
10. **Validate `with:` before any side effect** and reject an unknown `op`
    (exit `2`), and declare the interface label (`docs/ACTIVITY_IMAGES.md`).

### 10.2 The entry-script pattern

`images/activity-shell/entry.sh` is the reference; every first-party
`entry.sh` uses its supervisor:

```sh
set -u           # not -e: the workload's non-zero status must not abort before values.json
umask 022        # rule 7

# `command` is a JSON array: jq's @sh quotes each element, so no value can escape.
eval "set -- $(jq -r '(.command // []) | @sh' /orkstack/input/config.json)"

# run_supervised runs its ARGUMENTS as the workload, in the background, with a
# TERM trap that forwards to the child, and returns the child's real status.
# MUST NOT be called inside $( ) or on either side of a pipeline (rule 2b).
run_supervised() {
    "$@" &
    child=$!
    trap 'kill -TERM "$child" 2>/dev/null' TERM INT
    wait "$child"
    rc=$?
    # POSIX: a trapped signal makes `wait` return >128 IMMEDIATELY, before the
    # child has finished. Wait again on that path only — an unconditional
    # second wait would discard the status when no signal arrived.
    if [ "$rc" -gt 128 ]; then
        wait "$child"
        rc=$?
    fi
    trap - TERM INT
    return "$rc"
}

run_supervised "$@"
rc=$?
printf '{"exitCode":%d}\n' "$rc" > /orkstack/output/values.json
exit "$rc"
```

To capture a workload's stdout as data, redirect — `run_supervised "$@" >
"$TMP/stdout"`, then `cat` it back to the log stream — never substitute.

The first-party images:

| Image | `op` values | `values.json` | Artifacts |
|---|---|---|---|
| `activity-shell` | `exec` (default), `eval`, `script` | `exitCode`; `eval` adds `value`; `script` adds `scriptPath` | files the workload writes to `output/artifacts/` |
| `activity-http` | `get` (default), `post`, `put`, `patch`, `delete`, `head` | `status`, `headers`, `durationMs`, `sizeBytes`, and `json` when the body parses and is ≤ 1 MiB | the response body when `captureBodyAs` is set |
| `activity-file` | `copy`, `checksum`, `gzip`, `gunzip`, `tar`, `untar`, `concat` | `path`, `sizeBytes`, `sha256` | exactly one per op, none for `checksum` |
| `activity-json` | `query`, `merge` | `result`; with `as`, `path`, `sizeBytes`, `sha256` | the result, only when `as` is set |
| `activity-git` | `clone`, `resolve` | `commit`, `ref`; `clone` adds `path`, and for a tarball `sizeBytes`, `sha256` | `clone` with `as`: one source tarball; with `dest`, none — a working tree in the pin's `/orkstack/shared` |
| `activity-template` | `render` | `path`, `sizeBytes`, `sha256` | exactly one: the rendered file |

The work of `json`, `git` and `template` is jq, git and gomplate respectively,
behind the same `entry.sh` ABI: no OrkStack library on either side of the
line. Image-level semantics — `activity-http` exits 0 on a non-2xx response
(an HTTP status is data; a failure is failing to perform the request), and
`activity-shell`'s `eval` treats a non-zero child exit as data — are each
image's documented contract, not platform rules.

**There is no `activity-container` image, and there cannot be one.** §9.1
forbids every mechanism by which a container could reach a Docker daemon, and
a privileged exception for one first-party image is exactly what AGENTS.md §9
forbids. None is needed: **every task already is a container** — name the
image in `uses:` and give it a `command`:

```yaml
- id: migrate
  uses: ghcr.io/acme/migrate:v3
  command: ["/bin/migrate"]
```

**A templated image is deliberately unsupported.** `uses: "{{ .inputs.image }}"`
cannot be resolved at apply, because a run input does not exist until submit,
and AGENTS.md §5 forbids running an unresolved reference: it is a compile
error (`IMAGE_NOT_PINNED`, §4.2).

### 10.3 The contract test suite

```bash
orkctl activity test <image> [--timeout 2m] [--grace-period 10s] [--keep]
```

runs an image through the **same `OCIDispatcher` an Orker uses** — workspace
construction, output scan, readability check and commit protocol — against
local artifact and commit stores, with no Orkflow and no Temporal. The image
reference is used verbatim (not through the catalog), because the point is to
test an image you just built. Its probes are passed as activity-shell's
`with:` (`op: exec` / `eval`, `command` an argv array run through `sh` and
`jq` inside the image), so it applies as-is to images that follow that
configuration; any other image is exercised by the fixtures below. Six
assertions, each because a real defect would have passed without it:

1. **The container starts, writes `values.json`, and its stdout reaches the
   durable `stdout.log`** — a values-only check missed the `eval`
   substitution defect, which emptied `stdout.log` (§14.18).
2. **`/orkstack/input` is read-only**, asserted from inside the container.
3. **`task.json` is present and complete against the §3.2 table**, read by
   the workload with `jq`, and `image.pinned` matches the dispatched digest.
4. **Artifact names round-trip, and a directory in `output/artifacts/` is
   refused at the scan**, before any upload.
5. **Exit codes are not interpreted**: 1, 42, 125, 126 and 127 all fail, none
   specially, and 0 succeeds (§14.13).
6. **SIGTERM reaches the workload within the grace period, and the workload
   observes it** — both halves, because they fail independently (§14.14).

Fixtures in Bash, Go and Python under `test/fixtures/` run any image against
the ABI with only docker, `cat`, `jq` and a redirect;
`test/fixtures/first_party_contract.sh` covers `json`, `git` and `template`.
**The Bash fixture is mandatory and is the design's own falsification test**:
it runs the SIGTERM assertion on **every op** the image declares, because an
entry script can pass on `exec` and fail on `eval` (§14.18). If it
ever needs anything beyond `cat`, `jq` and a redirect, this design is wrong
(AGENTS.md §10).

Properties that need a broken workload or a broken host — a `umask 077`
output failing `OUTPUT_NOT_READABLE`, a maximal legal commit fitting
`commitMaxBytes` with the step-4 projection byte-identical to the stored
manifest, an `0xff` artifact name failing at the scan, a tampered embedded
layout failing `IMAGE_DIGEST_MISMATCH` before the runtime sees it, and no
labelled container surviving an Orker SIGKILL once its replacement boots — are
held by the Orker's unit and integration suites.

### 10.4 Running one task locally

```bash
orkctl task run --from-run RUN --task TASK [--file FILE.ork.yaml] [--input k=v]…
orkctl task run --file FILE.ork.yaml --task TASK [--output TASK=JSON]… [--artifact TASK/NAME=PATH]…
```

answers the question §10.3 does not: *why did this task, in this document,
with those inputs, fail?* It runs **one** task of an Orkflow on the caller's
machine and prints the rendered `config.json` before the container starts
(`--render-only` stops there).

**What it shares with an Orker.** The dispatcher is the Orker's, built by the
Orker's constructor, on this machine's runtime; the renderer is the Orker's,
so a `with:` that fails here fails `RENDER_FAILED` exactly as on an Orker; the
workspace, modes and §9.2 constants are the Orker's, with resources capped by
this machine's `ORK_ORKER_*` policy; and the `ExecuteTaskInput` is composed by
`engine.LocalTaskInput`, which a parity test holds equal to what `RunWorkflow`
schedules.

**Inputs.** With `--from-run`, every upstream value and artifact is the run's
committed bytes, read through the engine's namespace-checked,
integrity-verified artifact reads and re-verified when staged; a FAILED,
SKIPPED or never-run ancestor contributes nothing, as in a run. With `--file`,
the edited document is validated by the engine (compiled against the
deployment's catalog, images resolved to digests) and an ERROR refuses.
`--output`/`--outputs-file` and `--artifact` supply an upstream task from local
files. `--item N` runs one item of a fan-out. For a failure handler, `--need
TASK=succeeded|failed|skipped|canceled` sets how a direct need ended (without
`--from-run`, every direct need must be named), and the handler's `join` key is
written from it.

**What differs, deliberately.** The local input carries no `commit_uri` (a
local run has no durable result), no `pin` (a fresh one is minted for a pinned
task, whose `/orkstack/shared` starts empty unless `--shared DIR` seeds it),
no `telemetry`, no `image_policy` (a `--file` document already met the policy
at validation, and a run's own definition at submit), no `cache` (a cached
task always runs its container), and the contract-default limits rather than
the deployment's, which no client can read. `condition:` is not evaluated
(`orkctl run plan` answers that) and `retry:` is not applied: a local run is
one attempt. An awaited or composed task starts no container and is refused by
name, pointing at `orkctl run resolve` or at running the child run's task.

**Identity.** A local run is a **new logical activity**: run id `local-` and
16 hex digits, idempotency key `{namespace}/local-…/{task}`, fresh per
invocation. The source run's id and key never reach anything the workload can
read — reusing the production key would let an external system return the
production call's cached result, or record the local side effect under it. Any
external side effect the task performs is real, and the transcript says so
before the container starts.

**Secrets.** None is resolved implicitly. Every declared secret must be
supplied (`--secret NAME=file:PATH` or `NAME=env:VAR`) or explicitly skipped
(`--omit-secret NAME`); an unaccounted one refuses before any byte is fetched.
No value is printed, reported, or kept with `--keep`.

**What it never does.** It calls only the engine's read and validate RPCs and
never dials Temporal or object storage. Its dispatcher has no commit, attempt,
log or memo store, so the deployment gains no run, commit, attempt record,
log chunk, event, audit record or metric — the transcript's last line says
so.

Exit status: `0` succeeded (or `--render-only` rendered), `2` the task failed
(`ActivityFailed`, `ContractViolation`, `TimedOut`), `3` interrupted, `1` the
command could not run the task — including an `InfrastructureError`, which is
this machine's problem rather than the task's verdict.

---

## 14. Findings behind the rules

Each rule below was forced by an observed failure, not a preference. They are
kept so nobody "simplifies" one back; the tests named in the sections above
hold the tree to them.

- **14.1 Conditional create works on the pinned dev store**, but a sequential
  probe proves only the `412` mapping, not atomicity — hence §5.3's concurrent
  two-writer boot probe, and an object-store image pinned by digest in
  `deploy/docker-compose.yml` so the conditional-write boundary cannot move
  underneath it. The dev store is SeaweedFS, which documents the conditional
  create as atomic cluster-wide; an update is a PR that runs the proof.
- **14.2 The artifact store cannot hold a commit record.** A by-name `Stat` on
  the local backend fails (`malformed physical object key`) and a second `Put`
  of the same name with different bytes creates a second object instead of
  being refused — hence a separate, name-addressed `CommitStore` (§5.3).
- **14.3 The workspace and the §9.2 flag set compose.** A container running as
  an unknown UID read the read-only input, wrote `values.json` into a `1777`
  output, was refused a write to input, and showed `NoNewPrivs: 1`. A tmpfs at
  `/orkstack` failed instead (root-owned), which is why bind sources carry
  explicit modes.
- **14.4 Daemon introspection is not resolution.** `docker image inspect
  .RepoDigests` reports what the daemon negotiated for the platform it pulled
  — hence one registry-API resolver (§4.3).
- **14.5 The grace period is a real mechanism.** A container that traps
  SIGTERM exits at once with its handler run; one that ignores it is killed at
  the boundary with exit 137.
- **14.7 The publication primitive and the commit size.** `link(2)` to an
  existing path fails `EEXIST` and leaves it unchanged; `rename(2)` replaces
  it; `O_EXCL` at the final path after a torn write leaves the torn file
  permanently. A maximal legal commit (256 refs, 8-character names) is
  67,699 bytes — over a 64 KiB cap at 248 refs — hence 4 MiB and the pre-upload
  bound (§5.3, §5.4).
- **14.8 Output readability is host-dependent.** The same image, umask
  (`077`) and mode failed `Permission denied` on a native filesystem and read
  fine through a VM-shared bind mount that remapped ownership. `stat` succeeds
  where `open` fails, so `OUTPUT_NOT_READABLE` can always name mode and owner;
  `--user` of the Orker's own UID reads its own `0600` file (§3.1).
- **14.9 A `Struct` cannot carry byte-identical JSON.** A `Struct`/`protojson`
  round trip reorders keys, rewrites `1.00` as `1`, decodes `é`, and turns
  `12345678901234567890` into `12345678901234567000` — hence
  `values_json` is `bytes` (§2.2).
- **14.10 `protojson` output is deliberately unstable across builds** (it
  inserts random whitespace "to deter users from doing byte for byte
  comparison") — hence `CommitRef.sha256` is taken from stored bytes, never
  from re-marshalling (§2.2).
- **14.12 `docker load` accepts an OCI image layout**, and a layout's
  `index.json` names the index digest, so the embedded fallback needs no
  exotic tooling and its lookup is a direct key read (§4.6).
- **14.13 Exit codes 125/126/127 carry no runtime guarantee.** A started
  container's status passes through verbatim, including the numbers the CLI
  uses for its own diagnostics — hence no exit-code table and the
  `Create`/`Start`-versus-`Wait` split (§6.1, §8.2).
- **14.14 The naive entry script loses SIGTERM.** `sh -c "$cmd"; rc=$?; …`
  exits 143 with the workload's handler never run and no `values.json`;
  `exec sh -c` delivers the signal but writes no `values.json`; only
  background + trap + `wait` (with the second `wait` on the >128 path) does
  both (§10.1 rule 2a).
- **14.15 `docker load` verifies nothing about the root object.** A root index
  blob whose filename claimed one digest and whose bytes hashed to another
  loaded and ran — hence OrkStack hashes the root itself before import (§4.6).
- **14.16 Invalid UTF-8 in `ArtifactRef.name` is refused by every protobuf
  encoding**, binary included, inside `ArtifactRef`, `TaskRun` and
  `ActivityCommit` — hence the scan-time `ARTIFACT_NAME_NOT_UTF8` (§3.5).
- **14.17 An Orker crash leaves its container running.** After SIGKILL of the
  managing process the labelled container — and a `--rm` control — were still
  running; hence labels and the boot reaper (§5.5).
- **14.18 Command substitution loses cancellation and stdout.**
  `value=$(run_supervised …)` put the trap in a subshell (exit 143, no
  `values.json`, empty logs on cancel) and captured the workload's stdout away
  from `stdout.log`; redirection fixes both. The same harness showed
  `command: ["echo", "hello; touch /orkstack/output/artifacts/PWNED"]` printed
  as data with no file created — the argv form is a real mitigation (§10.1
  rules 2b and 8).

---

## 15. Non-goals

This contract does not add, and must not grow: a visual designer, a plugin
marketplace, an expression language, an embedded scripting runtime, a custom
scheduler or event bus or state machine, an OrkStack-owned execution database,
a Kubernetes operator, a general policy engine (the image policy is three
fixed rules, §4.7), named network profiles, a second container runtime
implementation, or a required workload SDK in any language. The activity ABI
is a directory of files and an exit code, and it stays that way.
