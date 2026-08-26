# Research round 2: what the code and external practice require to make `docs/OCI_ACTIVITY_CONTRACT.md` correct

Repository at `55b7367`, working tree dirty. All 25 review findings re-verified against current code; 12 were reproduced with executed probes this session, including one the previous round explicitly could not reproduce.

---

## Contract amendments received

None. No mid-run user message amended the objective or acceptance criteria during this stage.

---

## Contract conflicts requiring a user ruling — surfaced, not resolved

Five, not three. Each is a place where the design document silently picked a side. None may be settled by an agent.

| # | Conflict | Evidence | Note |
|---|---|---|---|
| **C1** | Acceptance criterion 7 groups timeout with *permanent* failures; AGENTS.md §4 says `TimedOut` → "retry according to policy". The design chose AGENTS.md (`docs/OCI_ACTIVITY_CONTRACT.md:1169`, `:1189`). | AGENTS.md §4 table; criterion 7 text | Review finding F22. Design's choice is defensible but unratified. |
| **C2** | Deliverable 10 names an "embedded" image fallback; §4.6 (`:767-771`) states "There is no 'embedded image'". | Deliverable 10 vs `:759-785` | F23. The design's rationale is technically sound (§4.6 below), but the literal deliverable is unmet. |
| **C3** | "Proto contract is forward-compatible (no breaking changes to existing code)" vs. retiring `Task.plugin` in a system the task says needs no compatibility shims. | `buf.yaml:13-15` sets `breaking: use: [FILE]`; `Task.plugin = 2` deleted | F21. **New evidence below materially changes its weight.** |
| **C4** *(new)* | AGENTS.md §2.2 requires "S3-compatible object storage"; the commit protocol requires atomic `If-None-Match: *` on `PutObject`, which several S3-compatible backends do not implement. | §5.3 `:870-878`; backend survey below | Nobody raised this. It narrows the deployment contract. |
| **C5** *(new)* | AGENTS.md §9 says first-party activities must use "**exactly** the same OCI contract" with no privileged internal path; the only mechanism that *guarantees* the Orker can read workload output is an Orker-owned helper container running as UID 0 with `CAP_FOWNER`, which §9.1 forbids. | Probe A′ below; §9.1 `:1246` | Emerges only once the readability gap is reproduced. |

---

## New evidence produced this session

Twelve executed probes. These are the load-bearing additions over the previous round.

### E1 — The workspace readability failure, reproduced on a native Linux daemon

The previous review round could not reproduce F07/F19 ("Docker Desktop on macOS remaps ownership"). A **native Linux daemon** (`docker version` → `linux/amd64 29.1.5`, `docker info` → `Ubuntu 24.04.2 LTS`, `SecurityOptions=[apparmor seccomp,profile=builtin cgroupns]`) was available this session. The failure reproduces exactly.

Setup mirrors §3.1: attempt root `0700` owned by the Orker's UID 1000; `output/` mode `1777`; the container sees **only** the bind target, so the `0700` parent is never traversed.

```
workload: -u 65534:65534 --read-only --cap-drop=ALL --security-opt no-new-privileges
          umask 077; printf '{"ok":true}\n' > /orkstack/output/values.json
        → -rw-------  1 65534 65534  12  values.json

Orker (uid 1000, owns the workspace, non-root as §9.2 prefers):
        → ls -ln  succeeds
        → cat     cat: can't open '/w/output/values.json': Permission denied     exit=1

control, same workload with umask 022:
        → -rw-r--r--  1 65534 65534  → cat succeeds, exit=0
```

`1777` guarantees **creation**, not **readability of what was created**. §3.1's "only readability is guaranteed" (`:482-484`) is **contradicted by evidence**. Lifecycle steps 3 (hash) and 4 (upload) cannot proceed, and §8.2 allocates **no reason code** for it — the nearest match, `ARTIFACT_UPLOAD_FAILED` (InfrastructureError, retryable, `:1200`), produces an infinite retry loop against deterministic image behavior.

Three candidate mechanisms, all tested:

| Mechanism | Result | Cost |
|---|---|---|
| **A′** Orker-owned relabel container, `-u 0:0 --cap-drop=ALL --cap-add=FOWNER`, `chmod -R a+rX`, mounting only `output/` | **Works** — file becomes `0644`, Orker reads it | One extra container start per task; needs UID 0 + `CAP_FOWNER`, which §9.1 forbids → **conflict C5**. Without `CAP_FOWNER` it fails: `chmod: /out: Operation not permitted` |
| **B** `--group-add <orker-gid>` + setgid dir (`3777`) | **Does not guarantee.** Group ownership *does* propagate (`-rw------- 1 65534 1000`) but `umask 077` strips group-read → still `Permission denied` | — |
| **C** Force `--user <orker-uid>:<orker-gid>` | **Works unconditionally** — the Orker owns the file, so `0600` is readable *by the owner* | Contradicts §3.1's "does not force `--user` by default" and §10.1's mandatory `USER 65532:65532`; breaks images needing their baked-in user |
| **D** Detect and refuse | **Always available** — `stat` succeeds even when `open` fails (`mode=644 owner=65534:65534`) | Requires a new `ContractViolation/OUTPUT_NOT_READABLE` code so the failure is terminal and actionable rather than a retry loop |

**Objective-aligned resolution:** D is the only mechanism that needs no §9.1 exemption and no forced `--user`; C is the only one that guarantees success without workload cooperation. A design may pick C-with-opt-out plus D as the backstop, but it must pick, and add the reason code either way.

### E2 — Atomic publish: `rename()` is the wrong primitive; `link()` is the right one

§5.3 (`:879-883`) specifies the local `CommitStore` as `os.OpenFile(path, O_CREATE|O_EXCL|O_WRONLY, 0o644)` at the final path, and justifies it by citing `pkg/artifacts/local.go:95-129`'s temp-file + `Rename` idiom. Both halves fail:

```
A. torn file visible at canonical path: "{\"schemaVersion\":"
A. second O_EXCL create -> file exists (EEXIST=true)  => torn file is PERMANENT

B. rename onto an EXISTING dest -> err=<nil>, dest now "{\"winner\":1}"  => rename REPLACES

C. link() onto an EXISTING dest -> EEXIST=true
C. link() onto a FRESH name    -> err=<nil>, content complete  => atomic + create-if-absent
```

- **A** reproduces F08/F12 and proves the torn file is *permanent*: `O_EXCL` guarantees nothing can ever replace it, and §5.4's decision table (`:891-902`) has no branch for a commit that exists but does not decode.
- **B** shows the cited in-house idiom is atomic **replacement**, not create-if-absent — so it cannot be adopted verbatim. `rename(2)`: *"If newpath already exists, it will be atomically replaced"* ([Linux man page](https://man7.org/linux/man-pages/man2/rename.2.html)).
- **C** identifies the correct primitive. POSIX.1-2024: *"The `link()` function shall **atomically** create a new hard link"*, `[EEXIST]` when the destination exists, and *"If `link()` fails, no link shall be created"* ([Open Group](https://pubs.opengroup.org/onlinepubs/9799919799.2024edition/functions/linkat.html)); Linux: *"If newpath exists, it will not be overwritten"* ([link(2)](https://man7.org/linux/man-pages/man2/link.2.html)).

**Objective-aligned resolution:** write to `.tmp/commit-<random>`, `fsync`, `close`, then `link()` to the canonical path and `unlink` the temp. This is precisely Git's loose-object publication ([`object-file.c`](https://github.com/git/git/blob/f78ce2f7b6df702f93d40b85d6bda92a3f65da79/object-file.c#L401-L419): `else if (link(tmpfile, filename))` … `if (ret && ret != EEXIST)`) and the portable `O_EXCL`-on-NFS lockfile algorithm the [`open(2)` man page](https://man7.org/linux/man-pages/man2/open.2.html) documents. Linux-only alternatives: `renameat2(RENAME_NOREPLACE)` (kernel 3.15+, per-filesystem support) and `O_TMPFILE` + `linkat(AT_EMPTY_PATH)`. Caveat to record: `link(2)` BUGS — *"On NFS filesystems, the return code may be wrong … Use `stat(2)` to find out if the link got created"*, so an `EEXIST` must be disambiguated by reading the object, which the adoption path already does.

### E3 — The commit-size cap, measured with the real encoder

Using the repo's actual generated `orkstack.v1.ArtifactRef` and the `protojson` encoder §2.2 specifies, with a realistic S3 URI:

```
one ref = 264 bytes
{"name":"artifact-0000", "uri":"s3://orkstack-artifacts/orkflows/deploy/ork-1786837143070480000-1/migrate/artifact-0000",
 "contentType":"application/octet-stream", "sizeBytes":"10485760", "sha256":"3b3b…"}

256 refs, name= 12 chars ->  66,319 bytes  (1.01x the 65,536 cap)
256 refs, name= 32 chars ->  76,559 bytes  (1.17x)
256 refs, name= 64 chars ->  92,943 bytes  (1.42x)
256 refs, name=128 chars -> 125,711 bytes  (1.92x)
cap crossed at n=252 refs with 13-char names
```

This is the **artifact list alone**. `ActivityCommit` adds 17 further fields including a full `ImageRef` (10 fields), so the true crossing point is below 252. F03/F13 **confirmed**: a workload respecting every documented limit (`artifact_max_count = 256`, `:183`) produces a commit that violates the design's own MUST (`:306-307`).

My numbers differ from the reviewer's 86,557 / crossing at 190 because URI and name length dominate; the direction and the conclusion are identical.

Two further facts collapse the fix:

- **The 64 KiB rationale does not hold.** §5.3 (`:876-878`) justifies the cap as *"The write MUST be single-shot; conditional multipart completion is not portable."* But `grep` finds **no** use of `feature/s3/manager`, `manager.NewUploader`, or `manager.Uploader` anywhere in the repo; every S3 write is a raw single-shot `client.PutObject` (`pkg/artifacts/s3.go:208`, `:374`, `logs_s3.go:54`, `archive_s3.go:70`, `:83`, `orkflow_store.go:645`, `orkflow_head.go:337`, `orkflow_current.go:212`, `namespace_store.go:307`). AWS single-`PutObject` accepts up to **5 GiB**. The constraint that actually matters is "never engage multipart," which the code already satisfies by construction. **64 KiB is ~80,000× stricter than its own stated reason requires.**
- **No reason code exists for exceeding it.** `grep "COMMIT_"` over the document returns only `COMMIT_READ_FAILED`, `COMMIT_WRITE_FAILED`, `COMMIT_SCHEMA_UNSUPPORTED`, `COMMIT_IDENTITY_MISMATCH`. An oversize commit falls to `COMMIT_WRITE_FAILED` — InfrastructureError, retryable (`:1209`) — which is the non-terminal loop AGENTS.md §11 forbids.

**Objective-aligned resolution:** raise the cap to a value derived from the real constraint (a few MiB is ample and still single-shot), *or* keep a small cap and add a pre-execution bound plus a `ContractViolation/COMMIT_TOO_LARGE` code so the failure is terminal. The "lower `artifact_max_count` to ~150" option is the worst of the three: it caps a workload property to protect a manifest property.

### E4 — `Payload`'s `Struct` cannot carry byte-identical JSON, and silently corrupts integers

§2.2 (`:287-291`) types `ExecuteTaskResult.values` as `Payload` and requires *"a byte-identical inline copy of values.json"*. Measured round trip through `structpb.Struct` + `protojson`:

```
IN  {"n":1.00,"z":0}              OUT {"n":1,"z":0}                    byte-identical=false
IN  {"b":  true ,"a":1}           OUT {"a":1,"b":true}                 byte-identical=false   ← keys REORDERED
IN  {"big":12345678901234567890}  OUT {"big":12345678901234567000}     byte-identical=false   ← PRECISION LOST
IN  {"s":"caf\u00e9"}             OUT {"s":"café"}                     byte-identical=false
```

F16 **confirmed and materially worse than reported**. Beyond number formatting: keys are **reordered**, unicode escapes are normalized, and an int64-range integer is **silently corrupted** (`…7890` → `…7000`) because `Struct` values are `double`. §3.4 (`:611-613`) promises "key order, spacing and number formatting are preserved, and the committed `sha256` is the sha256 of exactly what the workload wrote" — true for the uploaded object, false for the inline `Payload` copy, and the two are meant to be the same data.

**Objective-aligned resolution:** type the inline copy as `bytes` (or `string`) for verbatim data and keep the `sha256` as the only identity, or delete the byte-identity promise and define the normalized semantic form, explicitly documenting the int64 lossiness so the UI never displays a corrupted number.

### E5 — `protojson` output is deliberately byte-unstable across builds

Measured: 200 marshals of one identical message in one build produced **1 distinct encoding**, and six separate `go run` invocations produced the same bytes. So the output is stable *within a build*. Upstream is explicit that this does not generalise:

> "Do not depend on the output being stable. Its output will change across different builds of your program, even when using the same version of the protobuf module." — [`protojson.Marshal` docs](https://pkg.go.dev/google.golang.org/protobuf/encoding/protojson#Marshal)

The mechanism is deliberate: commit [`582ab3d`](https://github.com/protocolbuffers/protobuf-go/commit/582ab3de426ef0758666e018b422dd20390f7f26), *"add random whitespaces in encoding output … meant to deter users from doing byte for byte comparison"*, still live at [`encode.go:234-243`](https://github.com/protocolbuffers/protobuf-go/blob/f4a54025970a405fb35cada3b865101d8fa8ab9d/internal/encoding/json/encode.go#L234-L243) (`if detrand.Bool() { e.out = append(e.out, ' ') }`). Repo is on `google.golang.org/protobuf v1.36.12`.

Consequence for the design: `CommitRef.sha256` = *"hex sha256 of the exact committed JSON bytes"* (`:264`) is sound **only** if it is always computed from the bytes actually written/read, never by re-marshalling the decoded `ActivityCommit`. That must be stated, or a rebuilt Orker will compute a different digest for an unchanged commit.

### E6 — `proto.MarshalOptions{Deterministic:true}` is explicitly disqualified for §4.4's use

§4.4 (`:739-741`) defines:

> `orkflow_definition_digest = sha256(proto.MarshalOptions{Deterministic:true}.Marshal(orkflow))`

The upstream doc comment for that exact option, verbatim from [`proto/encode.go:34-55`](https://github.com/protocolbuffers/protobuf-go/blob/f4a54025970a405fb35cada3b865101d8fa8ab9d/proto/encode.go#L34-L55):

> "Note that the deterministic serialization is NOT canonical across languages. **It is not guaranteed to remain stable over time.** It is unstable across different builds with schema changes due to unknown fields. **Users who need canonical serialization (e.g., persistent storage in a canonical form, fingerprinting, etc.) must define their own canonicalization specification and implement their own serializer rather than relying on this API.**"

A definition digest stamped into an immutable version document and compared across engine releases is exactly "persistent storage in a canonical form / fingerprinting". **Nobody raised this.** It is a stronger objection than F14's, which only says the field set is wrong.

### E7 — `sameStoredOrkflow` and `sourceHash` make §4.4's three promised outcomes unreachable

The relevant code, read in full:

```go
// pkg/artifacts/orkflow_store.go:400-408
func sameStoredOrkflow(stored, incoming *orkstackv1.Orkflow) bool {
	if stored.GetSourceYaml() != incoming.GetSourceYaml() { return false }
	a := proto.Clone(stored).(*orkstackv1.Orkflow)
	b := proto.Clone(incoming).(*orkstackv1.Orkflow)
	a.CreatedAt, b.CreatedAt = nil, nil
	return proto.Equal(a, b)
}
```

```go
// internal/dsl/compile.go:190-192
if wf.GetVersion() == "" { wf.Version = sourceHash(source) }
```

Against §4.4's three claimed consequences (`:745-749`):

| §4.4 claim | What today's code does | Verdict |
|---|---|---|
| "Same YAML, same tag digest → byte-identical definition, apply is a no-op" | `ImageRef.resolved_at = 8` (`:141-142`) is stamped fresh per apply, lives inside `Task.image` inside the hashed/stored `Orkflow`, and `sameStoredOrkflow` clears **only** `CreatedAt` → `proto.Equal` false → refused as `ErrVersionExists` | **Contradicted** (F02/F14) |
| "Same YAML, moved tag, no explicit `version:` → a **new** version" | `sourceHash` covers only source bytes and runs in `Compile`, **before** resolution (`compile.go:190`) → the derived version string is **identical**; the definitions differ → `orkflow_store.go:676-683` refuses with `ErrVersionExists`. The author gets a refusal, not a new version. | **Contradicted — stronger than F14 states** |
| "Same YAML, moved tag, explicit `version:` → `ALREADY_EXISTS` conflict" | Holds | Confirmed |

And a committed design decision directly opposes §4.4's direction. `internal/dsl/hash.go:8-27` records that semantic hashing was **tried and reverted**:

> "This hash used to normalize the YAML and hash only its semantic content, so a comment-only edit of an unpinned document compiled to the stored version with different source and could not be applied at all — and the refusal's advice, 'publish a new version', was unfollowable, because a derived version is not the author's to name. **Two notions of 'the same document' in one system is one too many.**"

`orkflow_store.go:395-399` states the same coupling from the store side. §4.4 re-introduces the second notion without addressing why the first was removed.

**Objective-aligned resolution requires three coordinated decisions, none currently specified:** (i) which fields are cleared for the canonical hash — at minimum `source_yaml`, `created_at`, and every `Task.image.resolved_at`; (ii) whether `sameStoredOrkflow` clears the same set, since it compares `SourceYaml` first and would otherwise still refuse; (iii) **at which step a derived version is assigned** — it must move from `Compile` (pre-resolution) to `ApplyOrkflow` (post-resolution) for a moved tag to mint a new version, and that relocation contradicts `hash.go`'s stated "the version covers the source bytes" contract. Plus (iv) a canonicalization spec that is not `Deterministic:true`, per E6.

### E8 — `buf breaking` is configured but not enforced anywhere

```
buf.yaml:13-15   breaking: { use: [FILE] }
Makefile:13-16   proto:  buf lint ; buf generate        ← no `buf breaking`
.github/workflows/ci.yml   go build ./... ; go vet ./... ; go test ./... -count=1
```

The check F21 cites exists in configuration and is run by **no** automation. This does not dissolve conflict C3 — the objective's literal wording is still contradictory and only the user may rule — but it removes the "a configured gate will block this" framing. The design's §14.6 control (the current tree fails the identical `FIELD_NO_DELETE` rule against a `queue`-era baseline) stands.

### E9 — Conditional `PutObject` is not a property of "S3-compatible"

AGENTS.md §2.2 specifies "S3-compatible object storage". The commit protocol requires atomic `If-None-Match: *`. Survey (links in `research/web/2026-08-26-atomic-publish-protojson-s3compat-uid.md`):

| Backend | `If-None-Match: *` on PutObject | Evidence |
|---|---|---|
| AWS S3 | Yes | [conditional-writes docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) |
| MinIO | Yes, since `RELEASE.2023-02-17T17-52-43Z` ([PR #16551](https://github.com/minio/minio/pull/16551)) | Local stack runs `RELEASE.2025-09-07T16-13-09Z` per §14.1 |
| Cloudflare R2 | Yes | [R2 S3 extensions](https://developers.cloudflare.com/r2/api/s3/extensions/#conditional-operations-in-putobject) |
| Ceph RGW | Yes for unversioned simple PUT; versioned/multipart broken before ~v20 | [tracker #68183](https://tracker.ceph.com/issues/68183) |
| SeaweedFS | Yes, from 3.97 only | [PR #7154](https://github.com/seaweedfs/seaweedfs/pull/7154) |
| LocalStack | Yes (post 2024-11-27) | [commit b28bad3](https://github.com/localstack/localstack/commit/b28bad3bfd2de9cd320b3da55cd168b3b7c94988) |
| **Google Cloud Storage XML** | **No** — needs `x-goog-if-generation-match: 0` | [header reference](https://cloud.google.com/storage/docs/xml-api/reference-headers#ifnonematch) limits `If-None-Match` to HEAD/GET |
| **Backblaze B2 S3** | **No** — returns HTTP 501 | [Proxmox patch](https://lore.proxmox.com/all/20250728100154.556438-3-c.ebner@proxmox.com/), [ZeroFS #95](https://github.com/Barre/ZeroFS/issues/95) |
| **Garage** | **No, by design** — no consensus layer | [known issues](https://garagehq.deuxfleurs.fr/documentation/reference-manual/known-issues/) |

**Objective-aligned resolution:** the design must either name conditional `PutObject` (with 412/409 semantics, non-multipart, minimum tested versions) as an explicit backend requirement in AGENTS.md §2.2 terms, or add a per-backend commit strategy. A boot-time capability probe must **race two writers**, not do a sequential HEAD-then-PUT, since a sequential check proves nothing about atomicity.

### E10 — The workspace bindability failure reproduced live

Attempting a bind mount of a local path against the available (remote) daemon produced:

```
docker: Error response from daemon: invalid mount config for type "bind":
        bind source path does not exist: /tmp/uidprobe/output
```

This is the exact failure `pkg/plugins/container/container.go:377-387` special-cases and `examples/container-example.ork.yaml:5-14` warns about. §3.1's boot-time probe requirement (`:490-492`, `InfrastructureError/WORKDIR_NOT_BINDABLE`) is well-founded, and §8.2's `WORKSPACE_NOT_BINDABLE` as **non-retryable InfrastructureError** (`:1191`) is the right call — confirmed, not a finding.

---

## Disposition of all 25 review findings

Reviewer-a and reviewer-b duplicate heavily; 25 findings are 16 distinct issues.

| Findings | Issue | Verdict | Resolution the objective supports |
|---|---|---|---|
| F01, F11 | S3 output key is name-addressed + unconditional PUT; a losing attempt overwrites bytes a winning commit hashed | **Confirmed by code.** `pkg/artifacts/s3.go:196` → `s3ObjectKey(ProvenanceFromMetadata(...), name)`; `s3_manifest.go:34-49` → `s3ObjectKeySegments = 5` (namespace/orkflow/run/task/artifact — **no attempt**); `s3.go:208-215` `PutObject` with no `IfNoneMatch`. Local is content-addressed (`local.go:121`, `artifacts.go:85-94`) and immune. | Make output objects immutable *before* the commit write: either content-address the S3 key (append the content sha256 as a 6th segment, matching what local already does) or add an attempt segment. Content-addressing is preferable: it also makes step-4 uploads idempotent across retries. |
| F02, F14 | `resolved_at` in the canonical hash; no post-resolution version derivation | **Confirmed, and broader than reported** — see E7. Plus E6 disqualifies the chosen algorithm. | Four coordinated decisions in E7. |
| F03, F13 | 64 KiB commit cap unreachable at 256 artifacts; `COMMIT_WRITE_FAILED` retryable → loop | **Confirmed by measurement** — E3. | Raise the cap to the real constraint (single-shot PUT ≤ 5 GiB; repo never uses the multipart manager), and add `ContractViolation/COMMIT_TOO_LARGE` as the terminal backstop. |
| F04, F17 | `activity-container` is unbuildable under §9.1 | **Confirmed.** `pkg/plugins/container/docker.go:54-75` only resolves a CLI on `PATH`; the daemon connection is entirely ambient (`DOCKER_HOST`/default socket). §9.1 forbids the socket "as a source OR as a target" (`:1253`), any credential mount (`:1254`), `--privileged`, `--device`, host namespaces. No compliant mechanism exists. | The honest outcome is **retirement**: under the OCI contract every activity *is* a container, so `activity-container` is `uses: <image>` on an ordinary task. That is a contract change against user decision 4, which names it — escalate alongside C2 rather than deciding. |
| F05, F15 | `values_ref` on `TaskRun`, definition digest on `Run`, `TaskRun.plugin` orphaned | **Confirmed.** `run.proto` `TaskRun` = fields 1-10 (`task_id, plugin, status, attempt, output, artifacts, error, started_at, finished_at, pool`), no `values_ref`; `Run` = fields 1-12, no digest field. §2.3 (`:352-422`) covers only `common.proto`, `orkflow.proto`, `service.proto`. | Phase 1 must add: `TaskRun.values_ref = 11`, `TaskRun.commit = 12` (or a `CommitRef`), `TaskRun.exit_code = 13`, reserve `TaskRun.plugin = 2` alongside `Task.plugin`, and `Run.orkflow_definition_digest = 13`. Otherwise Phases 2/3 renegotiate the wire, defeating §11.1. |
| F06, F25 | "image digest differs" ambiguous between index and platform digest | **Confirmed by reading.** §2.2 `:143-147`: "The engine leaves both empty; only an Orker fills them". §5.4 `:897` says "image digest" unqualified. At step 0 the input's `platform_digest` is empty and a stored commit's is set → reading it as the platform digest rejects **every** adoption, and on a mixed-arch pool rejects two well-formed attempts. | State identity as `image.index_digest` explicitly, and enumerate the complete check set: `schema_version == 1`, `namespace`, `run_id`, `task_id`, `orkflow_definition_digest`, `task_idempotency_key`, `image.index_digest`. `platform_digest`/`platform` are **audit data from the winning attempt** and are never compared. Add the missing branch: existing-but-undecodable. |
| F07, F19 | Readability of workload-written outputs is asserted, not guaranteed | **Confirmed and reproduced** — E1. | E1's mechanism table; requires a user-level choice between C (forced `--user`) and D (`ContractViolation/OUTPUT_NOT_READABLE`), plus C5 if A′ is chosen. |
| F08, F12 | Torn `commit.json` at the canonical path; no undecodable branch | **Confirmed and reproduced** — E2, incl. proof the torn file is permanent. | `link(2)` publish from a synced temp file; add the undecodable branch to §5.4's table (it must be **retryable-once-then-terminal**, not silently one of the two opposite existing codes). |
| F09 | `commit_uri` and `WorkspaceLimits` built inside deterministic workflow code | **Confirmed.** §7.2 `:1121-1127` places `commit_uri = CommitKey(...)` inside "ENGINE (deterministic workflow)"; a URI needs `ORK_ARTIFACT_BACKEND`/`ORK_S3_ENDPOINT`. `internal/engine/defaults.go:12-18` is annotated "safe in workflow code: it has no side effects" — the repo marks such helpers explicitly, and this is not one. `internal/engine/plan.go:18-30` states the established pattern: the decision "is made once, by the process that accepts the submission, and travels as an argument". `router.go` is namespace→cluster and is not the seam. | Extend `RunPlan` (already workflow argument 4, already replay-pinned) with the commit-store root and the effective `WorkspaceLimits`, decided at submit in `internal/api`. The workflow then composes `commit_uri` from plan data + run/task coordinates, which is pure. This reuses a proven mechanism rather than adding one. |
| F10, F24 | Deliverable is untracked | **Confirmed, still true.** `git log --oneline -1` → `55b7367`; `git status --short` → `M README.md`, `?? docs/OCI_ACTIVITY_CONTRACT.md`, `?? research/…` ×6. | `git add` the document, the six research files, and the README edit; one Conventional Commit per AGENTS.md §11.1 step 6. Committing only tracked changes leaves the README link broken. |
| F16 | Byte identity through `Payload` | **Confirmed, worse than reported** — E4. | `bytes`/`string` for verbatim data, or drop the promise and document the int64 lossiness. |
| F18 | `task.json` is an example, not a schema | **Confirmed.** §3.2 `:508-576` is a single JSONC example. `resources`, `network` and `logMaxBytes` appear nowhere in it, although `WorkspaceLimits.log_max_bytes = 4` (`:185`) and `ExecuteTaskInput.resources`/`.network` (`:252-253`) exist and §3.2 calls the file "the complete activity context" (`:502`). | A field-by-field table: name, JSON type, required/optional, empty form, and the two policy rules the document never states — unknown-field handling (workloads MUST ignore; the Orker MUST NOT emit unknown keys) and `schemaVersion` handling (a workload seeing an unknown major MUST fail; the platform MUST NOT remove or retype a field within a major). |
| F20 | Commit key validates run-ID shape | **Confirmed with two independent proofs.** AGENTS.md:330 — "Nothing may parse a run ID or derive meaning from its parts." §5.2 `:822-824` requires "the run id matches the documented `ork-{timestamp}-{sequence}` shape". `internal/api/service_test.go` injects `"run-001"`, `"run-stream"`, `"run-resume"`, `"ghost"`, `"run-live"`, `"run-terminal-cursor"` — none match. `internal/engine/router.go:99-104` documents run-ID opacity as a design principle: `Locate` works "without inspecting the run id". | Encode the opaque ID as **one** safe key component. `pkg/artifacts/archive.go:283-317` already does exactly this for `metadata/runs/{encoded-run-id}.jsonl` — reuse it rather than inventing a rule, and validate only namespace and task id (which *are* slug-validated at `internal/dsl/validate.go:22-55`). |
| F21 | Self-resolved proto conflict | **Confirmed as a conflict; re-weighted by E8.** | Escalate as C3. |
| F22 | Timeout retryability | **Confirmed as a conflict.** | Escalate as C1. |
| F23 | Embedded fallback | **Confirmed as a conflict.** §4.6's technical rationale is sound and verified — `--iidfile`/`--quiet` yield a local config ID, not a manifest digest ([buildx build](https://docs.docker.com/reference/cli/docker/buildx/build/); [Moby API v1.52 lines 1870-1884](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/api/docs/v1.52.yaml#L1870-L1884)) — but the deliverable is literal. | Escalate as C2. |

---

## Answers to the nine verification items

**1. Output-key immutability and the conditional primitive.** Yes, a losing attempt can overwrite committed bytes on S3 — the key has no attempt or content component (`s3_manifest.go:34-49`) and the PUT is unconditional (`s3.go:208-215`). Local is immune by content-addressing. Conditional primitives actually available: S3 `If-None-Match: *` (see E9 for the compatibility caveat) and POSIX `link(2)` locally (E2). The repo's `.tmp/put-*` + `Rename` idiom (`local.go:95-129`) gives *torn-free* publication but **not** create-if-absent — E2 probe B. For `commit.json`, the two requirements compose only as temp-file + `fsync` + `link()`.

**2. Commit manifest size.** Measured over cap at the design's own default (E3). The bound that matters is the single-PUT ceiling, not 64 KiB, because the repo never uses the multipart manager. `COMMIT_WRITE_FAILED` being retryable (`:1209`) turns any oversize commit into a non-terminal loop, and no `COMMIT_TOO_LARGE` code exists.

**3. Definition digest and version derivation.** E6 + E7. `resolved_at` must be cleared from both the hash and `sameStoredOrkflow`; the derived version must move from `Compile` to post-resolution for §4.4's moved-tag promise to hold; `Deterministic:true` is disqualified by its own doc for fingerprinting; and `hash.go:8-27` records that the semantic-hash direction was already tried and reverted.

**4. Determinism boundary.** No — `commit_uri` and `WorkspaceLimits` cannot be built in workflow code. The engine must send the resolved values; `RunPlan` is the proven vehicle (F09 row). The Orker derives everything filesystem- and runtime-shaped: workspace paths, `task.json`, `config.json`, the platform digest, stream capture.

**5. Proto completeness and compatibility.** Missing set listed in the F05/F15 row. `Payload`'s `Struct` cannot carry byte-identical JSON (E4). `buf breaking` is configured but unenforced (E8); the objective conflict remains C3.

**6. Run-ID opacity.** Build the key from the opaque ID as one encoded component using `pkg/artifacts/archive.go:283-317`, the mechanism `metadata/runs/` already uses. Validate namespace and task id only.

**7. Retry adoption identity.** Compare `image.index_digest`; treat `platform_digest`/`platform` as audit data. Full check list and the missing undecodable branch in the F06/F25 row.

**8. Workspace ABI precision.** No mount-permission mechanism guarantees readability against an arbitrary umask (E1, probe B disproves the group route). Choose forced `--user` (guarantees, costs image compatibility), an Orker-owned relabel container (guarantees, needs a §9.1 exemption → C5), or detection with a new `ContractViolation` (always works, terminal, actionable). The `task.json` schema gaps are in the F18 row. What the new layout must preserve, confirmed against the code: the log sink opened first and closed **before** return (`internal/orker/activity.go:115-123`) so every chunk is durable before Temporal's terminal event; the `[stderr] `/`[stdout] ` framing the UI decodes (`pkg/plugin/linewriter.go:20-49`, proof M11.4); the `(RunID, TaskID, Attempt)` sink key and dense 1-based `Seq` (`internal/orker/logsink.go:445-521`); the replay/follow contract and terminator (`internal/api/logs.go:43-370`); and the failure-path rule that a workload's error is never replaced by a storage error (`activity.go:155-171`, proof M11.3). §3.6 and §7.1 step 14 already preserve all of these correctly.

**9. `activity-container` and the other three builtins.** No compliant mechanism exists (F04/F17 row). `http`, `shell` and `file` become Dockerfile + entry-script images exactly as §10.1–10.2 specify; that section is sound and the `eval`/non-2xx semantics are correctly demoted to per-image contracts rather than platform rules. One correction: §10.1 mandates `USER 65532:65532`, which under E1 means a first-party image with any non-default umask produces unreadable outputs — the first-party entry scripts must set `umask 022` explicitly, and the contract test suite (§10.3) should assert output modes.

---

## Phase implications

Phase 1's stated purpose — "The complete proto lands in Phase 1 so Phases 2 and 3 never renegotiate the wire" (`:1443-1445`) — is not currently achievable. Blocking Phase 1 items:

- `run.proto` changes (F05/F15) — otherwise Phase 2's commit and Phase 3's read model invent fields.
- The inline-values representation (E4) — changing `Payload` → `bytes` later is a wire break after the freeze.
- `RunPlan` carrying the commit root and limits (F09) — a workflow-argument shape change forces a *second* replay-fixture reset, which §11.4 correctly calls "a process failure, not an accident".
- The commit-identity field list and the `COMMIT_TOO_LARGE` / `OUTPUT_NOT_READABLE` reason codes — reason codes are `Error.code` strings, so late additions are cheap, but the *categories* drive `NonRetryableErrorTypes` in `ActivityOptions`, which is replay-visible.

Phases 2 and 3 are otherwise genuinely disjoint as scoped.

---

## Validation recommendations

1. **Race two writers at boot, per backend.** A sequential HEAD-then-PUT proves nothing about atomicity (E9). Model it on the existing `TestNamespaceStore_ConditionalWritesArbitrateBetweenTwoWriters`, which §14.1 already runs green against the pinned MinIO.
2. **Pin the MinIO tag.** `deploy/docker-compose.yml:30` is `minio/minio:latest`; the conditional-multipart boundary is `RELEASE.2025-09-07T16-13-09Z` and the conditional-PUT boundary is `RELEASE.2023-02-17T17-52-43Z`. A floating tag can silently move below either.
3. **Add the umask fixture to `orkstack activity test`.** A Bash image writing `values.json` under `umask 077` is a two-line falsification of §3.1 and is exactly the class of test §10.3 exists for.
4. **Assert the commit-manifest bound in a unit test at `artifact_max_count`.** Construct a maximal legal `ActivityCommit` and assert it serializes under the cap; the test fails today at 256 refs (E3).
5. **Test `link()` publication under a kill.** Write the temp, `SIGKILL` before `link()`, restart, assert the canonical path is absent and the retry succeeds — the property `O_EXCL` at the final path cannot provide (E2).
6. **Reset the replay fixture exactly once**, after every Phase 1 wire item above has landed, per `:1498-1500`.
7. **Prove cancellation end to end.** §14.7 correctly lists it as unverified. Submit a 120 s container, `CancelRun`, assert SIGTERM within the grace and exit 137 after it. §6.3's three-part requirement is correctly specified; only the proof is missing.
8. **Keep proofs M7.7, M11.1, M11.3, M11.4, M11.7 as regression gates** on the dispatcher rewrite — they cover container lifecycle, live stream separation, the failure-path publication rule, browser rendering, and cross-replica follow.

---

## Unverified / explicitly not established

- **The S3 overwrite race (F01/F11) was confirmed by reading code, not executed.** MinIO was not reachable this session (`curl http://localhost:9000/minio/health/live` → `000`); the available Docker daemon is remote, so port-local infra was unavailable. The key construction (`s3_manifest.go:34-49`) and the unconditional `PutObject` (`s3.go:208-215`) were read verbatim.
- **E1's probes used Docker named volumes with `volume-subpath`, not host bind mounts**, because the remote daemon cannot see this host's filesystem (E10). The kernel permission semantics under test — directory mode, sticky bit, file mode, UID ownership, setgid propagation, `CAP_FOWNER` — are identical for both mount types; the substitution does not affect the result. It does mean the *bind-specific* VirtioFS/gRPC-FUSE ownership question is untested here.
- **E5's cross-build instability was not reproduced**; six `go run` invocations gave identical bytes, which is consistent with `detrand` seeding per binary and Go's build cache reusing one binary. The instability claim rests on the upstream doc text and commit `582ab3d`, both quoted.
- `proto.MarshalOptions{Deterministic:true}` instability across schema changes (E6) was not executed; the doc comment is quoted verbatim from pinned upstream source.
- Backblaze B2's lack of conditional PUT rests on two independent third-party integration reports, not a Backblaze statement.
- No claim here about whether the design's §14 evidence blocks reproduce; they were read, not re-run.
- I did not modify any tracked file, run `make`, or commit anything. The only files written this session are `research/docs/2026-08-26-oci-contract-review-round-2.md`, `research/web/2026-08-26-atomic-publish-protojson-s3compat-uid.md`, and probe scratch under `/tmp`.