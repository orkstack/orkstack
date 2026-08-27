# Research round 4 — closing the 20 blocking findings against `docs/OCI_ACTIVITY_CONTRACT.md`

Repository at `10e016f` (`docs(oci-contract): close 27 review findings, four of them by measurement`), tree **clean**, document **2,952 lines**. All 20 findings re-verified. **Six reproduced with executed probes this session**, and for the highest-severity one I also built and verified a working fix.

---

## Contract amendments received

None. No mid-run user message amended the objective or acceptance criteria during this stage.

---

## Headline conclusions

1. **The specified `activity-shell` entry script fails its own mandatory contract on `op=eval`, and the naive fix fails too.** Built §10.1/§10.2 verbatim and ran under `--init` with `docker stop --timeout 5`: `op=exec` passes (exit 0, TERM trap fired, `values.json` written); `op=eval` fails **twice in a row** at **exit 143** with **no `values.json` and completely empty container logs**. Cause: `value=$(run_supervised "$cmd")` puts the trap in a command-substitution subshell. My first candidate fix used a pipeline (`| tee`) and **also failed at 143** — POSIX makes every command of a multi-command pipeline a subshell too. Only plain redirection works, and I verified it end to end.
2. **A second, independent defect on the `eval` success path**: the command substitution swallows the workload's stdout entirely. A *successful* `eval` produced container logs containing only `DIAG` (stderr) — `HELLO-ON-STDOUT` never reached the log stream at all. §3.6 declares byte-exact independent stdout capture a **regression gate** (proof M11.1/M11.4), so every `eval` task would ship an empty durable `stdout.log`.
3. **An Orker crash leaves the container running.** SIGKILLed the managing process; the labelled container was still `running=true`, `Up 6 seconds` three seconds later. `--rm` does **not** help — it is daemon-side but fires only *on container exit*, and the container never exits. §5.5 row 1 ("nothing on that host") is contradicted by evidence, and Temporal will retry elsewhere while the abandoned container keeps executing.
4. **`docker load` verifies nothing about the root object.** Altered a root blob's bytes while keeping its digest-shaped filename (`sha256:d9e853e8…` claimed, `sha256:85d9ccd2…` actual). `docker load` returned **exit 0**, `Loaded image: alpine:3.20`, and the image **ran successfully**. §4.6's fallback treats the filename as verification, so it would import an arbitrary image under a pinned digest, silently.
5. **Invalid-UTF-8 artifact names are rejected by *every* protobuf encoding, not just protojson.** `protojson.Marshal` *and* binary `proto.Marshal` both error on `ArtifactRef.name`. §3.2 legalizes such names (`nameB64` "always present and authoritative"), so a legal filesystem output can be staged and stored but can never enter `ActivityCommit`, `ExecuteTaskResult`, or `TaskRun`.
6. **`RunPlan` has exactly four fields and no transfer-budget carrier**, while §6.3's `StartToCloseTimeout` formula depends on two `ORK_ORKER_`-prefixed variables that the *engine* must read at submit — one of which (`ORK_ORKER_FLOOR_THROUGHPUT_BYTES_PER_SEC`) has no stated default at all.
7. **The pre-upload commit bound is not constructible from the frozen `Store` interface.** `Put/Get/Stat/List/Presign/Delete` has no method that predicts a ref from name+digest, and both key functions (`objectKey`, `s3ObjectKey`) are unexported.

Round-3 fixes landed and hold: `idempotency_key = 8` (criterion-1 name), `ImageRef.delivery = 11`, `run_as_orker_user`, `ContainerRuntime.LoadLayout`, `service.proto` fully specified, `TaskRun` reserving `plugin` plus four new fields, `Run.orkflow_definition_digest`, §7.1 step 14 now scans on the failure path, §6.3's derived transfer budget replacing the fixed 120 s, and §17 grown to seven conflicts with C6/C7 added.

---

## Escalations for a user ruling — surfaced, not resolved

Both are already recorded in §17 as C6 and C3. Recording is not ruling; the criteria remain literally unmet until the user chooses.

**E-A — the `container` builtin (F08, §17 C6).** User decision 4: *"Builtin images (http, shell, file, container) can be built locally OR pulled from remote registry."* §10.2:2181-2190 deletes it. The technical argument is verified: `pkg/plugins/container/docker.go:54-75` reaches a daemon only through an ambient CLI on `PATH` plus `DOCKER_HOST`/the default socket, and §9.1 forbids the socket as source *or* target, credential mounts, `--privileged`, `--device` and host namespaces. §17 C6's cost-to-flip is a §9.1 exemption for a socket-mounting image, "accepting that any workload using it can control the host's daemon". Only the user may pick.

**E-B — proto compatibility (F09, §17 C3).** The objective requires both a clean non-compatible cut with no shims *and* "no breaking changes to existing code". §14.6 now reports the measurement accurately: the repo's configured `breaking: use: [FILE]` (`buf.yaml:13-15`) yields **six deletions** — `Task.plugin`, `TaskRun.plugin`, `ListPluginsRequest`, `ListPluginsResponse`, `PluginInfo`, `PluginService` — while `WIRE` and `WIRE_JSON` pass clean. That is a genuine source/API break. Note as context, not as resolution: `buf breaking` is run by **no automation** — `.github/workflows/ci.yml` runs only `go build`, `go vet`, `go test`, and `Makefile`'s `proto:` target runs only `buf lint` and `buf generate`.

---

## Measurements

### M1 — The entry script, built verbatim and run under `--init`

Built §10.1's Dockerfile and §10.2's `entry.sh` exactly as written (`alpine:3.20` + `jq`, `USER 65532:65532`, exec-form `ENTRYPOINT`), workspace mounted per §3.1/§9.2, workload traps TERM and exits 0.

| Case | stop took | exit | `values.json` | container logs |
|---|---:|---:|---|---|
| `op=exec` + cancellation | 197 ms | **0** | `{"exitCode":0}` | `WORKLOAD-UP\|WORKLOAD-GOT-SIGTERM` |
| `op=eval` + cancellation (run 1) | 168 ms | **143** | **absent** | *(empty)* |
| `op=eval` + cancellation (run 2) | 141 ms | **143** | **absent** | *(empty)* |

Cause, confirmed against POSIX.1-2024 [§2.6.3](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_06_03): *"The shell shall expand the command substitution by executing commands in a subshell environment"*, and [§2.13](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_13): *"Changes made to the subshell environment shall not affect the shell environment"* and *"A subshell environment shall be created as a duplicate of the shell environment, except that … traps that are not being ignored shall be set to the default action."* So `run_supervised`'s `trap … TERM` lands in the substitution subshell; `entry.sh` — tini's only direct child — keeps the default TERM disposition and dies at once.

**On the success path the same construct swallows stdout:**

| Case | exit | container logs | `values.json` |
|---|---:|---|---|
| `op=eval`, success | 0 | `DIAG` **only** | `{"value":"HELLO-ON-STDOUT","exitCode":0}` |
| `op=exec`, success | 0 | `DIAG` \| `HELLO-ON-STDOUT` | `{"exitCode":0}` |

`HELLO-ON-STDOUT` is captured into the value and never reaches the container's stdout, so §3.6's durable `stdout.log` would be empty for every `eval` task.

**Candidate fix 1 — a pipeline — also fails.** I tried `run_supervised "$cmd" | tee "$TMPOUT"`, which keeps the value *and* the log. Result: **exit 143 again**. POSIX §2.13: *"each command of a multi-command pipeline is in a subshell environment"*. Same class of bug.

**Candidate fix 2 — plain redirection — verified working on both paths:**

```sh
run_supervised "$cmd" > "$TMPOUT"; rc=$?
cat "$TMPOUT"                       # stdout still reaches container logs (§3.6)
value=$(cat "$TMPOUT" 2>/dev/null || true)
```

| Case | exit | logs | values |
|---|---:|---|---|
| `eval` + cancellation | **0** | `WORKLOAD-UP\|WORKLOAD-GOT-SIGTERM` | `{"value":"WORKLOAD-UP\nWORKLOAD-GOT-SIGTERM","exitCode":0}` |
| `eval` + success | **0** | `DIAG\|HELLO-ON-STDOUT` | `{"value":"HELLO-ON-STDOUT","exitCode":0}` |

**The generalizable rule the document needs:** `run_supervised` must never be invoked inside `$( )` or on either side of `|`. Only plain invocation or redirection keeps it in the process tini signals. POSIX [§2.7](https://pubs.opengroup.org/onlinepubs/9799919799/utilities/V3_chap02.html#tag_19_07) confirms redirection alone creates no subshell.

### M2 — Orphaned container after an Orker crash

```
managing process pid=55352; container state before kill:  running=true pid=940389
SIGKILLed the managing process.
container state 3s later:                                 running=true exitcode=0
labeled containers still present:                         orphan-probe   Up 6 seconds

control with --rm:                                        --rm container still running: orphan-rm   Up 5 seconds
```

`--rm` is daemon-side in modern Moby (`AutoRemove` in daemon state), but it removes the container **on exit** — and a SIGKILLed client leaves a container that has not exited. Docker's `--sig-proxy` (default `true`) forwards *catchable* signals from an attached client, which is why a graceful Orker stop works; SIGKILL cannot be proxied, which is exactly the crash case §5.5 row 1 describes. containerd's runtime-v2 [shim API](https://github.com/containerd/containerd/blob/a9eb76fb198ed3efe7b2419c7fd7874582f1831c/api/runtime/task/v2/shim.proto#L29-L32) makes the same separation: a per-container shim owns the process, not the caller. **There is no daemon-side "kill if the client disconnects" or "kill after N seconds" guarantee.**

Today's code already has both halves of the seam: `pkg/plugins/container/docker.go:208-209` emits `--label`, and `:360` already runs `ps --all --quiet --filter label=ork.task=<taskID>`. `internal/orker/container_integration_test.go` already asserts no leftovers by that label.

### M3 — `docker load` does not verify the root

```
filename claims:  sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
actual content:   sha256:85d9ccd2b64b995b534c7a035578f40c130b5ae4afce4da85c53cdd01caf75e3
>>> MISMATCH: filename lies about content

$ docker load -i tampered.tar
Loaded image: alpine:3.20            exit=0
$ docker run --rm alpine:3.20 echo RAN
RAN                                  exit=0
```

Stronger than the finding stated: the reviewer expected the fault to surface later as an unusable image; on Docker 29.1.5 it did not surface at all. Moby's containerd image store calls containerd's archive importer, which hashes each tar entry while ingesting but **does not compare `blobs/<alg>/<encoded>`'s path to those bytes**, and neither OCI `index.json` nor Docker `manifest.json` supplies an authenticated root. (Caveat: my tamper added an annotation inside the index while children still matched, which is why it ran; a child-digest swap would produce a different image. The point is that *nothing verified the root*.)

### M4 — Invalid UTF-8 in `ArtifactRef.name`

```
name bytes: "report\xff.bin"
protojson.Marshal  -> ERROR: proto: field orkstack.v1.ArtifactRef.name contains invalid UTF-8
proto.Marshal(bin) -> ERROR: string field contains invalid UTF-8
protojson(TaskRun) -> ERROR: proto: field orkstack.v1.ArtifactRef.name contains invalid UTF-8
```

Binary refuses too, so there is no encoding escape. `bytes` is the proto3 type for arbitrary octets. Storage is already fine — the local backend base64-encodes opaque names (`pkg/artifacts/artifacts.go:85-116`) and S3 uses a companion manifest (`s3_manifest.go:144-187`) — only the wire cannot carry it.

---

## Disposition of all 20 findings

| Findings | Issue | Verdict | Objective-aligned resolution |
|---|---|---|---|
| F01, F10 | `eval` op loses SIGTERM | **Confirmed by measurement** (M1), twice | Redirection form, verified working. Add the rule: never `$( )`, never `\|`. Keep `cat "$TMPOUT"` so §3.6 still holds. Add the `eval` case to §10.3's SIGTERM fixture — §14.14's evidence covers only the shape, not the shipped script. |
| F11 | `op: script` input contract unimplemented | **Confirmed by reading.** `entry.sh` reads `.op` and `.command` only (`:2118-2119`) and routes `exec\|script` through the same `run_supervised "$cmd"` (`:2145-2147`). It never reads `.script`, never writes a script file, never honours an interpreter, never emits `scriptPath` — yet the table at `:2177` promises `script` adds `scriptPath`. | Either implement it (`script=$(jq -r '.script')`; `interpreter=$(jq -r '.interpreter // empty')`; write to `/orkstack/tmp/script`; `chmod +x`; honour shebang else `sh`; set `scriptPath` in `values.json`) or delete `script` from the op table and the §10.2 row. Today's `pkg/plugins/shell/shell.go:164-181` is the behaviour being ported and is the reference for interpreter/shebang precedence. |
| F07 | No shell-safe rendering for untrusted values | **Confirmed by reading.** Both FuncMaps expose exactly two helpers — `internal/dsl/template.go:16-22` (`artifact`, `env` stubs) and `internal/orker/render.go:152-167` (the real ones). **No quoting function of any kind.** The rendered `command` goes straight to `sh -c` (`:2128`). AGENTS.md:286: *"Shell commands must not interpolate untrusted input without the templating layer's escaping."* | Two independent mitigations, and the design should say which it takes. **(a)** Add a `sh` quote helper to the render FuncMap (POSIX single-quote wrapping with `'\''` escaping) and require first-party images to document that `command` is quoted at use. **(b)** Move the shell boundary out of the template: pass `command` as a JSON **array** in `config.json` and have the entry script use `"$@"`-style execution rather than `sh -c "$1"`, so no string ever becomes code. (b) is the stronger fix and matches `ExecuteTaskInput.command` already being `repeated string` (`§2.2`), but it changes the `activity-shell` input contract. Undecided by the code. |
| F02, F14 | Transfer budget has no `RunPlan` carrier | **Confirmed by reading.** §5.2's struct (`:1279-1299`) declares exactly `Namespace`, `Routes`, `CommitRoot`, `Limits`. §6.3:1716-1719 derives the budget from `ORK_ORKER_TRANSFER_BUDGET_SECONDS` or `limits.artifact_max_count × limits.artifact_max_bytes ÷ ORK_ORKER_FLOOR_THROUGHPUT_BYTES_PER_SEC` with `floor 300 s`; `:1724-1725` says both are "decided at submit … since `ActivityOptions` is set in workflow code and may not read process configuration". Neither has a carrier, the floor-throughput denominator has **no stated default**, and both carry an `ORK_ORKER_` prefix while being read by the engine. §11.1:2336-2338 warns that changing `RunPlan` after the fixture reset forces a second reset. | Cheapest correct shape: add **one** field, `TransferBudgetSeconds int32 \`json:"transfer_budget_seconds"\``, resolved at submit in `internal/api` and carried on `RunPlan`. The workflow then computes `timeout_seconds + grace + plan.TransferBudgetSeconds` as a pure function of its arguments — the same pattern `CommitRoot` already uses. Rename the env vars to the engine's own namespace (`ORK_TRANSFER_BUDGET_SECONDS`, `ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC`) and give the floor an explicit default; §5.2's own argument is that `ORK_ORKER_*` is Orker-local and cannot travel. A derivation from already-carried `Limits` alone is possible but still needs the denominator, so it does not avoid the new field. |
| F13 | Pre-upload commit bound not constructible | **Confirmed by reading.** §5.4 step 4 (`:1511`) projects the full `ActivityCommit` before step 5's uploads, but `ArtifactRef.uri`/`content_type`/`metadata` come back only from `Store.Put`. The `Store` interface (`pkg/artifacts/artifacts.go:40-61`) is `Put/Get/Stat/List/Presign/Delete` with **no ref prediction**; `Stat` needs a URI you do not yet have. Both key builders are unexported: `objectKey` (`artifacts.go:89`) and `s3ObjectKey` (`s3_manifest.go:64`). | The keys are already deterministic functions of data the Orker holds at step 3 — name plus content sha256 (local) or provenance plus name plus sha256 (S3, once §5.4's sixth content segment lands). So export a pure projector rather than a new I/O method: `func (s *S3Store) RefFor(p Provenance, name, sha string, size int64, contentType string) ArtifactRef` and the local equivalent, on a small `RefPredictor` interface the dispatcher can take. Step 4 then measures a manifest whose refs are byte-identical to what step 5 will produce. Alternative — move the bound to after step 3's hashing but compute only the *size delta* — is weaker: it still needs the URI length. Whichever is chosen must be a **Phase 1** interface addition, since Phase 2 is barred from `internal/orker`. |
| F15 | Canceled attempts publish no durable streams | **Confirmed by reading.** §3.6:924-928 promises each stream is "uploaded as its own object and recorded in `ExecuteTaskResult.system_artifacts` and `ActivityCommit.system_artifacts`". §6.3:1676-1684's cancellation sequence signals, waits, kills, removes on a detached context, closes the log sink, and returns `ctx.Err()` — no upload. §7.1 step 15 CLEANUP likewise. A canceled activity produces neither a result nor a commit, so both declared transports are absent. | The **live** chunks are already durable and readable: `internal/orker/logsink.go` writes to `metadata/logs/{run}/{task}/{attempt}/{seq}` and `internal/api/logs.go:172-276` serves them by `(run, task, attempt)` with no dependence on a result. So the honest resolution is to **narrow §3.6's promise**: `stdout.log`/`stderr.log` as `system_artifacts` exist on the success and failure paths; on cancellation the attempt's output remains available through the log stream at its `(run, task, attempt)` coordinates, which the CLI and UI already consume. If durable objects are wanted on the cancel path too, the upload must happen on a **detached context before returning `ctx.Err()`**, and the refs have nowhere typed to go — Temporal allows only `CanceledError` details on that path, no success result — so they would have to travel as `temporal.NewCanceledError(details…)`. State which. |
| F05 | Orphaned containers after an Orker crash | **Contradicted by evidence** (M2) — §5.5 row 1 is false | Three composable measures, all buildable on seams the repo already has. **(1)** Label every container with `ork.run`, `ork.task`, `ork.attempt` and `ork.orker` — §9.2 already specifies the first three. **(2)** A **startup reaper**: on boot the Orker lists `label=ork.orker=<its own id>` and force-removes survivors from its previous life; the exact `ps --all --quiet --filter label=…` call already exists at `pkg/plugins/container/docker.go:360`. **(3)** A **runtime-owned lifetime bound**, because a reaper cannot reach a *dead* Orker's containers on a host that never comes back: the container's own `timeout_seconds` should be enforced inside the container's namespace or by a daemon-side stop timeout, so an abandoned container terminates on its own budget. Correct the §5.5 row to say what actually survives, and note explicitly that duplicate *execution* is within the at-least-once contract while duplicate *resource consumption* is the new cost. |
| F06 | No typed failure payload | **Confirmed by reading.** §7.1 step 14 (`:1771-1775`) says "attach refs and any scan note to the error"; §8.3:1945-1946 has the engine read `ApplicationError.Type()` and details into `Error{code, message, details, retryable, category}`. But `Error.details` is `string details = 3` (`:428`) and **no ApplicationError detail message exists anywhere in §2**. A failed activity returns no `ExecuteTaskResult`, so there is no other transport. An exit-1 task can upload its test report and leave it unreachable. | Freeze one message in Phase 1, e.g. `ActivityFailureDetail{ string reason_code = 1; FailureCategory category = 2; int32 exit_code = 3; repeated ArtifactRef artifacts = 4; repeated ArtifactRef system_artifacts = 5; repeated string scan_notes = 6; ImageRef image = 7; int32 attempt = 8; }` passed as the single `details` argument to `temporal.NewApplicationError(msg, category, detail)`. Temporal's Go SDK carries details through the configured data converter and supports protobuf messages; the workflow retrieves them with `errors.As` to `*ApplicationError` then `.Details(&out)`. The engine then populates `TaskRun.artifacts`/`system_artifacts`/`exit_code` on the failure path exactly as on the success path. This also gives F19's attempt a home on the failure side. |
| F19 | No attempt in `ExecuteTaskResult` | **Confirmed by reading, with a nuance.** `ExecuteTaskResult` has fields 1-13 and no attempt (`§2.2:291-357`); `ActivityCommit.attempt = 16` (`:386`) and `TaskRun.attempt = 4` exist. Workflow code cannot call `activity.GetInfo`, and an adopted result carries a `CommitRef`, not the commit body. **Nuance:** `TaskRun.Attempt` is **never set today** anywhere in `internal/engine` or `internal/api` — grep finds `Attempt` only in the log path (`internal/api/logs.go:115,204,309,341`). So this is an unfulfilled new promise, not a regression. | Add `int32 attempt = 14;` to `ExecuteTaskResult`, set by the Orker from `activity.GetInfo(ctx).Attempt` on a fresh run and from `commit.attempt` on an adopted one. That makes the projection identical for fresh and adopted results — the same principle §5.4's values-refetch already applies — and is one field on a message Phase 1 is freezing anyway. The alternative (engine infers from its own retry count) is wrong for adoption, where the winning attempt belongs to a different Orker. |
| F18 | Opaque artifact names have no lossless wire field | **Confirmed by measurement** (M4), and broader than reported: binary proto refuses too | Two clean options; the code does not choose. **(a) Wire carries the bytes:** add `bytes name_bytes = 7;` to `ArtifactRef`, authoritative, with `name` set only when the bytes are valid UTF-8 — mirroring exactly what §3.2 already does for `name`/`nameB64` in `task.json`, and what `pkg/artifacts/s3_manifest.go:144-187` already does in the store. **(b) Filesystem rejects them:** §3.5 adds "an artifact file name that is not valid UTF-8 is `ContractViolation/ARTIFACT_NAME_NOT_UTF8`", and §3.2 drops the "opaque bytes" language. (a) preserves the stated ABI; (b) is one rule and no wire change. Either is defensible; leaving both stated is not. |
| F03, F20 | `task.json` image fields unreachable under §7.1 | **Confirmed by reading.** §3.2's example (`:733-734`) shows `platformDigest` and `platform` populated; the table (`:817-819`) marks `platformDigest`/`platform`/`delivery` optional, "present once the Orker resolved the platform manifest". §7.1 writes `task.json` at step 6 RENDER (`:1760`) and pulls at step 7 IMAGE (`:1761`). Under that order the three keys are always absent. | Move IMAGE before RENDER. The pull has no dependency on the workspace — `Runtime.Pull(image.pinned)` needs only the input — and pulling earlier also fails faster on `IMAGE_PULL_FAILED` before staging cost. Renumber §7.1 to `… 5 INPUT STAGE, 6 IMAGE, 7 RENDER, 8 CREATE …`. A later rewrite of `task.json` between RENDER and CREATE is the alternative and is worse: it makes the file mutable during setup and complicates the readability/permission story in §3.1. |
| F04, F17 | Surviving HEAD statements | **Confirmed by reading, exactly three.** §1 ownership table line **52** ("Registry credentials \| resolve-only (HEAD manifests), read-only"); §4.5 line **1132** ("used **only** for manifest HEADs"); OQ-13 line **2449** ("`ORK_REGISTRY_AUTH_FILE`, HEAD-only"). Against §4.3:1003-1007 ("It must be a GET, not a HEAD, and this is not a preference") and §13 item 16 at line 2511. Externally: registry `GET` and `HEAD` are both read access to the same resource and both require token scope `repository:<name>:pull`, so "HEAD-only" is not even a distinct or lesser credential. | Change all three to "resolve-only (manifest GET), read-only, no push, no execution". There is one credential surface and one resolver; the phrase "HEAD-only" describes a restriction that does not exist at the protocol level and forbids the resolver §4.3 requires. |
| F12 | Embedded layout trusts a filename | **Contradicted by evidence** (M3) — `docker load` verified nothing and the tampered image ran | `LoadLayout` must, in OrkStack code and before handing anything to the runtime: read `<layout>/index.json`, locate the descriptor whose `digest == image.index_digest`, read that blob's bytes, compute `sha256`, and compare — failing `ContractViolation/IMAGE_DIGEST_MISMATCH` on inequality. Only then import. §4.6:1162's "verify, run" must name *which bytes* are hashed and *by whom*; today it reads as though the layout's own naming is the proof. Worth stating the depth explicitly too: verifying the root proves the index, and child manifests/layers are verified by descriptor-following on load — say so rather than leaving it implied. |
| F16 | Phases still overlap | **Confirmed by reading.** §11.1:2317 gives Phase 1 `internal/engine`; §11.2:2362 gives Phase 2 "the failure-category read side in `internal/engine`". §11.3:2390 gives Phase 3 "`DescribeOrker` replacing `ListPlugins`", but the call sites are `internal/engine/temporal.go:155-181` and `internal/engine/pool_probe.go:140` (Phase 1) plus the Orker-side activity registration (Phase 1), with only `internal/api/service.go:32-36,1423-1435` and `ui/` in Phase 3. §11.2's `CommitStore`-interface split is a genuine improvement and holds. | Apply the same interface trick that already worked for `CommitStore`. **Phase 1** lands `DescribeOrker` end to end — the proto (already in §11.1's list), the Orker activity registration, the engine probe swap in `temporal.go`/`pool_probe.go` — because it owns both packages and the probe must keep pool liveness working through the cut (proof M11.6). **Phase 3** then owns only `internal/api`'s `PluginLister`→`ActivityLister` seam, the catalog, and `ui/`. Likewise move "failure categories into `Error.category`" wholly into Phase 1 (it is `internal/engine/workflow.go:384-402`, Phase-1 territory) and leave Phase 2 with `pkg/artifacts` plus the archive write. Then the ownership lines are true as written. |
| F08 | `container` builtin | **Confirmed as an unresolved contract conflict** — §17 C6 escalates it but does not rule | Escalation **E-A** |
| F09 | Proto compatibility | **Confirmed as an unresolved contract conflict.** §14.6 now reports the six deletions accurately, so this is a genuine conflict rather than a documentation error | Escalation **E-B** |

---

## Answers to the eight instruction items

**1. Wire completeness and lossless identity.** Attempt: add `ExecuteTaskResult.attempt = 14`, set from `activity.GetInfo` on a fresh run and from `commit.attempt` on adoption (F19 row) — note `TaskRun.attempt` is unpopulated today, so nothing regresses. Artifact names: `protojson` **and** binary proto both reject invalid UTF-8 (M4), so either add `ArtifactRef.name_bytes` or make such names a `ContractViolation` (F18 row). Failure transport: a frozen `ActivityFailureDetail` message passed as the `details` argument of `NewApplicationError`, since `Error.details` is a bare string and a failed activity returns no result (F06 row).

**2. Deterministic timeout derivation.** One new `RunPlan.TransferBudgetSeconds`, resolved at submit, mirroring `CommitRoot`; plus renaming the two env vars out of the `ORK_ORKER_` namespace and giving floor throughput a default (F02/F14 row). Deriving from `Limits` alone does not work because the denominator is still unspecified.

**3. Commit-bound constructibility.** Export a pure `RefFor(...)` projector — the keys are already deterministic functions of name and content digest — rather than adding an I/O method to `Store` (F13 row). Must land in Phase 1.

**4. Lifecycle ordering and durable streams.** Move IMAGE before RENDER (F03/F20 row). For cancellation, the honest answer is that the live log stream already carries the bytes durably at `(run, task, attempt)` and §3.6's `system_artifacts` promise should be narrowed to the success and failure paths; if durable objects are required on cancel, the upload must run on a detached context and the refs can only travel as `CanceledError` details (F15 row).

**5. Orphaned containers.** Labels + a startup reaper keyed on `ork.orker` + a runtime-owned lifetime bound, since a reaper cannot reach a permanently dead host's containers (F05 row). `--rm` is not a mitigation (M2).

**6. Image resolution and embedded verification.** One credential surface, described as manifest **GET** in all three remaining places (F04/F17 row). `LoadLayout` must hash the root index bytes against `image.index_digest` in OrkStack code before import (F12 row, M3).

**7. First-party activity contract.** Redirection, never `$( )` and never `|` (M1, verified fix). The implemented input contract reads only `.command` and must either gain `.script`/interpreter/`scriptPath` or drop the `script` op (F11 row). Shell escaping: either a quote helper in the render FuncMap or a JSON-array `command` consumed as `"$@"` — the second is stronger and matches `ExecuteTaskInput.command` already being `repeated string` (F07 row).

**8. Phase disjointness.** Move `DescribeOrker` and the `Error.category` write side wholly into Phase 1; leave Phase 3 with `internal/api`, the catalog and `ui/` (F16 row).

---

## Validation recommendations

1. **Build the shipped `entry.sh` and run the §10.3 SIGTERM fixture against every op, not just `exec`.** M1 shows the fixture passes on `exec` and fails on `eval` for the exact script in the document; a per-shape test would not have caught it.
2. **Add a stdout-reaches-the-log assertion to `orkstack activity test`.** The `eval` stdout swallowing (M1) is invisible to any test that only checks `values.json`.
3. **Add an orphan test to the §5.5 chaos matrix**: SIGKILL the Orker mid-container, then assert no container carrying `ork.run=<run>` survives after the replacement Orker boots.
4. **Add a tampered-layout test**: corrupt the root blob, assert `LoadLayout` returns `IMAGE_DIGEST_MISMATCH` and never calls the runtime. M3 shows the runtime will not catch it.
5. **Add a `name_bytes` (or rejection) round-trip test** with `0xff` in an artifact filename, asserting the commit serializes or the activity fails with the named `ContractViolation`.
6. **Assert the projected commit equals the committed one**: build the step-4 projection, run steps 5-7, and assert byte equality with the manifest written at step 8. That is the property `RefFor` must guarantee.
7. **Re-run `buf breaking` after the Phase-1 additions** (`ExecuteTaskResult.attempt`, `ActivityFailureDetail`, `ArtifactRef.name_bytes`, `RunPlan.TransferBudgetSeconds`) and refresh §14.6; the current transcript predates them.

---

## Unverified / explicitly not established

- **All probes ran against a remote Ubuntu 24.04 Linux daemon** (`docker version` → `linux/amd64 29.1.5`). Signal, exit-code, `--init`, `docker load` and container-lifetime behaviour are runtime properties and transfer; Docker Desktop, rootless Podman and containerd-direct were not tested.
- **M1's workspace used Docker named volumes with `volume-subpath`, not host bind mounts**, because the remote daemon cannot see this host's filesystem. Kernel permission and process semantics are identical; the bind-specific ownership question was not exercised here.
- **M3's tamper altered an annotation inside the root index while children still matched**, which is why the loaded image ran. A child-digest swap would produce a genuinely different image. The finding — that nothing verified the root — holds either way, but "an attacker can substitute an arbitrary image" was not demonstrated end to end.
- **The `activity-http` and `activity-file` images were not exercised at all**; only `activity-shell` is specified concretely enough to build.
- **Temporal cancellation end to end remains unproven**, as §14.15 already records. M1 proves the container-side half only.
- **`RefFor` (F13) and `ActivityFailureDetail` (F06) are proposals, not measurements.** Neither was compiled or benchmarked; both are shaped to the frozen interfaces I read, not validated against them.
- I did not modify any tracked file, run `make`, or commit anything. Files written this session: `research/docs/2026-08-26-oci-contract-review-round-4.md`, `research/web/2026-08-26-subshell-signals-container-lifetime-load-verify.md`, and scratch under `/tmp`.