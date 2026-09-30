# Deploying OrkStack

How to run OrkStack as a replicated deployment, and how to roll a new release
out without breaking runs that are already executing.

The short version, twice.

**Topology.** Every `ork` replica is interchangeable: nothing durable lives in
a process, so any replica answers for any run — including one it never started
and one whose replica has since died. Add replicas for API and workflow
capacity, add Orker pools for task execution. Neither holds state (§0).

**Releases.** `ork` and `orker` of one release share a **Worker Deployment
Version**. A run is **pinned** to the version that started it and finishes
there, whatever you deploy next. That guarantee costs you two rules — make each
version **Current** when you want it to serve (the first one especially: until
then it serves nothing), and do not stop the old workers until their version
reports `Drained`.

The claims below are held by integration tests against the built binaries:
the two-replica topology by `cmd/orkctl/tworeplica_read_integration_test.go`
and `cmd/orkctl/admin_surface_integration_test.go`, the rollout and drain rules
by `internal/engine/versioning_rollout_integration_test.go` and
`internal/engine/versioning_drain_integration_test.go`
(`make test-integration` with the stack up).

---

## 0. Topology — what you run, and where state lives

```
                         one address (LB / ingress / DNS RR)
                                        │
           ┌────────────────┬───────────┴────┬────────────────┐
       ork replica 1    ork replica 2       …            ork replica N
       ORK_HTTP_ADDR    ORK_HTTP_ADDR                  ORK_HTTP_ADDR
       :8081            :8082                          :808N
           └────────────────┴───────────┬────┴────────────────┘
                                        │   one Temporal namespace
                              ┌─────────┴─────────┐
                              │     Temporal      │  ork-tasks           run workflows + idempotency claims
                              │                   │  ork-engine-events   projection
                              └─────────┬─────────┘  ORK_ORKER_QUEUE…    task activities, one queue per pool
           ┌────────────────┬───────────┴────┬────────────────┐
     Orker pool 1      Orker pool 2         …           Orker pool M
           └────────────────┴───────────┬────┴────────────────┘
                                        │
                              one S3 / MinIO bucket
                    definitions · artifacts · finished runs
```

Two independent dials. **Replicas** carry the API and the run workflows;
**Orker pools** carry task execution. Neither holds state, so both scale by
adding processes and both survive losing one.

### Nothing durable lives in a process

| state | where it lives | code |
|---|---|---|
| orkflow definitions | the bucket | `artifacts.S3OrkflowStore`, opened at boot by `openOrkflowStore` (`cmd/ork/main.go`). It keeps no cache, so an `apply` on one replica is visible to every other |
| live runs — status, task state, event stream | Temporal history | `engine.TemporalRunReader` (`internal/engine/reader.go`), handed to the API as `Binding.Reader` by `engine.NamespaceRouter` (`internal/engine/router.go`) |
| finished runs, past retention | the bucket — **once `ORK_ARTIFACT_BACKEND=s3`** | `engine.NewRunArchiver` over `artifacts.NewRunArchive`, written by the terminal `PublishEvent`. The default backend is `local`: a directory on each host, so replicated it is one archive per replica (§1) |
| idempotency keys | Temporal | `engine.IdempotencyClaimWorkflow` (`internal/engine/claim.go`), held for `ORK_IDEMPOTENCY_TTL` |
| artifacts | the bucket — **once `ORK_ARTIFACT_BACKEND=s3`** | `pkg/artifacts`; events and payloads carry refs, never bytes. Same default, same consequence: on `local` an Orker writes to its own disk and no other host can read the bytes back |
| the in-process `RunStore` / `EventBus` | one process, and only that one | **write side only**: the candidate run a submission creates, the stream of a run whose start failed, and the archive + webhook write path. Behind a wired reader they are never read (`internal/api/service.go`, `LookupRun`) |

So a replica owns nothing. Kill one and you lose its in-flight HTTP
connections and nothing else: its runs keep executing on another replica's
workers, and a client that reconnects anywhere reads the same run
(`TestIntegration_SecondReplicaServesARunItNeverStarted`,
`cmd/orkctl/tworeplica_read_integration_test.go`).

**One variable decides whether two of those rows are true.**
`ORK_ARTIFACT_BACKEND` defaults to `local` — a directory per process
(`pkg/artifacts/config.go`) — which is right for `make run-ork` on a laptop
and wrong for every topology in this document. A replicated deployment sets
`s3` and the `ORK_S3_*` values **on both binaries** (§1). Orkflow definitions
are the one piece of state that needs no decision: the engine's store is S3
whatever the backend says (`openOrkflowStore`, `cmd/ork/main.go`), so `ork`
needs `ORK_S3_*` either way.

### Running N `ork` replicas

- **Identical configuration on every replica.** Same `ORK_TEMPORAL_ADDRESS`,
  `ORK_TEMPORAL_NAMESPACE`, `ORK_ORKER_QUEUE`, `ORK_DEPLOYMENT_NAME`,
  `ORK_BUILD_ID`, and the same bucket — `ORK_ARTIFACT_BACKEND=s3` with one set
  of `ORK_S3_*`. A replica differing in any of those is a different deployment
  wearing the same address.
- **`ORK_HTTP_ADDR` is the one variable that must differ** when replicas share
  a host. On separate hosts it does not have to.
- **No sticky sessions, no leader.** `GetRun`, `ListRuns`, `StreamEvents` and
  `/ws/runs/{id}` are answered from Temporal, so any replica serves any run —
  inside the namespace the request names. A run id addresses a run within a
  namespace, not across the deployment: every one of those four carries the
  namespace (`/ws/runs/{id}?namespace=…`), an absent one is `default`, and a
  run of another namespace is `NotFound`.
- **Reconnects resume rather than replay.** `StreamEvents.from_sequence`
  (`orkctl run watch --from-sequence`) and `/ws/runs/{id}?from_sequence=N`
  continue after an event number, and the number is the producer's — the
  workflow assigns it — so resuming against a *different* replica yields the
  same stream with nothing repeated and nothing skipped.
- **Task logs are the same claim, over the bucket.** `StreamTaskLogs` and
  `/ws/runs/{id}/tasks/{task}/logs` read the chunks an Orker wrote and the
  task's state from the cluster, so any replica follows any task — one whose
  Orker has already exited, and one whose run finished before the reader
  connected. They resume the same way, on `from_seq` instead of
  `from_sequence`, over a sequence the single Orker executing that attempt
  assigned (§1, "Live task logs").
- **Readiness: there is no `/healthz` yet.** Probe with a cheap unary call —
  `OrkflowService.ListOrkflows`, i.e. `orkctl --server … orkflow list`.
  `scripts/stack.sh` gates on exactly that; it reaches the bucket the replica
  loads definitions from, so it fails while the replica cannot serve.
- **Browser clients need `ORK_WS_ALLOWED_ORIGINS`** set on every replica, since
  any of them may take the socket. Unset, the engine allows same-origin only,
  which is right for production and wrong the moment the UI is served from its
  own port: the upgrade is refused with 403, the run page never goes live and
  its event log stays empty, while a page refresh still works because that
  reads through Connect rather than the socket. The engine logs
  `websocket upgrade refused` with the origin, the host and this variable's
  value, and `make up` probes the handshake and refuses to report a stack the
  browser cannot stream from.
- **Every replica is also a worker host.** It polls `ork-tasks` (run workflows
  and idempotency claims) and `ork-engine-events` (projection). Adding a
  replica therefore adds workflow capacity as well as API capacity, and it is
  why a run outlives the replica that started it: the next workflow task goes
  to another replica's poller.

### Running M Orker pools

- **A pool is a set of Orkers, and `ORK_ORKER_POOL` is how a process joins
  one.** Start Orkers with `ORK_ORKER_POOL=gpu` and they serve the pool a task
  reaches by writing `pool: gpu` (`docs/DSL.md`). Add processes to a pool for
  throughput; add a pool for isolation — a GPU host, a pool with a different
  Orker build or security profile, a pool near some data. `main` is the built-in pool and needs no
  configuration: `make run-ork` + `make run-orker` with nothing set is one
  Orker serving `main`.
- **`ORK_ORKER_QUEUE` is the operator escape hatch underneath it**, naming the
  raw Temporal task queue directly. It exists for the test harnesses, which
  isolate concurrent runs by pinning a queue no other agent polls (AGENTS.md
  §11.1). Prefer `ORK_ORKER_POOL`: a pool is a name the whole system speaks, a
  raw queue is nobody's pool and is reported as `main` with a boot log saying
  so.
- **Set both and they must agree, or the process refuses to boot.** The check
  is `task.ResolvePoolBinding`, and both binaries run it:

  | set | channel polled / dispatched to | pool reported |
  |---|---|---|
  | neither | `ork-tasks` | `main` |
  | `ORK_ORKER_POOL=gpu` | `ork-pool-gpu` | `gpu` |
  | `ORK_ORKER_QUEUE=ork-tasks-private` | that queue, verbatim | `main`, with a boot log naming the raw queue |
  | both, agreeing | that queue | that pool |
  | both, disagreeing | **boot fails** | `ORK_ORKER_POOL=gpu resolves to "ork-pool-gpu" but ORK_ORKER_QUEUE="ork-tasks-private"` |

  Refusing is the point. Silently picking a winner is how an Orker ends up
  polling one channel while the engine dispatches to another, which strands
  every run in `RUNNING` with nothing to say why.
- **The engine's default route must match its Orkers.** A task that names no
  `pool:` runs on its namespace's `default_pool`, then on `main`. Setting
  `ORK_ORKER_POOL` (or `ORK_ORKER_QUEUE`) on the engine pins that default
  route to the same channel its Orkers poll, so the variable goes on **both**
  binaries. A task that names a pool of its own goes to that pool's channel
  instead, which is how one step reaches a dedicated pool (`docs/DSL.md`) —
  with one exception that is not an exception: `pool: main` names *this
  deployment's default pool*, not the channel `ork-tasks`, so it follows
  whatever you bound here, exactly as an omitted `pool:` does. `main` is a
  word for the default, never an override of it, so no document can escape an
  operator's isolation by naming it (`task.RouteForPool`,
  `runState.routeForTask`,
  `TestPoolResolver_NamingTheDefaultPoolCannotEscapeTheOperatorsRoute`).
- **One set of Orkers has exactly one name.** With `ORK_ORKER_POOL=gpu` on both
  binaries the deployment's default channel *is* gpu's channel, so `main` and
  `gpu` are two words for the same machines. Everything reports the second:
  `ListPools` returns one pool, named `gpu`, marked default; a task that named
  no pool is scheduled, projected and logged on `gpu`. The collapse happens in
  one function (`task.RouteForPool`) and nowhere else. Reporting both would
  give an operator two pools to size and two to drain when there is one, and
  tell a client a task ran on `main` while gpu's Orkers ran it.
- **A submit into a pool with no live Orkers is refused, at submit.** The
  engine asks Temporal which workers are polling the pool's channel and
  refuses the submission when the answer is none, naming the pool, the tasks
  and the fix:

  ```
  pool "gpu" (namespace "payments", tasks: settle) has no live Orkers, so nothing
  would pick these tasks up. Start one with ORK_ORKER_POOL=gpu, or submit with
  allow_empty_pool to wait for one
  ```

  The window is 60 seconds of poller freshness, applied to each poller
  separately: a worker that stopped polling more than 60 seconds ago is not
  counted, however busy its neighbours are. Temporal remembers a poller for
  minutes after its worker died, so a pool you scaled from three Orkers to one
  would otherwise report three for as long as the survivor kept the newest
  record fresh. Two workers of one release can also be described twice while a
  version is being promoted, so the count is per worker identity, not per
  record.

  The residue is bounded and deliberate: inside those 60 seconds a stale record
  can still say "live" just after an Orker stopped. That degrades to the old
  behaviour — a run that waits — never to a false rejection of a healthy
  submit. `SubmitRunRequest.allow_empty_pool` submits anyway; it is off by
  default and taking it logs `WARN submit into empty pool` with the pool and
  the tasks.

  A cluster that cannot be asked is a third answer, and it is kept apart from
  the other two. "No Orkers" is `failed_precondition` — start one, or take the
  override. A probe that got no reply at all (Temporal unreachable, or slower
  than its five-second bound) measured nothing, and the submission it refuses
  may be perfectly good, so it is `unavailable`: ask again. Reported as
  `invalid_argument` it would send every client off to fix a request with
  nothing wrong with it for as long as the cluster was unwell.
- **A namespace's `allowed_pools` bounds what it may dispatch onto**, and it is
  enforced on the server at both apply and submit, so no client can skip it.
  An empty list means no restriction.

  The list is read in the same vocabulary as the dispatch — the collapsed name
  of the bullet above. With `ORK_ORKER_POOL=gpu` on the engine, a task that
  names no pool is dispatched to, stamped as and reported on `gpu`, so a
  namespace whose list is `[main]` is refused there and one whose list is
  `[gpu]` accepts `pool: main`:

  ```
  namespace "payments" does not allow pool "gpu" for task "settle" (allowed: main);
  this deployment dispatches pool "main" onto "gpu"'s Orkers, so allow "gpu" or
  drop the override
  ```

  Every pool a run reports is therefore one its namespace allows. Bind an
  execution channel to a pool's own queue and the namespaces on that
  deployment must allow that pool by name; `main` there names Orkers this
  deployment does not have.
- **"Is anything serving my pools?" has an answer.**
  `NamespaceService.ListPools` (`orkctl pool list`) reports each pool's name
  and its live Orker count. The count is the one described above — the
  Orkers polling now, one per worker — so scaling a pool down shows up in it
  within the window rather than minutes later. Every number is read from the
  cluster, so any replica answers, including one that started nothing. It
  reports each pool from the same channel a submit dispatches it to, so an
  engine using the `ORK_ORKER_QUEUE` escape hatch reports its default pool
  from that raw queue — the Orkers it accepts runs onto are the Orkers it
  says the pool has.
- **Under versioning, every pool a release dispatches to needs an Orker of that
  release's version.** A pool left on the old build is not "lagging": it is the
  pinned-activity exception (§2) firing, which sends that queue's tasks to
  whatever is current there — including tasks of a run pinned elsewhere. Roll
  every pool together, and check each queue appears in
  `describe-version` (§3 step 2).
- **A pool can name itself.** `ORK_ORKER_ENV_ALLOWLIST=ORK_BUILD_ID` on the
  Orker lets a task render `{{ env "ORK_BUILD_ID" }}`; the template resolves in
  the Orker process (`internal/orker/render.go`), so the task output names the
  build that executed it — evidence of which build ran a task that is
  independent of what the server records.

---

## 1. Configuration

`ork` and `orker` read one shared YAML schema, in three layers: built-in
defaults, then a config file, then the environment — each layer overriding
only what the layer before it set. Environment stays the top layer on
purpose: it is what a container orchestrator overrides per replica, and it
is the one place a secret can always live even when the file itself is not
trusted with one. Point either binary at a file with `--config PATH` (a
flag) or `ORK_CONFIG_FILE=PATH` (an environment variable; the flag wins);
with neither set, behavior is byte-identical to a deployment that has never
heard of this section — no file, no default path, nothing implied.

The file is YAML, decoded strictly: an unknown key is refused at boot,
naming its full path. Any scalar leaf may instead be written as
`{ork: X, orker: Y}` to give the two binaries different values from one
file — the shape `telemetry.admin_addr`
needs — and a plain scalar still applies to both. Any string leaf may be
written as `{file: /path}` in place of a literal, to pull a secret from a
file rather than the document itself (one trailing line ending, `\n` or
`\r\n`, trimmed; a relative path resolves against the directory holding the
config file itself, never the process's working directory, so the same
file behaves the same way wherever the binary is launched from);
`storage.s3.secret_key` and `telemetry.otlp.headers.*` are the ones that
matter here, though a named sink's own headers already have their own
`source:` for the same job (§7, "Named sinks"). A key whose documented
default below is a real, non-empty value refuses an explicit empty value
(`""`, `[]`, or `{}`) rather than accepting it, because every reader in this
codebase treats an empty environment variable as unset and would silently
fall back to that default instead — remove the key to get the default
instead of writing an empty value that can never take effect. `--check`
validates the file, prints the effective configuration for the binary you
ran it on as YAML — secret leaves redacted to `***`, and every value
labeled with where it came from: `file`, `env`, or `default` — then exits
0, or 1 naming the first error. `--check` never starts anything.

Two subtrees are documents rather than scalars: `activities:` and
`telemetry.sinks:` carry their own nested schema — the same shapes
`ORK_ACTIVITY_CATALOG` and `ORK_TELEMETRY_SINKS` always took — and are
re-serialized to that variable's YAML text underneath, so a reader who
already knows one of those two shapes need not learn a second one for the
file. `OTEL_BSP_*` and `OTEL_BLRP_*` remain environment only: the config
file has no keys for them, on either binary.

Every other setting is an environment variable prefixed `ORK_`, or, for the
telemetry block, the standard `OTEL_*` names — a config file sets the same
variables underneath, so the table below gives both: the file key first,
then the variable it becomes.

| file key | variable | default | set on | effect |
|---|---|---|---|---|
| `temporal.address` | `ORK_TEMPORAL_ADDRESS` | `localhost:7233` | both | Temporal frontend. The default is the SDK's `client.DefaultHostPort` |
| `temporal.namespace` | `ORK_TEMPORAL_NAMESPACE` | `ork-main` | both | hard partition; nothing crosses it. Provisioned at boot |
| `pool.name` | `ORK_ORKER_POOL` | `main` | **both** | the pool an Orker joins, and the pool the engine dispatches a task naming none to. The OrkStack-level setting; prefer it |
| `pool.queue` | `ORK_ORKER_QUEUE` | *(derived from the pool)* | **both** | operator escape hatch: the raw execution channel, for test harnesses that isolate concurrent runs (AGENTS.md §11.1). Set with `ORK_ORKER_POOL` and the two must agree or the process refuses to boot. It does not move the engine's own channels (§2) |
| `activities:` | `ORK_ACTIVITY_CATALOG` | built-in `shell`/`http`/`file` → `ghcr.io/orkstack/activity-*:0.1` | `ork` | the activity catalog: a YAML `activities:` map from catalog name to registry reference, resolved to a digest at apply time. `make up` points it at the local registry |
| `engine.registry_auth_file` | `ORK_REGISTRY_AUTH_FILE` | *(empty)* | `ork` | a docker `config.json` used only for manifest GETs during apply-time resolution; grants no push and no execution |
| `orker.id` | `ORK_ORKER_ID` | `hostname-<hash of the work dir>` | `orker` | this Orker's identity: labels its containers and keys its startup reaper, so two Orkers must never share one — across the whole deployment, not only on one host: it is also the Orker's address for `orkctl orker` (1–200 bytes, no whitespace or control characters; refused at boot otherwise) |
| `orker.image_layout` | `ORK_ORKER_IMAGE_LAYOUT` | `/var/lib/orkstack/images` | `orker` | directory of OCI image layouts an Orker may serve a pinned digest from when a pull fails. An absent directory is not an error — the fallback matters only to an air-gapped or rate-limited Orker |
| `orker.docker.bin` | `ORK_DOCKER_BIN` | `docker` on `PATH` | `orker` | the container CLI |
| `orker.docker.pull_timeout` | `ORK_DOCKER_PULL_TIMEOUT` | `5m` | `orker` | how long an image pull may take |
| `orker.resources.max_cpu`, `orker.resources.max_memory` | `ORK_ORKER_MAX_CPU`, `ORK_ORKER_MAX_MEMORY` | `8`, `16Gi` | `orker` | caps on what a task may request; a request above the cap is refused, never clamped |
| `orker.resources.default_cpu`, `orker.resources.default_memory` | `ORK_ORKER_DEFAULT_CPU`, `ORK_ORKER_DEFAULT_MEMORY` | `1`, `1Gi` | `orker` | what a task that requests nothing gets |
| `orker.resources.pids_limit` | `ORK_ORKER_PIDS_LIMIT` | `512` | `orker` | pids limit applied to every container |
| `orker.resources.tmpfs_bytes` | `ORK_ORKER_TMPFS_BYTES` | `64 MiB` | `orker` | size of the container's `/orkstack/tmp` tmpfs |
| `orker.resources.force_user` | `ORK_ORKER_FORCE_USER` | `false` | `orker` | `true` runs every container as the Orker's own user, as if each task had set `run_as_orker_user: true` — the readability escape hatch for hosts where the image's own USER leaves output unreadable |
| `orker.secret_schemes` | `ORK_ORKER_SECRET_SCHEMES` | `env,file` | `orker` | comma-separated secret sources this Orker resolves for a task's `secrets:` |
| `orker.secret_allow` | `ORK_ORKER_SECRET_ALLOW` | *(empty)* | `orker` | comma-separated `scheme:pattern` floor of the `secrets:` sources this Orker resolves, e.g. `env:SANDBOX_*,file:/etc/orkstack/secrets/*`; `*` is the one wildcard and does not cross `/`. Empty resolves every source of an allowed scheme, as today. A namespace's `allowed_secrets` can only narrow this, never widen it. Refused at boot if an entry is malformed or names a scheme not in `orker.secret_schemes` |
| `orker.registry_auth_file` | `ORK_ORKER_REGISTRY_AUTH_FILE` | *(empty)* | `orker` | a docker `config.json` used only for the manifest and blob GETs that verify a cosign signature when a namespace sets `require_signed_by`; grants no push, never reaches a container. Empty verifies anonymously, which public registries allow |
| `orker.env_allowlist` | `ORK_ORKER_ENV_ALLOWLIST` | *(empty)* | `orker` | comma-separated names `{{ env "NAME" }}` may read in a task's `with:` — resolved on the Orker, so the value is the pool's |
| `orker.workdir` | `ORK_ORKER_WORKDIR` | temp dir | `orker` | root under which each task attempt gets its own working directory. **It must be bind-mountable by the docker daemon**, since every task's `/orkstack` workspace is bind-mounted from it. The default is `os.TempDir()` — `/var/folders/...` on macOS — which a VM daemon (colima, Lima, Rancher, Docker Desktop) does not share, so a container task fails with `bind source path does not exist` on a path that plainly exists. `make up` sets it to `.data/work`, under `$HOME`, which every one of those VMs shares; it also probes the daemon at startup and warns if the path is unreachable |
| `orker.log.flush_bytes` | `ORK_ORKER_LOG_FLUSH_BYTES` | `64 KiB` | `orker` | live task logs: buffered bytes that trip a flush. Bigger batches a chatty task harder and costs less per object |
| `orker.log.flush_interval` | `ORK_ORKER_LOG_FLUSH_INTERVAL` | `500ms` | `orker` | live task logs: the longest a line waits before it is stored. This is the floor on how late a follower can see a slow task's output |
| `orker.log.max_bytes` | `ORK_ORKER_LOG_MAX_BYTES` | `16 MiB` | `orker` | live task logs: the cap on ONE task attempt. Past it the Orker stores a truncation notice and nothing more |
| `orker.heartbeat_interval` | `ORK_ORKER_HEARTBEAT_INTERVAL` | `5s` | `orker` | how often a running task's heartbeat actually reaches Temporal. Two consequences, both real: it is the longest a **cancellation** waits before the Orker acts on it — Temporal delivers cancellation only in a heartbeat's response, so a failing run's still-running siblings keep burning resources for up to this long — and it is the longest **Temporal frontend outage** a running container survives, since the SDK gives up on an activity whose heartbeat RPC keeps failing for a whole interval. Must be between `1s` and `10s` (a third of the 30 s heartbeat timeout); anything else is refused at boot rather than clamped. Lower it towards `2s` where Temporal is local or highly available; raise it where the frontend is flaky |
| `orker.pins.max_concurrent` | `ORK_ORKER_MAX_PINS` | `0` | `orker` | how many task pins (`docs/DSL.md`, "Pins") this Orker hosts at once. `0` hosts none and starts no session pollers, so an Orker that never set the key runs exactly as it does today; above zero makes this Orker a pin host. Pool-wide — see below |
| `deployment.name` | `ORK_DEPLOYMENT_NAME` | `orkstack` | **both** | the Worker Deployment both binaries join |
| `deployment.build_id` | `ORK_BUILD_ID` | *(empty)* | **both** | this release's identity. **Empty means versioning is off** |
| `engine.http_addr` | `ORK_HTTP_ADDR` | `:8080` | `ork` | Connect API + WebSocket bridge |
| `engine.ws_allowed_origins` | `ORK_WS_ALLOWED_ORIGINS` | *(empty)* | `ork` | comma-separated browser origins allowed onto `/ws/runs/{id}` and `/ws/runs/{id}/tasks/{task}/logs` |
| `engine.idempotency_ttl` | `ORK_IDEMPOTENCY_TTL` | `24h` | `ork` | how long an idempotency key stays bound |
| `engine.limits.values_max_bytes` | `ORK_VALUES_MAX_BYTES` | `256 KiB` | `ork` | contract limit: the largest `values.json` an activity may write. Resolved at submit and pinned into the run's plan, so every attempt of a run enforces the same number |
| `engine.limits.artifact_max_count` | `ORK_ARTIFACT_MAX_COUNT` | `256` | `ork` | contract limit: output artifacts per attempt |
| `engine.limits.artifact_max_bytes` | `ORK_ARTIFACT_MAX_BYTES` | `5 GiB` | `ork` | contract limit: bytes per output artifact |
| `engine.limits.log_max_bytes` | `ORK_LOG_MAX_BYTES` | `16 MiB` | `ork` | contract limit: durable `stdout.log`/`stderr.log` per stream per attempt. Distinct from `ORK_ORKER_LOG_MAX_BYTES`, which bounds the Orker's *live* stream |
| `engine.limits.commit_max_bytes` | `ORK_COMMIT_MAX_BYTES` | `4 MiB` | `ork` | contract limit: the serialized `ActivityCommit`. The Orker bounds the projected commit against this *before* uploading a byte |
| `engine.limits.transfer_budget_seconds` | `ORK_TRANSFER_BUDGET_SECONDS` | *(derived)* | `ork` | data-transfer slack inside `StartToCloseTimeout`. Unset derives `max(300, artifact_max_count × artifact_max_bytes ÷ floor throughput)`. An operator who knows their storage sets it directly |
| `engine.limits.floor_throughput_bytes_per_sec` | `ORK_FLOOR_THROUGHPUT_BYTES_PER_SEC` | `10 MiB/s` | `ork` | the denominator of that derivation. Only read when `ORK_TRANSFER_BUDGET_SECONDS` is unset |
| `storage.backend` | `ORK_ARTIFACT_BACKEND` | `local` | **both** | where artifacts, the run archive and task logs live. **A replicated deployment must set `s3`** — `local` is a directory on the host, so each replica and each Orker would keep its own (§0) |
| `engine.cleanup.enabled` | `ORK_CLEANUP_ENABLED` | `false` | `ork` | enables active per-namespace run retention: retires each finished run at its own namespace's run retention and reclaims its run-scoped logs and completion commits. Default-off because it deletes durable data; when off, every run is kept for the deployment maximum plus `ORK_CLEANUP_RETENTION_BUFFER` regardless of any namespace's own value |
| `engine.cleanup.interval` | `ORK_CLEANUP_INTERVAL` | `1h` | `ork` | interval between cleanup scans; the first scan runs when the worker starts |
| `engine.cleanup.retention_buffer` | `ORK_CLEANUP_RETENTION_BUFFER` | `24h` | `ork` | the enforcement margin: how far past the deployment maximum run retention the underlying (never user-facing) backstop TTL sits, so it can never be what actually retires a run |
| `engine.cleanup.batch_size` | `ORK_CLEANUP_BATCH_SIZE` | `100` | `ork` | maximum eligible runs checked in one cleanup iteration |
| `storage.dir` | `ORK_ARTIFACT_DIR` | `./.data/artifacts` | both | filesystem root for the `local` backend; ignored under `s3` |
| `storage.s3.endpoint` | `ORK_S3_ENDPOINT` | `http://localhost:9000` | both | S3 / MinIO endpoint. `ork` reads orkflow definitions from S3 whatever `ORK_ARTIFACT_BACKEND` says, so this one is never optional for the engine |
| `storage.s3.region` | `ORK_S3_REGION` | `us-east-1` | both | — |
| `storage.s3.bucket` | `ORK_S3_BUCKET` | `orkstack-artifacts` | both | one bucket holds definitions, artifacts, archived runs and task logs |
| `storage.s3.access_key` | `ORK_S3_ACCESS_KEY` | `orkstack` | both | the dev credential from `deploy/docker-compose.yml`. Supply a real one |
| `storage.s3.secret_key` | `ORK_S3_SECRET_KEY` | `orkstack-secret` | both | as above |
| `storage.s3.session_token` | `ORK_S3_SESSION_TOKEN` | *(empty)* | both | for temporary credentials |
| `storage.s3.path_style` | `ORK_S3_PATH_STYLE` | `true` | both | path-style addressing, which MinIO requires |
| `engine.stats_cache_ttl` | `ORK_STATS_CACHE_TTL` | `5s` | `ork` | how long one `GetStats` or `GetFailureBreakdown` answer is reused; `0` switches the cache off (`docs/STATS.md`) |
| `telemetry.log_format` | `ORK_LOG_FORMAT` | `text` | both | process log encoding: `text` or `json` |
| `telemetry.log_level` | `ORK_LOG_LEVEL` | `info` | both | minimum process log level: `debug`, `info`, `warn`, `error` |
| `telemetry.admin_addr` | `ORK_ADMIN_ADDR` | *(empty = no listener)* | both | serves `/healthz`, `/readyz`, `/metrics` (only with the `prometheus` metrics exporter) and `/debug/pprof/` (only with `ORK_PPROF_ENABLED`). Never the API port — see §7 |
| `telemetry.pprof_enabled` | `ORK_PPROF_ENABLED` | `false` | both | mounts `net/http/pprof` on the admin listener. Off by default: an Orker's heap holds resolved secret bytes |
| `telemetry.high_cardinality` | `ORK_TELEMETRY_HIGH_CARDINALITY` | `false` | both | adds `ork.run_id`/`ork.task_id`/`ork.orkflow.name`/`ork.orkflow.version` labels to `ork_*` metrics — see §7, "High-cardinality labels" |
| `orker.task_log_export` | `ORK_ORKER_TASK_LOG_EXPORT` | `false` | `orker` | exports every task stdout/stderr line as an OTel log record. Requires `OTEL_LOGS_EXPORTER=otlp` — see §7, "Task logs" |
| `telemetry.service_name` | `OTEL_SERVICE_NAME` | `ork` / `orker` | both | `service.name` on every signal — the standard OpenTelemetry variable, not `ORK_`-prefixed, honored verbatim (§7) |
| `telemetry.resource_attributes` | `OTEL_RESOURCE_ATTRIBUTES` | *(empty)* | both | standard resource attributes; overrides OrkStack's own `ork.*` attributes value-for-value (§7) |
| `telemetry.exporters.metrics` | `OTEL_METRICS_EXPORTER` | `none` | both | comma list of `prometheus`, `otlp`, `none`. `prometheus` requires `ORK_ADMIN_ADDR` |
| `telemetry.exporters.traces` | `OTEL_TRACES_EXPORTER` | `none` | both | `otlp` or `none`. `otlp` wires the Orkflow's spans end to end — see §7, "Traces" |
| `telemetry.sampler.name` | `OTEL_TRACES_SAMPLER` | *(SDK default: `parentbased_always_on`)* | both | standard OpenTelemetry sampler selector, honored verbatim by the SDK — see §7, "Traces" |
| `telemetry.sampler.arg` | `OTEL_TRACES_SAMPLER_ARG` | *(SDK default)* | both | the chosen sampler's argument, e.g. a ratio for `traceidratio` — see §7, "Traces" |
| — | `OTEL_PROPAGATORS` | *(SDK default: `tracecontext,baggage`)* | both | standard propagator selector for the context a trace travels in, honored verbatim by the SDK. Not in the config file schema — set it in the environment |
| `telemetry.exporters.logs` | `OTEL_LOGS_EXPORTER` | `none` | both | `otlp` or `none`. Also forwards the process's own log lines, including the Temporal SDK's |
| `telemetry.otlp.{endpoint,protocol,headers,timeout,certificate}`, `telemetry.otlp.{traces,metrics,logs}.{endpoint,headers}` | `OTEL_EXPORTER_OTLP_ENDPOINT`, `_PROTOCOL`, `_HEADERS`, `_TIMEOUT`, `_CERTIFICATE`, and the per-signal `OTEL_EXPORTER_OTLP_{LOGS,METRICS,TRACES}_*` | *(SDK defaults)* | both | standard OTLP exporter configuration, honored verbatim by the SDK — recipes in §7. Only `http/protobuf` is supported for `_PROTOCOL` |
| `telemetry.batch.metric_export_interval` | `OTEL_METRIC_EXPORT_INTERVAL` | *(SDK default)* | both | the metric export interval |
| environment only | `OTEL_BSP_*`, `OTEL_BLRP_*` | *(SDK defaults)* | both | batch/backpressure bounds on the span and log processors; a record that cannot be queued is dropped, never waited for. **Environment only** — no config file key exists for these |
| `telemetry.sinks:` | `ORK_TELEMETRY_SINKS` | *(empty)* | both | YAML table of named OTLP destinations a namespace may select by name, in place of the deployment's single `OTEL_*` environment — see §7, "Named sinks" |

The table above is the reference for what each key *means*; for the full,
always-current set of every key at its exact built-in default, see the
generated `deploy/orkstack.defaults.yaml` (regenerate it with `make
config-defaults` after any Schema change, or read it straight from a
binary with `ork --defaults` / `orker --defaults` — both print the
identical file to stdout and exit, with no config file or environment
needed) — every key present, in this table's order, each one commented out
at its own default. `deploy/orkstack.example.yaml` stays the worked,
hand-annotated example for a real deployment to start from; the generated
file is the reference, not something to copy and edit by hand.

### Pin hosting is a pool-wide setting

Pin hosting is a property of a **pool**, not of a process: set the same
`ORK_ORKER_MAX_PINS` on every Orker of a pool, because the submit-time probe
that refuses a pin with no host to open it on asks one Orker of the pool and
takes that answer as representative — a mixed pool is refused or accepted
depending on who answered, and a pin accepted onto Orkers that in fact set `0`
waits for a slot that never appears and then fails the run.

### Pins on a versioned deployment

Pins work with Worker Deployment Versioning (`ORK_BUILD_ID`, §2), and what
holds a pin in place is **not** the routing versioning uses. Measured on a real
cluster by `internal/engine/versioning_pin_integration_test.go`
(`TestIntegration_AnOpenPinStaysOnItsHostAcrossAVersionRollout`), with a pin
host on v1, a second on v2, and the Current Version rolled forward while a pin
was open:

- the pin's session **creation** activity is routed by version exactly like any
  other activity of a pinned run: scheduled on
  `<pool queue>__internal_session_creation` and dispatched to a pin host of the
  run's own version, with versioning in effect;
- the pin's **member** activities ride the session's own `<uuid>@<hostname>`
  queue, which the SDK polls *unversioned*, and reach the one process that
  polls it. The member scheduled after v2 became Current still executed on v1's
  Orker, as did the pin's release and the SDK's session completion activity;
- the run's unpinned tasks kept the ordinary version-pinned routing, and a
  pinned run submitted after the switch opened its pin on v2 — so the older
  pin stayed put by routing, not because the newer version was idle.

Two consequences for a rollout. First, a pin is bound to a **process**, not to
a version, so §3 step 5's wait for `Drained` is what keeps an open pin's host
alive; and restarting that same process — even at the same build id — loses the
pin rather than moving it, because the per-host session queue name is minted
fresh on every worker start. Second, the SDK's own doc comment still says
`DeploymentOptions.UseVersioning` "cannot be enabled at the same time as
`EnableSessionWorker`" (`temporalio/sdk-go#1227`), while the only enforced panic
covers the deprecated `UseBuildIDForVersioning` flag. The combination OrkStack
produces is therefore accepted and behaves as above, but it is not a supported
upstream configuration — which is why that integration test, not the doc
comment, is the contract, and why it runs on every proof.

§3 step 5's `Drained` is the **version**-scoped wait. Its **host**-scoped
complement is `orkctl orker drain` (see "Removing an Orker safely" in §5):
cordon one Orker so it opens no new pin, and wait until its open pins have
finished before the process is stopped.

### The bucket stops being optional at the second replica

`local` is the single-host default, and the one setting that breaks a
replicated deployment quietly: each process keeps its own directory, so a run
archived by replica 1 is invisible to replica 2 and an artifact written by one
Orker cannot be materialized by another. Set the same values on every `ork`
and every `orker`:

```bash
export ORK_ARTIFACT_BACKEND=s3
export ORK_S3_ENDPOINT="https://s3.internal.example"   # or http://minio:9000
export ORK_S3_BUCKET=orkstack-artifacts
export ORK_S3_REGION=us-east-1
export ORK_S3_ACCESS_KEY=…        # never the docker-compose dev credentials
export ORK_S3_SECRET_KEY=…
export ORK_S3_PATH_STYLE=true     # MinIO needs path style; AWS S3 does not
```

One bucket, seven key families (`pkg/artifacts`):

| keys | what |
|---|---|
| `metadata/orkflows/{namespace}/{name}/{version}.json` | orkflow definitions — written by `apply`, read at every boot; immutable once written |
| `metadata/orkflows/{namespace}/{name}/@current.json` | which version that orkflow runs: `{"version":…,"updated_at":…}` and nothing else. Moved by `orkctl orkflow promote`; absent means "the head" |
| `metadata/orkflows/{namespace}/{name}/@head.json` | which version was applied last: `{"version":…,"applied_at":…}`. Moved by `apply`, and moving it IS the apply's commit point |
| `metadata/namespaces/{name}.json` | namespace registry documents; `default` is synthesized when absent |
| `metadata/runs/{run-id}.jsonl` and `metadata/index/runs/{namespace}/…` | archived runs: the body, and the key-only index `ListRuns` scans |
| `metadata/logs/{run-id}/{task-id}/{attempt}/{seq}` | task output, one chunk per object, written while the task runs (below) |
| `orkflows/{orkflow}/{run-id}/{task-id}/{name}` | artifacts, addressed by provenance |

A basename beginning `@` is reserved for per-orkflow metadata stored beside the
versions, so a pointer can never be read as a version. Two replicas promoting
at once are arbitrated by the object store's own conditional PUT — the loser is
told it lost and swaps again against the winner's etag — so promotion needs no
lock, no lease and nothing in Temporal. Neither pointer holds a copy of the
document: a second place for it is a second place for it to be wrong.

The head is a pointer for the same reason, and for one more. Each replica
stamps `created_at` from its own clock, so a head read as "the greatest stamp"
is decided by whichever replica's clock runs fastest; and an edit's
`expected_head_version` has to be checked by the write itself, or a competing
apply lands between the check and the write and the stale edit succeeds
anyway. Both are settled by the swap. `applied_at` is monotonic — an apply
never stamps its version earlier than the head it extends — so `orkctl orkflow
versions` lists them in the order they were applied on a deployment whose
clocks disagree.

### Live task logs: where they are, what bounds them, how long they stay

A task's output is bytes, so it travels as stored objects and never through a
Temporal payload or the event stream (AGENTS.md §2.2). The Orker executing an
attempt buffers its lines and flushes them as numbered chunks **while the task
runs**; `orkctl run logs -f`, `RunService.StreamTaskLogs` and the
`/ws/runs/{id}/tasks/{task}/logs` socket read them back.

```
metadata/logs/{run-id}/{task-id}/{attempt}/{seq}         one chunk
metadata/logs/{run-id}/{task-id}/{attempt}/{seq}.trunc   the truncation notice
```

Both counters are zero-padded to a fixed width, so a lexicographic listing is
numeric order: **the key carries the order**, and no reader sorts by
modification time, which two clocks and a retry would make a lie.

Because the chunks are in the bucket rather than in the process that produced
them, **any replica serves any run's logs** — one it never started, one whose
Orker has exited, one that finished before the reader connected. Under
`ORK_ARTIFACT_BACKEND=local` that stops being true for the same reason the run
archive does: each process reads its own directory.

**Following and replaying are two different reads, and the flag decides which
— never the task.** `orkctl run logs -f` (`follow=true` on the RPC and the
socket) waits for chunks that do not exist yet and returns when the task
reaches a terminal state. Without `-f` the command prints the chunks the store
holds when it runs and returns, even against a task that is still writing, so
a script that wants a transcript is not left tailing a chatty task forever.
Both resume on `from_seq`, which is the last sequence already received: the
stream continues **after** it, so a reconnect replays nothing and skips
nothing.

**A follow moves with the task's retries.** The key above is per ATTEMPT, and
a Temporal activity retry changes nothing a task's status shows — the task
stays RUNNING while try 2 starts writing under a new prefix. So a follow that
named no attempt (`attempt=0`, which is the default and what the UI's pane
sends) re-reads which attempt is latest while the one it is on is silent, and
moves onto a later one, delivering it from its first chunk. A stream that
resolved the attempt once sat on a dead prefix, delivering nothing, until the
task ended — while reporting itself live.

That makes `from_seq` half a cursor: sequences are dense **per attempt**, so a
client that reconnects sends `from_attempt` beside it, naming the attempt its
cursor counted. A cursor from another attempt is dropped and the new attempt
is delivered from its start, which is the only safe reading — a retry is
usually shorter than the try that failed, so applying the old cursor would
deliver nothing at all. An explicit `--attempt N` is pinned and never moves:
it is a request for one try's transcript.

Every client sends both halves: the browser's pane
(`ui/src/lib/taskLogs.ts`), the WebSocket bridge that parses them
(`internal/api/ws.go`), and `orkctl run logs`, which takes `--from-attempt N`
beside `--from-seq N`. A cursor that names no attempt is **refused** by the
CLI rather than applied to whichever try happens to be latest: the CLI cannot
recover the missing half, and guessing it is how a resume silently skips a
retry's first chunks.

Two bounds decide when a chunk is written, and a deployment tunes both:

- `ORK_ORKER_LOG_FLUSH_BYTES` (64 KiB) batches a chatty task, so a hundred
  thousand lines cost a few hundred objects rather than a hundred thousand.
- `ORK_ORKER_LOG_FLUSH_INTERVAL` (500 ms) bounds the wait for a slow one, so a
  task printing a line a minute is visible in half a second rather than when it
  eventually reaches 64 KiB.

A third bounds the damage. `ORK_ORKER_LOG_MAX_BYTES` (16 MiB) caps ONE task
attempt: at the cap the Orker writes a final chunk saying so, in words, and
stores nothing more. **Truncation is visible in the stream**, never silent —
`orkctl run logs` prints the notice and the RPC sets `truncated` on it. The
Orker's own memory is one flush buffer per running task, and a single
over-long line is split rather than accumulated, so neither a chatty task nor
one that never prints a newline can exhaust it.

**What one reader costs the engine is bounded in bytes, not in chunks.** A
replica reads the store one page at a time and holds one page per open
subscriber: at most **4 MiB of chunk data, plus the one chunk that crossed
that budget** (`logChunkPage` and `logChunkPageBytes`, `internal/api/logs.go`).
Both bounds are needed, because a count is not a size — one chunk is whatever
the Orker flushed, so raising `ORK_ORKER_LOG_FLUSH_BYTES` raises what a page of
32 chunks weighs and nothing else would notice. Paging costs nothing in
completeness: a page cut by the budget is followed by another read from the
sequence it ended on, so a replay of a 16 MiB attempt is whole. Size a replica
for this per concurrently open pane or `orkctl run logs`, remembering that a
run detail page opens one per expanded task.

**Retention-aware cleanup is active and fail-closed.** With
`ORK_CLEANUP_ENABLED=true`, `ork` periodically lists archived runs whose
CREATION time is old enough that they *could* be past retention — a cheap
superset prefilter, using the shortest retention any configured namespace
has, never the actual gate. For each candidate it asks the underlying cluster
whether the execution still exists and, if so, when it CLOSED; a run whose
close time is not yet past its OWN OrkStack namespace's run retention
(falling back to the deployment maximum) is left alone. One past that cutoff
is explicitly retired: the cleaner calls the cluster's delete RPC itself
rather than waiting for anything to expire on its own, then removes that
run's `metadata/logs/{run-id}/` and
`metadata/commits/{orkstack-namespace}/{run-id}/` objects. An execution the
cluster no longer holds (already removed by an earlier pass, or by the
backstop TTL — see the run-retention section above) is treated as already
retired: storage is reclaimed with no delete call. The archive body, its
index, and its cleanup marker remain permanent.

Cleanup fails closed. If the retention policy or any namespace's own value is
unreadable, that iteration deletes nothing. An archived run whose execution
the cluster reports as STILL RUNNING is refused rather than retired — closing
would terminate it, and archiving only ever happens after a run's terminal
event, so this is a defect to surface, not a case to handle silently.
Per-run storage failures are logged and retried by a later iteration.
Repeating a successful pass is a no-op because the immutable
`artifacts_cleaned` sidecar records when a run was retired.

The cleaner deliberately does **not** remove workload artifacts under
`objects/`. Those keys are content-addressed by logical name and digest, not by
run id, so two runs can reference the same object and deleting by the producing
run would corrupt the other run's committed result.

#### Object reference accounting

The archive does now record which physical objects each run's completion
references, so safe reclamation is possible; what remains deferred is having
the cleaner perform it automatically. Archiving a run writes two pieces of
bookkeeping beside the run body:

```text
metadata/index/objects/{sha256(object-id)}/{encoded-run-id}   zero bytes
metadata/index/run-objects/{encoded-run-id}.json              this run's object ids
```

The marker key is the reference itself — an object with no keys under its
prefix is referenced by no archived run — so counting references is a prefix
scan that reads no bodies, exactly like the run index. The per-run list is what
makes releasing a run's claims cost one read plus one delete per referenced
object, instead of a scan of every marker in the bucket.

Two details in those key shapes are load-bearing:

- **The object identity is the whole content-addressed key, not the bare
  digest.** Equal bytes stored under two different logical names are two
  distinct stored objects, and refcounting on the digest alone would let one
  run's cleanup delete the other's object.
- **The marker's object segment is a SHA-256 *of* the object id, not the id.**
  An object key embeds a base64 logical name, so it easily exceeds the
  200-byte limit on one archive key segment; hashing it to fixed-width hex
  means no artifact name can make a run unarchivable.

Archiving a run writes one marker per referenced object, so its cost is linear
in the run's artifact count. Those writes are issued with bounded concurrency
(8 in flight, matching the engine's archive fetches) because archiving happens
inside a projection activity with a 10-second budget whose failure is only
logged — a run that exhausts it would end up with no archive body, which is the
only record of it once Temporal retention passes. Measured against MinIO on
loopback: 500 objects took 1.464s serially and 297ms with the writes
overlapped. This reduces the risk rather than removing it: the cost is still
linear, so a sufficiently wide fan-out run can still approach the budget. Treat
the ceiling as raised, not gone.

`DeleteRunArtifacts` releases only the calling run's markers and its list. It
never touches `objects/`. `DeleteUnreferencedObjects` deletes a physical object
only when that object's marker prefix is empty, and treats an object that is
already gone as a no-op rather than an error, so a concurrent cleaner, an
operator, or a lifecycle rule racing the same object cannot turn cleanup red or
inflate its reclaimed-bytes count.

**Automatic `objects/` reclamation is not wired yet.**
`DeleteUnreferencedObjects` exists and is covered by tests against live object
storage, but the cleanup worker deliberately does not call it. Bucket lifecycle
policy is the operational answer for `objects/` growth until automatic
reclamation is wired into the cleanup pass.

A note on what "bounded scan" buys. Cleanup pages the run index in index-key
order, asking for `ORK_CLEANUP_BATCH_SIZE * 2` entries at a time and resuming
each page after the previous page's last key. Two things follow, and they are
different claims:

- **Worker memory is proportional to a page**, not to the archive, because the
  worker holds one page at a time.
- **A whole iteration costs one pass over the index**, because the backend
  stops walking as soon as the page is full. On S3 that is `StartAfter` plus an
  early return; on the local backend it is a key-order descent that prunes any
  subtree sorting at or before the cursor.

Ordering by key rather than by time is what makes the second claim possible.
The run index key is
`metadata/index/runs/{namespace}/{orkflow}/{status}/{stamp}/{run-id}`, so the
stamp is the fourth segment: a newest-first page cannot be produced without
reading every key under the prefix and sorting it. Paging a newest-first query
would therefore re-read the whole index once per page — measured at 22,000 keys
enumerated over 11 pages for 2,000 archived runs at batch size 100, against
2,000 keys for the key-order walk. The amplified case is the steady state of a
healthy deployment, where every eligible run already carries its
`artifacts_cleaned` sidecar, so the walk pages through them to reach older work.

What is still not bounded is the number of queries. Cleanup state is not a
dimension of the immutable run index, so an iteration whose eligible runs are
all already cleaned still issues `archive-size / page` list calls to walk past
them, even though the total keys they enumerate stays linear.

**Bucket lifecycle rules are the independent retention policy.** Cleanup is
off by default, so a deployment that has not set `ORK_CLEANUP_ENABLED=true`
otherwise reclaims nothing. Even when enabled, cleanup retires an execution at
its own namespace's run retention, actively; a bucket lifecycle rule expires
objects by age regardless of that. The two mechanisms are complementary
rather than alternatives.

Lifecycle policy is also the only storage-level mechanism that can bound
growth under `objects/`, because the cleaner intentionally leaves those shared
content-addressed artifacts alone. Choose that expiry separately according to
how long archived artifact references must remain readable. A conservative
log-only rule can bound task-log growth without touching definitions,
artifacts, or archived runs:

```bash
# MinIO: expire task logs after 30 days, leaving definitions, artifacts and
# archived runs alone.
mc ilm rule add local/orkstack-artifacts \
  --prefix "metadata/logs/" --expire-days 30
```

```bash
# AWS S3: the same rule.
aws s3api put-bucket-lifecycle-configuration --bucket orkstack-artifacts \
  --lifecycle-configuration '{"Rules":[{"ID":"ork-task-logs","Status":"Enabled",
    "Filter":{"Prefix":"metadata/logs/"},"Expiration":{"Days":30}}]}'
```

Webhook delivery records (`metadata/deliveries/`, one small object per settled
delivery) are a second prefix an operator may expire the same way. Nothing
reads them to decide anything, so a rule only bounds how far back
`orkctl webhook deliveries` and Admin → Webhooks → Deliveries can look:

```bash
# MinIO: expire webhook delivery records after 90 days.
mc ilm rule add local/orkstack-artifacts \
  --prefix "metadata/deliveries/" --expire-days 90
```

This is separate from `DeleteOrkflow`. Orkflow deletion removes the stored
versions of an Orkflow only after its run-safety gate proves no affected run
is live. Retention cleanup removes one finished run's logs and commits only
after Temporal has already removed that execution. They act on different
objects, have different triggers, and neither deletes archived run records.

A reader that meets a removed chunk mid-stream stops at the gap rather than
skipping it, so a resumed read continues from the last sequence it actually
received.

### Versioning is off until you set a build id

With no `ORK_BUILD_ID`, both binaries register unversioned workers and behave
exactly as they did before versioning existed. That is what keeps a
single-process `make run-ork` + `make run-orker` working with no configuration
at all, and it is deliberate: a build id is the release's identity and there is
no sane value to invent for it.

`ORK_DEPLOYMENT_NAME` *does* have a default, so **one variable turns versioning
on in the workers**:

```bash
export ORK_BUILD_ID="2026.08.16-a1b2c3d"     # → deployment "orkstack", version "orkstack.a1b2c3d"
```

Both binaries print what they resolved, on every boot:

```
level=INFO msg="worker deployment versioning" enabled=true deployment=orkstack \
  build_id=2026.08.16-a1b2c3d version=orkstack.2026.08.16-a1b2c3d \
  run_workflow_behavior=Pinned claim_workflow_behavior=AutoUpgrade event_worker=unversioned
```

A deployment name may not contain `.` — the cluster reserves it. Both binaries
refuse to start on one rather than let the server reject it on the first poll.

### …and the first versioned release needs one call on the cluster

Registering a version does not route anything to it. Until the deployment has
a **Current Version**, the task queue's current version is nil, and

> A nil Current Version represents all the unversioned workers
>
> — `api@v1.63.4/workflowservice/v1/request_response.pb.go`,
> `SetWorkerDeploymentCurrentVersionRequest.BuildId`

which a versioned release does not have. So the first release that sets
`ORK_BUILD_ID` boots two healthy, polling workers that receive **nothing**:
submitted runs sit at `HistoryLength 2` (`WorkflowExecutionStarted`,
`WorkflowTaskScheduled`), no pending activities, indefinitely. Nothing in the
logs says why, because nothing is wrong with the workers.

One call fixes it, and no worker has to restart. It has to be told **where**,
though: the `temporal` CLI reads none of the `ORK_` variables above. Its own
defaults are the namespace `default` on `localhost:7233`, so a bare command
does not fail — it succeeds, against a namespace nobody deployed to, or on a
laptop rather than the cluster. Every invocation in this document therefore
passes `--namespace` and `--address`, and names the deployment through
`$ORK_DEPLOYMENT_NAME` rather than repeating a literal that §1 already owns.

```bash
temporal worker deployment set-current-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$ORK_BUILD_ID" \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
```

Backlogged runs start within seconds of it. §3 step 0 is the full first-release
procedure; every release after that inherits a Current Version from its
predecessor, and takes over with the same call at §3 step 4.

Measured by `TestIntegration_AVersionedReleaseIsIdleUntilItsVersionIsCurrent`
(`internal/engine`), which watches a versioned release receive no workflow task
at all for 15s and then complete the same run 1s after the routing change.

---

## 2. One deployment spans both binaries

`ork` and `orker` are **released in lockstep as one version**. This is not a
convention; it is what makes the pinning guarantee hold.

Temporal routes the activities of a `PINNED` workflow to the workflow's own
Deployment Version — with one exception:

> Activities of `PINNED` workflows are sent to the same Deployment Version.
> Exception … when the activity Task Queue workers are not present in the
> workflow's Deployment Version, in which case the activity will be sent to the
> **Current Deployment Version of its own task queue**.
>
> — `api@v1.63.4/enums/v1/workflow.pb.go`, `VERSIONING_BEHAVIOR_PINNED`

So if the Orker polling `ORK_ORKER_QUEUE` does not carry the engine's exact
`(ORK_DEPLOYMENT_NAME, ORK_BUILD_ID)`, the exception fires and an in-flight
run's tasks land on whatever is current — a newer Orker build, mid-run. Setting
`ORK_BUILD_ID` on the engine and forgetting it on the Orker is therefore the
one misconfiguration to watch for. Check both boot lines print the same
`version=`.

### What is versioned, and what is not

| worker | queue | versioned | behaviour |
|---|---|---|---|
| run worker (`ork`) | `ork-tasks` — **fixed** | yes | `RunWorkflow` = **Pinned**, `IdempotencyClaimWorkflow` = **AutoUpgrade** |
| Orker (`orker`) | `ORK_ORKER_QUEUE` (default `ork-tasks`) | yes | task activities only; must match the engine |
| projection worker (`ork`) | `ork-engine-events` | **no** | `PublishEvent` |

**`ORK_ORKER_QUEUE` does not move the engine.** It names the queue Orkers poll
and the queue the engine dispatches task activities to. The queue that hosts
`RunWorkflow` and the idempotency claims is `internal/task.DefaultQueue` — the
constant `ork-tasks` — whatever `ORK_ORKER_QUEUE` says (`cmd/ork/main.go`,
`queuesFor`; `TestQueues_OrkerQueueMovesTaskExecutionOnly`). With the variable
unset both names are `ork-tasks` and the distinction is invisible; set it, and
one release polls two queues:

```
msg="temporal workers started" task_queue=ork-tasks event_queue=ork-engine-events \
  orker_queue=ork-tasks-gpu-pool
```

One value is refused: `ORK_ORKER_QUEUE=ork-engine-events` is the projection
worker's own queue, so every task activity would be dispatched to it and the
pool a document names would resolve to the engine's internals. Both binaries
report it and exit at boot rather than starting — `task.ResolvePoolBinding`
refuses the channel wherever it came from, and `ork` checks it again against
its own queues (`cmd/ork/main.go`, `queues.validate`;
`TestQueues_RefuseAnExecutionChannelThatIsTheEnginesOwnEventQueue`).

Under versioning both appear, separately, in that version's poller list — the
engine's queue as a **workflow** poller and the Orker's as an **activity**
poller. That is the shape §3 step 2 checks, and reading it as one name aborts
healthy rollouts.

**Why `RunWorkflow` is Pinned.** It interprets a DAG whose scheduling order,
event sequence and activity arguments are decided by current code. Change the
ready-task ordering, the event counter, or the shape of the task activity
input, and replaying a half-finished history against the new build produces a
different command stream — a non-determinism error that kills a run the
operator did nothing to. AutoUpgrade would hand exactly that history to the new
build on its next workflow task. Pinned lets the run finish on the code that
started it.

**Why the claim workflow is AutoUpgrade.** Its history is one update and one
timer, its command stream is independent of run semantics, and it lives for
`ORK_IDEMPOTENCY_TTL` (default 24h). Pinning it would strand a long-lived key
holder on a version you want to decommission, and that version would never
report `Drained`.

**Why the projection worker is unversioned.** `PublishEvent` writes the read
model and the archive from arguments the workflow already computed. It carries
no run semantics, and it must stay executable by *any* live replica of *any*
version — that is what lets every replica answer a read for every run. With no
versioned poller on `ork-engine-events`, the exception clause above routes
those activities to the queue's own current version, i.e. to the unversioned
pollers. `internal/engine/versioning_rollout_integration_test.go` asserts this
directly: in a rollout, a v1-pinned run's remaining events are published by a
replica the v1 release does not contain.

---

## 3. Rolling a release forward

```bash
export ORK_DEPLOYMENT_NAME=orkstack          # §1's default, stated once
export ORK_TEMPORAL_NAMESPACE=ork-main
export ORK_TEMPORAL_ADDRESS=localhost:7233   # the frontend of the cluster you mean
NEW=2026.08.16-a1b2c3d      # v2
OLD=2026.08.09-9f8e7d6      # v1, currently serving
```

The last two are for **you**; the CLI reads neither (§1), which is why every
command below hands it both.

Steps 1–6 are every release that has a predecessor. **The very first versioned
release of a deployment has none, so start at step 0** — skip it and the
release runs nothing at all (§1).

0. **First release only: make it Current before you rely on it.**

   ```bash
   FIRST=2026.08.02-0a1b2c3
   ORK_BUILD_ID=$FIRST ./bin/ork &
   ORK_BUILD_ID=$FIRST ./bin/orker &

   # both queues must report pollers first — same check as step 2
   temporal worker deployment describe-version \
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$FIRST" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"

   temporal worker deployment set-current-version \
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$FIRST" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"

   # CurrentVersionBuildID must not be empty
   temporal worker deployment list \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
   ```

   Runs submitted before that call are backlogged, not lost: they start within
   seconds of it and no worker restarts. If you are switching a system that is
   already serving unversioned traffic, read §3.5 first — runs in flight across
   the switch behave differently from runs in flight across steps 1–6.

1. **Start v2 alongside v1.** Same namespace, same queues, new `ORK_BUILD_ID`.
   Do not stop anything yet.

   ```bash
   ORK_BUILD_ID=$NEW ./bin/ork &
   ORK_BUILD_ID=$NEW ./bin/orker &
   ```

   In a replicated deployment (§0) that means **all** of v2: every `ork`
   replica and an Orker in every pool the release dispatches to. v1's replicas
   stay up and keep serving the API — the two versions coexist behind one
   address, and which version a new run executes on is the deployment's
   Current Version, never the replica the client happened to reach: after the
   switch, a run submitted **to a v1 replica** executes wholly on v2.

2. **Check v2 polls every queue it should**, engine and Orker both:

   ```bash
   temporal worker deployment describe-version \
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$NEW" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
   ```

   Two entries must be there, one per role. They are two different queue names
   whenever `ORK_ORKER_QUEUE` is set (§2):

   ```
   Task Queues:
     Name                  Type
     ork-tasks             workflow     <- the engine's run worker (fixed name)
     ork-tasks-gpu-pool    activity     <- the Orker, i.e. ORK_ORKER_QUEUE
   ```

   Take the two names from v2's own boot line — `task_queue=` and
   `orker_queue=` — rather than assuming them. A missing **activity** entry
   means v2's engine is running without v2 Orkers: stop here, because a pinned
   run's tasks would fall through to that queue's own current version and land
   on the wrong Orker build. A missing **workflow** entry means v2's Orker is
   running without a v2 engine. Do not expect `ORK_ORKER_QUEUE` to appear under
   `workflow`: it never does, and treating that as a failure aborts a healthy
   rollout.

3. **Ramp** (optional but recommended):

   ```bash
   temporal worker deployment set-ramping-version \
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$NEW" --percentage 10 \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
   ```

   New runs split between the versions. Watch error rates. Raise the percentage
   as confidence grows.

4. **Make v2 current:**

   ```bash
   temporal worker deployment set-current-version \
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$NEW" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
   ```

   From this moment every **new** run executes on v2. Every run already in
   flight stays on v1 and finishes there.

   `temporal worker deployment list` reports v2 as Current the instant the call
   returns. That is the deployment's routing **config** — your intent, recorded
   by the deployment's own workflow. Dispatch reads something else: the task
   queue's own current version, which the server syncs to the queues just
   afterwards. On a healthy cluster the gap is milliseconds and no run is lost
   either way (a run started a moment early simply runs on v1 and is pinned
   there). It matters if you **automate** a check around the switch: assert on
   the queue, or you are asserting on a fact the dispatcher has not seen yet.

   ```bash
   # both physical queues, engine and Orker, must report the new version
   go run ./test/tools/queue-routing \
     -namespace "$ORK_TEMPORAL_NAMESPACE" -address "$ORK_TEMPORAL_ADDRESS" \
     -queue ork-tasks:workflow -queue ork-tasks-gpu-pool:activity \
     -want "$ORK_DEPLOYMENT_NAME.$NEW"
   ```

   It is a Go program because the CLI does not surface this: `versioning_info`
   comes back only in `DescribeTaskQueue`'s default api mode, and it is per
   **physical** queue — the engine's queue carries the version on its workflow
   side, the Orker's on its activity side, and asking either about the other
   answers `__unversioned__`.

5. **Wait for v1 to drain — do not skip this:**

   ```bash
   temporal worker deployment describe-version \
     --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$OLD" \
     --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS" \
     | grep -i drainage
   # DrainageStatus: Draining  → v1 still has pinned runs executing
   # DrainageStatus: Drained   → safe to stop v1
   ```

   `Draining` means pinned runs still need v1's workers. Stopping them there
   leaves those runs stuck with no worker to advance them; they resume if you
   bring v1 back, but until then they make no progress. Drainage is refreshed
   on a delay of a few minutes, so poll.

   Measured, not asserted:
   `TestIntegration_APinnedRunStrandsWhenItsVersionsOrkerStopsBeforeDrained`
   (`internal/engine`) stops v1's Orker with v2 Current and watches a run
   pinned to v1 for 45 seconds — its task stays scheduled and unstarted, the
   run stays `RUNNING`, and it finishes on v1 within a second of v1's Orker
   coming back.

   The same test holds the other half, which is the dangerous one: **v2 does
   not pick those tasks up.** The exception in §2 routes an activity to the
   Current Version when the workflow's own version has no workers on that task
   queue, and if it ever fired here, skipping this step would silently run an
   in-flight run's remaining tasks on a newer Orker build — the thing this whole
   document exists to prevent. The test fails the day that changes.

6. **Stop v1** once it reports `Drained` — every replica and every Orker
   carrying that build id. Stopping *some* of them earlier is safe (§5); the
   version is what must not disappear while runs are pinned to it.

A rollout done this way is held by
`TestIntegration_PinnedRunFinishesOnItsOwnVersionWhileNewRunsMoveOn`
(`internal/engine/versioning_rollout_integration_test.go`): the run in flight
when v2 became Current finishes on v1 and **both** of its tasks — including the
one scheduled after the switch — are executed by v1's Orker, while the run
submitted after the switch runs wholly on v2.

---

## 3.5 Turning versioning on for a system that is already running

Steps 1–6 move runs between two versions. Turning versioning on for the first
time moves them from *no* version, and that is not symmetric: a run that
started unversioned has **nothing pinning it**.

> Workflow execution does not have a Versioning Behavior and is called
> Unversioned. This is the legacy behavior.
>
> — `api@v1.63.4/enums/v1/workflow.pb.go`, `VERSIONING_BEHAVIOR_UNSPECIFIED`

So when the first version becomes Current, an unversioned run in flight
**upgrades onto it** at its next workflow task, and is pinned there from then
on: a run whose first task was executed by an unversioned Orker finishes on
the first version, and its second task runs on that version's Orker.

That is convenient — nothing strands — but it is the one moment when a
**partial history meets a different build**: the new version replays what the
unversioned build wrote. Pinning cannot protect it, because the run was not
pinned when it started. Two ways to handle it:

- **Drain first (safest, and the only safe option when the code changed).**
  Stop submitting, let running runs finish, then do §3 step 0. Nothing is ever
  replayed across builds.
- **Switch live.** Safe only when the versioned release is the *same binary*
  as the unversioned one already serving — the usual case, since a build id is
  a label on a build you are already running. Same binary means the same
  command stream by construction, which is the only thing that makes replaying
  an unversioned run's partial history onto the new version a non-event.

  It is **not** made safe by the replay guards in §6. Those guards compare current code against a fixture recorded at
  a known commit; they say nothing about whatever build is running in your
  cluster right now, and a fixture is only "older" than the code once someone
  commits a change after recording it.

  Worse, a live switch across an actual code change is exactly what breaks:
  an unversioned run whose recorded command stream no longer matches the new
  workflow body fails its next workflow task with a determinism violation.
  `TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream`
  (`internal/engine/replay_test.go`) shows the mechanism: it replays the
  committed history against a body carrying one extra emit and the SDK rejects
  it with `TMPRL1100`. Upgrading across such a change therefore means
  draining first.

Either way, keep the old workers up until their runs are done. That rule is
not about versioning: an Orker killed mid-task loses that attempt until its
`timeout_seconds` expires, and the default retry policy is a single attempt
(`internal/engine/workflow.go`, `retryPolicyFor`), so the task fails.

---

## 4. Rolling back

```bash
temporal worker deployment set-current-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$OLD" \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
```

New runs go back to v1 immediately. **Runs already pinned to v2 keep finishing
on v2** — that is the intended outcome, not a fault, and it is why v2's workers
must stay up until *they* drain.

Both halves follow from the same routing rule: with a run in flight on v2
and v1 made Current again, a run submitted **to the v2 replica** afterwards
executes on v1, while the in-flight run finishes on v2 and its task scheduled
*after* the rollback is executed by v2's Orker.

This only works while v1's workers are still running, which is the real rule
behind the drain wait: **never decommission the previous version until the next
one is proven.** Pinning cuts both ways: after a rollback, v2 is the version you
cannot decommission yet.

If v2 is already gone and a run is stranded on it, the escape hatch is a
per-execution versioning override:

```bash
temporal workflow update-options \
  --workflow-id ork-1786941899450567000 \
  --versioning-override-behavior pinned \
  --versioning-override-deployment-name "$ORK_DEPLOYMENT_NAME" \
  --versioning-override-build-id "$OLD" \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
```

That moves one run onto a version that did not start it, so the determinism
argument in §2 applies in full: if the two builds disagree about the command
stream, the run fails its next workflow task. Use it to rescue a run you would
otherwise abandon, not as a routine tool.

### Back to unversioned workers

To take a deployment out of versioned routing altogether — an escape hatch,
not a release step — point its Current Version at the unversioned workers:

```bash
temporal worker deployment set-current-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --unversioned \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
```

New runs then go to whatever unversioned pollers exist, so start those first;
`--allow-no-pollers` overrides the check if you have to do it in the other
order, and leaves new runs backlogged until an unversioned worker appears.
Runs already pinned to a version still need that version's workers.

---

## 5. Ops notes

- **Run ids** are `ork-{timestamp}-{sequence}` and single-use for all time.
  Nothing may parse one or derive meaning from its parts — the sequence
  carries a per-process nonce in its high half (`internal/engine/runid.go`),
  so it is comparable only within one replica and means nothing across two.
  Order runs by `created_at`. The one exception, and it is not a licence to
  order by id: two places compare ids to break a tie between runs with the
  *same* timestamp, so a page is stable rather than arbitrary — `mergeRuns`
  (`internal/api/service.go`, guarded by
  `TestListRuns_OrdersRunsByTimeNotByRunID`) and `RunQuery.finish`, which
  orders one page of the archive index (`pkg/artifacts/archive.go`). Those two
  are the whole list.
- **Any replica answers any run** (§0). Live runs are read from Temporal,
  finished ones from the archive in the bucket — the half that needs
  `ORK_ARTIFACT_BACKEND=s3` (§1), because on the default `local` backend each
  replica archives to its own disk and only that replica can answer for a run
  past Temporal retention. Nothing durable lives in a process, so a read does
  not need to reach the replica that started the run — or that replica to
  still exist.
- **Listing one namespace's runs is a question the cluster answers.** Each
  engine registers the `OrkflowNamespace` search attribute on its Temporal
  namespace at boot (`task.EnsureRunSearchAttributes`, beside the namespace
  itself) and stamps every run it starts with the namespace that run belongs
  to, so `run list -n payments` is a visibility predicate rather than a filter
  over a page of everyone's runs. That distinction is a tenancy one: filtered
  afterwards, how far the list could see was set by how busy the OTHER
  namespaces were, and a namespace with one live run behind a neighbour's
  hundred was told it had none.

  A cluster that refuses the registration — the operator API behind an
  authorizer, a visibility store without custom attributes — still boots, and
  says so once:

  ```
  WARN visibility does not index a run's orkflow namespace
       search_attribute=OrkflowNamespace
       consequence="listing a namespace's live runs scans the whole index"
  ```

  The reader then reads the index to the end instead of trusting a predicate
  the cluster cannot apply, which is slower and still the tenant's own answer;
  a list that cannot finish even that walk fails rather than returning a short
  one. Runs started *before* the attribute existed carry the namespace in
  their memo but not in the index, so they are listed by id and, once
  finished, from the archive — whose key already begins with the namespace.
- **A double submit is settled by the cluster, not by a replica.** One
  `--idempotency-key` sent to two replicas at once returns one run id, because
  the claim is a Temporal workflow (`engine.IdempotencyClaimWorkflow`), not
  process memory. Retries are safe from any address behind the load balancer.
  A key is scoped to the ork namespace it was submitted into, so two tenants
  can both submit `nightly` and get their own run
  (`api.submission.claimKey`); unscoped, the second would be answered with the
  first tenant's run id and its work would never start.
- **`ORK_ORKER_QUEUE` must be set on both binaries.** Set it on only one and
  runs strand in `RUNNING`: the Orker polls one queue while the engine
  dispatches to another. It moves **task execution only** — the engine's own
  workers stay on `ork-tasks` and `ork-engine-events` (§2). A task's own
  `pool:` in the DSL is how a step reaches a dedicated Orker pool
  (`docs/DSL.md`), and it cannot be used to escape this setting: `pool: main`
  is the name of whatever queue you bound here, so it follows the override
  rather than outranking it, and no document can route itself back onto
  `ork-tasks` by naming the default pool
  (`TestPoolResolver_NamingTheDefaultPoolCannotEscapeTheOperatorsRoute`).
- **One namespace, one deployment.** The engine's queue name is fixed, so two
  OrkStack deployments in one namespace would fight over the current version of
  `ork-tasks`. Give each environment its own namespace, as above.
- **`ORK_TEMPORAL_NAMESPACE` is the hard partition.** Nothing — no workflow, no
  activity, no queue, and no worker deployment — crosses it. Parallel test or
  staging environments get their own namespace.
- **Rolling a replica.** Stop it whenever you like: its runs continue on the
  other replicas' workers, and a client reconnects anywhere with
  `from_sequence`. Under versioning the constraint is the *version*, not the
  replica — do not take the **last** replica of a version down until that
  version reports `Drained` (§3 step 5).
- **Process hygiene.** Before any manual end-to-end, kill stray workers:

  ```bash
  pkill -f '/bin/orker$'; pkill -f '/bin/ork$'; sleep 2
  ps aux | grep -E 'bin/ork|bin/orker' | grep -v grep   # must be empty
  ```

  Anchor both patterns with `/` and `$`, as `AGENTS.md` §11.1 does. Unanchored,
  `bin/ork` also matches `bin/orkctl` — the cleanup then kills the CLI you are
  about to drive the deploy with.

### The admin surface: what a deployment is made of, and who is serving it

Eight reads answer every operational question about a deployment, and each is
available from the CLI and over Connect. Six of them are answered from the
cluster or the bucket, so **any replica answers them and they all answer the
same** (`AGENTS.md` §7). Two — system information and the activity catalog —
are deliberately not.

| read | CLI | where the answer comes from |
|---|---|---|
| system information | `orkctl admin info` | **this replica's own build and configuration** |
| maximum run retention | `orkctl admin retention get \| set <duration>` | the retention policy document in the bucket — **any replica answers**, and writable |
| pool health | `orkctl pool list -n <ns>` | the cluster's poller information, plus the Orkers themselves |
| namespaces | `orkctl namespace list \| get \| create \| update \| delete` | the registry in the bucket — each namespace's own `run_retention` lives here too |
| activity catalog | `orkctl activity list` | **this replica's own configuration** (`ORK_ACTIVITY_CATALOG`), resolved to pinned digests at apply time |
| audit trail | `orkctl admin audit` | the audit records in the bucket (`metadata/audit/`) — **any replica answers**, for writes any replica recorded |
| webhook deliveries | `orkctl webhook deliveries \| delivery \| redeliver` | the delivery records in the bucket (`metadata/deliveries/`) — **any replica answers**, for deliveries any replica made |
| Orker inventory | `orkctl orker list \| get \| cordon \| uncordon \| drain` | the cluster's poller information, plus each Orker itself — **any replica answers**; nothing stores an Orker |

`orkctl admin info` describes **the replica that answered**, and it says so in
its own output. Behind a load balancer that is whichever replica took the
request:

```
engine:       a1b2c3d
go:           go1.27.1
deployment:   orkstack
versioning:   off
artifacts:    s3://orkstack-artifacts (s3)
temporal:     localhost:7233
server time:  2026-01-02T03:04:05Z
```

Every line except `server time` is configuration- or build-derived — read from
the process, never measured. That is the point of it: replicas of one release
answer identically by construction, so a field that DIFFERS between two
addresses is information. Mid-rollout the field that differs is the build id,
which is why `versioning:` prints the state rather than the raw value:
`off` when no `ORK_BUILD_ID` is set (the documented off switch, §1), and
`on (build <id>)` when one is. `engine:` is stamped at build time by the
Makefile's `-ldflags`; a plain `go build ./cmd/ork` reports `dev`.

Deliberately **not** in it: whether the cluster is reachable. Pool health is
that read — it cannot answer at all without the cluster — and a connectivity
flag would be stale the moment it was sent, while looking like a measurement.
Deliberately not in it either: any credential. The artifact line names the
bucket or directory, never the key that opens it.

`orkctl pool list` is the health read, and it distinguishes two states a
single number cannot:

```
NAME     ORKERS                 DEFAULT  LAST SEEN
cold     0 (no live orkers)     -        -
gpu      1                      yes      2026-01-02T03:04:05Z
remote   unknown (unreachable)  -        -

A task naming no pool runs on gpu.

1 pool could not be reached, so its Orkers are unknown rather than zero:
  remote: cannot tell whether pool "remote" has live Orkers: context deadline exceeded
```

Three states, and the column keeps them apart because they need three different
actions:

| ORKERS | what it says | what to do |
|---|---|---|
| `1`, `2`, … | that many Orkers are polling for this pool's work right now | nothing |
| `0 (no live orkers)` | **measured**: nobody is serving this pool, and a task sent there waits | start an Orker with `ORK_ORKER_POOL=<pool>` |
| `unknown (unreachable)` | **not measured**: the cluster did not answer for this pool | ask again; the pool may be perfectly healthy |

A pool with no Orkers is **listed**, because "nobody is serving this" is the
answer the question was asked for. A pool that could not be asked about is
listed too, and never as a zero: reporting "not measured" as "measured zero"
sends an operator to scale a pool that may need nothing.

**Pool health answers as much as it can.** A source that cannot be reached
costs the answer the part that source held, is logged with the pool or the
store it lost, and costs nothing else. The partial-source rules are:

| what is lost | what the answer loses | where it is recorded |
|---|---|---|
| the cluster, for one pool | that pool's count and `LAST SEEN` | `Pool.unavailable_reason` on the wire, printed under the table, and a warning log |
| the orkflow store, for one namespace | the pools only that namespace's stored definitions name | the log |
| the namespace registry | every non-default pool | the log |

That rule is not a preference: an operator opens pool health when something is
already wrong, and an `Unavailable` there hides every pool that IS healthy in
order to report one that is not.

The one refusal left is the one that leaves nothing to report: `orkctl pool
list` on a namespace that cannot be resolved or whose default pool this
deployment cannot route (under which no task of it could run at all).

What that costs, stated so nobody has to discover it:

- **There is no per-pool inventory of what a pool can run.** An activity is an
  OCI image, and which images a deployment can run is engine configuration
  (`orkctl activity list`), not something an Orker reports. A pool's Orkers
  still prove they are alive by answering `DescribeOrker` on the pool's own
  channel; that probe is what `LAST SEEN` and the count are built from.
- **`LAST SEEN` never dates a dead pool.** Temporal remembers a poller for
  minutes after its worker died, so a record older than the freshness window is
  dropped before anything is counted or timestamped. A pool whose Orkers have
  all gone quiet shows `-`, not the moment the last one was heard. The column
  says how fresh the number beside it is, and nothing else.

The pool scope of that read is the durable state the replica can read: the
deployment's default route, namespace allow-lists, and pools named by any
stored orkflow version. A pool that no namespace or stored version names cannot
be inferred from this API. Temporal's task-queue description call requires a
queue name; it has no read that lists every task queue, and those names stay
below this user-facing pool surface.

The whole surface, answered by a second replica that started nothing — no
namespace registered, no orkflow applied, no run — is proven live in
`TestIntegration_AReplicaThatStartedNothingAnswersTheAdminSurface`
(`cmd/orkctl/admin_surface_integration_test.go`).

### Removing an Orker safely: cordon and drain

A pin never moves (`AGENTS.md` §5): stopping a pin host fails every run whose
pin it holds with `PIN_HOST_LOST`, and restarting it loses the pin just the
same. `orkctl orker` is the supported way to take one host out of service
without inflicting that:

```bash
orkctl orker list --pool build
orkctl orker cordon orker-7f3a --reason "CVE-2026-1234 runtime patch"
orkctl orker drain  orker-7f3a --timeout 4h --watch
```

```text
$ orkctl orker drain orker-7f3a --timeout 4h --watch
cordoned orker-7f3a: "orkctl orker drain"
waiting for 2 tasks and 1 pin to finish…
  09:15:40  2 tasks, 1 pin     pin "build" of run ork-20260922-…  expires in 2h11m
  10:02:18  0 tasks, 1 pin     pin "build" of run ork-20260922-…  expires in 1h24m
  11:26:51  0 tasks, 0 pins
orker-7f3a is drained; it is safe to stop this process.
```

Stop the process when `drain` exits `0`. Exit `4` means `--timeout` passed
with work still on the host, printed; stopping the process then fails the open
pins' runs with `PIN_HOST_LOST`, as the output says. Exit `1` means the Orker
stopped answering, or was uncordoned or restarted — a cordon is a flag in the
Orker **process** and nowhere else, so a restarted Orker comes back ready.

What a cordon does, precisely:

- **New tasks.** The Orker's activity slot supplier stops reserving slots, so
  its pool pollers stop polling. A poller already inside a long poll when the
  cordon lands may still be handed one task; with the SDK's two activity
  pollers, at most two tasks can land on an Orker after `CordonOrker`
  returned, each within about a minute. They are ordinary in-flight tasks:
  `running_tasks` counts them and drain waits for them.
- **New pins.** The SDK's session creation activity is refused with a
  retryable `ORKER_CORDONED`, and the SDK retries the creation on another
  host. When **every** pin host of the pool is cordoned, the refusal turns
  non-retryable after 45 attempts — about five minutes, the time a saturated
  pool takes — and the triggering task fails `PIN_UNAVAILABLE`, its message
  naming the cordon and its reason.
- **Everything in flight** is untouched: running containers finish, and open
  pins keep executing their remaining members, their release, and their
  completion on this host.
- **The pool probe.** A cordoned Orker stops answering `DescribeOrker` on its
  pool channel (only its own control channel still answers), so a pool whose
  every Orker is cordoned reports its poller-derived count for the freshness
  window and zero after it.

`SIGTERM` is unchanged: it stops polling and cancels in-flight work as before.
An operator who wants in-flight work to finish first cordons and drains; the
Orker does not cordon itself on `SIGTERM`.

`ORK_ORKER_ID` must be unique across the **whole deployment**: it is the
Orker's Temporal worker identity and names its control channel. Two Orkers
sharing one show as a single row whose `unavailable_reason` names the
collision, and a cordon could land on either. The derived default
(`<host>-<hash of the work root>`) is unique by construction. Each Orker holds
two more idle long polls against the cluster (its control and presence
channels) than before this capability existed. An Orker of an older release
polls neither: `orkctl pool list` still counts it, `orkctl orker list` does
not list it, and `orkctl orker cordon` answers `NotFound` for it.

Every cordon and uncordon is an audit record (`orker.cordon`,
`orker.uncordon`, target the Orker id, the reason in its detail, no
namespace).

### Namespace admission: `hold` and `max_running`

Two fields of a namespace's own document decide whether it admits a new run.
Both are checked at one place — `SubmitRun`'s preparation, the gate every door
goes through (`orkctl run submit`, `run again`, the UI's submit dialog, a
trigger invoke) — after the namespace and orkflow resolve and before any pool
is looked at, so `--allow-empty-pool` does not bypass them. A refusal is
`failed_precondition`, synchronous, and leaves nothing behind: no run id, no
idempotency claim, no recorded run. Nothing is queued; releasing a hold starts
nothing that was refused while it was set.

- **`orkctl namespace update NS --hold "REASON"`** closes the namespace to new
  runs; `--release-hold` reopens it. The reason is required (at most 512
  bytes, one printable line); the registry stamps `since` on the write that set
  the hold and keeps it across a reason edit. Runs already accepted are never
  touched — a hold is an admission decision, never a cancellation. The hold is
  read from the registry on every submit, so a hold written through one
  replica is enforced by every other on its next request.
- **`--max-running N`** caps how many runs of the namespace may be in flight
  (accepted and not finished — the population `GetStats.running` reports); `0`
  is unlimited and costs nothing. The count is read from the cluster's
  execution index at each submit, never from a counter OrkStack keeps, so any
  replica counts runs the others started. An index that cannot be read refuses
  the submit `unavailable` rather than admitting it.
- **The cap can overshoot, by design.** Each submit reads the count and then
  starts, with no lock between replicas (a lock is exactly what `AGENTS.md`
  §2.1 forbids building). Submits racing through the window between one
  replica's read and the cluster indexing another's start can each be
  admitted, so a burst against several replicas can land a few runs over the
  cap. It is a guard, not a semaphore; a guard that is occasionally one over
  is worth far more than a lock service.
- **A keyed repeat is refused while held or at the cap.** Admission runs
  before an `idempotency_key` is looked at, so repeating a key whose first
  submission was accepted returns the refusal instead of that run while the
  namespace is held or full. The run the key names is unaffected and readable
  by id and in `run list`.
- **Rolling upgrade.** A release older than this one discards both fields and
  admits, so during a rollout a hold is enforced only by replicas already on
  this release; an older `orkctl` writing a held namespace back through
  `namespace update` drops the hold — run the `orkctl` of the release.
- **Refusals are counted** on `ork.runs.refused` (§7, "Lifecycle metrics") and
  logged once per refusal at Info as `run refused`, with `namespace`,
  `orkflow`, `reason` (`NAMESPACE_HELD` or `MAX_RUNNING_REACHED`) and
  `hold_since` or `running`/`max_running`. `GetStats` does not report them:
  a refused submission is in neither record it derives from.

### Attributing control-plane writes

Every call of a control-plane **write** RPC — namespace create/update/delete,
orkflow apply/promote/delete, run cancel, trigger create/delete, webhook
register/delete (over Connect and over the legacy `/v1/webhooks` routes), the
retention maximum, and the image policy (`image_policy.update`, its keys
recorded by fingerprint, never as PEM) — writes one immutable record, succeeded or refused, to
`metadata/audit/{yyyy}/{mm}/{dd}/{id}.json` in the bucket, before the response
leaves the engine. Reads are never recorded, and neither is `SubmitRun` nor a
trigger invocation: a run's own record is already the durable fact.
`orkctl admin audit` and Admin → Audit read the trail back, newest first,
from any replica:

```
WHEN                  PRINCIPAL        ACTION            NAMESPACE      TARGET         RESULT
2026-03-14T11:02:44Z  svc-platform-ci  retention.update  -              deployment     ok
2026-02-28T16:44:58Z  unattributed     namespace.delete  staging        staging        refused: failed_precondition

Scope: every namespace, 2026-03-07T11:02:44Z to 2026-03-14T11:02:44Z (exclusive), newest first.
principal is the identity the deployment's gateway asserted in Ork-Principal;
OrkStack records it and does not authenticate it.
```

- **`principal` is an assertion, not an identity.** It is the value of the
  `Ork-Principal` request header, recorded verbatim (bounded, control
  characters replaced). OrkStack has no authentication and does not verify
  it. A request without the header is recorded with an empty principal,
  which both clients render as `unattributed`.
- **A gateway that authenticates callers must OVERWRITE `Ork-Principal`**
  with the identity it established, never pass a client's value through —
  otherwise any caller can claim any name. The UI's server forwards the
  header a gateway sets on browser requests to the engine unchanged.
- **`ORK_PRINCIPAL` makes `orkctl` assert one.** Set in `orkctl`'s
  environment, every request it sends carries `Ork-Principal:
  $ORK_PRINCIPAL`. It is an assertion like any other: useful on a `make up`
  deployment with no gateway, never proof of who typed the command.
- **`source_address` is the TCP peer the engine accepted the request from.**
  Behind a gateway that is the gateway's address; no `X-Forwarded-For` or
  other proxy header is consulted. The client's address is in the gateway's
  own access log.
- **The write is best-effort.** An audit store that refuses the write never
  changes what the write RPC returns: the engine logs `audit record dropped`
  at warn (action, target, principal, error) and counts
  `ork.audit.records{outcome="dropped"}` (§7 "Metrics").
- **Nothing reclaims the trail.** The cleanup worker never touches
  `metadata/audit/`; a bucket lifecycle rule on that prefix is the operator's
  retention for it. There is no setting: a deployment that upgrades gets
  records, unattributed until something asserts a principal.
- **A listing is bounded.** The default window is the seven days before now;
  `--since`/`--until` choose another. A page that would examine more than
  5000 records before filling is refused (`resource_exhausted`) rather than
  answered from the subset it read — narrow the window, and page a long
  compliance range month by month.

### Run retention: a deployment maximum, a value per namespace

OrkStack keeps Temporal as an implementation detail an operator never has to
think about (AGENTS.md §7) — including here. What an operator sets is **run
retention**: how long a finished run stays readable before OrkStack retires
it and reclaims its run-scoped storage (logs, completion commits, snapshots).
There are two knobs, and they compose:

- **A deployment maximum** (`orkctl admin retention get|set <duration>`,
  `SystemService.GetRetentionPolicy`/`UpdateRetentionPolicy`): the ceiling no
  namespace may exceed, and what a namespace that sets nothing inherits.
  Deployment-wide, stored with the deployment (not a live cluster read), so
  any replica answers the same.
- **A run retention per namespace**
  (`orkctl namespace create|update --run-retention <duration>`,
  `orkstack.v1.Namespace.run_retention`): how long THAT namespace's finished
  runs are kept. Unset inherits the deployment maximum. Setting one above the
  current maximum is refused with `INVALID_ARGUMENT`, naming both values;
  lowering the maximum below a namespace's explicit value is refused with
  `FAILED_PRECONDITION`, naming that namespace — lower it first.

Retention counts from when a run **finished**, never from when it was
submitted: a run that executed for two days under a 72h retention is readable
for 72h after it ended, not 72h after it started.

**Enforcement needs `ORK_CLEANUP_ENABLED=true`** (§ "The bucket stops being
optional…" above): the cleanup worker is what actually retires a run at its
namespace's own retention. A deployment that never enables it keeps every run
for the deployment maximum plus the enforcement margin
(`ORK_CLEANUP_RETENTION_BUFFER`) regardless of what any namespace asks for —
`orkctl admin retention get` and the Admin UI both say so plainly
(`namespace retention: not enforced`).

Internally — never exposed to an operator — OrkStack keeps the single
Temporal namespace behind the deployment (`internal/engine/router.go`'s
`SingleRouter`) at a retention TTL of `maximum + margin`, strictly above every
namespace's own retention, so that TTL can never be what actually retires a
run. The cleanup worker retires runs itself, explicitly, ahead of that TTL;
the TTL is purely a backstop for worker downtime or a large backlog. See
`internal/archive/retention.go` (`RetentionController`) and the cleanup
worker's own doc comment (`internal/archive/cleanup.go`) for the mechanism.

`<duration>` uses Go's `time.ParseDuration` syntax (`168h`, `24h30m`, …), the
same flavour every `ORK_*_TTL`/`ORK_*_INTERVAL` variable in the table above
uses. A zero or negative duration is refused locally, before any request
leaves the process:

```
$ orkctl admin retention get
maximum run retention:  72h0m0s
namespace retention:    not enforced (ORK_CLEANUP_ENABLED=false: every run is kept
                        for the maximum plus the enforcement margin)

Each namespace may keep runs for less — see `orkctl namespace update
--run-retention`.

$ orkctl admin retention set 168h
maximum run retention:  168h0m0s
namespace retention:    not enforced (ORK_CLEANUP_ENABLED=false: every run is kept
                        for the maximum plus the enforcement margin)

Each namespace may keep runs for less — see `orkctl namespace update
--run-retention`.

$ orkctl namespace update payments --run-retention 24h
Updated namespace payments
Name:           payments
...
Run retention:  24h0m0s
```

The same reads and writes are the UI's Admin → Retention page
(`ui/src/routes/admin/retention`) for the deployment maximum, and the Admin →
Namespaces page for each namespace's own value — the ceiling sits apart from
`/admin/namespaces` because it bounds all of them, not because it belongs to
one.

**Migration.** An existing deployment's current retention becomes the new
deployment maximum automatically, the first time an upgraded `ork` boots: it
observes the cluster's current retention TTL and stores it as the maximum
unchanged (nothing is tightened), then raises the underlying TTL by the
enforcement margin. Every existing namespace inherits that maximum until an
operator gives it its own, tighter value — nobody is opted into shorter
retention by upgrading. Enabling `ORK_CLEANUP_ENABLED` for the first time
moves retirement from whenever the (now-hidden) backstop TTL would have fired
to the moment each run actually crosses its own namespace's retention —
sooner, by up to the enforcement margin, and namespace-aware for the first
time.

### Secrets on a shared pool

A task names a secret by its `source:` (`env:NAME` or `file:/path`), and the
Orker that runs it resolves that name from its own environment or
filesystem. On a pool several namespaces share, bound those names in two
places; a source must pass **both**:

- **The operator's floor, per Orker:** `orker.secret_allow` /
  `ORK_ORKER_SECRET_ALLOW`, comma-separated `scheme:pattern` entries such as
  `env:SANDBOX_*,file:/etc/orkstack/secrets/sandbox/*`. It is checked by the
  process that reads the value, at dispatch, before the image is pulled; a
  source outside it fails the task `SECRET_NOT_PERMITTED` (a
  `ContractViolation`, never retried). Empty resolves every source of an
  allowed scheme, as before this setting existed. A malformed entry, or one
  naming a scheme outside `orker.secret_schemes`, stops the Orker at boot.
- **The tenant's list, per namespace:** `orkctl namespace update NAME
  --allow-secret 'env:SANDBOX_*'` (or Admin → Namespaces). It is enforced by
  the engine at validate and apply, as an issue on
  `tasks[i].secrets[j].source`, and again at every submit — `run submit`,
  `run again`, the UI, a trigger invoke — so a definition applied before the
  list existed is refused at its next submit. It is never retroactive: a run
  already accepted finishes on the definition it pinned; stop one with
  `orkctl run cancel`. Empty means unrestricted.

A namespace can therefore only ever narrow what its Orkers permit. The grammar
is deliberately small: a literal scheme, and `*` as the one wildcard, which
never crosses `/`; `?`, `[…]` and `\` are ordinary characters. A `file:`
source with a `..`, `.` or empty segment is refused outright rather than
normalized, so `file:/etc/orkstack/secrets/sandbox/../payments/db` never sits
inside `file:/etc/orkstack/secrets/sandbox/*`. The policy bounds the NAME a
tenant may write, not the filesystem behind it: a symlink placed under a
tenant's directory is a grant the operator made, and the Orker reads the path
as it always has. Nothing here stores, logs or compares a secret value;
refusals carry the source string only.

During a rolling upgrade the namespace list is enforced only by engine
replicas already at the release that added it; the Orker floor is unaffected
by the rollout. An older `orkctl` round-tripping a namespace through `update`
drops the list — run the `orkctl` of the release.

---

## 6. Replay safety across versions

A pinned run finishing on an old build is Temporal replaying an old history
against that build's code — fine by construction. The risk is the reverse:
current code meeting a history an older build wrote. That happens on a
rollback, on a versioning override, after any worker restart, and — the one
case a rollout cannot avoid — when a run that started unversioned upgrades onto
the first version (§3.5).

`internal/engine/replay_test.go` and `internal/engine/replay_payload_test.go`
guard it, together. Both read the same **committed** history
(`internal/engine/testdata/history/run-succeeded-v1.json`), recorded from a real
multi-task run by a versioned worker, and both run in `make test` with no
cluster.

**What the fixture is, exactly.** It was recorded by the commit that added it,
so on that day it was current code replaying its own command stream. It is a
frozen stream that AGES into a cross-version guard: every commit after that one
is measured against a build genuinely older than itself. It is not a recording
of any build you are running, and it cannot vouch for one (§3.5).

They catch different halves, and neither is enough alone:

| guard | what it compares | what it catches |
|---|---|---|
| `TestRunWorkflow_ReplaysCommittedV1History` | Temporal's own replayer: command types and their order | a reordered, added or deleted activity; a changed scheduling order |
| `TestCommittedHistory_CatchesAnEmitInsertedIntoTheStream` | the same replayer, run against a body with one extra emit — it must FAIL | the guard above going blind: it is the proof the fixture can reject an inserted command, on the day it is recorded rather than one change later |
| `TestGoldenHistory_ActivityPayloadsMatchWhatCurrentCodeSends` | the payloads, by re-executing the fixture's own input against the recorded ones | a changed `task.ExecuteTaskInput`, a renamed event type, a reworded message, a changed `Run` projection |

The replayer never looks inside a payload: adding a field to
`task.ExecuteTaskInput` replays perfectly clean, and an Orker of the recorded
build would silently drop it. That is why the third guard exists.

If any of them fails, current code no longer produces what the recorded build
did. Either restore it (`workflow.GetVersion` for a deliberate command-stream
change), or — if the contract genuinely changed and no old run can still be in
flight — re-record the fixture and say so in the commit:

```bash
ORK_RECORD_GOLDEN_HISTORY=1 ORK_REQUIRE_INFRA=1 go test -tags=integration -p 1 ./internal/engine/ \
  -run TestIntegration_RecordGoldenRunHistory
```

Re-recording resets the guard to today. Do it deliberately, never to make a
red test green.

---

## 7. Telemetry

**Off by default.** With no `OTEL_*`, `ORK_ADMIN_ADDR`, or `ORK_LOG_*`
variable set, `ork` and `orker` behave byte-identically to a deployment that
has never heard of this section: text logs on stderr, no extra listener, no
exporters, nothing registered with the Temporal client. Turning telemetry on
is opt-in, one variable at a time, and never changes what a run does — an
exporter that cannot reach its collector is logged and counted, never waited
for, and a record that cannot be queued is dropped rather than blocking the
process that produced it. Telemetry cannot fail a task or a run.

**One mechanism.** OpenTelemetry plus OTLP and a couple of standard scrape
endpoints is the only way a signal leaves `ork` or `orker` — there is no
per-vendor code, and no other exporter to pick between. The exporter is
selected by the standard `OTEL_*` environment (`OTEL_METRICS_EXPORTER`,
`OTEL_TRACES_EXPORTER`, `OTEL_LOGS_EXPORTER`), honored verbatim, and where it
goes is the standard `OTEL_EXPORTER_OTLP_*` family — this document adds no
variables of its own for "which vendor," only for the handful of switches
OpenTelemetry has no opinion on (§1's table, above).

**The admin listener is separate from the API, on purpose.**
`ORK_ADMIN_ADDR` — empty by default, so nothing opens — serves `/healthz`,
`/readyz`, `/metrics` (only once the `prometheus` metrics exporter is asked
for) and `/debug/pprof/` (only with `ORK_PPROF_ENABLED=true`). It is never
`ORK_HTTP_ADDR`: the API port answers client traffic, the admin port answers
an operator's scraper, and conflating them would mean a scrape config error
or a slow `/debug/pprof/` request competing with a run's own requests. Bind it
to a private network — a metrics scraper and a pprof profiler are not
something to expose publicly, and `ORK_PPROF_ENABLED`'s own default reflects
that an Orker's heap holds resolved secret bytes. Suggested addresses for a
single-host deployment: `:9464` for `ork`, `:9465` for `orker`, so one host can
run both without a collision.

### Image supply-chain policy

By default any `uses:` a document names is pulled and run: the engine pins it
to a digest at apply, which answers "did the image change under us?" and
never "should this image run here at all?". Two halves bound that, and an
image must pass both:

- **The deployment floor:** `orkctl admin image-policy set` (or Admin →
  Image policy), one document at `metadata/settings/image-policy.json`.
  `--require-digest` refuses a `uses:` written as a mutable tag — a catalog
  name (`uses: shell`) is exempt, because the catalog is the operator's own
  configuration; write the catalog's references with digests if that matters.
  `--allowed-registry PREFIX` (repeatable) bounds the resolved
  `{registry}/{repository}` to literal prefixes matched on `/` boundaries
  (`ghcr.io/acme/` matches `ghcr.io/acme/migrate`, never
  `ghcr.io/acmeevil/…`); a list that would exclude a catalog activity is
  refused. `--signing-key NAME=cosign.pub` registers a cosign PUBLIC key by
  name (ECDSA P-256; OrkStack never holds a private key). Empty — the state of
  every deployment until an operator writes one — permits everything, as
  before.
- **The tenant's tightening, per namespace:** `orkctl namespace update NAME
  --require-signed-by cosign:KEY` requires every image executed in the
  namespace to carry a valid cosign signature by that key. A namespace can
  tighten the floor and has no field with which to loosen it. A key a
  namespace names cannot be removed from the floor until the namespace is
  cleared (`--clear-require-signed-by`).

The policy is checked at three doors: at validate and apply (an issue on
`tasks[i].uses`), at every submit — `run submit`, `run again`, the UI, a
trigger invoke — before a run id is minted (`failed_precondition`, so a
definition stored before the policy tightened is refused at its next
submit), and by the Orker before it builds a workspace or pulls, against the
policy the run pinned at submit. A run keeps the policy **and the key** it
was submitted under: tightening the floor, moving a namespace's signer or
rotating a key never reaches a run already accepted; stop one with `orkctl
run cancel`. A committed attempt is adopted without being judged again.

The Orker verifies the signature itself with its own registry client:
`orker.registry_auth_file` / `ORK_ORKER_REGISTRY_AUTH_FILE` is a docker
`config.json` used only for the signature manifest and blob GETs (empty is
anonymous, which public registries allow); the engine uses
`ORK_REGISTRY_AUTH_FILE` for the same GETs at apply. A registry that cannot
be asked is never a pass: `unavailable` at apply and submit, and a retried
`IMAGE_SIGNATURE_UNAVAILABLE` at the Orker. A missing signature is
`IMAGE_NOT_SIGNED`, a signature by another key or over another digest
`IMAGE_SIGNATURE_INVALID`, an image outside the floor `IMAGE_NOT_PERMITTED`
— all three `ContractViolation`, never retried.

Signing an image, with the `cosign` CLI and default settings:

```bash
cosign generate-key-pair                                  # cosign.key, cosign.pub
cosign sign --key cosign.key ghcr.io/acme/migrate@sha256:<digest>
orkctl admin image-policy set --signing-key release=cosign.pub
orkctl namespace update prod-payments --require-signed-by cosign:release
```

To rotate a key without a gap, add the new key under a new name, re-sign,
move each namespace's `--require-signed-by`, then `--remove-signing-key` the
old name. During a rolling upgrade the policy is enforced only by engine
replicas at the release that added it, and a run submitted through an older
replica carries no policy for the Orker to check; the window is the rollout.
Every image digest that ever ran is already recorded per attempt
(`orkctl run attempts`, the `TaskAttempt.image` of each attempt record) —
that is the audit read, and nothing new is written for it.

### Recipes

Point `OTEL_EXPORTER_OTLP_ENDPOINT` (and, where noted, the per-signal
`OTEL_EXPORTER_OTLP_{LOGS,METRICS,TRACES}_ENDPOINT`) at whatever you already
run. `OTEL_EXPORTER_OTLP_PROTOCOL` is `http/protobuf` — this is the only
protocol the SDK here is configured for.

- **Loki ≥ 3**, native OTLP ingestion: `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=http://loki:3100/otlp`.
- **Prometheus ≥ 2.47**, started with `--web.enable-otlp-receiver`:
  `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://prometheus:9090/api/v1/otlp`
  and `OTEL_METRICS_EXPORTER=otlp` — or skip OTLP for metrics entirely and
  have Prometheus scrape `ORK_ADMIN_ADDR`'s `/metrics` with
  `OTEL_METRICS_EXPORTER=prometheus`.
- **Tempo or Jaeger**, OTLP/http on 4318: point
  `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` there and set
  `OTEL_TRACES_EXPORTER=otlp`.
- **Datadog Agent**, OTLP ingest on 4318: point `OTEL_EXPORTER_OTLP_ENDPOINT`
  at the agent; no Datadog-specific variable exists, or is needed.
- **An OTel Collector in front of any of the above**: point every endpoint at
  the Collector instead and let its own pipelines fan out from there — this is
  what `deploy/docker-compose.yml`'s `observability` profile does
  (`deploy/observability/README.md`).
- **Pyroscope, Grafana Alloy, or the Datadog Agent**, scraping profiles:
  `ORK_PPROF_ENABLED=true` and point the scraper at
  `ORK_ADMIN_ADDR/debug/pprof/` — see "Profiles", below.
- **A second destination for one namespace's runs** — a tenant's own Datadog
  account, a dedicated Loki/Tempo pair — without touching the `OTEL_*`
  environment every other namespace uses: `ORK_TELEMETRY_SINKS` plus that
  namespace's `--telemetry-sink`, see "Named sinks" and "Per-namespace policy",
  below.

A local, disposable stack for trying all of this — a Collector, Loki, Tempo,
Prometheus and Grafana — is the `observability` compose profile:

```bash
make infra-up-observability
```

It never starts on its own. Point `ork`/`orker` at it with a config file
(§1) — this is the Grafana profile in full, one file passed to both
binaries with `--config`:

```yaml
telemetry:
  log_format: json
  admin_addr: {ork: ":9464", orker: ":9465"}
  exporters:
    logs: otlp
    metrics: [prometheus, otlp]
    traces: otlp
  otlp:
    endpoint: http://localhost:4318
    protocol: http/protobuf
  sinks:
    laptop-collector:
      endpoint: http://localhost:4318
      headers:
        X-Scope-OrgID: {source: "env:LOKI_ORG_ID"}
orker:
  task_log_export: true   # ship task stdout/stderr as log records too
```

`service_name` needs no entry here: it defaults to `ork` / `orker` per
binary (§1), which is exactly what this profile wants. `deploy/observability/README.md`
has the file to save and the `--telemetry-sink laptop-collector` walkthrough
that goes with it — or, with environment variables instead of a file, see
the recipe there under "or, with environment variables". `make
proof-observability` is the evidence gate that proves the whole path —
Collector, Loki, Tempo, Prometheus — actually carries a real run's signals,
not just that the containers are up.

### Traces

`OTEL_TRACES_EXPORTER=otlp` turns on one trace that runs end to end through
both binaries: the engine starts a span for the run's own workflow processing,
`ork.task.attempt` is the Orker's span for one activity attempt, and
`ork.task.phase` (attribute `ork.phase`) is its child for one phase of that
attempt (image resolution, workspace setup, container run, artifact upload,
…). The parent-child link between the engine's span and the Orker's is carried
through Temporal's own headers by the tracing interceptor
(`internal/telemetry.TemporalInterceptors`), not by any OrkStack code stitching
the two together — see ARCHITECTURE.md, "Telemetry". Workflow code itself never
imports OpenTelemetry (replay must stay a pure function of history), so the
span the engine emits is started from the activity boundary, never from inside
`RunWorkflow`.

`OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` are the standard
OpenTelemetry sampler selector and argument, honored verbatim by the SDK — set
`traceidratio` with an argument below `1` to sample a fraction of runs on a
busy deployment. `OTEL_PROPAGATORS` selects the context propagation format;
the default (`tracecontext,baggage`) is what every recipe above assumes.

Every process log line this package emits carries `trace_id` (and `span_id`)
when one is live on the context it was logged from — the engine's structured
"run event" line (§ "Lifecycle metrics", below) included — so a trace found in
Tempo or Jaeger and a log line found in Loki are one click apart in either
direction: search the trace ID as a LogQL label, or read `trace_id` off a log
line and paste it into Tempo's search.

A task's `task.json` carries an optional `trace` key —
`{"traceparent": "...", "tracestate": "..."}` in the same shape the W3C
`traceparent`/`tracestate` HTTP headers use — so a workload that wants its own
work folded into the same trace can propagate it. This is informational, not
part of the OCI Activity Contract's ABI (§3.2): it is present only when tracing
is on and a real span exists, absent otherwise, and no image needs to read it
to run correctly. A workload that does not recognize the key ignores it.

### Profiles

`ORK_PPROF_ENABLED=true` mounts Go's `net/http/pprof` on the admin listener
(`ORK_ADMIN_ADDR`) — off by default because an Orker's heap holds resolved
secret bytes, and a profile endpoint is not something to expose beside it
without deciding to. With it on, point a continuous profiler at
`ORK_ADMIN_ADDR/debug/pprof/`:

- **Grafana Alloy**, with a `pyroscope.scrape` component:

  ```river
  pyroscope.scrape "ork" {
    targets = [{"__address__" = "ork:9464", "service_name" = "ork"}]
    forward_to = [pyroscope.write.default.receiver]
    profiling_config {
      profile.process_cpu { enabled = true }
      profile.memory { enabled = true }
      profile.goroutine { enabled = true }
    }
  }
  ```

- **Pyroscope's own agent-less scrape**, pointed directly at the same
  `/debug/pprof/` path with `application_name: ork` (or `orker`) and the
  matching `spy_name: gospy`.
- **Datadog Agent**, continuous profiler, `PYROSCOPE`-style pull is not what
  Datadog uses — instead enable `DD_PROFILING_ENABLED` in a small sidecar that
  scrapes `/debug/pprof/profile` and `/debug/pprof/heap` and pushes them, or
  point Datadog's own Go continuous profiler integration at the same paths per
  its documented pprof-scrape mode.

None of this needs any code change: `net/http/pprof`'s handlers are the
standard library's, and every scraper above speaks pprof's wire format
natively.

### Metrics

`ork_build_info{service.version,ork.deployment,ork.build_id,...}` is a
constant `1`, so its labels are the identity of whatever answered the scrape —
the same idea as `orkctl admin info` (§5), read as a time series instead of a
CLI call. `ork_orker_task_log_records_total{outcome=emitted|truncated}` and
`ork_orker_task_log_exports_total{outcome=ok|error}` account for the task-log
export path below. The Temporal SDK's own `temporal_*` metrics — activity and
workflow task latency, poll counts, failures — arrive via
`client.Options.MetricsHandler`, and Go's runtime and process collectors ride
along on the same `/metrics`.

`ork.audit.records{action,outcome}` (engine) counts every control-plane
audit write, once per audited write RPC: `outcome` is `written`, or `dropped`
when the audit store refused the write (the RPC itself still answered as it
would have — see §5 "Attributing control-plane writes"). A rising `dropped`
rate is an unwritable `metadata/audit/` prefix.

### Lifecycle metrics

These are the run and task metrics a dashboard alerts on, distinct from the
process-level metrics above: they are recorded from the engine's own lifecycle
events and the Orker's own attempt spans, and — when a namespace's telemetry
policy names one — they can land on a different sink than the process default
(see "Named sinks" and "Per-namespace policy", below).

| Metric | Labels | Emitted by |
|---|---|---|
| `ork.runs` | `status` | engine, once per terminal run (succeeded/failed/canceled) |
| `ork.run.duration` | `status` | engine, same event, `finished_at - started_at` |
| `ork.tasks` | `status`, `failure_category` | engine, once per terminal task (succeeded/failed) |
| `ork.task.duration` | `status` | engine, same event, `finished_at - scheduled_at` (dispatch to finish; `started_at` on a run recorded before `scheduled_at` existed) |
| `ork.task.attempts` | `status` | engine, same event: the attempt number the task finished on |
| `ork.orker.tasks` | `outcome`, `ork.failure.reason` | Orker, once per attempt (`outcome`: `succeeded`, `failed`, `canceled`, or `adopted` for a retry that found an existing durable commit and ran no container) |
| `ork.orker.task.duration` | `outcome` | Orker, same event |
| `ork.orker.task_phase.duration` | `ork.phase` | Orker, once per phase of an attempt |
| `ork.orker.commit_adopted` | — | Orker, once per adopted commit |
| `ork.runs.refused` | `ork.namespace`, `ork.refusal.reason` | engine, once per submission the namespace's hold or `max_running` refused (`namespace_held`, `max_running_reached`); not a run, not counted on `ork.runs` |

(The rows above omit `ork.namespace` even though every run and task metric
carries it; `ork.runs.refused` names it explicitly.) Under `prometheus` the
refusal counter renders as
`ork_runs_refused_total{ork_namespace="payments",ork_refusal_reason="namespace_held"}`,
by the same name derivation as every other counter here. Under high
cardinality it also carries `ork.orkflow.name` and the namespace's
`ork.ns.*` / orkflow's `ork.label.*` selection attributes — never
`ork.run_id`, because a refused submission has none.

`ORK_TELEMETRY_HIGH_CARDINALITY=true` additionally puts `ork.run_id` and
`ork.task_id` (and, on the engine's, `ork.orkflow.name`/`.version`) on the ones
above that know them — see "High-cardinality labels".

**At-least-once, not exactly-once.** A run or task's terminal metric is
recorded from the projection that observes its lifecycle event; a replica that
crashes and is retried, or an event redelivered after a partial failure, can
record the same terminal transition more than once. Treat these as you would
any other at-least-once counter: `rate()` and `increase()` over a window are
stable under an occasional duplicate, a raw total compared across two points in
time is not. Durations are likewise taken from the run or task record's own
`started_at`/`finished_at` snapshot timestamps, not measured by a timer live in
the process that recorded them, so a duration can be recorded once, late, well
after the interval it describes actually elapsed — a metrics backend that
prefers "arrived recently" over "happened recently" will place it there.

### Named sinks

`ORK_TELEMETRY_SINKS` declares every OTLP destination this deployment has,
once, so a namespace can later pick one by name instead of every deployment
carrying its own copy of the `OTEL_*` environment. Unset, or set to a document
with no `default:` entry, the `OTEL_*` environment IS the default sink and
nothing here changes anything above.

```yaml
sinks:
  default:
    endpoint: http://otel-collector.internal:4318
  datadog:
    endpoint: https://otlp.datadoghq.com
    headers:
      DD-API-KEY:
        source: env:DD_API_KEY
  loki-tempo:
    logs_endpoint: http://loki.internal:3100/otlp
    traces_endpoint: http://tempo.internal:4318/v1/traces
    metrics_endpoint: http://otel-collector.internal:4318/v1/metrics
```

A sink name is a slug (`^[a-z][a-z0-9_-]{0,63}$`) — it becomes a label, a
selector a namespace names, and eventually a file path segment, none of which
forgive arbitrary punctuation. `endpoint` is the base URL used for any signal
that has no more specific `logs_endpoint`/`traces_endpoint`/`metrics_endpoint`;
at least one of the two must resolve for every signal this deployment has
switched on for that sink, or boot refuses with the sink's name and the
missing signal. A header's value is either a literal `value` or a `source` of
`env:NAME` or `file:/path` — the same two schemes activity `secrets:` already
use (`docs/DSL.md`) — and exactly one of the two, never both and never
neither.

**Secrets never appear in this document, in a boot log line, or in an error.**
`SinkTable.String()`, used everywhere a sink table is logged, names a sink's
endpoints and its header NAMES only, never a header's value or its source: a
source names an environment variable or a file path, which is close enough to
a value that printing it defeats the point. An unresolvable header — the named
environment variable unset, the named file unreadable — refuses boot, naming
the sink and the header: a collector silently receiving a blank `DD-API-KEY`
and rejecting every export is a harder outage to diagnose than a process that
never started.

**Named sinks never open a listener.** `/metrics`, and the process's own logs
and the Temporal SDK's own metrics, always stay on the default sink — a named
sink only ever receives what a namespace's runs and tasks route to it (task
spans, task-scoped metrics, and, with task-log export on, that namespace's
task log lines). Set `ORK_TELEMETRY_SINKS` identically on `ork` and `orker`:
a namespace's policy names a sink by the same string on both binaries, and a
name only `orker` (or only `ork`) knows resolves nothing on the other.

A few more behaviors worth stating precisely:

- A named sink carries exactly the signals it declares an endpoint for — a
  sink with no `logs_endpoint` (and no `endpoint` to fall back to) simply
  exports no logs for the namespaces that select it, the same way it exports
  no traces or metrics without those endpoints. A sink with *no* endpoint at
  all (`endpoint`, `logs_endpoint`, `traces_endpoint`, and
  `metrics_endpoint` all unset) is refused at boot, naming the sink: a named
  destination that could never export anything is a configuration mistake,
  not a signal-by-signal choice.
- A sink is built, and can be selected, whenever `ORK_TELEMETRY_SINKS`
  declares it — even when no `OTEL_*_EXPORTER` environment variable is set
  at all. The two are independent: `OTEL_*` controls the deployment's
  *default* sink, and a named sink's own table entry is what builds it. A
  namespace can therefore enable task logs on a named sink alone, with no
  `OTEL_*` variable set anywhere in the deployment.
- The `default:` entry in `ORK_TELEMETRY_SINKS`, when present, merges
  `OTEL_EXPORTER_OTLP_HEADERS` underneath its own headers rather than
  replacing it: a header the `default:` entry sets wins on that key, and
  every other header from `OTEL_EXPORTER_OTLP_HEADERS` still applies.
- A namespace whose stored `telemetry.sink` names a sink no longer declared
  in `ORK_TELEMETRY_SINKS` (the table shrank since the namespace was
  configured) falls back to the default sink rather than failing the run.
  The engine logs a warning at boot naming every such namespace, so the gap
  is visible before it is silently swallowed one run at a time. The Orker
  warns once per unknown sink name it encounters (not once per run) and
  counts it on `ork.telemetry.sink_unknown`, so the drift is visible in
  metrics even between the engine's own boot-time checks.

### Per-namespace policy

A namespace's `TelemetryPolicy` — set through `orkctl namespace create` /
`update` — chooses **where among the deployment's declared sinks**, never a
destination or a credential of its own:

```bash
orkctl namespace update payments \
  --telemetry-sink datadog \
  --task-logs enabled \
  --telemetry-attribute team=payments \
  --telemetry-attribute cost-center=eng-42
```

- `--telemetry-sink` names a sink from `ORK_TELEMETRY_SINKS` by name; unset or
  empty means the deployment's default sink.
- `--task-logs` is `enabled`, `disabled`, or `inherit` (the default): it
  overrides the Orker's own `ORK_ORKER_TASK_LOG_EXPORT` for this namespace's
  runs either way, or leaves the Orker's default in force.
- `--telemetry-attribute key=value` (repeatable; a later `update` replaces the
  whole set) is stamped as `ork.ns.<key>` on every signal of this namespace's
  runs. At most 16 entries, keys `^[a-z][a-z0-9_.-]{0,63}$`, values at most 256
  bytes and free of control characters — this is operator/admin input
  rendered verbatim by `orkctl namespace get` and the UI's namespace page.

**Precedence.** A run's sink and its attributes are decided once, at submit,
from the namespace's policy — there is no per-run or per-orkflow override of
either. An orkflow's own `labels:` (docs/DSL.md) ride along too, but only as
attributes: at most 32 of them are stamped `ork.label.<key>` beside the
namespace's `ork.ns.<key>` (see docs/DSL.md for the export boundary's
sanitizing and 32-entry cap), and an orkflow author can add signal but never
choose a sink.

The task-log switch is the one exception to "decided once, at submit": with
`task_logs: inherit` (the default — the namespace set no override), the
*effective* switch for a given attempt is whichever Orker executes that
attempt's own `ORK_ORKER_TASK_LOG_EXPORT`, read when the Orker picks up the
task, not the namespace's policy at submit time. `enabled` and `disabled`
still override every Orker's own setting outright; only `inherit` defers to
the executing Orker. This means two attempts of the same task, retried onto
two Orkers configured differently, can export task logs differently even
though the namespace's policy never changed — and a rolling deploy that
changes an Orker's `ORK_ORKER_TASK_LOG_EXPORT` mid-run can do the same across
retries of one run. Set `ORK_ORKER_TASK_LOG_EXPORT` identically on every
Orker of a pool to avoid this: a namespace relying on `inherit` is relying on
the pool's Orkers agreeing with each other, not on any one of them.

`orkctl namespace get` (and the namespace page in the UI) shows the resolved
policy — sink, task-log override, attributes — exactly as stored. `inherit`
(the `task-logs` default, and an unset `telemetry-sink`) means literally that:
this namespace decided nothing, and the Orker's own default or the deployment
default sink applies, the same shape `run_retention` already uses for "this
namespace set no value of its own."

**Validation errors an admin will see:** an unset or blank sink name resolves
to default silently, but a sink name that is not in the deployment's own table
is refused (`orkctl namespace create/update` reports it, naming the sink and,
where the CLI can reach it, the sinks that do exist); an attribute key outside
`^[a-z][a-z0-9_.-]{0,63}$`, a value over 256 bytes or containing a control
character, or a key starting `ork.`, `service.`, or `host.` (reserved for the
platform's own attributes) is likewise refused rather than silently dropped or
truncated.

A namespace can only ever *select* a sink an operator already declared, never
name an endpoint or a credential of its own — the same reason `pool:` in an
orkflow selects among Orkers the operator has running rather than naming a
host: a tenant's own document is untrusted input, and untrusted input that
could steer telemetry at an arbitrary destination is untrusted input that
could exfiltrate the namespace's own attributes and task output to it.

### High-cardinality labels

`ORK_TELEMETRY_HIGH_CARDINALITY=true` adds `ork.run_id`, `ork.task_id`,
`ork.orkflow.name` and `ork.orkflow.version` to every `ork_*` metric that
knows them. This is a deliberate trade-off, not a strict improvement: every
distinct run becomes its own time series, which is fine for a small
deployment or a metrics backend with short retention, and expensive at scale —
a busy deployment can turn one counter into millions of series. Off by
default for that reason; turn it on when you want per-run metrics and have
priced what your backend charges for the cardinality.

### Task logs

Task output is always durable, whether or not any of this is configured:
every stdout/stderr line an activity container writes keeps flowing to object
storage as numbered chunks and out through the streaming API and UI (§1,
"Live task logs"), exactly as before this document's telemetry variables
existed. `ORK_ORKER_TASK_LOG_EXPORT=true` adds a second, independent copy —
each line is also emitted as an OTel log record (requires
`OTEL_LOGS_EXPORTER=otlp`), carrying `ork.stream` (`stdout`, `stderr`, or
`ork` for the Orker's own framing), `ork.run_id`, `ork.task_id`,
`ork.attempt`, `ork.namespace`, `ork.orkflow.name`/`.version`/`.digest`, and
`ork.pool`. It is off by default because its volume is exactly the workloads'
own output — a chatty task doubles its log traffic the moment this is turned
on.

Nothing here touches the OCI Activity Contract. An activity image needs no
agent, sidecar, SDK, environment variable, or file to have its logs exported
this way — the Orker does the whole thing from outside
(`docs/OCI_ACTIVITY_CONTRACT.md`, §3.6).
