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
reports `Drained`. `orkctl release` (§3) does both, and refuses what would
break them.

The topology and rollout claims below are held by integration tests against
the built binaries (`cmd/orkctl/tworeplica_read_integration_test.go`,
`internal/engine/versioning_*_integration_test.go`).

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
                              one S3-compatible bucket
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
- **Readiness.** With `ORK_ADMIN_ADDR` set, `/healthz` answers 200 once the
  process is up and `/readyz` 200 once it serves (§7) — the process's own
  claim. The authoritative probe is a cheap unary call that touches the
  bucket, `orkctl --server … orkflow list` (`OrkflowService.ListOrkflows`);
  `scripts/stack.sh` gates on exactly that.
- **The UI reaches the engine at two addresses.** Its server proxies every
  Connect call (same origin for the browser, so no CORS) to `ENGINE_API_URL`
  (default `http://127.0.0.1:8080`); the browser opens the run and task-log
  WebSockets directly to the engine at `PUBLIC_ENGINE_WS_URL` (default
  `ws(s)://<page host>:8080`). Set both whenever the engine is not on the UI
  host's `:8080`.
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

  The window is 90 seconds of poller freshness, applied to each poller
  separately: a worker that stopped polling more than 90 seconds ago is not
  counted, however busy its neighbours are. The window is wider than a live
  worker's polling cycle on purpose: Temporal stamps a poller when its long
  poll starts, an idle worker's poll lasts up to the cluster's long-poll
  interval (a minute by default, and never past the SDK's 70-second poll
  deadline), so a healthy Orker's record is routinely a little over 60 seconds
  old just before it polls again. Temporal remembers a poller for
  minutes after its worker died, so a pool you scaled from three Orkers to one
  would otherwise report three for as long as the survivor kept the newest
  record fresh. Two workers of one release can also be described twice while a
  version is being promoted, so the count is per worker identity, not per
  record.

  The residue is bounded and deliberate: inside those 90 seconds a stale record
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
  every pool together; `orkctl release status` (§3 step 2) names every pool in
  use that a release has no Orker for, and `promote` refuses until it has.
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
0, or 1 naming the first error. `--check` never starts anything. The other
flag that starts nothing is `--healthcheck`, which asks a running process
whether it is ready (§5, "Health checks").

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
| `activities:` | `ORK_ACTIVITY_CATALOG` | built-in `shell`/`http`/`file`/`json`/`git`/`template` → `ghcr.io/orkstack/activity-*:0.1` | `ork` | the activity catalog: a YAML `activities:` map from catalog name to registry reference, resolved to a digest at apply time. `make up` points it at the local registry |
| `engine.registry_auth_file` | `ORK_REGISTRY_AUTH_FILE` | *(empty)* | `ork` | a docker `config.json` used only for manifest GETs during apply-time resolution; grants no push and no execution |
| `orker.id` | `ORK_ORKER_ID` | `hostname-<hash of the work dir>` | `orker` | this Orker's identity: labels its containers and keys its startup reaper, so two Orkers must never share one — across the whole deployment, not only on one host: it is also the Orker's address for `orkctl orker` (1–200 bytes, no whitespace or control characters; refused at boot otherwise) |
| `orker.image_layout` | `ORK_ORKER_IMAGE_LAYOUT` | `/var/lib/orkstack/images` | `orker` | directory of OCI image layouts an Orker may serve a pinned digest from when a pull fails. An absent directory is not an error — the fallback matters only to an air-gapped or rate-limited Orker |
| `orker.docker.bin` | `ORK_DOCKER_BIN` | `docker` on `PATH` | `orker` | the container CLI |
| `orker.docker.pull_timeout` | `ORK_DOCKER_PULL_TIMEOUT` | `5m` | `orker` | how long an image pull may take |
| `orker.docker.bind_probe_timeout` | `ORK_DOCKER_BIND_PROBE_TIMEOUT` | `2m` | `orker` | how long the boot bind probe waits for the daemon at each step that waits on it: whether the probe's image is local, and each of two attempts to run its container. An attempt that overruns, or that the daemon ends with a deadline error of its own, is tried once more, and a daemon that is still too slow refuses boot as `BIND_PROBE_TIMED_OUT`; a refusal in the daemon's own words, or a container that exits non-zero, is still `WORKDIR_NOT_BINDABLE`. A daemon that never answers holds boot for up to three times this plus a minute (7 minutes at the default), plus `orker.docker.pull_timeout` when the probe image has to be pulled; [Health checks](#health-checks) says what that means for a supervisor |
| `orker.resources.max_cpu`, `orker.resources.max_memory` | `ORK_ORKER_MAX_CPU`, `ORK_ORKER_MAX_MEMORY` | `8`, `16Gi` | `orker` | caps on what a task may request; a request above the cap is refused, never clamped |
| `orker.resources.default_cpu`, `orker.resources.default_memory` | `ORK_ORKER_DEFAULT_CPU`, `ORK_ORKER_DEFAULT_MEMORY` | `1`, `1Gi` | `orker` | what a task that requests nothing gets |
| `orker.resources.pids_limit` | `ORK_ORKER_PIDS_LIMIT` | `512` | `orker` | pids limit applied to every container |
| `orker.resources.tmpfs_bytes` | `ORK_ORKER_TMPFS_BYTES` | `64 MiB` | `orker` | size of the container's `/orkstack/tmp` tmpfs |
| `orker.resources.force_user` | `ORK_ORKER_FORCE_USER` | `false` | `orker` | `true` runs every container as the Orker's own user, as if each task had set `run_as_orker_user: true` — the readability escape hatch for hosts where the image's own USER leaves output unreadable |
| `orker.secret_schemes` | `ORK_ORKER_SECRET_SCHEMES` | `env,file` | `orker` | comma-separated secret sources this Orker resolves for a task's `secrets:` |
| `orker.secret_allow` | `ORK_ORKER_SECRET_ALLOW` | *(empty)* | `orker` | comma-separated `scheme:pattern` floor of the `secrets:` sources this Orker resolves, e.g. `env:SANDBOX_*,file:/etc/orkstack/secrets/*`; `*` is the one wildcard and does not cross `/`. Empty resolves every source of an allowed scheme. A namespace's `allowed_secrets` can only narrow this, never widen it (§5, "Secrets on a shared pool"). Refused at boot if an entry is malformed or names a scheme not in `orker.secret_schemes` |
| `orker.registry_auth_file` | `ORK_ORKER_REGISTRY_AUTH_FILE` | *(empty)* | `orker` | a docker `config.json` used only for the manifest and blob GETs that verify a cosign signature when a namespace sets `require_signed_by` (§5, "Image supply-chain policy"); grants no push, never reaches a container. Empty verifies anonymously, which public registries allow |
| `orker.env_allowlist` | `ORK_ORKER_ENV_ALLOWLIST` | *(empty)* | `orker` | comma-separated names `{{ env "NAME" }}` may read in a task's `with:` — resolved on the Orker, so the value is the pool's |
| `orker.workdir` | `ORK_ORKER_WORKDIR` | temp dir | `orker` | root under which each task attempt gets its own working directory. **It must be bind-mountable by the docker daemon**, since every task's `/orkstack` workspace is bind-mounted from it. The default is `os.TempDir()` — `/var/folders/...` on macOS — which a VM daemon (colima, Lima, Rancher, Docker Desktop) does not share, so a container task fails with `bind source path does not exist` on a path that plainly exists. `make up` sets it to `.data/work`, under `$HOME`, which every one of those VMs shares; it also probes the daemon at startup and warns if the path is unreachable. It also holds two process-scoped scratch directories that boot empties: `ork-spool/`, where an upload whose source cannot be re-read is staged, and `ork-stage/`, the staging cache — attempts staging the same input digest at the same time (the items of a fan-out over one upstream) share one verified download, hard-linked into each attempt's read-only input tree and deleted when the last of them ends. Where the root cannot hard-link, each attempt downloads its own copy, as before |
| `orker.log.flush_bytes` | `ORK_ORKER_LOG_FLUSH_BYTES` | `64 KiB` | `orker` | live task logs: buffered bytes that trip a flush. Bigger batches a chatty task harder and costs less per object |
| `orker.log.flush_interval` | `ORK_ORKER_LOG_FLUSH_INTERVAL` | `500ms` | `orker` | live task logs: the longest a line waits before it is stored. This is the floor on how late a follower can see a slow task's output |
| `orker.log.max_bytes` | `ORK_ORKER_LOG_MAX_BYTES` | `16 MiB` | `orker` | live task logs: the cap on ONE task attempt. Past it the Orker stores a truncation notice and nothing more |
| `orker.heartbeat_interval` | `ORK_ORKER_HEARTBEAT_INTERVAL` | `5s` | `orker` | how often a running task's heartbeat actually reaches Temporal. Two consequences, both real: it is the longest a **cancellation** waits before the Orker acts on it — Temporal delivers cancellation only in a heartbeat's response, so a failing run's still-running siblings keep burning resources for up to this long — and it is the longest **Temporal frontend outage** a running container survives, since the SDK gives up on an activity whose heartbeat RPC keeps failing for a whole interval. Must be between `1s` and `10s` (a third of the 30 s heartbeat timeout); anything else is refused at boot rather than clamped. Lower it towards `2s` where Temporal is local or highly available; raise it where the frontend is flaky |
| `orker.max_concurrent` | `ORK_ORKER_MAX_CONCURRENT` | `16` | `orker` | how many tasks this Orker runs at once — the size of its task worker's activity slots, and the **Max concurrent** Admin → Activities reports. A whole number, 1 or more; anything else is refused at boot. The default fits a 16-CPU, 64 GiB host running default-sized tasks; size it to the host — see "Sizing an Orker's task ceiling" below |
| `orker.pins.max_concurrent` | `ORK_ORKER_MAX_PINS` | `0` | `orker` | how many task pins (`docs/DSL.md`, "Pins") this Orker hosts at once. `0` hosts none and starts no extra pollers; above zero makes this Orker a pin host. Pool-wide — see below |
| `deployment.name` | `ORK_DEPLOYMENT_NAME` | `orkstack` | **both** | the Worker Deployment both binaries join |
| `deployment.build_id` | `ORK_BUILD_ID` | *(empty)* | **both** | this release's identity. **Empty means versioning is off** |
| `engine.http_addr` | `ORK_HTTP_ADDR` | `:8080` | `ork` | Connect API + WebSocket bridge |
| `engine.ws_allowed_origins` | `ORK_WS_ALLOWED_ORIGINS` | *(empty)* | `ork` | comma-separated browser origins allowed onto `/ws/runs/{id}` and `/ws/runs/{id}/tasks/{task}/logs` |
| `engine.idempotency_ttl` | `ORK_IDEMPOTENCY_TTL` | `24h` | `ork` | how long an idempotency key stays bound |
| `engine.each_max_items` | `ORK_EACH_MAX_ITEMS` | `1000` | `ork` | the most items one Orkflow's fan-outs (`each:`) may declare in total — the sum of their `max_items`. Default 1000. Lowers it, or raises it up to the built-in 10,000; above 1,000 it needs `engine.continuation.enabled` (refused at boot otherwise, naming both keys). Checked at validate/apply and again at submit, so lowering it refuses new runs of an Orkflow applied under the old value. A submit that needs more than 1,000 items is refused while an `ork` process of a build that new runs start on (any process, on an unversioned fleet) runs a release older than the one that raised the maximum; a draining older build does not block it (§5, "Engine capabilities and the fleet check"). Before rolling back below that release, lower it to 1,000 and wait for such runs (§4) |
| `engine.orkflow_max_tasks` | `ORK_ORKFLOW_MAX_TASKS` | `4000` | `ork` | the most tasks one Orkflow may declare (`docs/DSL.md`, "How many tasks an Orkflow may declare"). A fan-out counts once whatever its `max_items`, and a composed task counts once whatever its child declares. A whole number, 1 or more; anything else is refused at boot. Checked at validate, apply and plan, and again at submit, so lowering it refuses new runs of an Orkflow applied above the new value. A deployment lowers or raises it. The default is the largest size the task soak measured (its per-generation history, snapshot, in-flight, workflow-code, read-latency and run-page thresholds, with plain tasks and small outputs); above it a deployment runs unmeasured, and the boot warns naming the measured size. The task soak runs in the nightly and pre-release proofs, not in every merge proof (docs/PROOFS.md). Independently, a stage of more than X plain tasks that start together may stop `RUN_HISTORY_LIMIT` in its first history, explicitly, and the boot warns when this is above X. X is a size to stay within, not a promise: at or below it a flat stage of plain tasks cannot reach that limit before it carries on, and other limits still apply. X is 4,222 at the defaults and moves with `engine.history.max_events`, `engine.continuation.events`, `engine.continuation.bytes`, `engine.continuation.restore_timeout` and `engine.run_max_in_flight`; §5, "Task ceiling and the flat-stage bound", gives its closed form. With `engine.continuation.enabled` off a run has one history: an unset key defaults to the smaller of 4,000 and `engine.history.max_events / 50` (1,024 at the default limit; the boot logs the effective value), and an explicit value above that quotient is refused at boot, naming the three keys |
| `engine.history.max_events` | `ORK_HISTORY_MAX_EVENTS` | `51200` | `ork` | the cluster's per-execution history event limit. A run stops `RUN_HISTORY_LIMIT` before reaching it, counting what its own failure path still needs, instead of being terminated by the cluster with no reason. **Set it to the cluster's real limit if an operator changed it:** a value above the real limit defeats the guard, and one below stops runs early. Even at the default, a run that would finish just under the limit with much work still pending now stops `RUN_HISTORY_LIMIT`, because its failure path's reserve is kept back: 16 events per running task, 8 per pending task, 24 per open await, 12 per open pin, 48 per failure handler, plus 96 — about 8,100 for a 1,000-task flat stage. At least 100; anything else is refused at boot. Read at submit, so a change applies to new runs |
| `engine.history.max_bytes` | `ORK_HISTORY_MAX_BYTES` | `50 MiB` | `ork` | the same, for history size. Before each dispatch the run also counts that dispatch's own command (a task's input, up to `engine.limits.values_max_bytes`), so it never records one its failure path could not follow. The reserve assumes completion results of ≤ 8 KiB each; a run whose failure path lands many larger results at once can still reach the cluster's size limit and be terminated by the cluster; this is the guard's one known gap, and such a run is still reported `RUN_HISTORY_LIMIT` (the reader maps the cluster's history-limit termination, "Workflow history count/size exceeds limit.", to it), never `RUN_TERMINATED`. At least 64 KiB; anything else is refused at boot. Read at submit, so a change applies to new runs. Both limits are carried once per run tree, on the top-level run's plan (about 0.4 KB), so a run whose plan is already within that much of its 256 KiB bound is refused at submit (`FAILED_PRECONDITION`, naming the size) |
| `engine.continuation.enabled` | `ORK_CONTINUATION_ENABLED` | `true` | `ork` | runs carry on into a fresh history once theirs has grown past the budget below, at their next moment with nothing running, so a large or long-lived run is bounded per history rather than stopped `RUN_HISTORY_LIMIT` (see §5, "A run that pauses new tasks", and §2, "Runs that span several executions"). On by default since the release after the one that first carried runs on (§4, "Rolling back"). The three keys below are validated only when this is true, so a deployment that lowers `engine.history.max_events` below 32,768 or `engine.history.max_bytes` below 40 MiB must lower `engine.continuation.events` or `.bytes` with it, or turn this off: it is refused at boot otherwise, naming the keys. Read at submit, so a change applies to new runs |
| `engine.continuation.events` | `ORK_CONTINUATION_EVENTS` | `8192` | `ork` | history growth (events) after which a run carries on at its next moment with nothing running. A run with no such moment pauses new tasks once its history plus what its failure path still needs reaches 60% of the limit, and carries on once what was running finishes. At least 50, at most a quarter of `engine.history.max_events`; anything else is refused at boot when continuation is enabled. Read at submit, so a change applies to new runs |
| `engine.continuation.bytes` | `ORK_CONTINUATION_BYTES` | `8 MiB` | `ork` | the same, for history bytes; also the most a run copies into any one of its histories (the one bounded copy AGENTS.md §2.2 allows into history): the saved state it carries forward, plus its definition when that is passed by reference (`engine.definition_by_reference`, below), which may take at most a quarter of it, compressed. A run whose saved state is larger carries on in its current history instead, and may stop `RUN_HISTORY_LIMIT` naming the size. At least 16 KiB, at most a fifth of `engine.history.max_bytes`; anything else is refused at boot when continuation is enabled |
| `engine.continuation.restore_timeout` | `ORK_CONTINUATION_RESTORE_TIMEOUT` | `1h` | `ork` | how long a run that carried on waits for object storage to return its saved state before it fails `RUN_STATE_UNAVAILABLE`, explicitly. Each read is retried with a backoff doubling from 1 s to at most 1 minute, and a retry that would start past the timeout is not made, so the run stops at its last read inside the window — up to one backoff early (31 s into a 1m timeout, within the last minute of the default hour). 1m to 8h, and its worst-case history — about `128 + 3 per 8 s` events at the default bytes (1,478 at 1h, 10,928 at 8h)— must fit in a quarter of `engine.history.max_events`; anything else is refused at boot when continuation is enabled, naming this key, `engine.continuation.bytes` and `engine.history.max_events`. Each await still open when a run carries on adds one more event to that restore (its deadline timer, recreated), which the run's history guard counts like any other |
| `engine.definition_by_reference` | `ORK_DEFINITION_BY_REFERENCE` | `true` | `ork` | a top-level run whose planned definition is over the 256 KiB a run's start may carry travels by reference instead: the start carries a stub naming it, and the run copies the definition into its own history in bounded chunks, sharing the copy bound below (`engine.continuation.bytes`), of which it may take at most a quarter, compressed (2 MiB at the default); a larger one is refused at submit and at apply, "split it into composed Orkflows" (`docs/DSL.md`, "How many tasks an Orkflow may declare"). Needs `engine.continuation.enabled`: an explicit `true` without it is refused at boot, naming both keys, and when neither the configuration file nor the environment sets this key it is on only where the built-in default is on and continuation is (the boot logs the effective value). A submit that needs it is refused while an `ork` process of a build that new runs start on (any process, on an unversioned fleet) runs an older release (§5, "Engine capabilities and the fleet check"). On by default from the release after the one that first had it; turn it off and wait for its runs before rolling back below that release (§4, R3) |
| `engine.snapshot_overlays` | `ORK_SNAPSHOT_OVERLAYS` | `true` | `ork` | a run that declares more than 256 rows — its tasks plus every fan-out's `max_items` — writes, for each event before its last, only the rows that changed since its last full snapshot (a snapshot overlay, `docs/STORAGE.md`), and a full snapshot at least every 64 events and whenever more than 256 rows changed; its last event's snapshot is always full. Decided per run, so a small child of a large parent keeps full snapshots. Read at submit, so a change applies to new runs. While any `ork` process polling `ork-engine-events` runs an older release (a draining build's included), or a process of a build that new runs start on does, new runs write full snapshots instead, and the engine logs why at most once a minute (§5, "Snapshot overlays"). On by default from the release after the one that first had it; turn it off and wait for its runs before rolling back below the release that has it (§4) |
| `engine.run_max_in_flight` | `ORK_RUN_MAX_IN_FLIGHT` | `1000` | `ork` | the most activities, child runs and pins one run keeps pending at once; more wait `PENDING`, exactly as on a saturated pool, and dispatch resumes as running work finishes. An open pin's needs are exempt, below an absolute stop at 1,900. 16 to 1,800; anything else is refused at boot. Without it, a run above the cluster's 2,000 pending activities would stall. Read at submit, so a change applies to new runs |
| `engine.pin_creation_timeout` | `ORK_PIN_CREATION_TIMEOUT` | `5m` | `ork` | how long a pin waits for some pin host of its pool to have a free slot (`ORK_ORKER_MAX_PINS`) before the task whose readiness asked for it fails `PIN_UNAVAILABLE`. A duration of whole seconds, `1s` to `24h`; anything else is refused at boot, naming the key. Read at submit and carried by the run, so a change applies to new runs only: a run already accepted keeps the wait it started with, and so does a child run of it. A pool whose pin hosts are all cordoned fails a pin after about five minutes whatever this says (§5, "Removing an Orker safely: cordon and drain") |
| `engine.compose_max_depth` | `ORK_COMPOSE_MAX_DEPTH` | `4` | `ork` | how many levels of child runs one run may start (a run is level 0). Lowers the built-in ceiling of 4, never raises it; checked at validate/apply and again at submit, so lowering it refuses new runs of an Orkflow applied under the old value |
| `engine.cache_max_ttl` | `ORK_CACHE_MAX_TTL` | `168h` | `ork` | the oldest memoized result a cached task (`cache:`) may reuse, measured from when it was committed; caps every task's `ttl_seconds`, and bounds a task that sets none. At least 1s. Read at submit, so a change applies to new runs |
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
| `engine.cleanup.batch_size` | `ORK_CLEANUP_BATCH_SIZE` | `100` | `ork` | maximum eligible runs checked in one cleanup iteration; a full iteration starts the next one at once (a backlog drains without waiting for the interval) |
| `storage.dir` | `ORK_ARTIFACT_DIR` | `./.data/artifacts` | both | filesystem root for the `local` backend; ignored under `s3` |
| `storage.s3.endpoint` | `ORK_S3_ENDPOINT` | `http://localhost:9000` | both | S3-compatible endpoint. `ork` reads orkflow definitions from S3 whatever `ORK_ARTIFACT_BACKEND` says, so this one is never optional for the engine |
| `storage.s3.region` | `ORK_S3_REGION` | `us-east-1` | both | — |
| `storage.s3.bucket` | `ORK_S3_BUCKET` | `orkstack-artifacts` | both | one bucket holds definitions, artifacts, archived runs and task logs |
| `storage.s3.access_key` | `ORK_S3_ACCESS_KEY` | `orkstack` | both | the dev credential from `deploy/docker-compose.yml`. Supply a real one |
| `storage.s3.secret_key` | `ORK_S3_SECRET_KEY` | `orkstack-secret` | both | as above |
| `storage.s3.session_token` | `ORK_S3_SESSION_TOKEN` | *(empty)* | both | for temporary credentials |
| `storage.s3.path_style` | `ORK_S3_PATH_STYLE` | `true` | both | path-style addressing; the dev store needs it, AWS S3 does not |
| `engine.stats_cache_ttl` | `ORK_STATS_CACHE_TTL` | `5s` | `ork` | how long one `GetStats`, `GetFailureBreakdown`, `GetFlakeIndex` or `GetCacheStats` answer is reused; `0` switches the cache off (`docs/STATS.md`) |
| `engine.usage_cache_ttl` | `ORK_USAGE_CACHE_TTL` | `5m` | `ork` | how long one usage measurement (`orkctl stats usage`, the dashboard Usage section, and an enforced budget's admission check) is reused; `0` switches the cache off, which makes every submit into an enforced namespace measure (`docs/STATS.md`, "Usage") |
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
needed) — every key present, each at its own default. `deploy/orkstack.example.yaml` stays the worked,
hand-annotated example for a real deployment to start from; the generated
file is the reference, not something to copy and edit by hand.

### Sizing an Orker's task ceiling

`orker.max_concurrent` is how many task containers one Orker runs at once.
Past it the Orker stops polling: further tasks wait in its pool for this Orker
or another to free a slot, which is the right place for them to wait — a
container the host cannot afford to run is a task that fails `OOMKilled` or
crawls, not one that runs sooner.

The default is `16`. Size it from the host and the task defaults: a task that
requests nothing is given `orker.resources.default_cpu` (`1`) and
`orker.resources.default_memory` (`1Gi`), so a host with 16 CPUs and 64 GiB
that runs mostly default-sized tasks is bounded by `min(16 ÷ 1, 64 ÷ 1) = 16`
— the host the default is sized for. A smaller host should lower it, a larger
one raise it explicitly: the ceiling is never derived from the host, because
the Orker cannot know what else shares it. Leave headroom for the Orker itself
and the container daemon, and size by the larger requests where a pool runs
them. (Before 2026-10 the default was the Temporal SDK's own `1000`, which
capped nothing a real host could run; an Orker that relied on it now takes at
most 16 tasks at once, and the rest wait in the pool — set the key to keep
the old behaviour.) A fan-out (`each:`) without `max_parallel` is the case this bounds:
every item is dispatched at once, and only this ceiling decides how many of
them one Orker takes. The value is logged at boot (`max_concurrent`) and
reported by Admin → Activities.

### Pin hosting is a pool-wide setting

Pin hosting is a property of a **pool**, not of a process: set the same
`ORK_ORKER_MAX_PINS` on every Orker of a pool, because the submit-time probe
that refuses a pin with no host to open it on asks one Orker of the pool and
takes that answer as representative — a mixed pool is refused or accepted
depending on who answered, and a pin accepted onto Orkers that in fact set `0`
waits for a slot that never appears and then fails the run.

The refusal is `failed_precondition`, the same code as an empty pool, and it
names the pool, the pins, the tasks and the fix:

```
pool "gpu" (namespace "ci", pin "build", tasks: clone, compile, test) has live
Orkers but none of them hosts pins, so the pin could never open. Start one with
ORK_ORKER_POOL=gpu ORK_ORKER_MAX_PINS=1, or submit with allow_empty_pool to wait
for one
```

`allow_empty_pool` downgrades it to a logged warning, as it does for an empty
pool. A pool Orker that does not answer the describe probe is not a refusal —
the submit proceeds and, if no host can open the pin, the triggering task
fails `PIN_UNAVAILABLE` after the pin creation timeout
(`engine.pin_creation_timeout`, five minutes by default; `docs/DSL.md`,
"Pins").
Pinning is therefore opt-in twice — by the author (the document) and by the
operator (this setting) — and a document that pins is refused until an
operator has opted its pool in, never left waiting. A pin host runs two extra
Temporal pollers (session creation and its own per-process session channel),
which is why the default is `0`: an Orker that never set the key starts
neither.

### Fan-out, caching and failure handlers need every Orker of a pool on a release that supports them

An Orkflow with a fan-out (`each:`, `docs/DSL.md` "Fan-out") needs Orkers that
understand items: an Orker built before fan-out would run an item without its
`task.json` `each` object, and would stage nothing for a fan-out's direct
dependent — a silently wrong result. So submit asks one Orker of the pool of
every fan-out and of every direct dependent of one whether it supports
fan-out, and refuses the run with `FailedPrecondition` naming the pool, the
tasks, the Orker and its build when it does not; `allow_empty_pool` does not
override that. Like pin hosting, the answer of one Orker stands for its pool:
run every Orker of one pool on one release.

Caching (`cache:`, `docs/DSL.md` "Caching") is the same: an Orker built before
caching cannot even decode a cached task's input — the SDK's payload converter
refuses the unknown field — and would fail every attempt for a reason that has
nothing to do with the task. So submit asks one Orker of the pool of every
cached task (a cached fan-out's items run on the fan-out's pool) whether it
implements caching, and refuses with `FailedPrecondition` naming the pool, the
tasks, the Orker and its build when it does not; neither `allow_empty_pool` nor
`no_cache` overrides that (a bypass still sends the cache context). Memos live
under `metadata/memo/` and need no setting of their own.

Failure handlers (`join: any_failed` or `all_done`, `docs/DSL.md` "Joins and
failure handlers") are the same: an Orker built before join policies cannot
decode a failure handler's input — the SDK's payload converter refuses the
unknown field — and would fail the compensation step of a run that is
already failing. So submit asks one Orker of the pool of every failure
handler that runs a container whether it reads join policies, and refuses
with `FailedPrecondition` naming the pool, the tasks, the Orker and its
build when it does not; `allow_empty_pool` does not override that. A
`none_failed` join needs nothing from an Orker.

All three probes are best-effort: a pool whose Orker does not answer within
the describe timeout is not refused — nothing was measured — and the engine
logs a warning (`submit: a live pool with … did not answer the describe-orker
probe`). The probe is a guard for a mixed rollout, not a substitute for
running every Orker of a pool on one release.

### Pins on a versioned deployment

Pins work with Worker Deployment Versioning (`ORK_BUILD_ID`, §2), and what
holds a pin in place is **not** the routing versioning uses:

- the pin's session **creation** activity is version-routed like any other
  activity of a pinned run: scheduled on `<pool queue>__internal_session_creation`
  and dispatched to a pin host of the run's own version;
- the pin's **member** activities, its release and the SDK's session
  completion ride the session's own `<uuid>@<hostname>` queue, which the SDK
  polls *unversioned*, so they reach the one process that opened the pin even
  after a newer version becomes Current;
- the run's unpinned tasks keep ordinary version-pinned routing, and a pin
  opened after the switch opens on the new version.

Two consequences for a rollout. A pin is bound to a **process**, not to a
version: §3 step 5's wait for `Drained` is what keeps an open pin's host
alive, and restarting that process — even at the same build id — loses the
pin rather than moving it, because the per-host session queue is minted fresh
on every worker start. And the SDK's doc comment still says
`DeploymentOptions.UseVersioning` "cannot be enabled at the same time as
`EnableSessionWorker`" (`temporalio/sdk-go#1227`); only the deprecated
`UseBuildIDForVersioning` flag is enforced. The combination is therefore not
an upstream-supported configuration, and
`internal/engine/versioning_pin_integration_test.go`
(`TestIntegration_AnOpenPinStaysOnItsHostAcrossAVersionRollout`) — not the
doc comment — is the contract; it runs on every proof.

`orkctl release status` and `release wait` name each open pin and the Orker
holding it. The **host**-scoped complement of the version-scoped `Drained` is
`orkctl orker drain` (§5, "Removing an Orker safely"): cordon one Orker so it
opens no new pin, and wait until its open pins have finished.

### The bucket stops being optional at the second replica

`local` is the single-host default, and the one setting that breaks a
replicated deployment quietly: each process keeps its own directory, so a run
archived by replica 1 is invisible to replica 2 and an artifact written by one
Orker cannot be materialized by another. Set the same values on every `ork`
and every `orker`:

```bash
export ORK_ARTIFACT_BACKEND=s3
export ORK_S3_ENDPOINT="https://s3.internal.example"   # or http://localhost:9000
export ORK_S3_BUCKET=orkstack-artifacts
export ORK_S3_REGION=us-east-1
export ORK_S3_ACCESS_KEY=…        # never the docker-compose dev credentials
export ORK_S3_SECRET_KEY=…
export ORK_S3_PATH_STYLE=true     # the dev store needs path style; AWS S3 does not
```

The store must support conditional `PutObject` (`If-None-Match`/`If-Match`):
commits, Orkflow pointers and registry documents are arbitrated by it, and
every Orker probes it at boot and refuses to start on a store that cannot. Any
S3-compatible store with that primitive works; the production default is AWS
S3. The local development stack uses **SeaweedFS** (Apache-2.0, pinned by
digest in `deploy/docker-compose.yml`, S3 API on `localhost:9000`), because
MinIO stopped publishing images and Garage cannot do conditional writes. What
lives under which key, who writes it and what removes it is
`docs/STORAGE.md`.

### Live task logs: where they are, what bounds them, how long they stay

A task's output is bytes, so it travels as stored objects and never through a
Temporal payload or the event stream (AGENTS.md §2.2). The Orker executing an
attempt buffers its lines and flushes them as numbered chunks **while the task
runs** (`metadata/logs/{run-id}/{task-id}/[{item}/]{attempt}/{seq}`, plus a
`.trunc` notice); `orkctl run logs -f`, `RunService.StreamTaskLogs` and the
`/ws/runs/{id}/tasks/{task}/logs` socket read them back. Both counters are
zero-padded, so **the key carries the order** and no reader sorts by
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
Both resume on `from_seq`, the last sequence already received: the stream
continues **after** it, so a reconnect replays nothing and skips nothing.

**A follow moves with the task's retries.** Keys are per ATTEMPT, and a
Temporal retry changes nothing a task's status shows — the task stays RUNNING
while try 2 writes under a new prefix. So a follow that named no attempt
(`attempt=0`, the default and what the UI sends) re-reads which attempt is
latest while the one it is on is silent, and moves onto a later one from its
first chunk; a stream that resolved the attempt once would sit on a dead
prefix while reporting itself live.

That makes `from_seq` half a cursor: sequences are dense **per attempt**, so a
reconnecting client sends `from_attempt` beside it. A cursor from another
attempt is dropped and the new attempt is delivered from its start — a retry
is usually shorter than the try that failed, so applying the old cursor would
deliver nothing. An explicit `--attempt N` is pinned and never moves. Every
client sends both halves (`ui/src/lib/taskLogs.ts`, `internal/api/ws.go`,
`orkctl run logs --from-seq N --from-attempt N`); the CLI **refuses** a cursor
that names no attempt rather than guess, which is how a resume silently skips
a retry's first chunks.

Two bounds decide when a chunk is written, and a deployment tunes both:

- `ORK_ORKER_LOG_FLUSH_BYTES` (64 KiB) batches a chatty task, so a hundred
  thousand lines cost a few hundred objects rather than a hundred thousand.
- `ORK_ORKER_LOG_FLUSH_INTERVAL` (500 ms) bounds the wait for a slow one, so a
  task printing a line a minute is visible in half a second. It is also the
  shortest gap between two uploads of one attempt, and it does not stay fixed:
  after every 120 chunks (about a minute of steady output) it doubles, up to
  8× (4 s at the default), so a two-hour task printing steadily writes about
  1,900 objects rather than 14,000 while its first minute keeps the
  half-second latency.

While the pace holds a chunk back, the chunk grows — up to 1 MiB — rather than
blocking the task. Only a task writing more than 1 MiB per interval (2 MiB/s at
the default) waits on its own `write(2)`, which is the bound: there is no
queue and nothing is dropped. A store that refuses a chunk after three tries
costs the task that one stall; for the next 5 s (doubling to 30 s while it
keeps failing) chunks are counted as dropped without being tried, and the first
chunk that lands afterwards says how many bytes were lost.

A third bounds the damage. `ORK_ORKER_LOG_MAX_BYTES` (16 MiB) caps ONE task
attempt: at the cap the Orker writes a final chunk saying so, in words, and
stores nothing more. **Truncation is visible in the stream**, never silent —
`orkctl run logs` prints the notice and the RPC sets `truncated` on it. The
Orker's own memory is one flush buffer per running task — at most 1 MiB plus
one line — and a single over-long line is split rather than accumulated.

**What one reader costs the engine is bounded in bytes, not in chunks.** A
replica holds one page per open subscriber: at most **4 MiB of chunk data,
plus the one chunk that crossed that budget** (`logChunkPage`,
`logChunkPageBytes`, `internal/api/logs.go`). A count alone is not a size —
raising `ORK_ORKER_LOG_FLUSH_BYTES` raises what a page weighs. A page cut by
the budget is followed by another read, so a replay is whole. Size a replica
for this per concurrently open pane or `orkctl run logs`; a run page opens one
per expanded task.

A reader that meets a removed chunk mid-stream stops at the gap rather than
skipping it, so a resumed read continues from the last sequence it actually
received.

### Retention cleanup and bucket lifecycle rules

Two independent mechanisms bound what the bucket keeps, and they are
complementary, not alternatives.

**The cleanup worker** (`ORK_CLEANUP_ENABLED=true`, off by default because it
deletes durable data) is what enforces run retention (§5, "Run retention"). Each
pass walks the run index in key order — memory proportional to one page of
`ORK_CLEANUP_BATCH_SIZE × 2` keys, one pass over the index per iteration —
prefiltering on creation time with the shortest retention any namespace has.
For each candidate it asks the cluster whether the execution still exists and
when it CLOSED; one not yet past its OWN namespace's run retention (else the
deployment maximum) is left alone. One past it is retired explicitly — the
cleaner deletes the execution itself rather than waiting for a TTL — and its
run-scoped storage is removed: logs, attempt records, snapshots, the commit
records, and its object-reference markers (`docs/STORAGE.md`, "Reclaiming
space"). An execution the cluster no longer holds is treated as already
retired. The archive body, its index rows and the cleanup sidecar are
permanent, so a retired run stays listable and readable.

On S3 a run's four run-scoped prefixes are emptied concurrently, one
`DeleteObjects` request per listing page (up to 1000 keys), so the store must
support `DeleteObjects` (AWS S3 and SeaweedFS do). A key the store refuses fails
that run's cleanup — it is not marked cleaned and a later pass retries it.

**A backlog drains without waiting.** `ORK_CLEANUP_BATCH_SIZE` bounds one
pass, not the rate: a pass that processed a full batch starts the next pass at
once, resuming the index walk after the last run it examined, and the worker
returns to `ORK_CLEANUP_INTERVAL` after the first pass that reaches the end of
the index. A backlog of retired runs therefore clears at the store's and the
cluster's speed, a batch at a time; the next ticked pass walks from the start
again, so a run a drain passed before it was due is examined once it is.

Cleanup **fails closed**: if the retention policy or any namespace's own value
is unreadable, that iteration deletes nothing; an archived run the cluster
reports as STILL RUNNING is refused rather than retired (deleting would
terminate it, and archiving happens only after a terminal event, so it is a
defect to surface); per-run storage failures are logged and retried by a later
pass; a repeated pass is a no-op because the `artifacts_cleaned` sidecar
records what was done.

**Workload artifact bytes are not reclaimed automatically.** The archive
records which objects each run references, and `DeleteUnreferencedObjects`
can delete an object no archived run claims, but the cleanup worker does not
call it: it would be a scan of the whole reference index, not a delete under
one run's prefixes. A bucket lifecycle rule is the operational answer for
`orkflows/` (and `objects/` on `local`); choose its expiry by how long archived
artifact references must stay readable — `orkctl run artifact provenance`
reports `bytes_present: false` for bytes a rule removed.

**Bucket lifecycle rules** expire objects by age regardless of retention, and
are the only retention for the prefixes nothing in OrkStack deletes. Each
rule below changes no recorded result, only how far back a read can look —
provided its expiry is longer than any run lasts (artifact bytes expired
under a running run fail its dependents' input staging):

| prefix | what a rule shortens |
|---|---|
| `metadata/logs/` | task output of finished runs (`orkctl run logs`) |
| `metadata/attempts/` | per-attempt records (`orkctl run attempts`, the flake index, usage and budgets, which report them as missing evidence) |
| `orkflows/`, `objects/` | artifact bytes; references stay, `bytes_present` turns false |
| `metadata/audit/` | the control-plane audit trail (`orkctl admin audit`); no setting shortens it |
| `metadata/deliveries/` | webhook delivery records (`orkctl webhook deliveries`); a log, never a queue |
| `metadata/memo/` | cached-task memos; keep the rule no shorter than `ORK_CACHE_MAX_TTL` (a memo past it can never be reused anyway) |

Do not expire the other prefixes: `metadata/orkflows/`, `metadata/namespaces/`,
`metadata/settings/`, `metadata/triggers/` and `metadata/webhooks/` are
definitions and registries; `metadata/commits/` holds the commits a retry
adopts and `metadata/snapshots/` what live reads need, both reclaimed by
cleanup at retention; `metadata/runs/` and `metadata/index/` are the archive.
That includes `metadata/snapshots/{ns}/{run}/continuations/`: the event
segments, switch snapshots and saved state a run that carried on into a fresh
history wrote (§2, "Runs that span several executions"). A live run reads its
segments for its whole life and its archive is built from them, so a rule that
expired one would turn part of the run's stream into a recorded gap. The
full `{seq}.pb` snapshots of a run that writes snapshot overlays are the same:
its `{seq}.ovl` overlays name them, so a rule that expired one would make the
run's live state at those events unreadable (§5, "Snapshot overlays").

```bash
# SeaweedFS or AWS S3: expire task logs after 30 days.
mc ilm rule add local/orkstack-artifacts --prefix "metadata/logs/" --expire-days 30

# AWS S3: the same rule.
aws s3api put-bucket-lifecycle-configuration --bucket orkstack-artifacts \
  --lifecycle-configuration '{"Rules":[{"ID":"ork-task-logs","Status":"Enabled",
    "Filter":{"Prefix":"metadata/logs/"},"Expiration":{"Days":30}}]}'
```

**Abort incomplete multipart uploads.** An Orker uploads an output artifact
larger than 64 MiB in 16 MiB parts, four at a time, so a failed part is
re-sent alone rather than the whole artifact. A failed upload is aborted, but
an Orker killed mid-upload leaves its parts in the bucket, invisible to every
listing and still billed. Add one bucket-wide rule that collects them; it
touches no complete object:

```bash
# SeaweedFS needs no rule: it removes incomplete uploads itself, after
# `stale_uploads_expiry` (24h by default; `mc admin config get local api`).

# AWS S3
aws s3api put-bucket-lifecycle-configuration --bucket orkstack-artifacts \
  --lifecycle-configuration '{"Rules":[{"ID":"ork-abort-incomplete-multipart","Status":"Enabled",
    "Filter":{"Prefix":""},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1}}]}'
```

A `put-bucket-lifecycle-configuration` call replaces the bucket's whole rule
set, so put this rule in the same document as any expiry rules above.

Retention is separate from `orkctl orkflow delete`, which removes an
Orkflow's stored versions only after its run-safety gate proves no affected
run is live; neither deletes archived run records.

### Versioning is off until you set a build id

With no `ORK_BUILD_ID`, both binaries register unversioned workers and every
run goes to whatever is polling. That is what keeps a
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
  run_workflow_behavior=Pinned claim_workflow_behavior=AutoUpgrade \
  schedule_fire_workflow_behavior=Pinned event_worker=unversioned
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

One call fixes it, and no worker has to restart:

```bash
orkctl release promote "$ORK_BUILD_ID"
```

`release promote` and `release rollback` name the release current now and
the one that will be, and ask for confirmation before they write; in a
script (no terminal on standard input) they refuse unless given `--yes`.

`orkctl release` speaks OrkStack's own vocabulary — a **release** is one build
of `ork` and `orker` deployed together — and reads the deployment, the
namespace and the cluster address from the engine it talks to, so there is
nothing to pass by hand. It refuses, with nothing changed, while the release
could not carry what it would take: no engine of it running, a pool in use
with no uncordoned Orker of it, a pool that hosts pins with no pin host of it,
or **runs started before versioning still executing** (§3.5) — the refusal
names each and what to start or wait for. The raw cluster
command it makes underneath is listed in §4, "Underneath: the cluster commands".

Backlogged runs start within seconds of it. §3 step 0 is the full first-release
procedure; every release after that inherits a Current Version from its
predecessor, and takes over with the same call at §3 step 4
(`TestIntegration_AVersionedReleaseIsIdleUntilItsVersionIsCurrent`).

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
| run worker (`ork`) | `ork-tasks` — **fixed** | yes | `RunWorkflow` = **Pinned**, `ScheduleFireWorkflow` = **Pinned**, `IdempotencyClaimWorkflow` = **AutoUpgrade** |
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
poller. That is the shape `orkctl release status` reads (§3 step 2: engines on
the one, Orkers per pool on the other), and reading it as one name aborts
healthy rollouts.

**Why `RunWorkflow` is Pinned.** It interprets a DAG whose scheduling order,
event sequence and activity arguments are decided by current code. Change the
ready-task ordering, the event counter, or the shape of the task activity
input, and replaying a half-finished history against the new build produces a
different command stream — a non-determinism error that kills a run the
operator did nothing to. AutoUpgrade would hand exactly that history to the new
build on its next workflow task. Pinned lets the run finish on the code that
started it.

**Why a schedule's fire is Pinned.** `ScheduleFireWorkflow` is the action
every Orkflow schedule takes: it prepares one run (a local activity on the run
worker) and starts it as its child, then lives exactly as long as that run so
the schedule's overlap policy applies to the run. It is pinned for the reason
`RunWorkflow` is, and it costs nothing more: it holds open no version its run
does not already hold open, and its child run is pinned to the same version. A
fire opened before a rollout finishes, with its run, on the version that
opened it; a fire started after the switch runs on the new version
(`internal/engine/schedule_integration_test.go`).

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

**A child run is a `RunWorkflow` its parent starts.** A composed task
(`uses: orkflow:`, `docs/DSL.md` "Composition") starts its child run as a
child workflow of the parent, on the **run channel the parent runs on**
(`ork-tasks`, never `ORK_ORKER_QUEUE`), with the parent-close policy
`REQUEST_CANCEL` and no retry or timeout of its own. A child started by a
Pinned parent on a queue of the parent's own version starts on that version —
Temporal's inheritance rule — so the child's plan, which the parent's release
built at submit, is interpreted by that same release's code; a child started
after a rollout's switch still runs on the parent's version
(`internal/engine/versioning_compose_integration_test.go`, the observation
rather than an assumption). A parent holds its release open until its
children end, exactly as any running run holds it, and `orkctl release`
counts a child as the run it is.

### Schedules: what the cluster shows

An Orkflow schedule (`docs/ORKFLOW_SCHEDULES.md`) is a Temporal Schedule and
nothing else. Operators who look at the cluster directly will see:

- `temporal schedule list` shows each one as `ork-schedule/{namespace}/{name}`,
  with the OrkStack identity in its memo and the authored record as its
  action's argument;
- each fire is a `ScheduleFireWorkflow` execution with id
  `ork-schedule-fire/{schedule id}-{nominal time}` on `ork-tasks`, and the run
  it starts is an ordinary `RunWorkflow` child with an `ork-…` id;
- a schedule's fire workflows retire with the cluster's execution retention
  (derived from the maximum run retention), so a recent fire older than that
  reads `unavailable` in `orkctl schedule get` — never blank, never
  successful. The schedule itself is not subject to that TTL.

Editing a schedule with the `temporal` CLI edits the record itself and is not
supported, on the same terms as editing the bucket by hand: the authored spec
travels inside the schedule's action, and `ork` logs a warning when the
cluster's policy no longer matches it. Change a schedule with
`orkctl schedule update`.

### Runs that span several executions: what the cluster shows

A run whose history grew past its budget carries on into a fresh history: it
continues as new, so on the cluster it is a chain of executions — generations
— under one workflow id, its run id. This is an operator-only view; every
OrkStack surface shows one run, with one id, one event stream and one archive.
Operators who look at the cluster directly will see:

- `temporal workflow list --query 'WorkflowId="ork-…"'` lists one record per
  generation; every closed one has status `ContinuedAsNew`, and only the
  newest is `Running` (or its final status). Each record's `StartTime` is its
  own generation's, so the run's own `created_at` travels in the
  `orkflow_created_at` memo, which every record of the chain carries.
- `temporal workflow show --workflow-id ork-…` shows the current generation:
  its start carries an empty run-id argument and the reference to its saved
  state, and its events begin after the carried sequence. The events of
  earlier generations are in their immutable event segments under
  `metadata/snapshots/{ns}/{run}/continuations/`, which every read follows.
- the cluster's index writes a switch's closing and starting records
  independently, in an order it does not guarantee, so for a moment a run can
  show as two `Running` records or only as the one that just closed. OrkStack
  deduplicates by workflow id, treats a `ContinuedAsNew` record that closed
  within the last **hour** (the switch-visibility window, W) as its run still
  live, and settles such records with one batched listing by id per 100 runs —
  a describe only for a run still caught between its two records. Every
  liveness question — lists, the deletion gate, admission, release and drain
  counts — therefore counts such a run once and never as gone. The assumption
  is that the index shows a switch's successor within W; the server contract
  test records the lag the test cluster shows (`TestIntegration_ContinueAsNewServerContract`,
  item j). A cluster whose visibility takes longer than an hour to index a
  switch can briefly under-count a switching run.
- retiring a run deletes every generation, oldest first, then the current one
  by its own execution id (the cleanup worker does this; an execution
  already gone counts as deleted).

To list the runs carrying on right now:
`temporal workflow list --query 'WorkflowType="RunWorkflow" AND ExecutionStatus="ContinuedAsNew"'`
returns the closed generations, and their workflow ids are the runs.

---

## 3. Rolling a release forward

```bash
NEW=2026.08.23-c4d5e6f      # v2
OLD=2026.08.16-a1b2c3d      # v1, currently serving
```

Every step is an `orkctl release` command against any engine replica, of
either release. Nothing is stored by OrkStack between them: the cluster's
routing is the only record of which release takes new runs, and every command
reads it fresh.

```text
$ orkctl release status
deployment:  orkstack
current:     2026.08.16-a1b2c3d since 2026-09-24T09:02:11Z
ramping:     -

RELEASE             ROLE       ENGINE  ORKERS  POOLS SERVED    RUNS  STATE
2026.08.16-a1b2c3d  current    2       4       build,gpu,main  14    serving
2026.08.23-c4d5e6f  candidate  1       2       gpu,main        0     not ready (1 blocker)

2026.08.23-c4d5e6f cannot take new runs yet:
  - pool "build" is in use (orker-b45e of release 2026.08.16-a1b2c3d serves it) and no Orker of release 2026.08.23-c4d5e6f serves it, so tasks its runs send there would not run on its own Orkers. Start one with ORK_BUILD_ID=2026.08.23-c4d5e6f ORK_ORKER_POOL=build.
```

Steps 1–6 are every release that has a predecessor. **The very first versioned
release of a deployment has none, so start at step 0** — skip it and the
release runs nothing at all (§1).

0. **First release only: make it current before you rely on it.**

   ```bash
   FIRST=2026.08.02-0a1b2c3
   ORK_BUILD_ID=$FIRST ./bin/ork &
   ORK_BUILD_ID=$FIRST ./bin/orker &

   orkctl release status            # FIRST is a candidate, nothing is current
   orkctl release promote "$FIRST"  # refused while it cannot carry new runs
   ```

   Runs submitted before the promotion are backlogged, not lost: they start
   within seconds of it and no worker restarts. If you are switching a system
   that is already serving unversioned traffic, read §3.5 first — `promote`
   refuses while runs started before versioning are still executing.

1. **Start v2 alongside v1.** Same namespace, same pools, new `ORK_BUILD_ID`.
   Do not stop anything yet.

   ```bash
   ORK_BUILD_ID=$NEW ./bin/ork &
   ORK_BUILD_ID=$NEW ./bin/orker &
   ```

   In a replicated deployment (§0) that means **all** of v2: every `ork`
   replica and an Orker in every pool the release dispatches to, with a pin
   host wherever v1 hosts pins. v1's replicas stay up and keep serving the API
   — the two releases coexist behind one address, and which release a new run
   executes on is the cluster's routing, never the replica the client happened
   to reach: after the switch, a run submitted **to a v1 replica** executes
   wholly on v2.

2. **Check v2 can carry the work:**

   ```bash
   orkctl release status
   ```

   v2's `STATE` reads `ready` when nothing blocks it. Otherwise every blocker is
   listed with what to start, in OrkStack's words: no engine of v2 running; a
   pool **anybody is serving now** with no uncordoned Orker of v2 (from the
   Orker inventory, so a pool that is quiet today is still judged); a pool
   that hosts pins with no pin host of v2; or an Orker that did not answer
   (`UNVERIFIED` — a read that could not be made is never a pass). A count
   the cluster could not give reads `unknown`, never 0.

3. **Ramp** (optional but recommended):

   ```bash
   orkctl release ramp "$NEW" --percent 10
   ```

   New runs split between the releases. Watch error rates. Raise the
   percentage as confidence grows; `--percent 0` returns every new run to v1.
   A ramp above 0 runs the same check as promote and is refused on the same
   blockers.

4. **Make v2 current:**

   ```bash
   orkctl release promote "$NEW"
   ```

   ```text
   release 2026.08.23-c4d5e6f is current (was 2026.08.16-a1b2c3d).
   New runs start on 2026.08.23-c4d5e6f; the 14 runs executing on 2026.08.16-a1b2c3d finish there.
   Keep 2026.08.16-a1b2c3d running until "orkctl release wait 2026.08.16-a1b2c3d" exits 0.
   ```

   From this moment every **new** run executes on v2. Every run already in
   flight stays on v1 and finishes there. The command returns only after the
   engine's own channel — what dispatch reads — reports the new routing, or
   says out loud after 10 s that the change is recorded but not yet applied.

   A promotion is refused, and nothing is written, when the routing changed
   since the command's own read (`the releases changed since you read them`):
   read again and decide again. There is no override flag; the cluster's own
   commands remain (§4, "Underneath").

5. **Wait for v1 to drain — do not skip this:**

   ```bash
   orkctl release wait "$OLD" --timeout 2h
   ```

   ```text
   waiting for release 2026.08.16-a1b2c3d to drain…
     09:15:40  3 runs, 1 pin   draining (checked 09:13:02)
               pin "build" of run ork-… on orker-7f3a, expires in 0h41m
     09:31:02  1 run, 0 pins   draining (checked 09:28:40)
     09:47:25  0 runs, 0 pins  drained
   release 2026.08.16-a1b2c3d is drained; its engines and Orkers may be stopped.
   ```

   It exits 0 only when the cluster reports v1 drained **and** no Orker of v1
   hosts an open pin **and** every Orker answered; 4 at `--timeout` (default
   1h), naming every run and pin left; 1 when v1 takes new runs, is unknown,
   or the engine stops answering. The cluster refreshes its drain verdict on a
   delay of minutes (three by default; the development stack and the proofs
   shorten it to seconds, §4 "Underneath"). `wait` is a loop in `orkctl`;
   nothing server-side holds rollout state.

   Stopping v1 while it is still draining strands its runs: their tasks stay
   scheduled and unstarted and the runs stay `RUNNING` until v1's workers come
   back, and then finish on v1. **v2 does not pick those tasks up** — if the
   §2 exception ever fired here, an in-flight run's remaining tasks would run
   silently on a newer Orker build, the thing this document exists to prevent
   (`TestIntegration_APinnedRunStrandsWhenItsVersionsOrkerStopsBeforeDrained`
   fails the day that changes).

6. **Stop v1** once `release wait` exits 0 — every replica and every Orker
   carrying that build id. Stopping *some* of them earlier is safe (§5); the
   release is what must not disappear while runs are pinned to it. An Orker
   that hosts an open pin must not be stopped before the pin closes:
   `orkctl orker drain <orker>` waits for exactly that. A run holds its release
   while it waits on an **awaited task** (at most 30 days, the cap on
   `await.timeout_seconds`), while any fan-out item is unsettled, while a
   composed child runs (children are counted as runs), and while a failing
   run's failure handlers run.

The rollout is held end to end by
`TestIntegration_PinnedRunFinishesOnItsOwnVersionWhileNewRunsMoveOn` (the run
in flight at the switch finishes wholly on v1, including the task scheduled
after it) and, through `orkctl`, `TestIntegration_ReleaseRolloutThroughOrkctl`.

**Why OrkStack's check is stronger than the cluster's own.** The cluster
refuses a promotion only for a version no poller was ever seen on, and for one
missing a channel the current version polled *only if that channel had work
recently* — a pool whose work arrives weekly passes on a quiet day. It knows
nothing of cordons, pin hosts, or runs started before versioning.
`orkctl release` judges every pool anybody is serving, from the Orker
inventory, and leaves the cluster's guards on underneath.

### Releases that add Orkflow constructs

A stored Orkflow version outlives the release that applied it. An older
engine refuses to *apply* a construct it does not know, but asked to *run* an
already-stored version it decodes the definition with unknown fields dropped.
So, during a ramp, apply a document that uses a construct only once the
release that introduced it is Current (versioned) or on every replica
(unversioned); and before rolling back past it, stop submitting versions that
use it. What an older release does with each:

| construct | an older release running a stored version that uses it |
|---|---|
| `await:` | treats the await as a container task and fails it — explicit |
| `each:` (fan-out) | runs the node once as a plain task, staging no items for its dependents; fails `RENDER_FAILED` only if `with:` reads `.each` — **silent** otherwise |
| `uses: orkflow:` | an older run worker cannot decode the start (`Task.orkflow`, `RunPlan.children`) and retries the workflow task until a new worker takes it — under Pinned, possibly never; a composed task reaching an older Orker fails `IMAGE_NOT_PINNED` |
| `outputs:` | returns the leaf aggregation instead of the declared outputs — **silent** |
| `join:` | runs every task as `all_succeeded`: a `none_failed` re-join is skipped (visible), but an **`any_failed` failure handler runs after its needs SUCCEEDED** (a rollback after a successful deploy) and an `all_done` handler only on success — **silent and destructive** |
| `cache:` | runs every task uncached; memos left behind are inert — benign |
| schedules | no `ScheduleFireWorkflow` is registered, so a fire cannot start — pause or delete every schedule (`orkctl schedule pause`/`delete`) first |

Before rolling back past join policies, hold every namespace whose current
versions declare a failure handler (or promote versions without one), then
release the hold only once none declares one or the deployment is rolled
forward again:

```bash
orkctl namespace update NS --hold "rolling back past join policies"
orkctl release rollback "$OLD"
orkctl release wait "$NEW"        # runs already on the newer release drain there
```

Runs already executing on the newer release — items, children and failing
runs included — stay pinned there and must drain (`orkctl release wait`). The
Orker-side half of fan-out, caching and failure handlers is checked at submit
per pool (§1, "Fan-out, caching and failure handlers need every Orker…").

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
  Stop submitting (`orkctl namespace update NS --hold REASON` per namespace),
  let running runs finish, then do §3 step 0. Nothing is ever replayed across
  builds. `orkctl release promote` enforces this: while no release is current
  and a run started before versioning is still executing, it refuses with
  `N run(s) started before any release was current are still executing`.
- **Switch live** — a raw procedure only (§4, "Underneath"), because OrkStack
  cannot verify the two builds are identical. Safe only when the versioned release is the *same binary*
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
orkctl release rollback "$OLD"
```

```text
release 2026.08.16-a1b2c3d is current again (was 2026.08.23-c4d5e6f).
New runs start on 2026.08.16-a1b2c3d; the 6 runs executing on 2026.08.23-c4d5e6f finish there.
Keep 2026.08.23-c4d5e6f running until "orkctl release wait 2026.08.23-c4d5e6f" exits 0.
```

`rollback` is `promote` aimed only at a release that was current before; it
refuses a candidate (use `promote`) and runs the same check, so it is refused
the same way if v1's Orkers are already gone.

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
per-execution versioning override. It stays a raw cluster command on purpose:
it moves one run onto a build that did not start it, which OrkStack never does
on its own.

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

**Rolling back past a release that added Orkflow constructs** (fan-out,
composition, `outputs:`, join policies, caching, schedules) needs the
preparation in §3, "Releases that add Orkflow constructs" — for join policies
it is a hold, because the silent outcome is destructive.

**A release that lets runs carry on into a fresh history** ships after the
release that reads them (below), and the default turns on only in a release
after one that already contained the writer. Rolling back below the writer,
but not below the reader, while runs are carrying on fails those runs
explicitly, `RUN_STATE_UNSUPPORTED` with a terminal event: an older release
refuses a saved-state format it does not know. Turn
`engine.continuation.enabled` off first and let runs that carry on finish
(the check below lists them).

**Never roll back below the first release that reads runs carried on into a
fresh history.** That release (the read side of continue-as-new) must ship
before any release that lets a run carry on, and stays the floor:

- a release older than it that picks up a run's later generation ends that
  generation before any side effect — the run fails, and every reader of this
  release or later reports `RUN_STATE_UNSUPPORTED` ("this run was carried
  forward by a newer release and an older release could not carry it on; roll
  the engines forward") — but it publishes no terminal event, so the run has
  no archive body and becomes unreadable once the cluster's retention expires
  it;
- once any run's archive records a lost range of its events (a `gap` line,
  `docs/STORAGE.md`), a rollback below it is forbidden permanently: an older
  release refuses the unknown archive line, so every read of that run fails.

Check before rolling back:
`temporal workflow list --query 'WorkflowType="RunWorkflow" AND ExecutionStatus="ContinuedAsNew"'`
lists the closed generations of runs carrying on; while it returns any, do
not go below that release.

**Before rolling back below the release that raised the fan-out maximum to
10,000 items**, lower `engine.each_max_items` to at most 1,000, then wait
until every run created before that change has finished: every running run
`orkctl run list` shows has a later `created_at` (a run that carries on into
a fresh history keeps its `created_at`). An older release computes a larger
fan-out's per-item bounds differently, so it could fail such a run's replay.
Once a rolled-back process polls, the fleet check (§5) already refuses new
submits that need more than 1,000 items.

**Before rolling back below the release that passes large definitions by
reference** (R3), set `engine.definition_by_reference` off on every replica,
then wait until every run started with its definition by reference has
finished. An older release cannot read such a run's definition: it replays
the run's history into a nondeterminism error and leaves the run stuck. The
check: each such start logs `definition_by_reference=true` with its
`run_id` ("run starts with its definition passed by reference"); list those
run ids and confirm none is still running (`orkctl run get <id>`). Under
versioning a `RunWorkflow` is Pinned, so such a run stays on the build that
started it until it finishes; drain that build before decommissioning it.
Once a rolled-back process polls, the fleet check (§5) refuses new submits
that would need a reference.

**Before rolling back below the release that writes snapshot overlays** (R4),
set `engine.snapshot_overlays` off on every replica, then wait until every
run started while it was on has finished: an older release cannot read a
`{seq}.ovl` snapshot, so its reads of such a run's live state, and its
publishes of the run's events, would fail. The check: the plan of such a run
carries `snapshot_overlay` (`temporal workflow show` on a running run, its
first event's fourth input, which
`TestDeployR4_TheOverlayFlagIsInTheFirstEventsFourthInput` in
`internal/engine/replay_snapshot_overlay_test.go` checks against a recorded
history), and only runs created before the flag went off can; confirm, as for
R3, that none is still running. A finished run's archive
body is always full — its last snapshot is never an overlay — so it stays
readable on any release.

### Back to unversioned workers

To take a deployment out of versioned routing altogether — an escape hatch,
not a release step, and deliberately not an `orkctl release` verb — point its
Current Version at the unversioned workers:

```bash
temporal worker deployment set-current-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --unversioned \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
```

New runs then go to whatever unversioned pollers exist, so start those first;
`--allow-no-pollers` overrides the check if you have to do it in the other
order, and leaves new runs backlogged until an unversioned worker appears.
Runs already pinned to a version still need that version's workers.

### Underneath: the cluster commands

`orkctl release` is a thin layer over the cluster's own worker-deployment API:
one read of the deployment, its releases, the engine's channel and the run
index, plus the Orker inventory; then, for a write, exactly one call with the
cluster's own guards left on and the read's conflict token. Nothing is hidden
from an operator who wants the raw commands, and they are the path for
anything `orkctl release` refuses — a switch-live first promotion of an
identical build (§3.5), a ramp or promotion you have decided to force, a
per-run override, a return to unversioned workers.

The `temporal` CLI reads none of the `ORK_` variables, and its own defaults
are the namespace `default` on `localhost:7233`, so a bare command succeeds
against a namespace nobody deployed to. Hand it every coordinate:

```bash
export ORK_DEPLOYMENT_NAME=orkstack          # §1's default, stated once
export ORK_TEMPORAL_NAMESPACE=ork-main
export ORK_TEMPORAL_ADDRESS=localhost:7233   # the frontend of the cluster you mean

# what `orkctl release status` reads (plus the Orker inventory and run index)
temporal worker deployment list \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"
temporal worker deployment describe-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$NEW" \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"

# what `orkctl release promote` / `rollback` write (§1, §3 step 0 and 4, §4)
temporal worker deployment set-current-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$NEW" \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"

# what `orkctl release ramp` writes (§3 step 3)
temporal worker deployment set-ramping-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$NEW" --percentage 10 \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS"

# what `orkctl release wait` polls for (§3 step 5) — the cluster's half of it
temporal worker deployment describe-version \
  --deployment-name "$ORK_DEPLOYMENT_NAME" --build-id "$OLD" \
  --namespace "$ORK_TEMPORAL_NAMESPACE" --address "$ORK_TEMPORAL_ADDRESS" \
  | grep -i drainage

# the routing dispatch itself reads, per channel (§3 step 4)
go run ./test/tools/queue-routing \
  -namespace "$ORK_TEMPORAL_NAMESPACE" -address "$ORK_TEMPORAL_ADDRESS" \
  -queue ork-tasks:workflow -queue ork-tasks:activity \
  -want "$ORK_DEPLOYMENT_NAME.$NEW"
```

`describe-version`'s cluster-side drain verdict does not see an open pin: a
pin lives in its host process. `orkctl release wait` adds that check from the
Orker inventory; the raw command does not.

**The development and proof Temporal drains in seconds; a production cluster
keeps its defaults.** The cluster decides that a version that stopped taking
new runs has drained by counting the running pinned workflows still on it, on
a delay set by two dynamic-config values. Both default to three minutes:
`matching.wv.VersionDrainageStatusVisibilityGracePeriod` before the first
count, which gives a workflow that started on the version a moment before it
stopped being current time to reach visibility, and
`matching.wv.VersionDrainageStatusRefreshInterval` between the counts that
follow (Temporal server 1.31, which the development image pins). The
`temporal` service of `deploy/docker-compose.yml` passes
`--dynamic-config-value` for both, `10s` and `3s`, so every test that waits for
a release to drain waits seconds rather than minutes. The proofs read the same
image and command from that service for the Temporal they start for
themselves (`proofrun temporal-args`, docs/PROOFS.md), so the settings reach
them too. They are development and proof settings, never a recommendation:
a shorter grace period can report a version drained while a workflow that
started on it has not yet reached visibility, and every refresh is a
visibility query and a new run of the version's own workflow on the cluster.
The quickstart's Temporal (`deploy/quickstart/compose.yaml`) and every
production cluster keep the defaults.

**Permissions.** A cluster behind an authorizer must grant the engine's
identity `DescribeWorkerDeployment`, `DescribeWorkerDeploymentVersion`,
`SetWorkerDeploymentCurrentVersion`, `SetWorkerDeploymentRampingVersion` and
`CountWorkflowExecutions` on its namespace. Without them the three release
RPCs answer `permission_denied` and nothing else in the engine changes.

**Releases that predate the Orker inventory.** Their Orkers are not in
`orkctl orker list`: they still make their pool in use while their release is
current, but they cannot make a pool served, so rolling **back** to such a
release is refused and the raw command is the path; rolling forward from one
cannot see its pin hosts, so check pin hosts by hand for that rollout.

---

## 5. Ops notes

- **Two route families face callers outside the operator's boundary.**
  `POST /v1/triggers/{id}` starts a run, and
  `POST /v1/runs/{run_id}/awaits/{task_id}` (with `/reject` for a rejection)
  resolves an awaited task that declares `callback: true`, authenticated by
  that await's one-time bearer token. Both are plain JSON on the API port, and
  the same advice holds for both: what to expose, to whom and how fast is the
  gateway's decision — OrkStack queues nothing, retries nothing and rate-limits
  nothing on either. Every other route is the operator surface.
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
- **Listing runs is a question the cluster answers.** At boot each engine
  registers three search attributes on its Temporal namespace —
  `OrkflowNamespace` (`task.EnsureRunSearchAttributes`) and
  `OrkflowOriginKind`/`OrkflowOriginId`
  (`task.EnsureRunOriginSearchAttributes`) — and stamps every run it starts
  with them, so `run list -n payments`, `run list --filter origin.kind=TRIGGER
  --filter origin.id=trg-…` and an `origin.kind` filter on the runs page are
  visibility predicates over live runs rather than filters over a page of
  everyone's. That is a tenancy
  property: filtered afterwards, how far a list could see would be set by how
  busy the OTHER namespaces were. Finished runs come from the archive, whose
  run and origin index keys begin with the namespace (`docs/STORAGE.md`).

  The boot line reports each answer separately
  (`INFO connected to temporal … run_namespace_index=true run_origin_index=true`),
  so a cluster that refuses one pair keeps the other. A cluster that refuses
  the registration — the operator API behind an authorizer, a visibility
  store without custom attributes — still boots and warns once
  (`WARN visibility does not index …`, naming the attribute and the
  consequence); the reader then walks the whole index instead of trusting a
  predicate the cluster cannot apply, and a list that cannot finish that walk
  fails rather than returning a short one. A run started before
  `OrkflowNamespace` was stamped carries its namespace only in its memo: it
  is read by id while live and listed from the archive once finished. A run
  started before origins were recorded renders `unrecorded` and matches no
  origin filter. No setting controls any of this.
- **A double submit is settled by the cluster, not by a replica.** One
  `--idempotency-key` sent to two replicas at once returns one run id, because
  the claim is a Temporal workflow (`engine.IdempotencyClaimWorkflow`), not
  process memory. Retries are safe from any address behind the load balancer.
  A key is scoped to the ork namespace it was submitted into, so two tenants
  can both submit `nightly` and get their own run
  (`api.submission.claimKey`); unscoped, the second would be answered with the
  first tenant's run id and its work would never start.
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
- **Process hygiene.** Before any manual end-to-end, make sure no stray
  Orker of yours is still polling the namespace: a forgotten worker steals
  activities from every later run and looks like a code bug. Stop only what
  you started, by its PID — never by name or pattern (`AGENTS.md` §11.1). A
  pattern such as `/bin/orker$` matches every worktree's dev stack and every
  proof running on the host, and an unanchored `bin/ork` also matches the
  `orkctl` you are about to drive the deploy with. `make down` stops this
  worktree's dev stack, including any process whose executable is this
  worktree's `bin/ork` or `bin/orker`, and `scripts/proof-stop.sh
  proof/<name>` stops a proof you started.

### Health checks

`ork --healthcheck` and `orker --healthcheck` ask the process's own admin
listener whether it is ready, and exit with the answer. They exist for a
container that has no shell: the `ork` and `orker` images are distroless, so a
Docker `HEALTHCHECK`, a Compose `healthcheck:` or an exec probe has no `curl`
or `wget` to run. The binary makes the request itself. It starts nothing: no
engine boot, no Temporal connection, no store.

- **What it asks.** `GET /readyz` on `ORK_ADMIN_ADDR` (`telemetry.admin_addr`),
  the listener of §7. It reads its configuration the way a boot does:
  `--config PATH`, `ORK_CONFIG_FILE` and `--no-config` mean what they mean for
  the daemon, and an environment variable wins over the file. Give the check
  the daemon's configuration, and the two cannot disagree about the address.
  A listener bound to every interface (`:9464`, `0.0.0.0:9464`, `[::]:9464`) is
  asked on loopback, `127.0.0.1` or `::1`, because a wildcard address says where
  to listen and is no place to connect to. Any other host is asked as written,
  since a listener bound to `10.1.2.3` does not answer on loopback.
- **What it answers.** Exit `0` and one line on stdout, `ork: ready (http://…)`,
  when `/readyz` answers `200`. Exit `1` and one line on stderr, with the
  reason, for everything else: `503 Service Unavailable: not ready` while the
  process boots or shuts down, `connection refused` when nothing listens,
  `no answer within 3s`, or any other status. An unset `ORK_ADMIN_ADDR` is exit
  `1` naming the variable, because a process with no admin listener has nothing
  to ask. A random port (`:0`) is refused for the same reason: a check cannot
  know it. A bad argument, and `--healthcheck` with `--check`, are exit `2`.
- **What ready means.** It is the process's own claim, not a probe of its
  dependencies. `ork` is ready once its API is serving, and `orker` once its
  task, control and presence workers are all polling. It flips once at boot
  and once at shutdown: a Temporal or object storage outage that begins after
  boot does not make either unready.
- **Wiring it.** Set `ORK_ADMIN_ADDR` inside the container, on loopback if
  nothing outside should reach it (`ORK_ADMIN_ADDR=127.0.0.1:9464`; it is never
  published), and run the same binary with the flag and the same environment:

  ```yaml
  healthcheck:
    test: ["CMD", "/usr/local/bin/ork", "--healthcheck"]   # orker: /usr/local/bin/orker
    interval: 10s
    timeout: 5s
    start_period: 30s
    retries: 3
  ```

  `start_period` is the boot: a check that fails inside it does not count
  against `retries`. The check itself gives up after 3 seconds.
- **How long an Orker's boot can take.** An Orker is not ready until its boot
  checks pass, and one of them runs a container to prove the work root can be
  bound. On a healthy daemon that takes seconds. On a daemon that is
  overloaded or wedged it can take up to three times
  `orker.docker.bind_probe_timeout` plus a minute for removing what it gave up
  on (7 minutes at the default), and longer by `orker.docker.pull_timeout`
  when the probe image is not local, before the Orker refuses to start with
  `BIND_PROBE_TIMED_OUT` ([REASONS.md](REASONS.md)). A supervisor that gives
  up sooner (a Compose check whose `start_period` plus `interval` times
  `retries` is shorter than that, a start-up timeout in an init system)
  reports the Orker unhealthy or failed before it can say why. Size the
  allowance to that bound, or lower the setting to the longest start you would
  wait for, and read the Orker's log for the reason. `make up` waits 60
  seconds for the Orker and then stops with a pointer to `.run/orker.log`,
  where the refusal appears when the probe ends.

### Running the published images

Each release publishes `ork`, `orker`, `orkctl` and `ui` images to
`ghcr.io/orkstack`, for linux/amd64 and linux/arm64, beside the first-party
activity images ([UPGRADING.md](UPGRADING.md), "Release artifacts").
[`deploy/quickstart/compose.yaml`](../deploy/quickstart/compose.yaml) runs all
of them on one machine and is the worked example of what follows; it is
checked by `internal/dist`'s tests and run end to end by
`scripts/release-smoke.sh` before a release publishes anything.

| Image | Runs | User | Settings it needs in a container |
|---|---|---|---|
| `ork` | `/usr/local/bin/ork` | non-root | the API on `ORK_HTTP_ADDR` (default `:8080`, every interface), Temporal (`ORK_TEMPORAL_ADDRESS`), the store (`ORK_ARTIFACT_BACKEND=s3`, `ORK_S3_*`), `ORK_WS_ALLOWED_ORIGINS` naming the UI's origin, and `ORK_ADMIN_ADDR` for a health check |
| `orker` | `/usr/local/bin/orker`, with the `docker` CLI | root | the Docker socket and a work directory, below; Temporal and the store as for `ork` |
| `orkctl` | `/usr/local/bin/orkctl` | non-root | `ORK_SERVER`; its working directory is `/work` |
| `ui` | `node build` on port 5173 | non-root | `ENGINE_API_URL` (where its server reaches the engine) and `PUBLIC_ENGINE_WS_URL` (where a browser reaches the engine's WebSockets) |

**The Orker in a container.** The Orker starts each task's container through
the host's Docker daemon, so its container needs that daemon's socket
(`/var/run/docker.sock`). Whoever can use the socket can start a privileged
container: the Orker container is as trusted as root on its host
([SECURITY_MODEL.md](SECURITY_MODEL.md), "The Orker's own access to the
host"). Give the socket to the Orker and to nothing else. Three more rules
follow from the daemon reading paths on the host:

- **The work directory is the same path inside and out.** The daemon
  bind-mounts each task's workspace from `ORK_ORKER_WORKDIR` as a host path,
  so mount the host directory at that same path in the container
  (`-v /var/lib/orkstack/work:/var/lib/orkstack/work`). On a machine whose
  daemon runs in a virtual machine (Docker Desktop, colima), the directory
  must also be one the virtual machine shares; the Orker's boot check refuses
  one it cannot bind (`WORKDIR_NOT_BINDABLE`).
- **One `ORK_ORKER_ID` per Orker, set explicitly.** The default id is derived
  from the container's host name and work directory, and the identity lock
  lives in the container's own `/tmp`: two Orker containers on one daemon
  must each be given a distinct id.
- **A stop grace longer than a task's.** A stopped Orker cancels its running
  tasks, each with its own grace (30 s by default) before SIGKILL; give the
  Orker's container longer than that (`stop_grace_period: 45s`) and run it
  with an init (`init: true`), so a signal reaches the process.

**The activity catalog.** A release's `ork` resolves `uses: shell` and the
other first-party names by the tag of its release line, for example
`ghcr.io/orkstack/activity-shell:0.1` for every 0.1 release; that tag moves to
each patch release of the line. To pin the exact release instead, set
`ORK_ACTIVITY_CATALOG` to the images by tag and digest, as the quickstart's
`.env` does.

### The admin surface: what a deployment is made of, and who is serving it

Every operational read is available from the CLI, over Connect and in the
UI's Admin pages. All but two are answered from the cluster or the bucket, so
**any replica answers them and they all answer the same** (`AGENTS.md` §7).
System information and the activity catalog deliberately describe the replica
that answered.

| read | CLI | where the answer comes from |
|---|---|---|
| system information | `orkctl admin info` | **this replica's own build and configuration** |
| activity catalog | `orkctl activity list` | **this replica's own configuration** (`ORK_ACTIVITY_CATALOG`), resolved to pinned digests at apply time |
| maximum run retention | `orkctl admin retention get \| set` | `metadata/settings/retention-policy.json`; writable |
| image policy | `orkctl admin image-policy get \| set` | `metadata/settings/image-policy.json`; writable |
| namespaces | `orkctl namespace list [--filter F…] \| get \| create \| update \| delete` | the registry in the bucket, including each namespace's retention, admission, budget and policies; Admin → Namespaces pages and filters it, and a page filtered by name alone reads only the documents on screen |
| pool health | `orkctl pool list -n <ns>` | the cluster's poller information, plus the Orkers themselves |
| Orker inventory | `orkctl orker list \| get \| cordon \| uncordon \| drain` | the cluster's poller information, plus each Orker itself; nothing stores an Orker |
| releases | `orkctl release status \| ramp \| promote \| rollback \| wait` | the cluster's routing, drain verdict and run index, plus the Orker inventory; writable (ramp, promote, rollback). Needs the worker-deployment permissions (§4, "Underneath") |
| audit trail | `orkctl admin audit` | `metadata/audit/` |
| webhook deliveries | `orkctl webhook deliveries \| delivery \| redeliver` | `metadata/deliveries/` |
| usage | `orkctl stats usage` | the run archive and attempt records (`docs/STATS.md` §8) |

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
be inferred from this API. The pools a namespace's stored versions name are
remembered by each replica for 30 seconds, because collecting them reads every
stored orkflow's version catalog: an apply or delete through a replica is seen
by that replica at once, and a pool named only by an orkflow applied through
ANOTHER replica appears in this replica's pool list (and its stats scope) within
30 seconds. A namespace whose catalog could not be read is asked again on the
next call, never remembered. Temporal's task-queue description call requires a
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
orkctl orker list --filter pool=build
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
  non-retryable after 45 attempts — about five minutes — and the triggering
  task fails `PIN_UNAVAILABLE`, its message naming the cordon and its reason.
  That bound is the Orker's own, so it does not follow
  `engine.pin_creation_timeout`: a pool whose pin hosts are all cordoned fails
  a pin after about five minutes whatever that is set to.
- **Everything in flight** is untouched: running containers finish, and open
  pins keep executing their remaining members, their release, and their
  completion on this host.
- **The pool probe.** A cordoned Orker stops answering `DescribeOrker` on its
  pool channel (only its own control channel still answers), so a pool whose
  every Orker is cordoned reports its poller-derived count for the freshness
  window and zero after it.

`SIGTERM` stops polling and cancels in-flight work; the Orker does not cordon
itself on it. An operator who wants in-flight work to finish first cordons
and drains.

**Why the cordon lives only in the process.** An Orker is stateless and
disposable (`AGENTS.md` §5): a stored cordon would be durable state about a
process that may no longer exist, and a replacement reusing its id would
inherit a stale refusal. So nothing stores an Orker or its cordon — no
registry, no drain state, no server-side drain (`drain` is a loop in
`orkctl`). The inventory is read from the cluster: every Orker also polls a
**presence** channel nothing is dispatched to, so a cordoned Orker that has
stopped polling its pool stays listed for the whole drain, and a **control**
channel of its own, so a cordon or a describe reaches exactly one host rather
than whichever Orker of a pool picks it up. Both are internal names, two idle
long polls per Orker.

`ORK_ORKER_ID` must therefore be unique across the **whole deployment**: it
is the Orker's worker identity and names its control channel. Two Orkers
sharing one show as a single row whose `unavailable_reason` names the
collision, and a cordon could land on either; the derived default
(`<host>-<hash of the work root>`) is unique by construction. An Orker of a
release that predates the inventory polls neither channel: `orkctl pool list`
still counts it, `orkctl orker list` does not list it, and `orkctl orker
cordon` answers `NotFound`.

Every cordon and uncordon is an audit record (`orker.cordon`,
`orker.uncordon`, target the Orker id, the reason in its detail, no
namespace).

### Namespace admission: `hold` and `max_running`

Two fields of a namespace's own document decide whether it admits a new run.
Both are checked at one place — `SubmitRun`'s preparation, the gate every door
goes through (`orkctl run submit`, `run again`, the UI's submit dialog, a
trigger invoke, a schedule fire) — after the namespace and orkflow resolve
and before any pool is looked at, so `--allow-empty-pool` does not bypass them. A refusal is
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
- **A run that carries on into a fresh history is one run.** Admission counts
  it once even while the cluster's index briefly shows both of its executions,
  or only the one that just closed (§2, "Runs that span several executions").
  Below the cap two index counts answer; at or above it the submit walks the
  namespace's live runs, deduplicated by run id, so a switching run never
  refuses a submit at `max_running − 1` and is never counted as gone.
- **A keyed repeat is refused while held or at the cap.** Admission runs
  before an `idempotency_key` is looked at, so repeating a key whose first
  submission was accepted returns the refusal instead of that run while the
  namespace is held or full. The run the key names is unaffected and readable
  by id and in `run list`.
- **Rolling upgrade.** A release that predates admission discards both
  fields and admits, so during a rollout a hold is enforced only by replicas
  that know it; an older `orkctl` writing a held namespace back through
  `namespace update` drops the hold — run the `orkctl` of the release. The
  same holds for every later namespace field (budget, `allowed_secrets`,
  `require_signed_by`).
- **A hold is a namespace write**, so it is audited as `namespace.update`
  with `hold=`/`max_running=` in the detail; a refused submit writes no audit
  record (its count is the metric below).
- **Refusals are counted** on `ork.runs.refused` (§7, "Lifecycle metrics") and
  logged once per refusal at Info as `run refused`, with `namespace`,
  `orkflow`, `reason` (`NAMESPACE_HELD` or `MAX_RUNNING_REACHED`) and
  `hold_since` or `running`/`max_running`. `GetStats` does not report them:
  a refused submission is in neither record it derives from.

### Attributing control-plane writes

Every call of a control-plane **write** RPC writes one immutable record,
succeeded or refused, to `metadata/audit/{yyyy}/{mm}/{dd}/{id}.json` before
the response leaves the engine. One Connect interceptor over every handler
does it, so no handler can forget; the classification table is
`auditedProcedures` (`internal/api/audit.go`), and the recorded actions are
listed in [UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Control-plane audit trail"
(webhook writes are recorded over the legacy `/v1/webhooks` routes too).
Reads are never recorded, and neither is `SubmitRun` nor a trigger or await
callback invocation: a run's own record and origin are already the durable
fact, and an object per submit would make the trail a log pipeline.
Deployment-wide writes carry no namespace. `orkctl admin audit` and Admin →
Audit read the trail back, newest first, from any replica:

```
WHEN                  PRINCIPAL        ACTION            NAMESPACE      TARGET         RESULT
2026-03-14T11:02:44Z  svc-platform-ci  retention.update  -              deployment     ok
2026-02-28T16:44:58Z  unattributed     namespace.delete  staging        staging        refused: failed_precondition

Window: 2026-03-07T11:02:44Z to 2026-03-14T11:02:44Z (exclusive), newest first.
Every namespace unless a namespace filter narrows it.
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
- **No secret is ever recorded.** The extractors never read a webhook
  signing secret or a trigger token.
- **The write is best-effort.** An audit store that refuses the write never
  changes what the write RPC returns — an audit outage must not stop an
  operator holding a namespace mid-incident: the engine logs `audit record
  dropped` at warn (action, target, principal, error) and counts
  `ork.audit.records{outcome="dropped"}` (§7 "Metrics").
- **It is a record, not a mechanism.** Nothing in OrkStack reads an audit
  record to decide anything, and nothing is notified when one lands. There is
  no hash chain: immutability comes from create-only writes plus the bucket's
  own object-lock or versioning policy, which is the operator's.
- **Nothing reclaims the trail.** The cleanup worker never touches
  `metadata/audit/`; a bucket lifecycle rule on that prefix is the operator's
  retention for it. There is no setting: a deployment that upgrades gets
  records, unattributed until something asserts a principal.
- **A listing is bounded.** The default window is the seven days before now;
  `at>=`/`at<` filters choose another, and `action^`, `principal=` and
  `namespace=` filters narrow it (`namespace=` alone selects the
  deployment-wide writes; docs/UI_CLI_PARITY.md "Filtering lists"). Pages hold
  50 records by default, 200 at most. A page that
  would examine more than 5000 records before filling is refused
  (`resource_exhausted`) rather than answered from the subset it read —
  narrow the window, and page a long compliance range month by month.

### Namespace budgets: reserved consumption, soft or enforced

A namespace can carry a **budget** (`Namespace.budget`): a bound on reserved
cpu-seconds and/or GiB-seconds over a rolling `period` (1h–744h). The units
are each attempt's reservation — the cpu and memory the Orker granted it, its
defaults and caps applied (`TaskAttempt.resources`) — multiplied by how long
the Orker held it. They are reservations, never measured utilization
(`docs/STATS.md` §8).

```bash
orkctl stats usage --since 720h --group-by namespace          # who reserved what, this month
orkctl namespace update analytics --budget-cpu-seconds 5000000 --budget-period 720h          # soft: reported only
orkctl -n analytics stats usage --since 720h                  # the gate's own question: does it answer?
orkctl namespace update analytics --budget-enforce            # now it refuses
orkctl namespace update analytics --budget-enforce=false      # back to reporting
orkctl namespace update analytics --clear-budget              # unbounded
```

- **Soft (the default) costs nothing at submit.** It is reported on every
  read — `orkctl stats usage --since <period>`, the dashboard Usage section, Admin
  → Usage — and never refuses anything.
- **Enforced** is the third reason inside the one admission gate, after the
  hold and the cap: a submit measures the namespace's usage over the trailing
  period (shared with `GetUsage`, cached for `ORK_USAGE_CACHE_TTL`) and is
  refused `FailedPrecondition` with `BUDGET_EXCEEDED` once a bounded dimension
  is at or above its bound, naming the usage, the budget, the period and both
  fixes. An accepted run is never touched. A trigger invoke is `412`, a
  schedule fire is refused, `run again` is refused, exactly as for a hold.
- **An enforced budget that cannot be measured refuses** with
  `BUDGET_UNMEASURABLE` — the period holds more finished runs than a usage read
  is bounded to (10000), or no archive or attempt store is configured. Admitting
  on a measurement nobody took would be the recovery magic `AGENTS.md` §14
  ranks below explicit failure. Before `--budget-enforce`, check that `orkctl
  stats usage --since <period>` answers; shorten the period if it does not. A
  read that merely failed (the bucket wobbled) is `Unavailable`, ask again.
- **The margin, stated.** Enforcement is a read-then-act with no lock, so it is
  best-effort by the sum of: consumption of runs that finished within one
  `ORK_USAGE_CACHE_TTL` of the measurement; every run still in flight (counted
  when it finishes); submits admitted between one measurement and the next;
  and, when records are missing, the consumption they would have shown. A lock
  would remove it, and `AGENTS.md` §2.1 forbids building one.
- **Missing evidence only understates.** A period longer than a namespace's run
  retention, with `ORK_CLEANUP_ENABLED=true`, measures only what cleanup has not
  reclaimed — `coverage begins …` shows where — so an enforced budget can admit
  where a complete count would refuse, never the reverse.
- **No alert, no dollar figure.** A soft budget is visible on every read
  and notifies nothing: "already warned" would be stored state. Alerting
  ahead of a threshold belongs to the telemetry pipeline
  (`ork.orker.task.duration`, labelled by namespace and pool). OrkStack knows
  what it reserved, never what it cost.
- **Rolling upgrade.** An Orker that predates reservations writes records
  without one (counted in task-seconds, reported as `attempts carry no
  reservation`). An `ork` that predates budgets discards `budget`, admits,
  and answers `GetUsage` with `Unimplemented`, so during a rollout an
  enforced budget is enforced only by replicas that know it.
- **Refusals are counted** on `ork.runs.refused` with
  `ork.refusal.reason="budget_exceeded"` or `"budget_unmeasurable"`, and logged
  once as `run refused` with `period`, `cpu_seconds_used`,
  `cpu_seconds_budget`, `gib_seconds_used`, `gib_seconds_budget` and
  `measured_at` (or `period` and `error`).

### Run retention: a deployment maximum, a value per namespace

OrkStack keeps Temporal as an implementation detail an operator never has to
think about (AGENTS.md §7) — including here. What an operator sets is **run
retention**: how long a finished run stays readable before OrkStack retires
it and reclaims its run-scoped storage (logs, attempt records, snapshots,
completion commits; the archived run record itself is kept).
There are two knobs, and they compose:

- **A deployment maximum** (`orkctl admin retention get|set <duration>`,
  `SystemService.GetRetentionPolicy`/`UpdateRetentionPolicy`): the ceiling no
  namespace may exceed, and what a namespace that sets nothing inherits.
  Stored at `metadata/settings/retention-policy.json` (not a live cluster
  read), so any replica answers the same.
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

**Enforcement needs `ORK_CLEANUP_ENABLED=true`** (§1, "Retention cleanup and
bucket lifecycle rules"): the cleanup worker is what actually retires a run at its
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
leaves the process. Lowering the maximum first reads the one in force, states
what the next cleanup pass removes (every finished run older than the new
maximum, in a namespace without its own run retention, loses its logs,
attempt records, snapshots and commits) and asks for confirmation; a script
passes `--yes`. Raising it asks nothing:

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
one. The page's field starts at the maximum in force. Raising it applies at
once; lowering it opens a confirmation that names the current and the new
maximum and what the next cleanup pass reclaims (the logs, attempt records,
snapshots and commits of finished runs older than the new value, in every
namespace without a run retention of its own), and it applies only once the
new value is typed into it. Cancel sends nothing.

**First boot.** With no stored policy, the engine stores the cluster's
current retention TTL (72h on a fresh cluster) as the maximum, unchanged —
nobody is opted into shorter retention by upgrading — then raises the
underlying TTL by the margin. It must store the value rather than re-derive
it each boot, or every restart would raise the TTL by another margin.
Enabling `ORK_CLEANUP_ENABLED` for the first time moves retirement from the
backstop TTL to the moment each run crosses its own namespace's retention:
sooner, by up to the margin.

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
  allowed scheme. A malformed entry, or one
  naming a scheme outside `orker.secret_schemes`, stops the Orker at boot.
- **The tenant's list, per namespace:** `orkctl namespace update NAME
  --allow-secret 'env:SANDBOX_*'` (or Admin → Namespaces). It is enforced by
  the engine at validate and apply, as an issue on
  `tasks[i].secrets[j].source`, and again at every submit — `run submit`,
  `run again`, the UI, a trigger invoke, a schedule fire — before a run id is
  minted, so a definition applied before the list existed is refused at its
  next submit. It is never retroactive: a run already accepted finishes on
  the definition it pinned; stop one with `orkctl run cancel`. Empty (or
  `--clear-allowed-secrets`) means unrestricted. The list never travels to
  the Orker: a list handed to the Orker by its caller would be no check on
  the caller, and the engine's submit gate is the one no client bypasses.

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
replicas that know it; the Orker floor is unaffected by the rollout.

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
  every deployment until an operator writes one — permits everything.
- **The tenant's tightening, per namespace:** `orkctl namespace update NAME
  --require-signed-by cosign:KEY` requires every image executed in the
  namespace to carry a valid cosign signature by that key. A namespace can
  tighten the floor and has no field with which to loosen it. A key a
  namespace names cannot be removed from the floor until the namespace is
  cleared (`--clear-require-signed-by`).

The policy is checked at three doors: at validate and apply (an issue on
`tasks[i].uses`), at every submit — `run submit`, `run again`, the UI, a
trigger invoke, a schedule fire — before a run id is minted (`failed_precondition`, so a
definition stored before the policy tightened is refused at its next
submit), and by the Orker before it builds a workspace or pulls, against the
policy the run pinned at submit. A run keeps the policy **and the key** it
was submitted under: tightening the floor, moving a namespace's signer or
rotating a key never reaches a run already accepted; stop one with `orkctl
run cancel`. A committed attempt is adopted without being judged again.

Verification is one fixed scheme — key-based cosign signatures over the
simple-signing payload; no keyless signing, transparency log, attestation or
policy language. The Orker verifies the signature itself, never taking a
caller's word that it was checked, with its own registry client:
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
replicas that know it, and a run submitted through an older replica carries
no policy for the Orker to check; the window is the rollout.
Every image digest that ever ran is already recorded per attempt
(`orkctl run attempts`, the `TaskAttempt.image` of each attempt record) —
that is the audit read, and nothing new is written for it.

---

### A run that pauses new tasks

Two things make a run hold NEW tasks — never running ones — exactly as a
saturated pool would:

- **The in-flight cap** (`engine.run_max_in_flight`, 1,000 by default): a run
  keeps at most that many activities, fan-out items, child runs and pins
  pending, and starts more as they finish. It never holds while nothing is in
  flight, and an open pin's own needs are exempt below an absolute 1,900, so it
  can never stall a run. Without it, a run above the cluster's 2,000 pending
  activities per execution would fail every workflow task — stuck. A run that
  keeps more than 1,000 activities pending at once now waits at 1,000.
- **Carrying on into a fresh history** (`engine.continuation.*`, on by
  default). Past its budget (8,192 events or 8 MiB of growth)
  a run carries on at its next moment with nothing running — the usual case
  for chains, staged DAGs and runs parked on awaits, which never pause. A run
  with no such moment (a fan-out that keeps refilling) holds new tasks once its
  history plus what its failure path would still need reaches 60% of the
  cluster's limit, for as long as the running work takes — bounded by that
  work's own timeouts, never by a clock of its own — then carries on. An open
  pin whose remaining work waits on an await makes the hold give up (the run
  tries again after more growth). A run that can never reach such a moment
  stops `RUN_HISTORY_LIMIT` before the cluster's limit, its message naming the
  pin and the await or the size of its saved state.

How to read it: the engine logs `run reached its in-flight cap`, `run will
carry on into a fresh history at its next idle point`, `run pausing new tasks
so it can carry on into a fresh history`, `run resumed new tasks: it cannot
reach a moment with nothing running`, `run carried on into a fresh history`
(with `hold_seconds`) and `run restored its saved state`, each once per
decision with `run_id`, `history_events`, `history_bytes` and `in_flight`;
the `ork.run.continuation*` metrics (§7); and `orkctl run inspect`, which
shows a held task with no attempt. A composed child that awaits for days holds
its parent's new tasks for that long if the parent pauses meanwhile.

### Task ceiling and the flat-stage bound

`engine.orkflow_max_tasks` (§1) is 4,000 by default: the largest size, of 4,000,
3,000 and 2,000, at which the task soak's every shape — a flat stage of long
and of short tasks, a staged DAG of 250 lanes and deep lanes of at most 380
tasks, all plain tasks with small outputs — met every threshold below. What
"measured" covers is exactly that list; nothing else is promised at 4,000:

- each generation's peak history under 80% of the cluster's limits, events
  and bytes; peak pending within the in-flight cap; snapshot bytes written
  under 1 GB per run; the archive identical to the stream;
- every generation replaying within 30 s with the deadlock detector on, and
  no live deadlock-detector trip;
- the closed form's per-task costs (below) and each generation's peak of
  history plus reserve within its prediction;
- `GetRun` p95 within 2 s live and from the archive, and the whole event
  stream (`Events(0)`) within 30 s, in every shape;
- the run page of a flat run at 4,000 tasks interactive within 5 s, live and
  finished.

These measurements come from the task soak, which runs in the nightly and
pre-release proofs, not in every merge proof (docs/PROOFS.md).

The read path measured is `GetRun`, the event stream and the run page. Two
other readers of the same snapshot are not thresholded: a `ListRuns` page of
archived runs reads one whole archive body per listed run (several MB at
4,000 tasks), and a webhook body carries every task row.

Above 4,000 a deployment runs unmeasured, and the boot warns (`engine.orkflow_max_tasks
(ORK_ORKFLOW_MAX_TASKS=…) is above the largest size measured (4,000): this
deployment runs unmeasured`): building a run's state, a pass's workflow code,
snapshot traffic, `GetRun` and the run page are then unmeasured. The first two
run under the 1 s deadlock detector, and a trip there is a stuck run; the last
two may be slow.

**The flat-stage bound.** Independently of the measured size, a run's shape
and the deployment's settings bound how many plain tasks it may declare
before its first history can reach the history guard (`RUN_HISTORY_LIMIT`,
§1 `engine.history.*`). A generation cannot carry on until it has grown by
its budget, and until its hold starts nothing stops it dispatching, so the
first generation is the worst. With L the cluster's event limit, F, p and f
the reserve's fixed, per-pending and per-in-flight terms (96, 8 and 16), G the
continuation budget's events (8,192), B a generation's base — the bootstrap's
worst case (`RestoreHistoryBound`, 1,478 at the defaults, because generation 0
too reads its definition from the store) plus 24 for generation 0's own start
— and d, c and u the measured events of one plain task's
dispatch (14), completion (12) and the part of a completion recorded when it
lands before the run collects it (5):

```
r₁ = d + f − p = 22        a dispatch: history plus reserve
r₂ = d + c − p = 18        a completion and the dispatch it unblocks
Δ(W) = (r₁ + u)·⌈G/d⌉                          when d·W ≥ G
Δ(W) = r₁·W + r₂·⌈(G − d·W)/(d + c)⌉ + u·W     otherwise
X(W) = ⌊(L − 1 − B − F − Δ(W)) / p⌋
```

W is the most tasks the shape keeps in flight (at most `engine.run_max_in_flight`).
The u term is the landings: the run dispatches every ready task in one pass
and collects completions only afterwards, so with short tasks every running
task's completion can land, uncollected, during the last dispatch before the
budget is due. At the defaults:

| Shape | W | Δ(W) | X |
|---|---|---|---|
| flat: every task needs nothing | 1,000 | 15,822 | **4,222** |
| staged: 250 lanes | 250 | 10,008 | 4,949 |
| deep: lanes of at most 380 | 15 | 5,931 | 5,458 |
| one chain | 1 | 5,697 | 5,488 (but rule 44 and `INPUT_TOO_LARGE` bound a chain far sooner) |

Any shape at least ⌈G/d⌉ = 586 wide has the flat bound, so the flat stage
binds: X is 4,222 at the defaults. It moves with the settings:
`engine.continuation.events` at its maximum `max_events/4` (12,800) gives
3,112; `engine.run_max_in_flight` 300 gives 4,841;
`engine.continuation.restore_timeout` at 8 h gives 3,041. The boot computes X
for the configured settings and warns when `engine.orkflow_max_tasks` is above
it, naming `engine.history.max_events`, `engine.continuation.events`,
`engine.continuation.bytes`, `engine.continuation.restore_timeout` and
`engine.run_max_in_flight`.

What X does not cover: it counts plain tasks only — awaits (24 reserved each,
outside the in-flight cap), pins, fan-outs, composed tasks and failure
handlers keep more back and trip the guard far below X; and it is a bound in
events only. The bytes form of the same arithmetic is derived, not measured,
and above the events form at the defaults (6,491 for 16 KiB dispatches in a
flat stage, 8,389 for 256 KiB in a deep one). A run within X can still stop,
explicitly, on history bytes, on its saved state's size (a saved state holds
every task's inline output: at 4,000 tasks it fits 8 MiB while tasks average
about 2,368 bytes of inline values or less), or on large inline outputs. X is a
size to stay within, never a promise that a run of that size completes.

### Engine capabilities and the fleet check

Some runs need an engine newer than the one that could otherwise decide
them: an `ork` process of an older release would compute that run's commands
differently, and the first newer process to replay its history would fail it
— a stuck run. Before such a run starts, the submit asks the cluster which
processes could decide it, and refuses while any of them is too old.

- **What each process advertises.** Every `ork` process polls Temporal with
  its SDK identity followed by ` ork-caps=<list>`, for example
  `48213@engine-2@ ork-caps=each-items-10k,definition-ref,snapshot-overlay`, as
  `temporal task-queue describe --task-queue ork-tasks` shows it. The run
  worker and the projection worker share the process's client, so both carry
  it. The releases page still counts one process per identity.
- **Which pollers a submit checks.** The engine channel's fresh pollers of
  this deployment that a run started now can land on: under versioning, the
  current build's (the unversioned processes when no current version is set)
  and the ramping build's; on an unversioned fleet, every process. A build
  that keeps polling only to drain its pinned runs blocks nothing — it never
  decides a run started now. Another deployment's engine sharing the
  namespace is ignored.
- **Which capabilities exist.** `each-items-10k`: a run tree in which some
  run declares more than 1,000 fan-out items (`engine.each_max_items`, §1).
  `definition-ref`: a top-level run whose planned definition is over the
  256 KiB a start may carry, with `engine.definition_by_reference` on (§1).
  `snapshot-overlay`: a run tree in which some run declares more than 256
  rows, with `engine.snapshot_overlays` on (§1); it also checks every process
  polling `ork-engine-events`, and never refuses — a tree it cannot be sure of
  writes full snapshots (§5, "Snapshot overlays"). Only such a submit — a
  direct submit, a trigger or a schedule fire — reads the poller list, once,
  never cached.
- **What a refused submit says.** `FAILED_PRECONDITION`, naming the Orkflow,
  how many items it declares and how many engine processes run a release
  that cannot run them ("2 engine processes … finish the rollout first") —
  a count, never an identity, because identities carry host names; for a
  definition too large to start, today's 256 KiB refusal followed by "2
  engine processes run a release that cannot yet read a definition this
  large". The identities are in the engine's own `missing_capability`
  warning (`identities`).
- **The reader side.** An older process can still serve API requests while
  a rollout drains, and cannot read a definition passed by reference. Roll
  out from a release that has the setting (it is on by default only in the
  release after the one that first had it), or turn
  `engine.definition_by_reference` off until every replica runs it (§1).
- **Fail closed.** A poller list that cannot be read refuses the submit
  `UNAVAILABLE`; ask again once the cluster answers.

### Snapshot overlays

With `engine.snapshot_overlays` on (§1), every event of a run that declares
more than 256 rows, except its last, names a `{seq}.ovl` overlay instead of a
full `{seq}.pb` snapshot (`docs/STORAGE.md`): the run's header and the rows
that differ from the last full snapshot it wrote, which the overlay names by
URI and by the SHA-256 of its bytes. A reader takes at most two reads — the
overlay and its base — and verifies the base against that digest.

- **The base cache.** Each `ork` process keeps the full snapshots overlays are
  written against in one cache, shared by the run worker that writes them,
  the projection worker and the read API: at most 8 bases or 128 MiB of
  snapshot bytes, least recently used first out. The writer caches every full
  snapshot it writes, so its own overlays read nothing. A process serving
  more large runs at once than the cache holds takes the miss path for each:
  every overlay write and every projection of an event re-reads its base
  (several MiB for a run of thousands of rows) besides the overlay, so the
  write traffic still drops while read traffic can rise above one full read
  per event. At the 4,000-task ceiling a saved state averages about 2,368 B
  per task, so a base is about 9.5 MB; the 8-entry cap therefore binds before
  the 128 MiB one, and at most eight large runs' bases stay cached per engine
  replica. The saving quoted for overlays — snapshot bytes written for a
  1,000-task run falling about tenfold — is for a cache that holds the run's
  base; plan engine memory for the decoded bases (a few times their bytes).
  That figure comes from the overlay bytes test at 1,000 tasks, which runs in
  the nightly and pre-release proofs, not in every merge proof
  (docs/PROOFS.md).
- **The fleet check.** Overlays are written only by runs whose submit found
  every `ork` process polling `ork-engine-events` — every one, whatever its
  build, because that channel is unversioned and any process may project any
  run's event — and every process of a build that new runs start on,
  advertising `snapshot-overlay` (§5, "Engine capabilities and the fleet
  check"). Otherwise new runs write full snapshots, never a refusal, and the
  engine logs "new runs write full snapshots, not overlays" with the
  `identities` lacking it, at most once a minute per process. Under
  versioning that keeps overlays off until the older build's processes are
  decommissioned, not merely drained. Only a submit whose run tree has a run
  over 256 rows reads the poller list, twice (the engine and projection
  channels).
- **A late write.** A failed attempt of an event's snapshot write can land
  after its retry and replace a full `{seq}.pb` that overlays were already
  written against. Those overlays then cannot be read: a live `GetRun` of
  that run answers `UNAVAILABLE`, never a mixed or degraded run, until the
  run's next full snapshot, at most 64 events later — sooner when the writer
  misses its cache and sees the change. A process whose cache still holds
  the verified bytes answers correctly. The run itself, its last snapshot
  and its archive are unaffected.
- **Never expire a `{seq}.pb` with a lifecycle rule while its run lives**:
  overlays name it (§1, "Retention cleanup and bucket lifecycle rules").

### Definitions passed by reference: the objects no run reads

With `engine.definition_by_reference` on (§1), a top-level run whose planned
definition is over the 256 KiB a start may carry has that definition written
under its own snapshot prefix (`continuations/000000-definition-{sha256}.pb.gz`,
`docs/STORAGE.md`) immediately before its start, and every history of the run
copies it in, sharing the one copy bound per history with the run's saved
state (`engine.continuation.bytes`; the definition takes at most a quarter of
it, compressed). Run cleanup reclaims it with the run's prefix. Three kinds of
such an object belong to no run that started, so no cleanup reaches them:

- the engine died between the write and the start;
- the cluster refused the run id as taken, and the submit started the run
  under a fresh id with its own copy;
- a schedule fire's preparation was retried after its write (only when the
  preparation's own result was lost), and minted a fresh id.

Each is at most a quarter of `engine.continuation.bytes` (2 MiB at the
default), never read, and immutable. A bucket lifecycle rule must never
expire `continuations/` (§1, "Retention cleanup and bucket lifecycle rules"),
so sweep them by hand when you want the space back: an object
`metadata/snapshots/{ns}/{run-id}/continuations/000000-definition-{sha256}.pb.gz`
whose run id `orkctl run get -n {ns} {run-id}` answers `NotFound` for, older
than the longest a submit takes (minutes), is one of these. The rule finds
the first and third kinds always, and the second when the run that held the
id belongs to another namespace. When that run belongs to the same
namespace, `orkctl run get` finds it, so the rule never names the object, and
none is needed: it lies under that run's own prefix, and that run's cleanup
reclaims it with the rest of the prefix. Never delete an object whose run id
`orkctl run get` finds — that run's own definition is stored the same way.

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
| `TestRunWorkflow_ReplaysCommittedJoinHistory` | the replayer against the join fixture (`run-join-v1.json`, `examples/branch-and-recover.ork.yaml` with a failing `deploy`), plus the order the replay schedules tasks in | a changed join decision, a failure handler dispatched in a different place, a moved failing note or `RUN_FAILED` |
| `TestJoinHistory_ActivityPayloadsMatchWhatCurrentCodeSends` | the join fixture's payloads, re-executed from its own input | a changed `ExecuteTaskInput.join` or failed-need reference, a reworded skip, cancel or failing sentence |
| `TestJoinHistory_CatchesAHandlerOnTheRunContext` | the replayer, against a body that dispatches failure handlers on the run's own context — it must FAIL | the join guard going blind to where a handler's activities are scheduled |
| `TestCommittedFixtures_DeclareNoJoin` | every other fixture's `RunWorkflow` input | a fixture that declares `join:`, which would void the premise that no earlier history reaches a join branch |
| `TestCommittedHistory_LeanEventPathWithoutItsGateIsCaught` | the replayer, against `run-succeeded-v1.json` and `run-each-v1.json` with the lean event path forced on without its `lean-event-path` version check — it must FAIL | the gate going missing: those fixtures predate the version, so they replay only because it sends them down the old path |
| `TestRunWorkflow_ReplaysCommittedLeanHistories`, `TestLeanHistory_CatchesASnapshotWrittenForTaskStarted`, `TestLeanHistory_TaskStartedCarriesItsScheduledURI` | the lean-path fixtures (`run-succeeded-v2.json`, `run-each-v2.json`): the command stream, a body that writes `TASK_STARTED` its own snapshot again (it must FAIL), and the recorded URI reuse | a change to the lean event path itself (a missing fixture skips, and `ORK_REQUIRE_LEAN_FIXTURES=1` makes that a failure) |

**The lean event path is a versioned change.** A run that records the
`lean-event-path` version publishes a task's `TASK_STARTED` with the snapshot
its `TASK_SCHEDULED` just wrote (nothing changes between the two), and
publishes a fan-out's snapshot-less item events without waiting for each — at
most 32 outstanding, every one awaited before the run's terminal event. A run
started before that version carries no marker, so it keeps the old command
stream for its whole life, on any build. Events, their sequence numbers and
the stream a reader sees are the same on both paths.

The replayer never looks inside a payload: adding a field to
`task.ExecuteTaskInput` replays perfectly clean, and an Orker of the recorded
build would silently drop it. That is why the third guard exists.

**Continuing as new and the in-flight cap are versioned changes too.** A run
that crosses its continuation budget records the `continue-as-new` version
there, and a run the in-flight cap first holds records `run-in-flight-cap`;
both are asked lazily, so a history recorded before them replays their
`DefaultVersion` branch, and a later generation never asks a decision its
carry already holds. Every number that decides one of these commands — the
budget, the hold line, the chunk and batch sizes of the next history's read,
the restore policy, the cap — is pinned in the run's plan at submit, never
read from a code constant, so a release that changes a default moves only new
runs. The fixtures `run-continued-g0-v1.json` and `run-continued-g1-v1.json`
(one run recorded across a switch, with the three objects it wrote) and
`run-in-flight-cap-v1.json` guard them, each with its command, payload and
negative tests (`internal/engine/testdata/history/README.md`); a carried
generation is replayed as the execution of the run's own workflow id, which
is where it reads its run id from.

**A definition passed by reference is a versioned change.** A run whose
plan names its definition by reference (§1, `engine.definition_by_reference`)
asks the `definition-by-reference` version once, in its first history, before
anything else, and every later history reads that decision from what the run
carried forward, never asking again; it then copies the definition into its
history in the pinned chunk and batch sizes of its continuation budget. A
history recorded by a build that ignored the plan field replays its
`DefaultVersion` branch: the start's stub has no tasks, and the run ends
`INVALID_ORKFLOW` as that build recorded. The fixture `run-definition-ref-v1.json`,
with the definition object its recording wrote, guards it with its command,
payload and negative tests (`internal/engine/testdata/history/README.md`).

**Snapshot overlays are a versioned change.** A run whose plan carries the
overlay policy (§1, `engine.snapshot_overlays`) and that declares more rows
than the policy's floor asks the `snapshot-overlay` version once, at its
first snapshot before its last event, and carries the decision forward; with
it, each such snapshot is written by the `WriteSnapshotOrOverlay` local
activity instead of `WriteSnapshot`, from either place a snapshot is written
(an event's own, and a fan-out's periodic item snapshot). Which of the two
runs is decided by the pinned floor, the run's declared rows and the event
type only; whether the activity wrote an overlay or a full snapshot is its
recorded result, which a replay reads, so a replay never diffs. A history
recorded before the gate replays its `DefaultVersion` branch, writing every
snapshot in full. The fixture `run-snapshot-overlay-v1.json`, with the
snapshots and overlays its recording wrote, guards it with its command,
payload and negative tests (`internal/engine/testdata/history/README.md`).

**The guard's lookahead at a dispatch is a versioned change.** Before each
dispatch the guard asks whether the run could still end cleanly once that
dispatch's own command — a task's input, up to the plan's values bound; a
child run's start; a pin's session — is recorded, not only as its history
stands; a run that could not stops `RUN_HISTORY_LIMIT` there instead of
dispatching and leaving its failure path less room than it keeps back. It
records the `run-history-guard-lookahead` version, asked only where the
lookahead decides otherwise; a history recorded before it replays its
`DefaultVersion` branch, which dispatched. The fixture
`run-history-guard-lookahead-v1.json` guards it.

**A cancel seen at a dispatch is a versioned change.** A run whose operator
cancel reaches it in the same workflow task as the completion — or the
refused switch — that made its next task ready ends canceled there, starting
nothing more, with no gap in its stream; it records the
`cancel-before-dispatch` version, asked only when its context is already
canceled at a dispatch point. A history recorded before the check replays its
`DefaultVersion` branch, which dispatched the task. The fixture
`run-cancel-before-dispatch-v1.json` guards it, with its command, payload and
negative tests.

**A refused route is a versioned change.** A task whose pool the run's plan
cannot route (`INVALID_POOL`, `RESERVED_QUEUE`) is now recorded `FAILED`, with
that code as its own reason and a `TASK_FAILED` event, so the stream and the
snapshot agree with the run's failure. It records the `refused-route-task-failed`
version, asked lazily where the first refusal happens: a run that never refuses
a route records no marker. A history recorded before the gate replays its
`DefaultVersion` branch, which returned the error alone and left the row as the
old failure path recorded it; version 1 is the new path. The fixtures
`run-refused-route-{plain,pinned,handler}-v1.json` (a plain dispatch, a pin's
opening, and a definition with an `any_failed` handler) and
`run-refused-route-legacy-{plain,pinned,handler}-v1.json` (the same three,
recorded with the gate off) guard it with command, behaviour and negative
tests (`internal/engine/testdata/history/README.md`); a missing fixture fails
those tests, it never skips them.

**A pin's creation timeout is a versioned change.** A run's plan carries
`engine.pin_creation_timeout` in whole seconds, pinned at submit and copied
into each child run's start, and the engine uses it as the wait of every pin
the run opens. It asks the `pin-creation-timeout` version once, where the
run's first pin opens: a run that opens no pin records no marker, and a
generation that continues a run answers from what the run already decided. A
history recorded before the gate replays its `DefaultVersion` branch, the five
minutes every earlier build waited, whatever its plan says; version 1 is the
plan's number, or five minutes for a plan that carries none. Version 1 also
words a pin's `PIN_UNAVAILABLE` by its cause: "within" the timeout only when no
pin host picked the pin up in time, and that every pin host refused it because
it is cordoned when the Orkers did; the `DefaultVersion` branch keeps the
earlier sentence, which always said "within". A build that
predates the setting ignores the plan's number, so a rollback to one waits five
minutes again. The replayer compares no timeout, so the replay tests compare
the wait the replayed code sends with the one the history recorded, and the
version it was answered with the recorded marker. The fixtures
`run-pin-creation-timeout-v1.json` and `run-pin-creation-timeout-legacy-v1.json`
(the same plan, recorded with the gate on and off) guard it with command and
negative tests.

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
`ORK_ADMIN_ADDR` — empty by default, so nothing opens — serves `/healthz`
(200 once the listener is up), `/readyz` (503 until the binary serves, then
200), `/metrics` (only once the `prometheus` metrics exporter is asked for)
and `/debug/pprof/` (only with `ORK_PPROF_ENABLED=true`). It is never
`ORK_HTTP_ADDR`: the API port answers client traffic, the admin port answers
an operator's scraper, and conflating them would mean a scrape config error
or a slow `/debug/pprof/` request competing with a run's own requests. Bind it
to a private network — a metrics scraper and a pprof profiler are not
something to expose publicly, and `ORK_PPROF_ENABLED`'s own default reflects
that an Orker's heap holds resolved secret bytes. Suggested addresses for a
single-host deployment: `:9464` for `ork`, `:9465` for `orker`, so one host can
run both without a collision.

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
"run event" line ("Lifecycle metrics", below) included — so a trace found in
Tempo or Jaeger and a log line found in Loki are one click apart in either
direction: search the trace ID as a LogQL label, or read `trace_id` off a log
line and paste it into Tempo's search.

A task's `task.json` carries an optional `trace` key —
`{"traceparent": "...", "tracestate": "..."}` in the same shape the W3C
`traceparent`/`tracestate` HTTP headers use — so a workload that wants its own
work folded into the same trace can propagate it. This is informational, not
part of the OCI Activity Contract's ABI (`docs/OCI_ACTIVITY_CONTRACT.md` §3.2): it is present only when tracing
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
- **Datadog**, which pushes rather than pulls: a small sidecar that scrapes
  `/debug/pprof/profile` and `/debug/pprof/heap` and forwards them.

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
| `ork.tasks` | `ork.pool`, `status`, `failure_category`, `ork.cache.outcome` | engine, once per terminal task (succeeded/failed) |
| `ork.task.duration` | `ork.pool`, `status`, `ork.cache.outcome` | engine, same event, `finished_at - scheduled_at` (dispatch to finish; `started_at` on a run recorded before `scheduled_at` existed) |
| `ork.task.attempts` | `ork.pool`, `status`, `ork.cache.outcome` | engine, same event: the attempt number the task finished on — one observation per fan-out item, none for a composed task |
| `ork.orker.tasks` | `outcome`, `ork.failure.reason` | Orker, once per attempt (`outcome`: `succeeded`, `failed`, `canceled`, or `adopted` for a retry that found an existing durable commit and ran no container) |
| `ork.orker.task.duration` | `outcome` | Orker, same event |
| `ork.orker.task_phase.duration` | `ork.phase` | Orker, once per phase of an attempt |
| `ork.orker.commit_adopted` | — | Orker, once per adopted commit |
| `ork.runs.refused` | `ork.namespace`, `ork.refusal.reason` | engine, once per submission the namespace's hold, `max_running` or enforced budget refused (`namespace_held`, `max_running_reached`, `budget_exceeded`, `budget_unmeasurable`); not a run, not counted on `ork.runs` |
| `ork.run.continuations` | `ork.namespace`, `outcome` | engine, once per run carried on into a fresh history: `written` (its saved state written), `write_failed` (object storage refused it; the run tries again after more growth), `over_cap` (its saved state is over `engine.continuation.bytes`; it never carries on), `restored` (the next history read it back), or a `RUN_STATE_*` code (the next history could not) |
| `ork.run.continuation.state.bytes` | `ork.namespace`, `kind` | engine, the stored size of what a switch wrote: `checkpoint` (the saved state), `events` (the earlier history's events), `snapshot` (the full state at the switch) |
| `ork.run.continuation.duration` | `ork.namespace`, `phase` | engine, seconds a switch's write (`write`) or a whole restore (`restore`) took |
| `ork.run.continuation.hold.duration` | `ork.namespace` | engine, seconds a run held new tasks before it carried on (0 when it found a moment with nothing running on its own) |
| `ork.run.continuation.read.attempts` | `ork.namespace` | engine, the attempt each successful read of a saved state succeeded on |

(The rows above omit `ork.namespace` even though every run and task metric
carries it; `ork.runs.refused` names it explicitly. `ork.cache.outcome` —
`hit`, `miss` or `bypassed` — is set only for a cached task, so a deployment
with none keeps exactly its series, and a hit's short duration never reads as
a fast container. A fan-out item is never a metric label; `ork.item` appears
only on task-log records and attempt spans, beside `ork.cache.outcome` and
`ork.cache.miss_reason` on the span.) Under `prometheus` the
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

A sink name is a slug (`^[a-z][a-z0-9_-]{0,63}$`) — it becomes a label and a
selector a namespace names. `endpoint` is the base URL used for any signal
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
"Live task logs"). `ORK_ORKER_TASK_LOG_EXPORT=true` adds a second, independent copy —
each line is also emitted as an OTel log record (requires
`OTEL_LOGS_EXPORTER=otlp`), carrying `ork.stream` (`stdout`, `stderr`, or
`ork` for the Orker's own framing), `ork.run_id`, `ork.task_id`,
`ork.attempt`, `ork.item` (a fan-out item), `ork.namespace`,
`ork.orkflow.name`/`.version`/`.digest`, and `ork.pool`. It is off by default because its volume is exactly the workloads'
own output — a chatty task doubles its log traffic the moment this is turned
on.

Nothing here touches the OCI Activity Contract. An activity image needs no
agent, sidecar, SDK, environment variable, or file to have its logs exported
this way — the Orker does the whole thing from outside
(`docs/OCI_ACTIVITY_CONTRACT.md`, §3.6).
