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

Two independent dials. **Replicas** carry the API, the web UI and the run
workflows; **Orker pools** carry task execution. Neither holds state, so both
scale by adding processes and both survive losing one.

### Nothing durable lives in a process

| state | where it lives | code |
|---|---|---|
| orkflow definitions | the bucket | `artifacts.S3OrkflowStore`, opened at boot by `openOrkflowStore` (`cmd/ork/main.go`). It keeps no cache, so a create on one replica is visible to every other |
| live runs — status, task state, event stream | Temporal history | `engine.TemporalRunReader` (`internal/engine/reader.go`), handed to the API as `Binding.Reader` by `engine.NamespaceRouter` (`internal/engine/router.go`) |
| finished runs, past retention | the bucket — **once `ORK_ARTIFACT_BACKEND=s3`** | `engine.NewRunArchiver` over `artifacts.NewRunArchive`, written by the terminal `PublishEvent`. The default backend is `local`: a directory on each host, so replicated it is one archive per replica (§1) |
| idempotency keys | Temporal | `engine.IdempotencyClaimWorkflow` (`internal/engine/claim.go`), held for `ORK_IDEMPOTENCY_TTL` |
| artifacts | the bucket — **once `ORK_ARTIFACT_BACKEND=s3`** | `pkg/artifacts`; events and payloads carry refs, never bytes. Same default, same consequence: on `local` an Orker writes to its own disk and no other host can read the bytes back |
| the in-process `RunStore` / `EventBus` | one process, and only that one | **write side only**: the candidate run a submission creates, the stream of a run whose start failed, and the archive + webhook write path. Behind a wired reader they are never read (`internal/api/service.go`, `LookupRun`). Its live feed hands each subscriber a run's events in sequence order without a gap, though publishes land in any order: an event waits until every number before it has arrived, a number the run never published is served as a `STREAM_GAP` event once the run's terminal event arrives, and a number still missing after a minute ends the stream `unavailable`, naming it (`internal/engine/bus.go`) |

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
- **One time zone database.** A stats series' day and hour boundaries are
  drawn from the engine's zone database (`docs/STATS.md`, "A series"): the copy
  linked into the release binaries, or the host's own zoneinfo, which Go reads
  first when it has one. Replicas of one image agree; replicas on hosts whose
  zone databases differ can draw a zone's boundaries differently wherever the
  two versions disagree about its rules.
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
  same stream with nothing repeated and nothing skipped. A stream that ends
  `unavailable` is resumed that way by `orkctl run watch` and the UI's run
  page, each up to six times in a row without an event. `orkctl run watch`
  resumes too when a reset of its connection to the engine reaches it as
  `canceled`, when the connection to the engine ends partway through a stream
  (a FIN, as when a replica dies or a load balancer closes it, which reaches it
  as `internal` or `unknown`), and when a stream ends cleanly while `GetRun` says
  the run is still going. A replica told to stop ends its streams with
  `unavailable` (`StreamEvents` and `StreamTaskLogs`, once a run's terminal event
  or a log's `last` chunk has not gone out), so every `orkctl` resumes; the run
  page's socket simply drops and reconnects. Any other
  error the engine itself reports ends the watch: it arrives in the stream's end
  message whatever its code, which a connection's end never does. So does an
  HTTP stream ended cleanly partway through a message, or reset, on a connection
  that stays up (`cmd/orkctl/root.go`, `connectionEnded`). The engine itself restarts a history read the
  cluster says changed under it (`CurrentBranchChanged`, which can answer a
  long poll of a run that has just completed), up to three times in a row
  without a new event, so that answer reaches a client only when it persists
  (`internal/engine/reader.go`, `historyBranchChanged`).
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
- **Every replica serves the UI, on its own origin.** The engine serves the
  web UI at `/` on `ORK_HTTP_ADDR`, beside the Connect API and the WebSocket
  bridge, so the browser makes its Connect calls and opens its run and
  task-log sockets on the address the page came from, whichever replica
  answers them. There is nothing to configure ("The web UI", below).
- **`ORK_WS_ALLOWED_ORIGINS` is for a page on another origin.** Unset, the
  engine admits a socket only from its own origin — the `Origin` header's
  host equal to the request's `Host` — which is all the UI it serves needs.
  Set it, on every replica since any of them may take the socket, when a
  browser page served from another origin opens the sockets, or when a proxy
  in front rewrites `Host`. A refused upgrade is a 403: the run page never
  goes live and its event log stays empty, while a page refresh still works
  because that reads through Connect rather than the socket. The engine logs
  `websocket upgrade refused` with the origin, the host and this variable's
  value.
- **Every replica is also a worker host.** It polls `ork-tasks` (run workflows
  and idempotency claims) and `ork-engine-events` (projection). Adding a
  replica therefore adds workflow capacity as well as API capacity, and it is
  why a run outlives the replica that started it: the next workflow task goes
  to another replica's poller.

### The web UI

Every `ork` replica serves the web UI at `/` on its API listener
(`ORK_HTTP_ADDR`), beside the Connect API (`/orkstack.v1.*`), the plain-HTTP
routes (`/v1/`) and the WebSocket bridge (`/ws/`). There is no UI process and
no UI image: one process and one origin, so the browser calls the API and
opens its sockets where the page came from, nothing proxies, nothing needs
CORS, and the bridge's same-origin default admits the UI with no
`ORK_WS_ALLOWED_ORIGINS` (`internal/webui`; `internal/api/server.go`,
`NewMux`).

- **What it serves.** The UI is a static single-page app: SvelteKit built
  with `adapter-static`, compiled into the `ork` binary. A file of the build
  is served as it is. Any other path is a route of the app and gets
  `index.html`, the page every route boots from — unless it names a file (its
  last segment has an extension) or lies under `/_app/`, which are 404, so a
  missing asset is never answered with a page a browser would keep as the
  asset. A dot file is never served, and only `GET` and `HEAD` are answered
  (405 otherwise).
- **It never takes a path of the API.** A path whose first segment is `v1`,
  `ws`, `healthz`, `readyz`, `metrics` or `debug`, or begins `orkstack.`,
  belongs to the API or to the admin listener: one that no route took is a
  404 from the UI, never the app's page, so a mistyped API call, or a probe
  aimed at this port instead of `ORK_ADMIN_ADDR`, fails plainly. The engine
  refuses to mount an API route outside those paths (`webui.Reserved`). Any
  other extension-less path is the app's page with a `200`, so a load
  balancer's health check belongs on `/readyz` of `ORK_ADMIN_ADDR`, never on
  this port.
- **Caching.** A file under `/_app/immutable/` is named by its content hash
  and sent `Cache-Control: public, max-age=31536000, immutable`. Everything
  else — `index.html`, `/_app/version.json`, the example gallery — is sent
  `Cache-Control: no-cache` with a strong `ETag`, so a browser revalidates it
  on every use (a `304` when it is unchanged) and picks up a new release at
  its next page load. An open tab also reads `/_app/version.json` every hour
  and whenever it regains focus; it reloads only as a rollout's failed
  navigation does (below). An error from the UI (a `404`, a `405`) is sent
  `Cache-Control: no-store`, so no cache keeps the 404 of an asset asked for
  during a rollout. Content types come from a fixed table, never the host's
  MIME database.
- **Compression.** The build holds a Brotli (`.br`) and a gzip (`.gz`) twin
  of each HTML, JavaScript, CSS, JSON and SVG file, and the engine sends one
  when the request's `Accept-Encoding` names it (`Content-Encoding`,
  `Vary: Accept-Encoding`), so nothing in front needs to compress the UI. The
  example gallery's YAML files and the fonts are sent as they are.
- **Headers.** Every UI response carries `X-Content-Type-Options: nosniff`.
  The page carries a strict `Content-Security-Policy`: the policy the UI's
  build writes into the page (`ui/vite.config.ts`, the `sveltekit` plugin's
  `csp`), which names the SHA-256 hash of each inline script the page runs,
  opened with `frame-ancestors 'none'`. It allows scripts, styles, fonts, images and
  connections (the Connect calls and the sockets) from the page's own origin,
  inline styles, `data:` images, and `eval`, for the YAML editor
  (SECURITY_MODEL.md says why); it allows no other inline script, no plugin
  and no `<base>`. The page also carries
  `X-Frame-Options: DENY` and `Referrer-Policy: same-origin`: the UI holds the
  operator's destructive controls, so no other origin may frame it. Every
  other UI response carries `default-src 'none'; frame-ancestors 'none'`
  ([SECURITY_MODEL.md](SECURITY_MODEL.md), "Transport, listeners and
  configuration"). Nothing in front needs to add a policy, and a proxy that
  sets its own `Content-Security-Policy` beside the engine's narrows the page
  to what both allow.
- **The names it answers to.** The listener answers a request only under a
  `Host` it knows, and refuses any other `403` before a route sees it: the
  UI, the API and the sockets alike (`internal/hostpolicy`). That is what
  stops a web page from reaching the engine by DNS rebinding through a public
  name. With `ORK_ALLOWED_HOSTS` unset it admits exactly, on any port, in any
  case:
  - an IP address, v4 or v6: `127.0.0.1:8080`, `[::1]:8080`, a pod or node
    address such as a NodePort's;
  - `localhost` and every name under it (`app.localhost`);
  - a name with no dot, written without a trailing dot: a compose service
    (`ork`), a Kubernetes Service from its own namespace, a host's short
    name. Such a name resolves only on the local network, never through
    public DNS. A dotless name a browser reads as an IPv4 address
    (`2130706433`, `0x7f000001`) is not admitted: a browser sends that
    address dotted, so only another client sends the number;
  - the host `ORK_HTTP_ADDR` names, when it names one (`ork.internal:8080`
    admits `ork.internal`);
  - a request with no `Host`, which no browser sends.

  That covers the quickstart (`localhost:8080`, and `ork:8080` from its
  `orkctl` service), the Kubernetes bases (a NodePort or a port-forward on an
  address), `orkctl`, `curl` and trigger callers reaching the engine by
  address or `localhost`, the development stack's Vite proxy (which
  keeps the browser's `localhost:5173`), and a proxy that rewrites `Host` to
  the address or dotless service name it dials (nginx's
  `proxy_pass http://ork:8080` default). **Any `Host` with a dot needs the
  setting**: a Kubernetes name such as `ork.orkstack` or
  `ork.orkstack.svc.cluster.local`, an FQDN, what a proxy that keeps the
  browser's `Host` forwards, and the name `orkctl`, `curl` or a trigger
  caller dials. List each in `ORK_ALLOWED_HOSTS`: a name,
  `*.example.com` for every name under `example.com` (list the narrowest you
  can: every name under it is admitted. A pattern over a top-level name,
  `*.com`, is refused at boot, but one over a suffix anyone may register
  under, such as `*.co.uk` or `*.github.io`, is not, and admits every name
  a page's author registers there), or `*` for any `Host`, which turns the
  check off. A refusal answers with the `Host` and the setting — a Connect client
  reads it as `permission_denied` — under the headers every other error
  carries, and is logged as `request refused: Host not allowed`, at most
  once every 10 seconds, with how many refusals since went unlogged. The
  admin listener applies the same rule under its own setting
  (§ "The API listener's bounds", last paragraph).

**Settings.** `ORK_UI_ENABLED` (`engine.ui.enabled`, default `true`) set to
`false` leaves the listener the API alone, and `/` is a 404. `ORK_UI_DIR`
(`engine.ui.dir`) serves the UI build in that directory instead of the one
compiled in — `ui/build`, which `cd ui && bun run build` writes. The engine
opens it so that a link leading out of it is never followed, and refuses at
boot a directory with no `index.html`, and the setting itself while
`ORK_UI_ENABLED=false` (§1). The boot logs where the UI comes from:
`web UI source=embedded`, `directory`, `placeholder` or `off`.

**A binary built without the UI** — `go build ./cmd/ork`, or `make build`
before `make ui` has written the UI's build into `internal/webui/dist/` —
serves a placeholder page at `/` saying how to build the UI or point
`ORK_UI_DIR` at a build; the API on the same port is unaffected. The release
binaries and the `ork` image carry the UI — the release build refuses to make
an `ork` without it — and so does the quickstart's source build
(`deploy/source/Dockerfile`).

**Behind a reverse proxy or an ingress.** One address serves the UI and the
API, so route the whole host to `ork`, not only the API's paths:

- forward WebSocket upgrades on `/ws/` (the `Upgrade` and `Connection`
  headers);
- keep the `Host` header the browser sent, or set `ORK_WS_ALLOWED_ORIGINS`
  to the page's origin: the bridge compares the socket's `Origin` with the
  `Host` it receives, so a proxy that rewrites `Host` makes the UI's own
  sockets look cross-origin and refused;
- list the name the browser uses in `ORK_ALLOWED_HOSTS` when the proxy keeps
  it (`ORK_ALLOWED_HOSTS=orkstack.example.com`), on every replica: a name
  with a dot is refused `403` otherwise ("The names it answers to", above);
- terminate TLS at the proxy as for any other service. The page derives its
  socket's address from its own, so an `https` page opens `wss` to the same
  host, and the engine behind the proxy stays plain HTTP;
- serve it at the root of a host. The page names its assets by absolute
  paths (`/_app/…`), so a proxy that mounts `ork` under a sub-path breaks it.

A gateway that authenticates callers covers the UI with the same rule: the
browser's calls reach the engine through it like any other caller's, and
carry the `Ork-Principal` it asserts (§5, "Attributing control-plane
writes").

**During a rollout.** Replicas of two releases behind one address each serve
their own release's UI. A page loaded from one release may ask a replica of
the other for one of its content-hashed assets, which that replica does not
have: a 404. When that happens on a navigation, SvelteKit reads
`/_app/version.json` and, when it names another build, reloads the page; a
page whose first load meets the miss does not start until it is loaded again,
and until every replica runs one release a reload can meet the same miss.
Session affinity at the load balancer while two releases are behind one
address avoids it, and nothing is lost either way: the UI keeps no run state
of its own, so a reload only redraws what the API already holds.

### The API listener's bounds

The API listener (`ORK_HTTP_ADDR`) bounds what a client may make it wait for
or hold, and never how long an answer takes (`internal/api/server.go`,
`NewServerWith` and `NewMux`):

| Bound | Value | What happens past it |
|---|---|---|
| Request line and headers, time to send | 10 s, fixed, over HTTP/1.1. Go's HTTP/2 server applies no header timeout: over h2c, a first request whose headers never end opens no stream, so the idle bound closes its connection; on a connection that already carries a stream, nothing bounds an unfinished header block until that stream ends | the connection is closed (over h2c, `GOAWAY` at the bound and the connection a second later) |
| Request line and headers, size | 64 KiB, fixed | `431 Request Header Fields Too Large` |
| A keep-alive connection with no request in flight | `ORK_HTTP_IDLE_TIMEOUT`, 15 min | the connection is closed |
| One Connect request message, as sent and decompressed | `ORK_REQUEST_MAX_BYTES`: unset, 1 MiB + 4 KiB × `engine.orkflow_max_tasks`, 17,432,576 bytes at the default 4,000 | `resource_exhausted` (HTTP `429` on the Connect protocol) |
| One Connect request's body, in all | twice the read cap | the engine stops reading, answers `resource_exhausted`, and closes the connection (HTTP/1.1) or resets the stream (h2c) |
| How long a request's body takes to arrive, or a client takes to read an answer | none | — |
| A plain-HTTP route's body (`/v1/…`) | the route's own: 1 MiB for webhooks and trigger invoke, a run's `values_max_bytes` for an await's completion, 4 KiB for its rejection | the route's own refusal |

- **Streams are never cut.** There is no whole-request read deadline and no
  write deadline: either would end a run's event stream, a task's live log or
  a WebSocket mid-way, since each stays open as long as its run or task does.
  A connection with a stream in flight is never idle. The price is the last
  row of the table: a client that sends a body slowly, or reads an answer
  slowly, holds its connection as long as it keeps going. Bound that at the
  proxy or gateway in front, which sees the client (nginx's
  `client_body_timeout` and `send_timeout`, Envoy's stream idle timeout), and
  bound how many connections one client may open there too.
- **A refused request is answered, not dropped.** The engine reads the rest
  of an oversized Connect body away, without holding it, before it answers,
  so the client gets a `resource_exhausted` error naming the cap
  (`message size N is larger than configured max M`), and the connection
  goes on serving. The cap bounds what the engine holds in memory: a
  gzip-compressed message stops inflating at the cap, and plain bytes past
  it are read and dropped only up to twice the cap. No request carries more
  than one message, so a body past that is never legitimate: the engine
  stops reading it, answers, and closes the connection (HTTP/1.1) or resets
  the stream (h2c), so a body that never ends cannot hold either.
- **The read cap scales with the task ceiling.** It is a bound on a
  request, never on how large an Orkflow may be, so it follows
  `engine.orkflow_max_tasks`: 1 MiB, plus 4 KiB a task. The heaviest task
  measured (a comment, needs, retry, resources, environment and an inline
  script) adds about 920 bytes to a create request, so a create request at the ceiling
  whose tasks average four times that fits, in either encoding: at the
  default 4,000 tasks, 3.8 MB as JSON against a cap of 17.4 MB. The 1 MiB
  holds a run's input at its own 256 KiB bound, and the plan of an
  automation document of a thousand schedules and a thousand triggers
  (0.5 MB) with half again to spare. Raising or lowering the ceiling moves
  the cap with it. `ORK_REQUEST_MAX_BYTES` set under that need is refused at
  boot, naming both settings; set it above the need for an automation
  document of thousands of objects, or for Orkflows whose tasks average more
  than 4 KiB, up to 1 GiB. A ceiling that needs more than 1 GiB (above about
  262,000 tasks) gets what it needs, warned at boot, and may be given up to
  twice that.
  The engine holds one such message in memory per request.
- **Behind a proxy.** 64 KiB of headers is above what nginx (32 KiB) and
  Envoy (60 KiB) forward by default. Set `ORK_HTTP_IDLE_TIMEOUT` above the
  backend keep-alive timeout of a load balancer in front, so the balancer
  always closes an idle connection first: 15 minutes is above the common ones
  (60 s on most, 610 s on Google Cloud's).

The admin listener (`ORK_ADMIN_ADDR`) has the same kinds of bound at its own
sizes: 5 s for the request line and headers, 64 KiB of them, and 2 minutes
idle, with no read or write deadline, so a CPU profile taken over
`/debug/pprof/profile?seconds=N` is never cut (`internal/telemetry/admin.go`).
It answers the same `Host`s the API listener does by default — an address,
`localhost` and the names under it, a dotless name, and the host
`ORK_ADMIN_ADDR` names — and refuses any other `403`, so a page that
rebinds a public name to it cannot read `/metrics` or a profile. A scraper
that dials it by a name with a dot needs that name in
`ORK_ADMIN_ALLOWED_HOSTS` (`telemetry.admin_allowed_hosts`), on both
binaries: a Prometheus static target such as `host.docker.internal`, as the
observability profile's is (`deploy/observability/README.md`), or a
Service name. A scraper that dials pod addresses, as Kubernetes service
discovery does, and a probe of `/healthz` or `/readyz` by address need
nothing. The setting is the admin listener's own, so a name the API is
reached by is not answered here, and the other way round.

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
  engine asks Temporal which workers are polling the pool's channel. When the
  answer is none it asks the Orker inventory — the presence channel, and each
  Orker on its own control channel, the read `orkctl orker list` makes — and
  refuses the submission only when no uncordoned Orker of the pool answers
  there either (one waiting out a store outage alone counts as uncordoned; see
  "When object storage goes away"), naming the pool, the tasks and the fix:

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

  The second question is what keeps a busy pool live. An Orker polls its
  pool's channel only while it has a free task slot, so a pool whose every
  Orker is running `ORK_ORKER_MAX_CONCURRENT` tasks shows no poller there once
  their last polls are 90 seconds old. Its presence and control channels have
  workers and slots of their own, so a busy Orker still answers there. A
  submit reads the inventory only when a pool's channel shows no poller or a
  capability probe (below) needs it, once per submit and once more for each
  composed child it plans; `orkctl pool list` reads it once per call. An Orker
  that does not answer within five seconds says nothing about which pool it
  serves, so it is not counted, and when the pool then reads empty the engine
  logs `a pool reads empty while some live Orkers did not answer when asked`.
  It is not made a "cannot tell": a stopped Orker's presence record stays
  fresh for up to 90 seconds and never answers, so that would tell every
  submit into an empty pool to ask again after any Orker anywhere stopped. A
  busy pool on a cluster too slow to answer a describe in five seconds
  therefore still reads empty.

  An Orker is matched to a pool by the pool it reports. Two raw
  `ORK_ORKER_QUEUE` channels in one Temporal namespace both report the default
  pool, so when the engine's own channel shows no poller, an Orker polling the
  other raw channel keeps the default pool live and the submit waits rather
  than being refused. Give each raw channel its own Temporal namespace, as the
  test harnesses do (AGENTS.md §11.1).

  The residue is bounded and deliberate: inside those 90 seconds a stale record
  can still say "live" just after an Orker stopped. That degrades to the old
  behaviour — a run that waits — never to a false rejection of a healthy
  submit. `SubmitRunRequest.allow_empty_pool` submits anyway; it is off by
  default and taking it logs `WARN submit into empty pool` with the pool and
  the tasks.

  A cluster that cannot be asked is a third answer, and it is kept apart from
  the other two. "No Orkers" is `failed_precondition` — start one, or take the
  override. A probe that got no reply at all (Temporal unreachable, or slower
  than its five-second bound, for the pool's channel or for the inventory)
  measured nothing, and the submission it refuses may be perfectly good, so it
  is `unavailable`: ask again. `allow_empty_pool` does not change that for the
  pool's channel; for the inventory alone it does, because the override would
  accept the pool busy or empty, and the engine logs `whether a pool with no
  poller is busy is unknown`. Reported as
  `invalid_argument` it would send every client off to fix a request with
  nothing wrong with it for as long as the cluster was unwell.
- **A namespace's `allowed_pools` bounds what it may dispatch onto**, and it is
  enforced on the server at both create and submit, so no client can skip it.
  An empty list means no restriction.

  The list is read in the same vocabulary as the dispatch — the collapsed name
  of the bullet above. With `ORK_ORKER_POOL=gpu` on the engine, a task that
  names no pool is dispatched to, stamped as and reported on `gpu`, so a
  namespace whose list holds only `main` is refused there and one whose list
  holds only `gpu` accepts `pool: main`:

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
  says the pool has, except for the busy-pool question above, which matches
  Orkers by the pool they report and so cannot tell two raw channels of one
  Temporal namespace apart.
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

The file is YAML, decoded strictly: a second YAML document is refused at
boot, naming its line, even an empty one or one that does not parse, as it is in
`ORK_TELEMETRY_SINKS` and `ORK_ACTIVITY_CATALOG`; an unknown key is refused at boot,
naming its full path, and so is an integer, boolean or duration that is not
one once the space around it is trimmed (the daemons are handed the bare
value: not every reader trims, and a padded one would quietly use the
default) and a value holding a NUL byte, which an environment variable cannot
carry. In `ORK_TELEMETRY_SINKS` and `ORK_ACTIVITY_CATALOG` a `!!merge` tag on a
key or a value, and `<<` written as a value that is not text, are refused at
boot naming the line and the path, where the YAML reader would drop the value. Any scalar leaf may instead be a mapping with `ork:` and `orker:` keys,
to give the two binaries different values from one file — the shape
`telemetry.admin_addr` needs — and a plain scalar still applies to both. Any
string leaf may instead be a mapping with a single `file:` key naming a path,
in place of a literal, to pull a secret from a
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
0, or 1 naming the first error. `--check` never starts anything. Two other
flags start nothing: `--healthcheck`, which asks a running process whether it
is ready (§5, "Health checks"), and `--version`, which prints the version and
the commit the binary was built from and reads no configuration
(`docker run --rm ghcr.io/orkstack/ork:<version> --version`, and the same for
`orker`; `orkctl version` is the CLI's). A release names its whole commit. A
development build names the revision version control recorded, and a build
with neither, such as the quickstart's source build, says `commit: unknown`.

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
| `activities:` | `ORK_ACTIVITY_CATALOG` | built-in `shell`/`http`/`file`/`json`/`git`/`template` → `ghcr.io/orkstack/activity-*:0.3` | `ork` | the activity catalog: a YAML `activities:` map from catalog name to registry reference, resolved to a digest when the Orkflow is created. `make up` points it at the local registry |
| `engine.registry_auth_file` | `ORK_REGISTRY_AUTH_FILE` | *(empty)* | `ork` | a docker `config.json` used only for manifest GETs during create-time resolution; grants no push and no execution |
| `webhooks.allowed_networks` | `ORK_WEBHOOK_ALLOWED_NETWORKS` | *(empty)* | `ork` | comma-separated CIDRs (`10.0.0.0/8,fd00::/8`) of non-public networks a webhook may be delivered to. Empty admits public addresses only: a loopback, link-local (169.254.169.254 among them), private, shared, multicast or reserved address is refused with `WEBHOOK_DESTINATION_BLOCKED`, at registration for a URL whose host is one and at every connection a delivery makes, so a redirect or a name rebound to one is refused too. List the network of an in-cluster or local receiver (`127.0.0.0/8` for a stub on the engine's host). An entry with host bits set (`10.0.0.1/8`), or no length, is refused at boot. An entry that admits a whole address family (`0.0.0.0/0`, `::/0`, `::ffff:0:0/96`) is accepted and warned of at boot, naming the entry, because it turns the check off for that family. Applies to every webhook, whoever registered it |
| `orker.id` | `ORK_ORKER_ID` | `hostname-<hash of the work dir>` | `orker` | this Orker's identity: labels its containers and keys its startup reaper, so two Orkers must never share one — across the whole deployment, not only on one host: it is also the Orker's address for `orkctl orker` (1–200 bytes of UTF-8, no whitespace or control characters; refused at boot otherwise) |
| `orker.image_layout` | `ORK_ORKER_IMAGE_LAYOUT` | `/var/lib/orkstack/images` | `orker` | directory of OCI image layouts an Orker may serve a pinned digest from when a pull fails. An absent directory is not an error — the fallback matters only to an air-gapped or rate-limited Orker |
| `orker.docker.bin` | `ORK_DOCKER_BIN` | `docker` on `PATH` | `orker` | the container CLI; `podman` on `PATH` when `orker.docker.cli` is `podman` |
| `orker.docker.cli` | `ORK_DOCKER_CLI` | `docker` | `orker` | the grammar that CLI speaks: `docker` (the docker CLI, against Docker or against Podman's Docker-compatible socket) or `podman` (the podman CLI); any other value fails every container command, naming the setting. The engine behind the CLI is asked at boot, logged, and verified by the boot probe ([Running on Podman](#running-on-podman)). `orkctl activity test` and `orkctl task run` read it too |
| `orker.docker.pull_timeout` | `ORK_DOCKER_PULL_TIMEOUT` | `5m` | `orker` | how long an image pull may take |
| `orker.docker.bind_probe_timeout` | `ORK_DOCKER_BIND_PROBE_TIMEOUT` | `2m` | `orker` | how long the boot bind probe waits for the daemon at each step that waits on it: identifying the engine (which one, and whether it is rootless), whether the probe's image is local, and each of two attempts to run its container, which the Orker then removes. An attempt that overruns, or that the daemon ends with a deadline error of its own, is tried once more, and a daemon that is still too slow refuses boot as `BIND_PROBE_TIMED_OUT`; a refusal of the bind in the daemon's own words is still `WORKDIR_NOT_BINDABLE`, and any other refusal, or a container that exits non-zero, is `RUNTIME_POSTURE_REFUSED`. The SIGTERM check that follows, in a container of its own, has the same budget, retry and reasons for each of two attempts to start that container, and the budget again for its workload to say it is ready, then up to 125 seconds for cancelling it (its 30-second grace included) and removing it. A daemon that is slow at every step holds boot for up to seven times this (identifying the engine, the image inspect, two attempts at each container and the ready file) plus 230 seconds for removing what it gave up on and the first container, and for that cancellation (17 minutes 50 seconds at the default), plus `orker.docker.pull_timeout` when the probe image has to be pulled; [Health checks](#health-checks) says what that means for a supervisor |
| `orker.resources.max_cpu`, `orker.resources.max_memory` | `ORK_ORKER_MAX_CPU`, `ORK_ORKER_MAX_MEMORY` | `8`, `16Gi` | `orker` | caps on what a task may request; a request above the cap is refused, never clamped |
| `orker.capacity.cpu`, `orker.capacity.memory` | `ORK_ORKER_CAPACITY_CPU`, `ORK_ORKER_CAPACITY_MEMORY` | *(empty)* | `orker` | what this Orker admits tasks within, in the Kubernetes spelling (`"16"`, `"64Gi"`). Unset admits by `max_concurrent` alone, best effort, and the Orker logs `capacity not declared: admission is best effort, bounded only by max_concurrent` at boot. Set, a poll is granted only while the per-task cap fits beside what is reserved; boot refuses a capacity the cap does not fit in. Either may be set alone — see "Declaring an Orker's capacity" below |
| `orker.resources.default_cpu`, `orker.resources.default_memory` | `ORK_ORKER_DEFAULT_CPU`, `ORK_ORKER_DEFAULT_MEMORY` | `1`, `1Gi` | `orker` | what a task that requests nothing gets |
| `orker.resources.pids_limit` | `ORK_ORKER_PIDS_LIMIT` | `512` | `orker` | pids limit applied to every container |
| `orker.resources.tmpfs_bytes` | `ORK_ORKER_TMPFS_BYTES` | `64 MiB` | `orker` | size of the container's `/tmp` tmpfs. `/orkstack/tmp` is a separate bind mount, which this does not size |
| `orker.resources.force_user` | `ORK_ORKER_FORCE_USER` | `false` | `orker` | `true` runs every container as the Orker's own user, as if each task had set `run_as_orker_user: true` — the readability escape hatch for hosts where the image's own USER leaves output unreadable. An Orker whose boot probe finds that user does not land as the Orker (a rootless or user-namespaced daemon) refuses to start with it set, as `RUN_AS_ORKER_UNAVAILABLE` |
| `orker.secret_schemes` | `ORK_ORKER_SECRET_SCHEMES` | `env,file` | `orker` | comma-separated secret sources this Orker resolves for a task's `secrets:` |
| `orker.secret_allow` | `ORK_ORKER_SECRET_ALLOW` | *(empty)* | `orker` | comma-separated `scheme:pattern` floor of the `secrets:` sources this Orker resolves, e.g. `env:SANDBOX_*,file:/etc/orkstack/secrets/*`; `*` is the one wildcard and does not cross `/`. Empty resolves every source of an allowed scheme except the Orker's own, and logs a warning at boot. No entry admits the Orker's own sources: its `ORK_*` and `OTEL_*` settings, its process files, its work root, its configuration and credential files (§5, "Secrets on a shared pool"). With `orker.workdir` unset the work root is the temp directory, so a `file:/tmp/…` source is refused and an entry under `/tmp` stops the Orker at boot: give tasks' secret files a directory of their own. A namespace's `allowed_secrets` can only narrow this, never widen it. Refused at boot if an entry is malformed, names a scheme not in `orker.secret_schemes`, or can match only the Orker's own sources |
| `orker.registry_auth_file` | `ORK_ORKER_REGISTRY_AUTH_FILE` | *(empty)* | `orker` | a docker `config.json` used only for the manifest and blob GETs that verify a cosign signature when a namespace sets `require_signed_by` (§5, "Image supply-chain policy"); grants no push, never reaches a container. Empty verifies anonymously, which public registries allow |
| `orker.env_allowlist` | `ORK_ORKER_ENV_ALLOWLIST` | *(empty)* | `orker` | comma-separated names `{{ env "NAME" }}` may read in a task's `with:` — resolved on the Orker, so the value is the pool's |
| `orker.workdir` | `ORK_ORKER_WORKDIR` | temp dir | `orker` | root under which each task attempt gets its own working directory. **It must be bind-mountable by the docker daemon**, since every task's `/orkstack` workspace is bind-mounted from it, and its path must be a plain one: a mount is a comma-separated option list, so an Orker whose work root holds a comma or a control character refuses to start (`WORKDIR_NOT_BINDABLE`, naming `ORK_ORKER_WORKDIR`). The default is `os.TempDir()` — `/var/folders/...` on macOS — which a Docker daemon running in a virtual machine (colima, Lima, Rancher) does not share, so a container task fails with `bind source path does not exist` on a path that plainly exists. `make up` sets it to `.data/work`, under `$HOME`, which every one of those VMs shares; it also probes the daemon at startup and warns if the path is unreachable. It also holds two process-scoped scratch directories that boot empties: `ork-spool/`, where an upload whose source cannot be re-read is staged, and `ork-stage/`, the staging cache — attempts staging the same input digest at the same time (the items of a fan-out over one upstream) share one verified download, hard-linked into each attempt's read-only input tree and deleted when the last of them ends. Where the root cannot hard-link, each attempt downloads its own copy, as before |
| `orker.log.flush_bytes` | `ORK_ORKER_LOG_FLUSH_BYTES` | `64 KiB` | `orker` | live task logs: buffered bytes that trip a flush. Bigger batches a chatty task harder and costs less per object |
| `orker.log.flush_interval` | `ORK_ORKER_LOG_FLUSH_INTERVAL` | `500ms` | `orker` | live task logs: the longest a line waits before it is stored. This is the floor on how late a follower can see a slow task's output |
| `orker.log.max_bytes` | `ORK_ORKER_LOG_MAX_BYTES` | `16 MiB` | `orker` | live task logs: the cap on ONE task attempt. Past it the Orker stores a truncation notice and nothing more |
| `orker.heartbeat_interval` | `ORK_ORKER_HEARTBEAT_INTERVAL` | `5s` | `orker` | how often a running task's heartbeat goes to Temporal, and so the longest a **cancellation** waits before the Orker acts on it: Temporal delivers cancellation only in a heartbeat's response, so a failing run's still-running siblings keep burning resources for up to this long. It does not bound how long a **Temporal frontend outage** a container survives: that is the 30 s heartbeat timeout, whatever the tick, after which the Orker stops the container and the attempt fails `ORKER_HEARTBEAT_LOST`; an open pin rides out 24 s. Must be between `1s` and `10s` (a third of the 30 s heartbeat timeout); anything else is refused at boot rather than clamped. Lower it towards `2s` for faster cancels |
| `orker.max_concurrent` | `ORK_ORKER_MAX_CONCURRENT` | `16` | `orker` | how many tasks this Orker runs at once — the size of its task worker's activity slots, and the **Max concurrent** Admin → Activities reports. A whole number, 1 or more; anything else is refused at boot. The default fits a 16-CPU, 64 GiB host running default-sized tasks; size it to the host — see "Sizing an Orker's task ceiling" below |
| `orker.store_check.enabled` | `ORK_ORKER_STORE_CHECK_ENABLED` | `true` | `orker` | whether this Orker checks object storage and cordons itself while the checks keep failing, so tasks wait without using attempts. `false` turns it off, and then an outage fails attempts as they arrive — see §5, "When object storage goes away" |
| `orker.store_check.interval` | `ORK_ORKER_STORE_CHECK_INTERVAL` | `15s` | `orker` | how often the store is checked: one conditional write the store refuses and one read of one small object, per Orker, per check. `1s` to `1h`; anything else is refused at boot |
| `orker.store_check.timeout` | `ORK_ORKER_STORE_CHECK_TIMEOUT` | `5s` | `orker` | how long one check may take before it counts as failed. Above zero and at most the interval; anything else is refused at boot |
| `orker.store_check.failures` | `ORK_ORKER_STORE_CHECK_FAILURES` | `3` | `orker` | how many failed checks in a row cordon this Orker. 1 or more; anything else is refused at boot. A shorter blip cordons nothing |
| `orker.store_check.recoveries` | `ORK_ORKER_STORE_CHECK_RECOVERIES` | `2` | `orker` | how many good checks in a row end the cordon. 1 or more; anything else is refused at boot |
| `orker.pins.max_concurrent` | `ORK_ORKER_MAX_PINS` | `0` | `orker` | how many task pins (`docs/DSL.md`, "Pins") this Orker hosts at once. `0` hosts none and starts no extra pollers; above zero makes this Orker a pin host. Pool-wide — see below |
| `deployment.name` | `ORK_DEPLOYMENT_NAME` | `orkstack` | **both** | the Worker Deployment both binaries join |
| `deployment.build_id` | `ORK_BUILD_ID` | *(empty)* | **both** | this release's identity. **Empty means versioning is off** |
| `engine.http_addr` | `ORK_HTTP_ADDR` | `:8080` | `ork` | Connect API, WebSocket bridge, plain-HTTP routes and the web UI at `/` (§ "The web UI") |
| `engine.ws_allowed_origins` | `ORK_WS_ALLOWED_ORIGINS` | *(empty)* | `ork` | comma-separated browser origins allowed onto `/ws/runs/{id}` and `/ws/runs/{id}/tasks/{task}/logs` (space around an entry and empty entries are ignored). Empty admits the engine's own origin only, which is all the UI it serves needs; list a page served from another origin, or the browser's origin behind a proxy that rewrites `Host` |
| `engine.http_idle_timeout` | `ORK_HTTP_IDLE_TIMEOUT` | `15m` | `ork` | how long a keep-alive connection to the API listener may sit with no request in flight before the engine closes it; at least `1s`, refused at boot otherwise. Set it above the backend keep-alive timeout of a load balancer in front, so the balancer always closes first. A stream in flight is never idle (§ "The API listener's bounds") |
| `engine.request_max_bytes` | `ORK_REQUEST_MAX_BYTES` | *(derived)* | `ork` | the largest Connect request message the API reads, in bytes, both as sent and once decompressed. Unset derives 1 MiB + 4 KiB × `engine.orkflow_max_tasks` (17,432,576 at the default 4000), so it rises and falls with the task ceiling and an apply at that ceiling always fits. A value under that derivation is refused at boot naming both settings, and so is one over 1 GiB (over twice the derivation, when that is more than 1 GiB, which the boot warns of). A larger request is refused `resource_exhausted`. Raise it for an automation document of thousands of objects, or for Orkflows whose tasks average more than 4 KiB (§ "The API listener's bounds") |
| `engine.allowed_hosts` | `ORK_ALLOWED_HOSTS` | *(empty)* | `ork` | comma-separated names the API listener answers to in a request's `Host` header, besides the ones it always admits: an IP address, `localhost` and every name under it, a name without a dot, and the host `ORK_HTTP_ADDR` names. An entry is a name (`orkstack.example.com`), `*.suffix` for every name under a suffix of two labels or more, or `*` for any `Host`; space around an entry and empty entries are ignored, and an entry with a scheme, a port or a path, or a pattern over a top-level name (`*.com`), is refused at boot. Any other `Host` is refused `403`, which stops DNS rebinding through a public name. List every name with a dot the engine is reached by: a Kubernetes `svc.namespace` name or FQDN, a proxy that keeps the browser's `Host`, the name `orkctl` or a caller dials (§ "The web UI") |
| `engine.ui.enabled` | `ORK_UI_ENABLED` | `true` | `ork` | serve the web UI at `/` on the API listener, beside the Connect API and the WebSocket bridge; `false` leaves the listener the API alone (§ "The web UI") |
| `engine.ui.dir` | `ORK_UI_DIR` | *(empty: the build compiled into `ork`)* | `ork` | serve the UI build in this directory (`ui/build`, written by `cd ui && bun run build`) instead of the one compiled into `ork`; refused at boot if it holds no `index.html`, or if `ORK_UI_ENABLED=false` |
| `engine.idempotency_ttl` | `ORK_IDEMPOTENCY_TTL` | `24h` | `ork` | how long an idempotency key stays bound; the cluster keeps a finished run at least this long plus five minutes (§5, "Ops notes", idempotency keys). Set it the same on every engine |
| `engine.each_max_items` | `ORK_EACH_MAX_ITEMS` | `1000` | `ork` | the most items one Orkflow's fan-outs (`each:`) may declare in total — the sum of their `max_items`. Default 1000. Lowers it, or raises it up to the built-in 10,000; above 1,000 it needs `engine.continuation.enabled` (refused at boot otherwise, naming both keys). Checked at validate and create, and again at submit, so lowering it refuses new runs of an Orkflow created under the old value. A submit that needs more than 1,000 items is refused while an `ork` process of a build that new runs start on (any process, on an unversioned fleet) runs a release older than the one that raised the maximum; a draining older build does not block it (§5, "Engine capabilities and the fleet check"). Before rolling back below that release, lower it to 1,000 and wait for such runs (§4) |
| `engine.orkflow_max_tasks` | `ORK_ORKFLOW_MAX_TASKS` | `4000` | `ork` | the most tasks one Orkflow may declare (`docs/DSL.md`, "How many tasks an Orkflow may declare"). A fan-out counts once whatever its `max_items`, and a composed task counts once whatever its child declares. A whole number, 1 or more; anything else is refused at boot. Checked at validate, create and plan, and again at submit, so lowering it refuses new runs of an Orkflow created above the new value. A deployment lowers or raises it. The default is the largest size the task soak measured (its per-generation history, snapshot, in-flight, workflow-code, read-latency and run-page thresholds, with plain tasks and small outputs); above it a deployment runs unmeasured, and the boot warns naming the measured size. The task soak runs in the nightly and pre-release proofs, not in every merge proof (docs/PROOFS.md). Independently, a stage of more than X plain tasks that start together may stop `RUN_HISTORY_LIMIT` in its first history, explicitly, and the boot warns when this is above X. X is a size to stay within, not a promise: at or below it a flat stage of plain tasks cannot reach that limit before it carries on, and other limits still apply. X is 4,389 at the defaults and moves with `engine.history.max_events`, `engine.continuation.events`, `engine.continuation.bytes`, `engine.continuation.restore_timeout` and `engine.run_max_in_flight`; §5, "Task ceiling and the flat-stage bound", gives its closed form. With `engine.continuation.enabled` off a run has one history: an unset key defaults to the smaller of 4,000 and `engine.history.max_events / 50` (1,024 at the default limit; the boot logs the effective value), and an explicit value above that quotient is refused at boot, naming the three keys |
| `engine.history.max_events` | `ORK_HISTORY_MAX_EVENTS` | `51200` | `ork` | the cluster's per-execution history event limit. A run stops `RUN_HISTORY_LIMIT` before reaching it, counting what its own failure path still needs, instead of being terminated by the cluster with no reason. **Set it to the cluster's real limit if an operator changed it:** a value above the real limit defeats the guard, and one below stops runs early. Even at the default, a run that would finish just under the limit with much work still pending now stops `RUN_HISTORY_LIMIT`, because its failure path's reserve is kept back: 16 events per running task, 8 per pending task, 24 per open await, 12 per open pin, 48 per failure handler, plus 96 — about 8,100 for a 1,000-task flat stage with every task running. At least 100; anything else is refused at boot. Read at submit, so a change applies to new runs |
| `engine.history.max_bytes` | `ORK_HISTORY_MAX_BYTES` | `50 MiB` | `ork` | the same, for history size. Before each dispatch the run also counts that dispatch's own command (a task's input, up to `engine.limits.values_max_bytes`), so it never records one its failure path could not follow. The reserve assumes completion results of ≤ 8 KiB each; a run whose failure path lands many larger results at once can still reach the cluster's size limit and be terminated by the cluster; this is the guard's one known gap, and such a run is still reported `RUN_HISTORY_LIMIT` (the reader maps the cluster's history-limit termination, "Workflow history count/size exceeds limit.", to it), never `RUN_TERMINATED`. At least 64 KiB; anything else is refused at boot. Read at submit, so a change applies to new runs. Both limits are carried once per run tree, on the top-level run's plan (about 0.4 KB), so a run whose plan is already within that much of its 256 KiB bound is refused at submit (`FAILED_PRECONDITION`, naming the size) |
| `engine.continuation.enabled` | `ORK_CONTINUATION_ENABLED` | `true` | `ork` | runs carry on into a fresh history once theirs has grown past the budget below, at their next moment with nothing running, so a large or long-lived run is bounded per history rather than stopped `RUN_HISTORY_LIMIT` (see §5, "A run that pauses new tasks", and §2, "Runs that span several executions"). On by default since the release after the one that first carried runs on (§4, "Rolling back"). The three keys below are validated only when this is true, so a deployment that lowers `engine.history.max_events` below 32,768 or `engine.history.max_bytes` below 40 MiB must lower `engine.continuation.events` or `.bytes` with it, or turn this off: it is refused at boot otherwise, naming the keys. Read at submit, so a change applies to new runs |
| `engine.continuation.events` | `ORK_CONTINUATION_EVENTS` | `8192` | `ork` | history growth (events) after which a run carries on at its next moment with nothing running. A run with no such moment pauses new tasks once its history plus what its failure path still needs reaches 60% of the limit, and carries on once what was running finishes. At least 50, at most a quarter of `engine.history.max_events`; anything else is refused at boot when continuation is enabled. Read at submit, so a change applies to new runs |
| `engine.continuation.bytes` | `ORK_CONTINUATION_BYTES` | `8 MiB` | `ork` | the same, for history bytes; also the most a run copies into any one of its histories (the one bounded copy AGENTS.md §2.2 allows into history): the saved state it carries forward, plus its definition when that is passed by reference (`engine.definition_by_reference`, below), which may take at most a quarter of it, compressed. A run whose saved state is larger carries on in its current history instead, and may stop `RUN_HISTORY_LIMIT` naming the size. At least 16 KiB, at most a fifth of `engine.history.max_bytes`; anything else is refused at boot when continuation is enabled |
| `engine.continuation.restore_timeout` | `ORK_CONTINUATION_RESTORE_TIMEOUT` | `1h` | `ork` | how long a run that carried on waits for object storage to return its saved state before it fails `RUN_STATE_UNAVAILABLE`, explicitly. Each read is retried with a backoff doubling from 1 s to at most 1 minute, and a retry that would start past the timeout is not made, so the run stops at its last read inside the window — up to one backoff early (31 s into a 1m timeout, within the last minute of the default hour). 1m to 8h, and its worst-case history — about `128 + 3 per 8 s` events at the default bytes (1,478 at 1h, 10,928 at 8h)— must fit in a quarter of `engine.history.max_events`; anything else is refused at boot when continuation is enabled, naming this key, `engine.continuation.bytes` and `engine.history.max_events`. Each await still open when a run carries on adds one more event to that restore (its deadline timer, recreated), which the run's history guard counts like any other |
| `engine.definition_by_reference` | `ORK_DEFINITION_BY_REFERENCE` | `true` | `ork` | a top-level run whose planned definition is over the 256 KiB a run's start may carry travels by reference instead: the start carries a stub naming it, and the run copies the definition into its own history in bounded chunks, sharing the copy bound below (`engine.continuation.bytes`), of which it may take at most a quarter, compressed (2 MiB at the default); a larger one is refused at submit and at create, "split it into composed Orkflows" (`docs/DSL.md`, "How many tasks an Orkflow may declare"). Needs `engine.continuation.enabled`: an explicit `true` without it is refused at boot, naming both keys, and when neither the configuration file nor the environment sets this key it is on only where the built-in default is on and continuation is (the boot logs the effective value). A submit that needs it is refused while an `ork` process of a build that new runs start on (any process, on an unversioned fleet) runs an older release (§5, "Engine capabilities and the fleet check"). On by default from the release after the one that first had it; turn it off and wait for its runs before rolling back below that release (§4, R3) |
| `engine.snapshot_overlays` | `ORK_SNAPSHOT_OVERLAYS` | `true` | `ork` | a run that declares more than 256 rows — its tasks plus every fan-out's `max_items` — writes, for each event before its last, only the rows that changed since its last full snapshot (a snapshot overlay, `docs/STORAGE.md`), and a full snapshot at least every 64 events and whenever more than 256 rows changed; its last event's snapshot is always full. Decided per run, so a small child of a large parent keeps full snapshots. Read at submit, so a change applies to new runs. While any `ork` process polling `ork-engine-events` runs an older release (a draining build's included), or a process of a build that new runs start on does, new runs write full snapshots instead, and the engine logs why at most once a minute (§5, "Snapshot overlays"). On by default from the release after the one that first had it; turn it off and wait for its runs before rolling back below the release that has it (§4) |
| `engine.run_max_in_flight` | `ORK_RUN_MAX_IN_FLIGHT` | `500` | `ork` | the most activities, child runs and pins one run keeps pending at once; more wait `PENDING`, exactly as on a saturated pool, and dispatch resumes as running work finishes. An open pin's needs are exempt, below an absolute stop at 1,900. 16 to 1,800; anything else is refused at boot. Without it, a run above the cluster's 2,000 pending activities would stall. Read at submit, so a change applies to new runs. Raising it is allowed, but it lowers the flat-stage bound X (`engine.orkflow_max_tasks`, §5): above about 700, X is below the 4,000-task default and the boot warns |
| `engine.pin_creation_timeout` | `ORK_PIN_CREATION_TIMEOUT` | `5m` | `ork` | how long a pin waits for some pin host of its pool to have a free slot (`ORK_ORKER_MAX_PINS`) before the task whose readiness asked for it fails `PIN_UNAVAILABLE`. A duration of whole seconds, `1s` to `24h`; anything else is refused at boot, naming the key. Read at submit and carried by the run, so a change applies to new runs only: a run already accepted keeps the wait it started with, and so does a child run of it. A pool whose pin hosts are all cordoned fails a pin after about five minutes whatever this says (§5, "Removing an Orker safely: cordon and drain") |
| `engine.compose_max_depth` | `ORK_COMPOSE_MAX_DEPTH` | `4` | `ork` | how many levels of child runs one run may start (a run is level 0). Lowers the built-in ceiling of 4, never raises it; checked at validate and create, and again at submit, so lowering it refuses new runs of an Orkflow created under the old value |
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
| `engine.cleanup.interval` | `ORK_CLEANUP_INTERVAL` | `1h` | `ork` | interval between cleanup scans; the first scan runs when the worker starts. Also the archive sweep's interval, and how long a run has been closed before the sweep looks at it, with cleanup on or off ("The archive sweep", below) |
| `engine.cleanup.retention_buffer` | `ORK_CLEANUP_RETENTION_BUFFER` | `24h` | `ork` | the enforcement margin: how far past the deployment maximum run retention the underlying (never user-facing) backstop TTL sits, so it can never be what actually retires a run. The maximum plus this margin is also how long a run with no archive survives an object-storage outage ("The archive sweep", below) |
| `engine.cleanup.batch_size` | `ORK_CLEANUP_BATCH_SIZE` | `100` | `ork` | maximum eligible runs checked in one cleanup iteration; a full iteration starts the next one at once (a backlog drains without waiting for the interval). Also the archive sweep's listing page size |
| `storage.dir` | `ORK_ARTIFACT_DIR` | `./.data/artifacts` | both | filesystem root for the `local` backend; ignored under `s3` |
| `storage.s3.endpoint` | `ORK_S3_ENDPOINT` | `http://localhost:9000` | both | S3-compatible endpoint. `ork` reads orkflow definitions from S3 whatever `ORK_ARTIFACT_BACKEND` says, so this one is never optional for the engine |
| `storage.s3.region` | `ORK_S3_REGION` | `us-east-1` | both | — |
| `storage.s3.bucket` | `ORK_S3_BUCKET` | `orkstack-artifacts` | both | one bucket holds definitions, artifacts, archived runs and task logs |
| `storage.s3.access_key` | `ORK_S3_ACCESS_KEY` | *(empty)* | both | set with `storage.s3.secret_key`; with neither set, the default credential chain signs, such as an instance or pod role ("The object store's credentials"). Half a key stops the boot |
| `storage.s3.secret_key` | `ORK_S3_SECRET_KEY` | *(empty)* | both | as above |
| `storage.s3.session_token` | `ORK_S3_SESSION_TOKEN` | *(empty)* | both | for a temporary key; set with both keys, never alone |
| `storage.s3.path_style` | `ORK_S3_PATH_STYLE` | `true` | both | path-style addressing; the dev store needs it, AWS S3 does not |
| `engine.stats_cache_ttl` | `ORK_STATS_CACHE_TTL` | `5s` | `ork` | how long one `GetStats`, `GetFailureBreakdown`, `GetFlakeIndex` or `GetCacheStats` answer is reused; `0` switches the cache off (`docs/STATS.md`) |
| `engine.usage_cache_ttl` | `ORK_USAGE_CACHE_TTL` | `5m` | `ork` | how long one usage measurement (`orkctl stats usage`, the dashboard Usage section, and an enforced budget's admission check) is reused; `0` switches the cache off, which makes every submit into an enforced namespace measure (`docs/STATS.md`, "Usage") |
| `telemetry.log_format` | `ORK_LOG_FORMAT` | `text` | both | process log encoding: `text` or `json` |
| `telemetry.log_level` | `ORK_LOG_LEVEL` | `info` | both | minimum process log level: `debug`, `info`, `warn`, `error` |
| `telemetry.admin_addr` | `ORK_ADMIN_ADDR` | *(empty = no listener)* | both | serves `/healthz`, `/readyz`, `/metrics` (only with the `prometheus` metrics exporter) and `/debug/pprof/` (only with `ORK_PPROF_ENABLED`). Never the API port — see §7 |
| `telemetry.admin_allowed_hosts` | `ORK_ADMIN_ALLOWED_HOSTS` | *(empty)* | both | comma-separated names the admin listener answers to in a request's `Host` header, besides the ones every OrkStack listener admits: an IP address, `localhost` and every name under it, a name without a dot, and the host `ORK_ADMIN_ADDR` names. The grammar is `engine.allowed_hosts`'s, and an entry it refuses stops the boot of either binary. Any other `Host` is refused `403`, so a page that rebinds a public name to the listener cannot read `/metrics` or a profile. List the name a scraper dials when it has a dot: a Prometheus static target such as `host.docker.internal`, or a Service name (§ "The API listener's bounds") |
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

`orker.max_concurrent` is how many task containers one Orker runs at once,
pinned members aside (they run in their pin's own slots).
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

**The ceiling counts tasks, not their size.** Each slot can be taken by a task
of any request up to the per-task caps, `orker.resources.max_cpu` (`8`) and
`orker.resources.max_memory` (`16Gi`), so with the defaults one Orker can
reserve `16 × 8 = 128` cpus. To rule out over-commit by unpinned tasks, size
the ceiling by the caps rather than the defaults:

```
max_concurrent ≤ min(host cpus ÷ max_cpu, host memory ÷ max_memory)
```

A pin's members do not take these slots: they run in the pin's own, beside
the ceiling, so a pin host also needs room for `ORK_ORKER_MAX_PINS` pins'
members running at once, each up to the caps. The bound is pessimistic: a 16-CPU
host with the default 8-cpu cap runs 2 tasks at once, however small they are.
Recover concurrency by lowering the caps to what the pool's tasks really ask
for, and give the rare large task a pool of its own whose Orkers set larger
caps and a smaller ceiling. Set the same caps on every Orker of a pool: a task
above the cap of the Orker that takes it fails `RESOURCE_REQUEST_REFUSED`, so a
pool whose Orkers disagree runs a task or fails it depending on which one
polled first ("Every Orker of a pool has the same settings", below).

### Declaring an Orker's capacity

`orker.capacity.cpu` and `orker.capacity.memory` (`ORK_ORKER_CAPACITY_CPU`,
`ORK_ORKER_CAPACITY_MEMORY`) declare what one Orker admits tasks within, in the
Kubernetes spelling: `"16"` and `"64Gi"`. Either may be set alone; the other
dimension is then not bounded.

**Unset, the default, admission is best effort.** The Orker admits by
`max_concurrent` alone, a count of tasks: nothing holds the sum of their
requests to what the host has, and the Orker never measures the host to find
out, because it cannot know what else shares it. A host that runs out of
memory may kill a container, or the runtime or the Orker itself; a task whose
container is killed fails as any killed workload does, retried under its own
policy. Size `max_concurrent` by the caps (above), or
declare a capacity. Such an Orker says so at boot:
`capacity not declared: admission is best effort, bounded only by max_concurrent`,
with its `max_concurrent` and per-task cap.

An Orker that declares a capacity admits no task that would take its
reservations above it. A Temporal worker reserves a slot before it polls and
cannot see the task until it has taken it, so the Orker holds each poll to the
one bound it knows in advance, the per-task cap (`orker.resources.max_cpu`,
`max_memory`):

- every poll for a task reserves the cap, and is granted only while that fits
  beside everything already reserved;
- once the task is in hand, its reservation is trimmed to the task's own
  resolved request, and an activity that runs no container to nothing. A
  request above this Orker's cap resolves to nothing, so it reserves nothing,
  whether it is refused or, for a result already committed or a memo hit,
  handed back without a container;
- the reservation is freed when the attempt's container is **removed**. A
  container whose removal failed keeps its reservation, and the Orker tries to
  remove it again every 30 seconds, as it does with no capacity declared
  ("What removes a task's container").

`orkctl orker list` shows `CAPACITY` and `RESERVED` for each Orker that declares
one. `RESERVED` counts each outstanding poll at the per-task cap, so an idle
Orker shows its two pollers' share; a container that could not be removed is
logged on every reap that fails, with its id. Boot refuses a capacity that does not read, or that the per-task cap does
not fit in (`ORKER_CAPACITY_INVALID`). `max_concurrent` stays a count ceiling
beside it.

**What it costs.** A poll needs a whole cap free, so an Orker whose capacity
equals its cap runs one task at a time, whatever their size: with the default
8-cpu cap, a 16-cpu Orker holds at most two polls. Keep the cap small relative
to the Orkers that serve a pool, and give the rare large task a pool of its
own. A cache hit or an adopted commit starts no container, but it is admitted
like any task, so under pressure it waits for a cap's worth too; it is priced
as today, reservation × held time.

**Pinned members are counted, not gated.** A pin's members run in the pin's own
slots, which no capacity can gate without breaking the pin's heartbeats or
deadlocking two pins, so a member's request joins the ledger on arrival and may
push it over capacity. While the reservations stand at or over capacity the
Orker grants no poll, so ordinary work yields and the overshoot drains. The
overshoot is bounded by `ORK_ORKER_MAX_PINS` × what one pin runs at once: leave
that much headroom on a pin host. A hard bound for pins is not provided.

**The cap is a pool-wide setting**, as pin hosting is. A submit asks every
uncordoned Orker of a task's pool (counting one that is only waiting out a store
outage) for its cap, on its own channel, and refuses
a task that declares a request above every one of them
(`RESOURCES_UNSATISFIABLE`), before a run id exists. That read costs a submit
whose tasks declare `resources:` one describe per live Orker of the deployment,
together, each bounded at five seconds; a submit that declares none pays
nothing for it. Set the same capacity, cap and default on every Orker of a
pool, with the rest of the settings the next section lists.

**After a crash**, the Orker's restart reaps its previous life's containers.
One it could not remove still holds its host, so a declared capacity starts
with that container's cap reserved, and frees it once a later reap removes it.

**Seeing a pool at its capacity.** A task no Orker has started is `RUNNING`,
waiting in its pool. When every Orker of that pool that takes new tasks
declares a capacity, runs a task and has no room for another within it, the run
page captions the task `Waiting for capacity · every Orker of pool <pool> is
full (reserved 4/4 cpu) · 2m 10s`, and `orkctl run inspect` prints
`<task>: waiting for capacity for 2m10s (scheduled …): every orker of pool
<pool> is full (reserved 4/4 cpu)`. The figures are summed over those Orkers,
for each dimension their capacity bounds. Otherwise — an Orker of the pool with
room but every slot taken, one declaring no capacity, one running nothing (what
it reserves is then its own polls, which take the next task), a pinned member,
a retry waiting out its backoff, an attempt waiting less than two seconds —
the run page says only `Waiting for capacity · 2m 10s` and `orkctl run inspect`
keeps `waiting for an orker`. No room means the
per-task cap no longer fits beside `RESERVED`, so a pool can read full with
less than its capacity reserved: an Orker with a 2-cpu cap and 3 of 4 cpus
reserved takes nothing more.

It is derived on read, never stored as state: `InspectRun` sets
`capacity_wait` on a waiting attempt from the Orker inventory, the rows
`orkctl orker list` shows. The inventory is read only when an attempt has
waited for an Orker for two seconds — one presence read and one describe per
live Orker, the cost of one `orkctl orker list` — and each engine replica
shares one reading among every caller: a read in flight is joined by every
answer that asks meanwhile, and a whole reading, one every live Orker
answered, answers for five seconds from when its read began, the run page's
and `orkctl run inspect -w`'s own poll period. A read ends at two
seconds whether or not the Orkers have answered, so no answer waits longer.
However many pages and watches are open, while its reads are whole a replica
asks the Orkers at most once every five seconds for them, and nothing once no
task waits; a caption is never older than one poll. While any live Orker does
not answer within the bound, no pool is called full, because the one that did
not answer could serve it with room: for the 90 seconds a stopped Orker stays
listed, every waiting task reads `Waiting for an orker`, as it does when the
inventory cannot be read at all, and the replica reads again for each answer,
still one read at a time. A failed read, or one some live Orker did not
answer, is never kept, and the engine warns of a failed one at most once a
minute.

**Capacity metrics.** With metrics on (`OTEL_METRICS_EXPORTER`, "Metrics"
below), an Orker that declares a capacity exports, on the default sink:

| Metric | Labels | What it reads |
|---|---|---|
| `ork.orker.capacity.cpu`, `ork.orker.capacity.memory` | `ork.orker_id` | the capacity, one gauge per dimension it bounds; an unbounded dimension has none |
| `ork.orker.capacity.reserved.cpu`, `ork.orker.capacity.reserved.memory` | `ork.orker_id` | what stands reserved, `RESERVED` in `orkctl orker list` |
| `ork.orker.admission.wait.duration` | `ork.orker_id`, `ork.admission.bound` | seconds a poll for a task waited for room before it was granted; `ork.admission.bound` is `capacity`, or `slots` when every `max_concurrent` slot was taken |
| `ork.orker.admission.refusals` | `ork.orker_id` | attempts refused `RESOURCE_ADMISSION_MISMATCH` |

The gauges read the ledger when the meter collects. Under `prometheus` they
render as `ork_orker_capacity_cpu`, `ork_orker_capacity_reserved_memory_bytes`,
`ork_orker_admission_wait_duration_seconds` and
`ork_orker_admission_refusals_total`. A poll waits whenever the Orker has no
room, whether or not a task is queued — an idle Orker whose capacity holds one
cap waits with its second poller — so the wait measures how long the Orker had
no room to ask for another task, not how long a task waited (`orkctl run
timing` answers that). A wait is recorded when its poll is granted, so a poll
still waiting is not yet in it; `reserved` at or above `capacity` shows that
one. Overshoot by pinned members is `reserved` minus `capacity`. Recording never blocks admission, and an Orker with metrics off
records nothing.

The ledger is held in the process, as a cordon is: never stored, never shared,
gone with the process. It is admission, not placement: Temporal decides which
polling Orker receives a task.

### Every Orker of a pool has the same settings

Temporal hands a task to whichever Orker of its pool polls for it, and
OrkStack never chooses a host, so a run can count only on what every Orker of
the pool offers. Give each Orker of a pool the same:

- per-task caps, `orker.resources.max_cpu` and `max_memory`;
- default request, `orker.resources.default_cpu` and `default_memory`;
- capacity, `orker.capacity.cpu` and `memory`, and `orker.max_concurrent`;
- secret schemes and floor, `ORK_ORKER_SECRET_SCHEMES` and
  `ORK_ORKER_SECRET_ALLOW`;
- `env` allow-list, `orker.env_allowlist`, and the values of the variables it
  names;
- registry credentials, `orker.registry_auth_file`, with which an Orker reads
  a signature when a namespace requires one (`require_signed_by`);
- runtime: `orker.resources.force_user`, and whether a task can run as the
  Orker's own user. Run a pool on runtimes where it can, or on runtimes where
  it cannot, not both (a rootless or user-namespaced daemon cannot;
  [REASONS.md](./REASONS.md), `RUN_AS_ORKER_UNAVAILABLE`);
- pin hosting, `ORK_ORKER_MAX_PINS` ("Pin hosting is a pool-wide setting",
  below).

Network modes need no setting: every Orker offers `default` and `none`.

While the settings match, a run never fails because of which Orker took a
task. When they differ, a task that lands on an Orker that will not run it
fails there, explicitly, with its own reason: `RESOURCE_REQUEST_REFUSED`,
`SECRET_SCHEME_FORBIDDEN`, `SECRET_NOT_PERMITTED` or
`RUN_AS_ORKER_UNAVAILABLE`, none of them retried; `RENDER_FAILED`, not
retried, for an `env` name outside the Orker's allow-list; or
`IMAGE_SIGNATURE_UNAVAILABLE`, retried under the task's policy, for a
signature its registry credentials cannot read. A differing default request
gives a task that declares none different limits on different Orkers, and a
differing capacity or `max_concurrent` fails nothing, only spreads the work
unevenly.

What a task that runs no container here is spared depends on why it runs
none ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md#71-the-sequence),
"§7.1"):

- **A result already committed** is never refused for any of these settings:
  the commit check comes before everything that refuses for them.
- **A memo hit** is never refused for the caps, the secret schemes and floor,
  or the runtime: those are checked only after the memo step misses. But a
  cached task's image policy is checked before its memo lookup, and its
  `with:` is rendered before it too, because the memo key hashes the rendered
  `config.json`. So a differing `env` allow-list refuses it when its `with:`
  names an `env` variable, and differing registry credentials refuse it when
  the run's image policy requires a signature, even where the memo would hit.

#### Changing a setting with runs in flight

Change a setting in the order a schema migration uses: widen every Orker
before any task needs more, and stop needing it before you narrow them. It
works with an ordinary rolling update, versioned or not.

**To widen** (raise a cap, allow another scheme or source):

1. Roll every Orker of the pool to the wider setting, and let the rollout
   finish (`orkctl orker list -F pool=<pool>` lists the pool's live
   Orkers).
2. Only then create and promote the Orkflow version that needs it
   (`orkctl orkflow create`, `orkctl orkflow promote <name> <version>`).

During the rollout no task asks for more than the old setting allows, so
Orkers that still differ are harmless.

**To narrow** (lower a cap, allow less):

1. Promote the Orkflow versions that no longer need the wider setting.
2. Repoint whatever pins an older version. `orkctl orkflow callers <name>
   --all-versions` lists the schedules, triggers and composing tasks that pin
   one. Move a schedule with `orkctl schedule update <name> --version <v>` (or
   `--follow-current`), a trigger with `orkctl trigger update <id> --version
   <v>` (or `--follow-current`), and a composing task by applying and promoting
   a version of its Orkflow that names the new version. Do not use
   `orkctl run again` on a run from before the promotion until the narrowing is
   done: it resubmits that run's pinned version unless `--version` says
   otherwise.
3. Wait until the runs that may still need the wider setting have finished:
   in every namespace whose runs use the pool,
   `orkctl run list -F 'status=PENDING,RUNNING' -F 'created_at<T'`
   lists none, with `T` the instant of the last repoint in step 2, an RFC 3339
   instant with a zone such as `2026-10-30T12:00:00Z`. A schedule or trigger
   still pinned to an older version can start a run until it is repointed,
   and so can a submit that names that version with `--version`. A parent run
   stays running while its child runs do, so this covers composed tasks too. A
   task that has already committed is adopted, never run again, so only work
   still to run matters.
4. Roll the narrower setting out to every Orker of the pool.

With versioning on, a release is already a generation of settings: change the
setting in a new release and roll it forward ("3. Rolling a release forward",
below): new runs go to the new release's Orkers, and a run already in flight
keeps to its own release's Orkers while they drain.

**What it does not cover:**

- an Orker configured wrong by mistake;
- an Orker that joins late with stale settings, such as one started from an
  old template;
- runs that stay open a long time, such as one waiting on an await: they only
  make step 3 take longer.

Either of the first two fails the run explicitly, with its own reason, as
above.

### Pin hosting is a pool-wide setting

Pin hosting is a property of a **pool**, not of a process: set the same
`ORK_ORKER_MAX_PINS` on every Orker of a pool. The submit-time probe that
refuses a pin with no host to open it on asks every uncordoned Orker of the
pool, each on its own control channel, and accepts the pin when any one of
them hosts pins; a pin accepted that way still opens only on a pin host, so in
a mixed pool it waits for that host's slots alone.

The refusal is `failed_precondition`, the same code as an empty pool, and it
names the pool, the pins, the tasks and the fix:

```
pool "gpu" (namespace "ci", pin "build", tasks: clone, compile, test) has live
Orkers but none of them hosts pins, so the pin could never open. Start one with
ORK_ORKER_POOL=gpu ORK_ORKER_MAX_PINS=1, or submit with allow_empty_pool to wait
for one
```

`allow_empty_pool` downgrades it to a logged warning, as it does for an empty
pool. A pool none of whose Orkers answers the probe is not a refusal —
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
dependent — a silently wrong result. So submit asks every uncordoned Orker of
the pool of every fan-out and of every direct dependent of one, each on its
own control channel, whether it supports fan-out, and refuses the run with
`FailedPrecondition` naming the pool, the tasks, and the first Orker (by id)
that does not and its build; `allow_empty_pool` does not override that. Any
Orker of the pool may take an item, so one old Orker refuses the pool: run
every Orker of one pool on one release, or cordon the old ones while they
drain.

Caching (`cache:`, `docs/DSL.md` "Caching") is the same: an Orker built before
caching cannot even decode a cached task's input — the SDK's payload converter
refuses the unknown field — and would fail every attempt for a reason that has
nothing to do with the task. So submit asks every uncordoned Orker of the
pool of every cached task (a cached fan-out's items run on the fan-out's pool)
whether it implements caching, and refuses with `FailedPrecondition` naming
the pool, the tasks, and the first Orker that does not and its build; neither `allow_empty_pool` nor
`no_cache` overrides that (a bypass still sends the cache context). Memos live
under `metadata/memo/` and need no setting of their own.

Failure handlers (`join: any_failed` or `all_done`, `docs/DSL.md` "Joins and
failure handlers") are the same: an Orker built before join policies cannot
decode a failure handler's input — the SDK's payload converter refuses the
unknown field — and would fail the compensation step of a run that is
already failing. So submit asks every uncordoned Orker of the pool of every
failure handler that runs a container whether it reads join policies, and
refuses with `FailedPrecondition` naming the pool, the tasks, and the first
Orker that does not and its build; `allow_empty_pool` does not override that. A
`none_failed` join needs nothing from an Orker.

All four probes ask each Orker on its own control channel, never on the
pool's channel, so a pool whose every task slot is taken answers them like an
idle one. They are best-effort: a pool none of whose uncordoned Orkers answers
within the describe timeout is not refused — nothing was measured — and the
engine logs a warning (`submit: no uncordoned Orker of a live pool with …
answered on its own channel, so whether … is unknown`). The probe is a guard
for a mixed rollout, not a substitute for running every Orker of a pool on one
release.

### Upgrading a pool's Orkers

From v0.4.0 an Orker numbers each task's attempts itself, claiming an ordinal
in the attempt store before the attempt starts anything (`docs/STORAGE.md`,
"Attempt ordinals"). An Orker of an earlier release numbers them as Temporal
does, so while one pool runs both, an older Orker can write an attempt record
over a newer one's, and the task's attempt list shows the wrong outcome for
that attempt. Nothing about the run's result changes: records are a history,
never what decides whether work reruns. Roll every Orker of a pool to the new
release together, or cordon the old ones while they drain (`orkctl orker
drain`).

Three things make a mixed pool visible rather than silent:

- a newer Orker never overwrites a record: records are written create-only,
  and one already there is kept, with a warning (`task attempt record not
  written: an Orker of an earlier release already wrote one under this
  attempt's number`);
- a newer Orker that finds a record with no claim in a task's scope starts its
  ordinals past it and logs `an attempt record of this task was written by an
  Orker of an earlier release`, naming the run, task and attempts; both cases
  count `ork.orker.attempt_record_legacy`, by where the record was met
  (`ork.found`: `claim` or `record`), so any count means the pool runs two
  releases;
- a submit that already asks a pool's Orkers a question (a fan-out, a cached
  task, a failure handler, a sized task, a pin) also asks every live pool of
  the run whether its Orkers claim ordinals (`DescribeOrkerResult`
  `attempt_ordinals_supported`), and refuses with `FailedPrecondition` naming
  the pool, the tasks, and the first Orker that does not and its build. A
  submit that asks nothing else asks nothing for this either, so it is no
  substitute for rolling a pool whole.

On a versioned deployment (section 2) a Pinned run's tasks wait for its own
release's Orkers even when none of them is polling, so the two numberings meet
only when the run's release never served the pool and its tasks there go to
the current release's Orkers: keep every release you ramp or promote serving
every pool in use, as `orkctl release` already requires.

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
export ORK_S3_ACCESS_KEY=…        # or neither key, to use a role: see below
export ORK_S3_SECRET_KEY=…
export ORK_S3_PATH_STYLE=true     # the dev store needs path style; AWS S3 does not
```

The store must support conditional `PutObject` (`If-None-Match`/`If-Match`,
an `If-Match` on a missing key refused with 404 or 412) and conditional
`DeleteObject` (`If-Match`, a wrong etag refused with 412):
commits, Orkflow pointers and registry documents are arbitrated by it, and
every Orker probes each of these answers at boot and refuses to start with
`COMMIT_STORE_NOT_CONDITIONAL` on a store that gets one wrong; every engine
asks the two Orkflow rollbacks rely on before its API accepts an apply, and
refuses to start naming the answer it got. Put
nothing between OrkStack and the store that resends a request on its own (a
retrying load balancer or proxy): OrkStack takes the store's refusal of a
conditional write's only send as final, and a silent resend of a write that
landed would be refused by that write itself (docs/STORAGE.md). Any
S3-compatible store with that primitive works; the production default is AWS
S3. The local development stack uses **SeaweedFS** (Apache-2.0, pinned by
digest in `deploy/docker-compose.yml`, S3 API on `localhost:9000`), because
MinIO stopped publishing images and Garage cannot do conditional writes. What
lives under which key, who writes it and what removes it is
`docs/STORAGE.md`.

### The object store's credentials

`ork` always signs requests to the object store, and an `orker` does when its
`ORK_ARTIFACT_BACKEND` is `s3`. No key has a default. What signs depends on what
you set:

| You set | What signs |
| --- | --- |
| `ORK_S3_ACCESS_KEY` and `ORK_S3_SECRET_KEY`, and `ORK_S3_SESSION_TOKEN` for a temporary key | that key, whatever else the environment holds |
| none of them | the AWS SDK's default credential chain: `AWS_ACCESS_KEY_ID` and its siblings, the shared credentials and config files, a web identity token, a container's credentials endpoint, then the instance role |
| one key alone, or a session token with no key | nothing: the boot stops, naming both keys |

The boot asks for the credential before it opens a store. With nothing to sign
with it stops and says so, rather than a run failing later. Otherwise it logs
`object store credentials resolved` with the `source` that answered, and never
the credential. An `orker` on the `local` backend signs nothing, so it asks
nothing of the chain, but it still refuses half a key.

The development store's key is set only by what starts the development stack:
`make up`, `make run-ork`, `make run-orker`, `make site-media`'s own Orker, the
quickstart's `compose.yaml`, the `kind` overlay, the tests and the proofs. The
store refuses any other.

**To use an instance or pod role:**

1. Leave both keys and the session token unset on every `ork` and `orker`, in
   the environment and in the configuration file.
2. Give the role read, write, list and delete on the bucket.
3. On AWS S3, set `ORK_S3_ENDPOINT=https://s3.<region>.amazonaws.com`,
   `ORK_S3_REGION` and `ORK_S3_PATH_STYLE=false`. The default endpoint is
   `http://localhost:9000`.
4. On Kubernetes, bind the role to the pod's service account, and create the
   `orkstack-s3` Secret with no keys:
   `kubectl create secret generic orkstack-s3`. The bases require that Secret,
   so a forgotten one stops the pod rather than falling through to the node's
   role.
5. Keep tasks away from the role. A container on the `default` network can
   reach the host's instance metadata service unless the host stops it, and
   that service hands out the role's credentials, which a task must never get
   ([AGENTS.md](../AGENTS.md) §2.3). OrkStack cannot stop it from inside a
   container's network. On EC2, require IMDSv2 with a hop limit of 1, which a
   container on a bridge network cannot answer across:
   `aws ec2 modify-instance-metadata-options --instance-id <id> --http-tokens required --http-put-response-hop-limit 1`.
   A rootless engine's user-space network sends a container's packets from a
   process on the host, so the hop limit may not stop them: block the address
   with a firewall rule there. On Kubernetes, prefer a role bound to the pod
   over a role on the node. A container's credentials endpoint (ECS, EKS Pod
   Identity) answers only with a path or token the Orker holds, under `AWS_*`
   and the token file those variables name, and no secret floor admits either
   to a task. A task with `network: none` reaches none of these services.

The SDK refreshes a role's temporary credentials. When it cannot, requests fail
as they do in any object-store outage.

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
`orkctl run logs -f` does this by itself when the engine goes away under it, as
`run watch` does: after an `unavailable`, a `canceled` it did not cause, the end
of its connection, or a clean end with no `last` chunk, up to six times in a row
without a chunk, saying each resume on stderr. Any other error ends it, and a
read without `-f` is not resumed.

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
deliver nothing. That would lose lines when a task retries while a follower is
away, so after a resume `orkctl run logs -f` reads the rest of the cursor's
attempt, and each attempt up to the one the engine moved to, before going on. A
line an Orker the cluster already gave up on writes after its retry began is not
seen, by a resumed follower or by an uninterrupted one. An explicit `--attempt N` is pinned and never moves. Every
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
space"). The archive body, its index rows and the cleanup sidecar are
permanent, so a retired run stays listable and readable.

**An execution the cluster no longer holds** is retired only once its own
namespace's run retention, counted from the finish its archive body records,
has passed. Nothing removes an execution earlier in normal operation — the
backstop TTL sits past every namespace's retention, and never below an
idempotency key's life (`engine.idempotency_ttl` plus five minutes) plus the
margin, so a retry under a key finds a finished run — so one missing earlier
means a restored or damaged cluster, or an `ork` replica pointed at the wrong
`ORK_TEMPORAL_NAMESPACE`. Such a run is kept, logged at error ("kept a run
whose execution is missing before its run retention") and counted
(`ork.archive.cleanup.runs{outcome="not_found_not_due"}`; alert on any), and
every pass examines it again until its retention has passed, when it is retired
like any other: its retention is the namespace's promise, and a missing
execution does not shorten it. A run whose body is missing, unreadable or
records no finish cannot be dated and is never retired; each pass fails on it
and logs it. When more than a tenth of the runs one pass looked up (at most
`ORK_CLEANUP_BATCH_SIZE`) are missing before their retention, that pass retires **no** run whose execution is missing,
even one already due (`not_found_held`), because it is reading the wrong cluster
or a damaged one, and every later pass holds them too until one whole walk of
the index has had no pass over that share (a restart forgets the hold; each run
is still checked against its own retention). Runs the cluster still holds are
retired as usual. After a
cluster restore, or before pointing a replica at another Temporal namespace,
set `ORK_CLEANUP_ENABLED=false` until the cluster is confirmed.

On S3 a run's four run-scoped prefixes are emptied concurrently, one
`DeleteObjects` request per listing page (up to 1000 keys), so the store must
support `DeleteObjects` (AWS S3 and SeaweedFS do). A key the store refuses fails
that run's cleanup — it is not marked cleaned and a later pass retries it.

**A backlog drains without waiting.** `ORK_CLEANUP_BATCH_SIZE` bounds one
pass, not the rate: a pass that processed a full batch starts the next pass at
once, resuming the index walk after the last run it examined, and the worker
returns to `ORK_CLEANUP_INTERVAL` after the first pass that reaches the end of
the index. A pass whose walk failed, or in which at least half the runs it
examined failed, does not drain at once: it waits one `ORK_CLEANUP_INTERVAL`
and then resumes where it stopped, so an outage of the cluster or the store is
not walked through at full speed. One run that keeps failing among others does
not slow the drain. A backlog of retired runs therefore clears at the store's and the
cluster's speed, a batch at a time; the next ticked pass walks from the start
again, so a run a drain passed before it was due is examined once it is.

Cleanup **fails closed**: if the retention policy or any namespace's own value
is unreadable, or the stored policy holds no positive maximum run retention,
that iteration deletes nothing and leaves the backstop as it is; an archived run the cluster
reports as STILL RUNNING is refused rather than retired (deleting would
terminate it, and a run is archived only at its end, so it is a defect to
surface); per-run storage failures are logged and retried by a later
pass; a repeated pass is a no-op because the `artifacts_cleaned` sidecar
records what was done.

**The archive sweep** runs on every `ork` replica whether or not cleanup is
enabled, because it only writes and the cluster's own retention fires either
way. A run's terminal publish writes its archive, so a run terminated from
outside the workflow runs nothing that could, and a run whose ending object
storage refused has that write refused too. Each pass lists the runs the
cluster holds that closed at least `ORK_CLEANUP_INTERVAL` ago, asks the archive
whether each has a body, and builds any missing archive
from the run's history, the same body a run's own write leaves: the run a live
`GetRun` answers, and its whole stream. It stores nothing, so any replica's pass
is complete and two replicas converge. From then on the run is an ordinary
archived run: listed, retired at its namespace's run retention, readable from
any replica after the cluster lets it go. Its cost is one `HEAD` per finished
run the cluster holds, on every replica, every pass: at one run a second and the
default 96 hours in the cluster, about 350,000 an hour per replica. Raising
`ORK_CLEANUP_INTERVAL` lowers that, but two intervals must stay well below the
time a run has in the cluster: a run waits out one interval and then the next
pass, and one deleted before a pass looks at it leaves nothing to report. Each
pass logs an error when two intervals reach that time. What cannot be recovered
is said:

- events lost from an earlier part of a long run, and numbers the run never
  published, are `gap` lines (`docs/STORAGE.md`);
- a record of the run's tasks that is gone or does not decode is replaced by the
  one before it, and the run's `Error.details` says its tasks are shown as of an
  earlier event; when that one cannot be read either, the run's name, input and
  outcome come from its history, its tasks are not shown, and `Error.details`
  says so. A run with no error of its own gets `RUN_RECORD_INCOMPLETE`
  (`docs/REASONS.md`), and keeps its status: a run that succeeded still reads
  `SUCCEEDED`.

A read that fails in a way that may heal, such as object storage not answering,
writes nothing and is tried on the next pass: an archive is immutable, so an
outage's holes must not be written into one.

**What it cannot do.** The cluster fixes a run's deletion when the run closes,
as its close plus the cluster's retention at that moment, and nothing a client
does postpones it afterwards: raising the retention moves only runs that close
later. (Read in the source of Temporal server 1.32.0, which the
`temporalio/temporal:1.9.1` image this repository pins bundles:
`GenerateDeleteHistoryEventTask` and `executeDeleteHistoryEventTask`. It has not
been tested against a running cluster.) So a
finished run with no archive is **lost** if object storage is down at every pass
from a cleanup interval after its close until that deletion: by default 95 hours
of the 96 the maximum run retention and `ORK_CLEANUP_RETENTION_BUFFER` add up
to. Nothing is left to say it existed. A run that already has its archive is
not at risk, and neither is a run that has not finished. While the risk stands,
every pass logs an error naming the runs nearest to expiry and when the first of
them expires, and sets `ork.archive.sweep.unconfirmed` and
`ork.archive.sweep.earliest_expiry` (§7, "Lifecycle metrics"); alert on the
first. A pass that cannot see sets `ork.archive.sweep.list_failed` or stops
advancing `ork.archive.sweep.last_success`; alert on those too, because a blind
sweep cannot count what it did not see. While object storage is down the pass
cannot tell an archived run from one that is not, so it counts every finished
run it holds, and its figures are an upper bound. A longer outage than you can
afford calls for a larger `ORK_CLEANUP_RETENTION_BUFFER`, which helps only the
runs that close after it is set.

An archive write that stops short, on all three attempts of the run's own
publish, leaves no body, because the body is its last write. The run reads as
not archived and the sweep finishes it. Cleanup never retires a run the cluster
still holds until its body is there. A release before this wrote the body first,
so a write of that time that stopped short keeps its body: the run reads by id,
and the sweep does not repair it, so it is not listed and not retired.

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
| `metadata/delivery-bodies/` | bodies of webhook deliveries still being sent; each delivery deletes its own when it settles, so a rule only reclaims those of a delivery stopped from outside. Keep it at a day or more: a shorter rule can expire a body before its last attempt, which then records the body as gone |
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
> — `api@v1.64.0/workflowservice/v1/request_response.pb.go`,
> `SetWorkerDeploymentCurrentVersionRequest.BuildId`

which a versioned release does not have. So a deployment's first versioned
release, the first to set `ORK_BUILD_ID`, boots two healthy, polling workers
that receive **nothing**:
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

Backlogged runs start within seconds of it. §3 step 0, "First versioned
release only", is the full procedure; every release after that inherits a
Current Version from its
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
> — `api@v1.64.0/enums/v1/workflow.pb.go`, `VERSIONING_BEHAVIOR_PINNED`

So if the Orker polling `ORK_ORKER_QUEUE` does not carry the engine's exact
`(ORK_DEPLOYMENT_NAME, ORK_BUILD_ID)`, the exception fires and an in-flight
run's tasks land on whatever is current — a newer Orker build, mid-run. Setting
`ORK_BUILD_ID` on the engine and forgetting it on the Orker is therefore the
one misconfiguration to watch for. Check both boot lines print the same
`version=`.

### What is versioned, and what is not

| worker | queue | versioned | behaviour |
|---|---|---|---|
| run worker (`ork`) | `ork-tasks` — **fixed** | yes | `RunWorkflow` = **Pinned**, `ScheduleFireWorkflow` = **Pinned**, `WebhookDeliveryWorkflow` = **Pinned**, `IdempotencyClaimWorkflow` = **AutoUpgrade** |
| Orker (`orker`) | `ORK_ORKER_QUEUE` (default `ork-tasks`) | yes | task activities only; must match the engine |
| projection worker (`ork`) | `ork-engine-events` | **no** | `PublishEvent`, and a webhook delivery's `SendWebhookAttempt` and `SettleWebhookDelivery` |

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

**Why a webhook delivery is Pinned.** `WebhookDeliveryWorkflow` is one
delivery: an activity that makes one `POST`, retried by its policy, and one
that records the delivery ([WEBHOOKS.md](WEBHOOKS.md), "Attempts and
retries"). It lives about a minute at the default policy, so it holds its
version open no longer than the runs whose transitions queued it, and its
history never replays on a build it did not start on. Its activities run on
the unversioned `ork-engine-events`, so while engines of a release older than
the one that queued a delivery still poll it, an attempt can reach one that has
no such activity; that attempt fails and counts against the delivery's four.
Finish a rollout's engines before relying on delivery during it.

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

0. **First versioned release only: make it current before you rely on it.**

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

A stored Orkflow version outlives the release that created it. An older
engine refuses to *create* a construct it does not know, but asked to *run* an
already-stored version it decodes the definition with unknown fields dropped.
So, during a ramp, create a document that uses a construct only once the
release that introduced it is Current (versioned) or on every replica
(unversioned); and before rolling back past it, stop submitting versions that
use it. What an older release does with each:

| construct | an older release running a stored version that uses it |
|---|---|
| `await:` | treats the await as a container task and fails it — explicit |
| `each:` (fan-out) | runs the node once as a plain task, staging no items for its dependents; fails `RENDER_FAILED` only if `with:` reads `.each` — **silent** otherwise |
| `uses: orkflow:` | an older run worker cannot decode the start (`Task.orkflow`, `RunPlan.children`) and retries the workflow task until a new worker takes it — under Pinned, possibly never; a composed task reaching an older Orker fails `IMAGE_NOT_PINNED` |
| `outputs:` | returns the values of the tasks no other task needs instead of the declared outputs — **silent** |
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
> — `api@v1.64.0/enums/v1/workflow.pb.go`, `VERSIONING_BEHAVIOR_UNSPECIFIED`

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
(`internal/engine/retry_spacing.go`, `retryPolicyFor`), so the task fails.

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
  --workflow-id ork-01m6qt8vete0691h2jn074a2yk \
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

**Rolling back below 0.2.0, the release that moved to the Temporal Go SDK
1.50**, is one-way for the runs it executed. From 0.2.0 the engine records a
marker in a run's history (the SDK's flag 9, ordered child cancellation) the
first time one cancellation reaches more than one of the run's open scopes,
so that they are canceled in the order they were opened. That happens on an
operator's cancel of a run that declares a failure handler, and on a run that
fails or is canceled while two or more of its awaits or pins are open. An
older engine does not know the marker. Handed such a history, it fails the
run's workflow task on every attempt (`unknown SDK flag: 9` in its log), and
the run stays `RUNNING`, making no progress, until an engine of 0.2.0 or
later picks it up again. Nothing is lost, but nothing moves. On an unversioned
fleet any engine may execute a run's next workflow task, so a run that started
on the older release can record the marker too. So:

- with versioning on, roll back as above: a run stays on the build that
  started it, so 0.2.0's runs finish on 0.2.0, whose processes keep running
  until `orkctl release wait` reports it drained;
- with versioning off, roll back below 0.2.0 only when no run is open. Any
  run that was open while a 0.2.0 engine ran may carry the marker, whenever
  it was created, so the check counts every open run, not only those created
  since 0.2.0. For each namespace that takes runs:
  1. hold it: `orkctl namespace update NS --hold "rolling back"`;
  2. wait until
     `orkctl run list -n NS -F 'status=PENDING,RUNNING'` lists no run.
     A run waiting on an await stays open until the await is resolved or its
     deadline passes, which may be hours: resolve it
     (`orkctl run resolve`) or cancel it (`orkctl run cancel`) rather than
     wait;
  3. once every namespace is empty, replace the engines, then release each
     hold: `orkctl namespace update NS --release-hold`;
- an unversioned fleet that mixes older and 0.2.0 engines meets the same
  failure whenever an older engine picks up such a run, so replace every
  engine replica together when rolling forward to 0.2.0.

**Rolling back below 0.3.0, the release whose runs no longer wait for each
task's start events to be published**, is one-way for the runs it started. A
run that a 0.3.0 engine submitted pins a publish window in its plan and
records the `task-publish-window` marker at its first plain task (§5, "A run
that pauses new tasks"); it then schedules several tasks' publishes in one
workflow task. Every run a 0.3.0 engine starts also records the
`emit-past-cancel` marker at its first event, and an older engine cannot
replay that either. An older engine replays such a history with every publish
awaited, so its commands differ from the recorded ones: it fails the run's
workflow task on every attempt, and the run stays `RUNNING`, making no
progress, until an engine of 0.3.0 or later picks it up again. A run submitted
by an older engine pins no window and keeps the awaited path on every build.
So the rules are 0.2.0's: with versioning on, roll back as above; with
versioning off, roll back below 0.3.0 only when no run is open, through the
same hold; and replace every engine replica together when rolling forward to
0.3.0.

**A release that lets runs carry on into a fresh history** ships after the
release that reads them (below), and the default turns on only in a release
after one that already contained the writer. Rolling back below the writer,
but not below the reader, while runs are carrying on fails those runs
explicitly, `RUN_STATE_UNSUPPORTED` with a terminal event: an older release
refuses a saved-state format it does not know. Turn
`engine.continuation.enabled` off first and let runs that carry on finish
(the check below lists them).

**Never roll back below the build that introduced the read side of
continue-as-new**, which reads runs carried on into a fresh history. That
build shipped before any build that lets a run carry on, and it stays the
floor; every release from 0.1.0 contains it:

- a release older than it that picks up a run's later generation ends that
  generation before any side effect — the run fails, and every reader of this
  release or later reports `RUN_STATE_UNSUPPORTED` ("this run was carried
  forward by a newer release and an older release could not carry it on; roll
  the engines forward") — but it publishes no terminal event, so its archive
  comes from the archive sweep (§1), and under a release older than the sweep
  it has none and becomes unreadable once the cluster's retention expires it;
- once any run's archive records a gap in its events (a `gap` line,
  `docs/STORAGE.md`), a rollback below it is forbidden permanently: an older
  release refuses the unknown archive line, so every read of that run fails.

Check before rolling back:
`temporal workflow list --query 'WorkflowType="RunWorkflow" AND ExecutionStatus="ContinuedAsNew"'`
lists the closed generations of runs carrying on; while it returns any, do
not go below that build.

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
follow (Temporal server 1.32, which the development image pins). The
`temporal` service of `deploy/docker-compose.yml` passes
`--dynamic-config-value` for both, `10s` and `3s`, so every test that waits for
a release to drain waits seconds rather than minutes. The proofs read the same
image and command from that service for the Temporal they start for
themselves (`proofrun temporal-args`, docs/PROOFS.md), so the settings reach
them too. They are development and proof settings, never a recommendation:
a shorter grace period can report a version drained while a workflow that
started on it has not yet reached visibility, and every refresh is a
visibility query and a new run of the version's own workflow on the cluster.
The quickstart's Temporal (the root `compose.yaml`) and every
production cluster keep the defaults.

The same service also passes `history.transferProcessorUpdateAckInterval` at
`5s`, against a default of `30s`. A closed execution's delete runs only once its
close task reads as acknowledged in the transfer queue's saved state, which this
setting saves, and which reaches only as far as the oldest task still pending.
The development server has one shard, so overlapping deletes hold each other
back: on this image a delete beside others took 31.9 to 136 seconds at the
default and 3.9 to 11 at `5s`. It is a development and proof setting too, and a
tradeoff a production cluster has no reason to take: it saves that state six
times as often to make a delete show sooner.

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
  nothing on either. Every other route is the operator surface, and that
  includes the web UI on the same port: `/`, its assets under `/_app/` and
  every route of the app, so a gateway that routes by path sends those to
  `ork` too (§0, "The web UI").
- **Activity inspects answered from memory.** The catalog read
  (`ListActivities`) carries each image's interface, icon and descriptions,
  reused for 5 minutes. An `InspectActivity` that sets `allow_cached` for a
  reference pinning a digest — what a client asks about the images a run's
  tasks ran — is answered from a per-process cache once that process has
  read the digest: at most 512 answers or about 16 MiB of memory, least
  recently used first out, never a failure, never a tag. Any API caller can
  fill it, and it stays bounded. A replica that restarts reads each digest
  once more. Without `allow_cached` (`orkctl activity inspect`, the Admin
  page's inspect form) every inspect reads the registry, as creating an Orkflow does.
- **Run ids** are `ork-` and a UUIDv7 in lowercase base32, 30 characters, and
  single-use for all time; a run started by an earlier release keeps
  `ork-{timestamp}-{sequence}`, and both stay valid. Nothing may parse one or
  derive meaning from its parts (`internal/engine/runid.go`): within one replica
  a later id sorts after an earlier one, and across two that means nothing.
  Order runs by `created_at`. The one exception, and it is not a licence to
  order by id: two places compare ids to break a tie between runs with the
  *same* timestamp, so a page is stable rather than arbitrary — `mergeRuns`
  (`internal/api/service.go`, guarded by
  `TestListRuns_OrdersRunsByTimeNotByRunID`) and `RunQuery.finish`, which
  orders one page of the archive index (`pkg/artifacts/archive.go`). Those two
  are the whole list.
- **Listing runs is a question the cluster answers.** At boot each engine
  registers four search attributes on its Temporal namespace —
  `OrkflowNamespace` (`task.EnsureRunSearchAttributes`),
  `OrkflowOriginKind`/`OrkflowOriginId`
  (`task.EnsureRunOriginSearchAttributes`) and `OrkflowOriginRootRunId`
  (`task.EnsureRunRootSearchAttribute`) — and stamps every run it starts
  with the ones that apply, so `run list -n payments`, `run list --filter
  origin.kind=TRIGGER --filter origin.id=trg-…`, `run list --filter
  origin.root_run_id=ork-…` and an `origin.kind` filter on the runs page are
  visibility predicates over live runs rather than filters over a page of
  everyone's. That is a tenancy
  property: filtered afterwards, how far a list could see would be set by how
  busy the OTHER namespaces were. Finished runs come from the archive, whose
  run, origin and root index keys begin with the namespace (`docs/STORAGE.md`).
  All four are keywords. A Temporal 1.32 dev server (the development stack's)
  takes 10 custom keyword attributes per namespace; measure your own visibility
  store if the namespace holds attributes of your own.

  The boot line reports each answer separately
  (`INFO connected to temporal … run_namespace_index=true run_origin_index=true
  run_root_index=true`), so a cluster that refuses one keeps the others. Before
  registering, the engine waits up to a minute for the cluster to see the
  Temporal namespace it just ensured (`task.WaitNamespaceVisible`): a namespace
  created a moment ago answers "not found" until the cluster's namespace cache
  catches up. The registrations keep asking on "not found" under the same
  minute, since with several frontends one may not see the namespace yet when
  another already does, and each call is bounded within it; a namespace still
  not found then fails the boot, naming it. Whether an
  attribute is usable is read back from the cluster, not from this engine's
  own registration call, so every replica gives the same answer. A plan
  pins the root answer at submit, so one run tree has one answer. A cluster that refuses
  the registration — the operator API behind an authorizer, a visibility
  store without custom attributes — still boots and warns once
  (`WARN visibility does not index …`, naming the attribute and the
  consequence); the reader then walks the whole index instead of trusting a
  predicate the cluster cannot apply, and a list that cannot finish that walk
  fails rather than returning a short one. A run started without
  `OrkflowNamespace` (before the attribute existed, or by an engine that booted
  unindexed) carries its namespace only in its memo, so a scoped read asks for
  `(OrkflowNamespace = "x" OR OrkflowNamespace IS NULL)` — `IS NULL` is
  accepted by every visibility store of a supported Temporal server — and keeps
  those runs by their memo: a namespace's list, stats and `max_running` count
  still see them, and another namespace's such runs are read and dropped, under
  the bound of a walk that filters.

  That has a transitional cost while such runs are within the cluster's
  retention. They exist only from before v0.3.0, when `OrkflowNamespace` was
  introduced, or from an engine that booted unindexed (the boot race the wait
  above closes). Until they age out, a scoped run list reads other namespaces'
  unstamped runs and, past its page bound, fails explicitly rather than
  returning a short page; and the `max_running` check walks the live unstamped
  runs on each submit, after one count finds any. Both end as those runs age
  out under run retention. Nothing re-stamps them: a closed execution's search
  attributes cannot be changed from outside its workflow, and OrkStack keeps no
  database to do it with. A run
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
- **Idempotency keys: one key, one submission, one run id, at most one run.**
  - *A key is bound to what was submitted.* The claim holds the submission's
    fingerprint: the Orkflow, the version as the caller wrote it (none when
    it wrote none, so a retry across a promotion is the same submission), the
    run it asks to run again, and the input as sent (key order and spacing
    do not matter). The same key with any of those different is refused
    `already_exists`. `allow_empty_pool`, `no_cache` and `origin_reference`
    are not part of it.
  - *A retry reads the key's run id, and what it reads decides.* It looks in
    the cluster first, then the archive. The key's own run is answered; a
    run the cluster let go is answered from the archive; and only when both
    say there is no such run is the id started, by the retry, under its own
    origin. Anything that cannot be read is `unavailable`, never a start. So
    a replica lost between its claim and its start strands nothing: the next
    retry starts the id.
  - *A start whose answer was lost is never recorded as a failure.* The
    engine reads the id: a run that landed is answered, and one that did not
    is `unavailable` with nothing recorded, for the retry to read again. Only
    a start the cluster refused (invalid, not found, permission, failed
    precondition, unimplemented) is recorded as `RUN_START_FAILED`; if that
    record cannot be written, the submission says so (`unavailable`) instead
    of reporting a failure no replica can find.
  - *An id held by something else is replaced once.* When the key's id was
    started by something that is not the key's own run, the key moves to a
    fresh id once; a second refused id is `unavailable`, and nothing is
    recorded under an id the submission does not own.
  - *The engine finishes on its own clock.* Once it has a candidate, it
    spends up to 30 s on the claim and the start whatever the caller's
    deadline; a caller that left learns the outcome by submitting again
    under its key. A callback token is disclosed only to the submission that
    started the run.
  - *A key claimed by v0.3.0* (or answered by a v0.3.0 engine during a
    rollout or a rollback) carries no fingerprint and is answered as v0.3.0
    answered it: its run if found, its pending run if not, never started.
    Each pending answer logs a warning and counts
    `ork.idempotency.legacy_pending`. All such claims end one
    `engine.idempotency_ttl` after the last engine is upgraded.
  - *The cluster keeps a finished run at least a key's life.* The retention
    OrkStack sets on the cluster is the larger of the maximum run retention
    and `engine.idempotency_ttl` plus five minutes, plus the enforcement
    margin; no namespace's run retention changes, and runs are retired when
    they were. Set `engine.idempotency_ttl` the same on every engine. After
    lowering it, or the maximum run retention, a key claimed before is not
    started again once its claim is older than the retention the cluster
    reports: such a submission is `unavailable` and asks for a new key.
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
    test:
      - CMD
      - "/usr/local/bin/ork"   # orker: /usr/local/bin/orker
      - "--healthcheck"
    interval: 10s
    timeout: 5s
    start_period: 30s
    retries: 3
  ```

  `start_period` is the boot: a check that fails inside it does not count
  against `retries`. The check itself gives up after 3 seconds.
- **How long an Orker's boot can take.** An Orker is not ready until its boot
  checks pass, and one of them runs a container, built as a task's is, to
  prove the work root can be bound and that the runtime gives a task its
  posture, then cancels a second one as a task is cancelled. On a healthy
  daemon that takes seconds. On a daemon that is overloaded or wedged it can
  take up to seven times `orker.docker.bind_probe_timeout` plus 230 seconds
  for removing what it gave up on and the first container, and for that
  cancellation (17 minutes 50 seconds at the default), and longer by
  `orker.docker.pull_timeout` when the probe image is not local, before the
  Orker refuses to start with
  `BIND_PROBE_TIMED_OUT` ([REASONS.md](REASONS.md)). A supervisor that gives
  up sooner (a Compose check whose `start_period` plus `interval` times
  `retries` is shorter than that, a start-up timeout in an init system)
  reports the Orker unhealthy or failed before it can say why. Size the
  allowance to that bound, or lower the setting to the longest start you would
  wait for, and read the Orker's log for the reason. `make up` waits 60
  seconds for the Orker and then stops with a pointer to `.run/orker.log`,
  where the refusal appears when the probe ends.

### Running the published images

Each release publishes `ork`, `orker` and `orkctl` images to
`ghcr.io/orkstack`, for linux/amd64 and linux/arm64, beside the first-party
activity images ([UPGRADING.md](UPGRADING.md), "Release artifacts"): nine
images in all. There is no UI image: `ork` serves the web UI itself (§0, "The
web UI"). The quickstart's [`compose.yaml`](../compose.yaml) runs the same
three services on one machine, built from a clone from the same stages
([`deploy/source/Dockerfile`](../deploy/source/Dockerfile)), and is the worked
example of what follows. It is checked by `internal/dist`'s tests, and run end
to end on a release's own images, by digest, by
`scripts/quickstart-smoke.sh --release` before the release publishes
anything.

| Image | Runs | User | Settings it needs in a container |
|---|---|---|---|
| `ork` | `/usr/local/bin/ork`, which serves the web UI at `/` on its API port | non-root | the API on `ORK_HTTP_ADDR` (default `:8080`, every interface), Temporal (`ORK_TEMPORAL_ADDRESS`), the store (`ORK_ARTIFACT_BACKEND=s3`, `ORK_S3_*`), and `ORK_ADMIN_ADDR` for a health check. The UI it serves needs no setting |
| `orker` | `/usr/local/bin/orker`, with the `docker` CLI | root | the Docker socket (or Podman's Docker-compatible one, [Running on Podman](#running-on-podman)) and a work directory, below; Temporal and the store as for `ork` |
| `orkctl` | `/usr/local/bin/orkctl` | non-root | `ORK_SERVER`; its working directory is `/work` |

**The Orker in a container.** The Orker starts each task's container through
the host's Docker daemon, so its container needs that daemon's socket
(`/var/run/docker.sock`). On Kubernetes, an Orker pod has an engine of its own
instead ("Orkers on Kubernetes", below). Whoever can use the socket can start
a privileged container: the Orker container is as trusted as root on its host
([SECURITY_MODEL.md](SECURITY_MODEL.md), "The Orker's own access to the
host"). Give the socket to the Orker and to nothing else. Three more rules
follow from the daemon reading paths on the host:

- **The work directory is the same path inside and out.** The daemon
  bind-mounts each task's workspace from `ORK_ORKER_WORKDIR` as a host path,
  so mount the host directory at that same path in the container
  (`-v /var/lib/orkstack/work:/var/lib/orkstack/work`). On a machine whose
  daemon runs in a virtual machine (colima, for example), the directory
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
`ghcr.io/orkstack/activity-shell:0.3` for every 0.3 release; that tag moves to
each patch release of the line. To pin the exact release instead, set
`ORK_ACTIVITY_CATALOG` to the images by tag and digest. (The quickstart's
catalog names the activities it built and pushed to its own registry, by tag;
each Orkflow created pins the digest the tag names then.)

### Running on Podman

Podman is a second engine of the one runtime the Orker ships
([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md), "§9"). Two shapes are
supported, each with the Orker as root inside the runtime:

- **The podman CLI**: `orker.docker.cli: podman` (`ORK_DOCKER_CLI=podman`),
  which also makes `podman` on `PATH` the default `orker.docker.bin`.
- **The release image on a Podman socket**: the `orker` image's docker CLI
  against Podman's Docker-compatible socket (`podman system service`, or
  `podman.socket`), mounted where the Docker socket would be. Nothing is set:
  the CLI is `docker`, and the engine is asked.

Either way the Orker asks the engine at boot, logs it (`container runtime
engine=podman cli=… rootless=… seccomp=engine-default`), and runs its boot
probe, which refuses to start with `RUNTIME_POSTURE_REFUSED` on a host that
does not give a task's container its posture, or whose cancelled workload
never hears SIGTERM ([Rootful Podman under AppArmor](#rootful-podman-under-apparmor)).
The probe is what makes the host requirements below checked rather than
assumed.

What differs from Docker, by design:

- **Seccomp.** The Podman engine has no name for its default profile and reads
  `seccomp=builtin` as a file path, so the Orker spells nothing and the engine
  applies its own default; the probe checks that a filter is active
  (`Seccomp: 2`), not that it is Docker's profile. DescribeOrker's security
  profile says `seccomp=engine-default`.
- **Proxy variables.** The podman CLI forwards its own process's
  `HTTP_PROXY`, `https_proxy` and `NO_PROXY`, credentials included, into every
  container unless told not to; the Orker passes `--http-proxy=false`, and the
  probe refuses any `*_proxy` variable it finds.
- **`/run` and `/var/tmp`.** Podman's `--read-only` leaves both writable, so
  each gets an empty read-only tmpfs. On an image whose `/var/tmp` is a
  symlink to `/tmp`, that turns `/tmp` read-only on Podman.
- **The escape hatch** (`run_as_orker_user`, `orker.resources.force_user`).
  On a rootless podman CLI it is rendered with `--userns=keep-id`, which lands
  as the Orker. The docker CLI cannot say that, so against a rootless Podman
  socket, run by a non-root Orker, the hatch lands as another host user, and
  the probe makes such tasks fail `RUN_AS_ORKER_UNAVAILABLE`.
- **The embedded image layout** (`orker.image_layout`) does not work on
  Podman: Podman does not find what it loaded by the pinned digest, so a task
  whose pull fails there fails `IMAGE_PULL_FAILED` rather than falling back.

Host requirements, each of which the probe or the first task finds when
missing:

- **cgroup v2 with the `cpu`, `memory` and `pids` controllers delegated** to
  the user a rootless Podman runs as, the systemd cgroup manager
  (`cgroup_manager = "systemd"` in `containers.conf`), and an OCI runtime
  built with systemd support (runc; a static crun without it failed). A limit
  that is not enforced refuses boot, naming it.
- **An init binary** for `--init`: `catatonit` where `containers.conf`'s
  `init_path` says.
- **A subordinate uid and gid range of at least 65536** for the Orker's user
  (`/etc/subuid`, `/etc/subgid`), for rootless Podman.
- **Qualified image references.** Podman's enforcing short-name mode refuses
  `alpine:3.24` where more than one search registry is configured; the boot
  probe's image is fully qualified and pinned, and an activity's reference
  is resolved before it reaches the runtime.
- **`insecure = true` for a plain-HTTP registry** in `registries.conf`, the
  local development registry included (`[[registry]] location =
  "localhost:5001"`). Docker trusts a loopback registry by itself; Podman does
  not, and a pull from it fails `IMAGE_PULL_FAILED`.
- **No SELinux or AppArmor relabelling yet.** Binds carry no relabel option,
  so on an enforcing SELinux host the boot probe's container cannot read its
  bind and boot refuses `WORKDIR_NOT_BINDABLE`; this has not been measured.
- **Rootful Podman under AppArmor 5 needs one rule added**, below.

#### Rootful Podman under AppArmor

On a host whose AppArmor stacks a container's processes with the OCI
runtime's own profile (AppArmor 5, Ubuntu 25.10 and later), rootful Podman
runs a container's init under its generated profile,
`containers-default-<version>`, and the workload the init starts under the
stacked label `containers-default-<version>//&crun`. That profile lets a
process signal only its own exact label, so the init cannot pass SIGTERM on
to the workload: a cancelled task is killed at the end of its grace without
its cleanup. The kernel log has one denial per cancelled container (measured
on ubuntu-26.04, Podman 5.7.0, `containers-default-0.66.0`):

```text
apparmor="DENIED" operation="signal" class="signal" profile="containers-default-0.66.0" comm="podman-init" requested_mask="send" denied_mask="send" signal=term peer="containers-default-0.66.0//&crun"
```

The boot probe finds it: its second container traps SIGTERM and is cancelled
as a task is, and an Orker whose workload never hears it refuses to start
with `RUNTIME_POSTURE_REFUSED`, quoting the stacked label. Rootless Podman
applies no AppArmor profile to its containers and is not affected.

Podman generates that profile and loads it into the kernel itself, so a file
under `/etc/apparmor.d/local/` does not reach it: those extend the `crun` and
`podman` profiles, and the denial is the container profile's. The rule
upstream proposes for the profile's template
([container-libs#919](https://github.com/podman-container-tools/container-libs/pull/919),
open as of 2026-10-29) can be added through the base abstraction, which the
generated profile includes:

1. Write the rule. Every profile that includes `abstractions/base` gets it,
   and in each it lets the profile signal only its own label stacked with
   another, so it allows nothing between two profiles:

   ```bash
   sudo mkdir -p /etc/apparmor.d/abstractions/base.d
   printf '%s\n' 'signal (send, receive) peer=@{profile_name}//&*,' | sudo tee /etc/apparmor.d/abstractions/base.d/orkstack-stacked-signals
   ```

2. Check that a profile that includes it still compiles. If it does not,
   remove the file: every profile that includes the base abstraction would
   fail to load, Podman's among them.

   ```bash
   printf 'include <tunables/global>\nprofile orkstack-check {\n  include <abstractions/base>\n}\n' | sudo apparmor_parser -QK
   ```

3. With no container of the engine running (stop the Orker first), unload the
   copy of the profile Podman loaded without the rule. Podman loads it again,
   with the rule, for its next container. A reboot does the same.

   ```bash
   sudo grep -o '^containers-default[^ ]*' /sys/kernel/security/apparmor/profiles |
     while read -r profile; do printf '%s' "$profile" | sudo tee /sys/kernel/security/apparmor/.remove >/dev/null; done
   ```

4. Start the Orker. Its boot probe checks that SIGTERM now reaches the
   workload.

Running Podman rootless avoids the denial altogether. Remove the file once a
containers-common whose profile carries the rule is installed. Neither
passing `--security-opt apparmor=unconfined` nor dropping `--init` is a fix:
the first removes the container's confinement, and without an init a shell
as PID 1 never hears SIGTERM at all. `podman.yml`'s rootful jobs show the
probe refusing the unfixed profile, then apply these steps and run every
suite on the fixed one.

`podman system reset` writes under `~/.config/containers` and
`~/.local/share/containers` even with storage configured elsewhere.
`podman.yml` runs the runtime-facing suites on Podman, rootless and rootful
([PROOFS.md](PROOFS.md), "§5").

### Orkers on Kubernetes

The Orker starts each task as a container on a Docker engine. On Kubernetes,
give each Orker pod an engine of its own, in a sidecar. Do not drive the
node's own container runtime: its containers would run behind the kubelet's
back, invisible to scheduling, accounting and eviction. Do not mount the
node's Docker socket either: that is root on the node, and outside the pod's
limits. [`deploy/kubernetes/`](../deploy/kubernetes/) holds the two supported
shapes as kustomize bases, and
[`scripts/kind-orker.sh`](../scripts/kind-orker.sh) proves both on a
throwaway kind cluster.

| Shape | Base | Privileged on the node | Needs |
|---|---|---|---|
| Engine in a user-namespaced pod | `orker-userns/` | nothing: the engine is root of the pod's user namespace, an unprivileged uid range on the node | user namespaces (`hostUsers: false`) and the `orkstack-engine` RuntimeClass, below |
| Engine in a privileged sidecar | `orker-privileged/` | the engine container. It is root on the node, and so is anything that escapes from a task's container to the engine | a namespace that admits privileged pods |

Prefer the first. Both need the `privileged` level of Pod Security Admission in
their namespace: the first gives the engine an unmasked `/proc` and no seccomp
or AppArmor confinement inside its user namespace, and the second is
privileged outright. The activity containers are the same in both: each task
runs under the Orker's own profile, with `no-new-privileges`, a seccomp filter,
no capabilities and a read-only root ([SECURITY_MODEL.md](SECURITY_MODEL.md),
"The mandatory profile").

Each base is used from an overlay of your own. The overlay names the release's
`orker` image by digest (the bases call it `orker`). It also creates the
settings the Orker reads in the same namespace: the `orkstack` ConfigMap
(`ORK_TEMPORAL_ADDRESS`, `ORK_ARTIFACT_BACKEND=s3`, `ORK_S3_ENDPOINT`,
`ORK_S3_BUCKET`, and the other settings of §1) and the `orkstack-s3` Secret
(`ORK_S3_ACCESS_KEY`, `ORK_S3_SECRET_KEY`, or no keys when a role signs: "The
object store's credentials"), and, when tasks read secrets, the
`orkstack-task-secrets` Secret (below). Each base's `kustomization.yaml`
shows the overlay.

**What a pod holds.**

- `engine` runs `docker:29.9.2-dind` by digest, the Docker release whose CLI the
  `orker` image carries. It is a native sidecar, an init container with
  `restartPolicy: Always`. It starts, and answers `docker info`, before the
  Orker boots. It stops only after the Orker has exited, so deleting a pod
  stops the Orker first. The Orker cancels each running task with that task's
  own grace (`SIGTERM`, then `SIGKILL`), and the engine is still there to
  deliver both signals.
- `orker` runs the release's `orker` image. Its `ORK_ORKER_ID` is
  `$(POD_NAME).$(POD_NAMESPACE)`, unique in the cluster and new for each pod.
- Three `emptyDir` volumes. `work` is the Orker's work root at
  `/var/lib/orkstack/work`. `engine-socket` holds the engine's API, a Unix
  socket and nothing else, and both containers' `DOCKER_HOST` points at it.
  `engine-data` is the engine's image cache and container layers at
  `/var/lib/docker`: a disposable cache, so a new pod pulls its images again.
- One Secret volume, `task-secrets`: the optional `orkstack-task-secrets`
  Secret, mounted read-only in the `orker` container alone, at
  `/etc/orkstack/task-secrets`. The pod starts without it.
- No service-account token. The pod sets
  `automountServiceAccountToken: false`: the Orker drives its engine over the
  shared socket, and neither container calls the Kubernetes API.

**A task's secrets come from `orkstack-task-secrets`.** The bases set the
Orker's secret floor ("Secrets on a shared pool", below) to
`ORK_ORKER_SECRET_ALLOW=file:/etc/orkstack/task-secrets/*`. A task may name a
key of that Secret as `file:/etc/orkstack/task-secrets/<key>`, and the Orker
resolves no other source. Every other source fails the task with
`SECRET_NOT_PERMITTED` before its image is pulled. That includes every
variable of the Orker's own environment, which holds its settings and the
`orkstack-s3` keys, and every other file of its container. Put the values
your tasks read in that Secret, one key each:

```bash
kubectl create secret generic orkstack-task-secrets --from-file=db-password=./db-password
```

Kubernetes updates a mounted Secret after its Secret changes, with a delay,
so a task that starts after the update reads the new value. A task names its
secret by that path:

```yaml
tasks:
  - id: migrate
    uses: ghcr.io/acme/migrate@sha256:<digest>
    secrets:
      - name: db-password
        source: file:/etc/orkstack/task-secrets/db-password
```

On a pool several namespaces share, every key of that Secret is open to each
of them unless a namespace's own list narrows it. Prefix the keys by
namespace, and give each namespace its own:
`orkctl namespace update payments --allow-secret 'file:/etc/orkstack/task-secrets/payments-*'`.

Name your other task-secret sources in the floor too: an overlay that admits
another directory or an `env:` prefix patches `ORK_ORKER_SECRET_ALLOW` on the
`orker` container, and mounts what that entry names. The base's own value
overrides one in the `orkstack` ConfigMap, because a container's `env` takes
precedence over its `envFrom`. The floor names the `file` scheme, so
`ORK_ORKER_SECRET_SCHEMES`, if you set it, must keep `file`, or the Orker
refuses to boot. A test holds both bases to this floor and checks that it
admits none of the Orker's own credentials
(`internal/dist/kubernetes_test.go`), and the cluster check below runs a task
through it on a live cluster.

**The work root is one path in both containers.** The engine bind-mounts each
task's workspace from the path the Orker gives it, so the `work` volume must be
mounted at the same path in `engine` and in `orker`. The Orker's boot check
catches a mismatch. With the engine's mount moved to `/srv/work`, the Orker
refuses to start and the pod never becomes ready:

```text
orker boot checks failed: WORKDIR_NOT_BINDABLE: path is not bind-mountable by the container runtime: probe bind of /var/lib/orkstack/work: docker exited 125: docker: Error response from daemon: invalid mount config for type "bind": bind source path does not exist: /var/lib/orkstack/work/ork-bindprobe-…
```

**Every task runs inside the pod's cgroup.** The engine's start-up puts the
daemon in `engine/` and each task's container under `tasks/` of its own
container's cgroup. The pod's limits therefore bound all its tasks together,
and the kubelet accounts for what they use. The start-up does not use the
image's own `dind` wrapper. containerd gives a privileged container no cgroup
namespace, so that wrapper rearranges the node's root cgroup, and its daemon
puts each task in `/docker/<id>` at the node's root, outside the pod's limits
and the kubelet's accounting. That was measured on containerd 2.3.4. The
start-up also enables the cgroup controllers below the engine before the
daemon starts. Without them, the daemon warns about swap limits on every
`docker create --memory`, and today's Orker reads that warning as part of the
container's id and fails the task (`InfrastructureError`).

**Size the engine to the Orker's capacity.** Every task's container is inside
the engine container, so its limits cover them all. The bases declare the
Orker's capacity ([Declaring an Orker's capacity](#declaring-an-orkers-capacity)),
`ORK_ORKER_CAPACITY_CPU=4` and `ORK_ORKER_CAPACITY_MEMORY=8Gi`, and give the
engine that plus the daemon's own share: 5 cpu and 9Gi, with its requests
equal to its limits. A task's reservation is its limit, and the Orker admits
no task that would take its reservations above its capacity, so its tasks
together stay within the engine's limits whatever `ORK_ORKER_MAX_CONCURRENT`
allows. Change the capacity and the engine's resources together; a test
refuses a base whose engine does not cover its capacity, or whose capacity
does not hold one task at the per-task cap (`internal/dist/kubernetes_test.go`).
The bases' cap is 1 cpu and 2Gi (`ORK_ORKER_MAX_CPU`, `ORK_ORKER_MAX_MEMORY`),
and `orkctl orker list` shows each Orker's `CAPACITY` and `RESERVED`. Within
that, the Orker enforces each task's own limit as it does on a host. A submit
whose task asks for more than the cap of every Orker of its pool is refused
with `RESOURCES_UNSATISFIABLE`, before a run exists. A task that uses more
memory than it asked for is killed by the kernel in its own cgroup and fails
`CONTAINER_EXIT_NONZERO`, naming its limit; the rest of the pod carries on.
The `emptyDir` volumes have `sizeLimit`s and the engine an `ephemeral-storage`
limit. A pod that exceeds either is evicted, and its running tasks end as any
stopped Orker's do.

**The `orkstack-engine` RuntimeClass.** A pod with its own user namespace
gets a cgroup that belongs to a uid outside that namespace, so its engine
cannot create the cgroups that enforce each task's limits, and does not start.
containerd's `cgroup_writable` option gives the pod's root its own cgroup.
Declare a runtime handler with it on each node that runs these Orkers. In
containerd 2's own configuration (version 3):

```toml
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.orkstack-engine]
  runtime_type = 'io.containerd.runc.v2'
  cgroup_writable = true
  [plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.orkstack-engine.options]
    SystemdCgroup = true
```

In a version 2 file, which kind writes, the table is
`plugins."io.containerd.grpc.v1.cri".containerd.runtimes.orkstack-engine`
([`deploy/kubernetes/kind/kind.yaml`](../deploy/kubernetes/kind/kind.yaml)).
Restart containerd, then apply the base: it declares the RuntimeClass
`orkstack-engine` with that handler. The proof ran on Kubernetes 1.37,
containerd 2.3.4, cgroup v2 and Linux 7.2. The node there had no AppArmor; on
a node that enforces it, the default profile denies the engine's mounts, which
is why the base sets the engine's AppArmor profile to `Unconfined`.

**Networks.** `network: none` gives a task only its loopback interface.
`network: default` is the engine's bridge, which leaves through the pod's own
network namespace, so a task reaches what the pod reaches, cluster Services
included. Both were measured in both shapes. The bridge's default subnet is
`172.17.0.0/16`. Where the cluster routes that range, add `--bip` with a free
range to the engine's `dockerd` command.

**Registries.** The engine pulls every task's image itself. For a registry
with a private certificate authority, mount the authority's certificate in the
engine at `/etc/docker/certs.d/<host>:<port>/ca.crt`, as the proof does.

**Before deleting a pod**, cordon and drain its Orker, as for any Orker ("Removing an
Orker safely" below): `SIGTERM` cancels in-flight work. A task of a deleted pod
ends `ORKER_HEARTBEAT_TIMEOUT` unless its `retry:` runs it again on another
Orker.

**Not supported, and why.**

- Docker's rootless engine (`docker:dind-rootless`). It has no systemd, and a
  rootless daemon needs systemd for cgroups. It accepts `--memory`, `--cpus` and
  `--pids-limit` and applies none of them: under `--memory 64m` a container
  reads `memory.max` as `max`. Today's Orker cannot tell, so every task would
  run without its limits.
- The node's own container runtime, or its Docker socket, for the reasons
  above.
- Sysbox, which installs onto the nodes, was not measured: kind's nodes are
  containers. Podman in the Orker container is not on `main` yet.

**Check your cluster before you trust it.** Run
[`scripts/k8s-orker-check.sh`](../scripts/k8s-orker-check.sh) against your
own cluster first, from a clone of the release you deploy:

```bash
scripts/k8s-orker-check.sh --context prod-eu \
  --server https://orkstack.internal.example \
  --orker-image ghcr.io/orkstack/orker:X.Y.Z@sha256:<the release's orker digest> \
  --settings-from orkstack
```

It needs `kubectl`, `jq` and Go (for `orkctl`), an `ork` engine at `--server`
whose catalog resolves the `shell` and `file` activities, and the `orkstack`
ConfigMap and `orkstack-s3` Secret your Orkers read, in the namespace
`--settings-from` names. It refuses, with exit `77` and the reason, a missing
prerequisite:

- Kubernetes before 1.33, or no `orkstack-engine` RuntimeClass, for the
  user-namespaced shape (`--shapes privileged` checks the fallback alone);
- the settings, or an engine that does not answer;
- Temporal, the object store or `--registry-host`, unreachable from a pod;
- a user-namespaced pod in that RuntimeClass that cannot make cgroups of its
  own, which means its handler lacks `cgroup_writable`;
- Pod Security, or any admission policy, refusing either shape. The check
  asks the API server with a dry-run of the shape's pod.

Then it deploys each base as it ships into a namespace of its own
(`orkstack-check` unless `--namespace` names another). It changes only what
[`deploy/kubernetes/check/common`](../deploy/kubernetes/check/common/)
changes: one replica in the pool `k8s-check`, the per-task cap its limits run
measures, and a smaller pod with a capacity to match. For each shape it
checks:

- a run that passes artifacts between tasks;
- tasks with no network, and one on the default network, which must reach
  the Kubernetes API's own Service;
- each task's limits and security profile;
- that each task's cgroup is under the engine container's, and, for the
  privileged shape, that the engine's is under its pod's;
- that the engine's root is a user-namespace range, or the node's root for
  the fallback;
- that the Orker reports the capacity its pod declares;
- that a task reads a key of `orkstack-task-secrets`, which the bases' floor
  lists, and that the floor refuses what it does not list: the Orker's own
  `ORK_S3_SECRET_KEY`, and `POD_NAME`, a variable of the Orker's
  environment. The key's value is the check's own, random, and appears in no
  log or record;
- a cancel; a task above the per-task cap, refused at submit by the cap the
  Orker reported, with no run left behind; a task at the cap, with the cap as
  its limit; an out-of-memory task; and a pod deletion.

Then it checks that the Orker refuses a mismatched work root, and that the
user-namespaced engine does not start without its RuntimeClass. It deletes its
namespace at the end (`KEEP=1` leaves it), the `orkstack-task-secrets` Secret
it makes there with it, and creates nothing cluster-wide: the RuntimeClass
must exist already. In OrkStack it leaves five Orkflows (`k8s-orker-proof`,
`k8s-orker-secrets`, `k8s-orker-cancel`, `k8s-orker-limits`,
`k8s-orker-too-big`) in
`--ork-namespace` (`default` unless named) and their runs, kept by that
namespace's retention. Its Orkers serve only the pool `k8s-check`, which that
namespace must allow, so they take no other run's tasks. Exit `0` means every
check held. Exit `1` names the check that failed, and the namespace's state
then is in its evidence (`.data/k8s-orker-check/<stamp>/`, or `--out`).

**On kind, in CI.** [`scripts/kind-orker.sh`](../scripts/kind-orker.sh) makes
a throwaway kind cluster that meets those prerequisites
([`deploy/kubernetes/kind/`](../deploy/kubernetes/kind/)): the containerd
handler on its nodes, the RuntimeClass, Temporal, SeaweedFS, a registry
served over TLS, and one `ork`, with `ork`, `orker` and two activities built
from the tree. It runs the check against that cluster with both shapes, then
measures that the rootless engine drops a container's limits, and deletes the
cluster (`KEEP=1` leaves it). It needs Docker, Go, curl, openssl and jq, and
installs nothing system-wide. kind and kubectl are their release binaries,
checked against pinned SHA-256s, and crane comes from an image pinned by
digest. Its log and evidence go under `.data/kind-orker/<stamp>/`, with the
cluster's state when a step fails. CI runs it as `kind-orker.yml` on a change
to what the bases run, every day, and by dispatch
([PROOFS.md](PROOFS.md) §5). It is not a required check and is part of no
proof tier.

### The admin surface: what a deployment is made of, and who is serving it

Every operational read is available from the CLI, over Connect and in the
UI's Admin pages. All but two are answered from the cluster or the bucket, so
**any replica answers them and they all answer the same** (`AGENTS.md` §7).
System information and the activity catalog deliberately describe the replica
that answered.

| read | CLI | where the answer comes from |
|---|---|---|
| system information | `orkctl admin info` | **this replica's own build and configuration** |
| activity catalog | `orkctl activity list` | **this replica's own configuration** (`ORK_ACTIVITY_CATALOG`), resolved to pinned digests when the Orkflow is created |
| maximum run retention | `orkctl admin retention get \| set` | `metadata/settings/retention-policy.json`; writable |
| image policy | `orkctl admin image-policy get \| set` | `metadata/settings/image-policy.json`; writable |
| namespaces | `orkctl namespace list [-F F…] \| get \| create \| update \| delete` | the registry in the bucket, including each namespace's retention, admission, budget and policies; Admin → Namespaces pages and filters it, and a page filtered by name alone reads only the documents on screen |
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
  remote: could not check pool "remote" for live Orkers: asking the cluster failed (context deadline exceeded); retry the request
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
  (`orkctl activity list`), not something an Orker reports. The count and
  `LAST SEEN` come from the pool's channel and from the Orker inventory: the
  larger of the channel's live pollers and the pool's uncordoned Orkers that
  answer on their own channels, so a pool whose every Orker is busy, and so
  polls nothing on its channel, still reports them. An Orker waiting out a store
  outage alone counts as uncordoned here ("When object storage goes away").
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
stored orkflow's version catalog: a create or delete through a replica is seen
by that replica at once, and a pool named only by an orkflow created through
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
orkctl orker list -F pool=build
orkctl orker cordon orker-7f3a --reason "CVE-2026-1234 runtime patch"
orkctl orker drain  orker-7f3a --timeout 4h -w
```

```text
$ orkctl orker drain orker-7f3a --timeout 4h -w
cordoned orker-7f3a: "orkctl orker drain"
waiting for 2 tasks and 1 pin to finish…
  09:15:40  2 tasks, 1 pin     pin "build" of run ork-01m6qt8v…  expires in 2h11m
  10:02:18  0 tasks, 1 pin     pin "build" of run ork-01m6qt8v…  expires in 1h24m
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
- **The pool probe.** A cordoned Orker stops polling its pool's channel but
  still answers on its own control channel, where submit's probes and the
  inventory read it as an Orker that takes no new task. So a pool whose every
  Orker is cordoned reports its poller-derived count for the freshness window
  and zero after it, and a cordoned Orker never keeps a pool live or speaks for
  what it can run.

An Orker also cordons itself, with these same effects on tasks and pins, while
object storage does not answer it. That cordon does keep its pool live ("When
object storage goes away", below).

`SIGTERM` (or `SIGINT`) stops the Orker carefully. It cordons itself at once
(`orkctl orker list` shows the reason `the orker is stopping`), so it asks for
no new task and opens no new pin, but it does not wait for in-flight work to
finish: an operator who wants that cordons and drains first. What a stop
does, before the Orker exits:

- an attempt with a result of its own finishes and reports it as usual: one
  adopting a commit or writing a memo hit's commit, and one whose container
  ended by itself, whatever its exit;
- every other unpinned attempt is handed back. Its container gets the
  cancellation sequence — `SIGTERM`, then `SIGKILL` once the task's
  `cancellation_grace_period_seconds` (30 by default) has passed — while the
  attempt keeps heartbeating, and nothing it left is committed. The Orker then
  reports the attempt `ORKER_STOPPED` itself, so the task runs again under its
  `retry:` policy at once, not after a heartbeat timeout; a task with no
  `retry:` fails `ORKER_STOPPED`. Its record says `FAILED` with
  `ORKER_STOPPED` (`orkctl run attempts`), and its log keeps every line the
  workload printed, the ones after `SIGTERM` included (`orkctl run logs`). A
  report the cluster never takes within 10 minutes is recorded
  `REPORT_UNDELIVERED`, and the task then ends `ORKER_HEARTBEAT_TIMEOUT`, or
  runs again, as for an Orker that died;
- a task that reaches the Orker after the signal starts nothing, leaves no
  record, and is handed back the same way;
- a pin it hosts fails `PIN_HOST_LOST`: a member still running is ended when
  the Orker stops its workers, and one not yet started is refused.

On a versioned fleet (`ORK_BUILD_ID` set) a handed-back task runs again only
on an Orker of its run's own release that serves its pool, and waits, still
`PENDING`, until one takes work: the cluster sends a run's task to the run's
own release on every pool that release has served, whether or not any of its
Orkers polls now (§3, "Rolling a release forward"). Nothing is lost while it
waits.

So a stop takes as long as the longest grace among its running tasks, plus a
few seconds to report and record them. Give the process manager at least
that long before it kills the Orker — Kubernetes'
`terminationGracePeriodSeconds`, systemd's `TimeoutStopSec`, `docker stop
--time` (the quickstart's `compose.yaml` sets `stop_grace_period: 45s`).

Every container-runtime CLI the Orker starts runs in a process group of its
own, so a signal sent to the Orker's whole process group — a terminal's
Ctrl-C, `make down`, a process manager that signals the group — reaches the
Orker alone and never a task's container. Under rootless Podman the
container's monitor (`conmon`) may still run inside the Orker's own service
cgroup, where a process manager that signals the whole cgroup (systemd's
default `KillMode=control-group`) signals the container directly; run such an
Orker with `KillMode=mixed`. A second `SIGTERM` or `SIGINT` changes nothing.
A third exits at once, killing every runtime CLI the Orker started, and leaves
what a crash leaves: the attempts `UNREPORTED`, and a container still inside
its grace running until this Orker's next boot reaps it.

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

### What removes a task's container

While an Orker lives, nothing stays. An attempt removes its container with
`rm --force --volumes` however it ended, and takes the container's anonymous
volumes with it. What can stay comes from a removal that failed and from an
Orker that died:

| What happened | What stays | For how long | What removes it |
|---|---|---|---|
| The task succeeded, exited non-zero, wrote malformed output, timed out, was cancelled, or ignored SIGTERM; its start was refused; it was pinned (the pin's shared directory stays, by design) | nothing | | the attempt |
| A commit or a memo answered the attempt | nothing: no container started | | |
| The daemon refused the removal | the stopped container | until the daemon allows it | this Orker, every 30 seconds |
| The daemon restarted under the task | the container, stopped; under `live-restore` still **running**. The attempt fails `CONTAINER_STOPPED_BY_RUNTIME` when the daemon stopped the container, and `RUNTIME_REFUSED` when it went away first, by a crash or under `live-restore`; both are retried under the task's `retry:`. On Podman, stopping the API service stops no container and `RUNTIME_REFUSED` is the only outcome | up to 30 seconds after the daemon answers | this Orker, every 30 seconds |
| The CLI gave up on a create the daemon then finished | a container in the `created` state, which holds no cpu or memory | up to 30 seconds after it appears, if that is within 10 minutes of the create; after that, until the next boot | this Orker, every 30 seconds for 10 minutes; then its next boot |
| The Orker was killed and restarted under the same `ORK_ORKER_ID` | the container, still **running** past its task's timeout, which is the Orker's own clock | until that boot | the replacement's boot, whatever its work root |
| The Orker was killed and came back under another identity, or never came back | the container, **running** until its workload exits, then stopped | until an operator removes it | **you** |

A removal this Orker owes is process-local: if it stops first, its next boot
under the same identity reaps by label instead. Each retry that fails is logged
with the container's id (`a container could not be removed`).

An Orker's boot removes only containers labelled with its own identity, because
nothing proves another identity is dead: an Orker on another host or in another
container may share this daemon, and its identity lock is not visible from
here. So keep the identity stable. The derived default is
`<host>-<hash of the work root>`, which changes with the work root and, for an
Orker in a container, whenever a recreated container gets a new host name. Set
`ORK_ORKER_ID` for an Orker that is replaced rather than restarted.

A boot does say what else it found. It lists the daemon's containers of other
identities, and when there are any it logs one warning (`containers of other orker
identities are on this container runtime`) with how many, how many are running,
the identities and up to 20 containers. It never stops or removes one, and a
listing that fails is logged and does not stop the boot. A live Orker sharing the
daemon appears in the warning at every boot, so check it against `orkctl orker
list` rather than acting on it alone.

**What an operator cleans up.** When that warning names an identity `orkctl orker
list` does not, or a host's Orker will not return or returned under another
identity, list what its daemon holds:

```bash
docker ps --all --filter label=ork.orker --format '{{.ID}} {{.Status}} {{.Labels}}'
```

Then remove one dead identity's containers. Never run this for an identity of a
live Orker, whose running tasks it would kill:

```bash
docker ps --all --quiet --filter label=ork.orker=<id> | xargs --no-run-if-empty docker rm --force --volumes
```

On the podman CLI the commands are the same with `podman`. A container also
carries `ork.run`, `ork.task` and `ork.attempt`, to say what it was running.

**What a killed attempt leaves on disk.** A process that is killed takes no exit
path, so its attempt's directory, `ork-<run>-<task>-<attempt>-<owner>-*` under the
work root, stays with the secret values staged for it in plain files. `<owner>`
is sixteen hex digits of the SHA-256 of the identity that made the directory, and
an Orker's boot removes the directories that name its own identity, under its own
work root, once it holds that identity's lock and has reaped its containers. It
logs each (`attempt directory reaped`), with no operator step. Nothing else
removes them:

| Under the work root | What removes it |
|---|---|
| A directory naming this Orker's identity, found by its boot | the boot, secrets first; a removal that fails is logged, and the next boot tries again |
| A file in one that a workload left and the Orker cannot remove, because the workload ran as another user and made its directory writable only by itself | **you**. The boot removes everything else, secrets included, and logs what stayed. The Orker logged it when the attempt ended (`attempt cleanup: a directory could not be removed`), naming the directory that refused it, its mode and owner (`refused_by`). First-party images leave none ([OCI_ACTIVITY_CONTRACT.md](./OCI_ACTIVITY_CONTRACT.md), §10.1 rule 11) |
| A directory naming another identity, which may be a live Orker's on this work root | never the boot. It logs them (`attempt directories under this work root are not this identity's`); once that Orker is gone, **you** |
| A directory named `ork-<run>-<task>-<attempt>-<digits>`, with no owner, from a release before 0.3.0 | **you**: nothing says whose it is |
| A pin directory, `ork-pin-*`, whose `lease.json` names this Orker's identity, found by its boot | the boot |
| A pin directory whose lease names another identity, or nobody (a release before 0.3.0, or a torn lease) | never the boot. Any Orker on this work root removes it when its lease expires, the pin's lifetime plus 6 minutes after it opened, or for a torn lease 6 minutes after it last changed |
| `ork-spool/<owner>/` and `ork-stage/<owner>/`, this Orker's identity's upload spool and input staging | the boot |
| `ork-spool/<owner>/` and `ork-stage/<owner>/` of another identity | never the boot. **You**, once that Orker is gone for good: the owner is computed as below, and these two directories are all there is to remove |
| A file directly under `ork-spool/` or `ork-stage/`, from a release before 0.3.0 | **you**, once no Orker of that release uses this work root. The boot logs them, because nothing says whose they are |
| Anything under another work root, when an Orker came back under the same id on a new one | **you**: a boot looks under its own work root only |

So when an Orker will not return, or returned under another identity or work
root, remove the directories naming its identity under its old work root. Check
first with `orkctl orker list` that no live Orker holds that identity. It takes
root, for the sealed input trees and for what a workload left as root:

```bash
owner="$(printf '%s' 'ORKER_ID' | sha256sum | cut -c1-16)"
```

```bash
sudo find "/path/to/work-root" -mindepth 1 -maxdepth 1 -type d -name "ork-*-$owner-*" -exec rm -rf -- {} +
```

A directory from before 0.3.0 names no identity, so nothing but its age and the
work root says whose it is: remove it by name, once no Orker of that release runs
on that work root.

**An identity must have one holder.** A boot removes containers and attempt
directories by its identity, and what says no live Orker answers to it is a lock
in the temp directory. Two processes that do not share a temp directory, such as
Orkers in containers of their own or units with `PrivateTmp=true`, do not see each
other's lock, so given one `ORK_ORKER_ID` each boot would remove the other's running
work, secrets included. The derived default cannot collide this way; an explicit
id is yours to keep unique across everything that shares a daemon or a work root.

### When object storage goes away

An Orker that cannot reach object storage cannot finish a task: an attempt
handed to it fails `InfrastructureError` and uses one of the task's retries, so
a long outage would fail runs that only had to wait. So every Orker checks the
store on a cadence, and while the checks keep failing it **cordons itself**,
with the same effects as an operator's cordon (above). Tasks stay queued for
the pool with no attempt started, and everything in flight is left alone. Good
checks end it, with no operator step.

With the defaults (`orker.store_check`, §1), an Orker cordons itself 30 to 45
seconds into an outage, or up to about 50 when a stalled store makes each check
run out its timeout, and a blip shorter than 30 seconds cordons nothing. It
resumes 15 to 30 seconds after the store answers again.

**Two independent causes.** An operator's cordon and a store outage are
separate, and the Orker is cordoned while either holds:

- Recovery never clears an operator's cordon.
- `orkctl orker uncordon` clears only the operator's. An Orker whose store is
  still failing stays cordoned, and the command says so.
- `orkctl orker drain` sets an operator's cordon on an Orker only an outage
  holds, so a recovery in the middle of a drain cannot put it back to work. An
  operator's uncordon during a drain ends the drain with exit `1`, even while
  the outage still holds.

Neither is stored: a restart clears both. While the store is out, though, a
restarted Orker refuses to boot, because its boot probe needs the store
(`COMMIT_STORE_UNAVAILABLE`), and whatever restarts it keeps trying.

**Reading it.** The inventory carries the cause: `cordoned` is true for either,
`cordon_reason` and `cordoned_at` are the operator's alone, and `store_outage`
(`reason`, `since`) is set while an outage holds. An Orker of a release without
this check sends no `store_outage`, and reads as it always has.

```text
$ orkctl orker list
ORKER       POOL   BUILD  STATE                                  TASKS  PINS  CAPACITY  RESERVED  LAST SEEN
orker-7f3a  build  -      cordoned (object storage unreachable)  0      -     -         -         2026-11-04T09:14:02Z

1 orker cordoned itself because object storage does not answer; tasks wait, and it resumes by itself once it does:
  orker-7f3a: object storage unreachable: the last check failed; this Orker's log has the error, since 2026-11-04T09:13:10Z
```

Admin → Pools stamps the row `Object Storage Unreachable`, apart from an
operator's `Cordoned`, and offers no Uncordon for it, because none would help.
The reason is plain words on purpose: the store's own error names endpoints
and keys, so it goes to the Orker's log, never to the inventory. The log lines
are `object storage check failed`, `orker cordoned itself: object storage
unreachable` on entering and `object storage answers again` on leaving; they are
always on, whatever the telemetry settings, and the metrics below are not. Nothing audits a store
outage's cordon, because no one asked for it. With metrics on:

| Metric | Labels | What it reads |
|---|---|---|
| `ork.orker.store_outage.active` | `ork.orker_id` | 1 while this Orker holds its cordon for a store outage, else 0 |
| `ork.orker.store_outage.transitions` | `ork.orker_id`, `ork.transition` | each entry (`entered`) and exit (`left`) |

Telemetry can never fail a task or change what the check does.

**What a check is.** One conditional write of the fixed object the boot probe
already wrote, which a live store refuses, and one read of it, with the Orker's
own store credentials and its own client. So it exercises writing and reading a
completion record, and adds no object: if that one object has gone, the write
puts it back, as the next boot would. A failure that spares that one key, such
as credentials scoped to some prefixes, goes unseen, and attempts fail as they
always did. Anything that keeps the
Orker from getting an answer fails it, revoked credentials included. Each check
is bounded by `orker.store_check.timeout`, and one that does not answer in time
counts as failed even when it cannot be interrupted, as on a hung mount, and no
second check starts behind it.

**The local backend** (`ORK_ARTIFACT_BACKEND=local`) runs the same check on its
directory: a temporary file written there, a refused link, and a read. It fires
when that directory cannot be written or read, such as a full or read-only
disk, or a mount gone away. A check that hangs on a hung mount counts as failed
as above.

**What it does not cover:**

- **Runs that finished before the outage.** No Orker work is left in them to
  pause, and what the engine still has to write for them, such as an archive,
  is the engine's to retry.
- **Runs that close without an Orker,** such as an operator's cancel or an
  await whose deadline passes. The engine closes those by itself, and an
  Orker's cordon does not hold them back.
- **A task that starts a pin during the outage.** It still fails
  `PIN_UNAVAILABLE` once every pin host of its pool has refused for about five
  minutes, as for any cordon ("Removing an Orker safely", above).
- **The window before the cordon.** An attempt that reaches the store during
  those seconds fails as it always did, and uses an attempt. So can
  the at most two tasks a cordon can still be handed (above), per Orker.
- **Work already in flight.** A running container is left to finish, and a
  member of an open pin still runs. If either still cannot write its outputs
  when it ends, that attempt fails `InfrastructureError` and uses an attempt;
  its retry then waits with everything else.

**Submitting while every Orker of a pool holds it.** A pool whose Orkers are
waiting out an outage has paused, and is not absent. Such an Orker still keeps
its pool live, so `SubmitRun` accepts the run, the run waits, and it starts once
the store answers. `orkctl pool list` keeps counting it, and submit's capability
probes still ask it. A submit needs the store itself, so during a whole outage it
fails on the engine's own reads; this acceptance shows when only the Orkers have
lost the store, and in the seconds after a recovery while they resume.

An operator's cordon says the opposite, that the Orker is leaving, so an Orker
that holds one does not keep its pool live, whatever else it holds. A pool that
has only such Orkers refuses a submit `has no live Orkers`, as ever.

While a task waits on such a pool, `orkctl run inspect` prints `waiting for
object storage` with the pool and since when, and the run page says the same in
place of `Waiting for capacity`. That wait is said only when no Orker of the
pool takes new tasks, at least one is cordoned for the outage alone, and every
live Orker answered when asked; otherwise it reads as any other wait for an
Orker. So a pool whose only Orkers an operator has cordoned too does not say it:
those would not resume when the store answers. A pinned task never reads it: it
is covered by the pin's own wait, above. A waiting run has no deadline of its
own, because a task's `timeout_seconds` starts with its container, so if the store
never returns, cancel the runs you no longer want.

A release is a different decision. `orkctl release` counts such an Orker as
cordoned, so a ramp or promote that needs it reports `POOL_NOT_SERVED`, naming
the outage's reason: moving new runs onto Orkers that cannot run them can wait
until the store is back.

### Fair dispatch across namespaces

When tasks of several namespaces wait for the same pool's Orkers, the cluster
can dispatch them fairly rather than first in, first out. Every task activity
of a run carries its namespace as a fairness key, and the namespace's
`fair_share_weight` (`orkctl namespace update NS --fair-share-weight 3`; 0, the
default, is a weight of 1, otherwise from 0.001 to 1000) as its weight. Both are decided at
submit and pinned in the run, so a changed weight moves only the runs
submitted after it. A namespace name longer than 64 bytes is keyed by a
readable prefix and a SHA-256 suffix.

The key reaches everything a run hands an Orker: each task, each item of a
fan-out, each member of a pin, and each pin's request for a pin host, which
waits for a free pin slot beside other namespaces' requests. A run that
carries on into a new history keeps its key and weight, and a composed
task's child run takes its parent's. A pin's request carries the key only
through the engine's own run worker (`ork` builds it so), and only in a run
submitted from v0.2.0 on: the submit marks it in the run, and a run submitted
before opens every pin unkeyed to its end, whatever release carries it on.

It is Temporal's own task-queue fairness, never an OrkStack queue, and it takes
effect only where the operator enables it on the Temporal cluster:
`matching.enableFairness=true` in the server's dynamic configuration, for the
Temporal namespace OrkStack uses, or globally. That is a setting of the
deployment's Temporal, not an OrkStack dependency; OrkStack cannot detect it.
Without it the keys are ignored and dispatch stays first in, first out.

What fairness does and does not do:

- it orders the tasks waiting on one pool's channel, per partition of that
  channel, so the effect is real but moderate, and it never moves a task to
  another pool;
- a pin's members wait on their pin host's own channel, so their keys order
  them only among that host's work; across namespaces it is the pin requests
  that compete for the pool's pin hosts;
- a weight pinned at submit never reorders a backlog already queued;
- during a release rollout it holds among the tasks queued for one worker
  version;
- the cluster keeps the state of at most its top 100 keys across a server
  restart.

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
A request refused for the type its body declares (`415`: a Connect call in a
codec the API does not serve, or a `POST /v1/webhooks` body not declared
`application/json`) is refused before any write is attempted and is not
recorded.
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
  otherwise any caller can claim any name. The UI's calls go from the
  browser through the gateway to the engine like any other caller's, so the
  gateway sets the header on them the same way.
- **`ORK_PRINCIPAL` makes `orkctl` assert one.** Set in `orkctl`'s
  environment, every request it sends carries `Ork-Principal:
  $ORK_PRINCIPAL`. It is an assertion like any other: useful on a `make up`
  deployment with no gateway, never proof of who typed the command.
- **A canceled run records the same assertion** as `Run.canceled_by`, which
  `orkctl run get`, `orkctl run list` and the API render as asserted. The
  engine carries it in the cancellation it asks Temporal for: the request's
  reason is `{"format":1,"principal":"…"}` (`RunCancelRequest`, as
  protojson, whose spacing may vary), which the cluster keeps as the `cause`
  of the run's `WorkflowExecutionCancelRequested` event, and the reader takes
  the name from there and from the archived run, never from the audit trail.
  Only the first request, recorded before the run published its end, counts,
  and only on a run that ended `RUN_CANCELED`. A cancel with no header, a
  run canceled with its parent, one canceled from the cluster directly (any
  other reason text) and a terminated run name nobody. On SDK 1.47 that
  request is sent to the frontend directly, so the engine asks again itself
  on `Unavailable`, `ResourceExhausted` and `Aborted`, as the SDK does:
  200 ms apart, doubling to 1 s, for at most 10 s (shorter than the SDK's
  60 s; a `CancelRun` refused so answers `UNAVAILABLE` or
  `RESOURCE_EXHAUSTED`, and may be asked again)
  (`TemporalStarter.CancelRun`); a cancel with no principal goes through the
  SDK as before. During a rollout, a run canceled through an engine of an
  earlier release names nobody, and one whose end an earlier release's
  projection worker archived names its canceller until it is retired, then
  nobody: the archived run is written once.
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
orkctl stats usage -s 720h -g namespace          # who reserved what, this month
orkctl namespace update analytics --budget-cpu-seconds 5000000 --budget-period 720h          # soft: reported only
orkctl -n analytics stats usage -s 720h                  # the gate's own question: does it answer?
orkctl namespace update analytics --budget-enforce            # now it refuses
orkctl namespace update analytics --budget-enforce=false      # back to reporting
orkctl namespace update analytics --clear-budget              # unbounded
```

- **Soft (the default) costs nothing at submit.** It is reported on every
  read — `orkctl stats usage -s <period>`, the dashboard Usage section, Admin
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
run. Where the maximum is shorter than an idempotency key's life
(`ORK_IDEMPOTENCY_TTL` plus five minutes), the key's life takes its place, so
a retry under a key always finds a finished run (§5, "Ops notes", idempotency keys). The cleanup worker retires runs itself, explicitly, ahead of that TTL;
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
  allowed scheme except the Orker's own (below), and the Orker logs a warning
  at boot that says so. A malformed entry, one naming a scheme outside
  `orker.secret_schemes`, or one that can match only the Orker's own sources
  stops the Orker at boot.
- **The tenant's list, per namespace:** `orkctl namespace update NAME
  --allow-secret 'env:SANDBOX_*'` (or Admin → Namespaces). It is enforced by
  the engine at validate and create, as an issue on
  `tasks[i].secrets[j].source`, and again at every submit — `run submit`,
  `run again`, the UI, a trigger invoke, a schedule fire — before a run id is
  minted, so a definition created before the list existed is refused at its
  next submit. It is never retroactive: a run already accepted finishes on
  the definition it pinned; stop one with `orkctl run cancel`. Empty (or
  `--clear-allowed-secrets`) means unrestricted. The list never travels to
  the Orker: a list handed to the Orker by its caller would be no check on
  the caller, and the engine's submit gate is the one no client bypasses.

**The Orker's own sources are never admitted.** Whatever the floor says, empty
or `env:*`, an Orker refuses as a task's source what belongs to it: its
`ORK_*` and `OTEL_*` settings, its proxy settings and the container CLI's
(registry credential helpers' `AWS_*`, `AZURE_*`, `CLOUDSDK_*` and
`GOOGLE_APPLICATION_CREDENTIALS` among them), `/proc`, `/dev`, its container's
credentials under `/run/secrets` and `/var/run/secrets`, its work root, its
configuration file and each file that reads, its telemetry sinks' header
sources, the container engine's state (wherever the engine reports keeping it
at boot), and the credential files its settings name. The work root is the
temp directory when `orker.workdir` is unset, so there a `file:/tmp/…` source
is refused and a floor entry under `/tmp` stops the Orker at boot, as one
under a data root the engine reports does. A `file:` source must also be a
plain absolute path, with no `..`, `.` or empty segment, of at most 4096
bytes, under any floor. A task that names one fails `SECRET_NOT_PERMITTED`
where the floor is checked: after the commit check and a cached task's memo
step, so a result already committed or a memo hit is still reused, and before
its image is pulled. A value a task needs goes under a source of the task's
own, such as a file in a directory the floor names, or a variable the floor
names. [SECURITY_MODEL.md](SECURITY_MODEL.md), "The Orker's own sources",
lists each one and why.

A namespace can therefore only ever narrow what its Orkers permit. The grammar
is deliberately small: a literal scheme, and `*` as the one wildcard, which
never crosses `/`; `?`, `[…]` and `\` are ordinary characters. A `file:`
source with a `..`, `.` or empty segment is refused outright rather than
normalized, so `file:/etc/orkstack/secrets/sandbox/../payments/db` never sits
inside `file:/etc/orkstack/secrets/sandbox/*`. The policy bounds the NAME a
tenant may write. A symbolic link under a tenant's directory is a grant the
operator made, and the Orker reads it where it leads, unless it leads to or
through one of the Orker's own files or directories, which is refused as the
Orker's own name is — judged against what those are when the task's secret is
resolved, so a Kubernetes update of the Orker's own mounted configuration is
seen at once. Nothing here stores, logs or compares a secret value; a refusal
carries the source string and what refused it, never a value.

During a rolling upgrade the namespace list is enforced only by engine
replicas that know it; the Orker floor is unaffected by the rollout.

### Image supply-chain policy

By default any `uses:` a document names is pulled and run: the engine pins it
to a digest at create, which answers "did the image change under us?" and
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

The policy is checked at three doors: at validate and create (an issue on
`tasks[i].uses`), at every submit — `run submit`, `run again`, the UI, a
trigger invoke, a schedule fire — before a run id is minted (`failed_precondition`, so a
definition stored before the policy tightened is refused at its next
submit), and by the Orker before it builds a workspace or pulls, against the
policy the run pinned at submit. A run keeps the policy **and the key** it
was submitted under: tightening the floor, moving a namespace's signer or
rotating a key never reaches a run already accepted; stop one with `orkctl
run cancel`. A committed attempt is adopted without being judged again.

Verification is one fixed scheme — key-based cosign signatures, ECDSA P-256,
over the image's digest; no keyless signing, transparency log, attestation or
policy language. It reads both layouts cosign stores a key-based signature
in: the legacy one, a manifest tagged `sha256-<hex>.sig` whose layers are
simple-signing payloads (cosign 2's default, and how OrkStack v0.1.0's images
are signed), and cosign 3's default, a Sigstore bundle stored as an OCI
referrer of the image, whose DSSE envelope signs an in-toto statement naming
the digest. A referrer is found through the registry's referrers API, or,
on a registry without it (ghcr.io, `registry:3`), under the referrers tag
`sha256-<hex>`, as cosign looks. A bundle's transparency-log entries, if it
has any, are not read: the key is the trust root. The Orker verifies the
signature itself, never taking a caller's word that it was checked, with its
own registry client: `orker.registry_auth_file` /
`ORK_ORKER_REGISTRY_AUTH_FILE` is a docker `config.json` used only for the
signature's manifest, referrers and blob GETs (empty is anonymous, which
public registries allow); the engine uses `ORK_REGISTRY_AUTH_FILE` for the
same GETs at create. A registry that cannot
be asked is never a pass: `unavailable` at create and submit, and a retried
`IMAGE_SIGNATURE_UNAVAILABLE` at the Orker. A missing signature is
`IMAGE_NOT_SIGNED`, a signature by another key or over another digest
`IMAGE_SIGNATURE_INVALID`, an image outside the floor `IMAGE_NOT_PERMITTED`
— all three `ContractViolation`, never retried.

Signing an image with the `cosign` CLI. Any cosign signs in a layout OrkStack
reads: cosign 2 in the legacy one, cosign 3 in the bundle. Both also record
the signature in Sigstore's public transparency log by default, which
OrkStack never reads. cosign 3 keeps it out under a signing config that
names no service, which is how OrkStack's own release signs
([`deploy/cosign-signing-config.json`](../deploy/cosign-signing-config.json),
what `cosign signing-config create` writes with no flags); cosign 2 keeps it
out with `--tlog-upload=false`. Leave either out to record the signature in
the log. An engine or Orker of OrkStack v0.1.0 reads only the legacy layout
and finds no signature in a bundle (`IMAGE_NOT_SIGNED`).

```bash
cosign generate-key-pair                                  # cosign.key, cosign.pub
cosign signing-config create --out signing-config.json    # names no service
cosign sign --key cosign.key --signing-config signing-config.json ghcr.io/acme/migrate@sha256:<digest>
orkctl admin image-policy set --signing-key release=cosign.pub
orkctl namespace update prod-payments --require-signed-by cosign:release
```

**OrkStack's own images.** Each release signs every image it publishes to
`ghcr.io/orkstack`, by digest, and its `SHA256SUMS`, with one key. Its public
half is [`deploy/cosign.pub`](../deploy/cosign.pub), and each GitHub release
attaches the same file as `cosign.pub`; take it from the repository at the
release's tag rather than from the release it vouches for. Register it like
any other key:

```bash
orkctl admin image-policy set --signing-key orkstack=cosign.pub
orkctl namespace update ops --require-signed-by cosign:orkstack
```

A namespace names one signer, so `cosign:orkstack` suits a namespace whose
tasks use only the first-party activities. A namespace that also runs your own
images needs every image signed by one key: verify the first-party images you
use, copy them into your registry, and sign them with yours. To check an image
or the binaries by hand:

```bash
cosign verify --key cosign.pub --insecure-ignore-tlog=true ghcr.io/orkstack/ork@sha256:<digest>
cosign verify-blob --key cosign.pub --insecure-ignore-tlog=true --bundle SHA256SUMS.sigstore.json SHA256SUMS
sha256sum --ignore-missing -c SHA256SUMS
```

Each release's page ends with these commands for each of its images, by
digest. The release records nothing in a public transparency log: the key is
the trust root, and `--insecure-ignore-tlog=true` tells cosign so (it prints a
warning). `SHA256SUMS.sigstore.json` is cosign's bundle of the checksum
signature; `SHA256SUMS.sig`, beside it, is the same signature alone, so
without cosign `openssl dgst -sha256 -verify cosign.pub -signature <(base64 -d
SHA256SUMS.sig) SHA256SUMS` checks it. v0.1.0 attaches only `SHA256SUMS.sig`:
verify its checksums with `--signature SHA256SUMS.sig` in place of `--bundle`
(cosign 3.1 warns that the flag is deprecated) or with openssl.

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

- **The in-flight cap** (`engine.run_max_in_flight`, 500 by default): a run
  keeps at most that many activities, fan-out items, child runs and pins
  pending, and starts more as they finish. It never holds while nothing is in
  flight, and an open pin's own needs are exempt below an absolute 1,900, so it
  can never stall a run. Without it, a run above the cluster's 2,000 pending
  activities per execution would fail every workflow task — stuck. A run that
  keeps more than 500 activities pending at once now waits at 500.

A run does not wait for each task's `TASK_SCHEDULED` and `TASK_STARTED`
publishes before dispatching the next task: it leaves at most 32 publishes
outstanding and sends at most 1 MiB of task input per workflow task (a
fan-out's items have their own 2 MiB, so one workflow task stays under 3 MiB),
both pinned in its plan at submit, and every publish is still awaited before
the run ends or carries on. An outstanding publish keeps 5 events back from
the history limit (its start, its completion and a workflow task to collect
it), pinned too, and a completed one keeps none. A stage of 300 tasks that start together reaches its
cap in seconds; it took about a minute when each publish was awaited, two
cluster round trips per task.
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
decision with `run_id`, `history_events`, `history_bytes` and `in_flight`
(the first also with `reserve_events` and `hold_line_events`: what the run's
failure path still needs and the line its history plus that reserve must
reach before it pauses, so a run that never pauses shows how far off it was);
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
dispatch, completion (12) and the part of a completion recorded when it
lands before the run collects it (5). A run publishes a plain task's
`TASK_SCHEDULED` and `TASK_STARTED` without waiting for them, at most P = 32
outstanding (the window a run pins). Each outstanding publish is reserved
e = 5 events, the history it can still append: its start and the event that
closes it, and a workflow task of its own to collect it. A publish that has
completed is no longer counted. A dispatch records
between d₀ = 8 events (the snapshot marker, the task's scheduling event and
its two publishes) and d₁ = 14 (each publish waking a workflow task of its
own, the cost before the detached path). X is the least over that range,
because a cheaper dispatch puts more tasks in flight before the budget is
due:

```
r₁ = d + f − p             a dispatch: history plus reserve (22 at d₁)
r₂ = d + c − p             a completion and the dispatch it unblocks
Δ(W) = (r₁ + u)·⌈G/d⌉ + P·e                          when d·W ≥ G
Δ(W) = r₁·W + r₂·⌈(G − d·W)/(d + c)⌉ + u·W + P·e     otherwise
X(W) = min over d₀ ≤ d ≤ d₁ of ⌊(L − 1 − B − F − Δ(W)) / p⌋
```

W is the most tasks the shape keeps in flight (at most `engine.run_max_in_flight`).
The u term is the landings: the run dispatches every ready task in one pass
and collects completions only afterwards, so with short tasks every running
task's completion can land, uncollected, during the last dispatch before the
budget is due. At the defaults:

| Shape | W | binding d | Δ(W) | X |
|---|---|---|---|---|
| flat: every task needs nothing | 500 | 14 | 14,488 | **4,389** |
| flat, `engine.run_max_in_flight` 1,000 | 1,000 | 8 | 21,280 | 3,540 |
| staged: 250 lanes | 250 | 14 | 10,168 | 4,929 |
| deep: lanes of at most 380 | 15 | 14 | 6,091 | 5,438 |
| one chain | 1 | 14 | 5,857 | 5,468 (but rule 44 and `INPUT_TOO_LARGE` bound a chain far sooner) |

X never rises with W, so the flat stage at the in-flight cap binds: X is 4,389
at the defaults. A wider cap lowers it: `engine.run_max_in_flight` above about
700 puts X below the 4,000 default of `engine.orkflow_max_tasks`, and the boot
warns. It moves with the other settings too: `engine.continuation.events` at
its maximum `max_events/4` (12,800) gives 3,988; `engine.run_max_in_flight`
300 gives 4,821; `engine.continuation.restore_timeout` at 8 h gives 3,207.
The boot computes X
for the configured settings and warns when `engine.orkflow_max_tasks` is above
it, naming `engine.history.max_events`, `engine.continuation.events`,
`engine.continuation.bytes`, `engine.continuation.restore_timeout` and
`engine.run_max_in_flight`.

What X does not cover: it counts plain tasks only — awaits (24 reserved each,
outside the in-flight cap), pins, fan-outs, composed tasks and failure
handlers keep more back and trip the guard far below X; and it is a bound in
events only. The bytes form of the same arithmetic is derived, not measured,
and above the events form at the defaults (6,175 for 16 KiB dispatches in a
flat stage, 8,369 for 256 KiB in a deep one). A run within X can still stop,
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
| `TestRunWorkflow_ReplaysCommittedJoinHistory` | the replayer against the join fixture (`run-join-v1.json`, `internal/dsl/testdata/orkflows/branch-and-recover.ork.yaml` with a failing `deploy`), plus the order the replay schedules tasks in | a changed join decision, a failure handler dispatched in a different place, a moved failing note or `RUN_FAILED` |
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
the restore policy, the cap, the publish window, what each outstanding
publish reserves and the per-workflow-task input budget — is pinned in the run's plan at submit, never read from a code
constant, so a release that changes a default moves only new runs. A run whose
plan pins the publish window records `task-publish-window` before its first
plain task's `TASK_SCHEDULED`; a plan from before it pins none and never asks.
A run records `emit-past-cancel` at its first event: from then on an
operator's cancel never stops the write or the publish of an event that
records what already happened (a task's outcome, the run's own end), and the
run announces no dispatch once it is canceled, so a task that finished as the
cancel arrived keeps its `TASK_SUCCEEDED` in the stream and the archive. A
history recorded before it has no marker and keeps its old commands.
The fixtures `run-continued-g0-v1.json` and `run-continued-g1-v1.json`
(one run recorded across a switch, with the three objects it wrote) and
`run-in-flight-cap-v1.json` guard them, each with its command, payload and
negative tests, and the `run-task-publish-window` and `run-emit-past-cancel`
pairs guard the publish window and the emit past a cancel with their command
and negative tests
(`internal/engine/testdata/history/README.md`); a carried
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

**An ending before start writes `RUN_CREATED` first.** A run whose definition,
passed by reference, cannot be used ends in its first generation with
`RUN_CREATED` at sequence 1 and its terminal event at 2. When object storage
refuses `RUN_CREATED`'s snapshot, it is written again under the same number,
so a store that is back for that write leaves both events. When that write
fails too, the number stays unused and the terminal event is still written at
2, under a key of its own. Its stream then starts at 2, and every read says so.
A live read from its start serves a `STREAM_GAP` event naming 1 and then the
terminal event; the archive records 1 as a gap line (`docs/STORAGE.md`), and a
read of the archive from its start fails `DataLoss` naming it, with nothing
sent, and the run page's socket closes with a shortened form of the same
sentence; a read from 1 serves the terminal event. Before this, the terminal
event was written at 2 the same way, and the stream silently began there. The
reader recognises the gap from the history alone, so a run recorded before the
gate, which wrote the terminal event after a single refused write, is read and
archived the same way. When `RUN_CREATED` is written and the terminal write
fails, the stream holds `RUN_CREATED` and the run's close is read as its
terminal event, at 2. When the terminal write fails as well as both of
`RUN_CREATED`'s, the run publishes nothing and its close is read as its one
event, at 1. In both of those it publishes no terminal event, so, like every run
that publishes none, its archive comes from the archive sweep (§1). It records the
`run-created-first` version, asked lazily where `RUN_CREATED`'s write first
fails: an ending whose writes take records no marker. A history recorded
before the gate replays its `DefaultVersion` branch, which wrote the terminal
event next; version 1 is the new path. The fixtures
`run-created-first-v1.json` (the second write takes),
`run-created-first-gap-v1.json` (neither does) and
`run-created-first-legacy-v1.json` (recorded with the gate off) guard it with
command, behaviour, reader and negative tests
(`internal/engine/testdata/history/README.md`).

**A number a run uses without an event is recorded wherever it falls.** When
object storage refuses an event's snapshot, the number stays used and no event
is published under it; the run's next event takes the next number. The workflow
is unchanged: the reader finds such numbers from the history alone, at the start
of the stream, inside an earlier part's event segment or at its end, so runs
recorded before this are read the same way. A live read of the stream serves a
`STREAM_GAP` event at the last of them, naming them all, and goes on to the
run's terminal event: `StreamEvents`, the run page's socket and `orkctl run
watch` agree, and a resume past that number does not see it again. `STREAM_GAP`
belongs to the read, and the run's archive holds no such event. It records the
numbers as a `gap` line with the reason `the run's history holds no event under
these numbers`, and a read of the archive over them ends `DataLoss`, as it does
over any gap, after the events below it; a resume past them serves what follows.
An archive written before this holds no line for such a number, and its stream
skips it silently.

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

**A pin's fairness key is a versioned change.** A run whose plan's fairness
selection says `pin_requests` (set at submit since v0.2.0) asks the
`fairness-pin-creation` version once, where its first pin opens, and every pin
request it then makes carries the plan's key and weight; a run that opens no
pin, or whose plan carries no such selection — every run submitted before —
records no marker, and a generation that continues a run answers from what the
run already decided. A history recorded before the gate replays its
`DefaultVersion` branch, which keys no pin request. The SDK builds a pin
request's options itself, so the key reaches it through an interceptor of the
engine's run worker (`engine.RunWorkerInterceptors`, set by `cmd/ork`); a run
worker without it opens pins unkeyed and changes nothing else. The fixtures
`run-fairness-pin-creation-v1.json` and
`run-fairness-pin-creation-legacy-v1.json` guard it with command and negative
tests, one of which replays the gated fixture without the interceptor and must
see the key missing.

**A retry's default maximum interval is a versioned change.** A run's plan
carries the maximum wait between two attempts of a task whose `retry:` sets no
`maximum_interval_seconds` (60 seconds, `engine.DefaultRetryMaxInterval`),
pinned at submit and copied into each child run's start. The run asks the
`retry-max-interval` version once, where it first schedules such a task. A
history recorded before the gate replays its `DefaultVersion` branch, the
100 seconds every earlier build used, whatever its plan says; so does a run
whose plan carries no number. Either way the maximum is never below the task's
own initial interval: the cluster refuses that policy by failing the workflow
task over and over, so no recorded history holds one, and every earlier build
left such a run stuck. The same holds for an explicit maximum below the
initial interval and a backoff coefficient between 0 and 1, which a version
created before DSL rule 47 may carry: the task is scheduled with the maximum
raised to the initial interval and the coefficient raised to 1, with no gate,
because no recorded history holds the refused command
(`TestIntegration_AVersionStoredBeforeTheRetryRuleIsScheduled`). A stored
coefficient that is not a number is different: the cluster accepts it (the
same test sends one), so a run may already have scheduled it. It reads as the
default 2.0 only on the gate's new path, in a run whose plan carries the
number, and the run asks the gate where it first schedules such a task if it
has not already; a run submitted before v0.2.0, and a history recorded before
the gate, keep sending the NaN as every earlier build did, so its spacing never
changes partway through a run. A build that
predates the field ignores the plan's
number, so a rollback to one waits 100 seconds again. The fixtures
`run-retry-max-interval-v1.json` and `run-retry-max-interval-legacy-v1.json`
guard it with command and negative tests, and
`run-retry-long-initial-v1.json` and `run-retry-long-initial-legacy-v1.json`
record the raise to the initial interval on both branches.

The replayer compares neither a priority nor a retry policy: it matches a
scheduled activity by its id and type alone
(`TestSDKPremise_TheReplayerComparesNoPriorityAndNoRetryPolicy`). So a key or a
spacing added to a recorded command never breaks a replay; a version gate is
what keeps a recorded run on the values it was recorded with, and these tests
compare those values themselves.

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

**The admin listener answers only names it knows.** It applies the API
listener's `Host` rule (§ "The API listener's bounds") under its own setting:
with nothing set it admits an IP address, `localhost` and every name under
it, a name without a dot, and the host `ORK_ADMIN_ADDR` names, and refuses
any other `Host` with `403`, logging `request refused: Host not allowed` with
`listener="admin listener"`. A scraper that dials a name with a dot — a
Prometheus static target such as `host.docker.internal`, a Kubernetes Service
FQDN such as `ork-admin.orkstack.svc.cluster.local` — is refused until that
name is listed in `ORK_ADMIN_ALLOWED_HOSTS` (`telemetry.admin_allowed_hosts`)
on the binary it scrapes. A deployment upgrading from 0.1.0 that scrapes by
such a name sets it before the upgrade. A scraper that dials an IP address,
such as Kubernetes pod discovery, needs nothing.

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
  admin_addr:
    ork: ":9464"
    orker: ":9465"
  admin_allowed_hosts:
    - host.docker.internal   # the name the profile's Prometheus scrapes by
  exporters:
    logs: otlp
    metrics:
      - prometheus
      - otlp
    traces: otlp
  otlp:
    endpoint: http://localhost:4318
    protocol: http/protobuf
  sinks:
    laptop-collector:
      endpoint: http://localhost:4318
      headers:
        X-Scope-OrgID:
          source: env:LOKI_ORG_ID
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
along on the same `/metrics`. An Orker that declares a capacity adds
`ork.orker.capacity.*` and `ork.orker.admission.*` ("Declaring an Orker's
capacity", above). Every Orker adds `ork.orker.store_outage.*` ("When object
storage goes away", above).

`ork.audit.records{action,outcome}` (engine) counts every control-plane
audit write, once per audited write RPC: `outcome` is `written`, or `dropped`
when the audit store refused the write (the RPC itself still answered as it
would have — see §5 "Attributing control-plane writes"). A rising `dropped`
rate is an unwritable `metadata/audit/` prefix.

`ork.idempotency.legacy_pending` (engine) counts keyed submissions answered
with a pending run that was not found, under a key claimed by an earlier
release (§5 "Ops notes", idempotency keys). It should stay at zero once one
`engine.idempotency_ttl` has passed after the last engine was upgraded; a
rising count during a rollout names runs that may never start.

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
| `ork.orker.attempt_record_legacy` | `ork.orker_id`, `ork.found` | Orker, each attempt record of an earlier release it met: in the scope it claimed in (`claim`, one per record) or under its own ordinal (`record`); any count means the pool runs two releases ("Upgrading a pool's Orkers") |
| `ork.orker.task.duration` | `outcome` | Orker, same event |
| `ork.orker.task_phase.duration` | `ork.phase` | Orker, once per phase of an attempt |
| `ork.orker.commit_adopted` | — | Orker, once per adopted commit |
| `ork.orker.workspace_cleanup_failures` | — | Orker, once per attempt whose own directory or secrets could not be removed when it ended and are left on disk (typically a directory a workload made as a uid other than the Orker's, which the Orker can neither write nor chmod); the Orker logs the path, the error and the directory that refused the removal with its mode and owner (`refused_by`) |
| `ork.runs.refused` | `ork.namespace`, `ork.refusal.reason` | engine, once per submission the namespace's hold, `max_running` or enforced budget refused (`namespace_held`, `max_running_reached`, `budget_exceeded`, `budget_unmeasurable`); not a run, not counted on `ork.runs` |
| `ork.run.continuations` | `ork.namespace`, `outcome` | engine, once per run carried on into a fresh history: `written` (its saved state written), `write_failed` (object storage refused it; the run tries again after more growth), `over_cap` (its saved state is over `engine.continuation.bytes`; it never carries on), `restored` (the next history read it back), or a `RUN_STATE_*` code (the next history could not) |
| `ork.run.continuation.state.bytes` | `ork.namespace`, `kind` | engine, the stored size of what a switch wrote: `checkpoint` (the saved state), `events` (the earlier history's events), `snapshot` (the full state at the switch) |
| `ork.run.continuation.duration` | `ork.namespace`, `phase` | engine, seconds a switch's write (`write`) or a whole restore (`restore`) took |
| `ork.run.continuation.hold.duration` | `ork.namespace` | engine, seconds a run held new tasks before it carried on (0 when it found a moment with nothing running on its own) |
| `ork.run.continuation.read.attempts` | `ork.namespace` | engine, the attempt each successful read of a saved state succeeded on |
| `ork.archive.sweep.runs` | `outcome` | engine, once per finished run with no archive the archive sweep came to (§1, "The archive sweep"): `archived`, `failed` (it tries again next pass), or `expired` (the cluster had already let it go: the run is lost). Every outcome's series is present, at zero, from the first pass |
| `ork.archive.cleanup.runs` | `outcome` | engine with cleanup on, once per run a cleanup pass came to and acted on or kept (§1, "Retention cleanup and bucket lifecycle rules"): `retired`, `failed` (left as it was; a later pass tries again), `not_found_not_due` (its execution is missing before its run retention, so it was kept: **alert on any**), or `not_found_held` (due, with its execution missing, but held because too many of that pass's runs were missing early) |
| `ork.archive.sweep.unconfirmed` | — | engine, each pass: finished runs it could not confirm have an archive. **Alert on this above zero.** While object storage is down it counts every finished run the pass did not reach, archived or not |
| `ork.archive.sweep.earliest_expiry` | — | engine, each pass: seconds left until the first of those runs expires: 0 when the pass has none or could not date them, so an undated risk reads as imminent, and negative once that time has passed. Read it only while `ork.archive.sweep.unconfirmed` is above zero |
| `ork.archive.sweep.list_failed` | — | engine, each pass: 1 when the pass could not read the cluster's index of finished runs to its end, so its counts are only what it reached; else 0. **Alert on 1** |
| `ork.archive.sweep.interval_too_long` | — | engine, each pass: 1 when two cleanup intervals reach the cluster's retention, so a run can be deleted before a pass looks at it; else 0. **Alert on 1**, and shorten `ORK_CLEANUP_INTERVAL` |
| `ork.archive.sweep.last_success` | — | engine: unix time of the last pass that saw everything, its index listed to its end and object storage answering whether each run has an archive. A store that answers but refuses the archive writes still advances it; that shows as `ork.archive.sweep.unconfirmed` above zero. **Alert when it is older than a few cleanup intervals**: the sweep is blind |

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

Beside the metrics, the engine logs one `run event` record per event it
publishes — at Info when the run's sink has logs on, Debug otherwise, and
always Debug for `TASK_LOG` — carrying `run_id`, `sequence`, `event_type`,
`status`, `task_id` for a task event, `namespace` and `orkflow`, the
namespace's selection attributes, and the trace context. `sequence` is the
event's number in its run's stream, the one `StreamEvents` and the archive
order by. These records, like the projection's other per-event work, can reach
a collector out of order: a retried publish arrives late, and a run does not
wait for some publishes before it goes on — a fan-out's item events (so since
v0.2.0) and, from 0.3.0, a plain task's `TASK_SCHEDULED` and `TASK_STARTED` —
so two can be in flight at once and complete in either order. A consumer that
needs a run's order sorts its records by `sequence`.

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
