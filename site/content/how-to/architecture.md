# Architecture

A deployment runs one Temporal cluster, one S3-compatible bucket, any number of `ork` replicas and one or more Orker pools. Replicas and Orkers hold no state, so any replica answers for any run.

<!-- site-diagram: architecture -->

## What runs where

| Part | Runs | Keeps |
|---|---|---|
| Temporal | The cluster you operate | Live runs, task state, retries, idempotency claims |
| S3-compatible bucket | Storage only | Definitions, artifacts, task logs, finished runs, audit records, cache memos |
| `ork` replicas | The API, the WebSocket bridge, the web UI, and run workflows | Nothing durable |
| `orker` pools | Task containers, on a Docker or Podman engine on each Orker's host | Nothing durable |

A replica can stop at any time. Its runs carry on through the other replicas, and a client that reconnects to any replica reads the same run.

## Replicas: N `ork`, one address

- Every replica runs with the same Temporal address and namespace, release identity, pool, and bucket settings. Only `ORK_HTTP_ADDR` must differ, and only when replicas share a host.
- Put one address in front of the replicas, with a load balancer, an ingress or DNS round robin. No affinity to one replica is needed.
- Each replica also polls the engine's own work, so adding a replica adds workflow capacity as well as API capacity.
- Point `orkctl` at any replica with `--server` or `ORK_SERVER`.

## Pools: M Orker pools

Each Orker joins one pool with `ORK_ORKER_POOL`. Pools isolate work: a GPU host, an Orker build with a different security profile, or a host near some data. Add Orkers to a pool for throughput. Add a pool for isolation. See [Pools, Orkers and pins](/docs/operate/pools-and-orkers/).

## The web UI

`ork` serves the web UI at `/` on its API port, beside the API and the WebSocket bridge. The browser's calls and live streams stay on one origin, and nothing needs configuring for that.

In front of the replicas:

- Route the whole host to `ork`, not only the API's paths. The UI's pages and assets are on the same port.
- Forward WebSocket upgrades under `/ws/`.
- Keep the `Host` header the browser sent. If a proxy rewrites it, list the page's origin in `ORK_WS_ALLOWED_ORIGINS`.
- List every name with a dot that the engine is reached by in `ORK_ALLOWED_HOSTS`. Any other `Host` is refused with `403`.
- Serve the UI at the root of a host, not under a sub-path.
- Terminate TLS at the proxy. The engine serves plain HTTP.

`ORK_UI_ENABLED=false` turns the UI off and leaves the port to the API.

## Where to go next

- [Requirements](/docs/deploy/requirements/): what to provide before you start
- [Object storage](/docs/deploy/object-storage/) and [Temporal](/docs/deploy/temporal/): the two stores
- [Production on Kubernetes](/docs/deploy/kubernetes/): the Orker shapes for a cluster

## See also

- [DEPLOY.md, "0. Topology — what you run, and where state lives"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#0-topology--what-you-run-and-where-state-lives): the topology, in full
- [DEPLOY.md, "Nothing durable lives in a process"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#nothing-durable-lives-in-a-process): which state lives where
- [DEPLOY.md, "Running N `ork` replicas"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-n-ork-replicas): replica configuration and reconnects
- [DEPLOY.md, "Running M Orker pools"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-m-orker-pools): how a process joins a pool
- [DEPLOY.md, "The web UI"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-web-ui): the UI, its origin and the proxy rules
- [DEPLOY.md, "The API listener's bounds"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#the-api-listeners-bounds): the host and origin rules
- [STORAGE.md, "Backends"](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md#backends): what each storage backend means for a replica
