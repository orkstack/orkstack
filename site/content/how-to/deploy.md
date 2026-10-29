# Deploy OrkStack

Run OrkStack as a replicated deployment: one Temporal, one S3-compatible bucket, any number of `ork` replicas, and one or more Orker pools. This page shows the shape and the settings to set first. The [full deployment manual](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md) has every key and rule.

## Example

Both binaries read one file. Put the values that must match on both binaries in it, then check the file before you start anything:

```yaml
# orkstack.yaml: read by ork and by orker
temporal:
  address: temporal.internal.example:7233
  namespace: ork-main
pool:
  name: main
storage:
  backend: s3
  s3:
    endpoint: https://s3.internal.example
    region: us-east-1
    bucket: orkstack-artifacts
    access_key:
      file: /run/secrets/s3_access_key
    secret_key:
      file: /run/secrets/s3_secret_key
    path_style: false
engine:
  ws_allowed_origins:  # only when the UI is on another origin
    - https://orkstack.internal.example
```

```bash
ork --config orkstack.yaml --check       # prints the effective settings, exits 0 when valid
orker --config orkstack.yaml --check
ork --config orkstack.yaml               # one engine replica; start as many as you need
orker --config orkstack.yaml             # one Orker; start one or more per pool
```

On each Orker host, set what is particular to that host:

```bash
export ORK_ORKER_WORKDIR=/srv/orkstack/work   # a path the Docker daemon can bind-mount
```

<!-- site-diagram: architecture -->

## Topology

- **Temporal** keeps execution state: which runs exist, what each task is doing, and retries. Run it yourself. A hosted Temporal Cloud is not supported yet.
- **S3-compatible object storage** holds Orkflow definitions, artifacts, task logs and finished runs. It must support conditional writes (`If-None-Match`). Each Orker checks this at boot and refuses to start without it.
- **`ork`** is the engine: the API, and the WebSocket bridge for live run streams. Its replicas hold no state, so any replica answers for any run. Put one address in front of them.
- **`orker`** runs the containers. It joins one pool and runs its tasks on the Docker daemon of its own host.
- **`ui`** is the web interface. Its server forwards API calls to `ork`. The browser opens live run streams to `ork` directly.

Point `orkctl` at any replica with `--server` or `ORK_SERVER`.

## Images

Each release publishes `ork`, `orker`, `orkctl`, `ui` and six first-party activity images to `ghcr.io/orkstack`, for linux/amd64 and linux/arm64. Deploy by digest, or by the `X.Y.Z` tag, which never moves.

No release is published yet, so there are no images to pull. The [release notes](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#release-artifacts) describe what a release publishes.

Each release signs its images and its `SHA256SUMS` with one cosign key. The public half is [`deploy/cosign.pub`](https://github.com/orkstack/orkstack/blob/main/deploy/cosign.pub) in the repository, and each release attaches it as `cosign.pub`. Register it as a signing key. A namespace whose tasks use only the first-party activities can then require it:

```bash
orkctl admin image-policy set --signing-key orkstack=cosign.pub
orkctl namespace update ops --require-signed-by cosign:orkstack
```

A namespace requires one key. To mix your own images with the first-party ones, copy the ones you use into your registry and sign everything with your key. To check an image by hand:

```bash
cosign verify --key cosign.pub --insecure-ignore-tlog=true ghcr.io/orkstack/ork@sha256:<digest>
```

The `orker` mounts the host's Docker socket. Whoever controls that socket controls the host. Activity containers never receive it.

## Settings that matter first

| Setting | Environment variable | Default | Set on |
|---|---|---|---|
| `temporal.address` | `ORK_TEMPORAL_ADDRESS` | `localhost:7233` | both |
| `temporal.namespace` | `ORK_TEMPORAL_NAMESPACE` | `ork-main` | both. One per environment; nothing crosses it |
| `storage.backend` | `ORK_ARTIFACT_BACKEND` | `local` | both. Set `s3` for a replicated deployment |
| `storage.s3.*` | `ORK_S3_*` | the development store | both. `ork` always needs these, because it reads definitions from the bucket |
| `pool.name` | `ORK_ORKER_POOL` | `main` | both. The pool an Orker joins, and the pool a task with no `pool:` runs on |
| `orker.workdir` | `ORK_ORKER_WORKDIR` | the temp directory | orker. Must be a path the Docker daemon can bind-mount, with no comma or control character in it |
| `orker.max_concurrent` | `ORK_ORKER_MAX_CONCURRENT` | `16` | orker. Tasks at once on one Orker; size it to the host |
| `orker.resources.max_cpu`, `max_memory` | `ORK_ORKER_MAX_CPU`, `ORK_ORKER_MAX_MEMORY` | `8`, `16Gi` | orker. A task that asks for more is refused |
| `orker.pins.max_concurrent` | `ORK_ORKER_MAX_PINS` | `0` | orker. Above zero makes the Orker a pin host |
| `deployment.build_id` | `ORK_BUILD_ID` | empty | both. Empty means versioning is off |
| `engine.ws_allowed_origins` | `ORK_WS_ALLOWED_ORIGINS` | empty | ork. Needed when the UI runs on another origin |
| `telemetry.admin_addr` | `ORK_ADMIN_ADDR` | empty | both. Empty opens no listener |
| `engine.cleanup.enabled` | `ORK_CLEANUP_ENABLED` | `false` | ork. Off keeps every run; on retires runs by their namespace's retention |

On a host, an Orker with no `ORK_ORKER_ID` derives a unique id from its host and work directory. In a container, set it explicitly, one distinct id per Orker.

## Health checks

`ork --healthcheck` and `orker --healthcheck` ask the process whether it is ready. The images have no shell, so use these as the container health check. They need `ORK_ADMIN_ADDR` set.

## Before you depend on it

OrkStack is pre-release. Breaking changes can land until 1.0. The [production checklist](https://github.com/orkstack/orkstack/blob/main/docs/PRODUCTION_CHECKLIST.md) lists what to take care of. It is a list of care items, not a certification. Start with these:

- Rehearse a rollout, a rollback, an Orker drain and a cleanup pass in a deployment that runs no real work.
- Give each environment its own Temporal namespace.
- Replace the development S3 credentials with your own.
- Put a gateway in front of `ork` that authenticates callers. OrkStack does not authenticate callers.
- Keep `ORK_ADMIN_ADDR` on a private address. Keep `ORK_PPROF_ENABLED` off unless you are profiling.
- Drain an Orker before you stop it, when it may host pins.

## See also

- [DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#0-topology--what-you-run-and-where-state-lives): the topology, in full
- [DEPLOY.md, “1. Configuration”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration): every key and its variable
- [PRODUCTION_CHECKLIST.md](https://github.com/orkstack/orkstack/blob/main/docs/PRODUCTION_CHECKLIST.md): the full list of care items
- [STORAGE.md](https://github.com/orkstack/orkstack/blob/main/docs/STORAGE.md): what OrkStack keeps in the bucket, and where
- [DEPLOY.md, “Image supply-chain policy”](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#image-supply-chain-policy): trusting the release's signatures, and checking the binaries
- [LIMITATIONS.md](https://github.com/orkstack/orkstack/blob/main/docs/LIMITATIONS.md): what is not built yet
- Next: [Roll out a release](/docs/how-to/roll-out-a-release/), [Manage namespaces](/docs/how-to/namespaces/), [Observe a deployment](/docs/how-to/observe/)
