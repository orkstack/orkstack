# Requirements

OrkStack needs a Temporal cluster, an S3-compatible bucket, Docker or Podman on each Orker host, and the published images. This page lists what to provide and what to decide before you start. To start the published images, see [Run the published images](/docs/deploy/published-images/).

## What you provide

| Part | What it must do |
|---|---|
| Temporal | Run a cluster you operate. A hosted Temporal Cloud is not supported yet. |
| S3-compatible storage | Support conditional `PutObject` (`If-None-Match`, `If-Match`), conditional `DeleteObject` (`If-Match`) and `DeleteObjects`. Every Orker and every engine checks conditional writes at boot and refuses to start without them. |
| Docker or Podman | Run on each Orker host. The Orker bind-mounts its work directory into each task. |
| Images | `ork`, `orker`, `orkctl` and the first-party activities, from `ghcr.io/orkstack`. |

The repository states no minimum server version. The development stack pins Temporal server 1.32.

## Settings to decide first

Set these before the first start. Both binaries read the same file, and each entry links to its full description.

| Setting | Why it matters |
|---|---|
| [`temporal.address`](/docs/configuration/#temporal-address), [`temporal.namespace`](/docs/configuration/#temporal-namespace) | Where the cluster is, and the partition for this environment. Use one namespace per environment. |
| [`storage.backend`](/docs/configuration/#storage-backend) | Set `s3` for any replicated deployment. The default `local` is one directory per process. |
| [`storage.s3.endpoint`](/docs/configuration/#storage-s3-endpoint), [`storage.s3.bucket`](/docs/configuration/#storage-s3-bucket) | The bucket every `ork` and `orker` shares. |
| [`storage.s3.secret_key`](/docs/configuration/#storage-s3-secret-key) | Set with `access_key`, reading them from files if you like, or leave both unset to use an instance or pod role. |
| [`pool.name`](/docs/configuration/#pool-name) | The pool each Orker joins. On `ork`, the value is where a task that names no `pool:` goes. Leave it unset on `ork` unless you change the default pool. |
| [`orker.workdir`](/docs/configuration/#orker-workdir) | A path the container daemon can bind-mount, the same inside and out. |
| [`orker.max_concurrent`](/docs/configuration/#orker-max-concurrent) | Tasks one Orker runs at once. Size it to the host. |
| [`telemetry.admin_addr`](/docs/configuration/#telemetry-admin-addr) | The address of the health and metrics listener. Empty opens none. |

Check the file before you start anything:

```bash
ork --config orkstack.yaml --check       # prints the effective settings, exits 0 when valid
orker --config orkstack.yaml --check
```

## Published images

Each release publishes `ork`, `orker` and `orkctl`, plus six first-party activity images: `shell`, `http`, `file`, `json`, `git` and `template`. Each image is built for linux/amd64 and linux/arm64. Tags carry the release version, such as `ghcr.io/orkstack/orker:0.2.0`. Deploy by digest, or by the version tag, which does not move.

There is no UI image. `ork` serves the web UI itself. Each release's images are signed; see [Image policy](/docs/operate/image-policy/) to require the signature.

## Containers on Orker hosts

The Orker starts each task's container through a Docker daemon on its host. Whoever can use that daemon's socket can start a privileged container, so the socket belongs to the Orker alone. Activity containers never receive it.

- **Docker:** mount the daemon's socket into the Orker container.
- **Podman:** run the podman CLI, or Podman's Docker-compatible socket. See [Running on Podman](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-on-podman).
- **Kubernetes:** give each Orker pod an engine of its own. Never use the node's container runtime or its Docker socket. See [Production on Kubernetes](/docs/deploy/kubernetes/).

## Health checks

`ork --healthcheck` and `orker --healthcheck` ask the process whether it is ready. The images have no shell, so use these as the container's health check. They need `ORK_ADMIN_ADDR` set, and they start nothing.

```yaml
services:
  ork:
    healthcheck:
      test:
        - CMD
        - "/usr/local/bin/ork"
        - "--healthcheck"
      interval: 10s
      timeout: 5s
      start_period: 30s
      retries: 3
```

An Orker's boot can take a long time on an overloaded daemon. It runs a probe container before it accepts work. Give the check a `start_period` long enough for that, or the supervisor marks the Orker unhealthy before it can say why.

## Before you depend on it

The [production checklist](https://github.com/orkstack/orkstack/blob/main/docs/PRODUCTION_CHECKLIST.md) lists what to care for. Start with these:

- Put a gateway in front of `ork` that authenticates callers. OrkStack does not authenticate callers.
- Terminate TLS at that gateway. The engine serves plain HTTP.
- Keep `ORK_ADMIN_ADDR` on a private address.
- Rehearse a rollout, a rollback, an Orker drain and a cleanup pass in a deployment that carries no real work.

## See also

- [DEPLOY.md, "Running the published images"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-the-published-images): each image, its user and the settings it needs
- [DEPLOY.md, "Health checks"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#health-checks): what ready means, and how long a boot can take
- [DEPLOY.md, "Running on Podman"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-on-podman): Podman's two shapes, and what differs from Docker
- [DEPLOY.md, "Rootful Podman under AppArmor"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#rootful-podman-under-apparmor): the AppArmor rule rootful Podman needs on recent Ubuntu releases
- [DEPLOY.md, "Orkers on Kubernetes"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#orkers-on-kubernetes): the two Kubernetes shapes
- [DEPLOY.md, "1. Configuration"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#1-configuration): every key and its variable
- [PRODUCTION_CHECKLIST.md](https://github.com/orkstack/orkstack/blob/main/docs/PRODUCTION_CHECKLIST.md): the full list of care items
- [LIMITATIONS.md, "Engine, cluster and gateway"](https://github.com/orkstack/orkstack/blob/main/docs/LIMITATIONS.md#engine-cluster-and-gateway): what is not supported yet
