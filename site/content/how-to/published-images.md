# Run the published images

Start `ork` and `orker` in production from the images each release publishes. This page gives a Compose file for each and says what each one needs. Every setting and its variable is in [Configuration](/docs/configuration/). The full reference is [DEPLOY.md, "Running the published images"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-the-published-images).

## The images

Each release publishes these to `ghcr.io/orkstack`, for linux/amd64 and linux/arm64:

| Image | Runs | User | Needs |
|---|---|---|---|
| `ghcr.io/orkstack/ork` | The engine and the web UI, on its API port | non-root | Temporal, the bucket, and `ORK_ADMIN_ADDR` for the health check |
| `ghcr.io/orkstack/orker` | An Orker, with the `docker` CLI | root | The Docker socket, a work directory at the same path, Temporal and the bucket |
| `ghcr.io/orkstack/orkctl` | The command line | non-root | `ORK_SERVER`. Its working directory is `/work` |

The release also publishes six first-party activity images: `activity-shell`, `activity-http`, `activity-file`, `activity-json`, `activity-git` and `activity-template`. There is no UI image, because `ork` serves the UI itself.

A version tag such as `0.3.1` never moves. Deploy by it, or by digest. The `ork` and `orker` images are distroless: they have no shell, so `curl` and `wget` are not available.

## The engine

Keep the settings in an env file outside git. Replace the placeholders:

```text
ORK_TEMPORAL_ADDRESS=temporal.internal.example:7233
ORK_TEMPORAL_NAMESPACE=ork-prod
ORK_ARTIFACT_BACKEND=s3
ORK_S3_ENDPOINT=https://s3.internal.example
ORK_S3_BUCKET=orkstack-artifacts
ORK_S3_ACCESS_KEY=replace-me
ORK_S3_SECRET_KEY=replace-me
ORK_S3_PATH_STYLE=false
ORK_ADMIN_ADDR=127.0.0.1:9464
ORK_DEPLOYMENT_NAME=orkstack
# Every name with a dot that clients or a gateway use to reach the engine.
ORK_ALLOWED_HOSTS=ork.internal
# Setting ORK_BUILD_ID turns versioning on. Leave it out to run unversioned.
ORK_BUILD_ID=0.3.1
```

```yaml
services:
  ork:
    image: ghcr.io/orkstack/ork:0.3.1
    env_file: ork.env
    ports:
      - "127.0.0.1:8080:8080"
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

Put a gateway in front of `ork`. The engine serves plain HTTP and does not authenticate callers. List every name with a dot that reaches it in `ORK_ALLOWED_HOSTS`: any other such `Host` is refused `403` ([`engine.allowed_hosts`](/docs/configuration/#engine-allowed-hosts)).

Create the bucket named by `ORK_S3_BUCKET` before the first start. No OrkStack process creates it. The Temporal namespace named by `ORK_TEMPORAL_NAMESPACE` is provisioned at boot, so you do not create it.

## Run orkctl against this deployment

The `orkctl` image runs the command line against your engine. Set `ORK_SERVER` to the engine's address, and put the command after the image name. This example promotes release `0.3.1`, so it changes which release takes new runs:

```bash
docker run --rm -it -e ORK_SERVER=http://ork.internal:8080 ghcr.io/orkstack/orkctl:0.3.1 release promote 0.3.1
```

The `-it` flags give the command a terminal, so it can ask you to confirm. A script has no terminal. Add `--yes` there, after the command:

```bash
docker run --rm -e ORK_SERVER=http://ork.internal:8080 ghcr.io/orkstack/orkctl:0.3.1 release promote 0.3.1 --yes
```

The image's working directory is `/work`. To create an Orkflow from a file, mount its folder there, for example with `-v "$PWD:/work:ro"`, and pass the file's name.

## An Orker

`orker.env` has the same Temporal, storage, admin and release lines as `ork.env`. Each Orker sets its own `ORK_ORKER_ID` in the Compose file. Run one service per Orker:

```yaml
services:
  orker-1:
    image: ghcr.io/orkstack/orker:0.3.1
    env_file: orker.env
    environment:
      ORK_ORKER_ID: orker-1
      ORK_ORKER_WORKDIR: /var/lib/orkstack/work
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - /var/lib/orkstack/work:/var/lib/orkstack/work
    init: true
    stop_grace_period: 45s
    healthcheck:
      test:
        - CMD
        - "/usr/local/bin/orker"
        - "--healthcheck"
      interval: 10s
      timeout: 5s
      start_period: 2m
      retries: 3
```

With `ORK_BUILD_ID` set in `ork.env` and `orker.env`, run `orkctl release promote 0.3.1` once, after the first start. Until you do, runs sit idle, because the new release has no traffic. To run unversioned, leave `ORK_BUILD_ID` out.

## What the Orker needs

- **The Docker socket.** Each task's container starts through the host's Docker daemon. Whoever can use the socket can start a privileged container, so give it to the Orker and to nothing else.
- **Root.** The `orker` image runs as root. The `ork` and `orkctl` images run as non-root.
- **The same path inside and out.** The daemon mounts each task's workspace from the host, so the work directory must be mounted at the same path. Where the Docker daemon runs in a virtual machine, as on colima, that machine must share the directory too. Otherwise the Orker refuses to boot with `WORKDIR_NOT_BINDABLE`.
- **One `ORK_ORKER_ID` per Orker,** unique across the whole deployment, not only on one host.
- **`init: true` and a longer stop grace.** A stopped Orker hands its tasks back, and each gets 30 seconds before it is killed. `stop_grace_period: 45s` leaves time for that.

## Health checks

`ork --healthcheck` and `orker --healthcheck` ask the process's own admin listener whether it is ready. They start nothing and exit `0` when ready, so `ORK_ADMIN_ADDR` must be set in the container. Readiness is set once at boot and cleared once at shutdown. A Temporal or storage outage after boot does not make the process unready.

An Orker's boot runs a probe container. On an overloaded daemon the boot can take about 18 minutes at the defaults. Set `start_period` to cover the boot you expect, or a supervisor marks the Orker unhealthy before it can say why. The reason appears in the Orker's log.

## What you choose

- **The release.** Use the version tag or the digest, and the same `ORK_DEPLOYMENT_NAME` and `ORK_BUILD_ID` on every `ork` and `orker`. Leaving `ORK_BUILD_ID` unset turns versioning off. Setting it turns versioning on, and then the first versioned release needs one call: `orkctl release promote 0.3.1`. Until that call, submitted runs wait. [Upgrades](/docs/deploy/upgrades/) covers the rest.
- **Activities.** `uses: shell` resolves to `ghcr.io/orkstack/activity-shell:0.3`. That tag moves with each patch release. To pin an exact release, or to use a mirror, set `ORK_ACTIVITY_CATALOG` on `ork`. See [Configuration](/docs/configuration/#activities).
- **Temporal.** The address, and one namespace per environment. See [Requirements](/docs/deploy/requirements/).
- **The bucket.** Its endpoint, region and path style. Every `ork` and `orker` must share it.
- **The pool.** Set `ORK_ORKER_POOL` on the Orkers that join a pool. Leave it unset on `ork` unless you change the default pool. See [Pools, Orkers and pins](/docs/operate/pools-and-orkers/).
- **The work directory and task limit.** Set `ORK_ORKER_WORKDIR` to a path your host can bind-mount, and size `ORK_ORKER_MAX_CONCURRENT` to the host.
- **Docker or Podman, or Kubernetes.** See [Requirements](/docs/deploy/requirements/) and [Production on Kubernetes](/docs/deploy/kubernetes/).

## See also

- [DEPLOY.md, "Running the published images"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-the-published-images): each image, its user and the settings it needs
- [DEPLOY.md, "Health checks"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#health-checks): what ready means, and how long a boot can take
- [DEPLOY.md, "Running M Orker pools"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#running-m-orker-pools): how a process joins a pool
- [UPGRADING.md, "Release artifacts"](https://github.com/orkstack/orkstack/blob/main/docs/UPGRADING.md#release-artifacts): what each release publishes and how to verify it
