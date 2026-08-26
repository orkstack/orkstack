---
title: "OCI image digest pinning and container isolation"
date: 2026-08-26
updated: 2026-08-26
status: research-notes
scope: "Docker/OCI digest resolution, registry authentication and caching, runtime isolation, signals, signing, and BuildKit"
sources: "Official OCI, Docker, cloud-registry, Sigstore, Podman, crane, and skopeo documentation"
---

# OCI image digest pinning and container isolation

## Executive conclusions

- Resolve a mutable tag at the registry, without downloading layers, and store an immutable reference of the form `registry/repository@sha256:<manifest-digest>`. For a multi-platform release, store the **image index / manifest-list digest** if the intent is to preserve the complete multi-architecture release and normal platform selection; optionally store the selected platform-manifest digest as an audit field. Store only the child manifest digest when the artifact is deliberately platform-specific.
- Prefer `docker buildx imagetools inspect IMAGE:TAG` or `crane digest IMAGE:TAG`. A registry `HEAD /v2/<name>/manifests/<tag>` is the protocol-level solution. `docker manifest inspect --verbose` is useful for finding platform-child digests, but for a multi-platform tag it does **not reliably print the parent index digest**. `skopeo inspect --raw` returns exact manifest bytes; hash those bytes (prefer `skopeo manifest-digest`) or use ordinary `skopeo inspect` and its `Digest` field.
- Digest pinning provides content identity/integrity, not publisher identity. Add `cosign verify` and, where applicable, admission policy.
- Treat `--privileged`, host namespace flags, and access to `/var/run/docker.sock` as intentional isolation breaks. A sensible hardened baseline is non-root/rootless where possible, `--cap-drop=ALL` plus minimal additions, `--security-opt no-new-privileges`, the default or stricter seccomp profile, `--read-only` plus tightly scoped `--tmpfs`, a private/none network, and CPU/memory/PID/ulimit bounds.

## 1. Resolving and pinning image digests without pulling layers

### Recommended commands

```sh
# Best Docker UX: shows the registry object and top-level Digest, then children.
docker buildx imagetools inspect registry.example/repo/image:tag

# Registry inspection; for a multi-platform tag, -v is most useful for CHILD digests.
docker manifest inspect --verbose registry.example/repo/image:tag

# Daemonless, prints the digest directly.
crane digest registry.example/repo/image:tag
crane digest --full-ref registry.example/repo/image:tag

# Daemonless: preserve the registry's exact bytes, then compute their digest.
skopeo inspect --raw docker://registry.example/repo/image:tag > manifest.json
skopeo manifest-digest manifest.json
# Ordinary `skopeo inspect ... | jq -r .Digest` is simpler when raw bytes are not needed.

# Protocol-level (auth and Accept headers omitted here for clarity).
curl -fsSI \
  -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
  https://registry.example/v2/repo/image/manifests/tag \
  | awk -F': ' 'tolower($1)=="docker-content-digest" {print $2}'
```

**Authoritative references:**

- [`docker buildx imagetools inspect`](https://docs.docker.com/reference/cli/docker/buildx/imagetools/inspect/) “shows details of an image in the registry”; its output has top-level `Name`, `MediaType`, and `Digest`, followed by `Manifests` for an index.
- [`docker manifest inspect`](https://docs.docker.com/reference/cli/docker/manifest/inspect/) displays an image manifest or manifest list; exact options are `-v, --verbose` and `--insecure`. Important limitation: for a manifest list, verbose output exposes each child `Ref`/`Descriptor.digest`; it is not a dependable way to obtain the parent list digest. Do not hash its pretty-printed/re-serialized JSON.
- Crane’s pinned source documentation defines `crane digest IMAGE [flags]`, `--full-ref`, and `--platform` ([source at commit `3f4ff3c…`, lines 1–23](https://github.com/google/go-containerregistry/blob/3f4ff3ca60f51c876ac4bd89e9a787157aaa4f15/cmd/crane/doc/crane_digest.md#L1-L23)):
  ```text
  Get the digest of an image
  crane digest IMAGE [flags]
  --full-ref ... print the full image reference by digest
  --platform platform ... (default all)
  ```
- Skopeo’s pinned manual says `--raw` outputs raw manifest/config data and supports `--authfile`, `--creds`, `--no-creds`, and `--registry-token` ([source at commit `8d7fe24…`, lines 22–29 and 39–68](https://github.com/containers/skopeo/blob/8d7fe24891aacac474a7f6ac58c83d2fc6545a86/docs/skopeo-inspect.1.md#L22-L68)). Its companion command computes the digest of a saved manifest ([same commit, lines 3–23](https://github.com/containers/skopeo/blob/8d7fe24891aacac474a7f6ac58c83d2fc6545a86/docs/skopeo-manifest-digest.1.md#L3-L23)):
  ```text
  skopeo-manifest-digest - Compute a manifest digest for a manifest-file
  skopeo manifest-digest manifest-file
  ```

### What the registry protocol guarantees

The OCI Distribution Spec defines `GET /v2/<name>/manifests/<tag-or-digest>`, recommends an `Accept` header, requires `200 OK`, and requires `Docker-Content-Digest` on success. It also says clients using a digest reference should verify the returned manifest ([OCI Distribution Spec at commit `fee2119…`, lines 141–177](https://github.com/opencontainers/distribution-spec/blob/fee21197eb94360ddfa6dda0b7edabcd12456809/spec.md#L141-L177)):

```text
To pull a manifest, perform a GET request ...
/v2/<name>/manifests/<tag-or-digest>
The client SHOULD include an Accept header ...
A successful response MUST contain ... Docker-Content-Digest.
If the <tag-or-digest> ... is a digest, clients SHOULD verify ... matches this digest.
```

For existence/digest discovery without a response body, the spec requires an existing manifest’s `HEAD` to return `200 OK`, `Docker-Content-Digest`, and `Content-Length` ([same commit, lines 200–214](https://github.com/opencontainers/distribution-spec/blob/fee21197eb94360ddfa6dda0b7edabcd12456809/spec.md#L200-L214)); the endpoint table explicitly permits both `GET` and `HEAD` ([lines 837–844](https://github.com/opencontainers/distribution-spec/blob/fee21197eb94360ddfa6dda0b7edabcd12456809/spec.md#L837-L844)).

Keep the same explicit `Accept` values when resolving and later pulling: content negotiation can select an OCI index, Docker manifest list, or a single manifest. If using `GET`, independently hash the **exact response bytes** and compare with `Docker-Content-Digest`; the spec explicitly requires that verification when a client uses the header.

### `repo@sha256:…` semantics and verification

`name@sha256:...` is a repository-qualified digest reference: the repository controls authorization/location, while the digest identifies the exact manifest (or index) bytes. It is not a tag and does not move when tags are updated. Docker’s [`docker image pull`](https://docs.docker.com/reference/cli/docker/image/pull/#pull-an-image-by-digest-immutable-identifier) documents `NAME[:TAG|@DIGEST]` and calls a digest an “immutable identifier.” Docker’s [image-digest documentation](https://docs.docker.com/dhi/core-concepts/digests/) states that pulling by digest ensures the exact image version is pulled. Thus:

```sh
docker pull registry.example/repo/image@sha256:<digest>
```

requests that exact object and verifies content against the digest; referenced configs/layers and index children are themselves content-addressed by descriptors. A mismatch fails rather than silently accepting different bytes. A digest pin proves content integrity/identity, but **does not prove who published it**.

## 2. Index/list digest vs platform-manifest digest; `RepoDigests`

An OCI **image index** (Docker term: **manifest list**) is a higher-level manifest containing descriptors for child manifests, normally one per OS/architecture/variant. The OCI Image Spec says exactly that and requires its `manifests` list ([Image Spec at commit `af26a05…`, lines 1–4 and 29–55](https://github.com/opencontainers/image-spec/blob/af26a05fba5ee648512f4ea3c9fda1fcc1b6d6dc/image-index.md#L1-L55)):

```text
The image index is a higher-level manifest which points to specific image manifests,
ideal for one or more platforms.
... `manifests` ... list of manifests for specific platforms.
```

Consequences:

- **Index/list digest:** hashes the complete multi-platform index. It changes if any child descriptor, platform metadata, annotation, attestation descriptor, or ordering/bytes in that index changes. Pulling `repo@<index-digest>` lets the client select the matching platform, whose child digest is then verified.
- **Platform-manifest digest:** hashes one child manifest (its config/layer descriptors). It pins only that platform. It is appropriate for a platform-locked runtime but not as the sole identity of a multi-arch release.
- **What to record:** record the **index/list digest** for immutability of the published multi-arch release across architectures. For auditability, record both `indexDigest` and the actually selected `platformDigest` plus `os/arch/variant`. If your product intentionally supports only one platform, record/use the child digest.

`docker image inspect` is local-store inspection. Its `.Id` is the digest of image **configuration**, not a repository manifest digest. `.RepoDigests` is a list of `repository@digest` references for locally available image manifests that reference the image; it normally appears after pull or push and multiple registry manifests can point to the same config. This is defined by the Moby Engine API ([commit `40baece…`, lines 1814–1824](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/api/docs/v1.52.yaml#L1814-L1824) and [1870–1884](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/api/docs/v1.52.yaml#L1870-L1884)):

```text
ID ... digest calculated from the image's configuration ...
This identifier differs from RepoDigests ... digests of image manifests ...
RepoDigests: List of content-addressable digests of locally available image manifests
that the image is referenced from.
```

For Docker’s classic single-platform local image store, `RepoDigests` commonly contains the **selected platform manifest**’s repository digest, even when pull output displayed the parent list digest; therefore do not use it as the canonical multi-arch release digest. Newer containerd-backed multi-platform stores additionally expose an experimental `.Descriptor` that points to the OCI index/manifest list and `.Manifests` child summaries ([same API, lines 1827–1849](https://github.com/moby/moby/blob/40baece19edb76aad9f1db0c1e615a59c19f3c01/api/docs/v1.52.yaml#L1827-L1849)). Prefer registry inspection for the canonical index digest.

## 3. Registry authentication and what a daemonless resolver needs

### Docker credential configuration

[`docker login`](https://docs.docker.com/reference/cli/docker/login/) stores configuration at `$HOME/.docker/config.json` on Linux (`%USERPROFILE%/.docker/config.json` on Windows). Without a credential store, the `auths` values are only base64-encoded, not encrypted. Prefer:

- `credsStore`: one global `docker-credential-<suffix>` program (for example `osxkeychain`, `wincred`, `pass`, `secretservice`).
- `credHelpers`: per-registry mapping to `docker-credential-<suffix>`; it takes precedence for that host.
- `docker login --username USER --password-stdin HOST` for noninteractive token input without shell-history leakage.

A daemonless resolver must implement or reuse all of the **client-side** pieces normally hidden by Docker: canonicalize registry/repository names; TLS and custom CA/insecure policy; load `DOCKER_CONFIG`/`~/.docker/config.json`; decode `auths`; invoke the exact configured helper’s `get` protocol; send an initial anonymous request; parse `401` `WWW-Authenticate`; request a bearer token using the challenge’s `realm`, `service`, and repository `scope=repository:<name>:pull`; retry with `Authorization: Bearer`; set compatible manifest `Accept` types; follow redirects; and verify the returned/header digest. The official [Distribution token flow](https://distribution.github.io/distribution/spec/auth/token/) specifies 401 challenge → auth-service bearer token → retry, and explicitly allows an anonymous identity for public pulls.

Do not assume public means “no token exchange”: a public registry may issue an **anonymous bearer token**. Also plan for HTTP `429`, `RateLimit-*`/vendor headers, token expiry/refresh, and least-privilege pull-only credentials.

### Provider flows

- **Amazon ECR:** official [ECR private-registry auth](https://docs.aws.amazon.com/AmazonECR/latest/userguide/registry_auth.html) recommends its credential helper or `aws ecr get-login-password --region REGION | docker login --username AWS --password-stdin ACCOUNT.dkr.ecr.REGION.amazonaws.com`. The authorization token has the IAM principal’s scope and is valid **12 hours**. Direct Registry API calls send the base64 ECR token as HTTP Basic on every request.
- **Google Artifact Registry / GCR ecosystem:** official [Artifact Registry authentication](https://cloud.google.com/artifact-registry/docs/docker/authentication) supports `gcloud auth configure-docker HOSTS`, the standalone `docker-credential-gcr` helper using Application Default Credentials, or a short-lived access token: `gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://LOCATION-docker.pkg.dev`. Access tokens are valid **60 minutes**; service-account JSON keys are the least-preferred long-lived option.
- **GitHub Container Registry (GHCR):** [GitHub’s container-registry docs](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry) allow anonymous access to public images. Private pulls need a classic PAT with `read:packages`, or `GITHUB_TOKEN` in Actions when the repository/package grants access: `echo "$CR_PAT" | docker login ghcr.io -u USER --password-stdin`.
- **Docker Hub limits (current 2026-08-26):** [Docker Hub usage and limits](https://docs.docker.com/docker-hub/usage/) lists 100 pulls/6 hours per IPv4 or IPv6 `/64` for unauthenticated clients, 200/6 hours for authenticated Personal users, and unlimited pulls (subject to fair use) for authenticated Pro/Team/Business. A separate adaptive abuse limit, roughly thousands of requests/minute, returns plain `429` and applies to all tiers. Since limits can change, do not hard-code them as permanent policy.

## 4. Pull-through caches / registry mirrors

The Distribution project’s official [pull-through-cache recipe](https://distribution.github.io/distribution/recipes/mirror/) configures the Registry (historically the `registry:2` image; current project branding is CNCF Distribution) with:

```yaml
proxy:
  remoteurl: https://registry-1.docker.io
  username: optional-user
  password: optional-secret
  ttl: 168h
```

The first request is fetched and cached; later requests are local. Tag pulls re-check upstream freshness. Use filesystem storage for correctness/performance and enable deletion for cleanup. One cache instance mirrors only one upstream. Credentials can expose every private repository that account can read, so authenticate/authorize the mirror itself.

Configure Docker Engine either with the exact daemon flag:

```sh
dockerd --registry-mirror=https://mirror.company.example
```

or persistent `/etc/docker/daemon.json`:

```json
{"registry-mirrors":["https://mirror.company.example"]}
```

Docker’s documented daemon mirror support is specifically for Docker Hub, mirror URLs must be a domain root (no path), and Hub fair-use/rate policies still apply.

## 5. Isolation flags: exact names and semantics

Primary references are Docker’s [`docker container run` reference](https://docs.docker.com/reference/cli/docker/container/run/), [running-containers security/capability guide](https://docs.docker.com/engine/containers/run/), [seccomp guide](https://docs.docker.com/engine/security/seccomp/), [resource constraints](https://docs.docker.com/engine/containers/resource_constraints/), [tmpfs mounts](https://docs.docker.com/engine/storage/tmpfs/), and [user namespace remapping](https://docs.docker.com/engine/security/userns-remap/).

### Explicit isolation breaks

| Exact flag / mount | Semantics and risk |
|---|---|
| `--privileged` | Enables all Linux capabilities; disables default seccomp and AppArmor; disables SELinux process label; grants all host devices; makes `/sys` and cgroup mounts writable. Docker warns it is not securely sandboxed and can obtain a host root shell. Prefer narrowly scoped flags. |
| `--pid=host` | Uses host PID namespace: container can see host processes/PIDs; destroys process-visibility isolation. Default is a private PID namespace. |
| `--network=host` | Shares host network namespace; no container IP isolation/NAT and published-port flags are ignored. Container can bind host ports and observe the host stack subject to other permissions. See [host network driver](https://docs.docker.com/engine/network/drivers/host/). |
| `--ipc=host` | Uses host System V/POSIX IPC namespace, exposing host shared-memory segments, semaphores, and message queues. |
| `--uts=host` | Uses host UTS namespace (hostname/domain); Docker disallows combining it with `--hostname`/`--domainname` because a container could alter host configuration. |
| `-v /var/run/docker.sock:/var/run/docker.sock` (or `--mount type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock`) | Gives the container control of the rootful Docker API. Because that API can create privileged containers and bind-mount host `/`, this is effectively host-root-equivalent authority, not ordinary file access. Docker’s [Engine security guide](https://docs.docker.com/engine/security/) says only trusted users may control the daemon and that daemon API access can readily cause privilege escalation. Do not expose it to untrusted workloads. |

### Restrictive controls

| Exact flag | Semantics / caveat |
|---|---|
| `--network=none` | Connects only to the `none` network: no external networking; the container retains loopback. See Docker’s [`none` network driver](https://docs.docker.com/engine/network/drivers/none/). |
| `--cap-add CAP` | Adds named Linux kernel capabilities (Docker accepts names with or without `CAP_`). Add only those proven necessary. |
| `--cap-drop=ALL` | Drops every capability; pair with minimal repeated `--cap-add`. Docker’s [security guide](https://docs.docker.com/engine/security/) calls removing all except explicitly required capabilities best practice. |
| `--security-opt no-new-privileges` (equivalent explicit value `no-new-privileges=true`) | Prevents processes from gaining new privileges (for example through setuid/setgid binaries); Docker notes `su`/`sudo` privilege elevation then fails. |
| `--security-opt seccomp=/path/profile.json` | Loads the named JSON seccomp allowlist; `seccomp=builtin` selects Docker’s built-in default and `seccomp=unconfined` disables it. The [default profile](https://docs.docker.com/engine/security/seccomp/) is an allowlist that denies roughly 44 of 300+ syscalls and Docker recommends not disabling it. |
| `--read-only` | Mounts the container root filesystem read-only. Volumes/bind mounts/tmpfs can still be writable, so explicitly constrain them. |
| `--tmpfs /path:rw,noexec,nosuid,size=64m` | Mounts ephemeral host-memory tmpfs at a needed writable path; data is lost when stopped/removed and may be swapped unless host swap is constrained. Prefer `noexec,nosuid` where workable. |
| `--user UID[:GID]` / `-u` | Overrides image `USER` with a username/UID and optional group/GID. Use a nonzero UID and ensure mounted-file permissions work; this is distinct from user-namespace mapping. |
| `--userns=host` | **Only valid Docker run value.** If daemon userns-remap is enabled, this disables remapping for this container and uses host user namespace—less isolation. Enable remapping at the daemon rather than assuming this flag enables it. |

### Rootless engines

Docker [Rootless mode](https://docs.docker.com/engine/security/rootless/) runs both daemon and containers as a non-root user inside a user namespace, reducing impact of daemon/runtime vulnerabilities (with feature/network/cgroup prerequisites and limitations). Podman is daemonless and supports rootless operation; its [`podman(1)` docs](https://docs.podman.io/en/latest/markdown/podman.1.html#rootless-mode) explain that rootless containers use a user namespace and cannot have more privileges than the launching account. Rootless is defense in depth, not permission to use `--privileged`, host namespaces, unsafe mounts, or untrusted images casually.

### Resource / denial-of-service controls

| Exact flag | Meaning |
|---|---|
| `--cpus=N` | Hard CFS quota expressed in CPU count; e.g. `1.5` is equivalent to `--cpu-period=100000 --cpu-quota=150000`. |
| `--cpu-shares=N` / `-c N` | Relative scheduling weight (default 1024), enforced only under CPU contention; **soft priority**, not a ceiling/reservation. |
| `--memory=SIZE` / `-m SIZE` | Hard memory limit (documented Linux minimum 6 MiB). |
| `--memory-swap=SIZE` | Total memory **plus swap**, meaningful with `--memory`. Equal values disable swap; `-1` allows unlimited host swap; unset commonly permits swap equal to memory if host swap exists. |
| `--pids-limit=N` | Caps the container’s process count to limit fork bombs; `-1` means unlimited. |
| `--ulimit type=soft[:hard]` | Sets per-container process rlimits (for example `--ulimit nofile=1024:1024`); if hard is omitted, soft is used for both; otherwise daemon defaults are inherited. |

Set hard CPU/memory/PID limits for untrusted or multi-tenant work. Docker emphasizes that containers have **no resource constraints by default**. Platform/kernel/cgroup version support matters, especially for swap, shares, and rootless cgroups.

## 6. Signals, graceful stop, and PID 1

Current [`docker stop`](https://docs.docker.com/reference/cli/docker/container/stop/) sends the configured stop signal to the container’s main process—`SIGTERM` if none is configured—waits, then sends `SIGKILL`. The current exact timeout option is `-t, --timeout SECONDS`; `-1` waits indefinitely. Linux default is 10 seconds, Windows default 30 seconds when no container default is configured.

`--time` still exists as a **deprecated compatibility alias** in current Docker CLI source; use `--timeout`. The pinned implementation says ([Docker CLI commit `3f096b4…`, lines 48–52](https://github.com/docker/cli/blob/3f096b4ffd8e30329221c583db6dfc9279673b96/cli/command/container/stop.go#L48-L52)):

```go
flags.IntVarP(&opts.timeout, "timeout", "t", 0, "Seconds to wait...")
flags.IntVar(&opts.timeout, "time", 0, "... (deprecated: use --timeout)")
flags.MarkDeprecated("time", "use --timeout instead")
```

Signal precedence/configuration:

- Dockerfile [`STOPSIGNAL`](https://docs.docker.com/reference/dockerfile/#stopsignal) sets the image stop signal (`SIGTERM` by default if unset).
- `docker run --stop-signal SIGNAL` overrides the image for the created container.
- `docker run --stop-timeout SECONDS` sets that container’s default stop timeout.
- `docker stop --signal SIGNAL` / `-s` selects the signal for this stop invocation; `docker stop --timeout N` / `-t N` selects the grace period.

### PID 1 and entrypoint form

Use Dockerfile exec form:

```dockerfile
ENTRYPOINT ["/app/server"]
CMD ["--serve"]
```

not shell form `ENTRYPOINT /app/server`. Docker’s [`JSONArgsRecommended` check](https://docs.docker.com/reference/build-checks/json-args-recommended/) explains that shell form runs the executable as a child of a shell that does not pass signals, so the application may not receive `SIGTERM`. If a wrapper script is necessary, end it with `exec "$@"` (or `exec /app/server ...`) so the application replaces the shell as PID 1.

PID 1 must also reap orphaned/zombie children. `docker run --init` inserts a small init as PID 1 that forwards signals and reaps children ([run reference](https://docs.docker.com/reference/cli/docker/container/run/#specify-an-init-process)); Docker uses `tini` as its default init. `--init` helps process lifecycle but does not compensate for an application that ignores its stop signal.

## 7. Signed images and policy

Digest pins prevent substitution after resolution but do not establish publisher identity. Sigstore’s official [Cosign verification guide](https://docs.sigstore.dev/cosign/verifying/verify/) gives:

```sh
# Public-key verification
cosign verify --key cosign.pub registry.example/repo/image@sha256:<digest>

# Keyless identity verification
cosign verify \
  --certificate-identity='EXPECTED_IDENTITY' \
  --certificate-oidc-issuer='https://EXPECTED_ISSUER' \
  registry.example/repo/image@sha256:<digest>
```

Cosign signature payloads include the image digest and `cosign verify` checks the digest by default. Do not use `--check-claims=false` in normal policy, and constrain keyless issuer **and** identity rather than merely accepting any valid Sigstore certificate.

Sigstore [policy-controller](https://docs.sigstore.dev/policy-controller/overview/) is a Kubernetes admission controller that validates signatures/attestations via `ClusterImagePolicy`; it also resolves tags so the image run cannot differ from the image admitted. Enforcement is namespace-configurable and the project documentation still labels the component actively developed, so pin/test versions and choose deliberate no-match and warn/deny behavior.

Docker Content Trust (Notary v1) is not a forward-looking choice: Docker’s [DCT documentation](https://docs.docker.com/engine/security/trust/) now states it **is being retired** and that `notary.docker.io` will shut down on **2026-12-08**. Migrate to current signing/policy mechanisms such as Sigstore/Cosign (or another actively supported Notary v2/Notation ecosystem where required).

## 8. On-demand local builds with Docker/BuildKit and obtaining IDs/digests

Docker’s [build-context documentation](https://docs.docker.com/build/building/context/) supports a local directory, Git repository URL, remote tarball, remote Dockerfile, or stdin. Examples:

```sh
# Local Dockerfile and context; load the single-platform result locally.
docker buildx build --load -t local/app:build .

# Legacy fragment syntax requested: branch/tag/full commit and optional subdirectory.
docker build 'https://github.com/user/repo.git#ref:subdir'

# Current structured syntax (recommended; Buildx >=0.28.0 / Dockerfile >=1.18.0).
docker buildx build \
  'https://github.com/user/repo.git?branch=ref&subdir=subdir&checksum=<commit>'
```

Git contexts are shallow-cloned with submodules. Fragment syntax is `#ref:dir`; a commit used as `ref` must be full length. Structured URL queries are recommended because `checksum=`/`commit=` can verify that a branch/tag/ref resolves to the expected commit. For private Git, use `--ssh` or `--secret`, **not `--build-arg`** for credentials.

Obtain results using exact BuildKit flags documented by [`docker buildx build`](https://docs.docker.com/reference/cli/docker/buildx/build/):

```sh
# Local image/config ID (not necessarily a registry manifest digest).
docker buildx build --load -t local/app:build --iidfile image-id.txt .
docker image inspect --format '{{.Id}}' local/app:build
# Or: docker buildx build --quiet ...  # prints image ID on success

# Registry manifest/index digest, robust for pushed/multi-platform output.
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --push -t registry.example/repo/app:build \
  --metadata-file build-metadata.json .
jq -r '.["containerimage.digest"]' build-metadata.json
# Independently confirm the pushed tag:
docker buildx imagetools inspect registry.example/repo/app:build
```

`--iidfile`/`--quiet` returns an image ID (configuration/content identity for a local image). `--metadata-file` is explicitly documented to write result metadata “such as the image digest”; with registry export (`--push`, shorthand for `--output=type=registry`) `containerimage.digest` is the pushed manifest or index digest. Do not conflate local `.Id` with a distributable repository manifest digest. If immutability matters, push to a registry/OCI exporter, capture the exporter digest, independently inspect it, and deploy `repo@digest`.

## Conflicts, version boundaries, and limitations

- As of 2026-08-26, Docker’s documented stop option is `--timeout`; `--time` is deprecated but retained in current CLI source. Older articles often call it `--time`.
- Docker Engine behavior differs between the classic single-platform image store and the containerd-backed multi-platform store. In particular, `RepoDigests` should not be assumed to equal a parent index digest.
- A tag can move between resolution and use. Resolve once, immediately persist the digest, and use only the digest reference afterward; admission-time tag-to-digest mutation is another defense.
- Manifest media-type negotiation can alter which object/digest is returned. Send an explicit `Accept` list and preserve/verify exact response bytes.
- Registry auth, rate limits, credential lifetimes, and cloud CLI commands are provider policy and can change. The values above are current to the note date.
- Isolation flags are Linux-centric; Docker Desktop adds a VM boundary, and Windows container isolation/resource semantics differ.
