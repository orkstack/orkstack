# Security model

This document states OrkStack's security model in one place. Each rule names
the code or the contract that enforces it, and each one describes this branch as
it is. It is a description of what is enforced, not an audit. Nothing here makes
OrkStack production-ready, and the bar for that is set
in [AGENTS.md](../AGENTS.md), §11.

The engineering contract behind this model is [AGENTS.md](../AGENTS.md) §2.3 (the
Orker is the security boundary), §6 (security and resource controls) and §15
(the product invariants). The container profile is specified in
[OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md), "Security".

```text
caller ──> gateway (authenticates; asserts Ork-Principal) ──> ork API port
                                                                │ Connect RPCs and the web UI; OrkStack does not authenticate them
bearer holder ──> trigger invoke, await callback (plain HTTP) ──┤ checked against a salted digest
                                                                │
ork ──> Temporal ──> orker ──> OCI runtime ──> container        │ the container holds no platform credential
                       └──> object storage (ork and orker hold its credentials)
ork ──> signed POST ──> subscriber URL (webhooks)
```

## Who holds what

| Component | Holds | Never holds |
| --- | --- | --- |
| `ork` (engine) | Temporal and object-storage credentials; a read-only registry credential, used for registry GETs | a push credential; any credential a container receives |
| `orker` | Temporal and object-storage credentials; registry pull and signature-read credentials; the container runtime | any path inside a container: the registry auth file is never mounted |
| Container | its rendered inputs, its resolved secret files, and the task's own `env:` map | any platform credential |
| Object store | artifacts, run records, audit and delivery records, webhook secrets, bearer-token digests | the plaintext of a trigger or await token |

The table follows [OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md), §1
("Ownership: who knows what"). The registry credentials are described in §4.5
and §4.7. `ORK_REGISTRY_AUTH_FILE` grants no push and no execution
(`internal/config/config.go`). `ORK_ORKER_REGISTRY_AUTH_FILE` is never mounted into
a container (`internal/orker/security_profile.go`).

## The Orker is the security boundary

An activity container receives no Temporal, object-storage or control-plane
credential. The Orker is the only process that talks to the container runtime,
and it re-checks the policies that govern each attempt at dispatch
([AGENTS.md](../AGENTS.md), §2.3). A container may be given a credential only
when a user supplies it explicitly, as a workload secret (see "Secrets" below).

The Orker's checks at dispatch are the image policy, the secret floor, the
resource caps and the network mode. It makes no control-plane read at dispatch.
The policy travels on the input, pinned at submit
([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md), §4.7).

### The Orker's own access to the host

The Orker starts every task's container through the container runtime: on a
host, the `docker` CLI and the daemon behind it. Whoever can use that daemon
can start a privileged container, so an Orker process is as trusted as root
on its host, whatever user it runs as, and an Orker host should run nothing
whose compromise through it you would not accept. The release's `orker` image
runs as root for that reason and says so; the quickstart mounts the Docker
socket into the `orker` service and into the step that pushes the first-party
activities to its registry, and into no other
([compose.yaml](../compose.yaml)), and a test refuses a compose file that
gives it to any other service. An activity
container never receives the socket, a host path or the host's namespaces
("What the Orker refuses" below).

On Kubernetes the daemon is not the node's: each Orker pod runs a Docker
engine of its own in a sidecar, and only the Orker container shares its
socket ([DEPLOY.md](DEPLOY.md), "Orkers on Kubernetes"). How much the node
trusts that engine depends on the shape. In a pod with its own user
namespace, the engine is root of that namespace, which is an unprivileged uid
range on the node. In the privileged fallback, the engine container is root
on the node, and so is anything that escapes from a task's container to it.
In both shapes the pod holds no service-account token, and the Orker's secret
floor admits only the files of one Secret of task secrets, mounted in the
Orker container alone. A task therefore cannot name the Orker's own settings,
its object-store keys or a token as a secret ("Which sources may be named",
below).

### What the Orker reads back

The boundary holds in the other direction too: nothing a workload leaves in
its output directory makes the Orker read outside it. The Orker reads the
output on the host, where a symlink resolves in the Orker's own mount
namespace, so a link the workload wrote could name any file the Orker can
open. It therefore reads `/orkstack/output` only through a confined root
(`os.Root`) opened on the attempt's output directory, `lstat`s every name it
reads before opening it, proves every open with a second `lstat` taken while
the descriptor is held, and reads each artifact only through the descriptor
its scan opened, so a name replaced afterwards is never resolved again. A
symlinked `values.json` fails `VALUES_MALFORMED`, and a symlinked or
non-directory `output/artifacts`, or any artifact entry that is not a regular
file, fails `ARTIFACT_NOT_A_FILE`. Both are contract violations, never
retried, and each names the container path and never a host path. The failure
path reads through the same root, so a failed attempt publishes nothing from
outside either. The Orker does not depend on the host's
`fs.protected_symlinks`, which a same-uid workload passes
([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md) §9.5;
`internal/orker/workspace.go`, `OutputTree`).

## What an activity container receives

The Orker renders the container's mounts from paths it created for the attempt
or, for a pinned task, for the pin (`internal/orker/workspace.go`, `Mounts`).
Nothing in an Orkflow can name a host path.

| Path in the container | Source | Access |
| --- | --- | --- |
| `/orkstack/input` | the attempt's rendered inputs, sealed before the container starts | read-only |
| `/orkstack/input/secrets` | the attempt's resolved secret files | read-only |
| `/orkstack/output` | the attempt's values and artifacts | read-write |
| `/orkstack/tmp` | the attempt's scratch | read-write |
| `/orkstack/shared` | the pin's shared directory | read-write, pinned tasks only |
| `/tmp` | a tmpfs, 64 MiB by default | read-write, `nosuid`, `nodev`, `noexec` |

- The root file system is read-only, `/run` and `/var/tmp` included, and the
  working directory is `/orkstack`.
- The environment is the image's own `ENV`, overridden by the task's `env:`
  map, plus what the container runtime adds (`HOSTNAME` under Docker). The
  platform adds no variable: required platform state travels as files
  ([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md) §3.7;
  `internal/orker/container_runtime.go`, `ContainerSpec.Env`).
- The container runs as the image's own user. It runs as the Orker's own user
  only when the task sets `run_as_orker_user: true`, or when the deployment sets
  `ORK_ORKER_FORCE_USER=true`, which defaults to off
  (`internal/orker/oci_dispatcher.go`; `internal/orker/security_profile.go`).
  The boot probe checks that this user lands as the Orker; where it does not,
  such a task fails `RUN_AS_ORKER_UNAVAILABLE` (`internal/orker/boot_probe.go`).
- Before it takes work, the Orker runs one container built as a task's is and
  checks from inside it that the runtime applied this posture, and cancels a
  second one as a task is cancelled to check that its workload hears
  SIGTERM; an Orker whose runtime does not refuses to start with
  `RUNTIME_POSTURE_REFUSED` ([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md) §3.1).

## What the Orker refuses

### Forbidden without exception

The Orker refuses to construct any of the following, and no configuration key or
task field expresses any of them ([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md),
§9.1). `ContainerSpec` has no field for them, and the argument builder
`dockerCreateArgs` (`internal/orker/runtime_docker.go`) has no argument for them:

- `--privileged`, any host namespace (PID, IPC, UTS, network, user), any added
  capability, any device, and any relaxed seccomp, AppArmor or SELinux setting;
- any host bind mount: every mount source is an absolute path the Orker created;
- the Docker socket, as a source or as a target (`forbiddenMountTargets`);
- a network mode other than `default` or `none`.

`dockerCreateArgs` also refuses a spec that weakens the mandatory profile, or
that leaves a resource unbounded. A request for a network mode the Orker does not
offer is refused as `NETWORK_MODE_REFUSED` (`internal/orker/security_profile.go`,
`ResolveNetwork`).

### The mandatory profile

Every container is created with the following, and the Orker cannot be configured
to omit them ([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md), §9.2):

| Flag | Effect |
| --- | --- |
| `--read-only` | the root file system cannot be written |
| `--cap-drop=ALL` | no Linux capability is kept |
| `--security-opt no-new-privileges` | a process cannot gain privilege it did not start with |
| `--security-opt seccomp=builtin` | the runtime's default seccomp profile, named explicitly; on the Podman engine, which has no name for its default, nothing is spelled and the engine applies its own, which the boot probe checks is a filter |
| `--init` | PID 1 reaps zombies and forwards signals |
| `--pids-limit` | 512 by default |
| `--cpus`, `--memory` | always set, never unlimited |
| `--network` | always explicit: `default` or `none` |

### Network modes

Exactly two modes exist. `none` gives the container no network (`--network none`).
`default` leaves the runtime's default network in place. OrkStack filters no
egress from either. Named, Orker-defined network profiles are deliberately absent,
and an Orkflow cannot define host networking (`AGENTS.md` §6). The Orker adds no
`--add-host` entry and no host-network configuration to an activity container
(`AGENTS.md` §11.1).

### Resource policy

A task may request `resources:`. Orker policy supplies the defaults and the caps
(`internal/orker/security_profile.go`):

| Setting | Default | Variable |
| --- | --- | --- |
| CPU when a task requests none | `1` | `ORK_ORKER_DEFAULT_CPU` |
| Memory when a task requests none | `1Gi` | `ORK_ORKER_DEFAULT_MEMORY` |
| CPU cap | `8` | `ORK_ORKER_MAX_CPU` |
| Memory cap | `16Gi` | `ORK_ORKER_MAX_MEMORY` |
| Process limit | `512` | `ORK_ORKER_PIDS_LIMIT` |
| Size of the `/tmp` tmpfs | `64 MiB` | `ORK_ORKER_TMPFS_BYTES` |

- A request above a cap is refused as `RESOURCE_REQUEST_REFUSED`. It is never
  clamped, because a workload that asked for more and got less fails later and
  confusingly.
- A zero, negative or non-finite request is refused, because the runtime reads a
  zero limit as unlimited.
- An unreadable cap refuses the request rather than leaving it uncapped. A policy
  that cannot be read stops the Orker at boot (`SecurityPolicy.Validate`).
- No workload receives unlimited resources, including one that requested nothing.

Both refusals are `ContractViolation` and are never retried.

## Image policy

By default, a `uses:` is pulled and run. The engine pins it to a digest at apply,
so a tag that moves cannot change a running execution. The digest answers "did
the image change under us?". It does not answer "should this image run here?".
The image policy answers the second question
([DEPLOY.md](DEPLOY.md), "Image supply-chain policy").

- **The deployment floor** is written by `orkctl admin image-policy set`. It can
  require digests (`--require-digest`), restrict registries to literal prefixes
  (`--allowed-registry`), and register cosign public keys by name
  (`--signing-key NAME=PATH`). OrkStack never holds a private key.
- **The namespace tightens it.** `orkctl namespace update NAME --require-signed-by
  cosign:KEY` requires a valid signature by that key on every image the namespace
  runs. A namespace has no field that loosens the floor.

The policy is checked at three doors: at validate and apply, at every submit before
a run ID is minted, and by the Orker at dispatch, against the policy the run
pinned at submit. A run keeps the policy and the key it was submitted under. A
committed attempt is adopted without being judged again. The Orker verifies the
signature itself, with its own registry client
([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md), §4.7).

| Reason | Category | Retried |
| --- | --- | --- |
| `IMAGE_NOT_PERMITTED` | `ContractViolation` | no |
| `IMAGE_NOT_SIGNED` | `ContractViolation` | no |
| `IMAGE_SIGNATURE_INVALID` | `ContractViolation` | no |
| `IMAGE_SIGNATURE_UNAVAILABLE` | `InfrastructureError` | yes |

An unreachable registry is never a pass, and it is never "unsigned": it is
`IMAGE_SIGNATURE_UNAVAILABLE` (`internal/task/image_policy.go`).

The engine speaks plain HTTP to a registry only when the registry is a loopback
host. Every other registry is reached over HTTPS (`internal/dsl/reference.go`,
`Scheme`).

## Secrets

- A task declares a secret by name and source under `secrets:`. The name must match
  `^[A-Za-z0-9._-]{1,128}$`. The source is `env:NAME` (read from the Orker's
  environment) or `file:/path` (read from the Orker's file system). The definition
  holds the source, never the value (`proto/orkstack/v1/activity.proto`,
  `SecretRef`).
- The Orker resolves the value at dispatch. It writes the value to the attempt's
  secrets directory, then seals the files read-only (`0444`, in a directory that is
  `0555`). The directory is removed on every exit path, including a panic and a
  cancellation (`internal/orker/workspace.go`; `internal/orker/oci_dispatcher.go`).
- A secret value never appears in a proto message, in Temporal history, in the
  archive, in a log chunk or in `task.json`, which carries names and paths only
  ([OCI_ACTIVITY_CONTRACT.md](OCI_ACTIVITY_CONTRACT.md), §9.4).
- A cached task resolves no secret on a cache hit. A task that mounts a secret
  should therefore not be cached ([ACTIVITY_IMAGES.md](ACTIVITY_IMAGES.md)).

### Which sources may be named

A source must pass two independent lists. Each list names a source by scheme and
pattern, and `*` is the only wildcard. It does not cross `/`.

| List | Set by | Checked |
| --- | --- | --- |
| The Orker's floor | `ORK_ORKER_SECRET_ALLOW` (`orker.secret_allow`) | at dispatch, by the process that reads the value, before the image is pulled |
| The namespace's allow-list | `orkctl namespace update NAME --allow-secret PATTERN`, cleared by `--clear-allowed-secrets` | at apply, at every submit, and never retroactively |

- An empty floor resolves every source of an allowed scheme except the Orker's
  own (below), and the Orker logs a warning at boot that names
  `ORK_ORKER_SECRET_ALLOW`. The schemes an Orker resolves are `env` and `file`
  by default (`ORK_ORKER_SECRET_SCHEMES`).
- A source outside the floor, or one of the Orker's own, fails the task as
  `SECRET_NOT_PERMITTED`, a `ContractViolation`. An unknown scheme is
  `SECRET_SCHEME_FORBIDDEN`, and an unset variable or an unreadable file is
  `SECRET_UNRESOLVED`, an `InfrastructureError`.
- A `file:` source with a `..`, `.` or empty segment is refused outright. It is
  never normalised, so `file:/etc/orkstack/secrets/sandbox/../payments/db` never
  matches `file:/etc/orkstack/secrets/sandbox/*`.
- A namespace's list never travels to the Orker. The engine's submit gate is the
  one check no client can bypass ([DEPLOY.md](DEPLOY.md), "Secrets on a shared
  pool").
- A refusal names the source string and what refused it: the floor's entries,
  the path rule, or that the source is one of the Orker's own, in the same
  words for every variable and for every file, never which one. A source, a
  scheme, a variable or a secret name longer than the longest `file:` source
  (4101 bytes) is quoted by its first 64 bytes and its length, in every
  refusal that names one. Nothing stores, logs or compares a value.

### The Orker's own sources

A source is read from the Orker's own environment and file system, and part of
what is there belongs to the Orker. No floor admits any of it, whether
`ORK_ORKER_SECRET_ALLOW` is empty or not. No entry re-admits one: a broad entry
such as `env:*` still does not reach them. To give a task one of these values,
supply it under a source of the task's own, such as a file in a directory the
floor names ([AGENTS.md](../AGENTS.md), §2.3). A floor entry that can match
only the Orker's own sources stops the Orker at boot
(`internal/orker/security_profile.go`, `internal/orker/runtime_docker.go`).

| The Orker's own | Why it is the Orker's |
| --- | --- |
| `env:ORK_*` | every OrkStack setting, the object store's keys among them |
| `env:OTEL_*` | the telemetry exporters' settings; their headers carry a collector's credentials |
| `env:HTTP_PROXY`, `env:HTTPS_PROXY`, `env:ALL_PROXY`, `env:http_proxy`, `env:https_proxy`, `env:all_proxy` | the proxies the Orker's HTTP clients and its container CLI use; a proxy URL may carry a user and a password |
| `env:DOCKER_*`, `env:CONTAINERS_*`, `env:CONTAINER_HOST`, `env:CONTAINER_CONNECTION`, `env:CONTAINER_SSHKEY`, `env:CONTAINER_PASSPHRASE`, `env:REGISTRY_AUTH_FILE` | the settings of the container CLI the Orker drives, which runs with the Orker's environment: the daemon it reaches, its registry credentials, its TLS key, a remote engine's SSH identity |
| `env:AWS_*`, `env:AZURE_*`, `env:GOOGLE_APPLICATION_CREDENTIALS`, `env:CLOUDSDK_*` | the cloud credentials a registry credential helper (ecr-login, gcr, acr) reads when the CLI pulls an image, from the environment it inherits: the Orker's |
| each telemetry sink header's `env:` source (`ORK_TELEMETRY_SINKS`) | a collector's credentials |
| `file:/proc` | the Orker's process: its environment, memory and descriptors, and every other process it can see |
| `file:/dev` | its devices, `/dev/fd` and `/dev/stdin` among them, which are its own descriptors |
| `file:/run/secrets`, `file:/var/run/secrets` | the credentials of the Orker's own pod or container: a Kubernetes service-account or projected token, a Docker or Compose secret |
| its work root (`ORK_ORKER_WORKDIR`; unset, the temp directory, `/tmp` on most Linux hosts) | every attempt's workspace: other tasks' secrets, inputs and outputs, and every pin's shared directory |
| its configuration file (`--config`), and each `{file: PATH}` it reads | the Orker's settings and the values they name |
| each telemetry sink's header file (`ORK_TELEMETRY_SINKS`) | a collector's credentials |
| the files `ORK_ORKER_REGISTRY_AUTH_FILE`, `ORK_REGISTRY_AUTH_FILE` and `OTEL_EXPORTER_OTLP_*CERTIFICATE`, `*_CLIENT_KEY` name | registry credentials, and the exporters' TLS files |
| `ORK_ARTIFACT_DIR`, when the artifact backend is `local` | every run's artifacts, commits and logs, of every namespace |
| `DOCKER_CONFIG`, `DOCKER_CERT_PATH`, `REGISTRY_AUTH_FILE`, `CONTAINER_SSHKEY`, `~/.docker`, `~/.dockercfg`, `~/.ssh`, `~/.config/containers` (or `$XDG_CONFIG_HOME/containers`), `$XDG_RUNTIME_DIR/containers`, `/run/containers` | the container CLI's registry credentials, TLS key and SSH identity |
| the files and directories `AWS_SHARED_CREDENTIALS_FILE`, `AWS_CONFIG_FILE`, `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`, `GOOGLE_APPLICATION_CREDENTIALS`, `CLOUDSDK_CONFIG`, `CLOUDSDK_AUTH_ACCESS_TOKEN_FILE`, `CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE`, `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_CONFIG_DIR` and `AZURE_CLIENT_CERTIFICATE_PATH` name, `~/.aws`, `~/.azure`, `~/.config/gcloud` | the credential helpers' cloud credentials |
| `/var/lib/docker`, `/var/lib/containerd`, `/var/lib/containers`, `/run/docker`, `/run/containerd`, `~/.local/share/docker`, `~/.local/share/containers` (or under `$XDG_DATA_HOME`), and each directory the engine reports at boot: the docker CLI's `DockerRootDir` (dockerd's `data-root`), the podman CLI's `GraphRoot` and `RunRoot` (`graphroot` and `runroot` in `storage.conf`) | the container engine's state: images, layers, every container's writable layer and its log, other tasks' among them |

The engine's state is asked at boot, within 30 seconds, because its settings
can move it off those defaults, and a container's writable layer, which its
task writes, moves with it. Each directory the engine names is the Orker's
own from then on, so a floor entry that admits only what lies under a moved
data root stops the Orker at boot, as one under `/var/lib/docker` does. An
answer the Orker cannot hold is refused line by line: a field the engine
leaves unset, a relative path, a path holding a control character, and `/`,
which would make every file the Orker's own and every `file:` floor entry
refused. Whatever the engine could not say — it did not answer, did not
answer in time, named nothing the Orker can hold, or named something else
beside what it held — leaves the defaults in force: the Orker boots and logs
a warning that names the engine setting to check and `ORK_ORKER_SECRET_ALLOW`.
A runtime that cannot be asked at all logs the same warning. Neither CLI
reports containerd's own root, so a containerd root moved off
`/var/lib/containerd` is not on the list; an Orker on such a host sets a
floor (`internal/orker/runtime_docker.go`, `EngineState`).

A file is named by the path itself and everything beneath it. A `file:` source
must be a plain absolute path, with no `..`, `.` or empty segment, of at most
4096 bytes, under any floor, empty included: the kernel applies `..` after it
follows a link, so a name with one need not be the file read, and it is
refused as a floor refuses it; a longer name is one the kernel never opens
(`internal/dsl/secret_policy.go`, `CheckSecretPath`). A plain path is
followed as the kernel follows it to open it, one name at a time, a link that
leads nowhere yet included, and it is the Orker's own when its name, any
directory its way looks a name up in, or the file it ends at is one of these:
by name, or as the file system holds that path when the source is judged, the
same file or directory under any name. So no symbolic link reaches one of them
under another name; a link through `/proc`, such as one to a descriptor of the
Orker's, is the Orker's own wherever its text points; a way that passes
through one of the Orker's directories is refused even when a link's `..`
leads out of it again; a Kubernetes ConfigMap or Secret mounted as links into
a dated directory stays the Orker's own after an update swaps the links to a
new directory; and a hard link to one of the Orker's own files is that file.
Judging a name costs the Orker one look at each name its way passes through,
whatever the task wrote — two when the way is cut short at a name that cannot
be looked at, since it then follows the name once more — and up to three
looks at each of its own paths. A refusal says only that the source is one of
the Orker's files, or one of its variables, the same words for each, never
which one, which setting named it or where a link led. A task that tries
names can still tell which of them are the Orker's own, since those are
refused; what it cannot learn from a refusal is which rule, setting or path
made one so.

Paths are compared byte for byte, as a case-sensitive file system names them.
On a file system that folds case, `/PROC/self/environ` opens
`/proc/self/environ`, and the comparison does not see it. Every Linux root
file system is case-sensitive; an Orker whose work root or task-secret
directories sit on one that is not (a macOS default volume, some mounted
shares) should set a floor that lists only directories of the tasks' own.

Some ways of reaching a file are out of the check's reach, by design: a file
beneath one of the Orker's directories that a bind mount or a hard link shows
under another name, another mount of the process or device file system than
`/proc` and `/dev`, and a symbolic link swapped between the check and the
read. Each needs write access outside the task's workspace and its
container's writable layer, where no task writes — the workspace lies under
the work root and the layer under the engine's state, both the Orker's own —
and [AGENTS.md](../AGENTS.md) §6 forbids host mounts. A Kubernetes update
swaps a mount's links while the Orker runs, so a name such as
`..data/orker.yaml` names the Orker's file before the update and after it.
The Orker looks at its own files before it follows a name and again after,
compares every file and directory the way came to with both looks, and
follows the name once more when its way was cut short at a name it could not
look at, such as the directory an update is removing. So one update that
lands while a name is judged is seen, whichever of its steps the way met.
Two cases stay out of reach: two updates of one mount while one name is judged —
the kubelet updates a mount on its periodic sync, once a minute by default —
and, in the moment an update writes its new dated directory or removes the
old one, a name of that directory itself, which the kubelet names with a
random number no task can list.
For the same reason the Orker checks a name and then reads it, rather than
opening the file and judging the descriptor it holds: such a read would close
a window no task can reach, at the cost of a second, platform-specific
reading of every path, and it is not planned.

An Orker's own object-store client signs only with `ORK_S3_ACCESS_KEY` and
`ORK_S3_SECRET_KEY` (`pkg/artifacts/s3.go`). The cloud credentials above are
on the list for the credential helpers, not for the object store. A task that
needs a cloud credential is given it under a variable or file of its own.

The list bounds what belongs to this Orker. Everything else on the host is
reachable by name until the floor narrows it: another Orker's work root and
configuration on the same host, and the host's other files. That is why every
Orker of a shared pool, and every Orker that shares a host, should set a floor
([DEPLOY.md](DEPLOY.md), "Secrets on a shared pool").

### Environment values are configuration

A task's `env:` map is stored in the definition and passed to the container. It
is configuration and never a secret. No `env:` value grants a credential, a
privilege or host access (`AGENTS.md` §6). A value that must stay secret belongs
in `secrets:`. The `env:` source of a secret, `env:NAME`, is a different thing:
it names a variable in the Orker's environment, and the Orker reads it.

## Callers, principals and bearer credentials

### Callers are authenticated by the gateway

OrkStack does not authenticate callers. The deployment's gateway does, and it may
assert a principal in the `Ork-Principal` header ([ARCHITECTURE.md](../ARCHITECTURE.md),
"API"). The Connect services and the webhook management routes accept any caller
that reaches the API port. The only server-side checks on an `Authorization`
header are on the two plain-HTTP routes that face outside callers: trigger invoke
and the await callback ([DEPLOY.md](DEPLOY.md), "Ops notes").

Which callers may reach the API port, and what the gateway lets them do, is the
gateway's decision. It is not configured in OrkStack.

### The principal is asserted, not authenticated

The `Ork-Principal` header is the principal. It is recorded as the gateway
sent it, subject to the bounds below, and it is never verified. No surface may
present it as a verified identity, and every rendering says it was asserted
([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Control-plane audit trail").

- In an audit record it is bounded to 256 bytes, and control characters are
  replaced by spaces (`pkg/artifacts/audit.go`, `AuditPrincipalMaxBytes`,
  `oneLine`).
- In a run's origin it is bounded to 256 bytes, and invalid UTF-8 is replaced
  (`internal/api/origin.go`, `boundOriginAssertion`).
- A gateway that authenticates callers must overwrite `Ork-Principal` with the
  identity it established. It must never pass through a client's value, because
  otherwise any caller can claim any name ([DEPLOY.md](DEPLOY.md), "Attributing
  control-plane writes").
- `ORK_PRINCIPAL` makes `orkctl` assert a name. It is an assertion like any other.
- `source_address` is the TCP peer. No `X-Forwarded-For` or other proxy header
  is consulted.

### Bearer credentials

Two bearer credentials are issued by OrkStack. Both use the same primitives: a
32-byte token from `crypto/rand`, hex encoded, with a 16-byte salt, and a digest
of `sha256(salt || token)`. A fast hash is deliberate, because a 256-bit random
token has no low-entropy space to search. Both are compared in constant time
([ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md), "Storage and secret handling";
`pkg/triggers/secret.go`).

| Credential | Issued by | Shown | Stored | Authorises |
| --- | --- | --- | --- | --- |
| Trigger token | `CreateTrigger` (`orkctl trigger create`) | once, in the creation response | the salt and the digest, in `metadata/triggers/{id}.json` | starting one run of the Orkflow the trigger names |
| Await callback token | `SubmitRun`, for each `callback: true` await | once, in the submit response (`awaitCallbacks`) | the salt and the digest, on the run's plan | resolving that one await (`internal/api/awaits.go`, `verifyAwaitToken`) |

- The token is never stored, and it appears in no listing, log line, record or
  audit detail. A trigger id is not confidential, so an unknown id (`404`) and a
  wrong token on a real id (`401`) are told apart
  ([ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md), "Invoking a trigger").
- A token is rotated by deleting the trigger and creating another. An await
  callback token can resolve only the await it was minted for.
- Trigger invocations and await callbacks are not audited. The run's own record
  and origin are the durable fact ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Control-plane
  audit trail").

## Webhook secrets and destinations

A webhook's signing secret is not a bearer credential. It signs outbound deliveries,
and it is stored in plaintext because delivery must replay it
([ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md), "Trigger versus webhook").

- A secret is generated, 32 random bytes hex encoded, or supplied by the caller.
  OrkStack sets no minimum length for a supplied secret (`pkg/webhooks/webhooks.go`).
- It is returned once, by the response that creates the subscription. Listings and
  reads never return it, and no audit detail carries it.
- It is stored in `metadata/webhooks/{id}.json` ([STORAGE.md](STORAGE.md), "Key
  layout"). Anyone who can read that object can sign a body as that subscription.
  Anyone who can read the registry can read every subscription's secret. Access to
  the object store is therefore part of the webhook trust boundary.
- Each delivery carries an HMAC-SHA256 of its exact body. A delivery record keeps
  the signature that was sent, and never the secret
  (`proto/orkstack/v1/webhook_service.proto`, `Delivery`).

### Destinations

- A subscription may name any absolute `http` or `https` URL with a host
  (`pkg/webhooks/webhooks.go`, `ValidateURL`). There is no allow-list of hosts, and
  no check that a host is internal or external.
- The dispatcher sets no redirect policy, so Go's HTTP client follows up to ten
  redirects. A `307` or `308` re-sends the body. When the host changes, the client
  strips the sensitive headers, such as `Authorization` and `Cookie`, and copies
  the rest, `X-Ork-Signature` among them. A subscriber URL should answer without
  redirecting ([WEBHOOKS.md](WEBHOOKS.md), "Attempts and retries").
- A delivery's `run` carries the run's input and output, and its origin.
  So choosing a subscription's scope and destination is a disclosure decision.
  A run's inputs may contain secrets ([UI_CLI_PARITY.md](UI_CLI_PARITY.md), "Rules
  both clients follow").
- A subscription with no scope receives the transitions of every namespace.

## The audit trail

Every control-plane write is audited, whether it succeeded or was refused. One
interceptor records every Connect write RPC, and the webhook HTTP adapter records
its own writes (`internal/api/audit.go`). A record is written once to
`metadata/audit/{yyyy}/{mm}/{dd}/{id}.json`, before the response leaves the engine
([DEPLOY.md](DEPLOY.md), "Attributing control-plane writes").

- The trail records writes only. Reads, `SubmitRun`, trigger invocations and
  await callbacks are not recorded.
- A request refused for the type its body declares is not a write and is not
  recorded: a Connect call in a codec the API does not serve, and a webhook
  registration whose body is not declared `application/json`.
- No record carries a secret or a token. The extractors never read a webhook
  secret or a trigger token.
- The write is best-effort. A failed audit write is logged and counted, and the
  operation's result does not change.
- The trail is a record, not a control. Nothing reads it to decide anything, and
  there is no hash chain. Immutability comes from create-only writes and from the
  bucket's own object-lock or versioning policy, which the operator sets.

## Transport, listeners and configuration

- **The API listener is plain HTTP.** The engine serves HTTP/1.1 and unencrypted
  HTTP/2 (h2c) on one address (`internal/api/server.go`, `NewServer`). It has no
  TLS listener.
- **The web UI shares the API's origin.** The engine serves the UI's static
  build at `/` on that same listener (`internal/webui`), so the browser's
  Connect calls and its run and task-log sockets are same-origin. That is where
  the page comes from, not a check: the Connect API still answers a request from
  any origin (below). The bridge's default, with `ORK_WS_ALLOWED_ORIGINS` unset,
  admits a socket only from the engine's own origin, and that is all the UI
  needs; the setting is only for a page served from another origin. Every UI
  response carries `X-Content-Type-Options: nosniff`.
- **The page runs only the scripts its build names.** The page is sent under a
  content security policy the UI's build writes into it (`ui/vite.config.ts`,
  the `sveltekit` plugin's `csp`) and the engine sends as a header
  (`internal/webui`, `pagePolicy`): scripts, styles, fonts and connections from the page's own
  origin only, and an inline script only when the policy names its SHA-256
  hash: app.html's theme script and SvelteKit's boot script, and nothing a
  page could have injected. There is no `'unsafe-inline'` for scripts, no
  plugin (`object-src 'none'`), no `<base>` (`base-uri 'none'`), and forms
  post only to the page's origin. Scripts allow `'unsafe-eval'`: the YAML
  editor's schema checks compile their validators at run time (ajv,
  `ui/src/lib/yaml/lint.ts`), from the generated schemas the build carries
  and nothing a caller sends. Styles allow `'unsafe-inline'`, because the
  page cannot run without inline styles: app.html wraps the app in a styled
  element, Svelte renders style attributes from its templates, and the YAML
  editor (CodeMirror) inserts its own `<style>` element at run time. Images
  allow `data:`, which the editor draws its markers with. Neither runs a
  script.
  The page refuses to be framed (`frame-ancestors 'none'`, which opens the
  policy, and `X-Frame-Options: DENY`), because it carries the operator's
  destructive controls. A page that names no policy, such as the placeholder,
  is sent the build's policy with no hash and no `'unsafe-eval'`, so it
  runs no inline script and no eval (`webui.DefaultPagePolicy`, which a test
  holds to `ui/vite.config.ts`); every
  other UI response is sent `default-src 'none'; frame-ancestors 'none'`, so a
  file of the build opened as a document runs nothing.
- **The UI serves only its own build.** The build holds no credential, and the
  engine serves only its files: no listing, no dot file, nothing outside the
  build (a directory named by
  `ORK_UI_DIR` is opened so that a link leading out of it is never followed),
  and never a path the API owns. Serving the UI adds no authentication: the
  engine still authenticates no caller, a browser's calls are a caller's like
  any other, and the UI is reached through the same gateway as the API
  ([DEPLOY.md](DEPLOY.md), "The web UI").
- **A page on another site cannot make the engine act.** The engine grants no
  CORS preflight: it answers one without an `Access-Control-Allow-Origin`. A
  browser therefore sends a request to another site only when it needs no
  preflight: a `GET`, `HEAD` or `POST` with no header beyond the CORS-safelisted
  ones and a body declared `text/plain`, `application/x-www-form-urlencoded` or
  `multipart/form-data`, or not declared at all. The API acts on no such
  request:
  - a Connect call needs a body declared as one of its codecs, JSON or proto,
    and anything else is `415` before any handler runs;
  - `POST /v1/webhooks` needs a body declared `application/json`, and anything
    else is `415` before the body is read (`internal/api/webhooks.go`,
    `declaresJSON`; [WEBHOOKS.md](WEBHOOKS.md), "Management");
  - trigger invoke and the await callback need an `Authorization: Bearer`
    header, which a page can set on a request to another site only after a
    preflight, so they read a body whatever type it declares ([ORKFLOW_TRIGGERS.md](ORKFLOW_TRIGGERS.md),
    "Invoking a trigger");
  - a browser never sends a `DELETE` to another site without a preflight, and
    no `GET` changes anything.

  Such a page cannot read a response either, since none grants it. This holds
  for a page on another origin only: a page that reaches the engine by DNS
  rebinding has the engine's own origin, and the `Host` allowlist (below) is
  what refuses it.
- **The engine answers only its own names.** DNS rebinding is a page on a
  domain its author controls whose name is then made to resolve to the
  engine. Its requests carry the engine's own origin, so no origin check can
  tell them from the UI's, but they carry the attacker's domain as their
  `Host`. The API listener refuses, `403` before any route sees it, a request
  whose `Host` it does not answer to (`internal/hostpolicy`, `Policy`). With
  nothing set it answers an IP address, `localhost` and every name under it,
  a name with no dot (written without a trailing one, and not a number a
  browser would read as an IPv4 address), the host `ORK_HTTP_ADDR` names,
  and a request with no `Host`. A page's author can make only a name
  registered in public DNS resolve where they like, and every such name has
  a dot; a dotless name resolves only on the local network.
  `ORK_ALLOWED_HOSTS` adds names, a `*.suffix` of two labels or more (a
  pattern over a top-level name, `*.com`, is refused at boot: anyone may
  register a name under it) or `*` for any `Host`; any `Host` with a dot
  that is not listed is refused. The policy judges the host the listener
  serves the request for: under an absolute-form target that is the
  target's, never `X-Forwarded-Host`, and a request with two `Host` headers
  is refused `400` before it is judged. What this
  does not stop: a page under a name the operator listed (a `*.suffix` admits
  every name under it, so a subdomain someone else controls is admitted
  too, and a suffix of two labels anyone may register under, such as
  `*.co.uk` or `*.github.io`, admits names a page's author registers there);
  anything once `*` is set; and an attacker who already controls name
  resolution on the local network, who can answer for a dotless name. Behind
  an authenticating gateway, the gateway's own check of the caller still
  stands either way. The admin listener applies the same rule under its own
  setting, `ORK_ADMIN_ALLOWED_HOSTS`, so a page that rebinds a public name
  to it cannot read `/metrics`, or `/debug/pprof/` when
  `ORK_PPROF_ENABLED=true`; keep `ORK_ADMIN_ADDR` on a private address all
  the same, since a process that reaches it by address is answered
  ([DEPLOY.md](DEPLOY.md), "The web UI"; "The API listener's bounds").
- **The listeners bound what a client may make them hold in memory.** The
  API listener closes an HTTP/1.1 connection that takes more than 10 s to
  send a request's headers, and any connection idle longer than
  `ORK_HTTP_IDLE_TIMEOUT`; it answers headers over 64 KiB with `431`; and it
  refuses a Connect message over `ORK_REQUEST_MAX_BYTES`, as sent or once
  decompressed, `resource_exhausted` before any handler runs, holding none
  of what is past the cap, inflating gzip no further than it, and reading a
  body no further than twice it before it closes the connection (HTTP/1.1)
  or resets the stream (h2c). The cap
  follows the task ceiling (1 MiB and 4 KiB a task, 17.4 MB at the default
  4,000), so it bounds a request and never how large an Orkflow may be. Over
  h2c no header timeout applies: a connection whose first request's headers
  never end carries no stream and is closed by the idle bound. The
  plain-HTTP routes bound their own bodies. The admin listener has the
  same kinds of bound. Neither sets a whole-request or write deadline, so a
  run's event stream, a live log or a WebSocket is never cut, and so neither
  bounds how long a body takes to arrive or an answer to be read: a slow
  client holds its connection while it keeps going. That, and how many
  connections a client may open, are for the proxy or gateway in front to
  bound ([DEPLOY.md](DEPLOY.md), "The API listener's bounds").
- **The admin listener is separate.** `ORK_ADMIN_ADDR` opens a listener only when it
  is set. It serves `/healthz` and `/readyz`, and `/metrics` when Prometheus is
  asked for. `/debug/pprof/` is mounted only when `ORK_PPROF_ENABLED=true`
  (`internal/telemetry/admin.go`; `internal/config/config.go`). None of these
  endpoints is served on the API port.
- **Temporal has no TLS or credential setting.** The configuration schema sets the
  Temporal address and namespace for the engine and the Orker. It has no TLS or
  credential key for that connection (`internal/config/config.go`, `temporal.*`).
- **Loopback is not an authentication boundary.** The quickstart publishes its
  ports on `127.0.0.1` only (`compose.yaml`). That keeps other machines out,
  but any process on the machine reaches the API. A web page can reach it only
  under a `Host` the engine answers to: by DNS rebinding through a name with a
  dot, never, since the `Host` allowlist (above) refuses such a name, and the
  admin listener's does too. Any
  process on the machine can also push to the
  quickstart's image registry, which takes a push without a login: its
  catalog names the first-party activities by a tag there, and an apply pins
  whatever digest that tag names at that moment. A deployment anyone else can
  reach needs the authenticating gateway this model assumes.
- **Object-storage keys have development defaults.** `ORK_S3_ACCESS_KEY` and
  `ORK_S3_SECRET_KEY` default to the values in the development compose file
  (`pkg/artifacts/config.go`). The operator must supply real credentials
  ([DEPLOY.md](DEPLOY.md), §1, `storage.s3.access_key`).
- **Telemetry is off by default.** A deployment that sets no telemetry configuration
  exports nothing and opens no admin listener (`AGENTS.md` §11).

## What this model does not cover

- **Caller authentication and authorisation.** The gateway does them. That covers
  who may reach the Connect API and the UI, who may register or change a
  webhook, and what the gateway lets a bearer-token holder reach. OrkStack does
  not configure any of it ([ARCHITECTURE.md](../ARCHITECTURE.md), "API";
  [DEPLOY.md](DEPLOY.md), "Ops notes").
- **Transport security.** The engine serves plain HTTP, and the configuration has
  no TLS setting for Temporal. Encryption of the listeners and of the connections to
  Temporal and to object storage is the deployment's to provide.
- **Network policy.** OrkStack configures no firewall and no network policy. Which
  hosts reach the API port, the admin listener, Temporal, the object store and the
  registry is the deployment's decision.
- **The Temporal cluster.** Its security is outside OrkStack. Temporal's own
  external requirements are not OrkStack dependencies ([AGENTS.md](../AGENTS.md),
  §13).
- **Object-store access.** Bucket policy, who may read the registry and the audit
  and delivery records, and who may read webhook secrets are all the operator's.
  Retention of those records is the operator's bucket lifecycle rule
  ([DEPLOY.md](DEPLOY.md), "Retention cleanup and bucket lifecycle rules").
- **Container isolation strength.** The profile narrows what a container can do.
  It does not replace the isolation the container runtime and the host kernel
  provide, and this document makes no claim about how strong that isolation is.
- **What a workload does.** A container that is given a secret can use it or
  disclose it. The profile limits what the container can reach, not what the image
  does with the files it was given. Egress from the `default` network is not
  filtered.
- **Image content.** Signature verification checks that a key the namespace names
  signed this digest. It does not inspect what the image contains.
- **Exactly-once effects.** Activities are at least once, and OrkStack claims no
  exactly-once execution for an external side effect. The `idempotencyKey` lets an
  external system deduplicate if it chooses to
  ([AGENTS.md](../AGENTS.md), §4).
- **Webhook receivers.** Verifying the signature, and deciding what to do with a
  delivery, is the receiver's job. OrkStack keeps no record of what a receiver did
  with one ([WEBHOOKS.md](WEBHOOKS.md), "Verifying the signature").
- **Production readiness.** OrkStack's failure and recovery scenarios must be
  proven before it is called production-ready
  ([AGENTS.md](../AGENTS.md), §11).
