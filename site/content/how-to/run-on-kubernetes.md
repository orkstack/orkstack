# Run Orkers on Kubernetes

Each Orker pod runs a Docker engine of its own, in a sidecar container. The Orker's tasks run in that engine. They never use the node's container runtime or the node's Docker socket.

## Choose a shape

The repository ships two kustomize bases in [`deploy/kubernetes/`](https://github.com/orkstack/orkstack/tree/main/deploy/kubernetes). Use `orker-userns/` unless your nodes cannot run it.

| Base | Needs | Privileged on the node |
|---|---|---|
| `orker-userns/` (prefer it) | user namespaces and the `orkstack-engine` RuntimeClass | nothing |
| `orker-privileged/` | a namespace that admits privileged pods | the engine container, never a task |

## Before you start

Both shapes need the `privileged` Pod Security level in the namespace the Orkers run in.

The user-namespaced shape needs user namespaces (`hostUsers: false`) on each node. It also needs a containerd runtime handler named `orkstack-engine` with `cgroup_writable = true`. In containerd 2's configuration (version 3), add:

```toml
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.orkstack-engine]
  runtime_type = 'io.containerd.runc.v2'
  cgroup_writable = true
  [plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.orkstack-engine.options]
    SystemdCgroup = true
```

For a version 2 configuration file, the table name is different. [DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#orkers-on-kubernetes) gives it.

Restart containerd on each node. The base declares the RuntimeClass, so you do not write it yourself.

## Deploy

Make an overlay directory of your own. Its `kustomization.yaml` uses the base and names the release's `orker` image by digest:

```yaml
# kustomization.yaml
resources:
  - <your clone>/deploy/kubernetes/orker-userns
images:
  - name: orker
    newName: ghcr.io/orkstack/orker
    digest: sha256:<the release's orker digest>
```

A release's `orker` image is `ghcr.io/orkstack/orker`, tagged with its version, such as `0.1.0`. To run a commit that no release holds, build the `orker` image from a clone (`deploy/source/Dockerfile`, target `orker`), push it to your registry, and name that image instead.

Next, create the settings the Orker reads. `orkstack.env` holds the ConfigMap's settings, such as `ORK_TEMPORAL_ADDRESS`, `ORK_ARTIFACT_BACKEND=s3`, `ORK_S3_ENDPOINT` and `ORK_S3_BUCKET`. `s3.env` holds `ORK_S3_ACCESS_KEY` and `ORK_S3_SECRET_KEY`. Keep `s3.env` out of version control.

Put both env files in the overlay directory. Then run these commands there, in the namespace the Orkers run in:

```bash
kubectl create configmap orkstack --from-env-file=orkstack.env
kubectl create secret generic orkstack-s3 --from-env-file=s3.env
kubectl apply -k .
```

## Check your cluster

Before you send real runs to the cluster, run the repository's check against it, from a clone of the release you deploy. It needs the settings you just created:

```bash
scripts/k8s-orker-check.sh --context <your context> --server <your ork engine> --orker-image <the release's orker image, by digest> --settings-from <the namespace of your settings>
```

It refuses, with the reason, whatever the bases need and the cluster lacks: the Kubernetes version, the `orkstack-engine` RuntimeClass and its handler, Pod Security admitting each shape, or Temporal, the object store or a registry it cannot reach. Then it runs both shapes in a namespace of its own and deletes it. It creates nothing cluster-wide. Its Orkers serve only the pool `k8s-check`, so they take no other run's tasks. [DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#orkers-on-kubernetes) lists every check, its options, and what it leaves in OrkStack.

## Check it works

```bash
kubectl rollout status deployment/orker
orkctl orker list
```

`kubectl rollout status` returns once the pods are ready. Both bases start two replicas, so `orkctl orker list` should show two Orkers. Each one is named `<pod>.<namespace>`, with STATE `ready`.

## Keep in mind

- The work root is one volume at one path in both containers. If the engine sees it at another path, the Orker refuses to start with `WORKDIR_NOT_BINDABLE`, and the pod never becomes ready.
- Every task runs inside the pod's cgroup. Size the engine container to hold `ORK_ORKER_MAX_CONCURRENT` tasks at `ORK_ORKER_MAX_MEMORY`, plus the daemon. Its cpu must be at least `ORK_ORKER_MAX_CPU`. The bases size it for four tasks of 1 cpu and 2Gi each.
- `network: none` and `network: default` both work. The default network reaches what the pod reaches, cluster Services included. If the cluster routes `172.17.0.0/16`, give the engine's bridge a free range with `--bip`, as [DEPLOY.md](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#orkers-on-kubernetes) describes.
- Drain an Orker before you delete its pod: `orkctl orker drain <orker-id>`. Deleting a pod cancels its in-flight work with `SIGTERM`. A task of a deleted pod ends `ORKER_HEARTBEAT_TIMEOUT`, unless its `retry:` runs it again on another Orker.
- Docker's rootless engine image (`docker:dind-rootless`) is not supported, because it applies none of the container limits.

CI runs the same check on a kind cluster, both shapes, on every change to what the bases run and every day. See [`.github/workflows/kind-orker.yml`](https://github.com/orkstack/orkstack/blob/main/.github/workflows/kind-orker.yml) and [`scripts/kind-orker.sh`](https://github.com/orkstack/orkstack/blob/main/scripts/kind-orker.sh).

## See also

- [DEPLOY.md, "Orkers on Kubernetes"](https://github.com/orkstack/orkstack/blob/main/docs/DEPLOY.md#orkers-on-kubernetes): the full reference, with sizing, the RuntimeClass and the proof
- [SECURITY_MODEL.md, "The Orker's own access to the host"](https://github.com/orkstack/orkstack/blob/main/docs/SECURITY_MODEL.md#the-orkers-own-access-to-the-host): how much each shape trusts its engine
- [PROOFS.md, "5. The CI job graph"](https://github.com/orkstack/orkstack/blob/main/docs/PROOFS.md#5-the-ci-job-graph): where the kind check runs, beside the proof tiers
- Next: [Deploy OrkStack](/docs/how-to/deploy/), [Choose where tasks run](/docs/how-to/pools-and-pins/)
