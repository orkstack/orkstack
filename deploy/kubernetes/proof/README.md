# The Kubernetes Orker proof

What `scripts/kind-orker.sh` applies to a throwaway kind cluster to prove the
manifests in `deploy/kubernetes/` (docs/DEPLOY.md, "Orkers on Kubernetes").
None of it is a deployment: Temporal is the development server, the store and
the registry keep their data in `emptyDir`s, and the registry's certificate
authority is made by the script for one run.

| Path | What it is |
|---|---|
| `kind.yaml` | the cluster: one control plane and one worker, and the `orkstack-engine` containerd handler the user-namespaced shape runs in |
| `infra/` | Temporal, SeaweedFS, a registry served over TLS, and one `ork` engine with the shell and file activities in its catalog |
| `common/` | a kustomize component every Orker overlay uses: the images the script built, the registry's CA in the engine, a smaller pod |
| `userns/`, `privileged/` | the two shapes, from `../orker-userns` and `../orker-privileged` as they are |
| `mismatch/` | `userns/` with the engine's work root at another path, which the Orker's boot check must refuse |
| `no-runtimeclass/` | `userns/` without the `orkstack-engine` RuntimeClass, whose engine must not start |
| `rootless-dind/` | a bare `docker:dind-rootless` pod, to measure that it drops each task's limits |
| `orkflows/` | the runs: artifacts and both network modes, a cancel, and the limits |
