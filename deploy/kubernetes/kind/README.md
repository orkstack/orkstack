# The kind cluster the CI check runs on

What `scripts/kind-orker.sh` adds to a throwaway kind cluster before it runs
`scripts/k8s-orker-check.sh` against it. None of it is a deployment: Temporal
is the development server, the store and the registry keep their data in
`emptyDir`s, and the registry's certificate authority is made for one run.

| Path | What it is |
|---|---|
| `kind.yaml` | the cluster: one control plane and one worker, each declaring the `orkstack-engine` containerd handler |
| `infra/` | Temporal, SeaweedFS, a registry served over TLS, and one `ork` engine with the shell and file activities in its catalog, and the settings the check copies |
| `rootless-dind/` | a bare `docker:dind-rootless` pod, to measure that it drops each task's limits |
