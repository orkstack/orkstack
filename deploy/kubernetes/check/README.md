# The Kubernetes Orker check

What `scripts/k8s-orker-check.sh` applies to a cluster you already have, to
check that the bases in `deploy/kubernetes/` work there before you trust them
(docs/DEPLOY.md, "Orkers on Kubernetes"). The script writes the rest itself:
the namespace, the orker image, its pull policy and a registry's certificate
authority are its options.

| Path | What it is |
|---|---|
| `common/` | a kustomize component every checked base gets: one replica in the check's own pool `k8s-check`, the per-task cap the limits run measures, a pod sized for a small node with a capacity to match, and no RuntimeClass of its own (the check requires it, never creates it) |
| `mismatch/` | the engine's work root at another path, which the Orker's boot check must refuse |
| `no-runtimeclass/` | the user-namespaced base without its RuntimeClass, whose engine must not start |
| `orkflows/` | the runs, every task on the pool `k8s-check`: artifacts and both network modes, a cancel, the limits, and a task above the per-task cap that the submit refuses |
