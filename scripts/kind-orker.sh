#!/usr/bin/env bash
# scripts/kind-orker.sh: scripts/k8s-orker-check.sh on a throwaway kind
# cluster, which this script makes and deletes (docs/DEPLOY.md "Orkers on
# Kubernetes").
#
#   scripts/kind-orker.sh           make the cluster, check it, delete it
#   KEEP=1 scripts/kind-orker.sh    leave the cluster up afterwards
#
# It makes the cluster the check expects of any other: nodes with the
# orkstack-engine containerd handler (deploy/kubernetes/kind/kind.yaml), the
# RuntimeClass, and an OrkStack deployment for the check to use, from
# deploy/kubernetes/kind/infra: Temporal's development server, SeaweedFS, a
# registry served over TLS with a certificate authority made for this run,
# and one ork engine. It builds ork, orker and the shell and file activities
# from this tree, loads the first two into the nodes, and pushes the
# activities to that registry. Then it runs the check against the cluster,
# both shapes, and measures one thing the check does not: that Docker's
# rootless engine drops a container's limits, which is why no rootless shape
# ships.
#
# Needs Docker, Go, curl, openssl and jq on the host, and installs nothing
# system-wide. Every tool is pinned: kind's and kubectl's release binaries
# are kept in .data/kind-orker/bin only if they hash to the SHA-256 below,
# crane runs from its image by digest, and every image is pinned by digest
# except ork, orker and the two activities, which are built from this tree.
# Each run's log and evidence is under .data/kind-orker/<stamp>/, the
# check's under its check/, with the cluster's state when a check fails. Not
# part of any proof tier: .github/workflows/kind-orker.yml runs it
# (docs/PROOFS.md §5), and it runs by hand.
set -euo pipefail

KIND_VERSION=v0.33.0
# The SHA-256 each release publishes beside each binary (kind-linux-<arch>.sha256sum, kubectl.sha256).
KIND_SHA256_amd64=aee6151561422756b764a4ae28e7f44cda5af5a9eead3cc9985112b1de8d8e0d
KIND_SHA256_arm64=20022bee6cfcd5086cb7234d218e3454e6090022f2a8f55d1fa7fcf42c3867a2
KUBECTL_VERSION=v1.37.0
KUBECTL_SHA256_amd64=6129359f4e1f3848a5572ccb0b26cf28b8ca08cef38c95a765b2f64a2c961a2f
KUBECTL_SHA256_arm64=922df28df248cc00a9e025f947704f1d1482de64ece54cfe57e61f19eaf1eef3
KIND_NODE_IMAGE=kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
CRANE_IMAGE=gcr.io/go-containerregistry/crane:v0.22.1@sha256:1f968817b95790bed063f71175aa6b8ff879fa17064020415f3e18bb6e6a36e1
PROBE_IMAGE=docker.io/library/alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
REGISTRY_HOST=registry.orkstack-infra.svc.cluster.local:5000

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
cluster=${KIND_ORKER_CLUSTER:-orkstack-kind-orker}
tools=$root/.data/kind-orker/bin
out=$root/.data/kind-orker/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$tools" "$out"
exec > >(tee "$out/run.log") 2>&1
control=$cluster-control-plane
worker=$cluster-worker
kubeconfig=$out/kubeconfig

log() { printf '%s %s\n' "$(date -u +%T)" "$*"; }
kctl() { "$tools/kubectl" --kubeconfig "$kubeconfig" "$@"; }
# What a failed step leaves for whoever reads it: the cluster as it was.
cluster_state() {
	docker inspect "$control" >/dev/null 2>&1 || {
		echo "no cluster $cluster"
		return 0
	}
	local pod
	echo "== kubectl get nodes -o wide"
	kctl get nodes -o wide 2>&1 || :
	echo "== kubectl get pods -A -o wide"
	kctl get pods -A -o wide 2>&1 || :
	echo "== kubectl get events -A --sort-by=.lastTimestamp"
	kctl get events -A --sort-by=.lastTimestamp 2>&1 || :
	for pod in $(kctl -n orkstack get pods -o name 2>/dev/null); do
		echo "== kubectl -n orkstack logs $pod (every container, the last 200 lines)"
		kctl -n orkstack logs "$pod" --all-containers --prefix --tail=200 2>&1 || :
	done
}
fail() {
	log "FAIL: $*"
	cluster_state >"$out/cluster-state.txt" 2>&1 || :
	log "the cluster's state at the failure: ${out#"$root"/}/cluster-state.txt"
	exit 1
}
expect() { # expect FILE PATTERN WHAT: the evidence file holds a line matching PATTERN
	grep -Eq -- "$2" "$1" || fail "$3: no line of $(basename "$1") matches /$2/"
	log "ok: $3"
}

for tool in docker go curl openssl jq; do
	command -v "$tool" >/dev/null || {
		echo "kind-orker: $tool is required" >&2
		exit 77
	}
done

cleanup() {
	if [ "${KEEP:-}" = 1 ]; then
		log "KEEP=1: cluster $cluster left running ($tools/kind delete cluster --name $cluster)"
	else
		"$tools/kind" delete cluster --name "$cluster" >/dev/null 2>&1 || :
	fi
}
trap cleanup EXIT

case $(uname -m) in
x86_64) arch=amd64 ;;
aarch64 | arm64) arch=arm64 ;;
*) fail "no kind or kubectl binary is pinned for $(uname -m)" ;;
esac
fetch() { # fetch NAME URL SHA256: keep $tools/NAME only if it hashes to SHA256
	local name=$1 url=$2 want=$3 got
	[ "$(sha256sum "$tools/$name" 2>/dev/null | cut -d' ' -f1)" != "$want" ] || return 0
	log "fetching $url"
	curl --fail --silent --show-error --location --proto '=https' --retry 2 --max-time 300 -o "$tools/$name.download" "$url"
	got=$(sha256sum "$tools/$name.download" | cut -d' ' -f1)
	[ "$got" = "$want" ] || fail "$url hashes to $got, not the pinned $want"
	chmod 0755 "$tools/$name.download"
	mv "$tools/$name.download" "$tools/$name"
}
want=KIND_SHA256_$arch
fetch kind "https://github.com/kubernetes-sigs/kind/releases/download/$KIND_VERSION/kind-linux-$arch" "${!want}"
want=KUBECTL_SHA256_$arch
fetch kubectl "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/$arch/kubectl" "${!want}"
"$tools/kind" version
"$tools/kubectl" version --client | head -n 1
log "building orkctl from this tree"
go build -o "$tools/orkctl" ./cmd/orkctl

"$tools/kind" delete cluster --name "$cluster" >/dev/null 2>&1 || :
log "creating cluster $cluster ($KIND_NODE_IMAGE)"
"$tools/kind" create cluster --name "$cluster" --image "$KIND_NODE_IMAGE" \
	--config deploy/kubernetes/kind/kind.yaml --kubeconfig "$kubeconfig" --wait 120s
node_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$worker")

# kind points each node's resolver at an address it DNATs to Docker's
# embedded DNS. Where Docker's own upstream is that same address (a daemon
# configured with "dns": ["172.17.0.1"]), every lookup loops. Removing the
# DNAT lets the nodes ask that resolver directly; it changes nothing else.
for node in "$control" "$worker"; do
	if ! docker exec "$node" curl -sS -o /dev/null https://registry-1.docker.io/v2/ 2>/dev/null; then
		log "$node cannot resolve registry-1.docker.io; removing kind's DNS DNAT on it"
		docker exec "$node" sh -c 'iptables -t nat -F DOCKER_OUTPUT && iptables -t nat -F DOCKER_POSTROUTING'
		docker exec "$node" curl -sS -o /dev/null https://registry-1.docker.io/v2/ || fail "$node still cannot reach registry-1.docker.io"
	fi
done
kctl -n kube-system rollout restart deploy/coredns >/dev/null
# kind bind-mounts files over /sys/class/dmi/id/product_{name,uuid} on each
# node, so that its nodes differ. With them sysfs is not fully visible there,
# and the kernel refuses a user namespace a sysfs of its own: no pod with
# hostUsers: false can start. A real node has no such mounts.
for node in "$control" "$worker"; do
	docker exec "$node" sh -c 'for _ in 1 2 3 4; do
		umount /sys/devices/virtual/dmi/id/product_uuid 2>/dev/null
		umount /sys/devices/virtual/dmi/id/product_name 2>/dev/null
	done
	! grep -q " /sys/devices/virtual/dmi/id/product_" /proc/self/mountinfo' ||
		fail "$node: kind's files over /sys/devices/virtual/dmi/id could not be unmounted"
done
# The nodes declare the handler (kind.yaml); the cluster declares the class.
kctl apply -f deploy/kubernetes/orker-userns/runtimeclass.yaml >/dev/null

{
	echo "kind $KIND_VERSION, kubectl $KUBECTL_VERSION, node image $KIND_NODE_IMAGE"
	kctl version 2>/dev/null | grep Server
	docker exec "$worker" containerd --version
	echo "kernel $(uname -r)"
	echo "tree $(git rev-parse HEAD) $(git status --porcelain | wc -l) uncommitted paths"
} | tee "$out/versions.txt"

log "a certificate authority for the registry"
tls=$out/tls
mkdir -p "$tls"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 2 \
	-subj "/CN=orkstack kind-orker CA" -keyout "$tls/ca.key" -out "$tls/ca.crt" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
	-subj "/CN=${REGISTRY_HOST%:*}" -keyout "$tls/tls.key" -out "$tls/tls.csr" 2>/dev/null
printf 'subjectAltName=DNS:%s,IP:%s\nextendedKeyUsage=serverAuth\n' "${REGISTRY_HOST%:*}" "$node_ip" >"$tls/ext.cnf"
openssl x509 -req -in "$tls/tls.csr" -CA "$tls/ca.crt" -CAkey "$tls/ca.key" -CAcreateserial -days 2 \
	-extfile "$tls/ext.cnf" -out "$tls/tls.crt" 2>/dev/null
for ns in orkstack-infra orkstack; do kctl create namespace "$ns" >/dev/null; done
kctl -n orkstack-infra create secret tls registry-tls --cert="$tls/tls.crt" --key="$tls/tls.key" >/dev/null
kctl -n orkstack create configmap proof-ca --from-file=ca.crt="$tls/ca.crt" >/dev/null

log "building ork, orker and the shell and file activities from this tree"
for target in ork orker; do
	docker build -q -f deploy/source/Dockerfile --target "$target" -t "localhost/orkstack-k8s/$target:proof" . >/dev/null
done
"$tools/kind" load docker-image --name "$cluster" localhost/orkstack-k8s/ork:proof localhost/orkstack-k8s/orker:proof >/dev/null
for activity in shell file; do
	docker build -q -t "localhost/orkstack-k8s/activity-$activity:proof" "images/activity-$activity" >/dev/null
done

log "infrastructure: Temporal, SeaweedFS, the registry and ork"
kctl apply -k deploy/kubernetes/kind/infra >/dev/null
kctl -n orkstack-infra rollout status deploy/registry --timeout=300s >/dev/null
for activity in shell file; do
	docker save "localhost/orkstack-k8s/activity-$activity:proof" -o "$out/activity-$activity.tar"
	chmod 0644 "$out/activity-$activity.tar" # crane's image runs as an unprivileged user
	# crane, from its image, on kind's network: the registry answers on the
	# worker's NodePort with the certificate this run's authority signed.
	docker run --rm --network kind -v "$out:/out:ro" -e SSL_CERT_FILE=/out/tls/ca.crt "$CRANE_IMAGE" \
		push "/out/activity-$activity.tar" "$node_ip:30500/orkstack/activity-$activity:proof" 2>/dev/null
	rm -f "$out/activity-$activity.tar"
done
kctl -n orkstack-infra rollout status deploy/temporal deploy/seaweedfs --timeout=300s >/dev/null ||
	fail "Temporal or SeaweedFS never became ready"
kctl -n orkstack rollout status deploy/ork --timeout=300s >/dev/null || fail "ork never became ready"

log "== scripts/k8s-orker-check.sh on the cluster"
KUBECTL=$tools/kubectl ORKCTL=$tools/orkctl scripts/k8s-orker-check.sh \
	--kubeconfig "$kubeconfig" \
	--server "http://$node_ip:30080" \
	--orker-image localhost/orkstack-k8s/orker:proof \
	--pull-policy Never \
	--settings-from orkstack \
	--registry-host "$REGISTRY_HOST" \
	--registry-ca "$tls/ca.crt" \
	--out "$out/check" ||
	fail "scripts/k8s-orker-check.sh did not pass (exit $?): $(grep -E ' (FAIL|MISSING): ' "$out/check/check.log" | tail -n 1)"

log "== Docker's rootless engine and a container's limits"
kctl apply -k deploy/kubernetes/kind/rootless-dind >/dev/null
kctl -n orkstack wait --for=condition=Ready pod/rootless-dind --timeout=300s >/dev/null ||
	fail "the rootless engine's pod never became ready"
kctl -n orkstack exec rootless-dind -- docker run --rm --memory 64m --cpus 0.5 --pids-limit 100 "$PROBE_IMAGE" \
	sh -c 'echo "memory.max: $(cat /sys/fs/cgroup/memory.max)"; echo "cpu.max: $(cat /sys/fs/cgroup/cpu.max)"' 2>/dev/null |
	tee "$out/rootless-limits.txt"
kctl -n orkstack logs rootless-dind >"$out/rootless-daemon.log"
expect "$out/rootless-limits.txt" '^memory.max: max$' "rootless engine: --memory 64m is not applied"
expect "$out/rootless-limits.txt" '^cpu.max: max ' "rootless engine: --cpus 0.5 is not applied"
expect "$out/rootless-daemon.log" 'Running in rootless-mode without cgroups' "rootless engine: the daemon says it runs without cgroups"
kctl delete -k deploy/kubernetes/kind/rootless-dind --ignore-not-found >/dev/null

log "PASS: the check held on kind, and the rootless engine drops limits; evidence in ${out#"$root"/}"
