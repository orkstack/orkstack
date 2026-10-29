#!/usr/bin/env bash
# scripts/kind-orker.sh: the Kubernetes Orker manifests (deploy/kubernetes/,
# docs/DEPLOY.md "Orkers on Kubernetes"), proven on a throwaway kind cluster.
#
#   scripts/kind-orker.sh           run every scenario, then delete the cluster
#   KEEP=1 scripts/kind-orker.sh    leave the cluster up afterwards
#
# For each shape, user-namespaced (deploy/kubernetes/orker-userns) and
# privileged (deploy/kubernetes/orker-privileged), applied as they ship with
# only deploy/kubernetes/proof/common's changes, it checks that:
#   1. a run passes artifacts between tasks, runs tasks with no network and
#      one on the default network, and gives each task its own limits and the
#      Orker's security profile;
#   2. each task's cgroup is inside the Orker pod's, never at the node's root;
#   3. a cancel reaches each task's container as SIGTERM, then SIGKILL after
#      that task's own grace;
#   4. a task cannot be given more than ORK_ORKER_MAX_MEMORY, and one that
#      uses more than it asked for is killed in its own cgroup;
#   5. deleting the pod stops the Orker first, which cancels its tasks the same
#      way, while the engine outlives it to deliver the signals.
# Then that the Orker refuses to boot when the engine sees the work root at
# another path, that the user-namespaced engine does not start without its
# RuntimeClass, and that Docker's rootless engine drops a container's limits,
# which is why no rootless shape ships.
#
# Needs Docker, Go, openssl and jq on the host, and nothing installed
# system-wide: kind and crane are built at pinned versions into
# .data/kind-orker/bin, kubectl is the kind node image's own, and every image
# is pinned by digest except ork, orker and the two activities, which are
# built from this tree. Each run's log and evidence is under
# .data/kind-orker/<stamp>/. Not part of any proof tier: it is run by hand.
set -euo pipefail

KIND_VERSION=v0.33.0
KIND_NODE_IMAGE=kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
CRANE_VERSION=v0.22.1
PROBE_IMAGE=docker.io/library/alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
REGISTRY_HOST=registry.orkstack-infra.svc.cluster.local:5000

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
cluster=${KIND_ORKER_CLUSTER:-orkstack-kind-orker}
tools=$root/.data/kind-orker/bin
out=$root/.data/kind-orker/$(date -u +%Y%m%dT%H%M%SZ)
mkdir -p "$tools" "$out"
exec > >(tee "$out/run.log") 2>&1

log() { printf '%s %s\n' "$(date -u +%T)" "$*"; }
fail() {
	log "FAIL: $*"
	exit 1
}
expect() { # expect FILE PATTERN WHAT: the evidence file holds a line matching PATTERN
	grep -Eq -- "$2" "$1" || fail "$3: no line of $(basename "$1") matches /$2/"
	log "ok: $3"
}

for tool in docker go openssl jq; do
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

log "building kind $KIND_VERSION, crane $CRANE_VERSION and orkctl from this tree"
GOBIN=$tools GOFLAGS=-mod=mod go install "sigs.k8s.io/kind@$KIND_VERSION"
GOBIN=$tools GOFLAGS=-mod=mod go install "github.com/google/go-containerregistry/cmd/crane@$CRANE_VERSION"
go build -o "$tools/orkctl" ./cmd/orkctl

"$tools/kind" delete cluster --name "$cluster" >/dev/null 2>&1 || :
log "creating cluster $cluster ($KIND_NODE_IMAGE)"
"$tools/kind" create cluster --name "$cluster" --image "$KIND_NODE_IMAGE" \
	--config deploy/kubernetes/proof/kind.yaml --kubeconfig "$out/kubeconfig" --wait 120s
control=$cluster-control-plane
worker=$cluster-worker
node_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$worker")

# kubectl is the control-plane node's own; kustomizations are read from a
# copy of deploy/kubernetes inside it.
kctl() { docker exec -i "$control" kubectl --kubeconfig /etc/kubernetes/admin.conf "$@"; }
tar -C deploy -c kubernetes | docker exec -i "$control" sh -c 'rm -rf /orkstack && mkdir -p /orkstack && tar -C /orkstack -x'
orkctl() { "$tools/orkctl" --server "http://$node_ip:30080" "$@"; }

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

{
	echo "kind $KIND_VERSION, node image $KIND_NODE_IMAGE"
	kctl version 2>/dev/null | grep Server
	docker exec "$worker" containerd --version
	echo "kernel $(uname -r)"
	echo "tree $(git rev-parse HEAD) $(git status --porcelain | wc -l) uncommitted paths"
} | tee "$out/versions.txt"

log "a certificate authority for the proof's registry"
tls=$out/tls
mkdir -p "$tls"
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 2 \
	-subj "/CN=orkstack kind-orker proof CA" -keyout "$tls/ca.key" -out "$tls/ca.crt" 2>/dev/null
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
	-subj "/CN=${REGISTRY_HOST%:*}" -keyout "$tls/tls.key" -out "$tls/tls.csr" 2>/dev/null
printf 'subjectAltName=DNS:%s,IP:%s\nextendedKeyUsage=serverAuth\n' "${REGISTRY_HOST%:*}" "$node_ip" >"$tls/ext.cnf"
openssl x509 -req -in "$tls/tls.csr" -CA "$tls/ca.crt" -CAkey "$tls/ca.key" -CAcreateserial -days 2 \
	-extfile "$tls/ext.cnf" -out "$tls/tls.crt" 2>/dev/null
tar -C "$tls" -c ca.crt tls.crt tls.key | docker exec -i "$control" sh -c 'mkdir -p /proof-tls && tar -C /proof-tls -x'
for ns in orkstack-infra orkstack; do kctl create namespace "$ns" >/dev/null; done
kctl -n orkstack-infra create secret tls registry-tls --cert=/proof-tls/tls.crt --key=/proof-tls/tls.key >/dev/null
kctl -n orkstack create configmap proof-ca --from-file=ca.crt=/proof-tls/ca.crt >/dev/null

log "building ork, orker and the shell and file activities from this tree"
for target in ork orker; do
	docker build -q -f deploy/source/Dockerfile --target "$target" -t "localhost/orkstack-k8s/$target:proof" . >/dev/null
done
"$tools/kind" load docker-image --name "$cluster" localhost/orkstack-k8s/ork:proof localhost/orkstack-k8s/orker:proof >/dev/null
for activity in shell file; do
	docker build -q -t "localhost/orkstack-k8s/activity-$activity:proof" "images/activity-$activity" >/dev/null
done

log "infrastructure: Temporal, SeaweedFS, the registry and ork"
kctl apply -k /orkstack/kubernetes/proof/infra >/dev/null
kctl -n orkstack-infra rollout status deploy/registry --timeout=300s >/dev/null
for activity in shell file; do
	docker save "localhost/orkstack-k8s/activity-$activity:proof" -o "$out/activity-$activity.tar"
	SSL_CERT_FILE=$tls/ca.crt "$tools/crane" push "$out/activity-$activity.tar" \
		"$node_ip:30500/orkstack/activity-$activity:proof" 2>/dev/null
	rm -f "$out/activity-$activity.tar"
done
kctl -n orkstack-infra rollout status deploy/temporal deploy/seaweedfs --timeout=300s >/dev/null
kctl -n orkstack rollout status deploy/ork --timeout=300s >/dev/null
for flow in deploy/kubernetes/proof/orkflows/*.ork.yaml; do
	orkctl apply "$flow" >/dev/null
done

# The Orker pod's name, once its Deployment has rolled out.
orker_pod() { kctl -n orkstack get pod -l app.kubernetes.io/name=orker --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}'; }
run_status() { orkctl run get "$1" --json | jq -r .run.status; }
wait_terminal() { # wait_terminal RUN: until the run has ended, at most 5 minutes
	local deadline=$((SECONDS + 300)) status
	while :; do
		status=$(run_status "$1")
		case $status in SUCCEEDED | FAILED | CANCELED) return 0 ;; esac
		[ $SECONDS -lt $deadline ] || fail "run $1 still $status after 5 minutes"
		sleep 2
	done
}
wait_started() { # wait_started RUN N: until N tasks have logged that they started
	local deadline=$((SECONDS + 300))
	until [ "$(orkctl run logs "$1" 2>/dev/null | grep -c ': started at ')" -ge "$2" ]; do
		[ $SECONDS -lt $deadline ] || fail "run $1: fewer than $2 tasks started within 5 minutes"
		sleep 1
	done
}
# The engine's own record of what it signalled, as "<unix ns> <action> <task> signal= exit=".
events_format='{{.TimeNano}} {{.Action}} {{index .Actor.Attributes "ork.task"}} signal={{index .Actor.Attributes "signal"}} exit={{index .Actor.Attributes "exitCode"}}'
signals() { # signals POD SINCE UNTIL
	kctl -n orkstack exec "$1" -c engine -- docker events --since "$2" --until "$3" \
		--filter type=container --filter event=kill --filter event=die --format "$events_format"
}
check_signals() { # check_signals FILE: TERM to both, graceful exits 0, stubborn KILLed after its 5 s grace
	local f=$1 term kill
	expect "$f" ' kill graceful signal=15 ' "graceful was sent SIGTERM"
	expect "$f" ' die graceful signal= exit=0$' "graceful exited 0 from its trap"
	expect "$f" ' kill stubborn signal=15 ' "stubborn was sent SIGTERM"
	expect "$f" ' kill stubborn signal=9 ' "stubborn was sent SIGKILL"
	expect "$f" ' die stubborn signal= exit=137$' "stubborn died of SIGKILL"
	term=$(awk '$2=="kill" && $3=="stubborn" && $4=="signal=15" {print $1; exit}' "$f")
	kill=$(awk '$2=="kill" && $3=="stubborn" && $4=="signal=9" {print $1; exit}' "$f")
	awk -v t="$term" -v k="$kill" 'BEGIN { d = (k - t) / 1e9; printf "SIGTERM to SIGKILL: %.2f s (grace 5 s)\n", d; exit !(d >= 5 && d < 8) }' |
		tee -a "$f.summary" || fail "stubborn's SIGKILL did not follow its SIGTERM by its 5 s grace"
}

prove_shape() { # prove_shape SHAPE
	local shape=$1 dir=$out/$1 pod run since until uid
	mkdir -p "$dir"
	log "== $shape: deploy/kubernetes/orker-$shape"
	kctl apply -k "/orkstack/kubernetes/proof/$shape" >/dev/null
	kctl -n orkstack rollout status deploy/orker --timeout=600s >/dev/null || {
		kctl -n orkstack describe pod -l app.kubernetes.io/name=orker >"$dir/describe.txt" || :
		fail "$shape: the Orker never became ready ($dir/describe.txt)"
	}
	pod=$(orker_pod)
	kctl -n orkstack get pod "$pod" -o yaml >"$dir/pod.yaml"
	orkctl orker list | tee "$dir/orker-list.txt"
	expect "$dir/orker-list.txt" "^$pod\\.orkstack +main .* ready " "$shape: the Orker is ready under its pod's id"
	kctl -n orkstack logs "$pod" -c orker >"$dir/orker-boot.log"
	expect "$dir/orker-boot.log" 'msg="orker started"' "$shape: the Orker booted (its bind probe passed)"

	log "$shape: artifacts, network none and default, limits and the task profile"
	run=$(orkctl run submit k8s-orker-proof --json | jq -r .run.id)
	wait_terminal "$run"
	orkctl run get "$run" >"$dir/proof-run.txt"
	orkctl run logs "$run" >"$dir/proof-logs.txt"
	orkctl run artifacts "$run" >"$dir/proof-artifacts.txt"
	orkctl run attempts "$run" >"$dir/proof-attempts.txt"
	[ "$(run_status "$run")" = SUCCEEDED ] || fail "$shape: k8s-orker-proof $run ended $(run_status "$run")"
	log "ok: $shape: k8s-orker-proof $run SUCCEEDED"
	expect "$dir/proof-logs.txt" '^\[produce\] interfaces: lo $' "$shape: network none: only the loopback interface"
	expect "$dir/proof-logs.txt" '^\[reach\] default network reached the engine Service: *HTTP/' "$shape: network default: a cluster Service answered"
	expect "$dir/proof-logs.txt" '^\[produce\] memory.max: 134217728$' "$shape: the task's memory limit is its 128Mi"
	expect "$dir/proof-logs.txt" '^\[produce\] cpu.max: 50000 100000$' "$shape: the task's cpu limit is its 0.5"
	expect "$dir/proof-logs.txt" '^\[produce\] NoNewPrivs: 1$' "$shape: the task runs with no-new-privileges"
	expect "$dir/proof-logs.txt" '^\[produce\] Seccomp: 2$' "$shape: the task runs under a seccomp filter"
	expect "$dir/proof-logs.txt" '^\[produce\] CapEff: 0000000000000000$' "$shape: the task holds no capabilities"
	expect "$dir/proof-logs.txt" '^\[produce\] root: read-only$' "$shape: the task's root filesystem is read-only"
	local sha
	sha=$(awk '/^\[produce\] [0-9a-f]{64} /{print $2}' "$dir/proof-logs.txt")
	[ -n "$sha" ] || fail "$shape: produce logged no sha256"
	expect "$dir/proof-logs.txt" "^\\[checksum\\] .*sha256:$sha\\)" "$shape: checksum read produce's artifact intact"
	expect "$dir/proof-logs.txt" "^\\[verify\\] $sha " "$shape: verify read produce's artifact intact"
	expect "$dir/proof-logs.txt" '^\[verify\] memory.max: 134217728$' "$shape: verify read compress's artifact back"
	expect "$dir/proof-artifacts.txt" '^compress +1 +OUTPUT +report.txt.gz ' "$shape: compress's artifact was committed"

	log "$shape: a cancel, and where each task's cgroup is"
	since=$(kctl -n orkstack exec "$pod" -c engine -- date +%s)
	run=$(orkctl run submit k8s-orker-cancel --json | jq -r .run.id)
	wait_started "$run" 2
	uid=$(kctl -n orkstack get pod "$pod" -o jsonpath='{.metadata.uid}' | tr - _)
	docker exec "$worker" sh -c '
		for f in /proc/[0-9]*/cgroup; do
			c=$(sed -n "s/^0:://p" "$f" 2>/dev/null) || continue
			case $c in *pod'"$uid"'.slice/*/tasks/*) ;; *) continue ;; esac
			p=${f#/proc/}; p=${p%/cgroup}
			echo "$(echo "$c" | sed "s|.*pod'"$uid"'.slice/|<pod>/|; s|cri-containerd-\([0-9a-f]\{12\}\)[0-9a-f]*\.scope|<engine \1>|") memory.max=$(cat /sys/fs/cgroup$c/memory.max) host-uid=$(awk "/^Uid:/{print \$2}" /proc/$p/status)"
		done | sort -u
		echo "at the node root: docker/ $(test -d /sys/fs/cgroup/docker && echo present || echo absent), tasks/ $(test -d /sys/fs/cgroup/tasks && echo present || echo absent)"
	' | tee "$dir/task-cgroups.txt"
	[ "$(grep -c '^<pod>/<engine [0-9a-f]*>/tasks/' "$dir/task-cgroups.txt")" -ge 2 ] ||
		fail "$shape: the two running tasks' processes are not in cgroups under the Orker pod's engine"
	log "ok: $shape: each running task's cgroup is under the Orker pod's engine container"
	expect "$dir/task-cgroups.txt" 'at the node root: docker/ absent, tasks/ absent' "$shape: no task cgroup at the node's root"
	orkctl run cancel "$run" --reason "kind-orker: a cancel reaches the container" >/dev/null
	wait_terminal "$run"
	[ "$(run_status "$run")" = CANCELED ] || fail "$shape: cancelled run $run ended $(run_status "$run")"
	log "ok: $shape: k8s-orker-cancel $run CANCELED"
	sleep 2
	until=$(kctl -n orkstack exec "$pod" -c engine -- date +%s)
	signals "$pod" "$since" "$until" >"$dir/cancel-signals.txt"
	orkctl run logs "$run" >"$dir/cancel-logs.txt"
	check_signals "$dir/cancel-signals.txt"
	expect "$dir/cancel-logs.txt" '^\[graceful\] graceful: SIGTERM at ' "$shape: graceful's trap ran"
	expect "$dir/cancel-logs.txt" '^\[stubborn\] stubborn: SIGTERM at .*ignoring it' "$shape: stubborn's trap ran"

	log "$shape: the per-task ceiling and a task's own limit"
	run=$(orkctl run submit k8s-orker-limits --json | jq -r .run.id)
	wait_terminal "$run"
	orkctl run get "$run" --json >"$dir/limits-run.json"
	orkctl run logs "$run" >"$dir/limits-logs.txt"
	jq -r '.run.taskRuns[] | "\(.taskId) \(.status) \(.error.code // "-") \(.error.message // "-")"' "$dir/limits-run.json" | tee "$dir/limits-tasks.txt"
	expect "$dir/limits-tasks.txt" '^within SUCCEEDED ' "$shape: a task at the 384Mi ceiling runs"
	expect "$dir/limits-logs.txt" '^\[within\] memory.max: 402653184$' "$shape: with the ceiling as its limit"
	expect "$dir/limits-tasks.txt" '^hog FAILED CONTAINER_EXIT_NONZERO .*exceeding its memory limit of 64Mi' "$shape: a task over its own 64Mi is killed by the kernel"
	expect "$dir/limits-tasks.txt" '^too-big FAILED RESOURCE_REQUEST_REFUSED ' "$shape: a task asking for more than the ceiling is refused"

	log "$shape: deleting the pod while two tasks run"
	run=$(orkctl run submit k8s-orker-cancel --json | jq -r .run.id)
	wait_started "$run" 2
	kctl -n orkstack exec "$pod" -c engine -- docker events \
		--filter type=container --filter event=kill --filter event=die --format "$events_format" \
		>"$dir/pod-delete-signals.txt" 2>/dev/null &
	local stream=$!
	sleep 2
	kctl -n orkstack delete pod "$pod" --wait=true >/dev/null
	wait "$stream" || :
	check_signals "$dir/pod-delete-signals.txt"
	log "ok: $shape: the engine outlived the Orker and delivered every signal"
	wait_terminal "$run"
	orkctl run get "$run" --json | jq -r '.run.taskRuns[] | "\(.taskId) \(.status) \(.error.code // "-")"' | tee "$dir/pod-delete-tasks.txt"
	expect "$dir/pod-delete-tasks.txt" '^graceful FAILED ORKER_HEARTBEAT_TIMEOUT$' "$shape: a task with no retry: ends explicitly when its Orker stops"
	kctl -n orkstack rollout status deploy/orker --timeout=600s >/dev/null || fail "$shape: the replacement pod never became ready"
	log "ok: $shape: the Deployment replaced the pod"
	# The RuntimeClass stays: the rootless measurement below runs in it too.
	kctl -n orkstack delete deployment orker --ignore-not-found --wait=true >/dev/null
	kctl -n orkstack wait --for=delete pod -l app.kubernetes.io/name=orker --timeout=300s >/dev/null 2>&1 || :
}

prove_shape userns
prove_shape privileged

log "== a work root the engine sees at another path"
kctl apply -k /orkstack/kubernetes/proof/mismatch >/dev/null
deadline=$((SECONDS + 300))
# The container restarts after each refusal, so keep the log that showed it.
until kctl -n orkstack logs -l app.kubernetes.io/name=orker -c orker --tail=-1 >"$out/mismatch-orker.log" 2>&1 &&
	grep -q WORKDIR_NOT_BINDABLE "$out/mismatch-orker.log"; do
	[ $SECONDS -lt $deadline ] || fail "the Orker did not refuse a mismatched work root within 5 minutes"
	sleep 2
done
kctl -n orkstack get pod -l app.kubernetes.io/name=orker | tee "$out/mismatch-pod.txt"
expect "$out/mismatch-orker.log" 'orker boot checks failed: WORKDIR_NOT_BINDABLE: .*bind source path does not exist: /var/lib/orkstack/work/ork-bindprobe-' \
	"the Orker refused to boot on a work root the engine cannot see"
expect "$out/mismatch-pod.txt" ' 1/2 ' "the mismatched pod never became ready"
kctl -n orkstack delete deployment orker --ignore-not-found --wait=true >/dev/null
kctl -n orkstack wait --for=delete pod -l app.kubernetes.io/name=orker --timeout=300s >/dev/null 2>&1 || :

log "== the user-namespaced engine without its RuntimeClass"
kctl apply -k /orkstack/kubernetes/proof/no-runtimeclass >/dev/null
deadline=$((SECONDS + 300))
until kctl -n orkstack logs -l app.kubernetes.io/name=orker -c engine --tail=-1 >"$out/no-runtimeclass-engine.log" 2>&1 &&
	grep -q "can't create directory" "$out/no-runtimeclass-engine.log"; do
	[ $SECONDS -lt $deadline ] || fail "the engine without its RuntimeClass did not refuse to start within 5 minutes"
	sleep 2
done
kctl -n orkstack get pod -l app.kubernetes.io/name=orker | tee "$out/no-runtimeclass-pod.txt"
expect "$out/no-runtimeclass-engine.log" "^mkdir: can't create directory '/sys/fs/cgroup/engine': " "without its RuntimeClass the engine cannot make its cgroups"
expect "$out/no-runtimeclass-pod.txt" ' 0/2 ' "and the pod never runs the Orker"
kctl -n orkstack delete deployment orker --ignore-not-found --wait=true >/dev/null
kctl -n orkstack wait --for=delete pod -l app.kubernetes.io/name=orker --timeout=300s >/dev/null 2>&1 || :

log "== Docker's rootless engine and a container's limits"
kctl apply -k /orkstack/kubernetes/proof/rootless-dind >/dev/null
kctl -n orkstack wait --for=condition=Ready pod/rootless-dind --timeout=300s >/dev/null
kctl -n orkstack exec rootless-dind -- docker run --rm --memory 64m --cpus 0.5 --pids-limit 100 "$PROBE_IMAGE" \
	sh -c 'echo "memory.max: $(cat /sys/fs/cgroup/memory.max)"; echo "cpu.max: $(cat /sys/fs/cgroup/cpu.max)"' 2>/dev/null |
	tee "$out/rootless-limits.txt"
kctl -n orkstack logs rootless-dind >"$out/rootless-daemon.log"
expect "$out/rootless-limits.txt" '^memory.max: max$' "rootless engine: --memory 64m is not applied"
expect "$out/rootless-limits.txt" '^cpu.max: max ' "rootless engine: --cpus 0.5 is not applied"
expect "$out/rootless-daemon.log" 'Running in rootless-mode without cgroups' "rootless engine: the daemon says it runs without cgroups"
kctl delete -k /orkstack/kubernetes/proof/rootless-dind --ignore-not-found >/dev/null

log "PASS: every scenario held; evidence in ${out#"$root"/}"
