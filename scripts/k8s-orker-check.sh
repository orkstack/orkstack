#!/usr/bin/env bash
# scripts/k8s-orker-check.sh: check that the Kubernetes Orker bases
# (deploy/kubernetes/, docs/DEPLOY.md "Orkers on Kubernetes") work on a
# cluster you already have, before you trust them with real runs.
#
#   scripts/k8s-orker-check.sh --server URL --orker-image REF [options]
#
#   --server URL            the ork engine's API, as orkctl reaches it from here
#   --orker-image REF       the orker image the check deploys, by digest
#   --kubeconfig PATH       the cluster, as kubectl reads it (default: kubectl's own)
#   --context NAME          a context of that kubeconfig (default: its current one)
#   --settings-from NS      the namespace whose orkstack ConfigMap and orkstack-s3
#                           Secret the check copies (default: orkstack)
#   --namespace NAME        the namespace the check creates and deletes
#                           (default: orkstack-check; refused if it exists)
#   --ork-namespace NAME    the OrkStack namespace its runs are submitted in
#                           (default: default). Its allowed_secrets must be
#                           empty or allow the sources the secrets Orkflow
#                           names: file:/etc/orkstack/task-secrets/*,
#                           env:ORK_S3_SECRET_KEY and env:POD_NAME; the check
#                           expects the Orker, not the namespace, to refuse
#                           the last two.
#   --shapes LIST           userns,privileged (the default), or one of them
#   --pull-policy POLICY    the orker image's pull policy (default: IfNotPresent)
#   --registry-host H:PORT  a registry the engine pulls task images from, with
#   --registry-ca FILE      its private certificate authority (both or neither)
#   --out DIR               the evidence (default: .data/k8s-orker-check/<stamp>)
#
# It needs, first: kubectl (KUBECTL names another), jq, Go to build orkctl
# from this tree (ORKCTL names another), an ork engine at --server whose
# catalog resolves the shell and file activities, and the settings your
# Orkers read in --settings-from. Every missing prerequisite is refused with
# exit 77 and its name: the cluster, its version and the orkstack-engine
# RuntimeClass (for the user-namespaced shape), the settings and the engine,
# before anything is created; then, from inside the cluster, Temporal, the
# object store and the registry, a user-namespaced pod in that RuntimeClass
# that may make cgroups of its own, and Pod Security (or any admission
# policy) admitting each shape.
#
# For each shape it then checks that a run passes artifacts between tasks,
# runs tasks with no network and one on the default network, and gives each
# task its own limits and the Orker's security profile; that a task reads a
# key of the orkstack-task-secrets Secret, which the bases' secret floor
# lists, and that the floor refuses what it does not list: the Orker's own
# ORK_S3_SECRET_KEY, and a variable of its environment; that each task's
# cgroup is inside the engine container's; that a cancel reaches each task's
# container as SIGTERM, then SIGKILL after that task's own grace; that the
# Orker declares the capacity its pod gives it; that a task may ask for up to
# ORK_ORKER_MAX_MEMORY, one that asks for more is refused at submit, and one
# that uses more than it asked for is killed in its own cgroup; and that
# deleting the pod stops the Orker first while the engine delivers the
# signals; and that with a stop grace a pod deleted mid-task lets the task
# finish on its one attempt, while one deleted at once loses it explicitly.
# Then that the Orker
# refuses to boot when the engine sees the work root at another path, and
# that the user-namespaced engine does not start without its RuntimeClass.
#
# What it creates, and what it leaves: in the cluster, the namespace and
# everything in it, the orkstack-task-secrets Secret with its one key among
# it, deleted at the end (KEEP=1 leaves it); nothing cluster-wide. In
# OrkStack: the six check Orkflows in --ork-namespace,
# created once and left (an identical create changes nothing), their runs,
# kept as that namespace's retention says, and Orkers of the pool k8s-check
# while the check runs, which that namespace must allow. Exit 0: every check
# held; 1: a check failed, with the namespace's state in the evidence; 2: a
# bad invocation; 77: a missing prerequisite.
set -euo pipefail

PROBE_IMAGE=docker.io/library/alpine:3.24@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf
POOL=k8s-check

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

usage() {
	sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p}' "$0" >&2
	exit 2
}
bad() {
	echo "k8s-orker-check: $*" >&2
	exit 2
}
server="" image="" kubeconfig="" context="" settings=orkstack ns=orkstack-check orkns=default
shapes=userns,privileged pull=IfNotPresent registry_host="" registry_ca="" out=""
while [ $# -gt 0 ]; do
	case $1 in
	-h | --help) usage ;;
	--server | --orker-image | --kubeconfig | --context | --settings-from | --namespace | --ork-namespace | \
		--shapes | --pull-policy | --registry-host | --registry-ca | --out)
		[ $# -ge 2 ] || bad "$1 needs a value"
		case $1 in
		--server) server=$2 ;;
		--orker-image) image=$2 ;;
		--kubeconfig) kubeconfig=$2 ;;
		--context) context=$2 ;;
		--settings-from) settings=$2 ;;
		--namespace) ns=$2 ;;
		--ork-namespace) orkns=$2 ;;
		--shapes) shapes=$2 ;;
		--pull-policy) pull=$2 ;;
		--registry-host) registry_host=$2 ;;
		--registry-ca) registry_ca=$2 ;;
		--out) out=$2 ;;
		esac
		shift 2
		;;
	*) bad "unknown argument $1 (--help lists them)" ;;
	esac
done
[ -n "$server" ] || bad "--server is required: the ork engine's API, as orkctl reaches it"
[ -n "$image" ] || bad "--orker-image is required: the orker image to deploy, by digest"
case $image in *[[:space:]\"\']*) bad "--orker-image $image holds a space or a quote" ;; esac
case $pull in IfNotPresent | Always | Never) ;; *) bad "--pull-policy is IfNotPresent, Always or Never, not $pull" ;; esac
case ,$shapes, in ,userns, | ,privileged, | ,userns,privileged, | ,privileged,userns,) ;; *) bad "--shapes is userns, privileged or both, not $shapes" ;; esac
if { [ -n "$registry_host" ] && [ -z "$registry_ca" ]; } || { [ -z "$registry_host" ] && [ -n "$registry_ca" ]; }; then
	bad "--registry-host and --registry-ca come together"
fi
[ -z "$registry_ca" ] || [ -r "$registry_ca" ] || bad "--registry-ca $registry_ca cannot be read"
case $registry_host in *[[:space:]\"\']*) bad "--registry-host $registry_host holds a space or a quote" ;; esac
for name in "$ns" "$settings"; do
	[[ $name =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || bad "$name is not a Kubernetes namespace name"
done
userns=false privileged=false
case ,$shapes, in *,userns,*) userns=true ;; esac
case ,$shapes, in *,privileged,*) privileged=true ;; esac

out=${out:-$root/.data/k8s-orker-check/$(date -u +%Y%m%dT%H%M%SZ)}
mkdir -p "$out"
exec > >(tee "$out/check.log") 2>&1

log() { printf '%s %s\n' "$(date -u +%T)" "$*"; }
refuse() {
	log "MISSING: $*"
	exit 77
}
read -r -a kubectl <<<"${KUBECTL:-kubectl}"
kflags=()
[ -z "$kubeconfig" ] || kflags+=(--kubeconfig "$kubeconfig")
[ -z "$context" ] || kflags+=(--context "$context")
k() { "${kubectl[@]}" "${kflags[@]}" "$@"; }
created=false
# What a failed check leaves for whoever reads it: its namespace as it was.
namespace_state() {
	local pod
	echo "== kubectl -n $ns get pods -o wide"
	k -n "$ns" get pods -o wide 2>&1 || :
	echo "== kubectl -n $ns get events --sort-by=.lastTimestamp"
	k -n "$ns" get events --sort-by=.lastTimestamp 2>&1 || :
	echo "== kubectl -n $ns describe pods"
	k -n "$ns" describe pods 2>&1 || :
	for pod in $(k -n "$ns" get pods -o name 2>/dev/null); do
		echo "== kubectl -n $ns logs $pod (every container, the last 200 lines)"
		k -n "$ns" logs "$pod" --all-containers --prefix --tail=200 2>&1 || :
	done
}
fail() {
	trap - ERR
	log "FAIL: $*"
	if $created; then
		namespace_state >"$out/namespace-state.txt" 2>&1 || :
		log "the namespace's state at the failure: $out/namespace-state.txt"
	fi
	exit 1
}
expect() { # expect FILE PATTERN WHAT: the evidence file holds a line matching PATTERN
	grep -Eq -- "$2" "$1" || fail "$3: no line of $(basename "$1") matches /$2/"
	log "ok: $3"
}
work=$(mktemp -d)
cleanup() {
	rm -rf "$work"
	if $created; then
		if [ "${KEEP:-}" = 1 ]; then
			log "KEEP=1: namespace $ns left in place (kubectl delete namespace $ns)"
		else
			k delete namespace "$ns" --wait=true --timeout=300s >/dev/null 2>&1 ||
				log "namespace $ns could not be deleted; delete it by hand"
		fi
	fi
}
trap cleanup EXIT
# A command that fails where no check expected it fails the check by name,
# with the namespace's state, rather than ending it with no reason. A
# command substitution leaves it to the command that holds it.
set -E
trap 'exit_status=$?; [ "$BASH_SUBSHELL" -gt 0 ] || fail "line $LINENO: \`$BASH_COMMAND\` exited $exit_status"' ERR

log "== prerequisites"
for tool in "${kubectl[0]}" jq; do
	command -v "$tool" >/dev/null || refuse "$tool is not installed"
done
version=$(k version -o json 2>"$out/kubectl-version.err") ||
	refuse "kubectl cannot reach the cluster: $(tail -n 1 "$out/kubectl-version.err")"
minor=$(jq -r '.serverVersion.minor' <<<"$version" | tr -dc 0-9)
log "cluster: Kubernetes $(jq -r '.serverVersion.gitVersion' <<<"$version")"
if $userns; then
	[ "${minor:-0}" -ge 33 ] ||
		refuse "the user-namespaced shape needs Kubernetes 1.33 or later, with user namespaces on; this cluster is 1.$minor (--shapes privileged checks the other alone)"
	handler=$(k get runtimeclass orkstack-engine -o jsonpath='{.handler}' 2>/dev/null) ||
		refuse "the user-namespaced shape needs the RuntimeClass orkstack-engine: declare its containerd handler on the nodes and apply deploy/kubernetes/orker-userns/runtimeclass.yaml (docs/DEPLOY.md, \"Orkers on Kubernetes\")"
	log "RuntimeClass orkstack-engine: handler $handler"
fi
k -n "$settings" get configmap orkstack >/dev/null 2>&1 ||
	refuse "no ConfigMap orkstack in namespace $settings: the settings your Orkers read (--settings-from)"
k -n "$settings" get secret orkstack-s3 >/dev/null 2>&1 ||
	refuse "no Secret orkstack-s3 in namespace $settings: the object store's keys your Orkers read (--settings-from)"
[ "$(k -n "$settings" get configmap orkstack -o jsonpath='{.data.ORK_ARTIFACT_BACKEND}')" = s3 ] ||
	refuse "the orkstack ConfigMap in $settings does not set ORK_ARTIFACT_BACKEND=s3, which every Orker of a cluster needs"
if [ -n "${ORKCTL:-}" ]; then
	orkctl_bin=$ORKCTL
else
	command -v go >/dev/null || refuse "go is not installed, to build orkctl (or set ORKCTL)"
	go build -o "$work/orkctl" ./cmd/orkctl
	orkctl_bin=$work/orkctl
fi
orkctl() { "$orkctl_bin" --server "$server" -n "$orkns" "$@"; }
orkctl orkflow list >/dev/null 2>"$out/engine.err" ||
	refuse "the engine at $server does not answer: $(tail -n 1 "$out/engine.err")"
log "engine: $server answers"
if k get namespace "$ns" >/dev/null 2>&1; then
	refuse "namespace $ns exists; the check creates its own (--namespace names another)"
fi

# The namespace, at the Pod Security level the bases need, and the settings.
k create namespace "$ns" >/dev/null
created=true
k label namespace "$ns" pod-security.kubernetes.io/enforce=privileged >/dev/null ||
	refuse "namespace $ns cannot take the Pod Security level privileged, which both shapes need"
for object in configmap/orkstack secret/orkstack-s3; do
	k -n "$settings" get "$object" -o json |
		jq --arg ns "$ns" '{apiVersion, kind, type, data, metadata: {name: .metadata.name, namespace: $ns}} | with_entries(select(.value != null))' |
		k apply -f - >/dev/null
done
[ -z "$registry_ca" ] || k -n "$ns" create configmap registry-ca --from-file=ca.crt="$registry_ca" >/dev/null
# The task secret the bases' floor lists: one key of orkstack-task-secrets,
# which every Orker of the check mounts, made here so its pods start with it.
# The check prints its hash, never its value.
od -An -N32 -tx1 /dev/urandom | tr -d ' \n' >"$work/k8s-check-value"
k -n "$ns" create secret generic orkstack-task-secrets --from-file=k8s-check-value="$work/k8s-check-value" >/dev/null
if command -v sha256sum >/dev/null; then
	secret_sha=$(sha256sum "$work/k8s-check-value" | cut -d ' ' -f 1)
else
	secret_sha=$(shasum -a 256 "$work/k8s-check-value" | cut -d ' ' -f 1)
fi

# Run a probe pod to its end; say what it printed, or why it never ran.
probe() { # probe NAME WHAT, the pod's manifest on stdin
	local name=$1 what=$2 phase="" deadline=$((SECONDS + 180))
	k -n "$ns" apply -f - >/dev/null 2>"$out/probe-$name.err" ||
		refuse "the cluster refuses the probe pod $name (Pod Security or an admission policy): $(tail -n 1 "$out/probe-$name.err")"
	while :; do
		phase=$(k -n "$ns" get pod "$name" -o jsonpath='{.status.phase}' 2>/dev/null || :)
		case $phase in Succeeded | Failed) break ;; esac
		if [ $SECONDS -ge $deadline ]; then
			k -n "$ns" get events --field-selector "involvedObject.name=$name" --sort-by=.lastTimestamp >"$out/probe-$name.events" 2>&1 || :
			refuse "$what: pod $name never ran: $(grep -E 'Warning|Failed' "$out/probe-$name.events" | tail -n 1)"
		fi
		sleep 2
	done
	k -n "$ns" logs "$name" >"$out/probe-$name.log" 2>&1 || :
	sed 's/^/  /' "$out/probe-$name.log"
	local why
	why=$(grep -E ' NOT |rror|denied|can.t|ermission' "$out/probe-$name.log" | head -n 3 | paste -sd ';' - || :)
	[ "$phase" = Succeeded ] || refuse "$what: ${why:-$(tail -n 1 "$out/probe-$name.log")}"
	k -n "$ns" delete pod "$name" --wait=false >/dev/null
}

log "reachability from inside the cluster"
probe reach "Temporal, the object store or the registry is not reachable from the cluster" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: reach
spec:
  restartPolicy: Never
  containers:
    - name: reach
      image: $PROBE_IMAGE
      envFrom:
        - configMapRef:
            name: orkstack
      env:
        - name: REGISTRY_HOST
          value: "$registry_host"
      command:
        - sh
        - -c
        - |
          ok=0
          try() {
            if nc -z -w 5 "\$2" "\$3"; then echo "\$1 \$2:\$3 reachable"; else echo "\$1 \$2:\$3 NOT reachable"; ok=1; fi
          }
          t=\${ORK_TEMPORAL_ADDRESS:-localhost:7233}
          try Temporal "\${t%:*}" "\${t##*:}"
          s=\${ORK_S3_ENDPOINT#*://}; s=\${s%%/*}
          case \$ORK_S3_ENDPOINT in https://*) d=443 ;; *) d=80 ;; esac
          case \$s in *:*) try "object store" "\${s%:*}" "\${s##*:}" ;; *) try "object store" "\$s" "\$d" ;; esac
          [ -z "\$REGISTRY_HOST" ] || try registry "\${REGISTRY_HOST%:*}" "\${REGISTRY_HOST##*:}"
          exit \$ok
EOF
if $userns; then
	log "a user-namespaced pod in the orkstack-engine RuntimeClass"
	probe userns "user-namespaced pods in the orkstack-engine RuntimeClass cannot make cgroups of their own here (its handler needs cgroup_writable = true)" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: userns
spec:
  hostUsers: false
  runtimeClassName: orkstack-engine
  restartPolicy: Never
  containers:
    - name: userns
      image: $PROBE_IMAGE
      command:
        - sh
        - -c
        - |
          set -e
          echo "uid_map: \$(cat /proc/self/uid_map)"
          mkdir /sys/fs/cgroup/orkstack-check
          rmdir /sys/fs/cgroup/orkstack-check
          echo "the pod may make cgroups of its own"
EOF
fi

# The bases as they ship, with deploy/kubernetes/check's components and the
# check's options, written into a copy of deploy/kubernetes.
cp -r deploy/kubernetes "$work/"
overlay() { # overlay SHAPE [COMPONENT]: the rendered manifests, on stdout
	local shape=$1 extra=${2:-} dir=$work/kubernetes/check/run-$1-${2:-plain}
	mkdir -p "$dir"
	{
		echo "apiVersion: kustomize.config.k8s.io/v1beta1"
		echo "kind: Kustomization"
		echo "namespace: $ns"
		echo "resources:"
		echo "  - ../../orker-$shape"
		echo "components:"
		echo "  - ../common"
		[ -z "$extra" ] || echo "  - ../$extra"
		echo "patches:"
		echo "  - target:"
		echo "      kind: Deployment"
		echo "      name: orker"
		echo "    patch: |-"
		echo "      - op: replace"
		echo "        path: /spec/template/spec/containers/0/image"
		echo "        value: \"$image\""
		echo "      - op: add"
		echo "        path: /spec/template/spec/containers/0/imagePullPolicy"
		echo "        value: $pull"
		if [ -n "$registry_ca" ]; then
			echo "  - target:"
			echo "      kind: Deployment"
			echo "      name: orker"
			echo "    patch: |-"
			echo "      apiVersion: apps/v1"
			echo "      kind: Deployment"
			echo "      metadata:"
			echo "        name: orker"
			echo "      spec:"
			echo "        template:"
			echo "          spec:"
			echo "            initContainers:"
			echo "              - name: engine"
			echo "                volumeMounts:"
			echo "                  - name: registry-ca"
			echo "                    mountPath: /etc/docker/certs.d/$registry_host"
			echo "                    readOnly: true"
			echo "            volumes:"
			echo "              - name: registry-ca"
			echo "                configMap:"
			echo "                  name: registry-ca"
		fi
	} >"$dir/kustomization.yaml"
	k kustomize "$dir"
}
for shape in userns privileged; do
	${!shape} || continue
	log "Pod Security and admission for the $shape shape"
	overlay "$shape" >"$out/$shape.yaml"
	k create --dry-run=client -f "$out/$shape.yaml" -o json |
		jq --arg ns "$ns" 'if .kind == "List" then .items[] else . end | select(.kind == "Deployment") |
			{apiVersion: "v1", kind: "Pod", metadata: {name: "orkstack-check-admission", namespace: $ns}, spec: .spec.template.spec}' >"$out/$shape-pod.json"
	k create --dry-run=server -f "$out/$shape-pod.json" >/dev/null 2>"$out/$shape-admission.err" ||
		refuse "Pod Security or an admission policy refuses the $shape shape in $ns: $(tail -n 1 "$out/$shape-admission.err")"
	log "ok: the $shape shape is admitted"
done

for flow in deploy/kubernetes/check/orkflows/*.ork.yaml; do
	orkctl orkflow create "$flow" >/dev/null
done

orker_pod() { k -n "$ns" get pod -l app.kubernetes.io/name=orker --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}'; }
run_status() { orkctl run get "$1" -j | jq -r .run.status; }
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
wait_rerun() { # wait_rerun RUN TASK...: until each task is RUNNING again after a hand-back, at most 5 minutes
	local run=$1 deadline=$((SECONDS + 300)) task got
	shift
	for task; do
		until got=$(orkctl run get "$run" -j | jq -r --arg t "$task" '.run.taskRuns[] | select(.taskId == $t) | "\(.status) \(.interruptions.handedBack // 0)"') &&
			[ "${got%% *}" = RUNNING ] && [ "${got##* }" -ge 1 ]; do
			[ $SECONDS -lt $deadline ] || fail "run $run: $task is '$got', not running again after its hand-back within 5 minutes"
			sleep 2
		done
	done
}
# The engine's own record of what it signalled, as "<unix ns> <action> <task> signal= exit=".
events_format='{{.TimeNano}} {{.Action}} {{index .Actor.Attributes "ork.task"}} signal={{index .Actor.Attributes "signal"}} exit={{index .Actor.Attributes "exitCode"}}'
signals() { # signals POD SINCE UNTIL
	k -n "$ns" exec "$1" -c engine -- docker events --since "$2" --until "$3" \
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
remove_orkers() {
	k -n "$ns" delete deployment orker --ignore-not-found --wait=true >/dev/null
	k -n "$ns" wait --for=delete pod -l app.kubernetes.io/name=orker --timeout=300s >/dev/null 2>&1 || :
}

check_shape() { # check_shape SHAPE
	local shape=$1 dir=$out/$1 pod run since until uid
	mkdir -p "$dir"
	log "== $shape: deploy/kubernetes/orker-$shape"
	k apply -f "$out/$shape.yaml" >/dev/null
	k -n "$ns" rollout status deploy/orker --timeout=600s >/dev/null ||
		fail "$shape: the Orker never became ready (a pull of $image, the engine's start, or the Orker's boot checks; see namespace-state.txt)"
	pod=$(orker_pod)
	k -n "$ns" get pod "$pod" -o yaml >"$dir/pod.yaml"
	orkctl orker list | tee "$dir/orker-list.txt"
	expect "$dir/orker-list.txt" "^$pod\\.$ns +$POOL .* ready " "$shape: the Orker is ready in pool $POOL under its pod's id"
	expect "$dir/orker-list.txt" "^$pod\\.$ns .* ready .* 1\\.5 cpu, 1536Mi " "$shape: the Orker admits tasks within the capacity its pod declares"
	k -n "$ns" logs "$pod" -c orker >"$dir/orker-boot.log"
	expect "$dir/orker-boot.log" 'msg="orker started"' "$shape: the Orker booted (its bind probe passed)"
	k -n "$ns" exec "$pod" -c engine -- cat /proc/self/uid_map | tee "$dir/engine-uid-map.txt"
	if [ "$shape" = userns ]; then
		expect "$dir/engine-uid-map.txt" '^ *0 +[1-9][0-9]* +[0-9]+$' "$shape: the engine's root is an unprivileged uid range of the node"
	else
		expect "$dir/engine-uid-map.txt" '^ *0 +0 +4294967295$' "$shape: the engine runs as the node's root, as the fallback says"
	fi

	log "$shape: artifacts, network none and default, limits and the task profile"
	run=$(orkctl run submit k8s-orker-proof -j | jq -r .run.id)
	wait_terminal "$run"
	orkctl run get "$run" >"$dir/proof-run.txt"
	orkctl run logs "$run" >"$dir/proof-logs.txt"
	orkctl run artifacts "$run" >"$dir/proof-artifacts.txt"
	orkctl run attempts "$run" >"$dir/proof-attempts.txt"
	if [ "$(run_status "$run")" != SUCCEEDED ]; then
		orkctl run get "$run" -j | jq -r '.run.taskRuns[] | select(.error) | "\(.taskId) \(.error.code): \(.error.message)"' >"$dir/proof-errors.txt" || :
		if grep -q ' IMAGE_PULL_FAILED: ' "$dir/proof-errors.txt"; then
			fail "$shape: the engine could not pull a task's image ($(grep -m1 ' IMAGE_PULL_FAILED: ' "$dir/proof-errors.txt" | cut -c1-300)); a registry with a private certificate authority needs --registry-host and --registry-ca"
		fi
		fail "$shape: k8s-orker-proof $run ended $(run_status "$run"): $(head -n 1 "$dir/proof-errors.txt" | cut -c1-300)"
	fi
	log "ok: $shape: k8s-orker-proof $run SUCCEEDED"
	expect "$dir/proof-logs.txt" '^\[produce\] interfaces: lo $' "$shape: network none: only the loopback interface"
	expect "$dir/proof-logs.txt" '^\[reach\] default network reached the Kubernetes API Service: *HTTP/' "$shape: network default: a cluster Service answered"
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

	log "$shape: a task's secrets: the floor admits what it lists and refuses what it does not"
	run=$(orkctl run submit k8s-orker-secrets -j | jq -r .run.id)
	wait_terminal "$run"
	orkctl run get "$run" -j >"$dir/secrets-run.json"
	orkctl run logs "$run" >"$dir/secrets-logs.txt"
	jq -r '.run.taskRuns[] | "\(.taskId) \(.status) \(.error.code // "-") \(.error.message // "-")"' "$dir/secrets-run.json" | tee "$dir/secrets-tasks.txt"
	expect "$dir/secrets-tasks.txt" '^listed SUCCEEDED ' "$shape: a task reads a key of orkstack-task-secrets, which the floor lists"
	expect "$dir/secrets-logs.txt" "^\\[listed\\] secret sha256: $secret_sha\$" "$shape: and is given the value the check put there"
	expect "$dir/secrets-tasks.txt" '^own FAILED SECRET_NOT_PERMITTED .*"env:ORK_S3_SECRET_KEY" is this orker.s own' \
		"$shape: the floor refuses what it does not list: the Orker's own ORK_S3_SECRET_KEY"
	expect "$dir/secrets-tasks.txt" '^unlisted FAILED SECRET_NOT_PERMITTED .*"env:POD_NAME" matches none of file:/etc/orkstack/task-secrets/\*' \
		"$shape: the floor refuses what it does not list: POD_NAME, a variable of the Orker's environment"
	! grep -q 'staged a secret the floor does not list' "$dir/secrets-logs.txt" ||
		fail "$shape: a task whose secret the floor does not list ran"
	! grep -qF -f "$work/k8s-check-value" "$dir/secrets-logs.txt" "$dir/secrets-run.json" ||
		fail "$shape: the secret's value appears in the run's logs or its record"
	log "ok: $shape: the secret's value appears nowhere in the run's logs or its record"

	log "$shape: a cancel, and where each task's cgroup is"
	since=$(k -n "$ns" exec "$pod" -c engine -- date +%s)
	run=$(orkctl run submit k8s-orker-cancel -j | jq -r .run.id)
	wait_started "$run" 2
	# Read from inside the engine: its PID 1 lives in <its container's
	# cgroup>/engine, and each task's process is in its PID namespace.
	k -n "$ns" exec "$pod" -c engine -- sh -c '
		self=$(sed -n "s/^0:://p" /proc/1/cgroup)
		box=${self%/engine}
		echo "engine container cgroup: ${box:-/ (its own cgroup namespace)}"
		for id in $(docker ps -q --filter label=ork.task); do
			pid=$(docker inspect -f "{{.State.Pid}}" "$id")
			c=$(sed -n "s/^0:://p" "/proc/$pid/cgroup")
			case $c in "$box"/tasks/*) where=inside ;; *) where=OUTSIDE ;; esac
			echo "task $(docker inspect -f "{{index .Config.Labels \"ork.task\"}}" "$id") cgroup=$c memory.max=$(cat "/sys/fs/cgroup$c/memory.max") $where"
		done' | tee "$dir/task-cgroups.txt"
	{ [ "$(grep -c '^task .* inside$' "$dir/task-cgroups.txt")" -ge 2 ] && ! grep -q ' OUTSIDE$' "$dir/task-cgroups.txt"; } ||
		fail "$shape: the running tasks' cgroups are not all under the engine container's own"
	log "ok: $shape: each running task's cgroup is under the engine container's"
	if [ "$shape" = privileged ]; then
		uid=$(k -n "$ns" get pod "$pod" -o jsonpath='{.metadata.uid}')
		grep -Eq "^engine container cgroup: .*pod(${uid//-/_}|$uid)" "$dir/task-cgroups.txt" ||
			fail "$shape: the engine container's cgroup is not under its pod's"
		log "ok: $shape: and the engine container's is under its pod's, which the kubelet accounts for"
	fi
	orkctl run cancel "$run" --reason "k8s-orker-check: a cancel reaches the container" >/dev/null
	wait_terminal "$run"
	[ "$(run_status "$run")" = CANCELED ] || fail "$shape: cancelled run $run ended $(run_status "$run")"
	log "ok: $shape: k8s-orker-cancel $run CANCELED"
	sleep 2
	until=$(k -n "$ns" exec "$pod" -c engine -- date +%s)
	signals "$pod" "$since" "$until" >"$dir/cancel-signals.txt"
	orkctl run logs "$run" >"$dir/cancel-logs.txt"
	check_signals "$dir/cancel-signals.txt"
	expect "$dir/cancel-logs.txt" '^\[graceful\] graceful: SIGTERM at ' "$shape: graceful's trap ran"
	expect "$dir/cancel-logs.txt" '^\[stubborn\] stubborn: SIGTERM at .*ignoring it' "$shape: stubborn's trap ran"

	log "$shape: the per-task cap and a task's own limit"
	if orkctl run submit k8s-orker-too-big -j >"$dir/too-big-submit.json" 2>"$dir/too-big-submit.err"; then
		fail "$shape: a task asking for more than the per-task cap was accepted as run $(jq -r .run.id "$dir/too-big-submit.json"), so the submit did not ask the pool's Orkers for their caps"
	fi
	cat "$dir/too-big-submit.err"
	# The refusal's reason, task and pool, then each cap the Orker reported on
	# its own: the sentence around the numbers is the engine's to word.
	expect "$dir/too-big-submit.err" 'failed_precondition: RESOURCES_UNSATISFIABLE: task "too-big" requests memory 512Mi, .*pool "k8s-check"' \
		"$shape: a task asking for more than the 384Mi cap is refused at submit"
	expect "$dir/too-big-submit.err" 'RESOURCES_UNSATISFIABLE: .*[^0-9.]0\.5 cpu\b' \
		"$shape: naming the 0.5 cpu cap its Orker reported"
	expect "$dir/too-big-submit.err" 'RESOURCES_UNSATISFIABLE: .*[^0-9]402653184 bytes\b' \
		"$shape: and the 384Mi memory cap"
	orkctl run list -F orkflow_name=k8s-orker-too-big -j >"$dir/too-big-runs.json"
	[ "$(jq '.runs // [] | length' "$dir/too-big-runs.json")" = 0 ] || fail "$shape: the refused submit left a run of k8s-orker-too-big"
	log "ok: $shape: and left no run behind"
	run=$(orkctl run submit k8s-orker-limits -j | jq -r .run.id)
	wait_terminal "$run"
	orkctl run get "$run" -j >"$dir/limits-run.json"
	orkctl run logs "$run" >"$dir/limits-logs.txt"
	jq -r '.run.taskRuns[] | "\(.taskId) \(.status) \(.error.code // "-") \(.error.message // "-")"' "$dir/limits-run.json" | tee "$dir/limits-tasks.txt"
	expect "$dir/limits-tasks.txt" '^within SUCCEEDED ' "$shape: a task at the 384Mi cap runs"
	expect "$dir/limits-logs.txt" '^\[within\] memory.max: 402653184$' "$shape: with the cap as its limit"
	expect "$dir/limits-tasks.txt" '^hog FAILED CONTAINER_EXIT_NONZERO .*exceeding its memory limit of 64Mi' "$shape: a task over its own 64Mi is killed by the kernel"

	log "$shape: deleting the pod while two tasks run"
	run=$(orkctl run submit k8s-orker-cancel -j | jq -r .run.id)
	wait_started "$run" 2
	k -n "$ns" exec "$pod" -c engine -- docker events \
		--filter type=container --filter event=kill --filter event=die --format "$events_format" \
		>"$dir/pod-delete-signals.txt" 2>/dev/null &
	local stream=$!
	sleep 2
	k -n "$ns" delete pod "$pod" --wait=true >/dev/null
	wait "$stream" || :
	check_signals "$dir/pod-delete-signals.txt"
	log "ok: $shape: the engine outlived the Orker and delivered every signal"
	k -n "$ns" rollout status deploy/orker --timeout=600s >/dev/null || fail "$shape: the replacement pod never became ready"
	log "ok: $shape: the Deployment replaced the pod"
	# The stopping Orker hands both back and reports them itself: graceful's
	# exit 0 after SIGTERM is never committed (docs/OCI_ACTIVITY_CONTRACT.md
	# §6.3). A hand-back is an interruption (AGENTS.md §4): neither task has a
	# retry:, and both run again on the replacement pod without spending one.
	wait_rerun "$run" graceful stubborn
	orkctl run get "$run" -j | jq -r '.run.taskRuns[] | "\(.taskId) \(.status) handed_back=\(.interruptions.handedBack // 0) own=\(.interruptions.ownAttemptsUsed // 0)"' | tee "$dir/pod-delete-tasks.txt"
	expect "$dir/pod-delete-tasks.txt" '^graceful RUNNING handed_back=1 own=0$' "$shape: a task with no retry: runs again when its Orker stops, spending no attempt"
	expect "$dir/pod-delete-tasks.txt" '^stubborn RUNNING handed_back=1 own=0$' "$shape: a task that ignores SIGTERM runs again too"
	orkctl run cancel "$run" --reason "k8s-orker-check: the re-run tasks are cancelled" >/dev/null
	wait_terminal "$run"
	orkctl run get "$run" -j | jq -r '.run.status, (.run.taskRuns[] | "\(.taskId) \(.status)")' | tee "$dir/pod-delete-cancel.txt"
	expect "$dir/pod-delete-cancel.txt" '^CANCELED$' "$shape: the re-run tasks' run is cancelled"
	expect "$dir/pod-delete-cancel.txt" '^graceful CANCELED$' "$shape: graceful ends cancelled"
	expect "$dir/pod-delete-cancel.txt" '^stubborn CANCELED$' "$shape: stubborn ends cancelled"
	check_stop_grace "$shape" "$dir"
	remove_orkers
}

# The pod's own Orker, by the stop timeout it was given: Running and not being
# deleted, so neither the pod it replaced nor one still terminating.
graced_pod() { # graced_pod SECONDS
	k -n "$ns" get pod -l app.kubernetes.io/name=orker -o json |
		jq -r --argjson t "$1" '.items[] | select(.metadata.deletionTimestamp == null and .status.phase == "Running" and .spec.terminationGracePeriodSeconds == $t) | .metadata.name' |
		head -n 1
}

# Drain-first on Kubernetes (docs/DEPLOY.md "Removing an Orker safely"): with a
# stop grace and a stop timeout that covers it, a pod deleted while a task runs
# lets the task finish and the run succeed on its one attempt, and the Orker
# exits as soon as it is idle, before the grace ends. A pod deleted at once
# (--grace-period=0) gives no grace: its task is lost, and ends explicitly, as
# stopped by its runtime or once its heartbeats stop. The stop timeout is the grace (3 min) plus the tail: 30 s
# of task cancellation, 10 min of hand-back reports, 75 s of open poll and 10 s.
# Until the pool's only Orker is the pod's own, so the next task can run on no
# other: a pod still stopping from an earlier deletion has left the inventory.
only_orker() { # only_orker POD
	local deadline=$((SECONDS + 300)) orkers
	while :; do
		orkers=$(orkctl orker list -F "pool=$POOL" -j | jq -r '[.orkers // [] | .[].orkerId] | sort | join(" ")')
		[ "$orkers" = "$1.$ns" ] && return 0
		[ $SECONDS -lt $deadline ] || fail "pool $POOL still has the Orkers '$orkers' after 5 minutes, want only $1.$ns"
		sleep 2
	done
}

check_stop_grace() { # check_stop_grace SHAPE DIR
	local shape=$1 dir=$2 grace=3m timeout=900 pod run started took
	log "$shape: deleting the pod within its stop grace ($grace)"
	# One pod at a time: the node has room for one, so a rolling update's
	# surge pod would wait Pending behind the pod it replaces.
	k -n "$ns" scale deploy/orker --replicas=0 >/dev/null
	k -n "$ns" wait --for=delete pod -l app.kubernetes.io/name=orker --timeout=300s >/dev/null 2>&1 || :
	k -n "$ns" patch deploy/orker --type=strategic -p "$(jq -nc --arg g "$grace" --argjson t "$timeout" \
		'{spec: {template: {spec: {terminationGracePeriodSeconds: $t, containers: [{name: "orker", env: [{name: "ORK_ORKER_STOP_GRACE", value: $g}]}]}}}}')" >/dev/null
	k -n "$ns" scale deploy/orker --replicas=1 >/dev/null
	k -n "$ns" rollout status deploy/orker --timeout=600s >/dev/null || fail "$shape: the Orker with a stop grace never became ready"
	pod=$(graced_pod "$timeout")
	[ -n "$pod" ] || fail "$shape: no running Orker pod has the ${timeout} s stop timeout"
	only_orker "$pod"
	run=$(orkctl run submit k8s-orker-drain -j | jq -r .run.id)
	wait_started "$run" 1
	# The stream replays the log from its start; wait for its first line so the
	# stopping line, written at the signal, cannot be missed.
	k -n "$ns" logs -f "$pod" -c orker >"$dir/grace-orker.log" 2>"$dir/grace-orker.err" &
	local logs=$! deadline=$((SECONDS + 60))
	until [ -s "$dir/grace-orker.log" ]; do
		[ $SECONDS -lt $deadline ] || fail "$shape: the Orker's log stream never connected"
		sleep 1
	done
	started=$SECONDS
	k -n "$ns" delete pod "$pod" --wait=true >/dev/null 2>"$dir/grace-delete.err" ||
		fail "$shape: deleting the pod failed: $(tail -n 1 "$dir/grace-delete.err")"
	took=$((SECONDS - started))
	kill "$logs" 2>/dev/null || :
	wait "$logs" 2>/dev/null || :
	wait_terminal "$run"
	orkctl run get "$run" -j | jq -r '.run.status, (.run.taskRuns[] | "\(.taskId) \(.status) attempt=\(.attempt) orker=\(.orkerId // "-")")' | tee "$dir/grace-tasks.txt"
	expect "$dir/grace-orker.log" 'msg="stopping: taking no new task or pin' "$shape: the deleted pod's Orker stopped drain-first"
	expect "$dir/grace-tasks.txt" '^SUCCEEDED$' "$shape: a run whose Orker's pod was deleted within the grace succeeded"
	expect "$dir/grace-tasks.txt" "^drain SUCCEEDED attempt=1 orker=($pod\\.$ns|-)\$" "$shape: its task finished on that Orker, on its one attempt"
	# The pod lived on while the task ran (at least 15 s of its remaining
	# ~35 s), and was gone well before the 3-minute grace: an idle stopping
	# Orker exits at once.
	echo "pod deletion took ${took} s" | tee -a "$dir/grace-tasks.txt"
	[ "$took" -ge 15 ] || fail "$shape: the pod was gone ${took} s after its deletion, before its running task could finish"
	[ "$took" -lt 150 ] || fail "$shape: the pod took ${took} s to go, so its Orker did not exit once its task ended"
	log "ok: $shape: the task finished within the grace and the Orker then exited (${took} s)"

	log "$shape: deleting the pod at once (--grace-period=0)"
	k -n "$ns" rollout status deploy/orker --timeout=600s >/dev/null || fail "$shape: the replacement pod never became ready"
	pod=$(graced_pod "$timeout")
	[ -n "$pod" ] || fail "$shape: no running Orker pod has the ${timeout} s stop timeout"
	only_orker "$pod"
	run=$(orkctl run submit k8s-orker-drain -j | jq -r .run.id)
	wait_started "$run" 1
	started=$SECONDS
	k -n "$ns" delete pod "$pod" --grace-period=0 --force --wait=true >/dev/null 2>"$dir/force-delete.err" ||
		fail "$shape: force-deleting the pod failed: $(tail -n 1 "$dir/force-delete.err")"
	wait_terminal "$run"
	took=$((SECONDS - started))
	orkctl run get "$run" -j | jq -r '.run.status, (.run.taskRuns[] | "\(.taskId) \(.status) \(.error.code // "-") \(.error.message // "-")")' | tee "$dir/force-tasks.txt"
	echo "the run ended ${took} s after the forced deletion" | tee -a "$dir/force-tasks.txt"
	orkctl run get "$run" -j | jq -r '.run.taskRuns[] | "interrupted \(.taskId) lost=\(.interruptions.lost // 0) handed_back=\(.interruptions.handedBack // 0) own=\(.interruptions.ownAttemptsUsed // 0)"' | tee -a "$dir/force-tasks.txt"
	# The kubelet still signals both containers. An Orker that saw its stop
	# signal hands the task back, CONTAINER_STOPPED_BY_RUNTIME while stopping;
	# one killed first loses it to the 30 s heartbeat timeout. Either is an
	# interruption (AGENTS.md §4), so the task, with no retry:, runs again on
	# the replacement pod without spending an attempt, after the loss's fence
	# when it was lost. Only a daemon that stopped the container before the
	# Orker saw its signal ends it, explicitly: CONTAINER_STOPPED_BY_RUNTIME
	# outside a stop is not in the class (AGENTS.md §3.2).
	if grep -q '^FAILED$' "$dir/force-tasks.txt"; then
		expect "$dir/force-tasks.txt" '^drain FAILED CONTAINER_STOPPED_BY_RUNTIME ' "$shape: a task its daemon stopped first ends explicitly"
	else
		expect "$dir/force-tasks.txt" '^SUCCEEDED$' "$shape: a run whose Orker was killed at once runs its task again and succeeds"
		# Handed back as its Orker saw the signal, then lost when it died before
		# the report landed, both count: each is an interruption.
		expect "$dir/force-tasks.txt" '^interrupted drain (lost=[1-9][0-9]* handed_back=[0-9]+|lost=0 handed_back=[1-9][0-9]*) own=0$' "$shape: its task, with no retry:, ran again without spending an attempt"
		if grep -Eq '^interrupted drain lost=[1-9]' "$dir/force-tasks.txt"; then
			[ "$took" -ge 60 ] || fail "$shape: the run ended ${took} s after the forced deletion, sooner than its loss's fence and the task's 40 s"
		fi
	fi
	k -n "$ns" rollout status deploy/orker --timeout=600s >/dev/null || fail "$shape: the Deployment did not replace the force-deleted pod"
	log "ok: $shape: a pod deleted at once interrupts its task, explicitly"
}

for shape in userns privileged; do
	! ${!shape} || check_shape "$shape"
done

log "== a work root the engine sees at another path"
first=privileged
! $userns || first=userns
overlay "$first" mismatch >"$out/mismatch.yaml"
k apply -f "$out/mismatch.yaml" >/dev/null
deadline=$((SECONDS + 300))
# The container restarts after each refusal, so keep the log that showed it.
until k -n "$ns" logs -l app.kubernetes.io/name=orker -c orker --tail=-1 >"$out/mismatch-orker.log" 2>&1 &&
	grep -q WORKDIR_NOT_BINDABLE "$out/mismatch-orker.log"; do
	[ $SECONDS -lt $deadline ] || fail "the Orker did not refuse a mismatched work root within 5 minutes"
	sleep 2
done
k -n "$ns" get pod -l app.kubernetes.io/name=orker | tee "$out/mismatch-pod.txt"
expect "$out/mismatch-orker.log" 'orker boot checks failed: WORKDIR_NOT_BINDABLE: .*bind source path does not exist: /var/lib/orkstack/work/ork-bindprobe-' \
	"the Orker refused to boot on a work root the engine cannot see"
expect "$out/mismatch-pod.txt" ' 1/2 ' "the mismatched pod never became ready"
remove_orkers

if $userns; then
	log "== the user-namespaced engine without its RuntimeClass"
	overlay userns no-runtimeclass >"$out/no-runtimeclass.yaml"
	k apply -f "$out/no-runtimeclass.yaml" >/dev/null
	deadline=$((SECONDS + 300))
	until k -n "$ns" logs -l app.kubernetes.io/name=orker -c engine --tail=-1 >"$out/no-runtimeclass-engine.log" 2>&1 &&
		grep -q "can't create directory" "$out/no-runtimeclass-engine.log"; do
		[ $SECONDS -lt $deadline ] || fail "the engine without its RuntimeClass did not refuse to start within 5 minutes"
		sleep 2
	done
	k -n "$ns" get pod -l app.kubernetes.io/name=orker | tee "$out/no-runtimeclass-pod.txt"
	expect "$out/no-runtimeclass-engine.log" "^mkdir: can't create directory '/sys/fs/cgroup/engine': " "without its RuntimeClass the engine cannot make its cgroups"
	expect "$out/no-runtimeclass-pod.txt" ' 0/2 ' "and the pod never runs the Orker"
	remove_orkers
fi

log "PASS: every check held on this cluster ($shapes); evidence in $out"
