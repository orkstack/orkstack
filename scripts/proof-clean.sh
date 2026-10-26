#!/usr/bin/env bash
# List what failed test processes kept for their post-mortem, and remove it
# when asked (AGENTS.md §11.1):
#
#   scripts/proof-clean.sh            list
#   scripts/proof-clean.sh --remove   remove what is listed
#
#   - object-storage buckets named orkstack-<label>-<pid>-<suffix>
#     (internal/testsupport/s3bucket keeps a failed process's bucket, and a
#     proof a signal stopped leaves its passing processes' ones): the ones the
#     store lists, and the ones this worktree's proofs recorded (proof/*/), since
#     SeaweedFS with its auth off lists none (test/tools/bucket-ensure,
#     ORK_BUCKET_ACTION=leftovers);
#   - Orker work roots under .data/work that tests create per test, named
#     <label>-p<pid>-<suffix> (internal/testsupport/proof.WorkRootPattern, and
#     the observability gate's own; a test removes its own unless a container
#     left files it cannot);
#   - proof Temporal containers whose proof is gone (the next proof stops
#     them anyway).
#
# Stop only what you started (AGENTS.md §11.1): a bucket or work root is
# removed only when the process its name records is gone, so this is safe to
# run beside a running proof or test — what a live process still uses is
# listed as in use and left alone. A work root whose name records no pid (a
# Playwright spec's .data/work/e2e/, or one left by an older tree) is listed
# and never removed here. The shared development bucket, orkstack-artifacts,
# and the dev stack's own work root are never touched. Nothing is removed
# without --remove.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

remove=""
case "${1:-}" in
"") ;;
--remove) remove=1 ;;
*)
	echo "usage: scripts/proof-clean.sh [--remove]" >&2
	exit 2
	;;
esac

# alive PID — true while a process with that pid exists, whoever owns it. A
# reused pid keeps a leftover a little longer; it never removes a live one.
alive() { ps -p "$1" >/dev/null 2>&1; }

s3env=(
	ORK_S3_ENDPOINT="${ORK_S3_ENDPOINT:-http://127.0.0.1:9000}"
	ORK_S3_ACCESS_KEY="${ORK_S3_ACCESS_KEY:-orkstack}"
	ORK_S3_SECRET_KEY="${ORK_S3_SECRET_KEY:-orkstack-secret}"
)
echo "== buckets kept by test processes"
if ! env "${s3env[@]}" ORK_BUCKET_ACTION=leftovers ORK_PROOF_ROOT=proof ${remove:+ORK_BUCKET_REMOVE=1} \
	go run ./test/tools/bucket-ensure 2>/dev/null; then
	echo "  (object storage not reachable; skipped)"
fi

echo "== per-test work roots under .data/work"
if [ -d .data/work ]; then
	for d in .data/work/*/; do
		[ -d "$d" ] || continue
		case "$(basename "$d")" in
		ork-pin-* | ork-spool | ork-stage | .*) continue ;; # the dev stack's own Orker state
		esac
		if [[ "$(basename "$d")" =~ -p([0-9]+)-[A-Za-z0-9]+$ ]]; then
			pid="${BASH_REMATCH[1]}"
		else
			echo "  $d  (its name records no owning pid; not removed here — remove it by hand once nothing on this host runs tests)"
			continue
		fi
		if alive "$pid"; then
			echo "  $d  (in use: pid $pid is alive; left alone)"
			continue
		fi
		echo "  $d"
		if [ -n "$remove" ]; then rm -rf "$d" 2>/dev/null || echo "    (some files are not ours to remove: try a container, or sudo)"; fi
	done
fi

echo "== proof Temporal containers whose proof is gone"
proc_start() { sed -E 's/^.*\) //' "/proc/$1/stat" 2>/dev/null | awk '{print $20}'; }
for c in $(docker ps -q --filter label=orkstack.proof 2>/dev/null); do
	owner="$(docker inspect -f '{{index .Config.Labels "orkstack.proof.owner"}}' "$c")"
	rest="${owner#*:}"
	pid="${rest%%:*}"
	start="${rest#*:}"
	if [ "$(proc_start "$pid")" != "$start" ]; then
		name="$(docker inspect -f '{{.Name}}' "$c")"
		echo "  ${name#/}"
		if [ -n "$remove" ]; then docker stop "${name#/}" >/dev/null; fi
	fi
done
