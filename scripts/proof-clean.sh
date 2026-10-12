#!/usr/bin/env bash
# List what failed test processes kept for their post-mortem, and remove it
# when asked (AGENTS.md §11.1):
#
#   scripts/proof-clean.sh            list
#   scripts/proof-clean.sh --remove   remove what is listed
#
#   - object-storage buckets named orkstack-<label>-<pid>-<suffix>
#     (internal/testsupport/s3bucket keeps a failed process's bucket);
#   - Orker work roots under .data/work that tests and specs create per test
#     (a test removes its own unless a container left files it cannot);
#   - proof Temporal containers whose proof is gone (the next proof stops
#     them anyway).
#
# The shared development bucket, orkstack-artifacts, and the dev stack's own
# work root are never touched. Nothing is removed without --remove.
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

s3env=(
	ORK_S3_ENDPOINT="${ORK_S3_ENDPOINT:-http://127.0.0.1:9000}"
	ORK_S3_ACCESS_KEY="${ORK_S3_ACCESS_KEY:-orkstack}"
	ORK_S3_SECRET_KEY="${ORK_S3_SECRET_KEY:-orkstack-secret}"
)
echo "== buckets kept by test processes"
if buckets="$(env "${s3env[@]}" ORK_BUCKET_ACTION=list go run ./test/tools/bucket-ensure 2>/dev/null)"; then
	for b in $(printf '%s\n' "$buckets" | grep -E '^orkstack-[a-z][a-z0-9-]*-[0-9]+-[0-9]+$'); do
		echo "  $b"
		if [ -n "$remove" ]; then
			env "${s3env[@]}" ORK_BUCKET_ACTION=cleanup ORK_S3_BUCKET="$b" go run ./test/tools/bucket-ensure
		fi
	done
else
	echo "  (object storage not reachable; skipped)"
fi

echo "== per-test work roots under .data/work"
if [ -d .data/work ]; then
	for d in .data/work/*/; do
		[ -d "$d" ] || continue
		case "$(basename "$d")" in
		ork-pin-* | ork-spool | ork-stage | .*) continue ;; # the dev stack's own Orker state
		esac
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
