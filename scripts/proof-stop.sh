#!/usr/bin/env bash
# Stop one proof and everything it started (AGENTS.md §11.1):
#
#   scripts/proof-stop.sh proof/<name>
#
# What a proof started is known exactly, so nothing here matches a name or a
# pattern: /bin/ork$ matches every worktree's dev stack and every running
# proof on the host. It stops, in order:
#
#   1. the proof's process group, from proof/<name>/proof.pid — asked first
#      (TERM, so the proof's own exit trap stops its Temporal), killed after
#      60 s — and only while the recorded pid is that proof (its start time
#      is recorded too, so a recycled pid is never signalled);
#   2. every process whose executable is one of the proof's own binaries,
#      under proof/<name>/bin/ (test binaries, engines and Orkers a crashed
#      test left behind);
#   3. the proof's Temporal container, by its exact name;
#   4. every container whose ork.orker label carries the proof's id
#      (internal/testsupport/proof.OrkerID).
set -uo pipefail

dir="${1:-}"
if [ -z "$dir" ] || [ ! -f "$dir/proof.pid" ]; then
	echo "usage: scripts/proof-stop.sh proof/<name>   (a proof directory with its proof.pid)" >&2
	exit 2
fi
dir="$(cd "$dir" && pwd)"
read -r pid pgid start id <"$dir/proof.pid"

proc_start() { sed -E 's/^.*\) //' "/proc/$1/stat" 2>/dev/null | awk '{print $20}'; }
alive() { [ -n "$1" ] && [ "$(proc_start "$1")" = "$2" ]; }

if alive "$pid" "$start"; then
	echo "proof-stop: asking proof $pid (process group $pgid) to stop"
	kill -TERM -- -"$pgid" 2>/dev/null
	for _ in $(seq 1 120); do
		alive "$pid" "$start" || break
		sleep 0.5
	done
	if alive "$pid" "$start"; then
		echo "proof-stop: still running after 60s; killing process group $pgid"
		kill -KILL -- -"$pgid" 2>/dev/null
	fi
else
	echo "proof-stop: the proof's own process ($pid) is gone"
fi

stragglers() {
	local p exe
	for p in /proc/[0-9]*; do
		exe="$(readlink "$p/exe" 2>/dev/null)" || continue
		case "$exe" in
		"$dir/bin/"*) echo "${p#/proc/} $exe" ;;
		esac
	done
}
while read -r p exe; do
	[ -n "$p" ] || continue
	echo "proof-stop: stopping $p ($exe)"
	kill -TERM "$p" 2>/dev/null
done < <(stragglers)
sleep 2
while read -r p exe; do
	[ -n "$p" ] || continue
	echo "proof-stop: killing $p ($exe)"
	kill -KILL "$p" 2>/dev/null
done < <(stragglers)

if [ -n "${id:-}" ] && command -v docker >/dev/null 2>&1; then
	name="orkstack-proof-$id-temporal"
	if docker stop "$name" >/dev/null 2>&1; then echo "proof-stop: stopped $name"; fi
	for c in $(docker ps -aq --filter label=ork.orker 2>/dev/null); do
		label="$(docker inspect -f '{{index .Config.Labels "ork.orker"}}' "$c" 2>/dev/null)"
		case "$label" in
		*"-$id-"*)
			docker rm -f "$c" >/dev/null 2>&1 && echo "proof-stop: removed container $c (ork.orker=$label)"
			;;
		esac
	done
fi
echo "proof-stop: done"
