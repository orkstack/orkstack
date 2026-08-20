#!/usr/bin/env bash
# Return this machine to a vanilla OrkStack: no processes, no Temporal
# history, no MinIO objects, no build output.
#
#   scripts/reset.sh          ask first
#   scripts/reset.sh --force  do not ask (CI, scripts)
#
# What it destroys, and what that costs:
#   - Temporal's sqlite volume: every run's history, and every worker
#     deployment version registered against it.
#   - MinIO's volume: every artifact, every archived run, and the orkflow
#     catalogue itself — an applied orkflow lives in the bucket (M7.5), so
#     after this the engine boots with nothing to run.
#   - bin/, .run/, ui/build, ui/.svelte-kit, ui/test-results.
#   - .data/: the local artifact store AND the Orker's work directory
#     (.data/work), which `make up` recreates on the next start.
#
# It does NOT touch the repository: committed proof artifacts, research notes
# and source stay exactly as they are.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO="$PWD"
COMPOSE=(docker compose -f "$REPO/deploy/docker-compose.yml")

force=0
[ "${1:-}" = "--force" ] && force=1

say() { printf '%s\n' "$*"; }

if [ "$force" -ne 1 ]; then
	say "This deletes ALL local OrkStack state:"
	say "  - Temporal history and worker deployment versions (docker volume)"
	say "  - MinIO objects: artifacts, archived runs, and the orkflow catalogue"
	say "  - bin/, .run/, ui/build, ui/.svelte-kit, ui/test-results"
	say ""
	say "The repository itself, including proof/, is untouched."
	say ""
	printf 'Type "reset" to continue: '
	read -r answer
	[ "$answer" = "reset" ] || { say "aborted"; exit 1; }
fi

say "== stopping everything =="
"$REPO/scripts/stack.sh" down >/dev/null 2>&1 || true
pkill -f "$REPO/bin/orker" 2>/dev/null || true
pkill -f "$REPO/bin/ork$" 2>/dev/null || true
sleep 1

say "== removing containers and their volumes =="
"${COMPOSE[@]}" down --volumes --remove-orphans

say "== removing build output and run state =="
rm -rf "$REPO/bin" "$REPO/.run" "$REPO/ui/build" "$REPO/ui/.svelte-kit" "$REPO/ui/test-results"

# The local-filesystem store. This is the one that bites: the default backend
# is local (artifacts.DefaultLocalDir = ./.data/artifacts, pkg/artifacts/config.go),
# so any process started without ORK_ARTIFACT_BACKEND archives runs and stores
# the orkflow catalogue HERE, not in MinIO. Wiping only the docker volume
# leaves those behind, and a fresh engine then lists runs that no longer exist
# anywhere else. The path is read from the same env var the binaries read.
rm -rf "$REPO/${ORK_ARTIFACT_DIR:-.data/artifacts}" "$REPO/.data"

say "== verifying =="
stray="$(pgrep -fl "$REPO/bin/ork" 2>/dev/null || true)"
[ -z "$stray" ] || { say "processes survived: $stray"; exit 1; }
volumes="$(docker volume ls -q --filter label=com.docker.compose.project=orkstack 2>/dev/null || true)"
[ -z "$volumes" ] || { say "volumes survived: $volumes"; exit 1; }
# Every place a run or an orkflow can persist, checked by looking rather than
# by trusting the rm above. The local store is listed explicitly because it is
# the one that survived an earlier version of this script.
for path in "$REPO/.data" "$REPO/bin" "$REPO/.run"; do
	[ ! -e "$path" ] || { say "state survived: $path"; exit 1; }
done
say "  no orkstack processes"
say "  no orkstack volumes"
say "  no local artifact store, build output or run state"

say ""
say "Vanilla. Bring it back with: make up"
