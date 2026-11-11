#!/usr/bin/env bash
# Return this machine to a vanilla OrkStack: no processes, no Temporal
# history, no object-store data, no build output.
#
#   scripts/reset.sh          ask first
#   scripts/reset.sh --force  do not ask (CI, scripts)
#
# What it destroys, and what that costs:
#   - Temporal's history: every run's, and every worker deployment version
#     registered against it. The development server keeps it in memory, so
#     removing its container is what loses it.
#   - the object store's volume: every artifact, every archived run, and the orkflow
#     catalogue itself — an applied orkflow lives in the bucket, so after
#     this the engine boots with nothing to run.
#   - the observability profile's containers and volumes (Grafana, Loki,
#     Prometheus, Tempo), when it ran: the compose down takes every profile.
#   - bin/, .run/, ui/build, ui/.svelte-kit, ui/test-results(-solo), and the UI
#     build `make ui` copied into internal/webui/dist (its committed .gitkeep
#     stays).
#   - .data/: the local artifact store AND the Orker's work directory
#     (.data/work), which `make up` recreates on the next start, workspaces
#     an Orker left read-only included.
#
# It does NOT touch the repository: source and committed files stay exactly
# as they are.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO="$PWD"
COMPOSE=(docker compose -f "$REPO/deploy/docker-compose.yml")

force=0
[ "${1:-}" = "--force" ] && force=1

say() { printf '%s\n' "$*"; }

if [ "$force" -ne 1 ]; then
	say "This deletes ALL local OrkStack state:"
	say "  - Temporal history and worker deployment versions (in the Temporal container's memory)"
	say "  - object-store data: artifacts, archived runs, and the orkflow catalogue"
	say "  - the observability profile's containers and volumes (Grafana, Loki, Prometheus, Tempo)"
	say "  - .data/: the local artifact store and the Orker's work directory"
	say "  - bin/, .run/, ui/build, ui/.svelte-kit, ui/test-results(-solo), internal/webui/dist's UI build"
	say ""
	say "The repository itself is untouched."
	say ""
	printf 'Type "reset" to continue: '
	read -r answer
	[ "$answer" = "reset" ] || { say "aborted"; exit 1; }
fi

say "== stopping everything =="
# stack.sh down stops what make up started, and any process whose executable
# is this worktree's bin/ork or bin/orker — by identity, never by name or
# pattern (AGENTS.md §11.1). Its own verdict is checked again below.
"$REPO/scripts/stack.sh" down >/dev/null 2>&1 || true

say "== removing containers and their volumes =="
# Every profile: a down without one leaves that profile's services and their
# volumes (the observability profile's Grafana, Loki, Prometheus and Tempo),
# and the check below then refuses.
"${COMPOSE[@]}" --profile '*' down --volumes --remove-orphans

say "== removing build output and run state =="
rm -rf "$REPO/bin" "$REPO/.run" "$REPO/ui/build" "$REPO/ui/.svelte-kit" "$REPO/ui/test-results" "$REPO/ui/test-results-solo"
# The UI build ork embeds; .gitkeep is committed, so a fresh ork still
# compiles (and serves its placeholder page).
find "$REPO/internal/webui/dist" -mindepth 1 -maxdepth 1 ! -name .gitkeep -exec rm -rf {} + 2>/dev/null || true

# The local-filesystem store. This is the one that bites: the default backend
# is local (artifacts.DefaultLocalDir = ./.data/artifacts, pkg/artifacts/config.go),
# so any process started without ORK_ARTIFACT_BACKEND archives runs and stores
# the orkflow catalogue HERE, not in the object store. Wiping only the docker volume
# leaves those behind, and a fresh engine then lists runs that no longer exist
# anywhere else. The path is read from the same env var the binaries read.
#
# The Orker seals a workspace's input/ read-only, and one that ended uncleanly
# (an Orker killed mid-task) leaves it under .data/work, where rm cannot empty
# it; so every directory is made the owner's to read, enter and write first.
# That is best effort: a file a workload wrote as another uid is not ours to
# chmod, though rm may still unlink it from a sticky output/, and rm and the
# check below are what judge. A symlinked path is left alone, so nothing
# outside the repository is changed. A file an activity container wrote as
# root stays the host's to remove (`make proof-clean REMOVE=1` empties a
# test's work root through a container).
state=("$REPO/${ORK_ARTIFACT_DIR:-.data/artifacts}" "$REPO/.data")
for path in "${state[@]}"; do
	{ [ ! -e "$path" ] || [ -L "$path" ] || chmod -R u+rwX -- "$path"; } 2>/dev/null || true
done
rm -rf -- "${state[@]}"

say "== verifying =="
# bin/ is gone by now, so a survivor's /proc/<pid>/exe reads
# "<path> (deleted)"; it is compared to the path all the same.
stray=""
for p in /proc/[0-9]*; do
	exe="$(readlink "$p/exe" 2>/dev/null)" || continue
	case "${exe% (deleted)}" in
	"$REPO/bin/ork" | "$REPO/bin/orker") stray="$stray ${p#/proc/}" ;;
	esac
done
[ -z "$stray" ] || { say "processes running this worktree's bin/ork or bin/orker survived: pids$stray"; exit 1; }
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
