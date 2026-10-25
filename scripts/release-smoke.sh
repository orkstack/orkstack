#!/usr/bin/env bash
# make release-smoke VERSION=vX.Y.Z: the release, built for this machine, run
# end to end from its quickstart bundle before anything is published.
#
#   1. make dist and make release-images (this machine's platform), unless
#      --layouts DIR names layouts already built (release.yml builds them);
#   2. a registry of its own (deploy/quickstart/smoke.yaml) takes every image,
#      copied byte for byte from its layout by the pinned skopeo image, and
#      each pushed digest is checked against the layout's;
#   3. `dist quickstart` writes the bundle exactly as a release does, naming
#      that registry instead of ghcr.io, and `docker compose up -d --wait`
#      starts it;
#   4. orkctl's image reports VERSION and applies each-fan-out; the archive's
#      orkctl submits it, watches it to SUCCEEDED and reads merge's log; the
#      UI answers, lists the example, and the engine accepts its WebSocket;
#   5. it removes what it started, by its own names and labels, whatever
#      happened.
#
# Everything it creates carries its token: the compose project, the registry
# tags, the Orker's id and work directory, and free ports. It stops nothing it
# did not start. Exit 0 pass, 1 a check failed, 2 an invalid invocation, 77 a
# missing prerequisite.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO="$PWD"

usage() {
	echo "usage: scripts/release-smoke.sh [--layouts DIR] vMAJOR.MINOR.PATCH" >&2
	exit 2
}
LAYOUTS=""
while [ $# -gt 0 ]; do
	case "$1" in
	--layouts) [ $# -ge 2 ] || usage; LAYOUTS="$2"; shift 2 ;;
	-*) usage ;;
	*) break ;;
	esac
done
[ $# -eq 1 ] || usage
VERSION="$1"
[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "release-smoke: $VERSION is not vMAJOR.MINOR.PATCH" >&2; exit 2; }

need() {
	printf 'release-smoke: %s\n' "$*" >&2
	exit 77
}
fail() {
	printf 'release-smoke: FAIL: %s\n' "$*" >&2
	exit 1
}
say() { printf '%s\n' "$*"; }

{ command -v docker >/dev/null && docker info >/dev/null 2>&1; } || need "Docker is not running"
docker compose version >/dev/null 2>&1 || need "docker compose (v2) is not installed"
command -v go >/dev/null || need "go is not installed (the dist tool builds the bundle)"
command -v curl >/dev/null || need "curl is not installed"
command -v python3 >/dev/null || need "python3 is not installed (free ports)"
SKOPEO="$(grep -v '^#' "$REPO/deploy/skopeo-image.txt" | head -1)"
[ -n "$SKOPEO" ] || need "deploy/skopeo-image.txt names no image"

# Every archive is stamped with the commit's time, never the clock.
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git -C "$REPO" log -1 --format=%ct)}"
ARCH="$(go env GOARCH)"
TOKEN="smoke-$$-$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')"
PROJECT="orkstack-$TOKEN"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/orkstack-$TOKEN.XXXXXX")"
BUNDLE="$WORK/orkstack-quickstart"
DIST_TOOL=(go run ./internal/dist/cmd/dist)
free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }

export SMOKE_REGISTRY_PORT ORKSTACK_API_PORT ORKSTACK_UI_PORT ORKSTACK_TEMPORAL_UI_PORT
SMOKE_REGISTRY_PORT="$(free_port)"
ORKSTACK_API_PORT="$(free_port)"
ORKSTACK_UI_PORT="$(free_port)"
ORKSTACK_TEMPORAL_UI_PORT="$(free_port)"
export ORKSTACK_ORKER_ID="$TOKEN"
export ORKSTACK_WORKDIR="$WORK/orker-work"
export ORKSTACK_DOCKER_SOCKET
ORKSTACK_DOCKER_SOCKET="$(docker context inspect --format '{{.Endpoints.docker.Host}}' | sed 's|^unix://||')"
[ -S "$ORKSTACK_DOCKER_SOCKET" ] || need "the Docker daemon is not on a local unix socket ($ORKSTACK_DOCKER_SOCKET)"
REGISTRY="127.0.0.1:$SMOKE_REGISTRY_PORT"

dc() {
	docker compose -p "$PROJECT" --project-directory "$BUNDLE" \
		-f "$BUNDLE/compose.yaml" -f "$REPO/deploy/quickstart/smoke.yaml" "$@"
}

cleanup() {
	local rc=$?
	set +e
	if [ -f "$BUNDLE/compose.yaml" ]; then
		[ "$rc" -eq 0 ] || dc logs --no-color --tail 80 ork orker ui >&2 2>/dev/null
		dc down -v --remove-orphans --timeout 50 >/dev/null 2>&1
	fi
	docker ps -aq --filter "label=ork.orker=$TOKEN" | xargs -r docker rm -f >/dev/null 2>&1
	# The stack pulled its images by digest, so they carry no tag to name.
	docker images --digests --format '{{.Repository}}@{{.Digest}}' | grep -F "$REGISTRY/" | xargs -r docker image rm >/dev/null 2>&1
	# The Orker wrote its work directory as root; a container of the pinned
	# skopeo image (the one image this script already trusts) clears it.
	[ -d "$ORKSTACK_WORKDIR" ] && docker run --rm --entrypoint rm -v "$WORK:/w" "$SKOPEO" -rf /w/orker-work >/dev/null 2>&1
	rm -rf -- "$WORK"
	local left
	left="$(docker ps -aq --filter "label=ork.orker=$TOKEN"; docker ps -aq --filter "label=com.docker.compose.project=$PROJECT")"
	[ -z "$left" ] || { echo "release-smoke: containers survived teardown: $left" >&2; rc=1; }
	exit "$rc"
}
trap cleanup EXIT

say "== release smoke $VERSION ($TOKEN, linux/$ARCH)"

if [ -z "$LAYOUTS" ]; then
	say "== build: make dist, make release-images"
	make -s dist VERSION="$VERSION"
	LAYOUTS="$WORK/layouts"
	make -s release-images VERSION="$VERSION" OUT="$LAYOUTS" PLATFORMS="linux/$ARCH"
fi
[ -d "$LAYOUTS" ] || need "no layouts at $LAYOUTS"
"${DIST_TOOL[@]}" digests -layouts "$LAYOUTS" >"$WORK/images.tsv"
say "$(wc -l <"$WORK/images.tsv" | tr -d ' ') images:"
sed 's/^/  /' "$WORK/images.tsv"

say "== bundle: dist quickstart, naming $REGISTRY"
"${DIST_TOOL[@]}" quickstart -version "$VERSION" -images "$WORK/images.tsv" \
	-registry "$REGISTRY" -tag "$TOKEN" -out "$WORK"
tar -xzf "$WORK/orkstack-quickstart.tar.gz" -C "$WORK"
{ [ -f "$BUNDLE/compose.yaml" ] && [ -f "$BUNDLE/.env" ]; } || fail "the bundle holds no compose.yaml and .env"
[ ! -e "$BUNDLE/smoke.yaml" ] || fail "the bundle holds the smoke override"

say "== registry: push each layout byte for byte"
dc up -d --wait registry >/dev/null
while IFS=$'\t' read -r name digest; do
	docker run --rm --network host -v "$LAYOUTS:/layouts:ro" "$SKOPEO" copy --quiet --all --preserve-digests \
		--dest-tls-verify=false "oci:/layouts/$name" "docker://$REGISTRY/orkstack/$name:$TOKEN"
	got="sha256:$(docker run --rm --network host "$SKOPEO" inspect --raw --tls-verify=false \
		"docker://$REGISTRY/orkstack/$name:$TOKEN" | sha256sum | cut -d' ' -f1)"
	[ "$got" = "$digest" ] || fail "$name: pushed $got, the layout says $digest"
	say "  $name $digest"
done <"$WORK/images.tsv"

say "== up: docker compose up -d --wait"
mkdir -p "$ORKSTACK_WORKDIR"
dc up -d --wait --quiet-pull

say "== orkctl (its image)"
got="$(dc run --rm -T orkctl version)"
grep -q "^orkctl: *$VERSION\$" <<<"$got" || fail "orkctl version says: $got"
say "  version $VERSION"
dc run --rm -T orkctl apply examples/each-fan-out.ork.yaml >"$WORK/apply.log" || { cat "$WORK/apply.log" >&2; fail "apply"; }
say "  applied each-fan-out"

say "== orkctl (the archive's binary)"
mkdir -p "$WORK/archive"
tar -xzf "$REPO/dist/orkctl-linux-$ARCH.tar.gz" -C "$WORK/archive"
ORKCTL="$(find "$WORK/archive" -type f -name orkctl | head -1)"
[ -x "$ORKCTL" ] || fail "dist/orkctl-linux-$ARCH.tar.gz holds no executable orkctl"
export ORK_SERVER="http://127.0.0.1:$ORKSTACK_API_PORT"
"$ORKCTL" run submit each-fan-out --watch >"$WORK/watch.log" 2>&1 || { cat "$WORK/watch.log" >&2; fail "run submit --watch"; }
run="$(grep -o 'ork-[0-9]*-[0-9]*' "$WORK/watch.log" | head -1)"
[ -n "$run" ] || fail "no run id in: $(head -3 "$WORK/watch.log")"
grep -q "finished with status SUCCEEDED" "$WORK/watch.log" || fail "the run did not finish SUCCEEDED: $(tail -3 "$WORK/watch.log")"
say "  $run SUCCEEDED"
"$ORKCTL" run logs "$run" --task merge >"$WORK/merge.log"
[ "$(tr -d '\r' <"$WORK/merge.log" | grep -xE '[abcd]' | tr -d '\n')" = "abcd" ] || fail "merge's log is: $(cat "$WORK/merge.log")"
say "  merge logged a b c d"

say "== ui"
UI="http://127.0.0.1:$ORKSTACK_UI_PORT"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$UI/orkflows")" = 200 ] || fail "$UI/orkflows does not answer 200"
# Read whole before grep, so grep's early exit cannot fail curl under pipefail.
gallery="$(curl -s "$UI/apply")"
grep -q 'each-fan-out' <<<"$gallery" || fail "the UI's example gallery does not list each-fan-out"
ws="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
	-H 'Connection: Upgrade' -H 'Upgrade: websocket' \
	-H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' \
	-H "Origin: http://localhost:$ORKSTACK_UI_PORT" \
	"$ORK_SERVER/ws/runs/ork-probe-nonexistent?from_sequence=0" || true)"
[ "$ws" = 404 ] || fail "the engine answers the UI's WebSocket with $ws (403 means it refuses the UI's origin)"
say "  /orkflows 200, the gallery lists each-fan-out, the WebSocket origin is accepted"

say "release-smoke: PASS ($VERSION, $TOKEN)"
