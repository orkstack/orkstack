#!/usr/bin/env bash
# make quickstart-smoke: the README's Quickstart, run as written on a fresh
# clone of this working tree, end to end.
# make release-smoke VERSION=vX.Y.Z: the same stack on one release's own
# images, built for this machine, before anything is published.
#
# Without --release (the quickstart):
#   1. commits the working tree, tracked and untracked files alike, to a
#      scratch bare repository named orkstack.git (this checkout's refs,
#      index and files are untouched), so the README's
#      `git clone … && cd orkstack` runs as written with only its URL
#      replaced by that repository's path;
#   2. runs the README's Quickstart commands as written, in bash, from an
#      empty directory: the clone builds and starts the stack, and orkctl,
#      from its image, applies each-fan-out and watches a run of it to the end.
# With --release VERSION [--layouts DIR]:
#   1. make dist, which builds the web UI into ork first (make ui, in the
#      pinned bun image), and make release-images (this machine's
#      platform), unless --layouts DIR names layouts already built
#      (release.yml builds them);
#   2. the stack's own registry takes every image, copied byte for byte from
#      its layout by the pinned skopeo image, and each pushed digest is
#      checked against the layout's; each is then signed by digest with a
#      throwaway key, by the pinned cosign image and the command and flags
#      release.yml's publish signs with, and verified with `cosign verify`,
#      and a file is signed as publish signs SHA256SUMS and verified with
#      `cosign verify-blob` and with openssl;
#   3. compose.yaml, with deploy/release-smoke.yaml naming those images by tag
#      and digest instead of building any, starts with `up -d --wait`;
#      orkctl's image reports VERSION and applies each-fan-out, and the
#      archive's orkctl submits it and watches it to the end.
# Then, either way: the run SUCCEEDED and merge logged a to d; for a release,
# OrkStack's own verifier accepts those signatures: with the throwaway key in
# the image policy, each-fan-out runs to SUCCEEDED in a namespace that
# requires it, and is refused in one that requires a key that signed nothing;
# the engine serves the web UI on its own port, the real build and not the
# placeholder, the build's gallery lists the example, and the engine accepts
# the UI's WebSocket from its own origin; and the script removes what it
# started, by its own names and labels, whatever happened.
#
# Everything it creates carries its token: the compose project, the images it
# builds or pulls, the Orker's id and work directory, and free ports (set
# through the ORKSTACK_* variables compose.yaml reads, never by editing a
# command). It stops nothing it did not start. Exit 0 pass, 1 a check failed,
# 2 an invalid invocation, 77 a missing prerequisite.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO="$PWD"

usage() {
	echo "usage: scripts/quickstart-smoke.sh [--release vMAJOR.MINOR.PATCH [--layouts DIR]]" >&2
	exit 2
}
VERSION=""
LAYOUTS=""
while [ $# -gt 0 ]; do
	case "$1" in
	--release) [ $# -ge 2 ] || usage; VERSION="$2"; shift 2 ;;
	--layouts) [ $# -ge 2 ] || usage; LAYOUTS="$2"; shift 2 ;;
	*) usage ;;
	esac
done
if [ -n "$VERSION" ]; then
	[[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "quickstart-smoke: $VERSION is not vMAJOR.MINOR.PATCH" >&2; exit 2; }
elif [ -n "$LAYOUTS" ]; then
	echo "quickstart-smoke: --layouts needs --release" >&2
	exit 2
fi

need() {
	printf 'quickstart-smoke: %s\n' "$*" >&2
	exit 77
}
fail() {
	printf 'quickstart-smoke: FAIL: %s\n' "$*" >&2
	exit 1
}
say() { printf '%s\n' "$*"; }

{ command -v docker >/dev/null && docker info >/dev/null 2>&1; } || need "Docker is not running"
docker compose version >/dev/null 2>&1 || need "docker compose (v2) is not installed"
command -v git >/dev/null || need "git is not installed"
command -v curl >/dev/null || need "curl is not installed"
command -v python3 >/dev/null || need "python3 is not installed (free ports)"
if [ -n "$VERSION" ]; then
	command -v go >/dev/null || need "go is not installed (the dist tool reads the layouts)"
	SKOPEO="$(grep -v '^#' "$REPO/deploy/skopeo-image.txt" | head -1)"
	[ -n "$SKOPEO" ] || need "deploy/skopeo-image.txt names no image"
	COSIGN="$(grep -v '^#' "$REPO/deploy/cosign-image.txt" | head -1)"
	[ -n "$COSIGN" ] || need "deploy/cosign-image.txt names no image"
	command -v openssl >/dev/null || need "openssl is not installed (it checks the checksum signature as the docs do)"
	# deploy/release-smoke.yaml forwards the registry into the Orker's network
	# namespace with compose.yaml's own pinned forwarder.
	export SMOKE_FORWARD_IMAGE
	SMOKE_FORWARD_IMAGE="$(grep -m1 -oE 'alpine/socat:[^ @]+@sha256:[0-9a-f]{64}' "$REPO/compose.yaml")"
	[ -n "$SMOKE_FORWARD_IMAGE" ] || need "compose.yaml names no pinned socat image"
fi
# The image that clears the Orker's root-owned work directory: the pinned
# docker CLI compose.yaml already runs.
CLEANER="$(grep -m1 -oE 'docker:[^ @]+@sha256:[0-9a-f]{64}' "$REPO/compose.yaml")"
[ -n "$CLEANER" ] || need "compose.yaml names no pinned docker CLI image"

export ORKSTACK_DOCKER_SOCKET
ORKSTACK_DOCKER_SOCKET="$(docker context inspect --format '{{.Endpoints.docker.Host}}' | sed 's|^unix://||')"
[ -S "$ORKSTACK_DOCKER_SOCKET" ] || need "the Docker daemon is not on a local unix socket ($ORKSTACK_DOCKER_SOCKET)"

TOKEN="qs-$$-$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')"
PROJECT="orkstack-$TOKEN"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/orkstack-$TOKEN.XXXXXX")"
free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }

export COMPOSE_PROJECT_NAME="$PROJECT"
export ORKSTACK_API_PORT ORKSTACK_TEMPORAL_UI_PORT ORKSTACK_REGISTRY_PORT
ORKSTACK_API_PORT="$(free_port)"
ORKSTACK_TEMPORAL_UI_PORT="$(free_port)"
ORKSTACK_REGISTRY_PORT="$(free_port)"
export ORKSTACK_ORKER_ID="$TOKEN"
export ORKSTACK_WORKDIR="$WORK/orker-work"
REGISTRY="127.0.0.1:$ORKSTACK_REGISTRY_PORT"

# The checkout compose runs from: the clone, or this repository for a release.
if [ -n "$VERSION" ]; then
	STACK="$REPO"
	FILES=(-f "$REPO/compose.yaml" -f "$REPO/deploy/release-smoke.yaml")
else
	STACK="$WORK/run/orkstack"
	FILES=(-f "$STACK/compose.yaml")
fi
dc() {
	docker compose -p "$PROJECT" --project-directory "$STACK" "${FILES[@]}" "$@"
}

cleanup() {
	local rc=$?
	set +e
	if [ -f "$STACK/compose.yaml" ]; then
		[ "$rc" -eq 0 ] || dc logs --no-color --tail 80 activities ork orker >&2 2>/dev/null
		# --rmi local removes the images compose built and named after the
		# project; the activities it built are named by the registry's port.
		dc down -v --remove-orphans --rmi local --timeout 50 >/dev/null 2>&1
	fi
	docker ps -aq --filter "label=ork.orker=$TOKEN" | xargs -r docker rm -f >/dev/null 2>&1
	docker images --format '{{.Repository}}:{{.Tag}}' | grep -F "$REGISTRY/" | xargs -r docker image rm >/dev/null 2>&1
	docker images --digests --format '{{.Repository}}@{{.Digest}}' | grep -F "$REGISTRY/" | xargs -r docker image rm >/dev/null 2>&1
	# The Orker wrote its work directory as root.
	[ -d "$ORKSTACK_WORKDIR" ] && docker run --rm --entrypoint rm -v "$WORK:/w" "$CLEANER" -rf /w/orker-work >/dev/null 2>&1
	rm -rf -- "$WORK"
	local left
	left="$(docker ps -aq --filter "label=ork.orker=$TOKEN"; docker ps -aq --filter "label=com.docker.compose.project=$PROJECT")"
	[ -z "$left" ] || { echo "quickstart-smoke: containers survived teardown: $left" >&2; rc=1; }
	exit "$rc"
}
trap cleanup EXIT
mkdir -p "$ORKSTACK_WORKDIR"

if [ -z "$VERSION" ]; then
	say "== quickstart smoke ($TOKEN)"
	# The working tree as a commit of its own, written with a scratch index so
	# this checkout's index and refs never move. It has no parent, so a shallow
	# checkout (CI's) pushes it as well as a full one.
	export GIT_INDEX_FILE="$WORK/index"
	git -C "$REPO" read-tree HEAD
	git -C "$REPO" add -A
	tree="$(git -C "$REPO" write-tree)"
	unset GIT_INDEX_FILE
	commit="$(GIT_AUTHOR_NAME=smoke GIT_AUTHOR_EMAIL=smoke@invalid GIT_COMMITTER_NAME=smoke GIT_COMMITTER_EMAIL=smoke@invalid \
		git -C "$REPO" commit-tree --no-gpg-sign -m "quickstart smoke" "$tree")"
	git init -q --bare "$WORK/orkstack.git"
	git -C "$REPO" -c core.hooksPath=/dev/null push -q "$WORK/orkstack.git" "$commit:refs/heads/main"
	git -C "$WORK/orkstack.git" symbolic-ref HEAD refs/heads/main
	say "  tree $tree"

	# The README's commands, as written: only the clone's URL is this tree's.
	readme="$(git -C "$WORK/orkstack.git" show main:README.md)"
	commands="$(awk '/^## Quickstart$/{q=1; next} q && /^## /{exit} q && /^```bash$/{f=1; next} f && /^```$/{exit} f{print}' <<<"$readme")"
	clone='git clone https://github.com/orkstack/orkstack.git && cd orkstack'
	[ "$(head -1 <<<"$commands")" = "$clone" ] || fail "the README's Quickstart does not start with: $clone"
	{
		echo 'set -euo pipefail'
		echo 'shopt -s expand_aliases'
		sed "1s|https://github.com/orkstack/orkstack.git|$WORK/orkstack.git|" <<<"$commands"
	} >"$WORK/quickstart.sh"
	say "== the README's Quickstart, in $WORK/run"
	sed 's/^/  $ /' <<<"$commands"
	mkdir -p "$WORK/run"
	start=$(date +%s)
	(cd "$WORK/run" && bash -x "$WORK/quickstart.sh") >"$WORK/quickstart.log" 2>&1 ||
		{ tail -60 "$WORK/quickstart.log" >&2; fail "a Quickstart command failed"; }
	say "  ran in $(($(date +%s) - start)) s"
	watch_log="$WORK/quickstart.log"
	orkctl() { dc run --rm -T orkctl "$@"; }
else
	say "== release smoke $VERSION ($TOKEN, linux/$(go env GOARCH))"
	# Every archive is stamped with the commit's time, never the clock.
	export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git -C "$REPO" log -1 --format=%ct)}"
	DIST_TOOL=(go run ./internal/dist/cmd/dist)
	if [ -z "$LAYOUTS" ]; then
		say "== build: make dist (the UI, then the binaries), make release-images"
		make -s dist VERSION="$VERSION"
		LAYOUTS="$WORK/layouts"
		make -s release-images VERSION="$VERSION" OUT="$LAYOUTS" PLATFORMS="linux/$(go env GOARCH)"
	fi
	[ -d "$LAYOUTS" ] || need "no layouts at $LAYOUTS"
	"${DIST_TOOL[@]}" digests -layouts "$LAYOUTS" >"$WORK/images.tsv"
	say "$(wc -l <"$WORK/images.tsv" | tr -d ' ') images:"
	sed 's/^/  /' "$WORK/images.tsv"
	# deploy/release-smoke.yaml names each image SMOKE_<NAME>_IMAGE.
	while IFS=$'\t' read -r name digest; do
		variable="SMOKE_$(tr 'a-z-' 'A-Z_' <<<"$name")_IMAGE"
		export "$variable=$REGISTRY/orkstack/$name:$TOKEN@$digest"
	done <"$WORK/images.tsv"

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

	say "== sign: each image by digest, with a throwaway key, as publish signs"
	# publish's command and flags (.github/workflows/release.yml); only the key,
	# the registry, and --allow-insecure-registry for its plain HTTP differ.
	# `other` signs nothing: the namespace that requires it is the refusal.
	export COSIGN_PASSWORD="$TOKEN"
	cos() { docker run --rm --network host --user "$(id -u):$(id -g)" -e HOME=/tmp -e COSIGN_PRIVATE_KEY -e COSIGN_PASSWORD -v "$WORK/keys:/keys" "$COSIGN" "$@"; }
	for key in release other; do
		mkdir -p "$WORK/keys/$key"
		cos generate-key-pair --output-key-prefix "/keys/$key/cosign" >"$WORK/keys.log" 2>&1 || { cat "$WORK/keys.log" >&2; fail "cosign generate-key-pair"; }
	done
	export COSIGN_PRIVATE_KEY
	COSIGN_PRIVATE_KEY="$(cat "$WORK/keys/release/cosign.key")"
	while IFS=$'\t' read -r name digest; do
		ref="$REGISTRY/orkstack/$name@$digest"
		cos sign --key env://COSIGN_PRIVATE_KEY --new-bundle-format=false --use-signing-config=false --tlog-upload=false --allow-insecure-registry --yes "$ref" >"$WORK/sign.log" 2>&1 ||
			{ cat "$WORK/sign.log" >&2; fail "cosign sign $ref"; }
		cos verify --key /keys/release/cosign.pub --insecure-ignore-tlog=true --allow-insecure-registry "$ref" >"$WORK/sign.log" 2>&1 ||
			{ cat "$WORK/sign.log" >&2; fail "cosign verify $ref"; }
		say "  $name signed, and cosign verify accepts it"
	done <"$WORK/images.tsv"
	# SHA256SUMS's signing, on a file of the smoke's own: the docs' two checks.
	cp "$WORK/images.tsv" "$WORK/keys/SUMS"
	cos sign-blob --key env://COSIGN_PRIVATE_KEY --new-bundle-format=false --use-signing-config=false --tlog-upload=false --yes --output-signature /keys/SUMS.sig /keys/SUMS >"$WORK/sign.log" 2>&1 ||
		{ cat "$WORK/sign.log" >&2; fail "cosign sign-blob"; }
	cos verify-blob --key /keys/release/cosign.pub --insecure-ignore-tlog=true --signature /keys/SUMS.sig /keys/SUMS >"$WORK/sign.log" 2>&1 ||
		{ cat "$WORK/sign.log" >&2; fail "cosign verify-blob"; }
	openssl dgst -sha256 -verify "$WORK/keys/release/cosign.pub" -signature <(base64 -d "$WORK/keys/SUMS.sig") "$WORK/keys/SUMS" >/dev/null ||
		fail "openssl does not verify the blob signature"
	unset COSIGN_PRIVATE_KEY
	say "  a blob signed, and cosign verify-blob and openssl accept it"

	say "== up: the release's images"
	dc up -d --wait --no-build --quiet-pull

	say "== orkctl (its image)"
	got="$(dc run --rm -T orkctl version)"
	grep -q "^orkctl: *$VERSION\$" <<<"$got" || fail "orkctl version says: $got"
	say "  version $VERSION"
	dc run --rm -T orkctl apply examples/each-fan-out.ork.yaml >"$WORK/apply.log" || { cat "$WORK/apply.log" >&2; fail "apply"; }
	say "  applied each-fan-out"

	say "== orkctl (the archive's binary)"
	mkdir -p "$WORK/archive"
	tar -xzf "$REPO/dist/orkctl-linux-$(go env GOARCH).tar.gz" -C "$WORK/archive"
	ORKCTL="$(find "$WORK/archive" -type f -name orkctl | head -1)"
	[ -x "$ORKCTL" ] || fail "dist/orkctl-linux-$(go env GOARCH).tar.gz holds no executable orkctl"
	export ORK_SERVER="http://127.0.0.1:$ORKSTACK_API_PORT"
	watch_log="$WORK/watch.log"
	"$ORKCTL" run submit each-fan-out --watch >"$watch_log" 2>&1 || { cat "$watch_log" >&2; fail "run submit --watch"; }
	orkctl() { "$ORKCTL" "$@"; }
fi

say "== the run"
run="$(grep -oE '^Submitted run ork-[0-9]+-[0-9]+' "$watch_log" | head -1 | cut -d' ' -f3)"
[ -n "$run" ] || fail "no run id in: $(tail -3 "$watch_log")"
grep -q "finished with status SUCCEEDED" "$watch_log" || fail "the run did not finish SUCCEEDED: $(tail -3 "$watch_log")"
say "  $run SUCCEEDED"
orkctl run logs "$run" --task merge >"$WORK/merge.log"
[ "$(tr -d '\r' <"$WORK/merge.log" | grep -xE '[abcd]' | tr -d '\n')" = "abcd" ] || fail "merge's log is: $(cat "$WORK/merge.log")"
say "  merge logged a b c d"

if [ -n "$VERSION" ]; then
	say "== signatures: OrkStack's own verifier, at apply, submit and dispatch"
	"$ORKCTL" admin image-policy set --signing-key "release=$WORK/keys/release/cosign.pub" \
		--signing-key "other=$WORK/keys/other/cosign.pub" >"$WORK/policy.log" 2>&1 || { cat "$WORK/policy.log" >&2; fail "admin image-policy set"; }
	"$ORKCTL" namespace create signed --require-signed-by cosign:release >"$WORK/policy.log" 2>&1 || { cat "$WORK/policy.log" >&2; fail "namespace create signed"; }
	"$ORKCTL" namespace create other-signer --require-signed-by cosign:other >"$WORK/policy.log" 2>&1 || { cat "$WORK/policy.log" >&2; fail "namespace create other-signer"; }
	# The refusal shows the policy is judged, not waved through: the key
	# `other` signed nothing.
	if "$ORKCTL" -n other-signer apply "$REPO/examples/each-fan-out.ork.yaml" >"$WORK/refused.log" 2>&1; then
		fail "a namespace that requires a key that signed nothing accepted each-fan-out"
	fi
	grep -q 'none verifies with key "other"' "$WORK/refused.log" || fail "the refusal is not the signature's: $(cat "$WORK/refused.log")"
	say "  refused under cosign:other: $(grep -m1 -o 'none verifies with key "other"' "$WORK/refused.log")"
	"$ORKCTL" -n signed apply "$REPO/examples/each-fan-out.ork.yaml" >"$WORK/signed.log" 2>&1 || { cat "$WORK/signed.log" >&2; fail "apply under cosign:release"; }
	"$ORKCTL" -n signed run submit each-fan-out --watch >"$WORK/signed.log" 2>&1 || { cat "$WORK/signed.log" >&2; fail "run submit --watch under cosign:release"; }
	signed_run="$(grep -oE '^Submitted run ork-[0-9]+-[0-9]+' "$WORK/signed.log" | head -1 | cut -d' ' -f3)"
	grep -q "finished with status SUCCEEDED" "$WORK/signed.log" || fail "the signed run did not finish SUCCEEDED: $(tail -3 "$WORK/signed.log")"
	say "  $signed_run SUCCEEDED in a namespace that requires cosign:release"
fi

say "== ui"
# The engine serves the UI on its own port: a client route boots from the
# build's index.html, whose scripts are the build's content-hashed files
# under /_app/immutable/. The placeholder an ork built without the UI serves
# names none, so this tells the real build from it.
UI="http://127.0.0.1:$ORKSTACK_API_PORT"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$UI/orkflows")" = 200 ] || fail "$UI/orkflows does not answer 200"
# Read whole before grep, so grep's early exit cannot fail curl under pipefail.
page="$(curl -s "$UI/orkflows")"
grep -qF '/_app/immutable/' <<<"$page" || fail "$UI/orkflows is not the UI's build (no /_app/immutable/ in it): ork serves the placeholder"
gallery="$(curl -s "$UI/apply/examples.json")"
grep -q 'each-fan-out' <<<"$gallery" || fail "the UI's example gallery ($UI/apply/examples.json) does not list each-fan-out"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$UI/apply/examples/each-fan-out.ork.yaml")" = 200 ] ||
	fail "$UI/apply/examples/each-fan-out.ork.yaml does not answer 200"
# The browser's handshake from the page's own origin, the host it connects
# to: the bridge's default admits it. A refused upgrade answers 403.
ws="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 \
	-H 'Connection: Upgrade' -H 'Upgrade: websocket' \
	-H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==' \
	-H "Origin: $UI" \
	"$UI/ws/runs/ork-probe-nonexistent?from_sequence=0" || true)"
[ "$ws" = 404 ] || fail "the engine answers the UI's WebSocket with $ws (403 means it refuses its own origin)"
say "  /orkflows is the UI's build, the gallery lists each-fan-out, the WebSocket from the UI's origin is accepted"

say "quickstart-smoke: PASS (${VERSION:-quickstart}, $TOKEN)"
