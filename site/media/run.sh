#!/usr/bin/env bash
# make site-media: regenerate every screenshot and recording the site shows,
# from the real stack, in both themes, and record where they came from in
# site/src/assets/media/manifest.json.
#
#   1. checks the local stack is up with the first-party images (it starts
#      nothing: make infra-up, make -C images push, make up come first);
#   2. media/capture.ts: the UI screenshots, at one and two device pixels
#      per CSS pixel, and the end-to-end recording, driven by Playwright in
#      the UI e2e suite's pinned browser image;
#   3. media/tapes/*.tape: the terminal recordings, rendered by VHS in its
#      image pinned by digest (media/vhs-image.txt), with a terminal theme
#      made from ui/src/tokens.css for each site theme.
#
# Exit 77 when a prerequisite is missing, as the proofs do.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
REPO="$PWD"
SITE="$REPO/site"
ENGINE="${ORK_SERVER:-http://127.0.0.1:8080}"
# The capture drives the BUILT UI, as the UI's own e2e suite does: the dev
# server's hundreds of module requests through the browser container's
# tunnel take longer than the example's fan-out runs. It is served on
# 127.0.0.1:5173, an origin make up's engine accepts for its live socket,
# beside the dev server (which listens on localhost).
UI_PORT="${SITE_MEDIA_UI_PORT:-5173}"
UI="http://127.0.0.1:$UI_PORT"
VHS_IMAGE="$(grep -v '^#' "$SITE/media/vhs-image.txt" | head -1)"

need() {
	printf 'site-media: %s\n' "$*" >&2
	exit 77
}

command -v ffmpeg >/dev/null || need "ffmpeg is not installed"

# The README's animation of the UI: the end-to-end recording as an animated
# WebP, which GitHub shows inline where it cannot play a video (a video file
# in the repository is only a link there). The README draws it 880 CSS
# pixels wide, so it is 1760 pixels wide: two device pixels per CSS pixel.
# Ten frames a second; a frame no different from the one before is dropped
# (its predecessor lasts longer, up to two seconds), and libwebp's animation
# encoder stores each frame as its change from the last, so the held screens
# cost next to nothing. It needs only ffmpeg and the recordings, so
# SITE_MEDIA_ONLY=readme redoes it alone.
readme_ui_demo() {
	for theme in light dark; do
		ffmpeg -y -loglevel error -i "$SITE/public/media/e2e-demo-$theme.mp4" \
			-vf "fps=10,scale=1760:-2:flags=lanczos,mpdecimate=hi=768:lo=320:frac=0.1:max=20" -fps_mode vfr \
			-loop 0 -c:v libwebp_anim -quality 72 -compression_level 4 \
			-an "$REPO/docs/images/ui-demo-$theme.webp"
		echo "  docs/images/ui-demo-$theme.webp ($(du -h "$REPO/docs/images/ui-demo-$theme.webp" | cut -f1))"
	done
}
# Read the whole list first: grep -q stops at the first match, and under
# pipefail the SIGPIPE ffmpeg then takes would fail the check.
encoders="$(ffmpeg -hide_banner -encoders 2>/dev/null)"
grep -q libwebp_anim <<<"$encoders" || need "ffmpeg has no libwebp_anim encoder (the README's UI animation is an animated WebP)"
if [ "${SITE_MEDIA_ONLY:-}" = readme ]; then
	readme_ui_demo
	exit 0
fi

{ command -v docker >/dev/null && docker info >/dev/null 2>&1; } || need "Docker is not running"


command -v bun >/dev/null || need "bun is not installed"
command -v node >/dev/null || need "node is not installed"
command -v go >/dev/null || need "go is not installed (the recordings use a static orkctl)"
[ -d "$SITE/node_modules" ] || need "site/node_modules is missing: cd site && bun install --frozen-lockfile"
[ -d "$REPO/ui/node_modules/playwright-core" ] || need "ui/node_modules is missing: cd ui && bun install (the browser server runs the UI's Playwright)"
# The browser server runs the UI's playwright-core; the capture script is the
# site's Playwright client. One protocol: the same version, or no capture.
ui_pw="$(node -p "require('$REPO/ui/node_modules/playwright-core/package.json').version")"
site_pw="$(node -p "require('$SITE/node_modules/@playwright/test/package.json').version")"
[ "$ui_pw" = "$site_pw" ] || need "Playwright differs: the UI runs $ui_pw, the site $site_pw; pin site/package.json's @playwright/test to $ui_pw"
[ -x "$REPO/bin/orkctl" ] || need "bin/orkctl is missing: run make up"
"$REPO/bin/orkctl" --server "$ENGINE" pool list --json 2>/dev/null | grep -q '"liveOrkerCount": [1-9]' ||
	need "no engine with a live Orker at $ENGINE: run make infra-up, make -C images push, make up"
curl -sf -o /dev/null "http://localhost:5173/orkflows" || need "the UI is not answering at localhost:5173: run make up"
! curl -s -o /dev/null "$UI/" || need "something already answers at $UI; set SITE_MEDIA_UI_PORT to a port make up's engine accepts"
docker image inspect "$VHS_IMAGE" >/dev/null 2>&1 || need "the VHS image is not on this host: docker pull $VHS_IMAGE"
# No frame may show a real host name, and an Orker's default id embeds this
# host's (internal/orker defaultOrkerID). Start the stack with a neutral id:
#   make down && ORK_ORKER_ID=orker-example make up
if "$REPO/bin/orkctl" --server "$ENGINE" orker list --json 2>/dev/null | grep -qi -- "$(hostname)"; then
	need "an Orker's id names this host ($(hostname)); restart the stack with ORK_ORKER_ID=orker-example make up"
fi

# The terminal recordings to make: every tape, or the ones SITE_MEDIA_TAPES
# names (SITE_MEDIA_TAPES=activity-test redoes that one, keeping the rest).
TAPES=()
selected="${SITE_MEDIA_TAPES:-$(cd "$SITE/media/tapes" && ls *.tape | sed 's/\.tape$//')}"
for name in ${selected//,/ }; do
	[ -f "$SITE/media/tapes/$name.tape" ] || need "SITE_MEDIA_TAPES names no tape $name (site/media/tapes/)"
	TAPES+=("$name")
done

# The quickstart recording shows a first apply, in the namespace the
# quickstart uses. An Orkflow version is immutable and pins the images it was
# applied against, so a second apply after the images were rebuilt is
# refused: record it on a stack that has not stored each-fan-out yet.
if [[ " ${TAPES[*]} " == *" quickstart "* ]] &&
	"$REPO/bin/orkctl" --server "$ENGINE" -n default orkflow get each-fan-out >/dev/null 2>&1; then
	need "each-fan-out is already stored in the default namespace, and the recording shows a first apply; record on a fresh stack (make reset, make infra-up, make -C images push, make up)"
fi

WORK="$SITE/.media-work"
# The scratch directory goes whatever happens, so a failed run leaves no copy
# of a binary or a half-made recording behind.
trap 'rm -rf -- "$WORK"' EXIT
mkdir -p "$WORK"

# SITE_MEDIA_ONLY=terminal redoes the recordings alone, keeping the UI captures.
if [ "${SITE_MEDIA_ONLY:-}" != terminal ]; then
echo "== UI build =="
(cd "$REPO/ui" && bun run build >/dev/null)
(cd "$REPO/ui" && HOST=127.0.0.1 PORT="$UI_PORT" ENGINE_API_URL="$ENGINE" PUBLIC_ENGINE_WS_URL="${ENGINE/http/ws}" \
	exec node build >"$WORK/ui.log" 2>&1) &
UI_PID=$!
trap 'kill "$UI_PID" 2>/dev/null || true; rm -rf -- "$WORK"' EXIT
for _ in $(seq 1 60); do curl -sf -o /dev/null "$UI/orkflows" && break; sleep 0.5; done
curl -sf -o /dev/null "$UI/orkflows" || { cat "$WORK/ui.log" >&2; exit 1; }

echo "== UI (Playwright) =="
(cd "$SITE" && ORK_UI_URL="$UI" bun media/capture.ts)
kill "$UI_PID" 2>/dev/null || true
fi

echo "== terminal (VHS) =="
VHS="$WORK/vhs"
mkdir -p "$VHS/bin" "$VHS/host" "$VHS/out" "$VHS/tapes" "$VHS/tmp"
# A static orkctl, so the container's own libc never matters.
(cd "$REPO" && CGO_ENABLED=0 go build -o "$VHS/bin/orkctl" ./cmd/orkctl)
cp "$SITE"/media/tapes/*.tape "$VHS/tapes/"

# host_tool NAME: a tool a tape runs that the VHS image does not carry (curl
# for a trigger, docker for orkctl activity test, which drives containers
# through the docker CLI as an Orker does). It is the host's own binary,
# copied with every shared library it loads and the host's own loader, and
# started through that loader, so the image's libc never matters.
host_tool() {
	local name="$1" bin dir loader
	bin="$(command -v "$name")" || need "$name is not installed (a terminal recording runs it)"
	dir="$VHS/host/$name"
	mkdir -p "$dir"
	cp -L "$bin" "$dir/$name.bin"
	ldd "$bin" | awk '$2 == "=>" && $3 ~ /^\// { print $3 } $1 ~ /^\// && $2 != "=>" { print $1 }' | sort -u |
		while read -r lib; do cp -L "$lib" "$dir/"; done
	loader="$(ldd "$bin" | awk '/ld-linux/ { print ($2 == "=>") ? $3 : $1; exit }')"
	[ -n "$loader" ] || need "$bin names no ld-linux loader (ldd)"
	printf '#!/bin/sh\nexec /work/host/%s/%s --library-path /work/host/%s /work/host/%s/%s.bin "$@"\n' \
		"$name" "$(basename "$loader")" "$name" "$name" "$name" >"$VHS/bin/$name"
	chmod +x "$VHS/bin/$name"
}
host_tool curl
host_tool docker
DOCKER_SOCK="$(docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)"
case "$DOCKER_SOCK" in
unix://*) DOCKER_SOCK="${DOCKER_SOCK#unix://}" ;;
*) need "the docker context is not a local socket ($DOCKER_SOCK); orkctl activity test is recorded against one" ;;
esac
# The activity test's workspaces are bind-mounted into the containers it
# starts by the host's daemon, so they live at a path the host and the VHS
# container share; a tape's hidden setup reads that path from host/tmpdir
# into its shell's ORK_ORKER_WORKDIR, the root orkctl takes first. It runs
# as root there; its leftovers are removed by a container too.
printf '%s\n' "$VHS/tmp" >"$VHS/host/tmpdir"
trap 'docker run --rm -v "$VHS/tmp:/t" --entrypoint sh "$VHS_IMAGE" -c "rm -rf /t/* /t/.[!.]*" >/dev/null 2>&1 || true; kill "${UI_PID:-}" 2>/dev/null || true; rm -rf -- "$WORK"' EXIT
for theme in light dark; do
	theme_json="$(node "$SITE/media/vhs-theme.mjs" "$theme")"
	for tape in "${TAPES[@]/#/$SITE/media/tapes/}"; do
		tape="$tape.tape"
		name="cli-$(basename "$tape" .tape)-$theme"
		# VHS takes paths relative to its working directory, /vhs.
		# The site plays the MP4 (a WebM of it was no smaller); the README
		# shows the quickstart as a GIF (media/finish.mjs).
		{
			echo "Output out/$name.mp4"
			[ "$name" != "cli-quickstart-$theme" ] || echo "Output out/$name.gif"
			echo "Set Theme $theme_json"
			echo "Source tapes/$(basename "$tape")"
		} >"$VHS/tapes/run-$name.tape"
		docker run --rm --network host -w /vhs \
			-v "$VHS/tapes:/vhs/tapes:ro" \
			-v "$VHS/out:/vhs/out" \
			-v "$VHS/bin:/work/bin:ro" \
			-v "$VHS/host:/work/host:ro" \
			-v "$VHS/tmp:$VHS/tmp" \
			-v "$DOCKER_SOCK:/var/run/docker.sock" \
			-v "$REPO/examples:/work/examples:ro" \
			"$VHS_IMAGE" "tapes/run-$name.tape"
		# The poster is the tape's own screenshot of the finished watch, not a
		# frame of the lossy video (and not its last frame, which shows only
		# the merge task's log).
		mv "$VHS/out/poster.png" "$VHS/out/$name.png"
	done
done
(cd "$SITE" && node media/finish.mjs "$VHS/out" "$VHS_IMAGE")
echo "== README animations =="
readme_ui_demo
echo "site-media: done. Look at every asset before committing it (site/src/assets/media, site/public/media)."
