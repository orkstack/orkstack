#!/usr/bin/env bash
# make site-media: regenerate every screenshot and recording the site shows,
# from the real stack, in both themes, and record where they came from in
# site/src/assets/media/manifest.json.
#
#   1. checks the local stack is up with the first-party images (it starts
#      nothing: make infra-up, make -C images push, make up come first);
#   2. media/capture.ts: the UI screenshots and the end-to-end recording,
#      driven by Playwright in the UI e2e suite's pinned browser image;
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

{ command -v docker >/dev/null && docker info >/dev/null 2>&1; } || need "Docker is not running"
{ command -v ffmpeg >/dev/null && command -v ffprobe >/dev/null; } || need "ffmpeg and ffprobe are not installed"
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

# The recording shows a first apply, in the namespace the quickstart uses. An
# Orkflow version is immutable and pins the images it was applied against, so
# a second apply after the images were rebuilt is refused: record on a stack
# that has not stored each-fan-out yet.
if "$REPO/bin/orkctl" --server "$ENGINE" -n default orkflow get each-fan-out >/dev/null 2>&1; then
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
mkdir -p "$VHS/bin" "$VHS/out" "$VHS/tapes"
# A static orkctl, so the container's own libc never matters.
(cd "$REPO" && CGO_ENABLED=0 go build -o "$VHS/bin/orkctl" ./cmd/orkctl)
cp "$SITE"/media/tapes/*.tape "$VHS/tapes/"
for theme in light dark; do
	theme_json="$(node "$SITE/media/vhs-theme.mjs" "$theme")"
	for tape in "$SITE"/media/tapes/*.tape; do
		name="cli-$(basename "$tape" .tape)-$theme"
		# VHS takes paths relative to its working directory, /vhs.
		{
			echo "Output out/$name.webm"
			echo "Output out/$name.mp4"
			echo "Set Theme $theme_json"
			echo "Source tapes/$(basename "$tape")"
		} >"$VHS/tapes/run-$name.tape"
		docker run --rm --network host -w /vhs \
			-v "$VHS/tapes:/vhs/tapes:ro" \
			-v "$VHS/out:/vhs/out" \
			-v "$VHS/bin:/work/bin:ro" \
			-v "$REPO/examples:/work/examples:ro" \
			"$VHS_IMAGE" "tapes/run-$name.tape"
		# The poster is the watch finished, three quarters in, not the last
		# frame (which shows only the merge task's log).
		dur="$(ffprobe -v error -show_entries format=duration -of csv=p=0 "$VHS/out/$name.mp4")"
		ffmpeg -y -loglevel error -ss "$(awk -v d="$dur" 'BEGIN { print d * 0.75 }')" -i "$VHS/out/$name.mp4" -frames:v 1 "$VHS/out/$name.png"
	done
done
(cd "$SITE" && node media/finish.mjs "$VHS/out" "$VHS_IMAGE")
echo "site-media: done. Look at every asset before committing it (site/src/assets/media, site/public/media)."
