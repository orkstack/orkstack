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
UI="${ORK_UI_URL:-http://127.0.0.1:5173}"
VHS_IMAGE="$(grep -v '^#' "$SITE/media/vhs-image.txt" | head -1)"

need() {
	printf 'site-media: %s\n' "$*" >&2
	exit 77
}

{ command -v docker >/dev/null && docker info >/dev/null 2>&1; } || need "Docker is not running"
command -v ffmpeg >/dev/null || need "ffmpeg is not installed"
command -v bun >/dev/null || need "bun is not installed"
[ -x "$REPO/bin/orkctl" ] || need "bin/orkctl is missing: run make up"
"$REPO/bin/orkctl" --server "$ENGINE" pool list --json 2>/dev/null | grep -q '"liveOrkerCount": [1-9]' ||
	need "no engine with a live Orker at $ENGINE: run make infra-up, make -C images push, make up"
curl -sf -o /dev/null "$UI/orkflows" || need "the UI is not answering at $UI: run make up"
docker image inspect "$VHS_IMAGE" >/dev/null 2>&1 || need "the VHS image is not on this host: docker pull $VHS_IMAGE"

WORK="$SITE/.media-work"

echo "== UI (Playwright) =="
(cd "$SITE" && bun media/capture.ts)

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
		{
			echo "Output /out/$name.webm"
			echo "Output /out/$name.mp4"
			echo "Set Theme $theme_json"
			echo "Source /tapes/$(basename "$tape")"
		} >"$VHS/tapes/run-$name.tape"
		docker run --rm --network host \
			-v "$VHS/tapes:/tapes:ro" \
			-v "$VHS/out:/out" \
			-v "$VHS/bin:/work/bin:ro" \
			-v "$REPO/examples:/work/examples:ro" \
			"$VHS_IMAGE" "/tapes/run-$name.tape"
		ffmpeg -y -loglevel error -sseof -2 -i "$VHS/out/$name.mp4" -frames:v 1 "$VHS/out/$name.png"
	done
done
(cd "$SITE" && node media/finish.mjs "$VHS/out" "$VHS_IMAGE")
rm -rf -- "$WORK"
echo "site-media: done. Look at every asset before committing it (site/src/assets/media, site/public/media)."
