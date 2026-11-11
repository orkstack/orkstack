#!/usr/bin/env bash
# release-notes.sh — keep every published release's notes equal to its
# CHANGELOG.md section.
#
#   make release-notes-check                      every published vX.Y.Z
#   make release-notes-publish VERSION=vX.Y.Z     republish one, after a diff and a yes
#
# A release's notes are its CHANGELOG.md section, word for word (docs/PROOFS.md
# §10). check compares the body each published release shows, above its image
# footer, with `dist notes-check` against this tree's CHANGELOG.md, and fails
# naming each version that differs and its first differing line. publish
# rebuilds one release's body from the section, keeps the footer the release
# already has (its digests), shows the diff, and edits the release only after
# a yes typed at a terminal; it refuses a release whose body has no footer. A
# release body is never edited by hand.
#
# Both read the releases over the network with gh, so they are in no proof;
# nightly.yml runs check. ORK_RELEASE_REPO names another repository
# (default orkstack/orkstack). Exit: 0 in sync or published, 1 a release
# differs or a step failed, 2 an invalid invocation.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

REPO="${ORK_RELEASE_REPO:-orkstack/orkstack}"
usage() {
	echo "usage: scripts/release-notes.sh check | publish vX.Y.Z" >&2
	exit 2
}
[ $# -ge 1 ] || usage
command -v gh >/dev/null || { echo "release-notes: gh is not on PATH" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
go build -o "$work/dist" ./internal/dist/cmd/dist

# body TAG FILE writes the body the release TAG publishes now into FILE,
# ending in exactly one newline, so a republish never grows its footer.
body() {
	local text
	text="$(gh release view "$1" --repo "$REPO" --json body --jq .body)"
	printf '%s\n' "$text" >"$2"
}

case "$1" in
check)
	[ $# -eq 1 ] || usage
	mapfile -t tags < <(gh release list --repo "$REPO" --limit 1000 --json tagName,isDraft \
		--jq '.[] | select(.isDraft | not) | .tagName' | grep -E '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$' | sort -V)
	if [ ${#tags[@]} -eq 0 ]; then
		echo "release-notes: $REPO has no published vX.Y.Z release"
		exit 0
	fi
	failed=0
	for tag in "${tags[@]}"; do
		body "$tag" "$work/$tag.md"
		"$work/dist" notes-check -version "$tag" -body "$work/$tag.md" || failed=1
	done
	exit "$failed"
	;;
publish)
	[ $# -eq 2 ] || usage
	tag="$2"
	body "$tag" "$work/published.md"
	"$work/dist" notes-republish -version "$tag" -body "$work/published.md" >"$work/new.md"
	if "$work/dist" notes-check -version "$tag" -body "$work/published.md" >/dev/null 2>&1; then
		echo "release-notes: $tag already matches its CHANGELOG.md section; nothing to publish"
		exit 0
	fi
	diff -u --label "$tag as published" --label "$tag from CHANGELOG.md" "$work/published.md" "$work/new.md" || true
	if [ ! -t 0 ]; then
		echo "release-notes: publishing needs a yes typed at a terminal; nothing was published" >&2
		exit 1
	fi
	read -r -p "Publish this body to the $tag release of $REPO? [y/N] " answer
	case "$answer" in
	y | Y | yes) ;;
	*) echo "release-notes: nothing was published"; exit 1 ;;
	esac
	gh release edit "$tag" --repo "$REPO" --notes-file "$work/new.md" >/dev/null
	body "$tag" "$work/after.md"
	"$work/dist" notes-check -version "$tag" -body "$work/after.md"
	;;
*)
	usage
	;;
esac
