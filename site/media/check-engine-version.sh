#!/usr/bin/env bash
# The version a capture would show in the UI's sidebar on every frame. make up
# builds with git describe unless told otherwise, so a capture of a development
# stack shows a build string such as v0.1.0-276-g9970b2c, which means nothing to
# a reader and is stale from the next commit on. Only a release-shaped version
# passes. Exit 77, as run.sh's other prerequisites do.
set -u
version="${1:-}"
if [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
	exit 0
fi
printf 'site-media: the engine calls itself %q, which every UI capture would show; restart the stack as a release: make down && VERSION=vX.Y.Z ORK_ORKER_ID=orker-example make up\n' "$version" >&2
exit 77
