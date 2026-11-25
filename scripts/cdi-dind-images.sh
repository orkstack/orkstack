#!/usr/bin/env bash
# cdi-dind-images.sh [--pull]: the images the CDI dind tests run
# (internal/testsupport/cdidind, whose pins are the one source), one a line; with
# --pull, pulls each that this host lacks, by its digest. The tests never pull.
# Exit 77: a registry or the container engine could not be reached, a missing
# prerequisite of a proof. Exit 1, which fails one: a pin that does not read as one
# image by digest (it lists nothing), or a registry's answer (a digest that is not
# there, or is another), and any failure not known to be the network's.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source=${ORK_CDI_DIND_SOURCE:-$repo/internal/testsupport/cdidind/cdidind.go}
pattern='^const [A-Za-z]+Image = "([a-z0-9./-]+(:[A-Za-z0-9._-]+)?@sha256:[0-9a-f]{64})"$'
total=$(grep -Ec '^const [A-Za-z]+Image\b' "$source" || true)
images=$(sed -En "s|$pattern|\1|p" "$source")
parsed=$(grep -c . <<<"$images" || true)
if [ "$total" -eq 0 ] || [ "$total" -ne "$parsed" ]; then
  echo "cdi-dind-images: $parsed of $total image pins of $source read as an image by digest" >&2
  exit 1
fi
if [ "${1:-}" != --pull ]; then
  printf '%s\n' "$images"
  exit 0
fi
if ! command -v docker >/dev/null; then
  echo "cdi-dind-images: docker is not on PATH" >&2
  exit 77
fi
# What a registry answered is judged before what the network did: a failed request
# may be a certificate nobody trusts.
answered='x509|not found|manifest unknown|denied: |unexpected commit digest|failed precondition'
unreachable='failed to do request|dial tcp|i/o timeout|TLS handshake timeout|connection reset|connection refused|no route to host|network is unreachable|broken pipe|\bEOF\b|Client.Timeout|context deadline exceeded|toomanyrequests|429 Too Many Requests|50[0234] [A-Z]|Cannot connect to the Docker daemon|failed to connect to the docker API|while trying to connect to the [Dd]ocker'
while read -r image; do
  docker image inspect "$image" >/dev/null 2>&1 && continue
  said=$(docker pull -q "$image" 2>&1 >/dev/null </dev/null) && continue
  echo "cdi-dind-images: $image could not be pulled: $said" >&2
  grep -Eq "$answered" <<<"$said" && exit 1
  grep -Eq "$unreachable" <<<"$said" && exit 77
  exit 1
done <<<"$images"
