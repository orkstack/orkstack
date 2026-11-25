#!/usr/bin/env bash
# nvidia-hooks-fetch.sh: fetch every build of NVIDIA's nvidia-cdi-hook that the
# compiled table (internal/orker/cdi_hook_table.go) lists for this host's arch,
# for TestIntegration_NVIDIAEmulation (docs/PROOFS.md). Each build is fetched
# by `gencdihooks -fetch`, which trusts what the generator trusts and no more:
# NVIDIA's repository key by its pinned fingerprint, the signed metadata, and
# the package's signed digest. The test then holds each binary to the table's
# own SHA-256 for its row.
#
#   scripts/nvidia-hooks-fetch.sh DIR   DIR must not exist yet; it gets one
#                                       <version>-<arch>-<packaging>/nvidia-cdi-hook
#                                       per row, and the test is run with
#                                       ORK_TEST_NVIDIA_HOOKS=DIR
#
# It needs network and Go, from the repository root.
set -euo pipefail

out=${1:?usage: nvidia-hooks-fetch.sh DIR (a directory that does not exist yet)}
cd "$(dirname "$0")/.."
arch=$(go env GOARCH)
table=internal/orker/cdi_hook_table.go

mkdir "$out"
rows=$(scripts/nvidia-hooks-rows.sh "$table" "$arch")
if [ -z "$rows" ]; then
  echo "nvidia-hooks-fetch: $table lists no build for $arch" >&2
  exit 1
fi
bin=$(mktemp -d)
trap 'rm -rf "$bin"' EXIT
go build -o "$bin/gencdihooks" ./internal/orker/cmd/gencdihooks
while read -r version packaging _; do
  "$bin/gencdihooks" -fetch "$version" -arch "$arch" -packaging "$packaging" -out "$out/$version-$arch-$packaging"
done <<<"$rows"
echo "nvidia-hooks-fetch: $(grep -c . <<<"$rows") builds in $out; run with ORK_TEST_NVIDIA_HOOKS=$out"
