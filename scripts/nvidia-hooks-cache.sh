#!/usr/bin/env bash
# nvidia-hooks-cache.sh: the proofs' copy of every nvidia-cdi-hook build the
# compiled table lists for this host's arch, for TestIntegration_NVIDIAEmulation
# (docs/PROOFS.md). It prints the directory to export as
# ORK_TEST_NVIDIA_HOOKS on stdout; everything else goes to stderr.
#
#   scripts/nvidia-hooks-cache.sh
#
# The cache is $NVIDIA_HOOKS_CACHE (default ~/.cache/orkstack-nvidia-hooks)/<sha>,
# <sha> being the SHA-256 of internal/orker/cdi_hook_table.go, so a changed table
# is a new directory. A miss is fetched by scripts/nvidia-hooks-fetch.sh into a
# temporary sibling and renamed into place, so a killed fetch never leaves a
# directory that looks present. A hit is trusted no more than a fresh fetch:
# every binary is hashed on every run against the table's own SHA-256.
#
# Exit status: 0 verified; 77 the repository could not be reached (a missing
# prerequisite); 1 a binary is absent or is not the table's, which fails the gate.
set -uo pipefail

cd "$(dirname "$0")/.."
table=internal/orker/cdi_hook_table.go
arch=$(go env GOARCH)
sha=$(sha256sum "$table" | cut -d' ' -f1)
root=${NVIDIA_HOOKS_CACHE:-$HOME/.cache/orkstack-nvidia-hooks}
dir=$root/$sha

if [ ! -d "$dir" ]; then
  # Not being able to make the cache directory is a fault of this host, not a
  # missing prerequisite: the proof fails.
  mkdir -p "$root" || { echo "nvidia-hooks-cache: cannot make $root" >&2; exit 1; }
  tmp=$root/.fetch-$sha.$$
  trap 'rm -rf "$tmp"' EXIT
  said=$(scripts/nvidia-hooks-fetch.sh "$tmp" 2>&1)
  rc=$?
  printf '%s\n' "$said" >&2
  if [ "$rc" -ne 0 ]; then
    echo "nvidia-hooks-cache: nothing is cached, the fetch failed ($rc): $(tail -n1 <<<"$said")" >&2
    [ "$rc" -eq 77 ] && exit 77
    exit 1
  fi
  # A concurrent proof may have renamed its own copy in first; it is verified
  # below like any hit, so the loser's temporary directory is just dropped.
  mv -T "$tmp" "$dir" 2>/dev/null || [ -d "$dir" ] || { echo "nvidia-hooks-cache: cannot place $dir" >&2; exit 1; }
fi

rows=$(scripts/nvidia-hooks-rows.sh "$table" "$arch") || exit 1
if [ -z "$rows" ]; then
  echo "nvidia-hooks-cache: $table lists no build for $arch" >&2
  exit 1
fi
bad=0
while read -r version packaging want; do
  bin=$dir/$version-$arch-$packaging/nvidia-cdi-hook
  got=$(sha256sum "$bin" 2>/dev/null | cut -d' ' -f1)
  if [ "$got" != "$want" ]; then
    echo "nvidia-hooks-cache: $bin is sha256:${got:-absent}, the table's row $version $arch $packaging says $want" >&2
    bad=1
  fi
done <<<"$rows"
[ "$bad" -eq 0 ] || exit 1
echo "nvidia-hooks-cache: $(grep -c . <<<"$rows") builds verified in $dir" >&2
echo "$dir"
