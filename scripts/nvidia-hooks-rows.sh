#!/usr/bin/env bash
# nvidia-hooks-rows.sh TABLE ARCH: the rows of the compiled hook table
# (internal/orker/cdi_hook_table.go, as gencdihooks writes it) for ARCH, one
# "version packaging binary-sha256" per line. It fails when any row of the table
# does not parse, so a change in the generator's formatting is an error here, not
# a row silently left unchecked (the caller also fails on no row for its arch).
set -euo pipefail
table=${1:?usage: nvidia-hooks-rows.sh TABLE ARCH}
arch=${2:?usage: nvidia-hooks-rows.sh TABLE ARCH}
pattern='^[[:space:]]*\{Version: "([^"]+)", Arch: "([a-z0-9]+)", Packaging: "([a-z]+)", Package: "[^"]+", PackageDigest: "[^"]+", BinarySHA256: "([0-9a-f]{64})"\},$'
total=$(grep -c '{Version:' "$table" || true)
parsed=$(grep -Ec "$pattern" "$table" || true)
if [ "$total" -eq 0 ] || [ "$total" -ne "$parsed" ]; then
  echo "nvidia-hooks-rows: $parsed of $total rows of $table parse; the table's format changed" >&2
  exit 1
fi
sed -En "s/$pattern/\1 \3 \4 \2/p" "$table" | awk -v a="$arch" '$4 == a {print $1, $2, $3}'
