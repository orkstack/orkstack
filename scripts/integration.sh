#!/usr/bin/env bash
# Run integration suites the way both proof tiers do (AGENTS.md §11.1):
# test/tools/proofrun compiles each package's test binary once, lists its
# tests, and runs them as separate processes in the package's directory,
# PROOF_JOBS at a time (PROOF_JOBS=1 runs them one after another). Every
# listed test needs an outcome, and a skip fails unless
# scripts/proof-allowed-skips.txt names it.
#
#   scripts/integration.sh                         every integration package
#   scripts/integration.sh ./internal/engine       one package
#   scripts/integration.sh ./internal/engine -run '^TestIntegration_X$'
#
# The ledger, plan and per-job logs land in a fresh directory under
# .data/integration/ (printed at the end). Needs `make infra-up` and this
# tree's activity images (`make -C images push`); ORK_REQUIRE_INFRA=1 turns
# every prerequisite skip into a failure.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

pkgs=()
run=()
while [ $# -gt 0 ]; do
	case "$1" in
	-run)
		[ $# -ge 2 ] || { echo "integration.sh: -run needs a pattern" >&2; exit 2; }
		run=(-run "$2")
		shift 2
		;;
	-*) echo "integration.sh: unknown option $1" >&2; exit 2 ;;
	*) pkgs+=("$1"); shift ;;
	esac
done
out="$PWD/.data/integration/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$out"
ns="${ORK_TEMPORAL_NAMESPACE:-ork}-$(date +%s)"
ORK_REQUIRE_INFRA="${ORK_REQUIRE_INFRA:-1}" go run ./test/tools/proofrun integration \
	-out "$out" -jobs "${PROOF_JOBS:-6}" -namespace-prefix "$ns" "${run[@]}" "${pkgs[@]}"
status=$?
echo "integration.sh: ledger, plan and per-job logs in $out"
exit "$status"
