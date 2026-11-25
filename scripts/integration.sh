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
# every prerequisite skip into a failure. A run that holds
# TestIntegration_NVIDIAEmulation first makes what it needs ready
# (scripts/nvidia-emulation.sh), unless ORK_TEST_NVIDIA_HOOKS is set already; it
# exits 77 when a registry or repository cannot be reached (a run with
# ORK_REQUIRE_INFRA=0 goes on) and 1 when an image or build is not the pinned one.
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
# holds_nvidia_emulation: the packages and pattern given hold that test. A
# pattern grep cannot read is taken to: more is made ready rather than less.
holds_nvidia_emulation() {
	local pkg held=1
	[ ${#pkgs[@]} -eq 0 ] && held=0
	for pkg in "${pkgs[@]}"; do
		case "${pkg%/}" in
		./internal/orker | ./internal/orker/... | ./internal/... | ./... | */orkstack/internal/orker) held=0 ;;
		esac
	done
	[ "$held" -eq 0 ] || return 1
	[ ${#run[@]} -eq 0 ] && return 0
	grep -Pq -- "${run[1]%%/*}" <<<TestIntegration_NVIDIAEmulation 2>/dev/null
	[ $? -ne 1 ]
}
if [ -z "${ORK_TEST_NVIDIA_HOOKS:-}" ] && holds_nvidia_emulation; then
	ORK_TEST_NVIDIA_HOOKS="$("${NVIDIA_EMULATION:-scripts/nvidia-emulation.sh}")"
	rc=$?
	if [ "$rc" -ne 0 ] && ! { [ "$rc" -eq 77 ] && [ "${ORK_REQUIRE_INFRA:-1}" = 0 ]; }; then
		echo "integration.sh: what TestIntegration_NVIDIAEmulation needs is not ready (exit $rc), nothing run" >&2
		exit "$rc"
	fi
	export ORK_TEST_NVIDIA_HOOKS
fi
# What dead proofs and test runs left (buckets holding the store's volumes,
# work roots, containers) is removed first, as every proof does: only what a
# process that is gone left, never what a running proof recorded (`proofrun
# clean`, docs/PROOFS.md §10).
cleanup="$(mktemp)"
go run ./test/tools/proofrun clean -remove >"$cleanup" 2>&1 || :
grep -m1 '^cleanup:' "$cleanup" || echo "cleanup: did not finish"
out="$PWD/.data/integration/$(date -u +%Y%m%dT%H%M%SZ)-$$"
mkdir -p "$out"
mv "$cleanup" "$out/cleanup.log"
ns="${ORK_TEMPORAL_NAMESPACE:-ork}-$(date +%s)"
ORK_REQUIRE_INFRA="${ORK_REQUIRE_INFRA:-1}" go run ./test/tools/proofrun integration \
	-out "$out" -jobs "${PROOF_JOBS:-6}" -namespace-prefix "$ns" "${run[@]}" "${pkgs[@]}"
status=$?
echo "integration.sh: ledger, plan and per-job logs in $out"
exit "$status"
