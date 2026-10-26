#!/usr/bin/env bash
# List what test processes that are gone left behind, and remove it when asked
# (AGENTS.md §11.1, docs/PROOFS.md §10):
#
#   scripts/proof-clean.sh            list
#   scripts/proof-clean.sh --remove   remove what is listed
#
#   - object-storage buckets named orkstack-<label>-<pid>-<suffix>
#     (internal/testsupport/s3bucket keeps a failed process's bucket, and a
#     proof that was stopped leaves its passing processes' ones): the ones the
#     store lists, and the ones the proofs of every worktree of this repository
#     recorded, since SeaweedFS with its auth off lists none;
#   - Orker work roots under each worktree's .data/work, named
#     <label>-p<pid>-<suffix> (internal/testsupport/proof.WorkRootPattern);
#   - activity containers whose Orker's process is gone (its proof Orker id
#     ends in the pid, or its default id hashes a work root naming the pid);
#   - proof Temporal containers whose proof is gone.
#
# It is `proofrun clean` (test/tools/proofrun/clean.go), which every proof and
# scripts/integration.sh also run first. Stop only what you started (AGENTS.md
# §11.1): a leftover is removed only when the process its name records is
# gone, and never when a proof that is still running recorded it, so this is
# safe to run beside a running proof or test. The shared development bucket,
# orkstack-artifacts, and the dev stack's own work root and containers are
# never touched. Nothing is removed without --remove.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

case "${1:-}" in
"") exec go run ./test/tools/proofrun clean ;;
--remove) exec go run ./test/tools/proofrun clean -remove ;;
*)
	echo "usage: scripts/proof-clean.sh [--remove]" >&2
	exit 2
	;;
esac
