#!/usr/bin/env bash
# proof-collect.sh — assemble the merged CI proof's copy of the parts.
#
#   scripts/proof-collect.sh PARTS JOBS OUT
#
# PARTS holds one directory per part attempt, as the proof job fetched them;
# JOBS holds GitHub's record of every attempt's jobs; OUT is the merged proof
# directory `proofrun merge` already wrote proof.json into. The copy carries
# what docs/PROOFS.md §7 says the merged artifact carries, and no more:
#
#   - every part's RECORDS: part.json, gates.tsv, its ledgers, its setup log
#     and its gate logs — never its bin/, layouts/ or artifacts/, which stay
#     in the part's own artifact;
#   - for a part that did NOT pass — a gate recorded as anything but `pass`,
#     or no part.json at all (its process died) — its events and per-job
#     logs too (`*.events.jsonl`, `integration/`, `unit.log`, `race.log`,
#     `race-repeat.log`, the fuzz lane's `fuzz/` logs), the files §8 reads a
#     failure from, and the fuzz lane's `crashers/`: the inputs that broke a
#     target. A passing part's are left out: for a green run they were 900 MB
#     of the merged proof, and the ledger holds every test's outcome either
#     way;
#   - GitHub's job records, OUT/jobs/, which hold the logs of failed jobs.
#
# `proofrun merge` reads PARTS, never this copy, so what is left out here
# changes no verdict. Each part's decision is printed, so the proof job's
# log says what the artifact holds.
set -uo pipefail

if [ $# -ne 3 ]; then
	echo "usage: scripts/proof-collect.sh PARTS JOBS OUT" >&2
	exit 2
fi
parts="$1" jobs="$2" out="$3"
[ -d "$parts" ] || { echo "proof-collect: $parts is not a directory" >&2; exit 2; }
mkdir -p "$out/parts" || exit 1

# part_passed: the part left a part.json whose every gate passed.
part_passed() { # <dir>
	[ -f "$1/part.json" ] && jq -e '[.gates[]? | select(.result != "pass")] | length == 0' "$1/part.json" >/dev/null 2>&1
}

code=0
for dir in "$parts"/*/; do
	[ -d "$dir" ] || continue
	name="$(basename "$dir")"
	excludes=(--exclude '/bin/' --exclude '/layouts/' --exclude '/artifacts/')
	if part_passed "$dir"; then
		excludes+=(--exclude '/*.events.jsonl' --exclude '/integration/' --exclude '/unit.log'
			--exclude '/race.log' --exclude '/race-repeat.log' --exclude '/fuzz/')
		echo "collect $name: passed; records only"
	else
		echo "collect $name: did not pass, or left no part.json; records, events and logs"
	fi
	if ! rsync -a "${excludes[@]}" "$dir" "$out/parts/$name/"; then
		echo "collect $name: copy failed" >&2
		code=1
	fi
done
if [ -d "$jobs" ]; then
	rsync -a "$jobs/" "$out/jobs/" || { echo "collect jobs: copy failed" >&2; code=1; }
else
	echo "collect jobs: $jobs is absent; the merged proof carries no job records"
fi
exit "$code"
