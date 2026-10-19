#!/usr/bin/env bash
# github-merge-gate.sh — enable, or check, the GitHub side of the merge gate.
#
#   make github-merge-gate                 check only (default)
#   make github-merge-gate APPLY=1         apply the queue-less mode
#   make github-merge-gate APPLY=1 QUEUE=1 also apply the merge queue
#   make github-merge-gate CODEOWNER_REVIEW=0   leave code-owner review out
#   scripts/github-merge-gate.sh --verify-landed SHA
#
# The gate is built and not enforced until the repository is public: rulesets
# and branch protection are refused on a private repository. This script is
# the one command that applies them once it is. Check mode is read-only.
#
# Every ruleset body is UNVERIFIED against the API until it has been applied:
# none has ever been posted from a private repository. Read the API's answer
# before anything else.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

APPLY="${APPLY:-0}"
QUEUE="${QUEUE:-0}"
CODEOWNER_REVIEW="${CODEOWNER_REVIEW:-1}"
MODE=check
VERIFY_LANDED=""
while [ $# -gt 0 ]; do
	case "$1" in
	--check) MODE=check; shift ;;
	--apply) MODE=apply; shift ;;
	--verify-landed) VERIFY_LANDED="$2"; shift 2 ;;
	-h | --help) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done
[ "$APPLY" = 1 ] && MODE=apply

fail=0
bad() { printf 'FAIL: %s\n' "$*"; fail=1; }
ok() { printf 'ok:   %s\n' "$*"; }
warn() { printf 'warn: %s\n' "$*"; }

for tool in gh git jq go; do
	command -v "$tool" >/dev/null 2>&1 || { bad "$tool is not on PATH"; echo "github-merge-gate: cannot check"; exit 1; }
done
if ! gh auth status >/dev/null 2>&1; then
	bad "gh auth status failed"
	echo "github-merge-gate: cannot check"; exit 1
fi

owner="$(gh repo view --json owner --jq .owner.login 2>/dev/null)"
name="$(gh repo view --json name --jq .name 2>/dev/null)"
[ -n "$owner" ] && [ -n "$name" ] || { bad "not in a GitHub repository"; exit 1; }
repo="$owner/$name"
api="repos/$repo"

if [ -n "$VERIFY_LANDED" ]; then
	# The first merge through either mode: prove that the artifact's tree is
	# the landed commit's tree.
	sha="$(git rev-parse "$VERIFY_LANDED")"
	tree="$(git rev-parse "$sha^{tree}")"
	tmp="$(mktemp -d)"
	trap 'rm -rf "$tmp"' EXIT
	run_id="$(gh api "$api/commits/$sha/check-runs" --jq '[.check_runs[] | select(.name=="proof" and .app.id==15368)][0].details_url // ""' 2>/dev/null |
		sed -E 's#.*/runs/([0-9]+).*#\1#')"
	[ -n "$run_id" ] || { bad "no proof check run on $sha"; exit 1; }
	gh run download "$run_id" -R "$repo" -n "proof-$run_id-a1" -D "$tmp" 2>/dev/null ||
		gh run download "$run_id" -R "$repo" -D "$tmp" 2>/dev/null || true
	got="$(jq -r .tree "$tmp/proof.json" 2>/dev/null)"
	if [ "$got" = "$tree" ]; then
		ok "proof run $run_id measured tree $tree, which is $sha's tree"
		exit 0
	fi
	bad "proof run $run_id measured tree ${got:-<none>}, and $sha's tree is $tree"
	exit 1
fi

repo_json="$(gh api "$api" 2>/dev/null)" || { bad "cannot read $api"; exit 1; }
visibility="$(jq -r '.visibility // ""' <<<"$repo_json")"
owner_type="$(jq -r '.owner.type // ""' <<<"$repo_json")"
admin="$(jq -r '.permissions.admin // false' <<<"$repo_json")"
merge_commit="$(jq -r '.allow_merge_commit // false' <<<"$repo_json")"

[ "$admin" = true ] && ok "you are an administrator of $repo" || bad "you are not an administrator of $repo"
[ "$visibility" = public ] && ok "$repo is public" || bad "$repo is $visibility: rulesets are refused on a private repository; the gate is not enforced until it is public"
[ "$owner_type" = Organization ] && ok "$repo is owned by an Organization" || {
	if [ "$QUEUE" = 1 ]; then
		bad "the merge queue needs an Organization-owned repository, and $repo is owned by a User:"
		echo "      1. convert the $owner account to an organization, or"
		echo "      2. transfer $repo to an organization."
		echo "      The queue-less mode needs neither: run without QUEUE=1."
	else
		ok "$repo is owned by a User; the queue-less mode needs no organization"
	fi
}
[ "$merge_commit" = true ] && ok "merge commits are allowed" || bad "allow_merge_commit is false: both modes land a merge commit whose first parent is main's old tip"

git fetch origin main --quiet 2>/dev/null || bad "git fetch origin main failed"
origin_tree="$(git rev-parse origin/main^{tree} 2>/dev/null)"
local_tree="$(git rev-parse main^{tree} 2>/dev/null)"
if [ -n "$origin_tree" ] && [ "$origin_tree" = "$local_tree" ]; then
	ok "origin/main is the merged tree"
else
	bad "origin/main (${origin_tree:-none}) is not local main (${local_tree:-none}): push main before enabling the gate"
fi
if git cat-file -e origin/main:.github/workflows/proof.yml 2>/dev/null; then
	ok "origin/main holds .github/workflows/proof.yml"
else
	bad "origin/main has no .github/workflows/proof.yml"
fi
if git cat-file -e origin/main:.github/CODEOWNERS 2>/dev/null; then
	ok "origin/main holds .github/CODEOWNERS"
elif [ "$CODEOWNER_REVIEW" = 1 ]; then
	bad "origin/main has no .github/CODEOWNERS, and code-owner review is on (CODEOWNER_REVIEW=0 leaves it out)"
else
	warn "origin/main has no .github/CODEOWNERS; code-owner review is off"
fi

if go test ./test/tools/proofrun -run '^TestWorkflows_TheProofCheckRunsTheRealProofOnPullRequests$' -count=1 >/dev/null 2>&1; then
	ok "the proof workflow runs the real proof on pull requests"
else
	bad "TestWorkflows_TheProofCheckRunsTheRealProofOnPullRequests failed: the workflow no longer proves what it must"
fi

actions_enabled="$(gh api "$api/actions/permissions" --jq '.enabled // false' 2>/dev/null)"
[ "$actions_enabled" = true ] && ok "Actions are enabled" || bad "Actions are disabled"

nightly="$(gh api "$api/actions/workflows/nightly.yml" 2>/dev/null | jq -r '.state // ""' 2>/dev/null)"
if [ "$nightly" = active ]; then
	ok "nightly.yml is active"
else
	warn "nightly.yml is ${nightly:-missing}; GitHub disables a schedule after 60 days without activity: gh workflow enable nightly.yml"
fi

check_proof="$(gh api "$api/commits/origin/main/check-runs" 2>/dev/null | jq '[.check_runs[]? | select(.name=="proof" and .app.id==15368 and .conclusion=="success")] | length' 2>/dev/null || echo 0)"
if [ "${check_proof:-0}" -gt 0 ] 2>/dev/null; then
	ok "the GitHub Actions app has reported a successful proof check"
else
	warn "no successful proof check from app 15368 yet; the first run creates it"
fi

rulesets="$(gh api "$api/rulesets" 2>/dev/null || echo '[]')"
mode_var="$(gh api "$api/actions/variables/MERGE_GATE_MODE" 2>/dev/null | jq -r '.value // ""' 2>/dev/null || true)"
if [ "$QUEUE" = 1 ]; then
	if [ "${mode_var:-}" = queue ]; then
		ok "MERGE_GATE_MODE=queue agrees with the queue mode"
	else
		bad "MERGE_GATE_MODE is ${mode_var:-unset}; the queue mode sets it to queue"
	fi
else
	if [ -z "${mode_var:-}" ]; then
		ok "MERGE_GATE_MODE is unset: queue-less"
	else
		bad "MERGE_GATE_MODE=${mode_var}, and this invocation asks for the queue-less mode"
	fi
fi

if [ "$fail" -ne 0 ]; then
	echo
	echo "github-merge-gate: NOT ready to apply. The gate is not enforced."
	[ "$MODE" = apply ] && exit 1
	exit 1
fi

if [ "$MODE" = check ]; then
	echo
	echo "github-merge-gate: every precondition holds. APPLY=1 applies the queue-less mode."
	echo "The gate is not enforced yet: APPLY writes the rulesets."
	exit 0
fi

echo
echo "UNVERIFIED against the API: this body has never been applied."
echo "--- scripts/github/ruleset-main.json"
cat scripts/github/ruleset-main.json

body="scripts/github/ruleset-main.json"
if [ "$QUEUE" = 1 ]; then
	echo "UNVERIFIED against the API: this body has never been applied."
	echo "--- scripts/github/ruleset-queue.json"
	cat scripts/github/ruleset-queue.json
	merged="$(mktemp)"
	jq -s '.[0].rules += .[1].rules |
	       .[0].rules = (.[0].rules | map(if .type == "required_status_checks" then .parameters.strict_required_status_checks_policy = false else . end))' \
		scripts/github/ruleset-main.json scripts/github/ruleset-queue.json >"$merged"
	body="$merged"
fi

apply_ruleset() { # <name> <body>
	local name="$1" body="$2" id
	id="$(gh api "$api/rulesets" --jq ".[] | select(.name == \"$name\") | .id" 2>/dev/null | head -n1)"
	if [ -n "$id" ]; then
		gh api --method PUT "$api/rulesets/$id" --input "$body" >/dev/null || return 1
		ok "updated ruleset $name"
	else
		gh api --method POST "$api/rulesets" --input "$body" >/dev/null || return 1
		ok "created ruleset $name"
	fi
}

apply_ruleset main-proof-gate "$body" || { bad "applying main-proof-gate failed; read the API's answer above"; exit 1; }

if [ "$CODEOWNER_REVIEW" = 1 ]; then
	echo "UNVERIFIED against the API: this body has never been applied."
	echo "--- scripts/github/ruleset-owners.json"
	cat scripts/github/ruleset-owners.json
	apply_ruleset gate-files-owner-review scripts/github/ruleset-owners.json || { bad "applying gate-files-owner-review failed"; exit 1; }
fi

if [ "$QUEUE" = 1 ]; then
	gh api --method POST "$api/actions/variables" \
		-f name=MERGE_GATE_MODE -f value=queue >/dev/null 2>&1 ||
		gh api --method PATCH "$api/actions/variables/MERGE_GATE_MODE" -f value=queue >/dev/null ||
		{ bad "setting MERGE_GATE_MODE failed"; exit 1; }
	ok "MERGE_GATE_MODE=queue"
else
	gh api --method DELETE "$api/actions/variables/MERGE_GATE_MODE" >/dev/null 2>&1 || true
	ok "MERGE_GATE_MODE unset: queue-less"
fi

echo
echo "github-merge-gate: applied. Run --verify-landed SHA once after the first merge through the gate."
