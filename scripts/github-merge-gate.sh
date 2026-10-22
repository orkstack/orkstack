#!/usr/bin/env bash
# github-merge-gate.sh — enable, or check, the GitHub side of the merge gate.
#
#   make github-merge-gate                 check only (default)
#   make github-merge-gate APPLY=1         apply the queue-less mode
#   make github-merge-gate APPLY=1 QUEUE=1 also apply the merge queue
#                                          (APPLY=1 alone switches back)
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
	-h | --help) sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done
[ "$APPLY" = 1 ] && MODE=apply

fail=0
bad() { printf 'FAIL: %s\n' "$*"; fail=1; }
ok() { printf 'ok:   %s\n' "$*"; }
warn() { printf 'warn: %s\n' "$*"; }

# gh_read <args…>: gh's answer in GH_OUT. Status 0 is an answer, 44 is a 404
# (the resource is absent), anything else is an error that is never to be read
# as "absent": a caller fails closed on it.
GH_OUT=""
gh_read() {
	local errf rc
	errf="$(mktemp)"
	GH_OUT="$(gh "$@" 2>"$errf")"
	rc=$?
	if [ "$rc" -ne 0 ] && grep -q 'HTTP 404' "$errf"; then rc=44; fi
	rm -f "$errf"
	return "$rc"
}

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
	# Pass the commit that landed on main. The queue-less mode lands a
	# GitHub-made merge commit that carries no check run (the run is on the pull
	# request's head, its second parent), so the head is read when the landed
	# commit has none; the merge commit's tree equals the head's merged tree.
	sha="$(git rev-parse "$VERIFY_LANDED")"
	tree="$(git rev-parse "$sha^{tree}")"
	tmp="$(mktemp -d)"
	trap 'rm -rf "$tmp"' EXIT
	proof_run_of() { # <sha> -> the run id in RUN_ID; status 1 on an API error
		gh_read api "$api/commits/$1/check-runs" --jq '[.check_runs[] | select(.name=="proof" and .app.id==15368)][0].details_url // ""' || return 1
		RUN_ID="$(printf '%s\n' "$GH_OUT" | sed -E 's#.*/runs/([0-9]+).*#\1#')"
		case "$RUN_ID" in *[!0-9]*) RUN_ID="" ;; esac
		return 0
	}
	RUN_ID=""
	proof_run_of "$sha" || { bad "cannot read the check runs of $sha"; exit 1; }
	run_id="$RUN_ID"
	from="$sha"
	if [ -z "$run_id" ]; then
		if head_sha="$(git rev-parse --verify --quiet "$sha^2^{commit}")" && [ -n "$head_sha" ]; then
			proof_run_of "$head_sha" || { bad "cannot read the check runs of $head_sha"; exit 1; }
			run_id="$RUN_ID"
			from="$head_sha"
		fi
	fi
	[ -n "$run_id" ] || { bad "no proof check run on $sha, nor on its pull request head (its second parent)"; exit 1; }
	# A pattern download puts each artifact in a directory of its own name;
	# the run's last attempt is the proof that decided the check.
	gh run download "$run_id" -R "$repo" -p "proof-$run_id-a*" -D "$tmp" 2>/dev/null || true
	proof_dir="$(find "$tmp" -mindepth 1 -maxdepth 1 -type d -name "proof-$run_id-a*" 2>/dev/null | sort -V | tail -n1)"
	if [ -z "$proof_dir" ] && [ -f "$tmp/proof.json" ]; then
		proof_dir="$tmp"
	fi
	[ -n "$proof_dir" ] && [ -f "$proof_dir/proof.json" ] || { bad "proof run $run_id left no proof-$run_id-a* artifact with a proof.json"; exit 1; }
	got="$(jq -r .tree "$proof_dir/proof.json" 2>/dev/null)"
	if [ "$got" = "$tree" ]; then
		[ "$from" = "$sha" ] || ok "$sha carries no proof check run; read the pull request head $from"
		ok "proof run $run_id ($(basename "$proof_dir")) measured tree $tree, which is $sha's tree"
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
# Precondition 10: CODEOWNERS exists and names an account that can approve,
# that is, one with write access. Otherwise the owner-review ruleset blocks
# every pull request that touches a gate file.
owners_fail() { if [ "$CODEOWNER_REVIEW" = 1 ]; then bad "$* (CODEOWNER_REVIEW=0 leaves code-owner review out)"; else warn "$*; code-owner review is off"; fi; }
if git cat-file -e origin/main:.github/CODEOWNERS 2>/dev/null; then
	ok "origin/main holds .github/CODEOWNERS"
	writers=""
	for owner in $(git show origin/main:.github/CODEOWNERS 2>/dev/null | sed -e 's/#.*//' | tr -s ' \t' '\n' | grep '^@' | sort -u); do
		who="${owner#@}"
		case "$who" in
		*/*)
			# A team: its permission on this repository.
			push="$(gh api -H 'Accept: application/vnd.github.v3.repository+json' "orgs/${who%%/*}/teams/${who#*/}/repos/$repo" --jq '.permissions.push // false' 2>/dev/null || true)"
			[ "$push" = true ] && writers="$writers $owner"
			;;
		*)
			perm="$(gh api "$api/collaborators/$who/permission" --jq '.permission // ""' 2>/dev/null || true)"
			case "$perm" in admin | maintain | write) writers="$writers $owner" ;; esac
			;;
		esac
	done
	if [ -n "$writers" ]; then
		ok "CODEOWNERS names an account with write access:$writers"
	else
		owners_fail "CODEOWNERS at origin/main names no account with write access to $repo"
	fi
else
	owners_fail "origin/main has no .github/CODEOWNERS"
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

# Precondition 6: the GitHub Actions app has reported a successful `proof`
# check on some commit: main's tip, or the head of the newest successful run
# of proof.yml (a pull request's head, or a validate/full push). A required
# check with an integration_id needs that report first. GitHub's commits
# endpoint takes a SHA or a branch, never a remote-tracking name.
proof_success() { # <sha>
	gh api "$api/commits/$1/check-runs?check_name=proof" 2>/dev/null |
		jq '[.check_runs[]? | select(.name=="proof" and .app.id==15368 and .conclusion=="success")] | length' 2>/dev/null || echo 0
}
main_sha="$(git rev-parse origin/main 2>/dev/null)"
check_proof=0
[ -n "$main_sha" ] && check_proof="$(proof_success "$main_sha")"
if ! [ "${check_proof:-0}" -gt 0 ] 2>/dev/null; then
	run_sha="$(gh api "$api/actions/workflows/proof.yml/runs?status=success&per_page=1" --jq '.workflow_runs[0].head_sha // ""' 2>/dev/null || true)"
	[ -n "$run_sha" ] && check_proof="$(proof_success "$run_sha")"
fi
if [ "${check_proof:-0}" -gt 0 ] 2>/dev/null; then
	ok "the GitHub Actions app (15368) has reported a successful proof check"
else
	bad "no successful proof check from the GitHub Actions app (15368) yet: run proof.yml once (a validate/full/** push or a pull request with ci:proof) before requiring it"
fi

# A 404 is "absent"; any other error is not, and fails closed.
rulesets='[]'
if gh_read api "$api/rulesets"; then
	rulesets="$GH_OUT"
elif [ $? -ne 44 ]; then
	bad "cannot read the rulesets of $repo: $GH_OUT"
fi
mode_var=""
mode_read=ok
if gh_read api "$api/actions/variables/MERGE_GATE_MODE"; then
	mode_var="$(jq -r '.value // ""' <<<"$GH_OUT" 2>/dev/null)"
elif [ $? -ne 44 ]; then
	mode_read=failed
	bad "cannot read the MERGE_GATE_MODE variable: a read that failed is not 'unset'"
fi
# The variable must agree with what is applied now — the existing
# main-proof-gate ruleset's merge_queue rule — never with the requested mode:
# the apply below changes both, in the order that is never looser than either
# mode (docs/PROOFS.md §10, "Enable the merge gate").
main_id="$(jq -r '[.[]? | select(.name == "main-proof-gate") | .id][0] // ""' <<<"$rulesets" 2>/dev/null || true)"
applied_queue=false
if [ -n "$main_id" ]; then
	if gh_read api "$api/rulesets/$main_id"; then
		jq -e '[.rules[]? | select(.type == "merge_queue")] | length > 0' >/dev/null 2>&1 <<<"$GH_OUT" && applied_queue=true
	else
		bad "cannot read ruleset $main_id (main-proof-gate): $GH_OUT"
	fi
fi
if [ "$mode_read" = failed ]; then
	: # already reported; no comparison can be trusted
elif [ "$applied_queue" = true ] && [ "${mode_var:-}" = queue ]; then
	ok "MERGE_GATE_MODE=queue agrees with main-proof-gate's merge_queue rule"
elif [ "$applied_queue" = false ] && [ "${mode_var:-}" != queue ]; then
	ok "MERGE_GATE_MODE is ${mode_var:-unset}, and no merge_queue rule is applied: queue-less"
elif [ "$applied_queue" = true ] && [ "$MODE" = apply ] && [ "$QUEUE" = 1 ]; then
	# A half-applied queue (the rule landed, the variable did not): this very
	# apply finishes it, rule first and variable last.
	warn "main-proof-gate has a merge_queue rule and MERGE_GATE_MODE is ${mode_var:-unset}: a half-applied queue; this apply sets the variable"
elif [ "$applied_queue" = true ]; then
	bad "main-proof-gate has a merge_queue rule, and MERGE_GATE_MODE is ${mode_var:-unset}: pull requests would refuse what the queue lands. Fix: gh variable set MERGE_GATE_MODE --body queue, or APPLY=1 QUEUE=1"
else
	bad "MERGE_GATE_MODE=${mode_var}, and main-proof-gate has no merge_queue rule: pull requests would skip the containment check with no queue to land them. Fix: gh variable delete MERGE_GATE_MODE"
fi
want_queue=false
[ "$QUEUE" = 1 ] && want_queue=true
if [ "$want_queue" != "$applied_queue" ]; then
	echo "      this invocation asks for the $([ "$want_queue" = true ] && echo queue || echo queue-less) mode; APPLY=1 switches to it"
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
	# A failed lookup must not read as "no such ruleset": that would POST a duplicate.
	gh_read api "$api/rulesets" --jq "[.[] | select(.name == \"$name\") | .id][0] // empty" || { bad "cannot list the rulesets to find $name: $GH_OUT"; return 1; }
	id="$GH_OUT"
	if [ -n "$id" ]; then
		gh api --method PUT "$api/rulesets/$id" --input "$body" >/dev/null || return 1
		ok "updated ruleset $name"
	else
		gh api --method POST "$api/rulesets" --input "$body" >/dev/null || return 1
		ok "created ruleset $name"
	fi
}

# Never looser than either mode in between: leaving the queue, the variable
# goes first (pull requests check containment again while the queue still
# lands); entering it, the variable goes last (the queue lands before pull
# requests stop checking).
unset_mode_var() {
	if [ -n "${mode_var:-}" ]; then
		gh api --method DELETE "$api/actions/variables/MERGE_GATE_MODE" >/dev/null || { bad "deleting MERGE_GATE_MODE failed"; exit 1; }
	fi
	ok "MERGE_GATE_MODE unset: queue-less"
}
set_mode_var() {
	if [ -z "${mode_var:-}" ]; then
		gh api --method POST "$api/actions/variables" -f name=MERGE_GATE_MODE -f value=queue >/dev/null ||
			{ bad "setting MERGE_GATE_MODE failed"; exit 1; }
	elif [ "$mode_var" != queue ]; then
		gh api --method PATCH "$api/actions/variables/MERGE_GATE_MODE" -f name=MERGE_GATE_MODE -f value=queue >/dev/null ||
			{ bad "setting MERGE_GATE_MODE failed"; exit 1; }
	fi
	ok "MERGE_GATE_MODE=queue"
}

[ "$QUEUE" = 1 ] || unset_mode_var

apply_ruleset main-proof-gate "$body" || { bad "applying main-proof-gate failed; read the API's answer above"; exit 1; }

if [ "$CODEOWNER_REVIEW" = 1 ]; then
	echo "UNVERIFIED against the API: this body has never been applied."
	echo "--- scripts/github/ruleset-owners.json"
	cat scripts/github/ruleset-owners.json
	apply_ruleset gate-files-owner-review scripts/github/ruleset-owners.json || { bad "applying gate-files-owner-review failed"; exit 1; }
fi

[ "$QUEUE" = 1 ] && set_mode_var

echo
echo "github-merge-gate: applied. Run --verify-landed SHA once after the first merge through the gate."
