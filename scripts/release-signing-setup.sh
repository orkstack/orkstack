#!/usr/bin/env bash
# scripts/release-signing-setup.sh: set up the key release.yml signs with, once,
# on a maintainer's own machine (docs/PROOFS.md §10, "Set up release signing").
#
#   1. checks that cosign, gh and git are on PATH, that gh is signed in and may
#      administer the repository, and that --dir lies outside every git work
#      tree, so the private key can never be committed; then, before it writes
#      anything, refuses a key file it would overwrite, an environment that
#      already holds COSIGN_PRIVATE_KEY or a committed deploy/cosign.pub
#      (unless --rotate), and an environment that admits a branch besides main;
#   2. asks for the key's password, silently and twice, and generates the pair
#      in --dir with cosign generate-key-pair;
#   3. proves the pair: signs a file with the private key and verifies it with
#      the public one, with the flags release.yml signs and verifies with;
#   4. creates or updates the environment: deployments from main only, and the
#      caller a required reviewer unless --no-reviewer;
#   5. sets the environment's COSIGN_PRIVATE_KEY (the key file) and
#      COSIGN_PASSWORD;
#   6. copies the public key to deploy/cosign.pub in this checkout, and prints
#      what is left to do.
#
# The password reaches cosign through COSIGN_PASSWORD in its environment and gh
# on stdin: never an argument, never echoed, never a file. Every step is safe
# to repeat: the environment is updated in place, and its branch policy is
# added only when it is missing. --dry-run prints every action and runs neither
# gh nor cosign. Exit 0 done, 1 refused or failed, 2 an invalid invocation, 77
# a missing prerequisite.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
	cat >&2 <<'EOF'
usage: scripts/release-signing-setup.sh [--repo OWNER/NAME] [--env NAME] [--dir DIR]
                                        [--rotate] [--no-reviewer] [--dry-run]
  --repo OWNER/NAME  the repository (default orkstack/orkstack)
  --env NAME         the environment release.yml's publish runs in (default release)
  --dir DIR          where the key pair is written (default ~/orkstack-release-key);
                     never inside a git work tree
  --rotate           replace a key the environment or deploy/cosign.pub already holds
  --no-reviewer      do not make you a required reviewer of the environment
  --dry-run          print every action; run neither gh nor cosign
EOF
	exit 2
}
REPO=orkstack/orkstack
ENVIRONMENT=release
DIR="${HOME:?}/orkstack-release-key"
ROTATE=0
REVIEWER=1
DRY=0
while [ $# -gt 0 ]; do
	case "$1" in
	--repo) [ $# -ge 2 ] || usage; REPO="$2"; shift 2 ;;
	--env) [ $# -ge 2 ] || usage; ENVIRONMENT="$2"; shift 2 ;;
	--dir) [ $# -ge 2 ] || usage; DIR="$2"; shift 2 ;;
	--rotate) ROTATE=1; shift ;;
	--no-reviewer) REVIEWER=0; shift ;;
	--dry-run) DRY=1; shift ;;
	*) usage ;;
	esac
done
[[ "$REPO" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || { echo "release-signing-setup: --repo $REPO is not OWNER/NAME" >&2; exit 2; }
[[ "$ENVIRONMENT" =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "release-signing-setup: --env $ENVIRONMENT is not an environment name" >&2; exit 2; }
[ -n "$DIR" ] || usage
case "$DIR" in /*) ;; *) DIR="$PWD/$DIR" ;; esac

need() {
	printf 'release-signing-setup: %s\n' "$*" >&2
	exit 77
}
refuse() {
	printf 'release-signing-setup: refused: %s\n' "$*" >&2
	exit 1
}
say() { printf '%s\n' "$*"; }
# step prints one action; in a dry run it returns 1, so the action never runs.
step() {
	if [ "$DRY" = 1 ]; then
		printf '  would %s\n' "$*"
		return 1
	fi
	printf '  %s\n' "$*"
}

ROTATION="a new key replaces the release key: commit the new deploy/cosign.pub in the same pull request, and every deployment that trusts OrkStack's releases adds it to its image policy (orkctl admin image-policy set --signing-key NAME=cosign.pub) and moves each namespace's --require-signed-by to it before it runs a release signed with it; releases already published stay signed by the old key"

for tool in cosign gh git; do
	command -v "$tool" >/dev/null || need "$tool is not on PATH"
done
[ -d "$ROOT/deploy" ] || need "$ROOT/deploy does not exist: run the script from an OrkStack checkout"
KEY="$DIR/cosign.key"
PUB="$DIR/cosign.pub"
COMMITTED="$ROOT/deploy/cosign.pub"

umask 077
password=""
work="$(mktemp -d)"
cleanup() {
	unset password
	rm -rf -- "$work"
}
trap cleanup EXIT

if [ "$DRY" = 1 ]; then
	say "== release signing for $REPO, environment $ENVIRONMENT (dry run: nothing runs)"
else
	say "== release signing for $REPO, environment $ENVIRONMENT"
fi

# The private key can never reach a commit: --dir is outside every work tree.
probe="$DIR"
while [ ! -d "$probe" ]; do probe="$(dirname "$probe")"; done
if [ "$(git -C "$probe" rev-parse --is-inside-work-tree 2>/dev/null || true)" = true ]; then
	refuse "--dir $DIR is inside the git work tree $(git -C "$probe" rev-parse --show-toplevel); keep the private key outside every repository"
fi
if [ -e "$COMMITTED" ] && [ "$ROTATE" = 0 ]; then
	refuse "deploy/cosign.pub is already committed; pass --rotate to replace it: $ROTATION"
fi
for f in "$KEY" "$PUB"; do
	[ ! -e "$f" ] || refuse "$f exists, and a key file is never overwritten: move it away or pick another --dir"
done

login=""
user_id=""
reviewers=""
policies=""
if [ "$DRY" = 1 ]; then
	say "  would check that gh is signed in and may administer $REPO, and read environment $ENVIRONMENT's secrets and branch policies"
else
	gh auth status >/dev/null 2>&1 || need "gh is not signed in: run gh auth login"
	[ "$(gh api "repos/$REPO" --jq .permissions.admin)" = true ] ||
		refuse "you cannot administer $REPO; an environment and its secrets need an administrator"
	me="$(gh api user --jq '.login + " " + (.id | tostring)')"
	read -r login user_id <<<"$me"
	[ -n "$login" ] && [[ "$user_id" =~ ^[0-9]+$ ]] || refuse "gh api user answered $me"
	# Only a 404 means the environment does not exist yet; any other error is
	# not read as absent.
	if reviewers="$(gh api "repos/$REPO/environments/$ENVIRONMENT" \
		--jq '.protection_rules[]? | select(.type == "required_reviewers") | .reviewers[]? | .type + " " + (.reviewer.id | tostring)' 2>"$work/err")"; then
		secrets="$(gh secret list --env "$ENVIRONMENT" --repo "$REPO" --json name --jq '.[].name')"
		if grep -qx COSIGN_PRIVATE_KEY <<<"$secrets" && [ "$ROTATE" = 0 ]; then
			refuse "environment $ENVIRONMENT already holds COSIGN_PRIVATE_KEY; pass --rotate to replace it: $ROTATION"
		fi
		if ! policies="$(gh api "repos/$REPO/environments/$ENVIRONMENT/deployment-branch-policies" --jq '.branch_policies[]? | .name' 2>"$work/err")"; then
			grep -q 'HTTP 404' "$work/err" || refuse "reading environment $ENVIRONMENT's branch policies: $(cat "$work/err")"
			policies=""
		fi
		others="$(grep -vx -e main -e '' <<<"$policies" || true)"
		[ -z "$others" ] || refuse "environment $ENVIRONMENT also admits deployments from: $(tr '\n' ' ' <<<"$others")— publish would hold the key for them; remove them in the environment's settings"
	else
		grep -q 'HTTP 404' "$work/err" || refuse "reading environment $ENVIRONMENT of $REPO: $(cat "$work/err")"
		reviewers=""
	fi

	printf 'password for the release key: ' >&2
	IFS= read -r -s password || true
	printf '\nagain: ' >&2
	IFS= read -r -s again || true
	printf '\n' >&2
	[ -n "$password" ] || refuse "an empty password: publish refuses a key without one"
	if [ "$password" != "$again" ]; then
		unset again
		refuse "the two passwords differ"
	fi
	unset again
fi

if step "generate the key pair in $DIR (cosign generate-key-pair)"; then
	[ -d "$DIR" ] || mkdir -p -m 700 "$DIR"
	COSIGN_PASSWORD="$password" cosign generate-key-pair --output-key-prefix "$DIR/cosign" >"$work/cosign.log" 2>&1 ||
		{ cat "$work/cosign.log" >&2; refuse "cosign generate-key-pair failed"; }
	[ -s "$KEY" ] && [ -s "$PUB" ] || refuse "cosign generate-key-pair wrote no $KEY and $PUB"
fi

if step "prove the pair: sign a file with $KEY and verify it with $PUB, with release.yml's flags"; then
	printf 'release signing setup for %s\n' "$REPO" >"$work/proof"
	COSIGN_PASSWORD="$password" cosign sign-blob --key "$KEY" --new-bundle-format=false --use-signing-config=false --tlog-upload=false --yes \
		--output-signature "$work/proof.sig" "$work/proof" >"$work/cosign.log" 2>&1 ||
		{ cat "$work/cosign.log" >&2; refuse "cosign sign-blob could not sign with the new key (cosign 2.6 or later is needed)"; }
	cosign verify-blob --key "$PUB" --insecure-ignore-tlog=true --signature "$work/proof.sig" "$work/proof" >"$work/cosign.log" 2>&1 ||
		{ cat "$work/cosign.log" >&2; refuse "cosign verify-blob does not accept the new pair's signature"; }
fi

# The caller joins any reviewers the environment already has.
body='{"deployment_branch_policy":{"protected_branches":false,"custom_branch_policies":true}'
reviewing="deployments from main only"
if [ "$REVIEWER" = 1 ]; then
	list=""
	while read -r kind id; do
		[ -n "$kind" ] || continue
		list="${list:+$list,}{\"type\":\"$kind\",\"id\":$id}"
	done < <(printf '%s\n%s\n' "$reviewers" "${user_id:+User $user_id}" | grep -E '^(User|Team) [0-9]+$' | sort -u)
	body="$body,\"reviewers\":[$list]"
	reviewing="$reviewing, ${login:-you} a required reviewer"
fi
body="$body}"
if step "create or update environment $ENVIRONMENT: $reviewing"; then
	gh api -X PUT "repos/$REPO/environments/$ENVIRONMENT" --input - <<<"$body" >/dev/null
fi
if ! grep -qx main <<<"$policies"; then
	if step "allow deployments to $ENVIRONMENT from main"; then
		gh api -X POST "repos/$REPO/environments/$ENVIRONMENT/deployment-branch-policies" -f name=main -f type=branch >/dev/null
	fi
fi

if step "set $ENVIRONMENT's COSIGN_PRIVATE_KEY from $KEY"; then
	gh secret set COSIGN_PRIVATE_KEY --env "$ENVIRONMENT" --repo "$REPO" <"$KEY" >/dev/null
fi
if step "set $ENVIRONMENT's COSIGN_PASSWORD (on stdin)"; then
	printf '%s' "$password" | gh secret set COSIGN_PASSWORD --env "$ENVIRONMENT" --repo "$REPO" >/dev/null
fi
unset password

if step "copy $PUB to deploy/cosign.pub"; then
	cp "$PUB" "$COMMITTED"
	chmod 644 "$COMMITTED"
fi

say "== next"
say "  1. Commit deploy/cosign.pub through a pull request: validate refuses every release until main carries it."
say "  2. Back up $KEY and its password offline, apart from each other; then delete $KEY from this machine."
if [ "$ROTATE" = 1 ]; then
	say "  3. Until that pull request lands, a release stops before it publishes anything. Then tell deployments: $ROTATION."
fi
