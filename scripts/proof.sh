#!/usr/bin/env bash
# Produce the evidence for a change: every gate, run against the real stack,
# with its full output kept under one fresh, gitignored directory.
#
#   scripts/proof.sh [name]     -> proof/<name>/   (default: <branch>-<UTC stamp>)
#   PROOF_GATES=unit,integration,ui scripts/proof.sh
#
# Exit status: 0 every gate passed, 1 a gate failed, 77 a prerequisite is
# absent. Prerequisites are probed BEFORE any output file is opened, so a run
# that cannot measure anything leaves nothing behind that looks like evidence.
#
# proof/ is gitignored on purpose: the evidence is regenerated per change and
# summarised in the commit or pull request; what the repository keeps is the
# reproduction — the tests, specs and this script.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
REPO="$PWD"

GATES="${PROOF_GATES:-unit,integration}"
NAME="${1:-$(git branch --show-current 2>/dev/null | tr '/' '-')-$(date -u +%Y%m%dT%H%M%SZ)}"
DIR="$REPO/proof/$NAME"
REGISTRY="${ORK_LOCAL_REGISTRY:-localhost:5001}"

say() { printf '%s\n' "$*"; }
has_gate() { case ",$GATES," in *",$1,"*) return 0 ;; *) return 1 ;; esac; }
tcp_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# ---- prerequisites, decided before a single file is written ----------------
missing=""
need() { # <what> <command...>
	if ! "${@:2}" >/dev/null 2>&1; then
		missing="${missing}
  - $1"
	fi
}
need "go on PATH" command -v go
if has_gate integration || has_gate ui; then
	need "a docker daemon (docker info)" docker info
	need "Temporal on 127.0.0.1:7233 (make infra-up)" tcp_open 7233
	need "MinIO on 127.0.0.1:9000 (make infra-up)" \
		curl -sf -o /dev/null http://127.0.0.1:9000/minio/health/live
	need "the local registry on $REGISTRY (make infra-up)" \
		curl -sf -o /dev/null "http://$REGISTRY/v2/"
	for activity in shell http file; do
		need "activity-$activity pushed to $REGISTRY (make -C images push)" \
			curl -sf -o /dev/null -H 'Accept: application/vnd.oci.image.index.v1+json' \
			"http://$REGISTRY/v2/orkstack/activity-$activity/manifests/dev"
	done
fi
if has_gate ui; then
	need "bun on PATH" command -v bun
	need "ui/node_modules (cd ui && bun install)" test -d "$REPO/ui/node_modules"
	need "the temporal CLI on PATH (ui/e2e/admin.spec.ts induces a real cluster failure with it)" command -v temporal
fi
if [ -n "$missing" ]; then
	say "proof: prerequisites absent, nothing measured, nothing written:$missing"
	exit 77
fi

# ---- the evidence directory -------------------------------------------------
mkdir -p "$DIR/artifacts"
README="$DIR/README.md"
{
	echo "# Proof: $NAME"
	echo
	echo "- commit: $(git rev-parse HEAD 2>/dev/null || echo unknown)$(git diff --quiet 2>/dev/null || echo ' (dirty)')"
	echo "- branch: $(git branch --show-current 2>/dev/null || echo unknown)"
	echo "- date:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "- host:   $(hostname) ($(uname -sm))"
	echo "- go:     $(go version | cut -d' ' -f3)"
	echo "- gates:  $GATES"
	echo
	echo "## Working tree"
	echo
	echo '```'
	git status --short 2>/dev/null || true
	echo '```'
	echo
	echo "## Gates"
	echo
} >"$README"
git diff --stat >"$DIR/diff.stat" 2>/dev/null || true

status=0
run() { # <gate> <log> <command...>
	local gate="$1" logname="$2" logfile="$DIR/$2"
	shift 2
	say "== $gate: $*"
	local started=$SECONDS
	if "$@" >"$logfile" 2>&1; then
		say "   pass ($((SECONDS - started))s) -> $logfile"
		echo "- **$gate**: pass in $((SECONDS - started))s — \`$*\` → \`$logname\`" >>"$README"
	else
		local code=$?
		status=1
		say "   FAIL exit $code ($((SECONDS - started))s) -> $logfile"
		echo "- **$gate**: FAIL (exit $code) in $((SECONDS - started))s — \`$*\` → \`$logname\`" >>"$README"
		tail -n 40 "$logfile" | sed 's/^/      /'
	fi
}

export ORK_PROOF_DIR="$DIR/artifacts"

if has_gate unit; then
	run lint lint.log make lint
	run build build.log make build
	run unit unit.log go test ./... -count=1 -v
	run vet-integration vet-integration.log go vet -tags=integration ./...
fi
if has_gate integration; then
	# A skip is not a pass: ORK_REQUIRE_INFRA turns every prerequisite skip
	# into a failure now that the prerequisites were probed above.
	run integration integration.log env ORK_REQUIRE_INFRA=1 \
		go test -tags=integration -p 1 -timeout 60m -count=1 ./... -v
	run activity-fixture activity-fixture.log make -C images test
fi
in_ui() { (cd "$REPO/ui" && "$@"); }
if has_gate ui; then
	# Screenshots and transcripts land under artifacts/ui/<spec>/.
	run ui-check ui-check.log in_ui bun run check
	run ui-unit ui-unit.log in_ui bun test test/
	run ui-e2e ui-e2e.log in_ui env ORK_PROOF_DIR="$DIR/artifacts/ui" bun run test
fi

{
	echo
	echo "## Artifacts"
	echo
	echo '```'
	(cd "$DIR" && find . -type f | sort)
	echo '```'
	echo
	if [ "$status" -eq 0 ]; then echo "Result: **PASS**"; else echo "Result: **FAIL**"; fi
} >>"$README"

say
say "proof: $([ "$status" -eq 0 ] && echo PASS || echo FAIL) -> $DIR/README.md"
exit "$status"
