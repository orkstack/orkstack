package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// A local proof with more than one Playwright worker starts one browser server
// for its gate through a short-lived `bun e2e/browser-server.ts start`, which
// labelled the container with its OWN pid as the owner and exited. The next
// proof's sweep then read the owner as gone and stopped the browser server of
// a proof still running: on 2026-10-30 at 20:54 local, proof
// upgrade-ui-site-3-feature's start (its scripts/proof.sh sweep, temporal.log:
// "stopped stale funny_feistel (its proof, pid 114342, is gone)") stopped
// container 44afe947702a, the browser server of proof
// api-hardening-feature, 89 specs into its gate; 94 specs failed after it.
//
// The recorded records: that proof's proof.pid ("3902900 3902900 18609085
// pr-048ac8024ee0"), its Temporal's labels, and the browser server's
// (orkstack.proof=pr-048ac8024ee0, orkstack.e2e=browser-server, owner the
// starter's pid 114342, which had exited).

const (
	incidentHost  = "sauroposeidon"
	incidentProof = "pr-048ac8024ee0"
)

func incidentContainers() []proofContainer {
	return []proofContainer{
		{"funny_feistel", incidentProof, incidentHost + ":114342:18612240"},
		{"orkstack-proof-" + incidentProof + "-temporal", incidentProof, incidentHost + ":3902900:18609085"},
	}
}

// incidentCleaner is the sweep of another proof starting on that host while
// the proof ran (alive) or after it had gone.
func incidentCleaner(t *testing.T, proofAlive bool, docker *fakeCleanDocker, out *strings.Builder) *cleaner {
	t.Helper()
	wt := t.TempDir()
	write(t, filepath.Join(wt, "proof", "api-hardening-feature", "proof.pid"), "3902900 3902900 18609085 "+incidentProof+"\n")
	// The sweeping proof's own record: it is running too.
	write(t, filepath.Join(wt, "proof", "upgrade-ui-site-3-feature", "proof.pid"), "4171848 4171848 18737132 pr-c9517f608fda\n")
	starts := map[int]string{4171848: "18737132"}
	if proofAlive {
		starts[3902900] = "18609085"
	}
	return &cleaner{
		worktrees: []string{wt},
		host:      incidentHost,
		alive:     func(pid int) bool { return starts[pid] != "" },
		procStart: func(pid int) string { return starts[pid] },
		docker:    docker,
		remove:    true,
		out:       out,
		removeAll: os.RemoveAll,
	}
}

func TestClean_ALiveProofsBrowserServerIsNeverSwept(t *testing.T) {
	var out strings.Builder
	docker := &fakeCleanDocker{temporals: incidentContainers()}
	incidentCleaner(t, true, docker, &out).run(context.Background())
	if len(docker.stopped) != 0 {
		t.Fatalf("the sweep stopped %q, of a proof whose proof.pid says it still runs:\n%s", docker.stopped, out.String())
	}
	if !strings.Contains(out.String(), "funny_feistel: in use: proof "+incidentProof+" is still running") {
		t.Errorf("the sweep does not say why it left the browser server:\n%s", out.String())
	}

	// Once the proof is gone, both of its containers are its leftovers.
	out.Reset()
	docker = &fakeCleanDocker{temporals: incidentContainers()}
	incidentCleaner(t, false, docker, &out).run(context.Background())
	if got, want := sorted(docker.stopped), []string{"funny_feistel", "orkstack-proof-" + incidentProof + "-temporal"}; !slices.Equal(got, want) {
		t.Errorf("stopped %q, want %q:\n%s", got, want, out.String())
	}
}

// scripts/proof.sh's own sweep at its Temporal's start is the same rule:
// `proofrun clean -proof-containers` stops a gone proof's containers alone,
// touching no bucket, work root or activity container, and says each one it
// stopped on a line of its own.
func TestClean_TheProofContainersSweepStopsOnlyAGoneProofsContainers(t *testing.T) {
	var out strings.Builder
	docker := &fakeCleanDocker{
		temporals:  incidentContainers(),
		containers: []cleanContainer{{id: "c-proof-dead", orker: "soak-pr-aaa-17"}},
	}
	store := &fakeCleanStore{held: map[string]bool{"orkstack-engine-11-1": true}, listed: []string{"orkstack-engine-11-1"}}
	c := incidentCleaner(t, false, docker, &out)
	c.store = store
	c.proofContainersOnly = true
	c.run(context.Background())
	if len(docker.removed) != 0 || len(store.removed) != 0 {
		t.Errorf("the proof-containers sweep removed containers %q and buckets %q", docker.removed, store.removed)
	}
	for _, name := range []string{"funny_feistel", "orkstack-proof-" + incidentProof + "-temporal"} {
		if !strings.Contains(out.String(), "\nstopped "+name+"\n") {
			t.Errorf("the sweep does not say it stopped %s:\n%s", name, out.String())
		}
	}
}

// scripts/proof.sh gives everything it starts for the gate its own process as
// the owner: the Temporal, and the browser server the short-lived starter
// starts, through ORK_PROOF_OWNER. Its own sweep is proofrun's.
func TestProofScript_ItsContainersNameTheProofAsTheirOwner(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(mustRepoRoot(), "scripts", "proof.sh"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	for _, want := range []string{
		`PROOF_OWNER="$(hostname):$$:$(proc_start $$)"`,
		`--label "orkstack.proof.owner=$PROOF_OWNER"`,
		`ORK_PROOF_OWNER="$PROOF_OWNER" bun e2e/browser-server.ts start "$PROOF_ID"`,
		`"$PR" clean -remove -proof-containers`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("scripts/proof.sh lacks %s", want)
		}
	}
	if strings.Contains(text, `orkstack.proof.owner=$(hostname)`) {
		t.Error("scripts/proof.sh names an owner other than PROOF_OWNER")
	}
}
