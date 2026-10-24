package main

import (
	"strings"
	"testing"
)

const partnerLoop = "partner CI loop, INC-4412"

// admissionFixture is a CLI fixture with one orkflow applied into payments.
func admissionFixture(t *testing.T) *fixture {
	t.Helper()
	f := newFixture(t)
	f.run("namespace", "create", "payments").requireExit(t, exitOK)
	f.run("apply", writeOrkflow(t, "reconcile", "work"), "-n", "payments").requireExit(t, exitOK)
	return f
}

func TestNamespaceUpdate_RefusesHoldAndReleaseHoldTogether(t *testing.T) {
	f := admissionFixture(t)
	before := len(f.requests())
	res := f.run("namespace", "update", "payments", "--hold", partnerLoop, "--release-hold")
	if res.code == exitOK {
		t.Fatalf("--hold with --release-hold succeeded:\n%s", res.stdout)
	}
	if !strings.Contains(res.stderr, "--hold") || !strings.Contains(res.stderr, "--release-hold") {
		t.Errorf("stderr %q does not name both flags", res.stderr)
	}
	if after := f.requests(); len(after) != before {
		t.Errorf("a local refusal made RPCs: %v", after[before:])
	}
}

func TestNamespaceUpdate_RefusesABlankHoldReasonLocally(t *testing.T) {
	for _, reason := range []string{"", "   "} {
		f := admissionFixture(t)
		before := len(f.requests())
		res := f.run("namespace", "update", "payments", "--hold", reason)
		if res.code == exitOK {
			t.Fatalf("--hold %q succeeded:\n%s", reason, res.stdout)
		}
		if !strings.Contains(res.stderr, "--hold needs a reason") {
			t.Errorf("stderr %q, want --hold needs a reason", res.stderr)
		}
		if after := f.requests(); len(after) != before {
			t.Errorf("a local refusal made RPCs: %v", after[before:])
		}
	}
	f := admissionFixture(t)
	before := len(f.requests())
	res := f.run("namespace", "update", "payments", "--max-running", "-1")
	if res.code == exitOK {
		t.Fatalf("--max-running -1 succeeded:\n%s", res.stdout)
	}
	if after := f.requests(); len(after) != before {
		t.Errorf("a local refusal made RPCs: %v", after[before:])
	}
}

func TestRunSubmit_RefusedByHoldNamesTheReasonAndTheReleaseCommand(t *testing.T) {
	f := admissionFixture(t)
	f.run("namespace", "update", "payments", "--hold", partnerLoop).requireExit(t, exitOK)

	res := f.run("run", "submit", "reconcile", "-n", "payments")
	if res.code == exitOK {
		t.Fatalf("a held submit succeeded:\n%s", res.stdout)
	}
	want := `submit run: failed_precondition: namespace "payments" is on hold since 2026-01-02T03:04:05Z: ` +
		`"partner CI loop, INC-4412". Release it with ` + "`orkctl namespace update payments --release-hold`"
	if !strings.Contains(res.stderr, want) {
		t.Errorf("stderr =\n%s\nwant it to carry\n%s", res.stderr, want)
	}
	f.run("run", "list", "-n", "payments").requireExit(t, exitOK).requireContains(t, "No runs")
	if started := f.starter.startedRuns(); len(started) != 0 {
		t.Errorf("started = %v, want none", started)
	}

	// Released, the same submit is admitted.
	f.run("namespace", "update", "payments", "--release-hold").requireExit(t, exitOK)
	f.run("run", "submit", "reconcile", "-n", "payments").requireExit(t, exitOK).requireContains(t, "run-001")
}

func TestRunAgain_IsRefusedByAHoldLikeAnyOtherSubmit(t *testing.T) {
	f := admissionFixture(t)
	f.run("run", "submit", "reconcile", "-n", "payments").requireExit(t, exitOK).requireContains(t, "run-001")
	f.run("namespace", "update", "payments", "--hold", partnerLoop).requireExit(t, exitOK)

	res := f.run("run", "again", "run-001", "-n", "payments")
	if res.code == exitOK {
		t.Fatalf("run again into a held namespace succeeded:\n%s", res.stdout)
	}
	if !strings.Contains(res.stderr, "run again: failed_precondition: ") ||
		!strings.Contains(res.stderr, partnerLoop) ||
		!strings.Contains(res.stderr, "--release-hold") {
		t.Errorf("stderr %q does not carry the refusal", res.stderr)
	}
	if started := f.starter.startedRuns(); len(started) != 1 {
		t.Errorf("started = %v, want only the first run", started)
	}
}
