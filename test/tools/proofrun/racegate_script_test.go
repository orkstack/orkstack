package main

import (
	"strings"
	"testing"
)

// The race gate and the nightly lanes are scripts/proof.sh's and
// scripts/proof-part.sh's own code, so these tests run the real scripts in the
// throwaway repository sitegate_test.go builds, with a stub proofrun that logs
// the unit and fuzz invocations it is asked for. What the gates then do —
// which packages, which flags, which result — is proofrun's (race_test.go,
// fuzz_test.go, merge_race_test.go); what these hold is the wiring.

const racePackagesOfTheRepeat = "./internal/engine/... ./internal/orker/... ./internal/api/... ./pkg/..."

func TestProofScript_TheRaceGateRunsEveryPackageUnderTheDetectorWithShort(t *testing.T) {
	r := newSiteScriptRepo(t)
	code, out := r.run([]string{"PROOF_GATES=race"}, "--tier", "full", "race-pass")
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	gates := r.gates("race-pass")
	if len(gates) != 1 || !strings.HasPrefix(gates[0], "race\tpass\t") || !strings.Contains(gates[0], "\trace.log\t") {
		t.Errorf("gates.tsv = %q, want one passing race gate logged to race.log", gates)
	}
	want := "unit -race -short -out proof/race-pass"
	if calls := r.proofrunCalls(); len(calls) != 1 || calls[0] != want {
		t.Errorf("proofrun was asked for %q, want exactly %q: the full proof's race gate runs every package, once", calls, want)
	}
}

func TestProofScript_AFailingRaceGateFailsTheProof(t *testing.T) {
	r := newSiteScriptRepo(t)
	// The stub proofrun merges nothing, so the verdict (and the exit status
	// it decides) is merge_race_test.go's; what the script owns is the gate's
	// record and the fail-fast stop.
	_, out := r.run([]string{"PROOF_GATES=race", "STUB_FAIL_UNIT=1"}, "--tier", "full", "race-fail")
	if gates := r.gates("race-fail"); len(gates) != 1 || !strings.HasPrefix(gates[0], "race\tfail\t") {
		t.Errorf("gates.tsv = %q, want one failing race gate", gates)
	}
	if !strings.Contains(out, "gate race failed") {
		t.Errorf("the proof does not say the race gate failed:\n%s", out)
	}
}

// The full and release tiers always run the race gate and the nightly tier
// never does: asked without PROOF_GATES on a host with no Docker, each says
// what it is missing, and the race gate's cgo prerequisite is named by the
// tiers that run it.
func TestProofScript_TheFullAndReleaseTiersRunTheRaceGateAndNightlyRunsItsLanes(t *testing.T) {
	for _, c := range []struct {
		tier                 string
		race, repeat, fuzzes bool
	}{{"full", true, false, false}, {"release", true, false, false}, {"nightly", false, true, true}} {
		t.Run(c.tier, func(t *testing.T) {
			r := newSiteScriptRepo(t)
			r.withBun()
			r.withNode()
			r.withNodeModules("ui", "site")
			// No cgo: every tier that runs a race gate refuses, and the others do not.
			code, out := r.run([]string{"CGO_ENABLED=0"}, "--tier", c.tier, "lanes-tier")
			if code != 77 || !strings.Contains(out, "a docker daemon") {
				t.Fatalf("exit %d, want 77 with the stack gates' prerequisites listed (the positive control):\n%s", code, out)
			}
			if got := strings.Contains(out, "cgo for the race detector"); got != (c.race || c.repeat) {
				t.Errorf("the %s tier names the cgo prerequisite = %v, want %v:\n%s", c.tier, got, c.race || c.repeat, out)
			}
			if !r.wroteNoProof() {
				t.Error("a refused proof left a proof directory behind")
			}
		})
	}
}

func TestProofScript_TheRaceDetectorNeedsCgoAndTheGateSaysSo(t *testing.T) {
	r := newSiteScriptRepo(t)
	code, out := r.run([]string{"PROOF_GATES=race", "CGO_ENABLED=0"}, "--tier", "full", "race-nocgo")
	if code != 77 || !strings.Contains(out, "cgo for the race detector: CGO_ENABLED=1 and a C compiler on PATH") {
		t.Fatalf("exit %d, want 77 naming cgo:\n%s", code, out)
	}
	if !r.wroteNoProof() {
		t.Error("a refused proof left a proof directory behind")
	}
	if len(r.proofrunCalls()) != 0 {
		t.Errorf("proofrun ran for a refused proof: %v", r.proofrunCalls())
	}
}

func TestProofScript_TheNightlyTierRunsTheRaceRepeatAndTheFuzzLaneAlone(t *testing.T) {
	r := newSiteScriptRepo(t)
	code, out := r.run([]string{"PROOF_GATES=race-repeat,fuzz"}, "--tier", "nightly", "lanes-pass")
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	gates := r.gates("lanes-pass")
	if len(gates) != 2 || !strings.HasPrefix(gates[0], "race-repeat\tpass\t") || !strings.HasPrefix(gates[1], "fuzz\tpass\t") {
		t.Errorf("gates.tsv = %q, want race-repeat then fuzz, both passing", gates)
	}
	want := []string{
		"unit -race -short -count 3 -out proof/lanes-pass " + racePackagesOfTheRepeat,
		"fuzz -out proof/lanes-pass -fuzztime 2m",
	}
	if calls := r.proofrunCalls(); strings.Join(calls, "\n") != strings.Join(want, "\n") {
		t.Errorf("proofrun was asked for\n%s\nwant\n%s", strings.Join(calls, "\n"), strings.Join(want, "\n"))
	}
}

func TestProofScript_FUZZTIMESetsTheFuzzLanesTime(t *testing.T) {
	r := newSiteScriptRepo(t)
	if code, out := r.run([]string{"PROOF_GATES=fuzz", "FUZZTIME=45s"}, "--tier", "nightly", "fuzztime-ok"); code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	if calls := r.proofrunCalls(); len(calls) != 1 || !strings.HasSuffix(calls[0], "-fuzztime 45s") {
		t.Errorf("proofrun was asked for %q, want -fuzztime 45s", calls)
	}
}

func TestProofScript_AFuzzTimeThatIsNotADurationOrACountIsRefused(t *testing.T) {
	for _, bad := range []string{"forever", "0s", "-5m", "2", "1.5m", "5m;rm", "x"} {
		t.Run(bad, func(t *testing.T) {
			r := newSiteScriptRepo(t)
			code, out := r.run([]string{"PROOF_GATES=fuzz", "FUZZTIME=" + bad}, "--tier", "nightly", "fuzztime-bad")
			if code != 2 || !strings.Contains(out, "FUZZTIME must be") {
				t.Errorf("exit %d, want 2 naming FUZZTIME:\n%s", code, out)
			}
			if !r.wroteNoProof() {
				t.Error("a refused proof left a proof directory behind")
			}
		})
	}
}

func TestProofScript_AFailingFuzzLaneFailsTheProof(t *testing.T) {
	r := newSiteScriptRepo(t)
	_, out := r.run([]string{"PROOF_GATES=fuzz", "STUB_FAIL_FUZZ=1"}, "--tier", "nightly", "fuzz-fail")
	if !strings.Contains(out, "gate fuzz failed") {
		t.Errorf("the proof does not say the fuzz gate failed:\n%s", out)
	}
	if gates := r.gates("fuzz-fail"); len(gates) != 1 || !strings.HasPrefix(gates[0], "fuzz\tfail\t") {
		t.Errorf("gates.tsv = %q", gates)
	}
}

// A feature proof runs the race gate for the packages the change touches, and
// not at all when it touches none.
func TestProofScript_AFeatureProofRacesOnlyTheChangedPackages(t *testing.T) {
	t.Run("two packages", func(t *testing.T) {
		r := newSiteScriptRepo(t)
		code, out := r.run([]string{"PROOF_GATES=race", "STUB_GATES=integration=no\nrace_packages=./internal/dsl ./internal/engine\nspecs=\n"},
			"--tier", "feature", "feat-race")
		if code != 0 {
			t.Fatalf("exit %d:\n%s", code, out)
		}
		want := "unit -race -short -out proof/feat-race-feature ./internal/dsl ./internal/engine"
		if calls := r.proofrunCalls(); len(calls) != 1 || calls[0] != want {
			t.Errorf("proofrun was asked for %q, want exactly %q", calls, want)
		}
	})
	t.Run("none", func(t *testing.T) {
		r := newSiteScriptRepo(t)
		code, out := r.run([]string{"PROOF_GATES=race", "STUB_GATES=integration=no\nrace_packages=\nspecs=\n"},
			"--tier", "feature", "feat-norace")
		if code != 0 {
			t.Fatalf("exit %d:\n%s", code, out)
		}
		if calls := r.proofrunCalls(); len(calls) != 0 {
			t.Errorf("a change that touches no package ran the race gate: %q", calls)
		}
	})
}

// ---- the CI parts --------------------------------------------------------------

func TestProofPart_TheRacePartRunsTheRaceGateAlone(t *testing.T) {
	r := newSiteScriptRepo(t)
	base := r.git("rev-parse", "HEAD")
	code, out := r.run(nil, "--part", "race", "--tier", "full", "--proof-id", "pr-race", "--attempt", "1", "--base-commit", base, "race-part")
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	gates := r.gates("race-part-part-race-a1")
	if len(gates) != 1 || !strings.HasPrefix(gates[0], "race\tpass\t") {
		t.Errorf("the race part records gates %q, want the race gate alone", gates)
	}
	want := "unit -race -short -out proof/race-part-part-race-a1"
	if calls := r.proofrunCalls(); len(calls) != 1 || calls[0] != want {
		t.Errorf("proofrun was asked for %q, want exactly %q", calls, want)
	}
}

func TestProofPart_AFailingRaceGateFailsThePart(t *testing.T) {
	r := newSiteScriptRepo(t)
	base := r.git("rev-parse", "HEAD")
	code, _ := r.run([]string{"STUB_FAIL_UNIT=1"}, "--part", "race", "--tier", "full", "--proof-id", "pr-race", "--attempt", "1", "--base-commit", base, "race-part")
	if code == 0 {
		t.Error("a race part whose gate fails exited 0")
	}
	if gates := r.gates("race-part-part-race-a1"); len(gates) != 1 || !strings.HasPrefix(gates[0], "race\tfail\t") {
		t.Errorf("gates = %q", gates)
	}
}

func TestProofPart_TheNightlyLaneParts(t *testing.T) {
	for _, c := range []struct {
		part string
		want string
	}{
		{"race-repeat", "unit -race -short -count 3 -out proof/lane-part-part-race-repeat-a1 " + racePackagesOfTheRepeat},
		{"fuzz", "fuzz -out proof/lane-part-part-fuzz-a1 -fuzztime 90s"},
	} {
		t.Run(c.part, func(t *testing.T) {
			r := newSiteScriptRepo(t)
			base := r.git("rev-parse", "HEAD")
			code, out := r.run([]string{"FUZZTIME=90s"}, "--part", c.part, "--tier", "nightly", "--proof-id", "pr-lane", "--attempt", "1", "--base-commit", base, "lane-part")
			if code != 0 {
				t.Fatalf("exit %d:\n%s", code, out)
			}
			gates := r.gates("lane-part-part-" + c.part + "-a1")
			if len(gates) != 1 || !strings.HasPrefix(gates[0], c.part+"\tpass\t") {
				t.Errorf("the %s part records gates %q, want that gate alone", c.part, gates)
			}
			if calls := r.proofrunCalls(); len(calls) != 1 || calls[0] != c.want {
				t.Errorf("proofrun was asked for %q, want exactly %q", calls, c.want)
			}
		})
	}
}

func TestProofPart_ThePartsThatRaceNeedCgo(t *testing.T) {
	for _, part := range []string{"race", "race-repeat"} {
		t.Run(part, func(t *testing.T) {
			r := newSiteScriptRepo(t)
			base := r.git("rev-parse", "HEAD")
			code, out := r.run([]string{"CGO_ENABLED=0"}, "--part", part, "--tier", "full", "--proof-id", "pr-race", "--attempt", "1", "--base-commit", base, "nocgo-part")
			if code != 77 || !strings.Contains(out, "cgo for the race detector") {
				t.Errorf("exit %d, want 77 naming cgo:\n%s", code, out)
			}
			if !r.wroteNoProof() {
				t.Error("a refused part left a proof directory behind")
			}
		})
	}
}

// The unit gate runs proofrun's own tests, and some of them run go test -race
// (race_test.go), so the unit gate needs the race detector's toolchain
// wherever it runs: a feature proof whose change touches no Go package selects
// no race gate and still runs the unit gate, and its tests must not meet a
// missing compiler as an opaque `go: -race requires cgo`.
func TestProofScript_TheUnitGateNeedsCgoWhetherOrNotAnyPackageRaces(t *testing.T) {
	for _, tier := range []string{"full", "feature"} {
		t.Run(tier, func(t *testing.T) {
			r := newSiteScriptRepo(t)
			env := []string{"PROOF_GATES=unit", "CGO_ENABLED=0", "STUB_GATES=integration=no\nrace_packages=\nspecs=\n"}
			code, out := r.run(env, "--tier", tier, "unit-nocgo")
			if code != 77 || !strings.Contains(out, "cgo for the race detector: CGO_ENABLED=1 and a C compiler on PATH") {
				t.Fatalf("exit %d, want 77 naming cgo:\n%s", code, out)
			}
			if !r.wroteNoProof() {
				t.Error("a refused proof left a proof directory behind")
			}
			if len(r.proofrunCalls()) != 0 {
				t.Errorf("proofrun ran for a refused proof: %v", r.proofrunCalls())
			}
		})
	}
}

func TestProofPart_TheFastPartNeedsCgoForTheUnitGate(t *testing.T) {
	r := newSiteScriptRepo(t)
	r.withBun()
	r.withNode()
	r.withNodeModules("ui", "site")
	code, out := r.run([]string{"CGO_ENABLED=0"}, "--part", "fast", "--tier", "full", "--proof-id", "pr-fast", "--attempt", "1",
		"--base-commit", r.git("rev-parse", "HEAD"), "nocgo-fast")
	if code != 77 || !strings.Contains(out, "cgo for the race detector") {
		t.Errorf("exit %d, want 77 naming cgo:\n%s", code, out)
	}
	if !r.wroteNoProof() {
		t.Error("a refused part left a proof directory behind")
	}
}
