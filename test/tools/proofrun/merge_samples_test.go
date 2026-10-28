package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The informational readings (docs/PROOFS.md §5) reach the merged proof.json
// (runner_health) and the README, and change no verdict: the floor judges each
// metric's first reading, as it always did. The fixtures are
// testdata/runnerhealth.

func (f *mergeFixture) readmeText() string {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(f.out, "README.md"))
	if err != nil {
		f.t.Fatal(err)
	}
	return string(b)
}

func healthOf(t *testing.T, all []partHealth, part string) partHealth {
	t.Helper()
	for _, h := range all {
		if h.Part == part {
			return h
		}
	}
	t.Fatalf("runner_health has no %s: %+v", part, all)
	return partHealth{}
}

func TestMerge_LaterReadingsReachProofJSONAndTheREADMEAndJudgeNothing(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	// Its later disk reads and its second fsync are far past the floors; its
	// first readings are healthy.
	f.writeHealth("integration-2-a1", "samples-above-floor.json")
	f.writeJobs(1)
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible {
		t.Fatalf("code %d result %s eligible %v: later readings past the floors changed a verdict: %v", code, r.Result, r.MergeEligible, r.Ineligible)
	}
	if len(r.DegradedRunners) != 0 || len(r.Reruns) != 0 {
		t.Errorf("a runner was degraded by a later reading: %+v %+v", r.DegradedRunners, r.Reruns)
	}
	h := healthOf(t, r.RunnerHealth, "integration:2")
	if h.Verdict != healthAboveFloor {
		t.Errorf("verdict = %s", h.Verdict)
	}
	read := healthMetricNamed(t, &runnerHealth{Metrics: h.Metrics}, "disk_read_mb_s")
	if len(read.Samples) != 3 || read.Samples[2].Offset != 11 || read.Samples[2].Value != 9.8 || read.Samples[2].Seconds != 1 {
		t.Errorf("proof.json lost the disk-read readings: %+v", read.Samples)
	}
	if fs := healthMetricNamed(t, &runnerHealth{Metrics: h.Metrics}, "fsync_p95_ms"); len(fs.Samples) != 2 || fs.Samples[1].Value != 400.25 {
		t.Errorf("proof.json lost the fsync readings: %+v", fs.Samples)
	}
	// proof.json on disk, not only the struct the merge returned.
	var onDisk proofRecord
	if err := readJSON(filepath.Join(f.out, "proof.json"), &onDisk); err != nil {
		t.Fatal(err)
	}
	if read := healthMetricNamed(t, &runnerHealth{Metrics: healthOf(t, onDisk.RunnerHealth, "integration:2").Metrics}, "disk_read_mb_s"); len(read.Samples) != 3 {
		t.Errorf("the written proof.json has %d disk-read readings, want 3", len(read.Samples))
	}
	readme := f.readmeText()
	for _, want := range []string{
		"## Runner health\n",
		"only the first reading of each metric is judged",
		"- integration:2 attempt 1 (github-hosted/ubuntu24/4cpu): above-floor, measured 13.1s; disk_read_mb_s 311 MB/s at +2.5s (judged), 12.5 at +8s, 9.8 at +11s; fsync_p95_ms 6.41 ms at +4.5s (judged), 400.25 at +12s\n",
	} {
		if !strings.Contains(readme, want) {
			t.Errorf("the README lacks %q:\n%s", want, readme)
		}
	}
	if strings.Contains(readme, "## Degraded runners") {
		t.Errorf("the README degrades a runner whose first readings were healthy:\n%s", readme)
	}
	if i, j := strings.Index(readme, "## Runner health"), strings.Index(readme, "## Gates"); i < 0 || j < i {
		t.Errorf("the runner health section (at %d) does not precede the gates (at %d)", i, j)
	}
	if !strings.HasSuffix(readme, "Result: **PASS**\n") {
		t.Errorf("the README does not close with the result line:\n%s", readme)
	}
}

func TestMerge_AFirstReadingBelowTheFloorStillDegradesWhateverTheLaterOnesRead(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	f.writeHealth("integration-2-a1", "samples-below-floor.json")
	f.writeJobs(1, githubJob{Name: "integration (2)", Conclusion: "failure", FailedStep: "integration shard"})
	r, code := f.merge()
	if code == 0 || r.MergeEligible {
		t.Fatalf("code %d eligible %v: a first burst at 31 MB/s, under a floor of 40, did not degrade the runner", code, r.MergeEligible)
	}
	if len(r.DegradedRunners) != 1 {
		t.Fatalf("degraded_runners = %+v, want exactly the first disk read", r.DegradedRunners)
	}
	if d := r.DegradedRunners[0]; d.Part != "integration:2" || d.Metric != "disk_read_mb_s" || d.Value != 31 || d.Floor != 40 {
		t.Errorf("degraded_runners[0] = %+v", d)
	}
	if want := "part integration:2 ran on a degraded runner (attempt 1, its runner measured below the floor: disk_read_mb_s 31 MB/s below the floor 40)"; !strings.Contains(strings.Join(r.Ineligible, "\n"), want) {
		t.Errorf("ineligible = %v\nwant %q", r.Ineligible, want)
	}
	if readme := f.readmeText(); !strings.Contains(readme, "disk_read_mb_s 31 MB/s at +2.5s (judged), 305.2 at +8s, 298.7 at +11s") {
		t.Errorf("the README lacks the readings of the degraded part:\n%s", readme)
	}
}

// A part measured before the readings existed, and a metric read once, print
// no readings and nothing breaks.
func TestMerge_AHealthFileWithoutReadingsPrintsItsVerdictAlone(t *testing.T) {
	f := newMergeFixture(t, tierFull, true)
	f.writeCI()
	f.writeShard(1, []entry{passEntry("TestA")}, nil, 1)
	f.writeShard(2, []entry{passEntry("TestB")}, nil, 1)
	f.writeHealth("integration-2-a1", "above-floor.json")
	f.writeJobs(1)
	r, code := f.merge()
	if code != 0 || !r.MergeEligible {
		t.Fatalf("code %d eligible %v: %v", code, r.MergeEligible, r.Ineligible)
	}
	for _, m := range healthOf(t, r.RunnerHealth, "integration:2").Metrics {
		if len(m.Samples) != 0 {
			t.Errorf("%s carries readings the file never had: %+v", m.Metric, m.Samples)
		}
	}
	if want := "- integration:2 attempt 1 (github-hosted/ubuntu24/4cpu): above-floor, measured 10.1s\n"; !strings.Contains(f.readmeText(), want) {
		t.Errorf("the README lacks %q:\n%s", want, f.readmeText())
	}
}

func TestMerge_ALocalProofListsItsReadingsToo(t *testing.T) {
	f := newMergeFixture(t, tierFull, false)
	f.writeLocal(ledger{Suite: "integration", Tests: []entry{passEntry("TestA"), passEntry("TestB")}})
	f.writeHealth("local-a1", "samples-below-floor.json")
	r, code := f.merge()
	if code != 0 || r.Result != "pass" || !r.MergeEligible || len(r.DegradedRunners) != 0 {
		t.Fatalf("code %d result %s eligible %v degraded %+v: the floor does not bind a local part", code, r.Result, r.MergeEligible, r.DegradedRunners)
	}
	if want := "- local attempt 1 (github-hosted/ubuntu24/4cpu): below-floor, measured 13.2s; disk_read_mb_s 31 MB/s at +2.5s (judged), 305.2 at +8s, 298.7 at +11s; fsync_p95_ms 6.41 ms at +4.5s (judged), 5.2 at +12s\n"; !strings.Contains(f.readmeText(), want) {
		t.Errorf("the README lacks %q:\n%s", want, f.readmeText())
	}
}

// A run with nothing measured has no section.
func TestWriteRunnerHealthSection_IsAbsentWithoutMeasurements(t *testing.T) {
	var b bytes.Buffer
	writeRunnerHealthSection(&b, nil)
	if b.Len() != 0 {
		t.Errorf("an empty section was written: %q", b.String())
	}
}

// The feature tier writes proof.json and the README's closing section from
// `record` alone: the local host's readings reach both.
func TestRecord_AFeatureProofCarriesTheReadingsIntoProofJSONAndTheVerdictSection(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "gates.tsv"), []byte("unit\tpass\t3\tunit.log\tproofrun unit\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "selection.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	healthFile := filepath.Join(dir, runnerHealthFile)
	b, err := os.ReadFile(filepath.Join(healthFixtures, "samples-above-floor.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(healthFile, b, 0o644); err != nil {
		t.Fatal(err)
	}
	quiet(t)
	prev := os.Stdout
	rd, wr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = wr
	code := cmdRecord([]string{"-dir", dir, "-tier", tierFeature, "-attempt", "1", "-tree-start", "t", "-tree-end", "t", "-clean-start", "-clean-end", "-runner-health", healthFile, "-select", filepath.Join(dir, "selection.json")})
	wr.Close()
	os.Stdout = prev
	out, _ := io.ReadAll(rd)
	if code > 1 {
		t.Fatalf("exit %d: %s", code, out)
	}
	var r proofRecord
	if err := readJSON(filepath.Join(dir, "proof.json"), &r); err != nil {
		t.Fatal(err)
	}
	read := healthMetricNamed(t, &runnerHealth{Metrics: healthOf(t, r.RunnerHealth, "local").Metrics}, "disk_read_mb_s")
	if len(read.Samples) != 3 || read.Samples[1].Value != 12.5 {
		t.Errorf("proof.json's local runner_health lost the readings: %+v", read.Samples)
	}
	section, verdict := strings.Index(string(out), "## Runner health"), strings.Index(string(out), "## Verdict")
	if section < 0 || verdict < section {
		t.Errorf("the closing section does not carry the runner health before the verdict:\n%s", out)
	}
	if want := "- local attempt 1 (github-hosted/ubuntu24/4cpu): above-floor, measured 13.1s; disk_read_mb_s 311 MB/s at +2.5s (judged), 12.5 at +8s, 9.8 at +11s"; !strings.Contains(string(out), want) {
		t.Errorf("the closing section lacks %q:\n%s", want, out)
	}
}
