package artifacts

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRecordTestWriter_OnlyTestsUseIt: the test writer and format 2 exist
// only under the integration tag, and nothing but a test file or test
// support names them, so no release binary can write a test's records or
// read format 2.
func TestRecordTestWriter_OnlyTestsUseIt(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	defining := filepath.Join(root, "pkg", "artifacts", "record_testwriter_integration.go")
	raw, err := os.ReadFile(defining)
	if err != nil || !strings.HasPrefix(string(raw), "//go:build integration\n") {
		t.Fatalf("%s is not integration-only: %v", defining, err)
	}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "gen", "ui", "site", ".data", "proof":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || path == defining ||
			strings.Contains(path, string(filepath.Separator)+"testsupport"+string(filepath.Separator)) {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, name := range []string{"NewRecordTestWriter", "EnableRecordFormat2", "recordFormats.Swap("} {
			if strings.Contains(string(body), name) {
				t.Errorf("%s names %s", path, name)
			}
		}
		if n := strings.Count(string(body), "recordFormats.Store("); n > 0 &&
			(filepath.Base(path) != "recorded_values.go" || n != 1 || !strings.Contains(string(body), "recordFormats.Store(1)")) {
			t.Errorf("%s sets the record formats this build reads", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// recordReadSites are the production files that read a RecordedValues,
// each an adopting site (it takes values, through RecordedFound.InTime) or
// an answering one (it only reports what is on record, also by InTime).
// A new caller is added here with its class, so a review sees it.
var recordReadSites = map[string]string{
	// Step 6a asks whether a result is on record, through Standing.
	"internal/identity/follows_check.go": "answer",
	// The await doors answer what is on record and tell the run; they adopt
	// nothing, and judge a record late by RecordedInTime.
	"internal/api/await_record.go": "answer",
	// The one read of a run's await, which adopts only what InTime does.
	"internal/engine/resolution_record.go": "adopt",
	// The listing shows what is on record, and tells a late record by
	// RecordedInTime.
	"internal/api/recorded_results.go": "answer",
	// An evidence export holds each record's stored bytes and its state as the
	// listing says it; it adopts nothing.
	"internal/api/evidence_records.go": "answer",
	// A composed task's record takes its child's outputs', which nothing is
	// due by: it adopts no resolution.
	"internal/identity/values_record.go": "answer",
	// What a resume reads of an admitted run: an answer's values only through
	// InTime; a composed task's, which nothing is due by, from its record.
	"internal/engine/resume_records.go": "adopt",
	// A resumed run's read-back holds a settled task's record to the values
	// its plan names, which ResumeRun took through InTime above; the values
	// it reads are the plan's, never the record's alone.
	"internal/engine/adopt_recorded.go": "answer",
}

// TestRecordReader_AdoptionSitesHoldRecordedInTime: every production file
// outside this package that reads a RecordedValues is a registered site,
// and an adopting site takes values only through InTime: RecordedFound
// gives no values otherwise.
func TestRecordReader_AdoptionSitesHoldRecordedInTime(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "gen", "ui", "site", ".data", "proof":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") ||
			filepath.Dir(path) == filepath.Join(root, "pkg", "artifacts") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(body), ".ReadRecordedValues(") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		switch recordReadSites[filepath.ToSlash(rel)] {
		case "answer":
		case "adopt":
			if !strings.Contains(string(body), ".InTime(") {
				t.Errorf("%s adopts records and never calls InTime", rel)
			}
		default:
			t.Errorf("%s reads RecordedValues and is not a registered site (adopt or answer)", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
