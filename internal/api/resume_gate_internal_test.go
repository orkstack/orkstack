package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestResumeGate_PreviewsOfAllRuns: however many runs are previewed at once,
// the process makes a bounded number of previews; one more waits for a place,
// and leaves nothing behind when its caller gives up.
func TestResumeGate_PreviewsOfAllRuns(t *testing.T) {
	var g resumeGate
	preview := func(ctx context.Context, run string) (func(), error) {
		return g.enter(ctx, &orkstackv1.ResumeRunRequest{RunId: run, DryRun: true}, time.Now(), 6)
	}
	var releases []func()
	for i := range maxResumePreviews {
		release, err := preview(t.Context(), fmt.Sprintf("ork-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	gone, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := preview(gone, "ork-more"); err == nil {
		t.Fatal("a preview past the bound was let in")
	}
	if _, err := g.enter(gone, &orkstackv1.ResumeRunRequest{RunId: "ork-more"}, time.Now(), 6); err != nil {
		t.Fatalf("a confirm waits for no preview: %v", err)
	}
	releases[0]()
	release, err := preview(t.Context(), "ork-more")
	if err != nil {
		t.Fatalf("a place was free: %v", err)
	}
	release()
}

// TestRecordedBy: whoever asked for a resume is named as the record says, an
// asserted one never as verified, on one line.
func TestRecordedBy(t *testing.T) {
	for want, rec := range map[string]*orkstackv1.ResumePlanRecord{
		"":                       {PrincipalVerified: true},
		`"ada" (asserted)`:       {Principal: "ada"},
		`"ada" (verified)`:       {Principal: "ada", PrincipalVerified: true},
		`"ada\nroot" (asserted)`: {Principal: "ada\nroot"},
	} {
		if got := recordedBy(rec); got != want {
			t.Errorf("recordedBy(%v) = %s, want %s", rec, got, want)
		}
	}
}
