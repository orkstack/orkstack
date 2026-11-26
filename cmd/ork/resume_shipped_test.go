package main

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
)

// oneFailedRun is a cluster holding one failed run.
type oneFailedRun struct{ api.RunReader }

func (oneFailedRun) GetRun(_ context.Context, runID string) (*orkstackv1.Run, error) {
	return &orkstackv1.Run{Id: runID, Namespace: "default", OrkflowName: "o", Status: orkstackv1.Status_STATUS_FAILED}, nil
}

// TestResumeShipped_OffWhileBlocked is the release guard of resume: while
// anything in engine.ResumeBlockers is not done, a default build resumes no
// run. Its wiring hands the service no resume seam, ResumeRun says the engine
// resumes no runs, and no run says it can be resumed.
func TestResumeShipped_OffWhileBlocked(t *testing.T) {
	if len(engine.ResumeBlockers) > 0 && engine.ResumeShipped {
		t.Fatalf("resume is switched on with blockers not done: %v", engine.ResumeBlockers)
	}
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`Resume:\s+resumeSeam\(starter\),`).Match(raw) {
		t.Error("cmd/ork does not wire resume through resumeSeam")
	}
	// Whatever asks whether a run is archived asks about its newest ending.
	for _, wired := range []string{
		"WithEndings(archive)", "WithEndings(endings, newestEndings)", "WithBodies(newestEndings)",
		"newestEndings.Reclaim(reclaimStore)", "newestEndings.Restore(store)", "WithResumes(archivecleanup.NewEndingRecords(endings))",
	} {
		if !strings.Contains(string(raw), wired) {
			t.Errorf("cmd/ork does not wire %s", wired)
		}
	}
	if engine.ResumeShipped {
		return
	}
	seam := resumeSeam(engine.NewTemporalStarter(nil, ""))
	if seam != nil {
		t.Fatalf("resumeSeam = %T, want none", seam)
	}
	svc := api.NewService(api.Config{Reader: oneFailedRun{}, Resume: seam})
	for _, dry := range []bool{true, false} {
		_, err := svc.ResumeRun(context.Background(), connect.NewRequest(&orkstackv1.ResumeRunRequest{
			RunId: "ork-r", DryRun: dry, ExpectedTerminalSequence: 9,
		}))
		if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "this engine resumes no runs") {
			t.Errorf("ResumeRun(dry_run=%v) = %v, want FailedPrecondition: this engine resumes no runs", dry, err)
		}
	}
	got, err := svc.GetRun(context.Background(), connect.NewRequest(&orkstackv1.GetRunRequest{RunId: "ork-r"}))
	if err != nil || got.Msg.GetRun().GetResumeHint() != nil {
		t.Errorf("GetRun = %v, %v; want no resume hint", got, err)
	}
}
