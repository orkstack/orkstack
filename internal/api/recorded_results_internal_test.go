package api

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/authn"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordedLookup is a router and reader of one run, whose start names its
// admission.
type recordedLookup struct {
	engine.RunReader
	start *engine.RunStartRecord
}

func (r recordedLookup) RunStart(context.Context, string) (*engine.RunStartRecord, error) {
	return r.start, nil
}

// TestListRecordedResults_WhatARowHides: a reader of the run learns that a
// result is on record and not where it lies or which Orker made it; a reader
// of its artifacts, or of the fleet, learns those too.
func TestListRecordedResults_WhatARowHides(t *testing.T) {
	ctx := context.Background()
	store, err := artifacts.NewRecordStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	commit, err := artifacts.CommitKey("default", "ork-1", "build")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordCommit(ctx, commit, &orkstackv1.ActivityCommit{SchemaVersion: 1, Namespace: "default", RunId: "ork-1", TaskId: "build",
		IdempotencyKey: "ork-ork-1-build", Values: &orkstackv1.ArtifactRef{Uri: "file:///v", Sha256: strings.Repeat("c", 64)},
		Recorded: &orkstackv1.CommitRecording{AdmissionSha256: sha, Pool: "gpu", OrkerId: "o-1", Time: timestamppb.Now()}}); err != nil {
		t.Fatal(err)
	}
	run := &orkstackv1.Run{Id: "ork-1", Namespace: "default", OrkflowName: "deploy",
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "build", Status: orkstackv1.Status_STATUS_SUCCEEDED}}}
	s := &Service{logger: slog.Default(), now: time.Now, recordedResults: store, recordedReads: newRecordedReads(nil),
		router: engine.NewSingleRouter(engine.Binding{Reader: recordedLookup{start: &engine.RunStartRecord{
			Plan: engine.RunPlan{Identity: &engine.IdentityPlan{AdmissionSHA256: sha}}}}})}
	s.access, _ = accessFor(t, authn.ModeEnforce,
		[]*orkstackv1.Role{{Name: "reader", Permissions: []string{"run.read"}}, {Name: "artifacts", Permissions: []string{"run.read", "run.artifacts.read"}},
			{Name: "fleet", Permissions: []string{"orker.read"}}},
		&orkstackv1.Binding{Name: "readers", Role: "reader", Scope: "namespace:default", Subjects: []string{"user:corp:vera", "user:corp:omar"}},
		&orkstackv1.Binding{Name: "artifacts", Role: "artifacts", Scope: "namespace:default", Subjects: []string{"user:corp:nina"}},
		&orkstackv1.Binding{Name: "fleet", Role: "fleet", Scope: "deployment", Subjects: []string{"user:corp:omar"}})
	for who, shown := range map[string]bool{"user:corp:vera": false, "user:corp:nina": true, "user:corp:omar": true} {
		as := authn.WithPrincipal(authn.WithMode(ctx, authn.ModeEnforce), &authn.Principal{Kind: authn.KindUser, ID: who, Issuer: "corp", Method: authn.MethodJWT})
		r, err := s.recordedRunOf(as, run)
		if err != nil {
			t.Fatal(err)
		}
		rows, _, err := r.page(as, "", "", 25)
		if err != nil {
			t.Fatal(err)
		}
		// Asked twice: what the replica keeps is hidden as what it reads.
		for range 2 {
			got, _, _ := r.read(as, rows, false, false)
			if len(got) != 1 || got[0].GetState() != orkstackv1.RecordedResultState_RECORDED_RESULT_STATE_ON_RECORD || got[0].GetValuesSha256() == "" {
				t.Fatalf("%s: %v", who, got)
			}
			if row := got[0]; (row.GetPool() == "gpu") != shown || (row.GetOrkerId() == "o-1") != shown || (row.GetUri() != "") != shown {
				t.Fatalf("%s is shown %v", who, row)
			}
		}
	}
}
