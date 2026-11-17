package api

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// reclaimedArchive is a RunArchive that keeps GC records.
type reclaimedArchive struct {
	RunArchive
	records map[string]artifacts.ReclaimRecord
	err     error
}

func (a reclaimedArchive) ReclaimRecord(_ context.Context, runID string) (artifacts.ReclaimRecord, error) {
	if a.err != nil {
		return artifacts.ReclaimRecord{}, a.err
	}
	rec, ok := a.records[runID]
	if !ok {
		return artifacts.ReclaimRecord{}, artifacts.ErrNotFound
	}
	return rec, nil
}

// goneStore answers NotFound for every object: the fault case of a read
// that races the GC's delete, which a real store cannot be made to answer at
// a chosen moment.
type goneStore struct{ artifacts.Store }

func (goneStore) Get(context.Context, string) (io.ReadCloser, error) {
	return nil, artifacts.ErrNotFound
}

const reclaimedObject = "orkflows/flow/ork-producer/task/out/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestReclaimedByGC(t *testing.T) {
	uri := "s3://bucket/" + reclaimedObject
	tests := []struct {
		name    string
		archive RunArchive
		uri     string
		want    bool
	}{
		{"producer reclaimed", reclaimedArchive{records: map[string]artifacts.ReclaimRecord{"ork-producer": {RunID: "ork-producer"}}}, uri, true},
		{"no record: a lifecycle rule or anything else", reclaimedArchive{records: map[string]artifacts.ReclaimRecord{}}, uri, false},
		{"kept for good", reclaimedArchive{records: map[string]artifacts.ReclaimRecord{"ork-producer": {RunID: "ork-producer", Kept: []string{reclaimedObject}}}}, uri, false},
		{"late commit", reclaimedArchive{records: map[string]artifacts.ReclaimRecord{"ork-producer": {RunID: "ork-producer", LateCommit: true}}}, uri, false},
		{"record unreadable", reclaimedArchive{err: errors.New("down")}, uri, false},
		{"archive without records", nil, uri, false},
		{"local layout", reclaimedArchive{records: map[string]artifacts.ReclaimRecord{"ork-producer": {RunID: "ork-producer"}}}, "file:///x/objects/n-a/0123", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewService(Config{Archive: tt.archive})
			if got := s.reclaimedByGC(context.Background(), tt.uri); got != tt.want {
				t.Fatalf("reclaimedByGC = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadArtifact_ReclaimedAnswersArtifactReclaimed(t *testing.T) {
	runs := engine.NewRunStore()
	runs.Save(&orkstackv1.Run{Id: "ork-reader", Status: orkstackv1.Status_STATUS_SUCCEEDED, TaskRuns: []*orkstackv1.TaskRun{{
		TaskId: "task", Attempt: 1,
		Artifacts: []*orkstackv1.ArtifactRef{{Name: "out", Uri: "s3://bucket/" + reclaimedObject, Sha256: strings.Repeat("a", 64)}},
	}}})
	for _, tt := range []struct {
		name    string
		records map[string]artifacts.ReclaimRecord
		code    connect.Code
	}{
		{"reclaimed", map[string]artifacts.ReclaimRecord{"ork-producer": {RunID: "ork-producer"}}, connect.CodeFailedPrecondition},
		{"missing otherwise", map[string]artifacts.ReclaimRecord{}, connect.CodeNotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := NewService(Config{Runs: runs, ArtifactStore: goneStore{}, Archive: reclaimedArchive{records: tt.records}})
			err := s.ReadArtifact(context.Background(), connect.NewRequest(&orkstackv1.ReadArtifactRequest{
				RunId: "ork-reader", TaskId: "task", Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "out",
			}), nil)
			if connect.CodeOf(err) != tt.code {
				t.Fatalf("code = %v (%v), want %v", connect.CodeOf(err), err, tt.code)
			}
			if tt.code == connect.CodeFailedPrecondition && !strings.Contains(err.Error(), task.ReasonArtifactReclaimed) {
				t.Fatalf("error %v does not name %s", err, task.ReasonArtifactReclaimed)
			}
		})
	}
}
