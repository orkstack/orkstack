package artifacts

import (
	"context"
	"errors"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// failingCommits answers every Get with err: a store that does not answer,
// which no real store produces on demand.
type failingCommits struct{ err error }

func (f failingCommits) Create(context.Context, string, *orkstackv1.ActivityCommit) (CommitRecord, error) {
	return CommitRecord{}, f.err
}
func (f failingCommits) Get(context.Context, string) (CommitRecord, error) {
	return CommitRecord{}, f.err
}

func TestReadCommit_AnswersWhatItFound(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCommitStore()
	good := &orkstackv1.ActivityCommit{SchemaVersion: CommitSchemaVersion, TaskId: "build"}
	if _, err := store.Create(ctx, "mem://c/good", good); err != nil {
		t.Fatal(err)
	}
	store.PutRaw("mem://c/garbage", []byte("not a commit"))
	future := &orkstackv1.ActivityCommit{SchemaVersion: CommitSchemaVersion + 1, TaskId: "build"}
	if _, err := store.Create(ctx, "mem://c/future", future); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		store CommitStore
		uri   string
		want  CommitAnswer
	}{
		{"found", store, "mem://c/good", CommitFound},
		{"absent", store, "mem://c/none", CommitAbsent},
		{"undecodable after the reread", store, "mem://c/garbage", CommitUndecodable},
		{"a schema this build does not know", store, "mem://c/future", CommitUndecodable},
		{"the store does not answer", failingCommits{errors.New("connection reset")}, "mem://c/good", CommitUnanswered},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, answer, err := ReadCommit(ctx, tc.store, tc.uri, time.Millisecond)
			if answer != tc.want {
				t.Fatalf("answer = %v (%v), want %v", answer, err, tc.want)
			}
			if (answer == CommitFound) != (err == nil) {
				t.Fatalf("answer %v with error %v: only a found commit has no error", answer, err)
			}
			if answer == CommitFound && rec.Commit.GetTaskId() != "build" {
				t.Fatalf("record = %+v", rec)
			}
		})
	}
}

func TestReadCommit_StopsWithItsContext(t *testing.T) {
	store := NewMemoryCommitStore()
	store.PutRaw("mem://c/garbage", []byte("x"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, answer, err := ReadCommit(ctx, store, "mem://c/garbage", time.Hour); answer != CommitUnanswered || err == nil {
		t.Fatalf("a canceled reread answered %v, %v; want unanswered", answer, err)
	}
}
