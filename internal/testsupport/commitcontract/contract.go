// Package commitcontract holds the behaviour every artifacts.CommitStore must
// show, so the local, in-process-fake and real S3 backends are measured by one
// body of assertions instead of three drifting ones.
//
// It is test support: no production code imports it.
package commitcontract

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// Run proves the CommitStore contract at one fresh uri. The caller supplies a
// uri no writer has used: Run creates the winning object itself.
func Run(t *testing.T, store artifacts.CommitStore, uri string) {
	t.Helper()
	ctx := context.Background()

	if _, err := store.Get(ctx, uri); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("Get before Create = %v, want ErrNotFound", err)
	}
	if _, err := store.Create(ctx, uri, nil); err == nil {
		t.Fatal("Create(nil) succeeded")
	}

	winner := Commit("run-contract-winner", "contract")
	first, err := store.Create(ctx, uri, winner)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if first.Commit == nil || first.Bytes == 0 || len(first.Raw) != int(first.Bytes) {
		t.Fatalf("Create record = %+v, want its bytes", first)
	}
	sum := sha256.Sum256(first.Raw)
	if first.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatalf("Create SHA256 = %q, want the digest of its own bytes", first.SHA256)
	}
	if !proto.Equal(first.Commit, winner) {
		t.Fatalf("Create decoded %v, want %v", first.Commit, winner)
	}

	got, err := store.Get(ctx, uri)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SHA256 != first.SHA256 || got.Bytes != first.Bytes || string(got.Raw) != string(first.Raw) {
		t.Fatalf("Get returned other bytes than Create stored: %+v against %+v", got, first)
	}
	if !proto.Equal(got.Commit, winner) {
		t.Fatalf("Get decoded %v, want %v", got.Commit, winner)
	}

	loser := Commit("run-contract-loser", "contract")
	if _, err := store.Create(ctx, uri, loser); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("a second Create = %v, want ErrPreconditionFailed", err)
	}
	if _, err := store.Create(ctx, uri, winner); !errors.Is(err, artifacts.ErrPreconditionFailed) {
		t.Fatalf("re-Create of the same bytes = %v, want ErrPreconditionFailed", err)
	}
	after, err := store.Get(ctx, uri)
	if err != nil {
		t.Fatalf("Get after the refused writers: %v", err)
	}
	if after.SHA256 != first.SHA256 || string(after.Raw) != string(first.Raw) {
		t.Fatal("a refused Create replaced the winner")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Create(canceled, uri, loser); err == nil {
		t.Fatal("Create with a canceled context succeeded")
	}
	if _, err := store.Get(canceled, uri); err == nil {
		t.Fatal("Get with a canceled context succeeded")
	}
}

// Commit is a minimally realistic commit for a contract run.
func Commit(runID, taskID string) *orkstackv1.ActivityCommit {
	return &orkstackv1.ActivityCommit{
		SchemaVersion:  artifacts.CommitSchemaVersion,
		Namespace:      artifacts.DefaultNamespace,
		RunId:          runID,
		TaskId:         taskID,
		OrkflowName:    "contract",
		OrkflowVersion: "1.0.0",
		IdempotencyKey: artifacts.DefaultNamespace + "/" + runID + "/" + taskID,
		Attempt:        1,
	}
}
