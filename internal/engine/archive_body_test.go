package engine_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// G1: LoadRunBody is the archive body byte for byte, the raw half of Load.
func TestRunArchiver_LoadRunBodyIsTheStoredJSONL(t *testing.T) {
	archiver, _, _ := newArchiveFixture(t)
	ctx := context.Background()
	created := at(t, "2026-08-10T10:00:00Z")
	run := sampleRun(t, "ork-body-1", orkstackv1.Status_STATUS_SUCCEEDED, created)
	events := sampleEvents(t, "ork-body-1", created)
	if err := archiver.Archive(ctx, run, events); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	want, err := engine.MarshalRunArchive(run, events)
	if err != nil {
		t.Fatal(err)
	}
	body, err := archiver.LoadRunBody(ctx, "ork-body-1")
	if err != nil {
		t.Fatalf("LoadRunBody: %v", err)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("LoadRunBody is not MarshalRunArchive(run, events):\n got %q\nwant %q", body, want)
	}
	decoded, _, err := engine.UnmarshalRunArchive(body)
	if err != nil || decoded.GetId() != "ork-body-1" {
		t.Fatalf("UnmarshalRunArchive(body) = %v, %v", decoded.GetId(), err)
	}

	if _, err := archiver.LoadRunBody(ctx, "ork-never"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("unknown run = %v, want ErrNotFound", err)
	}
	var none *engine.RunArchiver
	if _, err := none.LoadRunBody(ctx, "ork-body-1"); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("nil archiver = %v, want ErrNotFound", err)
	}
}
