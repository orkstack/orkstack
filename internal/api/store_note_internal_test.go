package api

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A refusal for an edit that lost its commit keeps what the store said it did
// with the version it wrote, and its code.
func TestEditRaceRefusal_CarriesTheStoresNote(t *testing.T) {
	moved := &artifacts.HeadMovedError{Namespace: "team-a", Name: "deploy", Expected: "1.0.0", Head: "3.0.0"}
	stored := fmt.Errorf("%w (the version stays stored at s3://b/k: the head or the current pointer names it)", moved)
	if note := storeNote(stored, moved); note != "(the version stays stored at s3://b/k: the head or the current pointer names it)" {
		t.Fatalf("storeNote = %q", note)
	}
	if note := storeNote(moved, moved); note != "" {
		t.Fatalf("storeNote of a bare refusal = %q, want none", note)
	}
	err := withStoreNote(editRaceRefusal("team-a", "deploy", "", "1.0.0", "3.0.0"), storeNote(stored, moved))
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) || connectErr.Code() != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "the version stays stored") || !strings.Contains(err.Error(), "re-apply") {
		t.Fatalf("refusal = %v, want FailedPrecondition with the store's note", err)
	}
}
