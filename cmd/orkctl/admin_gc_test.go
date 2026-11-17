package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestRenderReclaimPreview(t *testing.T) {
	var out bytes.Buffer
	err := renderReclaimPreview(&out, &orkstackv1.PreviewArtifactReclaimResponse{
		Mode: "dry_run", CleanupEnabled: true, Held: true, HeldReason: "lowwater_unknown",
		LowWaterKnown: true, LowWaterRunId: "ork-old", LowWaterCreatedAt: timestamppb.New(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)),
		RunsExamined: 3, WaitingRuns: 1, NextCursor: "c1",
		Namespaces: []*orkstackv1.ReclaimNamespaceSummary{{Namespace: "default", WouldDeleteObjects: 2, WouldDeleteBytes: 20,
			Kept: []*orkstackv1.ReclaimReasonCount{{Reason: "lowwater", Objects: 1}}}},
		Samples: []*orkstackv1.ReclaimSample{{RunId: "ork-p", TaskId: "build", ArtifactName: "out", Bytes: 10, Outcome: "kept", Reason: "lowwater",
			WaitsForRunsCreatedBefore: timestamppb.New(time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC))}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"dry_run", "lowwater_unknown", "held by run ork-old", "default", "lowwater 1",
		"waits for runs created before 2026-11-02T00:00:00Z", "--cursor c1"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output lacks %q:\n%s", want, out.String())
		}
	}
}

func TestAdminGCPreview_SamplesNeedANamespace(t *testing.T) {
	g := &globalFlags{}
	cmd := newAdminGCPreviewCommand(g)
	cmd.SetArgs([]string{"--samples", "5"})
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "--namespace") {
		t.Fatalf("err = %v", err)
	}
}
