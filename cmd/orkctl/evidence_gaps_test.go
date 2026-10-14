package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/evidence"
)

// runJSONLSource serves the one present member of a gapped bundle.
type runJSONLSource []byte

func (s runJSONLSource) Open(context.Context, evidence.Member) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s)), nil
}

// TestEvidenceVerifyCommand_PrintsTheGap: a bundle of a run that lost
// events before it was archived verifies (exit 0), and the gap is printed as
// a notice; --json carries it.
func TestEvidenceVerifyCommand_PrintsTheGap(t *testing.T) {
	body := []byte(`{"kind":"run","run":{"id":"ork-gap"}}` + "\n" +
		`{"kind":"gap","gap":{"first":1201,"last":2400,"reason":"segment object missing"}}` + "\n")
	sum := sha256.Sum256(body)
	at := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	manifest := &evidence.Manifest{
		SchemaVersion: evidence.SchemaVersionEventGaps, RunID: "ork-gap", Namespace: "default",
		OrkflowName: "deploy", OrkflowVersion: "1.0.0", RunStatus: "SUCCEEDED",
		RunCreatedAt: at, RunFinishedAt: at, ExportedAt: at, EngineVersion: "v0.9.3",
		Definition: evidence.Definition{State: evidence.StateReclaimed, Reason: evidence.ReasonDefinitionDeleted},
		Tasks:      []evidence.TaskEntry{},
		Members: []evidence.Member{{Path: "run.jsonl", Kind: evidence.KindRun, Origin: "metadata/runs/ork-gap.jsonl",
			SizeBytes: int64(len(body)), SHA256: hex.EncodeToString(sum[:]), State: evidence.StatePresent}},
		EventGaps: []evidence.EventGap{{First: 1201, Last: 2400, Reason: "segment object missing"}},
	}
	manifest.Counts = evidence.CountMembers(manifest.Members)
	var bundle bytes.Buffer
	if err := evidence.Write(context.Background(), &bundle, manifest, runJSONLSource(body)); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "gapped.tar.gz")
	if err := os.WriteFile(path, bundle.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	f := newFixture(t)
	res := f.run("run", "evidence", "verify", path)
	if res.code != exitOK {
		t.Fatalf("exit %d: %s%s", res.code, res.stdout, res.stderr)
	}
	want := "events 1201–2400 of this run were lost from object storage before it was archived (segment object missing)"
	if !strings.Contains(res.stdout, want) {
		t.Fatalf("verify output lacks the gap notice:\n%s", res.stdout)
	}
	t.Logf("verify:\n%s", res.stdout)
	jsonRes := f.run("run", "evidence", "verify", path, "--json")
	if jsonRes.code != exitOK {
		t.Fatalf("--json exit %d: %s", jsonRes.code, jsonRes.stderr)
	}
	var report struct {
		EventGaps []evidence.EventGap `json:"event_gaps"`
	}
	if err := json.Unmarshal([]byte(jsonRes.stdout), &report); err != nil {
		t.Fatalf("--json: %v\n%s", err, jsonRes.stdout)
	}
	if len(report.EventGaps) != 1 || report.EventGaps[0].First != 1201 {
		t.Fatalf("--json event_gaps = %+v", report.EventGaps)
	}
}
