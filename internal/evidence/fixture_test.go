package evidence

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

var fixtureExportedAt = time.Date(2026, 9, 22, 10, 51, 49, 0, time.UTC)

func digestOf(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// fixtureBundle is a manifest plus the bytes of its present members.
type fixtureBundle struct {
	manifest *Manifest
	bytes    map[string][]byte
}

func presentMember(path string, kind Kind, taskID string, attempt int32, name, origin string, content []byte) Member {
	return Member{Path: path, Kind: kind, TaskID: taskID, Attempt: attempt, Name: name, Origin: origin,
		SizeBytes: int64(len(content)), SHA256: digestOf(content), State: StatePresent}
}

// newFixture builds the golden manifest: one succeeded task retried once, with a
// reclaimed log and an unrecorded attempt on a second task.
func newFixture() fixtureBundle {
	contents := map[string][]byte{
		"run.jsonl":                               []byte("{\"kind\":\"run\",\"run\":{\"id\":\"ork-20260910-1\"}}\n"),
		"orkflow/source.yaml":                     []byte("name: payments-reconcile\ntasks: []\n"),
		"tasks/reconcile/commit.json":             []byte("{\n  \"schemaVersion\": 1\n}\n"),
		"tasks/reconcile/values.json":             []byte("{\"total\":42}\n"),
		"tasks/reconcile/artifacts/rates.parquet": bytes.Repeat([]byte("r"), 3000),
		"tasks/reconcile/attempts/1.json":         []byte("{\"attempt\":1,\"outcome\":\"ATTEMPT_OUTCOME_FAILED\"}\n"),
		"tasks/reconcile/attempts/1/stdout.log":   []byte("first try\n"),
		"tasks/reconcile/attempts/2.json":         []byte("{\"attempt\":2,\"outcome\":\"ATTEMPT_OUTCOME_SUCCEEDED\"}\n"),
		"tasks/reconcile/attempts/2/stdout.log":   []byte("second try\n"),
		"tasks/reconcile/attempts/2/stderr.log":   []byte(""),
		"tasks/extract/attempts/1.json":           []byte("{\"attempt\":1}\n"),
	}
	s3 := "s3://orkstack-artifacts/"
	members := []Member{
		presentMember("run.jsonl", KindRun, "", 0, "", "metadata/runs/ork-20260910-1.jsonl", contents["run.jsonl"]),
		presentMember("orkflow/source.yaml", KindSource, "", 0, "", "metadata/orkflows/payments/payments-reconcile/1.4.0.json#source_yaml", contents["orkflow/source.yaml"]),
		presentMember("tasks/reconcile/commit.json", KindCommit, "reconcile", 0, "", s3+"metadata/commits/payments/ork-20260910-1/reconcile/commit.json", contents["tasks/reconcile/commit.json"]),
		presentMember("tasks/reconcile/values.json", KindValues, "reconcile", 0, "", s3+"orkflows/payments-reconcile/ork-20260910-1/reconcile/values.json/aa", contents["tasks/reconcile/values.json"]),
		presentMember("tasks/reconcile/artifacts/rates.parquet", KindArtifact, "reconcile", 0, "rates.parquet", s3+"orkflows/payments-reconcile/ork-20260910-1/reconcile/rates.parquet/ab12", contents["tasks/reconcile/artifacts/rates.parquet"]),
		presentMember("tasks/reconcile/attempts/1.json", KindAttempt, "reconcile", 1, "", "metadata/attempts/ork-20260910-1/reconcile/0000000001.json", contents["tasks/reconcile/attempts/1.json"]),
		presentMember("tasks/reconcile/attempts/1/stdout.log", KindStdout, "reconcile", 1, "", s3+"logs/1/stdout.log", contents["tasks/reconcile/attempts/1/stdout.log"]),
		{Path: "tasks/reconcile/attempts/1/stderr.log", Kind: KindStderr, TaskID: "reconcile", Attempt: 1, Origin: s3 + "logs/1/stderr.log",
			SizeBytes: 12, SHA256: digestOf([]byte("gone forever")), State: StateReclaimed, Reason: ReasonObjectMissing},
		presentMember("tasks/reconcile/attempts/2.json", KindAttempt, "reconcile", 2, "", "metadata/attempts/ork-20260910-1/reconcile/0000000002.json", contents["tasks/reconcile/attempts/2.json"]),
		presentMember("tasks/reconcile/attempts/2/stdout.log", KindStdout, "reconcile", 2, "", s3+"logs/2/stdout.log", contents["tasks/reconcile/attempts/2/stdout.log"]),
		presentMember("tasks/reconcile/attempts/2/stderr.log", KindStderr, "reconcile", 2, "", s3+"logs/2/stderr.log", contents["tasks/reconcile/attempts/2/stderr.log"]),
		presentMember("tasks/extract/attempts/1.json", KindAttempt, "extract", 1, "", "metadata/attempts/ork-20260910-1/extract/0000000001.json", contents["tasks/extract/attempts/1.json"]),
		{Path: "tasks/extract/attempts/2.json", Kind: KindAttempt, TaskID: "extract", Attempt: 2, State: StateUnrecorded, Reason: ReasonAttemptRecordMissing},
	}
	m := &Manifest{
		SchemaVersion:    SchemaVersion,
		RunID:            "ork-20260910-1",
		Namespace:        "payments",
		OrkflowName:      "payments-reconcile",
		OrkflowVersion:   "1.4.0",
		DefinitionDigest: "sha256:9f2c000000000000000000000000000000000000000000000000000000000000",
		RunStatus:        "SUCCEEDED",
		RunCreatedAt:     time.Date(2026, 9, 10, 2, 0, 1, 0, time.UTC),
		RunFinishedAt:    time.Date(2026, 9, 10, 2, 8, 42, 0, time.UTC),
		ExportedAt:       fixtureExportedAt,
		EngineVersion:    "v0.9.3",
		BuildID:          "2026.09.22-1",
		Scope:            Scope{Logs: true, Artifacts: true},
		Definition:       Definition{State: StatePresent},
		Tasks: []TaskEntry{
			{TaskID: "reconcile", Status: "SUCCEEDED", OutcomeAttempt: 2, Attempts: []AttemptEntry{
				{Attempt: 1, State: StatePresent, Outcome: "FAILED"},
				{Attempt: 2, State: StatePresent, Outcome: "SUCCEEDED"},
			}},
			{TaskID: "extract", Status: "FAILED", OutcomeAttempt: 2, ExitCode: 1, Attempts: []AttemptEntry{
				{Attempt: 1, State: StatePresent, Outcome: "FAILED"},
				{Attempt: 2, State: StateUnrecorded},
			}},
		},
		Members: members,
	}
	m.Counts = CountMembers(m.Members)
	return fixtureBundle{manifest: m, bytes: contents}
}

// mapSource serves members from memory; override replaces one member's bytes
// and missing answers NotFound for one path.
type mapSource struct {
	bytes    map[string][]byte
	override map[string][]byte
	missing  map[string]bool
}

func (s mapSource) Open(_ context.Context, m Member) (io.ReadCloser, error) {
	if s.missing[m.Path] {
		return nil, fmt.Errorf("object %s: %w", m.Origin, artifacts.ErrNotFound)
	}
	if b, ok := s.override[m.Path]; ok {
		return io.NopCloser(bytes.NewReader(b)), nil
	}
	b, ok := s.bytes[m.Path]
	if !ok {
		return nil, fmt.Errorf("fixture has no bytes for %s", m.Path)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func writeFixture(t *testing.T, f fixtureBundle) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := Write(context.Background(), &out, f.manifest, mapSource{bytes: f.bytes}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return out.Bytes()
}

// tarEntry is one hand-built entry.
type tarEntry struct {
	name     string
	body     []byte
	typeflag byte
}

// buildTar assembles a bundle from explicit entries, for the verifier tests.
func buildTar(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var out bytes.Buffer
	gz := gzip.NewWriter(&out)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		flag := e.typeflag
		if flag == 0 {
			flag = tar.TypeReg
		}
		h := &tar.Header{Name: e.name, Typeflag: flag, Mode: 0o644, ModTime: fixtureExportedAt}
		if flag == tar.TypeReg {
			h.Size = int64(len(e.body))
		} else if flag == tar.TypeSymlink {
			h.Linkname = "/etc/passwd"
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if flag == tar.TypeReg {
			if _, err := tw.Write(e.body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// fixtureEntries is the fixture's bundle as hand-built entries, in order.
func fixtureEntries(t *testing.T, f fixtureBundle) []tarEntry {
	t.Helper()
	document, err := f.manifest.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	entries := []tarEntry{{name: ManifestPath, body: document}}
	for _, m := range f.manifest.Members {
		if m.State == StatePresent {
			entries = append(entries, tarEntry{name: m.Path, body: bytes.Clone(f.bytes[m.Path])})
		}
	}
	return entries
}
