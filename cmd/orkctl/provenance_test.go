package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// provenanceCLI is the CLI fixture with a real local run archive and a real
// local artifact store behind the served API, so every marker a golden file
// renders was written by the production archiver.
type provenanceCLI struct {
	*fixture
	local      *artifacts.LocalRunArchive
	archiveDir string
	archiver   *engine.RunArchiver
	store      *artifacts.LocalStore
	storeDir   string
	report     *orkstackv1.ArtifactRef
}

func newProvenanceCLI(t *testing.T) *provenanceCLI {
	t.Helper()
	archiveDir, storeDir := t.TempDir(), t.TempDir()
	local, err := artifacts.NewLocalRunArchive(archiveDir)
	if err != nil {
		t.Fatalf("NewLocalRunArchive: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	store, err := artifacts.NewLocalStore(storeDir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(local)
	f := newFixtureWith(t, func(cfg *api.Config) {
		cfg.Archive = archiver
		cfg.ArtifactStore = store
	})
	stored, err := store.Put(context.Background(), "report.txt", bytes.NewBufferString("hello\n"), artifacts.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	return &provenanceCLI{
		fixture: f, local: local, archiveDir: archiveDir, archiver: archiver, store: store, storeDir: storeDir,
		report: &orkstackv1.ArtifactRef{Name: stored.Name, Uri: stored.URI, SizeBytes: stored.SizeBytes, Sha256: stored.SHA256, ContentType: stored.ContentType},
	}
}

// seedRun puts a finished run recording report.txt into the read model and,
// when archived, into the archive through the production archiver.
func (p *provenanceCLI) seedRun(t *testing.T, runID string, archived bool) {
	t.Helper()
	run := &orkstackv1.Run{
		Id: runID, OrkflowName: "completed", OrkflowVersion: "0.1.0",
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(fixedNow), FinishedAt: timestamppb.New(fixedNow.Add(time.Second)),
		TaskRuns: []*orkstackv1.TaskRun{{
			TaskId: "completed", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1,
			Artifacts: []*orkstackv1.ArtifactRef{p.report},
		}},
	}
	p.runs.Apply(run)
	if archived {
		if err := p.archiver.Archive(context.Background(), run, nil); err != nil {
			t.Fatalf("Archive: %v", err)
		}
	}
}

func (p *provenanceCLI) provenance(args ...string) result {
	p.t.Helper()
	return p.run(append([]string{"run", "artifact", "provenance"}, args...)...)
}

func TestRunArtifactProvenance_GoldenHeld(t *testing.T) {
	p := newProvenanceCLI(t)
	p.seedRun(t, "ork-1-a1", true)

	human := p.provenance("ork-1-a1", "--task", "completed", "--kind", "output", "--name", "report.txt").requireExit(t, exitOK)
	requireGolden(t, "run-artifact-provenance.golden", human.stdout)

	machine := p.provenance("ork-1-a1", "--task", "completed", "--kind", "output", "--name", "report.txt", "--json").requireExit(t, exitOK)
	// The recorded URI carries this test's temporary directory; everything
	// else in the raw response is fixed.
	requireGolden(t, "run-artifact-provenance.json.golden", strings.ReplaceAll(machine.stdout, filepath.ToSlash(p.storeDir), "/STORE"))
	for _, want := range []string{`"ownReference": "HELD"`, `"kind": "OUTPUT"`, `"bytesPresent": true`} {
		if !strings.Contains(machine.stdout, want) {
			t.Errorf("--json is not the raw response in the short-enum dialect; missing %s:\n%s", want, machine.stdout)
		}
	}
}

func TestRunArtifactProvenance_GoldenReleased(t *testing.T) {
	p := newProvenanceCLI(t)
	p.seedRun(t, "ork-1-a1", true)
	if _, err := p.local.DeleteRunArtifacts(context.Background(), "default", "ork-1-a1"); err != nil {
		t.Fatal(err)
	}
	if err := p.local.MarkArtifactsCleaned(context.Background(), "ork-1-a1", time.Date(2026, 1, 9, 3, 4, 5, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	out := p.provenance("ork-1-a1", "--task", "completed", "--kind", "output", "--name", "report.txt").requireExit(t, exitOK)
	requireGolden(t, "run-artifact-provenance-released.golden", out.stdout)
}

func TestRunArtifactProvenance_GoldenAbsent(t *testing.T) {
	p := newProvenanceCLI(t)
	p.seedRun(t, "ork-1-a1", false)
	out := p.provenance("ork-1-a1", "--task", "completed", "--kind", "output", "--name", "report.txt").requireExit(t, exitOK)
	requireGolden(t, "run-artifact-provenance-absent.golden", out.stdout)
}

// Missing bytes are a finding, not an error: exit 0.
func TestRunArtifactProvenance_GoldenBytesMissing(t *testing.T) {
	p := newProvenanceCLI(t)
	p.seedRun(t, "ork-1-a1", true)
	if err := p.store.Delete(context.Background(), p.report.GetUri()); err != nil {
		t.Fatal(err)
	}
	out := p.provenance("ork-1-a1", "--task", "completed", "--kind", "output", "--name", "report.txt").requireExit(t, exitOK)
	requireGolden(t, "run-artifact-provenance-bytes-missing.golden", out.stdout)
}

func TestRunArtifactProvenance_GoldenUnreadable(t *testing.T) {
	p := newProvenanceCLI(t)
	p.seedRun(t, "ork-1-a1", true)
	objectID, ok := artifacts.ObjectIDFromURI(p.report.GetUri())
	if !ok {
		t.Fatalf("ObjectIDFromURI(%s)", p.report.GetUri())
	}
	key, err := artifacts.RunObjectRefKey(objectID, "ork-1-orphan")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(p.archiveDir, filepath.FromSlash(key))
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	out := p.provenance("ork-1-a1", "--task", "completed", "--kind", "output", "--name", "report.txt").requireExit(t, exitOK)
	requireGolden(t, "run-artifact-provenance-unreadable.golden", out.stdout)
}

func TestRunArtifactProvenance_RequiresTaskKindAndName(t *testing.T) {
	p := newProvenanceCLI(t)
	for _, args := range [][]string{
		{"ork-1-a1"},
		{"ork-1-a1", "--kind", "output", "--name", "report.txt"},
		{"ork-1-a1", "--task", "completed", "--name", "report.txt"},
		{"ork-1-a1", "--task", "completed", "--kind", "output"},
	} {
		out := p.provenance(args...).requireExit(t, exitError)
		if !strings.Contains(out.stderr, "--task, --kind and --name are required") {
			t.Fatalf("%v: stderr = %q", args, out.stderr)
		}
	}
	out := p.provenance("ork-1-a1", "--task", "completed", "--kind", "bogus", "--name", "report.txt").requireExit(t, exitError)
	if !strings.Contains(out.stderr, "--kind must be output, values, stdout, or stderr") {
		t.Fatalf("--kind bogus: stderr = %q", out.stderr)
	}
	if got := p.requests(); len(got) != 0 {
		t.Fatalf("a flag error reached the server: %v", got)
	}
}

func TestRunArtifactProvenance_MismatchExitsNonZeroWithTheServerMessage(t *testing.T) {
	p := newProvenanceCLI(t)
	p.seedRun(t, "ork-1-a1", true)
	other := strings.Repeat("0", 64)
	out := p.provenance("ork-1-a1", "--task", "completed", "--kind", "output", "--name", "report.txt", "--expect-sha256", other).
		requireExit(t, exitError)
	recorded := strings.TrimPrefix(p.report.GetSha256(), "sha256:")
	want := "get artifact provenance: failed_precondition: recorded sha256 " + recorded + " does not match expected " + other
	if !strings.Contains(out.stderr, want) {
		t.Fatalf("stderr = %q; want it to contain %q", out.stderr, want)
	}
	// The matching digest, with the prefix, is the same answer as no check.
	p.provenance("ork-1-a1", "--task", "completed", "--kind", "output", "--name", "report.txt", "--expect-sha256", "sha256:"+recorded).
		requireExit(t, exitOK)
}

func TestRunArtifactProvenance_AnotherNamespacesRunIsNotFound(t *testing.T) {
	p := newProvenanceCLI(t)
	p.seedRun(t, "ork-1-a1", true)
	p.run("namespace", "create", "payments").requireExit(t, exitOK)
	out := p.run("-n", "payments", "run", "artifact", "provenance", "ork-1-a1", "--task", "completed", "--kind", "output", "--name", "report.txt").
		requireExit(t, exitError)
	if !strings.Contains(out.stderr, "not_found") {
		t.Fatalf("stderr = %q; want not_found", out.stderr)
	}
}
