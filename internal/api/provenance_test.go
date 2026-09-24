package api_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// provenanceNow is the fixed clock every provenance fixture answers with, so
// computed_at is asserted exactly.
var provenanceNow = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

// provenanceFixture wires a service the way `ork` does for this read: the
// production archiver over a real local archive (so real markers exist), a
// real local artifact store, a cluster-backed reader, and a namespace
// registry. Nothing answers from process memory.
type provenanceFixture struct {
	svc        *api.Service
	reader     *fakeReader
	local      *artifacts.LocalRunArchive
	archiveDir string
	archiver   *engine.RunArchiver
	store      *artifacts.LocalStore
}

type provenanceOptions struct {
	archive func(*engine.RunArchiver) api.RunArchive
	store   func(artifacts.Store) artifacts.Store
	noStore bool
	noArch  bool
}

func newProvenanceFixture(t *testing.T, opts ...func(*provenanceOptions)) *provenanceFixture {
	t.Helper()
	var o provenanceOptions
	for _, opt := range opts {
		opt(&o)
	}
	archiveDir := t.TempDir()
	local, err := artifacts.NewLocalRunArchive(archiveDir)
	if err != nil {
		t.Fatalf("NewLocalRunArchive: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	store, err := artifacts.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	archiver := engine.NewRunArchiver(local)

	cfg := api.Config{
		Reader: &fakeReader{},
		Runs:   engine.NewRunStore(),
		Namespaces: registry{
			"default":  {Name: "default"},
			"payments": {Name: "payments"},
			"staging":  {Name: "staging"},
		},
		Starter: &fakeStarter{},
		Now:     func() time.Time { return provenanceNow },
	}
	if !o.noArch {
		cfg.Archive = archiver
		if o.archive != nil {
			cfg.Archive = o.archive(archiver)
		}
	}
	if !o.noStore {
		cfg.ArtifactStore = store
		if o.store != nil {
			cfg.ArtifactStore = o.store(store)
		}
	}
	return &provenanceFixture{
		svc: api.NewService(cfg), reader: cfg.Reader.(*fakeReader), local: local,
		archiveDir: archiveDir, archiver: archiver, store: store,
	}
}

// put stores content under name and returns the reference a task records.
func (f *provenanceFixture) put(t *testing.T, name, content string) *orkstackv1.ArtifactRef {
	t.Helper()
	ref, err := f.store.Put(context.Background(), name, bytes.NewBufferString(content), artifacts.PutOptions{})
	if err != nil {
		t.Fatalf("Put(%s): %v", name, err)
	}
	return &orkstackv1.ArtifactRef{Name: ref.Name, Uri: ref.URI, SizeBytes: ref.SizeBytes, Sha256: ref.SHA256, ContentType: ref.ContentType}
}

// provTask is one task run recording outputs.
type provTask struct {
	id      string
	outputs []*orkstackv1.ArtifactRef
}

// newRun builds a finished run; the reader answers for it, as Temporal does
// for any run whose history it still holds.
func (f *provenanceFixture) newRun(id, namespace string, created time.Time, tasks ...provTask) *orkstackv1.Run {
	run := &orkstackv1.Run{
		Id: id, Namespace: namespace, OrkflowName: "completed", OrkflowVersion: "0.1.0",
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(created), FinishedAt: timestamppb.New(created.Add(time.Second)),
	}
	for _, task := range tasks {
		run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{
			TaskId: task.id, Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, Artifacts: task.outputs,
		})
	}
	f.reader.setRun(run)
	return run
}

// archive writes the run through the production archiver: real markers.
func (f *provenanceFixture) archive(t *testing.T, run *orkstackv1.Run) {
	t.Helper()
	if err := f.archiver.Archive(context.Background(), run, nil); err != nil {
		t.Fatalf("Archive(%s): %v", run.GetId(), err)
	}
}

func (f *provenanceFixture) ask(namespace, runID, taskID, name string) (*orkstackv1.GetArtifactProvenanceResponse, error) {
	return f.askReq(&orkstackv1.GetArtifactProvenanceRequest{
		RunId: runID, Namespace: namespace, TaskId: taskID,
		Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: name,
	})
}

func (f *provenanceFixture) askReq(req *orkstackv1.GetArtifactProvenanceRequest) (*orkstackv1.GetArtifactProvenanceResponse, error) {
	resp, err := f.svc.GetArtifactProvenance(context.Background(), connect.NewRequest(req))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func requireCode(t *testing.T, err error, want connect.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("call succeeded; want %s", want)
	}
	if got := connect.CodeOf(err); got != want {
		t.Fatalf("code = %s (%v); want %s", got, err, want)
	}
}

func refRows(res *orkstackv1.GetArtifactProvenanceResponse) []string {
	rows := make([]string, 0, len(res.GetReferences()))
	for _, ref := range res.GetReferences() {
		rows = append(rows, ref.GetRunId()+"/"+ref.GetTaskId()+"/"+ref.GetKind().String())
	}
	return rows
}

var t0 = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func TestGetArtifactProvenance_AnArchivedRunHoldsItsOwnReference(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	f.archive(t, run)

	res, err := f.ask("", run.GetId(), "completed", "report.txt")
	if err != nil {
		t.Fatalf("GetArtifactProvenance: %v", err)
	}
	if res.GetArtifact().GetUri() != report.GetUri() || res.GetArtifact().GetSha256() != report.GetSha256() {
		t.Fatalf("artifact = %+v; want the recorded reference %+v", res.GetArtifact(), report)
	}
	if !res.GetBytesPresent() {
		t.Fatal("bytes_present = false for an object in the store")
	}
	if res.GetOwnReference() != orkstackv1.ReferenceState_REFERENCE_STATE_HELD {
		t.Fatalf("own_reference = %s; want HELD", res.GetOwnReference())
	}
	if res.GetReleasedAt() != nil {
		t.Fatalf("released_at = %v on a held reference", res.GetReleasedAt())
	}
	if len(res.GetReferences()) != 1 {
		t.Fatalf("references = %v; want one", refRows(res))
	}
	ref := res.GetReferences()[0]
	if ref.GetRunId() != run.GetId() || ref.GetTaskId() != "completed" || ref.GetKind() != orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT ||
		ref.GetRunStatus() != orkstackv1.Status_STATUS_SUCCEEDED || !ref.GetRunCreatedAt().AsTime().Equal(t0) ||
		ref.GetOrkflowName() != "completed" || ref.GetOrkflowVersion() != "0.1.0" {
		t.Fatalf("reference = %+v", ref)
	}
	if res.GetReferencesUnreadable() != 0 {
		t.Fatalf("references_unreadable = %d", res.GetReferencesUnreadable())
	}
	if !res.GetComputedAt().AsTime().Equal(provenanceNow) {
		t.Fatalf("computed_at = %v; want the fixed clock %v", res.GetComputedAt().AsTime(), provenanceNow)
	}
}

func TestGetArtifactProvenance_ALiveRunHasNoReferenceYet(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	run.Status = orkstackv1.Status_STATUS_RUNNING

	res, err := f.ask("default", run.GetId(), "completed", "report.txt")
	if err != nil {
		t.Fatalf("GetArtifactProvenance: %v", err)
	}
	if res.GetOwnReference() != orkstackv1.ReferenceState_REFERENCE_STATE_ABSENT {
		t.Fatalf("own_reference = %s; want ABSENT", res.GetOwnReference())
	}
	if len(res.GetReferences()) != 0 || res.GetReleasedAt() != nil || !res.GetBytesPresent() {
		t.Fatalf("live run = references %v released_at %v bytes %v", refRows(res), res.GetReleasedAt(), res.GetBytesPresent())
	}
}

func TestGetArtifactProvenance_ACleanedRunReportsReleasedWithItsTimestamp(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	f.archive(t, run)
	if _, err := f.local.DeleteRunArtifacts(context.Background(), "default", run.GetId()); err != nil {
		t.Fatal(err)
	}
	cleaned := time.Date(2026, 1, 9, 3, 4, 5, 0, time.UTC)
	if err := f.local.MarkArtifactsCleaned(context.Background(), run.GetId(), cleaned); err != nil {
		t.Fatal(err)
	}

	res, err := f.ask("default", run.GetId(), "completed", "report.txt")
	if err != nil {
		t.Fatalf("GetArtifactProvenance: %v", err)
	}
	if res.GetOwnReference() != orkstackv1.ReferenceState_REFERENCE_STATE_RELEASED {
		t.Fatalf("own_reference = %s; want RELEASED", res.GetOwnReference())
	}
	if res.GetReleasedAt() == nil || !res.GetReleasedAt().AsTime().Equal(cleaned) {
		t.Fatalf("released_at = %v; want %v", res.GetReleasedAt(), cleaned)
	}
	if len(res.GetReferences()) != 0 {
		t.Fatalf("references after release = %v; want none", refRows(res))
	}
}

func TestGetArtifactProvenance_SharedObjectListsEveryRunOfTheNamespaceAndNoOther(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "identical bytes\n")
	a := f.newRun("ork-1-a", "payments", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	b := f.newRun("ork-1-b", "payments", t0.Add(time.Hour), provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	c := f.newRun("ork-1-c", "staging", t0.Add(2*time.Hour), provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	for _, run := range []*orkstackv1.Run{a, b, c} {
		f.archive(t, run)
	}

	res, err := f.ask("payments", a.GetId(), "completed", "report.txt")
	if err != nil {
		t.Fatalf("payments: %v", err)
	}
	if got, want := strings.Join(refRows(res), ","), "ork-1-b/completed/ARTIFACT_KIND_OUTPUT,ork-1-a/completed/ARTIFACT_KIND_OUTPUT"; got != want {
		t.Fatalf("payments references = %s; want %s (newest first)", got, want)
	}
	if res.GetReferencesUnreadable() != 0 {
		t.Fatalf("another namespace's markers leaked into a count: references_unreadable = %d", res.GetReferencesUnreadable())
	}
	if strings.Contains(res.String(), "staging") || strings.Contains(res.String(), c.GetId()) {
		t.Fatalf("payments answer discloses the staging run: %s", res.String())
	}

	res, err = f.ask("staging", c.GetId(), "completed", "report.txt")
	if err != nil {
		t.Fatalf("staging: %v", err)
	}
	if got := strings.Join(refRows(res), ","); got != "ork-1-c/completed/ARTIFACT_KIND_OUTPUT" {
		t.Fatalf("staging references = %s; want exactly ork-1-c", got)
	}
	if strings.Contains(res.String(), "payments") || res.GetReferencesUnreadable() != 0 {
		t.Fatalf("staging answer discloses payments: %s", res.String())
	}
}

func TestGetArtifactProvenance_ARunRecordingTheObjectUnderTwoTasksYieldsTwoRows(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "same\n")
	older := f.newRun("ork-1-a", "default", t0, provTask{id: "zeta", outputs: []*orkstackv1.ArtifactRef{report}})
	run := f.newRun("ork-1-b", "default", t0.Add(time.Minute),
		provTask{id: "zeta", outputs: []*orkstackv1.ArtifactRef{report}},
		provTask{id: "alpha", outputs: []*orkstackv1.ArtifactRef{report}},
	)
	f.archive(t, older)
	f.archive(t, run)

	res, err := f.ask("default", run.GetId(), "alpha", "report.txt")
	if err != nil {
		t.Fatalf("GetArtifactProvenance: %v", err)
	}
	want := "ork-1-b/zeta/ARTIFACT_KIND_OUTPUT,ork-1-b/alpha/ARTIFACT_KIND_OUTPUT,ork-1-a/zeta/ARTIFACT_KIND_OUTPUT"
	if got := strings.Join(refRows(res), ","); got != want {
		t.Fatalf("references = %s; want %s (Run.task_runs order within a run)", got, want)
	}
}

func TestGetArtifactProvenance_EqualCreationTimesTieBreakOnRunIDDescending(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "same\n")
	for _, id := range []string{"ork-1-a", "ork-1-c", "ork-1-b"} {
		f.archive(t, f.newRun(id, "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}}))
	}
	res, err := f.ask("default", "ork-1-a", "completed", "report.txt")
	if err != nil {
		t.Fatalf("GetArtifactProvenance: %v", err)
	}
	want := "ork-1-c/completed/ARTIFACT_KIND_OUTPUT,ork-1-b/completed/ARTIFACT_KIND_OUTPUT,ork-1-a/completed/ARTIFACT_KIND_OUTPUT"
	if got := strings.Join(refRows(res), ","); got != want {
		t.Fatalf("references = %s; want %s", got, want)
	}
}

func TestGetArtifactProvenance_MissingBytesAreReportedNotHidden(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	f.archive(t, run)
	if err := f.store.Delete(context.Background(), report.GetUri()); err != nil {
		t.Fatal(err)
	}

	res, err := f.ask("default", run.GetId(), "completed", "report.txt")
	if err != nil {
		t.Fatalf("GetArtifactProvenance: %v", err)
	}
	if res.GetBytesPresent() {
		t.Fatal("bytes_present = true after the object was deleted")
	}
	if res.GetOwnReference() != orkstackv1.ReferenceState_REFERENCE_STATE_HELD || len(res.GetReferences()) != 1 {
		t.Fatalf("missing bytes hid the reference: %s %v", res.GetOwnReference(), refRows(res))
	}
}

// statFails is a store that cannot answer Stat: an outage, not an absence.
type statFails struct {
	artifacts.Store
	calls *atomic.Int64
}

func (s statFails) Stat(context.Context, string) (artifacts.ArtifactRef, error) {
	if s.calls != nil {
		s.calls.Add(1)
	}
	return artifacts.ArtifactRef{}, errors.New("connection reset by peer")
}

func TestGetArtifactProvenance_StoreFailureIsUnavailableNotFalse(t *testing.T) {
	f := newProvenanceFixture(t, func(o *provenanceOptions) {
		o.store = func(inner artifacts.Store) artifacts.Store { return statFails{Store: inner} }
	})
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	f.archive(t, run)

	_, err := f.ask("default", run.GetId(), "completed", "report.txt")
	requireCode(t, err, connect.CodeUnavailable)
}

// writeMarker writes a bare reference marker, as a crash between the marker
// writes and the index publish would leave it.
func (f *provenanceFixture) writeMarker(t *testing.T, ref *orkstackv1.ArtifactRef, runID string) {
	t.Helper()
	objectID, ok := artifacts.ObjectIDFromURI(ref.GetUri())
	if !ok {
		t.Fatalf("ObjectIDFromURI(%s)", ref.GetUri())
	}
	key, err := artifacts.RunObjectRefKey(objectID, runID)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.archiveDir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestGetArtifactProvenance_MarkerWithoutABodyCountsAsUnreadable(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	f.archive(t, run)
	f.writeMarker(t, report, "ork-1-orphan")

	res, err := f.ask("default", run.GetId(), "completed", "report.txt")
	if err != nil {
		t.Fatalf("GetArtifactProvenance: %v", err)
	}
	if res.GetReferencesUnreadable() != 1 {
		t.Fatalf("references_unreadable = %d; want 1", res.GetReferencesUnreadable())
	}
	if got := strings.Join(refRows(res), ","); got != "ork-1-1/completed/ARTIFACT_KIND_OUTPUT" {
		t.Fatalf("references = %s; the orphan marker must be listed nowhere", got)
	}
}

// loadFails is the api archive seam whose body reads fail with an error that
// is not NotFound, while its index half still answers.
type loadFails struct{ *engine.RunArchiver }

func (loadFails) LoadRun(context.Context, string) (*orkstackv1.Run, error) {
	return nil, errors.New("slow down: request rate exceeded")
}

func TestGetArtifactProvenance_BodyReadFailureIsUnavailable(t *testing.T) {
	f := newProvenanceFixture(t, func(o *provenanceOptions) {
		o.archive = func(a *engine.RunArchiver) api.RunArchive { return loadFails{a} }
	})
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	other := f.newRun("ork-1-2", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	f.archive(t, run)
	f.archive(t, other)

	_, err := f.ask("default", run.GetId(), "completed", "report.txt")
	requireCode(t, err, connect.CodeUnavailable)
}

func TestGetArtifactProvenance_ExpectSha256MismatchIsFailedPrecondition(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	f.archive(t, run)
	recorded := strings.TrimPrefix(report.GetSha256(), "sha256:")
	other := strings.Repeat("0", 64)

	req := func(expect string) *orkstackv1.GetArtifactProvenanceRequest {
		return &orkstackv1.GetArtifactProvenanceRequest{
			RunId: run.GetId(), TaskId: "completed", Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT,
			Name: "report.txt", ExpectSha256: expect,
		}
	}
	_, err := f.askReq(req(other))
	requireCode(t, err, connect.CodeFailedPrecondition)
	if !strings.Contains(err.Error(), recorded) || !strings.Contains(err.Error(), other) {
		t.Fatalf("mismatch message %q does not name both digests", err.Error())
	}
	for _, expect := range []string{recorded, "sha256:" + recorded} {
		if _, err := f.askReq(req(expect)); err != nil {
			t.Fatalf("matching expect_sha256 %q: %v", expect, err)
		}
	}
	for _, malformed := range []string{"abc", strings.ToUpper(recorded), recorded + "0", "md5:" + recorded, "sha256:"} {
		_, err := f.askReq(req(malformed))
		requireCode(t, err, connect.CodeInvalidArgument)
	}
}

func TestGetArtifactProvenance_ScopingMatchesEveryOtherRunRead(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "payments", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	f.archive(t, run)
	output := orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT

	for _, tc := range []struct {
		name    string
		req     *orkstackv1.GetArtifactProvenanceRequest
		code    connect.Code
		message string
	}{
		{"another namespace's run", &orkstackv1.GetArtifactProvenanceRequest{RunId: run.GetId(), Namespace: "staging", TaskId: "completed", Kind: output, Name: "report.txt"}, connect.CodeNotFound, "not found in namespace"},
		{"the default namespace", &orkstackv1.GetArtifactProvenanceRequest{RunId: run.GetId(), TaskId: "completed", Kind: output, Name: "report.txt"}, connect.CodeNotFound, "not found in namespace"},
		{"unknown task", &orkstackv1.GetArtifactProvenanceRequest{RunId: run.GetId(), Namespace: "payments", TaskId: "nope", Kind: output, Name: "report.txt"}, connect.CodeNotFound, `task "nope" not found`},
		{"unrecorded name", &orkstackv1.GetArtifactProvenanceRequest{RunId: run.GetId(), Namespace: "payments", TaskId: "completed", Kind: output, Name: "other.txt"}, connect.CodeNotFound, "is not recorded for task"},
		{"unrecorded kind", &orkstackv1.GetArtifactProvenanceRequest{RunId: run.GetId(), Namespace: "payments", TaskId: "completed", Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_STDOUT, Name: "report.txt"}, connect.CodeNotFound, "is not recorded for task"},
		{"missing run_id", &orkstackv1.GetArtifactProvenanceRequest{Namespace: "payments", TaskId: "completed", Kind: output, Name: "report.txt"}, connect.CodeInvalidArgument, "run_id, task_id, kind and name are required"},
		{"missing task_id", &orkstackv1.GetArtifactProvenanceRequest{RunId: run.GetId(), Namespace: "payments", Kind: output, Name: "report.txt"}, connect.CodeInvalidArgument, "run_id, task_id, kind and name are required"},
		{"missing name", &orkstackv1.GetArtifactProvenanceRequest{RunId: run.GetId(), Namespace: "payments", TaskId: "completed", Kind: output}, connect.CodeInvalidArgument, "run_id, task_id, kind and name are required"},
		{"unspecified kind", &orkstackv1.GetArtifactProvenanceRequest{RunId: run.GetId(), Namespace: "payments", TaskId: "completed", Name: "report.txt"}, connect.CodeInvalidArgument, "run_id, task_id, kind and name are required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.askReq(tc.req)
			requireCode(t, err, tc.code)
			if !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("message %q does not contain %q", err.Error(), tc.message)
			}
		})
	}

	for _, tc := range []struct {
		name    string
		opt     func(*provenanceOptions)
		message string
	}{
		{"nil artifact store", func(o *provenanceOptions) { o.noStore = true }, "artifact store is not configured"},
		{"nil archive", func(o *provenanceOptions) { o.noArch = true }, "run archive is not configured"},
		{"archive without an index", func(o *provenanceOptions) {
			o.archive = func(a *engine.RunArchiver) api.RunArchive { return archiveOnly{a} }
		}, "run archive is not configured"},
		{"typed-nil archiver", func(o *provenanceOptions) {
			o.archive = func(*engine.RunArchiver) api.RunArchive { return (*engine.RunArchiver)(nil) }
		}, "run archive is not configured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newProvenanceFixture(t, tc.opt)
			ref := g.put(t, "report.txt", "hello\n")
			r := g.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{ref}})
			_, err := g.ask("default", r.GetId(), "completed", "report.txt")
			requireCode(t, err, connect.CodeFailedPrecondition)
			if !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("message %q does not contain %q", err.Error(), tc.message)
			}
		})
	}
}

// archiveOnly is an api.RunArchive with no provenance half: embedding the
// interface hides the archiver's other methods.
type archiveOnly struct{ api.RunArchive }

func TestGetArtifactProvenance_MoreReferencesThanTheBoundIsResourceExhausted(t *testing.T) {
	f := newProvenanceFixture(t)
	report := f.put(t, "report.txt", "hello\n")
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{report}})
	f.archive(t, run)
	for i := 0; i < artifacts.MaxObjectReferences; i++ {
		f.writeMarker(t, report, fmt.Sprintf("ork-2-%04d", i))
	}

	_, err := f.ask("default", run.GetId(), "completed", "report.txt")
	requireCode(t, err, connect.CodeResourceExhausted)
	if !strings.Contains(err.Error(), fmt.Sprint(artifacts.MaxObjectReferences)) {
		t.Fatalf("message %q does not name the bound %d", err.Error(), artifacts.MaxObjectReferences)
	}
}

func TestGetArtifactProvenance_NeverAcceptsAURI(t *testing.T) {
	// The request has no field that could carry one.
	fields := (&orkstackv1.GetArtifactProvenanceRequest{}).ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		if name := string(fields.Get(i).Name()); strings.Contains(name, "uri") || strings.Contains(name, "key") {
			t.Fatalf("GetArtifactProvenanceRequest has field %q; a caller never names an object", name)
		}
	}

	var stats atomic.Int64
	f := newProvenanceFixture(t, func(o *provenanceOptions) {
		o.store = func(inner artifacts.Store) artifacts.Store { return statFails{Store: inner, calls: &stats} }
	})
	run := f.newRun("ork-1-1", "default", t0, provTask{id: "completed", outputs: []*orkstackv1.ArtifactRef{
		{Name: "report.txt", Uri: "mem://report.txt", Sha256: strings.Repeat("a", 64), SizeBytes: 6},
	}})
	_, err := f.ask("default", run.GetId(), "completed", "report.txt")
	requireCode(t, err, connect.CodeFailedPrecondition)
	if !strings.Contains(err.Error(), "is not a content-addressed object") {
		t.Fatalf("message = %q", err.Error())
	}
	if stats.Load() != 0 {
		t.Fatalf("a non-content-addressed reference reached the store (%d Stat calls)", stats.Load())
	}
}
