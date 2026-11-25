package api_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/pkg/artifacts"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

var evidenceNow = time.Date(2026, 9, 22, 10, 51, 49, 0, time.UTC)

const evidenceSource = "name: evidence\nversion: 0.1.0\ntasks:\n  - id: t\n    uses: shell\n"

// countingStore wraps the local artifact store so a test can count reads and
// make one object answer differently than the bucket would.
type countingStore struct {
	artifacts.Store
	mu       sync.Mutex
	gets     int
	stats    int
	statErr  error
	getErr   error
	vanished map[string]bool
}

func (c *countingStore) Get(ctx context.Context, uri string) (io.ReadCloser, error) {
	c.mu.Lock()
	c.gets++
	getErr, gone := c.getErr, c.vanished[uri]
	c.mu.Unlock()
	if getErr != nil {
		return nil, getErr
	}
	if gone {
		return nil, fmt.Errorf("lifecycle rule ran: %w", artifacts.ErrNotFound)
	}
	return c.Store.Get(ctx, uri)
}

func (c *countingStore) Stat(ctx context.Context, uri string) (artifacts.ArtifactRef, error) {
	c.mu.Lock()
	c.stats++
	statErr := c.statErr
	c.mu.Unlock()
	if statErr != nil {
		return artifacts.ArtifactRef{}, statErr
	}
	return c.Store.Stat(ctx, uri)
}

func (c *countingStore) counts() (gets, stats int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets, c.stats
}

// missingCommits answers NotFound for the uris it names, as a store whose
// commit the cleanup worker reclaimed.
type missingCommits struct {
	artifacts.CommitStore
	gone map[string]bool
}

func (m missingCommits) Get(ctx context.Context, uri string) (artifacts.CommitRecord, error) {
	if m.gone[uri] {
		return artifacts.CommitRecord{}, fmt.Errorf("commit %s: %w", uri, artifacts.ErrNotFound)
	}
	return m.CommitStore.Get(ctx, uri)
}

// failingAttempts is an attempt store whose reads fail.
type failingAttempts struct{ *artifacts.MemoryAttemptStore }

func (failingAttempts) ReadAttemptDocuments(context.Context, string, string, artifacts.Item) ([]artifacts.AttemptDocument, error) {
	return nil, errors.New("bucket unreachable")
}

// listOnlyAttempts implements AttemptStore and nothing else.
type listOnlyAttempts struct{ artifacts.AttemptStore }

// swappedBody serves another run's archive body under every id.
type swappedBody struct {
	*engine.RunArchiver
	body []byte
}

func (s swappedBody) LoadRunBody(context.Context, string) ([]byte, error) { return s.body, nil }

type evidenceFixture struct {
	t        *testing.T
	svc      *api.Service
	client   orkstackv1connect.RunServiceClient
	reader   *fakeReader
	archiver *engine.RunArchiver
	local    *artifacts.LocalStore
	store    *countingStore
	attempts *artifacts.MemoryAttemptStore
	commits  *artifacts.MemoryCommitStore
	orkflows *engine.MemoryOrkflowStore
	digest   string
	clock    time.Time
}

type evidenceOptions struct {
	cfg func(*api.Config)
}

func newEvidenceFixture(t *testing.T, opts ...func(*evidenceOptions)) *evidenceFixture {
	t.Helper()
	var o evidenceOptions
	for _, opt := range opts {
		opt(&o)
	}
	archive, err := artifacts.NewLocalRunArchive(t.TempDir())
	must.OK(t, err)
	t.Cleanup(func() { _ = archive.Close() })
	local, err := artifacts.NewLocalStore(t.TempDir())
	must.OK(t, err)
	t.Cleanup(func() { _ = local.Close() })
	f := &evidenceFixture{
		t:        t,
		reader:   &fakeReader{},
		archiver: engine.NewRunArchiver(archive),
		local:    local,
		store:    &countingStore{Store: local, vanished: map[string]bool{}},
		attempts: artifacts.NewMemoryAttemptStore(),
		commits:  artifacts.NewMemoryCommitStore(),
		orkflows: engine.NewMemoryOrkflowStore(),
		clock:    evidenceNow,
	}
	wf := &orkstackv1.Orkflow{
		Namespace: "default", Name: "evidence", Version: "0.1.0", SourceYaml: evidenceSource,
		Tasks: []*orkstackv1.Task{{Id: "t", Uses: "shell"}},
	}
	f.digest, err = dsl.DefinitionDigest(wf)
	must.OK(t, err)
	wf.DefinitionDigest = f.digest
	if _, _, err := f.orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatal(err)
	}
	cfg := api.Config{
		Reader:        f.reader,
		Runs:          engine.NewRunStore(),
		Starter:       &fakeStarter{},
		Orkflows:      f.orkflows,
		Archive:       f.archiver,
		ArtifactStore: f.store,
		Attempts:      f.attempts,
		Commits:       f.commits,
		Namespaces: registry{
			"default":  {Name: "default"},
			"payments": {Name: "payments"},
		},
		System: api.SystemInfo{EngineVersion: "v0.9.3", BuildID: "2026.09.22-1"},
		Now:    func() time.Time { return f.clock },
	}
	if o.cfg != nil {
		o.cfg(&cfg)
	}
	f.svc = api.NewService(cfg)
	server := httptest.NewServer(api.NewMux(f.svc, api.MuxOptions{}))
	t.Cleanup(server.Close)
	f.client = orkstackv1connect.NewRunServiceClient(server.Client(), server.URL)
	return f
}

func withConfig(change func(*api.Config)) func(*evidenceOptions) {
	return func(o *evidenceOptions) { o.cfg = change }
}

// put stores content and returns the reference a task records.
func (f *evidenceFixture) put(name string, content []byte) *orkstackv1.ArtifactRef {
	f.t.Helper()
	ref, err := f.local.Put(context.Background(), name, bytes.NewReader(content), artifacts.PutOptions{})
	must.OKf(f.t, err, "Put(%s)", name)
	return &orkstackv1.ArtifactRef{Name: ref.Name, Uri: ref.URI, SizeBytes: ref.SizeBytes, Sha256: ref.SHA256, ContentType: ref.ContentType}
}

// commit creates the task's commit record and returns the CommitRef the
// snapshot carries.
func (f *evidenceFixture) commit(runID, taskID string, values *orkstackv1.ArtifactRef, outputs []*orkstackv1.ArtifactRef) *orkstackv1.CommitRef {
	f.t.Helper()
	uri := "mem://commits/metadata/commits/default/" + runID + "/" + taskID + "/commit.json"
	rec, err := f.commits.Create(context.Background(), uri, &orkstackv1.ActivityCommit{
		SchemaVersion: artifacts.CommitSchemaVersion, Namespace: "default", RunId: runID, TaskId: taskID,
		OrkflowDefinitionDigest: f.digest, Values: values, Artifacts: outputs,
	})
	must.OKf(f.t, err, "commit Create")
	return &orkstackv1.CommitRef{Uri: uri, Sha256: rec.SHA256, Etag: rec.ETag}
}

func (f *evidenceFixture) record(runID, taskID string, attempt int32, outcome orkstackv1.AttemptOutcome, adopted bool, logs ...*orkstackv1.ArtifactRef) {
	f.t.Helper()
	if err := f.attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion, RunId: runID, TaskId: taskID, Attempt: attempt,
		Outcome: outcome, CommitAdopted: adopted, SystemArtifacts: logs,
	}); err != nil {
		f.t.Fatalf("PutAttempt: %v", err)
	}
}

// finish records a finished run with the reader and archives it through the
// production archiver.
func (f *evidenceFixture) finish(run *orkstackv1.Run) *orkstackv1.Run {
	f.t.Helper()
	if run.OrkflowName == "" {
		run.OrkflowName, run.OrkflowVersion, run.OrkflowDefinitionDigest = "evidence", "0.1.0", f.digest
	}
	if run.Status == orkstackv1.Status_STATUS_UNSPECIFIED {
		run.Status = orkstackv1.Status_STATUS_SUCCEEDED
	}
	run.CreatedAt = timestamppb.New(evidenceNow.Add(-time.Hour))
	run.FinishedAt = timestamppb.New(evidenceNow.Add(-time.Hour + time.Minute))
	f.reader.setRun(run)
	if err := f.archiver.Archive(context.Background(), run, []*orkstackv1.Event{{
		RunId: run.GetId(), Sequence: 1, Type: orkstackv1.EventType_EVENT_TYPE_RUN_CREATED,
		Status: orkstackv1.Status_STATUS_PENDING, Timestamp: run.GetCreatedAt(),
	}}); err != nil {
		f.t.Fatalf("Archive: %v", err)
	}
	return run
}

// succeeded is H1's run: one task, attempt 1 failed with its own logs,
// attempt 2 succeeded with an artifact, values, logs and a commit.
type succeededRun struct {
	run                 *orkstackv1.Run
	report, values      *orkstackv1.ArtifactRef
	stdout1, stderr1    *orkstackv1.ArtifactRef
	stdout2, stderr2    *orkstackv1.ArtifactRef
	commit              *orkstackv1.CommitRef
	commitRaw, runJSONL []byte
	reportBytes         []byte
}

func (f *evidenceFixture) succeeded(runID string) succeededRun {
	f.t.Helper()
	s := succeededRun{reportBytes: []byte("report of " + runID + "\n")}
	s.report = f.put("report.txt", s.reportBytes)
	s.values = f.put("values.json", []byte(`{"run":"`+runID+`"}`))
	s.stdout1 = f.put("stdout.log", []byte("attempt 1 of "+runID+"\n"))
	s.stderr1 = f.put("stderr.log", []byte("attempt 1 failed "+runID+"\n"))
	s.stdout2 = f.put("stdout.log", []byte("attempt 2 of "+runID+"\n"))
	s.stderr2 = f.put("stderr.log", []byte("attempt 2 warned "+runID+"\n"))
	s.commit = f.commit(runID, "t", s.values, []*orkstackv1.ArtifactRef{s.report})
	rec, err := f.commits.Get(context.Background(), s.commit.GetUri())
	must.OK(f.t, err)
	s.commitRaw = rec.Raw
	f.record(runID, "t", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, false, s.stdout1, s.stderr1)
	f.record(runID, "t", 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, false, s.stdout2, s.stderr2)
	s.run = f.finish(&orkstackv1.Run{Id: runID, TaskRuns: []*orkstackv1.TaskRun{{
		TaskId: "t", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 2,
		Artifacts: []*orkstackv1.ArtifactRef{s.report}, ValuesRef: s.values,
		SystemArtifacts: []*orkstackv1.ArtifactRef{s.stdout2, s.stderr2}, Commit: s.commit,
	}}})
	body, err := f.archiver.LoadRunBody(context.Background(), runID)
	must.OK(f.t, err)
	s.runJSONL = body
	return s
}

// evidenceResult is one drained stream.
type evidenceResult struct {
	manifest *evidence.Manifest
	data     []byte
	chunks   []*orkstackv1.EvidenceChunk
	last     bool
	err      error
}

func (r evidenceResult) code() connect.Code { return connect.CodeOf(r.err) }

// collectEvidence drains one export. It never fails the test on a stream
// error; it returns it, with whatever arrived before it.
func (f *evidenceFixture) collectEvidence(req *orkstackv1.ExportRunEvidenceRequest) evidenceResult {
	f.t.Helper()
	var r evidenceResult
	stream, err := f.client.ExportRunEvidence(context.Background(), connect.NewRequest(req))
	if err != nil {
		r.err = err
		return r
	}
	defer stream.Close()
	for stream.Receive() {
		chunk := stream.Msg()
		r.chunks = append(r.chunks, chunk)
		if r.last {
			f.t.Fatal("a chunk arrived after last")
		}
		r.data = append(r.data, chunk.GetData()...)
		r.last = chunk.GetLast()
	}
	r.err = stream.Err()
	if r.err != nil {
		if r.last {
			f.t.Fatalf("stream carried last and then failed: %v", r.err)
		}
		return r
	}
	if !r.last {
		f.t.Fatal("stream ended without error and without last")
	}
	if req.GetManifestOnly() {
		r.manifest, err = evidence.ParseManifest(r.data)
	} else {
		r.manifest, err = evidence.ReadManifest(bytes.NewReader(r.data))
	}
	must.OKf(f.t, err, "manifest")
	return r
}

func (f *evidenceFixture) mustExport(req *orkstackv1.ExportRunEvidenceRequest) evidenceResult {
	f.t.Helper()
	r := f.collectEvidence(req)
	if r.err != nil {
		f.t.Fatalf("ExportRunEvidence: %v", r.err)
	}
	return r
}

func memberAt(t *testing.T, m *evidence.Manifest, path string) evidence.Member {
	t.Helper()
	for _, member := range m.Members {
		if member.Path == path {
			return member
		}
	}
	t.Fatalf("manifest has no member %s; members: %v", path, memberPaths(m))
	return evidence.Member{}
}

func memberPaths(m *evidence.Manifest) []string {
	paths := make([]string, 0, len(m.Members))
	for _, member := range m.Members {
		paths = append(paths, member.Path+"="+string(member.State))
	}
	return paths
}

func hasMember(m *evidence.Manifest, path string) bool {
	for _, member := range m.Members {
		if member.Path == path {
			return true
		}
	}
	return false
}

// tarMembers returns the bundle's entries by name.
func tarMembers(t *testing.T, bundle []byte) map[string][]byte {
	t.Helper()
	entries, err := evidenceEntries(bundle)
	must.OK(t, err)
	return entries
}

func verifyBundle(t *testing.T, bundle []byte) *evidence.Report {
	t.Helper()
	report, err := evidence.Verify(bytes.NewReader(bundle))
	must.OKf(t, err, "Verify")
	if len(report.Failures) != 0 {
		t.Fatalf("Verify failures: %+v", report.Failures)
	}
	return report
}

// H1
func TestExportRunEvidence_FullBundleOfASucceededRun(t *testing.T) {
	f := newEvidenceFixture(t)
	s := f.succeeded("ork-ev-1")
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-1"})
	m := r.manifest

	want := []string{
		"run.jsonl", "orkflow/source.yaml", "tasks/t/commit.json", "tasks/t/values.json",
		"tasks/t/artifacts/report.txt",
		"tasks/t/attempts/1.json", "tasks/t/attempts/1/stdout.log", "tasks/t/attempts/1/stderr.log",
		"tasks/t/attempts/2.json", "tasks/t/attempts/2/stdout.log", "tasks/t/attempts/2/stderr.log",
	}
	if len(m.Members) != len(want) {
		t.Fatalf("members = %v, want %v", memberPaths(m), want)
	}
	for i, path := range want {
		if m.Members[i].Path != path || m.Members[i].State != evidence.StatePresent {
			t.Errorf("member %d = %s (%s), want %s present", i, m.Members[i].Path, m.Members[i].State, path)
		}
	}
	if m.Counts.Present != 11 {
		t.Fatalf("counts.present = %d, want 11", m.Counts.Present)
	}
	report := verifyBundle(t, r.data)
	if len(report.Verified) != 11 {
		t.Fatalf("verified %d, want 11", len(report.Verified))
	}
	t.Logf("H1 verified %d present members against MANIFEST.json", len(report.Verified))

	entries := tarMembers(t, r.data)
	if !bytes.Equal(entries["run.jsonl"], s.runJSONL) {
		t.Error("run.jsonl is not LoadRunBody")
	}
	if string(entries["orkflow/source.yaml"]) != evidenceSource {
		t.Error("orkflow/source.yaml is not the stored source_yaml")
	}
	if !bytes.Equal(entries["tasks/t/commit.json"], s.commitRaw) {
		t.Error("commit.json is not rec.Raw")
	}
	if !bytes.Equal(entries["tasks/t/artifacts/report.txt"], s.reportBytes) {
		t.Error("report.txt differs")
	}
	if commit := memberAt(t, m, "tasks/t/commit.json"); commit.SHA256 != s.commit.GetSha256() || commit.Origin != s.commit.GetUri() {
		t.Errorf("commit member = %+v, want the CommitRef's digest and uri", commit)
	}
	if artifact := memberAt(t, m, "tasks/t/artifacts/report.txt"); artifact.Name != "report.txt" || artifact.Origin != s.report.GetUri() ||
		artifact.SHA256 != evidence.NormalizeDigest(s.report.GetSha256()) {
		t.Errorf("artifact member = %+v", artifact)
	}
	if !m.ExportedAt.Equal(evidenceNow) || m.EngineVersion != "v0.9.3" || m.BuildID != "2026.09.22-1" {
		t.Errorf("exported_at %v engine %q build %q", m.ExportedAt, m.EngineVersion, m.BuildID)
	}
	if m.RunID != "ork-ev-1" || m.Namespace != "default" || m.DefinitionDigest != f.digest || m.RunStatus != "SUCCEEDED" ||
		m.Definition.State != evidence.StatePresent || !m.Scope.Logs || !m.Scope.Artifacts {
		t.Errorf("manifest header = %+v", m)
	}
	if len(m.Tasks) != 1 || m.Tasks[0].OutcomeAttempt != 2 || len(m.Tasks[0].Attempts) != 2 ||
		m.Tasks[0].Attempts[0].Outcome != "FAILED" || m.Tasks[0].Attempts[1].Outcome != "SUCCEEDED" {
		t.Errorf("tasks = %+v", m.Tasks)
	}
}

// H2
func TestExportRunEvidence_ManifestOnlyReadsNoLargeObject(t *testing.T) {
	f := newEvidenceFixture(t)
	f.succeeded("ork-ev-2")
	full := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-2"})

	f.clock = evidenceNow.Add(time.Hour)
	beforeGets, beforeStats := f.store.counts()
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-2", ManifestOnly: true})
	gets, stats := f.store.counts()
	if gets-beforeGets != 0 {
		t.Fatalf("manifest_only read %d objects, want 0", gets-beforeGets)
	}
	if stats-beforeStats != 6 {
		t.Fatalf("manifest_only stat'ed %d objects, want 6 (values, artifact, four logs)", stats-beforeStats)
	}
	t.Logf("H2 manifest_only: %d Get calls, %d Stat calls", gets-beforeGets, stats-beforeStats)
	document, err := r.manifest.MarshalJSON()
	must.OK(t, err)
	if !bytes.Equal(r.data, document) {
		t.Fatal("the manifest_only stream is not exactly the MANIFEST.json document")
	}
	if last := r.chunks[len(r.chunks)-1]; !last.GetLast() || len(last.GetData()) != 0 {
		t.Fatal("the stream must end with an empty last chunk")
	}
	if !r.manifest.ExportedAt.Equal(f.clock) {
		t.Fatalf("exported_at = %v", r.manifest.ExportedAt)
	}
	fullManifest := *full.manifest
	fullManifest.ExportedAt = r.manifest.ExportedAt
	a, _ := fullManifest.MarshalJSON()
	if !bytes.Equal(a, document) {
		t.Fatalf("manifest_only differs from the full bundle's manifest beyond exported_at:\n%s\n---\n%s", a, document)
	}
}

// H3
func TestExportRunEvidence_ReclaimedMembersKeepTheirRecordedDigest(t *testing.T) {
	var commits *missingCommits
	f := newEvidenceFixture(t, withConfig(func(cfg *api.Config) {
		commits = &missingCommits{CommitStore: cfg.Commits, gone: map[string]bool{}}
		cfg.Commits = commits
	}))
	s := f.succeeded("ork-ev-3")
	ctx := context.Background()
	for _, ref := range []*orkstackv1.ArtifactRef{s.report, s.stdout2} {
		must.OK(t, f.local.Delete(ctx, ref.GetUri()))
	}
	commits.gone[s.commit.GetUri()] = true

	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-3"})
	for _, c := range []struct {
		path, reason, sha string
	}{
		{"tasks/t/artifacts/report.txt", evidence.ReasonObjectMissing, s.report.GetSha256()},
		{"tasks/t/attempts/2/stdout.log", evidence.ReasonObjectMissing, s.stdout2.GetSha256()},
		{"tasks/t/commit.json", evidence.ReasonCommitMissing, s.commit.GetSha256()},
	} {
		m := memberAt(t, r.manifest, c.path)
		if m.State != evidence.StateReclaimed || m.Reason != c.reason || m.SHA256 != evidence.NormalizeDigest(c.sha) {
			t.Errorf("%s = %+v, want reclaimed %s with sha256 %s", c.path, m, c.reason, c.sha)
		}
		t.Logf("H3 %s reclaimed (%s) sha256 %s", c.path, m.Reason, m.SHA256)
		if _, in := tarMembers(t, r.data)[c.path]; in {
			t.Errorf("%s is in the tar", c.path)
		}
	}
	report := verifyBundle(t, r.data)
	if len(report.Reclaimed) != 3 || r.manifest.Counts.Reclaimed != 3 {
		t.Fatalf("reclaimed = %d (counts %d), want 3", len(report.Reclaimed), r.manifest.Counts.Reclaimed)
	}
}

// H4
func TestExportRunEvidence_UnrecordedAttemptsAreListed(t *testing.T) {
	f := newEvidenceFixture(t)
	out1 := f.put("stdout.log", []byte("one\n"))
	out3 := f.put("stdout.log", []byte("three\n"))
	f.record("ork-ev-4", "t", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, false, out1)
	f.record("ork-ev-4", "t", 3, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, false, out3)
	f.finish(&orkstackv1.Run{Id: "ork-ev-4", Status: orkstackv1.Status_STATUS_FAILED, TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "t", Status: orkstackv1.Status_STATUS_FAILED, Attempt: 3, SystemArtifacts: []*orkstackv1.ArtifactRef{out3}},
		{TaskId: "never", Status: orkstackv1.Status_STATUS_FAILED, Attempt: 0},
	}})
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-4"})
	if m := memberAt(t, r.manifest, "tasks/t/attempts/2.json"); m.State != evidence.StateUnrecorded || m.Reason != evidence.ReasonAttemptRecordMissing || m.SHA256 != "" {
		t.Fatalf("attempt 2 = %+v", m)
	}
	if got := r.manifest.Tasks[0].Attempts; len(got) != 3 || got[1].State != evidence.StateUnrecorded {
		t.Fatalf("tasks[0].attempts = %+v", got)
	}
	for _, path := range []string{"tasks/t/attempts/2/stdout.log", "tasks/t/attempts/2/stderr.log"} {
		if hasMember(r.manifest, path) {
			t.Errorf("unrecorded attempt 2 has a log member %s", path)
		}
	}
	for _, member := range r.manifest.Members {
		if member.TaskID == "never" {
			t.Errorf("a task with attempt 0 and no records has member %s", member.Path)
		}
	}
	if r.manifest.Tasks[1].TaskID != "never" || len(r.manifest.Tasks[1].Attempts) != 0 {
		t.Errorf("tasks[1] = %+v", r.manifest.Tasks[1])
	}
	verifyBundle(t, r.data)
}

// H5
func TestExportRunEvidence_OutcomeAttemptLogsComeFromTheSnapshot(t *testing.T) {
	f := newEvidenceFixture(t)
	out := f.put("stdout.log", []byte("committed by attempt 1\n"))
	values := f.put("values.json", []byte(`{}`))
	commit := f.commit("ork-ev-5", "t", values, nil)
	f.record("ork-ev-5", "t", 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, true, out)
	f.finish(&orkstackv1.Run{Id: "ork-ev-5", TaskRuns: []*orkstackv1.TaskRun{{
		TaskId: "t", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, ValuesRef: values, Commit: commit,
		SystemArtifacts: []*orkstackv1.ArtifactRef{out},
	}}})
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-5"})
	if m := memberAt(t, r.manifest, "tasks/t/attempts/1.json"); m.State != evidence.StateUnrecorded {
		t.Fatalf("attempt 1 record = %+v, want unrecorded", m)
	}
	first := memberAt(t, r.manifest, "tasks/t/attempts/1/stdout.log")
	second := memberAt(t, r.manifest, "tasks/t/attempts/2/stdout.log")
	if first.State != evidence.StatePresent || second.State != evidence.StatePresent || first.SHA256 != second.SHA256 {
		t.Fatalf("attempt 1 stdout %+v, attempt 2 stdout %+v", first, second)
	}
	attempts := r.manifest.Tasks[0].Attempts
	if len(attempts) != 2 || attempts[0].State != evidence.StateUnrecorded || !attempts[1].CommitAdopted || attempts[1].Outcome != "SUCCEEDED" {
		t.Fatalf("tasks[0].attempts = %+v", attempts)
	}
	t.Logf("H5 attempt 1 %s, attempt 2 commit_adopted: %v, both stdout.log sha256 %s",
		attempts[0].State, attempts[1].CommitAdopted, first.SHA256)
	verifyBundle(t, r.data)
}

// H6
func TestExportRunEvidence_DefinitionDeletedAndMismatchAreDistinct(t *testing.T) {
	t.Run("deleted", func(t *testing.T) {
		f := newEvidenceFixture(t)
		f.succeeded("ork-ev-6a")
		must.OK(t, f.orkflows.Delete(context.Background(), "default", "evidence"))
		r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-6a"})
		m := memberAt(t, r.manifest, "orkflow/source.yaml")
		if m.State != evidence.StateReclaimed || m.Reason != evidence.ReasonDefinitionDeleted || m.SHA256 != "" || m.SizeBytes != 0 ||
			r.manifest.Definition.State != evidence.StateReclaimed || r.manifest.Definition.Reason != evidence.ReasonDefinitionDeleted {
			t.Fatalf("source = %+v, definition = %+v", m, r.manifest.Definition)
		}
		verifyBundle(t, r.data)
	})
	t.Run("mismatch", func(t *testing.T) {
		f := newEvidenceFixture(t)
		s := f.succeeded("ork-ev-6b")
		pinned := "sha256:" + strings.Repeat("0", 64)
		// A run that pinned another digest under the same coordinates: the
		// stored document is not that run's definition.
		s.run.OrkflowDefinitionDigest = pinned
		s.run.Id = "ork-ev-6c"
		f.finish(s.run)
		r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-6c"})
		m := memberAt(t, r.manifest, "orkflow/source.yaml")
		if m.State != evidence.StateRejected || m.Reason != evidence.ReasonDefinitionDigestMismatch ||
			r.manifest.Definition.StoredDefinitionDigest != f.digest || r.manifest.DefinitionDigest != pinned {
			t.Fatalf("source = %+v, definition = %+v", m, r.manifest.Definition)
		}
		if _, in := tarMembers(t, r.data)["orkflow/source.yaml"]; in {
			t.Fatal("a rejected definition is in the tar")
		}
		verifyBundle(t, r.data)
	})
	t.Run("present", func(t *testing.T) {
		f := newEvidenceFixture(t)
		f.succeeded("ork-ev-6d")
		r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-6d"})
		if string(tarMembers(t, r.data)["orkflow/source.yaml"]) != evidenceSource {
			t.Fatal("source.yaml differs from the stored source")
		}
	})
}

// H7
// A child run of another namespace's shared Orkflow pinned its owner's
// document: the bundle holds that one, never the running namespace's own
// Orkflow of the same name, and says it is deleted only when the owner's is.
func TestExportRunEvidence_SharedChildHoldsItsOwnersDefinition(t *testing.T) {
	setUp := func(t *testing.T, runID string) (*evidenceFixture, string) {
		f := newEvidenceFixture(t)
		store := func(namespace, uses string) string {
			wf := &orkstackv1.Orkflow{Namespace: namespace, Name: "build", Version: "1.0.0", SourceYaml: "# of " + namespace + "\n",
				Tasks: []*orkstackv1.Task{{Id: "t", Uses: uses}}}
			digest, err := dsl.DefinitionDigest(wf)
			must.OK(t, err)
			wf.DefinitionDigest = digest
			_, _, err = f.orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation())
			must.OK(t, err)
			return digest
		}
		owners, own := store("platform", "shell"), store("payments", "http")
		if owners == own {
			t.Fatal("the two documents hash alike: the test would prove nothing")
		}
		s := f.succeeded(runID)
		s.run.Namespace, s.run.OrkflowNamespace = "payments", "platform"
		s.run.OrkflowName, s.run.OrkflowVersion, s.run.OrkflowDefinitionDigest = "build", "1.0.0", owners
		s.run.Id = runID + "-child"
		f.finish(s.run)
		return f, s.run.GetId()
	}
	t.Run("present", func(t *testing.T) {
		f, id := setUp(t, "ork-ev-s1")
		r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: id, Namespace: "payments"})
		if got := string(tarMembers(t, r.data)["orkflow/source.yaml"]); got != "# of platform\n" || r.manifest.Definition.State != evidence.StatePresent {
			t.Fatalf("source = %q, definition = %+v; want the owner's document", got, r.manifest.Definition)
		}
		if r.manifest.OrkflowNamespace != "platform" || r.manifest.Namespace != "payments" {
			t.Fatalf("manifest names %s's Orkflow run in %s", r.manifest.OrkflowNamespace, r.manifest.Namespace)
		}
		if m := memberAt(t, r.manifest, "orkflow/source.yaml"); m.Origin != "metadata/orkflows/platform/build/1.0.0.json#source_yaml" {
			t.Fatalf("origin = %q, want the owner's document", m.Origin)
		}
		verifyBundle(t, r.data)
	})
	t.Run("deleted by its owner", func(t *testing.T) {
		f, id := setUp(t, "ork-ev-s2")
		must.OK(t, f.orkflows.Delete(context.Background(), "platform", "build"))
		r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: id, Namespace: "payments"})
		if r.manifest.Definition.Reason != evidence.ReasonDefinitionDeleted {
			t.Fatalf("definition = %+v, want definition_deleted", r.manifest.Definition)
		}
	})
}

func TestExportRunEvidence_MismatchedBytesAbortWithoutLast(t *testing.T) {
	t.Run("artifact", func(t *testing.T) {
		f := newEvidenceFixture(t)
		s := f.succeeded("ork-ev-7")
		// Large and incompressible, so chunks leave before the digest is
		// checked at the end of the member.
		planted := make([]byte, 256<<10)
		if _, err := rand.Read(planted); err != nil {
			t.Fatal(err)
		}
		other := f.put("report.txt", planted)
		s.run.TaskRuns[0].Artifacts = []*orkstackv1.ArtifactRef{{Name: "report.txt", Uri: other.GetUri(),
			SizeBytes: other.GetSizeBytes(), Sha256: strings.Repeat("ab", 32)}}
		s.run.Id = "ork-ev-7b"
		f.finish(s.run)
		f.record("ork-ev-7b", "t", 2, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, false)
		r := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-7b"})
		if r.code() != connect.CodeDataLoss || r.last {
			t.Fatalf("stream ended %v (last %v), want DataLoss without last", r.err, r.last)
		}
		if len(r.data) == 0 {
			t.Fatal("no chunk left before the mismatch was found")
		}
		sent, err := evidence.ReadManifest(bytes.NewReader(r.data))
		must.OKf(t, err, "the manifest chunk already sent does not read")
		if m := memberAt(t, sent, "tasks/t/artifacts/report.txt"); m.State != evidence.StatePresent {
			t.Fatalf("the manifest already sent said %s", m.State)
		}
		t.Logf("H7 %v; last=%v after %d bytes", r.err, r.last, len(r.data))
	})
	t.Run("commit", func(t *testing.T) {
		f := newEvidenceFixture(t)
		s := f.succeeded("ork-ev-7c")
		different, err := artifacts.MarshalCommit(&orkstackv1.ActivityCommit{SchemaVersion: 1, RunId: "tampered", TaskId: "t"})
		must.OK(t, err)
		f.commits.PutRaw(s.commit.GetUri(), different)
		r := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-7c"})
		if r.code() != connect.CodeDataLoss || len(r.chunks) != 0 {
			t.Fatalf("stream ended %v after %d chunks, want DataLoss before any chunk", r.err, len(r.chunks))
		}
		f.commits.PutRaw(s.commit.GetUri(), []byte("{torn"))
		if r := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-7c"}); r.code() != connect.CodeDataLoss ||
			!strings.Contains(r.err.Error(), "undecodable") {
			t.Fatalf("undecodable commit = %v, want DataLoss", r.err)
		}
	})
}

// H8
func TestExportRunEvidence_StoreErrorsAreUnavailableNeverReclaimed(t *testing.T) {
	cases := []struct {
		name    string
		opt     func(*evidenceOptions)
		arrange func(*evidenceFixture)
		chunks  bool
	}{
		{name: "stat", arrange: func(f *evidenceFixture) { f.store.statErr = errors.New("connection reset") }},
		{name: "get", arrange: func(f *evidenceFixture) { f.store.getErr = errors.New("connection reset") }},
		{name: "attempts", opt: withConfig(func(cfg *api.Config) {
			cfg.Attempts = failingAttempts{cfg.Attempts.(*artifacts.MemoryAttemptStore)}
		})},
		{name: "orkflows", opt: withConfig(func(cfg *api.Config) {
			cfg.Orkflows = failingOrkflows{OrkflowStore: cfg.Orkflows, err: errors.New("bucket unreachable")}
		})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var opts []func(*evidenceOptions)
			if tc.opt != nil {
				opts = append(opts, tc.opt)
			}
			f := newEvidenceFixture(t, opts...)
			f.succeeded("ork-ev-8")
			if tc.arrange != nil {
				tc.arrange(f)
			}
			r := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-8"})
			if r.code() != connect.CodeUnavailable || r.last {
				t.Fatalf("%s: %v (last %v), want Unavailable without last", tc.name, r.err, r.last)
			}
			if tc.name != "get" && len(r.chunks) != 0 {
				t.Fatalf("%s: %d chunks were sent before the store error", tc.name, len(r.chunks))
			}
			t.Logf("H8 %s: %v", tc.name, r.err)
		})
	}
}

// H9
func TestExportRunEvidence_VanishedDuringStreamIsUnavailable(t *testing.T) {
	f := newEvidenceFixture(t)
	s := f.succeeded("ork-ev-9")
	f.store.vanished[s.report.GetUri()] = true
	r := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-9"})
	if r.code() != connect.CodeUnavailable || r.last ||
		!strings.Contains(r.err.Error(), `artifact "report.txt" of task "t" disappeared during export`) {
		t.Fatalf("stream ended %v (last %v)", r.err, r.last)
	}
	t.Logf("H9 %v", r.err)
}

// H10
func TestExportRunEvidence_ScopingAndRefusals(t *testing.T) {
	notDocuments := func(cfg *api.Config) { cfg.Attempts = listOnlyAttempts{cfg.Attempts} }
	cases := []struct {
		name    string
		cfg     func(*api.Config)
		arrange func(*evidenceFixture) *orkstackv1.ExportRunEvidenceRequest
		code    connect.Code
		message string
	}{
		{"empty run id", nil, func(*evidenceFixture) *orkstackv1.ExportRunEvidenceRequest {
			return &orkstackv1.ExportRunEvidenceRequest{}
		}, connect.CodeInvalidArgument, "run_id is required"},
		{"another namespace", nil, func(f *evidenceFixture) *orkstackv1.ExportRunEvidenceRequest {
			f.succeeded("ork-ev-10")
			return &orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-10", Namespace: "payments"}
		}, connect.CodeNotFound, "not found in namespace"},
		{"unknown task", nil, func(f *evidenceFixture) *orkstackv1.ExportRunEvidenceRequest {
			f.succeeded("ork-ev-10")
			return &orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-10", TaskId: "nope"}
		}, connect.CodeNotFound, `task "nope" not found in run "ork-ev-10"`},
		{"running", nil, func(f *evidenceFixture) *orkstackv1.ExportRunEvidenceRequest {
			f.reader.setRun(&orkstackv1.Run{Id: "ork-live", OrkflowName: "evidence", Status: orkstackv1.Status_STATUS_RUNNING})
			return &orkstackv1.ExportRunEvidenceRequest{RunId: "ork-live"}
		}, connect.CodeFailedPrecondition, `run "ork-live" is RUNNING; evidence is exported for finished runs only`},
		{"not yet archived", nil, func(f *evidenceFixture) *orkstackv1.ExportRunEvidenceRequest {
			f.reader.setRun(&orkstackv1.Run{Id: "ork-done", OrkflowName: "evidence", Status: orkstackv1.Status_STATUS_SUCCEEDED})
			return &orkstackv1.ExportRunEvidenceRequest{RunId: "ork-done"}
		}, connect.CodeFailedPrecondition, "not yet archived"},
		{"nil archive", func(cfg *api.Config) { cfg.Archive = nil }, nil, connect.CodeFailedPrecondition, "run archive is not configured"},
		{"nil artifact store", func(cfg *api.Config) { cfg.ArtifactStore = nil }, nil, connect.CodeFailedPrecondition, "artifact store is not configured"},
		{"nil attempts", func(cfg *api.Config) { cfg.Attempts = nil }, nil, connect.CodeFailedPrecondition, "task attempt store is not configured"},
		{"attempts without documents", notDocuments, nil, connect.CodeFailedPrecondition, "task attempt store is not configured"},
		{"nil commits", func(cfg *api.Config) { cfg.Commits = nil }, nil, connect.CodeFailedPrecondition, "commit store is not configured"},
		{"foreign archive body", nil, func(f *evidenceFixture) *orkstackv1.ExportRunEvidenceRequest {
			f.succeeded("ork-ev-10")
			f.succeeded("ork-ev-11")
			other, err := f.archiver.LoadRunBody(context.Background(), "ork-ev-11")
			must.OK(t, err)
			swapped := api.NewService(api.Config{
				Reader: f.reader, Runs: engine.NewRunStore(), Starter: &fakeStarter{}, Orkflows: f.orkflows,
				Archive: swappedBody{RunArchiver: f.archiver, body: other}, ArtifactStore: f.store,
				Attempts: f.attempts, Commits: f.commits,
			})
			server := httptest.NewServer(api.NewMux(swapped, api.MuxOptions{}))
			t.Cleanup(server.Close)
			f.client = orkstackv1connect.NewRunServiceClient(server.Client(), server.URL)
			return &orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-10"}
		}, connect.CodeDataLoss, `archive body of run "ork-ev-10" does not decode as that run`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var opts []func(*evidenceOptions)
			if tc.cfg != nil {
				opts = append(opts, withConfig(tc.cfg))
			}
			f := newEvidenceFixture(t, opts...)
			req := &orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-10"}
			if tc.arrange != nil {
				req = tc.arrange(f)
			} else {
				f.succeeded("ork-ev-10")
			}
			r := f.collectEvidence(req)
			if r.code() != tc.code || !strings.Contains(r.err.Error(), tc.message) {
				t.Fatalf("%v, want %v containing %q", r.err, tc.code, tc.message)
			}
			if len(r.chunks) != 0 {
				t.Fatalf("%d chunks were sent before the refusal", len(r.chunks))
			}
		})
	}
}

// H11
func TestExportRunEvidence_TaskAndExcludeFiltersNarrowTheMembers(t *testing.T) {
	f := newEvidenceFixture(t)
	run := &orkstackv1.Run{Id: "ork-ev-11"}
	for _, id := range []string{"a", "b"} {
		out := f.put("out-"+id+".txt", []byte("output of "+id))
		values := f.put("values.json", []byte(`{"task":"`+id+`"}`))
		stdout := f.put("stdout.log", []byte("stdout of "+id))
		f.record("ork-ev-11", id, 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, false, stdout)
		run.TaskRuns = append(run.TaskRuns, &orkstackv1.TaskRun{
			TaskId: id, Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, Artifacts: []*orkstackv1.ArtifactRef{out},
			ValuesRef: values, SystemArtifacts: []*orkstackv1.ArtifactRef{stdout}, Commit: f.commit("ork-ev-11", id, values, nil),
		})
	}
	f.finish(run)

	onlyB := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-11", TaskId: "b"})
	if onlyB.manifest.Scope.TaskID != "b" || len(onlyB.manifest.Tasks) != 1 || onlyB.manifest.Tasks[0].TaskID != "b" {
		t.Fatalf("scope %+v tasks %+v", onlyB.manifest.Scope, onlyB.manifest.Tasks)
	}
	for _, m := range onlyB.manifest.Members {
		if m.TaskID != "" && m.TaskID != "b" {
			t.Errorf("task_id b carries %s", m.Path)
		}
	}
	if !hasMember(onlyB.manifest, "run.jsonl") || !hasMember(onlyB.manifest, "orkflow/source.yaml") {
		t.Error("the run's own members are always included")
	}

	noLogs := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-11", ExcludeLogs: true})
	for _, m := range noLogs.manifest.Members {
		if m.Kind == evidence.KindStdout || m.Kind == evidence.KindStderr {
			t.Errorf("exclude_logs carries %s", m.Path)
		}
	}
	if noLogs.manifest.Scope.Logs {
		t.Error("scope.logs must be false")
	}

	noArtifacts := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-11", ExcludeArtifacts: true})
	for _, m := range noArtifacts.manifest.Members {
		if m.Kind == evidence.KindArtifact {
			t.Errorf("exclude_artifacts carries %s", m.Path)
		}
	}
	if noArtifacts.manifest.Scope.Artifacts || !hasMember(noArtifacts.manifest, "tasks/a/values.json") ||
		!hasMember(noArtifacts.manifest, "tasks/a/commit.json") {
		t.Error("exclude_artifacts keeps values and commit")
	}
	verifyBundle(t, noArtifacts.data)
}

// H12
func TestExportRunEvidence_FailedAndSkippedTasksHaveTheRightMembers(t *testing.T) {
	f := newEvidenceFixture(t)
	partial := f.put("partial.csv", []byte("a,b\n"))
	stdout := f.put("stdout.log", []byte("boom\n"))
	stderr := f.put("stderr.log", []byte("exit 1\n"))
	f.record("ork-ev-12", "broken", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, false, stdout, stderr)
	f.record("ork-ev-12", "stopped", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_CANCELED, false)
	f.finish(&orkstackv1.Run{Id: "ork-ev-12", Status: orkstackv1.Status_STATUS_FAILED, TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "broken", Status: orkstackv1.Status_STATUS_FAILED, Attempt: 1, ExitCode: 1,
			Artifacts: []*orkstackv1.ArtifactRef{partial}, SystemArtifacts: []*orkstackv1.ArtifactRef{stdout, stderr}},
		{TaskId: "later", Status: orkstackv1.Status_STATUS_SKIPPED},
		{TaskId: "stopped", Status: orkstackv1.Status_STATUS_CANCELED, Attempt: 1},
	}})
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-12"})
	byTask := map[string][]string{}
	for _, m := range r.manifest.Members {
		byTask[m.TaskID] = append(byTask[m.TaskID], m.Path)
	}
	want := map[string][]string{
		"broken":  {"tasks/broken/artifacts/partial.csv", "tasks/broken/attempts/1.json", "tasks/broken/attempts/1/stdout.log", "tasks/broken/attempts/1/stderr.log"},
		"stopped": {"tasks/stopped/attempts/1.json"},
	}
	for task, paths := range want {
		if strings.Join(byTask[task], ",") != strings.Join(paths, ",") {
			t.Errorf("%s members = %v, want %v", task, byTask[task], paths)
		}
	}
	if len(byTask["later"]) != 0 {
		t.Errorf("a skipped task has members %v", byTask["later"])
	}
	if len(r.manifest.Tasks) != 3 || r.manifest.Tasks[1].TaskID != "later" || r.manifest.Tasks[1].Status != "SKIPPED" ||
		r.manifest.Tasks[0].ExitCode != 1 {
		t.Fatalf("tasks = %+v", r.manifest.Tasks)
	}
	verifyBundle(t, r.data)
}

// H13
func TestExportRunEvidence_ArtifactNamesAreEncodedAndRecorded(t *testing.T) {
	f := newEvidenceFixture(t)
	odd := f.put("sub/dir.txt", []byte("nested\n"))
	f.finish(&orkstackv1.Run{Id: "ork-ev-13", TaskRuns: []*orkstackv1.TaskRun{{
		TaskId: "t", Status: orkstackv1.Status_STATUS_SUCCEEDED, Artifacts: []*orkstackv1.ArtifactRef{odd},
	}}})
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-13"})
	m := memberAt(t, r.manifest, "tasks/t/artifacts/~c3ViL2Rpci50eHQ")
	if m.Name != "sub/dir.txt" {
		t.Fatalf("name = %q", m.Name)
	}
	verifyBundle(t, r.data)
}

// H14
func TestExportRunEvidence_ChunksAreAtMost64KiBAndLastIsEmpty(t *testing.T) {
	f := newEvidenceFixture(t)
	big := make([]byte, 200<<10)
	if _, err := rand.Read(big); err != nil {
		t.Fatal(err)
	}
	ref := f.put("big.bin", big)
	f.finish(&orkstackv1.Run{Id: "ork-ev-14", TaskRuns: []*orkstackv1.TaskRun{{
		TaskId: "t", Status: orkstackv1.Status_STATUS_SUCCEEDED, Artifacts: []*orkstackv1.ArtifactRef{ref},
	}}})
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-14"})
	if len(r.chunks) < 4 {
		t.Fatalf("%d chunks for a 200 KiB artifact", len(r.chunks))
	}
	for i, chunk := range r.chunks {
		if len(chunk.GetData()) > 65536 {
			t.Errorf("chunk %d is %d bytes", i, len(chunk.GetData()))
		}
		if chunk.GetLast() != (i == len(r.chunks)-1) {
			t.Errorf("chunk %d last = %v", i, chunk.GetLast())
		}
	}
	if final := r.chunks[len(r.chunks)-1]; len(final.GetData()) != 0 {
		t.Fatal("the last chunk carries data")
	}
	if !bytes.Equal(tarMembers(t, r.data)["tasks/t/artifacts/big.bin"], big) {
		t.Fatal("big.bin differs")
	}
}

// evidenceEntries reads every tar entry of a complete bundle.
func evidenceEntries(bundle []byte) (map[string][]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(bundle))
	if err != nil {
		return nil, err
	}
	tr := tar.NewReader(gz)
	entries := map[string][]byte{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		entries[h.Name] = body
	}
}

// An awaited task (docs/DSL.md "Awaited tasks") has no container, so its
// bundle entry is the values.json the engine stored when it was resolved and
// nothing else: no commit.json, no attempt record, no logs.
func TestExportRunEvidence_AwaitedTaskHasOnlyItsValues(t *testing.T) {
	f := newEvidenceFixture(t)
	values := f.put("values.json", []byte(`{"approved":true}`))
	f.finish(&orkstackv1.Run{Id: "ork-ev-await", Status: orkstackv1.Status_STATUS_SUCCEEDED, TaskRuns: []*orkstackv1.TaskRun{
		{TaskId: "approval", Status: orkstackv1.Status_STATUS_SUCCEEDED, ValuesRef: values,
			Await: &orkstackv1.TaskAwait{ResolvedVia: orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API, ResolvedBy: "alice"}},
	}})
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-await"})
	var paths []string
	for _, m := range r.manifest.Members {
		if m.TaskID == "approval" {
			paths = append(paths, m.Path)
		}
	}
	if strings.Join(paths, ",") != "tasks/approval/values.json" {
		t.Errorf("awaited task members = %v, want only tasks/approval/values.json", paths)
	}
	if len(r.manifest.Tasks) != 1 || len(r.manifest.Tasks[0].Attempts) != 0 || r.manifest.Tasks[0].OutcomeAttempt != 0 {
		t.Fatalf("tasks = %+v", r.manifest.Tasks)
	}
	verifyBundle(t, r.data)
}

// TestEvidence_RunTimeoutRoundTrips: a timed-out run's Run.timeout — its
// limit, deadline, hard end and both flags — reaches the bundle's run.jsonl
// as archived and decodes back unchanged (docs/DSL.md "Run timeout").
func TestEvidence_RunTimeoutRoundTrips(t *testing.T) {
	f := newEvidenceFixture(t)
	timeout := &orkstackv1.RunTimeout{
		Seconds:        60,
		Deadline:       timestamppb.New(evidenceNow.Add(-time.Hour + 30*time.Second)),
		HardEnd:        timestamppb.New(evidenceNow.Add(-time.Hour + 40*time.Second)),
		TimedOut:       true,
		HardEndReached: true,
	}
	f.finish(&orkstackv1.Run{
		Id: "ork-ev-timeout", Status: orkstackv1.Status_STATUS_FAILED, Timeout: timeout,
		Error: &orkstackv1.Error{Code: "RUN_TIMED_OUT", Category: orkstackv1.FailureCategory_FAILURE_CATEGORY_TIMED_OUT},
	})
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-ev-timeout"})
	verifyBundle(t, r.data)
	archived, _, err := engine.UnmarshalRunArchive(tarMembers(t, r.data)["run.jsonl"])
	must.OKf(t, err, "UnmarshalRunArchive")
	if !proto.Equal(archived.GetTimeout(), timeout) {
		t.Fatalf("run.jsonl's timeout = %v, want %v", archived.GetTimeout(), timeout)
	}
	if r.manifest.RunStatus != "FAILED" {
		t.Fatalf("manifest run_status = %q, want FAILED", r.manifest.RunStatus)
	}
}

// A child's bundle lists what it received as artifact members of the
// reserved producer _inputs, with their source, and adds no task entry for
// it; a parent's composed task lists the artifacts its child returned like
// any task's, with their source too. --no-artifacts drops both.
func TestExport_ReceivedAndReturnedArtifactMembers(t *testing.T) {
	f := newEvidenceFixture(t)
	dataBytes := []byte("dataset\n")
	data := f.put("dataset.tar", dataBytes)
	source := func(ref *orkstackv1.ArtifactRef, name string) *orkstackv1.ArtifactRef {
		c := proto.Clone(ref).(*orkstackv1.ArtifactRef)
		c.Name = name
		c.Metadata = map[string]string{engine.ArtifactSourceRunKey: "ork-parent", engine.ArtifactSourceTaskKey: "build",
			engine.ArtifactSourceNameKey: "dataset.tar"}
		return c
	}
	f.finish(&orkstackv1.Run{Id: "ork-child",
		InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "dataset", RunId: "ork-parent", TaskId: "build",
			Artifact: source(data, "dataset")}}})
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-child"})
	member := memberAt(t, r.manifest, "tasks/_inputs/artifacts/dataset")
	if member.Kind != evidence.KindArtifact || member.TaskID != "_inputs" || member.Name != "dataset" ||
		member.Origin != data.GetUri() || member.State != evidence.StatePresent ||
		member.SourceRunID != "ork-parent" || member.SourceTaskID != "build" || member.SourceName != "dataset.tar" {
		t.Fatalf("received member = %+v", member)
	}
	for _, task := range r.manifest.Tasks {
		if task.TaskID == "_inputs" {
			t.Fatal("_inputs has a task entry")
		}
	}
	if !bytes.Equal(tarMembers(t, r.data)["tasks/_inputs/artifacts/dataset"], dataBytes) {
		t.Fatal("the received member's bytes differ")
	}
	verifyBundle(t, r.data)

	f.finish(&orkstackv1.Run{Id: "ork-parent", TaskRuns: []*orkstackv1.TaskRun{{
		TaskId: "train", Status: orkstackv1.Status_STATUS_SUCCEEDED, Child: &orkstackv1.TaskChild{RunId: "ork-child"},
		Artifacts: []*orkstackv1.ArtifactRef{source(data, "model")}}}})
	p := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-parent"})
	returned := memberAt(t, p.manifest, "tasks/train/artifacts/model")
	if returned.TaskID != "train" || returned.Name != "model" || returned.Origin != data.GetUri() ||
		returned.SourceRunID != "ork-parent" || returned.SourceName != "dataset.tar" {
		t.Fatalf("returned member = %+v", returned)
	}

	none := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-child", ExcludeArtifacts: true})
	for _, m := range none.manifest.Members {
		if m.Kind == evidence.KindArtifact {
			t.Fatalf("--no-artifacts kept %s", m.Path)
		}
	}
}

// A child's received artifacts are listed and read as the reserved task
// _inputs, resolved from the run's own record, never a caller URI.
func TestReadArtifact_ReceivedArtifactUnderInputs(t *testing.T) {
	f := newEvidenceFixture(t)
	payload := []byte("dataset\n")
	data := f.put("dataset.tar", payload)
	f.finish(&orkstackv1.Run{Id: "ork-child-read", InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "dataset",
		RunId: "ork-parent", TaskId: "build", Artifact: data}}})
	list, err := f.client.ListRunArtifacts(context.Background(), connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{
		RunId: "ork-child-read"}))
	must.OKf(t, err, "ListRunArtifacts")
	var listed *orkstackv1.RunArtifact
	for _, a := range list.Msg.GetArtifacts() {
		if a.GetTaskId() == "_inputs" {
			listed = a
		}
	}
	if listed == nil || listed.GetArtifact().GetName() != "dataset" || listed.GetKind() != orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT {
		t.Fatalf("listed = %v", list.Msg.GetArtifacts())
	}
	stream, err := f.client.ReadArtifact(context.Background(), connect.NewRequest(&orkstackv1.ReadArtifactRequest{
		RunId: "ork-child-read", TaskId: "_inputs", Kind: orkstackv1.ArtifactKind_ARTIFACT_KIND_OUTPUT, Name: "dataset"}))
	must.OKf(t, err, "ReadArtifact")
	var got []byte
	for stream.Receive() {
		got = append(got, stream.Msg().GetData()...)
	}
	must.OKf(t, stream.Err(), "ReadArtifact stream")
	if !bytes.Equal(got, payload) {
		t.Fatalf("read %q, want %q", got, payload)
	}
}

// A child's received artifacts are listed and exported by their reserved
// task, as every other task's are: --task _inputs on either narrows to them.
// A run that received nothing has no such task, and an item selects none.
func TestInputsTask_NarrowsTheListAndTheExport(t *testing.T) {
	f := newEvidenceFixture(t)
	ctx := context.Background()
	data := f.put("dataset.tar", []byte("dataset\n"))
	report := f.put("report.txt", []byte("report\n"))
	f.finish(&orkstackv1.Run{Id: "ork-inputs",
		InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "dataset", RunId: "ork-parent", TaskId: "build", Artifact: data}},
		TaskRuns: []*orkstackv1.TaskRun{{TaskId: "fit", Status: orkstackv1.Status_STATUS_SUCCEEDED,
			Artifacts: []*orkstackv1.ArtifactRef{report}}}})
	f.finish(&orkstackv1.Run{Id: "ork-plain", TaskRuns: []*orkstackv1.TaskRun{{TaskId: "fit",
		Status: orkstackv1.Status_STATUS_SUCCEEDED}}})

	list, err := f.client.ListRunArtifacts(ctx, connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{
		RunId: "ork-inputs", TaskId: "_inputs"}))
	must.OKf(t, err, "ListRunArtifacts of _inputs")
	if got := list.Msg.GetArtifacts(); len(got) != 1 || got[0].GetTaskId() != "_inputs" || got[0].GetArtifact().GetName() != "dataset" {
		t.Fatalf("listed %v, want the dataset alone", got)
	}
	r := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-inputs", TaskId: "_inputs"})
	memberAt(t, r.manifest, "tasks/_inputs/artifacts/dataset")
	for _, m := range r.manifest.Members {
		if m.TaskID == "fit" {
			t.Fatalf("an export of _inputs carries %s", m.Path)
		}
	}
	verifyBundle(t, r.data)
	scoped := f.mustExport(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-inputs", TaskId: "fit"})
	for _, m := range scoped.manifest.Members {
		if m.TaskID == "_inputs" {
			t.Fatalf("an export of fit carries %s", m.Path)
		}
	}

	if _, err := f.client.ListRunArtifacts(ctx, connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{
		RunId: "ork-plain", TaskId: "_inputs"})); connect.CodeOf(err) != connect.CodeNotFound ||
		!strings.Contains(err.Error(), `run "ork-plain" received no artifacts`) {
		t.Fatalf("a run that received nothing = %v", err)
	}
	if res := f.collectEvidence(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-plain", TaskId: "_inputs"}); res.code() != connect.CodeNotFound {
		t.Fatalf("an export of a run that received nothing = %v", res.err)
	}
	if _, err := f.client.ListRunArtifacts(ctx, connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{
		RunId: "ork-inputs", TaskId: "_inputs", Item: proto.Int32(0)})); err == nil {
		t.Fatal("an item of _inputs was listed")
	}
}

// A record whose received artifact holds no reference is damaged data, said
// so: never a panic, and never an artifact that is not there.
func TestInputsTask_ADamagedRecordIsDataLoss(t *testing.T) {
	f := newEvidenceFixture(t)
	f.finish(&orkstackv1.Run{Id: "ork-damaged", InputArtifacts: []*orkstackv1.NamedArtifact{{Name: "dataset"}}})
	_, err := f.client.ListRunArtifacts(context.Background(), connect.NewRequest(&orkstackv1.ListRunArtifactsRequest{RunId: "ork-damaged"}))
	if connect.CodeOf(err) != connect.CodeDataLoss || !strings.Contains(err.Error(), `artifact input "dataset"`) {
		t.Fatalf("ListRunArtifacts = %v, want data_loss naming the input", err)
	}
}
