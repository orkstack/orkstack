package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/gen/go/orkstack/v1/orkstackv1connect"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/evidence"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// memArtifactStore is a content-addressed artifact store whose URIs carry no
// temporary path, so a manifest built over it is a stable golden.
type memArtifactStore struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMemArtifactStore() *memArtifactStore {
	return &memArtifactStore{objects: map[string][]byte{}}
}

func (s *memArtifactStore) Put(_ context.Context, name string, r io.Reader, _ artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	content, err := io.ReadAll(r)
	if err != nil {
		return artifacts.ArtifactRef{}, err
	}
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	uri := "mem://bucket/objects/" + digest[:12] + "/" + name
	s.mu.Lock()
	s.objects[uri] = content
	s.mu.Unlock()
	return artifacts.ArtifactRef{Name: name, URI: uri, SizeBytes: int64(len(content)), SHA256: digest}, nil
}

func (s *memArtifactStore) Get(_ context.Context, uri string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	content, ok := s.objects[uri]
	if !ok {
		return nil, fmt.Errorf("%s: %w", uri, artifacts.ErrNotFound)
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (s *memArtifactStore) Stat(_ context.Context, uri string) (artifacts.ArtifactRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	content, ok := s.objects[uri]
	if !ok {
		return artifacts.ArtifactRef{}, fmt.Errorf("%s: %w", uri, artifacts.ErrNotFound)
	}
	return artifacts.ArtifactRef{URI: uri, SizeBytes: int64(len(content))}, nil
}

func (s *memArtifactStore) List(context.Context, string) ([]artifacts.ArtifactRef, error) {
	return nil, nil
}

func (s *memArtifactStore) Presign(context.Context, string, time.Duration) (string, error) {
	return "", artifacts.ErrUnsupported
}

func (s *memArtifactStore) Delete(_ context.Context, uri string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, uri)
	return nil
}

// evidenceCLI is the CLI fixture with every store ExportRunEvidence reads.
type evidenceCLI struct {
	*fixture
	store    *memArtifactStore
	attempts *artifacts.MemoryAttemptStore
	commits  *artifacts.MemoryCommitStore
	archiver *engine.RunArchiver
	orkflows *engine.MemoryOrkflowStore
	digest   string
	refs     map[string]*orkstackv1.ArtifactRef
}

const evidenceCLISource = "name: payments-reconcile\nversion: 1.4.0\ntasks:\n  - id: extract\n    uses: shell\n  - id: reconcile\n    uses: shell\n"

func newEvidenceCLI(t *testing.T) *evidenceCLI {
	t.Helper()
	e := &evidenceCLI{
		store:    newMemArtifactStore(),
		attempts: artifacts.NewMemoryAttemptStore(),
		commits:  artifacts.NewMemoryCommitStore(),
		orkflows: engine.NewMemoryOrkflowStore(),
		refs:     map[string]*orkstackv1.ArtifactRef{},
	}
	archive, err := artifacts.NewLocalRunArchive(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	e.archiver = engine.NewRunArchiver(archive)
	e.putDefinition(t, "payments-reconcile", "1.4.0", evidenceCLISource)
	e.fixture = newFixtureWith(t, func(cfg *api.Config) {
		cfg.Orkflows = e.orkflows
		cfg.Archive = e.archiver
		cfg.ArtifactStore = e.store
		cfg.Attempts = e.attempts
		cfg.Commits = e.commits
		cfg.System = api.SystemInfo{EngineVersion: "v0.9.3", BuildID: "2026.09.22-1"}
	})
	return e
}

func (e *evidenceCLI) putDefinition(t *testing.T, name, version, source string) string {
	t.Helper()
	wf := &orkstackv1.Orkflow{Namespace: "default", Name: name, Version: version, SourceYaml: source,
		Tasks: []*orkstackv1.Task{{Id: "extract", Uses: "shell"}, {Id: "reconcile", Uses: "shell"}}}
	digest, err := dsl.DefinitionDigest(wf)
	if err != nil {
		t.Fatal(err)
	}
	wf.DefinitionDigest = digest
	if _, _, err := e.orkflows.Put(context.Background(), wf, artifacts.NoHeadExpectation()); err != nil {
		t.Fatal(err)
	}
	if e.digest == "" {
		e.digest = digest
	}
	return digest
}

func (e *evidenceCLI) put(t *testing.T, key, name, content string) *orkstackv1.ArtifactRef {
	t.Helper()
	ref, err := e.store.Put(context.Background(), name, strings.NewReader(content), artifacts.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	proto := &orkstackv1.ArtifactRef{Name: ref.Name, Uri: ref.URI, SizeBytes: ref.SizeBytes, Sha256: ref.SHA256}
	e.refs[key] = proto
	return proto
}

func (e *evidenceCLI) record(t *testing.T, runID, taskID string, attempt int32, outcome orkstackv1.AttemptOutcome, logs ...*orkstackv1.ArtifactRef) {
	t.Helper()
	if err := e.attempts.PutAttempt(context.Background(), &orkstackv1.TaskAttempt{
		SchemaVersion: artifacts.AttemptSchemaVersion, RunId: runID, TaskId: taskID, Attempt: attempt,
		Outcome: outcome, SystemArtifacts: logs,
	}); err != nil {
		t.Fatal(err)
	}
}

// seed archives the C-suite run: "extract" succeeded on attempt 1;
// "reconcile" failed attempt 1 (whose logs are then reclaimed), left no record
// for attempt 2, and succeeded on attempt 3 with an artifact and a commit.
// The bundle therefore has 2 reclaimed members and 1 unrecorded one.
func (e *evidenceCLI) seed(t *testing.T, runID, digest string) {
	t.Helper()
	ctx := context.Background()
	extractValues := e.put(t, runID+"/extract/values", "values.json", `{"rows":12}`)
	extractOut := e.put(t, runID+"/extract/stdout", "stdout.log", "extracted 12 rows\n")
	extractCommit := e.commit(t, runID, "extract", digest, extractValues, nil)
	e.record(t, runID, "extract", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, extractOut)

	out1 := e.put(t, runID+"/reconcile/stdout1", "stdout.log", "reconcile attempt 1\n")
	err1 := e.put(t, runID+"/reconcile/stderr1", "stderr.log", "rates.parquet missing\n")
	rates := e.put(t, runID+"/reconcile/rates", "rates.parquet", strings.Repeat("rate,", 400))
	values := e.put(t, runID+"/reconcile/values", "values.json", `{"total":"42.00"}`)
	out3 := e.put(t, runID+"/reconcile/stdout3", "stdout.log", "reconcile attempt 3\n")
	commit := e.commit(t, runID, "reconcile", digest, values, []*orkstackv1.ArtifactRef{rates})
	e.record(t, runID, "reconcile", 1, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED, out1, err1)
	e.record(t, runID, "reconcile", 3, orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED, out3)
	for _, ref := range []*orkstackv1.ArtifactRef{out1, err1} {
		if err := e.store.Delete(ctx, ref.GetUri()); err != nil {
			t.Fatal(err)
		}
	}
	run := &orkstackv1.Run{
		Id: runID, OrkflowName: "payments-reconcile", OrkflowVersion: "1.4.0", OrkflowDefinitionDigest: digest,
		Status:    orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(fixedNow), StartedAt: timestamppb.New(fixedNow), FinishedAt: timestamppb.New(fixedNow.Add(8 * time.Minute)),
		TaskRuns: []*orkstackv1.TaskRun{
			{TaskId: "extract", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 1, ValuesRef: extractValues,
				SystemArtifacts: []*orkstackv1.ArtifactRef{extractOut}, Commit: extractCommit},
			{TaskId: "reconcile", Status: orkstackv1.Status_STATUS_SUCCEEDED, Attempt: 3, ValuesRef: values,
				Artifacts: []*orkstackv1.ArtifactRef{rates}, SystemArtifacts: []*orkstackv1.ArtifactRef{out3}, Commit: commit},
		},
	}
	if err := e.archiver.Archive(ctx, run, []*orkstackv1.Event{{RunId: runID, Sequence: 1,
		Type: orkstackv1.EventType_EVENT_TYPE_RUN_CREATED, Status: orkstackv1.Status_STATUS_PENDING, Timestamp: run.GetCreatedAt()}}); err != nil {
		t.Fatal(err)
	}
}

func (e *evidenceCLI) commit(t *testing.T, runID, taskID, digest string, values *orkstackv1.ArtifactRef, outputs []*orkstackv1.ArtifactRef) *orkstackv1.CommitRef {
	t.Helper()
	uri := "mem://bucket/metadata/commits/default/" + runID + "/" + taskID + "/commit.json"
	rec, err := e.commits.Create(context.Background(), uri, &orkstackv1.ActivityCommit{
		SchemaVersion: artifacts.CommitSchemaVersion, Namespace: "default", RunId: runID, TaskId: taskID,
		OrkflowName: "payments-reconcile", OrkflowVersion: "1.4.0", OrkflowDefinitionDigest: digest,
		Values: values, Artifacts: outputs,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &orkstackv1.CommitRef{Uri: uri, Sha256: rec.SHA256}
}

// inTempDir is a fresh directory for the files one test writes.
func inTempDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func verifyFile(t *testing.T, path string) *evidence.Report {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	report, err := evidence.Verify(file)
	if err != nil {
		t.Fatalf("Verify(%s): %v", path, err)
	}
	return report
}

func TestRunEvidenceExport_WritesTheBundleAndPrintsTheSummary(t *testing.T) {
	e := newEvidenceCLI(t)
	e.seed(t, "ork-evidence-1", e.digest)
	dir := inTempDir(t)
	res := e.run("run", "evidence", "export", "ork-evidence-1", "--output", filepath.Join(dir, "reconcile.tar.gz"))
	if res.code != exitOK {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	requireGolden(t, "run-evidence-export.golden", strings.ReplaceAll(res.stdout, dir+string(filepath.Separator), ""))
	file, err := os.Open(filepath.Join(dir, "reconcile.tar.gz"))
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatal(err)
	}
	first, err := tar.NewReader(gz).Next()
	_ = file.Close()
	if err != nil || first.Name != evidence.ManifestPath {
		t.Fatalf("first entry = %v, %v", first, err)
	}
	report := verifyFile(t, filepath.Join(dir, "reconcile.tar.gz"))
	if !report.OK() {
		t.Fatalf("verify failures: %+v", report.Failures)
	}
}

func TestRunEvidenceExport_JSONPrintsTheManifestVerbatim(t *testing.T) {
	e := newEvidenceCLI(t)
	e.seed(t, "ork-evidence-1", e.digest)
	res := e.run("run", "evidence", "export", "ork-evidence-1", "--json")
	if res.code != exitOK {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	requireGolden(t, "run-evidence-export.json.golden", res.stdout)

	stream, err := orkstackv1connect.NewRunServiceClient(e.httpClientFor(), e.baseURL).ExportRunEvidence(context.Background(),
		connect.NewRequest(&orkstackv1.ExportRunEvidenceRequest{RunId: "ork-evidence-1", ManifestOnly: true}))
	if err != nil {
		t.Fatal(err)
	}
	var sent bytes.Buffer
	for stream.Receive() {
		sent.Write(stream.Msg().GetData())
	}
	_ = stream.Close()
	if sent.String() != res.stdout {
		t.Fatal("--json stdout is not the manifest bytes the server sent")
	}

	dir := inTempDir(t)
	refused := e.run("run", "evidence", "export", "ork-evidence-1", "--json", "--output", filepath.Join(dir, "x.tar.gz"))
	if refused.code == exitOK || !strings.Contains(refused.stderr, "--json prints the manifest and writes no file") {
		t.Fatalf("--json --output = %d %q", refused.code, refused.stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, "x.tar.gz")); err == nil {
		t.Fatal("--json --output wrote a file")
	}
}

// httpClientFor is a plain client for the fixture's h2c server.
func (f *fixture) httpClientFor() connect.HTTPClient {
	g := &globalFlags{server: f.baseURL}
	return g.httpClient()
}

// evidenceStub is an engine whose RunService answers ExportRunEvidence however
// the test says, everything else Unimplemented.
type evidenceStub struct {
	orkstackv1connect.UnimplementedRunServiceHandler
	export func(*connect.ServerStream[orkstackv1.EvidenceChunk]) error
}

func (s evidenceStub) ExportRunEvidence(_ context.Context, _ *connect.Request[orkstackv1.ExportRunEvidenceRequest], stream *connect.ServerStream[orkstackv1.EvidenceChunk]) error {
	if s.export == nil {
		return connect.NewError(connect.CodeUnimplemented, errors.New("orkstack.v1.RunService.ExportRunEvidence is not implemented"))
	}
	return s.export(stream)
}

func startEvidenceStub(t *testing.T, stub evidenceStub) string {
	t.Helper()
	path, handler := orkstackv1connect.NewRunServiceHandler(stub)
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := api.NewServer("127.0.0.1:0", mux)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return "http://" + listener.Addr().String()
}

func TestRunEvidenceExport_IncompleteStreamLeavesNoFile(t *testing.T) {
	t.Setenv(serverEnvVar, "")
	t.Setenv(namespaceEnvVar, "")
	t.Setenv(configEnvVar, filepath.Join(t.TempDir(), "config.json"))
	server := startEvidenceStub(t, evidenceStub{export: func(stream *connect.ServerStream[orkstackv1.EvidenceChunk]) error {
		return stream.Send(&orkstackv1.EvidenceChunk{Data: []byte("partial bundle")})
	}})
	dir := inTempDir(t)
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"run", "evidence", "export", "ork-x", "--output", filepath.Join(dir, "out.tar.gz"), "--server", server}, &stdout, &stderr)
	if code == exitOK || !strings.Contains(stderr.String(), "evidence stream ended without completion") {
		t.Fatalf("exit %d stderr %q", code, stderr.String())
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("an incomplete export left %v", names)
	}
	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"run", "evidence", "export", "ork-x", "--json", "--server", server}, &stdout, &stderr)
	if code == exitOK || stdout.Len() != 0 || !strings.Contains(stderr.String(), "evidence stream ended without completion") {
		t.Fatalf("--json on an incomplete stream: exit %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestRunEvidenceExport_RefusesToClobberWithoutOverwrite(t *testing.T) {
	e := newEvidenceCLI(t)
	e.seed(t, "ork-evidence-1", e.digest)
	dir := inTempDir(t)
	if err := os.WriteFile(filepath.Join(dir, "bundle.tar.gz"), []byte("keep me"), 0o644); err != nil {
		t.Fatal(err)
	}
	res := e.run("run", "evidence", "export", "ork-evidence-1", "--output", filepath.Join(dir, "bundle.tar.gz"))
	if res.code == exitOK || !strings.Contains(res.stderr, "(pass --overwrite to replace it)") {
		t.Fatalf("exit %d stderr %q", res.code, res.stderr)
	}
	if content, _ := os.ReadFile(filepath.Join(dir, "bundle.tar.gz")); string(content) != "keep me" {
		t.Fatal("the existing file was replaced")
	}
	res = e.run("run", "evidence", "export", "ork-evidence-1", "--output", filepath.Join(dir, "bundle.tar.gz"), "--overwrite")
	if res.code != exitOK {
		t.Fatalf("--overwrite exit %d: %s", res.code, res.stderr)
	}
	if !verifyFile(t, filepath.Join(dir, "bundle.tar.gz")).OK() {
		t.Fatal("the overwritten bundle does not verify")
	}
	leftovers, _ := filepath.Glob(filepath.Join(dir, ".orkctl-evidence-*"))
	if len(leftovers) != 0 {
		t.Fatalf("temporaries left: %v", leftovers)
	}
}

// exportFixtureBundle writes the seeded run's bundle to name.
func (e *evidenceCLI) exportFixtureBundle(t *testing.T, name string) {
	t.Helper()
	if res := e.run("run", "evidence", "export", "ork-evidence-1", "--output", name); res.code != exitOK {
		t.Fatalf("export: %s", res.stderr)
	}
}

// rewriteBundle rewrites a bundle entry by entry, letting mutate change one
// entry's bytes.
func rewriteBundle(t *testing.T, from, to string, mutate func(name string, body []byte) []byte) {
	t.Helper()
	in, err := os.ReadFile(from)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)
	var out bytes.Buffer
	gw := gzip.NewWriter(&out)
	tw := tar.NewWriter(gw)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		body = mutate(h.Name, body)
		h.Size = int64(len(body))
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(to, out.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestGoldenRunEvidenceVerify_Passing(t *testing.T) {
	e := newEvidenceCLI(t)
	e.seed(t, "ork-evidence-1", e.digest)
	dir := inTempDir(t)
	e.exportFixtureBundle(t, filepath.Join(dir, "bundle.tar.gz"))
	res := e.run("run", "evidence", "verify", filepath.Join(dir, "bundle.tar.gz"))
	if res.code != exitOK {
		t.Fatalf("exit %d: %s", res.code, res.stderr)
	}
	requireGolden(t, "run-evidence-verify.golden", res.stdout)

	jsonRes := e.run("run", "evidence", "verify", filepath.Join(dir, "bundle.tar.gz"), "--json")
	if jsonRes.code != exitOK {
		t.Fatalf("--json exit %d: %s", jsonRes.code, jsonRes.stderr)
	}
	requireGolden(t, "run-evidence-verify.json.golden", jsonRes.stdout)
}

func TestGoldenRunEvidenceVerify_Failing(t *testing.T) {
	e := newEvidenceCLI(t)
	e.seed(t, "ork-evidence-1", e.digest)
	dir := inTempDir(t)
	e.exportFixtureBundle(t, filepath.Join(dir, "bundle.tar.gz"))
	rewriteBundle(t, filepath.Join(dir, "bundle.tar.gz"), filepath.Join(dir, "tampered.tar.gz"), func(name string, body []byte) []byte {
		if name == "tasks/reconcile/artifacts/rates.parquet" {
			body = bytes.Clone(body)
			body[0] ^= 0x01
		}
		return body
	})
	res := e.run("run", "evidence", "verify", filepath.Join(dir, "tampered.tar.gz"))
	if res.code != exitError {
		t.Fatalf("exit %d, want %d", res.code, exitError)
	}
	requireGolden(t, "run-evidence-verify-failed.golden", res.stdout)
	if !strings.Contains(res.stdout, "tasks/reconcile/artifacts/rates.parquet   digest mismatch") {
		t.Fatalf("the failure does not name the member: %s", res.stdout)
	}
}

func TestRunEvidenceVerify_AgainstStore(t *testing.T) {
	e := newEvidenceCLI(t)
	e.seed(t, "ork-evidence-1", e.digest)
	dir := inTempDir(t)
	e.exportFixtureBundle(t, filepath.Join(dir, "bundle.tar.gz"))

	agree := e.run("run", "evidence", "verify", filepath.Join(dir, "bundle.tar.gz"), "--against-store")
	if agree.code != exitOK || !strings.Contains(agree.stdout, "store:\n  every member agrees with the store\n") {
		t.Fatalf("exit %d stdout %q stderr %q", agree.code, agree.stdout, agree.stderr)
	}

	if err := e.store.Delete(context.Background(), e.refs["ork-evidence-1/reconcile/rates"].GetUri()); err != nil {
		t.Fatal(err)
	}
	reclaimed := e.run("run", "evidence", "verify", filepath.Join(dir, "bundle.tar.gz"), "--against-store")
	if reclaimed.code != exitOK || !strings.Contains(reclaimed.stdout, "tasks/reconcile/artifacts/rates.parquet   reclaimed since export") {
		t.Fatalf("exit %d stdout %q", reclaimed.code, reclaimed.stdout)
	}

	// A bundle whose manifest claims another definition digest than the run
	// pinned: the live manifest disagrees.
	rewriteBundle(t, filepath.Join(dir, "bundle.tar.gz"), filepath.Join(dir, "claims-other.tar.gz"), func(name string, body []byte) []byte {
		if name == evidence.ManifestPath {
			return bytes.Replace(body, []byte(e.digest), []byte("sha256:"+strings.Repeat("f", 64)), 1)
		}
		return body
	})
	differs := e.run("run", "evidence", "verify", filepath.Join(dir, "claims-other.tar.gz"), "--against-store")
	if differs.code != exitError || !strings.Contains(differs.stdout, "definition_digest") || !strings.Contains(differs.stdout, "store digest differs") {
		t.Fatalf("exit %d stdout %q", differs.code, differs.stdout)
	}

	before := len(e.requests())
	refused := e.run("run", "evidence", "verify", filepath.Join(dir, "bundle.tar.gz"), "--against-store", "-n", "other")
	if refused.code == exitOK || !strings.Contains(refused.stderr, `bundle is of namespace "default"; -n names "other"`) {
		t.Fatalf("exit %d stderr %q", refused.code, refused.stderr)
	}
	if after := e.requests()[before:]; len(after) != 0 {
		t.Fatalf("-n other reached the server: %v", after)
	}
}

func TestRunEvidence_ErrorsAreActionable(t *testing.T) {
	e := newEvidenceCLI(t)
	e.seed(t, "ork-evidence-1", e.digest)
	e.runs.Apply(&orkstackv1.Run{Id: "ork-live", OrkflowName: "payments-reconcile", Status: orkstackv1.Status_STATUS_RUNNING})
	dir := inTempDir(t)

	live := e.run("run", "evidence", "export", "ork-live", "--output", filepath.Join(dir, "live.tar.gz"))
	if live.code == exitOK || !strings.Contains(live.stderr, `run "ork-live" is RUNNING; evidence is exported for finished runs only`) {
		t.Fatalf("live run: exit %d stderr %q", live.code, live.stderr)
	}
	e.run("namespace", "create", "other").requireExit(t, exitOK)
	elsewhere := e.run("-n", "other", "run", "evidence", "export", "ork-evidence-1", "--json")
	if elsewhere.code == exitOK || !strings.Contains(elsewhere.stderr, "export run evidence: not_found") {
		t.Fatalf("another namespace: exit %d stderr %q", elsewhere.code, elsewhere.stderr)
	}

	t.Setenv(serverEnvVar, "")
	old := startEvidenceStub(t, evidenceStub{})
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"run", "evidence", "export", "ork-evidence-1", "--json", "--server", old}, &stdout, &stderr)
	if code == exitOK || !strings.Contains(stderr.String(), "export run evidence: unimplemented") {
		t.Fatalf("old engine: exit %d stderr %q", code, stderr.String())
	}
}
