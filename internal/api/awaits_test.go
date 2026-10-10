package api_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
	"github.com/orkstack/orkstack/pkg/triggers"
)

const awaitedYAML = `
name: awaited-api
version: "1.0.0"
tasks:
  - id: a
    uses: shell
    with: {op: exec, command: ["echo", "a"]}
  - id: gate
    needs: [a]
    await:
      timeout_seconds: 3600
      callback: true
      schema:
        approved: {type: boolean, required: true}
  - id: manual
    needs: [a]
    await: {timeout_seconds: 3600}
  - id: c
    uses: shell
    needs: [gate, manual]
    with: {op: exec, command: ["echo", "c"]}
`

const awaitOnlyYAML = `
name: await-only-api
version: "1.0.0"
tasks:
  - id: gate
    await: {timeout_seconds: 60, callback: true}
`

// recordingArtifactStore is a content-addressed in-memory artifact store that
// records every Put, and can be made to fail one.
type recordingArtifactStore struct {
	mu      sync.Mutex
	puts    []recordedPut
	objects map[string][]byte
	err     error
	// order, when set, is appended "put" on every successful Put — shared
	// with the starter's onSignal so a test can read the two in sequence.
	order *[]string
}

type recordedPut struct {
	name string
	body []byte
	opts artifacts.PutOptions
}

func (s *recordingArtifactStore) Put(_ context.Context, name string, r io.Reader, opts artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return artifacts.ArtifactRef{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return artifacts.ArtifactRef{}, s.err
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	uri := "memory://orkflows/" + opts.Metadata[artifacts.MetaOrkflow] + "/" + opts.Metadata[artifacts.MetaRunID] +
		"/" + opts.Metadata[artifacts.MetaTaskID] + "/" + name + "/" + digest
	if s.objects == nil {
		s.objects = map[string][]byte{}
	}
	s.objects[uri] = body
	s.puts = append(s.puts, recordedPut{name: name, body: body, opts: opts})
	if s.order != nil {
		*s.order = append(*s.order, "put")
	}
	return artifacts.ArtifactRef{
		Name: name, URI: uri, ContentType: opts.ContentType,
		SizeBytes: int64(len(body)), SHA256: digest, Metadata: opts.Metadata,
	}, nil
}

func (s *recordingArtifactStore) recorded() []recordedPut {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedPut(nil), s.puts...)
}

func (s *recordingArtifactStore) Get(_ context.Context, uri string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.objects[uri]
	if !ok {
		return nil, artifacts.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (s *recordingArtifactStore) Stat(context.Context, string) (artifacts.ArtifactRef, error) {
	return artifacts.ArtifactRef{}, artifacts.ErrUnsupported
}

func (s *recordingArtifactStore) List(context.Context, string) ([]artifacts.ArtifactRef, error) {
	return nil, nil
}

func (s *recordingArtifactStore) Presign(context.Context, string, time.Duration) (string, error) {
	return "", artifacts.ErrUnsupported
}

func (s *recordingArtifactStore) Delete(context.Context, string) error { return nil }

// awaitFixture is a Service whose read side is a fakeReader — the record any
// replica reads — over a recording store and a recording starter.
type awaitFixture struct {
	*fixture
	reader *fakeReader
	store  *recordingArtifactStore
	order  []string
	wf     *orkstackv1.Orkflow
}

const awaitToken = "3f9a0000000000000000000000000000000000000000000000000000000000aa"

func newAwaitFixture(t *testing.T, configure ...func(*api.Config)) *awaitFixture {
	t.Helper()
	af := &awaitFixture{reader: &fakeReader{}, store: &recordingArtifactStore{}}
	af.store.order = &af.order
	af.fixture = newFixtureWith(t, func(cfg *api.Config) {
		cfg.Reader = af.reader
		cfg.ArtifactStore = af.store
		for _, c := range configure {
			c(cfg)
		}
	})
	af.starter.onSignal = func(string, *orkstackv1.AwaitSignal) { af.order = append(af.order, "signal") }
	wf, err := dsl.Compile([]byte(awaitedYAML))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	af.wf = wf
	return af
}

// credentialFor is the plan entry a submit would have minted for token.
func credentialFor(t *testing.T, token string) engine.AwaitCredential {
	t.Helper()
	salt := []byte("0123456789abcdef")
	return engine.AwaitCredential{
		Salt:   hex.EncodeToString(salt),
		Digest: hex.EncodeToString(triggers.HashSecret(salt, token)),
	}
}

// seedRun records a running run of the awaited orkflow in namespace ns, with
// the given task statuses, and the start it was launched with.
func (f *awaitFixture) seedRun(t *testing.T, runID, ns string, statuses map[string]orkstackv1.Status) *orkstackv1.Run {
	t.Helper()
	run := &orkstackv1.Run{
		Id: runID, Namespace: ns, OrkflowName: f.wf.GetName(), OrkflowVersion: f.wf.GetVersion(),
		Status: orkstackv1.Status_STATUS_RUNNING,
	}
	for _, task := range f.wf.GetTasks() {
		tr := &orkstackv1.TaskRun{TaskId: task.GetId(), Status: orkstackv1.Status_STATUS_PENDING}
		if s, ok := statuses[task.GetId()]; ok {
			tr.Status = s
		}
		if task.GetAwait() != nil {
			tr.Await = &orkstackv1.TaskAwait{Callback: task.GetAwait().GetCallback()}
		}
		run.TaskRuns = append(run.TaskRuns, tr)
	}
	f.reader.setRun(run)
	f.reader.setStart(runID, &engine.RunStartRecord{
		Orkflow: f.wf,
		Plan: engine.RunPlan{
			Namespace: ns,
			Awaits:    map[string]engine.AwaitCredential{"gate": credentialFor(t, awaitToken)},
		},
	})
	return run
}

func taskRunFor(run *orkstackv1.Run, taskID string) *orkstackv1.TaskRun {
	for _, tr := range run.GetTaskRuns() {
		if tr.GetTaskId() == taskID {
			return tr
		}
	}
	return nil
}

func (f *awaitFixture) resolve(req *orkstackv1.ResolveAwaitRequest, principal string) (*orkstackv1.ResolveAwaitResponse, error) {
	r := connect.NewRequest(req)
	if principal != "" {
		r.Header().Set(api.AuditPrincipalHeader, principal)
	}
	resp, err := f.runClient().ResolveAwait(context.Background(), r)
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// door POSTs to the callback door and returns the status and body.
func (f *awaitFixture) door(t *testing.T, path string, token *string, body string, headers map[string]string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, f.baseURL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != nil {
		req.Header.Set("Authorization", *token)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, payload
}

func awaitPath(runID, taskID string) string {
	return "/v1/runs/" + runID + "/awaits/" + taskID
}

// untouched asserts that nothing was stored and nothing was delivered.
func (f *awaitFixture) untouched(t *testing.T, label string) {
	t.Helper()
	if puts := f.store.recorded(); len(puts) != 0 {
		t.Errorf("%s: %d values stored, want none", label, len(puts))
	}
	if signals := f.starter.delivered(); len(signals) != 0 {
		t.Errorf("%s: %d resolutions delivered, want none", label, len(signals))
	}
}

func (f *awaitFixture) apply(t *testing.T, yaml string) {
	t.Helper()
	if _, err := f.svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
}

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)

func TestSubmitRun_MintsOneCallbackPerCallbackAwait(t *testing.T) {
	f := newAwaitFixture(t)
	f.apply(t, awaitedYAML)
	resp, err := f.svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "awaited-api"}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	runID := resp.Msg.GetRun().GetId()
	callbacks := resp.Msg.GetAwaitCallbacks()
	if len(callbacks) != 1 || callbacks[0].GetTaskId() != "gate" {
		t.Fatalf("await_callbacks = %v, want exactly gate", callbacks)
	}
	token := callbacks[0].GetToken()
	if !hex64.MatchString(token) {
		t.Fatalf("token %q is not 32 bytes of hex", token)
	}
	if callbacks[0].GetPath() != "/v1/runs/"+runID+"/awaits/gate" {
		t.Fatalf("path = %q", callbacks[0].GetPath())
	}
	f.starter.mu.Lock()
	plan := f.starter.plans[0]
	f.starter.mu.Unlock()
	if len(plan.Awaits) != 1 {
		t.Fatalf("plan.Awaits = %v, want only gate", plan.Awaits)
	}
	cred := plan.Awaits["gate"]
	salt, err := hex.DecodeString(cred.Salt)
	if err != nil {
		t.Fatalf("salt: %v", err)
	}
	if hex.EncodeToString(triggers.HashSecret(salt, token)) != cred.Digest {
		t.Fatal("the plan's digest is not sha256(salt || token)")
	}
	// The token appears nowhere but the response.
	encodedPlan, _ := json.Marshal(plan)
	if strings.Contains(string(encodedPlan), token) {
		t.Fatal("the plan carries the plaintext token")
	}
	pending, _ := proto.Marshal(resp.Msg.GetRun())
	if bytes.Contains(pending, []byte(token)) {
		t.Fatal("the pending run carries the plaintext token")
	}
	if stored, ok := f.runs.Get(runID); ok {
		encoded, _ := proto.Marshal(stored)
		if bytes.Contains(encoded, []byte(token)) {
			t.Fatal("the run store carries the plaintext token")
		}
	}
	for _, event := range f.bus.Events(runID, 0) {
		encoded, _ := proto.Marshal(event)
		if bytes.Contains(encoded, []byte(token)) {
			t.Fatal("an event carries the plaintext token")
		}
	}
}

func TestSubmitRun_KeyedRepeatDisclosesNoCallback(t *testing.T) {
	f := newAwaitFixture(t)
	f.apply(t, awaitedYAML)
	submit := func() *orkstackv1.SubmitRunResponse {
		resp, err := f.svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
			OrkflowName: "awaited-api", IdempotencyKey: "nightly",
		}))
		if err != nil {
			t.Fatalf("SubmitRun: %v", err)
		}
		return resp.Msg
	}
	first := submit()
	if len(first.GetAwaitCallbacks()) != 1 {
		t.Fatalf("the submission that started the run disclosed %v", first.GetAwaitCallbacks())
	}
	repeat := submit()
	if repeat.GetRun().GetId() != first.GetRun().GetId() {
		t.Fatalf("keyed repeat answered %q, want %q", repeat.GetRun().GetId(), first.GetRun().GetId())
	}
	if len(repeat.GetAwaitCallbacks()) != 0 {
		t.Fatalf("a keyed repeat disclosed callbacks %v; the winner's plaintexts no longer exist", repeat.GetAwaitCallbacks())
	}
}

func TestSubmitRun_ReissuedRunIDDisclosesPathsForTheNewID(t *testing.T) {
	starter := &takenIDStarter{rejections: 1}
	svc := api.NewService(api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(),
		Starter:  starter,
	})
	if _, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: awaitedYAML})); err != nil {
		t.Fatalf("ApplyOrkflow: %v", err)
	}
	resp, err := svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "awaited-api"}))
	if err != nil {
		t.Fatalf("SubmitRun: %v", err)
	}
	rejected, started := starter.attempts()
	if len(rejected) != 1 || len(started) != 1 || rejected[0] == started[0] {
		t.Fatalf("rejected %v started %v, want one reissue", rejected, started)
	}
	callbacks := resp.Msg.GetAwaitCallbacks()
	if len(callbacks) != 1 || callbacks[0].GetPath() != "/v1/runs/"+started[0]+"/awaits/gate" {
		t.Fatalf("callbacks = %v, want the path of the run that started (%s)", callbacks, started[0])
	}
	if resp.Msg.GetRun().GetId() != started[0] {
		t.Fatalf("run id = %q, want %q", resp.Msg.GetRun().GetId(), started[0])
	}
}

func TestSubmitRun_AwaitOnlyOrkflowNeedsNoLivePool(t *testing.T) {
	f := newAwaitFixture(t, func(cfg *api.Config) { cfg.Pools = livePools{} })
	f.apply(t, awaitOnlyYAML)
	resp, err := f.svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "await-only-api"}))
	if err != nil {
		t.Fatalf("an await-only orkflow was refused with no Orker anywhere: %v", err)
	}
	f.starter.mu.Lock()
	plan := f.starter.plans[0]
	f.starter.mu.Unlock()
	if len(plan.Routes) != 0 {
		t.Fatalf("routes = %v, want none", plan.Routes)
	}
	if len(resp.Msg.GetAwaitCallbacks()) != 1 {
		t.Fatalf("callbacks = %v", resp.Msg.GetAwaitCallbacks())
	}
}

func TestSubmitRun_RefusedSubmissionMintsNothing(t *testing.T) {
	// A container task on a pool with no live Orker: the pool plan refuses,
	// and it refuses before any token is minted.
	f := newAwaitFixture(t, func(cfg *api.Config) { cfg.Pools = livePools{} })
	f.apply(t, awaitedYAML)
	resp, err := f.svc.SubmitRun(context.Background(),
		connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "awaited-api"}))
	if err == nil {
		t.Fatalf("SubmitRun into an empty pool succeeded: %v", resp.Msg)
	}
	f.starter.mu.Lock()
	defer f.starter.mu.Unlock()
	if len(f.starter.plans) != 0 {
		t.Fatalf("a refused submission reached the starter with plan %v", f.starter.plans)
	}
}

func TestResolveAwait_Refusals(t *testing.T) {
	type tc struct {
		name     string
		seed     map[string]orkstackv1.Status
		runState orkstackv1.Status
		taskErr  *orkstackv1.Error
		req      *orkstackv1.ResolveAwaitRequest
		noStart  bool
		code     connect.Code
		message  string
	}
	run := "ork-await-1"
	cases := []tc{
		{name: "missing run id", req: &orkstackv1.ResolveAwaitRequest{TaskId: "gate"},
			code: connect.CodeInvalidArgument, message: "run_id is required"},
		{name: "missing task id", req: &orkstackv1.ResolveAwaitRequest{RunId: run},
			code: connect.CodeInvalidArgument, message: "task_id is required"},
		{name: "another namespace", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			req:  &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", Namespace: "payments"},
			code: connect.CodeNotFound, message: "not found"},
		{name: "unknown task", req: &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "ghost"},
			code: connect.CodeNotFound, message: `run "ork-await-1" has no task "ghost"`},
		{name: "not an await", req: &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "a"},
			code: connect.CodeInvalidArgument, message: `task "a" of run "ork-await-1" is not an awaited task`},
		{name: "pending", req: &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", ValuesJson: `{"approved":true}`},
			code: connect.CodeFailedPrecondition, message: `await "gate" has not started awaiting`},
		{name: "skipped", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_SKIPPED},
			req:  &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", ValuesJson: `{"approved":true}`},
			code: connect.CodeFailedPrecondition, message: `await "gate" was skipped`},
		{name: "canceled", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_CANCELED},
			req:  &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", ValuesJson: `{"approved":true}`},
			code: connect.CodeFailedPrecondition, message: `await "gate" was canceled when its run ended`},
		{name: "timed out", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_FAILED},
			taskErr: &orkstackv1.Error{Code: engine.AwaitTimedOutCode, Message: "late"},
			req:     &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", ValuesJson: `{"approved":true}`},
			code:    connect.CodeFailedPrecondition, message: `await "gate" is FAILED (AWAIT_TIMED_OUT); it accepts no resolution`},
		{name: "otherwise resolved", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_SUCCEEDED},
			req:  &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", ValuesJson: `{"approved":false}`},
			code: connect.CodeFailedPrecondition, message: `await "gate" is SUCCEEDED; it accepts no resolution`},
		{name: "ended run", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			runState: orkstackv1.Status_STATUS_CANCELED,
			req:      &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", ValuesJson: `{"approved":true}`},
			code:     connect.CodeFailedPrecondition, message: `run "ork-await-1" has ended (CANCELED)`},
		{name: "history gone", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING}, noStart: true,
			req:  &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", ValuesJson: `{"approved":true}`},
			code: connect.CodeFailedPrecondition, message: "has ended and is no longer live"},
		{name: "reject with values", req: &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", Reject: true, Reason: "x", ValuesJson: "{}"},
			code: connect.CodeInvalidArgument, message: "values_json and reject are exclusive"},
		{name: "reason without reject", req: &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", Reason: "x"},
			code: connect.CodeInvalidArgument, message: "reason is only for a rejection"},
		{name: "empty reason", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			req:  &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", Reject: true},
			code: connect.CodeInvalidArgument, message: "reason is required"},
		{name: "over-long reason", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			req:  &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", Reject: true, Reason: strings.Repeat("x", 1025)},
			code: connect.CodeInvalidArgument, message: "reason is 1025 bytes, over the 1024-byte limit"},
		{name: "control-char reason", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			req:  &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", Reject: true, Reason: "bad\x07bell"},
			code: connect.CodeInvalidArgument, message: "reason contains control character 0x07"},
		{name: "schema violation", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			req:  &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate", ValuesJson: `{"approved":"yes"}`},
			code: connect.CodeInvalidArgument, message: "values.approved: must be a boolean, not a string"},
		{name: "over the bound", seed: map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			req: &orkstackv1.ResolveAwaitRequest{RunId: run, TaskId: "gate",
				ValuesJson: `{"approved":true,"pad":"` + strings.Repeat("x", 300*1024) + `"}`},
			code: connect.CodeInvalidArgument, message: "over the 262144-byte limit"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newAwaitFixture(t, func(cfg *api.Config) {
				cfg.Namespaces = newFakeRegistry(&orkstackv1.Namespace{Name: "payments"})
			})
			seeded := f.seedRun(t, run, "", c.seed)
			if c.runState != orkstackv1.Status_STATUS_UNSPECIFIED {
				seeded.Status = c.runState
			}
			if c.taskErr != nil {
				taskRunFor(seeded, "gate").Error = c.taskErr
			}
			if c.noStart {
				f.reader.mu.Lock()
				delete(f.reader.starts, run)
				f.reader.mu.Unlock()
			}
			_, err := f.resolve(c.req, "")
			if connect.CodeOf(err) != c.code || !strings.Contains(err.Error(), c.message) {
				t.Fatalf("error = %v, want %v containing %q", err, c.code, c.message)
			}
			f.untouched(t, c.name)
		})
	}
}

func TestResolveAwait_StoresExactBytesBeforeDelivering(t *testing.T) {
	f := newAwaitFixture(t)
	f.seedRun(t, "ork-await-2", "", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING})
	body := "{\r\n  \"approved\": true,\n  \"n\": 12345678901234567890 }"
	resp, err := f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: "ork-await-2", TaskId: "gate", ValuesJson: body}, "alice")
	if err != nil {
		t.Fatalf("ResolveAwait: %v", err)
	}
	if strings.Join(f.order, ",") != "put,signal" {
		t.Fatalf("order = %v, want the values stored before the resolution was delivered", f.order)
	}
	puts := f.store.recorded()
	if len(puts) != 1 || string(puts[0].body) != body {
		t.Fatalf("stored %+v, want the request's exact bytes %q", puts, body)
	}
	// The provenance an Orker records for the same (orkflow, run, task).
	want := artifacts.Provenance{Orkflow: "awaited-api", TaskID: "gate", RunID: "ork-await-2"}.Metadata(nil)
	want["artifact"] = "values.json"
	if puts[0].name != "values.json" || puts[0].opts.ContentType != "application/json" || fmt.Sprint(puts[0].opts.Metadata) != fmt.Sprint(want) {
		t.Fatalf("put %q %q %v, want values.json application/json %v", puts[0].name, puts[0].opts.ContentType, puts[0].opts.Metadata, want)
	}
	sum := sha256.Sum256([]byte(body))
	if resp.GetValuesSha256() != hex.EncodeToString(sum[:]) || resp.GetRepeat() || resp.GetReject() {
		t.Fatalf("response = %v", resp)
	}
	signals := f.starter.delivered()
	if len(signals) != 1 {
		t.Fatalf("delivered %d resolutions", len(signals))
	}
	sig := signals[0]
	if sig.GetTaskId() != "gate" || sig.GetValues().GetSha256() != resp.GetValuesSha256() ||
		string(sig.GetValuesJson()) != body || sig.GetVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_API ||
		sig.GetPrincipal() != "alice" {
		t.Fatalf("signal = %v", sig)
	}

	t.Run("inline at 4096 bytes, absent at 4097", func(t *testing.T) {
		for _, size := range []int{4096, 4097} {
			f := newAwaitFixture(t)
			f.seedRun(t, "ork-await-3", "", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING})
			prefix, suffix := `{"approved":true,"pad":"`, `"}`
			body := prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix
			if _, err := f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: "ork-await-3", TaskId: "gate", ValuesJson: body}, ""); err != nil {
				t.Fatalf("%d bytes: %v", size, err)
			}
			inline := len(f.starter.delivered()[0].GetValuesJson()) > 0
			if inline != (size <= 4096) {
				t.Fatalf("%d bytes: inline = %v", size, inline)
			}
		}
	})
}

func TestResolveAwait_StoreFailureSendsNothing(t *testing.T) {
	f := newAwaitFixture(t)
	f.store.err = errors.New("bucket unavailable")
	f.seedRun(t, "ork-await-4", "", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING})
	_, err := f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: "ork-await-4", TaskId: "gate", ValuesJson: `{"approved":true}`}, "")
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "store the resolution's values") {
		t.Fatalf("error = %v, want Unavailable", err)
	}
	if len(f.starter.delivered()) != 0 {
		t.Fatal("a resolution was delivered although its values were never stored")
	}
}

func TestResolveAwait_ClosedRunIsFailedPrecondition(t *testing.T) {
	f := newAwaitFixture(t)
	f.starter.signalErr = fmt.Errorf("signal run: %w", errors.Join(engine.ErrRunEnded, serviceerror.NewNotFound("closed")))
	f.seedRun(t, "ork-await-5", "", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING})
	_, err := f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: "ork-await-5", TaskId: "gate", Reject: true, Reason: "no"}, "")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), `run "ork-await-5" has ended`) {
		t.Fatalf("error = %v, want FailedPrecondition", err)
	}

	f.starter.signalErr = errors.New("cluster down")
	_, err = f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: "ork-await-5", TaskId: "gate", Reject: true, Reason: "no"}, "")
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("error = %v, want Unavailable", err)
	}
}

func TestResolveAwait_IdenticalRepeatSucceedsAndSendsNothing(t *testing.T) {
	body := `{"approved":true}`
	sum := sha256.Sum256([]byte(body))
	t.Run("completion", func(t *testing.T) {
		f := newAwaitFixture(t)
		run := f.seedRun(t, "ork-await-6", "", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_SUCCEEDED})
		taskRunFor(run, "gate").ValuesRef = &orkstackv1.ArtifactRef{Sha256: hex.EncodeToString(sum[:])}
		run.Status = orkstackv1.Status_STATUS_SUCCEEDED // ended since: still a repeat
		resp, err := f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: "ork-await-6", TaskId: "gate", ValuesJson: body}, "")
		if err != nil || !resp.GetRepeat() || resp.GetValuesSha256() != hex.EncodeToString(sum[:]) {
			t.Fatalf("repeat = %v, %v", resp, err)
		}
		f.untouched(t, "repeat completion")
		_, err = f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: "ork-await-6", TaskId: "gate", ValuesJson: `{"approved":false}`}, "")
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("a different completion = %v, want FailedPrecondition", err)
		}
	})
	t.Run("rejection", func(t *testing.T) {
		f := newAwaitFixture(t)
		run := f.seedRun(t, "ork-await-7", "", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_FAILED})
		taskRunFor(run, "gate").Error = &orkstackv1.Error{
			Code: engine.AwaitRejectedCode, Message: engine.AwaitRejectedMessage("gate", "job cancelled"),
		}
		run.Status = orkstackv1.Status_STATUS_FAILED
		resp, err := f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: "ork-await-7", TaskId: "gate", Reject: true, Reason: "job cancelled"}, "")
		if err != nil || !resp.GetRepeat() || !resp.GetReject() {
			t.Fatalf("repeat = %v, %v", resp, err)
		}
		f.untouched(t, "repeat rejection")
		_, err = f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: "ork-await-7", TaskId: "gate", Reject: true, Reason: "another reason"}, "")
		if connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatalf("a different rejection = %v, want FailedPrecondition", err)
		}
	})
}

func errorCode(t *testing.T, body []byte) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode error envelope: %v (%s)", err, body)
	}
	return envelope.Error.Code
}

func TestAwaitDoor_StatusTable(t *testing.T) {
	const run = "ork-door-1"
	good := bearer(awaitToken)
	cases := []struct {
		name   string
		seed   map[string]orkstackv1.Status
		path   string
		token  *string
		body   string
		status int
		code   string
	}{
		{"no authorization", nil, awaitPath(run, "gate"), nil, `{}`, 401, "unauthenticated"},
		{"not a bearer", nil, awaitPath(run, "gate"), func() *string { s := "Basic abc"; return &s }(), `{}`, 401, "unauthenticated"},
		{"wrong token", nil, awaitPath(run, "gate"), bearer("nope"), `{}`, 401, "unauthenticated"},
		{"unknown run", nil, awaitPath("ork-ghost", "gate"), good, `{}`, 404, "not_found"},
		{"task not a slug", nil, awaitPath(run, "Gate_1"), good, `{}`, 404, "not_found"},
		{"not a callback await", nil, awaitPath(run, "manual"), good, `{}`, 404, "not_found"},
		{"over the values limit", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			awaitPath(run, "gate"), good, `{"pad":"` + strings.Repeat("x", 300*1024) + `"}`, 413, "invalid_argument"},
		{"not one JSON object", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			awaitPath(run, "gate"), good, `[true]`, 400, "invalid_argument"},
		{"invalid UTF-8", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			awaitPath(run, "gate"), good, "{\"approved\":\"\xff\"}", 400, "invalid_argument"},
		{"schema violation", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			awaitPath(run, "gate"), good, `{"approved":1}`, 400, "invalid_argument"},
		{"bad reject envelope", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			awaitPath(run, "gate") + "/reject", good, `{"why":"x"}`, 400, "invalid_argument"},
		{"empty reason", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			awaitPath(run, "gate") + "/reject", good, `{"reason":""}`, 400, "invalid_argument"},
		{"task pending", nil, awaitPath(run, "gate"), good, `{"approved":true}`, 409, "failed_precondition"},
		{"task timed out", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_FAILED},
			awaitPath(run, "gate"), good, `{"approved":true}`, 409, "failed_precondition"},
		{"whitespace body is {} and {} breaks the schema", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			awaitPath(run, "gate"), good, "  \n ", 400, "invalid_argument"},
		{"completion", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			awaitPath(run, "gate"), good, `{"approved":true}`, 202, ""},
		{"rejection", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING},
			awaitPath(run, "gate") + "/reject", good, `{"reason":"upstream job cancelled"}`, 202, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newAwaitFixture(t)
			f.seedRun(t, run, "team-a", c.seed)
			status, body := f.door(t, c.path, c.token, c.body, nil)
			if status != c.status {
				t.Fatalf("status = %d, want %d: %s", status, c.status, body)
			}
			if c.status != 202 {
				if got := errorCode(t, body); got != c.code {
					t.Fatalf("error.code = %q, want %q: %s", got, c.code, body)
				}
				f.untouched(t, c.name)
				return
			}
			var resp map[string]any
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatalf("decode 202 body: %v (%s)", err, body)
			}
			if resp["runId"] != run || resp["namespace"] != "team-a" || resp["taskId"] != "gate" {
				t.Fatalf("202 body = %s", body)
			}
			if _, carriesRun := resp["run"]; carriesRun {
				t.Fatalf("the door's body carries the run: %s", body)
			}
			sig := f.starter.delivered()[0]
			if sig.GetVia() != orkstackv1.AwaitResolvedVia_AWAIT_RESOLVED_VIA_CALLBACK {
				t.Fatalf("via = %v", sig.GetVia())
			}
		})
	}

	t.Run("an empty body is {}", func(t *testing.T) {
		f := newAwaitFixture(t)
		f.wf.Tasks[1].Await.Schema = nil
		f.seedRun(t, run, "", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING})
		status, body := f.door(t, awaitPath(run, "gate"), good, " \t\n", nil)
		if status != 202 {
			t.Fatalf("status = %d: %s", status, body)
		}
		if puts := f.store.recorded(); len(puts) != 1 || string(puts[0].body) != "{}" {
			t.Fatalf("stored %v, want exactly {}", puts)
		}
	})
	t.Run("a repeat is 202 with repeat true", func(t *testing.T) {
		f := newAwaitFixture(t)
		seeded := f.seedRun(t, run, "", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_SUCCEEDED})
		sum := sha256.Sum256([]byte(`{"approved":true}`))
		taskRunFor(seeded, "gate").ValuesRef = &orkstackv1.ArtifactRef{Sha256: hex.EncodeToString(sum[:])}
		status, body := f.door(t, awaitPath(run, "gate"), good, `{"approved":true}`, nil)
		if status != 202 || !strings.Contains(string(body), `"repeat":true`) {
			t.Fatalf("status %d body %s", status, body)
		}
		f.untouched(t, "repeat")
	})
}

func TestAwaitDoor_NamespaceComesFromTheRunNeverTheRequest(t *testing.T) {
	f := newAwaitFixture(t)
	f.seedRun(t, "ork-door-ns", "team-a", map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING})
	status, body := f.door(t, awaitPath("ork-door-ns", "gate")+"?namespace=other", bearer(awaitToken),
		`{"approved":true,"namespace":"other"}`, map[string]string{"Ork-Namespace": "other"})
	if status != 202 {
		t.Fatalf("status %d: %s", status, body)
	}
	if !strings.Contains(string(body), `"namespace":"team-a"`) {
		t.Fatalf("body %s does not name the run's own namespace", body)
	}
	if puts := f.store.recorded(); len(puts) != 1 || puts[0].opts.Metadata[artifacts.MetaRunID] != "ork-door-ns" {
		t.Fatalf("stored %v", puts)
	}
}

func TestAwaitDoor_RefusesAnAwaitWithoutCallback(t *testing.T) {
	f := newAwaitFixture(t)
	f.seedRun(t, "ork-door-2", "", map[string]orkstackv1.Status{"manual": orkstackv1.Status_STATUS_AWAITING})
	status, body := f.door(t, awaitPath("ork-door-2", "manual"), bearer(awaitToken), `{}`, nil)
	if status != 404 || errorCode(t, body) != "not_found" {
		t.Fatalf("status %d body %s, want 404 even with a syntactically valid token", status, body)
	}
	f.untouched(t, "no callback")
}

// The second-broker guardrail: every non-202 row of the door leaves the store
// and the starter untouched.
func TestAwaitDoor_RefusalsStoreAndSendNothing(t *testing.T) {
	const run = "ork-door-3"
	refusals := []struct {
		path, body string
		token      *string
		seed       map[string]orkstackv1.Status
	}{
		{awaitPath(run, "gate"), `{}`, nil, nil},
		{awaitPath(run, "gate"), `{}`, bearer("wrong"), map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING}},
		{awaitPath(run, "manual"), `{}`, bearer(awaitToken), map[string]orkstackv1.Status{"manual": orkstackv1.Status_STATUS_AWAITING}},
		{awaitPath(run, "gate"), `{"approved":"no"}`, bearer(awaitToken), map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING}},
		{awaitPath(run, "gate"), `{"approved":true}`, bearer(awaitToken), nil},
		{awaitPath(run, "gate") + "/reject", `not json`, bearer(awaitToken), map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING}},
		{awaitPath(run, "gate"), `{"pad":"` + strings.Repeat("y", 300*1024) + `"}`, bearer(awaitToken), map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING}},
	}
	for i, r := range refusals {
		f := newAwaitFixture(t)
		f.seedRun(t, run, "", r.seed)
		status, body := f.door(t, r.path, r.token, r.body, nil)
		if status == 202 {
			t.Fatalf("refusal %d was accepted: %s", i, body)
		}
		f.untouched(t, fmt.Sprintf("refusal %d (%d)", i, status))
	}
}

func TestStreamTaskLogs_RefusesAnAwaitedTask(t *testing.T) {
	run := runningRun("ork-logs-await", "gate")
	run.TaskRuns[0].Status = orkstackv1.Status_STATUS_AWAITING
	run.TaskRuns[0].Await = &orkstackv1.TaskAwait{}
	fixture := newLogFixture(t, run)
	stream, err := fixture.runClient().StreamTaskLogs(context.Background(), connect.NewRequest(
		&orkstackv1.StreamTaskLogsRequest{RunId: run.GetId(), TaskId: "gate", Follow: true}))
	if err == nil {
		for stream.Receive() {
		}
		err = stream.Err()
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), `task "gate" of run "ork-logs-await" is an awaited task: it runs no container and writes no logs`) {
		t.Fatalf("error = %v", err)
	}
}
