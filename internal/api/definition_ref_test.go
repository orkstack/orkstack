package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/coder/websocket"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// A definition passed by reference, the submitting engine's half (spec 5.1)
// and the readers' (5.3).

// definitionObjects is an in-memory DefinitionObjectWriter that records every
// write, in order, into a log a starter can share, and can be made to fail.
type definitionObjects struct {
	mu      sync.Mutex
	objects map[string][]byte // run id → bytes
	writes  []string          // "write <run id>"
	log     *[]string
	err     error
}

func newDefinitionObjects(log *[]string) *definitionObjects {
	return &definitionObjects{objects: map[string][]byte{}, log: log}
}

func (d *definitionObjects) WriteContinuation(_ context.Context, namespace, runID string, generation int64, kind string, data []byte) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		// Shaped as S3RunSnapshotStore's: the error quotes the object's URI.
		key, err := artifacts.RunContinuationKey(namespace, runID, generation, kind, artifacts.ContinuationSHA256(data))
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("artifacts: put continuation %q: %w", "s3://test/"+key, d.err)
	}
	if generation != 0 || kind != artifacts.ContinuationKindDefinition || namespace == "" {
		return "", fmt.Errorf("unexpected write %s/%s gen %d kind %s", namespace, runID, generation, kind)
	}
	d.objects[runID] = bytes.Clone(data)
	d.writes = append(d.writes, "write "+runID)
	if d.log != nil {
		*d.log = append(*d.log, "write "+runID)
	}
	return "s3://test/" + runID, nil
}

func (d *definitionObjects) written() map[string][]byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[string][]byte{}
	for k, v := range d.objects {
		out[k] = v
	}
	return out
}

// recordingStarter records every start it is asked for — the run id, the
// start definition and the plan's JSON — into a log it can share with the
// object writer, refusing the first rejections ids as already taken.
type recordingStarter struct {
	fakeStarter
	mu         sync.Mutex
	rejections int
	log        *[]string
	attempts   []startAttempt
}

type startAttempt struct {
	runID string
	wf    *orkstackv1.Orkflow
	plan  []byte
}

func (s *recordingStarter) StartRun(ctx context.Context, runID string, wf *orkstackv1.Orkflow, input *orkstackv1.Payload, plan engine.RunPlan) error {
	encoded, _ := json.Marshal(plan)
	s.mu.Lock()
	s.attempts = append(s.attempts, startAttempt{runID: runID, wf: wf, plan: encoded})
	if s.log != nil {
		*s.log = append(*s.log, "start "+runID)
	}
	reject := s.rejections > 0
	if reject {
		s.rejections--
	}
	s.mu.Unlock()
	if reject {
		return errors.Join(engine.ErrRunIDTaken, serviceerror.NewWorkflowExecutionAlreadyStarted(
			"Workflow execution is already running", "start-request-id", "temporal-run-id"))
	}
	return s.fakeStarter.StartRun(ctx, runID, wf, input, plan)
}

// largeDoc is an Orkflow of n chained tasks, each `with:` carrying pad bytes
// of a deterministic pseudo-random note: over a start payload's 256 KiB
// when n·pad is, and about pad/2 bytes compressed per task (hex).
func largeDoc(name string, n, pad int) string {
	r := rand.New(rand.NewSource(int64(n*pad + len(name))))
	var b strings.Builder
	fmt.Fprintf(&b, "apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"1.0.0\"\ntasks:\n", name)
	for i := 0; i < n; i++ {
		note := make([]byte, pad/2)
		r.Read(note)
		fmt.Fprintf(&b, "  - id: t%04d\n    uses: shell\n    with: {op: exec, note: \"%x\"}\n", i, note)
		if i > 0 {
			fmt.Fprintf(&b, "    needs: [t%04d]\n", i-1)
		}
	}
	return b.String()
}

// byReferenceConfig is a deployment with the flag on.
func byReferenceConfig(starter api.RunStarter, objects api.DefinitionObjectWriter, caps engine.CapabilityChecker, bytes int64) api.Config {
	return api.Config{
		Orkflows: engine.NewMemoryOrkflowStore(), Starter: starter, Runs: engine.NewRunStore(), Bus: engine.NewEventBus(),
		ContinuationEnabled: true, ContinuationBytes: bytes, DefinitionByReference: true,
		DefinitionObjects: objects, Capabilities: caps,
	}
}

// TestPrepare_LargeDefinitionTravelsByReference: with the flag on, a planned
// definition over a start payload's 256 KiB travels by reference. The start
// carries the stub — name, version, namespace and digest, no tasks — the
// plan names the object with no URI, the fleet is asked for
// definition-ref once, and the object written under the started run's id is
// exactly the planned definition.
func TestPrepare_LargeDefinitionTravelsByReference(t *testing.T) {
	starter := &fakeStarter{}
	objects := newDefinitionObjects(nil)
	caps := &fakeCapabilities{}
	svc := api.NewService(byReferenceConfig(starter, objects, caps, 0))
	if err := applyDoc(t, svc, largeDoc("large", 400, 1000)); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if err := submitName(svc, "large"); err != nil {
		t.Fatalf("submit: %v", err)
	}
	started := starter.startedRuns()
	if len(started) != 1 {
		t.Fatalf("started %v, want one run", started)
	}
	stub, plan := starter.orkflows[0], starter.plans[0]
	if len(stub.GetTasks()) != 0 || stub.GetName() != "large" || stub.GetVersion() != "1.0.0" ||
		stub.GetNamespace() != "default" || stub.GetDefinitionDigest() == "" {
		t.Fatalf("the start carried %d tasks of %s@%s in %q (digest %q), want the stub", len(stub.GetTasks()),
			stub.GetName(), stub.GetVersion(), stub.GetNamespace(), stub.GetDefinitionDigest())
	}
	if size, _ := task.PayloadSize(stub); size > 1024 {
		t.Fatalf("the stub is %d bytes", size)
	}
	if plan.Definition == nil || len(plan.Definition.Parts) == 0 {
		t.Fatal("the plan names no definition by reference")
	}
	encoded, _ := json.Marshal(plan.Definition)
	if strings.Contains(strings.ToLower(string(encoded)), "uri") || strings.Contains(string(encoded), "s3://") {
		t.Fatalf("the plan's reference carries a location: %s", encoded)
	}
	if fmt.Sprint(caps.asked) != fmt.Sprintf("[%s/%d]", engine.CapabilityDefinitionRef, engine.ScopeWorkflow) {
		t.Fatalf("the fleet was asked %v, want definition-ref on the workflow channel", caps.asked)
	}
	data, ok := objects.written()[started[0]]
	if !ok {
		t.Fatalf("no definition was written under %s: %v", started[0], objects.writes)
	}
	loaded, err := engine.DecodeDefinitionObject(data, plan.Definition, stub)
	if err != nil {
		t.Fatalf("the written object does not decode against the plan and the stub: %v", err)
	}
	stored, err := svc.GetOrkflow(context.Background(), connect.NewRequest(&orkstackv1.GetOrkflowRequest{Name: "large"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.GetTasks()) != 400 || loaded.GetDefinitionDigest() != stored.Msg.GetOrkflow().GetDefinitionDigest() ||
		loaded.GetSourceYaml() != "" {
		t.Fatalf("the object holds %d tasks (digest %q, source %d bytes), want the planned definition's 400 and no source",
			len(loaded.GetTasks()), loaded.GetDefinitionDigest(), len(loaded.GetSourceYaml()))
	}
}

// TestPrepare_MeasuresTheStubAgainstTheStartBound: the start payload bound
// measures what actually travels. A definition over 256 KiB is refused with
// the flag off (today's refusal) and starts with it on; and the run input is
// still bounded on its own when the definition travels by reference.
func TestPrepare_MeasuresTheStubAgainstTheStartBound(t *testing.T) {
	doc := largeDoc("large", 400, 1000)
	off := &fakeStarter{}
	offCfg := byReferenceConfig(off, newDefinitionObjects(nil), &fakeCapabilities{}, 0)
	offCfg.DefinitionByReference = false
	offSvc := api.NewService(offCfg)
	if err := applyDoc(t, offSvc, doc); err != nil {
		t.Fatal(err)
	}
	err := submitName(offSvc, "large")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), "orkflow large@1.0.0 serializes to") {
		t.Fatalf("submit with the flag off = %v, want today's refusal", err)
	}
	on := &fakeStarter{}
	onSvc := api.NewService(byReferenceConfig(on, newDefinitionObjects(nil), &fakeCapabilities{}, 0))
	if err := applyDoc(t, onSvc, doc); err != nil {
		t.Fatal(err)
	}
	if err := submitName(onSvc, "large"); err != nil {
		t.Fatalf("submit with the flag on: %v", err)
	}
	// The input bound still holds on its own.
	inputDoc := strings.Replace(doc, "tasks:\n", "inputs:\n  blob: {type: string}\ntasks:\n", 1)
	inputDoc = strings.Replace(inputDoc, "name: large\n", "name: large-input\n", 1)
	if err := applyDoc(t, onSvc, inputDoc); err != nil {
		t.Fatal(err)
	}
	input := &orkstackv1.Payload{Data: mustStruct(t, map[string]any{"blob": strings.Repeat("x", task.MaxActivityInputBytes+1)})}
	_, err = onSvc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "large-input", Input: input}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), task.ReasonInputTooLarge) {
		t.Fatalf("an oversized input beside a by-reference definition = %v, want INPUT_TOO_LARGE", err)
	}
}

// TestPrepare_RefusesADefinitionOverAQuarterOfTheCap: a definition whose
// compressed object is over a quarter of the copy bound a generation may
// take is refused at submit — FAILED_PRECONDITION, "split it into composed
// Orkflows" — and nothing is started or written.
func TestPrepare_RefusesADefinitionOverAQuarterOfTheCap(t *testing.T) {
	doc := largeDoc("large", 400, 1000) // about 200 KB compressed
	objects := newDefinitionObjects(nil)
	roomyCfg := byReferenceConfig(&fakeStarter{}, objects, &fakeCapabilities{}, 0)
	roomy := api.NewService(roomyCfg)
	if err := applyDoc(t, roomy, doc); err != nil {
		t.Fatal(err)
	}
	// A deployment whose copy bound is 512 KiB carries at most 128 KiB.
	tight := byReferenceConfig(&fakeStarter{}, objects, &fakeCapabilities{}, 512<<10)
	tight.Orkflows = roomyCfg.Orkflows
	starter := tight.Starter.(*fakeStarter)
	svc := api.NewService(tight)
	err := submitName(svc, "large")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "orkflow large@1.0.0's planned definition is") ||
		!strings.Contains(err.Error(), "over the 0.1 MiB a run may carry forward; split it into composed Orkflows") {
		t.Fatalf("submit = %v, want the quarter-of-the-cap refusal", err)
	}
	if len(starter.startedRuns()) != 0 || len(objects.written()) != 0 {
		t.Fatalf("a refused submit started %v and wrote %d objects", starter.startedRuns(), len(objects.written()))
	}
}

// TestPrepare_DefinitionByReferenceRefusedWhileAnOlderEngineProcessPolls:
// while an engine process that could decide the run lacks definition-ref,
// the submit is refused as today, with a sentence counting the processes —
// never naming one; the identities go to the engine's log. An unreadable
// fleet, or none wired, refuses UNAVAILABLE.
func TestPrepare_DefinitionByReferenceRefusedWhileAnOlderEngineProcessPolls(t *testing.T) {
	doc := largeDoc("large", 400, 1000)
	identities := []string{"4242@engine-a.internal@ ork-caps=each-items-10k", "4343@engine-b.internal@"}
	var logs bytes.Buffer
	starter := &fakeStarter{}
	objects := newDefinitionObjects(nil)
	cfg := byReferenceConfig(starter, objects, &fakeCapabilities{lacking: identities}, 0)
	cfg.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	svc := api.NewService(cfg)
	if err := applyDoc(t, svc, doc); err != nil {
		t.Fatal(err)
	}
	err := submitName(svc, "large")
	if connect.CodeOf(err) != connect.CodeFailedPrecondition ||
		!strings.Contains(err.Error(), "orkflow large@1.0.0 serializes to") ||
		!strings.Contains(err.Error(), "2 engine processes run a release that cannot yet read a definition this large") {
		t.Fatalf("submit = %v, want today's refusal with the count of older processes", err)
	}
	for _, identity := range identities {
		host := strings.Split(identity, "@")[1]
		if strings.Contains(err.Error(), host) {
			t.Fatalf("the refusal names a process: %v", err)
		}
		if !strings.Contains(logs.String(), host) {
			t.Fatalf("the engine's log does not name %s: %s", host, logs.String())
		}
	}
	if len(starter.startedRuns()) != 0 || len(objects.written()) != 0 {
		t.Fatalf("a refused submit started %v and wrote %d objects", starter.startedRuns(), len(objects.written()))
	}
	for name, caps := range map[string]engine.CapabilityChecker{
		"an unreadable fleet": &fakeCapabilities{err: errors.New("describe failed")},
		"no cluster wired":    nil,
	} {
		other := byReferenceConfig(&fakeStarter{}, newDefinitionObjects(nil), caps, 0)
		other.Orkflows = cfg.Orkflows
		if err := submitName(api.NewService(other), "large"); connect.CodeOf(err) != connect.CodeUnavailable {
			t.Fatalf("%s: submit = %v, want unavailable", name, err)
		}
	}
	// A definition that fits a start payload never asks the fleet.
	small := &fakeCapabilities{lacking: identities}
	smallCfg := byReferenceConfig(&fakeStarter{}, newDefinitionObjects(nil), small, 0)
	smallSvc := api.NewService(smallCfg)
	if err := applyDoc(t, smallSvc, largeDoc("small", 3, 100)); err != nil {
		t.Fatal(err)
	}
	if err := submitName(smallSvc, "small"); err != nil || small.questions() != 0 {
		t.Fatalf("a small definition = %v after %d fleet questions, want started with none", err, small.questions())
	}
}

// TestApply_RefusesADefinitionThatCouldNotBeCarried: with the flag on, apply
// refuses a definition whose compressed object would be over a quarter of
// the copy bound — INVALID_ARGUMENT at tasks, nothing stored — and validate
// reports it as an issue at tasks. With the flag off apply takes it, as
// today (the submit refuses it at the start bound).
func TestApply_RefusesADefinitionThatCouldNotBeCarried(t *testing.T) {
	doc := largeDoc("large", 400, 1000)
	cfg := byReferenceConfig(&fakeStarter{}, newDefinitionObjects(nil), &fakeCapabilities{}, 512<<10)
	store := cfg.Orkflows.(*engine.MemoryOrkflowStore)
	svc := api.NewService(cfg)
	err := applyDoc(t, svc, doc)
	if connect.CodeOf(err) != connect.CodeInvalidArgument || !strings.Contains(err.Error(), "split it into composed Orkflows") {
		t.Fatalf("apply = %v, want invalid_argument", err)
	}
	if _, getErr := store.Get(context.Background(), "default", "large", ""); getErr == nil {
		t.Fatal("a refused apply stored a definition")
	}
	resp, err := svc.ValidateOrkflow(context.Background(), connect.NewRequest(&orkstackv1.ValidateOrkflowRequest{Yaml: doc}))
	if err != nil || len(resp.Msg.GetIssues()) != 1 || resp.Msg.GetIssues()[0].GetField() != "tasks" {
		t.Fatalf("validate = %v, %v; want one issue at tasks", resp.Msg.GetIssues(), err)
	}
	off := cfg
	off.DefinitionByReference = false
	off.Orkflows = engine.NewMemoryOrkflowStore()
	if err := applyDoc(t, api.NewService(off), doc); err != nil {
		t.Fatalf("apply with the flag off: %v", err)
	}
}

// TestStartPreparedRun_WritesTheDefinitionUnderTheFinalRunID: the object is
// written under each run id immediately before the start that uses it — the
// first id, which the cluster refuses as taken, and the reissued one — the
// plan is byte-identical across the reissue, and the run starts under the
// second id.
func TestStartPreparedRun_WritesTheDefinitionUnderTheFinalRunID(t *testing.T) {
	var log []string
	starter := &recordingStarter{rejections: 1, log: &log}
	objects := newDefinitionObjects(&log)
	svc := api.NewService(byReferenceConfig(starter, objects, &fakeCapabilities{}, 0))
	if err := applyDoc(t, svc, largeDoc("large", 400, 1000)); err != nil {
		t.Fatal(err)
	}
	resp, err := svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "large"}))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if len(starter.attempts) != 2 {
		t.Fatalf("%d start attempts, want 2", len(starter.attempts))
	}
	first, second := starter.attempts[0].runID, starter.attempts[1].runID
	if want := []string{"write " + first, "start " + first, "write " + second, "start " + second}; fmt.Sprint(log) != fmt.Sprint(want) {
		t.Fatalf("order = %v, want %v", log, want)
	}
	if !bytes.Equal(starter.attempts[0].plan, starter.attempts[1].plan) {
		t.Fatal("the plan changed across the reissue")
	}
	if resp.Msg.GetRun().GetId() != second || fmt.Sprint(starter.startedRuns()) != fmt.Sprintf("[%s]", second) {
		t.Fatalf("the run is %s, started %v; want %s", resp.Msg.GetRun().GetId(), starter.startedRuns(), second)
	}
	written := objects.written()
	if !bytes.Equal(written[first], written[second]) || len(written[second]) == 0 {
		t.Fatal("the two ids hold different definition objects")
	}
}

// TestStartPreparedRun_DefinitionWriteFailureStartsNothing: a definition
// that cannot be stored ends the submission UNAVAILABLE through the failed
// start's path — RUN_START_FAILED, with its stream and archive record — and
// nothing is started.
func TestStartPreparedRun_DefinitionWriteFailureStartsNothing(t *testing.T) {
	starter := &fakeStarter{}
	objects := newDefinitionObjects(nil)
	objects.err = errors.New("bucket unreachable")
	cfg := byReferenceConfig(starter, objects, &fakeCapabilities{}, 0)
	svc := api.NewService(cfg)
	if err := applyDoc(t, svc, largeDoc("large", 400, 1000)); err != nil {
		t.Fatal(err)
	}
	err := submitName(svc, "large")
	if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), "bucket unreachable") {
		t.Fatalf("submit = %v, want unavailable naming the store's error", err)
	}
	if len(starter.startedRuns()) != 0 {
		t.Fatalf("started %v", starter.startedRuns())
	}
	runs := cfg.Runs.List(engine.ListFilter{})
	if len(runs) != 1 || runs[0].GetError().GetCode() != "RUN_START_FAILED" {
		t.Fatalf("recorded %v, want one RUN_START_FAILED run", runs)
	}
	// Neither the caller nor the recorded run is told the object's name,
	// which spells its generation and kind (AGENTS.md §7): the reads hide it
	// the same way.
	for what, text := range map[string]string{"the submit's error": err.Error(), "the recorded run's error": runs[0].GetError().GetMessage()} {
		if strings.Contains(text, "-"+artifacts.ContinuationKindDefinition+"-") || strings.Contains(text, ".pb.gz") ||
			!strings.Contains(text, "<definition>") {
			t.Fatalf("%s = %q, want the object's name replaced by <definition>", what, text)
		}
	}
	if events := cfg.Bus.Events(runs[0].GetId(), 0); len(events) != 2 {
		t.Fatalf("published %d events, want RUN_CREATED and RUN_FAILED", len(events))
	}
}

// TestPrepareScheduledRun_WritesTheDefinitionLast: a fire's preparation
// writes the definition object under the run id it minted, after every
// refusal, and hands the fire the stub; a refused preparation — here its
// one-record bound — writes nothing.
func TestPrepareScheduledRun_WritesTheDefinitionLast(t *testing.T) {
	objects := newDefinitionObjects(nil)
	var counter int
	cfg := byReferenceConfig(&fakeStarter{}, objects, &fakeCapabilities{}, 0)
	cfg.Namespaces = newFakeRegistry()
	cfg.NewRunID = func() string { counter++; return fmt.Sprintf("ork-sched-%d", counter) }
	svc := api.NewService(cfg)
	doc := strings.Replace(largeDoc("large", 400, 1000), "tasks:\n", "inputs:\n  blob: {type: string, default: \"\"}\ntasks:\n", 1)
	if err := applyDoc(t, svc, doc); err != nil {
		t.Fatal(err)
	}
	acts := api.NewScheduleActivities(svc, fixedIndexes{})
	rec := &orkstackv1.ScheduleRecord{ScheduleId: "sch-4a1c0e2b7d3f8156", Name: "nightly", Namespace: "default",
		OrkflowName: "large", Spec: dailySpec()}
	prep, err := acts.PrepareScheduledRun(context.Background(), rec, time.Now())
	if err != nil {
		t.Fatalf("PrepareScheduledRun: %v", err)
	}
	var plan engine.RunPlan
	if err := json.Unmarshal(prep.GetPlan(), &plan); err != nil || plan.Definition == nil {
		t.Fatalf("plan = %v (%v), want one naming the definition", plan.Definition, err)
	}
	if len(prep.GetOrkflow().GetTasks()) != 0 || proto.Size(prep) > task.MaxActivityInputBytes {
		t.Fatalf("the preparation carries %d tasks in %d bytes, want the stub within 256 KiB",
			len(prep.GetOrkflow().GetTasks()), proto.Size(prep))
	}
	data, ok := objects.written()[prep.GetRun().GetId()]
	if !ok {
		t.Fatalf("no object under the minted %s: %v", prep.GetRun().GetId(), objects.writes)
	}
	if _, err := engine.DecodeDefinitionObject(data, plan.Definition, prep.GetOrkflow()); err != nil {
		t.Fatalf("the written object does not decode: %v", err)
	}
	// Refused by the preparation's own bound, checked after the run id is
	// minted: nothing written.
	before := len(objects.written())
	rec.Input = &orkstackv1.Payload{Data: mustStruct(t, map[string]any{"blob": strings.Repeat("x", task.MaxActivityInputBytes-8<<10)})}
	if _, err := acts.PrepareScheduledRun(context.Background(), rec, time.Now()); err == nil ||
		!strings.Contains(err.Error(), "prepared run exceeds 256 KiB") {
		t.Fatalf("an oversized preparation = %v, want its refusal", err)
	}
	if len(objects.written()) != before {
		t.Fatalf("a refused preparation wrote %d objects", len(objects.written())-before)
	}
}

// byReferenceReadStart turns the fixture run's start into one passed by
// reference, and returns the counter of object reads its resolution makes;
// broken, when set, makes every resolution fail as a missing object does.
func byReferenceReadStart(t *testing.T, reader *fakeReader, runID string, wf *orkstackv1.Orkflow, plan engine.RunPlan, broken bool) *int {
	t.Helper()
	data, ref, err := engine.EncodeDefinitionObject(wf)
	if err != nil {
		t.Fatal(err)
	}
	plan.Definition = &ref
	reader.setStart(runID, &engine.RunStartRecord{Orkflow: engine.DefinitionStub(wf), Plan: plan, ByReference: true})
	reads := new(int)
	reader.mu.Lock()
	reader.definitions = func(_ context.Context, asked string, start *engine.RunStartRecord) (*orkstackv1.Orkflow, error) {
		reader.mu.Lock()
		*reads++
		reader.mu.Unlock()
		if broken || asked != runID {
			return nil, fmt.Errorf("engine: run %s: read its definition: %w", asked, artifacts.ErrNotFound)
		}
		return engine.DecodeDefinitionObject(data, start.Plan.Definition, start.Orkflow)
	}
	reader.mu.Unlock()
	return reads
}

// TestRunNamespaceIn_IssuesNoObjectRead: the stream's tenancy check — under
// StreamRunEvents, both WebSocket upgrades and the source-run check at
// submit — answers for a run whose definition travels by reference from its
// start event alone, with no object read, and answers correctly while the
// object is missing: a run of the namespace is served, one of another
// namespace is NotFound.
func TestRunNamespaceIn_IssuesNoObjectRead(t *testing.T) {
	f := newForeignFixture(t)
	wf := &orkstackv1.Orkflow{Name: "hello-ork", Version: "0.1.0", Namespace: "default",
		Tasks: []*orkstackv1.Task{{Id: "hello", Image: &orkstackv1.ImageRef{Reference: "alpine"}}}}
	reads := byReferenceReadStart(t, f.reader, "ork-foreign-1", wf, engine.RunPlan{Namespace: "default"}, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sent := 0
	if err := f.svc.StreamRunEvents(ctx, "ork-foreign-1", "", 0, func(*orkstackv1.Event) error { sent++; return nil }); err != nil || sent != 4 {
		t.Fatalf("StreamRunEvents = %v after %d events, want the run's 4", err, sent)
	}
	ws := strings.Replace(f.baseURL, "http://", "ws://", 1)
	for _, path := range []string{"/ws/runs/ork-foreign-1", "/ws/runs/ork-foreign-1/tasks/hello/logs"} {
		conn, resp, err := websocket.Dial(ctx, ws+path, nil)
		if err != nil {
			t.Fatalf("dial %s: %v (%v)", path, err, resp)
		}
		conn.CloseNow()
	}
	_, err := f.svc.SubmitRun(ctx, connect.NewRequest(&orkstackv1.SubmitRunRequest{OrkflowName: "no-such-orkflow",
		SourceRunId: "ork-foreign-1"}))
	if connect.CodeOf(err) != connect.CodeNotFound || !strings.Contains(err.Error(), "no-such-orkflow") {
		t.Fatalf("a submit naming the run as its source = %v, want past the source check to the orkflow lookup", err)
	}
	if *reads != 0 {
		t.Fatalf("the tenancy checks read the definition object %d times, want none", *reads)
	}
	// The same run, started in namespace beta, asked for in default.
	beta := proto.Clone(wf).(*orkstackv1.Orkflow)
	beta.Namespace = "beta"
	reads = byReferenceReadStart(t, f.reader, "ork-foreign-1", beta, engine.RunPlan{Namespace: "beta"}, true)
	if err := f.svc.StreamRunEvents(ctx, "ork-foreign-1", "", 0, func(*orkstackv1.Event) error { return nil }); connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("a run of namespace beta asked for in default = %v, want NotFound", err)
	}
	if *reads != 0 {
		t.Fatalf("the tenancy checks read the definition object %d times, want none", *reads)
	}
}

// TestAwaitDoors_ResolveTheDefinitionReference: both await doors check a
// resolution against the definition's tasks, so for a run whose definition
// travels by reference they resolve it — one read per resolution — and an
// unreadable object is Unavailable (503 at the HTTP door), naming the run.
func TestAwaitDoors_ResolveTheDefinitionReference(t *testing.T) {
	const runID = "ork-await-ref-1"
	statuses := map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING}
	t.Run("ResolveAwait", func(t *testing.T) {
		f := newAwaitFixture(t)
		f.seedRun(t, runID, "", statuses)
		reads := byReferenceReadStart(t, f.reader, runID, f.wf, startPlan(t, f.reader, runID), false)
		if _, err := f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: runID, TaskId: "gate", ValuesJson: `{"approved":true}`}, ""); err != nil {
			t.Fatalf("ResolveAwait: %v", err)
		}
		if *reads != 1 || len(f.starter.delivered()) != 1 {
			t.Fatalf("%d definition reads and %d resolutions, want one of each", *reads, len(f.starter.delivered()))
		}
		// The definition's own schema still applies.
		if _, err := f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: runID, TaskId: "gate", ValuesJson: `{"approved":1}`}, ""); connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Fatalf("a schema violation = %v, want invalid_argument", err)
		}
	})
	t.Run("ResolveAwait, object unreadable", func(t *testing.T) {
		f := newAwaitFixture(t)
		f.seedRun(t, runID, "", statuses)
		byReferenceReadStart(t, f.reader, runID, f.wf, startPlan(t, f.reader, runID), true)
		_, err := f.resolve(&orkstackv1.ResolveAwaitRequest{RunId: runID, TaskId: "gate", ValuesJson: `{"approved":true}`}, "")
		if connect.CodeOf(err) != connect.CodeUnavailable || !strings.Contains(err.Error(), runID) {
			t.Fatalf("ResolveAwait = %v, want unavailable naming the run", err)
		}
		f.untouched(t, "unreadable definition")
	})
	t.Run("the callback door", func(t *testing.T) {
		f := newAwaitFixture(t)
		f.seedRun(t, runID, "", statuses)
		reads := byReferenceReadStart(t, f.reader, runID, f.wf, startPlan(t, f.reader, runID), false)
		if status, body := f.door(t, awaitPath(runID, "gate"), bearer(awaitToken), `{"approved":true}`, nil); status != 202 {
			t.Fatalf("status %d: %s", status, body)
		}
		if *reads != 1 {
			t.Fatalf("%d definition reads, want 1", *reads)
		}
	})
	t.Run("the callback door, object unreadable", func(t *testing.T) {
		f := newAwaitFixture(t)
		f.seedRun(t, runID, "", statuses)
		byReferenceReadStart(t, f.reader, runID, f.wf, startPlan(t, f.reader, runID), true)
		status, body := f.door(t, awaitPath(runID, "gate"), bearer(awaitToken), `{"approved":true}`, nil)
		if status != 503 || errorCode(t, body) != "unavailable" || !strings.Contains(string(body), runID) {
			t.Fatalf("status %d body %s, want 503 naming the run", status, body)
		}
		f.untouched(t, "unreadable definition at the door")
	})
}

// TestAwaitCallback_ReadsNoObjectBeforeTheToken: at the callback door a
// caller without the await's token never causes an object read: a missing
// or wrong bearer token and an await with no callback credential answer
// 401/404 exactly as for an inline run, with zero reads; a valid token reads
// the object once.
func TestAwaitCallback_ReadsNoObjectBeforeTheToken(t *testing.T) {
	const runID = "ork-await-ref-2"
	statuses := map[string]orkstackv1.Status{"gate": orkstackv1.Status_STATUS_AWAITING}
	for _, c := range []struct {
		name   string
		path   string
		token  *string
		status int
	}{
		{"no token", awaitPath(runID, "gate"), nil, 401},
		{"a wrong token", awaitPath(runID, "gate"), bearer("nope"), 401},
		{"an await with no callback", awaitPath(runID, "manual"), bearer(awaitToken), 404},
		{"an unknown task", awaitPath(runID, "nothing"), bearer(awaitToken), 404},
	} {
		t.Run(c.name, func(t *testing.T) {
			inline := newAwaitFixture(t)
			inline.seedRun(t, runID, "", statuses)
			inlineStatus, _ := inline.door(t, c.path, c.token, `{"approved":true}`, nil)

			f := newAwaitFixture(t)
			f.seedRun(t, runID, "", statuses)
			reads := byReferenceReadStart(t, f.reader, runID, f.wf, startPlan(t, f.reader, runID), false)
			status, body := f.door(t, c.path, c.token, `{"approved":true}`, nil)
			if status != c.status || inlineStatus != c.status {
				t.Fatalf("status %d (inline %d), want %d: %s", status, inlineStatus, c.status, body)
			}
			if *reads != 0 {
				t.Fatalf("%d object reads before the token was verified, want none", *reads)
			}
		})
	}
	f := newAwaitFixture(t)
	f.seedRun(t, runID, "", statuses)
	reads := byReferenceReadStart(t, f.reader, runID, f.wf, startPlan(t, f.reader, runID), false)
	if status, body := f.door(t, awaitPath(runID, "gate"), bearer(awaitToken), `{"approved":true}`, nil); status != 202 || *reads != 1 {
		t.Fatalf("a valid token: status %d after %d reads, want 202 after 1: %s", status, *reads, body)
	}
}

// startPlan is the plan seedRun recorded for runID.
func startPlan(t *testing.T, reader *fakeReader, runID string) engine.RunPlan {
	t.Helper()
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.starts[runID].Plan
}

// mustStruct is values as a protobuf Struct.
func mustStruct(t *testing.T, values map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
