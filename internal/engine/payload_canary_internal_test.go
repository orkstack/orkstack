package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// Canary is the run data the plaintext tests feed a sealed run through every
// door that takes any.
const Canary = "canary-5e1f9c"

// PlaintextSpy records every value a run's workflow code hands the SDK that
// the run's own converter writes unsealed, by site, after the production
// interceptors have marked them.
type PlaintextSpy struct {
	interceptor.WorkerInterceptorBase
	dc   *payloadcrypt.Converter
	run  string
	mu   sync.Mutex
	seen map[string][][]byte
	// failed is each value that did not convert: nothing is known of it.
	failed []string
	// PlainLocal are the local activities whose results the run may have
	// written in plaintext.
	PlainLocal []string
}

// NewPlaintextSpy spies on run, whose process holds keys.
func NewPlaintextSpy(run string, keys *payloadcrypt.EngineKeys) *PlaintextSpy {
	return &PlaintextSpy{dc: payloadcrypt.NewConverter(DataConverter(), keys), run: run, seen: map[string][][]byte{}}
}

// Options is a run worker's options as production builds them, under the spy.
func (s *PlaintextSpy) Options(keys *payloadcrypt.EngineKeys, o worker.Options) worker.Options {
	o.Interceptors = append([]interceptor.WorkerInterceptor{s, PayloadInterceptor(keys)}, RunWorkerInterceptors()...)
	if o.DeadlockDetectionTimeout == 0 {
		o.DeadlockDetectionTimeout = testDeadlockDetectionTimeout
	}
	return o
}

// Problems is what is wrong with what was written in plaintext: a value that
// holds the canary, and a site the test is about that wrote none.
func (s *PlaintextSpy) Problems(sites ...string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var got []string
	problems := slices.Clone(s.failed)
	for site, values := range s.seen {
		got = append(got, site)
		for _, v := range values {
			if bytes.Contains(v, []byte(Canary)) {
				problems = append(problems, fmt.Sprintf("%s is written in plaintext and holds the run's data: %s", site, v))
			}
		}
	}
	sort.Strings(got)
	for _, want := range sites {
		if !slices.ContainsFunc(got, func(g string) bool { return strings.HasPrefix(g, want) }) {
			problems = append(problems, fmt.Sprintf("no plaintext was written at %s (written at %v): the test proves nothing of it", want, got))
		}
	}
	return problems
}

func (s *PlaintextSpy) record(ctx workflow.Context, site string, v any) {
	if workflow.GetInfo(ctx).WorkflowExecution.ID != s.run {
		return
	}
	p, err := s.dc.WithWorkflowContext(ctx).ToPayload(v)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.failed = append(s.failed, fmt.Sprintf("%s: a %T does not convert: %v", site, v, err))
		return
	}
	if payloadcrypt.IsSealed(p) {
		return
	}
	s.seen[site] = append(s.seen[site], p.GetData())
}

// InterceptActivity records what a local activity of the run answers, as the
// SDK converts it for the history: with the root converter, which holds no
// run's key.
func (s *PlaintextSpy) InterceptActivity(_ context.Context, next interceptor.ActivityInboundInterceptor) interceptor.ActivityInboundInterceptor {
	return &spyActivity{ActivityInboundInterceptorBase: interceptor.ActivityInboundInterceptorBase{Next: next}, s: s}
}

type spiedCtxKey struct{}

type spyActivity struct {
	interceptor.ActivityInboundInterceptorBase
	s *PlaintextSpy
}

func (a *spyActivity) ExecuteActivity(ctx context.Context, in *interceptor.ExecuteActivityInput) (any, error) {
	// The test environment runs a mock's function as an activity inside the
	// one it stands for: what the outer one answers is what is written.
	nested := ctx.Value(spiedCtxKey{}) != nil
	out, err := a.Next.ExecuteActivity(context.WithValue(ctx, spiedCtxKey{}, true), in)
	info := activity.GetInfo(ctx)
	if nested || out == nil || !info.IsLocalActivity || info.WorkflowExecution.ID != a.s.run {
		return out, err
	}
	site := "local result " + info.ActivityType.Name
	ps, converted := out.(*commonpb.Payloads)
	var convErr error
	if !converted {
		ps, convErr = a.s.dc.WithContext(ctx).ToPayloads(out)
	}
	a.s.mu.Lock()
	defer a.s.mu.Unlock()
	if convErr != nil {
		a.s.failed = append(a.s.failed, fmt.Sprintf("%s: a %T does not convert: %v", site, out, convErr))
	}
	for _, p := range ps.GetPayloads() {
		if payloadcrypt.IsSealed(p) {
			continue
		}
		a.s.seen[site] = append(a.s.seen[site], p.GetData())
		if !slices.Contains(a.s.PlainLocal, info.ActivityType.Name) {
			a.s.failed = append(a.s.failed, site+" is written in plaintext")
		}
	}
	return out, err
}

func (s *PlaintextSpy) InterceptWorkflow(_ workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
	return &spyInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, s: s}
}

type spyInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	s *PlaintextSpy
}

func (i *spyInbound) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	return i.Next.Init(&spyOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: outbound}, s: i.s})
}

type spyOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	s *PlaintextSpy
}

func (o *spyOutbound) all(ctx workflow.Context, site string, args []any) {
	for _, a := range args {
		o.s.record(ctx, site, a)
	}
}

func (o *spyOutbound) memo(ctx workflow.Context, site string, m map[string]any) {
	for k, v := range m {
		if k == payloadcrypt.MemoField {
			continue // the wrapped key itself
		}
		o.s.record(ctx, site, v)
		o.s.record(ctx, site, payloadcrypt.Plain(k))
	}
}

func (o *spyOutbound) ExecuteActivity(ctx workflow.Context, name string, args ...any) workflow.Future {
	o.all(ctx, "activity "+name, args)
	return o.Next.ExecuteActivity(ctx, name, args...)
}

func (o *spyOutbound) ExecuteChildWorkflow(ctx workflow.Context, name string, args ...any) workflow.ChildWorkflowFuture {
	o.all(ctx, "child "+name, args)
	o.memo(ctx, "child memo", workflow.GetChildWorkflowOptions(ctx).Memo)
	return o.Next.ExecuteChildWorkflow(ctx, name, args...)
}

func (o *spyOutbound) SignalExternalWorkflow(ctx workflow.Context, id, runID, name string, arg any) workflow.Future {
	o.s.record(ctx, "signal "+name, arg)
	return o.Next.SignalExternalWorkflow(ctx, id, runID, name, arg)
}

func (o *spyOutbound) SignalChildWorkflow(ctx workflow.Context, id, name string, arg any) workflow.Future {
	o.s.record(ctx, "signal "+name, arg)
	return o.Next.SignalChildWorkflow(ctx, id, name, arg)
}

func (o *spyOutbound) UpsertMemo(ctx workflow.Context, m map[string]any) error {
	o.memo(ctx, "memo", m)
	return o.Next.UpsertMemo(ctx, m)
}

func (o *spyOutbound) NewContinueAsNewError(ctx workflow.Context, wfn any, args ...any) error {
	o.all(ctx, "continue-as-new", args)
	return o.Next.NewContinueAsNewError(ctx, wfn, args...)
}

// sealedFixture is the key field and the engine keys of a sealed run of this
// id, as a converter that reads for its activities has them.
func sealedFixture(t *testing.T, runID string) (*payloadcrypt.EngineKeys, payloadcrypt.KeyMemo, *payloadcrypt.Converter) {
	t.Helper()
	ek, err := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	must.OK(t, err)
	dk, err := payloadcrypt.NewKey(bytes.Repeat([]byte{9}, payloadcrypt.KeySize))
	must.OK(t, err)
	keys, err := payloadcrypt.NewEngineKeys(ek)
	must.OK(t, err)
	tree, memo, err := payloadcrypt.NewRunTree(context.Background(), payloadcrypt.NewStaticDataKeys("pk_t", dk), ek, "default", "ns_default", runID)
	must.OK(t, err)
	dc := payloadcrypt.NewConverter(DataConverter(), keys).WithReaderKeys(func(context.Context, payloadcrypt.Context) (*payloadcrypt.Key, error) { return tree, nil })
	return keys, memo, dc
}

// The spy's own control: a canary handed over in plaintext is a problem at
// each door, and so is a door nothing was written at.
func TestPlaintextSpy_CatchesACanaryAtEachDoor(t *testing.T) {
	keys, memo, dc := sealedFixture(t, "ork-spied")
	spy := NewPlaintextSpy("ork-spied", keys)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(dc)
	env.SetWorkerOptions(spy.Options(keys, worker.Options{}))
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-spied"})
	must.OK(t, env.SetMemoOnStart(map[string]any{payloadcrypt.MemoField: memo}))
	env.RegisterActivityWithOptions(func(context.Context, *orkstackv1.ReleasePinInput) error { return nil }, activity.RegisterOptions{Name: task.ActivityReleasePin})
	env.RegisterActivityWithOptions(func(context.Context, string) error { return nil }, activity.RegisterOptions{Name: "sealed"})
	env.RegisterActivityWithOptions(func(context.Context) (string, error) { return Canary, nil }, activity.RegisterOptions{Name: "answers"})
	env.OnSignalExternalWorkflow(mock.Anything, "gate", "", mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		local := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{ScheduleToCloseTimeout: time.Minute})
		var answer string
		if err := getLocal(local, runScope(ctx), workflow.ExecuteLocalActivity(local, "answers"), &answer); err != nil || answer != Canary {
			return fmt.Errorf("a sealed answer read as %q: %w", answer, err)
		}
		local = payloadcrypt.PlainLocalResult(local)
		if err := workflow.ExecuteLocalActivity(local, "answers").Get(local, &answer); err != nil {
			return err
		}
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		plain := payloadcrypt.PlainIn(runScope(ctx), Canary)
		if err := workflow.ExecuteActivity(ctx, "sealed", Canary).Get(ctx, nil); err != nil {
			return err
		}
		if err := workflow.ExecuteActivity(ctx, task.ActivityReleasePin, &orkstackv1.ReleasePinInput{RunId: Canary}).Get(ctx, nil); err != nil {
			return err
		}
		if err := workflow.UpsertMemo(ctx, plainMemo(ctx, map[string]any{"k": Canary})); err != nil {
			return err
		}
		return workflow.SignalExternalWorkflow(ctx, "gate", "", "s", plain).Get(ctx, nil)
	})
	must.OK(t, env.GetWorkflowError())
	problems := strings.Join(spy.Problems("child"), "\n")
	for _, want := range []string{"activity ReleasePin is written", "memo is written", "signal s is written", "local result answers is written", "no plaintext was written at child"} {
		if !strings.Contains(problems, want) {
			t.Errorf("the spy did not say %q; it said:\n%s", want, problems)
		}
	}
	if strings.Contains(problems, "activity sealed") {
		t.Errorf("a sealed argument was taken for plaintext:\n%s", problems)
	}
}

// A sealed run reads a local activity's answer only where it is sealed for
// the run, and as what the activity answered; anything else is the fixed,
// permanent failure, also where it is plaintext: nothing but this process
// answers one, and it seals.
func TestGetLocal_ReadsWhatIsSealedForTheRun(t *testing.T) {
	keys, memo, dc := sealedFixture(t, "ork-local")
	tree, err := memo.Open(keys, "")
	must.OK(t, err)
	elsewhere, err := payloadcrypt.NewScope(tree, payloadcrypt.RunContext("ork-other")).SealPayload(&commonpb.Payload{Data: []byte(`"x"`)})
	must.OK(t, err)
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetDataConverter(dc)
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: testDeadlockDetectionTimeout, Interceptors: []interceptor.WorkerInterceptor{PayloadInterceptor(keys)}})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-local"})
	must.OK(t, env.SetMemoOnStart(map[string]any{payloadcrypt.MemoField: memo}))
	for name, fn := range map[string]any{
		"bytes":     func(context.Context) ([]byte, error) { return []byte{0, 1, 2}, nil },
		"message":   func(context.Context) (*orkstackv1.ArtifactRef, error) { return &orkstackv1.ArtifactRef{Name: "a"}, nil },
		"nil":       func(context.Context) (*orkstackv1.ArtifactRef, error) { return nil, nil },
		"nothing":   func(context.Context) error { return nil },
		"text":      func(context.Context) (string, error) { return "x", nil },
		"elsewhere": func(context.Context) (converter.RawValue, error) { return converter.NewRawValue(elsewhere), nil },
	} {
		env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
	}
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{ScheduleToCloseTimeout: time.Minute})
		read := func(ctx workflow.Context, name string, ptr any) error {
			return getLocal(ctx, runScope(ctx), workflow.ExecuteLocalActivity(ctx, name), ptr)
		}
		var data []byte
		if err := read(ctx, "bytes", &data); err != nil || !bytes.Equal(data, []byte{0, 1, 2}) {
			t.Errorf("bytes read as %v, %v", data, err)
		}
		var ref *orkstackv1.ArtifactRef
		if err := read(ctx, "message", &ref); err != nil || ref.GetName() != "a" {
			t.Errorf("a message reads as %v, %v", ref, err)
		}
		ref = nil
		if err := read(ctx, "nil", &ref); err != nil || ref != nil {
			t.Errorf("no message reads as %v, %v", ref, err)
		}
		if err := read(ctx, "nothing", nil); err != nil {
			t.Errorf("nothing reads as %v", err)
		}
		text := ""
		for name, ctx := range map[string]workflow.Context{"text": payloadcrypt.PlainLocalResult(ctx), "elsewhere": payloadcrypt.PlainLocalResult(ctx)} {
			if err := read(ctx, name, &text); categoryOf(err).reasonCode() != PayloadUnreadableCode || text != "" {
				t.Errorf("%s, not sealed for the run, reads as %q, %v; want %s", name, text, err, PayloadUnreadableCode)
			}
		}
		var number int
		if err := read(ctx, "text", &number); categoryOf(err).reasonCode() != PayloadUnreadableCode {
			t.Errorf("an answer of another type reads as %v, want %s", err, PayloadUnreadableCode)
		}
		return nil
	})
	must.OK(t, env.GetWorkflowError())
}

// An answer recorded in a run's history that cannot be read stays so: the run
// says that, never that its store is away.
func TestGetLocal_AnUnreadableAnswerIsNotAnOutage(t *testing.T) {
	store := newMemSnapshotStore()
	carry := carryOver(t, store, awaitedRestoreRows(1), "team-a", refusalWorkflowID)
	runGuardWorkflow(t, func(env *testsuite.TestWorkflowEnvironment) {
		env.SetCurrentHistoryLength(restoreProbeLength)
		env.RegisterActivityWithOptions(NewSnapshotActivities(store).ReadContinuationChunk, activity.RegisterOptions{Name: ActivityReadContinuationChunk})
		env.OnActivity(ActivityReadContinuationChunk, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, payloadUnreadable("x"))
	}, func(ctx workflow.Context) error {
		s := newRunState(ctx, refusalWorkflowID, awaitedRestoreOrkflow(t), guardInput(t, map[string]any{}), refusalPlan())
		s.beginPass()
		if code, _ := s.restore(ctx, ctx, carry); code != RunStateCorruptCode {
			t.Errorf("restore ended %q, want %s", code, RunStateCorruptCode)
		}
		return nil
	})
}

// What a sealed run writes in plaintext of its concurrency slot is ids,
// sequences, times and its limit's own terms, whatever its key, its input and
// its pauses hold.
func TestPlaintext_ASealedRunsSlotRecordsHoldNoRunData(t *testing.T) {
	keys, memo, dc := sealedFixture(t, concRunID)
	spy := NewPlaintextSpy(concRunID, keys)
	h := newPausedConcHarness(t, granted(1), queued(7, 2), granted(9))
	h.env.SetDataConverter(dc)
	h.env.SetWorkerOptions(spy.Options(keys, worker.Options{}))
	must.OK(t, h.env.SetMemoOnStart(map[string]any{payloadcrypt.MemoField: memo}))
	plan := pausedConcPlan(t)
	plan.Concurrency.Slots[0].Key = Canary
	for _, p := range []struct {
		at    time.Duration
		pause bool
	}{{5 * time.Second, true}, {time.Hour, false}, {time.Hour + 30*time.Second, true}, {2 * time.Hour, false}} {
		sig := pauseSignal(p.pause, "0")
		sig["reason"], sig["principal"], sig["requestId"] = Canary, Canary, Canary
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(PauseSignal, sig) }, p.at)
	}
	wf := twoStepConcOrkflow()
	input, err := structpb.NewStruct(map[string]any{"k": Canary})
	must.OK(t, err)
	h.env.ExecuteWorkflow(RunWorkflow, concRunID, wf, &orkstackv1.Payload{Data: input}, plan, (*orkstackv1.RunContinuation)(nil))
	must.OK(t, h.env.GetWorkflowError())
	if len(h.single) != 2 || len(h.releases) != 1 {
		t.Fatalf("released %v and %v, want two single releases and the last one", h.single, h.releases)
	}
	for _, p := range spy.Problems("memo", "signal "+GateSignalRelease, "signal "+GateSignalReleaseAll, "activity "+ActivityPublishEvent) {
		t.Error(p)
	}
}

// A sealed run that cannot carry on ends with its one event sealed: of its
// message, its rows and its definition the cluster is told nothing, only the
// event's dedupe key.
func TestPlaintext_ASealedRunThatCannotCarryOnHoldsNoRunData(t *testing.T) {
	for _, code := range []string{RunStateCorruptCode, "RUN_CANCELED"} {
		t.Run(code, func(t *testing.T) {
			keys, memo, dc := sealedFixture(t, refusalWorkflowID)
			spy := NewPlaintextSpy(refusalWorkflowID, keys)
			h := newRefusalHarness(t)
			h.env.SetDataConverter(dc)
			h.env.SetWorkerOptions(spy.Options(keys, worker.Options{}))
			must.OK(t, h.env.SetMemoOnStart(map[string]any{payloadcrypt.MemoField: memo}))
			snapshot := switchSnapshotRun()
			for _, row := range snapshot.GetTaskRuns() {
				row.Error = &orkstackv1.Error{Code: "EXIT_NONZERO", Message: Canary}
			}
			carry := h.carryFor(t, unknownCarryVersion, snapshot, "team-a", refusalWorkflowID)
			wf := refusalOrkflow()
			wf.Description = Canary
			h.env.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
				s := &runState{runID: refusalWorkflowID, wf: wf, scope: runScope(ctx)}
				_, _ = s.endUnrestorable(ctx, carry, code, "stopped by "+Canary)
				return nil
			}, workflow.RegisterOptions{Name: "ends"})
			h.env.ExecuteWorkflow("ends")
			must.OK(t, h.env.GetWorkflowError())
			if len(h.events) != 1 {
				t.Fatalf("published %v, want one terminal event", h.events)
			}
			for _, p := range spy.Problems("activity " + ActivityPublishEvent) {
				t.Error(p)
			}
		})
	}
}

// A sealed run a limit kept from starting says so, in plaintext, by its
// reason alone: what its fire reads (fireOutcome).
func TestRunWorkflowRaw_ASealedRunALimitKeptFromStartingSaysSo(t *testing.T) {
	keys, memo, dc := sealedFixture(t, concRunID)
	h := newConcRunHarness(t, AcquireConcurrencyResult{Answer: gateAnswer{Proto: 2, Answer: gateAnswerFullBound}})
	h.env.SetDataConverter(dc)
	fc := payloadcrypt.NewFailureConverter(dc, true)
	h.env.SetFailureConverter(fc)
	spy := NewPlaintextSpy(concRunID, keys)
	h.env.SetWorkerOptions(spy.Options(keys, worker.Options{}))
	wf := concOrkflow()
	wf.Namespace = "default"
	plan := concPlan()
	plan.Concurrency.Slots[0].Key = Canary
	start := RunStartMemo(wf, plan)
	start[payloadcrypt.MemoField] = memo
	must.OK(t, h.env.SetMemoOnStart(start))
	tree, err := memo.Open(keys, "")
	must.OK(t, err)
	scope := payloadcrypt.NewScope(tree, payloadcrypt.RunContext(concRunID))
	h.env.ExecuteWorkflow("raw", payloadcrypt.Seal(scope, concRunID), payloadcrypt.Seal(scope, wf),
		payloadcrypt.Seal(scope, &orkstackv1.Payload{}), payloadcrypt.Seal(scope, plan))
	var sealed *payloadcrypt.SealedFailure
	if err := h.env.GetWorkflowError(); !errors.As(err, &sealed) || sealed.Reason() != RunConcurrencyFullCode+notStartedSuffix {
		t.Fatalf("the run ended %v, want its failure sealed under %s", err, RunConcurrencyFullCode+notStartedSuffix)
	}
	if code, _, notStarted := limitEnding(sealed); code != RunConcurrencyFullCode || !notStarted {
		t.Fatalf("a fire reads it as %s, not started %v", code, notStarted)
	}
	for _, p := range spy.Problems() {
		t.Error(p)
	}
}

// UnsealedErrorTypesForTest is unsealedErrorTypes.
func UnsealedErrorTypesForTest() []string { return slices.Clone(unsealedErrorTypes) }
