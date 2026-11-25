package payloadcrypt

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Seal and Plain hand back the very value for a run that is not sealed, so
// the SDK converts exactly what it converted before.
func TestSealAndPlain_ReturnTheValueItself(t *testing.T) {
	v := &wrapperspb.StringValue{Value: "x"}
	if got := Seal(Unsealed(), v); got != any(v) {
		t.Fatalf("Seal(Unsealed(), v) = %T, want v itself", got)
	}
	// A nil scope is a lost key: never written in plaintext.
	if _, err := NewConverter(converter.GetDefaultDataConverter(), nil).ToPayload(Seal(nil, v)); err == nil {
		t.Fatal("a value sealed under a lost key was written")
	}
	if got := Unplain(Plain(v)); got != any(v) {
		t.Fatalf("Unplain(Plain(v)) = %T, want v itself", got)
	}
}

// With nothing sealed every payload is byte for byte what the converter it
// wraps writes, and it reads back the same.
func TestConverter_UnsealedIsByteIdentical(t *testing.T) {
	inner := converter.GetDefaultDataConverter()
	for _, keys := range []*EngineKeys{nil, mustEngineKeys(t, testKey(t, 1))} {
		c := NewConverter(inner, keys)
		for _, v := range []any{nil, "s", 42, []byte{1, 2}, map[string]any{"a": 1}, &wrapperspb.StringValue{Value: "x"}, Plain("p")} {
			want, err := inner.ToPayload(Unplain(v))
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.ToPayload(v)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(want, got) {
				t.Errorf("%T: got %v, want %v", v, got, want)
			}
			wantB, _ := proto.MarshalOptions{Deterministic: true}.Marshal(want)
			gotB, _ := proto.MarshalOptions{Deterministic: true}.Marshal(got)
			if !bytes.Equal(wantB, gotB) {
				t.Errorf("%T: bytes differ", v)
			}
		}
	}
}

// A converter handed another unwraps it: the outer one's keys decide.
func TestNewConverter_UnwrapsAConverter(t *testing.T) {
	inner := NewConverter(converter.GetDefaultDataConverter(), nil)
	outer := NewConverter(inner, mustEngineKeys(t, testKey(t, 1)))
	if outer.inner != inner.inner {
		t.Fatal("a converter wrapped a converter")
	}
}

func TestConverter_SealsSealedValues(t *testing.T) {
	scope := NewScope(testKey(t, 3), RunContext("ork-1"))
	c := NewConverter(converter.GetDefaultDataConverter(), nil)
	p, err := c.ToPayload(Seal(scope, "canary-777"))
	if err != nil {
		t.Fatal(err)
	}
	if !IsSealed(p) || bytes.Contains(p.Data, []byte("canary-777")) {
		t.Fatalf("payload not sealed: %v", p)
	}
	ps, err := c.ToPayloads("plain", Seal(scope, "canary-777"))
	if err != nil {
		t.Fatal(err)
	}
	if IsSealed(ps.Payloads[0]) || !IsSealed(ps.Payloads[1]) {
		t.Fatal("ToPayloads sealed the wrong argument")
	}
	// Outside a run nothing is opened: no key is at hand.
	var s string
	if err := c.FromPayload(p, &s); !errors.Is(err, ErrUnreadable) {
		t.Fatalf("root FromPayload of a sealed payload = %v, want ErrUnreadable", err)
	}
	if got := c.ToString(p); strings.Contains(got, "canary") {
		t.Fatalf("ToString shows the plaintext: %s", got)
	}
}

// Rule 7: a raw value is carried as it is, written and read without being
// opened.
func TestConverter_RawValuesPassUnopened(t *testing.T) {
	scope := NewScope(testKey(t, 3), RunContext("ork-1"))
	c := NewConverter(converter.GetDefaultDataConverter(), nil)
	sealed, err := c.ToPayload(Seal(scope, "v"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.ToPayload(converter.NewRawValue(sealed))
	if err != nil || !proto.Equal(out, sealed) {
		t.Fatalf("raw write = %v, %v", out, err)
	}
	var raw converter.RawValue
	if err := c.FromPayload(sealed, &raw); err != nil || !proto.Equal(raw.Payload(), sealed) {
		t.Fatalf("raw read = %v, %v", raw.Payload(), err)
	}
}

func TestConverter_RefusesNestedWrappers(t *testing.T) {
	scope := NewScope(testKey(t, 3), RunContext("ork-1"))
	c := NewConverter(converter.GetDefaultDataConverter(), nil)
	for _, v := range []any{Seal(scope, Seal(scope, "x")), Seal(scope, PlainBinary(&wrapperspb.StringValue{})), PlainBinary(nil)} {
		if _, err := c.ToPayload(v); err == nil {
			t.Errorf("%T was written", v)
		}
	}
	if _, err := c.ToPayload(Seal(NewScope(nil, RunContext("ork-1")), "secret")); err == nil {
		t.Error("a scope with no key wrote its value")
	}
	if got := Seal(Unsealed(), "v"); got != any("v") {
		t.Fatalf("Seal(Unsealed(), v) = %v, want v itself", got)
	}
}

// A child of the tree is sealed under the same key for its own id.
func TestScope_For(t *testing.T) {
	scope := NewScope(testKey(t, 3), RunContext("ork-root"))
	child := scope.For(RunContext("ork-child"))
	p, err := SealPayload(child.key, child.Context(), jsonPayload(`1`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := child.Open(p); err != nil {
		t.Fatalf("the child's scope does not open its own payload: %v", err)
	}
	if _, err := scope.Open(p); !errors.Is(err, ErrUnreadable) {
		t.Fatalf("the parent's scope opened its child's payload: %v", err)
	}
	if Unsealed().For(RunContext("x")) != Unsealed() {
		t.Fatal("Unsealed().For is not Unsealed()")
	}
}

func TestConverter_PlainBinaryWritesBinaryProto(t *testing.T) {
	c := NewConverter(converter.GetDefaultDataConverter(), nil)
	m := &wrapperspb.BytesValue{Value: []byte{0, 1, 2, 255}}
	p, err := c.ToPayload(PlainBinary(m))
	if err != nil {
		t.Fatal(err)
	}
	if got := string(p.Metadata["encoding"]); got != "binary/protobuf" {
		t.Fatalf("encoding = %q", got)
	}
	var back wrapperspb.BytesValue
	if err := c.FromPayload(p, &back); err != nil || !bytes.Equal(back.Value, m.Value) {
		t.Fatalf("read back %v, %v", back.Value, err)
	}
}

func mustEngineKeys(t *testing.T, ks ...*Key) *EngineKeys {
	t.Helper()
	e, err := NewEngineKeys(ks...)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// testDeadlockDetectionTimeout is the deadlock detector's budget in this
// package's mocked-environment tests, as in internal/engine's harnesses: left
// unset, the SDK applies its production 1 s to every workflow task, so a
// loaded host, not the code under test, could fail an assertion.
const testDeadlockDetectionTimeout = 60 * time.Second

// sealedEnv is a test environment whose run carries memo's key field, with
// the converter built over engineKeys.
func sealedEnv(t *testing.T, memo *KeyMemo, engineKeys *EngineKeys) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{DeadlockDetectionTimeout: testDeadlockDetectionTimeout})
	dc := NewConverter(converter.GetDefaultDataConverter(), engineKeys)
	if memo != nil {
		// The activity worker's way to the run's key, as an engine's or an
		// Orker's reader has it.
		m := *memo
		dc = dc.WithReaderKeys(func(context.Context, Context) (*Key, error) { return m.Open(engineKeys, "") })
	}
	env.SetDataConverter(dc)
	env.SetFailureConverter(NewFailureConverter(dc, false))
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "ork-root"})
	if memo != nil {
		if err := env.SetMemoOnStart(map[string]any{MemoField: *memo}); err != nil {
			t.Fatal(err)
		}
	}
	env.SetWorkflowRunTimeout(time.Minute)
	return env
}

func echoWorkflow(ctx workflow.Context, in string) (string, error) { return in, nil }

func sealActivity(ctx context.Context, v string) (any, error) {
	scope := activityScope(ctx)
	return Seal(scope, v+"-from-activity"), nil
}

// activityScope stands in for the key an Orker is given for its run (P20b).
var testScope *Scope

func activityScope(context.Context) *Scope { return testScope }

func activityWorkflow(ctx workflow.Context, in string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
	var out string
	err := workflow.ExecuteActivity(ctx, "sealActivity", in).Get(ctx, &out)
	return out, err
}

// The workflow-side face opens a sealed start and a sealed activity result
// from the run's own memo and a key file, and calls nothing.
func TestConverter_WorkflowFaceOpensFromMemo(t *testing.T) {
	ek := testKey(t, 1)
	tree, memo := testRunMemo(t, ek)
	scope := NewScope(tree, RunContext("ork-root"))
	testScope = scope
	t.Cleanup(func() { testScope = nil })

	env := sealedEnv(t, &memo, mustEngineKeys(t, ek))
	env.RegisterWorkflow(activityWorkflow)
	env.RegisterActivityWithOptions(sealActivity, activity.RegisterOptions{Name: "sealActivity"})
	env.ExecuteWorkflow("activityWorkflow", Seal(scope, "in"))
	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow: completed=%v err=%v", env.IsWorkflowCompleted(), env.GetWorkflowError())
	}
	var out string
	if err := env.GetWorkflowResult(&out); err != nil || out != "in-from-activity" {
		t.Fatalf("result = %q, %v", out, err)
	}
}

// A payload sealed for another run, or under another tree's key, does not
// open: an error to workflow code, never a panic.
func TestConverter_WorkflowFaceRefusesAnotherRunsPayload(t *testing.T) {
	ek := testKey(t, 1)
	_, memo := testRunMemo(t, ek)
	otherTree, _ := DeriveRunTreeKey(testKey(t, 0x42), "ork-other")
	for name, scope := range map[string]*Scope{
		"another run's context": NewScope(mustTreeKey(t, memo, ek), RunContext("ork-sibling")),
		"another tree's key":    NewScope(otherTree, RunContext("ork-root")),
	} {
		t.Run(name, func(t *testing.T) {
			env := sealedEnv(t, &memo, mustEngineKeys(t, ek))
			env.RegisterWorkflow(echoWorkflow)
			env.ExecuteWorkflow("echoWorkflow", Seal(scope, "in"))
			err := env.GetWorkflowError()
			if err == nil || !strings.Contains(err.Error(), "PAYLOAD_UNREADABLE") {
				t.Fatalf("workflow error = %v, want PAYLOAD_UNREADABLE", err)
			}
			if strings.Contains(err.Error(), "panic") {
				t.Fatalf("a payload that does not open panicked: %v", err)
			}
		})
	}
}

func mustTreeKey(t *testing.T, m KeyMemo, ek *Key) *Key {
	t.Helper()
	k, err := m.Open(mustEngineKeys(t, ek), "")
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// Rule 3: the one condition that panics is a memo naming an engines' key
// this process does not hold, and it panics at the first open, whatever the
// payload.
func TestConverter_MissPanics(t *testing.T) {
	ek := testKey(t, 1)
	_, memo := testRunMemo(t, ek)
	for name, keys := range map[string]*EngineKeys{
		"another key file":           mustEngineKeys(t, testKey(t, 2)),
		"encryption off (the guard)": nil,
	} {
		t.Run(name, func(t *testing.T) {
			env := sealedEnv(t, &memo, keys)
			env.RegisterWorkflow(echoWorkflow)
			env.ExecuteWorkflow("echoWorkflow", "plain start")
			err := env.GetWorkflowError()
			var panicErr interface{ StackTrace() string }
			if err == nil || !errors.As(err, &panicErr) || !strings.Contains(err.Error(), ErrEngineKeyMissing.Error()) {
				t.Fatalf("workflow error = %v, want the guard's panic", err)
			}
		})
	}
}

// The guard is asked at a write too: an engine that lacks the run's key
// writes nothing of it, even before it reads.
func TestConverter_MissPanicsOnAWrite(t *testing.T) {
	ek := testKey(t, 1)
	_, memo := testRunMemo(t, ek)
	env := sealedEnv(t, &memo, mustEngineKeys(t, testKey(t, 2)))
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		return workflow.ExecuteActivity(ctx, "sealActivity", "x").Get(ctx, nil)
	}, workflow.RegisterOptions{Name: "writer"})
	env.RegisterActivityWithOptions(sealActivity, activity.RegisterOptions{Name: "sealActivity"})
	env.ExecuteWorkflow("writer")
	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), ErrEngineKeyMissing.Error()) {
		t.Fatalf("workflow error = %v, want the guard's panic", err)
	}
}

// A payload marked sealed in a run whose memo carries no key is not a miss:
// it opens for nobody and is an error, never a panic.
func TestConverter_SealedPayloadInUnsealedRun(t *testing.T) {
	scope := NewScope(testKey(t, 9), RunContext("ork-root"))
	env := sealedEnv(t, nil, mustEngineKeys(t, testKey(t, 1)))
	env.RegisterWorkflow(echoWorkflow)
	env.ExecuteWorkflow("echoWorkflow", Seal(scope, "in"))
	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), "PAYLOAD_UNREADABLE") || strings.Contains(err.Error(), "panic") {
		t.Fatalf("workflow error = %v, want PAYLOAD_UNREADABLE without a panic", err)
	}
}

// A memo key field that is malformed makes the run keyless: no payload of it
// opens, and nothing panics.
func TestConverter_MalformedMemoIsKeyless(t *testing.T) {
	ek := testKey(t, 1)
	_, memo := testRunMemo(t, ek)
	memo.Wrap = memo.Wrap[:4]
	env := sealedEnv(t, &memo, mustEngineKeys(t, ek))
	env.RegisterWorkflow(echoWorkflow)
	env.ExecuteWorkflow("echoWorkflow", "plain start")
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("an unsealed start in a keyless run: %v", err)
	}
}

func TestRunKeyOf(t *testing.T) {
	ek := testKey(t, 1)
	keys := mustEngineKeys(t, ek)
	tree, memo := testRunMemo(t, ek)
	memoPayload := func(v any) *commonpb.Memo {
		p, err := converter.GetDefaultDataConverter().ToPayload(v)
		if err != nil {
			t.Fatal(err)
		}
		return &commonpb.Memo{Fields: map[string]*commonpb.Payload{MemoField: p}}
	}
	t.Run("not sealed", func(t *testing.T) {
		st := RunKeyOf(nil, keys, "ork-root", "")
		if st.Sealed() || st.Err() != nil || st.Scope() != Unsealed() {
			t.Fatalf("state = %+v", st)
		}
	})
	t.Run("sealed", func(t *testing.T) {
		st := RunKeyOf(memoPayload(memo), keys, "ork-root", "")
		if !st.Sealed() || st.Err() != nil || st.Scope().Context() != RunContext("ork-root") || st.Scope().key.ID() != tree.ID() ||
			st.Namespace() != "payments" {
			t.Fatalf("state = %+v", st)
		}
	})
	t.Run("keyless", func(t *testing.T) {
		st := RunKeyOf(memoPayload(map[string]any{"v": 1}), keys, "ork-root", "")
		if !errors.Is(st.Err(), ErrKeyless) {
			t.Fatalf("state = %+v", st)
		}
	})
	t.Run("miss", func(t *testing.T) {
		st := RunKeyOf(memoPayload(memo), mustEngineKeys(t, testKey(t, 7)), "ork-root", "")
		if !errors.Is(st.Err(), ErrEngineKeyMissing) || st.Scope() != nil {
			t.Fatalf("state = %+v", st)
		}
	})
	t.Run("a memo field that is not json", func(t *testing.T) {
		p, _ := converter.GetDefaultDataConverter().ToPayload(&structpb.Value{})
		st := RunKeyOf(&commonpb.Memo{Fields: map[string]*commonpb.Payload{MemoField: p}}, keys, "ork-root", "")
		if !errors.Is(st.Err(), ErrKeyless) {
			t.Fatalf("state = %+v", st)
		}
	})
}
