package payloadcrypt

import (
	"context"
	"errors"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// rawEnv is a test environment whose workers carry the engine keys of keys
// in workflow code (WorkflowInterceptor), and whose run carries memo.
func rawEnv(t *testing.T, memo *KeyMemo, keys *EngineKeys) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	env := sealedEnv(t, memo, keys)
	env.SetWorkerOptions(worker.Options{
		DeadlockDetectionTimeout: testDeadlockDetectionTimeout,
		Interceptors:             []interceptor.WorkerInterceptor{WorkflowInterceptor(keys, "readsItsErrors")},
	})
	return env
}

// rawWorkflow opens its one raw argument with the run's key, as the
// interpreter opens its start.
func rawWorkflow(ctx workflow.Context, in converter.RawValue) (string, error) {
	st := RunKey(ctx)
	if errors.Is(st.Err(), ErrEngineKeyMissing) {
		panic(st.Err())
	}
	if st.Err() != nil {
		return "keyless", nil
	}
	var s string
	present, err := OpenRaw(st.Scope(), converter.GetDefaultDataConverter(), in, &s)
	if err != nil {
		return "unreadable", nil
	}
	if !present {
		return "absent", nil
	}
	return s, nil
}

func TestRunKey_AndOpenRaw(t *testing.T) {
	ek := testKey(t, 1)
	keys := mustEngineKeys(t, ek)
	tree, memo := testRunMemo(t, ek)
	own := NewScope(tree, RunContext("ork-root"))
	sibling := NewScope(tree, RunContext("ork-sibling"))

	cases := []struct {
		name string
		memo *KeyMemo
		arg  any
		want string
	}{
		{"a sealed start opens", &memo, Seal(own, "hello"), "hello"},
		{"a start sealed for a sibling does not", &memo, Seal(sibling, "hello"), "unreadable"},
		{"a plaintext start in a sealed run does not", &memo, "hello", "unreadable"},
		{"an unsealed run reads plaintext", nil, "hello", "hello"},
		{"an unsealed run never reads a sealed start", nil, Seal(own, "hello"), "unreadable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := rawEnv(t, tc.memo, keys)
			env.RegisterWorkflowWithOptions(rawWorkflow, workflow.RegisterOptions{Name: "raw"})
			env.ExecuteWorkflow("raw", tc.arg)
			var got string
			if err := env.GetWorkflowResult(&got); err != nil || got != tc.want {
				t.Fatalf("result = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
	t.Run("an argument not passed is absent, never unreadable", func(t *testing.T) {
		env := rawEnv(t, &memo, keys)
		env.RegisterWorkflowWithOptions(rawWorkflow, workflow.RegisterOptions{Name: "raw"})
		env.ExecuteWorkflow("raw")
		var got string
		if err := env.GetWorkflowResult(&got); err != nil || got != "absent" {
			t.Fatalf("result = %q, %v", got, err)
		}
	})
	t.Run("a worker without the interceptor holds no key: a miss", func(t *testing.T) {
		env := sealedEnv(t, &memo, keys)
		env.RegisterWorkflowWithOptions(rawWorkflow, workflow.RegisterOptions{Name: "raw"})
		env.ExecuteWorkflow("raw", Seal(own, "hello"))
		if err := env.GetWorkflowError(); err == nil {
			t.Fatal("a worker with no engine keys ran a sealed run")
		}
	})
	t.Run("a keyless memo", func(t *testing.T) {
		bad := memo
		bad.Wrap = bad.Wrap[:4]
		env := rawEnv(t, &bad, keys)
		env.RegisterWorkflowWithOptions(rawWorkflow, workflow.RegisterOptions{Name: "raw"})
		env.ExecuteWorkflow("raw", "x")
		var got string
		if err := env.GetWorkflowResult(&got); err != nil || got != "keyless" {
			t.Fatalf("result = %q, %v", got, err)
		}
	})
}

func TestOpenIfSealed(t *testing.T) {
	scope := NewScope(testKey(t, 2), RunContext("ork-1"))
	plain := jsonPayload(`1`)
	if got, err := OpenIfSealed(Unsealed(), plain); err != nil || got != plain {
		t.Fatalf("plaintext = %v, %v; want itself", got, err)
	}
	sealed, _ := SealPayload(scope.key, scope.ctx, plain)
	if got, err := OpenIfSealed(scope, sealed); err != nil || string(got.Data) != "1" {
		t.Fatalf("sealed for the run = %v, %v", got, err)
	}
	for name, s := range map[string]*Scope{"unsealed run": Unsealed(), "lost key": nil, "sibling": scope.For(RunContext("ork-2"))} {
		if _, err := OpenIfSealed(s, sealed); !errors.Is(err, ErrUnreadable) {
			t.Errorf("%s: %v, want ErrUnreadable", name, err)
		}
	}
}

func TestOpenRaw_Direct(t *testing.T) {
	dc := converter.GetDefaultDataConverter()
	var s string
	if present, err := OpenRaw(Unsealed(), dc, converter.NewRawValue(nil), &s); present || err != nil {
		t.Fatalf("nil payload = %v, %v", present, err)
	}
	if present, err := OpenRaw(nil, dc, converter.NewRawValue(&commonpb.Payload{}), &s); present || !errors.Is(err, ErrUnreadable) {
		t.Fatalf("a lost scope = %v, %v; want ErrUnreadable", present, err)
	}
}

func TestScope_SealPayload(t *testing.T) {
	plain := jsonPayload(`1`)
	if got, err := Unsealed().SealPayload(plain); err != nil || got != plain {
		t.Fatalf("Unsealed = %v, %v; want the payload itself", got, err)
	}
	var lost *Scope
	if _, err := lost.SealPayload(plain); err == nil {
		t.Fatal("a lost key sealed a payload")
	}
	scope := NewScope(testKey(t, 2), RunContext("ork-1"))
	got, err := scope.SealPayload(plain)
	if err != nil || !IsSealed(got) {
		t.Fatalf("= %v, %v", got, err)
	}
	if opened, err := scope.Open(got); err != nil || string(opened.Data) != "1" {
		t.Fatalf("opened = %v, %v", opened, err)
	}
}

// The root face opens a sealed payload through a reader's key source, for the
// context the payload names; the workflow face never calls it.
func TestConverter_ReaderKeys(t *testing.T) {
	key := testKey(t, 5)
	scope := NewScope(key, RunContext("ork-1"))
	calls := 0
	keys := func(_ context.Context, c Context) (*Key, error) {
		calls++
		if c == RunContext("ork-1") {
			return key, nil
		}
		return nil, errors.New("no such run")
	}
	c := NewConverter(converter.GetDefaultDataConverter(), nil).WithReaderKeys(keys)
	p, err := c.ToPayload(Seal(scope, "hello"))
	if err != nil {
		t.Fatal(err)
	}
	var got string
	if err := c.FromPayload(p, &got); err != nil || got != "hello" {
		t.Fatalf("FromPayload = %q, %v", got, err)
	}
	other, _ := c.ToPayload(Seal(NewScope(key, RunContext("ork-2")), "x"))
	if err := c.FromPayload(other, &got); err == nil {
		t.Fatal("a run the source has no key for opened")
	}
	if calls != 2 {
		t.Fatalf("the source was asked %d times, want 2", calls)
	}
}
