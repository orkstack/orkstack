package payloadcrypt

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// A local activity's error is written by the SDK into the run's history
// through the failure converter: it leaves with its run's key, so a process
// that seals neither writes it in plaintext nor withholds it.
func TestLocalActivity_AnErrorLeavesWithItsRunsKey(t *testing.T) {
	ek := testKey(t, 1)
	keys := mustEngineKeys(t, ek)
	tree, memo := testRunMemo(t, ek)
	// A run started without the memo, whose process holds keys, is not sealed.
	// A workflow of another type that carries no key is nobody's to write in
	// plaintext: withheld.
	for name, c := range map[string]struct {
		memo             *KeyMemo
		workflow         string
		sealed, withheld bool
	}{
		"a sealed run":                 {&memo, "readsItsErrors", true, false},
		"a sealed one of another type": {&memo, "other", true, false},
		"a run that is not sealed":     {nil, "readsItsErrors", false, false},
		"another type that has no key": {nil, "other", false, true},
	} {
		t.Run(name, func(t *testing.T) {
			env := rawEnv(t, c.memo, keys)
			fc := NewFailureConverter(NewConverter(converter.GetDefaultDataConverter(), keys), true)
			env.RegisterActivityWithOptions(func(context.Context) error {
				return temporal.NewNonRetryableApplicationError("canary", "CHILD_INPUT_ARTIFACT_MISSING", nil)
			}, activity.RegisterOptions{Name: "claim"})
			var got error
			var id string
			env.RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
				id = workflow.GetInfo(ctx).WorkflowExecution.ID
				ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
					ScheduleToCloseTimeout: time.Minute, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
				got = workflow.ExecuteLocalActivity(ctx, "claim").Get(ctx, nil)
				return nil
			}, workflow.RegisterOptions{Name: c.workflow})
			env.ExecuteWorkflow(c.workflow)
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			// The SDK writes a local activity's error into the history and
			// hands the workflow what it reads back of it; the test environment
			// only wraps it.
			got = fc.FailureToError(fc.ErrorToFailure(errors.Unwrap(got)))
			var sf *SealedFailure
			if errors.As(got, &sf) != c.sealed {
				t.Fatalf("read back as %v, want sealed %v", got, c.sealed)
			}
			opened, err := OpenFailure(NewScope(tree, RunContext(id)), fc, got)
			if err != nil {
				t.Fatal(err)
			}
			var app *temporal.ApplicationError
			if c.withheld {
				if strings.Contains(opened.Error(), "canary") || !errors.As(opened, &app) || app.Type() != "CHILD_INPUT_ARTIFACT_MISSING" {
					t.Fatalf("read back as %v, want it withheld but for its type", opened)
				}
				return
			}
			if !errors.As(opened, &app) || app.Message() != "canary" || app.Type() != "CHILD_INPUT_ARTIFACT_MISSING" {
				t.Fatalf("read back as %v, want the activity's own text and type", opened)
			}
			// What the history holds of a sealed run's is its reason alone.
			if c.sealed && sf.f.GetMessage() != "CHILD_INPUT_ARTIFACT_MISSING" {
				t.Fatalf("the history's message is %q", sf.f.GetMessage())
			}
		})
	}
}

// A local activity's result is written by the SDK into the run's history with
// the root converter, which holds no run's key: in a sealed run it leaves
// sealed for the run, or not at all.
func TestLocalActivity_AResultLeavesWithItsRunsKey(t *testing.T) {
	ek := testKey(t, 1)
	keys := mustEngineKeys(t, ek)
	tree, memo := testRunMemo(t, ek)
	lost := memo
	lost.Wrap = append([]byte(nil), memo.Wrap...)
	lost.Wrap[0] ^= 1
	for name, c := range map[string]struct {
		memo           *KeyMemo
		plain          bool
		sealed, failed bool
	}{
		"a sealed run":                        {&memo, false, true, false},
		"a sealed run's, marked plain":        {&memo, true, false, false},
		"a run that is not sealed":            {nil, false, false, false},
		"a run whose key is lost":             {&lost, false, false, true},
		"one whose key is lost, marked plain": {&lost, true, false, false},
	} {
		t.Run(name, func(t *testing.T) {
			env := rawEnv(t, c.memo, keys)
			env.RegisterActivityWithOptions(func(context.Context) (string, error) { return "canary", nil }, activity.RegisterOptions{Name: "reads"})
			var raw converter.RawValue
			var got error
			var id string
			env.ExecuteWorkflow(func(ctx workflow.Context) error {
				id = workflow.GetInfo(ctx).WorkflowExecution.ID
				ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
					ScheduleToCloseTimeout: time.Minute, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
				if c.plain {
					ctx = PlainLocalResult(ctx)
				}
				got = workflow.ExecuteLocalActivity(ctx, "reads").Get(ctx, &raw)
				return nil
			})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			if (got != nil) != c.failed {
				t.Fatalf("the read gave %v, want failed %v", got, c.failed)
			}
			if c.failed {
				return
			}
			p := raw.Payload()
			if IsSealed(p) != c.sealed {
				t.Fatalf("the result is written as %v, want sealed %v", p, c.sealed)
			}
			if c.sealed {
				opened, err := NewScope(tree, RunContext(id)).Open(p)
				if err != nil {
					t.Fatal(err)
				}
				p = opened
			}
			if string(p.GetData()) != `"canary"` {
				t.Fatalf("the result reads back as %s", p.GetData())
			}
		})
	}
}

// A result that cannot be sealed is never asked for again, and one beside an
// error never hides the error.
func TestLocalActivity_AResultThatCannotBeSealed(t *testing.T) {
	ek := testKey(t, 1)
	keys := mustEngineKeys(t, ek)
	_, memo := testRunMemo(t, ek)
	memo.Wrap = append([]byte(nil), memo.Wrap...)
	memo.Wrap[0] ^= 1
	for name, fails := range map[string]error{"the activity succeeds": nil, "it fails": temporal.NewNonRetryableApplicationError("text", "ITS_OWN", nil)} {
		t.Run(name, func(t *testing.T) {
			env := rawEnv(t, &memo, keys)
			ran := 0
			env.RegisterActivityWithOptions(func(context.Context) (string, error) { ran++; return "canary", fails }, activity.RegisterOptions{Name: "reads"})
			var got error
			env.ExecuteWorkflow(func(ctx workflow.Context) error {
				ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
					ScheduleToCloseTimeout: time.Minute, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3}})
				var s string
				got = workflow.ExecuteLocalActivity(ctx, "reads").Get(ctx, &s)
				return nil
			})
			if err := env.GetWorkflowError(); err != nil {
				t.Fatal(err)
			}
			var app *temporal.ApplicationError
			if !errors.As(got, &app) || !app.NonRetryable() || ran != 1 || (app.Type() == "ITS_OWN") != (fails != nil) {
				t.Fatalf("ran %d times and ended %v", ran, got)
			}
		})
	}
}

// A local activity that panics never reaches the interceptor's mark: the SDK
// writes what it panicked with, which a process that seals withholds.
func TestLocalActivity_APanicIsWithheld(t *testing.T) {
	ek := testKey(t, 1)
	keys := mustEngineKeys(t, ek)
	_, memo := testRunMemo(t, ek)
	env := rawEnv(t, &memo, keys)
	env.RegisterActivityWithOptions(func(context.Context) (string, error) { panic("canary") }, activity.RegisterOptions{Name: "panics"})
	var got error
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			ScheduleToCloseTimeout: time.Minute, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
		got = workflow.ExecuteLocalActivity(ctx, "panics").Get(ctx, nil)
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if got == nil || !strings.Contains(got.Error(), "canary") {
		t.Fatalf("the activity ended %v, want its panic", got)
	}
	f := NewFailureConverter(NewConverter(converter.GetDefaultDataConverter(), keys), true).ErrorToFailure(errors.Unwrap(got))
	if strings.Contains(f.String(), "canary") {
		t.Fatalf("the history's failure quotes the panic: %v", f)
	}
}

// Workflow code that panics fails its workflow task, whose failure the cluster
// keeps: withheld by a process that seals, whatever it panicked with.
func TestWorkflowPanic_IsWithheld(t *testing.T) {
	ek := testKey(t, 1)
	keys := mustEngineKeys(t, ek)
	_, memo := testRunMemo(t, ek)
	env := rawEnv(t, &memo, keys)
	env.ExecuteWorkflow(func(workflow.Context) error { panic("canary") })
	err := env.GetWorkflowError()
	var panicked *temporal.PanicError
	if !errors.As(err, &panicked) || !strings.Contains(panicked.Error(), "canary") {
		t.Fatalf("the workflow ended %v, want its panic", err)
	}
	f := NewFailureConverter(NewConverter(converter.GetDefaultDataConverter(), keys), true).ErrorToFailure(panicked)
	if strings.Contains(f.String(), "canary") {
		t.Fatalf("the workflow task's failure quotes the panic: %v", f)
	}
}

// An activity dispatched to a worker is not marked: only its Orker, or the
// engine's own boundary, knows what its failure carries. And a process that
// holds no key marks nothing.
func TestLocalActivity_OnlyLocalAndOnlyWithKeysIsMarked(t *testing.T) {
	ek := testKey(t, 1)
	_, memo := testRunMemo(t, ek)
	boom := errors.New("boom")
	run := func(keys *EngineKeys, memo *KeyMemo, local bool) error {
		env := rawEnv(t, memo, keys)
		env.RegisterActivityWithOptions(func(context.Context) error { return boom }, activity.RegisterOptions{Name: "fails"})
		var got error
		env.ExecuteWorkflow(func(ctx workflow.Context) error {
			once := &temporal.RetryPolicy{MaximumAttempts: 1}
			if local {
				ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{ScheduleToCloseTimeout: time.Minute, RetryPolicy: once})
				got = workflow.ExecuteLocalActivity(ctx, "fails").Get(ctx, nil)
				return nil
			}
			ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: time.Minute, RetryPolicy: once})
			got = workflow.ExecuteActivity(ctx, "fails").Get(ctx, nil)
			return nil
		})
		if err := env.GetWorkflowError(); err != nil {
			t.Fatal(err)
		}
		if got == nil {
			t.Fatal("the activity did not fail")
		}
		return got
	}
	var ke *keyedError
	if errors.As(run(nil, nil, true), &ke) {
		t.Error("a process without keys marked a local activity's error")
	}
	if errors.As(run(mustEngineKeys(t, ek), &memo, false), &ke) {
		t.Error("an activity that is not local was marked")
	}
}
