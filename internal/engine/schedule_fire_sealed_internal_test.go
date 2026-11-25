package engine

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// TestFireMemos_ASealedFireRecordsFixedWords: a memo is never sealed, so a
// fire that is not unsealed (one that holds a key, and one whose key is lost)
// records its code and fixed words: never a concurrency key rendered from
// inputs, a refusal's text or a run's failure. An unsealed fire records the
// text itself.
func TestFireMemos_ASealedFireRecordsFixedWords(t *testing.T) {
	const secret = "deploy-alice@example.com"
	text := "key " + secret + " was full; this fire started no run"
	key, err := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	scopes := []struct {
		name     string
		scope    *payloadcrypt.Scope
		wantText bool
	}{
		{"sealed", payloadcrypt.NewScope(key, payloadcrypt.ScheduleContext("sch-x")), false},
		{"key lost", nil, false},
		{"unsealed", payloadcrypt.Unsealed(), true},
	}
	writers := []struct {
		name, memo string
		write      func(workflow.Context) error
		wantCode   string
		wantFails  bool
	}{
		{"completeNotStarted", MemoScheduleFireSkipMessage, func(ctx workflow.Context) error {
			return completeNotStarted(ctx, FireSkippedConcurrencyCode, text)
		}, FireSkippedConcurrencyCode, false},
		{"refuseFire", MemoScheduleFireRefusal, func(ctx workflow.Context) error {
			return refuseFire(ctx, text, errors.New(text))
		}, "", true},
	}
	for _, w := range writers {
		for _, s := range scopes {
			t.Run(w.name+"/"+s.name, func(t *testing.T) {
				var suite testsuite.WorkflowTestSuite
				env := generousDeadlock(suite.NewTestWorkflowEnvironment())
				memo := map[string]string{}
				env.OnUpsertMemo(mock.Anything).Run(func(args mock.Arguments) {
					for k, v := range args.Get(0).(map[string]any) {
						memo[k] = memoText(t, v)
					}
				}).Return(nil)
				env.ExecuteWorkflow(func(ctx workflow.Context) error { return w.write(withRunScope(ctx, s.scope)) })
				if failed := env.GetWorkflowError() != nil; failed != w.wantFails {
					t.Fatalf("the fire ended %v, want failed %v", env.GetWorkflowError(), w.wantFails)
				}
				got := memo[w.memo]
				if got == "" {
					t.Fatalf("memo %v says nothing under %s", memo, w.memo)
				}
				if has := strings.Contains(fmt.Sprint(memo), secret); has != s.wantText {
					t.Fatalf("memo %v quotes the text: %v, want %v", memo, has, s.wantText)
				}
				if s.wantText && got != text {
					t.Fatalf("an unsealed fire recorded %q, want %q", got, text)
				}
				if memo[MemoScheduleFireSkipCode] != w.wantCode {
					t.Fatalf("skip code %q, want %q", memo[MemoScheduleFireSkipCode], w.wantCode)
				}
			})
		}
	}
}

// memoText is a memo value's text. That a memo is never sealed is
// TestPayloads_EveryCrossWorkflowWriteDecides's to hold, not this one's.
func memoText(t *testing.T, v any) string {
	t.Helper()
	p, err := NewDataConverter(nil).ToPayload(v)
	if err != nil {
		t.Fatal(err)
	}
	var s string
	if err := NewDataConverter(nil).FromPayload(p, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

// TestFireOutcome_ASealedRunALimitKeptFromStarting: a sealed run's failure
// reaches its fire sealed, and the fire holds no run's key. It tells a run a
// limit kept from starting from one that ran, and by which limit's ending,
// from the failure's plaintext reason alone.
func TestFireOutcome_ASealedRunALimitKeptFromStarting(t *testing.T) {
	const secret = "deploy-alice@example.com"
	key, err := payloadcrypt.NewKey(bytes.Repeat([]byte{7}, payloadcrypt.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	runScope := payloadcrypt.NewScope(key, payloadcrypt.RunContext("ork-6"))
	fireScope := payloadcrypt.NewScope(key, payloadcrypt.ScheduleContext("sch-x"))
	fc := payloadcrypt.NewFailureConverter(NewDataConverter(nil), true)
	for _, tc := range []struct {
		name, strategy, code string
		started              bool
		wantSkip             string
		wantFails            bool
	}{
		{"skip, key full", ConcurrencyStrategySkip, RunConcurrencyLimitCode, false, FireSkippedConcurrencyCode, false},
		{"refuse, key full", ConcurrencyStrategyRefuse, RunConcurrencyLimitCode, false, "", true},
		{"queue timed out", ConcurrencyStrategyQueue, RunQueueTimeoutCode, false, FireNotStartedConcurrencyCode, false},
		{"line full", ConcurrencyStrategyQueue, RunConcurrencyFullCode, false, FireNotStartedConcurrencyCode, false},
		{"it ran, then lost its slot", ConcurrencyStrategyQueue, RunConcurrencyUnavailableCode, true, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var details []any
			if !tc.started {
				details = []any{ConcurrencyRunNotStarted{NotStarted: true}}
			}
			failure := fc.ErrorToFailure(payloadcrypt.WithKey(
				temporal.NewNonRetryableApplicationError("key "+secret+" was full", tc.code, nil, details...), runScope, reasonCodeOf(
					temporal.NewNonRetryableApplicationError("", tc.code, nil, details...))))
			if strings.Contains(failure.String(), secret) {
				t.Fatalf("the cluster's record of the run's failure quotes it: %v", failure)
			}
			runErr := fc.FailureToError(failure)
			run := 0
			plan := RunPlan{Concurrency: &ConcurrencyPlan{Run: &run, Slots: []ConcurrencySlotPlan{{Key: secret, Strategy: tc.strategy}}}}
			var suite testsuite.WorkflowTestSuite
			env := generousDeadlock(suite.NewTestWorkflowEnvironment())
			memo := map[string]string{}
			env.OnUpsertMemo(mock.Anything).Run(func(args mock.Arguments) {
				for k, v := range args.Get(0).(map[string]any) {
					memo[k] = memoText(t, v)
				}
			}).Return(nil)
			env.ExecuteWorkflow(func(ctx workflow.Context) error { return fireOutcome(withRunScope(ctx, fireScope), plan, runErr) })
			if failed := env.GetWorkflowError() != nil; failed != tc.wantFails {
				t.Fatalf("the fire ended %v, want failed %v", env.GetWorkflowError(), tc.wantFails)
			}
			if memo[MemoScheduleFireSkipCode] != tc.wantSkip {
				t.Fatalf("memo %v, want skip code %q", memo, tc.wantSkip)
			}
			if tc.code == RunConcurrencyLimitCode && tc.strategy == ConcurrencyStrategyRefuse && memo[MemoScheduleFireRefusal] == "" {
				t.Fatalf("a refuse limit's full key did not refuse the fire: memo %v", memo)
			}
			if strings.Contains(fmt.Sprint(memo), secret) {
				t.Fatalf("memo %v quotes the run", memo)
			}
		})
	}
}
