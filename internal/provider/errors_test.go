package provider_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/provider"
	"github.com/orkstack/orkstack/internal/task"
)

// TestClassify_TheTableOfProvidersMd11 is the whole table from a class, the
// step and the call to a reason and its category.
func TestClassify_TheTableOfProvidersMd11(t *testing.T) {
	infra, contract := task.CategoryInfrastructureError, task.CategoryContractViolation
	for _, tc := range []struct {
		class     provider.Class
		where     provider.Where
		call      provider.Call
		reason    string
		category  string
		retryable bool
	}{
		{provider.Unavailable, provider.AtSetup, provider.CallLogin, "PROVIDER_UNAVAILABLE", infra, true},
		{provider.Unavailable, provider.AtSetup, provider.CallSecret, "PROVIDER_UNAVAILABLE", infra, true},
		{provider.Unavailable, provider.AtRefresh, provider.CallCredential, "CREDENTIAL_REFRESH_FAILED", infra, true},
		{provider.Unavailable, provider.AtRefresh, provider.CallLogin, "CREDENTIAL_REFRESH_FAILED", infra, true},
		{provider.Refused, provider.AtSetup, provider.CallLogin, "CREDENTIAL_REFUSED", contract, false},
		{provider.Refused, provider.AtSetup, provider.CallCredential, "CREDENTIAL_REFUSED", contract, false},
		{provider.Refused, provider.AtRefresh, provider.CallCredential, "CREDENTIAL_REFUSED", contract, false},
		{provider.Refused, provider.AtSetup, provider.CallSecret, "SECRET_ACCESS_DENIED", contract, false},
		{provider.NotFound, provider.AtSetup, provider.CallSecret, "SECRET_NOT_FOUND", contract, false},
		{provider.NotFound, provider.AtSetup, provider.CallCredential, "SECRET_NOT_FOUND", contract, false},
		{provider.Invalid, provider.AtSetup, provider.CallSecret, "PROVIDER_REQUEST_INVALID", contract, false},
		{provider.Invalid, provider.AtRefresh, provider.CallCredential, "PROVIDER_REQUEST_INVALID", contract, false},
	} {
		got := provider.Classify(tc.class, tc.where, tc.call)
		if got.Reason != tc.reason || got.Category != tc.category || got.Retryable != tc.retryable {
			t.Errorf("Classify(%s, %d, %d) = %+v; want %s %s retryable=%v", tc.class, tc.where, tc.call, got, tc.reason, tc.category, tc.retryable)
		}
	}
}

// TestClassify_EveryReasonHasANonRetryableCategoryUnlessRetried: a reason the
// table calls "not retried" has a category Temporal does not retry, or is an
// InfrastructureError that the Orker marks non-retryable itself.
func TestClassify_EveryContractViolationIsNonRetryable(t *testing.T) {
	nonRetryable := map[string]bool{}
	for _, c := range task.NonRetryableCategories() {
		nonRetryable[c] = true
	}
	outcomes := []provider.Outcome{provider.OutcomeTooLarge, provider.OutcomeMaskCapacity, provider.OutcomeNotConfigured, provider.OutcomeContractViolation, provider.OutcomeRefreshFailed}
	for _, class := range []provider.Class{provider.Unavailable, provider.Refused, provider.NotFound, provider.Invalid} {
		for _, where := range []provider.Where{provider.AtSetup, provider.AtRefresh} {
			for _, call := range []provider.Call{provider.CallLogin, provider.CallSecret, provider.CallCredential} {
				outcomes = append(outcomes, provider.Classify(class, where, call))
			}
		}
	}
	for _, o := range outcomes {
		if o.Category == task.CategoryContractViolation && !nonRetryable[o.Category] {
			t.Errorf("%s is a ContractViolation, which Temporal retries", o.Reason)
		}
		if o.Retryable && nonRetryable[o.Category] {
			t.Errorf("%s says retryable but its category %s is not", o.Reason, o.Category)
		}
	}
}

func TestClassOf_AnErrorAProviderDidNotClassifyIsUnavailable(t *testing.T) {
	for _, err := range []error{
		errors.New("plain"),
		context.DeadlineExceeded,
		fmt.Errorf("wrapped: %w", context.Canceled),
	} {
		if c := provider.ClassOf(err); c != provider.Unavailable {
			t.Errorf("ClassOf(%v) = %s; want unavailable", err, c)
		}
	}
	wrapped := fmt.Errorf("outer: %w", &provider.Error{Class: provider.Refused, Op: "login", Code: "AccessDenied"})
	if c := provider.ClassOf(wrapped); c != provider.Refused {
		t.Errorf("ClassOf(wrapped refusal) = %s; want refused", c)
	}
}

// TestError_TextIsBuiltFromOpAndCodeOnly: the cause is never printed, and a
// code or op that is not a short token is not printed either, so a provider
// cannot pass a message, a path or a value through either field.
func TestError_TextIsBuiltFromOpAndCodeOnly(t *testing.T) {
	secret := "arn:aws:iam::123456789012:role/payments-deploy password=hunter2-hunter2"
	for _, e := range []*provider.Error{
		{Class: provider.Refused, Op: "login", Code: "AccessDenied", Err: errors.New(secret)},
		{Class: provider.Refused, Op: "login", Code: secret},
		{Class: provider.Unavailable, Op: secret, Code: "503"},
		{Class: provider.NotFound, Op: "read", Code: "a b\nc"},
	} {
		text := e.Error()
		if strings.Contains(text, "hunter2") || strings.Contains(text, "123456789012") || strings.Contains(text, "\n") {
			t.Errorf("Error() = %q; it holds the cause, a message or a control character", text)
		}
	}
	got := (&provider.Error{Class: provider.Refused, Op: "login", Code: "AccessDenied"}).Error()
	if got != "provider login: refused (AccessDenied)" {
		t.Errorf("Error() = %q", got)
	}
}

func TestRetryDelay_TheOneRetryRule(t *testing.T) {
	for _, tc := range []struct {
		n        int
		min, max time.Duration
	}{
		{1, 5 * time.Second, 10 * time.Second},
		{2, 10 * time.Second, 20 * time.Second},
		{3, 20 * time.Second, 40 * time.Second},
		{4, 30 * time.Second, 60 * time.Second},
		{9, 30 * time.Second, 60 * time.Second},
	} {
		for _, unit := range []float64{0, 0.5, 0.999999, 1, -1} {
			d := provider.RetryDelay(tc.n, provider.DefaultRetryMin, unit)
			if d < tc.min || d > tc.max {
				t.Errorf("RetryDelay(%d, 5s, %v) = %s; want within [%s, %s]", tc.n, unit, d, tc.min, tc.max)
			}
		}
	}
	if d := provider.RetryDelay(1, provider.DefaultRetryMin, 0); d != 5*time.Second {
		t.Errorf("RetryDelay(1, 5s, 0) = %s; the low end of the first draw is retry_min", d)
	}
}

func TestRefreshAfter_NeverEarlierThanAThird(t *testing.T) {
	life := 60 * time.Minute
	for _, unit := range []float64{0, 0.25, 0.5, 0.999999, 1, -1} {
		after, ok := provider.RefreshAfter(life, unit)
		if !ok || after < life/3 || after > life/3+life/12 {
			t.Errorf("RefreshAfter(1h, %v) = %s, %v; want within [20m, 25m]", unit, after, ok)
		}
	}
	if _, ok := provider.RefreshAfter(0, 0.5); ok {
		t.Error("a bundle with no lifetime is refreshed; it never is")
	}
}

func TestRefusalConfirm_ThreeIdenticalAnswersOverThirtySeconds(t *testing.T) {
	t0 := time.Unix(1_000_000, 0)
	refused := &provider.Error{Class: provider.Refused, Op: "login", Code: "AccessDenied"}
	other := &provider.Error{Class: provider.Refused, Op: "login", Code: "IDPRejectedClaim"}
	var r provider.RefusalConfirm
	if r.Observe(refused, t0) || r.Observe(refused, t0.Add(15*time.Second)) {
		t.Fatal("confirmed before the third answer")
	}
	if !r.Observe(refused, t0.Add(30*time.Second)) {
		t.Fatal("three identical refusals over 30 s were not confirmed")
	}

	r = provider.RefusalConfirm{}
	r.Observe(refused, t0)
	r.Observe(refused, t0.Add(time.Second))
	if r.Observe(refused, t0.Add(2*time.Second)) {
		t.Error("three refusals in 2 s were confirmed; they must span 30 s")
	}

	r = provider.RefusalConfirm{}
	r.Observe(refused, t0)
	r.Observe(other, t0.Add(15*time.Second))
	if r.Observe(refused, t0.Add(30*time.Second)) {
		t.Error("a different code in between did not start again")
	}

	r = provider.RefusalConfirm{}
	r.Observe(refused, t0)
	r.Observe(nil, t0.Add(10*time.Second))
	r.Observe(refused, t0.Add(20*time.Second))
	if r.Observe(refused, t0.Add(30*time.Second)) {
		t.Error("a success in between did not start again")
	}
}

func TestLimiter_BoundsCallsAndWaitingCountsAgainstTheBudget(t *testing.T) {
	l := provider.NewLimiter(2)
	ctx := context.Background()
	r1, err1 := l.Acquire(ctx)
	r2, err2 := l.Acquire(ctx)
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	budget, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := l.Acquire(budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("a third call with the budget spent = %v; want the budget's error", err)
	}
	r1()
	r1() // a second release gives back nothing more
	r3, err := l.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	spent, cancel2 := context.WithCancel(ctx)
	cancel2()
	if _, err := l.Acquire(spent); err == nil {
		t.Fatal("a release given twice freed two slots")
	}
	r2()
	r3()
}
