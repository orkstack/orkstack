package orker

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/payloadcrypt"
)

// staticTaskKeys answers the keys of the run trees it holds, in tests only:
// no setting selects it, and production Orkers ask the engine (P20b).
type staticTaskKeys map[payloadcrypt.Context]*payloadcrypt.Key

func (s staticTaskKeys) KeyFor(_ context.Context, c payloadcrypt.Context) (*payloadcrypt.Key, error) {
	if k, ok := s[c]; ok {
		return k, nil
	}
	return nil, errors.New("no key for " + c.String())
}

func taskKey(t *testing.T, fill byte) *payloadcrypt.Key {
	t.Helper()
	k, err := payloadcrypt.NewKey(bytes.Repeat([]byte{fill}, payloadcrypt.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func sealedInput(t *testing.T, scope *payloadcrypt.Scope, in *orkstackv1.ExecuteTaskInput) converter.RawValue {
	t.Helper()
	p, err := payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil).ToPayload(payloadcrypt.Seal(scope, in))
	if err != nil {
		t.Fatal(err)
	}
	return converter.NewRawValue(p)
}

func reasonOf(t *testing.T, err error) (string, bool) {
	t.Helper()
	var app *temporal.ApplicationError
	if !errors.As(err, &app) {
		t.Fatalf("error %v is not an application error", err)
	}
	var detail *orkstackv1.ActivityFailureDetail
	if derr := app.Details(&detail); derr != nil {
		t.Fatalf("details: %v", derr)
	}
	return detail.GetReasonCode(), app.NonRetryable()
}

func TestOpenTaskInput(t *testing.T) {
	ctx := context.Background()
	key := taskKey(t, 3)
	own := payloadcrypt.NewScope(key, payloadcrypt.RunContext("ork-1"))
	in := &orkstackv1.ExecuteTaskInput{RunId: "ork-1", TaskId: "build"}
	keys := staticTaskKeys{payloadcrypt.RunContext("ork-1"): key}

	t.Run("plaintext is read as it is", func(t *testing.T) {
		got, scope, err := openTaskInput(ctx, nil, sealedInput(t, payloadcrypt.Unsealed(), in))
		if err != nil || got.GetTaskId() != "build" || scope != payloadcrypt.Unsealed() {
			t.Fatalf("= %v, %v, %v", got, scope, err)
		}
	})
	t.Run("sealed opens with the run tree's key", func(t *testing.T) {
		got, scope, err := openTaskInput(ctx, keys, sealedInput(t, own, in))
		if err != nil || got.GetTaskId() != "build" || !scope.Sealed() || scope.Context() != payloadcrypt.RunContext("ork-1") {
			t.Fatalf("= %v, %v, %v", got, scope, err)
		}
	})
	cases := []struct {
		name      string
		keys      TaskKeys
		raw       converter.RawValue
		reason    string
		retryable bool
	}{
		{"no key source: not configured", nil, sealedInput(t, own, in), ReasonPayloadEncryptionNotConfigured, false},
		{"the source has no key: retried", staticTaskKeys{}, sealedInput(t, own, in), ReasonPayloadKeyUnavailable, true},
		{"the key does not open it", staticTaskKeys{payloadcrypt.RunContext("ork-1"): taskKey(t, 4)}, sealedInput(t, own, in), ReasonPayloadUnreadable, false},
		{"sealed for another run than its input names", staticTaskKeys{payloadcrypt.RunContext("ork-2"): key},
			sealedInput(t, payloadcrypt.NewScope(key, payloadcrypt.RunContext("ork-2")), in), ReasonPayloadUnreadable, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := openTaskInput(ctx, tc.keys, tc.raw)
			reason, nonRetryable := reasonOf(t, err)
			if reason != tc.reason || nonRetryable == tc.retryable {
				t.Fatalf("reason %s non-retryable %v, want %s retryable %v", reason, nonRetryable, tc.reason, tc.retryable)
			}
		})
	}
}

// A sealed run's result and failure leave sealed under its key; an unsealed
// run's as they always did.
func TestSealTaskResult(t *testing.T) {
	scope := payloadcrypt.NewScope(taskKey(t, 3), payloadcrypt.RunContext("ork-1"))
	dc := payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil)
	res := &orkstackv1.ExecuteTaskResult{TaskId: "build", ValuesJson: []byte(`{"secret":"canary"}`)}
	v, err := sealTaskResult(withTaskScope(context.Background(), scope), res, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := dc.ToPayload(v)
	if err != nil || !payloadcrypt.IsSealed(p) || bytes.Contains(p.GetData(), []byte("canary")) {
		t.Fatalf("result payload = %v, %v", p, err)
	}
	if v, _ := sealTaskResult(context.Background(), res, nil); v != any(res) {
		t.Fatalf("an unsealed run's result = %T, want the result itself", v)
	}
	failure := (&Failure{Reason: ReasonValuesMissing, Message: "canary"}).TemporalError()
	_, err = sealTaskResult(withTaskScope(context.Background(), scope), nil, failure)
	f := payloadcrypt.NewFailureConverter(dc, true).ErrorToFailure(err)
	if f.GetMessage() != ReasonValuesMissing || bytes.Contains([]byte(f.String()), []byte("canary")) {
		t.Fatalf("failure = %v, want the reason code as its plaintext and the rest sealed", f)
	}
}

// A sealed run's heartbeat details are sealed under its key; an unsealed
// run's are the progress itself.
func TestHeartbeatDetail_IsSealedWithTheAttemptsScope(t *testing.T) {
	scope := payloadcrypt.NewScope(taskKey(t, 3), payloadcrypt.RunContext("ork-1"))
	progress := HeartbeatProgress{}
	if got, ok := heartbeatDetail(context.Background(), progress).(HeartbeatProgress); !ok {
		t.Fatalf("an unsealed attempt's detail = %T, want the progress itself", got)
	}
	got := heartbeatDetail(withTaskScope(context.Background(), scope), progress)
	p, err := payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil).ToPayload(got)
	if err != nil || !payloadcrypt.IsSealed(p) {
		t.Fatalf("a sealed attempt's detail = %v, %v", p, err)
	}
}

// The SDK compares ErrResultPending by identity: an attempt whose outcome the
// Orker reports itself returns it unwrapped, sealed run or not.
func TestSealTaskResult_KeepsResultPending(t *testing.T) {
	scope := payloadcrypt.NewScope(taskKey(t, 3), payloadcrypt.RunContext("ork-1"))
	for _, s := range []*payloadcrypt.Scope{scope, payloadcrypt.Unsealed()} {
		if _, err := sealTaskResult(withTaskScope(context.Background(), s), nil, activity.ErrResultPending); err != activity.ErrResultPending {
			t.Fatalf("%v: ErrResultPending came back as %T", s, err)
		}
	}
}

// A report the Orker sends on an attempt's behalf is sealed under the
// attempt's run key; an unsealed attempt's is what it was.
func TestSealReport(t *testing.T) {
	scope := payloadcrypt.NewScope(taskKey(t, 3), payloadcrypt.RunContext("ork-1"))
	res := &orkstackv1.ExecuteTaskResult{TaskId: "build", ValuesJson: []byte(`{"v":"canary"}`)}
	got, _ := sealReport(context.Background(), res, nil)
	if got != any(res) {
		t.Fatalf("an unsealed attempt's report = %T, want the result itself", got)
	}
	got, err := sealReport(withTaskScope(context.Background(), scope), res, nil)
	p, perr := payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil).ToPayload(got)
	if err != nil || perr != nil || !payloadcrypt.IsSealed(p) {
		t.Fatalf("a sealed attempt's report = %v, %v, %v", p, err, perr)
	}
}

// An unsealed run's report keeps its reason and details through a sealing
// failure converter: it is marked as an unsealed run's.
func TestSealReport_UnsealedKeepsItsReasonWhenSealing(t *testing.T) {
	report := (&Failure{Reason: ReasonOrkerStopped, Message: "handed back"}).TemporalError()
	// On an Orker that does not seal, the error is itself: the SDK's own
	// failure converter writes it as it always did.
	if _, err := sealReport(context.Background(), nil, report); err != report {
		t.Fatalf("an Orker that does not seal reported %T, want the error itself", err)
	}
	if _, err := sealTaskResult(context.Background(), nil, report); err != report {
		t.Fatalf("an Orker that does not seal returned %T, want the error itself", err)
	}
	_, err := sealReport(withTaskScope(context.Background(), payloadcrypt.Unsealed()), nil, report)
	fc := payloadcrypt.NewFailureConverter(payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil), true)
	f := fc.ErrorToFailure(err)
	if f.GetMessage() == payloadcrypt.Withheld {
		t.Fatalf("an unsealed run's hand-back was withheld: %v", f)
	}
	reason, _ := reasonOf(t, fc.FailureToError(f))
	if reason != ReasonOrkerStopped {
		t.Fatalf("reason = %q, want %s", reason, ReasonOrkerStopped)
	}
}

// With no source of keys a refusal is the error itself, for any failure
// converter; with one it keeps its reason through a sealing converter.
func TestOpenTaskInput_RefusalsKeepTheirReason(t *testing.T) {
	scope := payloadcrypt.NewScope(taskKey(t, 3), payloadcrypt.RunContext("ork-1"))
	raw := sealedInput(t, scope, &orkstackv1.ExecuteTaskInput{RunId: "ork-1"})
	for name, tc := range map[string]struct {
		keys TaskKeys
		fc   converter.FailureConverter
		want string
	}{
		"no source, the SDK's converter": {nil, temporal.GetDefaultFailureConverter(), ReasonPayloadEncryptionNotConfigured},
		"a source, a sealing converter": {staticTaskKeys{}, payloadcrypt.NewFailureConverter(
			payloadcrypt.NewConverter(converter.GetDefaultDataConverter(), nil), true), ReasonPayloadKeyUnavailable},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := openTaskInput(context.Background(), tc.keys, raw)
			f := tc.fc.ErrorToFailure(err)
			if got := f.GetApplicationFailureInfo().GetType(); got != CategoryInfrastructureError {
				t.Fatalf("type = %q, want %s", got, CategoryInfrastructureError)
			}
			if reason, _ := reasonOf(t, tc.fc.FailureToError(f)); reason != tc.want {
				t.Fatalf("reason = %q, want %s", reason, tc.want)
			}
		})
	}
}
