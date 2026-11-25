package payloadcrypt

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"
)

const canary = "canary-0xDEADBEEF"

func failureBytes(t *testing.T, f *failurepb.Failure) []byte {
	t.Helper()
	b, err := proto.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newFC(sealing bool) *FailureConverter {
	return NewFailureConverter(NewConverter(converter.GetDefaultDataConverter(), nil), sealing)
}

// A keyed error is sealed whole: every level's message and stack trace, and
// every detail; its plaintext is the type and the reason code.
func TestFailureConverter_SealsAKeyedErrorWhole(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	inner := fmt.Errorf("render %s: %w", canary, errors.New("deep "+canary))
	app := temporal.NewApplicationErrorWithCause("task failed: "+canary, "ActivityFailed", inner, "detail "+canary)
	err := WithKey(app, scope, "TEMPLATE_RENDER_FAILED")

	fc := newFC(true)
	f := fc.ErrorToFailure(err)
	if bytes.Contains(failureBytes(t, f), []byte("canary")) {
		t.Fatalf("a canary reached the failure in plaintext: %v", f)
	}
	if f.GetMessage() != "TEMPLATE_RENDER_FAILED" {
		t.Errorf("message = %q, want the reason code", f.GetMessage())
	}
	if f.GetApplicationFailureInfo().GetType() != "ActivityFailed" {
		t.Errorf("type = %q, want the category kept", f.GetApplicationFailureInfo().GetType())
	}
	for l := f; l != nil; l = l.GetCause() {
		if l.GetStackTrace() != "" || !IsSealed(l.GetEncodedAttributes()) {
			t.Errorf("level %q not sealed: %v", l.GetMessage(), l)
		}
	}

	// Read back by the SDK's side: still sealed, opened by the interpreter.
	got := fc.FailureToError(f)
	var sf *SealedFailure
	if !errors.As(got, &sf) || strings.Contains(got.Error(), "canary") {
		t.Fatalf("FailureToError = %T %v, want a SealedFailure that quotes nothing", got, got)
	}
	opened, oerr := OpenFailure(scope, fc, got)
	if oerr != nil {
		t.Fatal(oerr)
	}
	var appErr *temporal.ApplicationError
	if !errors.As(opened, &appErr) || !strings.Contains(appErr.Error(), canary) || appErr.Type() != "ActivityFailed" {
		t.Fatalf("opened = %v", opened)
	}
	var detail string
	if err := appErr.Details(&detail); err != nil || detail != "detail "+canary {
		t.Fatalf("detail = %q, %v", detail, err)
	}
	if !strings.Contains(fmt.Sprint(errors.Unwrap(appErr)), "deep "+canary) {
		t.Errorf("cause lost: %v", errors.Unwrap(appErr))
	}
}

// The key rides the error, printed only as its id.
func TestWithKey_PrintsNoKey(t *testing.T) {
	k := testKey(t, 0xCD)
	err := WithKey(errors.New("x"), NewScope(k, RunContext("r")), "R")
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		if out := fmt.Sprintf(verb, err); strings.Contains(strings.ToLower(out), "cdcd") {
			t.Errorf("%s leaks: %s", verb, out)
		}
	}
}

func TestOpenFailure_WithAnotherScope(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	fc := newFC(true)
	f := fc.ErrorToFailure(WithKey(errors.New(canary), scope, "R"))
	for name, other := range map[string]*Scope{
		"another key":     NewScope(testKey(t, 5), RunContext("ork-1")),
		"another context": NewScope(testKey(t, 4), RunContext("ork-2")),
		"no key":          nil,
	} {
		if _, err := OpenFailure(other, fc, fc.FailureToError(f)); !errors.Is(err, ErrUnreadable) {
			t.Errorf("%s: OpenFailure = %v, want ErrUnreadable", name, err)
		}
	}
}

// A failure that is not sealed is read as it is.
func TestOpenFailure_PlaintextAsItIs(t *testing.T) {
	fc := newFC(true)
	in := temporal.NewApplicationError("RUN_START_UNREADABLE", "InfrastructureError")
	err := fc.FailureToError(fc.ErrorToFailure(Keyless(in)))
	got, oerr := OpenFailure(NewScope(testKey(t, 1), RunContext("x")), fc, err)
	if oerr != nil || got != err {
		t.Fatalf("OpenFailure = %v, %v; want the error itself", got, oerr)
	}
	if got, oerr := OpenFailure(nil, fc, nil); got != nil || oerr != nil {
		t.Fatalf("OpenFailure(nil) = %v, %v", got, oerr)
	}
}

// An error that carries no key, in a process that seals: an unsealed run's
// and a keyless run's are written as they are; the SDK's own keep their own
// message and lose their details and causes; any other, a lost key (a nil
// scope) among them, is withheld.
func TestFailureConverter_UnkeyedErrors(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		sealing  bool
		wantText bool
		wantMsg  string // "" when the text itself is expected
	}{
		{"off: as the SDK writes it", errors.New(canary), false, true, ""},
		{"sealing, no key: withheld", errors.New(canary), true, false, Withheld},
		{"sealing, a lost key: withheld", WithKey(errors.New(canary), nil, "R"), true, false, Withheld},
		{"sealing, an unsealed run's error", WithKey(errors.New(canary), Unsealed(), "R"), true, true, ""},
		{"sealing, a keyless run's error", Keyless(errors.New(canary)), true, true, ""},
		{"sealing, keyless deep inside: withheld", fmt.Errorf("outer %s: %w", canary, Keyless(errors.New("x"))), true, false, Withheld},
		{"sealing, unsealed deep inside: withheld", fmt.Errorf("outer %s: %w", canary, WithKey(errors.New("x"), Unsealed(), "R")), true, false, Withheld},
		{"sealing, the SDK's cancel loses its details", temporal.NewCanceledError(canary), true, false, "canceled"},
		{"sealing, the SDK's timeout loses its cause", temporal.NewTimeoutError(0, errors.New(canary)), true, false, ""},
		{"sealing, a panic", temporal.NewApplicationError(canary, "PanicError"), true, false, Withheld},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFC(tc.sealing).ErrorToFailure(tc.err)
			has := bytes.Contains(failureBytes(t, f), []byte(canary))
			if has != tc.wantText {
				t.Fatalf("text in failure = %v, want %v: %v", has, tc.wantText, f)
			}
			if tc.wantMsg != "" && f.GetMessage() != tc.wantMsg {
				t.Fatalf("message = %q, want %q", f.GetMessage(), tc.wantMsg)
			}
		})
	}
}

// A key anywhere in an error's chain seals the whole failure, the outer
// text included.
func TestFailureConverter_KeyDeepInTheChainSeals(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	err := fmt.Errorf("outer %s: %w", canary, WithKey(errors.New(canary), scope, "R"))
	if f := newFC(true).ErrorToFailure(err); bytes.Contains(failureBytes(t, f), []byte(canary)) || f.GetMessage() != "R" {
		t.Fatalf("failure = %v", f)
	}
}

// A reason that is not a code of the closed vocabulary never becomes the
// plaintext message.
func TestFailureConverter_ReasonMustBeACode(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	for _, reason := range []string{"", "lower", "HAS SPACE " + canary, strings.Repeat("A", 65)} {
		f := newFC(true).ErrorToFailure(WithKey(errors.New("x"), scope, reason))
		if f.GetMessage() != "sealed" {
			t.Errorf("reason %q: message = %q, want sealed", reason, f.GetMessage())
		}
	}
}

// A failure's sealed text and a sealed value are told apart by their AAD:
// neither opens in the other's place.
func TestFailureConverter_TextIsNotAValue(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	f := newFC(true).ErrorToFailure(WithKey(errors.New(canary), scope, "R"))
	if _, err := scope.Open(f.GetEncodedAttributes()); !errors.Is(err, ErrUnreadable) {
		t.Fatalf("a failure's text opened as a value: %v", err)
	}
	value, _ := SealPayload(scope.key, scope.ctx, jsonPayload(`{"message":"forged","stack_trace":""}`))
	f.EncodedAttributes = value
	fc := newFC(true)
	if _, err := OpenFailure(scope, fc, fc.FailureToError(f)); !errors.Is(err, ErrUnreadable) {
		t.Fatalf("a sealed value opened as a failure's text: %v", err)
	}
}

// A sealed failure wrapped by the interpreter keeps its sealed original.
func TestFailureConverter_WrappedSealedFailure(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	fc := newFC(true)
	read := fc.FailureToError(fc.ErrorToFailure(WithKey(errors.New(canary), scope, "R")))
	again := fc.ErrorToFailure(WithKey(fmt.Errorf("task build: %w", read), scope, "R2"))
	opened, err := OpenFailure(scope, fc, fc.FailureToError(again))
	if err != nil || !strings.Contains(opened.Error(), canary) {
		t.Fatalf("opened = %v, %v", opened, err)
	}
}

// A failure handed on still sealed, as a fire hands on its run's, is written as
// it was: its reason kept, and no word of what wraps it. One that is not
// sealed whole is withheld, whoever wrote it.
func TestFailureConverter_HandsOnASealedFailure(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	fc := newFC(true)
	written := fc.ErrorToFailure(WithKey(temporal.NewNonRetryableApplicationError(canary, "T", errors.New(canary), canary), scope, "RUN_QUEUE_TIMEOUT"))
	again := fc.ErrorToFailure(fmt.Errorf("run %s failed: %w", canary, fc.FailureToError(written)))
	if strings.Contains(again.String(), canary) || again.GetMessage() != "RUN_QUEUE_TIMEOUT" {
		t.Fatalf("handed on as %v, want it sealed under its reason", again)
	}
	opened, err := OpenFailure(scope, fc, fc.FailureToError(again))
	if err != nil || !strings.Contains(opened.Error(), canary) {
		t.Fatalf("opened = %v, %v", opened, err)
	}
	for name, spoil := range map[string]func(f *failurepb.Failure){
		"a message that is no reason": func(f *failurepb.Failure) { f.Message = "a b " + canary },
		"a stack":                     func(f *failurepb.Failure) { f.StackTrace = canary },
		"a cause in plaintext":        func(f *failurepb.Failure) { f.Cause.Message, f.Cause.EncodedAttributes = canary, nil },
		"a detail in plaintext": func(f *failurepb.Failure) {
			f.GetApplicationFailureInfo().Details = &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: []byte(canary)}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := proto.Clone(written).(*failurepb.Failure)
			spoil(f)
			read := fc.FailureToError(f)
			var sf *SealedFailure
			if !errors.As(read, &sf) {
				t.Fatalf("read as %T, not a sealed failure", read)
			}
			if out := fc.ErrorToFailure(read); strings.Contains(out.String(), canary) {
				t.Fatalf("written as %v", out)
			}
		})
	}
}

// The guard's panic is never recovered into an error.
func TestRecover_TheGuardStillPanics(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	defer func() {
		r := recover()
		e, ok := r.(error)
		if !ok || !errors.Is(e, ErrEngineKeyMissing) {
			t.Fatalf("recovered %v, want the guard's panic to pass through Recover", r)
		}
	}()
	func() (err error) {
		defer Recover(scope, "ACTIVITY_PANIC", &err)
		panic(fmt.Errorf("%w (abc)", ErrEngineKeyMissing))
	}()
	t.Fatal("Recover swallowed the guard's panic")
}

// With sealing off every failure is the SDK default's, byte for byte.
func TestFailureConverter_OffIsTheDefault(t *testing.T) {
	dfc := temporal.NewDefaultFailureConverter(temporal.DefaultFailureConverterOptions{DataConverter: converter.GetDefaultDataConverter()})
	fc := newFC(false)
	for _, err := range []error{
		errors.New("x"),
		temporal.NewApplicationErrorWithCause("m", "T", errors.New("c"), "d"),
		temporal.NewNonRetryableApplicationError("m", "T", nil),
	} {
		want, got := dfc.ErrorToFailure(err), fc.ErrorToFailure(err)
		if !proto.Equal(want, got) {
			t.Errorf("%v: got %v, want %v", err, got, want)
		}
	}
}

// A panic in an activity leaves through the boundary as a keyed error.
func TestRecover_PanicIsKeyed(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	run := func() (err error) {
		defer Recover(scope, "ACTIVITY_PANIC", &err)
		panic(canary)
	}
	err := run()
	f := newFC(true).ErrorToFailure(err)
	if err == nil || bytes.Contains(failureBytes(t, f), []byte(canary)) || f.GetMessage() != "ACTIVITY_PANIC" {
		t.Fatalf("err = %v, failure = %v", err, f)
	}
}

func TestFailureConverter_DoesNotSealTwice(t *testing.T) {
	scope := NewScope(testKey(t, 4), RunContext("ork-1"))
	fc := newFC(true)
	f := fc.ErrorToFailure(WithKey(errors.New(canary), scope, "R"))
	// The interpreter returns an error it read back still sealed.
	again := fc.ErrorToFailure(WithKey(fc.FailureToError(f), scope, "R2"))
	opened, err := OpenFailure(scope, fc, fc.FailureToError(again))
	if err != nil || !strings.Contains(opened.Error(), canary) {
		t.Fatalf("opened = %v, %v", opened, err)
	}
}
