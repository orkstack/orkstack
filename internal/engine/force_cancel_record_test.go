package engine

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/internal/jwt"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordKeys boots an engine's key ring over a local key store: the real
// signer and verifier a force cancel's record is made and read with.
func recordKeys(t testing.TB) *identity.Issuer {
	t.Helper()
	store, err := artifacts.NewPublishedKeysStore(context.Background(),
		artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	must.OK(t, err)
	signer, err := jwt.NewSigner(k)
	must.OK(t, err)
	issuer, err := identity.New(identity.Config{Keys: []*jwt.Signer{signer}, Algorithm: jwt.ES256,
		ClockSkew: time.Minute, Engine: "engine-a"}, store, nil, nil)
	must.OK(t, err)
	must.OK(t, issuer.Publish(context.Background()))
	return issuer
}

func sampleRecord() ForceCancelRecord {
	return ForceCancelRecord{
		Namespace: "payments", RunID: "ork-r", TerminatedExecutionRunID: "exec-1",
		RequestID: "fc-1", WorkflowID: ForceCancelWorkflowIDPrefix("payments", "ork-r") + "fc-1",
		Principal: "user:corp:alice", PrincipalVerified: true, AuthMethod: "oidc",
		Permission: "run.cancel.force", Scope: "orkflow:payments/pay", Rule: "admins",
		PolicyGeneration: 7, Reason: "stuck in a workflow-task loop", GraceSeconds: 90,
		At: time.Now().UTC(), // a record is read as of its close, just after
	}
}

// terminated is the close a force cancel's termination leaves: its reason and
// the record as its one detail.
func terminated(t *testing.T, details *commonpb.Payloads) *historypb.HistoryEvent {
	t.Helper()
	return &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
				Reason: ForceCancelTerminateReason, Details: details,
			},
		},
	}
}

// TestForceCancelRecord_ReadsAsWhatEndedTheRun: a termination whose record
// verifies, for this run and this execution, reads CANCELED
// RUN_FORCE_CANCELED naming who and why; every way a record can fail to be
// this run's is told apart, and none reads as a force cancel.
func TestForceCancelRecord_ReadsAsWhatEndedTheRun(t *testing.T) {
	ctx := context.Background()
	keys := recordKeys(t)
	other := recordKeys(t) // a key ring this deployment never published
	signed := func(t *testing.T, signer *identity.Issuer, mutate func(*ForceCancelRecord)) *commonpb.Payloads {
		t.Helper()
		rec := sampleRecord()
		if mutate != nil {
			mutate(&rec)
		}
		details, err := SignForceCancelRecord(signer, rec)
		must.OK(t, err)
		return details
	}
	closedAt := time.Now()
	tests := []struct {
		name    string
		details *commonpb.Payloads
		mode    string
		run     string // the run the reader is reading; "" is ork-r
		exec    string // its closed execution; "" is exec-1
		want    ForceCancelVerification
	}{
		{"a verified record of this run", signed(t, keys, nil), "enforce", "", "", ForceCancelVerified},
		{"another run's record (bound to another run as signed)", signed(t, keys, nil), "enforce", "ork-other", "", ForceCancelBad},
		{"another execution's record", signed(t, keys, nil), "enforce", "", "exec-2", ForceCancelNotThisRun},
		{"another namespace's record (bound to it as signed)", signed(t, keys, func(r *ForceCancelRecord) { r.Namespace = "billing" }), "enforce", "", "", ForceCancelBad},
		{"a workflow id not of this run's force cancel", signed(t, keys, func(r *ForceCancelRecord) { r.WorkflowID = "ork-force-0000-fc-1" }), "enforce", "", "", ForceCancelNotThisRun},
		{"a key this deployment does not know", signed(t, other, nil), "enforce", "", "", ForceCancelUnknownKey},
		{"an unsigned record where callers are verified", unsignedRecord(t, sampleRecord()), "enforce", "", "", ForceCancelBad},
		{"an unsigned record in asserted mode", unsignedRecord(t, sampleRecord()), "asserted", "", "", ForceCancelAsserted},
		{"a record whose clock is behind the close", signed(t, keys, func(r *ForceCancelRecord) { r.At = r.At.Add(-time.Hour) }), "enforce", "", "", ForceCancelVerified},
		{"one whose clock is ahead of it", signed(t, keys, func(r *ForceCancelRecord) { r.At = r.At.Add(time.Hour) }), "enforce", "", "", ForceCancelVerified},
		{"no record at all", nil, "enforce", "", "", ForceCancelNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run, exec := tt.run, tt.exec
			if run == "" {
				run = "ork-r"
			}
			if exec == "" {
				exec = "exec-1"
			}
			got := ReadForceCancelClose(ctx, keys, tt.mode, "payments", run, exec, terminated(t, tt.details), closedAt)
			if got.Verification != tt.want {
				t.Fatalf("verification = %v, want %v", got.Verification, tt.want)
			}
			status, failure := got.Outcome("run terminated")
			switch tt.want {
			case ForceCancelVerified, ForceCancelAsserted:
				if status != orkstackv1.Status_STATUS_CANCELED || failure.GetCode() != RunForceCanceledCode ||
					failure.GetCategory() != orkstackv1.FailureCategory_FAILURE_CATEGORY_CANCELLED {
					t.Fatalf("outcome = %v %v, want CANCELED %s Cancelled", status, failure, RunForceCanceledCode)
				}
				want := "force-canceled by user:corp:alice after 1m30s without progress: stuck in a workflow-task loop; it ended without its own cleanup"
				if tt.want == ForceCancelAsserted {
					want = "force-canceled by user:corp:alice (asserted) after 1m30s without progress: stuck in a workflow-task loop; it ended without its own cleanup"
				}
				if failure.GetMessage() != want {
					t.Errorf("message = %q, want %q", failure.GetMessage(), want)
				}
				if got.Record.RequestID != "fc-1" || got.Record.Reason != "stuck in a workflow-task loop" {
					t.Errorf("record = %+v", got.Record)
				}
			default:
				if status != orkstackv1.Status_STATUS_CANCELED || failure.GetCode() != "RUN_TERMINATED" {
					t.Fatalf("outcome = %v %v, want CANCELED RUN_TERMINATED", status, failure)
				}
			}
			if tt.want == ForceCancelUnknownKey && !strings.Contains(failure.GetMessage(), "could not be verified (unknown key ") {
				t.Errorf("an unknown key's message = %q, want it to say the record could not be verified", failure.GetMessage())
			}
		})
	}
}

// TestReadForceCancelClose_AnUnknownExecutionDecidesNothing: a close is held to
// the execution that closed, so one whose execution is not known is not read
// as any.
func TestReadForceCancelClose_AnUnknownExecutionDecidesNothing(t *testing.T) {
	keys := recordKeys(t)
	details, err := SignForceCancelRecord(keys, sampleRecord())
	must.OK(t, err)
	if got := ReadForceCancelClose(context.Background(), keys, "enforce", "payments", "ork-r", "", terminated(t, details), time.Now()); got.Verification != ForceCancelUnavailable {
		t.Fatalf("verification = %v, want unavailable", got.Verification)
	}
}

// TestForceCancelRecord_WhereNobodyHoldsKeys: in asserted mode, where no engine
// ever published a key, a signed record is nobody's: it is bad, not one to be
// decided later, which would leave the run unreadable for good. Where any key
// was published, or that cannot be read, or the mode is another, an engine
// that holds none decides nothing. An unsigned one reads as asserted.
func TestForceCancelRecord_WhereNobodyHoldsKeys(t *testing.T) {
	ctx := context.Background()
	details, err := SignForceCancelRecord(recordKeys(t), sampleRecord())
	must.OK(t, err)
	unsigned := unsignedRecord(t, sampleRecord())
	none, err := artifacts.NewPublishedKeysStore(ctx, artifacts.Config{Backend: artifacts.BackendLocal, LocalDir: t.TempDir()})
	must.OK(t, err)
	some := keysDoc{doc: artifacts.PublishedKeys{Keys: []artifacts.PublishedKey{{KID: "k"}}}}
	for _, tt := range []struct {
		name      string
		published PublishedKeys
		mode      string
		details   *commonpb.Payloads
		want      ForceCancelVerification
	}{
		{"signed, no key ever published", none, "asserted", details, ForceCancelBad},
		{"unsigned, no key ever published", none, "asserted", unsigned, ForceCancelAsserted},
		{"signed, another engine's key published", some, "asserted", details, ForceCancelUnavailable},
		{"unsigned, another engine's key published", some, "asserted", unsigned, ForceCancelAsserted},
		{"signed, the document unreadable", keysDoc{err: errors.New("store down")}, "asserted", details, ForceCancelUnavailable},
		{"signed under observe", none, "observe", details, ForceCancelUnavailable},
		{"signed under enforce", none, "enforce", details, ForceCancelUnavailable},
		{"unsigned under enforce", none, "enforce", unsigned, ForceCancelUnavailable},
		{"signed under a mode nobody knows", none, "", details, ForceCancelUnavailable},
		{"unsigned under a mode nobody knows", none, "", unsigned, ForceCancelUnavailable},
		{"signed, nothing to ask", nil, "asserted", details, ForceCancelUnavailable},
	} {
		keyless := NewKeylessEngine(tt.published, func() string { return tt.mode })
		got := ReadForceCancelClose(ctx, keyless, tt.mode, "payments", "ork-r", "exec-1", terminated(t, tt.details), time.Now())
		if got.Verification != tt.want {
			t.Errorf("%s: %v, want %v", tt.name, got.Verification, tt.want)
		}
	}
	if got := ReadForceCancelClose(ctx, nil, "asserted", "payments", "ork-r", "exec-1", terminated(t, details), time.Now()); got.Verification != ForceCancelUnavailable {
		t.Errorf("a reader given no verifier: %v", got.Verification)
	}
}

// keysDoc is a published keys document, or a store that cannot answer it.
type keysDoc struct {
	doc artifacts.PublishedKeys
	err error
}

func (k keysDoc) Read(context.Context) (artifacts.PublishedKeys, error) { return k.doc, k.err }

// A record made at its ceiling, the run still progressing, says so.
func TestForceCancelRecord_AtTheCeilingSaysTheRunStillProgressed(t *testing.T) {
	keys := recordKeys(t)
	rec := sampleRecord()
	rec.ProgressingAtCeiling = true
	details, err := SignForceCancelRecord(keys, rec)
	must.OK(t, err)
	got := ReadForceCancelClose(context.Background(), keys, "enforce", "payments", "ork-r", "exec-1", terminated(t, details), time.Now())
	_, failure := got.Outcome("")
	want := "force-canceled by user:corp:alice at its ceiling, still making progress after 1m30s: stuck in a workflow-task loop; it ended without its own cleanup"
	if failure.GetMessage() != want {
		t.Errorf("message = %q, want %q", failure.GetMessage(), want)
	}
}

// A record is refused at signing when it would exceed its bound, and the
// prefix that names a run's force cancels is fixed by the run alone.
func TestForceCancelRecord_Bounds(t *testing.T) {
	keys := recordKeys(t)
	// Every field at its own bound still fits the record's.
	rec := sampleRecord()
	rec.Reason, rec.Principal, rec.PrincipalIssuer = strings.Repeat("x", 512), strings.Repeat("p", 256), strings.Repeat("i", 256)
	rec.RequestID, rec.WorkflowID = strings.Repeat("r", 128), strings.Repeat("w", 256)
	rec.AuthMethod, rec.Permission, rec.Scope, rec.Rule = strings.Repeat("a", 64), strings.Repeat("q", 64), strings.Repeat("s", 1024), strings.Repeat("u", 256)
	if _, err := SignForceCancelRecord(keys, rec); err != nil {
		t.Fatalf("a record with every field at its bound: %v", err)
	}
	rec.Reason = strings.Repeat("\"", 512)
	rec.Principal, rec.PrincipalIssuer, rec.Scope = strings.Repeat("\"", 256), strings.Repeat("\"", 256), strings.Repeat("\"", 1024)
	if _, err := SignForceCancelRecord(keys, rec); !errors.Is(err, ErrForceCancelRecordTooLarge) {
		t.Fatalf("a record past %d bytes once escaped: %v, want ErrForceCancelRecordTooLarge", ForceCancelRecordMaxBytes, err)
	}
	a, b := ForceCancelWorkflowIDPrefix("payments", "ork-r"), ForceCancelWorkflowIDPrefix("payment", "sork-r")
	if a == b || !strings.HasPrefix(a, "ork-force-") || len(a) != len("ork-force-")+32+1 {
		t.Errorf("prefixes %q and %q: want distinct ork-force-<32 hex>- of each run", a, b)
	}
}

func unsignedRecord(t *testing.T, rec ForceCancelRecord) *commonpb.Payloads {
	t.Helper()
	details, err := UnsignedForceCancelRecord(rec)
	must.OK(t, err)
	return details
}

// startEvent is a run's start with its planned definition in namespace, as
// RunWorkflow is started: its second argument is the definition.
func startEvent(t *testing.T, namespace string) *historypb.HistoryEvent {
	t.Helper()
	input, err := DataConverter().ToPayloads("ork-r", &orkstackv1.Orkflow{Name: "pay", Namespace: namespace})
	must.OK(t, err)
	return &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: input}}}
}

// describeOnly answers DescribeWorkflowExecution and nothing else: a describe
// that fails, and a run reset while it was read, are faults a real cluster
// does not produce on demand.
type describeOnly struct {
	runReadClient
	exec   string
	status enumspb.WorkflowExecutionStatus
	length int64 // of its history
	err    error
}

func (d describeOnly) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if d.err != nil {
		return nil, d.err
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "ork-r", RunId: d.exec}, Status: d.status, HistoryLength: d.length}}, nil
}

// TestReader_ForceCancelOutcome: the run reader reads a force cancel's
// termination by its record, for the namespace its definition names ("" and
// "default" are one) and the execution the cluster says closed. What it
// cannot decide now, unreadable keys or an execution it cannot name, is an
// error a later read may answer, never a permanent RUN_TERMINATED. Any other
// termination stays as closedExecution read it.
func TestReader_ForceCancelOutcome(t *testing.T) {
	keys := recordKeys(t)
	closeFor := func(t *testing.T, namespace string) *historypb.HistoryEvent {
		t.Helper()
		rec := sampleRecord()
		rec.Namespace = namespace
		rec.WorkflowID = ForceCancelWorkflowIDPrefix(namespace, rec.RunID) + rec.RequestID
		details, err := SignForceCancelRecord(keys, rec)
		must.OK(t, err)
		e := terminated(t, details)
		e.EventId, e.EventTime = 9, timestamppb.Now()
		return e
	}
	enforce := func() string { return "enforce" }
	reader := func(c runReadClient, v ForceCancelVerifier) *TemporalRunReader {
		r := &TemporalRunReader{client: c, converter: DataConverter()}
		if v != nil {
			r = r.WithForceCancelVerifier(v, enforce)
		}
		return r
	}
	closed := enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
	var noKeys *identity.Issuer
	tests := []struct {
		name      string
		reader    *TemporalRunReader
		startedIn string
		signedFor string
		execution string
		code      string // "" wants an error
	}{
		{"verified, its execution known", reader(nil, keys), "payments", "payments", "exec-1", RunForceCanceledCode},
		{"verified, its execution described", reader(describeOnly{exec: "exec-1", status: closed}, keys), "payments", "payments", "", RunForceCanceledCode},
		{"the default namespace unnamed", reader(nil, keys), "", "default", "exec-1", RunForceCanceledCode},
		{"the default namespace named", reader(nil, keys), "default", "default", "exec-1", RunForceCanceledCode},
		{"signed for another namespace", reader(nil, keys), "billing", "payments", "exec-1", "RUN_TERMINATED"},
		{"the execution it describes closed with this event", reader(describeOnly{exec: "exec-1", status: closed, length: 9}, keys), "payments", "payments", "", RunForceCanceledCode},
		{"the run was resumed and closed again while it was read", reader(describeOnly{exec: "exec-2", status: closed, length: 40}, keys), "payments", "payments", "", ""},
		{"another execution closed", reader(describeOnly{exec: "exec-2", status: closed}, keys), "payments", "payments", "", "RUN_TERMINATED"},
		{"the execution cannot be described", reader(describeOnly{err: errors.New("frontend down")}, keys), "payments", "payments", "", ""},
		{"the run was reset while it was read", reader(describeOnly{exec: "exec-2", status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}, keys), "payments", "payments", "", ""},
		{"a reader given no verifier", reader(nil, nil), "payments", "payments", "exec-1", ""},
		{"an engine with no keys", reader(nil, noKeys), "payments", "payments", "exec-1", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.reader.forceCancelOutcome(context.Background(), "ork-r", tt.execution, startEvent(t, tt.startedIn), closeFor(t, tt.signedFor))
			if tt.code == "" {
				if !errors.Is(err, ErrForceCancelUndecided) {
					t.Fatalf("outcome = %v, %v; want ErrForceCancelUndecided", got, err)
				}
				return
			}
			must.OK(t, err)
			if _, failure := got.Outcome(""); failure.GetCode() != tt.code {
				t.Fatalf("outcome = %v (%v), want %s", failure, got.Verification, tt.code)
			}
		})
	}
	other := &historypb.HistoryEvent{Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
		WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{Reason: "operator"}}}
	if got, err := reader(nil, nil).forceCancelOutcome(context.Background(), "ork-r", "", startEvent(t, "payments"), other); err != nil || got.Verification != ForceCancelNone {
		t.Fatalf("a termination with another reason = %v, %v; want none, asking nothing", got, err)
	}
}

// FuzzForceCancelRecord: whatever bytes a termination carries as its detail,
// reading them never panics; bytes that are no record of this run read as no
// force cancel, never as one; and what reads as a force cancel is this run's,
// this execution's, and within its bound.
func FuzzForceCancelRecord(f *testing.F) {
	keys := recordKeys(f)
	signed, err := SignForceCancelRecord(keys, sampleRecord())
	must.OK(f, err)
	unsigned, err := UnsignedForceCancelRecord(sampleRecord())
	must.OK(f, err)
	f.Add(signed.GetPayloads()[0].GetData(), "enforce")
	f.Add(unsigned.GetPayloads()[0].GetData(), "asserted")
	f.Add([]byte(`{"signed":"a.b.c"}`), "enforce")
	f.Add([]byte(`{"signed":"x","unsigned":{"format":2}}`), "asserted")
	f.Add([]byte(`not json`), "observe")
	f.Fuzz(func(t *testing.T, data []byte, mode string) {
		payload := proto.Clone(signed.GetPayloads()[0]).(*commonpb.Payload)
		payload.Data = data
		ev := terminated(t, &commonpb.Payloads{Payloads: []*commonpb.Payload{payload}})
		got := ReadForceCancelClose(context.Background(), keys, mode, "payments", "ork-r", "exec-1", ev, time.Now())
		if !got.IsForceCancel() {
			if _, failure := got.Outcome(""); failure.GetCode() != "RUN_TERMINATED" {
				t.Fatalf("a close that is no force cancel reads %s", failure.GetCode())
			}
			return
		}
		r := got.Record
		if r.Namespace != "payments" || r.RunID != "ork-r" || r.TerminatedExecutionRunID != "exec-1" ||
			!strings.HasPrefix(r.WorkflowID, ForceCancelWorkflowIDPrefix("payments", "ork-r")) {
			t.Fatalf("a record of another run read as this run's: %+v", r)
		}
		if got.Verification == ForceCancelAsserted && mode != "asserted" {
			t.Fatalf("an unsigned record was taken in mode %q", mode)
		}
		if len(data) > ForceCancelRecordMaxBytes+1024 && got.Verification == ForceCancelVerified {
			t.Fatalf("a verified record of %d bytes, past the bound", len(data))
		}
	})
}

// TestForceCancelRecord_RefusesTextNoSurfaceMayShow: a record's text reaches
// the API, the stream, webhooks and terminals, and in asserted mode nothing
// signs it: a record whose text is not plain or is over its bound is refused
// when made and reads as no force cancel.
func TestForceCancelRecord_RefusesTextNoSurfaceMayShow(t *testing.T) {
	keys := recordKeys(t)
	tests := map[string]func(*ForceCancelRecord){
		"a line break in the reason":     func(r *ForceCancelRecord) { r.Reason = "stuck\nSUCCEEDED" },
		"an escape in the reason":        func(r *ForceCancelRecord) { r.Reason = "\x1b[2J" },
		"a C1 control in the principal":  func(r *ForceCancelRecord) { r.Principal = "alice\u009b" },
		"a line separator in the reason": func(r *ForceCancelRecord) { r.Reason = "stuck\u2028SUCCEEDED" },
		"a reason over 512 bytes":        func(r *ForceCancelRecord) { r.Reason = strings.Repeat("x", 513) },
		"no reason":                      func(r *ForceCancelRecord) { r.Reason = "" },
		"a principal over 256 bytes":     func(r *ForceCancelRecord) { r.Principal = strings.Repeat("x", 257) },
		"a request id over 128 bytes":    func(r *ForceCancelRecord) { r.RequestID = strings.Repeat("x", 129) },
		"a scope over its bound":         func(r *ForceCancelRecord) { r.Scope = strings.Repeat("x", 1025) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			rec := sampleRecord()
			mutate(&rec)
			if _, err := SignForceCancelRecord(keys, rec); err == nil {
				t.Error("signed")
			}
			if _, err := UnsignedForceCancelRecord(rec); err == nil {
				t.Error("made unsigned")
			}
			rec.Format = ForceCancelRecordFormat
			written, err := DataConverter().ToPayloads(forceCancelDetails{Unsigned: &rec})
			must.OK(t, err)
			got := ReadForceCancelClose(context.Background(), keys, "asserted", "payments", "ork-r", "exec-1", terminated(t, written), time.Now())
			if got.IsForceCancel() {
				t.Errorf("reads as a force cancel: %+v", got.Record)
			}
		})
	}
}
