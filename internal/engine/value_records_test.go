package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/identity"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

type fakeValuesRecorder struct {
	outputs  identity.OutputsAsk
	composed identity.ComposedAsk
	got      identity.RecordedOutcome
	err      error
}

func (f *fakeValuesRecorder) RecordOutputs(_ context.Context, q identity.OutputsAsk) (identity.RecordedOutcome, error) {
	f.outputs = q
	return f.got, f.err
}

func (f *fakeValuesRecorder) RecordComposed(_ context.Context, q identity.ComposedAsk) (identity.RecordedOutcome, error) {
	f.composed = q
	return f.got, f.err
}

func TestValueRecordActivities_PassWhatTheRunSays(t *testing.T) {
	rec := &fakeValuesRecorder{got: identity.RecordedOutcome{RecordSHA256: "r", Values: []byte(`{"n":1}`),
		ValuesRef: &orkstackv1.ArtifactRef{Uri: "s3://v", Sha256: "v"}, Artifacts: []*orkstackv1.ArtifactRef{{Name: "a", Uri: "s3://a"}}}}
	act := NewValueRecordActivities(rec)
	got, err := act.RecordOutputs(context.Background(), RecordOutputsRequest{Namespace: "ns", RunID: "c", AdmissionSHA256: "x", RootScope: 4,
		Input: []byte(`{"env":"prod"}`), Offered: []ComposeUpstream{{TaskID: "build", URI: "s3://b", SHA256: "b"}}, MaxBytes: 9})
	if err != nil || got.RecordSHA256 != "r" || string(got.Values) != `{"n":1}` || got.ValuesRef.URI != "s3://v" || got.Artifacts[0].Name != "a" {
		t.Fatalf("%+v, %v", got, err)
	}
	if q := rec.outputs; q.Namespace != "ns" || q.RunID != "c" || q.AdmissionSHA256 != "x" || q.Scopes != (identity.Scopes{Root: 4}) ||
		string(q.Input) != `{"env":"prod"}` || q.Offered["build"].GetUri() != "s3://b" || q.Offered["build"].GetSha256() != "b" || q.MaxBytes != 9 {
		t.Fatalf("%+v", q)
	}
	composed, err := act.RecordComposed(context.Background(), RecordComposedRequest{Namespace: "ns", RunID: "p", AdmissionSHA256: "x", TaskID: "sub",
		ResumeScope: 3, Input: []byte("null"), ChildAdmissionSHA256: "y", ValuesSHA256: "z"})
	if err != nil || composed.ValuesRef.SHA256 != "v" {
		t.Fatalf("%+v, %v", composed, err)
	}
	if q := rec.composed; q.TaskID != "sub" || q.Scopes != (identity.Scopes{Resume: 3}) || q.ChildAdmissionSHA256 != "y" || q.ValuesSHA256 != "z" || string(q.Input) != "null" {
		t.Fatalf("%+v", q)
	}
}

// TestValueRecordActivities_TheBoundary: what the store answered is a
// refusal with its reason, never retried; what it did not is
// RECORD_STORE_UNANSWERED with its class, retried; a render's own refusal and
// the run's cancel are as they are.
func TestValueRecordActivities_TheBoundary(t *testing.T) {
	unanswered := &artifacts.RecordStoreUnanswered{Class: artifacts.UnansweredDenied, Err: errors.New("403")}
	for _, tc := range []struct {
		name   string
		err    error
		reason string
		retry  bool
		says   string
	}{
		{"a child's outputs are not on record", &identity.ValuesRefusal{Reason: identity.ReasonChildValuesNotRecorded, Message: "child run c ended"}, "CHILD_VALUES_NOT_RECORDED", false, "child run c ended"},
		{"a task is not on record", &identity.ValuesRefusal{Reason: identity.ReasonOutputsNotRecorded, Message: "task gate"}, "OUTPUTS_NOT_RECORDED", false, "task gate"},
		{"the render", composeActivityError("OUTPUTS_RENDER_FAILED", orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, "outputs.n: no key"), "OUTPUTS_RENDER_FAILED", false, "no key"},
		{"too large", composeActivityError("OUTPUTS_TOO_LARGE", orkstackv1.FailureCategory_FAILURE_CATEGORY_CONTRACT_VIOLATION, "big"), "OUTPUTS_TOO_LARGE", false, "big"},
		{"the store", fmt.Errorf("read: %w", unanswered), "", true, "denied"},
		{"the cluster", errors.New("frontend down"), "", true, "unavailable"},
		{"the attempt's time", fmt.Errorf("read: %w", context.DeadlineExceeded), "", true, "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			act := NewValueRecordActivities(&fakeValuesRecorder{err: tc.err})
			_, err := act.RecordOutputs(context.Background(), RecordOutputsRequest{})
			_, err2 := act.RecordComposed(context.Background(), RecordComposedRequest{})
			for _, err := range []error{err, err2} {
				var app *temporal.ApplicationError
				if !errors.As(err, &app) || app.NonRetryable() == tc.retry || !strings.Contains(app.Message(), tc.says) {
					t.Fatalf("%v", err)
				}
				if tc.retry {
					if app.Type() != RecordStoreUnansweredType {
						t.Fatalf("type %q", app.Type())
					}
					if code, _, _ := renderFailure("OUTPUTS", err); code != OutputsUnavailableCode {
						t.Fatalf("reads %s", code)
					}
					continue
				}
				if code := categoryOf(err).reasonCode(); code != tc.reason {
					t.Fatalf("%v: reason %q", err, code)
				}
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	act := NewValueRecordActivities(&fakeValuesRecorder{err: fmt.Errorf("read: %w", context.Canceled)})
	if _, err := act.RecordOutputs(ctx, RecordOutputsRequest{}); !temporal.IsCanceledError(err) {
		t.Fatalf("a cancel reads %v", err)
	}
	if _, err := NewValueRecordActivities(nil).RecordOutputs(context.Background(), RecordOutputsRequest{}); err == nil {
		t.Fatal("an engine with no recorder recorded")
	}
}

// TestRenderOutputs_AsTheRunRenders: the bytes are what the parent
// canonicalises from the run's own render, defaults applied.
func TestRenderOutputs_AsTheRunRenders(t *testing.T) {
	values := `{"n":1e21,"z":"<b>","a":[1,2.50]}`
	store := oneValue(values)
	inputs, err := structpb.NewStruct(map[string]any{"region": map[string]any{"default": "eu"}})
	if err != nil {
		t.Fatal(err)
	}
	wf := &orkstackv1.Orkflow{Name: "o", Outputs: map[string]string{"z": "{{ .tasks.build.output.z }}", "env": "{{ .inputs.env }}", "region": "{{ .inputs.region }}"},
		Inputs: inputs}
	sum := sha256.Sum256([]byte(values))
	up := map[string]*orkstackv1.ArtifactRef{"build": {Uri: "mem://v", Sha256: hex.EncodeToString(sum[:])}}
	compose := NewComposeActivities(store)
	got, err := compose.RenderOutputs(context.Background(), identity.OutputsRender{Definition: wf, Input: []byte(`{"env":"prod"}`), Upstream: up, MaxBytes: 1 << 10})
	if err != nil || string(got) != `{"env":"prod","region":"eu","z":"<b>"}` {
		t.Fatalf("%s, %v", got, err)
	}
	up["build"].Sha256 = strings.Repeat("0", 64)
	_, err = compose.RenderOutputs(context.Background(), identity.OutputsRender{Definition: wf, Input: []byte("null"), Upstream: up, MaxBytes: 1 << 10})
	var app *temporal.ApplicationError
	if !errors.As(err, &app) {
		t.Fatalf("%v", err)
	}
	ref, err := compose.StoreOutputs(context.Background(), artifacts.Admission{Orkflow: "o", RunID: "c"}, got)
	if err != nil || ref.GetSha256() == "" {
		t.Fatalf("%v, %v", ref, err)
	}
}

// TestRenderOutputs_AParentComputesTheSameBytes: what the engine records of a
// child's outputs is, for every shape of JSON value, what its parent renders of
// the result the cluster hands it; the two digests are compared.
func TestRenderOutputs_AParentComputesTheSameBytes(t *testing.T) {
	for _, v := range []string{`0`, `-0`, `1`, `-1`, `1.0`, `1.5`, `2.50`, `0.1`, `1e2`, `1e21`, `1E+21`, `1e-7`, `123456789012345680`, `9007199254740991`,
		`-1.7976931348623157e308`, `5e-324`, `true`, `"<b>&\u2028é"`, `""`, `[]`, `{}`, `[1,2.50,{"z":1e21,"a":null}]`, `{"b":{"y":1.10,"x":[]},"a":0.000001}`} {
		values := `{"v":` + v + `}`
		sum := sha256.Sum256([]byte(values))
		wf := &orkstackv1.Orkflow{Name: "o", Outputs: map[string]string{"v": "{{ .tasks.build.output.v }}", "text": "is {{ .tasks.build.output.v }}"}}
		got, err := NewComposeActivities(oneValue(values)).RenderOutputs(context.Background(), identity.OutputsRender{Definition: wf, Input: []byte(`{}`),
			Upstream: map[string]*orkstackv1.ArtifactRef{"build": {Uri: "mem://v", Sha256: hex.EncodeToString(sum[:])}}, MaxBytes: 1 << 12})
		if err != nil {
			t.Errorf("%s: %v", v, err)
			continue
		}
		// The child's result, as its parent receives it.
		output, err := payloadOfJSON(got)
		if err != nil {
			t.Errorf("%s: %v", v, err)
			continue
		}
		again, err := canonicalValuesJSON(payloadMap(output))
		if err != nil || string(again) != string(got) {
			t.Errorf("%s: the engine recorded %s, its parent computes %s (%v)", v, got, again, err)
		}
	}
}

// oneValue is a store holding one object.
type oneValue string

func (v oneValue) Get(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(string(v))), nil
}

func (oneValue) Put(_ context.Context, name string, r io.Reader, _ artifacts.PutOptions) (artifacts.ArtifactRef, error) {
	raw, err := io.ReadAll(r)
	sum := sha256.Sum256(raw)
	return artifacts.ArtifactRef{Name: name, URI: "mem://stored", SHA256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(raw))}, err
}
