package api_test

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/task"
)

// submitWithURL submits hello-ork with its one string input set to url.
func submitWithURL(t *testing.T, f *runIDFixture, url string) error {
	t.Helper()
	data, err := structpb.NewStruct(map[string]any{"url": url})
	if err != nil {
		t.Fatalf("input: %v", err)
	}
	_, err = f.svc.SubmitRun(context.Background(), connect.NewRequest(&orkstackv1.SubmitRunRequest{
		OrkflowName: "hello-ork",
		Input:       &orkstackv1.Payload{Data: data},
	}))
	return err
}

// TestSubmitRun_RefusesARunInputOverThePayloadBound pins AGENTS.md §2.2 at the
// workflow start: an input that would serialize past 256 KiB is refused as the
// caller's INPUT_TOO_LARGE before anything starts, rather than failing the
// start or growing a history Temporal later refuses.
func TestSubmitRun_RefusesARunInputOverThePayloadBound(t *testing.T) {
	starter := &fakeStarter{}
	f := newRunIDFixture(t, starter, nil)

	err := submitWithURL(t, f, strings.Repeat("x", task.MaxActivityInputBytes+1))
	if err == nil {
		t.Fatal("SubmitRun accepted an input over the payload bound")
	}
	if got := connect.CodeOf(err); got != connect.CodeInvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument: %v", got, err)
	}
	for _, want := range []string{task.ReasonInputTooLarge, "the run input serializes to", "256 KiB (262144 bytes)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not carry %q", err.Error(), want)
		}
	}
	if started := starter.startedRuns(); len(started) != 0 {
		t.Errorf("started %v, want nothing: a refused input must start no run", started)
	}
}

// TestSubmitRun_AcceptsARunInputUnderThePayloadBound keeps the bound a bound:
// a large input that still fits is started unchanged.
func TestSubmitRun_AcceptsARunInputUnderThePayloadBound(t *testing.T) {
	starter := &fakeStarter{}
	f := newRunIDFixture(t, starter, nil)

	url := strings.Repeat("x", task.MaxActivityInputBytes/2)
	if err := submitWithURL(t, f, url); err != nil {
		t.Fatalf("SubmitRun refused an input under the bound: %v", err)
	}
	inputs := starter.startedInputs()
	if len(inputs) != 1 || inputs[0]["url"] != url {
		t.Fatalf("started inputs = %d, want the one submitted input unchanged", len(inputs))
	}
}
