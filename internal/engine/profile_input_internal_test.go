package engine

import (
	"errors"
	"testing"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestTaskDispatchInput_CarriesTheDeviceRefs: a task naming profiles is
// dispatched with each one and the identity its run pinned; a task naming
// none carries none, so its payload is what it was before profiles existed;
// a profile the run did not pin is refused before dispatch, never sent with
// an empty identity.
func TestTaskDispatchInput_CarriesTheDeviceRefs(t *testing.T) {
	profiled := &orkstackv1.Task{Id: "read", Devices: []string{"sensor", "modem"}}
	plain := &orkstackv1.Task{Id: "plain"}
	s := &runState{runID: "ork-p", wf: &orkstackv1.Orkflow{Name: "p", Tasks: []*orkstackv1.Task{profiled, plain}},
		plan: RunPlan{ProfileIdentities: map[string]map[string]string{"edge": {"sensor": digestA, "modem": digestB}}}}

	in, err := s.taskDispatchInput(profiled, "read", "edge", nil)
	if err != nil {
		t.Fatalf("dispatch input: %v", err)
	}
	want := []*orkstackv1.ProfileRef{{Name: "sensor", IdentityDigest: digestA}, {Name: "modem", IdentityDigest: digestB}}
	if len(in.GetDevices()) != 2 || !proto.Equal(in.GetDevices()[0], want[0]) || !proto.Equal(in.GetDevices()[1], want[1]) {
		t.Fatalf("devices = %v", in.GetDevices())
	}

	in, err = s.taskDispatchInput(plain, "plain", "edge", nil)
	if err != nil || in.GetDevices() != nil {
		t.Fatalf("a plain task's devices = %v, %v", in.GetDevices(), err)
	}

	_, err = s.taskDispatchInput(profiled, "read", "other", nil)
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != ReasonProfileNotPinned || !app.NonRetryable() {
		t.Fatalf("an unpinned profile = %v, want a non-retryable %s", err, ReasonProfileNotPinned)
	}
}
