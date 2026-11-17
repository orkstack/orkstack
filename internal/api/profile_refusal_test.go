package api

import (
	"errors"
	"testing"

	"connectrpc.com/connect"

	"github.com/orkstack/orkstack/internal/engine"
)

// TestPoolPlanError_ProfileRefusals: a pool that cannot run a profiled task
// is a FailedPrecondition, a devices: name that is no profile an
// InvalidArgument.
func TestPoolPlanError_ProfileRefusals(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want connect.Code
	}{
		{&engine.ProfileUnavailableError{Pool: "edge", Profile: "sensor"}, connect.CodeFailedPrecondition},
		{&engine.ProfileMismatchError{Pool: "edge", Profile: "sensor"}, connect.CodeFailedPrecondition},
		{&engine.ProfileKindUnsupportedError{Pool: "edge", Profile: "sensor", Kind: "devices"}, connect.CodeFailedPrecondition},
		{&engine.DevicesNotAProfileError{Pool: "edge", Task: "read", Profile: "sensor"}, connect.CodeInvalidArgument},
	} {
		var ce *connect.Error
		if !errors.As(poolPlanError(tc.err), &ce) || ce.Code() != tc.want {
			t.Errorf("poolPlanError(%T) = %v, want %v", tc.err, ce, tc.want)
		}
	}
}
