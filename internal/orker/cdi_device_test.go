package orker

import (
	"strings"
	"testing"
)

// TestCreateArgs_CDIDevices: a task's bound gpu units reach the runtime as
// fully qualified CDI device names, and nothing else does: never a path, a
// flag, an unqualified name, or `all`.
func TestCreateArgs_CDIDevices(t *testing.T) {
	spec := validSpec()
	spec.CDIDevices = []string{"amd.com/gpu=0", "test.orkstack.dev/gpu-1a2b3c4d=intel", "nvidia.com/gpu=0"}
	got := renderedIn(t, spec, dockerDialect)
	for _, want := range []string{"--device amd.com/gpu=0", "--device test.orkstack.dev/gpu-1a2b3c4d=intel", "--device nvidia.com/gpu=0"} {
		if !strings.Contains(got, want) {
			t.Errorf("argv %s lacks %q", got, want)
		}
	}
	for _, bad := range []string{
		"/dev/dri/renderD128", "amd.com/gpu", "=0", "amd.com/gpu=", "amd.com/gpu=all",
		"nvidia.com/gpu=all", "--privileged", "amd.com/gpu=0 --privileged", "AMD.com/gpu=0",
		"amd.com/gpu=/dev/kfd",
	} {
		spec := validSpec()
		spec.CDIDevices = []string{bad}
		if args, err := createArgs(spec, dockerDialect); err == nil {
			t.Errorf("CDI device %q rendered: %q", bad, args)
		}
	}
}
