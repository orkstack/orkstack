package engine

import (
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestJudgeTaskProfiles_GPUUnitsNeedAGPUProfile: a task asking for gpu units
// must name a profile its pool defines as a gpu profile; one naming only a
// devices profile is refused at submit, by name (design §4.3), where an
// Orker would otherwise accept the count and give it nothing.
func TestJudgeTaskProfiles_GPUUnitsNeedAGPUProfile(t *testing.T) {
	const (
		dev = orkstackv1.ProfileKind_PROFILE_KIND_DEVICES
		gpu = orkstackv1.ProfileKind_PROFILE_KIND_GPU
	)
	kinds := []orkstackv1.ProfileKind{dev, gpu}
	orkers := []*orkstackv1.DescribeOrkerResult{deviceOrker("o1", kinds, offered("sensor", dev, digestA), offered("amd", gpu, digestB))}
	if _, err := judgeTaskProfiles("ns", "gpu", []*orkstackv1.Task{
		{Id: "train", Devices: []string{"amd"}, Resources: &orkstackv1.ResourceRequest{Gpu: 1}},
	}, orkers); err != nil {
		t.Fatalf("a gpu count on a gpu profile: %v", err)
	}
	_, err := judgeTaskProfiles("ns", "gpu", []*orkstackv1.Task{
		{Id: "read", Devices: []string{"sensor"}, Resources: &orkstackv1.ResourceRequest{Gpu: 1}},
	}, orkers)
	var refused *GPUWithoutGPUProfileError
	if !errors.As(err, &refused) || refused.Task != "read" || !strings.Contains(err.Error(), "gpu profile") {
		t.Fatalf("a gpu count on a devices profile = %v, want GPUWithoutGPUProfileError naming read", err)
	}
}

// TestRequestFitsOne_GPU: a gpu count fits an Orker whose max_request.gpu
// holds it, together with the cpu and memory; an Orker reporting no gpu holds
// none.
func TestRequestFitsOne_GPU(t *testing.T) {
	one := int64(1)
	two := int64(2)
	caps := []*orkstackv1.ResourceQuantity{{Cpu: 8, MemoryBytes: 16 << 30, Gpu: &one}, {Cpu: 2, MemoryBytes: 4 << 30, Gpu: &two}}
	for _, tc := range []struct {
		cpu  string
		gpu  int32
		want bool
	}{
		{"", 0, true},
		{"", 2, true},
		{"4", 1, true},
		{"4", 2, false}, // 4 cpu fits only the first, 2 gpu only the second
		{"", 3, false},
	} {
		if got := requestFitsOne(tc.cpu, "", tc.gpu, caps); got != tc.want {
			t.Errorf("cpu %q gpu %d = %v, want %v", tc.cpu, tc.gpu, got, tc.want)
		}
	}
	if requestFitsOne("", "", 1, []*orkstackv1.ResourceQuantity{{Cpu: 8, MemoryBytes: 16 << 30}}) {
		t.Error("an Orker with no gpu held a gpu count")
	}
	err := (&PoolResourcesUnsatisfiableError{Namespace: "ns", Pool: "gpu", Task: "train", GPU: 3,
		Largest: &orkstackv1.ResourceQuantity{Cpu: 8, MemoryBytes: 1, Gpu: &two}}).Error()
	for _, want := range []string{"gpu 3", "2 gpu units", "orker.resources.max_gpu"} {
		if !strings.Contains(err, want) {
			t.Errorf("the refusal %q does not say %q", err, want)
		}
	}
}
