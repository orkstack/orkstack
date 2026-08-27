package orker

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestSecurityPolicy_RefusesAnOverCapRequestRatherThanClamping is §9.3's
// central choice, stated as a test.
//
// Clamping is the tempting behavior and it is wrong: a workload that asked for
// 64 GiB and silently got 4 GiB fails later, confusingly, somewhere in its own
// code. Refusing fails now, with a message naming the cap and the variable
// that sets it.
func TestSecurityPolicy_RefusesAnOverCapRequestRatherThanClamping(t *testing.T) {
	policy := DefaultSecurityPolicy()
	policy.MaxCPU = "4"
	policy.MaxMemory = "8Gi"

	tests := []struct {
		name    string
		request *orkstackv1.ResourceRequest
		wantErr bool
		wantCPU string
		wantMem string
	}{
		{
			name:    "a request under both caps is honored verbatim",
			request: &orkstackv1.ResourceRequest{Cpu: "2", Memory: "4Gi"},
			wantCPU: "2", wantMem: "4Gi",
		},
		{
			name:    "absence is not a request: the policy default applies",
			request: nil,
			wantCPU: policy.DefaultCPU, wantMem: policy.DefaultMemory,
		},
		{
			name:    "a request at exactly the cap is allowed",
			request: &orkstackv1.ResourceRequest{Cpu: "4", Memory: "8Gi"},
			wantCPU: "4", wantMem: "8Gi",
		},
		{
			name:    "cpu over the cap is refused",
			request: &orkstackv1.ResourceRequest{Cpu: "8"},
			wantErr: true,
		},
		{
			name:    "memory over the cap is refused",
			request: &orkstackv1.ResourceRequest{Memory: "64Gi"},
			wantErr: true,
		},
		{
			name:    "millicpu is understood, and 500m is under a cap of 4",
			request: &orkstackv1.ResourceRequest{Cpu: "500m"},
			wantCPU: "500m", wantMem: policy.DefaultMemory,
		},
		{
			name:    "an unparseable quantity is refused rather than defaulted",
			request: &orkstackv1.ResourceRequest{Memory: "lots"},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cpu, memory, failure := policy.ResolveResources(tc.request)
			if tc.wantErr {
				if failure == nil {
					t.Fatalf("ResolveResources(%v) succeeded with cpu=%q memory=%q; want a refusal",
						tc.request, cpu, memory)
				}
				if failure.Reason != ReasonResourceRefused {
					t.Errorf("reason = %q, want %q", failure.Reason, ReasonResourceRefused)
				}
				return
			}
			if failure != nil {
				t.Fatalf("ResolveResources(%v) = %v, want success", tc.request, failure)
			}
			if cpu != tc.wantCPU || memory != tc.wantMem {
				t.Errorf("effective resources = (%q, %q), want (%q, %q)",
					cpu, memory, tc.wantCPU, tc.wantMem)
			}
		})
	}
}

// TestSecurityPolicy_NeverYieldsUnlimitedResources is the §9.3 invariant: no
// workload ever receives unlimited resources, INCLUDING one that requested
// nothing.
func TestSecurityPolicy_NeverYieldsUnlimitedResources(t *testing.T) {
	cpu, memory, failure := DefaultSecurityPolicy().ResolveResources(nil)
	if failure != nil {
		t.Fatalf("a task that requested nothing was refused: %v", failure)
	}
	if cpu == "" || memory == "" {
		t.Errorf("a task that requested nothing got cpu=%q memory=%q; both must always be bounded",
			cpu, memory)
	}
}

// TestSecurityPolicy_NetworkIsOneOfExactlyTwoModes covers §9.1's rule that no
// mode other than default|none can be expressed.
func TestSecurityPolicy_NetworkIsOneOfExactlyTwoModes(t *testing.T) {
	policy := DefaultSecurityPolicy()
	tests := []struct {
		mode    orkstackv1.NetworkMode
		want    string
		wantErr bool
	}{
		{orkstackv1.NetworkMode_NETWORK_MODE_UNSPECIFIED, NetworkDefault, false},
		{orkstackv1.NetworkMode_NETWORK_MODE_DEFAULT, NetworkDefault, false},
		{orkstackv1.NetworkMode_NETWORK_MODE_NONE, NetworkNone, false},
		{orkstackv1.NetworkMode(99), "", true},
	}
	for _, tc := range tests {
		got, failure := policy.ResolveNetwork(tc.mode)
		if tc.wantErr {
			if failure == nil {
				t.Errorf("ResolveNetwork(%v) = %q, want a refusal", tc.mode, got)
			} else if failure.Reason != ReasonNetworkModeRefused {
				t.Errorf("reason = %q, want %q", failure.Reason, ReasonNetworkModeRefused)
			}
			continue
		}
		if failure != nil || got != tc.want {
			t.Errorf("ResolveNetwork(%v) = (%q, %v), want (%q, nil)", tc.mode, got, failure, tc.want)
		}
	}
}

// TestSecurityPolicy_RefusesAnUnknownSecretScheme covers §9.4: an unknown
// scheme is refused rather than ignored. Ignoring it would run the workload
// with a secret file that silently is not there.
func TestSecurityPolicy_RefusesAnUnknownSecretScheme(t *testing.T) {
	policy := DefaultSecurityPolicy()
	if !policy.AllowsSecretScheme("env") || !policy.AllowsSecretScheme("file") {
		t.Fatalf("the default policy resolves %v; env and file are the documented schemes",
			policy.SecretSchemes)
	}
	for _, scheme := range []string{"vault", "http", "", "docker"} {
		if policy.AllowsSecretScheme(scheme) {
			t.Errorf("the default policy accepts secret scheme %q", scheme)
		}
	}
}

// TestSecurityPolicy_ProfileIDNamesTheEnforcedPosture keeps DescribeOrker
// honest: an operator reads this string to know what a pool's Orkers enforce.
func TestSecurityPolicy_ProfileIDNamesTheEnforcedPosture(t *testing.T) {
	id := DefaultSecurityPolicy().ProfileID()
	for _, want := range []string{"readonly", "cap-drop-all", "no-new-privileges", "seccomp=" + SeccompBuiltin} {
		if !strings.Contains(id, want) {
			t.Errorf("profile id %q does not name %q", id, want)
		}
	}
}

// TestParseMemory_UnderstandsTheKubernetesSpelling is the table the cap check
// depends on. A suffix read wrongly would refuse a legal request or admit an
// illegal one.
func TestParseMemory_UnderstandsTheKubernetesSpelling(t *testing.T) {
	tests := []struct {
		raw  string
		want float64
	}{
		{"1024", 1024},
		{"1Ki", 1024},
		{"1Mi", 1 << 20},
		{"4Gi", 4 << 30},
		{"1Ti", 1 << 40},
		{"1M", 1e6},
		{"2G", 2e9},
	}
	for _, tc := range tests {
		got, err := parseMemory(tc.raw)
		if err != nil {
			t.Errorf("parseMemory(%q): %v", tc.raw, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseMemory(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
	if _, err := parseMemory("4GB"); err == nil {
		t.Error("parseMemory(\"4GB\") succeeded; GB is not the Kubernetes spelling and " +
			"guessing at it would silently change a cap")
	}
}
