package orker

import (
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func TestSecurityPolicyFromEnvRefusesInvalidProcessLimits(t *testing.T) {
	for _, tc := range []struct {
		key   string
		value string
	}{
		{key: PidsLimitVar, value: "1k"},
		{key: PidsLimitVar, value: "0"},
		{key: PidsLimitVar, value: "-1"},
		{key: TmpfsBytesVar, value: "abc"},
		{key: TmpfsBytesVar, value: "0"},
		{key: TmpfsBytesVar, value: "-1"},
	} {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			policy := SecurityPolicyFromEnv()
			err := policy.Validate()
			if err == nil {
				t.Fatalf("%s=%q silently fell back to the default policy", tc.key, tc.value)
			}
			if message := err.Error(); !strings.Contains(message, tc.key) || !strings.Contains(message, tc.value) {
				t.Errorf("validation error %q must name %s and its rejected value %q", message, tc.key, tc.value)
			}
		})
	}
}

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

// TestSecurityPolicy_UnreadableCapIsRefusedNotRemoved covers the defect where
// an operator typo silently DELETED the ceiling instead of refusing.
//
// Both cap comparisons were guarded with `if parseErr == nil && requested >
// cap`, so a cap the parser cannot read simply skipped the comparison. The
// obvious typo — "16GB", a spelling this package's own parseMemory test
// already asserts is unreadable — therefore granted a workload any size it
// asked for, contradicting AGENTS.md §6 ("Workflows must not receive
// unlimited resources by default") and this file's own "None of them is
// unlimited".
func TestSecurityPolicy_UnreadableCapIsRefusedNotRemoved(t *testing.T) {
	tests := []struct {
		name      string
		maxCPU    string
		maxMemory string
		reqCPU    string
		reqMemory string
		wantVar   string
	}{
		{
			name:      "unreadable memory cap does not grant an enormous request",
			maxCPU:    "8",
			maxMemory: "16GB", // the operator typo: GB is not the Kubernetes spelling
			reqCPU:    "1",
			reqMemory: "1024Ti",
			wantVar:   MaxMemoryVar,
		},
		{
			name:      "unreadable cpu cap does not grant an enormous request",
			maxCPU:    "8 cores", // free text, unparseable
			maxMemory: "16Gi",
			reqCPU:    "4096",
			reqMemory: "1Gi",
			wantVar:   MaxCPUVar,
		},
		{
			name:      "empty memory cap is not an absent ceiling",
			maxCPU:    "8",
			maxMemory: "",
			reqCPU:    "1",
			reqMemory: "1024Ti",
			wantVar:   MaxMemoryVar,
		},
		{
			name:      "empty cpu cap is not an absent ceiling",
			maxCPU:    "",
			maxMemory: "16Gi",
			reqCPU:    "4096",
			reqMemory: "1Gi",
			wantVar:   MaxCPUVar,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policy := DefaultSecurityPolicy()
			policy.MaxCPU = tc.maxCPU
			policy.MaxMemory = tc.maxMemory

			cpu, memory, failure := policy.ResolveResources(&orkstackv1.ResourceRequest{
				Cpu:    tc.reqCPU,
				Memory: tc.reqMemory,
			})
			if failure == nil {
				t.Fatalf("an unreadable cap granted cpu=%q memory=%q with no failure; "+
					"a cap the parser cannot read must be a refusal, never a removed ceiling",
					cpu, memory)
			}
			if !strings.Contains(failure.Error(), tc.wantVar) {
				t.Errorf("failure %q does not name %s; an operator cannot fix a typo "+
					"they are not told about", failure.Error(), tc.wantVar)
			}
		})
	}
}

// TestSecurityPolicy_ValidateRefusesAnUnreadableCapAtBoot is the same defect
// caught one layer earlier. An operator typo should stop the process at boot
// with a message naming the variable, not wait to over-grant silently on the
// first dispatch.
func TestSecurityPolicy_ValidateRefusesAnUnreadableCapAtBoot(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*SecurityPolicy)
		wantVar string
	}{
		{"unreadable max memory", func(p *SecurityPolicy) { p.MaxMemory = "16GB" }, MaxMemoryVar},
		{"unreadable max cpu", func(p *SecurityPolicy) { p.MaxCPU = "8 cores" }, MaxCPUVar},
		{"unreadable default memory", func(p *SecurityPolicy) { p.DefaultMemory = "1GB" }, DefaultMemoryVar},
		{"unreadable default cpu", func(p *SecurityPolicy) { p.DefaultCPU = "one" }, DefaultCPUVar},
		{"empty max memory", func(p *SecurityPolicy) { p.MaxMemory = "" }, MaxMemoryVar},
		{"default above the cap", func(p *SecurityPolicy) { p.DefaultMemory = "32Gi" }, MaxMemoryVar},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			policy := DefaultSecurityPolicy()
			tc.mutate(&policy)
			err := policy.Validate()
			if err == nil {
				t.Fatal("Validate accepted a policy an operator cannot have meant; " +
					"a bad cap must stop the Orker at boot")
			}
			if !strings.Contains(err.Error(), tc.wantVar) {
				t.Errorf("error %q does not name %s", err.Error(), tc.wantVar)
			}
		})
	}
	if err := DefaultSecurityPolicy().Validate(); err != nil {
		t.Fatalf("the default policy must validate: %v", err)
	}
}
