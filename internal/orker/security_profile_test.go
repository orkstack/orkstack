package orker

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/quantity"
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
		{
			name:    "zero cpu is refused rather than interpreted as unlimited",
			request: &orkstackv1.ResourceRequest{Cpu: "0"},
			wantErr: true,
		},
		{
			name:    "negative cpu is refused",
			request: &orkstackv1.ResourceRequest{Cpu: "-1"},
			wantErr: true,
		},
		{
			name:    "zero millicpu is refused rather than interpreted as unlimited",
			request: &orkstackv1.ResourceRequest{Cpu: "0m"},
			wantErr: true,
		},
		{
			name:    "NaN cpu is refused",
			request: &orkstackv1.ResourceRequest{Cpu: "NaN"},
			wantErr: true,
		},
		{
			name:    "one cpu remains accepted",
			request: &orkstackv1.ResourceRequest{Cpu: "1"},
			wantCPU: "1", wantMem: policy.DefaultMemory,
		},
		// Memory has the same trap cpu has: docker reads `--memory 0` as NO
		// limit, so a zero, negative, fractional-byte or non-finite request is
		// an unlimited container, not a small one.
		{
			name:    "zero memory is refused rather than interpreted as unlimited",
			request: &orkstackv1.ResourceRequest{Memory: "0"},
			wantErr: true,
		},
		{
			name:    "zero memory with a unit is refused",
			request: &orkstackv1.ResourceRequest{Memory: "0Gi"},
			wantErr: true,
		},
		{
			name:    "less than one byte is refused, because it renders as zero",
			request: &orkstackv1.ResourceRequest{Memory: "0.5"},
			wantErr: true,
		},
		{
			name:    "negative memory is refused",
			request: &orkstackv1.ResourceRequest{Memory: "-1"},
			wantErr: true,
		},
		{
			name:    "negative memory with a unit is refused",
			request: &orkstackv1.ResourceRequest{Memory: "-4Gi"},
			wantErr: true,
		},
		{
			name:    "NaN memory is refused",
			request: &orkstackv1.ResourceRequest{Memory: "NaN"},
			wantErr: true,
		},
		{
			name:    "infinite memory is refused",
			request: &orkstackv1.ResourceRequest{Memory: "Inf"},
			wantErr: true,
		},
		{
			name:    "one byte remains accepted",
			request: &orkstackv1.ResourceRequest{Memory: "1"},
			wantCPU: policy.DefaultCPU, wantMem: "1",
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
// honest: an operator reads this string to know what a pool's Orkers enforce,
// and its seccomp part is what the runtime actually spelled (G1): `builtin`
// by name on Docker, nothing on the Podman engine, which applies its own
// default, and "unknown" for a runtime that could not say.
func TestSecurityPolicy_ProfileIDNamesTheEnforcedPosture(t *testing.T) {
	for spelled, want := range map[string]string{
		SeccompBuiltin:       "seccomp=" + SeccompBuiltin,
		SeccompEngineDefault: "seccomp=" + SeccompEngineDefault,
		"":                   "seccomp=unknown",
	} {
		id := DefaultSecurityPolicy().ProfileID(spelled)
		for _, part := range []string{"readonly", "cap-drop-all", "no-new-privileges", want} {
			if !strings.Contains(id, part) {
				t.Errorf("profile id %q does not name %q", id, part)
			}
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
		got, err := quantity.ParseMemory(tc.raw)
		if err != nil {
			t.Errorf("quantity.ParseMemory(%q): %v", tc.raw, err)
			continue
		}
		if got != tc.want {
			t.Errorf("quantity.ParseMemory(%q) = %v, want %v", tc.raw, got, tc.want)
		}
	}
	if _, err := quantity.ParseMemory("4GB"); err == nil {
		t.Error("quantity.ParseMemory(\"4GB\") succeeded; GB is not the Kubernetes spelling and " +
			"guessing at it would silently change a cap")
	}
}

// TestSecurityPolicy_UnreadableCapIsRefusedNotRemoved covers the defect where
// an operator typo silently DELETED the ceiling instead of refusing.
//
// Both cap comparisons were guarded with `if parseErr == nil && requested >
// cap`, so a cap the parser cannot read simply skipped the comparison. The
// obvious typo — "16GB", a spelling this package's own quantity.ParseMemory test
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
		// A cap that parses but is not a quantity is as much a typo as one
		// that does not parse: NaN is above and below nothing, so a NaN cap
		// removes the ceiling, and a zero default is an unlimited container.
		{"NaN max cpu", func(p *SecurityPolicy) { p.MaxCPU = "NaN" }, MaxCPUVar},
		{"zero max cpu", func(p *SecurityPolicy) { p.MaxCPU = "0" }, MaxCPUVar},
		{"infinite max cpu", func(p *SecurityPolicy) { p.MaxCPU = "Inf" }, MaxCPUVar},
		{"NaN max memory", func(p *SecurityPolicy) { p.MaxMemory = "NaN" }, MaxMemoryVar},
		{"zero max memory", func(p *SecurityPolicy) { p.MaxMemory = "0" }, MaxMemoryVar},
		{"infinite max memory", func(p *SecurityPolicy) { p.MaxMemory = "Inf" }, MaxMemoryVar},
		{"a max memory no int64 holds", func(p *SecurityPolicy) { p.MaxMemory = "1e30" }, MaxMemoryVar},
		{"zero default cpu", func(p *SecurityPolicy) { p.DefaultCPU = "0" }, DefaultCPUVar},
		{"negative default cpu", func(p *SecurityPolicy) { p.DefaultCPU = "-1" }, DefaultCPUVar},
		{"zero default memory", func(p *SecurityPolicy) { p.DefaultMemory = "0" }, DefaultMemoryVar},
		{"NaN default memory", func(p *SecurityPolicy) { p.DefaultMemory = "NaN" }, DefaultMemoryVar},
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

func TestSecurityPolicyFromEnv_ReadsTheSecretFloor(t *testing.T) {
	t.Setenv(SecretAllowVar, "env:SANDBOX_*, file:/etc/orkstack/secrets/*,")
	policy := SecurityPolicyFromEnv()
	if got := strings.Join(policy.SecretAllow, "|"); got != "env:SANDBOX_*|file:/etc/orkstack/secrets/*" {
		t.Fatalf("SecretAllow = %q, want the two trimmed entries", policy.SecretAllow)
	}
	if err := policy.Validate(); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
	t.Setenv(SecretAllowVar, "")
	if policy := SecurityPolicyFromEnv(); policy.SecretAllow != nil {
		t.Fatalf("unset floor = %q, want nil", policy.SecretAllow)
	}
	if DefaultSecurityPolicy().SecretAllow != nil {
		t.Fatal("DefaultSecurityPolicy carries a floor")
	}
}

func TestSecurityPolicy_ValidateRefusesAMalformedFloorAtBoot(t *testing.T) {
	for _, entry := range []string{"file:relative", "env:A B"} {
		policy := DefaultSecurityPolicy()
		policy.SecretAllow = []string{entry}
		err := policy.Validate()
		if err == nil || !strings.Contains(err.Error(), SecretAllowVar) || !strings.Contains(err.Error(), entry) {
			t.Errorf("Validate(%q) = %v, want a refusal naming %s and the entry", entry, err, SecretAllowVar)
		}
	}
	// Through the environment, as boot reads it: "A B" survives the comma
	// split and trim, so it reaches the rule.
	t.Setenv(SecretAllowVar, "env:A B")
	if err := SecurityPolicyFromEnv().Validate(); err == nil || !strings.Contains(err.Error(), SecretAllowVar) {
		t.Fatalf("Validate() from env = %v, want a refusal naming %s", err, SecretAllowVar)
	}
}

func TestSecurityPolicy_ValidateRefusesAFloorNamingAnUnresolvedScheme(t *testing.T) {
	policy := DefaultSecurityPolicy()
	policy.SecretAllow = []string{"env:SANDBOX_*", "vault:*"}
	err := policy.Validate()
	if err == nil {
		t.Fatal("Validate() accepted a floor naming a scheme this Orker never resolves")
	}
	for _, want := range []string{SecretAllowVar, SecretSchemesVar, `"vault:*"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Validate() = %q, want it to name %s", err, want)
		}
	}
}

func TestSecurityPolicy_AllowsSecretSourceIsTheSharedRule(t *testing.T) {
	for _, v := range dsl.SecretPolicyVectors() {
		policy := DefaultSecurityPolicy()
		if v.Pattern != "" {
			policy.SecretAllow = []string{v.Pattern}
		}
		// The Orker reads a file by its name, so a file source whose name is
		// not a plain absolute path is refused under an empty floor too, by
		// the same rule a floor refuses it with.
		permitted, why := v.Permitted, v.Why
		var path *dsl.SecretSourceRefusal
		if errors.As(dsl.CheckSecretPath(v.Source), &path) {
			permitted, why = false, path.Why
		}
		err := policy.AllowsSecretSource(v.Source)
		if permitted != (err == nil) {
			t.Errorf("floor %q, source %q: err = %v, want permitted=%v", v.Pattern, v.Source, err, permitted)
		}
		var refusal *dsl.SecretSourceRefusal
		if err != nil && (!errors.As(err, &refusal) || refusal.Why != why) {
			t.Errorf("floor %q, source %q: err = %#v, want Why %q", v.Pattern, v.Source, err, why)
		}
	}
	if err := DefaultSecurityPolicy().AllowsSecretSource("env:ANYTHING"); err != nil {
		t.Fatalf("an empty floor refused: %v", err)
	}
}

// TestReservedResources_AgreesWithResolveResources pins the numbers an
// attempt record carries to the strings the container is given: one
// resolution, two renderings, never two decisions.
func TestReservedResources_AgreesWithResolveResources(t *testing.T) {
	policy := DefaultSecurityPolicy()
	cases := []struct {
		name string
		req  *orkstackv1.ResourceRequest
		want *orkstackv1.ReservedResources // nil = refused
	}{
		{"empty request gets the defaults", nil, &orkstackv1.ReservedResources{Cpu: 1, MemoryBytes: 1 << 30}},
		{"whole cores and Gi", &orkstackv1.ResourceRequest{Cpu: "2", Memory: "4Gi"}, &orkstackv1.ReservedResources{Cpu: 2, MemoryBytes: 4 << 30}},
		{"millicores and Mi", &orkstackv1.ResourceRequest{Cpu: "500m", Memory: "512Mi"}, &orkstackv1.ReservedResources{Cpu: 0.5, MemoryBytes: 512 << 20}},
		{"plain bytes, default cpu", &orkstackv1.ResourceRequest{Memory: "1000000"}, &orkstackv1.ReservedResources{Cpu: 1, MemoryBytes: 1000000}},
		{"cpu above the cap is refused", &orkstackv1.ResourceRequest{Cpu: "9"}, nil},
		{"zero cpu is refused", &orkstackv1.ResourceRequest{Cpu: "0"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := policy.ReservedResources(tc.req)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("ReservedResources = %v, want nil for a refused request", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("ReservedResources = nil, want %v", tc.want)
			}
			if got.GetCpu() != tc.want.GetCpu() || got.GetMemoryBytes() != tc.want.GetMemoryBytes() {
				t.Fatalf("ReservedResources = {%v, %d}, want {%v, %d}",
					got.GetCpu(), got.GetMemoryBytes(), tc.want.GetCpu(), tc.want.GetMemoryBytes())
			}
			cpu, memory, failure := policy.ResolveResources(tc.req)
			if failure != nil {
				t.Fatalf("ResolveResources refused what ReservedResources admitted: %v", failure)
			}
			cores, err := quantity.ParseCPU(cpu)
			if err != nil || cores != got.GetCpu() {
				t.Errorf("task.json cpu %q parses to %v (%v), the record says %v", cpu, cores, err, got.GetCpu())
			}
			bytes, err := quantity.ParseMemory(memory)
			if err != nil || int64(bytes) != got.GetMemoryBytes() {
				t.Errorf("task.json memory %q parses to %v (%v), the record says %d", memory, bytes, err, got.GetMemoryBytes())
			}
		})
	}
}

// assertOwn holds every source to the built-in exclusion: refused as one of
// the Orker's own, by an *OwnSourceRefusal that names the source.
func assertOwn(t *testing.T, policy SecurityPolicy, sources ...string) {
	t.Helper()
	for _, source := range sources {
		err := policy.AllowsSecretSource(source)
		var refusal *OwnSourceRefusal
		if !errors.As(err, &refusal) {
			t.Errorf("AllowsSecretSource(%q) = %v, want it refused as one of this orker's own", source, err)
			continue
		}
		if refusal.Source != source || !strings.Contains(err.Error(), "this orker's own") {
			t.Errorf("AllowsSecretSource(%q) = %q (source %q), want the refusal to name the source", source, err, refusal.Source)
		}
	}
}

func assertNotOwn(t *testing.T, policy SecurityPolicy, sources ...string) {
	t.Helper()
	for _, source := range sources {
		if err := policy.AllowsSecretSource(source); err != nil {
			t.Errorf("AllowsSecretSource(%q) = %v, want it permitted", source, err)
		}
	}
}

// TestSecurityPolicy_TheDefaultFloorRefusesTheOrkersOwnEnvironment: with no
// floor at all, the Orker still never resolves the variables that configure
// it — OrkStack's settings, its telemetry exporters, the proxies its clients
// dial through — and a zero policy is no exception.
func TestSecurityPolicy_TheDefaultFloorRefusesTheOrkersOwnEnvironment(t *testing.T) {
	own := []string{
		"env:ORK_S3_SECRET_KEY", "env:ORK_S3_ACCESS_KEY", "env:ORK_S3_SESSION_TOKEN",
		"env:ORK_TELEMETRY_SINKS", "env:ORK_CONFIG_FILE", "env:ORK_",
		"env:OTEL_EXPORTER_OTLP_HEADERS", "env:OTEL_EXPORTER_OTLP_TRACES_HEADERS", "env:OTEL_ANYTHING",
		"env:HTTP_PROXY", "env:HTTPS_PROXY", "env:http_proxy", "env:https_proxy", "env:ALL_PROXY", "env:all_proxy",
	}
	notOwn := []string{
		"env:SANDBOX_DB", "env:ORK", "env:ORKSTACK_TASK_TOKEN", "env:OTEL",
		"env:NO_PROXY", "env:HTTPS_PROXY_TOKEN",
		// A variable's name is case-sensitive, so these are other
		// variables than the Orker's settings.
		"env:ork_s3_secret_key", "env:Ork_S3_Secret_Key", "env:otel_exporter_otlp_headers",
	}
	for _, policy := range []SecurityPolicy{DefaultSecurityPolicy(), {}} {
		assertOwn(t, policy, own...)
		assertNotOwn(t, policy, notOwn...)
	}
}

// TestSecurityPolicy_TheDefaultFloorRefusesTheOrkersProcessFiles: /proc and
// /dev are the Orker's own process — its environment, its memory and its
// descriptors — and /run/secrets is where its container's own credentials
// are mounted. A source is judged by its name and by what its links resolve
// to, so a link does not reach them either.
func TestSecurityPolicy_TheDefaultFloorRefusesTheOrkersProcessFiles(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "environ")
	if err := os.Symlink("/proc/self/environ", link); err != nil {
		t.Fatal(err)
	}
	dirLink := filepath.Join(dir, "self")
	if err := os.Symlink("/proc/self", dirLink); err != nil {
		t.Fatal(err)
	}
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A link that leads nowhere yet is judged by where it would lead.
	dangling := filepath.Join(dir, "dangling")
	if err := os.Symlink("/proc/self/no-such-file", dangling); err != nil {
		t.Fatal(err)
	}
	own := []string{
		"file:/proc", "file:/proc/self/environ", "file:/proc/1/environ", "file:/proc/thread-self/environ",
		"file:/proc/self/root/etc/hostname", "file:/proc/self/fd/0",
		"file:/dev/fd/0", "file:/dev/stdin", "file:/dev/zero",
		"file:/run/secrets/kubernetes.io/serviceaccount/token",
		"file:/var/run/secrets/kubernetes.io/serviceaccount/token",
		"file:/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
	}
	// A link is judged by where it leads, which takes a /proc to lead to.
	if _, err := os.Stat("/proc/self/environ"); err == nil {
		own = append(own, "file:"+link, "file:"+dirLink+"/environ", "file:"+dangling)
	}
	notOwn := []string{
		"file:/procfs/x", "file:/devices/x", "file:/run/secretsx/a", "file:/etc/orkstack/task-secrets/db",
		"file:" + plain,
		// A path is compared byte for byte, as a case-sensitive file
		// system names it, so this is another path than /proc's.
		"file:/PROC/self/environ",
	}
	for _, policy := range []SecurityPolicy{DefaultSecurityPolicy(), {}} {
		assertOwn(t, policy, own...)
		assertNotOwn(t, policy, notOwn...)
	}
}

// TestSecurityPolicy_AFileSourceIsReadOnlyByAPlainAbsoluteName: a name with a
// "..", "." or empty segment, or a relative one, is refused under any floor,
// empty included. The kernel applies ".." after it follows a link, so
// /var/run/../proc — where /var/run links to /run — is /proc, though the
// name, cleaned, is /var/proc: no check of such a name holds, and the floor's
// own rule already refuses it.
func TestSecurityPolicy_AFileSourceIsReadOnlyByAPlainAbsoluteName(t *testing.T) {
	dir := t.TempDir()
	deep := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	shallow := filepath.Join(dir, "shallow")
	if err := os.Symlink(deep, shallow); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ source, why string }{
		{"file:/var/run/../proc/self/environ", ".."},
		{"file:" + shallow + "/../../../proc/self/environ", ".."},
		{"file:/etc/../proc/self/environ", ".."},
		{"file:/proc//self/environ", "empty"},
		{"file:/proc/./self/environ", "."},
		{"file:/etc/orkstack/task-secrets/", "empty"},
		{"file:proc/self/environ", "relative"},
		{"file:./db", "relative"},
	} {
		for _, policy := range []SecurityPolicy{DefaultSecurityPolicy(), {}} {
			err := policy.AllowsSecretSource(tc.source)
			var refusal *dsl.SecretSourceRefusal
			if !errors.As(err, &refusal) || refusal.Why != tc.why || refusal.Source != tc.source {
				t.Errorf("AllowsSecretSource(%q) = %#v, want a refusal of its %s segment", tc.source, err, tc.why)
			}
		}
	}
}

// TestSecurityPolicy_TheOrkersOwnFilesAreRefusedByNameAndByLink: a file the
// Orker's own configuration names is refused under its own name, under any
// name that links to it, and — mounted the way Kubernetes mounts a ConfigMap
// or a Secret, as a link into a dated directory — under the names of that
// directory too. The refusal says only that the source is one of the
// Orker's files, never which one or where a link led.
func TestSecurityPolicy_TheOrkersOwnFilesAreRefusedByNameAndByLink(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config")
	dated := filepath.Join(cfg, "..2026_10_30_00_00_00.000000001")
	if err := os.MkdirAll(dated, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orker.yaml", "other.yaml"} {
		if err := os.WriteFile(filepath.Join(dated, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Base(dated), filepath.Join(cfg, "..data")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orker.yaml", "other.yaml"} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(cfg, name)); err != nil {
			t.Fatal(err)
		}
	}
	work := filepath.Join(dir, "work")
	if err := os.MkdirAll(filepath.Join(work, "ork-run-task-1-x", "secrets"), 0o755); err != nil {
		t.Fatal(err)
	}
	elsewhere := filepath.Join(dir, "elsewhere")
	if err := os.Symlink(filepath.Join(work, "ork-run-task-1-x", "secrets"), elsewhere); err != nil {
		t.Fatal(err)
	}
	// A link into the work root that leads nowhere yet: a later attempt's
	// workspace.
	dangling := filepath.Join(dir, "dangling")
	if err := os.Symlink(filepath.Join(work, "ork-run-task-2-y", "secrets", "db"), dangling); err != nil {
		t.Fatal(err)
	}

	policy := DefaultSecurityPolicy().WithOwnPaths(
		OwnPath{Path: filepath.Join(cfg, "orker.yaml"), Why: "its configuration file"},
		OwnPath{Path: work, Why: "its work root"},
	)
	own := []string{
		"file:" + filepath.Join(cfg, "orker.yaml"),
		"file:" + filepath.Join(cfg, "..data", "orker.yaml"),
		"file:" + filepath.Join(dated, "orker.yaml"),
		"file:" + work,
		"file:" + filepath.Join(work, "ork-run-task-1-x", "secrets", "db"),
		"file:" + filepath.Join(elsewhere, "db"),
		"file:" + dangling,
	}
	assertOwn(t, policy, own...)
	assertNotOwn(t, policy,
		"file:"+filepath.Join(cfg, "other.yaml"),
		"file:"+filepath.Join(dir, "workx", "db"),
	)

	// Every refusal of a file says the same thing: never which of the
	// Orker's files a name is, nor where a link led, so a task cannot learn
	// the Orker's paths by trying names.
	for _, source := range own {
		err := policy.AllowsSecretSource(source)
		want := fmt.Sprintf("%q is this orker's own (one of its files), which no secret floor admits", source)
		if err == nil || err.Error() != want {
			t.Errorf("AllowsSecretSource(%q) = %v, want %q", source, err, want)
		}
	}
}

// TestSecurityPolicy_AnOwnPathIsResolvedAsTheKernelOpensIt: an own path is
// what the kernel would open for its name, "..", "." and relative names
// included — a ".." after a link is the link target's parent, not the
// link's — and its cleaned name is held as the Orker's own too.
func TestSecurityPolicy_AnOwnPathIsResolvedAsTheKernelOpensIt(t *testing.T) {
	dir := t.TempDir()
	deep := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(deep, filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	t.Chdir(dir)
	policy := DefaultSecurityPolicy().WithOwnPaths(
		OwnPath{Path: filepath.Join(dir, "link") + "/../secret", Why: "its configuration"},
		OwnPath{Path: "rel/./key", Why: "its configuration"},
	)
	assertOwn(t, policy,
		"file:"+filepath.Join(dir, "a", "secret"),
		"file:"+filepath.Join(dir, "secret"),
		"file:"+filepath.Join(dir, "rel", "key"),
	)
	assertNotOwn(t, policy,
		"file:"+filepath.Join(deep, "secret"),
		"file:"+filepath.Join(dir, "a", "key"),
	)
}

// TestSecurityPolicy_AnOwnFileIsJudgedAsItIsWhenTheSourceIsJudged: an own
// file is compared with what it is when a source is judged, not only with
// what its name led to when the Orker started. Kubernetes updates a mounted
// ConfigMap or Secret by writing a new dated directory, pointing ..data at it
// and removing the old one; after that, every name of the new directory that
// reaches the Orker's file is still the Orker's own. So is a hard link to it,
// which is the same file under a name no link resolves.
func TestSecurityPolicy_AnOwnFileIsJudgedAsItIsWhenTheSourceIsJudged(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "config")
	dated := func(stamp string) string {
		d := filepath.Join(cfg, "..2026_10_31_00_00_00."+stamp)
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"orker.yaml", "other.yaml"} {
			if err := os.WriteFile(filepath.Join(d, name), []byte(stamp), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return d
	}
	first := dated("000000001")
	if err := os.Symlink(filepath.Base(first), filepath.Join(cfg, "..data")); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orker.yaml", "other.yaml"} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(cfg, name)); err != nil {
			t.Fatal(err)
		}
	}
	policy := DefaultSecurityPolicy().WithOwnPaths(OwnPath{Path: filepath.Join(cfg, "orker.yaml"), Why: "its configuration"})

	// The update, as the kubelet makes it: a new directory, ..data swapped to
	// it by a rename, the old one removed.
	second := dated("000000002")
	if err := os.Symlink(filepath.Base(second), filepath.Join(cfg, "..data_tmp")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(cfg, "..data_tmp"), filepath.Join(cfg, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(first); err != nil {
		t.Fatal(err)
	}
	elsewhere := filepath.Join(dir, "elsewhere")
	if err := os.Symlink(filepath.Join(second, "orker.yaml"), elsewhere); err != nil {
		t.Fatal(err)
	}
	hard := filepath.Join(dir, "hard")
	if err := os.Link(filepath.Join(second, "orker.yaml"), hard); err != nil {
		t.Fatal(err)
	}
	// An own path written with ".." after a link is the file the kernel opens
	// for it, not the file its cleaned name names, and so is a hard link to it.
	inner := filepath.Join(dir, "deep", "inner")
	if err := os.MkdirAll(inner, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(inner, filepath.Join(dir, "via")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "deep", "key"), []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(dir, "deep", "key"), filepath.Join(dir, "hard-key")); err != nil {
		t.Fatal(err)
	}
	policy = policy.WithOwnPaths(OwnPath{Path: filepath.Join(dir, "via") + "/../key", Why: "its credentials"})
	assertOwn(t, policy,
		"file:"+filepath.Join(cfg, "orker.yaml"),
		"file:"+filepath.Join(cfg, "..data", "orker.yaml"),
		"file:"+filepath.Join(second, "orker.yaml"),
		"file:"+elsewhere,
		"file:"+hard,
		"file:"+filepath.Join(dir, "hard-key"),
	)
	assertNotOwn(t, policy,
		"file:"+filepath.Join(cfg, "other.yaml"),
		"file:"+filepath.Join(cfg, "..data", "other.yaml"),
		"file:"+filepath.Join(second, "other.yaml"),
	)
}

// TestSecurityPolicy_AnUpdateWhileASourceIsJudgedIsSeen: the kubelet may swap
// a mount while a source is being judged, so the way the judgement walks can
// come to the directory the update is removing, or pass through the one it
// made after the own paths were looked at. Either way the source is the
// Orker's own, because a walk cut short looks again and every name a walk
// came to is compared with the own paths once more after it; a source that
// is not the Orker's own stays permitted under the same update.
func TestSecurityPolicy_AnUpdateWhileASourceIsJudgedIsSeen(t *testing.T) {
	cfg := filepath.Join(t.TempDir(), "config")
	stamp, current := 0, ""
	dated := func(n int) string {
		return filepath.Join(cfg, fmt.Sprintf("..2026_10_31_00_00_00.%09d", n))
	}
	// update is the kubelet's: a new directory, ..data swapped to it by a
	// rename, the old one removed.
	update := func() {
		stamp++
		next := dated(stamp)
		if err := os.MkdirAll(filepath.Join(next, "gcloud"), 0o755); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"orker.yaml", "other.yaml", filepath.Join("gcloud", "credentials.db")} {
			if err := os.WriteFile(filepath.Join(next, name), []byte(next), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Symlink(filepath.Base(next), filepath.Join(cfg, "..data_tmp")); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(cfg, "..data_tmp"), filepath.Join(cfg, "..data")); err != nil {
			t.Fatal(err)
		}
		if current != "" {
			if err := os.RemoveAll(current); err != nil {
				t.Fatal(err)
			}
		}
		current = next
	}
	update()
	for _, name := range []string{"orker.yaml", "gcloud"} {
		if err := os.Symlink(filepath.Join("..data", name), filepath.Join(cfg, name)); err != nil {
			t.Fatal(err)
		}
	}
	policy := DefaultSecurityPolicy().WithOwnPaths(
		OwnPath{Path: filepath.Join(cfg, "orker.yaml"), Why: "its configuration"},
		OwnPath{Path: filepath.Join(cfg, "gcloud"), Why: "its credentials"},
	)
	update() // since boot: what the policy resolved then is gone

	for _, tc := range []struct {
		what string
		// source and at are asked as the case runs: at is the name the walk
		// is about to look at when the update lands.
		source, at func() string
		own        bool
	}{
		{"its file, as the walk comes to the directory the update removes",
			func() string { return filepath.Join(cfg, "..data", "orker.yaml") }, func() string { return current }, true},
		{"another file there",
			func() string { return filepath.Join(cfg, "..data", "other.yaml") }, func() string { return current }, false},
		{"its directory's file, as the walk comes to the directory the update makes",
			func() string { return filepath.Join(dated(stamp+1), "gcloud", "credentials.db") },
			func() string { return dated(stamp + 1) }, true},
		{"another file there",
			func() string { return filepath.Join(dated(stamp+1), "other.yaml") }, func() string { return dated(stamp + 1) }, false},
	} {
		source, at, landed := "file:"+tc.source(), tc.at(), false
		lstat = func(name string) (fs.FileInfo, error) {
			if name == at && !landed {
				landed = true
				update()
			}
			return os.Lstat(name)
		}
		if tc.own {
			assertOwn(t, policy, source)
		} else {
			assertNotOwn(t, policy, source)
		}
		lstat = os.Lstat
		if !landed {
			t.Fatalf("%s: the walk never came to %s, so no update landed while %s was judged", tc.what, at, source)
		}
	}
}

// TestSecurityPolicy_ASourceWhoseWayPassesThroughAnOwnDirectoryIsTheOrkers: a
// source is the Orker's own when the kernel, opening it, looks a name up in
// one of the Orker's directories, wherever it ends. A link to one of the
// Orker's descriptors ends at a name the link's text gives, a deleted file's
// for instance, that lies in none of them; the way there passes through
// /proc, so it is the Orker's own. The rule is the same for every own
// directory, the work root included, and for a way that leaves one again by
// a link's "..": no lookup in one is judged by what it reads.
func TestSecurityPolicy_ASourceWhoseWayPassesThroughAnOwnDirectoryIsTheOrkers(t *testing.T) {
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		t.Skip("no /proc/self/fd on this host")
	}
	dir := t.TempDir()
	gone, err := os.CreateTemp(dir, "deleted-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gone.Close() })
	if err := os.Remove(gone.Name()); err != nil {
		t.Fatal(err)
	}
	descriptor := filepath.Join(dir, "descriptor")
	if err := os.Symlink(fmt.Sprintf("/proc/self/fd/%d", gone.Fd()), descriptor); err != nil {
		t.Fatal(err)
	}
	devfd := filepath.Join(dir, "devfd")
	if err := os.Symlink(fmt.Sprintf("/dev/fd/%d", gone.Fd()), devfd); err != nil {
		t.Fatal(err)
	}
	work := filepath.Join(dir, "work")
	if err := os.MkdirAll(filepath.Join(work, "ork-run-task-1-x"), 0o755); err != nil {
		t.Fatal(err)
	}
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	through := filepath.Join(dir, "through")
	if err := os.Symlink(filepath.Join(work, "ork-run-task-1-x")+"/../../plain", through); err != nil {
		t.Fatal(err)
	}
	policy := DefaultSecurityPolicy().WithWorkRoot(work)
	assertOwn(t, policy, "file:"+descriptor, "file:"+devfd, "file:"+through)
	assertNotOwn(t, policy, "file:"+plain)
}

// TestSecurityPolicy_ARefusalOfAnOwnSourceNamesNoRule: a task's refusal of
// one of the Orker's own sources says the same words for every variable, as
// for every file, never which rule or setting made it the Orker's: what the
// Orker's configuration names is the Orker's to know.
func TestSecurityPolicy_ARefusalOfAnOwnSourceNamesNoRule(t *testing.T) {
	policy := DefaultSecurityPolicy().WithOwnVars(OwnVar{Name: "VENDOR_KEY", Why: "its telemetry credentials"})
	for source, label := range map[string]string{
		"env:ORK_S3_SECRET_KEY":          "one of its variables",
		"env:OTEL_EXPORTER_OTLP_HEADERS": "one of its variables",
		"env:HTTPS_PROXY":                "one of its variables",
		"env:AWS_SECRET_ACCESS_KEY":      "one of its variables",
		"env:VENDOR_KEY":                 "one of its variables",
		"file:/proc/self/environ":        "one of its files",
	} {
		want := fmt.Sprintf("%q is this orker's own (%s), which no secret floor admits", source, label)
		if err := policy.AllowsSecretSource(source); err == nil || err.Error() != want {
			t.Errorf("AllowsSecretSource(%q) = %v, want %q", source, err, want)
		}
	}
}

// TestSecurityPolicy_ARefusalQuotesALongSourceByItsHead: a variable's name
// has no bound of its own, so a refusal quotes a source in full only up to
// the longest file source the Orker reads, and a longer one by its head and
// its length, whichever rule refused it.
func TestSecurityPolicy_ARefusalQuotesALongSourceByItsHead(t *testing.T) {
	long := strings.Repeat("A", 200<<10)
	floor := DefaultSecurityPolicy()
	floor.SecretAllow = []string{"env:SANDBOX_*"}
	for _, tc := range []struct {
		policy SecurityPolicy
		source string
	}{
		{DefaultSecurityPolicy(), "env:ORK_" + long},
		{floor, "env:PROD_" + long},
	} {
		err := tc.policy.AllowsSecretSource(tc.source)
		if err == nil {
			t.Fatalf("AllowsSecretSource(%d bytes) = nil, want a refusal", len(tc.source))
		}
		head := fmt.Sprintf("%q… (%d bytes)", tc.source[:64], len(tc.source))
		if len(err.Error()) > 512 || !strings.HasPrefix(err.Error(), head) {
			t.Errorf("AllowsSecretSource(%d bytes) = %d bytes beginning %q, want it to begin %s",
				len(tc.source), len(err.Error()), clip(err.Error()), head)
		}
	}
	fileSource := "file:/" + strings.Repeat("a", dsl.MaxSecretPathBytes-1)
	if err := floor.AllowsSecretSource(fileSource); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("%q", fileSource)) {
		t.Errorf("AllowsSecretSource(a file source of %d bytes) = %v, want it quoted in full", len(fileSource), err)
	}
}

// TestSecurityPolicy_JudgingAFileSourceIsBoundedWhateverItsName: a task
// writes its secret's source, so the name is the task's to choose, and the
// Orker judges it on every attempt. Whatever the name — as long as a start
// payload carries, or as long as the kernel opens and running through a link
// to its own directory again and again — judging it costs a bounded amount
// of the Orker's CPU, measured on this thread's clock where there is one, so
// a loaded host cannot move it, on an Orker configured with many files of its
// own. Each name of the way is looked at once a walk, and each own path once
// before and once after it, in at most two walks, so the looks are counted
// too: at most two for each name the source holds, and three for each own
// path.
func TestSecurityPolicy_JudgingAFileSourceIsBoundedWhateverItsName(t *testing.T) {
	dir := t.TempDir()
	if err := os.Symlink(".", filepath.Join(dir, "x")); err != nil {
		t.Fatal(err)
	}
	loop := dir
	for len(loop)+len("/x/secret") <= dsl.MaxSecretPathBytes {
		loop += "/x"
	}
	loop += "/secret"
	nowhere := "/zz"
	for len(nowhere)+len("/a") <= dsl.MaxSecretPathBytes {
		nowhere += "/a"
	}
	payload := "/zz" + strings.Repeat("/a", 100<<10)
	// A look at a name costs the kernel as much as the depth of the
	// directory it lies in, which the host's own tree sets and no task
	// deepens outside the Orker's own directories, where a walk stops; a
	// tree deeper than most hosts hold stands in for it.
	deep := filepath.Join(dir, "deep") + strings.Repeat("/a", 64)
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	deep += "/secret"
	if err := os.WriteFile(deep, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	own := make([]OwnPath, 128)
	for i := range own {
		path := filepath.Join(dir, "own", fmt.Sprintf("%03d.yaml", i))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		own[i] = OwnPath{Path: path, Why: "its configuration"}
	}
	policy := SecurityPolicyFromEnv().WithWorkRoot(t.TempDir()).WithOwnPaths(own...)
	roots := len(fixedOwnRoots()) + len(policy.own)

	const bound = 100 * time.Millisecond
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	for _, tc := range []struct{ name, source string }{
		{"the longest path the kernel opens, through a link to its own directory", "file:" + loop},
		{"the longest path the kernel opens, of nothing", "file:" + nowhere},
		{"a path of directories that exist, 64 deep", "file:" + deep},
		{"a path as long as a start payload", "file:" + payload},
	} {
		startCPU, cpu := threadCPU()
		startWall := time.Now()
		_ = policy.AllowsSecretSource(tc.source)
		cost, clock := time.Since(startWall), "wall clock"
		if end, ok := threadCPU(); cpu && ok {
			cost, clock = end-startCPU, "thread CPU time"
		}

		var names, looks, ownLooks int
		for _, name := range strings.Split(tc.source, "/") {
			if name != "" && name != "." {
				names++
			}
		}
		lstat = func(name string) (fs.FileInfo, error) { looks++; return os.Lstat(name) }
		stat = func(name string) (fs.FileInfo, error) { ownLooks++; return os.Stat(name) }
		_ = policy.AllowsSecretSource(tc.source)
		lstat, stat = os.Lstat, os.Stat

		t.Logf("%s (%d bytes): %v of %s; %d looks at its %d names, %d at %d own paths",
			tc.name, len(tc.source), cost, clock, looks, names, ownLooks, roots)
		if cost > bound {
			t.Errorf("%s (%d bytes): judging it took %v of %s, want at most %v", tc.name, len(tc.source), cost, clock, bound)
		}
		if looks > 2*names || ownLooks > 3*roots {
			t.Errorf("%s: judging it looked %d times at its %d names and %d times at %d own paths, "+
				"want at most two looks a name and three an own path", tc.name, looks, names, ownLooks, roots)
		}
	}
}

// TestSecurityPolicy_NoFloorEntryAdmitsTheOrkersOwn: the exclusion is
// absolute. A broad floor is a valid floor, and it still admits none of the
// Orker's own sources.
func TestSecurityPolicy_NoFloorEntryAdmitsTheOrkersOwn(t *testing.T) {
	policy := DefaultSecurityPolicy().WithOwnVars(OwnVar{Name: "VENDOR_KEY", Why: "its telemetry credentials"})
	policy.SecretAllow = []string{"env:*", "file:/*/*/*"}
	if err := policy.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want a broad floor accepted", err)
	}
	assertOwn(t, policy, "env:ORK_S3_SECRET_KEY", "env:OTEL_EXPORTER_OTLP_HEADERS", "env:HTTPS_PROXY", "env:VENDOR_KEY",
		"file:/proc/self/environ", "file:/dev/fd/0", "file:/run/secrets/token")
	assertNotOwn(t, policy, "env:SANDBOX_DB", "file:/etc/orkstack/db")
}

// TestSecurityPolicy_ValidateRefusesAFloorEntryThatAdmitsOnlyTheOrkersOwn:
// an entry every match of which is one of the Orker's own admits nothing,
// and "admits nothing" must not boot looking like "was applied" — the rule
// validateSecretAllow already holds an unresolved scheme to.
func TestSecurityPolicy_ValidateRefusesAFloorEntryThatAdmitsOnlyTheOrkersOwn(t *testing.T) {
	work := t.TempDir()
	withWork := func(entries ...string) SecurityPolicy {
		policy := DefaultSecurityPolicy().
			WithOwnPaths(OwnPath{Path: work, Why: "its work root"}).
			WithOwnVars(OwnVar{Name: "VENDOR_KEY", Why: "its telemetry credentials"})
		policy.SecretAllow = entries
		return policy
	}
	for _, entry := range []string{
		"env:ORK_S3_SECRET_KEY", "env:ORK_*", "env:ORK_S3_*", "env:OTEL_*", "env:HTTPS_PROXY", "env:VENDOR_KEY",
		"file:/proc/self/environ", "file:/proc/*/environ", "file:/dev/*",
		"file:/var/run/secrets/*/serviceaccount/token", "file:" + work + "/*",
	} {
		err := withWork(entry).Validate()
		if err == nil || !strings.Contains(err.Error(), SecretAllowVar) || !strings.Contains(err.Error(), entry) ||
			!strings.Contains(err.Error(), "this orker's own") {
			t.Errorf("Validate(%q) = %v, want a refusal naming %s, the entry and the reason", entry, err, SecretAllowVar)
		}
	}
	for _, entry := range []string{
		"env:*", "env:OR*", "env:ORK*", "env:HTTPS_PROXY_*", "env:VENDOR_*", "file:/*/environ", "file:/etc/orkstack/task-secrets/*",
		"file:/*",
	} {
		if err := withWork(entry).Validate(); err != nil {
			t.Errorf("Validate(%q) = %v, want an entry that also admits sources of the tasks' own accepted", entry, err)
		}
	}
}

// TestSecurityPolicyFromEnv_OwnsTheFilesItsSettingsName: a credential file an
// OrkStack or telemetry setting of this process names, and the local artifact
// store, are the Orker's own.
func TestSecurityPolicyFromEnv_OwnsTheFilesItsSettingsName(t *testing.T) {
	dir := t.TempDir()
	settings := map[string]string{
		RegistryAuthFileVar:                     filepath.Join(dir, "orker-registry.json"),
		"ORK_REGISTRY_AUTH_FILE":                filepath.Join(dir, "engine-registry.json"),
		"OTEL_EXPORTER_OTLP_CERTIFICATE":        filepath.Join(dir, "ca.pem"),
		"OTEL_EXPORTER_OTLP_CLIENT_KEY":         filepath.Join(dir, "otlp.key"),
		"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE": filepath.Join(dir, "otlp.pem"),
		"OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY":  filepath.Join(dir, "traces.key"),
		"OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY": filepath.Join(dir, "metrics.key"),
		"OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY":    filepath.Join(dir, "logs.key"),
		"ORK_ARTIFACT_BACKEND":                  "local",
		"ORK_ARTIFACT_DIR":                      filepath.Join(dir, "artifacts"),
	}
	for key, value := range settings {
		t.Setenv(key, value)
	}
	policy := SecurityPolicyFromEnv()
	var own []string
	for key, value := range settings {
		if key != "ORK_ARTIFACT_BACKEND" {
			own = append(own, "file:"+value)
		}
	}
	own = append(own, "file:"+filepath.Join(dir, "artifacts", "runs", "ork-1", "commit.json"))
	assertOwn(t, policy, own...)
	assertNotOwn(t, policy, "file:"+filepath.Join(dir, "task-secrets", "db"))

	// The artifact directory is the Orker's store only when it is the backend.
	t.Setenv("ORK_ARTIFACT_BACKEND", "s3")
	assertNotOwn(t, SecurityPolicyFromEnv(), "file:"+filepath.Join(dir, "artifacts", "db"))
}
