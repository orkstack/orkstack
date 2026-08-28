package orker

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// Network modes. Exactly two exist, and named Orker-defined profiles are
// deliberately absent: an Orkflow must not define arbitrary host-network
// configuration (AGENTS.md §6, §9.1).
const (
	NetworkDefault = "default"
	NetworkNone    = "none"
)

// SeccompBuiltin names the default seccomp profile explicitly rather than
// leaving it implicit. "The default is fine" and "the default is what we
// chose" look identical in a container that is running and completely
// different in a review (§9.2).
const SeccompBuiltin = "builtin"

// Orker resource-policy environment variables (§9.3). Policy is Orker-local
// and per-pool: the engine owns contract limits, the Orker owns resource
// policy, and neither claims knowledge it does not have (§5.2).
const (
	MaxCPUVar        = "ORK_ORKER_MAX_CPU"
	MaxMemoryVar     = "ORK_ORKER_MAX_MEMORY"
	DefaultCPUVar    = "ORK_ORKER_DEFAULT_CPU"
	DefaultMemoryVar = "ORK_ORKER_DEFAULT_MEMORY"
	PidsLimitVar     = "ORK_ORKER_PIDS_LIMIT"
	TmpfsBytesVar    = "ORK_ORKER_TMPFS_BYTES"
	ForceUserVar     = "ORK_ORKER_FORCE_USER"
	SecretSchemesVar = "ORK_ORKER_SECRET_SCHEMES"
)

// Policy defaults. None of them is unlimited: a workload that requested
// nothing still gets a bound (§9.3).
const (
	defaultCPU        = "1"
	defaultMemory     = "1Gi"
	defaultMaxCPU     = "8"
	defaultMaxMemory  = "16Gi"
	defaultPidsLimit  = 512
	defaultTmpfsBytes = 64 << 20
)

// SecurityPolicy is the Orker-local half of §9: the caps it enforces, the
// defaults it supplies, and the secret sources it understands.
//
// There is deliberately NO field here for anything in §9.1. An Orkflow cannot
// express `--privileged`, a host namespace, a capability to add, a device, or
// an arbitrary host bind mount, and neither can an operator: no configuration
// key exists for any of them, which is a stronger guarantee than validation.
type SecurityPolicy struct {
	configErr error
	// DefaultCPU and DefaultMemory are what a task that requested nothing
	// receives.
	DefaultCPU    string
	DefaultMemory string
	// MaxCPU and MaxMemory are the caps. A request above a cap is REFUSED,
	// not silently clamped: a workload that asked for 64 GiB and got 4 GiB
	// fails confusingly later, which is worse than failing now (§9.3).
	MaxCPU    string
	MaxMemory string
	// PidsLimit bounds process creation in every container.
	PidsLimit int64
	// TmpfsBytes sizes the small /tmp tmpfs, which is separate from the
	// workload's /orkstack/tmp scratch bind.
	TmpfsBytes int64
	// ForceUser makes every container run as the Orker's own uid:gid. It is
	// the deployment-wide form of the §3.1 readability escape hatch, and it is
	// opt-in because it overrides the image's own USER and breaks images that
	// need their baked-in identity.
	ForceUser bool
	// SecretSchemes are the SecretRef.source schemes this Orker resolves. An
	// unknown scheme is refused rather than ignored (§9.4).
	SecretSchemes []string
}

// DefaultSecurityPolicy returns the policy an Orker enforces with no operator
// configuration at all.
func DefaultSecurityPolicy() SecurityPolicy {
	return SecurityPolicy{
		DefaultCPU:    defaultCPU,
		DefaultMemory: defaultMemory,
		MaxCPU:        defaultMaxCPU,
		MaxMemory:     defaultMaxMemory,
		PidsLimit:     defaultPidsLimit,
		TmpfsBytes:    defaultTmpfsBytes,
		SecretSchemes: []string{"env", "file"},
	}
}

// SecurityPolicyFromEnv reads the §9.3 policy variables over the defaults.
func SecurityPolicyFromEnv() SecurityPolicy {
	policy := DefaultSecurityPolicy()
	if v := strings.TrimSpace(os.Getenv(DefaultCPUVar)); v != "" {
		policy.DefaultCPU = v
	}
	if v := strings.TrimSpace(os.Getenv(DefaultMemoryVar)); v != "" {
		policy.DefaultMemory = v
	}
	if v := strings.TrimSpace(os.Getenv(MaxCPUVar)); v != "" {
		policy.MaxCPU = v
	}
	if v := strings.TrimSpace(os.Getenv(MaxMemoryVar)); v != "" {
		policy.MaxMemory = v
	}
	if v := strings.TrimSpace(os.Getenv(PidsLimitVar)); v != "" {
		parsed, err := positivePolicyInt(PidsLimitVar, v)
		policy.configErr = errors.Join(policy.configErr, err)
		if err == nil {
			policy.PidsLimit = parsed
		}
	}
	if v := strings.TrimSpace(os.Getenv(TmpfsBytesVar)); v != "" {
		parsed, err := positivePolicyInt(TmpfsBytesVar, v)
		policy.configErr = errors.Join(policy.configErr, err)
		if err == nil {
			policy.TmpfsBytes = parsed
		}
	}
	if v := strings.TrimSpace(os.Getenv(ForceUserVar)); v == "1" || strings.EqualFold(v, "true") {
		policy.ForceUser = true
	}
	if v := strings.TrimSpace(os.Getenv(SecretSchemesVar)); v != "" {
		var schemes []string
		for _, scheme := range strings.Split(v, ",") {
			if scheme = strings.TrimSpace(scheme); scheme != "" {
				schemes = append(schemes, scheme)
			}
		}
		policy.SecretSchemes = schemes
	}
	return policy
}

func positivePolicyInt(key, raw string) (int64, error) {
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s=%q must be a positive base-10 integer: %w", key, raw, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("%s=%q must be a positive base-10 integer", key, raw)
	}
	return parsed, nil
}

// ProfileID names the enforced profile for DescribeOrker, so an operator can
// see which posture a pool's Orkers hold without reading their configuration.
func (p SecurityPolicy) ProfileID() string {
	return fmt.Sprintf("orkstack/v1:readonly,cap-drop-all,no-new-privileges,seccomp=%s,pids=%d",
		SeccompBuiltin, p.PidsLimit)
}

// AllowsSecretScheme reports whether this Orker resolves a source scheme.
func (p SecurityPolicy) AllowsSecretScheme(scheme string) bool {
	for _, allowed := range p.SecretSchemes {
		if allowed == scheme {
			return true
		}
	}
	return false
}

// Validate refuses a policy an operator cannot have meant.
//
// It exists because an unreadable cap used to be indistinguishable from an
// absent one: ResolveResources guarded each comparison with `parseErr == nil`,
// so "16GB" — the obvious typo for "16Gi", and a spelling parseMemory has
// always rejected — did not refuse the request, it REMOVED the ceiling. A
// workload could then ask for 1024Ti and get it.
//
// Boot is the right place to say so. An operator typo should stop the process
// with a message naming the variable and the value, rather than surviving
// until the first dispatch and silently over-granting there (AGENTS.md §6:
// "Workflows must not receive unlimited resources by default").
func (p SecurityPolicy) Validate() error {
	if p.configErr != nil {
		return p.configErr
	}
	capCPU, err := parseCPU(p.MaxCPU)
	if err != nil {
		return fmt.Errorf("%s=%q is not a valid cpu quantity (%v); use the Kubernetes "+
			"spelling, e.g. \"8\" or \"500m\"", MaxCPUVar, p.MaxCPU, err)
	}
	capMem, err := parseMemory(p.MaxMemory)
	if err != nil {
		return fmt.Errorf("%s=%q is not a valid memory quantity (%v); use the Kubernetes "+
			"spelling, e.g. \"16Gi\" — note Gi, not GB", MaxMemoryVar, p.MaxMemory, err)
	}
	defCPU, err := parseCPU(p.DefaultCPU)
	if err != nil {
		return fmt.Errorf("%s=%q is not a valid cpu quantity (%v)", DefaultCPUVar, p.DefaultCPU, err)
	}
	defMem, err := parseMemory(p.DefaultMemory)
	if err != nil {
		return fmt.Errorf("%s=%q is not a valid memory quantity (%v)", DefaultMemoryVar, p.DefaultMemory, err)
	}
	if defCPU > capCPU {
		return fmt.Errorf("%s=%q exceeds %s=%q; a task that requests nothing would be "+
			"refused its own default", DefaultCPUVar, p.DefaultCPU, MaxCPUVar, p.MaxCPU)
	}
	if defMem > capMem {
		return fmt.Errorf("%s=%q exceeds %s=%q; a task that requests nothing would be "+
			"refused its own default", DefaultMemoryVar, p.DefaultMemory, MaxMemoryVar, p.MaxMemory)
	}
	return nil
}

// ResolveResources applies policy to a request and returns the EFFECTIVE
// values, which are what the container gets and what task.json reports.
//
// A request above a cap is refused rather than clamped (§9.3). Absence is not
// a request: a task that asked for nothing gets the policy default, never
// unlimited.
func (p SecurityPolicy) ResolveResources(req *orkstackv1.ResourceRequest) (cpu, memory string, err *Failure) {
	cpu = strings.TrimSpace(req.GetCpu())
	memory = strings.TrimSpace(req.GetMemory())
	if cpu == "" {
		cpu = p.DefaultCPU
	}
	if memory == "" {
		memory = p.DefaultMemory
	}

	requestedCPU, cpuErr := parseCPU(cpu)
	if cpuErr != nil {
		return "", "", Fail(ReasonResourceRefused, "cpu %q is not a valid quantity: %v", cpu, cpuErr)
	}
	capCPU, cpuErr := parseCPU(p.MaxCPU)
	if cpuErr != nil {
		// An unreadable cap is a refusal, NOT an absent ceiling. Skipping the
		// comparison here is what let "8 cores" grant cpu 4096.
		return "", "", Fail(ReasonResourceRefused,
			"this orker's cpu cap %q (%s) is not a valid quantity: %v; the request is refused "+
				"rather than run uncapped, because an operator typo must never widen a limit",
			p.MaxCPU, MaxCPUVar, cpuErr)
	}
	if requestedCPU > capCPU {
		return "", "", Fail(ReasonResourceRefused,
			"cpu request %q exceeds this orker's cap of %q (%s); the request is refused rather than "+
				"silently reduced, because a workload that asked for more and got less fails confusingly later",
			cpu, p.MaxCPU, MaxCPUVar)
	}

	requestedMem, memErr := parseMemory(memory)
	if memErr != nil {
		return "", "", Fail(ReasonResourceRefused, "memory %q is not a valid quantity: %v", memory, memErr)
	}
	capMem, memErr := parseMemory(p.MaxMemory)
	if memErr != nil {
		// Same defect, memory half: "16GB" is the obvious typo for "16Gi" and
		// used to remove the ceiling instead of refusing.
		return "", "", Fail(ReasonResourceRefused,
			"this orker's memory cap %q (%s) is not a valid quantity: %v; the request is refused "+
				"rather than run uncapped, because an operator typo must never widen a limit",
			p.MaxMemory, MaxMemoryVar, memErr)
	}
	if requestedMem > capMem {
		return "", "", Fail(ReasonResourceRefused,
			"memory request %q exceeds this orker's cap of %q (%s); the request is refused rather than "+
				"silently reduced, because a workload that asked for more and got less fails confusingly later",
			memory, p.MaxMemory, MaxMemoryVar)
	}
	return cpu, memory, nil
}

// ResolveNetwork maps the proto enum to the runtime's word, refusing anything
// that is not one of the two modes that exist.
func (p SecurityPolicy) ResolveNetwork(mode orkstackv1.NetworkMode) (string, *Failure) {
	switch mode {
	case orkstackv1.NetworkMode_NETWORK_MODE_UNSPECIFIED, orkstackv1.NetworkMode_NETWORK_MODE_DEFAULT:
		return NetworkDefault, nil
	case orkstackv1.NetworkMode_NETWORK_MODE_NONE:
		return NetworkNone, nil
	default:
		return "", Fail(ReasonNetworkModeRefused,
			"network mode %q is not %q or %q; named orker-defined network profiles are deliberately absent",
			mode.String(), NetworkDefault, NetworkNone)
	}
}

// parseCPU reads the Kubernetes CPU spelling: "2", "0.5", "500m".
func parseCPU(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty cpu quantity")
	}
	if milli, ok := strings.CutSuffix(raw, "m"); ok {
		value, err := strconv.ParseFloat(milli, 64)
		if err != nil {
			return 0, err
		}
		return value / 1000, nil
	}
	return strconv.ParseFloat(raw, 64)
}

// memorySuffixes is the Kubernetes memory spelling, binary and decimal.
var memorySuffixes = []struct {
	suffix string
	factor float64
}{
	{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40},
	{"K", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12},
	{"k", 1e3},
}

// parseMemory reads the Kubernetes memory spelling: "4Gi", "512Mi", "1000000".
func parseMemory(raw string) (float64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, fmt.Errorf("empty memory quantity")
	}
	for _, unit := range memorySuffixes {
		if trimmed, ok := strings.CutSuffix(raw, unit.suffix); ok {
			value, err := strconv.ParseFloat(strings.TrimSpace(trimmed), 64)
			if err != nil {
				return 0, err
			}
			return value * unit.factor, nil
		}
	}
	return strconv.ParseFloat(raw, 64)
}
