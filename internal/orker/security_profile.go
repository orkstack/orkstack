package orker

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/quantity"
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

	// RegistryAuthFileVar is ORK_ORKER_REGISTRY_AUTH_FILE: a docker
	// config.json used ONLY for the manifest and blob GETs signature
	// verification makes. It grants no push and is never mounted into a
	// container. The engine's ORK_REGISTRY_AUTH_FILE is the same shape for
	// the same GETs at apply; the two are separate keys because they are
	// read by separate binaries with separate ownership (AGENTS.md §12).
	RegistryAuthFileVar = "ORK_ORKER_REGISTRY_AUTH_FILE"
	SecretAllowVar      = "ORK_ORKER_SECRET_ALLOW"
)

// Policy defaults. None of them is unlimited: a workload that requested
// nothing still gets a bound (§9.3). Exported so internal/config's Schema
// can bind ORK_ORKER_DEFAULT_CPU, _MEMORY, ORK_ORKER_MAX_CPU, _MEMORY,
// ORK_ORKER_PIDS_LIMIT, ORK_ORKER_TMPFS_BYTES, ORK_ORKER_FORCE_USER and
// ORK_ORKER_SECRET_SCHEMES's documented defaults to the same constants
// DefaultSecurityPolicy itself falls back to, rather than a duplicated
// literal.
const (
	DefaultCPU              = "1"
	DefaultMemory           = "1Gi"
	DefaultMaxCPU           = "8"
	DefaultMaxMemory        = "16Gi"
	DefaultPidsLimit  int64 = 512
	DefaultTmpfsBytes int64 = 64 << 20
	// DefaultForceUser is DefaultSecurityPolicy's ForceUser: containers run
	// as the image's own USER unless an operator opts in.
	DefaultForceUser = false
)

// DefaultSecretSchemes is DefaultSecurityPolicy's SecretSchemes: the
// SecretRef.source schemes an Orker resolves with no operator
// configuration at all. A var, not a const, because Go has no constant
// slice — treat it as read-only.
var DefaultSecretSchemes = []string{"env", "file"}

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
	// SecretAllow is the floor of SecretRef.source values this Orker
	// resolves, as scheme:pattern entries in the grammar
	// internal/dsl.ValidSecretPattern states. Empty means every source of an
	// allowed scheme, which is what every Orker resolves today. It is the
	// operator's half of the policy Namespace.allowed_secrets is the tenant's
	// half of, and it composes with that list by intersection: a source must
	// pass both, so a namespace can only narrow what its Orkers permit. It is
	// checked here, by the process that reads the value, because the Orker
	// is the security boundary (AGENTS.md §2.3) and trusts no caller to have
	// checked first.
	SecretAllow []string
}

// DefaultSecurityPolicy returns the policy an Orker enforces with no operator
// configuration at all.
func DefaultSecurityPolicy() SecurityPolicy {
	return SecurityPolicy{
		DefaultCPU:    DefaultCPU,
		DefaultMemory: DefaultMemory,
		MaxCPU:        DefaultMaxCPU,
		MaxMemory:     DefaultMaxMemory,
		PidsLimit:     DefaultPidsLimit,
		TmpfsBytes:    DefaultTmpfsBytes,
		ForceUser:     DefaultForceUser,
		SecretSchemes: append([]string(nil), DefaultSecretSchemes...),
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
	if v := strings.TrimSpace(os.Getenv(SecretAllowVar)); v != "" {
		var allow []string
		for _, entry := range strings.Split(v, ",") {
			if entry = strings.TrimSpace(entry); entry != "" {
				allow = append(allow, entry)
			}
		}
		policy.SecretAllow = allow
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

// AllowsSecretSource reports whether this Orker's floor permits a source:
// nil when SecretAllow is empty or the source matches, otherwise the
// *dsl.SecretSourceRefusal for the caller to wrap with its coordinates.
func (p SecurityPolicy) AllowsSecretSource(source string) error {
	if len(p.SecretAllow) == 0 {
		return nil
	}
	return dsl.CheckSecretSource(p.SecretAllow, source)
}

// validateSecretAllow holds the floor to the shared grammar and to this
// Orker's own schemes. A floor naming a scheme this process never resolves
// is an operator typo that would otherwise silently permit nothing of that
// scheme, and "permits nothing" and "was never applied" must not be the same
// boot log.
func (p SecurityPolicy) validateSecretAllow() error {
	if err := dsl.ValidSecretPatterns(p.SecretAllow); err != nil {
		return fmt.Errorf("%s: %w", SecretAllowVar, err)
	}
	for _, entry := range p.SecretAllow {
		scheme, _, _ := strings.Cut(entry, ":")
		if !p.AllowsSecretScheme(scheme) {
			return fmt.Errorf("%s entry %q names scheme %q, which %s (%s) does not resolve; "+
				"add the scheme there or drop the entry", SecretAllowVar, entry, scheme,
				SecretSchemesVar, strings.Join(p.SecretSchemes, ", "))
		}
	}
	return nil
}

// Validate refuses a policy an operator cannot have meant.
//
// It exists because an unreadable cap must never pass for an absent one: were
// ResolveResources to skip a comparison whose cap failed to parse, "16GB" —
// the obvious typo for "16Gi", and a spelling quantity.ParseMemory rejects — would not
// refuse the request, it would REMOVE the ceiling. A workload could then ask
// for 1024Ti and get it.
//
// Boot is the right place to say so. An operator typo should stop the process
// with a message naming the variable and the value, rather than surviving
// until the first dispatch and silently over-granting there (AGENTS.md §6:
// "Workflows must not receive unlimited resources by default").
func (p SecurityPolicy) Validate() error {
	if p.configErr != nil {
		return p.configErr
	}
	if err := p.validateSecretAllow(); err != nil {
		return err
	}
	// A quantity that parses is still not a limit unless it is positive and
	// finite: NaN is above and below nothing, so a NaN cap removes the
	// ceiling, and zero is what the runtime reads as unlimited.
	capCPU, err := quantity.ParseCPU(p.MaxCPU)
	if err == nil && !quantity.PositiveFinite(capCPU) {
		err = quantity.ErrNotPositiveCPU
	}
	if err != nil {
		return fmt.Errorf("%s=%q is not a valid cpu quantity (%v); use the Kubernetes "+
			"spelling, e.g. \"8\" or \"500m\"", MaxCPUVar, p.MaxCPU, err)
	}
	capMem, err := quantity.ParseMemory(p.MaxMemory)
	if err == nil && !quantity.ByteCount(capMem) {
		err = quantity.ErrNotByteCount
	}
	if err != nil {
		return fmt.Errorf("%s=%q is not a valid memory quantity (%v); use the Kubernetes "+
			"spelling, e.g. \"16Gi\" — note Gi, not GB", MaxMemoryVar, p.MaxMemory, err)
	}
	defCPU, err := quantity.ParseCPU(p.DefaultCPU)
	if err == nil && !quantity.PositiveFinite(defCPU) {
		err = quantity.ErrNotPositiveCPU
	}
	if err != nil {
		return fmt.Errorf("%s=%q is not a valid cpu quantity (%v)", DefaultCPUVar, p.DefaultCPU, err)
	}
	defMem, err := quantity.ParseMemory(p.DefaultMemory)
	if err == nil && !quantity.ByteCount(defMem) {
		err = quantity.ErrNotByteCount
	}
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

	requestedCPU, cpuErr := quantity.ParseCPU(cpu)
	if cpuErr != nil {
		return "", "", Fail(ReasonResourceRefused, "cpu %q is not a valid quantity: %v", cpu, cpuErr)
	}
	if !quantity.PositiveFinite(requestedCPU) {
		// Docker reads `--cpus 0` as NO limit, so a non-positive or non-finite
		// request is an unlimited container, not a small one (§9.2, §9.3).
		return "", "", Fail(ReasonResourceRefused,
			"cpu request %q is not a positive, finite quantity; it is refused rather than passed to the "+
				"runtime, because a zero or negative limit means UNLIMITED there", cpu)
	}
	capCPU, cpuErr := quantity.ParseCPU(p.MaxCPU)
	if cpuErr == nil && !quantity.PositiveFinite(capCPU) {
		cpuErr = quantity.ErrNotPositiveCPU
	}
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

	requestedMem, memErr := quantity.ParseMemory(memory)
	if memErr != nil {
		return "", "", Fail(ReasonResourceRefused, "memory %q is not a valid quantity: %v", memory, memErr)
	}
	if !quantity.ByteCount(requestedMem) {
		// The memory half of the same trap: `--memory 0` is no limit, and a
		// fraction of a byte renders as 0.
		return "", "", Fail(ReasonResourceRefused,
			"memory request %q is not a quantity of at least one byte; it is refused rather than passed to the "+
				"runtime, because a zero or negative limit means UNLIMITED there", memory)
	}
	capMem, memErr := quantity.ParseMemory(p.MaxMemory)
	if memErr == nil && !quantity.ByteCount(capMem) {
		memErr = quantity.ErrNotByteCount
	}
	if memErr != nil {
		// Same rule, memory half: "16GB" is the obvious typo for "16Gi" and
		// must refuse rather than remove the ceiling.
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

// ReservedResources is what an attempt of req is granted on this Orker, as
// numbers: the effective values ResolveResources returns as the strings
// task.json carries, parsed by the same quantity.ParseCPU and quantity.ParseMemory the caps
// were checked with. Nil when the policy refuses the request — an attempt
// refused RESOURCE_REFUSED reserved nothing. Pure: it reads the policy and
// the request and nothing else, so it agrees with the value execute()
// resolved for the same attempt by construction.
func (p SecurityPolicy) ReservedResources(req *orkstackv1.ResourceRequest) *orkstackv1.ReservedResources {
	cpu, memory, failure := p.ResolveResources(req)
	if failure != nil {
		return nil
	}
	// Both parses already succeeded inside ResolveResources, so the discarded
	// errors are unreachable.
	cores, _ := quantity.ParseCPU(cpu)
	bytes, _ := quantity.ParseMemory(memory)
	return &orkstackv1.ReservedResources{Cpu: cores, MemoryBytes: int64(bytes)}
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
