package orker

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/quantity"
	"github.com/orkstack/orkstack/pkg/artifacts"
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
	// allowed scheme but the Orker's own, which no floor admits. It is the
	// operator's half of the policy Namespace.allowed_secrets is the tenant's
	// half of, and it composes with that list by intersection: a source must
	// pass both, so a namespace can only narrow what its Orkers permit. It is
	// checked here, by the process that reads the value, because the Orker
	// is the security boundary (AGENTS.md §2.3) and trusts no caller to have
	// checked first.
	SecretAllow []string
	// own are the files and directories this process's own settings name —
	// its work root, its configuration and the files that read, its
	// credential files — beside the fixed ones every Orker holds (ownFiles),
	// each resolved by name once, when the policy took it (WithOwnPaths), and
	// looked at as it is whenever a source is judged (ownNow). They, and
	// everything beneath them, are the Orker's own: no floor admits one as a
	// task's source, empty or not (AllowsSecretSource).
	// SecurityPolicyFromEnv adds what the environment names; a caller adds
	// what only it knows (WithWorkRoot, cmd/orker's configuration file).
	own []ownRoot
	// ownVars are environment variables this process's own settings name —
	// a telemetry sink header's env: source — beside the fixed names every
	// Orker holds (ownEnv). They are the Orker's own as own paths are.
	ownVars []OwnVar
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
	return policy.WithOwnPaths(ownPathsFromEnv(os.Getenv)...)
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
// seccomp is what the runtime actually spelled for the default profile
// (RuntimeInfo.Seccomp): SeccompBuiltin by name, or SeccompEngineDefault on an
// engine that has no name for it. Empty means a runtime that could not say.
func (p SecurityPolicy) ProfileID(seccomp string) string {
	if seccomp == "" {
		seccomp = "unknown"
	}
	return fmt.Sprintf("orkstack/v1:readonly,cap-drop-all,no-new-privileges,seccomp=%s,pids=%d",
		seccomp, p.PidsLimit)
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

// AllowsSecretSource reports whether this Orker's floor permits a source,
// whatever SecretAllow says: a *dsl.SecretSourceRefusal when a file source is
// not a plain absolute path or is longer than the kernel opens
// (dsl.CheckSecretPath), an *OwnSourceRefusal when the source is one of the
// Orker's own; otherwise nil when SecretAllow is empty or the source matches
// it, and the *dsl.SecretSourceRefusal when it does not. The caller wraps any
// of them with its coordinates.
func (p SecurityPolicy) AllowsSecretSource(source string) error {
	// A file is read by its name. A name with a "..", "." or empty segment, or
	// a relative one, would be normalized by the kernel after it follows a
	// link, so the file read need not be the file named and no check of the
	// name would hold: it is refused, floor or no floor, as a floor refuses it.
	// A name longer than the kernel opens is refused too, before anything
	// looks at the file system for it.
	if err := dsl.CheckSecretPath(source); err != nil {
		return err
	}
	if refusal := p.ownSource(source); refusal != nil {
		return refusal
	}
	if len(p.SecretAllow) == 0 {
		return nil
	}
	return dsl.CheckSecretSource(p.SecretAllow, source)
}

// The Orker's own sources.
//
// A task's secret names a source the Orker reads from its OWN environment
// and filesystem. Part of what is there is the Orker's own: the settings it
// was started with, the credentials it and the container CLI it drives use,
// its process, and the files its configuration names. None of it is a
// task's, so the floor never admits any of it — not when SecretAllow is
// empty, and not when an entry would match it. The exclusion is absolute,
// with no entry that re-admits one: a pattern written for convenience
// ("env:*") must not hand a task the Orker's credentials, and an operator who
// means a task to hold such a value supplies it under a source of the task's
// own, which is what supplying a workload secret explicitly means (AGENTS.md
// §2.3).
//
// It bounds names, as the floor does, and reads no value. A file: source is
// a plain absolute path of at most PATH_MAX bytes (dsl.CheckSecretPath),
// followed as the kernel follows it to open it, and it is the Orker's own
// when its name, a directory its way passes through or the file it ends at
// is one of the Orker's own, by name or as the file system holds it at that
// moment (reachesOwn). So a symbolic link does not reach an own file under
// another name, a Kubernetes mount whose links an update swaps after boot
// does not either, and a hard link to an own file is that file. What the way
// there cannot show is out of this check's reach: a file beneath an own
// directory that a bind mount or a hard link shows under another name, and a
// link swapped between the check and the read. Each needs write access
// outside the task's workspace, which no task has (AGENTS.md §6 forbids host
// mounts).

// OwnPath is a file or directory of the Orker's own: it and everything
// beneath it. Why says what it is in the boot refusal of a floor entry that
// admits only it; a task's refusal says only that the source is one of the
// Orker's files (ownFileLabel).
type OwnPath struct {
	Path string
	Why  string
}

// OwnVar is an environment variable of the Orker's own, by its exact name.
// Why names it in the boot refusal of a floor entry that admits only it; a
// task's refusal says only that the source is one of the Orker's variables
// (ownVarLabel).
type OwnVar struct {
	Name string
	Why  string
}

// ownFileLabel and ownVarLabel are what a task's refusal says of every file
// and every variable of the Orker's own: one label for each scheme, never
// which file or variable it is, which rule or setting made it the Orker's,
// nor where a link led, so a task cannot learn the Orker's configuration by
// trying names.
const (
	ownFileLabel = "one of its files"
	ownVarLabel  = "one of its variables"
)

// ownRoot is an OwnPath as the policy compares it. By name: its name made
// absolute and cleaned, and where that name led as the kernel follows it
// (resolvePath), both worked out once, when the policy took it. As it is:
// opened, its name made absolute as a read of it opens it, never cleaned,
// which each judgement of a source looks at afresh (ownNow).
type ownRoot struct {
	OwnPath
	opened, named, resolved string
}

// newOwnRoot resolves own: a relative path from this process's working
// directory, as a read of it would open it.
func newOwnRoot(own OwnPath) (ownRoot, error) {
	raw := own.Path
	if !filepath.IsAbs(raw) {
		wd, err := os.Getwd()
		if err != nil {
			return ownRoot{}, fmt.Errorf("%s, %q, is a relative path, and the working directory it is relative to "+
				"cannot be read: %w", own.Why, raw, err)
		}
		raw = wd + "/" + raw
	}
	return ownRoot{OwnPath: own, opened: raw, named: filepath.Clean(raw), resolved: resolvePath(raw)}, nil
}

// WithOwnPaths returns the policy with paths among the Orker's own, each
// resolved now for the comparison by name. What each one is — the file or
// directory its name leads to — is looked at again whenever a source is
// judged, so a link an operator or Kubernetes swaps at one of these names
// later is seen at once. An empty path names nothing and is skipped; a
// relative one that cannot be placed is an error Validate reports. The
// caller's policy is not changed.
func (p SecurityPolicy) WithOwnPaths(paths ...OwnPath) SecurityPolicy {
	own := append([]ownRoot(nil), p.own...)
	for _, path := range paths {
		if path.Path == "" {
			continue
		}
		root, err := newOwnRoot(path)
		if err != nil {
			p.configErr = errors.Join(p.configErr, err)
			continue
		}
		own = append(own, root)
	}
	p.own = own
	return p
}

// WithOwnVars returns the policy with vars among the Orker's own variables.
// An empty name names nothing and is skipped. The caller's policy is not
// changed.
func (p SecurityPolicy) WithOwnVars(vars ...OwnVar) SecurityPolicy {
	own := append([]OwnVar(nil), p.ownVars...)
	for _, v := range vars {
		if v.Name != "" {
			own = append(own, v)
		}
	}
	p.ownVars = own
	return p
}

// ownName is an environment variable of the Orker's own: an exact name, or
// a prefix when it ends in "*".
type ownName struct {
	pattern string
	why     string
}

// ownEnv is the Orker's own environment, OrkStack's half; runtimeOwnEnv
// (runtime_docker.go) is the container CLI's, its credential helpers' cloud
// credentials among them.
var ownEnv = []ownName{
	// Every OrkStack setting, the object store's keys among them.
	{"ORK_*", "its OrkStack settings, ORK_*"},
	// The telemetry exporters', whose headers carry a collector's
	// credentials.
	{"OTEL_*", "its telemetry settings, OTEL_*"},
	// The proxies its HTTP clients and the container CLI dial through; a
	// proxy URL may carry a user and a password. NO_PROXY carries neither.
	{"HTTP_PROXY", "its proxy settings"},
	{"HTTPS_PROXY", "its proxy settings"},
	{"ALL_PROXY", "its proxy settings"},
	{"http_proxy", "its proxy settings"},
	{"https_proxy", "its proxy settings"},
	{"all_proxy", "its proxy settings"},
}

// ownFiles are the paths every Orker holds as its own, wherever it runs,
// resolved once (fixedOwnRoots).
var ownFiles = []OwnPath{
	// Its environment, memory, descriptors and root, and every other
	// process's that the host or the pod shows it.
	{Path: "/proc", Why: "its process, /proc"},
	// /dev/fd, /dev/stdin and the rest are its own descriptors through
	// /proc/self/fd, and a device is never a secret: /dev/zero would be
	// read without end.
	{Path: "/dev", Why: "its devices, /dev"},
	// Where Kubernetes mounts the pod's service-account and projected
	// tokens (IRSA, EKS Pod Identity, workload identity), and Docker and
	// Compose the container's secrets: the identity of the Orker's own pod
	// or container. A task's secrets belong in a directory of their own
	// that the floor names, as the Kubernetes bases' task-secrets volume.
	{Path: "/run/secrets", Why: "its container's credentials, /run/secrets"},
	{Path: "/var/run/secrets", Why: "its container's credentials, /var/run/secrets"},
}

// fixedOwnRoots is ownFiles, resolved the first time a source is judged.
// Every path in it is absolute, so none fails to place.
var fixedOwnRoots = sync.OnceValue(func() []ownRoot {
	return DefaultSecurityPolicy().WithOwnPaths(ownFiles...).own
})

// ownPathsFromEnv are the files this process's own settings name as a
// credential or as its own data, and the container CLI's (runtimeOwnPaths).
// What only the caller knows — the work root, the configuration file — the
// caller adds (WithWorkRoot, cmd/orker).
func ownPathsFromEnv(getenv func(string) string) []OwnPath {
	var paths []OwnPath
	add := func(path, why string) {
		if path != "" {
			paths = append(paths, OwnPath{Path: path, Why: why})
		}
	}
	// The registry credentials signature verification reads: this binary's,
	// and the engine's, which `orkctl activity test` reads too.
	add(getenv(RegistryAuthFileVar), "its registry credentials, "+RegistryAuthFileVar)
	add(getenv("ORK_REGISTRY_AUTH_FILE"), "its registry credentials, ORK_REGISTRY_AUTH_FILE")
	// The OTLP exporters' TLS files, a client key among them.
	for _, signal := range []string{"", "TRACES_", "METRICS_", "LOGS_"} {
		for _, kind := range []string{"CERTIFICATE", "CLIENT_CERTIFICATE", "CLIENT_KEY"} {
			add(getenv("OTEL_EXPORTER_OTLP_"+signal+kind), "its telemetry TLS files, OTEL_EXPORTER_OTLP_*")
		}
	}
	// The local artifact store holds every run's artifacts, commits and
	// logs, of every namespace.
	backend, dir := getenv("ORK_ARTIFACT_BACKEND"), getenv("ORK_ARTIFACT_DIR")
	if backend == "" {
		backend = artifacts.DefaultBackend
	}
	if dir == "" {
		dir = artifacts.DefaultLocalDir
	}
	if backend == artifacts.BackendLocal {
		add(dir, "its artifact store, ORK_ARTIFACT_DIR")
	}
	return append(paths, runtimeOwnPaths(getenv)...)
}

// WithWorkRoot returns the policy with root, this Orker's work root — the
// temp dir when root is empty, as makeAttemptDir falls back to it — among
// its own paths: every attempt's workspace lives there, another task's
// staged secrets, inputs and outputs and every pin's shared directory among
// them. The caller's policy is not changed.
func (p SecurityPolicy) WithWorkRoot(root string) SecurityPolicy {
	if root == "" {
		root = os.TempDir()
	}
	return p.WithOwnPaths(OwnPath{Path: root, Why: "its work root, " + WorkRootVar})
}

// OwnSourceRefusal is why a source was refused as one of the Orker's own.
type OwnSourceRefusal struct {
	Source string
	// Why is the label of what the source is: ownVarLabel for every
	// variable, ownFileLabel for every file.
	Why string
}

// Error renders exactly one of:
//
//	"env:ORK_S3_SECRET_KEY" is this orker's own (one of its variables), which no secret floor admits
//	"file:/proc/self/environ" is this orker's own (one of its files), which no secret floor admits
//
// It names the source a task wrote, quoted as dsl.QuoteSecretSource quotes
// it, and one label per scheme: never which of the Orker's own it is.
func (r *OwnSourceRefusal) Error() string {
	return fmt.Sprintf("%s is this orker's own (%s), which no secret floor admits", dsl.QuoteSecretSource(r.Source), r.Why)
}

// ownSource reports which of the Orker's own sources source is, or nil.
func (p SecurityPolicy) ownSource(source string) *OwnSourceRefusal {
	scheme, locator := SplitSecretSource(source)
	switch scheme {
	case "env":
		if p.ownVar(locator, false) != "" {
			return &OwnSourceRefusal{Source: source, Why: ownVarLabel}
		}
	case "file":
		if p.reachesOwn(locator) {
			return &OwnSourceRefusal{Source: source, Why: ownFileLabel}
		}
	}
	return nil
}

// ownVar reports why name is a variable of the Orker's own, or "". With
// prefix set, name is the literal head of a floor entry's pattern, and only
// an own prefix that head begins with holds every name the entry matches.
func (p SecurityPolicy) ownVar(name string, prefix bool) string {
	for _, names := range [][]ownName{ownEnv, runtimeOwnEnv} {
		for _, own := range names {
			if head, isPrefix := strings.CutSuffix(own.pattern, "*"); isPrefix && strings.HasPrefix(name, head) {
				return own.why
			}
			if !prefix && name == own.pattern {
				return own.why
			}
		}
	}
	for _, own := range p.ownVars {
		if !prefix && name == own.Name {
			return own.Why
		}
	}
	return ""
}

// ownNamed reports which own path holds path, an absolute path, by its name
// alone. A path that is not absolute is no own path's; AllowsSecretSource
// refuses it before it gets here.
func (p SecurityPolicy) ownNamed(path string) (ownRoot, bool) {
	if !filepath.IsAbs(path) {
		return ownRoot{}, false
	}
	return p.holds(filepath.Clean(path))
}

// reachesOwn reports whether reading path, an absolute path, would reach the
// Orker's own: whether its name lies within an own path, or the way the
// kernel takes to open it (walkPath) looks a name up in, or ends at, one of
// the Orker's own — by name, or as the file system holds that own path at
// this moment (ownNow), which is what makes a Kubernetes mount whose links
// an update has swapped since boot, and a hard link to an own file, the
// Orker's own too. A way that passes through an own directory is the
// Orker's own even when it leaves it again by a link's "..": no lookup in
// one is judged by what it reads.
//
// The own paths are looked at once before the walk and, for the file it
// ends at, once after, so an update that swaps a mount while the walk runs
// is seen either way. A path whose links go on past maxLinks is one no read
// opens, and is left to the read to refuse.
func (p SecurityPolicy) reachesOwn(path string) bool {
	if _, ok := p.ownNamed(path); ok {
		return true
	}
	before := p.ownNow()
	end, last, stopped := walkPath(path, func(at string, info fs.FileInfo) bool {
		_, named := p.holds(at)
		return named || before.holds(info)
	})
	if stopped {
		return true
	}
	if end == "" {
		return false
	}
	// A name the walk could not look at is judged by its name, so a link that
	// leads nowhere yet is seen by where it would lead.
	if _, ok := p.holds(end); ok {
		return true
	}
	return last != nil && p.ownNow().holds(last)
}

// ownNodes is what the Orker's own paths are at one moment: each one's name,
// as a read of it opens it, followed to the file or directory it leads to
// (os.Stat). A path that is not there, or cannot be looked at, has none and
// is compared by name alone.
type ownNodes []fs.FileInfo

// ownNow is the Orker's own paths, fixed and configured, as the file system
// holds them now.
func (p SecurityPolicy) ownNow() ownNodes {
	var nodes ownNodes
	for _, roots := range [][]ownRoot{fixedOwnRoots(), p.own} {
		for _, root := range roots {
			if info, err := os.Stat(root.opened); err == nil {
				nodes = append(nodes, info)
			}
		}
	}
	return nodes
}

// holds reports whether info is one of the nodes: the same file or
// directory, whatever names lead to it (os.SameFile).
func (n ownNodes) holds(info fs.FileInfo) bool {
	for _, own := range n {
		if os.SameFile(own, info) {
			return true
		}
	}
	return false
}

// holds reports which own path path, absolute and clean, lies within by name.
func (p SecurityPolicy) holds(path string) (ownRoot, bool) {
	for _, roots := range [][]ownRoot{fixedOwnRoots(), p.own} {
		for _, root := range roots {
			if within(path, root.named) || within(path, root.resolved) {
				return root, true
			}
		}
	}
	return ownRoot{}, false
}

// ownEntry reports why a floor entry admits nothing but the Orker's own, or
// "" when it admits a source of a task's own too. Every match of an entry
// begins with its pattern up to the first "*": for env, a name prefix; for
// file, the directory that prefix ends in. When an own source holds that
// whole prefix by its name, every match is the Orker's own by its name. A
// directory that is the Orker's own only through a link is not judged here:
// a link beneath it may lead out again, so the entry may admit a task's
// source, and AllowsSecretSource judges each match by its links instead.
func (p SecurityPolicy) ownEntry(entry string) string {
	scheme, pattern := SplitSecretSource(entry)
	literal, wild := pattern, false
	if i := strings.IndexByte(pattern, '*'); i >= 0 {
		literal, wild = pattern[:i], true
	}
	switch scheme {
	case "env":
		return p.ownVar(literal, wild)
	case "file":
		dir := literal
		if wild {
			// A file pattern begins with "/" (dsl.ValidSecretPattern), so
			// the cut keeps at least "/".
			dir = literal[:strings.LastIndexByte(literal, '/')+1]
		}
		if root, ok := p.ownNamed(dir); ok {
			return root.Why
		}
	}
	return ""
}

// maxLinks bounds how many links resolvePath follows for one name: the bound
// filepath.EvalSymlinks keeps, well above the 40 Linux follows before it
// refuses to open a name (ELOOP). A name the kernel opens is resolved in
// full; a name past the bound is one no read opens.
const maxLinks = 255

// resolvePath is where path leads as the kernel follows it to open it
// (walkPath), or "" for a path that is not absolute or whose links go on past
// maxLinks.
func resolvePath(path string) string {
	end, _, _ := walkPath(path, nil)
	return end
}

// walkPath follows path as the kernel follows it to open it: from the root,
// one name at a time, a link replaced by its target and a ".." taken from
// where the path has led so far. It calls reached, when it is not nil, with
// every name it comes to that is not a link — each directory the way looks a
// name up in, and the file it ends at — and what Lstat said of it, and stops
// the first time reached returns true, reporting stopped. Where a name does
// not exist, or cannot be looked at, it and the rest of path are kept as
// named, so a link that leads nowhere yet is seen by where it would lead.
//
// It returns where path leads, "" for a path that is not absolute or whose
// links go on past maxLinks, and what Lstat said of the name it ends at, nil
// when that is not there or was reached by a "..". Each name is looked at
// once, so the cost is bounded by path's length and maxLinks link targets,
// never by how many of its prefixes exist.
func walkPath(path string, reached func(at string, info fs.FileInfo) bool) (end string, last fs.FileInfo, stopped bool) {
	if !strings.HasPrefix(path, "/") {
		return "", nil, false
	}
	resolved, rest, links := "/", path[1:], 0
	for rest != "" {
		var name string
		name, rest, _ = strings.Cut(rest, "/")
		switch name {
		case "", ".":
			continue
		case "..":
			resolved, last = filepath.Dir(resolved), nil
			continue
		}
		next := filepath.Join(resolved, name)
		info, err := os.Lstat(next)
		if err != nil {
			return filepath.Join(next, rest), nil, false
		}
		if info.Mode()&fs.ModeSymlink == 0 {
			resolved, last = next, info
			if reached != nil && reached(next, info) {
				return next, info, true
			}
			continue
		}
		if links++; links > maxLinks {
			return "", nil, false
		}
		target, err := os.Readlink(next)
		if err != nil {
			return filepath.Join(next, rest), nil, false
		}
		if strings.HasPrefix(target, "/") {
			resolved, last = "/", nil
		}
		rest = target + "/" + rest
	}
	return resolved, last, false
}

// within reports whether path is root or lies beneath it. An empty side is
// never within anything.
func within(path, root string) bool {
	if path == "" || root == "" {
		return false
	}
	if root == "/" {
		return true
	}
	return path == root || strings.HasPrefix(path, root+"/")
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
		// The same rule for an entry that can match nothing but the Orker's
		// own sources, which no floor admits.
		if why := p.ownEntry(entry); why != "" {
			return fmt.Errorf("%s entry %q admits only this orker's own sources (%s), which no secret floor "+
				"admits; drop the entry, and give a task that needs such a value a source of its own",
				SecretAllowVar, entry, why)
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

	requestedCPU, failure := parseRequestCPU(cpu)
	if failure != nil {
		return "", "", failure
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

	requestedMem, failure := parseRequestMemory(memory)
	if failure != nil {
		return "", "", failure
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

// requestShapeFailure refuses a request whose own fields are malformed,
// zero, negative or non-finite, exactly as ResolveResources does. It reads
// nothing of this Orker's policy — an empty field takes the Orker's default
// later, and boot holds that default valid — so its answer is the same on
// every Orker, and the dispatcher asks it before the commit check.
func requestShapeFailure(req *orkstackv1.ResourceRequest) *Failure {
	if cpu := strings.TrimSpace(req.GetCpu()); cpu != "" {
		if _, failure := parseRequestCPU(cpu); failure != nil {
			return failure
		}
	}
	if memory := strings.TrimSpace(req.GetMemory()); memory != "" {
		if _, failure := parseRequestMemory(memory); failure != nil {
			return failure
		}
	}
	return nil
}

// parseRequestCPU parses one cpu request and refuses one that is not a
// positive, finite quantity.
func parseRequestCPU(cpu string) (float64, *Failure) {
	cores, err := quantity.ParseCPU(cpu)
	if err != nil {
		return 0, Fail(ReasonResourceRefused, "cpu %q is not a valid quantity: %v", cpu, err)
	}
	if !quantity.PositiveFinite(cores) {
		// Docker reads `--cpus 0` as NO limit, so a non-positive or non-finite
		// request is an unlimited container, not a small one (§9.2, §9.3).
		return 0, Fail(ReasonResourceRefused,
			"cpu request %q is not a positive, finite quantity; it is refused rather than passed to the "+
				"runtime, because a zero or negative limit means UNLIMITED there", cpu)
	}
	return cores, nil
}

// parseRequestMemory parses one memory request and refuses one that is not a
// quantity of at least one byte.
func parseRequestMemory(memory string) (float64, *Failure) {
	bytes, err := quantity.ParseMemory(memory)
	if err != nil {
		return 0, Fail(ReasonResourceRefused, "memory %q is not a valid quantity: %v", memory, err)
	}
	if !quantity.ByteCount(bytes) {
		// The memory half of the same trap: `--memory 0` is no limit, and a
		// fraction of a byte renders as 0.
		return 0, Fail(ReasonResourceRefused,
			"memory request %q is not a quantity of at least one byte; it is refused rather than passed to the "+
				"runtime, because a zero or negative limit means UNLIMITED there", memory)
	}
	return bytes, nil
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

// ceiling is the per-task cap as a reservation: what every poll of an Orker
// declaring a capacity holds until the task it took is admitted.
func (p SecurityPolicy) ceiling() (amount, error) {
	return policyAmount(p.MaxCPU, p.MaxMemory)
}

// defaults is what a task that requests nothing is given, as a reservation.
func (p SecurityPolicy) defaults() (amount, error) {
	return policyAmount(p.DefaultCPU, p.DefaultMemory)
}

func policyAmount(cpu, memory string) (amount, error) {
	cores, err := quantity.CPU(cpu)
	if err != nil {
		return amount{}, err
	}
	bytes, err := quantity.Memory(memory)
	if err != nil {
		return amount{}, err
	}
	return amountOf(cores, int64(bytes)), nil
}
