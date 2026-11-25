package orker

import (
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"

	cdispec "tags.cncf.io/container-device-interface/specs-go"
)

// The hook grammar of an NVIDIA CDI spec (design E91 §4.10 item 4, P4b;
// AGENTS.md §6 and §13). It is a closed set compiled into the Orker, never
// operator data and never an extension point: exactly the rows below, each
// with a captured or source-built vector in the tests, and every other hook,
// path, argument or env is refused. Its decisions (Q4b to Q4h, accepted
// 2026-11-20):
//
//   - update-ldcache is refused: it runs ldconfig against the container's
//     root at create (CVE-2025-23267). A spec is generated with
//     `--disable-hook update-ldcache`.
//   - enable-cuda-compat and update-application-profile are refused: both
//     write inside the container's root from host-derived input.
//   - apply-cuda-memory-limits (a createRuntime hook, toolkit 1.20.1 and up)
//     is refused: a runtime-namespace hook widens the audited surface beyond
//     createContainer.
//   - chmod (off by default in the generator) and every other subcommand are
//     refused; nvidia-cdi-hook itself runs an unknown subcommand as a no-op,
//     so only an exact row is admitted.
//   - create-symlinks and disable-device-node-modification are admitted as
//     exact typed rows.
//
// Which build of the binary the path names is the table's business
// (cdi_hook_builds.go): the Orker never executes it, it hashes it.

// The ORKER_PROFILE_INVALID details of the hook audit. CDI_EDIT_REFUSED, for
// the edits around the hooks, is cdi_audit.go's.
const (
	// DetailCDIHookRefused: a hook, or its hookName or subcommand, is not a
	// row of the grammar.
	DetailCDIHookRefused = "CDI_HOOK_REFUSED"
	// DetailCDIHookPath: the hook's path is not the fixed one, or the binary
	// there is not root's.
	DetailCDIHookPath = "CDI_HOOK_PATH"
	// DetailCDIHookArgs: a row's arguments, env or timeout are not its
	// vector's.
	DetailCDIHookArgs = "CDI_HOOK_ARGS"
	// DetailHookBuildUnknown: the binary's build is no row of the table. It
	// disables the gpu profile alone (the Orker is not cordoned): the
	// profile's reason is HOOK_BUILD_UNKNOWN.
	DetailHookBuildUnknown = "HOOK_BUILD_UNKNOWN"
)

const (
	// cdiHookBinary is the one path a hook may name: where both NVIDIA
	// packages install it, and what nvidia-ctk writes by default.
	cdiHookBinary = "/usr/bin/nvidia-cdi-hook"
	// cdiHookArgv0 is argv[0] of every hook: the binary's name, as the
	// generator writes it.
	cdiHookArgv0 = "nvidia-cdi-hook"
	// cdiHookEnv is the one env entry every hook carries.
	cdiHookEnv = "NVIDIA_CTK_DEBUG=false"
	// cdiHookName is the one lifecycle point a hook may run at.
	cdiHookName = "createContainer"

	hookCreateSymlinks = "create-symlinks"
	hookDisableNodeMod = "disable-device-node-modification"
)

// errHookBuildUnknown is the audit's answer when the hook binary is of a
// build the table does not list: the gpu profile is unavailable
// (HOOK_BUILD_UNKNOWN), not refused, and the Orker keeps serving the rest.
var errHookBuildUnknown = errors.New("the hook binary is of a build this release does not list")

// errHookBinaryUnreadable is the audit's answer when the hook binary cannot be
// held to root's (CDI_HOOK_PATH): absent, not root's, or unreadable. After
// boot it is a changed binary, not a changed spec.
var errHookBinaryUnreadable = errors.New("the hook binary cannot be audited")

// cdiRefusal is a refusal with its detail: the error's text is
// "DETAIL: why", which the audit prefixes with the unit it is of.
type cdiRefusal struct{ detail, why string }

func (r *cdiRefusal) Error() string { return r.detail + ": " + r.why }

func refuse(detail, format string, args ...any) error {
	return &cdiRefusal{detail: detail, why: fmt.Sprintf(format, args...)}
}

// refusedHooks says why each subcommand a stock generator writes is refused,
// by name. Unlisted subcommands are refused as not rows.
var refusedHooks = map[string]string{
	"update-ldcache": "it runs ldconfig against the container's root at create; generate the spec with " +
		"`nvidia-ctk cdi generate --disable-hook update-ldcache`",
	"enable-cuda-compat": "it writes inside the container's root from the host's driver version; generate the spec " +
		"with `--disable-hook enable-cuda-compat`",
	"update-application-profile": "it writes a profile file inside the container's root; generate the spec with " +
		"`--disable-hook update-application-profile`",
	"apply-cuda-memory-limits": "it is a createRuntime hook, which runs in the runtime's namespace; generate the spec " +
		"with `--disable-hook apply-cuda-memory-limits`",
	"chmod": "it changes modes inside the container's root",
}

// checkHook holds one hook to the grammar, or says why not as a *cdiRefusal.
// It never looks at the host: the binary behind the path is the audit's.
func checkHook(h *cdispec.Hook) error {
	if h == nil {
		return refuse(DetailCDIHookRefused, "an empty hook")
	}
	// A refused subcommand is named whatever its hookName and path are, so
	// the operator is told the flag, not a symptom of it.
	sub := ""
	if len(h.Args) > 1 {
		sub = h.Args[1]
	}
	if why, ok := refusedHooks[sub]; ok {
		return refuse(DetailCDIHookRefused, "hook %s is refused: %s", sub, why)
	}
	if h.HookName != cdiHookName {
		return refuse(DetailCDIHookRefused, "hookName %q is refused; only %s hooks run", h.HookName, cdiHookName)
	}
	if h.Path != cdiHookBinary {
		return refuse(DetailCDIHookPath, "hook path %q is refused; the only path is %s", h.Path, cdiHookBinary)
	}
	if len(h.Args) == 0 {
		return refuse(DetailCDIHookArgs, "a hook has no args; argv[0] is %s and argv[1] its subcommand", cdiHookArgv0)
	}
	if h.Args[0] != cdiHookArgv0 {
		return refuse(DetailCDIHookArgs, "hook argv[0] is %q, want %q", h.Args[0], cdiHookArgv0)
	}
	if len(h.Args) < 2 {
		return refuse(DetailCDIHookArgs, "a hook names no subcommand")
	}
	if len(h.Env) != 1 || h.Env[0] != cdiHookEnv {
		return refuse(DetailCDIHookArgs, "hook env is %q, want exactly [%s]", h.Env, cdiHookEnv)
	}
	if h.Timeout != nil {
		return refuse(DetailCDIHookArgs, "a hook timeout is refused; the generator sets none")
	}
	args := h.Args[2:]
	switch sub {
	case hookCreateSymlinks:
		return checkSymlinkArgs(args)
	case hookDisableNodeMod:
		if len(args) != 0 {
			return refuse(DetailCDIHookArgs, "hook %s takes no arguments, not %q", sub, args)
		}
		return nil
	}
	return refuse(DetailCDIHookRefused, "hook subcommand %q is not a row of this release's grammar", sub)
}

// Where a create-symlinks link may be made, and what it may lead to. The
// hook makes the link inside the container's root, so the link's directory
// is held to the library directories and to the by-path directory the
// generator writes the per-GPU DRM links in, and its target to stay beside
// them.
var (
	// cdiLibRoots are the container's library directories: where a link, a
	// library mount and NVIDIA_CTK_LIBCUDA_DIR may be.
	cdiLibRoots = []string{"/usr/lib", "/usr/lib64", "/lib", "/lib64"}

	linkSegment = regexp.MustCompile(`^[A-Za-z0-9._+-]+$`)
	// linkTarget is a relative target: names and ".." steps, never a colon.
	linkTargetSegment = regexp.MustCompile(`^(\.\.|[A-Za-z0-9._+-]+)$`)
	byPathName        = regexp.MustCompile(`^pci-[0-9a-fA-F]{1,8}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\.[0-7]-(card|render)$`)
	drmTarget         = regexp.MustCompile(`^(card|renderD)[0-9]+$`)
)

const byPathDir = "/dev/dri/by-path"

// checkSymlinkArgs holds create-symlinks' arguments to `--link target::link`
// pairs, one or more.
func checkSymlinkArgs(args []string) error {
	if len(args) == 0 {
		return refuse(DetailCDIHookArgs, "hook %s needs at least one --link pair", hookCreateSymlinks)
	}
	if len(args)%2 != 0 {
		return refuse(DetailCDIHookArgs, "hook %s: arguments are --link target::link pairs, %q is not", hookCreateSymlinks, args)
	}
	for i := 0; i < len(args); i += 2 {
		if args[i] != "--link" {
			return refuse(DetailCDIHookArgs, "hook %s: argument %q is not --link", hookCreateSymlinks, args[i])
		}
		if err := checkLinkPair(args[i+1]); err != nil {
			return err
		}
	}
	return nil
}

// checkLinkPair holds one "target::link" to the grammar.
func checkLinkPair(pair string) error {
	parts := strings.Split(pair, "::")
	if len(parts) != 2 {
		return refuse(DetailCDIHookArgs, "hook %s: %q is not target::link (exactly one ::)", hookCreateSymlinks, pair)
	}
	target, link := parts[0], parts[1]
	if err := checkLinkTarget(target); err != nil {
		return err
	}
	if link == "" || !path.IsAbs(link) || path.Clean(link) != link {
		return refuse(DetailCDIHookArgs, "hook %s: link path %q is not a clean absolute path", hookCreateSymlinks, link)
	}
	dir := path.Dir(link)
	if dir == byPathDir {
		if !byPathName.MatchString(path.Base(link)) {
			return refuse(DetailCDIHookArgs, "hook %s: link path %q is not a pci-<address>-card or -render link in %s", hookCreateSymlinks, link, byPathDir)
		}
		if resolved := path.Join(dir, target); path.Dir(resolved) != "/dev/dri" || !drmTarget.MatchString(path.Base(resolved)) {
			return refuse(DetailCDIHookArgs, "hook %s: target %q of %s leaves /dev/dri's card and render nodes", hookCreateSymlinks, target, link)
		}
		return nil
	}
	if !strictlyUnder(link, cdiLibRoots) {
		return refuse(DetailCDIHookArgs, "hook %s: link path %q is not under %s or %s", hookCreateSymlinks, link, strings.Join(cdiLibRoots, ", "), byPathDir)
	}
	for _, seg := range strings.Split(link[1:], "/") {
		if !linkSegment.MatchString(seg) {
			return refuse(DetailCDIHookArgs, "hook %s: link path %q has a segment of characters a library path does not", hookCreateSymlinks, link)
		}
	}
	if resolved := path.Join(dir, target); !underAny(resolved, cdiLibRoots) {
		return refuse(DetailCDIHookArgs, "hook %s: target %q of %s leaves the library directories", hookCreateSymlinks, target, link)
	}
	return nil
}

// checkLinkTarget holds a link's target to a relative path of names and
// parent steps.
func checkLinkTarget(target string) error {
	if target == "" || path.IsAbs(target) {
		return refuse(DetailCDIHookArgs, "hook %s: target %q is not a relative path", hookCreateSymlinks, target)
	}
	for _, seg := range strings.Split(target, "/") {
		if !linkTargetSegment.MatchString(seg) {
			return refuse(DetailCDIHookArgs, "hook %s: target %q has a segment that is not a name or ..", hookCreateSymlinks, target)
		}
	}
	return nil
}

// strictlyUnder is underAny with the roots themselves excluded: a link is made
// in a library directory, never as one.
func strictlyUnder(p string, roots []string) bool {
	return !slices.Contains(roots, p) && underAny(p, roots)
}

// underAny reports whether p is a root, or below one: by path segments, so
// /usr/library is not under /usr/lib.
func underAny(p string, roots []string) bool {
	for _, r := range roots {
		if p == r || strings.HasPrefix(p, r+"/") {
			return true
		}
	}
	return false
}
