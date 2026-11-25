package orker

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	cdispec "tags.cncf.io/container-device-interface/specs-go"
)

// hook builds a hook the way nvidia-ctk 1.20.2 writes one (the fixtures in
// testdata/cdi are the captured originals): createContainer, the fixed path,
// argv[0] the binary's name, the one debug env.
func hook(args ...string) *cdispec.Hook {
	return &cdispec.Hook{
		HookName: "createContainer",
		Path:     "/usr/bin/nvidia-cdi-hook",
		Args:     append([]string{"nvidia-cdi-hook"}, args...),
		Env:      []string{"NVIDIA_CTK_DEBUG=false"},
	}
}

// TestCDIHooks_PermittedRows is the closed set of hook rows an NVIDIA spec
// may carry (design §4.10 item 4; decisions E91-Q4b..Q4h): each row has a
// capture or a source-built vector, and anything else is refused. Changing a
// row without changing this table fails the build.
func TestCDIHooks_PermittedRows(t *testing.T) {
	for name, h := range map[string]*cdispec.Hook{
		// Captured: testdata/cdi/nvidia-1.20.2-admitted.yaml.
		"create-symlinks, libcuda":         hook("create-symlinks", "--link", "libcuda.so.1::/lib/x86_64-linux-gnu/libcuda.so"),
		"create-symlinks, many pairs":      hook("create-symlinks", "--link", "libcuda.so.1::/usr/lib/x86_64-linux-gnu/libcuda.so", "--link", "../libnvidia-allocator.so.1::/usr/lib64/gbm/nvidia-drm_gbm.so", "--link", "libGLX_nvidia.so.580.95.05::/usr/lib/x86_64-linux-gnu/libGLX_indirect.so.0"),
		"create-symlinks, by-path":         hook("create-symlinks", "--link", "../card1::/dev/dri/by-path/pci-0000:01:00.0-card", "--link", "../renderD129::/dev/dri/by-path/pci-0000:01:00.0-render"),
		"disable-device-node-modification": hook("disable-device-node-modification"),
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkHook(h); err != nil {
				t.Fatalf("checkHook = %v, want admitted", err)
			}
		})
	}
}

func TestCDIHooks_RefusedForms(t *testing.T) {
	with := func(h *cdispec.Hook, edit func(*cdispec.Hook)) *cdispec.Hook { edit(h); return h }
	link := func(p string) *cdispec.Hook { return hook("create-symlinks", "--link", p) }
	for _, tc := range []struct {
		name   string
		h      *cdispec.Hook
		detail string
		says   string
	}{
		// Decisions: refused by name.
		{"update-ldcache (Q4b)", hook("update-ldcache", "--folder", "/lib/x86_64-linux-gnu"), DetailCDIHookRefused, "--disable-hook update-ldcache"},
		{"update-ldcache, bare (1.19.1)", hook("update-ldcache"), DetailCDIHookRefused, "update-ldcache"},
		{"enable-cuda-compat (Q4h)", hook("enable-cuda-compat", "--host-driver-version=580.95.05"), DetailCDIHookRefused, "enable-cuda-compat"},
		{"update-application-profile (Q4h)", hook("update-application-profile"), DetailCDIHookRefused, "update-application-profile"},
		{"apply-cuda-memory-limits as createRuntime (Q4g)", with(hook("apply-cuda-memory-limits", "--driver-root", "/", "--gpu-id", "GPU-0a1b2c3d-0000-4000-8000-000000000000"),
			func(h *cdispec.Hook) { h.HookName = "createRuntime" }), DetailCDIHookRefused, "createRuntime"},
		{"apply-cuda-memory-limits as createContainer", hook("apply-cuda-memory-limits", "--driver-root", "/", "--gpu-id", "GPU-0a1b2c3d-0000-4000-8000-000000000000"), DetailCDIHookRefused, "apply-cuda-memory-limits"},
		{"chmod", hook("chmod", "--mode", "755", "--path", "/dev/dri"), DetailCDIHookRefused, "chmod"},
		{"an unknown subcommand", hook("noop"), DetailCDIHookRefused, "noop"},
		{"an empty subcommand", hook(""), DetailCDIHookRefused, "subcommand"},
		{"no subcommand", with(hook(), func(h *cdispec.Hook) {}), DetailCDIHookArgs, "subcommand"},
		{"no args", with(hook(), func(h *cdispec.Hook) { h.Args = nil }), DetailCDIHookArgs, "args"},
		// hookName.
		{"a prestart hook", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.HookName = "prestart" }), DetailCDIHookRefused, "hookName"},
		{"a poststart hook", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.HookName = "poststart" }), DetailCDIHookRefused, "hookName"},
		{"a createRuntime hook", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.HookName = "createRuntime" }), DetailCDIHookRefused, "hookName"},
		{"no hookName", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.HookName = "" }), DetailCDIHookRefused, "hookName"},
		// Path.
		{"nvidia-ctk as the hook", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Path = "/usr/bin/nvidia-ctk" }), DetailCDIHookPath, "/usr/bin/nvidia-cdi-hook"},
		{"another directory", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Path = "/usr/local/bin/nvidia-cdi-hook" }), DetailCDIHookPath, "/usr/bin/nvidia-cdi-hook"},
		{"a relative path", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Path = "nvidia-cdi-hook" }), DetailCDIHookPath, "/usr/bin/nvidia-cdi-hook"},
		{"a dotted path", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Path = "/usr/bin/../bin/nvidia-cdi-hook" }), DetailCDIHookPath, "/usr/bin/nvidia-cdi-hook"},
		{"a shell", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Path = "/bin/sh" }), DetailCDIHookPath, "/usr/bin/nvidia-cdi-hook"},
		// argv[0] and shape.
		{"argv[0] of nvidia-ctk", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Args[0] = "nvidia-ctk" }), DetailCDIHookArgs, "argv[0]"},
		{"argv[0] a path", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Args[0] = "/usr/bin/nvidia-cdi-hook" }), DetailCDIHookArgs, "argv[0]"},
		{"ctk's hook subcommand", with(hook("hook", "disable-device-node-modification"), func(h *cdispec.Hook) { h.Args[0] = "nvidia-ctk" }), DetailCDIHookArgs, "argv[0]"},
		{"an extra argument", hook("disable-device-node-modification", "--debug"), DetailCDIHookArgs, "no arguments"},
		{"the hidden --container-spec", hook("create-symlinks", "--container-spec", "/x", "--link", "a::/usr/lib/b"), DetailCDIHookArgs, "--link"},
		{"create-symlinks, no link", hook("create-symlinks"), DetailCDIHookArgs, "at least one"},
		{"create-symlinks, a lone --link", hook("create-symlinks", "--link"), DetailCDIHookArgs, "--link"},
		{"create-symlinks, --link=pair", hook("create-symlinks", "--link=a::/usr/lib/b"), DetailCDIHookArgs, "--link"},
		{"create-symlinks, a stray argument", hook("create-symlinks", "--link", "a::/usr/lib/b", "extra"), DetailCDIHookArgs, "--link"},
		{"a pair with no ::", link("libcuda.so.1:/usr/lib/libcuda.so"), DetailCDIHookArgs, "::"},
		{"a pair with three parts", link("a::/usr/lib/b::c"), DetailCDIHookArgs, "::"},
		{"an empty target", link("::/usr/lib/b"), DetailCDIHookArgs, "target"},
		{"an empty link", link("a::"), DetailCDIHookArgs, "link"},
		{"a relative link path", link("a::usr/lib/b"), DetailCDIHookArgs, "link"},
		{"a link path with ..", link("a::/usr/lib/../../etc/passwd"), DetailCDIHookArgs, "link"},
		{"an unclean link path", link("a::/usr/lib//b"), DetailCDIHookArgs, "link"},
		{"a trailing slash", link("a::/usr/lib/b/"), DetailCDIHookArgs, "link"},
		{"a link under /etc", link("a::/etc/passwd"), DetailCDIHookArgs, "link"},
		{"a link under /usr/bin", link("a::/usr/bin/sh"), DetailCDIHookArgs, "link"},
		{"a link under /dev, not by-path", link("../x::/dev/nvidia0"), DetailCDIHookArgs, "link"},
		{"a link that is a library directory", link("a::/usr/lib"), DetailCDIHookArgs, "link"},
		{"a link that is /lib", link("a::/lib"), DetailCDIHookArgs, "link"},
		{"a link that is /lib64", link("a::/lib64"), DetailCDIHookArgs, "link"},
		{"a link path with a space", link("a::/usr/lib/b c"), DetailCDIHookArgs, "link"},
		{"a link path with a shell character", link("a::/usr/lib/$(x)"), DetailCDIHookArgs, "link"},
		{"a link path with a control character", link("a::/usr/lib/b\n"), DetailCDIHookArgs, "link"},
		{"a target with a control character", link("a\x00b::/usr/lib/b"), DetailCDIHookArgs, "target"},
		{"a target with a colon", link("a:b::/usr/lib/c"), DetailCDIHookArgs, "target"},
		{"an absolute target", link("/etc/shadow::/usr/lib/x86_64-linux-gnu/x"), DetailCDIHookArgs, "target"},
		{"a target that climbs out of the library directories", link("../../../etc/shadow::/usr/lib/x86_64-linux-gnu/x"), DetailCDIHookArgs, "target"},
		{"a by-path link to another node", link("../../etc/shadow::/dev/dri/by-path/pci-0000:01:00.0-card"), DetailCDIHookArgs, "target"},
		{"a by-path link that is not a pci name", link("../card1::/dev/dri/by-path/x"), DetailCDIHookArgs, "link"},
		{"a colon in a library link path", link("a::/usr/lib/b:c"), DetailCDIHookArgs, "link"},
		{"a target with a space", link("a b::/usr/lib/c"), DetailCDIHookArgs, "target"},
		// env and timeout.
		{"no env", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Env = nil }), DetailCDIHookArgs, "env"},
		{"debug on", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Env = []string{"NVIDIA_CTK_DEBUG=true"} }), DetailCDIHookArgs, "env"},
		{"an extra env", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Env = append(h.Env, "LD_PRELOAD=/x.so") }), DetailCDIHookArgs, "env"},
		{"a replaced env", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { h.Env = []string{"LD_PRELOAD=/x.so"} }), DetailCDIHookArgs, "env"},
		{"a timeout", with(hook("disable-device-node-modification"), func(h *cdispec.Hook) { t := 5; h.Timeout = &t }), DetailCDIHookArgs, "timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkHook(tc.h)
			if err == nil || !strings.HasPrefix(err.Error(), tc.detail+":") || !strings.Contains(err.Error(), tc.says) {
				t.Fatalf("checkHook = %v, want %s naming %q", err, tc.detail, tc.says)
			}
		})
	}
	if err := checkHook(nil); err == nil || !strings.HasPrefix(err.Error(), DetailCDIHookRefused) {
		t.Errorf("checkHook(nil) = %v", err)
	}
}

// TestCDIHooks_StockSpecIsRefusedNamingTheFlag: nvidia-ctk's default output
// carries enable-cuda-compat, update-ldcache and the modeset node; the audit
// refuses it at the first of them and says how to generate the admitted one.
func TestCDIHooks_StockSpecIsRefusedNamingTheFlag(t *testing.T) {
	h := nvidiaTestHost(t)
	dir := specDir(t, map[string]string{"nvidia.yaml": nvidiaFixture(t, "nvidia-1.20.2-stock.yaml")})
	_, err := auditGPUSpecs([]string{dir}, nvidiaKind, []string{"all"}, h)
	if err == nil || !strings.Contains(err.Error(), DetailCDIHookRefused) || !strings.Contains(err.Error(), "--disable-hook") {
		t.Fatalf("auditGPUSpecs = %v, want %s naming --disable-hook", err, DetailCDIHookRefused)
	}
}

// TestCDIHooks_BinaryBuild: the hook binary behind an admitted hook is
// hashed, and a build that is no row of the table (for this arch) disables
// the profile (HOOK_BUILD_UNKNOWN), as a binary that is not root's, or not
// there, refuses it (CDI_HOOK_PATH). A hookless spec never looks at it.
func TestCDIHooks_BinaryBuild(t *testing.T) {
	dir := specDir(t, map[string]string{"nvidia.yaml": nvidiaFixture(t, "nvidia-1.20.2-admitted.yaml")})
	known := knownTestBuild(t)

	t.Run("a table row's build is admitted", func(t *testing.T) {
		audit, err := auditGPUSpecs([]string{dir}, nvidiaKind, []string{"all"}, nvidiaTestHost(t))
		if err != nil {
			t.Fatalf("auditGPUSpecs = %v", err)
		}
		if got := audit.hooks[cdiHookBinary]; got != known.BinarySHA256 {
			t.Fatalf("audit records hook digest %q, want %q", got, known.BinarySHA256)
		}
	})
	t.Run("an unknown digest", func(t *testing.T) {
		h := nvidiaTestHost(t)
		h.hookBinary = func(string) (hostHook, error) { return hostHook{SHA256: strings.Repeat("ab", 32)}, nil }
		_, err := auditGPUSpecs([]string{dir}, nvidiaKind, []string{"all"}, h)
		if !errors.Is(err, errHookBuildUnknown) || !strings.Contains(err.Error(), DetailHookBuildUnknown) ||
			!strings.Contains(err.Error(), strings.Repeat("ab", 32)) {
			t.Fatalf("auditGPUSpecs = %v, want errHookBuildUnknown naming the digest", err)
		}
	})
	t.Run("a known digest of another arch", func(t *testing.T) {
		h := nvidiaTestHost(t)
		h.arch = "arm64"
		if known.Arch != "amd64" {
			t.Skip("the test build is not amd64")
		}
		_, err := auditGPUSpecs([]string{dir}, nvidiaKind, []string{"all"}, h)
		if !errors.Is(err, errHookBuildUnknown) {
			t.Fatalf("auditGPUSpecs = %v, want errHookBuildUnknown", err)
		}
	})
	t.Run("a binary that is not trusted", func(t *testing.T) {
		h := nvidiaTestHost(t)
		h.hookBinary = func(p string) (hostHook, error) {
			return hostHook{}, fmt.Errorf("%s is owned by uid 1000, not root", p)
		}
		_, err := auditGPUSpecs([]string{dir}, nvidiaKind, []string{"all"}, h)
		if err == nil || errors.Is(err, errHookBuildUnknown) || !strings.Contains(err.Error(), DetailCDIHookPath) {
			t.Fatalf("auditGPUSpecs = %v, want %s", err, DetailCDIHookPath)
		}
	})
	t.Run("a binary that is not there", func(t *testing.T) {
		h := nvidiaTestHost(t)
		h.hookBinary = func(p string) (hostHook, error) { return hostHook{}, fmt.Errorf("%s: %w", p, os.ErrNotExist) }
		_, err := auditGPUSpecs([]string{dir}, nvidiaKind, []string{"all"}, h)
		if err == nil || !strings.Contains(err.Error(), DetailCDIHookPath) {
			t.Fatalf("auditGPUSpecs = %v, want %s", err, DetailCDIHookPath)
		}
	})
	t.Run("a hookless spec never asks for the binary", func(t *testing.T) {
		h := nvidiaTestHost(t)
		h.hookBinary = func(string) (hostHook, error) { t.Fatal("the hook binary was asked for"); return hostHook{}, nil }
		hookless := specDir(t, map[string]string{"nvidia.yaml": nvidiaFixture(t, "nvidia-1.20.2-hookless.yaml")})
		audit, err := auditGPUSpecs([]string{hookless}, nvidiaKind, []string{"all"}, h)
		if err != nil || len(audit.hooks) != 0 {
			t.Fatalf("auditGPUSpecs = %+v, %v", audit, err)
		}
	})
}
