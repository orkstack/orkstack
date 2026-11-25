package orker

import (
	"path"
	"slices"
	"strings"
	"testing"

	cdispec "tags.cncf.io/container-device-interface/specs-go"
)

// FuzzCDIHookGrammar: whatever a spec's hook, env or mount holds, the
// grammar never panics, and what it admits is exactly a row of it: a
// createContainer hook at the one path whose arguments are a fixed
// subcommand's, every link of create-symlinks a relative target to a clean
// absolute path under the library directories (or the by-path directory), and
// every mount a read-only, nosuid, nodev bind of a regular file under the
// compiled roots landing under the container roots (or the void placeholder).
// Seeds are the tables' rows (cdi_hooks_test.go, cdi_nvidia_test.go).
func FuzzCDIHookGrammar(f *testing.F) {
	f.Add("createContainer", cdiHookBinary, "nvidia-cdi-hook", "create-symlinks", "--link", "libcuda.so.1::/lib/x86_64-linux-gnu/libcuda.so", "NVIDIA_CTK_DEBUG=false", "NVIDIA_VISIBLE_DEVICES=void", "/usr/lib/x86_64-linux-gnu/libcuda.so.580", "/usr/lib/x86_64-linux-gnu/libcuda.so.580", "ro,nosuid,nodev,rbind,rprivate")
	f.Add("createContainer", cdiHookBinary, "nvidia-cdi-hook", "disable-device-node-modification", "", "", "NVIDIA_CTK_DEBUG=false", "NVIDIA_CTK_LIBCUDA_DIR=/usr/lib64:/lib", "/usr/bin/nvidia-smi", "/usr/bin/nvidia-smi", "ro,nosuid,nodev")
	f.Add("createContainer", cdiHookBinary, "nvidia-cdi-hook", "create-symlinks", "--link", "../card1::/dev/dri/by-path/pci-0000:01:00.0-card", "NVIDIA_CTK_DEBUG=false", "NVIDIA_VISIBLE_DEVICES=all", "/dev/null", "/var/run/nvidia-container-devices/void", "ro,nosuid,nodev,bind")
	f.Add("createRuntime", cdiHookBinary, "nvidia-cdi-hook", "apply-cuda-memory-limits", "--driver-root", "/", "NVIDIA_CTK_DEBUG=false", "LD_PRELOAD=/x.so", "/etc/shadow", "/etc/passwd", "rw")
	f.Add("createContainer", "/usr/bin/nvidia-ctk", "nvidia-ctk", "hook", "update-ldcache", "", "", "", "/tmp/nvidia-mps", "/tmp/nvidia-mps", "")
	f.Add("createContainer", cdiHookBinary, "nvidia-cdi-hook", "create-symlinks", "--link", "a::/usr/lib/../../etc/x", "NVIDIA_CTK_DEBUG=false", "", "/usr/lib/../x", "/usr/lib//x", "ro,nosuid,nodev")
	ctx := &specAudit{kind: "nvidia.com/gpu", nvidia: true, h: nvidiaFuzzHost()}
	f.Fuzz(func(t *testing.T, hookName, hookPath, argv0, sub, a1, a2, env1, env2, src, dst, opts string) {
		h := &cdispec.Hook{HookName: hookName, Path: hookPath, Args: []string{argv0, sub, a1, a2}, Env: []string{env1}}
		for _, args := range [][]string{h.Args, h.Args[:3], h.Args[:2], {argv0, sub}} {
			h.Args = args
			if err := checkHook(h); err == nil {
				if h.HookName != "createContainer" || h.Path != cdiHookBinary || h.Args[0] != cdiHookArgv0 ||
					!slices.Equal(h.Env, []string{cdiHookEnv}) || h.Timeout != nil {
					t.Fatalf("admitted %+v", h)
				}
				switch h.Args[1] {
				case hookDisableNodeMod:
					if len(h.Args) != 2 {
						t.Fatalf("admitted arguments to %s: %q", h.Args[1], h.Args)
					}
				case hookCreateSymlinks:
					if len(h.Args) < 4 || len(h.Args)%2 != 0 {
						t.Fatalf("admitted %q", h.Args)
					}
					for i := 2; i < len(h.Args); i += 2 {
						target, link, _ := strings.Cut(h.Args[i+1], "::")
						if h.Args[i] != "--link" || path.IsAbs(target) || strings.Contains(h.Args[i+1], ":::") ||
							path.Clean(link) != link || !path.IsAbs(link) || strings.Contains(link, "..") ||
							!(underAny(link, cdiLibRoots) || path.Dir(link) == byPathDir) {
							t.Fatalf("admitted the link %q", h.Args[i+1])
						}
					}
				default:
					t.Fatalf("admitted the subcommand %q", h.Args[1])
				}
			}
		}
		if err := checkNVIDIAEnv([]string{env1, env2}); err == nil {
			for _, e := range []string{env1, env2} {
				if e != "NVIDIA_VISIBLE_DEVICES=void" && !strings.HasPrefix(e, "NVIDIA_CTK_LIBCUDA_DIR=/") {
					t.Fatalf("admitted the env %q", e)
				}
			}
		}
		m := &cdispec.Mount{HostPath: src, ContainerPath: dst, Options: strings.Split(opts, ",")}
		if source, err := ctx.auditMount(m); err == nil {
			resolved := source.Resolved
			for _, want := range mountRequired {
				if !slices.Contains(m.Options, want) {
					t.Fatalf("admitted a mount without %s: %+v", want, m)
				}
			}
			void := m.HostPath == nvidiaVoidSource && m.ContainerPath == nvidiaVoidTarget
			if !void && (!normalPath(m.HostPath) || !normalPath(m.ContainerPath) || !underAny(resolved, nvidiaHostRoots) ||
				!underAny(m.ContainerPath, nvidiaContainerRoots)) {
				t.Fatalf("admitted the mount %+v (resolved %s)", m, resolved)
			}
		}
	})
}

// nvidiaFuzzHost is a host where every path is a regular file that is its
// own resolution: the grammar's shape and roots decide alone.
func nvidiaFuzzHost() cdiHost {
	return cdiHost{inspectMount: func(p string) (hostMount, error) { return hostMount{Resolved: p, Kind: mountFile}, nil }}
}
