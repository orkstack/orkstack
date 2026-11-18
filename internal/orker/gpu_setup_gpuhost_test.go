//go:build integration && gpuhost

package orker

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	cdi "tags.cncf.io/container-device-interface/pkg/cdi"
)

// TestGPUHost_SetupSpecPassesTheAudit: the spec scripts/gpu-host-setup.sh
// installs is one dockerd reads (its CDISpecDirs name /etc/cdi), one
// dockerd's own validator accepts (cdi.ReadSpec, the version moby vendors),
// and one the Orker's audit accepts on this host's real nodes: unit amd is
// the amdgpu render node and /dev/kfd, unit intel the i915 render node, each
// a GPU class, and every node a character device the host has.
func TestGPUHost_SetupSpecPassesTheAudit(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dirs := gpuHostDocker(ctx, t, "info", "--format", "{{json .CDISpecDirs}}")
	if !strings.Contains(dirs, `"/etc/cdi"`) {
		t.Fatalf("dockerd reads CDI specs from %s, which does not name /etc/cdi; the spec there is never resolved", dirs)
	}
	parsed, err := cdi.ReadSpec(spec.path, 0)
	if err != nil {
		t.Fatalf("dockerd's validator refuses %s: %v", spec.path, err)
	}
	if got := parsed.GetVendor() + "/" + parsed.GetClass(); got != spec.kind {
		t.Fatalf("%s declares kind %q; want %q", spec.path, got, spec.kind)
	}

	raw, err := os.ReadFile(procDevicesPath)
	if err != nil {
		t.Fatal(err)
	}
	found, err := auditGPUSpecs(DefaultCDISpecDirs, spec.kind, []string{"amd", "intel"},
		hostCDI(parseProcDevices(string(raw)), runtimeSocketPath()))
	if err != nil {
		t.Fatalf("the Orker's audit refuses %s: %v", spec.path, err)
	}
	if _, ok := found.files[spec.path]; !ok {
		t.Fatalf("the audit set is %v; want it to hold %s", found.files, spec.path)
	}
	if len(found.units) != 2 {
		t.Fatalf("the audit found %d units; want amd and intel", len(found.units))
	}
	want := map[string][]string{"amd": {"drm-render", "kfd"}, "intel": {"drm-render"}}
	for _, u := range found.units {
		var classes []string
		for _, n := range u.nodes {
			if !n.host.Char {
				t.Errorf("unit %s: %s is not a character device on this host", u.name, n.hostPath)
			}
			classes = append(classes, n.class)
		}
		slices.Sort(classes)
		if !slices.Equal(classes, want[u.name]) {
			t.Errorf("unit %s's nodes are of classes %v; want %v", u.name, classes, want[u.name])
		}
		if len(u.gids) != 1 {
			t.Errorf("unit %s carries groups %v; want the render group alone", u.name, u.gids)
		}
	}
}

// TestIntegrationGPU_AdditionalGidsApplied (design Q3): a task running as a
// user with no group of its own (65534) holds the render group the spec's
// additionalGids name, which is the render node's group on the host, and
// opens its render node read-only. The open is the only access; no ioctl or
// compute reaches the GPU. The runtime gives a CDI node the container user's
// own uid and gid (unit_probe.go, checkCDINode), and this host's render nodes
// are 0666, so the open is not the group's doing: the group is what the spec
// adds, shown against a control without it. The containers are started with
// the docker CLI, as the dispatcher's own tasks run as the image's user.
func TestIntegrationGPU_AdditionalGidsApplied(t *testing.T) {
	spec := requireGPUHost(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	g := newGPUOrker(ctx, t, spec, gpuOrkerOptions{label: "gpuhost-gids"})
	audit, _ := g.s.audit(gpuHostProfile)
	const intel = 1
	node := audit.unitNodes[intel][0]
	gid := node.Host.GID

	out := gpuHostDocker(ctx, t, "run", "--rm", "--network", "none", "--user", "65534:65534",
		"--label", "ork.orker="+g.orkerID,
		"--device", spec.kind+"=intel",
		gpuHostImage(t), "/bin/sh", "-c",
		fmt.Sprintf(`id -G; if : < %s; then echo opened; else echo refused; fi`, node.ContainerPath))
	lines := strings.Split(out, "\n")
	if len(lines) != 2 {
		t.Fatalf("the task printed %q; want its groups and whether the open succeeded", out)
	}
	if !slices.Contains(strings.Fields(lines[0]), fmt.Sprint(gid)) {
		t.Fatalf("a user-65534 task's groups are %q; want them to include the render group %d the spec names", lines[0], gid)
	}
	if lines[1] != "opened" {
		t.Fatalf("a user-65534 task could not open %s read-only", node.ContainerPath)
	}

	// The control: the same node passed without the spec (--device) leaves
	// the same user without the render group, so the group above is the
	// spec's additionalGids and nothing else.
	out = gpuHostDocker(ctx, t, "run", "--rm", "--network", "none", "--user", "65534:65534",
		"--label", "ork.orker="+g.orkerID,
		"--device", node.Host.Resolved+":"+node.ContainerPath+":r",
		gpuHostImage(t), "/bin/sh", "-c", "id -G")
	if slices.Contains(strings.Fields(strings.TrimSpace(out)), fmt.Sprint(gid)) {
		t.Fatalf("a user-65534 task without the spec holds groups %q, the render group %d among them; the test cannot tell the spec's group apart", out, gid)
	}
}
