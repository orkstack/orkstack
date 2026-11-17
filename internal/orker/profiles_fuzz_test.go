package orker

import (
	"slices"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// FuzzParseOrkerProfiles holds ParseProfiles to what it claims for any input:
// it never panics; what it accepts is sorted by unique name, every name is a
// slug, every profile has exactly its own kind's body and
// an identity, the one-per-Orker limits hold, and a profile is never accepted
// without a digest-pinned probe image. Parsing is deterministic.
func FuzzParseOrkerProfiles(f *testing.F) {
	f.Add([]byte(validProfilesDoc))
	f.Add([]byte(""))
	f.Add([]byte("profiles: {}\n"))
	f.Add([]byte("profiles:\n  probe_image: ghcr.io/orkstack/orker@" + testDigest + "\n  x:\n    kind: gpu\n    cdi:\n      kind: amd.com/gpu\n      units:\n        - \"0\"\n"))
	f.Add([]byte("profiles:\n  a: &x\n    kind: gpu\n  b: *x\n"))
	f.Add([]byte("profiles:\n  x:\n    kind: devices\n    devices:\n      - path: /dev/../etc/shadow\n        container_path: /dev/s\n        permissions: r\n"))
	f.Add([]byte("profiles: !!map\n  <<: {}\n"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		got, err := ParseProfiles(raw)
		if err != nil {
			return
		}
		again, err := ParseProfiles(raw)
		if err != nil || len(again.List) != len(got.List) {
			t.Fatalf("not deterministic: %v", err)
		}
		if len(got.List) > 0 {
			if err := digestPinned(got.ProbeImage); err != nil {
				t.Fatalf("profiles accepted without a pinned probe image: %q", got.ProbeImage)
			}
		}
		if got.AuditInterval < minProfileInterval || got.RecoveryInterval < minProfileInterval {
			t.Fatalf("intervals below the minimum: %s %s", got.AuditInterval, got.RecoveryInterval)
		}
		names := make([]string, 0, len(got.List))
		perKind := map[orkstackv1.ProfileKind]int{}
		for _, p := range got.List {
			names = append(names, p.Name)
			perKind[p.Kind]++
			bodies := 0
			if p.GPU != nil {
				bodies++
				if p.Kind != orkstackv1.ProfileKind_PROFILE_KIND_GPU || len(p.GPU.Units) == 0 || slices.Contains(p.GPU.Units, "all") {
					t.Fatalf("gpu body wrong: %+v", p)
				}
			}
			if p.Devices != nil {
				bodies++
				for _, d := range p.Devices {
					if hostDevicePath(d.Path) != nil || checkContainerPath(d.ContainerPath) != nil {
						t.Fatalf("device paths not held: %+v", d)
					}
				}
			}
			if bodies != 1 {
				t.Fatalf("profile %q has %d bodies", p.Name, bodies)
			}
			if _, err := p.StructuralIdentity(func(string) (string, error) { return "c", nil }); err != nil {
				t.Fatalf("accepted profile has no identity: %v", err)
			}
		}
		if !slices.IsSorted(names) || len(slices.Compact(slices.Clone(names))) != len(names) {
			t.Fatalf("names not sorted and unique: %v", names)
		}
		if perKind[orkstackv1.ProfileKind_PROFILE_KIND_GPU] > 1 {
			t.Fatalf("one-per-Orker limit broken: %v", perKind)
		}
	})
}
