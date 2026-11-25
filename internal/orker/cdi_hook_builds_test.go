package orker

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestCDIHookTable_RowsAtOrAboveFloor: every row of the generated table is a
// release at or above the floor, of an arch the Orker ships, of either
// packaging, with digests of the right shape, and no build is listed twice.
// Raising the floor without regenerating the table fails here.
func TestCDIHookTable_RowsAtOrAboveFloor(t *testing.T) {
	if len(cdiHookTable) == 0 {
		t.Fatal("the hook table is empty")
	}
	floor := releaseOf(t, cdiHookFloor)
	hex64 := regexp.MustCompile(`^[0-9a-f]{64}$`)
	seen := map[string]bool{}
	for _, b := range cdiHookTable {
		v := releaseOf(t, b.Version)
		if compareRelease(v, floor) < 0 {
			t.Errorf("%s %s %s is below the floor %s", b.Version, b.Arch, b.Packaging, cdiHookFloor)
		}
		if b.Arch != "amd64" && b.Arch != "arm64" {
			t.Errorf("%+v: arch %q", b, b.Arch)
		}
		want := map[string]string{"deb": "sha256:", "rpm": "sha512:"}[b.Packaging]
		if want == "" || !strings.HasPrefix(b.PackageDigest, want) || !hex64.MatchString(b.BinarySHA256) ||
			!strings.HasPrefix(b.Package, "https://nvidia.github.io/libnvidia-container/stable/") {
			t.Errorf("%+v is not a row the generator writes", b)
		}
		key := b.Version + "/" + b.Arch + "/" + b.Packaging
		if seen[key] {
			t.Errorf("%s is listed twice", key)
		}
		seen[key] = true
	}
}

// TestKnownHookBuild_ByDigestAndArch: a build is known by its binary's digest
// on its own arch, and no other.
func TestKnownHookBuild_ByDigestAndArch(t *testing.T) {
	b := cdiHookTable[0]
	if got, ok := knownHookBuild(b.BinarySHA256, b.Arch); !ok || got != b {
		t.Fatalf("knownHookBuild(%s, %s) = %+v, %v", b.BinarySHA256, b.Arch, got, ok)
	}
	other := map[string]string{"amd64": "arm64", "arm64": "amd64"}[b.Arch]
	if _, ok := knownHookBuild(b.BinarySHA256, other); ok {
		t.Fatal("a build was known on another arch")
	}
	if _, ok := knownHookBuild(strings.Repeat("0", 64), b.Arch); ok {
		t.Fatal("an unknown digest was known")
	}
}

func releaseOf(t *testing.T, s string) [3]int {
	t.Helper()
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		t.Fatalf("%q is not MAJOR.MINOR.PATCH", s)
	}
	var v [3]int
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			t.Fatalf("%q is not MAJOR.MINOR.PATCH", s)
		}
		v[i] = n
	}
	return v
}

func compareRelease(a, b [3]int) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}
