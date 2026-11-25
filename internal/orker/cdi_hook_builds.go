package orker

// The builds of NVIDIA's nvidia-cdi-hook an Orker accepts behind an
// nvidia.com CDI spec's hook (design E91 §4.10 item 4). The table itself,
// cdi_hook_table.go, is generated from NVIDIA's signed package repository by
// `go run ./internal/orker/cmd/gencdihooks`, never edited by hand; its -check
// is for the nightly cdi-hook-table job P4b's workflow change adds. The Orker never
// executes the binary: it hashes it, at boot and on every audit, and a digest
// that is no row of the table disables the gpu profile alone.

// cdiHookBuild is one row of cdiHookTable.
type cdiHookBuild struct {
	// Version is the toolkit release, "1.20.2".
	Version string
	// Arch is the Orker's spelling of the build's arch: "amd64" or "arm64".
	Arch string
	// Packaging is "deb" or "rpm": NVIDIA's two builds of one release differ.
	Packaging string
	// Package is the package's URL in NVIDIA's repository.
	Package string
	// PackageDigest is the digest the signed metadata gives of the package.
	PackageDigest string
	// BinarySHA256 is the SHA-256 of the nvidia-cdi-hook it installs.
	BinarySHA256 string
}

// knownHookBuild is the row whose binary has digest sha256hex for arch, or
// false: an unknown build (distro-patched, self-built, below the floor, or
// newer than the table).
func knownHookBuild(sha256hex, arch string) (cdiHookBuild, bool) {
	for _, b := range cdiHookTable {
		if b.BinarySHA256 == sha256hex && b.Arch == arch {
			return b, true
		}
	}
	return cdiHookBuild{}, false
}
