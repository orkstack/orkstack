// Package dist describes, builds and packs one release's artifacts: which
// binaries are published for which platforms, how they are built, and how they
// are archived and checksummed so that the same commit always produces the same
// bytes. It also names the images a release publishes (images.go), reads the
// digests of their OCI layouts, and packs the quickstart bundle that names
// them (quickstart.go). cmd/dist (internal/dist/cmd/dist) is its only caller.
//
// It publishes nothing and talks to no registry. release.yml owns publishing.
package dist

import "slices"

// The three binaries a release builds. Each is the command under cmd/.
const (
	// Ork is the engine.
	Ork = "ork"
	// Orker is the activity worker.
	Orker = "orker"
	// Orkctl is the command-line client.
	Orkctl = "orkctl"
)

// BuildTags are the Go build tags every release binary is built with.
// timetzdata links the time zone database into the binary, because the
// release images have no zoneinfo files and schedules, automation documents
// and the API load named time zones.
const BuildTags = "timetzdata"

// Target is one binary built for one platform.
type Target struct {
	// Binary is the command's name: ork, orker or orkctl.
	Binary string
	// GOOS and GOARCH are the platform, in Go's words.
	GOOS   string
	GOARCH string
}

// matrix is the release matrix: orkctl runs on a person's machine, so it is
// published for linux, darwin and windows; ork and orker run in containers,
// so they are published for linux only. Every one is built for amd64 and
// arm64.
var matrix = []Target{
	{Ork, "linux", "amd64"},
	{Ork, "linux", "arm64"},
	{Orker, "linux", "amd64"},
	{Orker, "linux", "arm64"},
	{Orkctl, "linux", "amd64"},
	{Orkctl, "linux", "arm64"},
	{Orkctl, "darwin", "amd64"},
	{Orkctl, "darwin", "arm64"},
	{Orkctl, "windows", "amd64"},
	{Orkctl, "windows", "arm64"},
}

// Targets returns the release matrix. The caller owns the returned slice.
func Targets() []Target {
	return slices.Clone(matrix)
}

// String is the target as its archive names it, without the extension:
// "orkctl-windows-arm64".
func (t Target) String() string {
	return t.Binary + "-" + t.GOOS + "-" + t.GOARCH
}

// Package is the main package that builds the binary, relative to the
// repository root.
func (t Target) Package() string {
	return "./cmd/" + t.Binary
}

// BuildEnv is the environment a release build adds to the caller's: a static
// binary, for this platform. It also fixes every variable that would change
// the binary's bytes, at Go's own defaults, so a caller's GOFLAGS, GOAMD64 or
// GOEXPERIMENT never reaches a release.
func (t Target) BuildEnv() []string {
	return []string{
		"CGO_ENABLED=0", "GOOS=" + t.GOOS, "GOARCH=" + t.GOARCH,
		"GOFLAGS=", "GOEXPERIMENT=", "GOAMD64=v1", "GOARM64=v8.0",
	}
}

// BuildArgs are the arguments to `go`, run from the repository root, that
// build the target into out and stamp it with version, which is what
// `orkctl version` and the engine's admin surface report. -trimpath keeps the
// build directory out of the binary, and -buildvcs=false keeps the checkout's
// state out of it, so a tree with an untracked file, or a source archive with
// no .git, builds the same bytes as the tagged checkout. Symbols are not
// stripped.
func (t Target) BuildArgs(version, out string) []string {
	return []string{
		"build", "-trimpath", "-buildvcs=false", "-tags", BuildTags,
		"-ldflags", "-X main.version=" + version,
		"-o", out, t.Package(),
	}
}

// isWindows reports whether the target's executable carries the .exe suffix
// and its archive is a zip.
func (t Target) isWindows() bool {
	return t.GOOS == "windows"
}

// Executable is the file name of the built binary, inside its archive and in
// the directory the images copy from: "orkctl", or "orkctl.exe" on Windows.
func (t Target) Executable() string {
	if t.isWindows() {
		return t.Binary + ".exe"
	}
	return t.Binary
}

// Asset is the name of the archive the release publishes for the target. It
// carries no version, so https://github.com/<repo>/releases/latest/download/<name>
// keeps working from one release to the next: the binary, the platform, and
// .zip for Windows or .tar.gz for everything else.
func (t Target) Asset() string {
	if t.isWindows() {
		return t.String() + ".zip"
	}
	return t.String() + ".tar.gz"
}
