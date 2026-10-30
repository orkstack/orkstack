package dist

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"time"
)

// LicenseFile is the licence every archive carries, at the repository root.
const LicenseFile = "LICENSE"

// UIIndex is the page every route of the web UI boots from, in the build
// `make ui` writes for ork to embed (internal/webui), as a path from the
// repository root. Without it, ork compiles and serves a placeholder page in
// the UI's place, which a release must never ship.
const UIIndex = "internal/webui/dist/index.html"

// CheckUI refuses a repository root that holds no web UI build for ork to
// embed: no UIIndex, or one that is not a regular file. `make ui` writes the
// build, and `make dist` runs it first.
func CheckUI(root string) error {
	info, err := os.Stat(filepath.Join(root, filepath.FromSlash(UIIndex)))
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("no web UI build at %s: a release's ork embeds the UI and would serve a placeholder "+
			"page in its place; run `make ui` first (`make dist` does)", UIIndex)
	case err != nil:
		return fmt.Errorf("read the web UI build: %w", err)
	case !info.Mode().IsRegular():
		return fmt.Errorf("%s is not a regular file, so it is no web UI build; run `make ui` (`make dist` does)", UIIndex)
	}
	return nil
}

// Builder compiles one target from the module at root into the file out,
// stamping it with version. GoBuilder is the real one; tests substitute a fake
// so that the layout of a release can be checked without a ten-binary compile.
type Builder func(ctx context.Context, root string, target Target, version, out string) error

// GoBuilder builds target with `go build`, from root, with the flags and
// environment Target.BuildArgs and Target.BuildEnv give: static, trimmed,
// the time zone database linked in, and the version stamped.
func GoBuilder(ctx context.Context, root string, target Target, version, out string) error {
	cmd := exec.CommandContext(ctx, "go", target.BuildArgs(version, out)...)
	cmd.Dir = root
	// Later entries win, so the target's platform overrides the caller's.
	cmd.Env = append(os.Environ(), target.BuildEnv()...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("go build: %w\n%s", err, bytes.TrimSpace(output))
	}
	return nil
}

// BuildOptions say what Build builds, from where, and where it puts it.
type BuildOptions struct {
	// Root is the repository root: the module Builder compiles and where
	// LICENSE is read from.
	Root string
	// Out is the release directory. Archives go directly in it; the linux
	// binaries the images copy go under bin/linux-<arch>/.
	Out string
	// Version is stamped into every binary.
	Version Version
	// CommitTime stamps every archive entry. It is the commit's time, never
	// the clock, so the same commit gives the same archives.
	CommitTime time.Time
	// Builder compiles a target. Nil means GoBuilder.
	Builder Builder
}

// Build compiles every release target, packs each into its archive and writes
// the archives to opts.Out. The linux binaries are also written, unpacked, to
// opts.Out/bin/linux-<arch>/<name>, which is where the container images copy
// them from. It returns the archive names written, sorted. It compiles
// nothing from a root without the web UI build (CheckUI), and a build that
// fails stops at the first failing target and names it.
func Build(ctx context.Context, opts BuildOptions) ([]string, error) {
	if opts.CommitTime.IsZero() {
		return nil, errors.New("no commit time: archives are stamped with the commit's time, never the clock " +
			"(pass -commit-time, or set SOURCE_DATE_EPOCH)")
	}
	build := opts.Builder
	if build == nil {
		build = GoBuilder
	}
	license, err := os.ReadFile(filepath.Join(opts.Root, LicenseFile))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", LicenseFile, err)
	}
	if err := CheckUI(opts.Root); err != nil {
		return nil, err
	}
	scratch, err := os.MkdirTemp("", "orkstack-dist-")
	if err != nil {
		return nil, fmt.Errorf("make a build directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(scratch) }()
	if err := os.MkdirAll(opts.Out, 0o755); err != nil {
		return nil, fmt.Errorf("make the release directory: %w", err)
	}

	var assets []string
	for _, target := range Targets() {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("stopped before %s: %w", target, err)
		}
		built := filepath.Join(scratch, target.String(), target.Executable())
		if err := os.MkdirAll(filepath.Dir(built), 0o755); err != nil {
			return nil, err
		}
		if err := build(ctx, opts.Root, target, opts.Version.String(), built); err != nil {
			return nil, fmt.Errorf("build %s: %w", target, err)
		}
		binary, err := os.ReadFile(built)
		if err != nil {
			return nil, fmt.Errorf("build %s: read the binary: %w", target, err)
		}
		asset, err := Pack(target, binary, license, opts.CommitTime)
		if err != nil {
			return nil, err
		}
		if err := writeFile(filepath.Join(opts.Out, asset.Name), asset.Data, 0o644); err != nil {
			return nil, fmt.Errorf("%s: %w", target, err)
		}
		assets = append(assets, asset.Name)
		if target.GOOS == "linux" {
			dir := filepath.Join(opts.Out, "bin", "linux-"+target.GOARCH)
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, err
			}
			if err := writeFile(filepath.Join(dir, target.Executable()), binary, 0o755); err != nil {
				return nil, fmt.Errorf("%s: %w", target, err)
			}
		}
	}
	slices.Sort(assets)
	return assets, nil
}

// writeFile writes data to path and sets its mode, whatever mode a file left
// there by an earlier run had.
func writeFile(path string, data []byte, mode os.FileMode) error {
	if err := os.WriteFile(path, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("set the mode of %s: %w", filepath.Base(path), err)
	}
	return nil
}

var epochSeconds = regexp.MustCompile(`^[0-9]+$`)

// earliestCommitTime is the first instant a zip entry's MS-DOS date can say.
// A commit time before it is a mistake, such as the 0 of a failed `git log`,
// and a zip would show it as a different year.
var earliestCommitTime = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)

// ParseCommitTime reads a commit time as whole seconds since the Unix epoch,
// which is what SOURCE_DATE_EPOCH and `git log -1 --format=%ct` give, or as an
// RFC 3339 timestamp. The result is in UTC, and never before 1980.
func ParseCommitTime(s string) (time.Time, error) {
	var t time.Time
	if epochSeconds.MatchString(s) {
		seconds, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return time.Time{}, fmt.Errorf("commit time %q is out of range: %w", s, err)
		}
		t = time.Unix(seconds, 0).UTC()
	} else {
		parsed, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return time.Time{}, fmt.Errorf("commit time %q is neither seconds since the epoch nor an RFC 3339 timestamp", s)
		}
		t = parsed.UTC()
	}
	if t.Before(earliestCommitTime) {
		return time.Time{}, fmt.Errorf("commit time %q is before 1980, which no commit is and a zip cannot record; is git available?", s)
	}
	return t, nil
}
