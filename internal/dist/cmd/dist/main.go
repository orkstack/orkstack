// Command dist builds, checks and describes one OrkStack release's artifacts
// (internal/dist). Run it from the repository root:
//
//	go run ./internal/dist/cmd/dist check -version v0.1.0
//	git tag --list >tags && go run ./internal/dist/cmd/dist order -version v0.1.0 -tags tags
//	go run ./internal/dist/cmd/dist build -version v0.1.0 -out dist
//	go run ./internal/dist/cmd/dist sums dist
//	go run ./internal/dist/cmd/dist notes -version v0.1.0
//	go run ./internal/dist/cmd/dist digests -layouts dist/layouts
//
// It publishes nothing: release.yml uploads what it writes. Exit status: 0
// done, 1 the work failed (a refused release, a compile error), 2 an invalid
// invocation.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/orkstack/orkstack/internal/dist"
	"github.com/orkstack/orkstack/internal/dsl"
)

func main() {
	os.Exit(run(os.Args[1:], runEnv{
		stdout: os.Stdout, stderr: os.Stderr, getenv: os.Getenv, builder: dist.GoBuilder,
	}))
}

// runEnv is everything run touches outside its arguments, so a test can drive
// it without a compiler, a terminal or the process environment.
type runEnv struct {
	stdout, stderr io.Writer
	getenv         func(string) string
	builder        dist.Builder
}

const usageText = `usage: dist <command> [flags]

commands:
  check -version vX.Y.Z           refuse a version the tree cannot release: the built-in
                                  activity catalog must be at X.Y, and CHANGELOG.md must
                                  have a "## [X.Y.Z]" section and nothing left under
                                  "## [Unreleased]"
  order -version vX.Y.Z -tags FILE
                                  print latest=true or latest=false: whether no tag FILE
                                  lists (one per line) is a higher release; refuse a version
                                  below a released tag of its own X.Y
  build -version vX.Y.Z -out DIR  build every release binary, pack the archives into DIR
                                  and the linux binaries into DIR/bin/linux-<arch>/
  sums DIR                        write DIR/SHA256SUMS for the files in DIR
  notes -version vX.Y.Z           print the "## [X.Y.Z]" section of CHANGELOG.md, without its
                                  heading: the GitHub release's notes, word for word
  digests -layouts DIR            print "<image><TAB><digest>" for each of the ten OCI image layouts
                                  in DIR, sorted: the digest skopeo pushes each under

Run it from the repository root, or name the root with -root.
`

func run(args []string, env runEnv) int {
	if len(args) == 0 {
		fmt.Fprint(env.stderr, usageText)
		return 2
	}
	switch args[0] {
	case "-h", "-help", "--help", "help":
		fmt.Fprint(env.stdout, usageText)
		return 0
	case "check":
		return env.check(args[1:])
	case "order":
		return env.order(args[1:])
	case "build":
		return env.build(args[1:])
	case "sums":
		return env.sums(args[1:])
	case "notes":
		return env.notes(args[1:])
	case "digests":
		return env.digests(args[1:])
	}
	fmt.Fprintf(env.stderr, "dist: unknown command %q\n\n%s", args[0], usageText)
	return 2
}

// flags makes a flag set for one command, reporting to stderr.
func (e runEnv) flags(name string) *flag.FlagSet {
	set := flag.NewFlagSet("dist "+name, flag.ContinueOnError)
	set.SetOutput(e.stderr)
	return set
}

// parse parses args. It returns done=true with the exit status when the
// command should stop: help was asked for, or the arguments are invalid.
func (e runEnv) parse(set *flag.FlagSet, args []string, positional int) (code int, done bool) {
	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0, true
		}
		return 2, true
	}
	if set.NArg() != positional {
		fmt.Fprintf(e.stderr, "dist %s: takes %d argument(s) after its flags, got %d\n", set.Name()[len("dist "):], positional, set.NArg())
		return 2, true
	}
	return 0, false
}

// usageError reports an invalid invocation.
func (e runEnv) usageError(format string, args ...any) int {
	fmt.Fprintf(e.stderr, "dist: "+format+"\n", args...)
	return 2
}

// failure reports a command that ran and could not do its work.
func (e runEnv) failure(err error) int {
	fmt.Fprintf(e.stderr, "dist: %v\n", err)
	return 1
}

// versionFlag registers -version and -root, shared by check, build and notes.
func versionFlags(set *flag.FlagSet) (version, root *string) {
	version = set.String("version", "", "the release, vMAJOR.MINOR.PATCH (required)")
	root = set.String("root", ".", "the repository root")
	return version, root
}

// parseVersion reads -version, which every command except sums requires.
func (e runEnv) parseVersion(raw string) (dist.Version, int) {
	if raw == "" {
		return dist.Version{}, e.usageError("-version is required, for example -version v0.1.0")
	}
	version, err := dist.ParseVersion(raw)
	if err != nil {
		return dist.Version{}, e.usageError("%v", err)
	}
	return version, 0
}

// commitTimeFlag registers build's -commit-time.
func commitTimeFlag(set *flag.FlagSet) *string {
	return set.String("commit-time", "",
		"the commit's time, seconds since the epoch or RFC 3339 (default: $SOURCE_DATE_EPOCH; no clock fallback)")
}

// commitTime reads the flag's value, or SOURCE_DATE_EPOCH when the flag is
// empty. It never falls back to the clock: archives are stamped with the
// commit, so the same commit gives the same bytes.
func (e runEnv) commitTime(flagValue string) (time.Time, int) {
	commit := flagValue
	if commit == "" {
		commit = e.getenv("SOURCE_DATE_EPOCH")
	}
	if commit == "" {
		return time.Time{}, e.usageError("no commit time: pass -commit-time or set SOURCE_DATE_EPOCH " +
			"(git log -1 --format=%%ct prints it); archives are stamped with the commit, never the clock")
	}
	commitTime, err := dist.ParseCommitTime(commit)
	if err != nil {
		return time.Time{}, e.usageError("%v (from -commit-time or SOURCE_DATE_EPOCH)", err)
	}
	return commitTime, 0
}

func (e runEnv) check(args []string) int {
	set := e.flags("check")
	raw, root := versionFlags(set)
	if code, done := e.parse(set, args, 0); done {
		return code
	}
	version, code := e.parseVersion(*raw)
	if code != 0 {
		return code
	}
	if err := dist.CheckCatalog(version, dsl.BuiltinActivityCatalog()); err != nil {
		return e.failure(err)
	}
	changelog, err := os.ReadFile(filepath.Join(*root, dist.ChangelogFile))
	if err != nil {
		return e.failure(fmt.Errorf("read %s: %w", dist.ChangelogFile, err))
	}
	if _, err := dist.ReleaseNotes(changelog, version); err != nil {
		return e.failure(err)
	}
	if err := dist.CheckNothingUnreleased(changelog, version); err != nil {
		return e.failure(err)
	}
	fmt.Fprintf(e.stdout, "%s: the built-in activity catalog is at :%s and %s has its section and nothing unreleased\n",
		version, version.Series(), dist.ChangelogFile)
	return 0
}

func (e runEnv) order(args []string) int {
	set := e.flags("order")
	raw := set.String("version", "", "the release, vMAJOR.MINOR.PATCH (required)")
	tagsFile := set.String("tags", "", "the repository's tags, one per line, as `git tag --list` prints them (required)")
	if code, done := e.parse(set, args, 0); done {
		return code
	}
	version, code := e.parseVersion(*raw)
	if code != 0 {
		return code
	}
	if *tagsFile == "" {
		return e.usageError("-tags is required: the file `git tag --list` wrote")
	}
	data, err := os.ReadFile(*tagsFile)
	if err != nil {
		return e.failure(fmt.Errorf("read the tags: %w", err))
	}
	latest, err := dist.ReleaseOrder(version, strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n"))
	if err != nil {
		return e.failure(err)
	}
	fmt.Fprintf(e.stdout, "latest=%t\n", latest)
	return 0
}

func (e runEnv) notes(args []string) int {
	set := e.flags("notes")
	raw, root := versionFlags(set)
	if code, done := e.parse(set, args, 0); done {
		return code
	}
	version, code := e.parseVersion(*raw)
	if code != 0 {
		return code
	}
	changelog, err := os.ReadFile(filepath.Join(*root, dist.ChangelogFile))
	if err != nil {
		return e.failure(fmt.Errorf("read %s: %w", dist.ChangelogFile, err))
	}
	notes, err := dist.ReleaseNotes(changelog, version)
	if err != nil {
		return e.failure(err)
	}
	fmt.Fprint(e.stdout, notes)
	return 0
}

func (e runEnv) sums(args []string) int {
	set := e.flags("sums")
	if code, done := e.parse(set, args, 1); done {
		return code
	}
	dir := set.Arg(0)
	names, err := dist.WriteSums(dir)
	if err != nil {
		return e.failure(err)
	}
	for _, name := range names {
		fmt.Fprintln(e.stdout, name)
	}
	fmt.Fprintf(e.stdout, "wrote %s for %d files\n", filepath.Join(dir, dist.SumsFile), len(names))
	return 0
}

func (e runEnv) build(args []string) int {
	set := e.flags("build")
	raw, root := versionFlags(set)
	out := set.String("out", "", "the release directory (required)")
	stamp := commitTimeFlag(set)
	if code, done := e.parse(set, args, 0); done {
		return code
	}
	version, code := e.parseVersion(*raw)
	if code != 0 {
		return code
	}
	if *out == "" {
		return e.usageError("-out is required, for example -out dist")
	}
	commitTime, code := e.commitTime(*stamp)
	if code != 0 {
		return code
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	assets, err := dist.Build(ctx, dist.BuildOptions{
		Root: *root, Out: *out, Version: version, CommitTime: commitTime, Builder: e.builder,
	})
	if err != nil {
		return e.failure(err)
	}
	for _, name := range assets {
		fmt.Fprintln(e.stdout, name)
	}
	fmt.Fprintf(e.stdout, "built %s: %d archives in %s, linux binaries in %s\n",
		version, len(assets), *out, filepath.Join(*out, "bin"))
	return 0
}

func (e runEnv) digests(args []string) int {
	set := e.flags("digests")
	layouts := set.String("layouts", "", "the directory of OCI image layouts, one per image (required)")
	if code, done := e.parse(set, args, 0); done {
		return code
	}
	if *layouts == "" {
		return e.usageError("-layouts is required, for example -layouts dist/layouts")
	}
	images, err := dist.ReadLayouts(*layouts)
	if err != nil {
		return e.failure(err)
	}
	fmt.Fprint(e.stdout, dist.FormatImages(images))
	return 0
}
