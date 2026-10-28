package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// The fuzz lane (AGENTS.md §11.2, docs/PROOFS.md §2.3): every Fuzz target of
// the module, each for a bounded time, in the nightly tier. A target the
// engine cannot break passes; a crasher fails the lane, and the input that
// broke it is kept under the lane's output directory, so a nightly's artifact
// carries what a person needs to reproduce it. The engine writes a crasher
// into the fuzzed package's testdata/fuzz/<Target>/ — a file that would turn
// the proof's tree into one it did not start with — so the runner moves it out.

// fuzzWorkDir is the directory the lane's go commands run in; empty is the
// process's own. Tests point it at a throwaway module, because a crasher is
// written into the tree that was fuzzed.
var fuzzWorkDir string

// defaultFuzzTime is how long each target fuzzes when -fuzztime is not given:
// FUZZTIME, or two minutes.
func defaultFuzzTime() string {
	if v := os.Getenv("FUZZTIME"); v != "" {
		return v
	}
	return "2m"
}

var fuzzIterations = regexp.MustCompile(`^[1-9][0-9]*x$`)

// checkFuzzTime accepts what go test -fuzztime accepts and the lane can bound:
// a positive duration, or an iteration count such as 100x.
func checkFuzzTime(s string) error {
	if fuzzIterations.MatchString(s) {
		return nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("-fuzztime %q is neither a duration (2m, 30s) nor an iteration count (100x)", s)
	}
	if d <= 0 {
		return fmt.Errorf("-fuzztime %q must be positive", s)
	}
	return nil
}

// fuzzDeadline bounds one target's whole run: the fuzzing itself, the build
// that precedes it and the minimization that follows a crasher.
func fuzzDeadline(fuzztime string) time.Duration {
	if d, err := time.ParseDuration(fuzztime); err == nil {
		return d + 10*time.Minute
	}
	return 30 * time.Minute
}

// parseFuzzTargets reads `go test -json -list ^Fuzz` output: one entry per
// Fuzz function of each package, in listing order, once.
func parseFuzzTargets(out []byte) []testKey {
	_, all := parseJSONListing(out)
	var targets []testKey
	for _, k := range all {
		if strings.HasPrefix(k.Test, "Fuzz") {
			targets = append(targets, k)
		}
	}
	return targets
}

// fuzzSlug names a package for a file path.
func fuzzSlug(pkg string) string {
	return strings.NewReplacer("/", "_", ".", "_", ":", "_").Replace(pkg)
}

// fuzzCorpus is the files of a target's testdata/fuzz directory.
func fuzzCorpus(dir string) map[string]bool {
	files := map[string]bool{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return files
	}
	for _, e := range entries {
		if !e.IsDir() {
			files[e.Name()] = true
		}
	}
	return files
}

// cmdFuzz is `proofrun fuzz`: the fuzz lane.
func cmdFuzz(args []string) int {
	fs := flag.NewFlagSet("fuzz", flag.ContinueOnError)
	out := fs.String("out", "", "directory for the ledger, each target's log and the crashers (required)")
	fuzztime := fs.String("fuzztime", defaultFuzzTime(), "how long each target fuzzes (go test -fuzztime: a duration or Nx); the FUZZTIME environment variable sets the default, 2m without it")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" {
		fmt.Fprintln(os.Stderr, "proofrun fuzz: -out is required")
		return 2
	}
	if err := checkFuzzTime(*fuzztime); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun fuzz:", err)
		return 2
	}
	patterns := fs.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	if err := os.MkdirAll(filepath.Join(*out, "fuzz"), 0o755); err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}

	targets, dirs, err := discoverFuzzTargets(patterns)
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	l := ledger{Suite: suiteFuzz, Problems: []string{}}
	if len(targets) == 0 {
		l.Problems = append(l.Problems, "no Fuzz target found in "+strings.Join(patterns, " ")+": a lane that fuzzed nothing proves nothing")
	}
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Package != targets[j].Package {
			return targets[i].Package < targets[j].Package
		}
		return targets[i].Test < targets[j].Test
	})
	for _, k := range targets {
		e := fuzzTarget(k, dirs[k.Package], *fuzztime, *out)
		l.Summary.Expected++
		switch e.Outcome {
		case outcomePass:
			l.Summary.Pass++
		default:
			l.Summary.Fail++
			l.Problems = append(l.Problems, fmt.Sprintf("%s failed: %s", k, e.Reason))
		}
		fmt.Fprintf(stdout, "fuzz: %s: %s (%.0fs)\n", k, e.Outcome, e.Seconds)
		l.Tests = append(l.Tests, e)
	}
	return writeLedgerAndReport(*out, l)
}

// discoverFuzzTargets lists the Fuzz functions of the packages and the
// directory of each package that has one.
func discoverFuzzTargets(patterns []string) ([]testKey, map[string]string, error) {
	list := exec.Command("go", append([]string{"test", "-json", "-list", "^Fuzz"}, patterns...)...)
	list.Dir = fuzzWorkDir
	var listErr bytes.Buffer
	list.Stderr = &listErr
	listOut, err := list.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("go test -list: %v\n%s%s", err, listOut, listErr.String())
	}
	targets := parseFuzzTargets(listOut)
	dirsCmd := exec.Command("go", append([]string{"list", "-f", "{{.ImportPath}}\t{{.Dir}}"}, patterns...)...)
	dirsCmd.Dir = fuzzWorkDir
	var dirsErr bytes.Buffer
	dirsCmd.Stderr = &dirsErr
	dirsOut, err := dirsCmd.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("go list: %v\n%s", err, dirsErr.String())
	}
	dirs := map[string]string{}
	for _, line := range strings.Split(string(dirsOut), "\n") {
		if pkg, dir, ok := strings.Cut(strings.TrimSpace(line), "\t"); ok {
			dirs[pkg] = dir
		}
	}
	return targets, dirs, nil
}

// fuzzTarget fuzzes one target and reports it as a ledger entry. Its output
// goes to <out>/fuzz/<package>.<target>.log; a crasher the engine wrote into
// the package is copied to <out>/crashers/<package>/<target>/ and removed.
func fuzzTarget(k testKey, dir, fuzztime, out string) entry {
	e := entry{Package: k.Package, Test: k.Test, Job: suiteFuzz, Outcome: outcomePass}
	logPath := filepath.Join(out, "fuzz", fuzzSlug(k.Package)+"."+k.Test+".log")
	if dir == "" {
		// Without the directory the crasher cannot be found, and a lane that
		// cannot keep one must not report a pass.
		e.Outcome, e.Reason = outcomeFail, "go list gave no directory for its package"
		return e
	}
	corpus := filepath.Join(dir, "testdata", "fuzz", k.Test)
	before := fuzzCorpus(corpus)

	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), fuzzDeadline(fuzztime))
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "-run", "^$", "-fuzz", "^"+k.Test+"$", "-fuzztime", fuzztime, k.Package)
	cmd.Dir = fuzzWorkDir
	logFile, err := os.Create(logPath)
	if err != nil {
		e.Outcome, e.Reason = outcomeFail, "cannot create its log: "+err.Error()
		return e
	}
	defer logFile.Close()
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.WaitDelay = 30 * time.Second
	runErr := cmd.Run()
	e.Seconds = time.Since(started).Seconds()

	var crashers []string
	for name := range fuzzCorpus(corpus) {
		if !before[name] {
			crashers = append(crashers, name)
		}
	}
	sort.Strings(crashers)
	var kept []string
	for _, name := range crashers {
		dst, err := keepCrasher(filepath.Join(corpus, name), filepath.Join(out, "crashers", fuzzSlug(k.Package), k.Test, name))
		if err != nil {
			e.Outcome = outcomeFail
			e.Reason = "a crasher was found and could not be kept: " + err.Error()
			return e
		}
		kept = append(kept, dst)
	}
	if len(crashers) > 0 {
		// Only what this run made: the directories go if they are empty.
		for _, d := range []string{corpus, filepath.Dir(corpus), filepath.Dir(filepath.Dir(corpus))} {
			_ = os.Remove(d)
		}
	}
	switch {
	case ctx.Err() != nil:
		e.Outcome = outcomeFail
		e.Reason = fmt.Sprintf("the target did not finish within %s of its %s", fuzzDeadline(fuzztime), fuzztime)
	case len(kept) > 0:
		e.Outcome = outcomeFail
		e.Reason = fmt.Sprintf("crasher kept at %s (the log is %s)", strings.Join(kept, ", "), logPath)
	case runErr != nil:
		e.Outcome = outcomeFail
		e.Reason = fmt.Sprintf("go test -fuzz: %v (the log is %s)", runErr, logPath)
	}
	return e
}

// keepCrasher copies src to dst and removes src.
func keepCrasher(src, dst string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	f, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, in); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(src); err != nil {
		return "", err
	}
	return dst, nil
}
