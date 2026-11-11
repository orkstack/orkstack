package engine_test

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/orkstack/orkstack/internal/engine"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

const (
	// runIDHelperEnv switches this test binary into its child role.
	runIDHelperEnv = "ORK_RUNID_HELPER"
	// runIDHelperIDs is how many ids each child prints.
	runIDHelperIDs = 5000
	// runIDHelperProcesses is how many children run concurrently. Eight is
	// enough that a per-process counter overlaps ~40 000 times, which is what
	// makes the failure unmissable rather than probabilistic.
	runIDHelperProcesses = 8
)

// runIDShape is the documented id shape: `ork-` and a UUIDv7 in base32.
var runIDShape = regexp.MustCompile(`^ork-[0-7][0-9a-hjkmnp-tv-z]{25}$`)

// TestRunIDHelperProcess is the child half of
// TestNewRunID_UniqueAcrossSeparateProcesses. Under `go test` it is skipped;
// re-exec'd with ORK_RUNID_HELPER=1 it prints runIDHelperIDs ids and exits.
//
// A child is a genuinely separate process, which is the point: goroutines in
// one process share the generator, so only a fresh process can show what two
// engine replicas do to each other.
func TestRunIDHelperProcess(t *testing.T) {
	if os.Getenv(runIDHelperEnv) != "1" {
		t.Skip("child role; runs only when re-exec'd by TestNewRunID_UniqueAcrossSeparateProcesses")
	}
	out := bufio.NewWriter(os.Stdout)
	for range runIDHelperIDs {
		fmt.Fprintln(out, engine.NewRunID())
	}
	must.OKf(t, out.Flush(), "flush ids")
}

// TestNewRunID_UniqueAcrossSeparateProcesses is the cross-replica uniqueness
// test: run ids must be unique across engine replicas, not merely within one
// process. It re-execs this binary runIDHelperProcesses times, concurrently,
// and requires every id from every child to be distinct.
func TestNewRunID_UniqueAcrossSeparateProcesses(t *testing.T) {
	binary, err := os.Executable()
	must.OKf(t, err, "locate this test binary")

	perChild := make([][]string, runIDHelperProcesses)
	failures := make([]error, runIDHelperProcesses)
	var wg sync.WaitGroup
	for i := range runIDHelperProcesses {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ids, err := runRunIDHelper(binary)
			perChild[i], failures[i] = ids, err
		}(i)
	}
	wg.Wait()

	for i, err := range failures {
		must.OKf(t, err, "child %d", i)
	}

	seen := make(map[string]int, runIDHelperProcesses*runIDHelperIDs)
	duplicates := 0
	var sample string
	for child, ids := range perChild {
		if len(ids) != runIDHelperIDs {
			t.Fatalf("child %d printed %d ids matching %v, want %d",
				child, len(ids), runIDShape, runIDHelperIDs)
		}
		for _, id := range ids {
			seen[id]++
			if seen[id] == 2 {
				duplicates++
				if sample == "" {
					sample = id
				}
			}
		}
	}
	if duplicates != 0 {
		t.Errorf("%d of %d ids collided across %d processes (e.g. %q); "+
			"a run id is the Temporal workflow id, so a collision fails or hijacks a run",
			duplicates, runIDHelperProcesses*runIDHelperIDs, runIDHelperProcesses, sample)
	}
}

// runRunIDHelper re-execs this binary in its child role and returns the ids it
// printed.
func runRunIDHelper(binary string) ([]string, error) {
	cmd := exec.Command(binary, "-test.run=^TestRunIDHelperProcess$")
	cmd.Env = append(os.Environ(), runIDHelperEnv+"=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("re-exec %s: %w: %s", binary, err, stderr.String())
	}

	var ids []string
	for _, line := range strings.Split(stdout.String(), "\n") {
		line = strings.TrimSpace(line)
		// The test framework writes its own lines (PASS, coverage) to the same
		// stream; only candidate ids are considered, and a malformed one is
		// reported rather than silently dropped.
		if !strings.HasPrefix(line, "ork") {
			continue
		}
		if !runIDShape.MatchString(line) {
			return nil, fmt.Errorf("printed %q, want the documented shape %s", line, runIDShape)
		}
		ids = append(ids, line)
	}
	return ids, nil
}
