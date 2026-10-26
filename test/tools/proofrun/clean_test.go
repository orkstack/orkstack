package main

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// fakeCleanStore holds buckets; it lists only those in listed (SeaweedFS 4.48
// with its auth off lists none) and records what it removed.
type fakeCleanStore struct {
	mu      sync.Mutex
	held    map[string]bool
	listed  []string
	removed []string
}

func (f *fakeCleanStore) List(context.Context) ([]string, error) { return f.listed, nil }

func (f *fakeCleanStore) Exists(_ context.Context, bucket string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.held[bucket], nil
}

func (f *fakeCleanStore) Remove(_ context.Context, bucket string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.held, bucket)
	f.removed = append(f.removed, bucket)
	return nil
}

// fakeCleanDocker lists containers and proof Temporals and records what it
// removed and stopped.
type fakeCleanDocker struct {
	containers [][2]string
	temporals  [][2]string
	removed    []string
	stopped    []string
}

func (f *fakeCleanDocker) Containers(context.Context) ([][2]string, error) { return f.containers, nil }
func (f *fakeCleanDocker) ProofTemporals(context.Context) ([][2]string, error) {
	return f.temporals, nil
}
func (f *fakeCleanDocker) Remove(_ context.Context, id string) error {
	f.removed = append(f.removed, id)
	return nil
}
func (f *fakeCleanDocker) Stop(_ context.Context, name string) error {
	f.stopped = append(f.stopped, name)
	return nil
}

func write(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func sorted(s []string) []string {
	out := append([]string(nil), s...)
	slices.Sort(out)
	return out
}

// The host: pid 40 is a running proof (start time 900), pid 41 a live test
// process of it, pid 42 a dead test process that running proof recorded, pid
// 50 a live scripts/integration.sh run, and pids 11-19 dead test processes of
// proofs that were stopped or killed. Everything a dead process left is
// removed; nothing of a live process, nothing a running proof recorded,
// nothing whose owner cannot be told, and never the development bucket.
func TestClean_RemovesOnlyWhatDeadProcessesLeft(t *testing.T) {
	wt := t.TempDir()
	other := t.TempDir() // a second worktree of the repository
	host := "builder"

	// A proof a signal stopped: a passing process's hand-off file its sweep
	// never reached, and a process killed mid-test, named only at its log's head.
	write(t, filepath.Join(wt, "proof", "stopped", "proof.pid"), "31 31 700 pr-aaa\n")
	write(t, filepath.Join(wt, "proof", "stopped", "integration", "engine-001.bucket"), "orkstack-engine-11-1\n")
	write(t, filepath.Join(wt, "proof", "stopped", "integration", "engine-002.log"),
		"s3bucket: this test binary's object store is s3://orkstack-engine-12-2 (http://x), created in 1s\n=== RUN TestX\n")
	// A failed proof in another worktree: its failing process kept its bucket.
	write(t, filepath.Join(other, "proof", "failed", "proof.pid"), "32 32 701 pr-bbb\n")
	write(t, filepath.Join(other, "proof", "failed", "integration", "orker-003.log"),
		"s3bucket: this test binary's object store is s3://orkstack-orker-13-3 (http://x), created in 1s\n--- FAIL: TestY\n")
	// An old proof whose buckets are already gone.
	write(t, filepath.Join(wt, "proof", "old", "proof.pid"), "33 33 702 pr-ccc\n")
	write(t, filepath.Join(wt, "proof", "old", "integration", "engine-004.bucket"), "orkstack-engine-14-4\n")
	// The running proof: a live process's bucket, and a dead process's bucket
	// it recorded (the sweep has not reached it yet).
	write(t, filepath.Join(wt, "proof", "running", "proof.pid"), "40 40 900 pr-ddd\n")
	write(t, filepath.Join(wt, "proof", "running", "integration", "engine-005.log"),
		"s3bucket: this test binary's object store is s3://orkstack-engine-41-5 (http://x)\n")
	write(t, filepath.Join(wt, "proof", "running", "integration", "engine-006.bucket"), "orkstack-engine-42-6\n")
	// A live integration.sh run, and a dead one.
	write(t, filepath.Join(wt, ".data", "integration", "20261026T000000Z-50", "integration", "api-001.log"),
		"s3bucket: this test binary's object store is s3://orkstack-api-51-7 (http://x)\n")
	write(t, filepath.Join(wt, ".data", "integration", "20261026T000000Z-60", "integration", "api-001.log"),
		"s3bucket: this test binary's object store is s3://orkstack-api-15-8 (http://x)\n")

	// Work roots: dead, live, recorded by the running proof, no pid, the dev
	// stack's own.
	for _, d := range []string{"chaos-orker-a-p16-1", "pin-affinity-p41-2", "cli-p42-3", "e2e", "ork-spool", "ork-pin-x"} {
		if err := os.MkdirAll(filepath.Join(wt, ".data", "work", d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	deadRoot := filepath.Join(wt, ".data", "work", "chaos-orker-a-p16-1")
	liveRoot := filepath.Join(wt, ".data", "work", "pin-affinity-p41-2")

	store := &fakeCleanStore{held: map[string]bool{
		"orkstack-artifacts":   true,
		"orkstack-engine-11-1": true,
		"orkstack-engine-12-2": true,
		"orkstack-orker-13-3":  true,
		"orkstack-engine-41-5": true,
		"orkstack-engine-42-6": true,
		"orkstack-api-51-7":    true,
		"orkstack-api-15-8":    true,
		"orkstack-ork-19-9":    true, // no proof here recorded it; a listing store shows it
	}, listed: []string{"orkstack-artifacts", "orkstack-ork-19-9"}}
	docker := &fakeCleanDocker{
		containers: [][2]string{
			{"c-proof-dead", "soak-pr-aaa-17"},
			{"c-proof-live", "soak-pr-ddd-41"},
			{"c-hash-dead", hashedOrkerID(host, deadRoot)},
			{"c-hash-live", hashedOrkerID(host, liveRoot)},
			{"c-unknown", "join-i8"},
			{"c-dev", hashedOrkerID(host, filepath.Join(wt, ".data", "work"))},
		},
		temporals: [][2]string{
			{"orkstack-proof-pr-gone-temporal", host + ":18:500"},
			{"orkstack-proof-pr-ddd-temporal", host + ":40:900"},
			{"orkstack-proof-pr-elsewhere-temporal", "other-host:18:500"},
		},
	}
	live := map[int]bool{40: true, 41: true, 50: true}
	starts := map[int]string{40: "900", 41: "910", 50: "950"}

	for _, remove := range []bool{false, true} {
		var out strings.Builder
		c := &cleaner{
			worktrees: []string{wt, other},
			host:      host,
			alive:     func(pid int) bool { return live[pid] },
			procStart: func(pid int) string { return starts[pid] },
			store:     store,
			docker:    docker,
			remove:    remove,
			out:       &out,
		}
		n := c.run(context.Background())

		if want := (cleanCounts{buckets: 5, workRoots: 1, containers: 3}); n != want {
			t.Errorf("remove=%v: counts = %+v, want %+v\n%s", remove, n, want, out.String())
		}
		summary := "cleanup: would remove 5 buckets, 1 work roots, 3 containers of dead proofs\n"
		if remove {
			summary = "cleanup: removed 5 buckets, 1 work roots, 3 containers of dead proofs\n"
		}
		if !strings.HasSuffix(out.String(), summary) {
			t.Errorf("remove=%v: output does not end with %q:\n%s", remove, summary, out.String())
		}
		for _, want := range []string{
			"  c-proof-live of Orker soak-pr-ddd-41  (in use: pid 41 is alive; left alone)",
			"(in use: a running proof recorded pid 42; left alone)",
			"(its name records no owning pid; not removed here",
			// join-i8, and the dev stack's own Orker, whose root names no pid.
			"  2 containers whose owner cannot be told",
		} {
			if !strings.Contains(out.String(), want) {
				t.Errorf("remove=%v: output is missing %q:\n%s", remove, want, out.String())
			}
		}
		// A running proof's and a live run's records only protect: their
		// buckets are never candidates, so never named.
		for _, never := range []string{"orkstack-artifacts", "orkstack-engine-14-4", "orkstack-engine-41-5",
			"orkstack-engine-42-6", "orkstack-api-51-7", "ork-spool", "ork-pin-x"} {
			if strings.Contains(out.String(), never) {
				t.Errorf("remove=%v: output names %s, which is no leftover:\n%s", remove, never, out.String())
			}
		}
		if !remove {
			if len(store.removed) != 0 || len(docker.removed) != 0 || len(docker.stopped) != 0 {
				t.Fatalf("a listing removed something: buckets %q, containers %q, stopped %q", store.removed, docker.removed, docker.stopped)
			}
			if _, err := os.Stat(deadRoot); err != nil {
				t.Fatalf("a listing removed a work root: %v", err)
			}
		}
	}

	if got, want := sorted(store.removed), []string{
		"orkstack-api-15-8", "orkstack-engine-11-1", "orkstack-engine-12-2", "orkstack-ork-19-9", "orkstack-orker-13-3",
	}; !slices.Equal(got, want) {
		t.Errorf("removed buckets %q, want %q", got, want)
	}
	if got, want := sorted(docker.removed), []string{"c-hash-dead", "c-proof-dead"}; !slices.Equal(got, want) {
		t.Errorf("removed containers %q, want %q", got, want)
	}
	if got, want := docker.stopped, []string{"orkstack-proof-pr-gone-temporal"}; !slices.Equal(got, want) {
		t.Errorf("stopped %q, want %q", got, want)
	}
	if _, err := os.Stat(deadRoot); !os.IsNotExist(err) {
		t.Errorf("the dead process's work root is still there: %v", err)
	}
	for _, kept := range []string{liveRoot, filepath.Join(wt, ".data", "work", "cli-p42-3"), filepath.Join(wt, ".data", "work", "e2e"),
		filepath.Join(wt, ".data", "work", "ork-spool")} {
		if _, err := os.Stat(kept); err != nil {
			t.Errorf("%s was removed: %v", kept, err)
		}
	}
}

// Without an object store or a container runtime the cleanup says so, does
// what it can, and still ends with its summary line.
func TestClean_WithoutAStoreOrARuntimeSaysSo(t *testing.T) {
	var out strings.Builder
	c := &cleaner{
		worktrees: []string{t.TempDir()},
		alive:     func(int) bool { return false },
		procStart: func(int) string { return "" },
		remove:    true,
		out:       &out,
	}
	if n := c.run(context.Background()); n != (cleanCounts{}) {
		t.Errorf("counts = %+v, want none", n)
	}
	for _, want := range []string{
		"  (object storage not reachable; skipped)",
		"  (no container runtime; skipped)",
		"cleanup: removed 0 buckets, 0 work roots, 0 containers of dead proofs\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output is missing %q:\n%s", want, out.String())
		}
	}
}
