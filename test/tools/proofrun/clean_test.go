package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
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
// removed and stopped, and the work roots it was asked to empty (it empties
// them, unless treeErr says the container failed).
type fakeCleanDocker struct {
	containers [][2]string
	temporals  [][2]string
	removed    []string
	stopped    []string
	trees      []string
	treeErr    error
}

func (f *fakeCleanDocker) RemoveTree(_ context.Context, root string) error {
	f.trees = append(f.trees, root)
	if f.treeErr != nil {
		return f.treeErr
	}
	entries, _ := os.ReadDir(root)
	for _, e := range entries {
		_ = os.RemoveAll(filepath.Join(root, e.Name()))
	}
	return nil
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
			removeAll: os.RemoveAll,
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

// A dead process's work root that holds root-owned files (an activity
// container ran as root and wrote them) is not the cleanup's to unlink. It is
// emptied through a throwaway container, once, and only when the host's own
// removal failed for lack of permission, a container runtime is there and the
// cleanup is removing: a listing runs nothing, a live process's root and a
// root a running proof recorded never get this far, and a container that
// fails leaves the root listed, not retried and not counted.
func TestClean_RootOwnedWorkRootsAreEmptiedThroughAContainer(t *testing.T) {
	denied := &fs.PathError{Op: "unlinkat", Path: "x", Err: fs.ErrPermission}
	cases := []struct {
		name      string
		pid       int // the process the root's name records
		listing   bool
		noRuntime bool
		hostErr   error
		treeErr   error

		wantTrees   int    // RemoveTree calls
		wantHost    int    // host removals attempted
		wantCounted int    // work roots in the summary line
		wantLog     string // in the output
		wantGone    bool
	}{
		{
			name: "dead root, permission denied: emptied through a container",
			pid:  16, hostErr: denied,
			wantTrees: 1, wantHost: 1, wantCounted: 1, wantGone: true,
			wantLog: "    removed through a container: %s\n",
		},
		{
			name: "dead root, the container fails too: listed, not retried, not counted",
			pid:  16, hostErr: denied, treeErr: errors.New("docker: no space left"),
			wantTrees: 1, wantHost: 1, wantCounted: 0,
			wantLog: "    (not removed, even through a container: docker: no space left)\n",
		},
		{
			name: "dead root, another error: not sent to a container",
			pid:  16, hostErr: errors.New("input/output error"),
			wantTrees: 0, wantHost: 1, wantCounted: 0,
			wantLog: "    (not removed: input/output error)\n",
		},
		{
			name: "live process's root: never reaches a container",
			pid:  41, hostErr: denied,
			wantTrees: 0, wantHost: 0, wantCounted: 0,
			wantLog: "(in use: pid 41 is alive; left alone)",
		},
		{
			name: "root a running proof recorded: never reaches a container",
			pid:  42, hostErr: denied,
			wantTrees: 0, wantHost: 0, wantCounted: 0,
			wantLog: "(in use: a running proof recorded pid 42; left alone)",
		},
		{
			name: "a listing runs nothing",
			pid:  16, listing: true, hostErr: denied,
			wantTrees: 0, wantHost: 0, wantCounted: 1,
		},
		{
			name: "no container runtime: listed as before",
			pid:  16, noRuntime: true, hostErr: denied,
			wantTrees: 0, wantHost: 1, wantCounted: 0,
			wantLog: "(some files are not ours to remove: try a container, or sudo: ",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wt := t.TempDir()
			// pid 40 is a running proof that recorded dead pid 42.
			write(t, filepath.Join(wt, "proof", "running", "proof.pid"), "40 40 900 pr-ddd\n")
			write(t, filepath.Join(wt, "proof", "running", "integration", "engine-006.bucket"), "orkstack-engine-42-6\n")
			root := filepath.Join(wt, ".data", "work", fmt.Sprintf("chaos-orker-a-p%d-1", tc.pid))
			write(t, filepath.Join(root, "task", "out", "root-owned.txt"), "written by a container\n")

			docker := &fakeCleanDocker{treeErr: tc.treeErr}
			hostCalls := 0
			var out strings.Builder
			c := &cleaner{
				worktrees: []string{wt},
				host:      "builder",
				alive:     func(pid int) bool { return pid == 40 || pid == 41 },
				procStart: func(pid int) string { return map[int]string{40: "900"}[pid] },
				remove:    !tc.listing,
				out:       &out,
				removeAll: func(string) error { hostCalls++; return tc.hostErr },
			}
			if !tc.noRuntime {
				c.docker = docker
			}
			n := c.run(context.Background())

			if n.workRoots != tc.wantCounted {
				t.Errorf("counted %d work roots, want %d\n%s", n.workRoots, tc.wantCounted, out.String())
			}
			verb := "removed"
			if tc.listing {
				verb = "would remove"
			}
			summary := fmt.Sprintf("cleanup: %s 0 buckets, %d work roots, 0 containers of dead proofs\n", verb, tc.wantCounted)
			if !strings.HasSuffix(out.String(), summary) {
				t.Errorf("output does not end with %q:\n%s", summary, out.String())
			}
			if hostCalls != tc.wantHost {
				t.Errorf("the host removed %d times, want %d", hostCalls, tc.wantHost)
			}
			if len(docker.trees) != tc.wantTrees {
				t.Errorf("container removals %q, want %d\n%s", docker.trees, tc.wantTrees, out.String())
			}
			if tc.wantTrees > 0 && len(docker.trees) > 0 && docker.trees[0] != root {
				t.Errorf("the container was given %q, want %q", docker.trees[0], root)
			}
			if tc.wantLog != "" {
				want := tc.wantLog
				if strings.Contains(want, "%s") {
					want = fmt.Sprintf(want, root)
				}
				if !strings.Contains(out.String(), want) {
					t.Errorf("output is missing %q:\n%s", want, out.String())
				}
			}
			if tc.wantTrees == 0 && strings.Contains(out.String(), "through a container") {
				t.Errorf("output speaks of a container though none ran:\n%s", out.String())
			}
			_, err := os.Stat(root)
			if gone := os.IsNotExist(err); gone != tc.wantGone {
				t.Errorf("root gone = %v, want %v (%v)", gone, tc.wantGone, err)
			}
		})
	}
}

// The container is only ever handed a work root this run found: an absolute,
// cleaned path of a real directory (not a link to one) named like a test's
// work root, and one whose text the -v syntax cannot misread.
func TestClean_AContainerIsOnlyHandedAFoundWorkRoot(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "work", "chaos-p16-1")
	write(t, filepath.Join(root, "f"), "x")
	other := filepath.Join(dir, "work", "chaos-p17-2") // a real work root this run did not find
	write(t, filepath.Join(other, "f"), "x")
	link := filepath.Join(dir, "work", "link-p18-3")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "work", "file-p19-4")
	write(t, file, "x")

	for _, tc := range []struct {
		name  string
		path  string
		found []string
		ok    bool
	}{
		{"found", root, []string{root}, true},
		{"not found by this run", other, []string{root}, false},
		{"a link to a found root", link, []string{link}, false},
		{"a file", file, []string{file}, false},
		{"gone", filepath.Join(dir, "work", "gone-p20-5"), []string{filepath.Join(dir, "work", "gone-p20-5")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			docker := &fakeCleanDocker{}
			c := &cleaner{docker: docker}
			found := map[string]bool{}
			for _, p := range tc.found {
				found[p] = true
			}
			err := c.emptyThroughContainer(context.Background(), tc.path, found)
			if (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok = %v", err, tc.ok)
			}
			if want := map[bool]int{true: 1, false: 0}[tc.ok]; len(docker.trees) != want {
				t.Errorf("container removals %q, want %d", docker.trees, want)
			}
		})
	}
}

// The container a work root is emptied by is an ordinary, unprivileged one
// with one bind mount, the root itself; the arguments are built by a pure
// function so what they never contain is checked without a daemon.
func TestClean_RemoveTreeArgs_MountOnlyTheRootAndNothingPrivileged(t *testing.T) {
	root := "/home/u/repo/.data/work/chaos-orker-a-p16-1"
	args, err := removeTreeArgs("orkstack-clean-1-2", root)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) == 0 || args[0] != "run" {
		t.Fatalf("args = %q, want them to start with run", args)
	}
	count := func(want string) (n int) {
		for _, a := range args {
			if a == want {
				n++
			}
		}
		return n
	}
	if count("-v") != 1 || count("--volume") != 0 || count("--mount") != 0 {
		t.Errorf("want exactly one bind mount (-v): %q", args)
	}
	if i := slices.Index(args, "-v"); i < 0 || args[i+1] != root+":/w" {
		t.Errorf("the mount is not the root at /w: %q", args)
	}
	if count("--rm") != 1 {
		t.Errorf("want --rm once: %q", args)
	}
	if i := slices.Index(args, "--network"); i < 0 || args[i+1] != "none" {
		t.Errorf("want --network none: %q", args)
	}
	if i := slices.Index(args, "--name"); i < 0 || args[i+1] != "orkstack-clean-1-2" {
		t.Errorf("want the unique name given: %q", args)
	}
	if count(firstPartyBase) != 1 || !strings.Contains(firstPartyBase, "@sha256:") {
		t.Errorf("want the digest-pinned base once: %q", args)
	}
	for _, a := range args {
		for _, never := range []string{"--privileged", "--pid", "--ipc", "--uts", "--userns", "--cap-add", "--device", "--volumes-from", "docker.sock", "host"} {
			if strings.HasPrefix(a, never) || (never == "docker.sock" || never == "host") && strings.Contains(a, never) {
				t.Errorf("argument %q must never be there: %q", a, args)
			}
		}
	}
	// What it runs deletes inside /w and never /w itself or anything above.
	cmd := strings.Join(args[slices.Index(args, firstPartyBase)+1:], " ")
	if want := "find /w -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +"; cmd != want {
		t.Errorf("command = %q, want %q", cmd, want)
	}
}

// The arguments are refused for a path that is not a test's own work root as
// the cleanup names them.
func TestClean_RemoveTreeArgs_RefusesWhatIsNotAWorkRoot(t *testing.T) {
	for _, root := range []string{
		"",
		"/",
		"/home/u/repo/.data/work",     // the parent, not a root
		"/home/u/repo/.data/work/e2e", // no pid in its name
		"relative/chaos-p16-1",        // not absolute
		"/home/u/repo/.data/work/../work/chaos-p16-1", // not cleaned
		"/home/u/repo/.data/work/chaos-p16-1/",        // not cleaned
		"/home/u/repo/.data/work/chaos-p16-1/inner",   // not the root itself
		"/home/u/repo/.data/work/chaos-p16-1:/etc:rw", // -v syntax
		"/home/u/repo/.data/wo:rk/chaos-p16-1",        // -v syntax
		"/etc",
	} {
		if args, err := removeTreeArgs("n", root); err == nil {
			t.Errorf("%q was accepted: %q", root, args)
		}
	}
}

// The base the throwaway container runs is the first-party images' own
// digest-pinned alpine, so the cleanup pulls nothing a build has not.
func TestClean_RemoveTreeArgs_BaseIsTheFirstPartyImagesBase(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "..", "images", "*", "Dockerfile"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no first-party Dockerfiles found: %v", err)
	}
	alpine := regexp.MustCompile(`alpine:[0-9.]+@sha256:[0-9a-f]{64}`)
	seen := 0
	for _, file := range files {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, ref := range alpine.FindAllString(string(b), -1) {
			seen++
			if ref != firstPartyBase {
				t.Errorf("%s builds from %s; the cleanup's base is %s", file, ref, firstPartyBase)
			}
		}
	}
	if seen == 0 {
		t.Errorf("no first-party Dockerfile builds from a pinned alpine; the base %s is nobody's", firstPartyBase)
	}
}

// dockerCLI.RemoveTree runs the container with the arguments above, bounded
// by a timeout; when it fails or outlasts the timeout the container is
// force-removed by the name it was given, so none is left running.
func TestClean_DockerRemoveTreeLeavesNoContainerRunning(t *testing.T) {
	root := filepath.Join(t.TempDir(), "chaos-p16-1")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		mode      string
		wantErr   string
		wantForce bool
	}{
		{mode: "ok"},
		{mode: "fail", wantErr: "boom", wantForce: true},
		{mode: "hang", wantErr: "timed out", wantForce: true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			bin := t.TempDir()
			calls := filepath.Join(t.TempDir(), "calls")
			script := "#!/bin/sh\necho \"$@\" >> \"$FAKE_DOCKER_CALLS\"\n" +
				"if [ \"$1\" = run ]; then\n  case \"$FAKE_DOCKER_MODE\" in\n" +
				"  fail) echo boom >&2; exit 1 ;;\n  hang) exec sleep 60 ;;\n  esac\nfi\nexit 0\n"
			if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("FAKE_DOCKER_CALLS", calls)
			t.Setenv("FAKE_DOCKER_MODE", tc.mode)
			old := workRootRemoveTimeout
			workRootRemoveTimeout = time.Second
			t.Cleanup(func() { workRootRemoveTimeout = old })

			err := dockerCLI{}.RemoveTree(context.Background(), root)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("err = %v, want none", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want one saying %q", err, tc.wantErr)
			}
			b, _ := os.ReadFile(calls)
			lines := strings.Split(strings.TrimSpace(string(b)), "\n")
			if len(lines) == 0 || !strings.HasPrefix(lines[0], "run --rm --name ") {
				t.Fatalf("first docker call = %q, want a run --rm --name", lines)
			}
			name := strings.Fields(lines[0])[3]
			wantLines := 1
			if tc.wantForce {
				wantLines = 2
			}
			if len(lines) != wantLines {
				t.Fatalf("docker was called %q, want %d calls", lines, wantLines)
			}
			if tc.wantForce && lines[1] != "rm -f "+name {
				t.Errorf("second docker call = %q, want %q", lines[1], "rm -f "+name)
			}
		})
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
		removeAll: os.RemoveAll,
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
