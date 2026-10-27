package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// proofrun clean: what test processes that are gone left behind.
//
// A test process owns a bucket (internal/testsupport/s3bucket), an Orker work
// root under .data/work (internal/testsupport/proof.WorkRootPattern; a
// Playwright spec's Orkers get one of the same shape, ui/e2e/harness.ts) and
// the activity containers its Orkers started, each of which bind-mounts its
// workspace from inside that root. It removes them itself when its
// tests pass, and the runner's sweep removes a passing process's bucket; a
// failing process keeps them for the post-mortem, and a proof that was
// stopped or killed leaves them all. Left there, a bucket holds the object
// store's volumes and a work root the host's disk, so every proof and
// scripts/integration.sh runs this first, and `make proof-clean` runs it by
// hand (docs/PROOFS.md §10).
//
// The one rule is AGENTS.md §11.1's exception to "stop only what you started":
// a leftover is removed only when the process its name records is gone, and
// never when a proof that is still running recorded it. A thing whose owner
// cannot be told is listed and left alone.
//
// An activity container runs as root and writes root-owned files into its
// bind-mounted work root, which this unprivileged process cannot unlink. Only
// then (the host's removal was refused for permission, the process is gone,
// the cleanup is removing and a container runtime is there) is that one root
// emptied through a throwaway container: see removeTreeArgs for what it is.
//
// It finds buckets by the names proofs recorded, not by the store's listing
// alone: the development store, SeaweedFS 4.48 with its auth off, answers the
// S3 bucket listing with no bucket at all. Every proof directory and
// scripts/integration.sh run directory of every worktree of this repository
// is read: each process's hand-off file (integration/<job>.bucket) and the
// head of each process's log, where s3bucket.Main names the bucket it made.

// leftoverBucket is the shape of a test process's bucket name: its label, the
// pid of the process that made it, and a suffix.
var leftoverBucket = regexp.MustCompile(`^orkstack-[a-z][a-z0-9-]*-([0-9]+)-[0-9]+$`)

// bucketMention finds a bucket name of that shape in a log.
var bucketMention = regexp.MustCompile(`s3://(orkstack-[a-z][a-z0-9-]*-[0-9]+-[0-9]+)\b`)

// workRootName is the shape of a test's own Orker work root: <label>-p<pid>-<suffix>.
var workRootName = regexp.MustCompile(`-p([0-9]+)-[A-Za-z0-9]+$`)

// proofOrkerID is the shape of internal/testsupport/proof.OrkerID inside a
// proof: <base>-<proof id>-<pid>.
var proofOrkerID = regexp.MustCompile(`-pr-(?:ci-)?[0-9a-z]+-([0-9]+)$`)

// cleanLogHead is how much of a process's log is read for the bucket it made:
// s3bucket.Main names it before any test runs.
const cleanLogHead = 16 << 10

// cleanStore is the object store as the cleanup sees it.
type cleanStore interface {
	List(ctx context.Context) ([]string, error)
	Exists(ctx context.Context, bucket string) (bool, error)
	Remove(ctx context.Context, bucket string) error
}

// cleanContainer is one activity container as the cleanup sees it: its id,
// its Orker's id (the ork.orker label) and the host paths it bind-mounts.
type cleanContainer struct {
	id, orker string
	mounts    []string
}

// cleanDocker is the container runtime as the cleanup sees it.
type cleanDocker interface {
	// Containers lists every container labelled ork.orker.
	Containers(ctx context.Context) ([]cleanContainer, error)
	// ProofTemporals lists the proofs' own Temporal containers: name and the
	// orkstack.proof.owner label (host:pid:start).
	ProofTemporals(ctx context.Context) ([][2]string, error)
	Remove(ctx context.Context, id string) error
	Stop(ctx context.Context, name string) error
	// RemoveTree empties a work root through a throwaway container.
	RemoveTree(ctx context.Context, root string) error
}

// cleaner holds what one cleanup looks at and how it acts.
type cleaner struct {
	worktrees []string // the repository's worktrees
	workDirs  []string // Orker work-root parents besides each worktree's .data/work
	host      string
	alive     func(pid int) bool
	procStart func(pid int) string // a process's start time, "" when there is none
	store     cleanStore           // nil when the object store is not reachable
	docker    cleanDocker          // nil when there is no container runtime
	remove    bool
	out       io.Writer
	removeAll func(path string) error // the host's removal of a work root: os.RemoveAll

	protected map[int]bool // pids a running proof recorded
}

// cleanCounts is what one cleanup removed.
type cleanCounts struct{ buckets, workRoots, containers int }

func cmdClean(args []string) int {
	flags := flag.NewFlagSet("clean", flag.ContinueOnError)
	remove := flags.Bool("remove", false, "remove what is listed (without it, nothing is removed)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	ctx := context.Background()
	c := &cleaner{
		worktrees: repoWorktrees(),
		alive:     processAlive,
		procStart: processStart,
		remove:    *remove,
		out:       os.Stdout,
		removeAll: os.RemoveAll,
	}
	c.host, _ = os.Hostname()
	if dir := os.Getenv("ORK_ORKER_WORKDIR"); dir != "" {
		c.workDirs = append(c.workDirs, dir)
	}
	if store, err := newS3CleanStore(ctx); err == nil {
		c.store = store
	} else {
		fmt.Fprintf(c.out, "(object storage: %v; buckets skipped)\n", err)
	}
	if _, err := exec.LookPath("docker"); err == nil {
		c.docker = dockerCLI{}
	}
	c.run(ctx)
	return 0
}

// run lists, and with remove removes, every leftover, and ends with the
// summary line scripts/proof.sh copies into setup.log.
func (c *cleaner) run(ctx context.Context) cleanCounts {
	var n cleanCounts
	running, dead := c.recordRoots()
	c.protected = map[int]bool{}
	for _, root := range running {
		for _, name := range recordedBuckets(root) {
			if m := leftoverBucket.FindStringSubmatch(name); m != nil {
				pid, _ := strconv.Atoi(m[1])
				c.protected[pid] = true
			}
		}
	}

	fmt.Fprintln(c.out, "== buckets kept by test processes")
	n.buckets = c.buckets(ctx, dead)

	// Containers go before the work roots they mount.
	roots := c.workRoots()
	fmt.Fprintln(c.out, "== activity containers of test processes")
	n.containers = c.containers(ctx)
	fmt.Fprintln(c.out, "== proof Temporal containers whose proof is gone")
	n.containers += c.temporals(ctx)
	fmt.Fprintln(c.out, "== per-test work roots")
	n.workRoots = c.removeWorkRoots(ctx, roots)

	verb := "would remove"
	if c.remove {
		verb = "removed"
	}
	fmt.Fprintf(c.out, "cleanup: %s %d buckets, %d work roots, %d containers of dead proofs\n",
		verb, n.buckets, n.workRoots, n.containers)
	return n
}

// owner says why a leftover of pid stays, or "" when it is a dead process's.
func (c *cleaner) owner(pid int) string {
	switch {
	case c.alive(pid):
		return fmt.Sprintf("in use: pid %d is alive; left alone", pid)
	case c.protected[pid]:
		return fmt.Sprintf("in use: a running proof recorded pid %d; left alone", pid)
	}
	return ""
}

// recordRoots are the run directories of every worktree: each proof's
// directory and each scripts/integration.sh run's, split into the ones still
// running and the rest.
func (c *cleaner) recordRoots() (running, dead []string) {
	for _, w := range c.worktrees {
		proofs, _ := filepath.Glob(filepath.Join(w, "proof", "*", "proof.pid"))
		for _, pidFile := range proofs {
			root := filepath.Dir(pidFile)
			if c.proofRunning(pidFile) {
				running = append(running, root)
			} else {
				dead = append(dead, root)
			}
		}
		runs, _ := filepath.Glob(filepath.Join(w, ".data", "integration", "*"))
		for _, root := range runs {
			// integration.sh names its run directory <stamp>-<its pid>.
			pid, err := strconv.Atoi(root[strings.LastIndex(root, "-")+1:])
			if err == nil && c.alive(pid) {
				running = append(running, root)
			} else {
				dead = append(dead, root)
			}
		}
	}
	return running, dead
}

// proofRunning reads a proof.pid (pid, pid, start, proof id) and says whether
// that process still runs: the pid alone could have been reused.
func (c *cleaner) proofRunning(pidFile string) bool {
	b, err := os.ReadFile(pidFile)
	if err != nil {
		return false
	}
	f := strings.Fields(string(b))
	if len(f) < 3 {
		return false
	}
	pid, err := strconv.Atoi(f[0])
	return err == nil && c.procStart(pid) == f[2]
}

// recordedBuckets are the bucket names one run directory recorded: the
// hand-off files a passing process wrote and no sweep removed, and the bucket
// each process's log names at its head.
func recordedBuckets(root string) []string {
	var names []string
	files, _ := filepath.Glob(filepath.Join(root, "integration", "*.bucket"))
	for _, file := range files {
		if b, err := os.ReadFile(file); err == nil {
			names = append(names, strings.TrimSpace(string(b)))
		}
	}
	logs, _ := filepath.Glob(filepath.Join(root, "integration", "*.log"))
	for _, file := range logs {
		f, err := os.Open(file)
		if err != nil {
			continue
		}
		head, _ := io.ReadAll(io.LimitReader(f, cleanLogHead))
		_ = f.Close()
		for _, m := range bucketMention.FindAllSubmatch(head, -1) {
			names = append(names, string(m[1]))
		}
	}
	return names
}

// buckets lists and removes the leftover buckets: what the store lists and
// what the dead runs recorded, each checked against the store, never one a
// running proof recorded or a live process owns.
func (c *cleaner) buckets(ctx context.Context, dead []string) int {
	if c.store == nil {
		fmt.Fprintln(c.out, "  (object storage not reachable; skipped)")
		return 0
	}
	held := map[string]bool{}
	listed, err := c.store.List(ctx)
	if err != nil {
		fmt.Fprintf(c.out, "  (the store refused its bucket listing: %v; only recorded names are checked)\n", err)
	}
	for _, name := range listed {
		if leftoverBucket.MatchString(name) {
			held[name] = true
		}
	}
	var recorded []string
	seen := map[string]bool{}
	for _, root := range dead {
		for _, name := range recordedBuckets(root) {
			if !seen[name] && !held[name] && leftoverBucket.MatchString(name) {
				seen[name] = true
				recorded = append(recorded, name)
			}
		}
	}
	for name := range c.existing(ctx, recorded) {
		held[name] = true
	}

	names := make([]string, 0, len(held))
	for name := range held {
		names = append(names, name)
	}
	sort.Strings(names)
	removed := 0
	for _, name := range names {
		pid, _ := strconv.Atoi(leftoverBucket.FindStringSubmatch(name)[1])
		if why := c.owner(pid); why != "" {
			fmt.Fprintf(c.out, "  %s  (%s)\n", name, why)
			continue
		}
		fmt.Fprintf(c.out, "  %s\n", name)
		if !c.remove {
			removed++
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, bucketRemoveTimeout)
		err := c.store.Remove(rctx, name)
		cancel()
		if err != nil {
			fmt.Fprintf(c.out, "    (not removed: %v)\n", err)
			continue
		}
		removed++
	}
	return removed
}

// existing are the names among recorded the store still holds, asked a few
// at a time; a name the store cannot answer for is reported and left out.
func (c *cleaner) existing(ctx context.Context, recorded []string) map[string]bool {
	out := map[string]bool{}
	var mu sync.Mutex
	queue := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range queue {
				hctx, cancel := context.WithTimeout(ctx, 20*time.Second)
				ok, err := c.store.Exists(hctx, name)
				cancel()
				mu.Lock()
				if err != nil {
					fmt.Fprintf(c.out, "  %s  (recorded; the store did not say whether it holds it: %v)\n", name, err)
				} else if ok {
					out[name] = true
				}
				mu.Unlock()
			}
		}()
	}
	for _, name := range recorded {
		queue <- name
	}
	close(queue)
	wg.Wait()
	return out
}

// workRoot is one test's own Orker work root and the pid its name records.
type workRoot struct {
	path string
	pid  int
}

// workParents are the directories test work roots are made in: every
// worktree's .data/work and the extra parents.
func (c *cleaner) workParents() []string {
	var parents []string
	for _, w := range c.worktrees {
		parents = append(parents, filepath.Join(w, ".data", "work"))
	}
	return append(parents, c.workDirs...)
}

// workRoots are the test work roots under every work-root parent; a
// directory whose name records no pid is listed and left alone.
func (c *cleaner) workRoots() []workRoot {
	var roots []workRoot
	seen := map[string]bool{}
	for _, parent := range c.workParents() {
		entries, err := os.ReadDir(parent)
		if err != nil {
			continue
		}
		for _, e := range entries {
			name := e.Name()
			path := filepath.Join(parent, name)
			if !e.IsDir() || seen[path] || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "ork-pin-") ||
				name == "ork-spool" || name == "ork-stage" {
				continue // the dev stack's own Orker state
			}
			seen[path] = true
			m := workRootName.FindStringSubmatch(name)
			if m == nil {
				fmt.Fprintf(c.out, "  %s/  (its name records no owning pid; not removed here — remove it by hand once nothing on this host runs tests)\n", path)
				continue
			}
			pid, _ := strconv.Atoi(m[1])
			roots = append(roots, workRoot{path, pid})
		}
	}
	return roots
}

// removeWorkRoots removes the work roots of processes that are gone. A root
// the host cannot remove for lack of permission holds files an activity
// container wrote as root; with a container runtime it is emptied through a
// throwaway container, once, and a failure there is listed and not retried.
func (c *cleaner) removeWorkRoots(ctx context.Context, roots []workRoot) int {
	found := map[string]bool{}
	for _, r := range roots {
		found[r.path] = true
	}
	removed := 0
	for _, r := range roots {
		if why := c.owner(r.pid); why != "" {
			fmt.Fprintf(c.out, "  %s/  (%s)\n", r.path, why)
			continue
		}
		fmt.Fprintf(c.out, "  %s/\n", r.path)
		if !c.remove {
			removed++
			continue
		}
		err := c.removeAll(r.path)
		switch {
		case err == nil:
			removed++
		case !errors.Is(err, fs.ErrPermission):
			fmt.Fprintf(c.out, "    (not removed: %v)\n", err)
		case c.docker == nil:
			fmt.Fprintf(c.out, "    (some files are not ours to remove: try a container, or sudo: %v)\n", err)
		default:
			if err := c.emptyThroughContainer(ctx, r.path, found); err != nil {
				fmt.Fprintf(c.out, "    (not removed, even through a container: %v)\n", err)
				continue
			}
			fmt.Fprintf(c.out, "    removed through a container: %s\n", r.path)
			removed++
		}
	}
	return removed
}

// emptyThroughContainer removes a dead process's work root that holds
// root-owned files: a throwaway container deletes what is inside it, then the
// empty root goes from the host. Only a root this run found is ever handed to
// the container, as a real directory and not a link to one.
func (c *cleaner) emptyThroughContainer(ctx context.Context, path string, found map[string]bool) error {
	if !found[path] {
		return fmt.Errorf("%s is not a work root this run found", path)
	}
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	abs, err := filepath.Abs(path) // ORK_ORKER_WORKDIR may be relative
	if err != nil {
		return err
	}
	if err := c.docker.RemoveTree(ctx, abs); err != nil {
		return err
	}
	return os.Remove(path)
}

// mountOwner is the pid of the test work root a container's workspace lies
// in, and whether there is one. A work root is a directory named like one
// (workRootName) right under a work-root parent: one of parents, or any
// .data/work on this host, since the worktree that made it may be gone. The
// workspace may lie deeper inside the root (a test that gives each of its
// Orkers a directory of its own there). The development stack's Orker works
// in .data/work itself, so its containers have no owner here.
func mountOwner(parents map[string]bool, mounts []string) (int, bool) {
	for _, mount := range mounts {
		if !filepath.IsAbs(mount) {
			continue
		}
		for dir := filepath.Clean(mount); dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
			parent := filepath.Dir(dir)
			if !parents[parent] && (filepath.Base(parent) != "work" || filepath.Base(filepath.Dir(parent)) != ".data") {
				continue
			}
			if m := workRootName.FindStringSubmatch(filepath.Base(dir)); m != nil {
				pid, _ := strconv.Atoi(m[1])
				return pid, true
			}
			break
		}
	}
	return 0, false
}

// containers removes the activity containers whose Orker's process is gone.
// The owner is the pid a proof's Orker id ends in, or the pid of the test
// work root the container mounts (mountOwner); a container whose owner cannot
// be told is counted and left alone.
func (c *cleaner) containers(ctx context.Context) int {
	if c.docker == nil {
		fmt.Fprintln(c.out, "  (no container runtime; skipped)")
		return 0
	}
	parents := map[string]bool{}
	for _, p := range c.workParents() {
		if abs, err := filepath.Abs(p); err == nil { // ORK_ORKER_WORKDIR may be relative
			parents[abs] = true
		}
	}
	list, err := c.docker.Containers(ctx)
	if err != nil {
		fmt.Fprintf(c.out, "  (containers not listed: %v)\n", err)
		return 0
	}
	removed, unknown := 0, 0
	for _, ct := range list {
		id, orker := ct.id, ct.orker
		pid, ok := mountOwner(parents, ct.mounts)
		if m := proofOrkerID.FindStringSubmatch(orker); m != nil {
			pid, _ = strconv.Atoi(m[1])
			ok = true
		}
		if !ok {
			unknown++
			continue
		}
		if why := c.owner(pid); why != "" {
			fmt.Fprintf(c.out, "  %s of Orker %s  (%s)\n", id, orker, why)
			continue
		}
		fmt.Fprintf(c.out, "  %s of Orker %s\n", id, orker)
		if !c.remove {
			removed++
			continue
		}
		if err := c.docker.Remove(ctx, id); err != nil {
			fmt.Fprintf(c.out, "    (not removed: %v)\n", err)
			continue
		}
		removed++
	}
	if unknown > 0 {
		fmt.Fprintf(c.out, "  %d containers whose owner cannot be told (no pid in their Orker id, and no test work root among their mounts); left alone\n", unknown)
	}
	return removed
}

// temporals stops the proofs' own Temporal containers whose proof is gone:
// one labelled with this host, a pid and a start time no process here has.
func (c *cleaner) temporals(ctx context.Context) int {
	if c.docker == nil {
		return 0
	}
	list, err := c.docker.ProofTemporals(ctx)
	if err != nil {
		fmt.Fprintf(c.out, "  (containers not listed: %v)\n", err)
		return 0
	}
	stopped := 0
	for _, ct := range list {
		name, owner := ct[0], ct[1]
		f := strings.SplitN(owner, ":", 3)
		if len(f) != 3 || f[0] != c.host {
			continue
		}
		pid, err := strconv.Atoi(f[1])
		if err != nil || c.procStart(pid) == f[2] {
			continue
		}
		fmt.Fprintf(c.out, "  %s\n", name)
		if !c.remove {
			stopped++
			continue
		}
		if err := c.docker.Stop(ctx, name); err != nil {
			fmt.Fprintf(c.out, "    (not stopped: %v)\n", err)
			continue
		}
		stopped++
	}
	return stopped
}

// repoWorktrees are this repository's worktrees, as git lists them; the
// current directory alone when git cannot say.
func repoWorktrees() []string {
	out, err := exec.Command("git", "worktree", "list", "--porcelain").Output()
	if err != nil {
		wd, _ := os.Getwd()
		return []string{wd}
	}
	var trees []string
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		if path, ok := strings.CutPrefix(sc.Text(), "worktree "); ok {
			trees = append(trees, path)
		}
	}
	return trees
}

// processAlive reports whether a process with pid exists, whoever owns it. A
// reused pid keeps a leftover a little longer; it never removes a live one.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// processStart is a process's start time in clock ticks since boot (field 22
// of /proc/<pid>/stat), as scripts/proof.sh records it; "" when there is no
// such process.
func processStart(pid int) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return ""
	}
	s := string(b)
	i := strings.LastIndex(s, ") ")
	if i < 0 {
		return ""
	}
	f := strings.Fields(s[i+2:])
	if len(f) < 20 {
		return ""
	}
	return f[19]
}

// s3CleanStore is the object store the environment names
// (artifacts.ConfigFromEnv, the one test processes use).
type s3CleanStore struct{ client *s3.Client }

func newS3CleanStore(ctx context.Context) (*s3CleanStore, error) {
	client, err := s3bucket.Client(ctx, artifacts.ConfigFromEnv())
	if err != nil {
		return nil, err
	}
	return &s3CleanStore{client}, nil
}

func (s *s3CleanStore) List(ctx context.Context) ([]string, error) {
	lctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	out, err := s.client.ListBuckets(lctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, b := range out.Buckets {
		names = append(names, aws.ToString(b.Name))
	}
	return names, nil
}

func (s *s3CleanStore) Exists(ctx context.Context, bucket string) (bool, error) {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return true, nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchBucket") {
		return false, nil
	}
	return false, err
}

func (s *s3CleanStore) Remove(ctx context.Context, bucket string) error {
	return s3bucket.Remove(ctx, s.client, bucket)
}

// dockerCLI is the docker command line.
type dockerCLI struct{}

// output is what a docker command printed, also when it then failed.
func (dockerCLI) output(ctx context.Context, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	out, err := exec.CommandContext(cctx, "docker", args...).Output()
	return strings.TrimSpace(string(out)), err
}

func (d dockerCLI) lines(ctx context.Context, args ...string) ([][2]string, error) {
	out, err := d.output(ctx, args...)
	if err != nil {
		return nil, err
	}
	var rows [][2]string
	for _, line := range strings.Split(out, "\n") {
		if a, b, ok := strings.Cut(line, "\t"); ok {
			rows = append(rows, [2]string{a, b})
		}
	}
	return rows, nil
}

// Containers lists the containers, then inspects them for their mounts. One
// removed in between makes the inspection fail after it printed the others,
// which are still the listing.
func (d dockerCLI) Containers(ctx context.Context) ([]cleanContainer, error) {
	ids, err := d.output(ctx, "ps", "-aq", "--filter", "label=ork.orker")
	if err != nil || ids == "" {
		return nil, err
	}
	out, err := d.output(ctx, append([]string{"inspect", "--format",
		`{{.Id}}{{"\t"}}{{index .Config.Labels "ork.orker"}}{{range .Mounts}}{{"\t"}}{{.Source}}{{end}}`},
		strings.Fields(ids)...)...)
	if err != nil && out == "" {
		return nil, err
	}
	var list []cleanContainer
	for _, line := range strings.Split(out, "\n") {
		f := strings.Split(line, "\t")
		if len(f) < 2 {
			continue
		}
		id := f[0]
		if len(id) > 12 {
			id = id[:12]
		}
		list = append(list, cleanContainer{id: id, orker: f[1], mounts: f[2:]})
	}
	return list, nil
}

func (d dockerCLI) ProofTemporals(ctx context.Context) ([][2]string, error) {
	return d.lines(ctx, "ps", "--filter", "label=orkstack.proof", "--format", `{{.Names}}	{{.Label "orkstack.proof.owner"}}`)
}

func (dockerCLI) Remove(ctx context.Context, id string) error {
	cctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return exec.CommandContext(cctx, "docker", "rm", "-f", id).Run()
}

func (dockerCLI) Stop(ctx context.Context, name string) error {
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	return exec.CommandContext(cctx, "docker", "stop", name).Run()
}

// firstPartyBase is the digest-pinned alpine the first-party images
// (images/*/Dockerfile) are built from, so a host that built them has it and
// the cleanup pulls nothing new.
const firstPartyBase = "alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc"

// workRootRemoveTimeout bounds one work root's removal through a container;
// a variable only so a test can shorten it.
var workRootRemoveTimeout = 2 * time.Minute

// removeTreeArgs are the arguments of the throwaway container that empties
// one work root: the root is its only mount, and it has no privilege, host
// namespace, added capability or network. The root must be a cleaned,
// absolute path named like a test's work root, and one `-v` cannot misread.
func removeTreeArgs(name, root string) ([]string, error) {
	switch {
	case !filepath.IsAbs(root) || filepath.Clean(root) != root:
		return nil, fmt.Errorf("%q is not an absolute, cleaned path", root)
	case !workRootName.MatchString(filepath.Base(root)):
		return nil, fmt.Errorf("%q is not named like a test's work root", root)
	case strings.Contains(root, ":"):
		return nil, fmt.Errorf("%q cannot be mounted: its name holds a colon", root)
	}
	return []string{
		"run", "--rm", "--name", name, "--network", "none",
		"--read-only", "--security-opt", "no-new-privileges",
		"-v", root + ":/w", firstPartyBase,
		// Everything inside /w, dotfiles included; never /w itself.
		"find", "/w", "-mindepth", "1", "-maxdepth", "1", "-exec", "rm", "-rf", "--", "{}", "+",
	}, nil
}

// RemoveTree deletes everything inside a work root through a throwaway
// container (root can unlink what an activity container wrote as root), which
// is force-removed by its name when it fails or outlasts the timeout: the
// docker client dying does not stop it.
func (dockerCLI) RemoveTree(ctx context.Context, root string) error {
	name := fmt.Sprintf("orkstack-clean-%d-%d", os.Getpid(), time.Now().UnixNano())
	args, err := removeTreeArgs(name, root)
	if err != nil {
		return err
	}
	cctx, cancel := context.WithTimeout(ctx, workRootRemoveTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "docker", args...)
	cmd.WaitDelay = 10 * time.Second
	out, err := cmd.CombinedOutput()
	if err == nil {
		return nil
	}
	fctx, fcancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer fcancel()
	_ = exec.CommandContext(fctx, "docker", "rm", "-f", name).Run()
	if errors.Is(cctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("timed out after %s (container %s removed)", workRootRemoveTimeout, name)
	}
	tail := strings.TrimSpace(string(out))
	if len(tail) > 300 {
		tail = tail[len(tail)-300:]
	}
	return fmt.Errorf("%w: %s", err, tail)
}
