package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/orkstack/orkstack/internal/testsupport/s3bucket"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The bucket sweep.
//
// Every integration test process owns a bucket (internal/testsupport/s3bucket),
// and removing one is the object store's heaviest per-bucket operation: it
// retires volumes, and at a higher PROOF_JOBS that churn, done on the slot as
// each process ended, stalled other processes' engine boots (docs/PROOFS.md
// §2.1). A proof therefore sets ORK_TEST_BUCKET_LEFT_FILE for each process: one
// whose tests passed names its bucket in that file and exits, and the runner
// removes the named bucket off the slot, a few at a time, while the phase runs
// on. A process with a failure keeps its bucket for the post-mortem, as it
// always did, and never writes the file.
//
// The removals run during the phase, not after it, because a bucket holds at
// least one of the store's volumes until it is removed, and a store has a
// bounded number of them (a SeaweedFS mini server sizes it from its disk): a
// phase that kept every passing process's bucket until its end needed as many
// volumes as it had jobs, and ran out. A slot hands its process's bucket to
// the sweepers before it starts the next process, and waits while
// bucketSweepBacklog buckets already wait, so the buckets alive at once never
// exceed the slots, the backlog and the removals in flight together.
//
// The sweep is housekeeping: a bucket it cannot remove costs disk, never a
// verdict, because no later run reads it. It is the runner's own, not the
// tests': a test never removes a bucket it does not own, and the sweep removes
// only a bucket named by a file the runner asked a passing process to write.

// bucketSweepers is how many buckets the sweep removes at once.
const bucketSweepers = 2

// bucketSweepBacklog is how many passing processes' buckets may wait for a
// sweeper while the phase runs before a slot waits to hand over its own.
const bucketSweepBacklog = 4

// bucketRemoveTimeout bounds one bucket's removal.
const bucketRemoveTimeout = 2 * time.Minute

// bucketSweeper removes passing processes' buckets while the phase runs.
type bucketSweeper struct {
	queue chan string // the labels handed over, bucketSweepBacklog at most waiting
	wg    sync.WaitGroup

	mu      sync.Mutex
	removed int
	// tried holds every label a sweeper removed (true) or failed to remove
	// (false): the end-of-phase pass counts them and never tries them again.
	tried map[string]bool
}

// sweepTarget is one bucket the sweep may remove.
type sweepTarget struct{ label, bucket, file string }

// bucketFile is the file the process labelled label names its bucket in.
func (r *runner) bucketFile(label string) string {
	return filepath.Join(r.jobDir, label+".bucket")
}

// noteSweepable records whether the process of j may have its bucket removed
// by the sweep: it exited cleanly and every test it was asked to run has a pass
// or a skip. Anything else (a failure, a quarantined failure, a test nothing
// reported) keeps the bucket, whatever file the process left.
func (r *runner) noteSweepable(j job, runErr error, local *collector) {
	ok := runErr == nil
	for _, t := range j.Tests {
		e := local.results[testKey{j.Package, t}]
		if e == nil || (e.Outcome != outcomePass && e.Outcome != outcomeSkip) {
			ok = false
			break
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sweepable == nil {
		r.sweepable = map[string]bool{}
	}
	r.sweepable[jobLabel(j)] = ok
}

// target reads the bucket the process labelled label named, and says why it
// is not the sweep's to remove when it is not. A process that did not pass,
// a file that cannot be read, and a name of a shape no test process makes are
// all kept.
func (r *runner) target(label string) (sweepTarget, string) {
	file := r.bucketFile(label)
	data, err := os.ReadFile(file)
	bucket := strings.TrimSpace(string(data))
	r.mu.Lock()
	sweepable := r.sweepable[label]
	r.mu.Unlock()
	switch {
	case err != nil:
		return sweepTarget{}, fmt.Sprintf("kept the bucket of %s: cannot read %s: %v", label, file, err)
	case !sweepable:
		return sweepTarget{}, fmt.Sprintf("kept s3://%s of %s: not every test of it passed", bucket, label)
	case !s3bucket.IsName(bucket):
		return sweepTarget{}, fmt.Sprintf("kept %q of %s: not the name of a bucket a test process makes", bucket, label)
	}
	return sweepTarget{label, bucket, file}, ""
}

// removeTarget removes one bucket and its hand-off file, and says whether the
// bucket is gone. A removal that fails is logged and its file kept.
func (r *runner) removeTarget(ctx context.Context, p sweepTarget, remove func(context.Context, string) error) bool {
	rctx, cancel := context.WithTimeout(ctx, bucketRemoveTimeout)
	err := remove(rctx, p.bucket)
	cancel()
	if err != nil {
		r.logf("bucket sweep: left s3://%s of %s: %v", p.bucket, p.label, err)
		return false
	}
	if err := os.Remove(p.file); err != nil {
		r.logf("bucket sweep: removed s3://%s of %s, but not its file: %v", p.bucket, p.label, err)
	}
	return true
}

// startSweeper starts the sweepers that remove passing processes' buckets
// while the phase runs. It does nothing for a runner that does not sweep.
// Once ctx is done they remove nothing more: a stopped run leaves its
// hand-off files for scripts/proof-clean.sh.
func (r *runner) startSweeper(ctx context.Context, remove func(context.Context, string) error) {
	if !r.sweep {
		return
	}
	s := &bucketSweeper{queue: make(chan string, bucketSweepBacklog), tried: map[string]bool{}}
	for i := 0; i < bucketSweepers; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for label := range s.queue {
				if ctx.Err() != nil {
					continue
				}
				// A bucket that is not the sweep's to remove is left for the
				// end-of-phase pass, which says why it was kept.
				p, why := r.target(label)
				if why != "" {
					continue
				}
				gone := r.removeTarget(ctx, p, remove)
				s.mu.Lock()
				s.tried[label] = gone
				if gone {
					s.removed++
				}
				s.mu.Unlock()
			}
		}()
	}
	r.mu.Lock()
	r.sweeper = s
	r.mu.Unlock()
}

// handOff gives the bucket of the passing process labelled label to the
// sweepers. It waits while bucketSweepBacklog buckets already wait, so a slot
// starts its next process only once its last one's bucket is on its way out;
// a run that is stopping (ctx done) hands nothing over, and the end-of-phase
// pass finds the file instead.
func (r *runner) handOff(ctx context.Context, label string) {
	r.mu.Lock()
	s, ok := r.sweeper, r.sweepable[label]
	r.mu.Unlock()
	if s == nil || !ok {
		return
	}
	select {
	case s.queue <- label:
	case <-ctx.Done():
	}
}

// sweepBuckets ends the sweep once the phase's last process has ended: it
// waits for the sweepers to finish what was handed to them, then removes what
// they had not reached, and logs what it kept and how many of the passing
// processes' buckets are gone. It does nothing for a runner that does not
// sweep. A run that was stopped (ctx is done) removes nothing more and says
// where the names are, so scripts/proof-clean.sh and a person can. A removal
// that fails is logged and its file kept; nothing here fails the proof.
func (r *runner) sweepBuckets(ctx context.Context, remove func(context.Context, string) error) {
	if !r.sweep {
		return
	}
	r.mu.Lock()
	s := r.sweeper
	r.sweeper = nil
	r.mu.Unlock()
	removedEarly, tried := 0, map[string]bool{}
	if s != nil {
		close(s.queue)
		s.wg.Wait()
		removedEarly, tried = s.removed, s.tried
	}

	files, _ := filepath.Glob(filepath.Join(r.jobDir, "*.bucket"))
	sort.Strings(files)
	var todo []sweepTarget
	for _, file := range files {
		label := strings.TrimSuffix(filepath.Base(file), ".bucket")
		if _, ok := tried[label]; ok {
			continue
		}
		p, why := r.target(label)
		if why != "" {
			r.logf("bucket sweep: %s", why)
			continue
		}
		todo = append(todo, p)
	}
	passing := len(tried) + len(todo)
	if passing == 0 {
		return
	}
	if removedEarly > 0 {
		r.logf("bucket sweep: %d passing job buckets were removed while the phase ran", removedEarly)
	}
	if len(todo) > 0 && ctx.Err() != nil {
		r.logf("bucket sweep: skipped (proofrun was stopped); %d passing job buckets are named in %s", len(todo), filepath.Join(r.jobDir, "*.bucket"))
		return
	}

	started := time.Now()
	queue := make(chan sweepTarget)
	var (
		wg      sync.WaitGroup
		removed = removedEarly
		mu      sync.Mutex
	)
	for i := 0; i < bucketSweepers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range queue {
				if r.removeTarget(ctx, p, remove) {
					mu.Lock()
					removed++
					mu.Unlock()
				}
			}
		}()
	}
	for _, p := range todo {
		if ctx.Err() != nil {
			break
		}
		queue <- p
	}
	close(queue)
	wg.Wait()
	r.logf("bucket sweep: removed %d of %d passing job buckets in %.0fs", removed, passing, time.Since(started).Seconds())
	if ctx.Err() != nil {
		r.logf("bucket sweep: stopped by a signal; what is left is named in %s", filepath.Join(r.jobDir, "*.bucket"))
	}
}

// bucketRemover makes the function the sweep removes one bucket with. Tests
// replace it.
var bucketRemover = s3Remover

// s3Remover removes buckets from the object store the environment names
// (artifacts.ConfigFromEnv, the one the test processes used). The client is
// made on the first removal, so a run with nothing to sweep never reaches for
// the store.
func s3Remover() func(context.Context, string) error {
	var (
		once   sync.Once
		client *s3.Client
		err    error
	)
	return func(ctx context.Context, bucket string) error {
		once.Do(func() { client, err = s3bucket.Client(ctx, artifacts.ConfigFromEnv()) })
		if err != nil {
			return fmt.Errorf("object store client: %w", err)
		}
		return s3bucket.Remove(ctx, client, bucket)
	}
}
