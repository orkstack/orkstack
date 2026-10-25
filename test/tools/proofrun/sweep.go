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
// removes the named buckets here, once, after the integration phase, a few at a
// time. A process with a failure keeps its bucket for the post-mortem, as it
// always did, and never writes the file.
//
// The sweep is housekeeping: a bucket it cannot remove costs disk, never a
// verdict, because no later run reads it. It is the runner's own, not the
// tests': a test never removes a bucket it does not own, and the sweep removes
// only a bucket named by a file the runner asked a passing process to write.

// bucketSweepers is how many buckets the sweep removes at once.
const bucketSweepers = 4

// bucketRemoveTimeout bounds one bucket's removal.
const bucketRemoveTimeout = 2 * time.Minute

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

// sweepBuckets removes the buckets the passing processes of this run left. It
// does nothing for a runner that does not sweep. A run that was stopped (ctx is
// done) removes nothing and says where the names are, so scripts/proof-clean.sh
// and a person can. A removal that fails is logged and its file kept; nothing
// here fails the proof.
func (r *runner) sweepBuckets(ctx context.Context, remove func(context.Context, string) error) {
	if !r.sweep {
		return
	}
	files, _ := filepath.Glob(filepath.Join(r.jobDir, "*.bucket"))
	sort.Strings(files)

	type pending struct{ label, bucket, file string }
	var todo []pending
	r.mu.Lock()
	sweepable := make(map[string]bool, len(r.sweepable))
	for k, v := range r.sweepable {
		sweepable[k] = v
	}
	r.mu.Unlock()
	for _, file := range files {
		label := strings.TrimSuffix(filepath.Base(file), ".bucket")
		data, err := os.ReadFile(file)
		bucket := strings.TrimSpace(string(data))
		switch {
		case err != nil:
			r.logf("bucket sweep: kept the bucket of %s: cannot read %s: %v", label, file, err)
		case !sweepable[label]:
			r.logf("bucket sweep: kept s3://%s of %s: not every test of it passed", bucket, label)
		case !s3bucket.IsName(bucket):
			r.logf("bucket sweep: kept %q of %s: not the name of a bucket a test process makes", bucket, label)
		default:
			todo = append(todo, pending{label, bucket, file})
		}
	}
	if len(todo) == 0 {
		return
	}
	if ctx.Err() != nil {
		r.logf("bucket sweep: skipped (proofrun was stopped); %d passing job buckets are named in %s", len(todo), filepath.Join(r.jobDir, "*.bucket"))
		return
	}

	started := time.Now()
	queue := make(chan pending)
	var (
		wg      sync.WaitGroup
		removed int
		mu      sync.Mutex
	)
	for i := 0; i < bucketSweepers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range queue {
				rctx, cancel := context.WithTimeout(ctx, bucketRemoveTimeout)
				err := remove(rctx, p.bucket)
				cancel()
				if err != nil {
					r.logf("bucket sweep: left s3://%s of %s: %v", p.bucket, p.label, err)
					continue
				}
				if err := os.Remove(p.file); err != nil {
					r.logf("bucket sweep: removed s3://%s of %s, but not its file: %v", p.bucket, p.label, err)
				}
				mu.Lock()
				removed++
				mu.Unlock()
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
	r.logf("bucket sweep: removed %d of %d passing job buckets in %.0fs", removed, len(todo), time.Since(started).Seconds())
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
