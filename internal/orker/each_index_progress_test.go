package orker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/orkstack/orkstack/internal/eachlines"
	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// TestIndexEachLines_HeartbeatsProgress: the indexer reports how far it has
// read, in bytes and non-empty lines, as it reads, ending at the whole
// source; and the attempt's heartbeat carries the last report.
func TestIndexEachLines_HeartbeatsProgress(t *testing.T) {
	rig := newLinesRig(t, fiveRows)
	var mu sync.Mutex
	var reports [][2]int64
	_, err := indexEachLines(context.Background(), artifacts.Namespaces{}, rig.store, rig.indexes, rig.input(1, 10),
		slog.New(slog.DiscardHandler), func(read, lines int64) {
			mu.Lock()
			reports = append(reports, [2]int64{read, lines})
			mu.Unlock()
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) == 0 {
		t.Fatal("the indexer reported no progress")
	}
	for i := 1; i < len(reports); i++ {
		if reports[i][0] < reports[i-1][0] || reports[i][1] < reports[i-1][1] {
			t.Fatalf("progress went backwards: %v", reports)
		}
	}
	// fiveRows' last line has no LF: it is counted once the read has ended,
	// after the last report, which therefore names the four lines before it.
	if last := reports[len(reports)-1]; last[0] != int64(len(fiveRows)) || last[1] != 4 {
		t.Fatalf("last progress = %d bytes, %d lines; want %d bytes, 4 lines", last[0], last[1], len(fiveRows))
	}

	// The activity itself: its beat names the phase and carries the read.
	a := NewActivitiesWithOptions(rig.store, Options{OrkerID: cordonOrkerID, Pool: "build", WorkRoot: t.TempDir(),
		Runtime: &reapRecorder{labelled: map[string]int{}}, EachIndexes: rig.indexes})
	beat := &Heartbeater{}
	in := rig.input(1, 10)
	in.TaskId = "convert-beat" // its own index key
	if _, err := a.indexWith(context.Background(), in, beat); err != nil {
		t.Fatal(err)
	}
	got := beat.progress()
	if got.Phase != PhaseIndexing || got.IndexBytesRead != int64(len(fiveRows)) || got.IndexLines != 4 {
		t.Fatalf("heartbeat = %+v; want the indexing phase with %d bytes and 4 lines", got, len(fiveRows))
	}
}

// countedBody counts the bytes a ranged read hands the indexer.
type countedBody struct {
	io.ReadCloser
	n *atomic.Int64
}

func (b countedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.n.Add(int64(n))
	return n, err
}

// countedRanges is a store whose ranged reads are counted as they are read.
type countedRanges struct {
	artifacts.Store
	n atomic.Int64
}

func (c *countedRanges) GetRange(ctx context.Context, uri string, offset, length int64) (io.ReadCloser, error) {
	body, err := c.Store.GetRange(ctx, uri, offset, length)
	if err != nil {
		return nil, err
	}
	return countedBody{ReadCloser: body, n: &c.n}, nil
}

// TestIndexEachLines_StopsOnCancelWithinOneBuffer: a cancel in the middle of
// a source stops the read before the indexer takes more than one more read
// buffer, writes no index, and is retried.
func TestIndexEachLines_StopsOnCancelWithinOneBuffer(t *testing.T) {
	var b strings.Builder
	for i := 0; b.Len() < 24<<20; i++ {
		fmt.Fprintf(&b, "{\"n\":%d}\n", i)
	}
	rig := newLinesRig(t, b.String())
	in := rig.input(1_000, 10_000) // up to 10,000,000 lines; the source holds ~2.4 million
	in.SliceMaxBytes = 64 << 20
	store := &countedRanges{Store: rig.store}
	indexes := &countingIndexes{inner: rig.indexes}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const cancelAt = 4 << 20
	var atCancel atomic.Int64
	_, err := indexEachLines(ctx, artifacts.Namespaces{}, store, indexes, in, slog.New(slog.DiscardHandler), func(read, _ int64) {
		if read >= cancelAt && atCancel.Load() == 0 {
			atCancel.Store(store.n.Load())
			cancel()
		}
	})
	if atCancel.Load() == 0 {
		t.Fatalf("the index finished before it was cancelled: %v", err)
	}
	if typ, retry := applicationError(t, err); typ != eachlines.ReasonExpansionFailed || !retry {
		t.Fatalf("= %s retryable=%v; want EACH_EXPANSION_FAILED, retried", typ, retry)
	}
	if after := store.n.Load() - atCancel.Load(); after > eachlines.ReadBufferSize {
		t.Fatalf("read %d bytes after the cancel; want at most one %d-byte buffer", after, eachlines.ReadBufferSize)
	}
	if store.n.Load() >= int64(b.Len()) {
		t.Fatalf("read all %d bytes of the source despite the cancel", store.n.Load())
	}
	if n := indexes.calls.Load(); n != 0 {
		t.Fatalf("the line-index store was asked %d times after a cancel", n)
	}
}

// TestDrain_WaitsForIndex: while the line indexer runs, the Orker reports it
// as a running task — what `orkctl orker drain` waits on — and a stopping
// Orker's grace does not end; once the index answers, the stop ends idle. The
// activity registered under the indexer's name is a stand-in that blocks: the
// cordon interceptor counts by activity type, which is what is under test.
func TestDrain_WaitsForIndex(t *testing.T) {
	a := newCordonActivities(t, t.TempDir())
	env := activityEnv(a)
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	free := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(free)
	env.RegisterActivityWithOptions(func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-time.After(stopDeadline):
			return errors.New("never released")
		}
	}, activity.RegisterOptions{Name: task.ActivityIndexEachLines})
	done := make(chan error, 1)
	go func() {
		_, err := env.ExecuteActivity(task.ActivityIndexEachLines)
		done <- err
	}()
	<-started

	if got := a.RunningTasks(); got != 1 {
		t.Fatalf("running_tasks while indexing = %d; want 1", got)
	}
	var stopped atomic.Bool
	out := runStop(a, StopSequence{Grace: 24 * time.Hour, StopWorker: func() { stopped.Store(true) }})
	// Positive control: the stop has begun while the index still runs.
	if _, ok := a.Cordon().Stopping(); !ok {
		t.Fatal("the stop did not set the stopping cordon")
	}
	if stopped.Load() {
		t.Fatal("the worker was stopped while an index ran inside the grace")
	}
	free()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got := awaitStop(t, out)
	if got.Ended != GraceEndedIdle || !stopped.Load() || a.RunningTasks() != 0 {
		t.Fatalf("outcome = %+v stopped=%v running=%d; want an idle end once the index answered", got, stopped.Load(), a.RunningTasks())
	}
}

// lateIndex is a line-index store that answers not found the first time an
// index is asked for: a multi-filer store showing a new object late, a fault
// the local store cannot produce on demand.
type lateIndex struct {
	eachIndexReader
	misses atomic.Int32
}

func (l *lateIndex) Get(ctx context.Context, uri string, maxBytes int64) ([]byte, artifacts.ArtifactRef, error) {
	if l.misses.Add(1) == 1 {
		return nil, artifacts.ArtifactRef{}, fmt.Errorf("%s: %w", uri, artifacts.ErrNotFound)
	}
	return l.eachIndexReader.Get(ctx, uri, maxBytes)
}

// fixedIndex is a line-index store that holds one index, whatever is asked:
// a forged index no indexer writes.
type fixedIndex []byte

func (f fixedIndex) Get(context.Context, string, int64) ([]byte, artifacts.ArtifactRef, error) {
	return f, artifacts.ArtifactRef{}, nil
}

// TestExecute_LinesIndexAnswers: an index the store shows late is retried and
// then read; a batch whose span holds fewer lines than its index entry says is
// EACH_ITEM_UNAVAILABLE, its slice file removed.
func TestExecute_LinesIndexAnswers(t *testing.T) {
	ctx := context.Background()
	rig := newLinesRig(t, fiveRows)
	t.Run("index 404 once then readable", func(t *testing.T) {
		got, err := rig.index(t, rig.input(1, 10))
		if err != nil {
			t.Fatal(err)
		}
		r := rig.resolver(t)
		r.indexes = &lateIndex{eachIndexReader: rig.indexes}
		r.last = func(context.Context) bool { return false }
		in := rig.itemInput(got, 1, 0)
		if _, f := r.resolve(ctx, in, &Workspace{Input: t.TempDir()}); f == nil || f.Reason != ReasonInputUnavailable || !RetryableReason(f.Reason) {
			t.Fatalf("the first attempt = %v; want a retried %s", f, ReasonInputUnavailable)
		}
		item, f := r.resolve(ctx, in, &Workspace{Input: t.TempDir()})
		if f != nil {
			t.Fatalf("the retry = %v; want the index read", f)
		}
		if string(item.Value) != `{"n":1}` {
			t.Fatalf("item 1 = %s; want {\"n\":1}", item.Value)
		}
	})
	t.Run("batch line count mismatch", func(t *testing.T) {
		in := rig.input(2, 10)
		in.TaskId = "convert-short" // one task has one batch size
		got, err := rig.index(t, in)
		if err != nil {
			t.Fatal(err)
		}
		body, _, err := rig.indexes.Get(ctx, got.GetIndex().GetUri(), 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		table, err := eachlines.Decode(body)
		if err != nil {
			t.Fatal(err)
		}
		// Item 0's span is cut to its first line, its digest recomputed: a
		// well-formed index whose entry says two lines its span does not hold.
		first := strings.Index(fiveRows, "\n")
		table.Entries[0].Length = uint32(first)
		table.Entries[0].SHA256 = sha256.Sum256([]byte(fiveRows[:first]))
		forged := table.Encode()
		if _, err := eachlines.Decode(forged); err != nil {
			t.Fatalf("the forged index does not decode: %v", err)
		}
		sum := sha256.Sum256(forged)
		item := rig.itemInput(got, 0, 2)
		item.Each.Lines.Index = cloneRef(item.Each.Lines.Index)
		item.Each.Lines.Index.Sha256 = hex.EncodeToString(sum[:])
		r := rig.resolver(t)
		r.indexes = fixedIndex(forged)
		ws := &Workspace{Input: t.TempDir()}
		_, f := r.resolve(ctx, item, ws)
		if f == nil || f.Reason != ReasonEachItemUnavailable || !strings.Contains(f.Error(), "holds 1 lines, the index says 2") {
			t.Fatalf("= %v; want EACH_ITEM_UNAVAILABLE naming the line counts", f)
		}
		if _, err := os.Stat(filepath.Join(ws.Input, "each", "lines.jsonl")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the slice of a mismatched batch was left: %v", err)
		}
	})
}
