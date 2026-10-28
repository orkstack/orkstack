//go:build integration

package engine_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
)

// What a live GetRun reads, counted beside its latency. A GetRun's latency
// is a wall-clock number a smaller host stretches; what it READS does not
// depend on the host. The soaks' wall-clock bound (soakGetRunP95Max) is
// enforced on the reference host and excused on a smaller CI runner
// (docs/PROOFS.md §12), so a structural slowdown there would be invisible if
// the bound were all there is. These are the bounds that stay hard on every
// host, whatever its speed.
//
// The reader is the API's own, over a client and a snapshot store that count:
// the history events it iterates and their decoded size, and the saved-state
// objects it reads and their size. Nothing about the reader changes.

// readCounter is what one reader has read so far.
type readCounter struct {
	events, eventBytes, snapshotReads, snapshotBytes atomic.Int64
}

type readTotals struct {
	Events, EventBytes, SnapshotReads, SnapshotBytes int64
}

func (c *readCounter) totals() readTotals {
	return readTotals{c.events.Load(), c.eventBytes.Load(), c.snapshotReads.Load(), c.snapshotBytes.Load()}
}

func (a readTotals) minus(b readTotals) readTotals {
	return readTotals{a.Events - b.Events, a.EventBytes - b.EventBytes, a.SnapshotReads - b.SnapshotReads, a.SnapshotBytes - b.SnapshotBytes}
}

// soakCountingClient is a Temporal client whose history iterators count the
// events they hand out.
type soakCountingClient struct {
	client.Client
	reads *readCounter
}

func (c soakCountingClient) GetWorkflowHistory(ctx context.Context, workflowID, runID string, isLongPoll bool,
	filterType enums.HistoryEventFilterType,
) client.HistoryEventIterator {
	return soakCountingIterator{HistoryEventIterator: c.Client.GetWorkflowHistory(ctx, workflowID, runID, isLongPoll, filterType), reads: c.reads}
}

type soakCountingIterator struct {
	client.HistoryEventIterator
	reads *readCounter
}

func (i soakCountingIterator) Next() (*historypb.HistoryEvent, error) {
	event, err := i.HistoryEventIterator.Next()
	if err == nil {
		i.reads.events.Add(1)
		i.reads.eventBytes.Add(int64(proto.Size(event)))
	}
	return event, err
}

// soakCountingSnapshots is a snapshot store that counts the objects read through
// it and their size.
type soakCountingSnapshots struct {
	engine.SnapshotStore
	reads *readCounter
}

func (c soakCountingSnapshots) ReadSnapshot(ctx context.Context, uri string) ([]byte, error) {
	data, err := c.SnapshotStore.ReadSnapshot(ctx, uri)
	c.reads.snapshotReads.Add(1)
	c.reads.snapshotBytes.Add(int64(len(data)))
	return data, err
}

func (c soakCountingSnapshots) ReadContinuationRange(ctx context.Context, uri string, wantSize, offset, length int64) ([]byte, error) {
	data, err := c.SnapshotStore.ReadContinuationRange(ctx, uri, wantSize, offset, length)
	c.reads.snapshotReads.Add(1)
	c.reads.snapshotBytes.Add(int64(len(data)))
	return data, err
}

// countedReader is a reader over this soak's cluster and store like the
// control reader, in all but counting what it reads into e.reads. The API
// the soak serves reads through it, and nothing else does.
func (e *eachSoak) countedReader() *engine.TemporalRunReader {
	e.reads = &readCounter{}
	return engine.NewTemporalRunReader(soakCountingClient{Client: e.control, reads: e.reads}).
		WithSnapshotStore(soakCountingSnapshots{SnapshotStore: e.store, reads: e.reads})
}

// liveGetRuns reads the live run through the API n times, soakGetRunSpacing
// apart as a page a reader keeps open re-reads it, timing each read and
// counting what it read. The counts are the API reader's alone: the soak
// makes no other API call while it reads, and its streams and checks read
// through the control reader.
func (e *eachSoak) liveGetRuns(t *testing.T, ctx context.Context, runID string, n int) ([]time.Duration, []getRunRead, *orkstackv1.Run) {
	t.Helper()
	var times []time.Duration
	var reads []getRunRead
	var last *orkstackv1.Run
	for i := 0; i < n; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(soakGetRunSpacing):
			}
		}
		beforeExecution, _ := e.historyOf(t, ctx, runID, "")
		counted := e.reads.totals()
		start := time.Now()
		resp, err := e.api.runs.GetRun(ctx, connect.NewRequest(&orkstackv1.GetRunRequest{RunId: runID}))
		took := time.Since(start)
		if err != nil {
			t.Fatalf("GetRun %s: %v", runID, err)
		}
		read := e.reads.totals().minus(counted)
		afterExecution, history := e.historyOf(t, ctx, runID, "")
		if afterExecution != beforeExecution {
			// The run switched generations during the read, which scans the
			// one it found and follows it into the next.
			_, ended := e.historyOf(t, ctx, runID, beforeExecution)
			history += ended
		}
		times = append(times, took)
		reads = append(reads, getRunRead{Seconds: took.Seconds(), Events: read.Events, EventBytes: read.EventBytes,
			SnapshotReads: read.SnapshotReads, SnapshotBytes: read.SnapshotBytes, History: history})
		last = resp.Msg.GetRun()
	}
	return times, reads, last
}

// historyOf is the execution the run is in (execution "") or the named one,
// and its history length in events.
func (e *eachSoak) historyOf(t *testing.T, ctx context.Context, runID, execution string) (string, int64) {
	t.Helper()
	described, err := e.control.DescribeWorkflowExecution(ctx, runID, execution)
	if err != nil {
		t.Fatalf("describe run %s execution %q: %v", runID, execution, err)
	}
	info := described.GetWorkflowExecutionInfo()
	return info.GetExecution().GetRunId(), info.GetHistoryLength()
}
