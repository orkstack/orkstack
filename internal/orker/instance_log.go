package orker

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/orkstack/orkstack/internal/orker/sourcehost"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// InstanceLogStore is where an instance attempt's log chunks go.
type InstanceLogStore interface {
	PutInstanceLogChunk(ctx context.Context, a artifacts.InstanceAttempt, seq int64, data []byte) error
}

// instanceChunks writes a sink's chunks as an instance attempt's.
type instanceChunks struct {
	store   InstanceLogStore
	attempt artifacts.InstanceAttempt
	stored  atomic.Int64
}

func (c *instanceChunks) PutLogChunk(ctx context.Context, chunk artifacts.LogChunk) error {
	err := c.store.PutInstanceLogChunk(ctx, c.attempt, chunk.Seq, chunk.Data)
	if err == nil {
		c.stored.Add(int64(len(chunk.Data)))
	}
	return err
}

// instanceLog is one instance attempt's log: a rolling window over the sink a
// task's log has, with no cap on the attempt, because an instance does not end.
type instanceLog struct {
	window *sourcehost.LogWindow
	sink   *logChunkSink
	chunks *instanceChunks
}

func newInstanceLog(ctx context.Context, store InstanceLogStore, attempt artifacts.InstanceAttempt, opts LogOptions, perHour int64, failed func(error)) *instanceLog {
	opts.MaxBytes = math.MaxInt64
	chunks := &instanceChunks{store: store, attempt: attempt}
	sink := newLogChunkSink(ctx, chunks, artifacts.LogRef{RunID: attempt.SourceID, TaskID: "_source"}, opts).withErrorReporter(failed)
	return &instanceLog{sink: sink, chunks: chunks, window: &sourcehost.LogWindow{PerHour: perHour, Setting: SourceHostLogBytesPerHourVar, Write: sink.Write, Now: time.Now}}
}

// Line is a LineWriter's logger: one line of the container's, framed.
func (l *instanceLog) Line(format string, args ...any) { l.window.Line(fmt.Sprintf(format, args...)) }

// Orker writes one of the Orker's own lines.
func (l *instanceLog) Orker(format string, args ...any) {
	l.window.Orker("[ork] " + fmt.Sprintf(format, args...))
}

// Bytes is how much of the log is stored.
func (l *instanceLog) Bytes() int64 { return l.chunks.stored.Load() }

// Close stores what is buffered, and says what was dropped.
func (l *instanceLog) Close() {
	l.window.Close()
	l.sink.Close()
}
