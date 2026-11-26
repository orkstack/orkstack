package sourcehost

import (
	"fmt"
	"sync"
	"time"

	"github.com/orkstack/orkstack/pkg/logstream"
)

// logHeadBytes is what an attempt's log always holds, whatever is printed after
// it.
const logHeadBytes = 1 << 20

// LogWindow keeps an instance's log within its budget: the first megabyte of an
// attempt, then so many bytes an hour of each stream. What is over is dropped,
// and the log says so. The Orker's own lines are kept.
type LogWindow struct {
	// PerHour is each stream's budget, and Setting what an operator sets it by.
	PerHour int64
	Setting string
	Write   func(line string)
	Now     func() time.Time

	mu      sync.Mutex
	head    int64
	streams [2]logStream
}

type logStream struct {
	since   time.Time
	used    int64
	dropped int64
}

var streamNames = [2]string{"stdout", "stderr"}

// Line takes one line as a LineWriter frames it.
func (w *LogWindow) Line(line string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	size := int64(len(line)) + 1
	i := 0
	if _, stderr := logstream.DecodeStreamLine(line); stderr {
		i = 1
	}
	s, now := &w.streams[i], w.Now()
	if w.head+size <= logHeadBytes {
		w.head += size
		w.Write(line)
		return
	}
	// The head is the first megabyte, never what still fits after it.
	w.head = logHeadBytes
	if s.since.IsZero() || now.Sub(s.since) >= time.Hour {
		w.account(i)
		*s = logStream{since: now}
	}
	if s.dropped > 0 || s.used+size > w.PerHour {
		if s.dropped == 0 {
			w.Write(fmt.Sprintf("[ork] %s passed its %d bytes an hour (%s); what it prints until %s is dropped",
				streamNames[i], w.PerHour, w.Setting, s.since.Add(time.Hour).UTC().Format(time.RFC3339)))
		}
		s.dropped += size
		return
	}
	s.used += size
	w.Write(line)
}

// Orker writes one of the Orker's own lines.
func (w *LogWindow) Orker(line string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.Write(line)
}

// Close says what was dropped and not said yet.
func (w *LogWindow) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.streams {
		w.account(i)
		w.streams[i].dropped = 0
	}
}

// account says how much of a stream was dropped in the hour that ended. Locked.
func (w *LogWindow) account(i int) {
	if n := w.streams[i].dropped; n > 0 {
		w.Write(fmt.Sprintf("[ork] %d bytes of %s were dropped", n, streamNames[i]))
	}
}
