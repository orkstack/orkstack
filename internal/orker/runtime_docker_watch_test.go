package orker

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// callWatch is a live feed of the invocations a countingDocker receives, so a
// test that must act while a call is in flight waits on the call itself, not
// on a clock. The stand-in writes each argv line to a pipe the watch holds
// open, and the watch keeps every line: none is forgotten between two waits.
type callWatch struct {
	mu      sync.Mutex
	lines   []string
	changed chan struct{} // closed, and replaced, when a line arrives
}

// watch starts the feed. It is made before the calls it will be asked about:
// the stand-in writes to the pipe only while the pipe exists.
func (f *countingDocker) watch() *callWatch {
	f.t.Helper()
	pipe := filepath.Join(f.dir, "live")
	if err := syscall.Mkfifo(pipe, 0o600); err != nil {
		f.t.Fatalf("make the stand-in's call feed: %v", err)
	}
	// Opened for reading and writing, so the open does not wait for a writer
	// and a writer never waits for a reader: the watch is its own reader for
	// as long as it lives.
	file, err := os.OpenFile(pipe, os.O_RDWR, 0)
	if err != nil {
		f.t.Fatalf("open the stand-in's call feed: %v", err)
	}
	w := &callWatch{changed: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			w.mu.Lock()
			w.lines = append(w.lines, scanner.Text())
			close(w.changed)
			w.changed = make(chan struct{})
			w.mu.Unlock()
		}
	}()
	f.t.Cleanup(func() {
		// Remove the pipe first, so no later invocation writes to it, then
		// close it, which ends the reader.
		_ = os.Remove(pipe)
		_ = file.Close()
		<-done
	})
	return w
}

// waitFor returns once the stand-in has been invoked with an argv holding
// every one of words, whether before or after the call, and fails the test at
// a deadline.
func (w *callWatch) waitFor(t *testing.T, words ...string) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	seen := 0
	for {
		w.mu.Lock()
		for ; seen < len(w.lines); seen++ {
			found := true
			for _, word := range words {
				found = found && strings.Contains(w.lines[seen], word)
			}
			if found {
				w.mu.Unlock()
				return
			}
		}
		changed, all := w.changed, strings.Join(w.lines, "\n")
		w.mu.Unlock()
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatalf("no call holding %q within 30 s:\n%s", words, all)
		}
	}
}
