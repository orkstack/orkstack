// Package listenaddr reads where a child process listens from that child's
// own log.
//
// A test that hands a child a port it found free (listen on :0, close, pass
// the port on) races every other process on the host: two tests' engines were
// handed the same port in the same second, one lost the bind and exited, and
// the other test's engine answered the loser's readiness probe (proof
// 44677556224). A child started on port 0 binds a port no other process
// holds, and its boot log names it: `ork` logs "api listener ready" and both
// binaries log "admin listener ready", each with addr=<host:port>. A test
// that reads the address from the log of the process it started can only
// ever reach that process.
package listenaddr

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// The boot log lines that name a listener's bound address.
const (
	// API is `ork`'s Connect listener (cmd/ork, bindAndServe).
	API = "api listener ready"
	// Admin is either binary's admin listener (internal/telemetry, StartAdmin).
	Admin = "admin listener ready"
)

// Loopback is the address a test starts a child's listener on: loopback,
// with the port left to the kernel.
const Loopback = "127.0.0.1:0"

// Closed is a loopback address nothing listens on, for a test that needs a
// connection refused: port 1 is privileged and unused, so no test process can
// come to hold it, as a port found free and closed again could.
const Closed = "127.0.0.1:1"

// Log is a child's combined output: the process's pipes write it, the test
// reads it while the process runs.
type Log struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	changed chan struct{}
}

// Write appends to the log and wakes every Addr waiting on it.
func (l *Log) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, err := l.buf.Write(p)
	if l.changed != nil {
		close(l.changed)
		l.changed = nil
	}
	return n, err
}

// String is everything written so far.
func (l *Log) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// snapshot is the log so far and a channel that closes on the next write.
func (l *Log) snapshot() (string, <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.changed == nil {
		l.changed = make(chan struct{})
	}
	return l.buf.String(), l.changed
}

// textAddr is the addr attribute of a slog text line, quoted or bare.
var textAddr = regexp.MustCompile(`(?:^|\s)addr=("(?:[^"\\]|\\.)*"|\S+)`)

// Find is the address the first line logging msg names, if one does. It
// reads slog's text form (msg="api listener ready" addr=127.0.0.1:41234)
// and its JSON form ("msg":"api listener ready",...,"addr":"127.0.0.1:41234").
func Find(log, msg string) (string, bool) {
	quoted := "msg=" + fmt.Sprintf("%q", msg)
	for _, line := range strings.Split(log, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "{") {
			var record struct {
				Msg  string `json:"msg"`
				Addr string `json:"addr"`
			}
			if json.Unmarshal([]byte(line), &record) == nil && record.Msg == msg && record.Addr != "" {
				return record.Addr, true
			}
			continue
		}
		if !strings.Contains(line, quoted+" ") && !strings.Contains(line, "msg="+msg+" ") {
			continue
		}
		m := textAddr.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		addr := m[1]
		if strings.HasPrefix(addr, `"`) {
			var unquoted string
			if json.Unmarshal([]byte(addr), &unquoted) != nil {
				continue
			}
			addr = unquoted
		}
		return addr, true
	}
	return "", false
}

// Addr waits until the child logs msg with the address it bound, and returns
// it. It fails at once when the process exits first, and after deadline
// otherwise; the error carries the log so far, so a failed boot says why.
func (l *Log) Addr(ctx context.Context, what, msg string, exited <-chan struct{}, deadline time.Duration) (string, error) {
	timer := time.NewTimer(deadline)
	defer timer.Stop()
	for {
		log, changed := l.snapshot()
		if addr, ok := Find(log, msg); ok {
			return addr, nil
		}
		select {
		case <-changed:
		case <-exited:
			// The exit may race its last write: read once more.
			if addr, ok := Find(l.String(), msg); ok {
				return addr, nil
			}
			return "", fmt.Errorf("%s exited before it logged %q; its log:\n%s", what, msg, l.String())
		case <-timer.C:
			return "", fmt.Errorf("%s did not log %q within %s; its log:\n%s", what, msg, deadline, l.String())
		case <-ctx.Done():
			return "", fmt.Errorf("waiting for %s to log %q: %w", what, msg, ctx.Err())
		}
	}
}
