package listenaddr_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/listenaddr"
)

func TestFind(t *testing.T) {
	cases := []struct {
		name, log, msg, want string
		found                bool
	}{
		{"text", `time=2026-11-20T10:00:00Z level=INFO msg="api listener ready" addr=127.0.0.1:41234` + "\n", listenaddr.API, "127.0.0.1:41234", true},
		{"text among others", "level=INFO msg=\"admin listener ready\" addr=127.0.0.1:9464 metrics=true\nlevel=INFO msg=\"api listener ready\" addr=[::1]:8080\n", listenaddr.API, "[::1]:8080", true},
		{"admin, not api", `level=INFO msg="admin listener ready" addr=127.0.0.1:9464 metrics=true`, listenaddr.Admin, "127.0.0.1:9464", true},
		{"json", `{"time":"2026-11-20T10:00:00Z","level":"INFO","msg":"api listener ready","addr":"127.0.0.1:5555"}`, listenaddr.API, "127.0.0.1:5555", true},
		{"another line names an addr", `level=INFO msg="API listener" addr=127.0.0.1:0 idle_timeout=2m0s`, listenaddr.API, "", false},
		{"a prefix of the message is not it", `level=INFO msg="api listener ready soon" addr=127.0.0.1:1`, listenaddr.API, "", false},
		{"nothing yet", "level=INFO msg=booting\n", listenaddr.API, "", false},
		{"a quoted addr", `level=INFO msg="api listener ready" addr="127.0.0.1:7777"`, listenaddr.API, "127.0.0.1:7777", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := listenaddr.Find(c.log, c.msg)
			if ok != c.found || got != c.want {
				t.Fatalf("Find = %q, %v; want %q, %v", got, ok, c.want, c.found)
			}
		})
	}
}

// Addr returns once the child logs its address, however the writes are cut.
func TestLog_AddrWaitsForTheLine(t *testing.T) {
	var log listenaddr.Log
	exited := make(chan struct{})
	got := make(chan string, 1)
	go func() {
		addr, err := log.Addr(context.Background(), "ork", listenaddr.API, exited, 10*time.Second)
		if err != nil {
			t.Error(err)
		}
		got <- addr
	}()
	_, _ = log.Write([]byte("level=INFO msg=booting\nlevel=INFO msg=\"api listener re"))
	_, _ = log.Write([]byte("ady\" addr=127.0.0.1:40001\n"))
	select {
	case addr := <-got:
		if addr != "127.0.0.1:40001" {
			t.Fatalf("Addr = %q", addr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Addr did not return after the line was written")
	}
}

// A child that exits before it logs its address fails the wait at once, with
// its log.
func TestLog_AddrFailsWhenTheChildExits(t *testing.T) {
	var log listenaddr.Log
	_, _ = log.Write([]byte("level=ERROR msg=\"listen on 127.0.0.1:0: address in use\"\n"))
	exited := make(chan struct{})
	close(exited)
	_, err := log.Addr(context.Background(), "ork-A", listenaddr.API, exited, time.Hour)
	if err == nil || !strings.Contains(err.Error(), "ork-A exited") || !strings.Contains(err.Error(), "address in use") {
		t.Fatalf("Addr error = %v; want the exit, naming the child and carrying its log", err)
	}
}

func TestLog_AddrFailsAfterItsDeadline(t *testing.T) {
	var log listenaddr.Log
	_, err := log.Addr(context.Background(), "ork", listenaddr.API, make(chan struct{}), 10*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "did not log") {
		t.Fatalf("Addr error = %v; want the deadline", err)
	}
}
