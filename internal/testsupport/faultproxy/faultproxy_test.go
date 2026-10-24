package faultproxy_test

import (
	"bufio"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/faultproxy"
)

// echoServer answers every line it reads with the same line.
func echoServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener.Addr().String()
}

// roundTrip writes one line through addr and reads it back within d.
func roundTrip(addr string, d time.Duration) error {
	conn, err := net.DialTimeout("tcp", addr, d)
	if err != nil {
		return err
	}
	defer conn.Close()
	return roundTripOn(conn, d)
}

func roundTripOn(conn net.Conn, d time.Duration) error {
	if err := conn.SetDeadline(time.Now().Add(d)); err != nil {
		return err
	}
	if _, err := conn.Write([]byte("ping\n")); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}
	if line != "ping\n" {
		return errors.New("echo returned " + line)
	}
	return nil
}

func TestProxy_ForwardsUntilCutAndAgainAfterRestore(t *testing.T) {
	p := faultproxy.New(t, echoServer(t))
	live, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if err := roundTripOn(live, 2*time.Second); err != nil {
		t.Fatalf("forwarding: %v", err)
	}

	p.Cut()
	// The connection that was open is severed...
	if err := roundTripOn(live, 2*time.Second); err == nil {
		t.Fatal("a connection open at the cut still forwards")
	}
	// ...and a new one fails fast, never waiting out its deadline.
	start := time.Now()
	if err := roundTrip(p.Addr(), 5*time.Second); err == nil {
		t.Fatal("a connection made while cut forwarded")
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("a refused connection took %s to fail, want fast", waited)
	}

	p.Restore()
	if err := roundTrip(p.Addr(), 2*time.Second); err != nil {
		t.Fatalf("after restore: %v", err)
	}
	stats := p.Stats()
	if stats.Refused == 0 || stats.Severed == 0 || stats.Forwarded < 2 {
		t.Fatalf("stats %+v: want a refusal, a severed connection and two forwards", stats)
	}
	if got := len(stats.Changes); got != 3 {
		t.Fatalf("%d mode changes recorded, want forwarding, cut, forwarding", got)
	}
}

func TestProxy_StallHoldsAConnectionUntilItsClientGivesUp(t *testing.T) {
	p := faultproxy.New(t, echoServer(t))
	p.Stall()
	start := time.Now()
	err := roundTrip(p.Addr(), 700*time.Millisecond)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("a stalled connection returned %v, want the client's own deadline", err)
	}
	if waited := time.Since(start); waited < 600*time.Millisecond {
		t.Fatalf("a stalled connection failed after %s, before its client's deadline", waited)
	}
	if p.Mode() != faultproxy.Stalled || p.Stats().Stalled != 1 {
		t.Fatalf("mode %v, stats %+v", p.Mode(), p.Stats())
	}

	p.Restore()
	if err := roundTrip(p.Addr(), 2*time.Second); err != nil {
		t.Fatalf("after restore: %v", err)
	}
}

func TestProxy_CloseEndsEveryConnection(t *testing.T) {
	p, err := faultproxy.Listen(echoServer(t))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", p.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := roundTripOn(conn, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { p.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return with a connection open")
	}
	if err := roundTripOn(conn, time.Second); err == nil {
		t.Fatal("a connection still forwards after Close")
	}
	if _, err := net.DialTimeout("tcp", p.Addr(), time.Second); err == nil {
		t.Fatal("the proxy still accepts after Close")
	}
}
