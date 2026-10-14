// Package faultproxy simulates an outage of one upstream service for one
// integration test, without touching the service itself.
//
// The development stack's Temporal (`start-dev`) keeps its state in memory,
// and its MinIO and registry are shared by every suite and every proof on the
// host (AGENTS.md §11.1): stopping either would disturb tests the outage is
// not about. A test that needs "object storage unreachable" or "the Temporal
// frontend unreachable" instead points the clients under test at a Proxy and
// cuts the proxy. The test's own control client dials the upstream directly,
// so it keeps signalling, cancelling and reading history throughout.
//
// The proxy forwards TCP, byte for byte, in three modes:
//
//   - forwarding (the initial mode, and the one Restore returns to);
//   - cut: every live connection is closed and every new one is closed as
//     soon as it is accepted, so a client fails fast (connection reset);
//   - stalled: every live connection is closed, and every new one is accepted
//     and held open with nothing forwarded either way, so a client blocks
//     until its own deadline.
//
// Every mode change is counted, as is every connection by the mode it met,
// so a test can quote what its clients saw.
package faultproxy

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// Mode is what the proxy does with a connection.
type Mode int

const (
	// Forwarding copies bytes between the client and the upstream.
	Forwarding Mode = iota
	// Cut closes every connection.
	Cut
	// Stalled holds every connection open and forwards nothing.
	Stalled
)

func (m Mode) String() string {
	switch m {
	case Forwarding:
		return "forwarding"
	case Cut:
		return "cut"
	case Stalled:
		return "stalled"
	}
	return "unknown"
}

// Stats is what the proxy saw: connections by the mode they met, and the
// mode changes it was asked for.
type Stats struct {
	Forwarded   int // connections accepted and forwarded
	Refused     int // connections closed on accept while cut
	Stalled     int // connections accepted and held while stalled
	Severed     int // live connections closed by Cut or Stall
	DialFailure int // connections whose upstream could not be dialled
	Changes     []Change
}

// Change is one mode change, with when it happened.
type Change struct {
	At   time.Time `json:"at"`
	Mode string    `json:"mode"`
}

// Proxy is one TCP forwarder in front of one upstream address.
type Proxy struct {
	upstream string
	listener net.Listener

	mu     sync.Mutex
	mode   Mode
	live   map[net.Conn]struct{} // every client and upstream connection open now
	stats  Stats
	closed bool
	wg     sync.WaitGroup
}

// New listens on 127.0.0.1:0 and forwards to upstream ("host:port"). The
// proxy is closed by t.Cleanup.
func New(t testing.TB, upstream string) *Proxy {
	t.Helper()
	p, err := Listen(upstream)
	if err != nil {
		t.Fatalf("faultproxy: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// Listen is New without a test: the caller closes the proxy.
func Listen(upstream string) (*Proxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{upstream: upstream, listener: listener, live: map[net.Conn]struct{}{}}
	p.stats.Changes = []Change{{At: time.Now(), Mode: Forwarding.String()}}
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

// Addr is the address the clients under test dial instead of the upstream.
func (p *Proxy) Addr() string { return p.listener.Addr().String() }

// Mode reports the proxy's current mode.
func (p *Proxy) Mode() Mode {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode
}

// Cut closes every live connection and refuses every new one until Restore.
func (p *Proxy) Cut() { p.set(Cut) }

// Stall closes every live connection, and holds every new one open with
// nothing forwarded until Restore or Cut.
func (p *Proxy) Stall() { p.set(Stalled) }

// Restore forwards again. Connections held by Stall are closed, so their
// clients retry on a fresh connection.
func (p *Proxy) Restore() { p.set(Forwarding) }

// Stats is a copy of what the proxy saw so far.
func (p *Proxy) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := p.stats
	out.Changes = append([]Change(nil), p.stats.Changes...)
	return out
}

// Close stops the proxy and closes every connection.
func (p *Proxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.severLocked()
	p.mu.Unlock()
	_ = p.listener.Close()
	p.wg.Wait()
}

func (p *Proxy) set(mode Mode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	// Every change severs what is open: a cut or a stall must reach a client
	// already connected, and a restore must not leave a stalled client
	// holding a connection that will never forward.
	p.severLocked()
	p.mode = mode
	p.stats.Changes = append(p.stats.Changes, Change{At: time.Now(), Mode: mode.String()})
}

func (p *Proxy) severLocked() {
	for conn := range p.live {
		_ = conn.Close()
		p.stats.Severed++
	}
	p.live = map[net.Conn]struct{}{}
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		p.mu.Lock()
		mode, closed := p.mode, p.closed
		switch {
		case closed || mode == Cut:
			p.stats.Refused++
			p.mu.Unlock()
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetLinger(0) // a reset, so the client fails at once
			}
			_ = conn.Close()
			continue
		case mode == Stalled:
			p.stats.Stalled++
			p.live[conn] = struct{}{} // held until the next mode change
			p.mu.Unlock()
			continue
		}
		p.stats.Forwarded++
		p.live[conn] = struct{}{}
		p.mu.Unlock()
		p.wg.Add(1)
		go p.forward(conn)
	}
}

func (p *Proxy) forward(client net.Conn) {
	defer p.wg.Done()
	upstream, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		p.mu.Lock()
		p.stats.DialFailure++
		delete(p.live, client)
		p.mu.Unlock()
		_ = client.Close()
		return
	}
	p.mu.Lock()
	if _, ok := p.live[client]; !ok {
		// Severed while the upstream was being dialled.
		p.mu.Unlock()
		_ = upstream.Close()
		_ = client.Close()
		return
	}
	p.live[upstream] = struct{}{}
	p.mu.Unlock()

	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	go pipe(upstream, client)
	go pipe(client, upstream)
	<-done
	_ = client.Close()
	_ = upstream.Close()
	<-done
	p.mu.Lock()
	delete(p.live, client)
	delete(p.live, upstream)
	p.mu.Unlock()
}
