// Package tlsfront terminates TLS in front of a plaintext server for one
// test: the proof's own Temporal serves cleartext HTTP/2, so a Front with a
// certificate (and, for mutual TLS, a CA every client must present a
// certificate of) stands between the client under test and it, and copies
// bytes. The development server stays as it is; HTTP/2 over TLS, terminated,
// is the cleartext HTTP/2 it already serves.
//
// A Front records every handshake: the client certificate it was shown, or
// the failure. A test reads them to show which credential a client used, and
// that a client it should refuse was refused.
package tlsfront

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"
)

// Options say what a Front presents and demands.
type Options struct {
	// Upstream is the plaintext server's address.
	Upstream string
	// Certificate is the Front's own.
	Certificate tls.Certificate
	// ClientCAs, when set, are the roots a client's certificate must chain
	// to; a client without one is refused at the handshake.
	ClientCAs *x509.CertPool
}

// Handshake is one client's handshake.
type Handshake struct {
	// Client is the serial number of the certificate the client presented,
	// nil when it presented none.
	Client *big.Int
	// Err is the handshake's failure, nil when it completed.
	Err error
}

// Front is a running TLS terminator.
type Front struct {
	// Addr is where clients dial it.
	Addr string

	ln         net.Listener
	upstream   string
	mu         sync.Mutex
	handshakes []Handshake
	conns      map[net.Conn]struct{}
	closed     bool
	wg         sync.WaitGroup
}

// Start listens on a free loopback port until the test ends.
func Start(t testing.TB, o Options) *Front {
	t.Helper()
	cfg := &tls.Config{
		Certificates: []tls.Certificate{o.Certificate},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"}, // gRPC requires ALPN h2
	}
	if o.ClientCAs != nil {
		cfg.ClientCAs = o.ClientCAs
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tlsfront: listen: %v", err)
	}
	f := &Front{Addr: ln.Addr().String(), ln: ln, upstream: o.Upstream, conns: map[net.Conn]struct{}{}}
	f.wg.Add(1)
	go f.accept()
	t.Cleanup(f.Close)
	return f
}

// Handshakes are the handshakes so far, in the order they ended.
func (f *Front) Handshakes() []Handshake {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Handshake(nil), f.handshakes...)
}

// Close stops listening and closes every connection. It may be called more
// than once.
func (f *Front) Close() {
	_ = f.ln.Close()
	f.mu.Lock()
	f.closed = true
	for c := range f.conns {
		_ = c.Close()
	}
	f.mu.Unlock()
	f.wg.Wait()
}

// Drop closes every live connection and keeps listening, so each client
// reconnects and makes a new handshake.
func (f *Front) Drop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.conns {
		_ = c.Close()
	}
}

func (f *Front) accept() {
	defer f.wg.Done()
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.wg.Add(1)
		go f.serve(conn.(*tls.Conn))
	}
}

// track adds or removes a live connection; an add after Close is refused,
// and the caller closes the connection.
func (f *Front) track(c net.Conn, on bool) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !on {
		delete(f.conns, c)
		return true
	}
	if f.closed {
		return false
	}
	f.conns[c] = struct{}{}
	return true
}

func (f *Front) serve(c *tls.Conn) {
	defer f.wg.Done()
	defer c.Close()
	if !f.track(c, true) {
		return
	}
	defer f.track(c, false)

	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	err := c.Handshake()
	_ = c.SetDeadline(time.Time{})
	h := Handshake{Err: err}
	if err == nil {
		if peers := c.ConnectionState().PeerCertificates; len(peers) > 0 {
			h.Client = peers[0].SerialNumber
		}
	}
	f.mu.Lock()
	f.handshakes = append(f.handshakes, h)
	f.mu.Unlock()
	if err != nil {
		return
	}

	up, err := net.DialTimeout("tcp", f.upstream, 10*time.Second)
	if err != nil {
		return
	}
	defer up.Close()
	if !f.track(up, true) {
		return
	}
	defer f.track(up, false)

	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	go pipe(up, c)
	go pipe(c, up)
	<-done
	// One direction ended: close both, so the other copy ends too.
	_ = c.Close()
	_ = up.Close()
	<-done
}
