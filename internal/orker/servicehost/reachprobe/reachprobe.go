// Package reachprobe is the program the reach probe's containers run
// (docs/SECURITY_MODEL.md, "Service containers"; docs/DEPLOY.md, "Source
// hosts"): the release's probe image is a static binary of it and nothing
// else. It does one of two things.
//
//	probe dial [-timeout D] TARGET...   try each target, write one observation per line
//	probe listen PORT                   accept connections until stopped (a canary)
//
// A TARGET is QUESTION,KIND,VIA,HOST:PORT (servicehost's constants). The
// answer on stdout is what servicehost.ParseProbeAnswer reads; the program
// judges nothing, the Orker does (servicehost.JudgeProbe). Its argv is the
// Orker's, never a tenant's, but it is still bounded and strict.
//
// It is built on the standard library and grpc-go, which the module already
// pins: the gRPC question (would the cluster serve a caller with no
// credential) is answered by making the call. grpcurl and grpc-health-probe
// were weighed and not used: neither reports a TLS handshake refused for
// want of a client certificate apart from any other failure, and either
// would add a second binary, and a shell to drive it, to an image that must
// hold neither.
package reachprobe

import (
	"context"
	"crypto/tls"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
)

// Bounds of one run.
const (
	// MaxTargets bounds one dial run, below the answer's own bound.
	MaxTargets = 512
	// DefaultTimeout is one target's budget: the connection, the
	// handshake and the call together.
	DefaultTimeout = 3 * time.Second
	maxTimeout     = time.Minute
	maxAddr        = 256
	parallel       = 16
	// tls13Settle is how long the TLS 1.3 check waits for the server's
	// verdict on a client that sent no certificate: in TLS 1.3 the
	// client's side of the handshake completes before the server has
	// judged it, and a refusal arrives as an alert on the first read.
	tls13Settle = 500 * time.Millisecond
)

// Target is one thing to try.
type Target struct {
	Question int
	Kind     string
	Via      string
	Addr     string
}

// ParseTarget reads QUESTION,KIND,VIA,HOST:PORT.
func ParseTarget(s string) (Target, error) {
	if len(s) > maxAddr+32 {
		return Target{}, errors.New("target: an address is at most 256 bytes")
	}
	f := strings.Split(s, ",")
	if len(f) != 4 {
		return Target{}, fmt.Errorf("target %q: want four fields, QUESTION,KIND,VIA,HOST:PORT", s)
	}
	q, ok := canonicalInt(f[0])
	if !ok || q < servicehost.QuestionMetadata || q > servicehost.QuestionCanary {
		return Target{}, fmt.Errorf("target %q: the question is 1 to 4", s)
	}
	t := Target{Question: q, Kind: f[1], Via: f[2], Addr: f[3]}
	switch t.Kind {
	case servicehost.TargetService, servicehost.TargetMembership, servicehost.TargetAddress:
	default:
		return Target{}, fmt.Errorf("target %q: kind is service, membership or address", s)
	}
	switch t.Via {
	case servicehost.ViaTCP:
	case servicehost.ViaPlaintext, servicehost.ViaTLS:
		if t.Kind != servicehost.TargetService {
			return Target{}, fmt.Errorf("target %q: only a service port is called; others are tried by tcp", s)
		}
	default:
		return Target{}, fmt.Errorf("target %q: via is tcp, plaintext or tls", s)
	}
	if len(t.Addr) > maxAddr {
		return Target{}, errors.New("target: an address is at most 256 bytes")
	}
	host, port, err := net.SplitHostPort(t.Addr)
	if err != nil {
		return Target{}, fmt.Errorf("target %q: the address is host:port", s)
	}
	if host == "" {
		return Target{}, fmt.Errorf("target %q: the address names no host", s)
	}
	// A port no dialler takes (+80) would fail to dial, and a failed dial
	// reads as a closed path: refused here instead.
	if p, ok := canonicalInt(port); !ok || p < 1 || p > 65535 {
		return Target{}, fmt.Errorf("target %q: the port is 1 to 65535", s)
	}
	return t, nil
}

// canonicalInt reads a number in its one spelling: no sign, no leading
// zero, so what is accepted formats back to the same text.
func canonicalInt(s string) (int, bool) {
	n, err := strconv.Atoi(s)
	return n, err == nil && strconv.Itoa(n) == s
}

// Ask tries every target, at most parallel at once, each within timeout,
// and returns one observation per target in the order given.
func Ask(ctx context.Context, targets []Target, timeout time.Duration) []servicehost.Observation {
	listed := metadataLiterals(targets)
	out := make([]servicehost.Observation, len(targets))
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			tctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			out[i] = ask(tctx, t, listed)
		}()
	}
	wg.Wait()
	return out
}

// metadataLiterals is the set of addresses question 1 names literally: a
// name for question 1 is tried only at one of these or a link-local one.
func metadataLiterals(targets []Target) map[string]bool {
	m := map[string]bool{}
	for _, t := range targets {
		if t.Question != servicehost.QuestionMetadata {
			continue
		}
		host, _, _ := net.SplitHostPort(t.Addr)
		if ip := net.ParseIP(host); ip != nil {
			m[ip.String()] = true
		}
	}
	return m
}

func ask(ctx context.Context, t Target, listed map[string]bool) servicehost.Observation {
	o := servicehost.Observation{Question: t.Question, Target: t.Addr, Kind: t.Kind, Via: t.Via}
	addrs, err := dialAddrs(ctx, t, listed)
	if err != nil || len(addrs) == 0 {
		o.Unresolved = true
		return o
	}
	for _, addr := range addrs {
		if ctx.Err() != nil {
			break
		}
		c, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		if err != nil {
			// A name's own lookup is part of its dial: one that failed asked
			// nothing.
			var dns *net.DNSError
			o.Unresolved = errors.As(err, &dns)
			continue
		}
		_ = c.Close()
		o.Connected = true
		switch t.Via {
		case servicehost.ViaPlaintext:
			o.Status = call(ctx, addr, insecure.NewCredentials())
		case servicehost.ViaTLS:
			o.TLS = handshake(ctx, addr)
			if o.TLS == servicehost.TLSOK {
				o.Status = call(ctx, addr, credentials.NewTLS(clientTLS(nil)))
			}
		}
		return o
	}
	return o
}

// dialAddrs is where a target is tried: an address at itself, and a name at
// itself too, so a TLS server sees the name it is known by, once the name is
// known to lead somewhere that may be asked. None, or an error, is a name
// that gave nothing to ask. A name for question 1 is tried only at those of
// its addresses that askable keeps.
func dialAddrs(ctx context.Context, t Target, listed map[string]bool) ([]string, error) {
	host, port, _ := net.SplitHostPort(t.Addr)
	if net.ParseIP(host) != nil {
		return []string{t.Addr}, nil
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	return nameAddrs(t, ips, port, listed), nil
}

// nameAddrs is where a name that resolved to ips is tried (dialAddrs).
func nameAddrs(t Target, ips []net.IPAddr, port string, listed map[string]bool) []string {
	addrs := askable(t.Question, ips, port, listed)
	if len(addrs) > 0 && t.Question != servicehost.QuestionMetadata {
		return []string{t.Addr}
	}
	return addrs
}

// askable is which of a name's addresses may be asked. For question 1, those
// that are link-local or that it lists literally (14 §7): a resolver that
// answers every name with one address would otherwise make every host of a
// fleet unfit together. For the others, any but loopback and the unspecified
// address, which from a container lead back to the container.
func askable(question int, ips []net.IPAddr, port string, listed map[string]bool) []string {
	var out []string
	for _, ip := range ips {
		keep := !ip.IP.IsLoopback() && !ip.IP.IsUnspecified()
		if question == servicehost.QuestionMetadata {
			keep = ip.IP.IsLinkLocalUnicast() || listed[ip.IP.String()]
		}
		if keep {
			out = append(out, net.JoinHostPort(ip.IP.String(), port))
		}
	}
	return out
}

// clientTLS is a client that accepts whatever certificate the server shows
// and presents none: the question is only whether the server serves such a
// caller, never whom it proves itself to be. asked, when not nil, records
// that the server asked for a client certificate.
func clientTLS(asked *atomic.Bool) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, // the probe asks whether the server serves, not who it is
		NextProtos:         []string{"h2"},
		MinVersion:         tls.VersionTLS12,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			if asked != nil {
				asked.Store(true)
			}
			return &tls.Certificate{}, nil
		},
	}
}

// handshake reports how a TLS handshake with no client certificate ends.
// A server that asked for a certificate and then ended the handshake, or
// (TLS 1.3, where the client's side completes before the server has judged
// it) ended the connection before any data, demands one: whichever alert
// it sent (Go and OpenSSL send handshake_failure under TLS 1.2,
// certificate_required under TLS 1.3), the request is what says so.
func handshake(ctx context.Context, addr string) string {
	var asked atomic.Bool
	d := &tls.Dialer{Config: clientTLS(&asked)}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		if ctx.Err() != nil {
			// Cut off by the deadline, not refused: no answer, which the
			// Orker reads as a port that took the connection.
			return ""
		}
		return refused(&asked)
	}
	defer func() { _ = c.Close() }()
	tc := c.(*tls.Conn)
	if tc.ConnectionState().Version < tls.VersionTLS13 || !asked.Load() {
		return servicehost.TLSOK
	}
	deadline := time.Now().Add(tls13Settle)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = tc.SetReadDeadline(deadline)
	var b [1]byte
	if _, err := tc.Read(b[:]); err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return servicehost.TLSOK
		}
		return refused(&asked)
	}
	return servicehost.TLSOK
}

// refused reads a handshake the server ended: one that asked for a client
// certificate demands one; any other is a failed handshake, which
// servicehost reads as not reached too.
func refused(asked *atomic.Bool) string {
	if asked.Load() {
		return servicehost.TLSClientCertRequire
	}
	return servicehost.TLSFailed
}

// call makes one health check and returns the status code's name: any
// answer at all, Unimplemented included, says the server got as far as
// judging a call from a caller with no credential.
func call(ctx context.Context, addr string, creds credentials.TransportCredentials) string {
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return ""
	}
	defer func() { _ = conn.Close() }()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return status.Code(err).String()
}

// Main runs the program and returns its exit code: 0 done, 1 a failure
// after the invocation was accepted, 2 an invocation refused (nothing is
// written to stdout then).
func Main(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: probe dial [-timeout D] TARGET... | probe listen PORT")
		return 2
	}
	switch args[0] {
	case "dial":
		return dialMain(ctx, args[1:], stdout, stderr)
	case "listen":
		return listenMain(ctx, args[1:], stderr)
	default:
		fmt.Fprintf(stderr, "probe: unknown command %q\n", args[0])
		return 2
	}
}

func dialMain(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dial", flag.ContinueOnError)
	fs.SetOutput(stderr)
	timeout := fs.Duration("timeout", DefaultTimeout, "one target's budget")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *timeout <= 0 || *timeout > maxTimeout {
		fmt.Fprintf(stderr, "probe: -timeout is above 0 and at most %s\n", maxTimeout)
		return 2
	}
	rest := fs.Args()
	if len(rest) == 0 || len(rest) > MaxTargets {
		fmt.Fprintf(stderr, "probe: dial takes 1 to %d targets\n", MaxTargets)
		return 2
	}
	targets := make([]Target, 0, len(rest))
	for _, s := range rest {
		t, err := ParseTarget(s)
		if err != nil {
			fmt.Fprintln(stderr, "probe:", err)
			return 2
		}
		targets = append(targets, t)
	}
	obs := Ask(ctx, targets, *timeout)
	if ctx.Err() != nil {
		// Stopped before it finished: the targets it never tried would
		// read as closed, so it answers nothing.
		fmt.Fprintln(stderr, "probe: stopped before every target was tried")
		return 1
	}
	for _, o := range obs {
		line, err := json.Marshal(o)
		if err != nil {
			fmt.Fprintln(stderr, "probe:", err)
			return 1
		}
		if _, err := fmt.Fprintf(stdout, "%s\n", line); err != nil {
			fmt.Fprintln(stderr, "probe:", err)
			return 1
		}
	}
	return 0
}

func listenMain(ctx context.Context, args []string, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "probe: listen takes one PORT")
		return 2
	}
	if p, err := strconv.Atoi(args[0]); err != nil || p < 1 || p > 65535 {
		fmt.Fprintln(stderr, "probe: the port is 1 to 65535")
		return 2
	}
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", ":"+args[0])
	if err != nil {
		fmt.Fprintln(stderr, "probe:", err)
		return 1
	}
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
		case <-stopped:
		}
		_ = ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return 0
			}
			fmt.Fprintln(stderr, "probe:", err)
			return 1
		}
		_ = c.Close()
	}
}
