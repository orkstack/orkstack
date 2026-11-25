package reachprobe

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
)

func TestParseTarget(t *testing.T) {
	tests := []struct {
		in      string
		want    Target
		wantErr string
	}{
		{in: "3,service,tls,10.0.0.1:7233", want: Target{Question: 3, Kind: "service", Via: "tls", Addr: "10.0.0.1:7233"}},
		{in: "1,address,tcp,[fd00:ec2::254]:80", want: Target{Question: 1, Kind: "address", Via: "tcp", Addr: "[fd00:ec2::254]:80"}},
		{in: "1,address,tcp,metadata.google.internal:80", want: Target{Question: 1, Kind: "address", Via: "tcp", Addr: "metadata.google.internal:80"}},
		{in: "3,membership,tcp,10.0.0.1:6933", want: Target{Question: 3, Kind: "membership", Via: "tcp", Addr: "10.0.0.1:6933"}},
		{in: "", wantErr: "four fields"},
		{in: "3,service,tls", wantErr: "four fields"},
		{in: "5,address,tcp,1.2.3.4:1", wantErr: "question"},
		{in: "x,address,tcp,1.2.3.4:1", wantErr: "question"},
		{in: "01,address,tcp,1.2.3.4:1", wantErr: "question"},
		{in: "+1,address,tcp,1.2.3.4:1", wantErr: "question"},
		{in: "1,address,tcp,1.2.3.4:080", wantErr: "port"},
		{in: "1,address,tcp,1.2.3.4:+80", wantErr: "port"},
		{in: "1,other,tcp,1.2.3.4:1", wantErr: "kind"},
		{in: "1,address,udp,1.2.3.4:1", wantErr: "via"},
		{in: "1,address,tcp,1.2.3.4", wantErr: "host:port"},
		{in: "1,address,tcp,1.2.3.4:0", wantErr: "port"},
		{in: "1,address,tcp,1.2.3.4:99999", wantErr: "port"},
		{in: "1,address,tcp,:80", wantErr: "host"},
		{in: "1,address,tls,1.2.3.4:80", wantErr: "only a service"},
		{in: "1,service,tcp," + strings.Repeat("a", 300) + ":80", wantErr: "256"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseTarget(tt.in)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseTarget(%q) error = %v; want one containing %q", tt.in, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTarget(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("ParseTarget(%q) = %+v; want %+v", tt.in, got, tt.want)
			}
		})
	}
}

// listen opens a loopback listener the test closes.
func listen(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return ln
}

// closedAddr is a loopback address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// acceptAll accepts connections and closes each when the test ends, as a
// membership port that speaks no protocol a probe knows would.
func acceptAll(t *testing.T) string {
	t.Helper()
	ln := listen(t)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = c.Close() })
		}
	}()
	return ln.Addr().String()
}

func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "cluster"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// grpcServer serves the health service; deny makes every call answer
// Unauthenticated, as a cluster that asks for a credential does.
func grpcServer(t *testing.T, tlsConf *tls.Config, deny bool) string {
	t.Helper()
	var opts []grpc.ServerOption
	if tlsConf != nil {
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConf)))
	}
	if deny {
		opts = append(opts, grpc.UnaryInterceptor(func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
			return nil, status.Error(codes.Unauthenticated, "no credential")
		}))
	}
	srv := grpc.NewServer(opts...)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	ln := listen(t)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

func TestAsk_ClassifiesWhatEachTargetDoes(t *testing.T) {
	cert := selfSigned(t)
	open := grpcServer(t, nil, false)
	denied := grpcServer(t, nil, true)
	tlsOpen := grpcServer(t, &tls.Config{Certificates: []tls.Certificate{cert}}, false)
	mtls12 := grpcServer(t, &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAnyClientCert, MaxVersion: tls.VersionTLS12}, false)
	mtls13 := grpcServer(t, &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAnyClientCert, MinVersion: tls.VersionTLS13}, false)
	member := acceptAll(t)
	closed := closedAddr(t)

	tests := []struct {
		name    string
		target  Target
		want    servicehost.Observation
		reached bool
	}{
		{"an open plaintext frontend serves", Target{3, "service", "plaintext", open},
			servicehost.Observation{Connected: true, Status: "OK"}, true},
		{"a frontend that asks for a credential turns the caller away", Target{3, "service", "plaintext", denied},
			servicehost.Observation{Connected: true, Status: "Unauthenticated"}, false},
		{"a TLS-only frontend refuses plaintext but is still reached", Target{3, "service", "plaintext", tlsOpen},
			servicehost.Observation{Connected: true, Status: "Unavailable"}, true},
		{"a TLS-only frontend serves a caller with no certificate", Target{3, "service", "tls", tlsOpen},
			servicehost.Observation{Connected: true, TLS: servicehost.TLSOK, Status: "OK"}, true},
		{"mutual TLS 1.2 turns the caller away at the handshake", Target{3, "service", "tls", mtls12},
			servicehost.Observation{Connected: true, TLS: servicehost.TLSClientCertRequire}, false},
		{"mutual TLS 1.3 turns the caller away after the handshake", Target{3, "service", "tls", mtls13},
			servicehost.Observation{Connected: true, TLS: servicehost.TLSClientCertRequire}, false},
		{"TLS to a plaintext port fails its handshake", Target{3, "service", "tls", open},
			servicehost.Observation{Connected: true, TLS: servicehost.TLSFailed}, false},
		{"a membership port that accepts is reached", Target{3, "membership", "tcp", member},
			servicehost.Observation{Connected: true}, true},
		{"a closed port is not", Target{2, "address", "tcp", closed},
			servicehost.Observation{}, false},
		{"a closed service port is not called", Target{3, "service", "tls", closed},
			servicehost.Observation{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Ask(context.Background(), []Target{tt.target}, 3*time.Second)
			if len(got) != 1 {
				t.Fatalf("Ask returned %d observations; want 1", len(got))
			}
			o := got[0]
			want := tt.want
			want.Question, want.Kind, want.Via, want.Target = tt.target.Question, tt.target.Kind, tt.target.Via, tt.target.Addr
			if o != want {
				t.Fatalf("observation = %+v; want %+v", o, want)
			}
			if o.Reached() != tt.reached {
				t.Fatalf("Reached() = %v; want %v", o.Reached(), tt.reached)
			}
		})
	}
}

// A cluster that asks both ends for a certificate is closed on its port,
// judged by both legs together, as the Orker judges it.
func TestAsk_MutualTLSPortIsClosed(t *testing.T) {
	cert := selfSigned(t)
	for _, v := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		addr := grpcServer(t, &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAnyClientCert, MinVersion: v, MaxVersion: v}, false)
		obs := Ask(context.Background(), []Target{{3, "service", "plaintext", addr}, {3, "service", "tls", addr},
			{1, "address", "tcp", closedAddr(t)}, {2, "address", "tcp", closedAddr(t)}, {4, "address", "tcp", closedAddr(t)}}, 3*time.Second)
		if f := servicehost.JudgeProbe(obs, 1, 2, 3, 4); !f.Fit {
			t.Errorf("TLS %x: a mutual-TLS port was judged open: %q (%+v)", v, f.Failures, obs[:2])
		}
	}
}

// A metadata name is dialled only where it resolves to a link-local address
// or one of the literal metadata addresses the same run lists: a resolver
// that answers every name must not make a host unfit (14 §7, question 1).
func TestAsk_MetadataNameDialledOnlyWhenLinkLocalOrListed(t *testing.T) {
	ln := listen(t)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	// localhost resolves to loopback, which is no metadata address.
	byName := Target{1, "address", "tcp", net.JoinHostPort("localhost", port)}
	got := Ask(context.Background(), []Target{byName}, 2*time.Second)
	if got[0].Connected {
		t.Fatalf("a name resolving to loopback was dialled: %+v", got[0])
	}
	// The same listener named by its literal address, as an operator's
	// probe address, is dialled.
	literal := Target{1, "address", "tcp", ln.Addr().String()}
	got = Ask(context.Background(), []Target{literal}, 2*time.Second)
	if !got[0].Connected {
		t.Fatalf("a literal address was not dialled: %+v", got[0])
	}
	// A name that resolves to a literal address the same run lists for
	// question 1 is dialled.
	got = Ask(context.Background(), []Target{literal, byName}, 2*time.Second)
	if !got[1].Connected {
		t.Fatalf("a name resolving to a listed address was not dialled: %+v", got[1])
	}
	// A name for another question is dialled whatever it resolves to.
	other := Target{2, "address", "tcp", net.JoinHostPort("localhost", port)}
	got = Ask(context.Background(), []Target{other}, 2*time.Second)
	if !got[0].Connected {
		t.Fatalf("a question-2 name was not dialled: %+v", got[0])
	}
}

func TestMain_DialWritesParseableAnswer(t *testing.T) {
	member := acceptAll(t)
	closed := closedAddr(t)
	var out, errOut bytes.Buffer
	code := Main(context.Background(), []string{"dial", "-timeout", "2s",
		"3,membership,tcp," + member, "2,address,tcp," + closed}, &out, &errOut)
	if code != 0 {
		t.Fatalf("Main = %d; stderr %q", code, errOut.String())
	}
	obs, err := servicehost.ParseProbeAnswer(out.Bytes())
	if err != nil {
		t.Fatalf("the answer does not parse: %v\n%s", err, out.String())
	}
	if len(obs) != 2 || !obs[0].Connected || obs[1].Connected {
		t.Fatalf("observations = %+v", obs)
	}
	if servicehost.JudgeProbe(obs, 2, 3).Fit {
		t.Fatal("a reached membership port judged fit")
	}
}

func TestMain_RefusesBadInvocations(t *testing.T) {
	tests := [][]string{
		nil,
		{"frobnicate"},
		{"dial"},
		{"dial", "nonsense"},
		{"dial", "-timeout", "0s", "2,address,tcp,127.0.0.1:1"},
		{"dial", "-timeout", "2m", "2,address,tcp,127.0.0.1:1"},
		{"listen"},
		{"listen", "0"},
		{"listen", "70000"},
		{"listen", "80", "extra"},
	}
	for _, args := range tests {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, errOut bytes.Buffer
			if code := Main(context.Background(), args, &out, &errOut); code != 2 {
				t.Fatalf("Main(%q) = %d; want 2", args, code)
			}
			if out.Len() != 0 {
				t.Fatalf("a refused invocation wrote an answer: %q", out.String())
			}
		})
	}
}

func TestMain_RefusesTooManyTargets(t *testing.T) {
	args := []string{"dial"}
	for i := range MaxTargets + 1 {
		args = append(args, "2,address,tcp,127.0.0.1:"+strconv.Itoa(1+i))
	}
	var out, errOut bytes.Buffer
	if code := Main(context.Background(), args, &out, &errOut); code != 2 {
		t.Fatalf("Main with %d targets = %d; want 2", MaxTargets+1, code)
	}
}

// A canary listens until it is stopped, and takes a connection.
func TestMain_ListenAcceptsUntilCancelled(t *testing.T) {
	_, port, _ := net.SplitHostPort(closedAddr(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	var out, errOut bytes.Buffer
	go func() { done <- Main(ctx, []string{"listen", port}, &out, &errOut) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", "127.0.0.1:"+port, time.Second)
		if err == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the canary never listened: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("listen exited %d; stderr %q", code, errOut.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listen did not stop when cancelled")
	}
}

// A probe stopped before it finished is no answer: it writes nothing and
// exits 1, so the targets it never tried are not read as closed.
func TestMain_DialStoppedWritesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errOut bytes.Buffer
	if code := Main(ctx, []string{"dial", "2,address,tcp," + closedAddr(t)}, &out, &errOut); code != 1 || out.Len() != 0 {
		t.Fatalf("a stopped probe exited %d and wrote %q", code, out.String())
	}
}
