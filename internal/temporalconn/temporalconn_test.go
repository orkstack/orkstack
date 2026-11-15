package temporalconn_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/orkstack/orkstack/internal/temporalconn"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/testpki"
)

// marker is written into every file a test hands New, so a test can show
// that no refusal and no log line ever carries a file's content.
const marker = "CONTENT-MARKER-7f3a"

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *fakeClock { return &fakeClock{now: time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type observer struct {
	mu       sync.Mutex
	reloads  []string
	notAfter map[string]time.Time
	auth     int
}

func (o *observer) FileReloaded(setting string, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reloads = append(o.reloads, fmt.Sprintf("%s:%t", setting, ok))
}

func (o *observer) CertificateLoaded(setting string, notAfter time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.notAfter == nil {
		o.notAfter = map[string]time.Time{}
	}
	o.notAfter[setting] = notAfter
}

func (o *observer) AuthFailure() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.auth++
}

func (o *observer) snapshot() ([]string, map[string]time.Time, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	na := map[string]time.Time{}
	for k, v := range o.notAfter {
		na[k] = v
	}
	return append([]string(nil), o.reloads...), na, o.auth
}

// logs is a slog handler's output, safe to read while the code under test
// writes it.
type logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *logs) count(level string) int {
	return strings.Count(l.String(), "level="+level)
}

type env struct {
	clock *fakeClock
	obs   *observer
	logs  *logs
}

func newEnv() *env { return &env{clock: newClock(), obs: &observer{}, logs: &logs{}} }

func (e *env) options() temporalconn.Options {
	return temporalconn.Options{
		Logger:   slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Observer: e.obs,
		Now:      e.clock.Now,
	}
}

func ptr(b bool) *bool { return &b }

// server is a TLS listener that completes each handshake and reports the
// client certificate it was shown.
type server struct {
	addr    string
	results chan *big.Int
}

func startServer(t *testing.T, cert tls.Certificate, clientCAs *x509.CertPool) *server {
	t.Helper()
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if clientCAs != nil {
		cfg.ClientCAs = clientCAs
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	must.OKf(t, err, "listen")
	t.Cleanup(func() { ln.Close() })
	s := &server{addr: ln.Addr().String(), results: make(chan *big.Int, 16)}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c *tls.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(10 * time.Second))
				if err := c.Handshake(); err != nil {
					return
				}
				var serial *big.Int
				if peers := c.ConnectionState().PeerCertificates; len(peers) > 0 {
					serial = peers[0].SerialNumber
				}
				s.results <- serial
				_, _ = c.Write([]byte{1})
			}(conn.(*tls.Conn))
		}
	}()
	return s
}

// handshake dials s with cfg and returns the client certificate s saw.
func handshake(t *testing.T, cfg *tls.Config, s *server) (*big.Int, error) {
	t.Helper()
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", s.addr, cfg.Clone())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	one := make([]byte, 1)
	if _, err := conn.Read(one); err != nil {
		return nil, err
	}
	select {
	case serial := <-s.results:
		return serial, nil
	case <-time.After(5 * time.Second):
		return nil, errors.New("server reported nothing")
	}
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	must.OKf(t, os.WriteFile(path, data, 0o600), "write %s", path)
}

func TestNew_NothingSetIsTheZeroSecurity(t *testing.T) {
	sec, err := temporalconn.New(temporalconn.Settings{}, "127.0.0.1:7233", newEnv().options())
	must.OKf(t, err, "New")
	if sec.Enabled() || sec.TLS != nil || sec.Credentials != nil || sec.DialOptions != nil || len(sec.Files()) != 0 {
		t.Fatalf("New with nothing set = %+v, want the zero Security", sec)
	}
	sec, err = temporalconn.New(temporalconn.Settings{Enabled: ptr(false)}, "127.0.0.1:7233", newEnv().options())
	must.OKf(t, err, "New with enabled false")
	if sec.Enabled() {
		t.Fatal("enabled: false alone dials with TLS")
	}
}

func TestNew_BootRulesRefuseNamingTheSetting(t *testing.T) {
	dir := t.TempDir()
	ca := testpki.NewCA(t, "ca")
	client := ca.Issue(t, testpki.LeafOptions{Name: "orker", Client: true})
	other := ca.Issue(t, testpki.LeafOptions{Name: "other", Client: true})
	caFile := testpki.Write(t, dir, "ca.pem", ca.PEM)
	certFile := testpki.Write(t, dir, "client.pem", client.CertPEM)
	keyFile := testpki.Write(t, dir, "client.key", client.KeyPEM)
	otherKey := testpki.Write(t, dir, "other.key", other.KeyPEM)
	garbage := testpki.Write(t, dir, "garbage.pem", []byte("-----BEGIN CERTIFICATE-----\n"+marker+"\n-----END CERTIFICATE-----\n"))
	noPEM := testpki.Write(t, dir, "nopem.pem", []byte(marker+"\n"))
	blankKey := testpki.Write(t, dir, "blank.key", []byte(" \n\t\r\n"))
	apiKey := testpki.Write(t, dir, "api.key", []byte(marker+"\n"))
	missing := filepath.Join(dir, "missing.pem")

	tests := []struct {
		name    string
		s       temporalconn.Settings
		setting string
	}{
		{"enabled false beside a CA", temporalconn.Settings{Enabled: ptr(false), CAFile: caFile}, "temporal.tls.enabled"},
		{"enabled false beside an API key", temporalconn.Settings{Enabled: ptr(false), APIKeyFile: apiKey}, "temporal.tls.enabled"},
		{"enabled false beside a server name", temporalconn.Settings{Enabled: ptr(false), ServerName: "temporal"}, "temporal.tls.enabled"},
		{"a certificate without its key", temporalconn.Settings{CertFile: certFile}, "temporal.tls.key_file"},
		{"a key without its certificate", temporalconn.Settings{KeyFile: keyFile}, "temporal.tls.cert_file"},
		{"an unreadable CA file", temporalconn.Settings{CAFile: missing}, "temporal.tls.ca_file"},
		{"a CA file that does not parse", temporalconn.Settings{CAFile: garbage}, "temporal.tls.ca_file"},
		{"a CA file with no certificate", temporalconn.Settings{CAFile: noPEM}, "temporal.tls.ca_file"},
		{"an unreadable certificate", temporalconn.Settings{CertFile: missing, KeyFile: keyFile}, "temporal.tls.cert_file"},
		{"a certificate that does not parse", temporalconn.Settings{CertFile: garbage, KeyFile: keyFile}, "temporal.tls.cert_file"},
		{"a key that does not match", temporalconn.Settings{CertFile: certFile, KeyFile: otherKey}, "temporal.tls.key_file"},
		{"an unreadable API key", temporalconn.Settings{APIKeyFile: missing}, "temporal.api_key_file"},
		{"an empty API key", temporalconn.Settings{APIKeyFile: blankKey}, "temporal.api_key_file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv()
			_, err := temporalconn.New(tt.s, "127.0.0.1:7233", e.options())
			if err == nil {
				t.Fatalf("New(%+v) = nil error, want a refusal naming %s", tt.s, tt.setting)
			}
			if !strings.Contains(err.Error(), tt.setting) {
				t.Errorf("refusal %q does not name %s", err, tt.setting)
			}
			if strings.Contains(err.Error(), marker) || strings.Contains(e.logs.String(), marker) {
				t.Errorf("a file's content reached the refusal or the log: %q", err)
			}
		})
	}
}

func TestNew_EveryFileOrNameTurnsTLSOn(t *testing.T) {
	dir := t.TempDir()
	ca := testpki.NewCA(t, "ca")
	client := ca.Issue(t, testpki.LeafOptions{Name: "orker", Client: true})
	caFile := testpki.Write(t, dir, "ca.pem", ca.PEM)
	certFile := testpki.Write(t, dir, "client.pem", client.CertPEM)
	keyFile := testpki.Write(t, dir, "client.key", client.KeyPEM)
	apiKey := testpki.Write(t, dir, "api.key", []byte("k\n"))
	for _, tt := range []struct {
		name  string
		s     temporalconn.Settings
		files []string
	}{
		{"enabled", temporalconn.Settings{Enabled: ptr(true)}, nil},
		{"ca_file", temporalconn.Settings{CAFile: caFile}, []string{caFile}},
		{"client pair", temporalconn.Settings{CertFile: certFile, KeyFile: keyFile}, []string{certFile, keyFile}},
		{"server_name", temporalconn.Settings{ServerName: "temporal.internal"}, nil},
		{"api_key_file", temporalconn.Settings{APIKeyFile: apiKey}, []string{apiKey}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sec, err := temporalconn.New(tt.s, "127.0.0.1:7233", newEnv().options())
			must.OKf(t, err, "New")
			if !sec.Enabled() || sec.TLS.MinVersion != tls.VersionTLS12 {
				t.Fatalf("New(%+v): TLS = %+v, want TLS with a TLS 1.2 minimum", tt.s, sec.TLS)
			}
			if fmt.Sprint(sec.Files()) != fmt.Sprint(tt.files) {
				t.Errorf("Files() = %v, want %v", sec.Files(), tt.files)
			}
			if (tt.s.APIKeyFile != "") != (sec.Credentials != nil) {
				t.Errorf("Credentials = %v with api_key_file %q", sec.Credentials, tt.s.APIKeyFile)
			}
		})
	}
}

func TestTLS_VerifiesTheServer(t *testing.T) {
	dir := t.TempDir()
	good := testpki.NewCA(t, "good")
	wrong := testpki.NewCA(t, "wrong")
	srv := startServer(t, good.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"temporal.internal", "127.0.0.1"}}).TLS(t), nil)
	goodFile := testpki.Write(t, dir, "good.pem", good.PEM)
	wrongFile := testpki.Write(t, dir, "wrong.pem", wrong.PEM)

	for _, tt := range []struct {
		name string
		s    temporalconn.Settings
		ok   bool
	}{
		{"the right CA, the address's host", temporalconn.Settings{CAFile: goodFile}, true},
		{"the right CA, a named server", temporalconn.Settings{CAFile: goodFile, ServerName: "temporal.internal"}, true},
		{"a wrong CA", temporalconn.Settings{CAFile: wrongFile}, false},
		{"the right CA, a name the certificate lacks", temporalconn.Settings{CAFile: goodFile, ServerName: "elsewhere.internal"}, false},
		{"the system roots", temporalconn.Settings{Enabled: ptr(true)}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sec, err := temporalconn.New(tt.s, srv.addr, newEnv().options())
			must.OKf(t, err, "New")
			_, err = handshake(t, sec.TLS, srv)
			if tt.ok && err != nil {
				t.Fatalf("handshake failed: %v", err)
			}
			if !tt.ok && err == nil {
				t.Fatal("handshake succeeded against a server the settings do not trust")
			}
		})
	}
}

func TestTLS_CARotatesWithoutARestart(t *testing.T) {
	dir := t.TempDir()
	oldCA := testpki.NewCA(t, "old")
	newCA := testpki.NewCA(t, "new")
	srv := startServer(t, newCA.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"127.0.0.1"}}).TLS(t), nil)
	caFile := testpki.Write(t, dir, "ca.pem", oldCA.PEM)
	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{CAFile: caFile}, srv.addr, e.options())
	must.OKf(t, err, "New")
	if _, err := handshake(t, sec.TLS, srv); err == nil {
		t.Fatal("the old CA verified the new server")
	}
	write(t, caFile, append(append([]byte{}, newCA.PEM...), '\n'))
	e.clock.Advance(2 * time.Second)
	if _, err := handshake(t, sec.TLS, srv); err != nil {
		t.Fatalf("after the CA file changed, handshake: %v", err)
	}
	if reloads, _, _ := e.obs.snapshot(); !contains(reloads, "temporal.tls.ca_file:true") {
		t.Errorf("reloads = %v, want temporal.tls.ca_file:true", reloads)
	}
}

func TestTLS_ClientCertificateRotatesAndAFailedReloadKeepsTheLastGood(t *testing.T) {
	dir := t.TempDir()
	serverCA := testpki.NewCA(t, "server")
	clientCA := testpki.NewCA(t, "client")
	srv := startServer(t, serverCA.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"127.0.0.1"}}).TLS(t), clientCA.Pool())
	first := clientCA.Issue(t, testpki.LeafOptions{Name: "first", Client: true})
	second := clientCA.Issue(t, testpki.LeafOptions{Name: "second", Client: true})
	caFile := testpki.Write(t, dir, "ca.pem", serverCA.PEM)
	certFile := testpki.Write(t, dir, "client.pem", first.CertPEM)
	keyFile := testpki.Write(t, dir, "client.key", first.KeyPEM)
	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{CAFile: caFile, CertFile: certFile, KeyFile: keyFile}, srv.addr, e.options())
	must.OKf(t, err, "New")

	expect := func(want *big.Int, when string) {
		t.Helper()
		got, err := handshake(t, sec.TLS, srv)
		if err != nil {
			t.Fatalf("%s: handshake: %v", when, err)
		}
		if got == nil || got.Cmp(want) != 0 {
			t.Fatalf("%s: the server saw certificate %v, want %v", when, got, want.String())
		}
	}
	expect(first.Cert.SerialNumber, "at boot")

	// Half a pair: the new certificate beside the old key. Not adopted.
	write(t, certFile, second.CertPEM)
	e.clock.Advance(2 * time.Second)
	expect(first.Cert.SerialNumber, "with half a pair written")
	e.clock.Advance(2 * time.Second)
	expect(first.Cert.SerialNumber, "with the same half pair still there")
	if n := e.logs.count("ERROR"); n != 1 {
		t.Errorf("a failed reload, seen twice, logged %d errors, want 1:\n%s", n, e.logs)
	}

	// The key follows: the pair is whole and is adopted.
	write(t, keyFile, second.KeyPEM)
	e.clock.Advance(2 * time.Second)
	expect(second.Cert.SerialNumber, "after the key followed")

	reloads, notAfter, _ := e.obs.snapshot()
	if !contains(reloads, "temporal.tls.cert_file:false") || !contains(reloads, "temporal.tls.cert_file:true") {
		t.Errorf("reloads = %v, want a refused and an adopted temporal.tls.cert_file", reloads)
	}
	if !notAfter["temporal.tls.cert_file"].Equal(second.Cert.NotAfter) {
		t.Errorf("not_after(client.pem) = %v, want %v", notAfter["temporal.tls.cert_file"], second.Cert.NotAfter)
	}
	if strings.Contains(e.logs.String(), "PRIVATE KEY") {
		t.Error("a key's content reached the log")
	}
}

func TestReload_OnceAMinuteEvenWhenTheFileLooksTheSame(t *testing.T) {
	dir := t.TempDir()
	path := testpki.Write(t, dir, "api.key", []byte("aaaa\n"))
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	must.OK(t, os.Chtimes(path, stamp, stamp))
	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{APIKeyFile: path}, "127.0.0.1:7233", e.options())
	must.OKf(t, err, "New")
	key := func() string {
		t.Helper()
		k, err := temporalconn.APIKeyForTest(sec, context.Background())
		must.OKf(t, err, "api key")
		return k
	}
	if got := key(); got != "aaaa" {
		t.Fatalf("api key = %q, want the file's content with its line end removed", got)
	}
	// The same size, the same modification time, the same inode.
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	must.OK(t, err)
	_, err = f.WriteAt([]byte("bbbb"), 0)
	must.OK(t, err)
	must.OK(t, f.Close())
	must.OK(t, os.Chtimes(path, stamp, stamp))
	e.clock.Advance(30 * time.Second)
	if got := key(); got != "aaaa" {
		t.Fatalf("within the minute api key = %q, want the old one (nothing looked changed)", got)
	}
	e.clock.Advance(31 * time.Second)
	if got := key(); got != "bbbb" {
		t.Fatalf("after a minute api key = %q, want the re-read one", got)
	}
}

func TestReload_ALinkSwappedToANewDirectoryIsSeen(t *testing.T) {
	// A Kubernetes projected volume: the file is a link through ..data,
	// which is replaced by a link to a new directory.
	dir := t.TempDir()
	v1 := filepath.Join(dir, "..v1")
	v2 := filepath.Join(dir, "..v2")
	must.OK(t, os.Mkdir(v1, 0o700))
	must.OK(t, os.Mkdir(v2, 0o700))
	testpki.Write(t, v1, "api.key", []byte("one"))
	testpki.Write(t, v2, "api.key", []byte("two"))
	stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	must.OK(t, os.Chtimes(filepath.Join(v1, "api.key"), stamp, stamp))
	must.OK(t, os.Chtimes(filepath.Join(v2, "api.key"), stamp, stamp))
	data := filepath.Join(dir, "..data")
	must.OK(t, os.Symlink("..v1", data))
	path := filepath.Join(dir, "api.key")
	must.OK(t, os.Symlink(filepath.Join("..data", "api.key"), path))

	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{APIKeyFile: path}, "127.0.0.1:7233", e.options())
	must.OKf(t, err, "New")
	if k, _ := temporalconn.APIKeyForTest(sec, context.Background()); k != "one" {
		t.Fatalf("api key = %q, want one", k)
	}
	tmp := filepath.Join(dir, "..data_tmp")
	must.OK(t, os.Symlink("..v2", tmp))
	must.OK(t, os.Rename(tmp, data))
	e.clock.Advance(2 * time.Second)
	if k, _ := temporalconn.APIKeyForTest(sec, context.Background()); k != "two" {
		t.Fatalf("after the swap api key = %q, want two", k)
	}
}

func TestReload_AnEmptiedAPIKeyKeepsTheLastGood(t *testing.T) {
	dir := t.TempDir()
	path := testpki.Write(t, dir, "api.key", []byte("good-key"))
	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{APIKeyFile: path}, "127.0.0.1:7233", e.options())
	must.OKf(t, err, "New")
	write(t, path, []byte("\n"))
	e.clock.Advance(2 * time.Second)
	if k, err := temporalconn.APIKeyForTest(sec, context.Background()); err != nil || k != "good-key" {
		t.Fatalf("api key after the file was emptied = %q, %v; want the last good key", k, err)
	}
	if reloads, _, _ := e.obs.snapshot(); !contains(reloads, "temporal.api_key_file:false") {
		t.Errorf("reloads = %v, want temporal.api_key_file:false", reloads)
	}
}

func TestExpiry_IsReportedAndWarnedOnceADay(t *testing.T) {
	dir := t.TempDir()
	ca := testpki.NewCA(t, "ca")
	e := newEnv()
	ends := e.clock.Now().Add(3 * 24 * time.Hour)
	leaf := ca.Issue(t, testpki.LeafOptions{Name: "orker", Client: true, NotAfter: ends})
	certFile := testpki.Write(t, dir, "client.pem", leaf.CertPEM)
	keyFile := testpki.Write(t, dir, "client.key", leaf.KeyPEM)
	sec, err := temporalconn.New(temporalconn.Settings{CertFile: certFile, KeyFile: keyFile}, "127.0.0.1:7233", e.options())
	must.OKf(t, err, "New")
	if _, notAfter, _ := e.obs.snapshot(); !notAfter["temporal.tls.cert_file"].Equal(leaf.Cert.NotAfter) {
		t.Fatalf("not_after = %v, want %v", notAfter["temporal.tls.cert_file"], leaf.Cert.NotAfter)
	}
	certificate := func() {
		t.Helper()
		_, err := sec.TLS.GetClientCertificate(&tls.CertificateRequestInfo{})
		must.OKf(t, err, "GetClientCertificate")
	}
	certificate()
	if n := e.logs.count("WARN"); n != 1 {
		t.Fatalf("a certificate three days from its end logged %d warnings at boot, want 1:\n%s", n, e.logs)
	}
	e.clock.Advance(time.Hour)
	certificate()
	if n := e.logs.count("WARN"); n != 1 {
		t.Fatalf("an hour later: %d warnings, want still 1", n)
	}
	e.clock.Advance(24 * time.Hour)
	certificate()
	if n := e.logs.count("WARN"); n != 2 {
		t.Fatalf("a day later: %d warnings, want 2", n)
	}
}

// TestAPIKey_IsSentAsABearerAndARefusalIsCounted dials a gRPC stub with
// the Temporal SDK: the stub records the call's metadata and refuses it as
// unauthenticated.
func TestAPIKey_IsSentAsABearerAndARefusalIsCounted(t *testing.T) {
	dir := t.TempDir()
	ca := testpki.NewCA(t, "ca")
	serverCert := ca.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"127.0.0.1"}}).TLS(t)
	var (
		mu   sync.Mutex
		seen []string
	)
	stub := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS12})),
		grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
			md, _ := metadata.FromIncomingContext(stream.Context())
			mu.Lock()
			seen = append(seen, md.Get("authorization")...)
			mu.Unlock()
			return status.Error(codes.Unauthenticated, "refused")
		}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must.OK(t, err)
	go func() { _ = stub.Serve(ln) }()
	t.Cleanup(stub.Stop)

	caFile := testpki.Write(t, dir, "ca.pem", ca.PEM)
	keyFile := testpki.Write(t, dir, "api.key", []byte("  the-key\r\n"))
	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{CAFile: caFile, APIKeyFile: keyFile}, ln.Addr().String(), e.options())
	must.OKf(t, err, "New")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.DialContext(ctx, client.Options{
		HostPort:    ln.Addr().String(),
		Namespace:   "default",
		Credentials: sec.Credentials,
		ConnectionOptions: client.ConnectionOptions{
			TLS:         sec.TLS,
			DialOptions: sec.DialOptions,
		},
	})
	if err == nil {
		c.Close()
		t.Fatal("dial succeeded against a stub that refuses every call")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 || seen[0] != "Bearer the-key" {
		t.Fatalf("authorization = %q, want [Bearer the-key]", seen)
	}
	if _, _, auth := e.obs.snapshot(); auth == 0 {
		t.Error("a call refused as unauthenticated was not counted")
	}
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func TestSettingsFromEnv(t *testing.T) {
	for _, tt := range []struct {
		name    string
		env     map[string]string
		want    temporalconn.Settings
		refused bool
	}{
		{"nothing", nil, temporalconn.Settings{}, false},
		{"every key", map[string]string{
			"ORK_TEMPORAL_TLS_ENABLED":     "true",
			"ORK_TEMPORAL_TLS_CA_FILE":     "/ca.pem",
			"ORK_TEMPORAL_TLS_CERT_FILE":   "/c.pem",
			"ORK_TEMPORAL_TLS_KEY_FILE":    "/c.key",
			"ORK_TEMPORAL_TLS_SERVER_NAME": "temporal",
			"ORK_TEMPORAL_API_KEY_FILE":    "/api.key",
		}, temporalconn.Settings{Enabled: ptr(true), CAFile: "/ca.pem", CertFile: "/c.pem", KeyFile: "/c.key", ServerName: "temporal", APIKeyFile: "/api.key"}, false},
		{"false", map[string]string{"ORK_TEMPORAL_TLS_ENABLED": "false"}, temporalconn.Settings{Enabled: ptr(false)}, false},
		{"not a boolean", map[string]string{"ORK_TEMPORAL_TLS_ENABLED": "yes"}, temporalconn.Settings{}, true},
		{"a path of white space alone", map[string]string{"ORK_TEMPORAL_TLS_CA_FILE": "  "}, temporalconn.Settings{}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := temporalconn.SettingsFromEnv(func(k string) string { return tt.env[k] })
			if tt.refused {
				if err == nil || !strings.Contains(err.Error(), "temporal.tls.") {
					t.Fatalf("err = %v, want a refusal naming the setting", err)
				}
				return
			}
			must.OK(t, err)
			if !sameSettings(got, tt.want) {
				t.Fatalf("SettingsFromEnv = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func deref(b *bool) string {
	if b == nil {
		return "unset"
	}
	return fmt.Sprint(*b)
}

func sameSettings(a, b temporalconn.Settings) bool {
	return deref(a.Enabled) == deref(b.Enabled) && a.CAFile == b.CAFile && a.CertFile == b.CertFile &&
		a.KeyFile == b.KeyFile && a.ServerName == b.ServerName && a.APIKeyFile == b.APIKeyFile
}

// TestTLS_HandshakesWhileFilesRotate runs handshakes beside rotations of the
// client pair, each handshake a look at the files: every one presents one of
// the two whole pairs, never a mix, none fails, the pair is adopted while it
// rotates, and once the rotation stops the last pair written is the one in
// use.
func TestTLS_HandshakesWhileFilesRotate(t *testing.T) {
	dir := t.TempDir()
	serverCA := testpki.NewCA(t, "server")
	clientCA := testpki.NewCA(t, "client")
	srv := startServer(t, serverCA.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"127.0.0.1"}}).TLS(t), clientCA.Pool())
	a := clientCA.Issue(t, testpki.LeafOptions{Name: "a", Client: true})
	b := clientCA.Issue(t, testpki.LeafOptions{Name: "b", Client: true})
	caFile := testpki.Write(t, dir, "ca.pem", serverCA.PEM)
	certFile := testpki.Write(t, dir, "client.pem", a.CertPEM)
	keyFile := testpki.Write(t, dir, "client.key", a.KeyPEM)
	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{CAFile: caFile, CertFile: certFile, KeyFile: keyFile}, srv.addr, e.options())
	must.OKf(t, err, "New")

	stop := make(chan struct{})
	var rotations sync.WaitGroup
	var last struct {
		sync.Mutex
		leaf testpki.Leaf
	}
	rotations.Add(1)
	go func() {
		defer rotations.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
			leaf := a
			if i%2 == 1 {
				leaf = b
			}
			for _, f := range []struct {
				path string
				data []byte
			}{{certFile, leaf.CertPEM}, {keyFile, leaf.KeyPEM}} {
				tmp := f.path + ".tmp"
				if err := os.WriteFile(tmp, f.data, 0o600); err != nil {
					t.Error(err)
					return
				}
				if err := os.Rename(tmp, f.path); err != nil {
					t.Error(err)
					return
				}
			}
			last.Lock()
			last.leaf = leaf
			last.Unlock()
		}
	}()
	stopped := false
	stopRotating := func() {
		if !stopped {
			stopped = true
			close(stop)
			rotations.Wait()
		}
	}
	defer stopRotating()

	for n := 0; n < 200; n++ {
		e.clock.Advance(2 * time.Second) // each handshake is a look
		serial, err := handshake(t, sec.TLS, srv)
		if err != nil {
			t.Fatalf("handshake %d failed while the pair rotated: %v", n, err)
		}
		if serial.Cmp(a.Cert.SerialNumber) != 0 && serial.Cmp(b.Cert.SerialNumber) != 0 {
			t.Fatalf("handshake %d presented certificate %v, neither pair's", n, serial)
		}
	}
	stopRotating()
	e.clock.Advance(2 * time.Second)
	serial, err := handshake(t, sec.TLS, srv)
	must.OKf(t, err, "handshake after the rotation stopped")
	if want := last.leaf.Cert.SerialNumber; serial.Cmp(want) != 0 {
		t.Fatalf("after the rotation the handshake presented %v, want the last pair written, %v", serial, want)
	}
	if reloads, _, _ := e.obs.snapshot(); !contains(reloads, "temporal.tls.cert_file:true") {
		t.Fatalf("reloads = %v: the pair was never adopted while it rotated", reloads)
	}
}

// TestRefresh_SeesAChangeWithoutAHandshake: a connection that stays up makes
// no handshake; Refresh (Watch's step) still reloads and reports.
func TestRefresh_SeesAChangeWithoutAHandshake(t *testing.T) {
	dir := t.TempDir()
	ca := testpki.NewCA(t, "ca")
	e := newEnv()
	first := ca.Issue(t, testpki.LeafOptions{Name: "first", Client: true})
	second := ca.Issue(t, testpki.LeafOptions{Name: "second", Client: true, NotAfter: e.clock.Now().Add(2 * 24 * time.Hour)})
	certFile := testpki.Write(t, dir, "client.pem", first.CertPEM)
	keyFile := testpki.Write(t, dir, "client.key", first.KeyPEM)
	apiKey := testpki.Write(t, dir, "api.key", []byte("one"))
	sec, err := temporalconn.New(temporalconn.Settings{CertFile: certFile, KeyFile: keyFile, APIKeyFile: apiKey}, "127.0.0.1:7233", e.options())
	must.OKf(t, err, "New")
	write(t, certFile, second.CertPEM)
	write(t, keyFile, second.KeyPEM)
	write(t, apiKey, []byte("two"))
	e.clock.Advance(2 * time.Second)
	sec.Refresh()
	reloads, notAfter, _ := e.obs.snapshot()
	if !contains(reloads, "temporal.tls.cert_file:true") || !contains(reloads, "temporal.api_key_file:true") {
		t.Fatalf("reloads = %v, want the pair and the key", reloads)
	}
	if !notAfter["temporal.tls.cert_file"].Equal(second.Cert.NotAfter) {
		t.Fatalf("not_after = %v, want %v", notAfter["temporal.tls.cert_file"], second.Cert.NotAfter)
	}
	if !strings.Contains(e.logs.String(), "ends within a week") {
		t.Fatalf("no expiry warning:\n%s", e.logs)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sec.Watch(ctx)                                      // returns once ctx has ended
	temporalconn.Security{}.Watch(context.Background()) // nothing set: returns at once
}

// TestServerName_IsTheNameSentAndVerified: a server named apart from the
// address is the name the handshake asks for (SNI), not the address's host.
func TestServerName_IsTheNameSentAndVerified(t *testing.T) {
	dir := t.TempDir()
	ca := testpki.NewCA(t, "ca")
	serverCert := ca.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"temporal.internal"}}).TLS(t)
	sni := make(chan string, 8)
	stub := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				sni <- hello.ServerName
				return &serverCert, nil
			},
		})),
		grpc.UnknownServiceHandler(func(any, grpc.ServerStream) error {
			return status.Error(codes.Unauthenticated, "refused")
		}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	must.OK(t, err)
	go func() { _ = stub.Serve(ln) }()
	t.Cleanup(stub.Stop)

	caFile := testpki.Write(t, dir, "ca.pem", ca.PEM)
	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{CAFile: caFile, ServerName: "temporal.internal"}, ln.Addr().String(), e.options())
	must.OKf(t, err, "New")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := client.DialContext(ctx, client.Options{
		HostPort:          ln.Addr().String(),
		Namespace:         "default",
		ConnectionOptions: client.ConnectionOptions{TLS: sec.TLS, DialOptions: sec.DialOptions},
	})
	if err == nil {
		c.Close()
	}
	select {
	case got := <-sni:
		if got != "temporal.internal" {
			t.Fatalf("the handshake asked for %q, want temporal.internal", got)
		}
	default:
		t.Fatal("no handshake reached the server")
	}
	if _, _, auth := e.obs.snapshot(); auth == 0 {
		t.Fatalf("the call never got past the handshake (dial: %v)", err)
	}
}

func TestNew_RefusesAFileThatIsNotASmallRegularFile(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "api.fifo")
	must.OK(t, syscall.Mkfifo(fifo, 0o600))
	huge := testpki.Write(t, dir, "huge.key", bytes.Repeat([]byte("k"), 1<<20+1))
	for _, tt := range []struct{ name, path string }{
		{"a FIFO", fifo},
		{"a directory", dir},
		{"over a MiB", huge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() {
				_, err := temporalconn.New(temporalconn.Settings{APIKeyFile: tt.path}, "127.0.0.1:7233", newEnv().options())
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), "temporal.api_key_file") {
					t.Fatalf("err = %v, want a refusal naming temporal.api_key_file", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("New blocked reading the file")
			}
		})
	}
}

func TestNew_RefusesAnAPIKeyNoHeaderCanCarry(t *testing.T) {
	dir := t.TempDir()
	for _, key := range []string{"two\nlines", "a key", "café"} {
		path := testpki.Write(t, dir, "api.key", []byte(key))
		_, err := temporalconn.New(temporalconn.Settings{APIKeyFile: path}, "127.0.0.1:7233", newEnv().options())
		if err == nil || !strings.Contains(err.Error(), "temporal.api_key_file") || strings.Contains(err.Error(), key) {
			t.Errorf("key %q: err = %v, want a refusal naming the setting and not the key", key, err)
		}
	}
}

func TestNew_RefusesABundleCutShort(t *testing.T) {
	dir := t.TempDir()
	ca := testpki.NewCA(t, "ca")
	cut := append(append([]byte{}, ca.PEM...), []byte("-----BEGIN CERTIFICATE-----\nMIIBkTCCATegAwIBAgIBAT\n")...)
	path := testpki.Write(t, dir, "ca.pem", cut)
	_, err := temporalconn.New(temporalconn.Settings{CAFile: path}, "127.0.0.1:7233", newEnv().options())
	if err == nil || !strings.Contains(err.Error(), "temporal.tls.ca_file") {
		t.Fatalf("a bundle whose second certificate is cut short: err = %v, want a refusal naming temporal.tls.ca_file", err)
	}
}

// TestReload_AFailureThatStaysIsLoggedAgainAndARecoveryIsLogged: a refused
// file is logged once, again each hour while it stays refused, and its
// return to the value in force is logged too.
func TestReload_AFailureThatStaysIsLoggedAgainAndARecoveryIsLogged(t *testing.T) {
	dir := t.TempDir()
	path := testpki.Write(t, dir, "api.key", []byte("good-key"))
	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{APIKeyFile: path}, "127.0.0.1:7233", e.options())
	must.OKf(t, err, "New")
	look := func(d time.Duration) {
		e.clock.Advance(d)
		sec.Refresh()
	}
	write(t, path, []byte("\n"))
	look(2 * time.Second)
	look(61 * time.Second)
	if n := e.logs.count("ERROR"); n != 1 {
		t.Fatalf("after two looks at the same refused file: %d errors, want 1", n)
	}
	look(61 * time.Minute)
	if n := e.logs.count("ERROR"); n != 2 {
		t.Fatalf("an hour later: %d errors, want 2 (a failure that stays is logged again)", n)
	}
	write(t, path, []byte("good-key"))
	look(2 * time.Second)
	if !strings.Contains(e.logs.String(), "good again") {
		t.Fatalf("the file's return to the value in force was not logged:\n%s", e.logs)
	}
}

// TestReload_ALookThatDoesNotReturnIsReported: while one look is stuck in a
// read, the others serve the value in force, and once it has been stuck for
// longer than a re-read's interval that is logged.
func TestReload_ALookThatDoesNotReturnIsReported(t *testing.T) {
	dir := t.TempDir()
	path := testpki.Write(t, dir, "api.key", []byte("good-key"))
	e := newEnv()
	sec, err := temporalconn.New(temporalconn.Settings{APIKeyFile: path}, "127.0.0.1:7233", e.options())
	must.OKf(t, err, "New")
	temporalconn.HoldALookForTest(sec)
	e.clock.Advance(30 * time.Second)
	if k, _ := temporalconn.APIKeyForTest(sec, context.Background()); k != "good-key" {
		t.Fatalf("api key while a look is stuck = %q, want the value in force", k)
	}
	if strings.Contains(e.logs.String(), "has not returned") {
		t.Fatal("a look stuck for 30 s was reported")
	}
	e.clock.Advance(2 * time.Minute)
	temporalconn.APIKeyForTest(sec, context.Background())
	temporalconn.APIKeyForTest(sec, context.Background())
	if n := strings.Count(e.logs.String(), "has not returned"); n != 1 {
		t.Fatalf("a look stuck for over a minute was reported %d times, want 1:\n%s", n, e.logs)
	}
}
