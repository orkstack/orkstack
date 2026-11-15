// Package temporalconn builds how ork and orker reach Temporal securely: TLS,
// mutual TLS and an API key, from the temporal.tls.* and temporal.api_key_file
// settings (docs/DEPLOY.md §1). It reads only the files those settings name,
// re-reads them when they change, and never logs or returns their content.
//
// The one place its result is used is telemetry.ClientOptions, which copies
// it into the client options both binaries dial with; with nothing set the
// result is the zero Security and the options are what they were before this
// package existed.
//
// There is no setting that skips verification of the server. With a CA file
// the server's chain is verified against that file's current roots by the
// connection's own verification callback (verifyServer), because the
// standard library reads RootCAs once per configuration and the file may be
// rotated; without one, by the standard library against the system roots.
package temporalconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Settings are the temporal.tls.* settings and temporal.api_key_file, as
// written. Every path is a file the process reads; none is a value.
type Settings struct {
	// Enabled is temporal.tls.enabled: nil when not written.
	Enabled    *bool
	CAFile     string
	CertFile   string
	KeyFile    string
	ServerName string
	APIKeyFile string
}

// The settings, by the names a refusal and a metric use.
const (
	SettingEnabled    = "temporal.tls.enabled"
	SettingCAFile     = "temporal.tls.ca_file"
	SettingCertFile   = "temporal.tls.cert_file"
	SettingKeyFile    = "temporal.tls.key_file"
	SettingServerName = "temporal.tls.server_name"
	SettingAPIKeyFile = "temporal.api_key_file"
)

// envOf is each setting's variable.
var envOf = map[string]string{
	SettingEnabled:    "ORK_TEMPORAL_TLS_ENABLED",
	SettingCAFile:     "ORK_TEMPORAL_TLS_CA_FILE",
	SettingCertFile:   "ORK_TEMPORAL_TLS_CERT_FILE",
	SettingKeyFile:    "ORK_TEMPORAL_TLS_KEY_FILE",
	SettingServerName: "ORK_TEMPORAL_TLS_SERVER_NAME",
	SettingAPIKeyFile: "ORK_TEMPORAL_API_KEY_FILE",
}

// SettingsFromEnv reads the settings from the environment, which the
// configuration file's keys are mapped onto (internal/config).
func SettingsFromEnv(getenv func(string) string) (Settings, error) {
	var s Settings
	for _, f := range []struct {
		setting string
		into    *string
	}{
		{SettingCAFile, &s.CAFile}, {SettingCertFile, &s.CertFile}, {SettingKeyFile, &s.KeyFile},
		{SettingServerName, &s.ServerName}, {SettingAPIKeyFile, &s.APIKeyFile},
	} {
		raw := getenv(envOf[f.setting])
		*f.into = strings.TrimSpace(raw)
		if raw != "" && *f.into == "" {
			// A value that is only white space is a mistake (a template whose
			// secret was missing), never "unset": read as unset it would dial
			// in plaintext without a word.
			return Settings{}, refusal(f.setting, "is white space alone; leave it unset or give it a value")
		}
	}
	switch v := strings.TrimSpace(getenv(envOf[SettingEnabled])); strings.ToLower(v) {
	case "":
	case "true", "1":
		s.Enabled = ptr(true)
	case "false", "0":
		s.Enabled = ptr(false)
	default:
		return Settings{}, refusal(SettingEnabled, fmt.Sprintf("%q is not true or false", v))
	}
	return s, nil
}

// SettingError is a refusal of one setting. Its reason never holds a file's
// content.
type SettingError struct {
	Setting string
	Reason  string
}

func (e *SettingError) Error() string {
	return fmt.Sprintf("%s (%s): %s", e.Setting, envOf[e.Setting], e.Reason)
}

func refusal(setting, reason string) error {
	return &SettingError{Setting: setting, Reason: reason}
}

// Observer hears what the files did. Its methods must not block; the one
// implementation is telemetry's metrics. setting is the setting that names
// the file (SettingCAFile, …), never its path.
type Observer interface {
	// FileReloaded reports a re-read after boot, adopted (ok) or refused.
	FileReloaded(setting string, ok bool)
	// CertificateLoaded reports the end of the certificates a file holds:
	// the earliest of them.
	CertificateLoaded(setting string, notAfter time.Time)
	// AuthFailure counts one call the cluster refused as unauthenticated.
	AuthFailure()
}

// Options are what New needs beside the settings.
type Options struct {
	Logger   *slog.Logger
	Observer Observer
	// Now is the clock that paces re-reads and expiry warnings; nil means
	// time.Now. A certificate's validity is always judged on the real clock.
	Now func() time.Time
}

// Security is what a Temporal client dials with: the TLS configuration, the
// API key's credentials and the dial options that count refused calls. The
// zero value is a plaintext connection with no credential.
type Security struct {
	TLS         *tls.Config
	Credentials client.Credentials
	DialOptions []grpc.DialOption
	files       []string
	apiKey      func(context.Context) (string, error)
	looks       []func()
	keyWatch    *watched[apiKey] // for tests
}

// Refresh looks at every file now, as a handshake or a call would: a file
// that changed is re-read, its reload and its end are reported, and a
// certificate near its end is warned about. Watch calls it.
func (s Security) Refresh() {
	for _, look := range s.looks {
		look()
	}
}

// Watch refreshes once a minute until ctx ends. A connection that stays up
// makes no handshake, so without it a rotated or expiring file would be seen
// only at the next reconnect. Nothing set, it returns at once.
func (s Security) Watch(ctx context.Context) {
	if len(s.looks) == 0 {
		return
	}
	ticker := time.NewTicker(rereadEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Refresh()
		}
	}
}

// Enabled reports whether s dials with TLS.
func (s Security) Enabled() bool { return s.TLS != nil }

// Files are the paths the settings name, for the Orker's own paths: no secret
// floor may admit one as a task's secret.
func (s Security) Files() []string { return append([]string(nil), s.files...) }

// New checks the settings as boot does and builds the Security they ask for.
// hostPort is temporal.address, whose host is the name the server's
// certificate must hold unless ServerName says otherwise.
//
// The rules (docs/DEPLOY.md, "Temporal over TLS"): any file or a server name
// turns TLS on, and enabled: false beside one is refused, so a key is never
// sent in plaintext; a certificate and its key come together; every named file
// must read and parse, the key must match its certificate and an API key must
// not be empty. The minimum is TLS 1.2.
func New(s Settings, hostPort string, o Options) (Security, error) {
	o = o.withDefaults()
	named := s.CAFile != "" || s.CertFile != "" || s.KeyFile != "" || s.ServerName != "" || s.APIKeyFile != ""
	if s.Enabled != nil && !*s.Enabled && named {
		return Security{}, refusal(SettingEnabled, "false beside a TLS file, a server name or an API key: TLS cannot be off while one is set")
	}
	if !named && (s.Enabled == nil || !*s.Enabled) {
		return Security{}, nil
	}
	if (s.CertFile == "") != (s.KeyFile == "") {
		if s.CertFile == "" {
			return Security{}, refusal(SettingCertFile, "not set beside "+SettingKeyFile+": a client key needs its certificate")
		}
		return Security{}, refusal(SettingKeyFile, "not set beside "+SettingCertFile+": a client certificate needs its key")
	}
	serverName := s.ServerName
	if serverName == "" {
		host, _, err := net.SplitHostPort(hostPort)
		if err != nil || host == "" {
			return Security{}, refusal(SettingServerName, fmt.Sprintf("not set, and temporal.address %q is not a host:port whose host the server's certificate can be verified against", hostPort))
		}
		serverName = host
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	sec := Security{TLS: cfg}
	if s.CAFile != "" {
		roots, err := watch(o, SettingCAFile, []string{s.CAFile}, loadRoots)
		if err != nil {
			return Security{}, err
		}
		// The standard library would verify against RootCAs, read once;
		// verifyServer verifies against the file's current roots instead,
		// in full: chain, validity, usage and name. It is the only thing
		// InsecureSkipVerify hands verification to, and it fails closed.
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			return verifyServer(cs, serverName, roots.current().pool)
		}
		sec.looks = append(sec.looks, func() { roots.current() })
		sec.files = append(sec.files, s.CAFile)
	}
	if s.CertFile != "" {
		pair, err := watch(o, SettingCertFile, []string{s.CertFile, s.KeyFile}, loadPair)
		if err != nil {
			return Security{}, err
		}
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			c := pair.current().cert
			return &c, nil
		}
		sec.looks = append(sec.looks, func() { pair.current() })
		sec.files = append(sec.files, s.CertFile, s.KeyFile)
	}
	if s.APIKeyFile != "" {
		key, err := watch(o, SettingAPIKeyFile, []string{s.APIKeyFile}, loadAPIKey)
		if err != nil {
			return Security{}, err
		}
		sec.apiKey = func(context.Context) (string, error) { return key.current().key, nil }
		sec.Credentials = client.NewAPIKeyDynamicCredentials(sec.apiKey)
		sec.looks = append(sec.looks, func() { key.current() })
		sec.keyWatch = key
		sec.files = append(sec.files, s.APIKeyFile)
	}
	sec.DialOptions = []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(countUnauthenticated(o.Observer)),
		grpc.WithChainStreamInterceptor(countUnauthenticatedStream(o.Observer)),
	}
	return sec, nil
}

func (o Options) withDefaults() Options {
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	if o.Observer == nil {
		o.Observer = nopObserver{}
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// verifyServer verifies the server's chain as the standard library would
// with RootCAs set to roots and ServerName to name.
func verifyServer(cs tls.ConnectionState, name string, roots *x509.CertPool) error {
	if len(cs.PeerCertificates) == 0 {
		return errors.New("temporal TLS: the server presented no certificate")
	}
	if roots == nil {
		return errors.New("temporal TLS: no roots to verify the server against")
	}
	intermediates := x509.NewCertPool()
	for _, c := range cs.PeerCertificates[1:] {
		intermediates.AddCert(c)
	}
	_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		DNSName:       name,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err != nil {
		return fmt.Errorf("temporal TLS: the server's certificate does not verify against %s: %w", SettingCAFile, err)
	}
	return nil
}

func countUnauthenticated(obs Observer) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if status.Code(err) == codes.Unauthenticated {
			obs.AuthFailure()
		}
		return err
	}
}

func countUnauthenticatedStream(obs Observer) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if status.Code(err) == codes.Unauthenticated {
			obs.AuthFailure()
		}
		return stream, err
	}
}

type nopObserver struct{}

func (nopObserver) FileReloaded(string, bool)           {}
func (nopObserver) CertificateLoaded(string, time.Time) {}
func (nopObserver) AuthFailure()                        {}

func ptr(b bool) *bool { return &b }
