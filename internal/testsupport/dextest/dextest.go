// Package dextest runs Dex, the OpenID Connect provider the development
// stack (make up, through test/tools/dexrun), the sign-in interop
// test (internal/api) and the sign-in Playwright spec (through dexrun) sign
// people in at. It starts the image deploy/dex-image.txt pins with the
// configuration deploy/dex/config.yaml holds, its users and clients there,
// serving https on a loopback port with a certificate it makes. It never
// pulls the image: `make up` and the proof hosts' setup pull it.
//
// Dex is a real server, so it cannot be made to fail on demand. The negative
// cases (a broken key set, a key rotated mid-session, clock skew, a refresh
// or revocation the issuer refuses, no S256) stay with
// internal/testsupport/oidctest.
package dextest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// The files of the repository this package reads.
const (
	ImageFile  = "deploy/dex-image.txt"
	ConfigFile = "deploy/dex/config.yaml"
)

// Password is every configured user's password.
const Password = "password"

// The users deploy/dex/config.yaml configures, by email, and their groups.
const (
	DevUser   = "dev@example.test"   // dev-admins
	AdminUser = "admin@example.test" // admins
	ViewUser  = "vera@example.test"  // viewers
)

// The clients deploy/dex/config.yaml configures.
const (
	WebClientID = "orkstack-web"
	CLIClientID = "orkctl"
)

// pinned is the only shape deploy/dex-image.txt may name.
var pinned = regexp.MustCompile(`^ghcr\.io/dexidp/dex:v[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$`)

// RepoRoot is the root of the module that holds dir: the first directory at
// or above it with a go.mod.
func RepoRoot(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("dextest: resolve %s: %w", dir, err)
	}
	for d := abs; ; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d, nil
		}
		if filepath.Dir(d) == d {
			return "", fmt.Errorf("dextest: no go.mod at or above %s", abs)
		}
	}
}

// Image is the image deploy/dex-image.txt under root pins: its first line
// that is neither blank nor a comment, which must be a digest-pinned Dex.
func Image(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(ImageFile)))
	if err != nil {
		return "", fmt.Errorf("dextest: read %s: %w", ImageFile, err)
	}
	return imageFrom(string(data))
}

func imageFrom(data string) (string, error) {
	for _, line := range strings.Split(data, "\n") {
		if line = strings.TrimSpace(line); line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !pinned.MatchString(line) {
			return "", fmt.Errorf("dextest: %s names %q, not ghcr.io/dexidp/dex:vX.Y.Z@sha256:<64 hex>", ImageFile, line)
		}
		return line, nil
	}
	return "", fmt.Errorf("dextest: %s names no image", ImageFile)
}

// ErrNoImage is an image the daemon does not hold: the caller pulls it, or
// skips where its suite allows.
var ErrNoImage = errors.New("the Dex image is not on this host")

// Options is one Dex to start.
type Options struct {
	// Root is the repository root (RepoRoot).
	Root string
	// Dir holds what this Dex keeps across starts: its CA and certificate
	// (tls/), and the web client's secret (web-client.secret). Made when
	// absent, kept when present, so a browser or orkctl that trusts the CA
	// keeps trusting it.
	Dir string
	// Addr is the loopback address it is published on; a port of 0 picks a
	// free one.
	Addr string
	// WebRedirect is the engine's web callback, the one redirect the
	// orkstack-web client may use.
	WebRedirect string
	// ProofID and Owner, when Owner is set, label the container as a proof's
	// own (orkstack.proof, orkstack.proof.owner host:pid:start), so
	// `proofrun clean` stops it once that owner is gone. The development
	// stack leaves Owner empty: its Dex lives until `make down`.
	ProofID, Owner string
}

// Dex is one Dex that runs.
type Dex struct {
	// ID is the container's id.
	ID string
	// Issuer is its issuer URL: https://<addr>/dex.
	Issuer string
	// CAFile is the PEM CA its certificate chains to.
	CAFile string
	// WebSecret is the orkstack-web client's secret, and SecretFile the file
	// that holds it.
	WebSecret, SecretFile string
}

// Start starts a Dex and waits, at most a minute, for its discovery document
// to answer over https. It never pulls: an image the daemon lacks is
// ErrNoImage.
func Start(ctx context.Context, o Options) (*Dex, error) {
	image, err := Image(o.Root)
	if err != nil {
		return nil, err
	}
	if out, err := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", image).CombinedOutput(); err != nil {
		// Only an image the daemon does not hold is a missing prerequisite;
		// no docker CLI or no daemon is a plain failure, not a pull to run.
		if strings.Contains(strings.ToLower(string(out)), "no such image") {
			return nil, fmt.Errorf("%w: %s", ErrNoImage, image)
		}
		return nil, fmt.Errorf("dextest: inspect %s: %v: %s", image, err, strings.TrimSpace(string(out)))
	}
	root, err := filepath.Abs(o.Root)
	if err != nil {
		return nil, fmt.Errorf("dextest: %w", err)
	}
	dir, err := filepath.Abs(o.Dir)
	if err != nil {
		return nil, fmt.Errorf("dextest: %w", err)
	}
	// docker -v reads a relative path as a volume's name, never a directory.
	o.Root, o.Dir = root, dir
	addr, err := freeAddr(o.Addr)
	if err != nil {
		return nil, err
	}
	tlsDir := filepath.Join(o.Dir, "tls")
	caFile, err := EnsureTLS(tlsDir)
	if err != nil {
		return nil, err
	}
	secretFile := filepath.Join(o.Dir, "web-client.secret")
	secret, err := ensureSecret(secretFile)
	if err != nil {
		return nil, err
	}
	issuer := "https://" + addr + "/dex"
	args := RunArgs(image, o, addr, issuer, tlsDir, filepath.Join(o.Root, filepath.FromSlash(ConfigFile)))
	cmd := exec.CommandContext(ctx, "docker", args...)
	// The secret goes to Dex through docker's environment, never its argv.
	cmd.Env = append(os.Environ(), "DEX_WEB_SECRET="+secret)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("dextest: docker run %s: %v: %s", image, err, strings.TrimSpace(string(out)))
	}
	d := &Dex{ID: strings.TrimSpace(string(out)), Issuer: issuer, CAFile: caFile, WebSecret: secret, SecretFile: secretFile}
	if err := d.wait(ctx); err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		logs, _ := exec.CommandContext(cleanup, "docker", "logs", "--tail", "40", d.ID).CombinedOutput()
		if running(cleanup, d.ID) {
			if stopErr := Stop(cleanup, d.ID); stopErr != nil {
				err = errors.Join(err, stopErr)
			}
		}
		return nil, fmt.Errorf("%w; its log:\n%s", err, logs)
	}
	return d, nil
}

// RunArgs is the `docker` command line that starts Dex: the pinned image,
// never pulled, removed when stopped, as this user (so it reads the key it
// is handed), published on loopback only, its configuration and certificate
// mounted read-only, and DEX_WEB_SECRET taken from docker's own environment.
func RunArgs(image string, o Options, addr, issuer, tlsDir, config string) []string {
	args := []string{"run", "-d", "--rm", "--pull", "never",
		"--user", strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid()),
		"-p", addr + ":5554",
		"-v", config + ":/etc/dex/config.yaml:ro",
		"-v", tlsDir + ":/etc/dex/tls:ro",
		"-e", "DEX_ISSUER=" + issuer,
		"-e", "DEX_WEB_REDIRECT=" + o.WebRedirect,
		"-e", "DEX_WEB_SECRET",
		"--label", "orkstack.dex=" + issuer,
	}
	if o.Owner != "" {
		args = append(args, "--label", "orkstack.proof="+o.ProofID, "--label", "orkstack.proof.owner="+o.Owner)
	}
	return append(args, image, "dex", "serve", "/etc/dex/config.yaml")
}

// Stop stops a Dex; --rm removes it.
func Stop(ctx context.Context, id string) error {
	if out, err := exec.CommandContext(ctx, "docker", "stop", "-t", "5", id).CombinedOutput(); err != nil {
		return fmt.Errorf("dextest: docker stop %s: %v: %s", id, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// Client is an HTTP client that trusts this Dex's CA alone.
func (d *Dex) Client() (*http.Client, error) {
	return ClientTrusting(d.CAFile)
}

// ClientTrusting is an HTTP client that trusts the PEM CA in caFile alone.
func ClientTrusting(caFile string) (*http.Client, error) {
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("dextest: read %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("dextest: %s holds no certificate", caFile)
	}
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}, nil
}

func (d *Dex) wait(ctx context.Context) error {
	c, err := d.Client()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var last error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.Issuer+"/.well-known/openid-configuration", nil)
		if err != nil {
			return fmt.Errorf("dextest: %w", err)
		}
		resp, err := c.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			err = fmt.Errorf("discovery answered %s", resp.Status)
		}
		last = err
		// A Dex that exited (a configuration it refuses, a port taken) is
		// gone at once: --rm removed it with its log, and waiting the rest of
		// the minute would only hide that.
		if !running(ctx, d.ID) {
			return fmt.Errorf("dextest: Dex at %s exited before it answered (run it in the foreground to see why: docker run --rm with the same arguments): %v", d.Issuer, last)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("dextest: Dex at %s did not answer within a minute: %v", d.Issuer, last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// running reports whether the container still runs.
func running(ctx context.Context, id string) bool {
	out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.Running}}", id).Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

// freeAddr is addr, or addr's host at a free port when its port is 0. The
// host must be a loopback IP: Dex is published on loopback only.
func freeAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("dextest: address %q: %w", addr, err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return "", fmt.Errorf("dextest: address %q is not a loopback IP", addr)
	}
	if port != "0" {
		return addr, nil
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return "", fmt.Errorf("dextest: pick a port: %w", err)
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}

// EnsureTLS makes, under dir, a CA (ca.pem) and a server certificate it
// signs for 127.0.0.1, ::1 and localhost (tls.crt, tls.key), once: files
// already there are kept. It returns the CA's path.
func EnsureTLS(dir string) (string, error) {
	caFile, crtFile, keyFile := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "tls.crt"), filepath.Join(dir, "tls.key")
	if allExist(caFile, crtFile, keyFile) {
		return caFile, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("dextest: %w", err)
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", fmt.Errorf("dextest: CA key: %w", err)
	}
	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: "OrkStack development Dex CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(5, 0, 0),
		IsCA: true, BasicConstraintsValid: true, MaxPathLenZero: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return "", fmt.Errorf("dextest: CA certificate: %w", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		return "", fmt.Errorf("dextest: CA certificate: %w", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", fmt.Errorf("dextest: server key: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.AddDate(5, 0, 0),
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		KeyUsage:    x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return "", fmt.Errorf("dextest: server certificate: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", fmt.Errorf("dextest: server key: %w", err)
	}
	// The key last: its presence is what says the set is whole.
	for _, f := range []struct {
		path  string
		block *pem.Block
	}{
		{caFile, &pem.Block{Type: "CERTIFICATE", Bytes: caDER}},
		{crtFile, &pem.Block{Type: "CERTIFICATE", Bytes: der}},
		{keyFile, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}},
	} {
		if err := os.WriteFile(f.path, pem.EncodeToMemory(f.block), 0o600); err != nil {
			return "", fmt.Errorf("dextest: %w", err)
		}
	}
	return caFile, nil
}

// ensureSecret is the secret in path, made once: 32 random bytes in
// base64url.
func ensureSecret(path string) (string, error) {
	b, err := os.ReadFile(path)
	switch {
	case err == nil && strings.TrimSpace(string(b)) != "":
		return strings.TrimSpace(string(b)), nil
	case err != nil && !errors.Is(err, os.ErrNotExist):
		return "", fmt.Errorf("dextest: read %s: %w", path, err)
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("dextest: secret: %w", err)
	}
	s := base64.RawURLEncoding.EncodeToString(buf)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("dextest: %w", err)
	}
	if err := os.WriteFile(path, []byte(s+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("dextest: %w", err)
	}
	return s, nil
}

// ProcessOwner is this process as an orkstack.proof.owner label names it:
// host:pid:start, the start its /proc/self/stat records (field 22), or
// ORK_PROOF_OWNER when a proof names its own. Empty where neither can be
// told (off Linux): the container is then not labelled, and is removed by
// its own Stop alone.
func ProcessOwner() string {
	if named := os.Getenv("ORK_PROOF_OWNER"); named != "" {
		return named
	}
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return ""
	}
	// The command name, field 2, is in parentheses and may hold spaces: the
	// fields that follow start after its last ')'.
	i := strings.LastIndexByte(string(stat), ')')
	if i < 0 {
		return ""
	}
	fields := strings.Fields(string(stat)[i+1:])
	if len(fields) < 20 {
		return ""
	}
	return host + ":" + strconv.Itoa(os.Getpid()) + ":" + fields[19]
}

func allExist(paths ...string) bool {
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			return false
		}
	}
	return true
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return big.NewInt(time.Now().UnixNano())
	}
	return n
}
