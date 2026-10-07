//go:build integration

package main

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
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// tlsGitServer is the test git remote of the transform-image suites
// (first-party-transform-images §12.4): git's own `http-backend` behind a TLS
// listener whose leaf names host.docker.internal — the name every Go
// integration suite uses for a container's route to the host — signed by a CA
// generated for the test. A second listener, "elsewhere", serves the same
// handler on another port, which is another ORIGIN: the target of the
// /moved/ redirect (X9).
//
// The middleware, in order:
//
//	(0) counts every request under a repository, whatever its credentials and
//	    outcome (Requests);
//	(a) authenticates /private/*: no Basic Authorization gets 401 with a
//	    challenge (git's first request is always unauthenticated, and the
//	    challenge is what makes it ask askpass), wrong credentials get 401,
//	    and every request that passed is counted (AuthenticatedRequests);
//	    /public/* is served with no check;
//	(b) holds /stall.git/* until the request ends, and signals the test;
//	(c) answers the FIRST authenticated GET /private/flaky.git/info/refs
//	    with 500;
//	(d) answers GET /healthz with 200, for the reachability probe;
//	(e) answers GET /moved/app.git/<rest> with a 302 to the elsewhere
//	    listener's /private/app.git/<rest>.
type tlsGitServer struct {
	t    *testing.T
	git  string
	root string // GIT_PROJECT_ROOT: private/app.git, private/flaky.git, public/lib.git, stall.git
	work string // scratch work trees, one per repository

	caPEM         string
	main          *httptest.Server
	elsewhere     *httptest.Server
	port          int
	elsewherePort int

	mu             sync.Mutex
	original       string // the token minted at start: what the Orker mounts
	token          string // the token /private/* accepts now
	requests       map[string]int
	authenticated  map[string]int
	flakyFailed    bool
	elsewhereAuths []string
	stallHeld      chan struct{}
	stallOnce      sync.Once
}

// gitTestEnv is the environment of every host git command the helper runs:
// no system or global configuration, and a fixed identity, so neither a
// developer's configuration nor a CI machine without one changes what it
// builds.
func gitTestEnv() []string {
	return append(os.Environ(),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_AUTHOR_NAME=orkstack-test",
		"GIT_COMMITTER_NAME=orkstack-test",
		"GIT_AUTHOR_EMAIL=test@example.invalid",
		"GIT_COMMITTER_EMAIL=test@example.invalid",
		"GIT_TERMINAL_PROMPT=0",
	)
}

// startTLSGitServer builds the repositories and starts both listeners. A host
// without git is a missing prerequisite (infraAbsent).
func startTLSGitServer(t *testing.T) *tlsGitServer {
	t.Helper()
	gitPath, err := exec.LookPath("git")
	if err != nil {
		infraAbsent(t, "git is not on PATH: %v", err)
		return nil
	}
	base := t.TempDir()
	s := &tlsGitServer{
		t:             t,
		git:           gitPath,
		root:          filepath.Join(base, "repos"),
		work:          filepath.Join(base, "work"),
		token:         "",
		requests:      map[string]int{},
		authenticated: map[string]int{},
		stallHeld:     make(chan struct{}),
	}

	s.token = mintGitToken(t)
	s.original = s.token

	// private/app.git: main, feature/main and release branches; a lightweight
	// tag `release` (so the short name is ambiguous) and an annotated v1.0.0.
	s.newRepo("private/app.git", map[string]string{
		"VERSION":                 "1.0.0\n",
		"templates/app.conf.tmpl": "env={{ .env }}\nport={{ (ds \"defaults\").port }}\n",
		"defaults.yaml":           "port: 8080\n",
	})
	s.inWork("private/app.git", "branch", "feature/main")
	s.inWork("private/app.git", "branch", "release")
	s.inWork("private/app.git", "tag", "release")
	s.inWork("private/app.git", "tag", "-a", "v1.0.0", "-m", "v1.0.0")
	s.writeAndCommit("private/app.git", "README", "the app\n", "add a README to main only")
	s.inWork("private/app.git", "push", "--quiet", "origin", "refs/heads/main", "refs/heads/feature/main",
		"refs/heads/release", "refs/tags/release", "refs/tags/v1.0.0")

	s.newRepo("private/flaky.git", map[string]string{"VERSION": "flaky-1\n"})
	s.inWork("private/flaky.git", "push", "--quiet", "origin", "main")
	s.newRepo("public/lib.git", map[string]string{"VERSION": "lib-1\n"})
	s.inWork("public/lib.git", "push", "--quiet", "origin", "main")
	s.newRepo("stall.git", map[string]string{"VERSION": "stall\n"})
	s.inWork("stall.git", "push", "--quiet", "origin", "main")

	caPEM, leaf := testCertificates(t)
	s.caPEM = caPEM
	backend := &cgi.Handler{
		Path: gitPath,
		Args: []string{"http-backend"},
		Env:  []string{"GIT_PROJECT_ROOT=" + s.root, "GIT_HTTP_EXPORT_ALL=1"},
	}
	s.main, s.port = startTLSListener(t, leaf, s.middleware(backend, false))
	s.elsewhere, s.elsewherePort = startTLSListener(t, leaf, s.middleware(backend, true))
	t.Cleanup(func() {
		s.main.Close()
		s.elsewhere.Close()
	})
	t.Logf("TLS git server on :%d (elsewhere :%d)", s.port, s.elsewherePort)
	return s
}

func mintGitToken(t *testing.T) string {
	t.Helper()
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("mint: %v", err)
	}
	return fmt.Sprintf("%x", b)
}

func startTLSListener(t *testing.T, leaf tls.Certificate, handler http.Handler) (*httptest.Server, int) {
	t.Helper()
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.TLS = &tls.Config{Certificates: []tls.Certificate{leaf}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	return server, listener.Addr().(*net.TCPAddr).Port
}

// testCertificates generates a CA and a leaf for host.docker.internal (and
// the loopback names, so a host-side check can use them too).
func testCertificates(t *testing.T) (string, tls.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("CA key: %v", err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "orkstack transform-images test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "host.docker.internal"},
		DNSNames:     []string{"host.docker.internal", "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf certificate: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})),
		tls.Certificate{Certificate: [][]byte{leafDER}, PrivateKey: leafKey}
}

// repoOf names the repository a request path is under — everything up to and
// including the first segment ending in ".git" — or "" for none.
func repoOf(path string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, segment := range segments {
		if strings.HasSuffix(segment, ".git") {
			return strings.Join(segments[:i+1], "/")
		}
	}
	return ""
}

func (s *tlsGitServer) middleware(backend http.Handler, elsewhere bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if elsewhere {
			if header := r.Header.Get("Authorization"); header != "" {
				decoded := header
				if raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(header, "Basic ")); err == nil {
					decoded = string(raw)
				}
				s.mu.Lock()
				s.elsewhereAuths = append(s.elsewhereAuths, decoded)
				s.mu.Unlock()
			}
		}
		// (d)
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		repo := repoOf(r.URL.Path)
		// (0)
		if repo != "" {
			s.mu.Lock()
			s.requests[repo]++
			s.mu.Unlock()
		}
		// (e)
		if rest, ok := strings.CutPrefix(r.URL.Path, "/moved/app.git/"); ok && !elsewhere {
			location := fmt.Sprintf("https://host.docker.internal:%d/private/app.git/%s", s.elsewherePort, rest)
			if r.URL.RawQuery != "" {
				location += "?" + r.URL.RawQuery
			}
			http.Redirect(w, r, location, http.StatusFound)
			return
		}
		// (a)
		if strings.HasPrefix(r.URL.Path, "/private/") {
			user, password, ok := r.BasicAuth()
			s.mu.Lock()
			token := s.token
			s.mu.Unlock()
			if !ok || user != "x-access-token" || password != token {
				w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
				http.Error(w, "authentication required", http.StatusUnauthorized)
				return
			}
			s.mu.Lock()
			s.authenticated[repo]++
			// (c)
			fail := repo == "private/flaky.git" && strings.HasSuffix(r.URL.Path, "/info/refs") && !s.flakyFailed
			if fail {
				s.flakyFailed = true
			}
			s.mu.Unlock()
			if fail {
				http.Error(w, "forced failure", http.StatusInternalServerError)
				return
			}
		}
		// (b)
		if repo == "stall.git" {
			s.stallOnce.Do(func() { close(s.stallHeld) })
			<-r.Context().Done()
			return
		}
		backend.ServeHTTP(w, r)
	})
}

// URL is a container-side URL of a repository path on the main listener.
func (s *tlsGitServer) URL(path string) string {
	return fmt.Sprintf("https://host.docker.internal:%d/%s", s.port, path)
}

// HealthURLs are the probe URLs of both listeners.
func (s *tlsGitServer) HealthURLs() []string {
	return []string{
		fmt.Sprintf("https://host.docker.internal:%d/healthz", s.port),
		fmt.Sprintf("https://host.docker.internal:%d/healthz", s.elsewherePort),
	}
}

func (s *tlsGitServer) ElsewherePort() int { return s.elsewherePort }

// CA is the PEM of the CA that signed both listeners' leaf.
func (s *tlsGitServer) CA() string { return s.caPEM }

// Token is the token /private/* currently accepts.
func (s *tlsGitServer) Token() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

// originalToken is the token minted at start — the value a deployment
// mounts, and the one no output may ever carry, revoked or not.
func (s *tlsGitServer) originalToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.original
}

// SetToken replaces the accepted token: the old one is revoked.
func (s *tlsGitServer) SetToken(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = token
}

func (s *tlsGitServer) Requests(repo string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[repo]
}

func (s *tlsGitServer) AuthenticatedRequests(repo string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.authenticated[repo]
}

func (s *tlsGitServer) ElsewhereAuthorizations() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.elsewhereAuths...)
}

// StallHeld is closed once a /stall.git request is being held.
func (s *tlsGitServer) StallHeld() <-chan struct{} { return s.stallHeld }

// Commit is `git rev-parse <rev>` in the bare repository.
func (s *tlsGitServer) Commit(repo, rev string) string {
	s.t.Helper()
	return strings.TrimSpace(s.run(filepath.Join(s.root, repo), "rev-parse", rev))
}

// CommitVersion pushes a commit to main that changes only VERSION, and
// returns its id.
func (s *tlsGitServer) CommitVersion(repo, content string) string {
	s.t.Helper()
	s.writeAndCommit(repo, "VERSION", content, "bump VERSION")
	s.inWork(repo, "push", "--quiet", "origin", "main")
	return s.Commit(repo, "refs/heads/main")
}

func (s *tlsGitServer) run(dir string, args ...string) string {
	s.t.Helper()
	cmd := exec.Command(s.git, args...)
	cmd.Dir = dir
	cmd.Env = gitTestEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		s.t.Fatalf("git %s (in %s): %v\n%s", strings.Join(args, " "), dir, err, out)
	}
	return string(out)
}

func (s *tlsGitServer) inWork(repo string, args ...string) {
	s.t.Helper()
	s.run(filepath.Join(s.work, repo), args...)
}

// newRepo creates the bare repository, points its HEAD at refs/heads/main
// whatever the host's init.defaultBranch says, allows fetching a reachable
// commit by id, and commits files to main in a scratch work tree whose
// origin is the bare repository.
func (s *tlsGitServer) newRepo(repo string, files map[string]string) {
	s.t.Helper()
	bare := filepath.Join(s.root, repo)
	if err := os.MkdirAll(bare, 0o755); err != nil {
		s.t.Fatalf("mkdir: %v", err)
	}
	s.run(bare, "init", "--quiet", "--bare")
	s.run(bare, "symbolic-ref", "HEAD", "refs/heads/main")
	s.run(bare, "config", "uploadpack.allowReachableSHA1InWant", "true")

	work := filepath.Join(s.work, repo)
	if err := os.MkdirAll(work, 0o755); err != nil {
		s.t.Fatalf("mkdir: %v", err)
	}
	s.run(work, "init", "--quiet")
	s.run(work, "symbolic-ref", "HEAD", "refs/heads/main")
	s.run(work, "remote", "add", "origin", bare)
	for name, content := range files {
		path := filepath.Join(work, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			s.t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			s.t.Fatalf("write %s: %v", name, err)
		}
	}
	s.run(work, "add", "-A")
	s.run(work, "commit", "--quiet", "-m", "initial")
}

func (s *tlsGitServer) writeAndCommit(repo, name, content, message string) {
	s.t.Helper()
	work := filepath.Join(s.work, repo)
	if err := os.WriteFile(filepath.Join(work, name), []byte(content), 0o644); err != nil {
		s.t.Fatalf("write %s: %v", name, err)
	}
	s.run(work, "add", name)
	s.run(work, "commit", "--quiet", "-m", message)
}

// requireContainerCanReachTLS is requireContainerCanReach for an HTTPS
// origin: the probe alone skips certificate verification (it proves the
// route, not the trust). An unreachable origin is a missing prerequisite of
// the machine — infraAbsent.
func requireContainerCanReachTLS(ctx context.Context, t *testing.T, url string) {
	t.Helper()
	image := os.Getenv("ORK_TEST_CONTAINER_IMAGE")
	if image == "" {
		image = "alpine:3.20"
	}
	probe := exec.CommandContext(ctx, "docker", "run", "--rm", image,
		"wget", "-q", "-O", "/dev/null", "--no-check-certificate", url)
	if out, err := probe.CombinedOutput(); err != nil {
		infraAbsent(t, "a container cannot reach the test git server %s (%v): %s", url, err, out)
	}
}
