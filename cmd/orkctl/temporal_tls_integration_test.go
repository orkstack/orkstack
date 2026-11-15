//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/orkstack/orkstack/internal/task"
	"github.com/orkstack/orkstack/internal/testsupport/bootwait"
	"github.com/orkstack/orkstack/internal/testsupport/must"
	"github.com/orkstack/orkstack/internal/testsupport/testbin"
	"github.com/orkstack/orkstack/internal/testsupport/testpki"
	"github.com/orkstack/orkstack/internal/testsupport/tlsfront"
)

// tlsStack is an engine and an Orker that reach the proof's Temporal only
// through a front that terminates TLS (internal/testsupport/tlsfront), each
// with its own Temporal settings.
type tlsStack struct {
	t         *testing.T
	ctx       context.Context
	cli       func(args ...string) (string, string, int)
	namespace string
	engine    <-chan struct{}
	orker     <-chan struct{}
}

// startTLSStack boots the Orker and the engine against frontAddr, with
// engineTLS and orkerTLS after their usual settings.
func startTLSStack(t *testing.T, frontAddr string, engineTLS, orkerTLS []string) *tlsStack {
	t.Helper()
	address := temporalAddress()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	if err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	}
	_ = conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)

	pool := privateTaskPool()
	queue, err := task.QueueForPool(pool, "")
	must.OKf(t, err, "map test pool")
	temporalNamespace := privateTemporalNamespace()
	orkctl := testbin.Path(t, "./cmd/orkctl")
	root := repoRoot(t)

	orkerEnv := append([]string{
		"ORK_TEMPORAL_ADDRESS=" + frontAddr,
		"ORK_TEMPORAL_NAMESPACE=" + temporalNamespace,
		"ORK_ORKER_QUEUE=" + queue,
		"ORK_ORKER_WORKDIR=" + bindableOrkerWorkRoot(t),
		"ORK_ARTIFACT_BACKEND=local",
		"ORK_ARTIFACT_DIR=" + sharedLocalArtifactDir(t),
	}, orkerTLS...)
	s := &tlsStack{t: t, ctx: ctx}
	s.orker = startProcess(ctx, t, "orker", testbin.Path(t, "./cmd/orker"), orkerEnv...)
	addr := freeAddress(t)
	s.engine = startProcess(ctx, t, "ork", testbin.Path(t, "./cmd/ork"),
		append(engineEnv(t, addr, frontAddr, temporalNamespace, queue), engineTLS...)...)
	server := "http://" + addr
	s.cli = func(args ...string) (string, string, int) {
		full := append(append([]string(nil), args...), "--server", server)
		if s.namespace != "" && !containsArg(args, "-n") {
			full = append(full, "-n", s.namespace)
		}
		return runBinary(ctx, t, orkctl, root, full...)
	}
	waitForEngine(t, s.engine, s.cli)
	namespace := fmt.Sprintf("tls-%d-%d", os.Getpid(), time.Now().UnixNano())
	if _, stderr, code := s.cli("namespace", "create", namespace); code != 0 {
		t.Fatalf("namespace create: %d %s", code, stderr)
	}
	s.namespace = namespace
	waitForPoolLive(t, s.cli, s.namespace, pool)
	return s
}

func (s *tlsStack) must(args ...string) string {
	s.t.Helper()
	stdout, stderr, code := s.cli(args...)
	if code != 0 {
		s.t.Fatalf("orkctl %v exit %d\nstdout:\n%s\nstderr:\n%s", args, code, stdout, stderr)
	}
	return stdout
}

// submit creates an Orkflow of one shell task running script and submits it.
func (s *tlsStack) submit(name, script string) string {
	s.t.Helper()
	doc := fmt.Sprintf("apiVersion: orkstack/v1\nkind: Orkflow\nname: %s\nversion: \"0.1.0\"\ntasks:\n"+
		"  - id: work\n    uses: shell\n    timeout_seconds: 180\n    with:\n      op: exec\n"+
		"      command: [\"sh\", \"-c\", %q]\n", name, script)
	path := filepath.Join(s.t.TempDir(), name+".ork.yaml")
	must.OK(s.t, os.WriteFile(path, []byte(doc), 0o600))
	s.must("orkflow", "create", path)
	return decodeSubmittedID(s.t, s.must("--json", "run", "submit", name))
}

// status is a run's status as `run get --json` reports it.
func (s *tlsStack) status(runID string) string {
	s.t.Helper()
	var doc runJSON
	must.OKf(s.t, json.Unmarshal([]byte(s.must("run", "get", runID, "--json")), &doc), "decode run get")
	return doc.Run.Status
}

// until waits for the run's status to hold one of want.
func (s *tlsStack) until(runID string, want ...string) string {
	s.t.Helper()
	var last string
	_, err := bootwait.Wait{What: fmt.Sprintf("run %s to be %v", runID, want), Logf: s.t.Logf}.Until(s.ctx,
		func(context.Context) error {
			last = s.status(runID)
			for _, w := range want {
				if strings.Contains(last, w) {
					return nil
				}
			}
			return fmt.Errorf("status %s", last)
		})
	if err != nil {
		s.t.Fatalf("%v; last status %s", err, last)
	}
	return last
}

// alive fails the test if either process has exited: a rotation must not
// need a restart.
func (s *tlsStack) alive() {
	s.t.Helper()
	for name, done := range map[string]<-chan struct{}{"ork": s.engine, "orker": s.orker} {
		select {
		case <-done:
			s.t.Fatalf("%s exited", name)
		default:
		}
	}
}

// mtlsFiles writes a client pair and the server's CA for one process and
// returns its settings.
func mtlsFiles(t *testing.T, dir, who string, serverCA *testpki.CA, leaf testpki.Leaf) (env []string, cert, key string) {
	t.Helper()
	ca := testpki.Write(t, dir, who+"-ca.pem", serverCA.PEM)
	cert = testpki.Write(t, dir, who+".pem", leaf.CertPEM)
	key = testpki.Write(t, dir, who+".key", leaf.KeyPEM)
	return []string{
		"ORK_TEMPORAL_TLS_CA_FILE=" + ca,
		"ORK_TEMPORAL_TLS_CERT_FILE=" + cert,
		"ORK_TEMPORAL_TLS_KEY_FILE=" + key,
	}, cert, key
}

// replace writes data to path through a rename, as a secret store does.
func replace(t *testing.T, path string, data []byte) {
	t.Helper()
	tmp := path + ".new"
	must.OK(t, os.WriteFile(tmp, data, 0o600))
	must.OK(t, os.Rename(tmp, path))
}

func serials(hs []tlsfront.Handshake) map[string]int {
	seen := map[string]int{}
	for _, h := range hs {
		if h.Err == nil && h.Client != nil {
			seen[h.Client.String()]++
		}
	}
	return seen
}

// TestIntegration_ARunCompletesOverMutualTLS runs an engine and an Orker
// whose only way to Temporal is a front that demands a client certificate:
// the run completes, and every handshake the front saw presented one of the
// two processes' certificates.
func TestIntegration_ARunCompletesOverMutualTLS(t *testing.T) {
	dir := t.TempDir()
	serverCA := testpki.NewCA(t, "temporal-server")
	clientCA := testpki.NewCA(t, "temporal-clients")
	engineLeaf := clientCA.Issue(t, testpki.LeafOptions{Name: "ork", Client: true})
	orkerLeaf := clientCA.Issue(t, testpki.LeafOptions{Name: "orker", Client: true})
	front := tlsfront.Start(t, tlsfront.Options{
		Upstream:    temporalAddress(),
		Certificate: serverCA.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"127.0.0.1"}}).TLS(t),
		ClientCAs:   clientCA.Pool(),
	})
	engineTLS, _, _ := mtlsFiles(t, dir, "ork", serverCA, engineLeaf)
	orkerTLS, _, _ := mtlsFiles(t, dir, "orker", serverCA, orkerLeaf)
	s := startTLSStack(t, front.Addr, engineTLS, orkerTLS)

	runID := s.submit("over-mtls", "echo over mutual TLS")
	if got := s.until(runID, "SUCCEEDED", "FAILED", "CANCELED"); !strings.Contains(got, "SUCCEEDED") {
		t.Fatalf("run %s ended %s", runID, got)
	}
	for _, h := range front.Handshakes() {
		if h.Err != nil {
			t.Errorf("a handshake failed: %v", h.Err)
			continue
		}
		if h.Client == nil || (h.Client.Cmp(engineLeaf.Cert.SerialNumber) != 0 && h.Client.Cmp(orkerLeaf.Cert.SerialNumber) != 0) {
			t.Errorf("a handshake presented %v, neither process's certificate", h.Client)
		}
	}
	seen := serials(front.Handshakes())
	if seen[engineLeaf.Cert.SerialNumber.String()] == 0 || seen[orkerLeaf.Cert.SerialNumber.String()] == 0 {
		t.Fatalf("handshakes by certificate = %v, want both the engine's and the Orker's", seen)
	}
	t.Logf("handshakes by certificate: %v", seen)
}

// TestIntegration_ClientCertificateReplacedMidRun replaces both processes'
// client pairs while a task runs and drops their connections: each
// reconnects with its new certificate, the run finishes, and neither process
// restarted.
func TestIntegration_ClientCertificateReplacedMidRun(t *testing.T) {
	dir := t.TempDir()
	serverCA := testpki.NewCA(t, "temporal-server")
	clientCA := testpki.NewCA(t, "temporal-clients")
	front := tlsfront.Start(t, tlsfront.Options{
		Upstream:    temporalAddress(),
		Certificate: serverCA.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"127.0.0.1"}}).TLS(t),
		ClientCAs:   clientCA.Pool(),
	})
	oldEngine := clientCA.Issue(t, testpki.LeafOptions{Name: "ork-1", Client: true})
	oldOrker := clientCA.Issue(t, testpki.LeafOptions{Name: "orker-1", Client: true})
	engineTLS, engineCert, engineKey := mtlsFiles(t, dir, "ork", serverCA, oldEngine)
	orkerTLS, orkerCert, orkerKey := mtlsFiles(t, dir, "orker", serverCA, oldOrker)
	s := startTLSStack(t, front.Addr, engineTLS, orkerTLS)

	runID := s.submit("cert-rotation", "sleep 20; echo done")
	s.until(runID, "RUNNING")

	newEngine := clientCA.Issue(t, testpki.LeafOptions{Name: "ork-2", Client: true})
	newOrker := clientCA.Issue(t, testpki.LeafOptions{Name: "orker-2", Client: true})
	replace(t, engineCert, newEngine.CertPEM)
	replace(t, engineKey, newEngine.KeyPEM)
	replace(t, orkerCert, newOrker.CertPEM)
	replace(t, orkerKey, newOrker.KeyPEM)
	want := []*big.Int{newEngine.Cert.SerialNumber, newOrker.Cert.SerialNumber}
	_, err := bootwait.Wait{What: "both processes to reconnect with their new certificates", Logf: t.Logf}.Until(s.ctx,
		func(context.Context) error {
			front.Drop()
			seen := serials(front.Handshakes())
			for _, serial := range want {
				if seen[serial.String()] == 0 {
					return fmt.Errorf("no handshake yet with %v (seen %v)", serial, seen)
				}
			}
			return nil
		})
	must.OK(t, err)
	if got := s.until(runID, "SUCCEEDED", "FAILED", "CANCELED"); !strings.Contains(got, "SUCCEEDED") {
		t.Fatalf("run %s ended %s after the rotation", runID, got)
	}
	s.alive()
	t.Logf("handshakes by certificate: %v", serials(front.Handshakes()))
}

// TestIntegration_APIKeyReplacedMidRun runs against a front that holds every
// call to an API key. While a task runs the key files are replaced and the
// front accepts both keys; once the new key is in use the old one is
// refused, and the run still finishes: no call kept the old key.
func TestIntegration_APIKeyReplacedMidRun(t *testing.T) {
	dir := t.TempDir()
	serverCA := testpki.NewCA(t, "temporal-server")
	front := tlsfront.StartKeyFront(t, temporalAddress(),
		serverCA.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"127.0.0.1"}}).TLS(t), "key-one")
	ca := testpki.Write(t, dir, "ca.pem", serverCA.PEM)
	engineKey := testpki.Write(t, dir, "ork.key", []byte("key-one\n"))
	orkerKey := testpki.Write(t, dir, "orker.key", []byte("key-one\n"))
	s := startTLSStack(t, front.Addr,
		[]string{"ORK_TEMPORAL_TLS_CA_FILE=" + ca, "ORK_TEMPORAL_API_KEY_FILE=" + engineKey},
		[]string{"ORK_TEMPORAL_TLS_CA_FILE=" + ca, "ORK_TEMPORAL_API_KEY_FILE=" + orkerKey})

	runID := s.submit("key-rotation", "sleep 20; echo done")
	s.until(runID, "RUNNING")
	front.SetKeys("key-one", "key-two")
	replace(t, engineKey, []byte("key-two\n"))
	replace(t, orkerKey, []byte("key-two\n"))
	mark := len(front.Seen())
	_, err := bootwait.Wait{What: "calls with the new key", Logf: t.Logf}.Until(s.ctx, func(context.Context) error {
		for _, k := range front.Seen()[mark:] {
			if k == "key-two" {
				return nil
			}
		}
		return errors.New("not yet")
	})
	must.OK(t, err)
	front.SetKeys("key-two")
	if got := s.until(runID, "SUCCEEDED", "FAILED", "CANCELED"); !strings.Contains(got, "SUCCEEDED") {
		t.Fatalf("run %s ended %s after the rotation", runID, got)
	}
	s.alive()
	for _, k := range front.Seen() {
		if k == "" {
			t.Fatal("a call reached the front with no API key")
		}
	}
}

// TestIntegration_BootRefusesABadTemporalTLSSetting: a wrong CA, half a
// client pair and an unreadable file each stop the boot of both binaries,
// and the output names the setting.
func TestIntegration_BootRefusesABadTemporalTLSSetting(t *testing.T) {
	address := temporalAddress()
	if conn, err := net.DialTimeout("tcp", address, 2*time.Second); err != nil {
		infraAbsent(t, "temporal is not reachable at %s: %v", address, err)
	} else {
		_ = conn.Close()
	}
	dir := t.TempDir()
	serverCA := testpki.NewCA(t, "temporal-server")
	wrongCA := testpki.NewCA(t, "wrong")
	front := tlsfront.Start(t, tlsfront.Options{
		Upstream:    address,
		Certificate: serverCA.Issue(t, testpki.LeafOptions{Name: "temporal", Hosts: []string{"127.0.0.1"}}).TLS(t),
	})
	leaf := serverCA.Issue(t, testpki.LeafOptions{Name: "client", Client: true})
	wrong := testpki.Write(t, dir, "wrong-ca.pem", wrongCA.PEM)
	cert := testpki.Write(t, dir, "client.pem", leaf.CertPEM)
	missing := filepath.Join(dir, "missing.pem")

	cases := []struct {
		name    string
		env     []string
		setting string
	}{
		{"a wrong CA", []string{"ORK_TEMPORAL_TLS_CA_FILE=" + wrong}, "temporal.tls.ca_file"},
		{"half a client pair", []string{"ORK_TEMPORAL_TLS_CERT_FILE=" + cert}, "temporal.tls.key_file"},
		{"an unreadable CA file", []string{"ORK_TEMPORAL_TLS_CA_FILE=" + missing}, "temporal.tls.ca_file"},
	}
	for _, binary := range []string{"./cmd/ork", "./cmd/orker"} {
		for _, tc := range cases {
			t.Run(filepath.Base(binary)+"/"+tc.name, func(t *testing.T) {
				home := t.TempDir()
				ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, testbin.Path(t, binary))
				cmd.Dir = home
				cmd.Env = append(os.Environ(),
					"HOME="+home,
					"ORK_TEMPORAL_ADDRESS="+front.Addr,
					"ORK_TEMPORAL_NAMESPACE="+privateTemporalNamespace(),
					"ORK_HTTP_ADDR="+freeAddress(t),
					"ORK_ORKER_WORKDIR="+filepath.Join(home, "work"),
					"ORK_ARTIFACT_BACKEND=local",
					"ORK_ARTIFACT_DIR="+filepath.Join(home, "artifacts"),
				)
				cmd.Env = append(cmd.Env, tc.env...)
				var output bytes.Buffer
				cmd.Stdout, cmd.Stderr = &output, &output
				err := cmd.Run()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || ctx.Err() != nil {
					t.Fatalf("the boot did not refuse: err %v, ctx %v\n%s", err, ctx.Err(), output.String())
				}
				if !strings.Contains(output.String(), tc.setting) {
					t.Fatalf("output does not name %s:\n%s", tc.setting, output.String())
				}
				if strings.Contains(output.String(), "BEGIN CERTIFICATE") {
					t.Fatalf("output prints a file's content:\n%s", output.String())
				}
			})
		}
	}
}
