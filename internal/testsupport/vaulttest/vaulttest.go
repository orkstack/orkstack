// Package vaulttest runs HashiCorp Vault's own development server for the
// tests of the "vault" provider type: the image deploy/vault-image.txt pins,
// serving https on a loopback port Docker picks, with a certificate Vault
// makes. The proof hosts' setup pulls the image; a host that lacks it pulls it
// once, here.
//
// A test starts its own server, so nothing it configures is shared. What a
// real Vault does on demand (throttling, an expired token, a revoked lease)
// is asked of it; what it does not (an answer cut short, a redirect) is the
// provider's own fault test's.
package vaulttest

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/dockerport"
)

// ImageFile is the file that pins the image.
const ImageFile = "deploy/vault-image.txt"

var pinned = regexp.MustCompile(`^hashicorp/vault:[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$`)

// ErrNoImage is an image the daemon does not hold and could not pull, or no
// docker at all: a missing prerequisite, which a suite skips for only where
// it may.
var ErrNoImage = errors.New("the Vault image is not on this host")

// Image is the image ImageFile under root pins.
func Image(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(ImageFile)))
	if err != nil {
		return "", fmt.Errorf("vaulttest: read %s: %w", ImageFile, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !pinned.MatchString(line) {
			return "", fmt.Errorf("vaulttest: %s names %q, not hashicorp/vault:X.Y.Z@sha256:<64 hex>", ImageFile, line)
		}
		return line, nil
	}
	return "", fmt.Errorf("vaulttest: %s names no image", ImageFile)
}

// Options is one server to start.
type Options struct {
	// Root is the repository root.
	Root string
	// Dir is where its CA is written.
	Dir string
	// ProofID and Owner label the container as a proof's own, so `proofrun
	// clean` stops it once that owner is gone.
	ProofID, Owner string
	// LeasedKV mounts every kv engine as Vault's leased one
	// (-dev-leased-kv): a read of a secret with a ttl carries a renewable
	// lease of Vault's own lease manager, with no server beside it. There is
	// no KV version 2 on such a server.
	LeasedKV bool
}

// Vault is one server that runs.
type Vault struct {
	// ID is the container's id.
	ID string
	// Addr is host:port, Address the https URL, and CAFile the PEM CA its
	// certificate chains to.
	Addr, Address, CAFile string

	rootToken string
	client    *http.Client
}

const certDir = "/tmp/orkstack-vault-tls"

// Start starts a server and waits, at most a minute, until it is unsealed.
func Start(ctx context.Context, o Options) (*Vault, error) {
	image, err := Image(o.Root)
	if err != nil {
		return nil, err
	}
	if _, err := exec.LookPath("docker"); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNoImage, err)
	}
	if exec.CommandContext(ctx, "docker", "image", "inspect", image).Run() != nil {
		if out, err := exec.CommandContext(ctx, "docker", "pull", "--quiet", image).CombinedOutput(); err != nil {
			return nil, fmt.Errorf("%w: pull %s: %v: %s", ErrNoImage, image, err, strings.TrimSpace(string(out)))
		}
	}
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return nil, fmt.Errorf("vaulttest: %w", err)
	}
	v := &Vault{rootToken: "orkstack-dev-" + hex.EncodeToString(random)}
	flags := ""
	if o.LeasedKV {
		flags = " -dev-leased-kv"
	}
	args := []string{"run", "-d", "--rm", "--pull", "never",
		"-p", dockerport.Publish("8200"),
		"-e", "SKIP_SETCAP=true", "-e", "VAULT_DEV_ROOT_TOKEN_ID"}
	if o.Owner != "" {
		args = append(args, "--label", "orkstack.proof="+o.ProofID, "--label", "orkstack.proof.owner="+o.Owner)
	}
	cmd := exec.CommandContext(ctx, "docker", append(args, "--entrypoint", "/bin/sh", image, "-c",
		"mkdir -p "+certDir+" && exec vault server -dev -dev-no-store-token -dev-tls -dev-tls-cert-dir="+certDir+" -dev-listen-address=0.0.0.0:8200"+flags)...)
	// The root token reaches Vault through docker's environment, never argv.
	cmd.Env = append(os.Environ(), "VAULT_DEV_ROOT_TOKEN_ID="+v.rootToken)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("vaulttest: docker run %s: %v: %s", image, err, strings.TrimSpace(string(out)))
	}
	v.ID = strings.TrimSpace(string(out))
	if err := v.ready(ctx, o.Dir); err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		logs, _ := exec.CommandContext(cleanup, "docker", "logs", "--tail", "40", v.ID).CombinedOutput()
		return nil, errors.Join(fmt.Errorf("%w; its log:\n%s", err, bytes.ReplaceAll(logs, []byte(v.rootToken), []byte("***"))), v.Stop(cleanup))
	}
	return v, nil
}

func (v *Vault) ready(ctx context.Context, dir string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	addr, err := dockerport.Addr(ctx, v.ID, "8200")
	if err != nil {
		return err
	}
	v.Addr, v.Address, v.CAFile = addr, "https://"+addr, filepath.Join(dir, "vault-ca.pem")
	var last error
	for {
		if last = v.probe(ctx); last == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("vaulttest: Vault is not ready: %w", last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (v *Vault) probe(ctx context.Context) error {
	if v.client == nil {
		ca, err := exec.CommandContext(ctx, "docker", "exec", v.ID, "cat", certDir+"/vault-ca.pem").Output()
		if err != nil {
			return fmt.Errorf("read its CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return errors.New("its CA is not written yet")
		}
		if err := os.WriteFile(v.CAFile, ca, 0o600); err != nil {
			return err
		}
		v.client = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}
	}
	_, err := v.Do(ctx, http.MethodGet, "sys/health", nil)
	return err
}

// Stop stops the server; --rm removes it.
func (v *Vault) Stop(ctx context.Context) error {
	if out, err := exec.CommandContext(ctx, "docker", "stop", "-t", "2", v.ID).CombinedOutput(); err != nil {
		return fmt.Errorf("vaulttest: docker stop %s: %v: %s", v.ID, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// StatusError is an answer that is not a success.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("vault answered %d: %s", e.Code, e.Body) }

// Do makes one request as root and returns its answer.
func (v *Vault) Do(ctx context.Context, method, path string, in any) (jsontext.Value, error) {
	return v.As(ctx, v.rootToken, method, path, in)
}

// As makes one request with token.
func (v *Vault) As(ctx context.Context, token, method, path string, in any) (jsontext.Value, error) {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return nil, fmt.Errorf("vaulttest: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, v.Address+"/v1/"+path, body)
	if err != nil {
		return nil, fmt.Errorf("vaulttest: %w", err)
	}
	req.Header.Set("X-Vault-Token", token)
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("vaulttest: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("vaulttest: %s %s: %w", method, path, err)
	}
	if resp.StatusCode > 299 {
		return nil, &StatusError{Code: resp.StatusCode, Body: string(raw)}
	}
	return raw, nil
}
