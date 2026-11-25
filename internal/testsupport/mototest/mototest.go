// Package mototest runs Moto, the AWS stand-in the aws provider's integration
// tests call (internal/provider/aws): the image deploy/moto-image.txt pins,
// published on a loopback port Docker chooses. An image the daemon lacks is
// pulled by its digest; the proof hosts' setup pulls it first.
//
// What Moto judges, and so what a test on it may claim: that a parameter, a
// secret or a key exists, a ciphertext's key and encryption context, and, once
// told to, a call's signature and the role's policy (its refusal of a JSON
// service's call is an XML error, which an AWS SDK cannot read). It judges no web identity
// token (signature, issuer, audience, subject or expiry), no trust policy, no
// session length and no session tag: a test of those on Moto claims nothing.
package mototest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/dockerport"
)

// ImageFile is the file of the repository that pins the image.
const ImageFile = "deploy/moto-image.txt"

const port = "5000"

// pinned is the only shape ImageFile may name.
var pinned = regexp.MustCompile(`^motoserver/moto:[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$`)

// Image is the image ImageFile under root pins: its one line that is neither
// blank nor a comment.
func Image(root string) (string, error) {
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(ImageFile)))
	if err != nil {
		return "", fmt.Errorf("mototest: read %s: %w", ImageFile, err)
	}
	return imageFrom(string(data))
}

func imageFrom(data string) (string, error) {
	var found string
	for line := range strings.Lines(data) {
		if line = strings.TrimSpace(line); line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if found != "" || !pinned.MatchString(line) {
			return "", fmt.Errorf("mototest: %s must name one image, motoserver/moto:X.Y.Z@sha256:<64 hex>; it holds %q", ImageFile, line)
		}
		found = line
	}
	if found == "" {
		return "", fmt.Errorf("mototest: %s names no image", ImageFile)
	}
	return found, nil
}

// ErrAbsent is a Docker daemon that cannot be asked, or an image it does not
// hold and could not pull: a missing prerequisite, for which the caller skips
// where its suite allows.
var ErrAbsent = errors.New("Moto cannot be run on this host")

// Moto is one Moto that runs.
type Moto struct {
	// ID is the container's id.
	ID string
	// Addr is its loopback address.
	Addr string
}

// Start starts a Moto and waits, at most a minute, for it to answer.
// proofID and owner, when owner is set, label the container as a proof's own,
// so `proofrun clean` stops it once that owner is gone.
func Start(ctx context.Context, root, proofID, owner string) (*Moto, error) {
	image, err := Image(root)
	if err != nil {
		return nil, err
	}
	if out, err := exec.CommandContext(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", image).CombinedOutput(); err != nil {
		if !strings.Contains(strings.ToLower(string(out)), "no such image") {
			return nil, fmt.Errorf("%w: inspect %s: %v: %s", ErrAbsent, image, err, strings.TrimSpace(string(out)))
		}
		if out, err := exec.CommandContext(ctx, "docker", "pull", "-q", image).CombinedOutput(); err != nil {
			return nil, fmt.Errorf("%w: pull %s: %v: %s", ErrAbsent, image, err, strings.TrimSpace(string(out)))
		}
	}
	cmd := exec.CommandContext(ctx, "docker", RunArgs(image, proofID, owner)...)
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			out = exit.Stderr
		}
		return nil, fmt.Errorf("mototest: docker run %s: %v: %s", image, err, strings.TrimSpace(string(out)))
	}
	m := &Moto{ID: strings.TrimSpace(string(out))}
	m.Addr, err = dockerport.Addr(ctx, m.ID, port)
	if err == nil {
		err = m.wait(ctx)
	}
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		logs, _ := exec.CommandContext(cleanup, "docker", "logs", "--tail", "40", m.ID).CombinedOutput()
		return nil, errors.Join(fmt.Errorf("%w; its log:\n%s", err, logs), m.Stop(cleanup))
	}
	return m, nil
}

// RunArgs is the `docker` command line that starts Moto: the pinned image,
// pulled before, removed when stopped, published on a loopback port Docker
// chooses.
func RunArgs(image, proofID, owner string) []string {
	args := []string{"run", "-d", "--rm", "--pull", "never", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"-p", dockerport.Publish(port), "--label", "orkstack.moto=1"}
	if owner != "" {
		args = append(args, "--label", "orkstack.proof="+proofID, "--label", "orkstack.proof.owner="+owner)
	}
	return append(args, image)
}

// Stop stops the Moto; --rm removes it.
func (m *Moto) Stop(ctx context.Context) error {
	if out, err := exec.CommandContext(ctx, "docker", "stop", "-t", "2", m.ID).CombinedOutput(); err != nil {
		return fmt.Errorf("mototest: docker stop %s: %v: %s", m.ID, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// DeniedStatus and DeniedBody are Moto's answer to a JSON service's call that
// the caller's policies do not allow, byte for byte (moto/core/exceptions.py,
// AccessDeniedError): an XML document, where AWS answers JSON. A test that
// meets it holds it to these, so a Moto that words it otherwise is seen.
const DeniedStatus = http.StatusForbidden

// DeniedBody is the body for caller's call of action.
func DeniedBody(caller, action string) string {
	return `<?xml version="1.0" encoding="utf-8"?>` + "\n<ErrorResponse><Errors><Error><Code>AccessDenied</Code><Message>User: " +
		caller + " is not authorized to perform: " + action + "</Message><Type>Sender</Type></Error></Errors></ErrorResponse>"
}

// URL is its endpoint for every service.
func (m *Moto) URL() string { return "http://" + m.Addr }

func (m *Moto) wait(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var last error
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.URL()+"/moto-api/data.json", nil)
		if err != nil {
			return fmt.Errorf("mototest: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			err = fmt.Errorf("answered %s", resp.Status)
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("mototest: Moto at %s did not answer within a minute: %v", m.Addr, last)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
