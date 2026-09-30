//go:build integration

package orker_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/orker"
	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"
	"github.com/orkstack/orkstack/internal/testsupport/localcatalog"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
)

// shellRepo is the first-party shell image's repository in every registry
// these tests copy it into.
const shellRepo = "orkstack/activity-shell"

// privateShell starts a registry private to this test (deletion enabled, so a
// signature can be removed without touching the shared registry), copies the
// local activity-shell:dev into it preserving its digest, and returns the
// registry and the ImageRef the engine would pin for it.
func privateShell(t *testing.T) (*cosigntest.PrivateRegistry, *orkstackv1.ImageRef) {
	t.Helper()
	registry := cosigntest.StartRegistry(t, func(format string, args ...any) { infraAbsent(t, format, args...) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	digest, err := (&cosigntest.Client{}).CopyImage(ctx, localcatalog.Registry(), registry.Host, shellRepo, "dev")
	if err != nil {
		infraAbsent(t, "copy %s/%s:dev into the private registry: %v (run `make -C images push`)",
			localcatalog.Registry(), shellRepo, err)
	}
	return registry, &orkstackv1.ImageRef{
		Reference:   registry.Host + "/" + shellRepo + ":dev",
		Registry:    registry.Host,
		Repository:  shellRepo,
		IndexDigest: digest,
		Pinned:      registry.Host + "/" + shellRepo + "@" + digest,
	}
}

// shellInput runs activity-shell's `exec` op with a command that succeeds.
func shellInput(t *testing.T, taskID string, image *orkstackv1.ImageRef) *orkstackv1.ExecuteTaskInput {
	t.Helper()
	in := baseInput(t, taskID)
	in.Image = image
	in.Command = nil
	with, err := structpb.NewStruct(map[string]any{"op": "exec", "command": []any{"true"}})
	if err != nil {
		t.Fatal(err)
	}
	in.With = with
	return in
}

// sigCounter counts requests whose path ends in ".sig", through the
// verifier's own HTTP client.
type sigCounter struct {
	mu    sync.Mutex
	count int
	next  http.RoundTripper
}

func (c *sigCounter) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, ".sig") {
		c.mu.Lock()
		c.count++
		c.mu.Unlock()
	}
	return c.next.RoundTrip(req)
}

func (c *sigCounter) n() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

// imagePolicyContractRows are
// TestIntegration_ContractViolationsAreRefusedWithTheirReasonCode's two
// image-policy rows, on real Docker: an image outside the selection's
// registries is IMAGE_NOT_PERMITTED, and a signer whose image has no .sig in
// a real registry is IMAGE_NOT_SIGNED — neither pulls nor creates anything.
func imagePolicyContractRows(t *testing.T) {
	t.Run("an image outside the selection's registries", func(t *testing.T) {
		dispatcher, _ := newDispatcher(t)
		counting := &countingRuntime{ContainerRuntime: dispatcher.Runtime}
		dispatcher.Runtime = counting
		in := baseInput(t, "violate-registry", "/bin/sh", "-c", `printf '{}' > /orkstack/output/values.json`)
		in.Image.Registry, in.Image.Repository = "docker.io", "library/alpine"
		in.ImagePolicy = &orkstackv1.ImagePolicySelection{AllowedRegistries: []string{"ghcr.io/acme/"}}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, err := dispatcher.Execute(ctx, in)
		if err == nil || !strings.Contains(err.Error(), orker.ReasonImageNotPermitted) {
			t.Fatalf("error = %v, want %s", err, orker.ReasonImageNotPermitted)
		}
		if pulls, creates := counting.counts(); pulls != 0 || creates != 0 {
			t.Fatalf("runtime asked %d pulls and %d creates; want none", pulls, creates)
		}
		t.Logf("refused: %v", err)
	})

	t.Run("a signer with no .sig in the registry", func(t *testing.T) {
		_, image := privateShell(t)
		key, err := cosigntest.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		dispatcher, _ := newDispatcher(t)
		counting := &countingRuntime{ContainerRuntime: dispatcher.Runtime}
		dispatcher.Runtime = counting
		dispatcher.Signatures = &dsl.SignatureVerifier{Resolver: dsl.NewResolver(""), AuthFileVar: orker.RegistryAuthFileVar}
		in := shellInput(t, "violate-unsigned", image)
		in.ImagePolicy = &orkstackv1.ImagePolicySelection{RequireSignedBy: "cosign:release", SigningKeyPem: key.PublicPEM}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		_, err = dispatcher.Execute(ctx, in)
		if err == nil || !strings.Contains(err.Error(), orker.ReasonImageNotSigned) ||
			!strings.Contains(err.Error(), orker.CategoryContractViolation) {
			t.Fatalf("error = %v, want %s (%s)", err, orker.ReasonImageNotSigned, orker.CategoryContractViolation)
		}
		if pulls, creates := counting.counts(); pulls != 0 || creates != 0 {
			t.Fatalf("runtime asked %d pulls and %d creates; want none", pulls, creates)
		}
		t.Logf("refused: %v", err)
	})
}

// TestIntegration_ACommittedAttemptIsAdoptedAfterItsSignatureIsRemoved is the
// AGENTS.md §11 "after commit.json but before Temporal completion" scenario
// with a signer in force: the retry Temporal makes after the Orker died
// before reporting completion adopts the committed result even though the
// signature has since been removed from the registry, and never asks the
// registry about it. A fresh activity of the same image is then refused
// IMAGE_NOT_SIGNED, which proves the signature really is gone.
func TestIntegration_ACommittedAttemptIsAdoptedAfterItsSignatureIsRemoved(t *testing.T) {
	requireDocker(t)
	registry, image := privateShell(t)
	key, err := cosigntest.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	client := &cosigntest.Client{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := client.Sign(ctx, key, registry.Host, shellRepo, image.GetIndexDigest()); err != nil {
		t.Fatalf("sign: %v", err)
	}

	dispatcher, _ := newDispatcher(t)
	counter := &sigCounter{next: http.DefaultTransport}
	resolver := dsl.NewResolver("")
	resolver.Client = &http.Client{Transport: counter, Timeout: 30 * time.Second}
	dispatcher.Signatures = &dsl.SignatureVerifier{Resolver: resolver, AuthFileVar: orker.RegistryAuthFileVar}

	selection := &orkstackv1.ImagePolicySelection{RequireSignedBy: "cosign:release", SigningKeyPem: key.PublicPEM}
	in := shellInput(t, "adopt-signed", image)
	in.CommitUri = "mem://commits/" + in.RunId + "/adopt-signed/commit.json"
	in.ImagePolicy = selection

	var lines []string
	logf := func(format string, args ...any) {
		t.Logf(format, args...)
		lines = append(lines, strings.TrimSpace(fmt.Sprintf(format, args...)))
	}

	first, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("first Execute: %v", err)
	}
	if first.GetCommitAdopted() {
		t.Fatal("the first attempt adopted a commit that did not exist")
	}
	if counter.n() != 1 {
		t.Fatalf("the first attempt made %d .sig GETs, want 1", counter.n())
	}
	logf("attempt 1: signed image %s ran, committed=%v adopted=%v, .sig GETs=%d",
		image.GetPinned(), first.GetValuesRef() != nil, first.GetCommitAdopted(), counter.n())

	if err := client.Unsign(ctx, registry.Host, shellRepo, image.GetIndexDigest()); err != nil {
		t.Fatalf("unsign: %v", err)
	}
	logf("signature removed from %s: %s/%s:%s now answers 404", registry.Host, registry.Host, shellRepo,
		cosigntest.SignatureTag(image.GetIndexDigest()))

	second, err := dispatcher.Execute(ctx, in)
	if err != nil {
		t.Fatalf("the retry of a committed attempt was judged again: %v", err)
	}
	if !second.GetCommitAdopted() {
		t.Fatal("the retry did not adopt the committed result")
	}
	if string(second.GetValuesJson()) != string(first.GetValuesJson()) ||
		second.GetValuesRef().GetUri() != first.GetValuesRef().GetUri() || second.GetAttempt() != first.GetAttempt() {
		t.Fatalf("adopted result differs: values %q/%q ref %q/%q attempt %d/%d",
			second.GetValuesJson(), first.GetValuesJson(), second.GetValuesRef().GetUri(),
			first.GetValuesRef().GetUri(), second.GetAttempt(), first.GetAttempt())
	}
	if counter.n() != 1 {
		t.Fatalf("the adopted retry asked the registry about the signature: .sig GETs=%d, want 1", counter.n())
	}
	logf("attempt 2 (same activity, retry): adopted=%v, values_json equal, values_ref %s equal, attempt %d equal, .sig GETs still %d",
		second.GetCommitAdopted(), second.GetValuesRef().GetUri(), second.GetAttempt(), counter.n())

	fresh := shellInput(t, "fresh-unsigned", image)
	fresh.CommitUri = "mem://commits/" + fresh.RunId + "/fresh-unsigned/commit.json"
	fresh.ImagePolicy = selection
	_, err = dispatcher.Execute(ctx, fresh)
	if err == nil || !strings.Contains(err.Error(), orker.ReasonImageNotSigned) {
		t.Fatalf("a fresh activity of the unsigned image = %v, want %s", err, orker.ReasonImageNotSigned)
	}
	logf("fresh activity of the same image: %v", err)
	proof.Write(t, "orker", "image-policy-adopt-after-unsign.txt", []byte(strings.Join(lines, "\n")+"\n"))
}
