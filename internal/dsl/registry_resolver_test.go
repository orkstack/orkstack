package dsl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// indexBody is a two-platform OCI index, as a registry would serve one.
func indexBody(t *testing.T) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeOCIIndex,
		"manifests": []any{
			map[string]any{"platform": map[string]any{"os": "linux", "architecture": "amd64"}},
			map[string]any{"platform": map[string]any{"os": "linux", "architecture": "arm64", "variant": "v8"}},
			// An attestation manifest, as buildx emits. It has no os/arch
			// worth naming and must not become a bogus "/" platform.
			map[string]any{"platform": map[string]any{"os": "unknown", "architecture": "unknown"}},
		},
	})
	if err != nil {
		t.Fatalf("index: %v", err)
	}
	return body
}

func digestOf(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// registryStub serves /v2/<repo>/manifests/<ref> the way a registry does.
type registryStub struct {
	t          *testing.T
	body       []byte
	mediaType  string
	sendDigest string // Docker-Content-Digest to send; "" sends none
	status     int
	gotAccept  string
	gotMethod  string
	requests   int
}

func (s *registryStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.requests++
	s.gotAccept = r.Header.Get("Accept")
	s.gotMethod = r.Method
	if s.status != 0 && s.status != http.StatusOK {
		w.WriteHeader(s.status)
		fmt.Fprintln(w, "refused")
		return
	}
	if s.sendDigest != "" {
		w.Header().Set("Docker-Content-Digest", s.sendDigest)
	}
	w.Header().Set("Content-Type", s.mediaType)
	w.WriteHeader(http.StatusOK)
	w.Write(s.body)
}

// newStubResolver returns a resolver and the reference naming the stub. The
// stub listens on 127.0.0.1, which the resolver treats as plain HTTP.
func newStubResolver(t *testing.T, stub *registryStub) (*Resolver, string) {
	t.Helper()
	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "http://")
	return NewResolver(""), host + "/acme/migrate:v3"
}

func TestResolve_PinsTheIndexDigestAndPlatforms(t *testing.T) {
	body := indexBody(t)
	stub := &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex, sendDigest: digestOf(body)}
	resolver, reference := newStubResolver(t, stub)

	image, err := resolver.Resolve(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}

	if stub.gotMethod != http.MethodGet {
		t.Fatalf("method = %s, want GET — platforms live in the body (§4.3 step 2)", stub.gotMethod)
	}
	if stub.gotAccept != ManifestAccept {
		t.Fatalf("Accept = %q, want the exact pull-time set %q", stub.gotAccept, ManifestAccept)
	}
	if image.GetIndexDigest() != digestOf(body) {
		t.Fatalf("index digest = %q, want the digest of the exact response bytes", image.GetIndexDigest())
	}
	if image.GetReference() != "migrate" {
		t.Fatalf("reference = %q, want the author's spelling verbatim", image.GetReference())
	}
	if image.GetRepository() != "acme/migrate" {
		t.Fatalf("repository = %q", image.GetRepository())
	}
	want := image.GetRegistry() + "/acme/migrate@" + digestOf(body)
	if image.GetPinned() != want {
		t.Fatalf("pinned = %q, want %q", image.GetPinned(), want)
	}
	// Index order, not sorted; the "unknown/unknown" attestation entry is
	// carried because it IS in the index — dropping it would make platforms
	// disagree with what the registry returned.
	got := image.GetPlatforms()
	wantPlatforms := []string{"linux/amd64", "linux/arm64/v8", "unknown/unknown"}
	if len(got) != len(wantPlatforms) {
		t.Fatalf("platforms = %v, want %v", got, wantPlatforms)
	}
	for i := range got {
		if got[i] != wantPlatforms[i] {
			t.Fatalf("platforms = %v, want %v (index order is hashed, §4.3 step 4)", got, wantPlatforms)
		}
	}
}

func TestResolve_SingleManifestLeavesPlatformsEmpty(t *testing.T) {
	body := []byte(`{"schemaVersion":2,"config":{},"layers":[]}`)
	stub := &registryStub{t: t, body: body, mediaType: MediaTypeOCIManifest, sendDigest: digestOf(body)}
	resolver, reference := newStubResolver(t, stub)

	image, err := resolver.Resolve(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(image.GetPlatforms()) != 0 {
		t.Fatalf("platforms = %v, want empty for a single manifest (§2.2)", image.GetPlatforms())
	}
	if image.GetMediaType() != MediaTypeOCIManifest {
		t.Fatalf("media type = %q", image.GetMediaType())
	}
}

// §4.3 step 3: a Docker-Content-Digest that disagrees with the body is a
// mismatch. This is the only thing standing between a pinned digest and a
// proxy that rewrote the body in transit.
func TestResolve_RejectsAHeaderDigestThatDisagreesWithTheBody(t *testing.T) {
	body := indexBody(t)
	stub := &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex,
		sendDigest: "sha256:" + strings.Repeat("00", 32)}
	resolver, reference := newStubResolver(t, stub)

	_, err := resolver.Resolve(context.Background(), "migrate", reference)
	if got := ResolveCode(err); got != ReasonImageDigestMismatch {
		t.Fatalf("code = %q, want %s (err=%v)", got, ReasonImageDigestMismatch, err)
	}
}

// §4.3 step 5: an already-pinned reference still gets the GET, and the served
// body must hash to the pin. Apply must not succeed with an unpullable pin.
func TestResolve_PinnedReferenceMustMatchWhatTheRegistryServes(t *testing.T) {
	body := indexBody(t)

	t.Run("a pin the registry honours resolves", func(t *testing.T) {
		stub := &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex, sendDigest: digestOf(body)}
		server := httptest.NewServer(stub)
		defer server.Close()
		host := strings.TrimPrefix(server.URL, "http://")
		resolver := NewResolver("")
		image, err := resolver.Resolve(context.Background(), "x",
			host+"/acme/migrate@"+digestOf(body))
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if image.GetIndexDigest() != digestOf(body) {
			t.Fatalf("digest = %q", image.GetIndexDigest())
		}
	})

	t.Run("a pin the registry contradicts is a mismatch", func(t *testing.T) {
		// The registry serves a different object than the pin names and does
		// not send a Docker-Content-Digest to give it away.
		other := []byte(`{"schemaVersion":2,"mediaType":"` + MediaTypeOCIIndex + `","manifests":[]}`)
		stub := &registryStub{t: t, body: other, mediaType: MediaTypeOCIIndex}
		server := httptest.NewServer(stub)
		defer server.Close()
		host := strings.TrimPrefix(server.URL, "http://")
		resolver := NewResolver("")
		_, err := resolver.Resolve(context.Background(), "x",
			host+"/acme/migrate@"+digestOf(body))
		if got := ResolveCode(err); got != ReasonImageDigestMismatch {
			t.Fatalf("code = %q, want %s (err=%v)", got, ReasonImageDigestMismatch, err)
		}
	})
}

func TestResolve_ErrorTaxonomy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		want   string
	}{
		{"404 is not found", http.StatusNotFound, ReasonImageNotFound},
		{"403 is unauthorized", http.StatusForbidden, ReasonImageResolveUnauthorized},
		{"500 is unavailable", http.StatusInternalServerError, ReasonImageResolveUnavailable},
		{"503 is unavailable", http.StatusServiceUnavailable, ReasonImageResolveUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &registryStub{t: t, status: tc.status}
			resolver, reference := newStubResolver(t, stub)
			_, err := resolver.Resolve(context.Background(), "migrate", reference)
			if got := ResolveCode(err); got != tc.want {
				t.Fatalf("status %d -> code %q, want %s (err=%v)", tc.status, got, tc.want, err)
			}
		})
	}

	t.Run("an unreachable registry is unavailable", func(t *testing.T) {
		resolver := NewResolver("")
		// Port 1 on loopback refuses connections deterministically; no DNS,
		// no sleep, no external dependency.
		_, err := resolver.Resolve(context.Background(), "x", "127.0.0.1:1/acme/migrate:v3")
		if got := ResolveCode(err); got != ReasonImageResolveUnavailable {
			t.Fatalf("code = %q, want %s (err=%v)", got, ReasonImageResolveUnavailable, err)
		}
	})
}

// A 401 carrying a Bearer challenge is the normal first response from Docker
// Hub and ghcr.io, not an error. The resolver must complete the handshake.
func TestResolve_CompletesTheBearerTokenHandshake(t *testing.T) {
	body := indexBody(t)
	var tokenScope string
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	defer server.Close()

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		tokenScope = r.URL.Query().Get("scope")
		json.NewEncoder(w).Encode(map[string]string{"token": "issued-token"})
	})
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer issued-token" {
			w.Header().Set("WWW-Authenticate",
				fmt.Sprintf(`Bearer realm="%s/token",service="stub",scope="repository:acme/migrate:pull"`,
					server.URL))
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Docker-Content-Digest", digestOf(body))
		w.Header().Set("Content-Type", MediaTypeOCIIndex)
		w.Write(body)
	})

	host := strings.TrimPrefix(server.URL, "http://")
	image, err := NewResolver("").Resolve(context.Background(), "migrate", host+"/acme/migrate:v3")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if image.GetIndexDigest() != digestOf(body) {
		t.Fatalf("digest = %q", image.GetIndexDigest())
	}
	if tokenScope != "repository:acme/migrate:pull" {
		t.Fatalf("token scope = %q", tokenScope)
	}
}

// A scope value contains commas, so splitting the challenge on every comma
// truncates it and the issued token is scoped to the wrong thing.
func TestParseBearerChallenge_KeepsCommasInsideQuotedValues(t *testing.T) {
	got := parseBearerChallenge(
		`Bearer realm="https://auth.example/token",service="registry",scope="repository:a/b:pull,push"`)
	if got == nil {
		t.Fatal("challenge did not parse")
	}
	if got.Scope != "repository:a/b:pull,push" {
		t.Fatalf("scope = %q, want the comma preserved", got.Scope)
	}
	if got.Realm != "https://auth.example/token" || got.Service != "registry" {
		t.Fatalf("challenge = %+v", got)
	}
}

func TestResolveOrkflow_ResolvesEachDistinctUsesOnce(t *testing.T) {
	body := indexBody(t)
	stub := &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex, sendDigest: digestOf(body)}
	server := httptest.NewServer(stub)
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")

	wf := &orkstackv1.Orkflow{Name: "demo", Tasks: []*orkstackv1.Task{
		{Id: "a", Uses: "shell"}, {Id: "b", Uses: "shell"}, {Id: "c", Uses: "shell"},
	}}
	catalog := Catalog{"shell": host + "/orkstack/activity-shell:dev"}

	if err := NewResolver("").ResolveOrkflow(context.Background(), catalog, wf); err != nil {
		t.Fatalf("resolve orkflow: %v", err)
	}
	if stub.requests != 1 {
		t.Fatalf("issued %d manifest GETs for one distinct image, want 1", stub.requests)
	}
	for _, task := range wf.GetTasks() {
		if task.GetImage().GetIndexDigest() != digestOf(body) {
			t.Fatalf("task %q was not stamped: %v", task.GetId(), task.GetImage())
		}
	}
}

func TestParseReference(t *testing.T) {
	for _, tc := range []struct {
		in         string
		registry   string
		repository string
		tag        string
		digest     string
	}{
		{"alpine", DefaultRegistry, "library/alpine", "latest", ""},
		{"alpine:3.20", DefaultRegistry, "library/alpine", "3.20", ""},
		{"acme/migrate:v3", DefaultRegistry, "acme/migrate", "v3", ""},
		{"ghcr.io/acme/migrate:v3", "ghcr.io", "acme/migrate", "v3", ""},
		{"ghcr.io/acme/migrate", "ghcr.io", "acme/migrate", "latest", ""},
		{"localhost:5001/orkstack/activity-shell:dev", "localhost:5001", "orkstack/activity-shell", "dev", ""},
		{"ghcr.io/a/b@sha256:" + strings.Repeat("ab", 32), "ghcr.io", "a/b", "", "sha256:" + strings.Repeat("ab", 32)},
	} {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseReference(tc.in)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if got.Registry != tc.registry || got.Repository != tc.repository ||
				got.Tag != tc.tag || got.Digest != tc.digest {
				t.Fatalf("parse(%q) = %+v, want registry=%q repository=%q tag=%q digest=%q",
					tc.in, got, tc.registry, tc.repository, tc.tag, tc.digest)
			}
		})
	}

	t.Run("a port is not a tag", func(t *testing.T) {
		got, err := ParseReference("localhost:5001/x/y")
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		if got.Registry != "localhost:5001" || got.Tag != "latest" {
			t.Fatalf("parse = %+v; a registry port was read as a tag", got)
		}
	})

	t.Run("a malformed digest is refused", func(t *testing.T) {
		if _, err := ParseReference("ghcr.io/a/b@sha256:nothex"); err == nil {
			t.Fatal("a malformed digest parsed")
		}
	})

	t.Run("loopback speaks plain http and nothing else does", func(t *testing.T) {
		local, _ := ParseReference("localhost:5001/x/y:1")
		if local.Scheme() != "http" {
			t.Fatalf("localhost scheme = %q", local.Scheme())
		}
		remote, _ := ParseReference("ghcr.io/x/y:1")
		if remote.Scheme() != "https" {
			t.Fatalf("ghcr.io scheme = %q — a non-loopback registry must use TLS", remote.Scheme())
		}
	})

	t.Run("docker hub's api host is not its reference name", func(t *testing.T) {
		hub, _ := ParseReference("alpine:3.20")
		if hub.Registry != DefaultRegistry {
			t.Fatalf("registry = %q", hub.Registry)
		}
		if hub.APIHost() != DefaultRegistryAPIHost {
			t.Fatalf("api host = %q, want %q", hub.APIHost(), DefaultRegistryAPIHost)
		}
	})
}
