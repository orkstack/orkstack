package dsl

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// stubObject is one content-addressed object a registryStub serves.
type stubObject struct {
	body      []byte
	mediaType string
}

// testImage is one image as a registry holds it: a config blob, the
// platform manifest naming it, and the attestation manifest buildx emits
// beside it.
type testImage struct {
	config      []byte
	manifest    []byte
	attestation []byte
}

// newTestImage builds an image whose config carries labels (nil: none).
func newTestImage(t *testing.T, labels map[string]string) testImage {
	t.Helper()
	config, err := json.Marshal(map[string]any{
		"architecture": "amd64", "os": "linux",
		"config": map[string]any{"Labels": labels},
	})
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return newTestImageWithConfig(t, config)
}

func newTestImageWithConfig(t *testing.T, config []byte) testImage {
	t.Helper()
	manifest, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeOCIManifest,
		"config": map[string]any{"mediaType": "application/vnd.oci.image.config.v1+json",
			"digest": digestOf(config), "size": len(config)},
		"layers": []any{},
	})
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	attestation := []byte(`{"schemaVersion":2,"mediaType":"` + MediaTypeOCIManifest + `","config":{"digest":"sha256:` +
		strings.Repeat("0", 64) + `"},"layers":[]}`)
	return testImage{config: config, manifest: manifest, attestation: attestation}
}

// index is a multi-platform OCI index over the image. entries are rendered in
// order; "attestation" is buildx's os: unknown entry.
func (img testImage) index(t *testing.T, entries ...string) []byte {
	t.Helper()
	manifests := make([]any, 0, len(entries))
	for _, entry := range entries {
		if entry == "attestation" {
			manifests = append(manifests, map[string]any{"mediaType": MediaTypeOCIManifest,
				"digest":   digestOf(img.attestation),
				"platform": map[string]any{"os": "unknown", "architecture": "unknown"}})
			continue
		}
		parts := strings.Split(entry, "/")
		platform := map[string]any{"os": parts[0], "architecture": parts[1]}
		if len(parts) > 2 {
			platform["variant"] = parts[2]
		}
		manifests = append(manifests, map[string]any{"mediaType": MediaTypeOCIManifest,
			"digest": digestOf(img.manifest), "platform": platform})
	}
	body, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     MediaTypeOCIIndex,
		"manifests":     manifests,
	})
	if err != nil {
		t.Fatalf("index: %v", err)
	}
	return body
}

// objects are the content-addressed paths a registry serves for the image.
func (img testImage) objects() map[string]stubObject {
	return map[string]stubObject{
		"manifests/" + digestOf(img.manifest):    {img.manifest, MediaTypeOCIManifest},
		"manifests/" + digestOf(img.attestation): {img.attestation, MediaTypeOCIManifest},
		"blobs/" + digestOf(img.config):          {img.config, "application/octet-stream"},
	}
}

// indexBody is a two-platform OCI index, as a registry would serve one, over
// an unlabelled image. Every stub that serves it also serves the image's
// content-addressed objects (see indexStub).
func indexBody(t *testing.T) []byte {
	t.Helper()
	return newTestImage(t, nil).index(t, "linux/amd64", "linux/arm64/v8", "attestation")
}

// indexStub serves indexBody at the tag and the image behind it by digest.
func indexStub(t *testing.T) *registryStub {
	t.Helper()
	body := indexBody(t)
	return &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex, sendDigest: digestOf(body),
		objects: newTestImage(t, nil).objects()}
}

// registryStub serves /v2/<repo>/manifests/<ref> the way a registry does:
// body at the tag (or any reference objects does not name), and every
// content-addressed object by its digest.
type registryStub struct {
	t          *testing.T
	body       []byte
	mediaType  string
	sendDigest string // Docker-Content-Digest to send; "" sends none
	status     int
	objects    map[string]stubObject // "manifests/<digest>" or "blobs/<digest>"
	blobStatus int                   // when set, every blob GET answers it
	gotAccept  string
	gotMethod  string
	requests   int
	paths      []string
}

func (s *registryStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.requests++
	s.paths = append(s.paths, r.URL.Path)
	s.gotMethod = r.Method
	if s.status != 0 && s.status != http.StatusOK {
		w.WriteHeader(s.status)
		fmt.Fprintln(w, "refused")
		return
	}
	_, rest, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/v2/"), "/manifests/")
	isBlob := strings.Contains(r.URL.Path, "/blobs/")
	if isBlob {
		_, digest, _ := strings.Cut(r.URL.Path, "/blobs/")
		if s.blobStatus != 0 {
			w.WriteHeader(s.blobStatus)
			fmt.Fprintln(w, "refused")
			return
		}
		obj, ok := s.objects["blobs/"+digest]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", obj.mediaType)
		w.Write(obj.body)
		return
	}
	if obj, ok := s.objects["manifests/"+rest]; ok {
		w.Header().Set("Content-Type", obj.mediaType)
		w.Write(obj.body)
		return
	}
	s.gotAccept = r.Header.Get("Accept")
	if s.sendDigest != "" {
		w.Header().Set("Docker-Content-Digest", s.sendDigest)
	}
	w.Header().Set("Content-Type", s.mediaType)
	w.WriteHeader(http.StatusOK)
	w.Write(s.body)
}

// tagRequests counts GETs of the tag (not of content-addressed objects).
func (s *registryStub) tagRequests() int {
	n := 0
	for _, p := range s.paths {
		if strings.HasSuffix(p, "/manifests/v3") || strings.HasSuffix(p, ":dev") || strings.HasSuffix(p, "/manifests/dev") {
			n++
		}
	}
	return n
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
	stub := indexStub(t)
	body := stub.body
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
	img := newTestImage(t, nil)
	body := img.manifest
	stub := &registryStub{t: t, body: body, mediaType: MediaTypeOCIManifest, sendDigest: digestOf(body),
		objects: img.objects()}
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
		stub := indexStub(t)
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
	server, stub, tokens := bearerRegistry(t, indexStub(t))
	body := stub.body

	host := strings.TrimPrefix(server.URL, "http://")
	image, err := NewResolver("").Resolve(context.Background(), "migrate", host+"/acme/migrate:v3")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if image.GetIndexDigest() != digestOf(body) {
		t.Fatalf("digest = %q", image.GetIndexDigest())
	}
	if tokens.scope != "repository:acme/migrate:pull" {
		t.Fatalf("token scope = %q", tokens.scope)
	}
}

type tokenLog struct {
	scope string
	hits  int
}

// bearerRegistry fronts stub with a token-auth registry: every /v2/ request
// without the issued token answers 401 with a Bearer challenge.
func bearerRegistry(t *testing.T, stub *registryStub) (*httptest.Server, *registryStub, *tokenLog) {
	t.Helper()
	tokens := &tokenLog{}
	mux := http.NewServeMux()
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		tokens.hits++
		tokens.scope = r.URL.Query().Get("scope")
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
		stub.ServeHTTP(w, r)
	})
	return server, stub, tokens
}

// §4.3 step 7c: the token the manifest GET earned is the credential of the
// platform-manifest and config-blob GETs. One Inspect, one handshake.
func TestInspect_ReusesTheBearerTokenForTheBlobGet(t *testing.T) {
	img := newTestImage(t, map[string]string{InterfaceLabel: `{"schemaVersion":1}`})
	body := img.index(t, "linux/amd64")
	server, stub, tokens := bearerRegistry(t, &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex,
		sendDigest: digestOf(body), objects: img.objects()})

	host := strings.TrimPrefix(server.URL, "http://")
	res, err := NewResolver("").Inspect(context.Background(), "migrate", host+"/acme/migrate:v3")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if res.Interface == nil {
		t.Fatalf("interface not read: %+v", res)
	}
	if tokens.hits != 1 {
		t.Fatalf("token endpoint hit %d times for manifest and blob together, want 1", tokens.hits)
	}
	if stub.requests != 3 {
		t.Fatalf("authorized GETs = %d, want 3 (index, manifest, blob): %v", stub.requests, stub.paths)
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
	stub := indexStub(t)
	body := stub.body
	server := httptest.NewServer(stub)
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")

	wf := &orkstackv1.Orkflow{Name: "demo", Tasks: []*orkstackv1.Task{
		{Id: "a", Uses: "shell"}, {Id: "b", Uses: "shell"}, {Id: "c", Uses: "shell"},
	}}
	catalog := Catalog{"shell": host + "/orkstack/activity-shell:dev"}

	resolutions, err := NewResolver("").ResolveOrkflow(context.Background(), catalog, wf)
	if err != nil {
		t.Fatalf("resolve orkflow: %v", err)
	}
	// One distinct image is one tag GET, one platform-manifest GET and one
	// config-blob GET — not three of each.
	if got := stub.tagRequests(); got != 1 {
		t.Fatalf("issued %d tag manifest GETs for one distinct image, want 1", got)
	}
	if stub.requests != 3 {
		t.Fatalf("issued %d GETs for one distinct image, want 3 (index, manifest, blob): %v", stub.requests, stub.paths)
	}
	if len(resolutions) != 1 || resolutions["shell"] == nil {
		t.Fatalf("resolutions = %v, want one keyed by uses", resolutions)
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

const testLabel = `{"schemaVersion":1,"ops":{"copy":{"fields":{"src":{"type":"string","required":true}}}}}`

func singleManifestStub(t *testing.T, img testImage) *registryStub {
	t.Helper()
	return &registryStub{t: t, body: img.manifest, mediaType: MediaTypeOCIManifest,
		sendDigest: digestOf(img.manifest), objects: img.objects()}
}

func TestInspect_ReadsTheLabelFromASingleManifestConfig(t *testing.T) {
	img := newTestImage(t, map[string]string{InterfaceLabel: testLabel, "other": "x"})
	resolver, reference := newStubResolver(t, singleManifestStub(t, img))

	res, err := resolver.Inspect(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if res.InterfaceError != "" {
		t.Fatalf("interface error = %q", res.InterfaceError)
	}
	if got := res.Interface.GetOps()["copy"].GetFields()["src"]; !got.GetRequired() || got.GetType() != "string" {
		t.Fatalf("interface = %v, want the label's copy.src", res.Interface)
	}
	image, err := resolver.Resolve(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !proto.Equal(image, res.Image) {
		t.Fatalf("Inspect's image %v differs from Resolve's %v", res.Image, image)
	}
}

func TestInspect_SelectsTheFirstRunnablePlatformOfAnIndex(t *testing.T) {
	img := newTestImage(t, map[string]string{InterfaceLabel: testLabel})
	body := img.index(t, "attestation", "linux/amd64")
	stub := &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex, sendDigest: digestOf(body),
		objects: img.objects()}
	resolver, reference := newStubResolver(t, stub)

	res, err := resolver.Inspect(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if res.Interface == nil {
		t.Fatalf("interface not read (error %q)", res.InterfaceError)
	}
	want := []string{
		"/v2/acme/migrate/manifests/v3",
		"/v2/acme/migrate/manifests/" + digestOf(img.manifest),
		"/v2/acme/migrate/blobs/" + digestOf(img.config),
	}
	if !reflect.DeepEqual(stub.paths, want) {
		t.Fatalf("GETs = %v, want exactly %v — the attestation entry is skipped", stub.paths, want)
	}
}

func TestInspect_AnIndexWithNoRunnablePlatformReportsItAsUnreadable(t *testing.T) {
	img := newTestImage(t, map[string]string{InterfaceLabel: testLabel})
	body := img.index(t, "attestation")
	stub := &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex, sendDigest: digestOf(body),
		objects: img.objects()}
	resolver, reference := newStubResolver(t, stub)

	res, err := resolver.Inspect(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatalf("inspect: %v — an unreadable interface must never fail resolution", err)
	}
	if res.InterfaceError != "index advertises no platform manifest to read a config from" {
		t.Fatalf("interface error = %q", res.InterfaceError)
	}
	if res.Interface != nil || res.Image.GetIndexDigest() != digestOf(body) {
		t.Fatalf("resolution = %+v, want a pinned image and no interface", res)
	}
}

func TestInspect_NoLabelMeansNoInterface(t *testing.T) {
	resolver, reference := newStubResolver(t, indexStub(t))
	res, err := resolver.Inspect(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if res.Interface != nil || res.InterfaceError != "" {
		t.Fatalf("an unlabelled image declared %v / %q, want nothing", res.Interface, res.InterfaceError)
	}
}

func TestInspect_AMalformedLabelNeverFailsResolution(t *testing.T) {
	img := newTestImage(t, map[string]string{InterfaceLabel: `{"schemaVersion":2}`})
	resolver, reference := newStubResolver(t, singleManifestStub(t, img))

	res, err := resolver.Inspect(context.Background(), "migrate", reference)
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if res.Interface != nil {
		t.Fatalf("interface = %v, want nil", res.Interface)
	}
	if res.InterfaceError != "schemaVersion 2 is not supported (this engine reads 1)" {
		t.Fatalf("interface error = %q, want ParseInterfaceLabel's message", res.InterfaceError)
	}
	if res.Image.GetPinned() == "" {
		t.Fatal("the image was not pinned")
	}
}

func TestInspect_ConfigBlobFailureTaxonomy(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stub  func(*testing.T) *registryStub
		want  string
		match string
	}{
		{"blob 404 is not found", func(t *testing.T) *registryStub {
			s := singleManifestStub(t, newTestImage(t, nil))
			s.blobStatus = http.StatusNotFound
			return s
		}, ReasonImageNotFound, "has no blob sha256:"},
		{"blob 403 is unauthorized", func(t *testing.T) *registryStub {
			s := singleManifestStub(t, newTestImage(t, nil))
			s.blobStatus = http.StatusForbidden
			return s
		}, ReasonImageResolveUnauthorized, ""},
		{"blob 503 is unavailable", func(t *testing.T) *registryStub {
			s := singleManifestStub(t, newTestImage(t, nil))
			s.blobStatus = http.StatusServiceUnavailable
			return s
		}, ReasonImageResolveUnavailable, ""},
		{"a blob that does not hash to config.digest is a mismatch", func(t *testing.T) *registryStub {
			img := newTestImage(t, nil)
			s := singleManifestStub(t, img)
			s.objects["blobs/"+digestOf(img.config)] = stubObject{[]byte(`{"config":{}}`), "application/octet-stream"}
			return s
		}, ReasonImageDigestMismatch, ""},
		{"a manifest without config is unsupported", func(t *testing.T) *registryStub {
			body := []byte(`{"schemaVersion":2,"layers":[]}`)
			return &registryStub{t: t, body: body, mediaType: MediaTypeOCIManifest, sendDigest: digestOf(body)}
		}, ReasonImageResolveUnsupported, "manifest has no readable config descriptor"},
		{"a blob over 4 MiB is unsupported", func(t *testing.T) *registryStub {
			big := make([]byte, maxManifestBytes+1)
			for i := range big {
				big[i] = ' '
			}
			return singleManifestStub(t, newTestImageWithConfig(t, big))
		}, ReasonImageResolveUnsupported, "larger than"},
		{"a config that is not JSON is unsupported", func(t *testing.T) *registryStub {
			return singleManifestStub(t, newTestImageWithConfig(t, []byte("not json")))
		}, ReasonImageResolveUnsupported, "config blob is not valid JSON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver, reference := newStubResolver(t, tc.stub(t))
			_, err := resolver.Inspect(context.Background(), "migrate", reference)
			if got := ResolveCode(err); got != tc.want {
				t.Fatalf("code = %q, want %s (err=%v)", got, tc.want, err)
			}
			if tc.match != "" && !strings.Contains(err.Error(), tc.match) {
				t.Fatalf("error %q does not say %q", err, tc.match)
			}
		})
	}
}

func TestResolve_IsUnchangedByTheLabel(t *testing.T) {
	plain := newTestImage(t, nil)
	labelled := newTestImage(t, map[string]string{InterfaceLabel: testLabel})
	var images []*orkstackv1.ImageRef
	for _, img := range []testImage{plain, labelled} {
		body := img.index(t, "linux/amd64", "attestation")
		resolver, reference := newStubResolver(t, &registryStub{t: t, body: body, mediaType: MediaTypeOCIIndex,
			sendDigest: digestOf(body), objects: img.objects()})
		image, err := resolver.Resolve(context.Background(), "migrate", reference)
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		images = append(images, image)
	}
	// The two indexes differ (they name different manifests), so the digest
	// does; everything the label could have touched does not.
	for _, image := range images {
		image.IndexDigest, image.Pinned, image.Registry = "", "", ""
	}
	if !proto.Equal(images[0], images[1]) {
		t.Fatalf("the label changed the ImageRef:\n%v\n%v", images[0], images[1])
	}
}

func TestResolveOrkflow_SkipsAnAwaitedTask(t *testing.T) {
	stub := indexStub(t)
	server := httptest.NewServer(stub)
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")

	wf := &orkstackv1.Orkflow{Name: "demo", Tasks: []*orkstackv1.Task{
		{Id: "a", Uses: "shell"},
		{Id: "gate", Needs: []string{"a"}, Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}},
	}}
	catalog := Catalog{"shell": host + "/orkstack/activity-shell:dev"}
	resolutions, err := NewResolver("").ResolveOrkflow(context.Background(), catalog, wf)
	if err != nil {
		t.Fatalf("resolve orkflow: %v", err)
	}
	if len(resolutions) != 1 || resolutions[""] != nil {
		t.Fatalf("resolutions = %v, want only the container task's image", resolutions)
	}
	if wf.GetTasks()[1].GetImage() != nil {
		t.Fatalf("the await was stamped with an image: %v", wf.GetTasks()[1].GetImage())
	}
}

func TestResolveOrkflow_AnAwaitOnlyOrkflowContactsNoRegistry(t *testing.T) {
	wf := &orkstackv1.Orkflow{Name: "demo", Tasks: []*orkstackv1.Task{
		{Id: "gate", Await: &orkstackv1.AwaitSpec{TimeoutSeconds: 60}},
	}}
	resolutions, err := NewResolver("").ResolveOrkflow(context.Background(), Catalog{}, wf)
	if err != nil || len(resolutions) != 0 {
		t.Fatalf("ResolveOrkflow = %v, %v; want no resolutions and no error", resolutions, err)
	}
}
