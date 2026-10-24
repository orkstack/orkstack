package api_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/dsl"
)

// movableRegistry serves a manifest whose bytes the test can change, which is
// how "the tag moved" is expressed without a second registry. It is a real
// registry in miniature: the tag serves an index, and every platform manifest
// and config blob an index names is served by its digest, because resolution
// reads the image config for its interface label (§4.3 step 7).
type movableRegistry struct {
	body       atomic.Value // []byte, the index served at every tag
	mu         sync.Mutex
	objects    map[string][]byte // "manifests/<digest>" or "blobs/<digest>"
	blobStatus atomic.Int32      // when non-zero, every blob GET answers it
}

func (m *movableRegistry) set(body []byte) { m.body.Store(body) }

// addImage makes the registry serve img's platform manifest and config.
func (m *movableRegistry) addImage(img testImage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.objects == nil {
		m.objects = map[string][]byte{}
	}
	m.objects["manifests/"+digestOf(img.manifest)] = img.manifest
	m.objects["blobs/"+digestOf(img.config)] = img.config
}

// setImage points every tag at an index over an image whose config carries
// labels (nil: an unlabelled image).
func (m *movableRegistry) setImage(t *testing.T, labels map[string]string, platforms ...string) testImage {
	t.Helper()
	img := newTestImage(t, labels)
	m.addImage(img)
	m.set(img.index(t, platforms...))
	return img
}

func (m *movableRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, digest, isBlob := strings.Cut(r.URL.Path, "/blobs/"); isBlob {
		if status := m.blobStatus.Load(); status != 0 {
			w.WriteHeader(int(status))
			return
		}
		m.mu.Lock()
		body, ok := m.objects["blobs/"+digest]
		m.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Write(body)
		return
	}
	_, target, _ := strings.Cut(r.URL.Path, "/manifests/")
	m.mu.Lock()
	manifest, ok := m.objects["manifests/"+target]
	m.mu.Unlock()
	if ok {
		w.Header().Set("Content-Type", dsl.MediaTypeOCIManifest)
		w.Write(manifest)
		return
	}
	body := m.body.Load().([]byte)
	sum := sha256.Sum256(body)
	w.Header().Set("Docker-Content-Digest", "sha256:"+hex.EncodeToString(sum[:]))
	w.Header().Set("Content-Type", dsl.MediaTypeOCIIndex)
	w.Write(body)
}

// testImage is one image as a registry holds it.
type testImage struct{ config, manifest []byte }

// newTestImage builds an image whose config carries labels (nil: none).
func newTestImage(t *testing.T, labels map[string]string) testImage {
	t.Helper()
	config, err := json.Marshal(map[string]any{"architecture": "amd64", "os": "linux",
		"config": map[string]any{"Labels": labels}})
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	manifest, err := json.Marshal(map[string]any{"schemaVersion": 2, "mediaType": dsl.MediaTypeOCIManifest,
		"config": map[string]any{"mediaType": "application/vnd.oci.image.config.v1+json",
			"digest": digestOf(config), "size": len(config)},
		"layers": []any{}})
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	return testImage{config: config, manifest: manifest}
}

// index is an OCI index whose every platform entry names img's manifest.
func (img testImage) index(t *testing.T, platforms ...string) []byte {
	t.Helper()
	manifests := make([]any, 0, len(platforms))
	for _, p := range platforms {
		parts := strings.SplitN(p, "/", 2)
		manifests = append(manifests, map[string]any{
			"mediaType": dsl.MediaTypeOCIManifest,
			"digest":    digestOf(img.manifest),
			"platform":  map[string]any{"os": parts[0], "architecture": parts[1]},
		})
	}
	body, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     dsl.MediaTypeOCIIndex,
		"manifests":     manifests,
	})
	if err != nil {
		t.Fatalf("index: %v", err)
	}
	return body
}

// indexWithPlatforms is an index over the unlabelled image every
// movableRegistry serves (newMovableRegistry).
func indexWithPlatforms(t *testing.T, platforms ...string) []byte {
	t.Helper()
	return newTestImage(t, nil).index(t, platforms...)
}

// newMovableRegistry serves indexWithPlatforms(linux/amd64) and the
// unlabelled image behind every indexWithPlatforms body.
func newMovableRegistry(t *testing.T) *movableRegistry {
	t.Helper()
	registry := &movableRegistry{}
	registry.addImage(newTestImage(t, nil))
	registry.set(indexWithPlatforms(t, "linux/amd64"))
	return registry
}

func digestOf(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// applyServiceWithRegistry wires a Service against a live local registry stub.
func applyServiceWithRegistry(t *testing.T) (*api.Service, *movableRegistry, string) {
	t.Helper()
	registry := newMovableRegistry(t)
	server := httptest.NewServer(registry)
	t.Cleanup(server.Close)
	host := strings.TrimPrefix(server.URL, "http://")

	catalog := dsl.Catalog{"shell": host + "/orkstack/activity-shell:dev"}
	svc := api.NewService(api.Config{
		Catalog:  catalog,
		Resolver: dsl.NewResolver(""),
	})
	return svc, registry, host
}

const applyDocument = `apiVersion: orkstack/v1
kind: Orkflow
name: pinned
tasks:
  - id: one
    uses: shell
    with:
      op: exec
`

func applyOnce(t *testing.T, svc *api.Service, yaml string) *orkstackv1.ApplyOrkflowResponse {
	t.Helper()
	resp, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: yaml}))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	return resp.Msg
}

// Apply resolves `uses:` through the catalog, pins the index digest, and
// records BOTH what the author wrote and what it resolved to (§4.3 step 6).
func TestApplyOrkflow_PinsTheIndexDigestAtApplyTime(t *testing.T) {
	svc, registry, host := applyServiceWithRegistry(t)
	body := indexWithPlatforms(t, "linux/amd64", "linux/arm64")
	registry.set(body)

	applied := applyOnce(t, svc, applyDocument)
	image := applied.GetOrkflow().GetTasks()[0].GetImage()

	if image.GetReference() != "shell" {
		t.Fatalf("reference = %q, want the author's spelling", image.GetReference())
	}
	if image.GetIndexDigest() != digestOf(body) {
		t.Fatalf("index digest = %q, want %q", image.GetIndexDigest(), digestOf(body))
	}
	want := host + "/orkstack/activity-shell@" + digestOf(body)
	if image.GetPinned() != want {
		t.Fatalf("pinned = %q, want %q", image.GetPinned(), want)
	}
	if got := image.GetPlatforms(); len(got) != 2 || got[0] != "linux/amd64" {
		t.Fatalf("platforms = %v", got)
	}
	if applied.GetOrkflow().GetDefinitionDigest() == "" {
		t.Fatal("the applied version carries no definition digest")
	}
}

// Re-applying an unchanged document against an unchanged tag is a no-op: same
// version, created=false. This is what `artifacts.SameStoredOrkflow` decides, and it is
// the case ImageRef.resolved_at had to be removed for (§4.4).
func TestApplyOrkflow_ReApplyingAnUnchangedDocumentIsIdempotent(t *testing.T) {
	svc, _, _ := applyServiceWithRegistry(t)

	first := applyOnce(t, svc, applyDocument)
	if !first.GetCreated() {
		t.Fatal("the first apply did not create a version")
	}
	second := applyOnce(t, svc, applyDocument)

	if second.GetCreated() {
		t.Fatal("re-applying an unchanged document created a second version")
	}
	if first.GetOrkflow().GetVersion() != second.GetOrkflow().GetVersion() {
		t.Fatalf("version changed across an idempotent re-apply: %q -> %q",
			first.GetOrkflow().GetVersion(), second.GetOrkflow().GetVersion())
	}
	if first.GetOrkflow().GetDefinitionDigest() != second.GetOrkflow().GetDefinitionDigest() {
		t.Fatalf("definition digest changed across an idempotent re-apply: %q -> %q",
			first.GetOrkflow().GetDefinitionDigest(), second.GetOrkflow().GetDefinitionDigest())
	}
}

// A moved tag is a DIFFERENT definition, so the same source mints a new
// version. This is the defect §4.4(b) exists to fix: deriving the version
// before resolution minted the same version for a different definition, and
// the store then refused the apply as a rewrite of an immutable version.
func TestApplyOrkflow_AMovedTagMintsANewVersionForTheSameSource(t *testing.T) {
	svc, registry, _ := applyServiceWithRegistry(t)

	before := applyOnce(t, svc, applyDocument)

	// The tag now points somewhere else.
	registry.set(indexWithPlatforms(t, "linux/amd64", "linux/arm64", "linux/s390x"))
	after := applyOnce(t, svc, applyDocument)

	if before.GetOrkflow().GetVersion() == after.GetOrkflow().GetVersion() {
		t.Fatal("a moved tag reused the version of a different definition — " +
			"the store would refuse this as a rewrite of an immutable version")
	}
	if before.GetOrkflow().GetDefinitionDigest() == after.GetOrkflow().GetDefinitionDigest() {
		t.Fatal("a moved tag produced the same definition digest")
	}
	if !after.GetCreated() {
		t.Fatal("the re-apply after a moved tag did not create a new version")
	}
	beforeImage := before.GetOrkflow().GetTasks()[0].GetImage()
	afterImage := after.GetOrkflow().GetTasks()[0].GetImage()
	if beforeImage.GetIndexDigest() == afterImage.GetIndexDigest() {
		t.Fatal("the pinned digest did not move with the tag")
	}
}

// A run pins a definition, so a moved tag must not change what an ALREADY
// applied version runs.
func TestApplyOrkflow_AMovedTagDoesNotRewriteTheStoredVersion(t *testing.T) {
	svc, registry, _ := applyServiceWithRegistry(t)

	before := applyOnce(t, svc, applyDocument)
	pinned := before.GetOrkflow().GetTasks()[0].GetImage().GetIndexDigest()

	registry.set(indexWithPlatforms(t, "linux/riscv64"))
	applyOnce(t, svc, applyDocument)

	stored, err := svc.GetOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.GetOrkflowRequest{
			Name:    "pinned",
			Version: before.GetOrkflow().GetVersion(),
		}))
	if err != nil {
		t.Fatalf("get the original version: %v", err)
	}
	got := stored.Msg.GetOrkflow().GetTasks()[0].GetImage().GetIndexDigest()
	if got != pinned {
		t.Fatalf("the stored version's pin changed under it: %q -> %q", pinned, got)
	}
}

// An unresolvable reference is an APPLY-time error, not a run-time surprise.
func TestApplyOrkflow_AnUnresolvableReferenceFailsAtApply(t *testing.T) {
	svc := api.NewService(api.Config{
		Catalog:  dsl.Catalog{"shell": "127.0.0.1:1/orkstack/activity-shell:dev"},
		Resolver: dsl.NewResolver(""),
	})
	_, err := svc.ApplyOrkflow(context.Background(),
		connect.NewRequest(&orkstackv1.ApplyOrkflowRequest{Yaml: applyDocument}))
	if err == nil {
		t.Fatal("apply succeeded against a registry that cannot be reached")
	}
	if !strings.Contains(err.Error(), dsl.ReasonImageResolveUnavailable) {
		t.Fatalf("error does not carry the reason code: %v", err)
	}
	// Unreachable is retryable: nothing about the document is wrong.
	if connect.CodeOf(err) != connect.CodeUnavailable {
		t.Fatalf("code = %v, want Unavailable", connect.CodeOf(err))
	}
}

func TestListActivities_ReportsTheCatalogOrderedByName(t *testing.T) {
	registry := newMovableRegistry(t)
	server := httptest.NewServer(registry)
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")

	catalog := dsl.Catalog{
		"shell": host + "/orkstack/activity-shell:dev",
		"file":  host + "/orkstack/activity-file:dev",
		"http":  host + "/orkstack/activity-http:dev",
	}
	svc := api.NewService(api.Config{
		Catalog:    catalog,
		Activities: &api.CatalogLister{Catalog: catalog, Resolver: dsl.NewResolver("")},
	})

	resp, err := svc.ListActivities(context.Background(),
		connect.NewRequest(&orkstackv1.ListActivitiesRequest{}))
	if err != nil {
		t.Fatalf("list activities: %v", err)
	}
	got := resp.Msg.GetActivities()
	want := []string{"file", "http", "shell"}
	if len(got) != len(want) {
		t.Fatalf("got %d activities, want %d", len(got), len(want))
	}
	for i, name := range want {
		if got[i].GetName() != name {
			t.Fatalf("activities = %v, want %v (order is contract, §2.3)",
				[]string{got[0].GetName(), got[1].GetName(), got[2].GetName()}, want)
		}
	}
	if got[0].GetIndexDigest() == "" {
		t.Fatal("a resolvable catalog entry reported no digest")
	}
}

// A catalog entry whose registry is down is still a catalog entry. Failing the
// whole listing would tell an operator only that something is down; reporting
// the entry with a blank digest tells them something true.
func TestListActivities_SurvivesAnUnresolvableEntry(t *testing.T) {
	catalog := dsl.Catalog{"shell": "127.0.0.1:1/orkstack/activity-shell:dev"}
	lister := &api.CatalogLister{Catalog: catalog, Resolver: dsl.NewResolver("")}

	activities, err := lister.ListActivities(context.Background())
	if err != nil {
		t.Fatalf("list activities: %v", err)
	}
	if len(activities) != 1 {
		t.Fatalf("got %d activities, want the entry to survive", len(activities))
	}
	if activities[0].GetIndexDigest() != "" {
		t.Fatal("an unresolvable entry reported a digest")
	}
	if activities[0].GetReference() == "" {
		t.Fatal("an unresolvable entry lost its configured reference")
	}
}
