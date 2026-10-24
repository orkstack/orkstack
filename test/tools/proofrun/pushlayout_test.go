package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// plRegistry is an in-memory subset of the OCI distribution API: blob HEAD,
// monolithic uploads, and manifests by digest or tag. Like a real registry it
// refuses a manifest whose blobs or child manifests it does not hold yet.
type plRegistry struct {
	mu        sync.Mutex
	blobs     map[string][]byte // repo@digest
	manifests map[string]plStored
	tags      map[string]string // repo:tag -> digest
	puts      []string          // manifest PUTs in order, repo:ref
	lie       string            // a digest to answer for every manifest HEAD
}

type plStored struct {
	mediaType string
	body      []byte
}

func plDigest(b []byte) string {
	s := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(s[:])
}

func newPLRegistry(t *testing.T) (*plRegistry, string) {
	t.Helper()
	reg := &plRegistry{blobs: map[string][]byte{}, manifests: map[string]plStored{}, tags: map[string]string{}}
	srv := httptest.NewServer(http.HandlerFunc(reg.serve))
	t.Cleanup(srv.Close)
	return reg, strings.TrimPrefix(srv.URL, "http://")
}

func (reg *plRegistry) serve(w http.ResponseWriter, r *http.Request) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	rest := strings.TrimPrefix(r.URL.Path, "/v2/")
	body, _ := io.ReadAll(r.Body)
	switch {
	case strings.Contains(rest, "/blobs/uploads/"):
		i := strings.LastIndex(rest, "/blobs/uploads/")
		repo, id := rest[:i], rest[i+len("/blobs/uploads/"):]
		switch {
		case r.Method == http.MethodPost && id == "":
			// A relative Location that already carries a query string, as
			// registry:3 answers.
			w.Header().Set("Location", "/v2/"+repo+"/blobs/uploads/u1?_state=a%3Db")
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPut && id == "u1":
			d := r.URL.Query().Get("digest")
			if r.URL.Query().Get("_state") != "a=b" || plDigest(body) != d {
				http.Error(w, "DIGEST_INVALID", http.StatusBadRequest)
				return
			}
			reg.blobs[repo+"@"+d] = body
			w.Header().Set("Docker-Content-Digest", d)
			w.WriteHeader(http.StatusCreated)
		default:
			http.Error(w, "unsupported", http.StatusMethodNotAllowed)
		}
	case strings.Contains(rest, "/blobs/"):
		i := strings.LastIndex(rest, "/blobs/")
		if _, ok := reg.blobs[rest[:i]+"@"+rest[i+len("/blobs/"):]]; ok {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	case strings.Contains(rest, "/manifests/"):
		i := strings.LastIndex(rest, "/manifests/")
		repo, ref := rest[:i], rest[i+len("/manifests/"):]
		switch r.Method {
		case http.MethodPut:
			var m ociContent
			if json.Unmarshal(body, &m) != nil {
				http.Error(w, "MANIFEST_INVALID", http.StatusBadRequest)
				return
			}
			need := append([]ociDescriptor{}, m.Layers...)
			if m.Config != nil {
				need = append(need, *m.Config)
			}
			for _, d := range need {
				if _, ok := reg.blobs[repo+"@"+d.Digest]; !ok {
					http.Error(w, "MANIFEST_BLOB_UNKNOWN "+d.Digest, http.StatusBadRequest)
					return
				}
			}
			for _, d := range m.Manifests {
				if _, ok := reg.manifests[repo+"@"+d.Digest]; !ok {
					http.Error(w, "MANIFEST_UNKNOWN "+d.Digest, http.StatusBadRequest)
					return
				}
			}
			d := plDigest(body)
			reg.manifests[repo+"@"+d] = plStored{mediaType: r.Header.Get("Content-Type"), body: body}
			if !strings.HasPrefix(ref, "sha256:") {
				reg.tags[repo+":"+ref] = d
			} else if ref != d {
				http.Error(w, "DIGEST_INVALID", http.StatusBadRequest)
				return
			}
			reg.puts = append(reg.puts, repo+":"+ref)
			w.Header().Set("Docker-Content-Digest", d)
			w.WriteHeader(http.StatusCreated)
		case http.MethodHead, http.MethodGet:
			d := ref
			if !strings.HasPrefix(ref, "sha256:") {
				d = reg.tags[repo+":"+ref]
			}
			got, ok := reg.manifests[repo+"@"+d]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if reg.lie != "" {
				d = reg.lie
			}
			w.Header().Set("Content-Type", got.mediaType)
			w.Header().Set("Docker-Content-Digest", d)
			w.WriteHeader(http.StatusOK)
		}
	default:
		w.WriteHeader(http.StatusOK)
	}
}

// plWriteBlob stores b in the layout at dir and returns its descriptor.
func plWriteBlob(t *testing.T, dir, mediaType string, b []byte) ociDescriptor {
	t.Helper()
	d := ociDescriptor{MediaType: mediaType, Digest: plDigest(b), Size: int64(len(b))}
	p := filepath.Join(dir, "blobs", "sha256", strings.TrimPrefix(d.Digest, "sha256:"))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return d
}

func plJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// plImage writes one image manifest, its config and its layer into the
// layout, and returns the manifest's descriptor and the layer's.
func plImage(t *testing.T, dir, name, arch string) (manifest, layer ociDescriptor) {
	t.Helper()
	cfg := plWriteBlob(t, dir, "application/vnd.oci.image.config.v1+json",
		plJSON(t, map[string]any{"architecture": arch, "os": "linux", "config": map[string]any{"Labels": map[string]string{"name": name}}}))
	layer = plWriteBlob(t, dir, "application/vnd.oci.image.layer.v1.tar+gzip", []byte("layer of activity-"+name+" for "+arch))
	m := plJSON(t, map[string]any{"schemaVersion": 2, "mediaType": mediaOCIManifest, "config": cfg, "layers": []ociDescriptor{layer}})
	return plWriteBlob(t, dir, mediaOCIManifest, m), layer
}

// plLayout writes root/activity-<name>, an OCI image layout of one image:
// a single-platform manifest, or, with index, an index of two manifests that
// share their config's shape and differ by platform.
func plLayout(t *testing.T, root, name string, index bool) (dir string, top, layer ociDescriptor) {
	t.Helper()
	dir = filepath.Join(root, "activity-"+name)
	top, layer = plImage(t, dir, name, "amd64")
	if index {
		other, _ := plImage(t, dir, name, "arm64")
		idx := plJSON(t, map[string]any{"schemaVersion": 2, "mediaType": mediaOCIIndex, "manifests": []ociDescriptor{top, other}})
		top = plWriteBlob(t, dir, mediaOCIIndex, idx)
	}
	if err := os.WriteFile(filepath.Join(dir, "oci-layout"), []byte(`{"imageLayoutVersion":"1.0.0"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "index.json"),
		plJSON(t, map[string]any{"schemaVersion": 2, "mediaType": mediaOCIIndex, "manifests": []ociDescriptor{top}}), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, top, layer
}

// plOutput captures what the command reports.
func plOutput(t *testing.T) *bytes.Buffer {
	t.Helper()
	var b bytes.Buffer
	prev := stdout
	stdout = &b
	t.Cleanup(func() { stdout = prev })
	return &b
}

func TestPushLayout_CopiesBytesVerbatimAndKeepsTheDigest(t *testing.T) {
	out := plOutput(t)
	reg, host := newPLRegistry(t)
	root := t.TempDir()
	jsonDir, jsonTop, _ := plLayout(t, root, "json", false)
	shellDir, shellTop, _ := plLayout(t, root, "shell", false)
	tsv := filepath.Join(t.TempDir(), "images.tsv")

	code := cmdPushLayout([]string{"-layouts", root, "-registry", host, "-tag", "pr-1", "-out", tsv})
	if code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	got, err := os.ReadFile(tsv)
	if err != nil {
		t.Fatal(err)
	}
	want := "json\t" + host + "/orkstack/activity-json:pr-1\t" + jsonTop.Digest + "\n" +
		"shell\t" + host + "/orkstack/activity-shell:pr-1\t" + shellTop.Digest + "\n"
	if string(got) != want {
		t.Errorf("images.tsv =\n%s\nwant\n%s", got, want)
	}
	for name, dir := range map[string]string{"json": jsonDir, "shell": shellDir} {
		repo := "orkstack/activity-" + name
		files, _ := filepath.Glob(filepath.Join(dir, "blobs", "sha256", "*"))
		if len(files) != 3 {
			t.Fatalf("%s: the layout holds %d blobs, want 3", name, len(files))
		}
		for _, f := range files {
			b, _ := os.ReadFile(f)
			d := "sha256:" + filepath.Base(f)
			pushed, isBlob := reg.blobs[repo+"@"+d]
			if !isBlob {
				pushed = reg.manifests[repo+"@"+d].body
			}
			if !bytes.Equal(pushed, b) {
				t.Errorf("%s: %s was not copied byte for byte", name, d)
			}
		}
	}
	if reg.tags["orkstack/activity-shell:pr-1"] != shellTop.Digest {
		t.Errorf("the tag resolves to %s, want the layout's %s", reg.tags["orkstack/activity-shell:pr-1"], shellTop.Digest)
	}
	if mt := reg.manifests["orkstack/activity-shell@"+shellTop.Digest].mediaType; mt != mediaOCIManifest {
		t.Errorf("the manifest was sent as %q, want %q", mt, mediaOCIManifest)
	}
}

func TestPushLayout_RefusesABlobThatDoesNotMatchItsDigest(t *testing.T) {
	out := plOutput(t)
	reg, host := newPLRegistry(t)
	root := t.TempDir()
	dir, _, layer := plLayout(t, root, "shell", false)
	file := filepath.Join(dir, "blobs", "sha256", strings.TrimPrefix(layer.Digest, "sha256:"))
	if err := os.WriteFile(file, []byte("not the bytes that were built"), 0o644); err != nil {
		t.Fatal(err)
	}
	tsv := filepath.Join(t.TempDir(), "images.tsv")

	if code := cmdPushLayout([]string{"-layouts", root, "-registry", host, "-tag", "pr-1", "-out", tsv}); code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", code, out)
	}
	if !strings.Contains(out.String(), file) {
		t.Errorf("the refusal does not name %s:\n%s", file, out)
	}
	if _, ok := reg.blobs["orkstack/activity-shell@"+layer.Digest]; ok {
		t.Error("the mismatching blob reached the registry")
	}
	if len(reg.tags) != 0 {
		t.Errorf("a tag was pushed: %v", reg.tags)
	}
	if _, err := os.Stat(tsv); !os.IsNotExist(err) {
		t.Errorf("a failed push wrote %s", tsv)
	}
}

func TestPushLayout_RefusesWhenTheRegistryAnswersAnotherDigest(t *testing.T) {
	out := plOutput(t)
	reg, host := newPLRegistry(t)
	reg.lie = "sha256:" + strings.Repeat("0", 64)
	root := t.TempDir()
	_, top, _ := plLayout(t, root, "shell", false)
	tsv := filepath.Join(t.TempDir(), "images.tsv")

	if code := cmdPushLayout([]string{"-layouts", root, "-registry", host, "-tag", "pr-1", "-out", tsv}); code != 1 {
		t.Fatalf("exit %d, want 1:\n%s", code, out)
	}
	if !strings.Contains(out.String(), "answers "+reg.lie) || !strings.Contains(out.String(), top.Digest) {
		t.Errorf("the refusal does not name both digests:\n%s", out)
	}
	if _, err := os.Stat(tsv); !os.IsNotExist(err) {
		t.Errorf("a failed push wrote %s", tsv)
	}
}

func TestPushLayout_PushesAnIndexAndItsChildren(t *testing.T) {
	out := plOutput(t)
	reg, host := newPLRegistry(t)
	root := t.TempDir()
	dir, top, _ := plLayout(t, root, "git", true)
	tsv := filepath.Join(t.TempDir(), "images.tsv")

	if code := cmdPushLayout([]string{"-layouts", root, "-registry", host, "-namespace", "ns", "-tag", "pr-2", "-out", tsv}); code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	var idx ociContent
	b, _ := os.ReadFile(filepath.Join(dir, "blobs", "sha256", strings.TrimPrefix(top.Digest, "sha256:")))
	if err := json.Unmarshal(b, &idx); err != nil || len(idx.Manifests) != 2 {
		t.Fatalf("index: %v, %d children", err, len(idx.Manifests))
	}
	want := []string{"ns/activity-git:" + idx.Manifests[0].Digest, "ns/activity-git:" + idx.Manifests[1].Digest, "ns/activity-git:pr-2"}
	if strings.Join(reg.puts, " ") != strings.Join(want, " ") {
		t.Errorf("manifest pushes = %v, want the children by digest, then the index by tag: %v", reg.puts, want)
	}
	if reg.tags["ns/activity-git:pr-2"] != top.Digest {
		t.Errorf("the tag resolves to %s, want the index %s", reg.tags["ns/activity-git:pr-2"], top.Digest)
	}
	if mt := reg.manifests["ns/activity-git@"+top.Digest].mediaType; mt != mediaOCIIndex {
		t.Errorf("the index was sent as %q", mt)
	}
	got, _ := os.ReadFile(tsv)
	if string(got) != "git\t"+host+"/ns/activity-git:pr-2\t"+top.Digest+"\n" {
		t.Errorf("images.tsv = %q", got)
	}
}

func TestLayoutDigests_ReadsTheTopDescriptor(t *testing.T) {
	out := plOutput(t)
	root := t.TempDir()
	_, shellTop, _ := plLayout(t, root, "shell", false)
	gitDir, gitTop, _ := plLayout(t, root, "git", true)
	tsv := filepath.Join(t.TempDir(), "images.tsv")

	// Nothing listens on the registry it names: the digests come from the
	// layouts alone.
	args := []string{"-layouts", root, "-registry", "127.0.0.1:1", "-tag", "pr-3", "-out", tsv}
	if code := cmdLayoutDigests(args); code != 0 {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	got, _ := os.ReadFile(tsv)
	want := "git\t127.0.0.1:1/orkstack/activity-git:pr-3\t" + gitTop.Digest + "\n" +
		"shell\t127.0.0.1:1/orkstack/activity-shell:pr-3\t" + shellTop.Digest + "\n"
	if string(got) != want {
		t.Errorf("images.tsv =\n%s\nwant\n%s", got, want)
	}

	// A top manifest that is not the bytes its descriptor names is refused.
	if err := os.WriteFile(filepath.Join(gitDir, "blobs", "sha256", strings.TrimPrefix(gitTop.Digest, "sha256:")), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := cmdLayoutDigests(args); code != 1 {
		t.Errorf("exit %d for a corrupted top manifest, want 1:\n%s", code, out)
	}

	// A directory with no layout in it names nothing: an invalid invocation.
	empty := t.TempDir()
	if code := cmdLayoutDigests([]string{"-layouts", empty, "-registry", "127.0.0.1:1", "-tag", "pr-3", "-out", tsv}); code != 2 {
		t.Errorf("exit %d for a directory with no layout, want 2", code)
	}
}
