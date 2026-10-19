package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// The activity images travel between a CI proof's parts as OCI image layouts,
// built once by the build part (make -C images oci). Every part that needs
// them pushes the layout's bytes, unchanged, into its own registry, so the
// digest every part resolves is the layout's by construction, and push-layout
// checks it anyway: the registry must answer the layout's own digest for the
// tag, or the push fails.

const (
	mediaOCIIndex      = "application/vnd.oci.image.index.v1+json"
	mediaOCIManifest   = "application/vnd.oci.image.manifest.v1+json"
	mediaDockerList    = "application/vnd.docker.distribution.manifest.list.v2+json"
	mediaDockerImage   = "application/vnd.docker.distribution.manifest.v2+json"
	layoutActivityGlob = "activity-*"
)

// registryManifestAccept is the Accept header of a manifest request: every
// manifest form a layout can hold, so the registry never converts one.
var registryManifestAccept = strings.Join([]string{mediaOCIIndex, mediaOCIManifest, mediaDockerList, mediaDockerImage}, ", ")

var (
	sha256Digest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	registryTag  = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]{0,127}$`)
	registryHost = regexp.MustCompile(`^[A-Za-z0-9.-]+(:[0-9]+)?$`)
)

// ociDescriptor is the part of an OCI content descriptor push-layout needs.
type ociDescriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
}

// ociContent is any manifest or index, read for the descriptors it names.
type ociContent struct {
	MediaType string          `json:"mediaType"`
	Config    *ociDescriptor  `json:"config"`
	Layers    []ociDescriptor `json:"layers"`
	Blobs     []ociDescriptor `json:"blobs"`
	Manifests []ociDescriptor `json:"manifests"`
}

// layoutImage is one activity's layout: DIR/activity-<Name>.
type layoutImage struct {
	Name string
	Dir  string
	Top  ociDescriptor
}

func isManifestMedia(mt string) bool {
	switch mt {
	case mediaOCIIndex, mediaOCIManifest, mediaDockerList, mediaDockerImage:
		return true
	}
	return false
}

// errNoLayouts is a -layouts directory that names nothing to push.
var errNoLayouts = errors.New("no layouts")

// readLayouts reads every DIR/activity-<name> layout, sorted by name. Each must
// be an OCI image layout whose index.json holds exactly one image.
func readLayouts(dir string) ([]layoutImage, error) {
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("%w: %s is not a directory", errNoLayouts, dir)
	}
	dirs, err := filepath.Glob(filepath.Join(dir, layoutActivityGlob))
	if err != nil {
		return nil, err
	}
	sort.Strings(dirs)
	var out []layoutImage
	for _, d := range dirs {
		if st, err := os.Stat(d); err != nil || !st.IsDir() {
			continue
		}
		if _, err := os.Stat(filepath.Join(d, "oci-layout")); err != nil {
			return nil, fmt.Errorf("%s is not an OCI image layout: %v", d, err)
		}
		var idx ociContent
		if err := readJSON(filepath.Join(d, "index.json"), &idx); err != nil {
			return nil, err
		}
		if len(idx.Manifests) != 1 {
			return nil, fmt.Errorf("%s/index.json holds %d images; a layout of one activity holds exactly one", d, len(idx.Manifests))
		}
		top := idx.Manifests[0]
		if !isManifestMedia(top.MediaType) {
			return nil, fmt.Errorf("%s/index.json names %s, which is not an image manifest or index", d, orDash(top.MediaType))
		}
		out = append(out, layoutImage{Name: strings.TrimPrefix(filepath.Base(d), "activity-"), Dir: d, Top: top})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: %s holds no %s directory", errNoLayouts, dir, layoutActivityGlob)
	}
	return out, nil
}

// layoutBlobPath is where a layout keeps the blob a descriptor names.
func layoutBlobPath(dir string, d ociDescriptor) (string, error) {
	if !sha256Digest.MatchString(d.Digest) {
		return "", fmt.Errorf("%s: digest %q is not a sha256 digest", dir, d.Digest)
	}
	return filepath.Join(dir, "blobs", "sha256", strings.TrimPrefix(d.Digest, "sha256:")), nil
}

// checkLayoutBlob hashes a layout's blob and refuses it unless its size and digest
// are the descriptor's: nothing is sent that is not exactly what was built.
func checkLayoutBlob(dir string, d ociDescriptor) (string, error) {
	p, err := layoutBlobPath(dir, d)
	if err != nil {
		return "", err
	}
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", err
	}
	if got := "sha256:" + hex.EncodeToString(h.Sum(nil)); got != d.Digest || n != d.Size {
		return "", fmt.Errorf("%s holds %d bytes hashing to %s; its descriptor says %d bytes, %s", p, n, got, d.Size, d.Digest)
	}
	return p, nil
}

// readLayoutBlob is checkLayoutBlob for a manifest, which is small and read whole.
func readLayoutBlob(dir string, d ociDescriptor) ([]byte, error) {
	p, err := layoutBlobPath(dir, d)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	if got := "sha256:" + hex.EncodeToString(sum[:]); got != d.Digest || int64(len(b)) != d.Size {
		return nil, fmt.Errorf("%s holds %d bytes hashing to %s; its descriptor says %d bytes, %s", p, len(b), got, d.Size, d.Digest)
	}
	return b, nil
}

// registryClient speaks the subset of the OCI distribution API a push needs,
// over plain HTTP: the proof's registries are local.
type registryClient struct {
	base   string
	client *http.Client
	pushed map[string]bool
}

func newRegistryClient(host string) *registryClient {
	return &registryClient{base: "http://" + host, client: &http.Client{Timeout: 10 * time.Minute}, pushed: map[string]bool{}}
}

func (c *registryClient) do(method, u string, header map[string]string, body io.Reader, size int64) (*http.Response, error) {
	req, err := http.NewRequest(method, u, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.ContentLength = size
	}
	for k, v := range header {
		req.Header.Set(k, v)
	}
	return c.client.Do(req)
}

func registryError(method, u string, resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("%s %s: %s %s", method, u, resp.Status, strings.TrimSpace(string(b)))
}

// pushBlob uploads one layout blob unless the repository already holds it:
// a monolithic upload, POST then PUT, the bytes checked before they are sent.
func (c *registryClient) pushBlob(repo, dir string, d ociDescriptor) error {
	key := repo + "@" + d.Digest
	if c.pushed[key] {
		return nil
	}
	p, err := checkLayoutBlob(dir, d)
	if err != nil {
		return err
	}
	head := c.base + "/v2/" + repo + "/blobs/" + d.Digest
	resp, err := c.do(http.MethodHead, head, nil, nil, 0)
	if err != nil {
		return err
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		c.pushed[key] = true
		return nil
	case http.StatusNotFound:
	default:
		return registryError(http.MethodHead, head, resp)
	}

	start := c.base + "/v2/" + repo + "/blobs/uploads/"
	resp, err = c.do(http.MethodPost, start, nil, nil, 0)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusAccepted {
		defer resp.Body.Close()
		return registryError(http.MethodPost, start, resp)
	}
	resp.Body.Close()
	loc := resp.Header.Get("Location")
	if loc == "" {
		return fmt.Errorf("POST %s: %s with no Location", start, resp.Status)
	}
	base, _ := url.Parse(start)
	rel, err := url.Parse(loc)
	if err != nil {
		return fmt.Errorf("POST %s: Location %q: %v", start, loc, err)
	}
	u := base.ResolveReference(rel)
	if u.RawQuery != "" {
		u.RawQuery += "&"
	}
	u.RawQuery += "digest=" + url.QueryEscape(d.Digest)

	f, err := os.Open(p)
	if err != nil {
		return err
	}
	defer f.Close()
	resp, err = c.do(http.MethodPut, u.String(), map[string]string{"Content-Type": "application/octet-stream"}, f, d.Size)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return registryError(http.MethodPut, u.String(), resp)
	}
	if got := resp.Header.Get("Docker-Content-Digest"); got != "" && got != d.Digest {
		return fmt.Errorf("PUT %s: the registry stored the blob as %s, not %s", u, got, d.Digest)
	}
	c.pushed[key] = true
	return nil
}

// pushContent pushes a manifest or an index and everything it names, children
// first, then the content itself under ref (its digest, or the tag for the top).
func (c *registryClient) pushContent(repo, dir string, d ociDescriptor, ref string) error {
	b, err := readLayoutBlob(dir, d)
	if err != nil {
		return err
	}
	var m ociContent
	if err := json.Unmarshal(b, &m); err != nil {
		return fmt.Errorf("%s: manifest %s: %v", dir, d.Digest, err)
	}
	for _, child := range m.Manifests {
		if isManifestMedia(child.MediaType) {
			if err := c.pushContent(repo, dir, child, child.Digest); err != nil {
				return err
			}
			continue
		}
		if err := c.pushBlob(repo, dir, child); err != nil {
			return err
		}
	}
	var blobs []ociDescriptor
	if m.Config != nil {
		blobs = append(blobs, *m.Config)
	}
	blobs = append(blobs, m.Layers...)
	blobs = append(blobs, m.Blobs...)
	for _, bd := range blobs {
		if err := c.pushBlob(repo, dir, bd); err != nil {
			return err
		}
	}
	mt := d.MediaType
	if mt == "" {
		mt = m.MediaType
	}
	if !isManifestMedia(mt) {
		return fmt.Errorf("%s: manifest %s has no manifest media type (%q)", dir, d.Digest, mt)
	}
	put := c.base + "/v2/" + repo + "/manifests/" + ref
	resp, err := c.do(http.MethodPut, put, map[string]string{"Content-Type": mt}, bytes.NewReader(b), int64(len(b)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return registryError(http.MethodPut, put, resp)
	}
	if got := resp.Header.Get("Docker-Content-Digest"); got != "" && got != d.Digest {
		return fmt.Errorf("PUT %s: the registry stored the manifest as %s; the layout's bytes are %s", put, got, d.Digest)
	}
	return nil
}

// manifestDigest asks the registry which digest a reference resolves to.
func (c *registryClient) manifestDigest(repo, ref string) (string, error) {
	u := c.base + "/v2/" + repo + "/manifests/" + ref
	resp, err := c.do(http.MethodHead, u, map[string]string{"Accept": registryManifestAccept}, nil, 0)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", registryError(http.MethodHead, u, resp)
	}
	d := resp.Header.Get("Docker-Content-Digest")
	if d == "" {
		return "", fmt.Errorf("HEAD %s: no Docker-Content-Digest", u)
	}
	return d, nil
}

// layoutFlags are push-layout's and layout-digests' common flags.
type layoutFlags struct {
	layouts, registry, namespace, tag, out string
}

func parseLayoutFlags(name string, args []string) (layoutFlags, bool) {
	var o layoutFlags
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.StringVar(&o.layouts, "layouts", "", "directory holding activity-<name>/ OCI image layouts (required)")
	fs.StringVar(&o.registry, "registry", "", "the registry, HOST:PORT, spoken to over plain HTTP (required)")
	fs.StringVar(&o.namespace, "namespace", "orkstack", "repository namespace: HOST:PORT/NS/activity-<name>")
	fs.StringVar(&o.tag, "tag", "", "the tag each image is pushed under (required)")
	fs.StringVar(&o.out, "out", "", "where to write name<TAB>reference<TAB>digest per image (required)")
	if err := fs.Parse(args); err != nil {
		return o, false
	}
	usage := "proofrun " + name + " -layouts DIR -registry HOST:PORT [-namespace NS] -tag TAG -out FILE"
	switch {
	case fs.NArg() > 0 || o.layouts == "" || o.registry == "" || o.tag == "" || o.out == "" || o.namespace == "":
		fmt.Fprintln(os.Stderr, usage)
	case !registryHost.MatchString(o.registry):
		fmt.Fprintf(os.Stderr, "proofrun %s: -registry %q must be HOST:PORT, with no scheme or path\n", name, o.registry)
	case !registryTag.MatchString(o.tag):
		fmt.Fprintf(os.Stderr, "proofrun %s: -tag %q is not a valid tag\n", name, o.tag)
	default:
		return o, true
	}
	return o, false
}

func (o layoutFlags) repo(img layoutImage) string {
	return o.namespace + "/activity-" + img.Name
}

func (o layoutFlags) reference(img layoutImage) string {
	return o.registry + "/" + o.repo(img) + ":" + o.tag
}

func (o layoutFlags) readLayouts(name string) ([]layoutImage, int) {
	imgs, err := readLayouts(o.layouts)
	if err != nil {
		fmt.Fprintf(stdout, "proofrun %s: %v\n", name, err)
		if errors.Is(err, errNoLayouts) {
			return nil, 2
		}
		return nil, 1
	}
	return imgs, 0
}

// writeImagesTSV writes the images.tsv format proof.json's image digests are
// read from: name, reference and digest, one image per line.
func writeImagesTSV(file string, o layoutFlags, imgs []layoutImage) error {
	var b strings.Builder
	for _, img := range imgs {
		fmt.Fprintf(&b, "%s\t%s\t%s\n", img.Name, o.reference(img), img.Top.Digest)
	}
	return os.WriteFile(file, []byte(b.String()), 0o644)
}

// cmdPushLayout is `proofrun push-layout`: push each activity's layout
// verbatim, then require the registry to answer the layout's digest for the
// tag. The images file is written only when every image passed.
func cmdPushLayout(args []string) int {
	o, ok := parseLayoutFlags("push-layout", args)
	if !ok {
		return 2
	}
	imgs, code := o.readLayouts("push-layout")
	if code != 0 {
		return code
	}
	c := newRegistryClient(o.registry)
	failed := false
	for _, img := range imgs {
		repo := o.repo(img)
		if err := c.pushContent(repo, img.Dir, img.Top, o.tag); err != nil {
			fmt.Fprintf(stdout, "push-layout: activity-%s: %v\n", img.Name, err)
			failed = true
			continue
		}
		got, err := c.manifestDigest(repo, o.tag)
		if err != nil {
			fmt.Fprintf(stdout, "push-layout: activity-%s: %v\n", img.Name, err)
			failed = true
			continue
		}
		if got != img.Top.Digest {
			fmt.Fprintf(stdout, "push-layout: activity-%s: the registry answers %s for %s; the layout's digest is %s\n",
				img.Name, got, o.reference(img), img.Top.Digest)
			failed = true
			continue
		}
		fmt.Fprintf(stdout, "pushed %s = %s (the layout's own digest)\n", o.reference(img), got)
	}
	if failed {
		return 1
	}
	if err := writeImagesTSV(o.out, o, imgs); err != nil {
		fmt.Fprintln(stdout, "push-layout:", err)
		return 1
	}
	return 0
}

// cmdLayoutDigests is `proofrun layout-digests`: the images file a push of
// these layouts would produce, read from the layouts alone, with each top
// manifest checked against its descriptor. The build part records it.
func cmdLayoutDigests(args []string) int {
	o, ok := parseLayoutFlags("layout-digests", args)
	if !ok {
		return 2
	}
	imgs, code := o.readLayouts("layout-digests")
	if code != 0 {
		return code
	}
	for _, img := range imgs {
		if _, err := readLayoutBlob(img.Dir, img.Top); err != nil {
			fmt.Fprintf(stdout, "layout-digests: activity-%s: %v\n", img.Name, err)
			return 1
		}
	}
	if err := writeImagesTSV(o.out, o, imgs); err != nil {
		fmt.Fprintln(stdout, "layout-digests:", err)
		return 1
	}
	return 0
}
