package dist

import (
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"github.com/orkstack/orkstack/internal/dsl"
)

// ImageNamespace is the registry namespace every release image is published
// under: ghcr.io/orkstack/<name>.
const ImageNamespace = "orkstack"

// ActivityPrefix is what the layout and repository of a first-party activity
// image start with: the catalog's "shell" is the image "activity-shell".
const ActivityPrefix = "activity-"

// serviceImages are the three images of the platform itself. The web UI has
// none of its own: ork serves it, built into the binary (internal/webui).
var serviceImages = []string{Ork, Orker, Orkctl}

// activityNames are the catalog names of the first-party activities, in the
// order the quickstart's compose file (compose.yaml) pushes them.
var activityNames = []string{"shell", "http", "file", "json", "git", "template"}

// ServiceImages returns the platform's images: ork, orker and orkctl. The
// caller owns the returned slice.
func ServiceImages() []string { return slices.Clone(serviceImages) }

// ActivityNames returns the catalog names of the first-party activities, each
// published as the image ActivityPrefix+name. The caller owns the returned
// slice.
func ActivityNames() []string { return slices.Clone(activityNames) }

// Images returns every image a release publishes, sorted: the one list the
// layouts and the digests are checked against. The caller owns the returned
// slice.
func Images() []string {
	images := slices.Clone(serviceImages)
	for _, name := range activityNames {
		images = append(images, ActivityPrefix+name)
	}
	sort.Strings(images)
	return images
}

// Image is a published image and the digest its OCI layout's index names: the
// digest `skopeo copy --all --preserve-digests` pushes the layout under, so
// the digest a registry reports for the pushed reference.
type Image struct {
	Name   string
	Digest string
}

// layoutIndex is the part of an OCI layout's index.json that names the image.
type layoutIndex struct {
	Manifests []struct {
		Digest string `json:"digest"`
	} `json:"manifests"`
}

// layoutDigest reads the digest of the one manifest an image layout holds.
func layoutDigest(layout string) (string, error) {
	const file = "index.json"
	data, err := os.ReadFile(filepath.Join(layout, file))
	if err != nil {
		return "", fmt.Errorf("read %s: %w", file, err)
	}
	var index layoutIndex
	if err := json.Unmarshal(data, &index); err != nil {
		return "", fmt.Errorf("%s is not an OCI image index: %w", file, err)
	}
	if len(index.Manifests) != 1 {
		return "", fmt.Errorf("%s names %d manifests, want exactly 1: the image to push, whose digest the registry keeps",
			file, len(index.Manifests))
	}
	digest := index.Manifests[0].Digest
	if !dsl.ValidDigest(digest) {
		return "", fmt.Errorf("%s names the digest %q, which is not sha256:<64 lowercase hex>", file, digest)
	}
	return digest, nil
}

// ReadLayouts reads the digest of every image layout in dir: one OCI image
// layout per subdirectory, named after its image, as `make release-images`
// writes them. The result is sorted by name. It refuses a directory that holds
// anything but exactly the images a release publishes, and a layout whose
// index.json does not name exactly one manifest by a well-formed digest.
func ReadLayouts(dir string) ([]Image, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read the layouts directory: %w", err)
	}
	var unexpected []string
	have := map[string]bool{}
	for _, entry := range entries {
		switch {
		case !entry.IsDir():
			return nil, fmt.Errorf("%s: %s is not a directory; the layouts directory holds one OCI image layout "+
				"per image and nothing else", dir, entry.Name())
		case !slices.Contains(Images(), entry.Name()):
			unexpected = append(unexpected, entry.Name())
		default:
			have[entry.Name()] = true
		}
	}
	var missing []string
	for _, name := range Images() {
		if !have[name] {
			missing = append(missing, name)
		}
	}
	if len(unexpected) > 0 || len(missing) > 0 {
		var problems []string
		if len(unexpected) > 0 {
			problems = append(problems, "unexpected layout(s) "+strings.Join(unexpected, ", "))
		}
		if len(missing) > 0 {
			problems = append(problems, "missing layout(s) "+strings.Join(missing, ", "))
		}
		return nil, fmt.Errorf("%s: %s; a release publishes exactly %s", dir, strings.Join(problems, "; "), strings.Join(Images(), ", "))
	}
	images := make([]Image, 0, len(entries))
	for _, name := range Images() {
		digest, err := layoutDigest(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		images = append(images, Image{Name: name, Digest: digest})
	}
	return images, nil
}

// FormatImages writes images as the list `dist digests` prints, which the
// release smoke (scripts/quickstart-smoke.sh) and release.yml read: one
// "<name><TAB><digest>" line per image, sorted by name.
func FormatImages(images []Image) string {
	sorted := slices.Clone(images)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var out strings.Builder
	for _, image := range sorted {
		out.WriteString(image.Name + "\t" + image.Digest + "\n")
	}
	return out.String()
}

// ParseImages reads the list FormatImages writes back: one
// "<name><TAB><digest>" line per image, in any order, each digest
// well-formed. It refuses a list that is not exactly the images a release
// publishes, each once, naming the line or the image at fault.
func ParseImages(list []byte) ([]Image, error) {
	text := strings.TrimSuffix(strings.ReplaceAll(string(list), "\r\n", "\n"), "\n")
	if text == "" {
		return nil, fmt.Errorf("the image list names no image; a release publishes %s", strings.Join(Images(), ", "))
	}
	var images []Image
	seen := map[string]bool{}
	for i, line := range strings.Split(text, "\n") {
		name, digest, ok := strings.Cut(line, "\t")
		switch {
		case !ok || strings.Contains(digest, "\t"):
			return nil, fmt.Errorf("image list line %d is %q, not <image><TAB><digest>", i+1, line)
		case !slices.Contains(Images(), name):
			return nil, fmt.Errorf("image list line %d names %q, which a release does not publish; it publishes %s",
				i+1, name, strings.Join(Images(), ", "))
		case seen[name]:
			return nil, fmt.Errorf("image list line %d names %s a second time", i+1, name)
		case !dsl.ValidDigest(digest):
			return nil, fmt.Errorf("image list line %d gives %s the digest %q, which is not sha256:<64 lowercase hex>", i+1, name, digest)
		}
		seen[name] = true
		images = append(images, Image{Name: name, Digest: digest})
	}
	var missing []string
	for _, name := range Images() {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("the image list names no digest for %s; a release publishes %s",
			strings.Join(missing, ", "), strings.Join(Images(), ", "))
	}
	return images, nil
}
