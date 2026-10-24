// Package localcatalog builds the ORK_ACTIVITY_CATALOG document that binds the
// built-in activity names to the local registry `make infra-up` starts.
//
// It exists because apply-time image resolution (§4.3 of
// docs/OCI_ACTIVITY_CONTRACT.md) runs in the engine against the catalog it was
// booted with, and the BUILT-IN catalog names the published
// `ghcr.io/orkstack/activity-*` references a real deployment must resolve.
// Those images are not published yet, so an integration test that applies
// `uses: shell` through the default catalog is refused with
// IMAGE_RESOLVE_UNAUTHORIZED. That refusal is correct — an unresolvable
// reference must fail at apply — so the tests move to references that exist
// rather than the default weakening to a laptop-only one. §4.6 prescribes
// exactly this shape for local development: `make -C images push` pushes each activity
// to `localhost:5001/orkstack/activity-<name>:dev` and the local stack points
// ORK_ACTIVITY_CATALOG at that registry. A proof pushes under its own tag
// instead, and ORK_TEST_IMAGE_TAG names it (Tag).
//
// It is a package rather than a test helper because two commands' integration
// suites (cmd/ork and cmd/orkctl) start engines and both need it; one copy is
// the point.
package localcatalog

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// EnvVar is the registry override an operator or a proof script may set.
const EnvVar = "ORK_TEST_REGISTRY"

// DefaultRegistry is the registry deploy/docker-compose.yml publishes.
const DefaultRegistry = "localhost:5001"

// TagVar names the tag the first-party activity images were pushed under. A
// proof pushes them ONCE, from the tree it proves, under a tag of its own
// (`pr-<hex>`, scripts/proof.sh), so two proofs never move each other's
// images and the images under test are this tree's; tests only resolve it.
const TagVar = "ORK_TEST_IMAGE_TAG"

// DefaultTag is what `make -C images push` pushes outside a proof.
const DefaultTag = "dev"

// Names is the built-in catalog's key set (§4.2).
var Names = []string{"shell", "http", "file", "json", "git", "template"}

// Registry is the registry these tests resolve against.
func Registry() string {
	if r := os.Getenv(EnvVar); r != "" {
		return r
	}
	return DefaultRegistry
}

// Tag is the tag these tests resolve the first-party images under.
func Tag() string {
	if tag := os.Getenv(TagVar); tag != "" {
		return tag
	}
	return DefaultTag
}

// Reference spells one first-party image in the local registry.
func Reference(name string) string {
	return fmt.Sprintf("%s/orkstack/activity-%s:%s", Registry(), name, Tag())
}

var manifestAccept = strings.Join([]string{
	"application/vnd.oci.image.index.v1+json",
	"application/vnd.oci.image.manifest.v1+json",
	"application/vnd.docker.distribution.manifest.list.v2+json",
	"application/vnd.docker.distribution.manifest.v2+json",
}, ", ")

// CatalogEnv returns the `ORK_ACTIVITY_CATALOG=...` assignment for the local
// registry, after proving every manifest is actually there.
//
// The error is a MISSING PREREQUISITE, not a defect: a caller maps it onto its
// package's infraAbsent, so a machine with no registry skips and a machine
// running under ORK_REQUIRE_INFRA fails. It never returns a catalog naming an
// image it did not see.
func CatalogEnv() (string, error) {
	registry, tag := Registry(), Tag()
	client := &http.Client{Timeout: 5 * time.Second}
	lines := make([]string, 0, len(Names))
	for _, name := range Names {
		reference := fmt.Sprintf("%s/orkstack/activity-%s:%s", registry, name, tag)
		url := fmt.Sprintf("http://%s/v2/orkstack/activity-%s/manifests/%s", registry, name, tag)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return "", fmt.Errorf("build manifest request for %s: %w", reference, err)
		}
		req.Header.Set("Accept", manifestAccept)
		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("local registry %s is not reachable (%w); "+
				"run `make infra-up && make -C images push`", registry, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("activity image %s is not in the local registry "+
				"(manifest GET returned %d); run `make -C images push`", reference, resp.StatusCode)
		}
		lines = append(lines, fmt.Sprintf("  %s: %s", name, reference))
	}
	return "ORK_ACTIVITY_CATALOG=activities:\n" + strings.Join(lines, "\n") + "\n", nil
}
