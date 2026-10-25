package dist

import (
	"fmt"
	"sort"
	"strings"

	"github.com/orkstack/orkstack/internal/dsl"
)

// CatalogFile is where the built-in activity catalog is written, and so what
// a refusal tells the person who bumps a version to edit.
const CatalogFile = "internal/dsl/activity_catalog.go"

// BuiltinCatalogTag is the one tag every reference of the engine's built-in
// activity catalog carries, such as "0.1". It is read from the catalog the
// engine compiles in, not from a copy.
func BuiltinCatalogTag() (string, error) {
	return CatalogTag(dsl.BuiltinActivityCatalog())
}

// CatalogTag returns the tag shared by every reference in catalog. It refuses
// an empty catalog, a reference without a tag (a digest-only reference is
// pinned, not tagged) and a catalog whose references disagree.
func CatalogTag(catalog dsl.Catalog) (string, error) {
	if len(catalog) == 0 {
		return "", fmt.Errorf("the built-in activity catalog in %s is empty", CatalogFile)
	}
	tags := map[string][]string{}
	for _, name := range catalog.Names() {
		tag := referenceTag(catalog[name])
		if tag == "" {
			return "", fmt.Errorf("the built-in activity catalog in %s has no tag for %s (%q)", CatalogFile, name, catalog[name])
		}
		tags[tag] = append(tags[tag], name)
	}
	if len(tags) > 1 {
		var parts []string
		for tag, names := range tags {
			parts = append(parts, fmt.Sprintf(":%s for %s", tag, strings.Join(names, ", ")))
		}
		sort.Strings(parts)
		return "", fmt.Errorf("the built-in activity catalog in %s mixes tags (%s); a release publishes one",
			CatalogFile, strings.Join(parts, "; "))
	}
	// One tag is shared, so any entry names it.
	return referenceTag(catalog[catalog.Names()[0]]), nil
}

// referenceTag is the tag of an image reference, or "" when it has none: the
// part after the last colon of the final path element, ahead of any digest.
func referenceTag(reference string) string {
	last := reference
	if slash := strings.LastIndex(reference, "/"); slash >= 0 {
		last = reference[slash+1:]
	}
	if at := strings.Index(last, "@"); at >= 0 {
		last = last[:at]
	}
	if colon := strings.LastIndex(last, ":"); colon >= 0 {
		return last[colon+1:]
	}
	return ""
}

// CheckCatalog refuses a release whose MAJOR.MINOR is not the tag of the
// built-in activity catalog. A release publishes the activity images as
// :MAJOR.MINOR, and the engine resolves a short name such as `shell` to
// ghcr.io/orkstack/activity-shell:<catalog tag>; a release whose catalog names
// another tag would run, and then fail to pull the images it names.
func CheckCatalog(version Version, catalog dsl.Catalog) error {
	tag, err := CatalogTag(catalog)
	if err != nil {
		return err
	}
	if series := version.Series(); tag != series {
		return fmt.Errorf("release %s publishes the activity images as :%s, but the built-in activity catalog "+
			"(%s) resolves them as :%s, so this release would pull images it never published. "+
			"Change the catalog's tag to %s, and every document that names the old one "+
			"(go test ./internal/docscheck -run NameTheBuiltinCatalogTag lists them), "+
			"in the change that moves the series",
			version, series, CatalogFile, tag, series)
	}
	return nil
}
