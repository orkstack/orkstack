package dist_test

import (
	"strings"
	"testing"

	"github.com/orkstack/orkstack/internal/dist"
	"github.com/orkstack/orkstack/internal/dsl"
)

func catalogAt(tag string) dsl.Catalog {
	catalog := dsl.Catalog{}
	for _, name := range []string{"shell", "http", "file"} {
		catalog[name] = "ghcr.io/orkstack/activity-" + name + ":" + tag
	}
	return catalog
}

func mustVersion(t *testing.T, s string) dist.Version {
	t.Helper()
	v, err := dist.ParseVersion(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestCheckCatalog: a release publishes the activity images under its
// MAJOR.MINOR, so the catalog compiled into that release must name that tag.
func TestCheckCatalog(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		version string
		catalog dsl.Catalog
		wantErr bool
	}{
		{"the same series", "v0.1.0", catalogAt("0.1"), false},
		{"a patch release stays in its series", "v0.1.7", catalogAt("0.1"), false},
		{"the next minor needs a new catalog", "v0.2.0", catalogAt("0.1"), true},
		{"a major needs a new catalog", "v1.0.0", catalogAt("0.1"), true},
		{"the catalog ahead of the release", "v0.1.3", catalogAt("0.2"), true},
		{"a major.minor that only looks the same", "v0.10.0", catalogAt("0.1"), true},
		{"and the other way round", "v0.1.0", catalogAt("0.10"), true},
		{"a full version is not a series", "v0.1.0", catalogAt("0.1.0"), true},
		{"a floating tag", "v0.1.0", catalogAt("latest"), true},
		{"a mixed catalog", "v0.1.0", dsl.Catalog{
			"shell": "ghcr.io/orkstack/activity-shell:0.1",
			"http":  "ghcr.io/orkstack/activity-http:0.2",
		}, true},
		{"a reference with no tag", "v0.1.0", dsl.Catalog{"shell": "ghcr.io/orkstack/activity-shell"}, true},
		{"a digest-pinned reference", "v0.1.0", dsl.Catalog{"shell": "ghcr.io/orkstack/activity-shell@sha256:" + strings.Repeat("a", 64)}, true},
		{"an empty catalog", "v0.1.0", dsl.Catalog{}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := dist.CheckCatalog(mustVersion(t, tc.version), tc.catalog)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CheckCatalog(%s) = %v, want an error: %v", tc.version, err, tc.wantErr)
			}
		})
	}
}

// TestCheckCatalog_MessageNamesTheFile: the refusal says where to change it,
// which tag it found and which the release needs.
func TestCheckCatalog_MessageNamesTheFile(t *testing.T) {
	t.Parallel()
	err := dist.CheckCatalog(mustVersion(t, "v0.2.0"), catalogAt("0.1"))
	if err == nil {
		t.Fatal("want a refusal")
	}
	for _, want := range []string{"internal/dsl/activity_catalog.go", "v0.2.0", "0.2", "0.1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message %q does not mention %q", err, want)
		}
	}
	err = dist.CheckCatalog(mustVersion(t, "v0.1.0"), dsl.Catalog{
		"shell": "ghcr.io/orkstack/activity-shell:0.1",
		"http":  "ghcr.io/orkstack/activity-http:0.2",
	})
	if err == nil || !strings.Contains(err.Error(), "internal/dsl/activity_catalog.go") ||
		!strings.Contains(err.Error(), "http") {
		t.Errorf("a mixed catalog's refusal %v should name the file and the entry", err)
	}
}

// TestBuiltinCatalogTag_IsTheCompiledInTag: the tag the guard reads is the one
// the engine resolves a short name to, so the two cannot drift, and a release
// of that series passes its own guard.
func TestBuiltinCatalogTag_IsTheCompiledInTag(t *testing.T) {
	t.Parallel()
	tag, err := dist.BuiltinCatalogTag()
	if err != nil {
		t.Fatal(err)
	}
	for name, ref := range dsl.BuiltinActivityCatalog() {
		if !strings.HasSuffix(ref, ":"+tag) {
			t.Errorf("catalog entry %s = %q does not end in :%s", name, ref, tag)
		}
	}
	version := mustVersion(t, "v"+tag+".0")
	if err := dist.CheckCatalog(version, dsl.BuiltinActivityCatalog()); err != nil {
		t.Errorf("a %s release is refused by its own catalog: %v", version, err)
	}
}
