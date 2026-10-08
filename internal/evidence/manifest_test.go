package evidence

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata goldens")

func TestManifest_JSONKeysAndDialectAreTheContract(t *testing.T) {
	f := newFixture()
	got, err := f.manifest.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	golden := filepath.Join("testdata", "manifest.golden.json")
	if *updateGolden {
		if err := os.WriteFile(golden, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run with -update to create): %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("MANIFEST.json differs from %s:\n%s", golden, got)
	}
	text := string(got)
	for _, key := range []string{`"schema_version": 1`, `"run_status": "SUCCEEDED"`, `"definition_digest": "sha256:9f2c`,
		`"bytes_present"`, `"size_bytes"`, `"outcome_attempt"`, `"stored_definition_digest"`} {
		if !strings.Contains(text, key) {
			t.Errorf("manifest lacks %s", key)
		}
	}
	if strings.Contains(text, "STATUS_") || strings.Contains(text, "ATTEMPT_OUTCOME_SUCCEEDED\"\n") {
		t.Error("enum values must use the short dialect")
	}
	if strings.Count(text, `"sha256": "sha256:`) != 0 {
		t.Error("a file digest must carry no sha256: prefix")
	}
	if !strings.HasSuffix(text, "}\n") {
		t.Error("MANIFEST.json must end with a newline")
	}

	parsed, err := ParseManifest(got)
	if err != nil {
		t.Fatalf("ParseManifest: %v", err)
	}
	if !reflect.DeepEqual(parsed, f.manifest) {
		t.Fatalf("ParseManifest does not round-trip:\n got %+v\nwant %+v", parsed, f.manifest)
	}
	fromBundle, err := ReadManifest(bytes.NewReader(writeFixture(t, f)))
	if err != nil {
		t.Fatalf("ReadManifest: %v", err)
	}
	if !reflect.DeepEqual(fromBundle, f.manifest) {
		t.Fatal("ReadManifest of a written bundle does not round-trip")
	}

	if _, err := ParseManifest(bytes.Replace(got, []byte(`"schema_version": 1`), []byte(`"schema_version": 2`), 1)); err == nil ||
		!strings.Contains(err.Error(), "schema_version 2") {
		t.Fatalf("schema_version 2 = %v, want a refusal naming the version", err)
	}
}
