package dsl_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/dsl"
)

const preHeaderDir = "testdata/pre-header"

// preHeaderSource is how a pre-header vector is compiled: as it was written.
func preHeaderSource(t *testing.T, raw []byte) []byte {
	t.Helper()
	return raw
}

// TestCompile_PreHeaderVectorsKeepTheirDigests compiles documents written
// before the header existed and holds each to the digest, version and
// canonical form recorded at the base compiler, and to the compiled message
// recorded then (source_yaml aside). The vectors are pre-header documents by
// design and are never given a header in the repository.
// Regenerate with: go test ./internal/dsl -run '^TestCompile_PreHeaderVectorsKeepTheirDigests$' -update
func TestCompile_PreHeaderVectorsKeepTheirDigests(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(preHeaderDir, "*.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob vectors: %v (%d found)", err, len(paths))
	}
	sort.Strings(paths)
	var got strings.Builder
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		wf, err := dsl.Compile(preHeaderSource(t, raw))
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		digest, err := dsl.DefinitionDigest(wf)
		if err != nil {
			t.Fatal(err)
		}
		canonical, err := dsl.CanonicalOrkflow(wf)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(canonical)
		fmt.Fprintf(&got, "%s %s %s %s\n", filepath.Base(path), digest, wf.GetVersion(), hex.EncodeToString(sum[:]))

		bare := proto.Clone(wf).(*orkstackv1.Orkflow)
		bare.SourceYaml = ""
		compiledPath := strings.TrimSuffix(path, ".yaml") + ".compiled.json"
		if *updateCompat {
			out, err := protojson.MarshalOptions{Multiline: true}.Marshal(bare)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(compiledPath, append(out, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		recorded, err := os.ReadFile(compiledPath)
		if err != nil {
			t.Fatalf("read %s: %v (record it with -update)", compiledPath, err)
		}
		var want orkstackv1.Orkflow
		if err := protojson.Unmarshal(recorded, &want); err != nil {
			t.Fatalf("%s: %v", compiledPath, err)
		}
		if !proto.Equal(&want, bare) {
			t.Errorf("%s: the compiled message moved:\n got %v\nwant %v", path, bare, &want)
		}
	}
	golden := filepath.Join(preHeaderDir, "digests.golden")
	if *updateCompat {
		if err := os.WriteFile(golden, []byte(got.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read %s: %v (record it with -update)", golden, err)
	}
	if got.String() != string(want) {
		t.Fatalf("pre-header digests moved:\n got:\n%s\nwant:\n%s", got.String(), want)
	}
}
