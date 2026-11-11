package stablejson

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestModule_ImportsOnlyEncodingJSONv2 holds the module to encoding/json/v2
// and encoding/json/jsontext: no Go file imports encoding/json, whose decoder
// keeps the last of two values for one key and matches field names without
// regard to case (docs/JSON.md). The bytes encoding/json wrote are written
// with this package.
func TestModule_ImportsOnlyEncodingJSONv2(t *testing.T) {
	root := moduleRoot(t)
	files := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			name := entry.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "testdata" || name == "proof") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		files++
		for _, spec := range parsed.Imports {
			if imported, _ := strconv.Unquote(spec.Path.Value); imported == "encoding/json" {
				rel, _ := filepath.Rel(root, path)
				t.Errorf("%s imports encoding/json: use encoding/json/v2 (and internal/stablejson for bytes that are hashed, stored or read by a container)", rel)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The walk reached the module, not an empty directory.
	if files < 500 {
		t.Fatalf("walked %d Go files under %s; the module holds far more", files, root)
	}
}

// moduleRoot is the directory holding go.mod above this package.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the package")
		}
		dir = parent
	}
}
