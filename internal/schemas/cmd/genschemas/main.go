// Command genschemas writes the JSON Schemas of the authored documents to
// docs/schemas/, and the two kinds' schemas again to the UI's copy in
// ui/src/lib/schemas/ (internal/schemas). Run it from the repository root:
//
//	go run ./internal/schemas/cmd/genschemas
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/orkstack/orkstack/internal/schemas"
)

func main() {
	if err := generate(".", ".", os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "genschemas:", err)
		os.Exit(1)
	}
}

// generate writes every schema generated from the tree at root under dest,
// printing each path it wrote.
func generate(root, dest string, out io.Writer) error {
	files, err := schemas.Files(root)
	if err != nil {
		return err
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		target := filepath.Join(dest, path)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(target, files[path], 0o644); err != nil {
			return err
		}
		fmt.Fprintln(out, path)
	}
	return nil
}
