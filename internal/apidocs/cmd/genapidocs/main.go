// Command genapidocs writes docs/API.md, the client-facing API reference,
// from the proto sources (internal/apidocs). Run it from the repository
// root:
//
//	go run ./internal/apidocs/cmd/genapidocs
package main

import (
	"fmt"
	"os"

	"github.com/orkstack/orkstack/internal/apidocs"
)

func main() {
	doc, err := apidocs.Render(".")
	if err != nil {
		fmt.Fprintln(os.Stderr, "genapidocs:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(apidocs.Path, append(doc, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "genapidocs:", err)
		os.Exit(1)
	}
	fmt.Println(apidocs.Path)
}
