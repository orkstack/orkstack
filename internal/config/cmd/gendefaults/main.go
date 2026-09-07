// Command gendefaults prints config.RenderDefaults() to stdout: the
// generator behind "make config-defaults" (Makefile) and
// deploy/orkstack.defaults.yaml. `ork --defaults` / `orker --defaults`
// print the identical text at runtime (cmd/ork/main.go, cmd/orker/main.go)
// — this command exists only so the Makefile target need not build either
// full binary just to regenerate one file.
package main

import (
	"fmt"

	"github.com/orkstack/orkstack/internal/config"
)

func main() {
	fmt.Print(config.RenderDefaults())
}
