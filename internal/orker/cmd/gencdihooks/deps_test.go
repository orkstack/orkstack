package main

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// moduleDeps is every module the packages given link, from `go list -deps`.
func moduleDeps(t *testing.T, pkgs ...string) map[string]bool {
	t.Helper()
	args := append([]string{"list", "-deps", "-f", "{{with .Module}}{{.Path}}{{end}}"}, pkgs...)
	out, err := exec.Command("go", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	mods := map[string]bool{}
	for _, m := range strings.Fields(string(out)) {
		mods[m] = true
	}
	return mods
}

// TestGeneratorDeps_StayOutOfTheReleaseBinaries: the modules gencdihooks
// links that this module's own code does not are derived, not listed, and no
// other main package of the module (ork, orker, orkctl, the examples and the
// tools) links any of them.
func TestGeneratorDeps_StayOutOfTheReleaseBinaries(t *testing.T) {
	if testing.Short() {
		t.Skip("go list -deps of every main package is slow; the full unit tier runs it")
	}
	out, err := exec.Command("go", "list", "-f", "{{if eq .Name \"main\"}}{{.ImportPath}}{{end}}", "github.com/orkstack/orkstack/...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list the main packages: %v\n%s", err, out)
	}
	self := "github.com/orkstack/orkstack/internal/orker/cmd/gencdihooks"
	var mains []string
	for _, p := range strings.Fields(string(out)) {
		if p != self {
			mains = append(mains, p)
		}
	}
	if !slices.Contains(mains, "github.com/orkstack/orkstack/cmd/orker") {
		t.Fatalf("the main packages %v do not include cmd/orker", mains)
	}
	gen, rest := moduleDeps(t, self), moduleDeps(t, mains...)
	var own []string
	for m := range gen {
		if !rest[m] {
			own = append(own, m)
		}
	}
	for _, want := range []string{"github.com/ProtonMail/go-crypto", "github.com/sassoftware/go-rpmutils", "pault.ag/go/debian"} {
		if !slices.Contains(own, want) {
			t.Errorf("%s is linked by another main package, or no longer by gencdihooks: own set %v", want, own)
		}
	}
}
