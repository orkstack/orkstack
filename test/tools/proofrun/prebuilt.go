package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// compiledManifestName is the manifest `proofrun compile` writes beside the
// test binaries it built: what `proofrun integration` checks before it adopts
// them instead of compiling its own.
const compiledManifestName = "compiled.json"

// compiledManifest records one `proofrun compile`: the tree it compiled, and
// each package's binary, the binary's SHA-256 and its listing's.
type compiledManifest struct {
	Format   int               `json:"format"`
	Tree     string            `json:"tree"`
	Packages []compiledPackage `json:"packages"`
}

type compiledPackage struct {
	ImportPath    string `json:"import_path"`
	Binary        string `json:"binary"`
	BinarySHA256  string `json:"binary_sha256"`
	ListingSHA256 string `json:"listing_sha256"`
}

const compiledFormat = 1

// integrationPackageSet is the packages a run compiles: a selection's
// (chosen holds the tests of each package it names test by test, whole the
// packages it runs whole), or every integration package the patterns match.
// `proofrun compile` and `proofrun integration` both decide it here, so the
// binaries one builds are exactly the set the other plans.
func integrationPackageSet(sel *selection, patterns []string) (pkgs []pkgInfo, chosen map[string][]string, whole map[string]bool, err error) {
	whole = map[string]bool{}
	if sel == nil {
		pkgs, err = integrationPackages(patterns)
		return pkgs, nil, whole, err
	}
	chosen = map[string][]string{}
	var names []string
	for p, ps := range sel.Integration {
		if ps.Whole != "" {
			whole[p] = true
			names = append(names, p)
		} else if tests := ps.run(); len(tests) > 0 {
			chosen[p] = tests
			names = append(names, p)
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, chosen, whole, nil
	}
	dirs, err := packageDirs("integration", names)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, n := range names {
		pkgs = append(pkgs, pkgInfo{ImportPath: n, Dir: dirs[n]})
	}
	return pkgs, chosen, whole, nil
}

// cmdCompile is `proofrun compile`: compile and list the integration test
// binaries a later `proofrun integration -bin DIR` plans, so a proof can
// build them while its Temporal starts and its images push, rather than
// after. It writes compiled.json last, so a compile that failed leaves no
// manifest and the planner compiles for itself.
func cmdCompile(args []string) int {
	fs := flag.NewFlagSet("compile", flag.ContinueOnError)
	out := fs.String("out", "", "directory to compile the integration test binaries into, with their manifest compiled.json (required; proofrun integration's -bin)")
	selectFile := fs.String("select", "", "a feature selection (proofrun select): compile only the packages it runs, as proofrun integration -select plans them")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" {
		fmt.Fprintln(os.Stderr, "proofrun compile: -out is required")
		return 2
	}
	var sel *selection
	if *selectFile != "" {
		sel = &selection{}
		if err := readJSON(*selectFile, sel); err != nil {
			fmt.Fprintln(os.Stderr, "proofrun compile:", err)
			return 2
		}
	}
	patterns := fs.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	root, err := repoRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun compile:", err)
		return 2
	}
	tree, err := checkoutTree(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun compile:", err)
		return 2
	}
	pkgs, _, _, err := integrationPackageSet(sel, patterns)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proofrun compile:", err)
		return 2
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun compile:", err)
		return 1
	}
	if err := os.Remove(filepath.Join(*out, compiledManifestName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintln(os.Stderr, "proofrun compile:", err)
		return 1
	}
	started := time.Now()
	bins, err := compile(pkgs, *out, stdout)
	if err != nil {
		fmt.Fprintln(stdout, err)
		return 1
	}
	m := compiledManifest{Format: compiledFormat, Tree: tree, Packages: []compiledPackage{}}
	for _, p := range pkgs {
		names, err := list(bins[p.ImportPath], p.Dir)
		if err != nil {
			fmt.Fprintln(stdout, err)
			return 1
		}
		sha, err := fileSHA256(bins[p.ImportPath])
		if err != nil {
			fmt.Fprintln(stdout, err)
			return 1
		}
		m.Packages = append(m.Packages, compiledPackage{
			ImportPath: p.ImportPath, Binary: filepath.Base(bins[p.ImportPath]),
			BinarySHA256: sha, ListingSHA256: sha256String(strings.Join(names, "\n")),
		})
		fmt.Fprintf(stdout, "listed %s: %d tests, binary sha256 %s\n", p.ImportPath, len(names), sha)
	}
	if err := writeJSON(filepath.Join(*out, compiledManifestName), m); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun compile:", err)
		return 1
	}
	fmt.Fprintf(stdout, "compiled and listed %d integration packages in %.1fs -> %s\n", len(pkgs), time.Since(started).Seconds(), *out)
	return 0
}

// prebuiltBinaries adopts the binaries `proofrun compile` left in binDir. It
// returns nil, nil when binDir holds no manifest (compile as before), the
// binaries when the manifest names this working tree, exactly these
// packages, and binaries whose bytes and listings still hash as recorded, and
// an error for anything else: a manifest that does not match is never
// silently replaced by a fresh compile, nor silently trusted.
func prebuiltBinaries(binDir string, pkgs []pkgInfo) (map[string]string, error) {
	file := filepath.Join(binDir, compiledManifestName)
	var m compiledManifest
	if err := readJSON(file, &m); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	if m.Format != compiledFormat {
		return nil, fmt.Errorf("%s: format %d, want %d", file, m.Format, compiledFormat)
	}
	root, err := repoRoot()
	if err != nil {
		return nil, err
	}
	tree, err := checkoutTree(root)
	if err != nil {
		return nil, err
	}
	if m.Tree == "" || m.Tree != tree {
		return nil, fmt.Errorf("%s was compiled from tree %s, and this checkout is %s", file, orDash(m.Tree), tree)
	}
	byPath := map[string]compiledPackage{}
	for _, p := range m.Packages {
		byPath[p.ImportPath] = p
	}
	var want, have []string
	for _, p := range pkgs {
		want = append(want, p.ImportPath)
	}
	for p := range byPath {
		have = append(have, p)
	}
	sort.Strings(want)
	sort.Strings(have)
	if !slicesEqual(want, have) {
		missing, extra := listingDiff(want, have)
		return nil, fmt.Errorf("%s does not hold the packages this run plans: missing %s; extra %s", file, orDash(strings.Join(missing, ", ")), orDash(strings.Join(extra, ", ")))
	}
	bins := map[string]string{}
	for _, p := range pkgs {
		c := byPath[p.ImportPath]
		bin := filepath.Join(binDir, c.Binary)
		sha, err := fileSHA256(bin)
		if err != nil {
			return nil, fmt.Errorf("prebuilt binary of %s: %w", p.ImportPath, err)
		}
		if sha != c.BinarySHA256 {
			return nil, fmt.Errorf("prebuilt binary %s hashes to %s, and %s says %s", bin, sha, file, c.BinarySHA256)
		}
		names, err := list(bin, p.Dir)
		if err != nil {
			return nil, err
		}
		if l := sha256String(strings.Join(names, "\n")); l != c.ListingSHA256 {
			return nil, fmt.Errorf("prebuilt binary %s lists tests hashing to %s, and %s says %s", bin, l, file, c.ListingSHA256)
		}
		bins[p.ImportPath] = bin
	}
	return bins, nil
}
