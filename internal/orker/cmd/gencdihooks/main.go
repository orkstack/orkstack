// Command gencdihooks writes internal/orker/cdi_hook_table.go: every build of
// NVIDIA's nvidia-cdi-hook that an Orker may find behind an nvidia.com CDI
// spec's hook (design E91 §4.10 item 4). It reads NVIDIA's own package
// repository and trusts nothing it serves until its signature is checked:
//
//   - the repository's signing key, whose primary fingerprint is pinned here;
//   - for deb, each arch's clearsigned InRelease, the SHA-512 it gives of
//     Packages, and the SHA-256 Packages gives of each package;
//   - for rpm, each arch's repomd.xml and its detached signature, the
//     SHA-512 it gives of primary.xml.gz, and the SHA-512 that gives of each
//     package (NVIDIA's rpms are not signed themselves, only the metadata).
//
// For each nvidia-container-toolkit-base release at or above the floor, per
// arch and per packaging, it downloads the package, checks it against the
// signed digest, extracts /usr/bin/nvidia-cdi-hook and records the binary's
// SHA-256. deb and rpm builds of one release differ, so each is a row.
//
// Run it from the repository root, with network:
//
//	go run ./internal/orker/cmd/gencdihooks          writes the table
//	go run ./internal/orker/cmd/gencdihooks -check   fails when the table differs
//
// -check is for the nightly's cdi-hook-table job, which P4b's workflow change
// adds (design §4.10 item 4, E91-N1): a release NVIDIA published at or above
// the floor that the table lacks, or any row that no longer derives, fails
// it. Until that job runs, a new release reaches the table only when someone
// runs this command. The table is never edited by hand.
//
// Freshness: the generator trusts what the signed metadata lists, so a mirror
// replaying an older, validly signed index hides newer releases (it can add
// no row). The InRelease Date is printed on every run so a stale index shows.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Repository is NVIDIA's package repository for the Container Toolkit.
const Repository = "https://nvidia.github.io/libnvidia-container"

// SigningKeyFingerprint is the primary fingerprint of the key NVIDIA signs
// that repository's metadata with, as keyserver.ubuntu.com and NVIDIA's own
// NVFlare documentation print it. A key the repository serves with another
// fingerprint is refused.
const SigningKeyFingerprint = "C95B321B61E88C1809C4F759DDCAE044F796ECB0"

// Floor is the first release that fixes every published advisory affecting
// CDI-mode hooks (design §4.10 item 4; E91-Q4a): 1.19.1 fixes CVE-2026-24260
// in the create-symlinks and cuda-compat hooks, after 1.17.8's
// CVE-2025-23266 and CVE-2025-23267. Raising it is a reviewed change to this
// constant, and every row below it then goes.
const Floor = "1.19.1"

// PackageName is the package that ships nvidia-cdi-hook.
const PackageName = "nvidia-container-toolkit-base"

// HookPath is where every package installs the hook.
const HookPath = "usr/bin/nvidia-cdi-hook"

// HookPathBase is the hook's file name.
const HookPathBase = "nvidia-cdi-hook"

// TablePath is the file this command writes, from the repository root.
const TablePath = "internal/orker/cdi_hook_table.go"

func main() {
	check := flag.Bool("check", false, "compare the derived table with "+TablePath+" and fail on any difference, writing nothing")
	fetch := flag.String("fetch", "", "fetch one build, `VERSION`, instead of deriving the table: its nvidia-cdi-hook (and any -extra file) is written to -out after the signed chain verifies")
	arch := flag.String("arch", runtime.GOARCH, "with -fetch: amd64 or arm64")
	packaging := flag.String("packaging", "deb", "with -fetch: deb or rpm")
	out := flag.String("out", "", "with -fetch: a directory that does not exist yet; files are written there by base name")
	extra := flag.String("extra", "", "with -fetch: comma-separated further package paths to extract, such as usr/bin/nvidia-ctk")
	flag.Parse()
	if *fetch != "" {
		if *check {
			fmt.Fprintln(os.Stderr, "gencdihooks: -fetch and -check are different modes; use one")
			os.Exit(2)
		}
		var more []string
		if *extra != "" {
			more = strings.Split(*extra, ",")
		}
		if err := runFetch(*fetch, *arch, *packaging, *out, more); err != nil {
			fmt.Fprintln(os.Stderr, "gencdihooks:", err)
			// A repository that could not be reached is a missing
			// prerequisite (77), as the proof's own exit codes say; a file
			// that failed its signature or digest is a failure.
			if errors.As(err, new(*networkError)) {
				os.Exit(77)
			}
			os.Exit(1)
		}
		return
	}
	if err := run(*check); err != nil {
		fmt.Fprintln(os.Stderr, "gencdihooks:", err)
		os.Exit(1)
	}
}

// run derives the table and writes it, or, with check, compares it.
func run(check bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	src := &repository{base: Repository, client: &http.Client{Timeout: 5 * time.Minute, CheckRedirect: sameHostHTTPS}}
	rows, err := deriveRows(ctx, src, Floor, SigningKeyFingerprint)
	if err != nil {
		return err
	}
	table, err := render(rows, Floor)
	if err != nil {
		return err
	}
	if check {
		have, err := os.ReadFile(TablePath)
		if err != nil {
			return err
		}
		if !bytes.Equal(have, table) {
			return fmt.Errorf("%s differs from what NVIDIA's signed repository derives (%d rows); "+
				"regenerate it with `go run ./internal/orker/cmd/gencdihooks` and review the new rows with their fixtures",
				TablePath, len(rows))
		}
		fmt.Printf("gencdihooks: %s matches the repository (%d rows)\n", TablePath, len(rows))
		return nil
	}
	// Written beside the table and renamed over it, so a failed write leaves
	// the old table whole.
	tmp, err := os.CreateTemp(filepath.Dir(TablePath), ".cdi_hook_table-*.go")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(table); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), TablePath); err != nil {
		return err
	}
	fmt.Printf("gencdihooks: wrote %s (%d rows)\n", TablePath, len(rows))
	return nil
}

// runFetch writes one build's hook, and the extra files, into out. It is the
// proof host's way to a real nvidia-cdi-hook for the dind tests and a test's
// way to an nvidia-ctk for fixtures: nothing it writes was not vouched for by
// the pinned key's signed metadata (fetchBuild), and the caller holds the
// hook to the table's row by digest (the audit's own check).
func runFetch(ver, arch, packaging, out string, extra []string) error {
	v, err := parseVersion(ver)
	if err != nil {
		return err
	}
	if out == "" {
		return fmt.Errorf("-fetch needs -out, a directory that does not exist yet")
	}
	want := []string{HookPath}
	bases := map[string]bool{HookPathBase: true}
	for _, e := range extra {
		e = strings.TrimPrefix(strings.TrimSpace(e), "/")
		if e == "" || e == HookPath || bases[filepath.Base(e)] {
			return fmt.Errorf("-extra %q: name another file of the package, whose base name no other file written takes", e)
		}
		bases[filepath.Base(e)] = true
		want = append(want, e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	src := &repository{base: Repository, client: &http.Client{Timeout: 5 * time.Minute, CheckRedirect: sameHostHTTPS}}
	files, err := fetchBuild(ctx, src, SigningKeyFingerprint, v, arch, packaging, want)
	if err != nil {
		return err
	}
	// Mkdir, not MkdirAll: an existing directory may hold a planted file.
	if err := os.Mkdir(out, 0o755); err != nil {
		return err
	}
	for _, p := range want {
		name := filepath.Join(out, filepath.Base(p))
		if err := os.WriteFile(name, files[p], 0o755); err != nil {
			return err
		}
		fmt.Printf("gencdihooks: wrote %s (%d bytes)\n", name, len(files[p]))
	}
	return nil
}

// errRedirect is a redirect sameHostHTTPS refuses.
var errRedirect = errors.New("refused")

// sameHostHTTPS refuses a redirect off https or to another host: every file
// is checked against signed digests anyway, so this only keeps a run from
// being sent elsewhere.
func sameHostHTTPS(req *http.Request, via []*http.Request) error {
	if len(via) >= 5 {
		return fmt.Errorf("%w: more than 5 redirects", errRedirect)
	}
	if req.URL.Scheme != "https" || req.URL.Host != via[0].URL.Host {
		return fmt.Errorf("%w: a redirect to %s leaves %s over https", errRedirect, req.URL.Redacted(), via[0].URL.Host)
	}
	return nil
}

// repository reads files under base.
type repository struct {
	base   string
	client *http.Client
}

// maxFile bounds every file read from the repository: the largest package is
// about 5 MiB, and metadata is far smaller.
const maxFile = 64 << 20

// networkError is a request the repository could not serve: the connection
// or its timeout, a 5xx or a 429, or a body that did not arrive. A certificate
// nobody trusts and a refused redirect are not.
type networkError struct{ err error }

func (e *networkError) Error() string { return e.err.Error() }
func (e *networkError) Unwrap() error { return e.err }

// get is the file at rel under the repository, or why not.
func (r *repository) get(ctx context.Context, rel string) ([]byte, error) {
	url := r.base + "/" + rel
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		err = fmt.Errorf("GET %s: %w", url, err)
		// The host answered: with a certificate nobody trusts, or a redirect
		// that is not followed.
		if errors.As(err, new(*tls.CertificateVerificationError)) || errors.Is(err, errRedirect) {
			return nil, err
		}
		return nil, &networkError{err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("GET %s: %s", url, resp.Status)
		// Only a repository that cannot serve now (a server error, or a
		// refusal to serve this often) is a missing prerequisite. A 404 or 403
		// for a file the signed metadata names is the repository contradicting
		// its own signed index, which is a failure of what is being proven.
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return nil, &networkError{err}
		}
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFile+1))
	if err != nil {
		return nil, &networkError{fmt.Errorf("GET %s: %w", url, err)}
	}
	if len(body) > maxFile {
		return nil, fmt.Errorf("GET %s: larger than %d bytes", url, maxFile)
	}
	return body, nil
}
