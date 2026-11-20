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
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

// TablePath is the file this command writes, from the repository root.
const TablePath = "internal/orker/cdi_hook_table.go"

func main() {
	check := flag.Bool("check", false, "compare the derived table with "+TablePath+" and fail on any difference, writing nothing")
	flag.Parse()
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

// sameHostHTTPS refuses a redirect off https or to another host: every file
// is checked against signed digests anyway, so this only keeps a run from
// being sent elsewhere.
func sameHostHTTPS(req *http.Request, via []*http.Request) error {
	if len(via) >= 5 {
		return fmt.Errorf("more than 5 redirects")
	}
	if req.URL.Scheme != "https" || req.URL.Host != via[0].URL.Host {
		return fmt.Errorf("a redirect to %s leaves %s over https", req.URL.Redacted(), via[0].URL.Host)
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

// get is the file at rel under the repository, or why not.
func (r *repository) get(ctx context.Context, rel string) ([]byte, error) {
	url := r.base + "/" + rel
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFile+1))
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	if len(body) > maxFile {
		return nil, fmt.Errorf("GET %s: larger than %d bytes", url, maxFile)
	}
	return body, nil
}
