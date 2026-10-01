// Command cosign-sign writes and removes cosign key-based signatures in a
// local, anonymous, plain-HTTP registry, in exactly the layout `cosign sign
// --key` writes and internal/dsl.SignatureVerifier reads.
//
//	cosign-sign keygen DIR                  # DIR/cosign.key (EC PRIVATE KEY), DIR/cosign.pub
//	cosign-sign sign --key cosign.key REF   # REF: host/repo:tag or host/repo@sha256:…
//	cosign-sign unsign REF                  # the .sig tag answers 404 afterwards
//
// WHY THIS EXISTS. The image-policy suites need signed and unsigned images on
// demand, and a signature removed mid-run, without depending on the `cosign`
// CLI for every scenario. The interoperability scenario
// (TestIntegration_ASignatureProducedByTheCosignCLIVerifies) runs the real
// `cosign` against the object this tool writes and this tool's verifier
// against the object `cosign` writes, which is what makes this layout the
// contract rather than a description of it.
//
// It is test tooling: it holds a private key, which nothing OrkStack ships
// ever does, and no binary under cmd/ links it. REF is resolved through
// internal/dsl.NewResolver — the engine's one resolver — so the digest it
// signs is the digest an apply pins.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/orkstack/orkstack/internal/dsl"
	"github.com/orkstack/orkstack/internal/testsupport/cosigntest"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "cosign-sign:", err)
		os.Exit(1)
	}
}

func usage() error {
	return fmt.Errorf("usage: cosign-sign keygen DIR | sign --key KEY REF | unsign REF")
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	switch args[0] {
	case "keygen":
		if len(args) != 2 {
			return usage()
		}
		key, err := cosigntest.GenerateKey()
		if err != nil {
			return err
		}
		if err := os.MkdirAll(args[1], 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(args[1], "cosign.key"), []byte(key.PrivatePEM), 0o600); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(args[1], "cosign.pub"), []byte(key.PublicPEM), 0o644); err != nil {
			return err
		}
		fmt.Printf("wrote %s and %s\n", filepath.Join(args[1], "cosign.key"), filepath.Join(args[1], "cosign.pub"))
		return nil
	case "sign":
		flags := flag.NewFlagSet("sign", flag.ContinueOnError)
		keyPath := flags.String("key", "", "PEM EC PRIVATE KEY written by keygen")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *keyPath == "" || flags.NArg() != 1 {
			return usage()
		}
		raw, err := os.ReadFile(*keyPath)
		if err != nil {
			return err
		}
		key, err := cosigntest.ParsePrivateKey(raw)
		if err != nil {
			return fmt.Errorf("--key %s: %w", *keyPath, err)
		}
		host, repo, digest, err := resolve(ctx, flags.Arg(0))
		if err != nil {
			return err
		}
		if err := (&cosigntest.Client{}).Sign(ctx, key, host, repo, digest); err != nil {
			return err
		}
		fmt.Printf("signed %s/%s@%s (%s/%s:%s)\n", host, repo, digest, host, repo, cosigntest.SignatureTag(digest))
		return nil
	case "unsign":
		if len(args) != 2 {
			return usage()
		}
		host, repo, digest, err := resolve(ctx, args[1])
		if err != nil {
			return err
		}
		if err := (&cosigntest.Client{}).Unsign(ctx, host, repo, digest); err != nil {
			return err
		}
		fmt.Printf("unsigned %s/%s@%s\n", host, repo, digest)
		return nil
	default:
		return usage()
	}
}

// resolve pins REF through the engine's resolver.
func resolve(ctx context.Context, ref string) (host, repo, digest string, err error) {
	image, err := dsl.NewResolver(os.Getenv("ORK_REGISTRY_AUTH_FILE")).Resolve(ctx, ref, ref)
	if err != nil {
		return "", "", "", err
	}
	return image.GetRegistry(), image.GetRepository(), image.GetIndexDigest(), nil
}
