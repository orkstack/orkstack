// Command dexrun starts and stops Dex, the identity provider of the
// development stack (scripts/stack.sh, make up) and of the
// sign-in Playwright spec (ui/e2e/signin.spec.ts), through
// internal/testsupport/dextest, so both run the image deploy/dex-image.txt
// pins with the users and clients deploy/dex/config.yaml holds.
//
//	dexrun start -dir DIR -addr 127.0.0.1:5556 -redirect URL [-proof ID -owner host:pid:start]
//	dexrun stop ID
//
// start prints one JSON object: the container id, the issuer, the CA file
// and the web client's secret file. It never pulls the image: an image the
// daemon lacks exits 77 with the docker pull to run.
package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/orkstack/orkstack/internal/testsupport/dextest"
)

// exitMissing is a missing prerequisite, as the proofs say it.
const exitMissing = 77

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "dexrun:", err)
		if errors.Is(err, dextest.ErrNoImage) {
			os.Exit(exitMissing)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: dexrun start or stop")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	switch args[0] {
	case "start":
		return start(ctx, args[1:])
	case "stop":
		if len(args) != 2 || args[1] == "" {
			return errors.New("usage: dexrun stop ID")
		}
		return dextest.Stop(ctx, args[1])
	}
	return fmt.Errorf("unknown command %q: start or stop", args[0])
}

func start(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	dir := fs.String("dir", "", "the directory that keeps Dex's CA, certificate and web client secret")
	addr := fs.String("addr", "127.0.0.1:0", "the loopback address Dex is published on (port 0: a free one)")
	redirect := fs.String("redirect", "", "the engine's web callback URL")
	proof := fs.String("proof", "", "the proof id the container is labelled with (with -owner)")
	owner := fs.String("owner", "", "host:pid:start of the process the container lives for; empty for the development stack")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *redirect == "" {
		return errors.New("start needs -dir and -redirect")
	}
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	root, err := dextest.RepoRoot(wd)
	if err != nil {
		return err
	}
	d, err := dextest.Start(ctx, dextest.Options{Root: root, Dir: *dir, Addr: *addr, WebRedirect: *redirect,
		ProofID: *proof, Owner: *owner})
	if err != nil {
		if errors.Is(err, dextest.ErrNoImage) {
			if image, ierr := dextest.Image(root); ierr == nil {
				return fmt.Errorf("%w: docker pull %s", err, image)
			}
		}
		return err
	}
	return json.MarshalWrite(os.Stdout, map[string]string{
		"id": d.ID, "issuer": d.Issuer, "ca_file": d.CAFile, "secret_file": d.SecretFile,
	})
}
