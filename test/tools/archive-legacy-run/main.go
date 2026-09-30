// Command archive-legacy-run archives one finished run that carries NO origin
// — exactly the record every run archived before run origins existed has —
// through the engine's own archiver, into the archive the engine opened.
//
//	ORK_ARTIFACT_BACKEND=s3 ORK_S3_ENDPOINT=... go run ./test/tools/archive-legacy-run <namespace> <orkflow> <version> <run-id>
//
// WHY THIS EXISTS. ui/e2e/run-origin.spec.ts proves that a run recorded before
// origins existed reads "unrecorded" — never "submit" — on the run page and in
// the runs list. Every run a current engine starts carries an origin, so the
// only honest way to have a pre-origin run is to write the archive body the
// old engine wrote: the same RunArchiver.Archive call, on a Run with no
// origin field.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: archive-legacy-run <namespace> <orkflow> <version> <run-id>")
		os.Exit(2)
	}
	namespace, orkflow, version, runID := os.Args[1], os.Args[2], os.Args[3], os.Args[4]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	archive, err := artifacts.NewRunArchive(artifacts.ConfigFromEnv())
	if err != nil {
		fmt.Fprintf(os.Stderr, "archive-legacy-run: open the run archive: %v\n", err)
		os.Exit(1)
	}
	at := time.Now().UTC().Add(-time.Hour)
	run := &orkstackv1.Run{
		Id:             runID,
		Namespace:      namespace,
		OrkflowName:    orkflow,
		OrkflowVersion: version,
		Status:         orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt:      timestamppb.New(at),
		StartedAt:      timestamppb.New(at),
		FinishedAt:     timestamppb.New(at.Add(time.Second)),
	}
	if err := engine.NewRunArchiver(archive).Archive(ctx, run, nil); err != nil {
		fmt.Fprintf(os.Stderr, "archive-legacy-run: archive %s: %v\n", runID, err)
		os.Exit(1)
	}
	fmt.Printf("archived pre-origin run %s of %s@%s in namespace %s\n", runID, orkflow, version, namespace)
}
