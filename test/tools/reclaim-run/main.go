// Command reclaim-run does to one archived run exactly what the cleanup worker
// does when the run's retention ends: artifacts.RunArchive.DeleteRunArtifacts,
// which removes the run's logs, completion commits, pin snapshots, object
// markers — and its per-attempt records — while the archived body and its
// index key stay.
//
//	ORK_ARTIFACT_BACKEND=s3 ORK_S3_ENDPOINT=... go run ./test/tools/reclaim-run <namespace> <run-id>
//
// WHY THIS EXISTS. ui/e2e/logs-dashboard.spec.ts proves that the dashboard's
// failure fingerprints say `unavailable` — not blank, not zero — once a run's
// attempt records are gone. Waiting out a real retention period is not a test,
// and no RPC deletes attempt records (none should: reclaiming is the cleanup
// worker's job, internal/archive/cleanup.go). So the spec invokes the worker's
// own call on the worker's own store, through pkg/artifacts, against the
// bucket the engine and the Orker opened.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: reclaim-run <namespace> <run-id>")
		os.Exit(2)
	}
	namespace, runID := os.Args[1], os.Args[2]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	archive, err := artifacts.NewRunArchive(artifacts.ConfigFromEnv())
	if err != nil {
		fmt.Fprintf(os.Stderr, "reclaim-run: open the run archive: %v\n", err)
		os.Exit(1)
	}
	result, err := archive.DeleteRunArtifacts(ctx, namespace, runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "reclaim-run: delete run artifacts of %s: %v\n", runID, err)
		os.Exit(1)
	}
	fmt.Printf("reclaimed run %s in namespace %s: %+v\n", runID, namespace, result)
}
