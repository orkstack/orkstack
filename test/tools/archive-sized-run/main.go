// Command archive-sized-run archives one finished run of n SUCCEEDED plain
// tasks, each with an inline output of about bytes bytes of random values,
// through the engine's own archiver, into the archive the engine opened.
//
//	ORK_ARTIFACT_BACKEND=s3 ORK_S3_ENDPOINT=... go run ./test/tools/archive-sized-run \
//	    <namespace> <orkflow> <version> <definition-digest> <run-id> <tasks> <bytes>
//
// WHY THIS EXISTS. ui/e2e/run-page.spec.ts quotes how long the run page of
// the output-maximal run at the task ceiling takes to become interactive
// (spec T2 (e), E7: quoted, not gated). Such a run cannot finish on a real
// deployment at the ceiling — its saved state, every task's inline output,
// is over the 8 MiB a run carries forward, so it stops RUN_HISTORY_LIMIT when
// it would carry on — and running thousands of containers in a browser suite
// would take hours, so the page is measured on the rows such a run would
// hold: the same RunArchiver.Archive call a finished run's archive is written
// with. Its task rows are shaped as T2 (e)'s are.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func main() {
	if len(os.Args) != 8 {
		fmt.Fprintln(os.Stderr, "usage: archive-sized-run <namespace> <orkflow> <version> <definition-digest> <run-id> <tasks> <bytes>")
		os.Exit(2)
	}
	namespace, orkflow, version, digest, runID := os.Args[1], os.Args[2], os.Args[3], os.Args[4], os.Args[5]
	tasks, err := strconv.Atoi(os.Args[6])
	if err != nil || tasks < 1 {
		fmt.Fprintf(os.Stderr, "archive-sized-run: tasks %q: want a whole number, 1 or more\n", os.Args[6])
		os.Exit(2)
	}
	size, err := strconv.Atoi(os.Args[7])
	if err != nil || size < 0 {
		fmt.Fprintf(os.Stderr, "archive-sized-run: bytes %q: want a whole number\n", os.Args[7])
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	archive, err := artifacts.NewRunArchive(artifacts.ConfigFromEnv())
	if err != nil {
		fmt.Fprintf(os.Stderr, "archive-sized-run: open the run archive: %v\n", err)
		os.Exit(1)
	}
	at := time.Now().UTC().Add(-time.Hour)
	run := &orkstackv1.Run{
		Id: runID, Namespace: namespace, OrkflowName: orkflow, OrkflowVersion: version,
		OrkflowDefinitionDigest: digest, Status: orkstackv1.Status_STATUS_SUCCEEDED,
		CreatedAt: timestamppb.New(at), StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(30 * time.Minute)),
	}
	for i := range tasks {
		row, err := sizedRow(namespace, runID, fmt.Sprintf("t%d", i), size, at)
		if err != nil {
			fmt.Fprintf(os.Stderr, "archive-sized-run: %v\n", err)
			os.Exit(1)
		}
		run.TaskRuns = append(run.TaskRuns, row)
	}
	if err := engine.NewRunArchiver(archive).Archive(ctx, run, nil); err != nil {
		fmt.Fprintf(os.Stderr, "archive-sized-run: archive %s: %v\n", runID, err)
		os.Exit(1)
	}
	fmt.Printf("archived run %s of %s@%s in namespace %s: %d tasks, %d-byte inline outputs\n",
		runID, orkflow, version, namespace, tasks, size)
}

// sizedRow is one SUCCEEDED task row with a commit, its values and two logs,
// and an inline output of a random blob of about size bytes.
func sizedRow(namespace, runID, id string, size int, at time.Time) (*orkstackv1.TaskRun, error) {
	raw := make([]byte, size*3/4)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	data, err := structpb.NewStruct(map[string]any{"blob": base64.StdEncoding.EncodeToString(raw)})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	sha := hex.EncodeToString(sum[:])
	dir := "s3://orkstack-artifacts/metadata/commits/" + namespace + "/" + runID + "/" + id + "/"
	ref := func(name string) *orkstackv1.ArtifactRef {
		return &orkstackv1.ArtifactRef{Name: name, Uri: dir + name, Sha256: sha, SizeBytes: int64(size)}
	}
	return &orkstackv1.TaskRun{
		TaskId: id, Status: orkstackv1.Status_STATUS_SUCCEEDED, Pool: "main", OrkerId: "orker-sized", Attempt: 1,
		ScheduledAt: timestamppb.New(at), StartedAt: timestamppb.New(at), FinishedAt: timestamppb.New(at.Add(time.Second)),
		ValuesRef:       ref("values.json"),
		Commit:          &orkstackv1.CommitRef{Uri: dir + "commit.json", Sha256: sha, Etag: sha[:32], CreatedAt: timestamppb.New(at)},
		SystemArtifacts: []*orkstackv1.ArtifactRef{ref("stdout.log"), ref("stderr.log")},
		Output:          &orkstackv1.Payload{Data: data},
	}, nil
}
