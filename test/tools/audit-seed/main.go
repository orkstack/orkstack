// Command audit-seed writes n audit records of one action, all on the day
// days days before today (from its midday, a millisecond apart), into the
// audit trail the engine reads, through the store's own PutAuditRecord.
//
//	ORK_ARTIFACT_BACKEND=s3 ORK_S3_ENDPOINT=... audit-seed <n> <action> <days>
//
// WHY THIS EXISTS. ui/e2e/signin.spec.ts proves that Access → People steps
// its window down when a long one examines more audit records than one
// request may (artifacts.AuditScanMaxObjects, 5000). Writing that many
// records through the API would take thousands of audited requests; the
// limit is a constant with no setting, so the spec puts the records there
// directly, shaped as the engine writes them.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// writers is how many records are written at once.
const writers = 32

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: audit-seed <n> <action> <days>")
		os.Exit(2)
	}
	n, err1 := strconv.Atoi(os.Args[1])
	days, err2 := strconv.Atoi(os.Args[3])
	// At most a million records, so they stay inside their day; at least a day
	// back, so none is stamped in the future.
	if err1 != nil || err2 != nil || n < 1 || n > 1_000_000 || days < 1 {
		fmt.Fprintln(os.Stderr, "audit-seed: n must be 1 to 1000000 and days 1 or more")
		os.Exit(2)
	}
	if err := seed(context.Background(), n, os.Args[2], days); err != nil {
		fmt.Fprintln(os.Stderr, "audit-seed:", err)
		os.Exit(1)
	}
}

func seed(ctx context.Context, n int, action string, days int) error {
	store, err := artifacts.NewAuditStore(ctx, artifacts.ConfigFromEnv())
	if err != nil {
		return err
	}
	// Midday of the day, so every record falls in it whatever the zone.
	y, m, d := time.Now().UTC().AddDate(0, 0, -days).Date()
	base := time.Date(y, m, d, 12, 0, 0, 0, time.UTC)
	jobs := make(chan int)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for i := range jobs {
				at := base.Add(time.Duration(i) * time.Millisecond)
				rec := &orkstackv1.AuditRecord{SchemaVersion: artifacts.AuditSchemaVersionVerified, Id: artifacts.NewAuditID(at),
					At: timestamppb.New(at), Action: action, Principal: "user:seed:noise", PrincipalVerified: true,
					Namespace: "default", Target: "default", Succeeded: true, Procedure: "seed"}
				if err := store.PutAuditRecord(ctx, rec); err != nil {
					errs <- fmt.Errorf("record %d: %w", i, err)
					return
				}
			}
		})
	}
	for i := range n {
		select {
		case jobs <- i:
		case err := <-errs:
			close(jobs)
			wg.Wait()
			return err
		}
	}
	close(jobs)
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	fmt.Printf("seeded %d %s records %d days back\n", n, action, days)
	return nil
}
