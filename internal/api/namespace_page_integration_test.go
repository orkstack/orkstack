//go:build integration

package api_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/api"
	"github.com/orkstack/orkstack/internal/engine"
	"github.com/orkstack/orkstack/internal/testsupport/proof"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// namespacePagingTenants is the corpus size: tenant-0000 … tenant-2999.
const namespacePagingTenants = 3000

// TestIntegration_NamespacePagesReadOnlyTheirDocuments is the cost Admin →
// Namespaces' paging exists for, measured against real MinIO.
//
// A bucket private to this test holds tenant-0000 … tenant-2999, every tenth
// labelled team=payments; `default` is NOT written, so it sorts first and is
// synthesized after its GET answers NotFound. DocumentsRead counts every
// namespace GetObject — NotFound included — and the delta around each call
// is exactly the plan docs/UI_CLI_PARITY.md "Namespace policy" states:
//
//   - a first page of 25, unfiltered: 25 (default's NotFound + 24 tenants);
//   - a first page of 25, name^tenant-1: 25 (the key listing decides it);
//   - a first page of 25, labels.team=payments: 256 (the 26th match is name
//     252, in the 32nd batch of 8, names 249–256);
//   - page_size 0, unfiltered: 3001 (every stored document + default's GET).
func TestIntegration_NamespacePagesReadOnlyTheirDocuments(t *testing.T) {
	cfg := artifacts.ConfigFromEnv()
	cfg.Backend = artifacts.BackendS3
	requireAuditMinIO(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	raw := auditS3Client(t, cfg)
	cfg.S3Bucket = fmt.Sprintf("orkstack-ns-paging-%d-%d", os.Getpid(), time.Now().UnixNano()%1_000_000_000)
	if _, err := raw.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(cfg.S3Bucket)}); err != nil {
		t.Fatalf("create private bucket %s: %v", cfg.S3Bucket, err)
	}
	t.Cleanup(func() { emptyAndDeleteBucket(raw, cfg.S3Bucket) })
	store, err := artifacts.NewS3NamespaceStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3NamespaceStore: %v", err)
	}
	seedTenants(ctx, t, store)

	svc := api.NewService(api.Config{
		Orkflows:   engine.NewMemoryOrkflowStore(),
		Namespaces: store,
		Runs:       engine.NewRunStore(),
		Bus:        engine.NewEventBus(),
		Starter:    &fakeStarter{},
		NewRunID:   func() string { return "run-001" },
	})

	var transcript strings.Builder
	measure := func(label string, size int32, token string, texts ...string) (*orkstackv1.ListNamespacesResponse, int64) {
		t.Helper()
		before := store.DocumentsRead()
		start := time.Now()
		resp, err := svc.ListNamespaces(ctx, connect.NewRequest(&orkstackv1.ListNamespacesRequest{
			PageSize: size, PageToken: token, Filters: filters(t, texts...),
		}))
		wall := time.Since(start)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		read := store.DocumentsRead() - before
		if label != "" {
			fmt.Fprintf(&transcript, "%s: %d documents in %s\n", label, read, wall.Round(time.Millisecond))
		}
		return resp.Msg, read
	}

	tenant := func(i int) string { return fmt.Sprintf("tenant-%04d", i) }
	expect := func(label string, got *orkstackv1.ListNamespacesResponse, read, wantRead int64, want []string, wantToken bool) {
		t.Helper()
		if read != wantRead {
			t.Errorf("%s read %d documents, want %d", label, read, wantRead)
		}
		names := namespaceNames(got.GetNamespaces())
		if strings.Join(names, " ") != strings.Join(want, " ") {
			t.Errorf("%s = %d rows from %v, want %d rows from %v", label, len(names), head(names), len(want), head(want))
		}
		if (got.GetNextPageToken() != "") != wantToken {
			t.Errorf("%s next_page_token = %q, want one: %v", label, got.GetNextPageToken(), wantToken)
		}
	}

	page, read := measure("unfiltered page of 25", 25, "")
	want := []string{"default"}
	for i := range 24 {
		want = append(want, tenant(i))
	}
	expect("unfiltered page of 25", page, read, 25, want, true)

	page, read = measure("name^tenant-1 page of 25", 25, "", "name^tenant-1")
	want = nil
	for i := range 25 {
		want = append(want, tenant(1000+i))
	}
	expect("name^tenant-1 page of 25", page, read, 25, want, true)

	page, read = measure("labels.team=payments page of 25", 25, "", "labels.team=payments")
	want = nil
	for i := range 25 {
		want = append(want, tenant(i*10))
	}
	expect("labels.team=payments page of 25", page, read, 256, want, true)

	page, read = measure("page_size 0", 0, "")
	want = []string{"default"}
	for i := range namespacePagingTenants {
		want = append(want, tenant(i))
	}
	expect("page_size 0", page, read, namespacePagingTenants+1, want, false)

	// Every page of the property filter: 300 matches in 12 pages of 25, no
	// repeat and no gap, the last page with no token.
	var walked []string
	token := ""
	pages := 0
	for {
		page, _ := measure("", 25, token, "labels.team=payments")
		pages++
		walked = append(walked, namespaceNames(page.GetNamespaces())...)
		if token = page.GetNextPageToken(); token == "" {
			break
		}
		if pages > 20 {
			t.Fatalf("labels.team=payments did not end within 20 pages")
		}
	}
	want = nil
	for i := range namespacePagingTenants / 10 {
		want = append(want, tenant(i*10))
	}
	if pages != 12 || strings.Join(walked, " ") != strings.Join(want, " ") {
		t.Errorf("walked %d pages holding %d names, want 12 pages holding the 300 matches once each, in order", pages, len(walked))
	}

	proof.Write(t, "", "namespaces-paging.txt", []byte(transcript.String()))
}

// seedTenants writes the corpus through the store's own Put, 32 at a time.
func seedTenants(ctx context.Context, t *testing.T, store *artifacts.S3NamespaceStore) {
	t.Helper()
	start := time.Now()
	slots := make(chan struct{}, 32)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for i := range namespacePagingTenants {
		ns := &orkstackv1.Namespace{Name: fmt.Sprintf("tenant-%04d", i), Description: "paging corpus"}
		if i%10 == 0 {
			ns.Labels = map[string]string{"team": "payments"}
		}
		wg.Add(1)
		slots <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			if _, err := store.Put(ctx, ns, artifacts.PutConditions{IfAbsent: true}); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("Put(%s): %w", ns.GetName(), err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	t.Logf("seeded %d namespaces in %s", namespacePagingTenants, time.Since(start).Round(time.Millisecond))
}

func head(names []string) []string {
	if len(names) > 3 {
		return names[:3]
	}
	return names
}
