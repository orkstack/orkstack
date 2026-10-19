//go:build integration

// Integration coverage for the S3 run archive.
//
// Run against the repo's local infrastructure (`make infra-up`):
//
//	go test -tags=integration ./pkg/artifacts/... -count=1
//
// The tests skip (never fail) when MinIO is unreachable.
package artifacts_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

func newS3RunArchive(t *testing.T) (*artifacts.S3RunArchive, artifacts.Config) {
	t.Helper()
	cfg := s3TestConfig()
	requireMinIO(t, cfg)

	archive, err := artifacts.NewS3RunArchive(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	return archive, cfg
}

// archiveTestRun invents a run id shaped like a real one, unique per test so a
// shared dev bucket cannot make two runs collide.
func archiveTestRun(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("ork-%d-%s%s", time.Now().UnixNano(), uniqueToken(t)[:6], suffix)
}

// cleanupArchivedRun removes a run's body and index keys so repeated runs of the
// suite leave the dev bucket as they found it.
func cleanupArchivedRun(t *testing.T, cfg artifacts.Config, runID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		client := newRawS3Client(t, cfg)
		body, err := artifacts.RunArchiveKey(runID)
		if err != nil {
			t.Fatalf("RunArchiveKey: %v", err)
		}
		list, err := artifacts.RunObjectListKey(runID)
		if err != nil {
			t.Fatalf("RunObjectListKey: %v", err)
		}
		keys := []string{body, list}
		for _, prefix := range []string{artifacts.RunIndexNamespace + "/", artifacts.RunOriginIndexNamespace + "/", artifacts.RunRecentIndexNamespace + "/", artifacts.RunObjectRefNamespace + "/"} {
			for _, key := range listKeys(t, ctx, client, cfg.S3Bucket, prefix) {
				if strings.HasSuffix(key, "/"+runID) {
					keys = append(keys, key)
				}
			}
		}
		for _, key := range keys {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(cfg.S3Bucket),
				Key:    aws.String(key),
			}); err != nil {
				t.Logf("cleanup %q: %v", key, err)
			}
		}
	})
}

// TestS3RunArchive_WritesRunJSONLAtTheContractPath measures the contract path
// against real object storage: the body lands at s3://{bucket}/metadata/runs/{run-id}.jsonl,
// one record per line, and a raw S3 client (not our store) can see it there.
func TestS3RunArchive_WritesRunJSONLAtTheContractPath(t *testing.T) {
	archive, cfg := newS3RunArchive(t)
	ctx := context.Background()
	runID := archiveTestRun(t, "-1")
	cleanupArchivedRun(t, cfg, runID)

	body := []byte(strings.Join([]string{
		`{"kind":"run","run":{"id":"` + runID + `","status":"STATUS_SUCCEEDED"}}`,
		`{"kind":"event","event":{"runId":"` + runID + `","sequence":"1"}}`,
		`{"kind":"event","event":{"runId":"` + runID + `","sequence":"2"}}`,
		"",
	}, "\n"))

	if err := archive.PutRun(ctx, artifacts.RunRecord{
		RunID:   runID,
		Orkflow: "hello-ork",
		Status:  "succeeded",
		Time:    time.Now().UTC(),
		JSONL:   body,
	}); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	wantKey := "metadata/runs/" + runID + ".jsonl"
	uri, err := archive.URI(runID)
	if err != nil {
		t.Fatalf("URI: %v", err)
	}
	if want := "s3://" + cfg.S3Bucket + "/" + wantKey; uri != want {
		t.Fatalf("archive URI = %q, want %q", uri, want)
	}

	client := newRawS3Client(t, cfg)
	keys := listKeys(t, ctx, client, cfg.S3Bucket, wantKey)
	if len(keys) != 1 || keys[0] != wantKey {
		t.Fatalf("bucket keys under %q = %v, want exactly that key", wantKey, keys)
	}

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(cfg.S3Bucket),
		Key:    aws.String(wantKey),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if got := aws.ToString(head.ContentType); got != artifacts.RunArchiveContentType {
		t.Fatalf("content type = %q, want %q", got, artifacts.RunArchiveContentType)
	}

	read, err := archive.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if string(read) != string(body) {
		t.Fatalf("GetRun body = %q, want the archived bytes", read)
	}
	lines := strings.Split(strings.TrimSuffix(string(read), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("archived body has %d lines, want 3 (1 run + 2 events)", len(lines))
	}
}

// TestS3RunArchive_BodyIsImmutable proves an archived run cannot be rewritten,
// which is what makes the write-through activity safe to retry.
func TestS3RunArchive_BodyIsImmutable(t *testing.T) {
	archive, cfg := newS3RunArchive(t)
	ctx := context.Background()
	runID := archiveTestRun(t, "-1")
	cleanupArchivedRun(t, cfg, runID)

	record := artifacts.RunRecord{
		RunID:   runID,
		Orkflow: "hello-ork",
		Status:  "succeeded",
		Time:    time.Now().UTC(),
		JSONL:   []byte("first\n"),
	}
	if err := archive.PutRun(ctx, record); err != nil {
		t.Fatalf("PutRun: %v", err)
	}
	record.JSONL = []byte("second\n")
	if err := archive.PutRun(ctx, record); err != nil {
		t.Fatalf("PutRun (retry): %v", err)
	}

	body, err := archive.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if string(body) != "first\n" {
		t.Fatalf("body = %q, want the first archived copy", body)
	}
}

// TestS3RunArchive_MissingRunIsNotFound is the signal the API's fallback needs
// to answer NotFound rather than Internal.
func TestS3RunArchive_MissingRunIsNotFound(t *testing.T) {
	archive, _ := newS3RunArchive(t)
	if _, err := archive.GetRun(context.Background(), archiveTestRun(t, "-absent")); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("GetRun error = %v, want ErrNotFound", err)
	}
}

// TestS3RunArchive_PrefixScanFiltersByOrkflowStatusAndDateRange measures the
// query at the storage layer: real ListObjectsV2 prefix scans answer it, and
// the keys they return carry every filtered field.
func TestS3RunArchive_PrefixScanFiltersByOrkflowStatusAndDateRange(t *testing.T) {
	archive, cfg := newS3RunArchive(t)
	ctx := context.Background()

	// A per-test orkflow name keeps this run of the suite isolated from every
	// other object already in the shared dev bucket.
	orkflow := "archive-it-" + uniqueToken(t)[:8]
	other := orkflow + "-other"
	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)

	seed := []struct {
		suffix  string
		orkflow string
		status  string
		day     int
	}{
		{"-1", orkflow, "succeeded", 0},
		{"-2", orkflow, "failed", 1},
		{"-3", orkflow, "succeeded", 2},
		{"-4", other, "succeeded", 3},
		{"-5", orkflow, "succeeded", 4},
	}
	ids := make(map[string]string, len(seed))
	for _, s := range seed {
		runID := archiveTestRun(t, s.suffix)
		cleanupArchivedRun(t, cfg, runID)
		ids[s.suffix] = runID
		if err := archive.PutRun(ctx, artifacts.RunRecord{
			RunID:   runID,
			Orkflow: s.orkflow,
			Status:  s.status,
			Time:    base.AddDate(0, 0, s.day),
			JSONL:   []byte(`{"kind":"run","run":{"id":"` + runID + `"}}` + "\n"),
		}); err != nil {
			t.Fatalf("PutRun(%s): %v", runID, err)
		}
	}

	tests := []struct {
		name  string
		query artifacts.RunQuery
		want  []string
	}{
		{
			name:  "by orkflow",
			query: artifacts.RunQuery{Orkflow: orkflow},
			want:  []string{"-5", "-3", "-2", "-1"},
		},
		{
			name:  "by orkflow and status",
			query: artifacts.RunQuery{Orkflow: orkflow, Status: "SUCCEEDED"},
			want:  []string{"-5", "-3", "-1"},
		},
		{
			name: "by orkflow, status and date range",
			query: artifacts.RunQuery{
				Orkflow: orkflow,
				Status:  "succeeded",
				Since:   base.AddDate(0, 0, 1),
				Until:   base.AddDate(0, 0, 4),
			},
			want: []string{"-3"},
		},
		{
			name:  "limit keeps the newest",
			query: artifacts.RunQuery{Orkflow: orkflow, Limit: 2},
			want:  []string{"-5", "-3"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries, err := archive.ListRuns(ctx, tc.query)
			if err != nil {
				t.Fatalf("ListRuns: %v", err)
			}
			want := make([]string, 0, len(tc.want))
			for _, suffix := range tc.want {
				want = append(want, ids[suffix])
			}
			if got := runIDs(entries); !equalStrings(got, want) {
				t.Fatalf("run ids = %v, want %v", got, want)
			}
			for _, entry := range entries {
				if entry.Orkflow != tc.query.Orkflow {
					t.Fatalf("entry %s carries orkflow %q, want %q",
						entry.RunID, entry.Orkflow, tc.query.Orkflow)
				}
				if entry.Status == "" || entry.Time.IsZero() {
					t.Fatalf("entry %+v lost a field the key is supposed to carry", entry)
				}
			}
		})
	}

	// The index holds no bytes: everything a query filters on is in the key.
	client := newRawS3Client(t, cfg)
	indexKeys := listKeys(t, ctx, client, cfg.S3Bucket, artifacts.RunIndexNamespace+"/"+artifacts.DefaultNamespace+"/"+orkflow+"/")
	if len(indexKeys) != 4 {
		t.Fatalf("index keys for %s = %v, want 4", orkflow, indexKeys)
	}
	for _, key := range indexKeys {
		head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(cfg.S3Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("HeadObject(%q): %v", key, err)
		}
		if size := aws.ToInt64(head.ContentLength); size != 0 {
			t.Fatalf("index object %q is %d bytes, want 0: the key is the record", key, size)
		}
	}
}

func TestS3RunArchive_DeletesObjectOnlyAfterLastReference(t *testing.T) {
	archive, cfg := newS3RunArchive(t)
	ctx := context.Background()
	store, err := artifacts.NewS3Store(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}
	shared, err := store.Put(ctx, "shared-"+uniqueToken(t), bytes.NewBufferString("shared bytes"), artifacts.PutOptions{
		Metadata: artifacts.Provenance{Orkflow: "cleanup", RunID: archiveTestRun(t, "-producer"), TaskID: "task"}.Metadata(nil),
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Cleanup(func() { _ = store.Delete(context.Background(), shared.URI) })
	objectID, ok := artifacts.ObjectIDFromURI(shared.URI)
	if !ok {
		t.Fatalf("ObjectIDFromURI(%q) failed", shared.URI)
	}
	// Assert the object is really there before any cleanup runs, so a survival
	// assertion cannot pass vacuously against an empty or shared dev bucket.
	if _, err := store.Stat(ctx, shared.URI); err != nil {
		t.Fatalf("shared object absent before cleanup: %v", err)
	}
	run1 := archiveTestRun(t, "-ref-1")
	run2 := archiveTestRun(t, "-ref-2")
	cleanupArchivedRun(t, cfg, run1)
	cleanupArchivedRun(t, cfg, run2)
	for i, runID := range []string{run1, run2} {
		if err := archive.PutRun(ctx, artifacts.RunRecord{RunID: runID, Namespace: "default", Orkflow: "cleanup", Status: "succeeded", Time: time.Now().UTC().Add(time.Duration(i) * time.Second), ObjectIDs: []string{objectID}, JSONL: []byte(runID + "\n")}); err != nil {
			t.Fatalf("PutRun(%s): %v", runID, err)
		}
	}
	if _, err := archive.DeleteRunArtifacts(ctx, "default", run1); err != nil {
		t.Fatalf("DeleteRunArtifacts(first): %v", err)
	}
	result, err := archive.DeleteUnreferencedObjects(ctx, []string{objectID})
	if err != nil || result.ObjectsDeleted != 0 {
		t.Fatalf("shared result = %+v, %v", result, err)
	}
	if _, err := store.Stat(ctx, shared.URI); err != nil {
		t.Fatalf("shared object disappeared: %v", err)
	}
	if _, err := archive.DeleteRunArtifacts(ctx, "default", run2); err != nil {
		t.Fatalf("DeleteRunArtifacts(last): %v", err)
	}
	result, err = archive.DeleteUnreferencedObjects(ctx, []string{objectID, objectID})
	if err != nil || result.ObjectsDeleted != 1 || result.BytesReclaimed != shared.SizeBytes {
		t.Fatalf("last-reference result = %+v, %v", result, err)
	}
	result, err = archive.DeleteUnreferencedObjects(ctx, []string{objectID})
	if err != nil || result != (artifacts.CleanupResult{}) {
		t.Fatalf("idempotent result = %+v, %v", result, err)
	}
	for _, runID := range []string{run1, run2} {
		if _, err := archive.GetRun(ctx, runID); err != nil {
			t.Fatalf("archived body %s disappeared: %v", runID, err)
		}
	}
	t.Logf("EVIDENCE s3_shared_object_present_before_cleanup=true shared_survived_first_run=true "+
		"deleted_after_last_reference=true bytes_reclaimed=%d repeat_pass_no_op=true archive_bodies_present=true",
		shared.SizeBytes)
}

// TestS3RunArchive_ObjectDeletedMidScanIsIdempotent is the live half of the
// mid-scan race: the reference index says the object is unreclaimed and the
// bytes vanish before the delete reaches MinIO. Recovery must be silent and
// repeatable, and a still-referenced object whose bytes vanished must not be
// counted as reclaimed either.
func TestS3RunArchive_ObjectDeletedMidScanIsIdempotent(t *testing.T) {
	archive, cfg := newS3RunArchive(t)
	ctx := context.Background()
	store, err := artifacts.NewS3Store(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}
	raced, err := store.Put(ctx, "raced-"+uniqueToken(t), bytes.NewBufferString("raced bytes"), artifacts.PutOptions{})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Cleanup(func() { _ = store.Delete(context.Background(), raced.URI) })
	objectID, ok := artifacts.ObjectIDFromURI(raced.URI)
	if !ok {
		t.Fatalf("ObjectIDFromURI(%q) failed", raced.URI)
	}
	if _, err := store.Stat(ctx, raced.URI); err != nil {
		t.Fatalf("object absent before the race: %v", err)
	}

	runID := archiveTestRun(t, "-raced")
	cleanupArchivedRun(t, cfg, runID)
	if err := archive.PutRun(ctx, artifacts.RunRecord{RunID: runID, Namespace: "default", Orkflow: "cleanup",
		Status: "succeeded", Time: time.Now().UTC(), ObjectIDs: []string{objectID}, JSONL: []byte(runID + "\n")}); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	// Still referenced, bytes already gone: a no-op, not an error, not a count.
	if err := store.Delete(ctx, raced.URI); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	result, err := archive.DeleteUnreferencedObjects(ctx, []string{objectID})
	if err != nil || result != (artifacts.CleanupResult{}) {
		t.Fatalf("referenced-but-missing = %+v, %v", result, err)
	}

	// Unreferenced and already gone: still a no-op, and stable across passes.
	if _, err := archive.DeleteRunArtifacts(ctx, "default", runID); err != nil {
		t.Fatalf("DeleteRunArtifacts: %v", err)
	}
	for pass := range 2 {
		result, err = archive.DeleteUnreferencedObjects(ctx, []string{objectID, objectID})
		if err != nil || result != (artifacts.CleanupResult{}) {
			t.Fatalf("recovery pass %d = %+v, %v", pass, result, err)
		}
	}
	if _, err := archive.GetRun(ctx, runID); err != nil {
		t.Fatalf("archived body disappeared: %v", err)
	}
	t.Logf("EVIDENCE s3_midscan_referenced_missing_noop=true s3_midscan_unreferenced_missing_noop=true " +
		"s3_midscan_repeat_pass_no_op=true s3_midscan_archive_body_present=true")
}

// Archiving writes one reference marker per referenced object, so its cost is
// linear in a run's artifact count — and it happens inside a projection
// activity with a 10-second budget whose failure is only logged, which would
// leave a wide fan-out run with no archive body at all. This measures the
// marker write against real object storage, and pins that overlapping the round
// trips is materially cheaper than issuing them one at a time.
//
// It bounds the ratio, not the absolute time: a shared dev bucket and a loaded
// machine make wall-clock thresholds flaky, while the serial baseline is
// measured in the same conditions in the same test.
func TestS3RunArchive_MarkerWritesDoNotScaleSerially(t *testing.T) {
	archive, cfg := newS3RunArchive(t)
	ctx := context.Background()
	client := newRawS3Client(t, cfg)

	digest := strings.Repeat("ab", 32)
	objectIDsFor := func(n int) []string {
		ids := make([]string, 0, n)
		for i := 0; i < n; i++ {
			ids = append(ids, fmt.Sprintf("objects/n-%s%04d/%s", uniqueToken(t)[:6], i, digest))
		}
		return ids
	}

	type sample struct {
		objects    int
		concurrent time.Duration
		serial     time.Duration
	}
	var samples []sample
	for _, objects := range []int{0, 50, 200, 500} {
		objectIDs := objectIDsFor(objects)

		runID := archiveTestRun(t, fmt.Sprintf("-conc-%d", objects))
		cleanupArchivedRun(t, cfg, runID)
		start := time.Now()
		if err := archive.PutRun(ctx, artifacts.RunRecord{
			RunID: runID, Namespace: "default", Orkflow: "cleanup", Status: "succeeded",
			Time: time.Now().UTC(), ObjectIDs: objectIDs, JSONL: []byte(runID + "\n"),
		}); err != nil {
			t.Fatalf("PutRun(%d objects): %v", objects, err)
		}
		concurrent := time.Since(start)

		// The same marker keys, written one at a time through the same client
		// and bucket: the cost this replaced, measured rather than remembered.
		serialRun := archiveTestRun(t, fmt.Sprintf("-serial-%d", objects))
		cleanupArchivedRun(t, cfg, serialRun)
		serialKeys := make([]string, 0, objects)
		for _, objectID := range objectIDs {
			key, err := artifacts.RunObjectRefKey(objectID, serialRun)
			if err != nil {
				t.Fatalf("RunObjectRefKey: %v", err)
			}
			serialKeys = append(serialKeys, key)
		}
		start = time.Now()
		for _, key := range serialKeys {
			if _, err := client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
				Body: bytes.NewReader(nil), ContentLength: aws.Int64(0),
			}); err != nil {
				t.Fatalf("serial marker %q: %v", key, err)
			}
		}
		samples = append(samples, sample{objects: objects, concurrent: concurrent, serial: time.Since(start)})

		// Every marker still has to be durable: bounded concurrency must not
		// turn into partial success, or a later DeleteUnreferencedObjects would
		// delete an object this run still references.
		for _, objectID := range objectIDs {
			key, err := artifacts.RunObjectRefKey(objectID, runID)
			if err != nil {
				t.Fatalf("RunObjectRefKey: %v", err)
			}
			if _, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(cfg.S3Bucket), Key: aws.String(key),
			}); err != nil {
				t.Fatalf("marker %q missing after PutRun(%d objects): %v", key, objects, err)
			}
		}
	}

	var evidence []string
	for _, s := range samples {
		speedup := 1.0
		if s.concurrent > 0 {
			speedup = float64(s.serial) / float64(s.concurrent)
		}
		evidence = append(evidence, fmt.Sprintf("objects=%d concurrent=%s serial=%s speedup=%.1fx",
			s.objects, s.concurrent.Round(time.Millisecond), s.serial.Round(time.Millisecond), speedup))
	}
	// The speedup is evidence, not a bound: on a 2-vCPU runner the concurrent
	// and serial forms can measure the same wall clock. What this test holds
	// is durability — every marker above is present — and the code's bound is
	// archiveRefWriteConcurrency, which the source pins.
	t.Logf("EVIDENCE s3_marker_write_scaling %s", strings.Join(evidence, " | "))
}

// The key-order page contract is what makes one cleanup iteration enumerate the
// run index once instead of once per page, and S3 is the backend it was
// reasoned about — its StartAfter is the cursor. Every other test of that
// contract drives the local archive, and the cleanup worker's own tests drive a
// fake that MODELS StartAfter rather than exercising it, so this is the only
// place the production listing semantics are pinned.
//
// The corpus is scoped to one generated namespace so the counts are exact in a
// shared dev bucket, and the ordering hazard lives in the segments beneath it:
// sibling orkflows where one name is a prefix of another and where '-' (0x2D)
// sorts BELOW the '/' (0x2F) that separates segments, so directory-name order
// and key order disagree. Timestamps repeat five ways, so ties are the norm.
func TestS3RunArchive_KeyOrderPagingIsOnePassOverTheIndex(t *testing.T) {
	archive, cfg := newS3RunArchive(t)
	ctx := context.Background()
	client := newRawS3Client(t, cfg)

	namespace := "keyorder-" + uniqueToken(t)
	orkflows := []string{"f", "f-g", "f-", "fg", "nightly", "nightly-build"}
	statuses := []string{"succeeded", "failed", "canceled"}
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	const runs = 60

	t.Cleanup(func() {
		deleteByPrefix(t, ctx, client, cfg.S3Bucket,
			artifacts.RunIndexNamespace+"/"+namespace+"/")
	})
	for i := 0; i < runs; i++ {
		runID := archiveTestRun(t, fmt.Sprintf("-ko-%02d", i))
		cleanupArchivedRun(t, cfg, runID)
		if err := archive.PutRun(ctx, artifacts.RunRecord{
			RunID:     runID,
			Namespace: namespace,
			Orkflow:   orkflows[i%len(orkflows)],
			Status:    statuses[i%len(statuses)],
			// i/5 makes five runs share every stamp.
			Time:  base.Add(time.Duration(i/5) * time.Minute),
			JSONL: []byte(runID + "\n"),
		}); err != nil {
			t.Fatalf("PutRun %q: %v", runID, err)
		}
	}

	scoped := artifacts.RunQuery{Namespace: namespace, KeyOrder: true}
	walk := func(pageSize int) (ids []string, keysEnumerated int64) {
		before := archive.IndexKeysScanned()
		q := scoped
		q.Limit = pageSize
		for {
			page, err := archive.ListRuns(ctx, q)
			if err != nil {
				t.Fatalf("ListRuns(page=%d, after=%q): %v", pageSize, q.StartAfterKey, err)
			}
			if len(page) == 0 {
				break
			}
			for _, entry := range page {
				ids = append(ids, entry.RunID)
			}
			if pageSize <= 0 || len(page) < pageSize {
				break
			}
			cursor, err := artifacts.RunIndexKeyForEntry(page[len(page)-1])
			if err != nil {
				t.Fatalf("RunIndexKeyForEntry: %v", err)
			}
			q.StartAfterKey = cursor
		}
		return ids, archive.IndexKeysScanned() - before
	}

	// A single unpaged key-order walk is the reference every paged traversal
	// must reproduce exactly — same runs, same order, each one once.
	want, wantKeys := walk(0)
	if len(want) != runs {
		t.Fatalf("unpaged key-order walk returned %d runs, want %d", len(want), runs)
	}
	seen := make(map[string]int, len(want))
	for _, id := range want {
		seen[id]++
	}
	if len(seen) != runs {
		t.Fatalf("unpaged walk returned %d distinct runs, want %d", len(seen), runs)
	}

	var evidence []string
	for _, pageSize := range []int{1, 3, 7, 60, 61} {
		got, keys := walk(pageSize)
		if len(got) != len(want) {
			t.Fatalf("page=%d visited %d runs, want %d", pageSize, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("page=%d diverged at %d: got %q, want %q", pageSize, i, got[i], want[i])
			}
		}
		// The whole point of the key-order page: cost is one enumeration per
		// traversal, not one per page. A page size that changed this number
		// would mean the backend re-reads the index to answer each page.
		if keys != wantKeys {
			t.Errorf("page=%d enumerated %d index keys, want %d — paging is re-scanning",
				pageSize, keys, wantKeys)
		}
		evidence = append(evidence, fmt.Sprintf("page=%d:keys=%d", pageSize, keys))
	}

	// The contrast the key-order mode exists to remove: the default newest-first
	// query cannot stop early, because the stamp is the fourth key segment, so
	// every page re-enumerates the whole scope.
	const defaultPages, defaultPage = 5, 12
	beforeDefault := archive.IndexKeysScanned()
	for i := 0; i < defaultPages; i++ {
		if _, err := archive.ListRuns(ctx, artifacts.RunQuery{
			Namespace: namespace, Limit: defaultPage,
		}); err != nil {
			t.Fatalf("default-order ListRuns: %v", err)
		}
	}
	newestFirstKeys := archive.IndexKeysScanned() - beforeDefault
	if newestFirstKeys <= wantKeys {
		t.Errorf("default-order paging enumerated %d keys over %d pages, expected it to exceed "+
			"the %d of one key-order traversal", newestFirstKeys, defaultPages, wantKeys)
	}

	t.Logf("EVIDENCE s3_keyorder runs=%d unpaged_keys=%d %s newest_first_pages=%d newest_first_keys=%d",
		runs, wantKeys, strings.Join(evidence, " "), defaultPages, newestFirstKeys)
}

// TestS3RunArchive_ListObjectReferencesReadsMarkerKeysOnly is the keys-only
// property of the provenance read against real object storage: each of one
// run's objects lists exactly that run, and the archive issues ListObjectsV2
// and nothing else — no GetObject of a marker or of a body.
func TestS3RunArchive_ListObjectReferencesReadsMarkerKeysOnly(t *testing.T) {
	cfg := s3TestConfig()
	requireMinIO(t, cfg)
	ctx := context.Background()

	var mu sync.Mutex
	operations := map[string]int{}
	counting := func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
			return stack.Initialize.Add(middleware.InitializeMiddlewareFunc("CountOperations",
				func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
					mu.Lock()
					operations[awsmiddleware.GetOperationName(ctx)]++
					mu.Unlock()
					return next.HandleInitialize(ctx, in)
				}), middleware.After)
		})
	}
	writer, err := artifacts.NewS3RunArchive(ctx, cfg)
	if err != nil {
		t.Fatalf("NewS3RunArchive: %v", err)
	}
	reader, err := artifacts.NewS3RunArchiveWithOptions(ctx, cfg, counting)
	if err != nil {
		t.Fatalf("NewS3RunArchiveWithOptions: %v", err)
	}

	runID := archiveTestRun(t, "-provenance")
	cleanupArchivedRun(t, cfg, runID)
	token := uniqueToken(t)[:8]
	objectIDs := []string{
		fmt.Sprintf("orkflows/provenance/%s/task/report-%s.txt/%s", runID, token, strings.Repeat("a", 64)),
		fmt.Sprintf("orkflows/provenance/%s/task/values.json/%s", runID, strings.Repeat("b", 64)),
		fmt.Sprintf("orkflows/provenance/%s/task/stdout.log/%s", runID, strings.Repeat("c", 64)),
	}
	if err := writer.PutRun(ctx, artifacts.RunRecord{
		RunID: runID, Namespace: "default", Orkflow: "provenance", Status: "succeeded",
		Time: time.Now().UTC(), ObjectIDs: objectIDs, JSONL: []byte(runID + "\n"),
	}); err != nil {
		t.Fatalf("PutRun: %v", err)
	}

	for _, objectID := range objectIDs {
		got, err := reader.ListObjectReferences(ctx, objectID)
		if err != nil {
			t.Fatalf("ListObjectReferences(%s): %v", objectID, err)
		}
		if len(got) != 1 || got[0] != runID {
			t.Fatalf("ListObjectReferences(%s) = %v, want [%s]", objectID, got, runID)
		}
	}
	unreferenced := fmt.Sprintf("orkflows/provenance/%s/task/never/%s", runID, strings.Repeat("d", 64))
	if got, err := reader.ListObjectReferences(ctx, unreferenced); err != nil || len(got) != 0 {
		t.Fatalf("ListObjectReferences(unreferenced) = %v, %v; want empty", got, err)
	}

	mu.Lock()
	defer mu.Unlock()
	if operations["GetObject"] != 0 || operations["HeadObject"] != 0 {
		t.Fatalf("provenance listing read object bodies or heads: %v", operations)
	}
	if operations["ListObjectsV2"] != len(objectIDs)+1 {
		t.Fatalf("ListObjectsV2 calls = %d, want %d (one page per object): %v", operations["ListObjectsV2"], len(objectIDs)+1, operations)
	}
	t.Logf("EVIDENCE s3_provenance_keys_only objects=%d references_per_object=1 list_objects_v2=%d get_object=%d head_object=%d",
		len(objectIDs), operations["ListObjectsV2"], operations["GetObject"], operations["HeadObject"])
}

// TestS3RunArchive_OriginIndexAgainstMinIO writes runs from two triggers and a
// submit into real object storage and lists them back through the origin
// family: kind-only, kind and id, and a query with no origin filter that
// still sees every run through the run index.
func TestS3RunArchive_OriginIndexAgainstMinIO(t *testing.T) {
	archive, cfg := newS3RunArchive(t)
	ctx := context.Background()
	namespace := "origin-it-" + uniqueToken(t)[:8]
	triggerA := "trg-" + uniqueToken(t)[:12]
	triggerB := "trg-" + uniqueToken(t)[:12]
	base := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	trigger := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_TRIGGER
	submit := orkstackv1.RunOriginKind_RUN_ORIGIN_KIND_SUBMIT

	ids := map[string]string{}
	for i, seed := range []struct {
		name string
		kind orkstackv1.RunOriginKind
		id   string
	}{
		{"a1", trigger, triggerA}, {"a2", trigger, triggerA}, {"b1", trigger, triggerB}, {"s1", submit, ""}, {"legacy", 0, ""},
	} {
		runID := archiveTestRun(t, "-"+seed.name)
		cleanupArchivedRun(t, cfg, runID)
		ids[seed.name] = runID
		if err := archive.PutRun(ctx, artifacts.RunRecord{
			RunID: runID, Namespace: namespace, Orkflow: "deploy-service", Status: "succeeded",
			Time: base.Add(time.Duration(i) * time.Minute), OriginKind: seed.kind, OriginID: seed.id,
			JSONL: []byte(`{"kind":"run","run":{"id":"` + runID + `"}}` + "\n"),
		}); err != nil {
			t.Fatalf("PutRun(%s): %v", runID, err)
		}
	}
	list := func(q artifacts.RunQuery) []string {
		t.Helper()
		entries, err := archive.ListRuns(ctx, q)
		if err != nil {
			t.Fatalf("ListRuns(%+v): %v", q, err)
		}
		out := make([]string, 0, len(entries))
		for _, entry := range entries {
			out = append(out, entry.RunID)
		}
		return out
	}
	want := func(names ...string) []string {
		out := make([]string, 0, len(names))
		for _, name := range names {
			out = append(out, ids[name])
		}
		return out
	}
	if got := list(artifacts.RunQuery{Namespace: namespace, OriginKind: trigger}); !equalStrings(got, want("b1", "a2", "a1")) {
		t.Fatalf("kind-only = %v, want %v", got, want("b1", "a2", "a1"))
	}
	if got := list(artifacts.RunQuery{Namespace: namespace, OriginKind: trigger, OriginID: triggerA}); !equalStrings(got, want("a2", "a1")) {
		t.Fatalf("kind+id = %v, want %v", got, want("a2", "a1"))
	}
	if got := list(artifacts.RunQuery{Namespace: namespace, OriginKind: submit}); !equalStrings(got, want("s1")) {
		t.Fatalf("submit = %v, want %v", got, want("s1"))
	}
	if got := list(artifacts.RunQuery{Namespace: namespace}); !equalStrings(got, want("legacy", "s1", "b1", "a2", "a1")) {
		t.Fatalf("run index = %v, want every run", got)
	}
	key, _, err := artifacts.RunOriginIndexKey(artifacts.RunRecord{
		RunID: ids["a1"], Namespace: namespace, Orkflow: "deploy-service", Status: "succeeded",
		Time: base, OriginKind: trigger, OriginID: triggerA,
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := listKeys(t, ctx, newRawS3Client(t, cfg), cfg.S3Bucket, artifacts.RunOriginIndexNamespace+"/"+namespace+"/")
	found := false
	for _, k := range keys {
		if k == key {
			found = true
		}
	}
	if !found {
		t.Fatalf("origin row %q not in MinIO; keys under the namespace: %v", key, keys)
	}
	t.Logf("EVIDENCE origin_row=%s", key)
}

// TestS3RunArchive_RecentPaging is the newest-first family's paging-safety
// test against real object storage: page one of a namespace reads its page
// and boundary instant rather than the namespace, and paging visits every run
// exactly once, in exactly the order the full scan of the run index answers —
// ties included. The cursor carries the last entry's exact recent key, so a
// store that treats StartAfter as a prefix (SeaweedFS) cannot skip the
// boundary instant's remaining ties.
func TestS3RunArchive_RecentPaging(t *testing.T) {
	archive, cfg := newS3RunArchive(t)
	ctx := context.Background()
	client := newRawS3Client(t, cfg)

	namespace := "recent-" + uniqueToken(t)
	orkflows := []string{"f", "f-g", "nightly", "nightly-build"}
	statuses := []string{"succeeded", "failed", "canceled"}
	base := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	const runs, ties = 60, 5

	t.Cleanup(func() {
		for _, family := range []string{artifacts.RunIndexNamespace, artifacts.RunRecentIndexNamespace} {
			deleteByPrefix(t, ctx, client, cfg.S3Bucket, family+"/"+namespace+"/")
		}
	})
	for i := 0; i < runs; i++ {
		runID := archiveTestRun(t, fmt.Sprintf("-re-%02d", i))
		cleanupArchivedRun(t, cfg, runID)
		if err := archive.PutRun(ctx, artifacts.RunRecord{
			RunID:     runID,
			Namespace: namespace,
			Orkflow:   orkflows[i%len(orkflows)],
			Status:    statuses[i%len(statuses)],
			Time:      base.Add(time.Duration(i/ties) * time.Minute),
			JSONL:     []byte(runID + "\n"),
		}); err != nil {
			t.Fatalf("PutRun %q: %v", runID, err)
		}
	}

	page := func(q artifacts.RunQuery) []artifacts.RunIndexEntry {
		got, err := archive.ListRuns(ctx, q)
		if err != nil {
			t.Fatalf("ListRuns(%+v): %v", q, err)
		}
		return got
	}
	walk := func(q artifacts.RunQuery) []string {
		var ids []string
		for {
			got := page(q)
			for _, entry := range got {
				ids = append(ids, entry.RunID)
			}
			if len(got) < q.Limit {
				return ids
			}
			last := got[len(got)-1]
			q.BeforeTime, q.BeforeRunID = last.Time, last.RunID
			if q.Recent {
				key, err := artifacts.RunRecentIndexKeyForEntry(last)
				if err != nil {
					t.Fatalf("recent key for %s: %v", last.RunID, err)
				}
				q.BeforeKey = key
			}
		}
	}

	const limit = 7
	before := archive.IndexKeysScanned()
	first := page(artifacts.RunQuery{Namespace: namespace, Limit: limit, Recent: true})
	if scanned := archive.IndexKeysScanned() - before; scanned > limit+ties+1 {
		t.Errorf("page one scanned %d index keys, want at most %d", scanned, limit+ties+1)
	}
	// A page may exceed Limit by the boundary instant's ties: it is cut only
	// after a whole instant has been read, so none are lost to the cursor.
	if len(first) < limit || len(first) > limit+ties {
		t.Fatalf("page one holds %d runs, want %d or up to %d with the boundary instant's ties", len(first), limit, limit+ties)
	}

	for _, q := range []artifacts.RunQuery{
		{Namespace: namespace, Limit: limit},
		{Namespace: namespace, Orkflow: "f-g", Limit: 3},
		{Namespace: namespace, Status: "failed", Limit: 4},
	} {
		want := walk(q)
		recent := q
		recent.Recent = true
		got := walk(recent)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("query %+v: the newest-first walk listed\n%v\nthe run index lists\n%v", q, got, want)
		}
		seen := map[string]bool{}
		for _, id := range got {
			if seen[id] {
				t.Fatalf("query %+v: run %s listed twice", q, id)
			}
			seen[id] = true
		}
	}
	if all := walk(artifacts.RunQuery{Namespace: namespace, Limit: limit, Recent: true}); len(all) != runs {
		t.Fatalf("paging the namespace listed %d runs, want %d", len(all), runs)
	}
}
