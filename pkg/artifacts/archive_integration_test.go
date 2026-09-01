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
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

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
		for _, prefix := range []string{artifacts.RunIndexNamespace + "/", artifacts.RunObjectRefNamespace + "/"} {
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

// TestS3RunArchive_WritesRunJSONLAtTheContractPath is acceptance 1 against real
// object storage: the body lands at s3://{bucket}/metadata/runs/{run-id}.jsonl,
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

// TestS3RunArchive_PrefixScanFiltersByOrkflowStatusAndDateRange is acceptance 2
// at the storage layer: real ListObjectsV2 prefix scans answer the query, and
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
	widest := samples[len(samples)-1]
	if widest.concurrent >= widest.serial {
		t.Fatalf("bounded concurrency bought nothing at %d objects: concurrent=%s serial=%s",
			widest.objects, widest.concurrent, widest.serial)
	}
	t.Logf("EVIDENCE s3_marker_write_scaling %s", strings.Join(evidence, " | "))
}
