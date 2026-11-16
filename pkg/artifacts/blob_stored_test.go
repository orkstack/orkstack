package artifacts

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestLocalBlobs_GetStoredAnswersWhenTheObjectWasWritten: a read answers the
// object's own instant, which lies between when its create was sent and
// when the create answered (verify 49 on the local backend), and a missing
// object is errBlobNotFound.
func TestLocalBlobs_GetStoredAnswersWhenTheObjectWasWritten(t *testing.T) {
	b, err := openLocalBlobs("test", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sent := time.Now()
	if err := b.create(ctx, "metadata/attested/ns/r/t/resolution.json", "application/json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	answered := time.Now()
	body, stored, err := b.getStored(ctx, "metadata/attested/ns/r/t/resolution.json", 0)
	if err != nil || string(body) != "{}" {
		t.Fatalf("getStored = %q, %v", body, err)
	}
	if stored.Before(sent.Truncate(time.Second)) || stored.After(answered) {
		t.Fatalf("stored at %s, not between %s and %s", stored, sent, answered)
	}
	if _, _, err := b.getStored(ctx, "metadata/attested/ns/r/t/none.json", 0); !errors.Is(err, errBlobNotFound) {
		t.Fatalf("a missing object = %v", err)
	}
}

// TestLocalBlobs_DelimitedListingFoldsATasksRecords: a listing of a run's
// attested prefix with delimiter "/" answers one entry per task, whatever
// it holds (commit, items, resume scopes, records), and the run's outputs
// as an object (verify 51 on the local backend).
func TestLocalBlobs_DelimitedListingFoldsATasksRecords(t *testing.T) {
	b, err := openLocalBlobs("test", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const ns, run = "ns", "ork-1"
	var keys []string
	add := func(k string, err error) {
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
	}
	commit, _ := CommitKey(ns, run, "build")
	add(AttestedCommitKey(commit))
	item, _ := ItemCommitKey(ns, run, "convert", 7)
	add(AttestedCommitKey(item))
	add(RecordKey(RecordResolution, ns, run, "approve", 0))
	add(RecordKey(RecordResolution, ns, run, "approve", 2))
	add(RecordKey(RecordComposed, ns, run, "child", 0))
	add(OutputsRecordKey(ns, run))
	for _, k := range keys {
		if err := b.create(ctx, k, "application/json", []byte("{}")); err != nil {
			t.Fatal(err)
		}
	}
	prefix, _ := AttestedRunPrefix(ns, run)
	page, err := b.list(ctx, blobListInput{Prefix: prefix, Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{prefix + "approve/", prefix + "build/", prefix + "child/", prefix + "convert/"}
	if len(page.Prefixes) != len(want) || len(page.Objects) != 1 || page.Objects[0].Key != prefix+"outputs.json" {
		t.Fatalf("listing = prefixes %v, objects %v", page.Prefixes, page.Objects)
	}
	for i, p := range want {
		if page.Prefixes[i] != p {
			t.Fatalf("prefix %d = %q, want %q", i, page.Prefixes[i], p)
		}
	}
}
