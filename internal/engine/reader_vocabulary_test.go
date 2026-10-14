package engine

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// internalWords are what AGENTS.md §7 (amendment A2) forbids a user-facing
// surface — an error message included — to name: a generation, its
// execution, a checkpoint or a carry. A user reads "saved state" and
// "earlier events" instead. The reader's errors reach a client unchanged
// (LookupRun, the event stream), and a lost segment's reason is written into
// the archive's gap line, the DataLoss message and an evidence bundle's
// event_gaps forever, so a wrong word there can never be taken back.
var internalWords = []string{"generation", "checkpoint", "carry", "execution"}

// continuationKeyName is the last segment of a continuation object's key,
// which spells its generation and kind; a user-facing error never quotes it.
var continuationKeyName = regexp.MustCompile(`\d{6}-(checkpoint|events|snapshot)-[0-9a-f]{64}`)

func requireRunVocabulary(t *testing.T, where, text string) {
	t.Helper()
	lower := strings.ToLower(text)
	for _, word := range internalWords {
		if strings.Contains(lower, word) {
			t.Errorf("%s names %q: %s", where, word, text)
		}
	}
	if name := continuationKeyName.FindString(text); name != "" {
		t.Errorf("%s quotes the object %s: %s", where, name, text)
	}
}

// storeDownFor is a transient store failure worded the way a real store's
// is: it quotes the object's URI, and the bucket path the request went to.
func storeDownFor(uri string) error {
	key := strings.TrimPrefix(uri, "s3://test/")
	return fmt.Errorf(`artifacts: s3 snapshot %q: operation error S3: GetObject: Get "http://minio:9000/test/%s": dial tcp: connection refused`, uri, key)
}

func TestReaderErrors_NameNoGenerationCheckpointOrCarry(t *testing.T) {
	retarget := func(uri string) string {
		return strings.Replace(uri, "/"+chainRunID+"/", "/ork-1700000000-8/", 1)
	}
	recarry := func(mutate func(c *orkstackv1.RunContinuation)) func(f *chainFixture) {
		return func(f *chainFixture) {
			carry := proto.Clone(f.carry).(*orkstackv1.RunContinuation)
			mutate(carry)
			f.client.generations[f.runID][f.current].events[0] = f.started(carry)
		}
	}
	alter := func(uri func(f *chainFixture) string, change func([]byte) []byte) func(f *chainFixture) {
		return func(f *chainFixture) {
			data, _ := f.store.get(uri(f))
			f.store.put(uri(f), change(append([]byte(nil), data...)))
		}
	}
	segment := func(i int) func(f *chainFixture) string {
		return func(f *chainFixture) string { return f.carry.GetSegments()[i].GetObject().GetUri() }
	}
	checkpoint := func(f *chainFixture) string { return f.carry.GetCheckpoint().GetUri() }
	snapshot := func(f *chainFixture) string { return f.carry.GetSnapshot().GetUri() }
	flip := func(data []byte) []byte { data[len(data)/2] ^= 0xff; return data }
	grow := func(data []byte) []byte { return append(data, 0) }
	both := func(steps ...func(f *chainFixture)) func(f *chainFixture) {
		return func(f *chainFixture) {
			for _, step := range steps {
				step(f)
			}
		}
	}
	missing := func(uri func(f *chainFixture) string) func(f *chainFixture) {
		return func(f *chainFixture) { delete(f.store.objects, uri(f)) }
	}
	down := func(uri func(f *chainFixture) string) func(f *chainFixture) {
		return func(f *chainFixture) { f.store.fail[uri(f)] = storeDownFor(uri(f)) }
	}

	scenarios := map[string]func(f *chainFixture){
		// Every rule validateCarry checks before anything is read.
		"numbered before the start": recarry(func(c *orkstackv1.RunContinuation) { c.Generation = 0 }),
		"too few event ranges":      recarry(func(c *orkstackv1.RunContinuation) { c.Segments = c.Segments[:1] }),
		"event range misnumbered":   recarry(func(c *orkstackv1.RunContinuation) { c.Segments[1].Generation = 7 }),
		"event range gap":           recarry(func(c *orkstackv1.RunContinuation) { c.Segments[1].FirstSequence++ }),
		"event range overfull":      recarry(func(c *orkstackv1.RunContinuation) { c.Segments[0].EventCount = 99 }),
		"another run's events": recarry(func(c *orkstackv1.RunContinuation) {
			c.Segments[0].Object.Uri = retarget(c.Segments[0].Object.Uri)
		}),
		"last sequence":           recarry(func(c *orkstackv1.RunContinuation) { c.EventSequence++ }),
		"no saved-state parts":    recarry(func(c *orkstackv1.RunContinuation) { c.Checkpoint.Parts = nil }),
		"saved-state part gap":    recarry(func(c *orkstackv1.RunContinuation) { c.Checkpoint.Parts[1].Offset++ }),
		"saved-state parts short": recarry(func(c *orkstackv1.RunContinuation) { c.Checkpoint.SizeBytes++ }),
		"another run's saved state": recarry(func(c *orkstackv1.RunContinuation) {
			c.Checkpoint.Uri = retarget(c.Checkpoint.Uri)
		}),
		"another run's full copy": recarry(func(c *orkstackv1.RunContinuation) { c.Snapshot.Uri = retarget(c.Snapshot.Uri) }),
		// Every way an earlier generation's events fail to read.
		"events missing":    missing(segment(0)),
		"events altered":    alter(segment(0), flip),
		"events grown":      alter(segment(1), grow),
		"events store down": down(segment(1)),
		// Every way the saved state fails to read.
		"saved state altered":    alter(checkpoint, flip),
		"saved state store down": down(checkpoint),
		"full copy altered":      both(missing(checkpoint), alter(snapshot, grow)),
	}
	for name, damage := range scenarios {
		t.Run(name, func(t *testing.T) {
			f := newChain(t, chainSpec{closed: []int{5, 4}, current: 0})
			damage(f)
			ctx := context.Background()
			seen := 0
			if _, err := f.reader.GetRun(ctx, f.runID); err != nil {
				seen++
				requireRunVocabulary(t, "GetRun", err.Error())
			}
			if _, err := f.reader.Events(ctx, f.runID, 0); err != nil {
				seen++
				requireRunVocabulary(t, "Events", err.Error())
			}
			if err := f.reader.StreamEvents(ctx, f.runID, 0, func(*orkstackv1.Event) error { return errStopStream }); err != nil &&
				!errors.Is(err, errStopStream) {
				seen++
				requireRunVocabulary(t, "StreamEvents", err.Error())
			}
			_, gaps, err := f.reader.EventsForArchive(ctx, f.runID)
			if err != nil {
				seen++
				requireRunVocabulary(t, "EventsForArchive", err.Error())
			}
			for _, gap := range gaps {
				seen++
				requireRunVocabulary(t, "an archived gap's reason", gap.Reason)
			}
			if seen == 0 {
				t.Fatal("the damage produced no error and no gap: the scenario proves nothing")
			}
		})
	}
}

func TestContinuationDecoders_NameNoGenerationCheckpointOrCarry(t *testing.T) {
	run := checkpointRunForTest()
	parts := checkpointPartsForTest(run, 1)
	withHeader := make([]*orkstackv1.RunCheckpointPart, len(parts))
	for i, part := range parts {
		withHeader[i] = proto.Clone(part).(*orkstackv1.RunCheckpointPart)
	}
	withHeader[2].Header = &orkstackv1.Run{Id: "ork-1-2"}
	repeated := append(append([]*orkstackv1.RunCheckpointPart(nil), parts...), proto.Clone(parts[1]).(*orkstackv1.RunCheckpointPart))
	short := append([]*orkstackv1.RunCheckpointPart(nil), parts[:len(parts)-1]...)
	data, obj := checkpointObjectForTest(t, checkpointPartsForTest(run, 2), "team-a", "ork-1-2", 0)
	partDigest := proto.Clone(obj).(*orkstackv1.ContinuationObject)
	partDigest.Parts[0].Sha256 = strings.Repeat("0", 64)
	snapData, snapObj := gzipObjectForTest(t, run, "team-a", "ork-1-2", 0, artifacts.ContinuationKindSnapshot)
	segData, segObj := gzipObjectForTest(t, &orkstackv1.RunEventSegment{RunId: "ork-1-2"}, "team-a", "ork-1-2", 0,
		artifacts.ContinuationKindEvents)
	plain := []byte("plain")
	plainObj := &orkstackv1.ContinuationObject{SizeBytes: int64(len(plain)), Sha256: artifacts.ContinuationSHA256(plain)}

	failures := map[string]func() error{
		"no header":          func() error { _, err := CheckpointRun(parts[1:]...); return err },
		"header on a part":   func() error { _, err := CheckpointRun(withHeader...); return err },
		"repeated rows":      func() error { _, err := CheckpointRun(repeated...); return err },
		"an item short":      func() error { _, err := CheckpointRun(short...); return err },
		"saved state size":   func() error { _, err := DecodeCheckpoint(data[:len(data)-1], obj); return err },
		"saved state digest": func() error { _, err := DecodeCheckpoint(data, partDigest); return err },
		"full copy size":     func() error { _, err := DecodeSwitchSnapshot(append(snapData, 0), snapObj); return err },
		"full copy gzip":     func() error { _, err := DecodeSwitchSnapshot(plain, plainObj); return err },
		"full copy unnamed":  func() error { _, err := DecodeSwitchSnapshot(snapData, nil); return err },
		"events digest":      func() error { _, err := DecodeEventSegment(segData, snapObj); return err },
		"events gzip":        func() error { _, err := DecodeEventSegment(plain, plainObj); return err },
		"events size":        func() error { _, err := DecodeEventSegment(segData[1:], segObj); return err },
	}
	for name, decode := range failures {
		err := decode()
		if !errors.Is(err, ErrContinuationObjectInvalid) {
			t.Fatalf("%s: err = %v, want ErrContinuationObjectInvalid", name, err)
		}
		requireRunVocabulary(t, name, err.Error())
	}
	requireRunVocabulary(t, "ErrContinuationObjectInvalid", ErrContinuationObjectInvalid.Error())
	requireRunVocabulary(t, "ErrEventSegmentLost", ErrEventSegmentLost.Error())
}

// TestWithoutObjectName_KeepsTheErrorChain: the rewritten store error still
// reads as NotFound when it was one, so a missing object keeps every
// fallback and every mapping it had.
func TestWithoutObjectName_KeepsTheErrorChain(t *testing.T) {
	uri := "s3://test/metadata/snapshots/team-a/ork-1-2/continuations/000001-checkpoint-" + strings.Repeat("a", 64) + ".pb.gz"
	err := withoutObjectName(fmt.Errorf("mem snapshot %q: %w", uri, artifacts.ErrNotFound), uri, subjectSavedState)
	if !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("rewritten error %v no longer matches ErrNotFound", err)
	}
	if want := `mem snapshot "s3://test/metadata/snapshots/team-a/ork-1-2/continuations/<saved state>": `; !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("rewritten error = %q, want it to start %q", err, want)
	}
	plain := errors.New("connection refused")
	if got := withoutObjectName(plain, uri, subjectSavedState); got != plain {
		t.Fatalf("an error that does not quote the object was rewritten: %v", got)
	}
}
