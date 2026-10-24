package artifacts_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/orkstack/orkstack/pkg/artifacts"
)

// The log chunk key is the whole ordering contract. A reader lists a prefix
// and sends what it finds; if lexicographic order were not numeric order, a
// task's 10th chunk would arrive before its 2nd and nobody would notice until
// a log read like a shuffled deck.
func TestLogChunkKey_SortsLexicographicallyInSequenceOrder(t *testing.T) {
	ref := artifacts.LogRef{RunID: "ork-1786856120616785000-1", TaskID: "build", Attempt: 1}

	var keys []string
	for _, seq := range []int64{1, 2, 9, 10, 11, 100, 1000, 999999} {
		key, err := artifacts.LogChunkKey(ref, seq, false)
		if err != nil {
			t.Fatalf("LogChunkKey(seq=%d): %v", seq, err)
		}
		keys = append(keys, key)
	}
	for i := 1; i < len(keys); i++ {
		if !(keys[i-1] < keys[i]) {
			t.Errorf("key %q does not sort before %q; listing order would not be sequence order",
				keys[i-1], keys[i])
		}
	}

	// The prefix is the one the design fixes, and the sequence is padded so
	// the ordering above is a property of the key rather than of a listing.
	first := keys[0]
	want := "metadata/logs/ork-1786856120616785000-1/build/0000000001/00000000000000000001"
	if first != want {
		t.Errorf("LogChunkKey(seq=1) = %q, want %q", first, want)
	}
}

// A truncation notice must sort after every ordinary chunk of the same
// sequence, or a resume that starts after seq N could hand N's notice out
// twice.
func TestLogChunkKey_TruncationNoticeSortsAfterItsSequence(t *testing.T) {
	ref := artifacts.LogRef{RunID: "run", TaskID: "task", Attempt: 2}
	plain, err := artifacts.LogChunkKey(ref, 7, false)
	if err != nil {
		t.Fatalf("LogChunkKey: %v", err)
	}
	notice, err := artifacts.LogChunkKey(ref, 7, true)
	if err != nil {
		t.Fatalf("LogChunkKey(truncated): %v", err)
	}
	if !(plain < notice) {
		t.Errorf("plain key %q must sort before notice key %q", plain, notice)
	}
	next, err := artifacts.LogChunkKey(ref, 8, false)
	if err != nil {
		t.Fatalf("LogChunkKey: %v", err)
	}
	if !(notice < next) {
		t.Errorf("notice key %q must sort before the next sequence %q", notice, next)
	}
}

func TestParseLogChunkKey_RoundTripsEveryCoordinate(t *testing.T) {
	cases := []struct {
		name      string
		ref       artifacts.LogRef
		seq       int64
		truncated bool
	}{
		{"plain", artifacts.LogRef{RunID: "ork-1-1", TaskID: "build", Attempt: 1}, 1, false},
		{"notice", artifacts.LogRef{RunID: "ork-1-1", TaskID: "build", Attempt: 3}, 42, true},
		{"awkward ids", artifacts.LogRef{RunID: "a/b", TaskID: "..", Attempt: 7}, 9, false},
		{"unicode task", artifacts.LogRef{RunID: "run", TaskID: "tâche", Attempt: 1}, 5, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, err := artifacts.LogChunkKey(tc.ref, tc.seq, tc.truncated)
			if err != nil {
				t.Fatalf("LogChunkKey: %v", err)
			}
			ref, seq, truncated, ok := artifacts.ParseLogChunkKey(key)
			if !ok {
				t.Fatalf("ParseLogChunkKey(%q) reported not-ok", key)
			}
			if ref != tc.ref || seq != tc.seq || truncated != tc.truncated {
				t.Errorf("round trip of %q gave (%+v, %d, %v), want (%+v, %d, %v)",
					key, ref, seq, truncated, tc.ref, tc.seq, tc.truncated)
			}
		})
	}
}

func TestParseLogChunkKey_RejectsForeignKeys(t *testing.T) {
	for _, key := range []string{
		"",
		"metadata/runs/ork-1-1.jsonl",
		"metadata/logs/run/task/0000000001", // no sequence segment
		"metadata/logs/run/task/0000000001/00000000000000000001/x", // too deep
		"metadata/logs/run/task/notanumber/00000000000000000001",
		"metadata/logs/run/task/0000000001/notanumber",
		"metadata/logs/run/task/0000000001/00000000000000000001.gz",
	} {
		if _, _, _, ok := artifacts.ParseLogChunkKey(key); ok {
			t.Errorf("ParseLogChunkKey(%q) accepted a key this package never wrote", key)
		}
	}
}

// A sequence that cannot be rendered in the fixed width would break the
// ordering silently, so it is refused loudly.
func TestLogChunkKey_RejectsUnrepresentableCoordinates(t *testing.T) {
	ref := artifacts.LogRef{RunID: "run", TaskID: "task", Attempt: 1}
	for _, seq := range []int64{0, -1} {
		if _, err := artifacts.LogChunkKey(ref, seq, false); err == nil {
			t.Errorf("LogChunkKey(seq=%d) succeeded; sequences are 1-based", seq)
		}
	}
	if _, err := artifacts.LogChunkKey(artifacts.LogRef{RunID: "", TaskID: "t", Attempt: 1}, 1, false); err == nil {
		t.Error("LogChunkKey with an empty run id succeeded")
	}
	if _, err := artifacts.LogChunkKey(artifacts.LogRef{RunID: "r", TaskID: "t", Attempt: 0}, 1, false); err == nil {
		t.Error("LogChunkKey with attempt 0 succeeded; 0 means `the latest`, never a stored attempt")
	}
}

// The local store is the one a developer runs with no infrastructure at all,
// so its behavior is the contract every backend answers.
func TestLocalLogStore_ReadsChunksInOrderAndResumesWithoutRepeating(t *testing.T) {
	store := newLocalLogStore(t)
	ctx := context.Background()
	ref := artifacts.LogRef{RunID: "ork-1-1", TaskID: "build", Attempt: 1}

	for seq := int64(1); seq <= 12; seq++ {
		if err := store.PutLogChunk(ctx, artifacts.LogChunk{
			LogRef: ref, Seq: seq, Data: []byte(fmt.Sprintf("line %d\n", seq)),
		}); err != nil {
			t.Fatalf("PutLogChunk(%d): %v", seq, err)
		}
	}

	all, err := store.ReadLogChunks(ctx, ref, 0, 0, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks: %v", err)
	}
	if len(all) != 12 {
		t.Fatalf("read %d chunks, want 12", len(all))
	}
	for i, chunk := range all {
		wantSeq := int64(i + 1)
		if chunk.Seq != wantSeq {
			t.Fatalf("chunk %d has seq %d, want %d: the stream is not in order", i, chunk.Seq, wantSeq)
		}
		if want := fmt.Sprintf("line %d\n", wantSeq); string(chunk.Data) != want {
			t.Errorf("chunk %d carries %q, want %q", i, chunk.Data, want)
		}
	}

	// Resume: everything after the 5th, nothing repeated, nothing skipped.
	resumed, err := store.ReadLogChunks(ctx, ref, 5, 0, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks(after 5): %v", err)
	}
	if len(resumed) != 7 || resumed[0].Seq != 6 || resumed[len(resumed)-1].Seq != 12 {
		t.Fatalf("resume after seq 5 returned %d chunks spanning %d..%d, want 7 spanning 6..12",
			len(resumed), resumed[0].Seq, resumed[len(resumed)-1].Seq)
	}

	// A limit bounds one read, and the next resume continues exactly where it
	// stopped. That is what keeps an engine's memory to one page.
	page, err := store.ReadLogChunks(ctx, ref, 0, 3, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks(limit 3): %v", err)
	}
	if len(page) != 3 || page[2].Seq != 3 {
		t.Fatalf("limited read returned %d chunks ending at %d, want 3 ending at 3", len(page), page[2].Seq)
	}
}

// A COUNT IS NOT A SIZE. A chunk is whatever the Orker flushed, so a page of
// N chunks is a page of an amount of memory the reader does not decide. The
// byte budget is the bound that holds whatever the writer's flush bounds are,
// and it stops a read WITHOUT losing anything: the next read resumes at the
// sequence the last one ended on.
func TestLocalLogStore_AReadStopsAtItsByteBudgetAndResumesWhereItStopped(t *testing.T) {
	store := newLocalLogStore(t)
	ctx := context.Background()
	ref := artifacts.LogRef{RunID: "ork-1-1", TaskID: "fat", Attempt: 1}

	const (
		chunkBytes = 1000
		chunks     = 6
	)
	for seq := int64(1); seq <= chunks; seq++ {
		if err := store.PutLogChunk(ctx, artifacts.LogChunk{
			LogRef: ref, Seq: seq, Data: bytes.Repeat([]byte("x"), chunkBytes),
		}); err != nil {
			t.Fatalf("PutLogChunk(%d): %v", seq, err)
		}
	}

	// 2500 bytes of budget over 1000-byte chunks: three chunks, because the
	// third is the one that crosses it and a chunk is never returned in half.
	page, err := store.ReadLogChunks(ctx, ref, 0, 0, 2500)
	if err != nil {
		t.Fatalf("ReadLogChunks(maxBytes 2500): %v", err)
	}
	if len(page) != 3 {
		t.Fatalf("a 2500-byte budget returned %d chunks of %d bytes, want 3",
			len(page), chunkBytes)
	}

	// Nothing was lost: the rest is there, from the sequence the page ended on.
	rest, err := store.ReadLogChunks(ctx, ref, page[len(page)-1].Seq, 0, 2500)
	if err != nil {
		t.Fatalf("ReadLogChunks(resume): %v", err)
	}
	if len(rest) != 3 || rest[0].Seq != 4 {
		t.Fatalf("the resumed read returned %d chunks starting at %d, want 3 starting at 4",
			len(rest), rest[0].Seq)
	}

	// A chunk larger than the whole budget is still delivered WHOLE. A page
	// that could come back empty would stall a reader on a chunk it can never
	// receive.
	big := artifacts.LogRef{RunID: "ork-1-1", TaskID: "one-big-line", Attempt: 1}
	if err := store.PutLogChunk(ctx, artifacts.LogChunk{
		LogRef: big, Seq: 1, Data: bytes.Repeat([]byte("y"), 4096),
	}); err != nil {
		t.Fatalf("PutLogChunk(big): %v", err)
	}
	only, err := store.ReadLogChunks(ctx, big, 0, 0, 16)
	if err != nil {
		t.Fatalf("ReadLogChunks(tiny budget): %v", err)
	}
	if len(only) != 1 || len(only[0].Data) != 4096 {
		t.Fatalf("a 16-byte budget returned %d chunks; the first chunk must arrive whole",
			len(only))
	}
}

func TestLocalLogStore_TruncationNoticeIsReadBackAsTruncated(t *testing.T) {
	store := newLocalLogStore(t)
	ctx := context.Background()
	ref := artifacts.LogRef{RunID: "ork-1-1", TaskID: "chatty", Attempt: 1}

	if err := store.PutLogChunk(ctx, artifacts.LogChunk{
		LogRef: ref, Seq: 1, Data: []byte("hello\n"),
	}); err != nil {
		t.Fatalf("PutLogChunk: %v", err)
	}
	if err := store.PutLogChunk(ctx, artifacts.LogChunk{
		LogRef: ref, Seq: 2, Data: []byte("[log truncated]\n"), Truncated: true,
	}); err != nil {
		t.Fatalf("PutLogChunk(truncated): %v", err)
	}

	chunks, err := store.ReadLogChunks(ctx, ref, 0, 0, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("read %d chunks, want 2", len(chunks))
	}
	if chunks[0].Truncated {
		t.Error("the first chunk is marked truncated")
	}
	if !chunks[1].Truncated {
		t.Error("the truncation notice came back unmarked; truncation would be silent")
	}
	if !strings.Contains(string(chunks[1].Data), "truncated") {
		t.Errorf("the notice reads %q; it must say so in words", chunks[1].Data)
	}
}

// Attempt 0 means "the latest one that wrote anything", which is what a caller
// who never saw a retry asks for. It is answered from the store, because the
// store is the only thing that knows which attempts produced output.
func TestLocalLogStore_LatestAttemptIsTheHighestThatWroteAnything(t *testing.T) {
	store := newLocalLogStore(t)
	ctx := context.Background()

	if _, err := store.LatestLogAttempt(ctx, "ork-1-1", "flaky", artifacts.NoItem); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatalf("LatestLogAttempt on a task with no logs = %v, want ErrNotFound", err)
	}

	for _, attempt := range []int32{1, 2, 10} {
		if err := store.PutLogChunk(ctx, artifacts.LogChunk{
			LogRef: artifacts.LogRef{RunID: "ork-1-1", TaskID: "flaky", Attempt: attempt},
			Seq:    1, Data: []byte("try\n"),
		}); err != nil {
			t.Fatalf("PutLogChunk(attempt %d): %v", attempt, err)
		}
	}
	latest, err := store.LatestLogAttempt(ctx, "ork-1-1", "flaky", artifacts.NoItem)
	if err != nil {
		t.Fatalf("LatestLogAttempt: %v", err)
	}
	if latest != 10 {
		t.Errorf("LatestLogAttempt = %d, want 10: attempts are compared numerically, not as text", latest)
	}
}

// One task's chunks must never appear in another's stream, and a run id that
// is a prefix of another must not leak either.
func TestLocalLogStore_KeepsAttemptsAndTasksApart(t *testing.T) {
	store := newLocalLogStore(t)
	ctx := context.Background()

	write := func(runID, taskID string, attempt int32, body string) {
		t.Helper()
		if err := store.PutLogChunk(ctx, artifacts.LogChunk{
			LogRef: artifacts.LogRef{RunID: runID, TaskID: taskID, Attempt: attempt},
			Seq:    1, Data: []byte(body),
		}); err != nil {
			t.Fatalf("PutLogChunk: %v", err)
		}
	}
	write("ork-1", "build", 1, "one\n")
	write("ork-1", "build", 2, "two\n")
	write("ork-1", "buildx", 1, "other task\n")
	write("ork-11", "build", 1, "other run\n")

	chunks, err := store.ReadLogChunks(ctx,
		artifacts.LogRef{RunID: "ork-1", TaskID: "build", Attempt: 1}, 0, 0, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks: %v", err)
	}
	if len(chunks) != 1 || !bytes.Equal(chunks[0].Data, []byte("one\n")) {
		t.Fatalf("attempt 1 of ork-1/build read %d chunks (%q); the coordinates leak",
			len(chunks), chunkBodies(chunks))
	}
}

// Reading an attempt nothing ever wrote is empty, not an error: a task that
// produced no output is normal, and the UI opens the pane unconditionally.
func TestLocalLogStore_UnknownAttemptReadsEmpty(t *testing.T) {
	store := newLocalLogStore(t)
	chunks, err := store.ReadLogChunks(context.Background(),
		artifacts.LogRef{RunID: "nothing", TaskID: "here", Attempt: 1}, 0, 0, 0)
	if err != nil {
		t.Fatalf("ReadLogChunks on an unknown attempt: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("read %d chunks from an attempt nothing wrote", len(chunks))
	}
}

func newLocalLogStore(t *testing.T) *artifacts.LocalLogStore {
	t.Helper()
	store, err := artifacts.NewLocalLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalLogStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func chunkBodies(chunks []artifacts.LogChunk) string {
	var out []string
	for _, chunk := range chunks {
		out = append(out, string(chunk.Data))
	}
	return strings.Join(out, "|")
}
