package artifacts

import (
	"context"
	"sort"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func sampleAttempt(runID, taskID string, attempt int32) *orkstackv1.TaskAttempt {
	return &orkstackv1.TaskAttempt{
		SchemaVersion: AttemptSchemaVersion,
		RunId:         runID,
		TaskId:        taskID,
		Attempt:       attempt,
		Outcome:       orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_FAILED,
		Error: &orkstackv1.Error{
			Code:      "CONTAINER_EXIT_NONZERO",
			Message:   "task \"flaky\": the container exited 1",
			Details:   "task \"flaky\"",
			Retryable: true,
			Category:  orkstackv1.FailureCategory_FAILURE_CATEGORY_ACTIVITY_FAILED,
		},
		ExitCode: 1,
		OrkerId:  "orker-a1",
		Pool:     "main",
		Phase:    "run",
		BuildId:  "build-7",
	}
}

// The attempt segment is zero-padded to the SAME width the log keys use, so a
// lexicographic listing is numeric order across both prefixes. Unpadded, "10"
// sorts before "2" and a ten-attempt task reads its timeline backwards.
func TestAttemptKey_PaddedSegmentMakesLexicographicOrderNumeric(t *testing.T) {
	keys := make([]string, 0, 4)
	for _, attempt := range []int32{2, 10, 1, 9} {
		key, err := AttemptKey(AttemptRef{RunID: "ork-1", TaskID: "flaky", Attempt: attempt})
		if err != nil {
			t.Fatalf("AttemptKey(%d): %v", attempt, err)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{
		"metadata/attempts/ork-1/flaky/0000000001.json",
		"metadata/attempts/ork-1/flaky/0000000002.json",
		"metadata/attempts/ork-1/flaky/0000000009.json",
		"metadata/attempts/ork-1/flaky/0000000010.json",
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("sorted keys = %q, want %q", keys, want)
		}
	}
	if attemptWidth != logAttemptWidth {
		t.Fatalf("attemptWidth = %d, logAttemptWidth = %d; the two prefixes must agree",
			attemptWidth, logAttemptWidth)
	}
}

// Attempt numbers are 1-based; 0 is a bug at the call site, not attempt zero.
func TestAttemptKey_RefusesANonPositiveAttempt(t *testing.T) {
	for _, attempt := range []int32{0, -1} {
		if _, err := AttemptKey(AttemptRef{RunID: "ork-1", TaskID: "t", Attempt: attempt}); err == nil {
			t.Fatalf("AttemptKey(attempt=%d) = nil error, want a refusal", attempt)
		}
	}
}

func TestParseAttemptKey_RoundTripsAndSkipsForeignKeys(t *testing.T) {
	ref := AttemptRef{RunID: "ork-2026/09/09-3", TaskID: "flaky", Attempt: 4}
	key, err := AttemptKey(ref)
	if err != nil {
		t.Fatalf("AttemptKey: %v", err)
	}
	got, ok := ParseAttemptKey(key)
	if !ok || got != ref {
		t.Fatalf("ParseAttemptKey(%q) = %+v, %v; want %+v, true", key, got, ok, ref)
	}

	// A key this package did not generate reports ok=false rather than an
	// error, so a scan skips foreign objects instead of failing on them.
	for _, foreign := range []string{
		"metadata/logs/ork-1/flaky/0000000001/00000000000000000001",
		"metadata/attempts/ork-1/flaky/0000000001",            // no suffix
		"metadata/attempts/ork-1/flaky/1.json",                // unpadded
		"metadata/attempts/ork-1/flaky/extra/0000000001.json", // too deep
		"objects/n-values/abc",
	} {
		if _, ok := ParseAttemptKey(foreign); ok {
			t.Errorf("ParseAttemptKey(%q) = ok, want it skipped", foreign)
		}
	}
}

// The bounds are what stop a workload-controlled string from turning a
// diagnosis record into an amplifier. A record already inside them is stored
// verbatim, marker and all.
func TestBoundAttempt_TruncatesTheErrorStringsAndLeavesTheRestAlone(t *testing.T) {
	record := sampleAttempt("ork-1", "flaky", 1)
	record.Error.Message = strings.Repeat("m", AttemptErrorMessageMaxBytes*2)
	record.Error.Details = strings.Repeat("d", AttemptErrorDetailsMaxBytes*2)

	bounded, err := BoundAttempt(record)
	if err != nil {
		t.Fatalf("BoundAttempt: %v", err)
	}
	if len(bounded.GetError().GetMessage()) > AttemptErrorMessageMaxBytes {
		t.Errorf("message = %d bytes, limit is %d",
			len(bounded.GetError().GetMessage()), AttemptErrorMessageMaxBytes)
	}
	if len(bounded.GetError().GetDetails()) > AttemptErrorDetailsMaxBytes {
		t.Errorf("details = %d bytes, limit is %d",
			len(bounded.GetError().GetDetails()), AttemptErrorDetailsMaxBytes)
	}
	if !strings.HasSuffix(bounded.GetError().GetMessage(), AttemptTruncationMarker) {
		t.Error("a truncated message carries no marker; a reader cannot tell it is short")
	}
	if !strings.HasSuffix(bounded.GetError().GetDetails(), AttemptTruncationMarker) {
		t.Error("truncated details carry no marker")
	}
	// The coordinates and the outcome are what the record exists for, and
	// bounding must never touch them.
	if bounded.GetOutcome() != record.GetOutcome() ||
		bounded.GetPhase() != record.GetPhase() ||
		bounded.GetError().GetCode() != record.GetError().GetCode() {
		t.Errorf("bounding changed the outcome, phase or code: %+v", bounded)
	}
	// The caller's own message is untouched: it may still be observed or
	// logged, and a store that rewrote it would be editing somebody else's
	// value.
	if len(record.GetError().GetMessage()) != AttemptErrorMessageMaxBytes*2 {
		t.Error("BoundAttempt mutated the record it was given")
	}
}

// Whatever the strings hold, the SERIALIZED record fits the cap — that is the
// bound the store enforces, and the per-field limits alone do not imply it.
func TestBoundAttempt_KeepsTheSerializedRecordUnderTheCap(t *testing.T) {
	record := sampleAttempt("ork-1", "flaky", 1)
	// Multi-byte runes, so a naive byte cut would land mid-rune, and enough of
	// them that message+details alone approach the whole-record cap.
	record.Error.Message = strings.Repeat("é", AttemptErrorMessageMaxBytes)
	record.Error.Details = strings.Repeat("→", AttemptErrorDetailsMaxBytes)

	document, err := MarshalAttempt(record)
	if err != nil {
		t.Fatalf("MarshalAttempt: %v", err)
	}
	if len(document) > AttemptMaxBytes {
		t.Fatalf("serialized record = %d bytes, cap is %d", len(document), AttemptMaxBytes)
	}
	decoded, err := UnmarshalAttempt(document)
	if err != nil {
		t.Fatalf("UnmarshalAttempt: %v", err)
	}
	if strings.Contains(decoded.GetError().GetMessage(), "�") {
		t.Error("truncation cut a rune in half")
	}
	if decoded.GetError().GetCode() != "CONTAINER_EXIT_NONZERO" {
		t.Errorf("code = %q, want the reason to survive every bound", decoded.GetError().GetCode())
	}
}

// A record with nothing to truncate round-trips byte for byte through the
// encoder pair, which is what lets a reader compare what it stored.
func TestMarshalAttempt_RoundTrips(t *testing.T) {
	record := sampleAttempt("ork-1", "flaky", 3)
	document, err := MarshalAttempt(record)
	if err != nil {
		t.Fatalf("MarshalAttempt: %v", err)
	}
	decoded, err := UnmarshalAttempt(document)
	if err != nil {
		t.Fatalf("UnmarshalAttempt: %v", err)
	}
	if !proto.Equal(record, decoded) {
		t.Fatalf("round trip changed the record:\n got %v\nwant %v", decoded, record)
	}
}

// The two in-tree AttemptStores answer identically. The local one is the
// backend a `make up` deployment runs on; the memory one is what the Orker's
// own tests write into, and a divergence there would make those tests prove
// nothing about production.
func TestAttemptStores_AnswerIdentically(t *testing.T) {
	local, err := NewLocalAttemptStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalAttemptStore: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })

	for name, store := range map[string]AttemptStore{
		"local":  local,
		"memory": NewMemoryAttemptStore(),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// A task with no records reads EMPTY, never an error: that is the
			// ordinary state of a task that has not run, and of every run that
			// finished before this key family existed.
			empty, err := store.ListAttempts(ctx, "ork-1", "flaky")
			if err != nil {
				t.Fatalf("ListAttempts on an empty task: %v", err)
			}
			if len(empty) != 0 {
				t.Fatalf("ListAttempts on an empty task = %d records, want 0", len(empty))
			}

			// Written out of order on purpose: the reader's contract is
			// ascending by attempt, not by write time.
			for _, attempt := range []int32{3, 1, 2} {
				record := sampleAttempt("ork-1", "flaky", attempt)
				if attempt == 3 {
					record.Outcome = orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED
					record.Error = nil
					record.ExitCode = 0
				}
				if err := store.PutAttempt(ctx, record); err != nil {
					t.Fatalf("PutAttempt(%d): %v", attempt, err)
				}
			}
			// A second task of the same run must not leak into the first's
			// timeline.
			if err := store.PutAttempt(ctx, sampleAttempt("ork-1", "other", 1)); err != nil {
				t.Fatalf("PutAttempt for a sibling task: %v", err)
			}

			got, err := store.ListAttempts(ctx, "ork-1", "flaky")
			if err != nil {
				t.Fatalf("ListAttempts: %v", err)
			}
			if len(got) != 3 {
				t.Fatalf("ListAttempts = %d records, want 3", len(got))
			}
			for i, record := range got {
				if record.GetAttempt() != int32(i+1) {
					t.Fatalf("record %d has attempt %d; want ascending 1,2,3", i, record.GetAttempt())
				}
				if record.GetTaskId() != "flaky" {
					t.Fatalf("record %d belongs to task %q", i, record.GetTaskId())
				}
			}
			if got[2].GetOutcome() != orkstackv1.AttemptOutcome_ATTEMPT_OUTCOME_SUCCEEDED {
				t.Errorf("attempt 3 outcome = %v, want SUCCEEDED", got[2].GetOutcome())
			}
			if got[0].GetError().GetCode() != "CONTAINER_EXIT_NONZERO" {
				t.Errorf("attempt 1 error code = %q", got[0].GetError().GetCode())
			}

			// Temporal never runs one attempt number twice, so a repeat write
			// is last-writer-wins rather than an arbitration point — and it
			// must not produce a second row.
			again := sampleAttempt("ork-1", "flaky", 1)
			again.Phase = "commit"
			if err := store.PutAttempt(ctx, again); err != nil {
				t.Fatalf("PutAttempt (repeat): %v", err)
			}
			after, err := store.ListAttempts(ctx, "ork-1", "flaky")
			if err != nil {
				t.Fatalf("ListAttempts after repeat: %v", err)
			}
			if len(after) != 3 {
				t.Fatalf("a repeat write produced %d records, want 3", len(after))
			}
			if after[0].GetPhase() != "commit" {
				t.Errorf("attempt 1 phase = %q, want the last write to win", after[0].GetPhase())
			}
		})
	}
}

// A nil record is a programming error at the call site, refused rather than
// stored as an empty row that would read as an attempt nobody made.
func TestAttemptStore_RefusesANilRecord(t *testing.T) {
	store := NewMemoryAttemptStore()
	if err := store.PutAttempt(context.Background(), nil); err == nil {
		t.Fatal("PutAttempt(nil) = nil error, want a refusal")
	}
}

// One ORK_ARTIFACT_BACKEND selects where artifacts, logs, snapshots AND
// attempt records live. A backend that can hold logs but not attempts would
// silently drop retry history on exactly the deployments that configured
// storage correctly.
func TestAttemptStoreBackends_MatchTheLogStoreBackends(t *testing.T) {
	attempts := strings.Join(AttemptStoreBackends(), ",")
	logs := strings.Join(LogStoreBackends(), ",")
	if attempts != logs {
		t.Fatalf("attempt store backends = %q, log store backends = %q", attempts, logs)
	}
}
