package artifacts

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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

// The per-field limits keep the strings short, but not their ENCODING: a
// control character costs six bytes as JSON. A record still over the cap after
// those limits gives up its details, then its message, to the marker alone —
// and keeps the code, the phase and the coordinates that say which attempt
// failed and why. An empty field stays empty rather than claiming a cut.
func TestBoundAttempt_ShrinksDetailsThenMessageToFitTheCap(t *testing.T) {
	escaped := func(n int) string { return strings.Repeat("\x01", n) }
	for _, tc := range []struct {
		name                 string
		message, details     string
		orkerID              string
		wantMessage, wantDet string
	}{
		{
			name:    "the details alone go",
			message: "task \"flaky\": the container exited 1", details: escaped(AttemptErrorDetailsMaxBytes),
			wantMessage: "task \"flaky\": the container exited 1", wantDet: AttemptTruncationMarker,
		},
		{
			name:    "then the message",
			message: escaped(AttemptErrorMessageMaxBytes), details: escaped(AttemptErrorDetailsMaxBytes),
			orkerID:     strings.Repeat("o", 42000),
			wantMessage: AttemptTruncationMarker, wantDet: AttemptTruncationMarker,
		},
		{
			name:    "empty details stay empty",
			message: escaped(AttemptErrorMessageMaxBytes), details: "",
			orkerID:     strings.Repeat("o", 42000),
			wantMessage: AttemptTruncationMarker, wantDet: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := sampleAttempt("ork-1", "flaky", 2)
			record.Error.Message, record.Error.Details = tc.message, tc.details
			if tc.orkerID != "" {
				record.OrkerId = tc.orkerID
			}
			bounded, err := BoundAttempt(record)
			if err != nil {
				t.Fatalf("BoundAttempt: %v", err)
			}
			if got := bounded.GetError().GetDetails(); got != tc.wantDet {
				t.Errorf("details = %d bytes %q..., want %q", len(got), prefix(got, 20), tc.wantDet)
			}
			if got := bounded.GetError().GetMessage(); got != tc.wantMessage {
				t.Errorf("message = %d bytes %q..., want %q", len(got), prefix(got, 20), tc.wantMessage)
			}
			if bounded.GetError().GetCode() != "CONTAINER_EXIT_NONZERO" || bounded.GetPhase() != "run" ||
				bounded.GetAttempt() != 2 || bounded.GetOrkerId() != record.GetOrkerId() {
				t.Errorf("bounded record lost a field the ladder must keep: %v", bounded)
			}
			document, err := MarshalAttempt(record)
			if err != nil {
				t.Fatalf("MarshalAttempt: %v", err)
			}
			if len(document) > AttemptMaxBytes {
				t.Fatalf("serialized record = %d bytes, cap is %d", len(document), AttemptMaxBytes)
			}
			if record.GetError().GetMessage() != tc.message || record.GetError().GetDetails() != tc.details {
				t.Error("BoundAttempt mutated the record it was given")
			}
		})
	}
}

func prefix(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
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
			empty, err := store.ListAttempts(ctx, "ork-1", "flaky", NoItem)
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

			got, err := store.ListAttempts(ctx, "ork-1", "flaky", NoItem)
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
			after, err := store.ListAttempts(ctx, "ork-1", "flaky", NoItem)
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

// sampleSpans is a closed §7.1 span list, the shape the Orker writes.
func sampleSpans() []*orkstackv1.PhaseSpan {
	base := time.Date(2026, 9, 22, 9, 14, 4, 0, time.UTC)
	names := []string{"start", "commit-check", "workspace", "stage-inputs", "image", "render", "create", "run", "exit"}
	spans := make([]*orkstackv1.PhaseSpan, len(names))
	at := base
	for i, name := range names {
		next := at.Add(time.Duration(i+1) * 17 * time.Millisecond)
		spans[i] = &orkstackv1.PhaseSpan{
			Phase:      name,
			StartedAt:  timestamppb.New(at),
			FinishedAt: timestamppb.New(next),
		}
		at = next
	}
	return spans
}

// TestAttemptRecord_PhasesRoundTripThroughLocalAndS3Stores is S1's unit half:
// a record's phase spans survive the encoder and every in-tree store without
// a code change in this package — the field is additive. The S3 half is
// TestIntegration_S3AttemptStore_PhasesRoundTrip, against real MinIO.
func TestAttemptRecord_PhasesRoundTripThroughLocalAndS3Stores(t *testing.T) {
	local, err := NewLocalAttemptStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalAttemptStore: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	for name, store := range map[string]AttemptStore{"local": local, "memory": NewMemoryAttemptStore()} {
		t.Run(name, func(t *testing.T) {
			record := sampleAttempt("ork-spans", "flaky", 1)
			record.Phase = "exit"
			record.Phases = sampleSpans()
			if err := store.PutAttempt(context.Background(), record); err != nil {
				t.Fatalf("PutAttempt: %v", err)
			}
			got, err := store.ListAttempts(context.Background(), "ork-spans", "flaky", NoItem)
			if err != nil || len(got) != 1 {
				t.Fatalf("ListAttempts = %d records, %v", len(got), err)
			}
			if !proto.Equal(got[0], record) {
				t.Fatalf("round trip changed the record:\n got %v\nwant %v", got[0], record)
			}
		})
	}
}

// TestBoundAttempt_PhasesSurviveMessageTruncation is S2: a record over the cap
// because of its error strings is truncated in those strings only; every span
// survives.
func TestBoundAttempt_PhasesSurviveMessageTruncation(t *testing.T) {
	record := sampleAttempt("ork-1", "flaky", 1)
	record.Phases = sampleSpans()
	record.Error.Message = strings.Repeat("m", AttemptMaxBytes)
	record.Error.Details = strings.Repeat("d", AttemptMaxBytes)

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
	if len(decoded.GetPhases()) != len(record.GetPhases()) {
		t.Fatalf("bounding kept %d spans, want all %d", len(decoded.GetPhases()), len(record.GetPhases()))
	}
	for i := range record.GetPhases() {
		if !proto.Equal(decoded.GetPhases()[i], record.GetPhases()[i]) {
			t.Errorf("span %d changed: %v, want %v", i, decoded.GetPhases()[i], record.GetPhases()[i])
		}
	}
}

// TestUnmarshalAttempt_ARecordWithoutPhasesReadsAnEmptyList is S3: a record
// written before phases existed decodes on this build as an empty list — the
// reader's "record predates phase spans" — and a record WITH phases decodes on
// a reader that does not know the field (simulated by an unknown field) rather
// than failing.
func TestUnmarshalAttempt_ARecordWithoutPhasesReadsAnEmptyList(t *testing.T) {
	old := `{"schemaVersion":1,"runId":"ork-1","taskId":"flaky","attempt":1,` +
		`"outcome":"ATTEMPT_OUTCOME_SUCCEEDED","phase":"commit"}`
	decoded, err := UnmarshalAttempt([]byte(old))
	if err != nil {
		t.Fatalf("UnmarshalAttempt of a pre-phases record: %v", err)
	}
	if len(decoded.GetPhases()) != 0 {
		t.Fatalf("a pre-phases record decoded %d spans, want none", len(decoded.GetPhases()))
	}
	if decoded.GetPhase() != "commit" {
		t.Errorf("phase = %q, want commit", decoded.GetPhase())
	}
	future := `{"schemaVersion":1,"runId":"ork-1","taskId":"flaky","attempt":1,"phase":"run",` +
		`"phasesFromTheFuture":[{"phase":"run"}]}`
	if _, err := UnmarshalAttempt([]byte(future)); err != nil {
		t.Fatalf("an unknown field broke the reader: %v", err)
	}
}

// runWideStores is every backend in this package that implements
// RunAttemptReader without a network.
func runWideStores(t *testing.T) map[string]interface {
	AttemptStore
	RunAttemptReader
} {
	t.Helper()
	local, err := NewLocalAttemptStore(t.TempDir())
	if err != nil {
		t.Fatalf("local attempt store: %v", err)
	}
	t.Cleanup(func() { _ = local.Close() })
	return map[string]interface {
		AttemptStore
		RunAttemptReader
	}{
		"memory": NewMemoryAttemptStore(),
		"local":  local,
	}
}

func TestRunAttemptReader_ListsEveryTaskOfOneRunInKeyOrder(t *testing.T) {
	const run, sibling, extended = "ork-1-1", "ork-1-2", "ork-1-10"
	for name, store := range runWideStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for _, record := range []*orkstackv1.TaskAttempt{
				sampleAttempt(run, "b", 2), sampleAttempt(run, "a", 1), sampleAttempt(run, "b", 1),
				sampleAttempt(sibling, "a", 1), sampleAttempt(extended, "a", 1),
			} {
				if err := store.PutAttempt(ctx, record); err != nil {
					t.Fatalf("PutAttempt: %v", err)
				}
			}
			refs, err := store.ListRunAttemptRefs(ctx, run)
			if err != nil {
				t.Fatalf("ListRunAttemptRefs: %v", err)
			}
			want := []AttemptRef{{RunID: run, TaskID: "a", Attempt: 1}, {RunID: run, TaskID: "b", Attempt: 1}, {RunID: run, TaskID: "b", Attempt: 2}}
			if len(refs) != len(want) {
				t.Fatalf("ListRunAttemptRefs = %v, want %v", refs, want)
			}
			for i := range want {
				if refs[i] != want[i] {
					t.Fatalf("ListRunAttemptRefs = %v, want %v", refs, want)
				}
			}
			empty, err := store.ListRunAttemptRefs(ctx, "ork-9-9")
			if err != nil || len(empty) != 0 {
				t.Fatalf("a run with no records = %v, %v; want empty and no error", empty, err)
			}
		})
	}
}

func TestRunAttemptReader_GetAttemptReadsBackWhatWasPutAndMissingIsNotFound(t *testing.T) {
	for name, store := range runWideStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			record := sampleAttempt("ork-2-1", "build", 3)
			record.Resources = &orkstackv1.ReservedResources{Cpu: 0.5, MemoryBytes: 512 << 20}
			if err := store.PutAttempt(ctx, record); err != nil {
				t.Fatalf("PutAttempt: %v", err)
			}
			got, err := store.GetAttempt(ctx, AttemptRef{RunID: "ork-2-1", TaskID: "build", Attempt: 3})
			if err != nil {
				t.Fatalf("GetAttempt: %v", err)
			}
			want, err := BoundAttempt(record)
			if err != nil {
				t.Fatalf("BoundAttempt: %v", err)
			}
			if !proto.Equal(got, want) {
				t.Fatalf("GetAttempt = %v, want %v", got, want)
			}
			_, err = store.GetAttempt(ctx, AttemptRef{RunID: "ork-2-1", TaskID: "build", Attempt: 4})
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("GetAttempt of a missing record = %v, want ErrNotFound", err)
			}
		})
	}
}
