package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/artifacts"
)

// recordedOf is a task of each kind that keeps a record, with its row as a
// run ends it.
func recordedOf() map[taskKind]*orkstackv1.TaskRun {
	out := map[taskKind]*orkstackv1.TaskRun{}
	for kind, row := range map[taskKind]*orkstackv1.TaskRun{kindAwait: recordedAwait(), kindComposed: recordedChild(), kindSleep: recordedSleep()} {
		row.TaskId, row.Status, row.Output, row.Attempt = "x", sOK, &orkstackv1.Payload{}, 1
		out[kind] = row
	}
	out[kindAwait].Await.Callback = true
	out[kindComposed].Child.OrkflowName, out[kindComposed].Child.OrkflowVersion = "other", "1"
	out[kindSleep].Sleep.Seconds = 300
	return out
}

// A row is cut to what the resumed run cannot derive, and is not shared.
func TestCutRecordedRow(t *testing.T) {
	want := map[taskKind]*orkstackv1.TaskRun{kindAwait: recordedAwait(), kindComposed: recordedChild(), kindSleep: recordedSleep()}
	for kind, row := range recordedOf() {
		before := proto.Clone(row)
		cut := cutRecordedRow(kind, row)
		if !proto.Equal(cut, want[kind]) {
			t.Errorf("kind %d: cut = %v, want %v", kind, cut, want[kind])
		}
		if why := recordedRowFault(kind, cut); why != "" {
			t.Errorf("kind %d: its own cut is refused: %s", kind, why)
		}
		if recordedRowFault(kind, row) == "" {
			t.Errorf("kind %d: a row that was not cut is taken", kind)
		}
		cut.FinishedAt.Seconds++
		if !proto.Equal(row, before) {
			t.Errorf("kind %d: the cut shares the base's row", kind)
		}
	}
}

// FuzzCutRecordedRow: whatever a row holds, cutting it is idempotent and
// leaves it as it was, a cut row is only refused for what it lacks, and one
// that is taken packs into a plan and reads back as itself.
func FuzzCutRecordedRow(f *testing.F) {
	for kind, row := range recordedOf() {
		raw, err := proto.Marshal(row)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(uint8(kind), raw)
		if raw, err = proto.Marshal(cutRecordedRow(kind, row)); err == nil {
			f.Add(uint8(kind), raw)
		}
	}
	f.Add(uint8(kindAwait), []byte{})
	f.Add(uint8(kindSleep), []byte{0xff})
	tasks := map[taskKind]string{kindAwait: "approve", kindComposed: "child", kindSleep: "nap"}
	f.Fuzz(func(t *testing.T, k uint8, raw []byte) {
		kind := taskKind(k % 5)
		if tasks[kind] == "" {
			return
		}
		row := &orkstackv1.TaskRun{}
		if proto.Unmarshal(raw, row) != nil {
			return
		}
		before := proto.Clone(row)
		cut := cutRecordedRow(kind, row)
		if !proto.Equal(row, before) {
			t.Fatal("cutting a row changed it")
		}
		if again := cutRecordedRow(kind, cut); !proto.Equal(again, cut) {
			t.Fatalf("cut twice it is %v, cut once %v", again, cut)
		}
		why := recordedRowFault(kind, cut)
		if why == "it holds more than a record does" || why == "it has no row" {
			t.Fatalf("a cut row is refused: %s", why)
		}
		if why != "" || hasUnknownFields(cut.ProtoReflect()) {
			return
		}
		units := unitsPlan()
		for i := range units {
			if units[i].Task == tasks[kind] {
				units[i].Row = cut
			}
		}
		b := unitsBlock()
		if err := EncodeResumeUnits(unitsOrkflow(), b, units, false); err != nil {
			if strings.Contains(err.Error(), "too large") {
				return
			}
			t.Fatalf("a row that is taken does not pack: %v", err)
		}
		got, err := DecodeResumeUnits(unitsOrkflow(), b, false)
		if err != nil || !sameUnits(got, units) {
			t.Fatalf("read back %+v (%v), want %+v", got, err, units)
		}
	})
}

// A settled task's values read back inline up to the entry's bound, and
// checked against the digest the run recorded.
func TestAdoptRecorded_ReadsSettledValues(t *testing.T) {
	f := newAdoptFixture(t)
	put := func(body string) *orkstackv1.ArtifactRef {
		ref, err := f.values.Put(context.Background(), "values.json", strings.NewReader(body), artifacts.PutOptions{})
		if err != nil {
			t.Fatal(err)
		}
		return &orkstackv1.ArtifactRef{Uri: ref.URI, Sha256: ref.SHA256, SizeBytes: ref.SizeBytes}
	}
	wf := &orkstackv1.Orkflow{Namespace: "default"}
	small, large := put(`{"ok":true}`), put(`{"a":"`+strings.Repeat("x", 5000)+`"}`)
	for name, tc := range map[string]struct {
		task   *orkstackv1.Task
		ref    *orkstackv1.ArtifactRef
		inline bool
	}{
		"an await's":                 {awaitTask("w"), small, true},
		"an await's large":           {awaitTask("w"), large, false},
		"a composed task's":          {composedTask("c"), small, true},
		"a composed task's, however": {composedTask("c"), large, true},
	} {
		out, err := f.acts.AdoptRecorded(context.Background(), adoptInput(settledEntryOf(wf, "ork-1", tc.task, tc.ref)))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		r := out.GetEntries()[0].GetResult()
		if r.GetValuesInlined() != tc.inline || (len(r.GetValuesJson()) > 0) != tc.inline || !proto.Equal(r.GetValuesRef(), tc.ref) {
			t.Errorf("%s: %v, want inline %v", name, r, tc.inline)
		}
	}
	gone := proto.Clone(small).(*orkstackv1.ArtifactRef)
	gone.Uri = strings.Replace(gone.Uri, gone.Sha256[:8], "00000000", 1)
	other := proto.Clone(small).(*orkstackv1.ArtifactRef)
	other.Sha256 = strings.Repeat("0", 64)
	for name, ref := range map[string]*orkstackv1.ArtifactRef{"gone": gone, "other bytes": other} {
		_, err := f.acts.AdoptRecorded(context.Background(), adoptInput(settledEntryOf(wf, "ork-1", awaitTask("w"), ref)))
		if got := categoryOf(err); got.appType() != ReasonValuesMissing || !got.nonRetryable() {
			t.Errorf("%s: %v, want VALUES_MISSING for good", name, err)
		}
	}
	// The observer counts a call by its first entry's namespace.
	if e := settledEntryOf(&orkstackv1.Orkflow{}, "ork-1", awaitTask("w"), small); e.GetExpected().GetNamespace() != "default" {
		t.Errorf("an entry of the default namespace names %q", e.GetExpected().GetNamespace())
	}
	for name, change := range map[string]func(*orkstackv1.AdoptEntry){
		"no values": func(e *orkstackv1.AdoptEntry) { e.Values = nil },
		"no bound":  func(e *orkstackv1.AdoptEntry) { e.InlineLimit = 0 },
		"no run":    func(e *orkstackv1.AdoptEntry) { e.Expected.RunId = "" },
	} {
		e := settledEntryOf(wf, "ork-1", awaitTask("w"), small)
		change(e)
		if _, err := f.acts.AdoptRecorded(context.Background(), adoptInput(e)); categoryOf(err).appType() != ReasonCommitReadFailed {
			t.Errorf("%s: %v, want refused", name, err)
		}
	}
}

// tornOnce serves a values.json cut short the first time: a read no store
// produces on demand.
type tornOnce struct {
	body string
	gets int
}

func (s *tornOnce) Get(context.Context, string) (io.ReadCloser, error) {
	s.gets++
	if s.gets == 1 {
		return io.NopCloser(strings.NewReader(s.body[:3])), nil
	}
	return io.NopCloser(strings.NewReader(s.body)), nil
}

// A read cut short does not decide that an answer is gone: it is read again.
func TestAdoptRecorded_RereadsSettledValuesCutShort(t *testing.T) {
	store := &tornOnce{body: `{"ok":true}`}
	sum := sha256.Sum256([]byte(store.body))
	ref := &orkstackv1.ArtifactRef{Uri: "mem://v", Sha256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(store.body))}
	acts := NewResumeActivities(artifacts.NewMemoryCommitStore(), store)
	out, err := acts.AdoptRecorded(context.Background(), adoptInput(settledEntryOf(&orkstackv1.Orkflow{}, "ork-1", awaitTask("w"), ref)))
	if err != nil || string(out.GetEntries()[0].GetResult().GetValuesJson()) != store.body || store.gets != 2 {
		t.Fatalf("read %v (%v) in %d reads, want the whole of it in 2", out, err, store.gets)
	}
}

// An answer the base's row does not hold is not settled from it.
func TestBuildResumeBlock_RefusesAnAnswerTheRowDoesNotHold(t *testing.T) {
	f := newPlan([]*orkstackv1.Task{awaitTask("approve"), ptask("x")}, prow("approve", sWait), prow("x", sFail))
	f.in.Evidence.Resolutions = map[string]ResolutionAnswer{"approve": {Completed: true}}
	p := f.plan(t)
	if u := unitOf(t, p, "approve", -1); u.Disposition != DispositionSettle {
		t.Fatalf("approve = %+v, want settled from its answer in history", u)
	}
	_, err := BuildResumeBlock(ResumeBlockInput{Input: f.in, Plan: p, SeqBase: 100, Scope: 100,
		Verification: ResumeVerification{Sizes: map[ResumeUnit]int64{nodeUnit("approve"): 10}}})
	if err == nil || !strings.Contains(err.Error(), "its row does not say how") {
		t.Fatalf("error = %v, want it to say the row does not hold the answer", err)
	}
}

// A row a resume settled and could not read back is settled again, and waits
// again, or cannot, when its values are gone.
func TestResumePlan_ASettledResultThatWasNotReadBackIsCheckedAgain(t *testing.T) {
	build := func(answer KeptAnswer) *planFixture {
		row := prow("approve", sFail)
		row.Error = &orkstackv1.Error{Code: ReasonValuesMissing}
		row.ResumeDisposition = dSet
		f := newPlan([]*orkstackv1.Task{awaitTask("approve"), ptask("deploy", "approve")}, row, prow("deploy", sCanc))
		f.in.Facts.Dispatched = map[ResumeUnit]bool{}
		f.in.Evidence.Kept[nodeUnit("approve")] = answer
		return f
	}
	if u := unitOf(t, build(KeptValid).plan(t), "approve", -1); u.Disposition != DispositionSettle || u.Detail != unreadKeptDetail {
		t.Errorf("there: %+v, want settled, to be read back", u)
	}
	if u := unitOf(t, build(KeptGone).plan(t), "approve", -1); u.Disposition != DispositionWait || !u.InRerunSet {
		t.Errorf("gone: %+v, want it to wait again", u)
	}
}
