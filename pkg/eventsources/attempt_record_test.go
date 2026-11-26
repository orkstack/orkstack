package eventsources

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// fullAttemptRecord is a record with every field set.
func fullAttemptRecord() AttemptRecord {
	at := time.Date(2026, 11, 25, 1, 2, 3, 0, time.UTC)
	code := int32(137)
	return AttemptRecord{V: 1, Namespace: "payments", Name: "orders", ID: "evs-0123456789abcdef", Index: 2, Epoch: 7, Attempt: 3, OrkerID: "o-1",
		BuildID: "b-1", Pool: "main", Service: strings.Repeat("a", 64), Judging: 31, Image: "ghcr.io/acme/source@sha256:" + strings.Repeat("b", 64),
		Phase: "run", TakenAt: at, StartedAt: at.Add(time.Second), ReadyAt: at.Add(2 * time.Second), EndedAt: at.Add(time.Hour),
		Reason: "SERVICE_FAILED", Cause: "CONFIG_INVALID", ExitCode: &code, Held: "unread=policy", HeldSince: at, LogBytes: 9}
}

// TestAttemptRecord: a record reads back as it was; whatever it is given, it is
// within its bound and has no line or control character of an image's.
func TestAttemptRecord(t *testing.T) {
	want := fullAttemptRecord()
	for i := range reflect.TypeFor[AttemptRecord]().NumField() {
		if reflect.ValueOf(want).Field(i).IsZero() {
			t.Errorf("%s is not set", reflect.TypeFor[AttemptRecord]().Field(i).Name)
		}
	}
	if got, ok := ReadAttemptRecord(want.Encode()); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("%+v, %v\nwant %+v", got, ok, want)
	}
	// What JSON writes longest within each bound.
	worst := fullAttemptRecord()
	for _, s := range []*string{&worst.Namespace, &worst.Name, &worst.ID, &worst.OrkerID, &worst.BuildID, &worst.Pool, &worst.Service, &worst.Image,
		&worst.Phase, &worst.Reason, &worst.Cause, &worst.Held} {
		*s = strings.Repeat(`"`, 4000)
	}
	worst.Index, worst.Epoch, worst.Attempt, worst.Judging, worst.LogBytes = -1<<31, -1<<63, -1<<31, -1<<63, -1<<63
	if _, ok := ReadAttemptRecord(worst.Encode()); !ok {
		t.Fatalf("the longest record, of %d bytes, does not read back", len(worst.Encode()))
	}
	long := strings.Repeat("é\n\x00", 4000)
	big := AttemptRecord{Namespace: long, Name: long, ID: long, OrkerID: long, BuildID: long, Pool: long, Service: long, Image: long, Phase: long,
		Reason: long, Cause: long, Held: long, TakenAt: time.Date(99999, 1, 1, 0, 0, 0, 0, time.UTC)}
	raw := big.Encode()
	got, ok := ReadAttemptRecord(raw)
	if !ok || len(raw) > MaxAttemptRecordBytes || strings.ContainsAny(got.Reason, "\n\x00") || got.Reason == "" || !got.TakenAt.IsZero() {
		t.Fatalf("%d bytes, %v: %+v", len(raw), ok, got)
	}
	for _, raw := range []string{"", "null", "[]", `{"v":2}`, `{}`, `{"v":1,"epoch":"x"}`, `{"v":1,"v":1}`, `{"v":1,"name":"` + strings.Repeat("a", MaxAttemptRecordBytes) + `"}`} {
		if got, ok := ReadAttemptRecord([]byte(raw)); ok {
			t.Errorf("read %.40q as %+v", raw, got)
		}
	}
	if _, ok := ReadAttemptRecord([]byte(`{"v":1,"later":true}`)); !ok {
		t.Error("a field of a later release refused the record")
	}
}

// FuzzReadAttemptRecord: what is read is of this format and encodes to what
// reads back the same.
func FuzzReadAttemptRecord(f *testing.F) {
	f.Add(fullAttemptRecord().Encode())
	for _, s := range []string{"", "{}", `{"v":1}`, `{"v":1,"takenAt":"9999-12-31T23:59:59Z","reason":"\ud83d"}`} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		got, ok := ReadAttemptRecord(raw)
		if !ok {
			return
		}
		once := got.Encode()
		again, ok := ReadAttemptRecord(once)
		if !ok || len(once) > MaxAttemptRecordBytes || string(again.Encode()) != string(once) {
			t.Fatalf("%q encodes to %q, which does not read back", raw, once)
		}
	})
}
