package artifacts

import (
	"context"
	"errors"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

func planRecord(reason string) *orkstackv1.ResumePlanRecord {
	return &orkstackv1.ResumePlanRecord{
		Format: ResumePlanFormat, Namespace: "ns", RunId: "ork-1", Scope: 42, N: 1,
		BaseExecutionRunId: "base-run", RStartedEventId: 9, RScheduledEventId: 8,
		Block:  &orkstackv1.ResumeBlock{SeqBase: 42, Scope: 42},
		Reason: reason,
	}
}

func TestResumePlanKey_RendersTheDocumentedLayout(t *testing.T) {
	got, err := ResumePlanKey("ns", "ork-1", 42, 1)
	if err != nil || got != "metadata/snapshots/ns/ork-1/resume-plans/00000000000000000042.001.json" {
		t.Fatalf("ResumePlanKey = %q, %v", got, err)
	}
	for _, bad := range []struct {
		scope uint64
		n     int
	}{{0, 1}, {42, -1}, {42, maxResumePlan + 1}} {
		if _, err := ResumePlanKey("ns", "ork-1", bad.scope, bad.n); !errors.Is(err, ErrInvalidRecordKey) {
			t.Fatalf("ResumePlanKey(%d, %d) = %v", bad.scope, bad.n, err)
		}
	}
}

// The digest a reset names is the digest of the bytes stored, and a read
// answers that same digest for the same bytes.
func TestResumePlan_WriteThenReadAnswersTheStoredDigest(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestRecordStore(t)
	digest, err := s.WriteResumePlan(ctx, planRecord("flaky network"))
	if err != nil {
		t.Fatal(err)
	}
	if again, err := s.WriteResumePlan(ctx, planRecord("flaky network")); err != nil || again != digest {
		t.Fatalf("an identical second write = %q, %v; want %q", again, err, digest)
	}
	if _, err := s.WriteResumePlan(ctx, planRecord("another reason")); !errors.Is(err, ErrResumePlanDiffers) {
		t.Fatalf("other bytes at the key = %v, want ErrResumePlanDiffers", err)
	}
	got, read, err := s.ReadResumePlan(ctx, "ns", "ork-1", 42, 1)
	if err != nil || got.Answer != RecordFound || read != digest || got.Value.GetReason() != "flaky network" {
		t.Fatalf("read = %+v, %q, %v; want found at %q", got, read, err, digest)
	}
	if none, _, err := s.ReadResumePlan(ctx, "ns", "ork-1", 42, 2); err != nil || none.Answer != RecordNone {
		t.Fatalf("an absent plan = %+v, %v", none, err)
	}
}

func TestDecodeResumePlan_RefusesWhatItCannotTrust(t *testing.T) {
	good, err := MarshalResumePlan(planRecord(""))
	if err != nil {
		t.Fatal(err)
	}
	other := planRecord("")
	other.Format = ResumePlanFormat + 1
	future, _ := MarshalResumePlan(other)
	noBlock := planRecord("")
	noBlock.Block = nil
	blockless, _ := MarshalResumePlan(noBlock)
	for _, tc := range []struct {
		name  string
		raw   []byte
		run   string
		scope uint64
		want  RecordAnswer
	}{
		{"its own plan", good, "ork-1", 42, RecordFound},
		{"another run's", good, "ork-2", 42, RecordInvalid},
		{"another plan", good, "ork-1", 43, RecordInvalid},
		{"a format this build does not know", future, "ork-1", 42, RecordUnsupported},
		{"no block", blockless, "ork-1", 42, RecordInvalid},
		{"not JSON", []byte("{"), "ork-1", 42, RecordInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := DecodeResumePlan(tc.raw, "ns", tc.run, tc.scope, 1); got.Answer != tc.want {
				t.Fatalf("answer = %v (%s), want %v", got.Answer, got.Reason, tc.want)
			}
		})
	}
}

// A stored plan record comes from object storage: the decoder never panics
// and finds only a record that names the plan asked for.
func FuzzDecodeResumePlan(f *testing.F) {
	good, _ := MarshalResumePlan(planRecord("seed"))
	f.Add(good)
	f.Add([]byte("{}"))
	f.Add([]byte(strings.Repeat("[", 64)))
	f.Fuzz(func(t *testing.T, raw []byte) {
		got := DecodeResumePlan(raw, "ns", "ork-1", 42, 1)
		if got.Answer == RecordFound && (got.Value.GetRunId() != "ork-1" || got.Value.GetScope() != 42 || got.Value.GetN() != 1 || got.Value.GetBlock() == nil) {
			t.Fatalf("found a record of another plan: %v", got.Value)
		}
	})
}
