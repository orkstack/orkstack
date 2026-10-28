package artifacts

import (
	"bytes"
	"strings"
	"testing"
	"time"
	"unicode"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/pkg/triggers"
	"github.com/orkstack/orkstack/pkg/webhooks"
)

// FuzzRecords_ADocumentThatDecodesIsOneTheStoreWritesBack: every record the
// stores keep is read back from object storage, which a person can edit and a
// newer engine can write. A document a decoder accepts is a record the same
// package can write again, within its stated bound, and reading what it wrote
// gives the record back; a decoder refuses everything else by name, and never
// panics.
func FuzzRecords_ADocumentThatDecodesIsOneTheStoreWritesBack(f *testing.F) {
	const kinds = 14
	seeds := map[uint8]proto.Message{
		0: &orkstackv1.TaskAttempt{RunId: "ork-1-1", TaskId: "build", Attempt: 1},
		1: &orkstackv1.ActivityCommit{RunId: "ork-1-1", TaskId: "build", SchemaVersion: 1},
		2: &orkstackv1.CacheMemo{Key: strings.Repeat("a", 64)},
		3: &orkstackv1.AuditRecord{Id: "aud-20261009T000000.000000000Z-0123456789abcdef", At: timestamppb.New(time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)), Detail: "x\ny"},
		4: &orkstackv1.ImagePolicy{AllowedRegistries: []string{"ghcr.io/acme"}, RequireDigest: true},
		5: &orkstackv1.Namespace{Name: "payments"},
		6: &orkstackv1.RetentionPolicy{},
		7: &orkstackv1.Orkflow{Name: "nightly", Version: "1"},
	}
	for kind, message := range seeds {
		document, err := (protoJSON{}).marshal(message)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(kind, document)
	}
	created := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	if document, err := marshalWebhookRecord(webhooks.Webhook{ID: "wh-0123456789abcdef", URL: "https://example.test/hook", Events: []webhooks.Event{webhooks.EventRunSucceeded}, CreatedAt: created, Secret: "s"}); err == nil {
		f.Add(uint8(8), document)
	}
	if document, err := marshalTriggerRecord(triggers.Trigger{ID: "trg-0123456789abcdef", Namespace: "default", OrkflowName: "nightly", CreatedAt: created, SecretSalt: []byte{1}, SecretHash: []byte{2}}); err == nil {
		f.Add(uint8(9), document)
	}
	if document, err := marshalHeadPointer(headPointer{Version: "1", AppliedAt: created}); err == nil {
		f.Add(uint8(10), document)
	}
	if document, err := marshalCurrentPointer(currentPointer{Version: "1", UpdatedAt: created}); err == nil {
		f.Add(uint8(11), document)
	}
	f.Add(uint8(12), marshalS3NameManifest("objects/a", "name"))
	f.Add(uint8(12), marshalS3NameManifest("objects/a", ""))
	f.Add(uint8(13), []byte(`{"schemaVersion":1}`))
	for kind := uint8(0); kind < kinds; kind++ {
		for _, bad := range []string{``, `{`, `[]`, `null`, `{"x":1}`, "{\"runId\":\"\xff\"}"} {
			f.Add(kind, []byte(bad))
		}
	}
	f.Fuzz(func(t *testing.T, kind uint8, document []byte) {
		switch kind % kinds {
		case 0:
			attempt, err := UnmarshalAttempt(document)
			if err != nil {
				return
			}
			bounded, err := BoundAttempt(attempt)
			if err != nil {
				return
			}
			again, err := BoundAttempt(bounded)
			if err != nil || !proto.Equal(bounded, again) {
				t.Fatalf("BoundAttempt is not idempotent for %q: %v", document, err)
			}
			written, err := MarshalAttempt(attempt)
			if err != nil {
				return
			}
			back, err := UnmarshalAttempt(written)
			if len(written) > AttemptMaxBytes || err != nil || !proto.Equal(back, bounded) {
				t.Fatalf("MarshalAttempt(%q) wrote %d bytes that read back as %v, %v; want the bounded record", document, len(written), back, err)
			}
		case 1:
			roundTrip(t, document, UnmarshalCommit, MarshalCommit, UnmarshalCommit, 0)
		case 2:
			roundTrip(t, document, UnmarshalMemo, MarshalMemo, UnmarshalMemo, MemoMaxBytes)
		case 3:
			record, err := UnmarshalAuditRecord(document)
			if err != nil {
				return
			}
			written, err := MarshalAuditRecord(record)
			if err != nil {
				return
			}
			back, err := UnmarshalAuditRecord(written)
			if err != nil || len(written) > AuditMaxBytes {
				t.Fatalf("MarshalAuditRecord(%q) wrote %d bytes that read back as %v, %v", document, len(written), back, err)
			}
			for _, text := range []string{back.GetPrincipal(), back.GetTarget(), back.GetDetail(), back.GetNamespace(), back.GetProcedure(), back.GetSourceAddress(), back.GetAction(), back.GetErrorCode()} {
				if strings.IndexFunc(text, unicode.IsControl) >= 0 {
					t.Fatalf("MarshalAuditRecord(%q) kept a control character in %q", document, text)
				}
			}
		case 4:
			roundTrip(t, document, unmarshalImagePolicy, marshalImagePolicy, unmarshalImagePolicy, 0)
		case 5:
			roundTrip(t, document, unmarshalNamespace, marshalNamespace, unmarshalNamespace, 0)
		case 6:
			roundTrip(t, document, unmarshalRetentionPolicy, marshalRetentionPolicy, unmarshalRetentionPolicy, 0)
		case 7:
			roundTrip(t, document, unmarshalOrkflow, marshalOrkflow, unmarshalOrkflow, 0)
		case 8:
			hook, err := unmarshalWebhookRecord(document)
			if err != nil {
				return
			}
			written, err := marshalWebhookRecord(hook)
			if err != nil {
				t.Fatalf("a webhook record that decoded, %q, cannot be written: %v", document, err)
			}
			back, err := unmarshalWebhookRecord(written)
			if err != nil || !back.CreatedAt.Equal(hook.CreatedAt) || back.ID != hook.ID || back.URL != hook.URL || back.Secret != hook.Secret ||
				back.Namespace != hook.Namespace || back.OrkflowName != hook.OrkflowName || back.TaskID != hook.TaskID || back.Name != hook.Name ||
				len(back.Events) != len(hook.Events) {
				t.Fatalf("a webhook record did not survive being written: %+v -> %+v, %v", hook, back, err)
			}
		case 9:
			trigger, err := unmarshalTriggerRecord(document)
			if err != nil {
				return
			}
			written, err := marshalTriggerRecord(trigger)
			if err != nil {
				t.Fatalf("a trigger record that decoded, %q, cannot be written: %v", document, err)
			}
			back, err := unmarshalTriggerRecord(written)
			if err != nil || !back.CreatedAt.Equal(trigger.CreatedAt) || back.ID != trigger.ID || back.Namespace != trigger.Namespace ||
				back.OrkflowName != trigger.OrkflowName || back.OrkflowVersion != trigger.OrkflowVersion || back.Name != trigger.Name ||
				back.Description != trigger.Description || !bytes.Equal(back.SecretSalt, trigger.SecretSalt) || !bytes.Equal(back.SecretHash, trigger.SecretHash) {
				t.Fatalf("a trigger record did not survive being written: %+v -> %+v, %v", trigger, back, err)
			}
		case 10:
			pointer, err := unmarshalHeadPointer(document)
			if err != nil {
				return
			}
			written, err := marshalHeadPointer(pointer)
			back, backErr := unmarshalHeadPointer(written)
			if err != nil || backErr != nil || back.Version != pointer.Version || !back.AppliedAt.Equal(pointer.AppliedAt) {
				t.Fatalf("a head pointer did not survive being written: %+v -> %+v, %v, %v", pointer, back, err, backErr)
			}
		case 11:
			pointer, err := unmarshalCurrentPointer(document)
			if err != nil {
				return
			}
			written, err := marshalCurrentPointer(pointer)
			back, backErr := unmarshalCurrentPointer(written)
			if err != nil || backErr != nil || back.Version != pointer.Version || !back.UpdatedAt.Equal(pointer.UpdatedAt) {
				t.Fatalf("a current pointer did not survive being written: %+v -> %+v, %v, %v", pointer, back, err, backErr)
			}
		case 12:
			name, err := unmarshalS3NameManifest(document, "objects/a")
			if err != nil {
				return
			}
			if again := marshalS3NameManifest("objects/a", name); !bytes.Equal(again, document) {
				t.Fatalf("a manifest that decoded to %q is not the one the writer renders: %q vs %q", name, document, again)
			}
		case 13:
			record, err := UnmarshalDelivery("", document)
			if err == nil {
				t.Fatalf("UnmarshalDelivery found a record under the empty key: %v", record)
			}
		}
	})
}

// protoJSON renders a seed document the way the stores do.
type protoJSON struct{}

func (protoJSON) marshal(message proto.Message) ([]byte, error) {
	switch typed := message.(type) {
	case *orkstackv1.TaskAttempt:
		return MarshalAttempt(typed)
	case *orkstackv1.ActivityCommit:
		return MarshalCommit(typed)
	case *orkstackv1.CacheMemo:
		return MarshalMemo(typed)
	case *orkstackv1.AuditRecord:
		return MarshalAuditRecord(typed)
	case *orkstackv1.ImagePolicy:
		return marshalImagePolicy(typed)
	case *orkstackv1.Namespace:
		return marshalNamespace(typed)
	case *orkstackv1.RetentionPolicy:
		return marshalRetentionPolicy(typed)
	case *orkstackv1.Orkflow:
		return marshalOrkflow(typed)
	}
	panic("unknown seed type")
}

// roundTrip: a document the decoder accepts, written by the store's encoder,
// is accepted again as the same message, and the encoder's bound (when it has
// one) holds. An encoder may refuse a record, but only over its bound.
func roundTrip[M proto.Message](t *testing.T, document []byte, decode func([]byte) (M, error), encode func(M) ([]byte, error), decodeAgain func([]byte) (M, error), bound int) {
	t.Helper()
	record, err := decode(document)
	if err != nil {
		return
	}
	written, err := encode(record)
	if err != nil {
		if bound == 0 {
			t.Fatalf("a record that decoded, %q, cannot be written: %v", document, err)
		}
		return
	}
	if bound > 0 && len(written) > bound {
		t.Fatalf("the encoder wrote %d bytes, over its bound of %d", len(written), bound)
	}
	back, err := decodeAgain(written)
	if err != nil || !proto.Equal(back, record) {
		t.Fatalf("a record did not survive being written: %v -> %v, %v", record, back, err)
	}
}
