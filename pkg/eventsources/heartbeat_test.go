package eventsources

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"
)

func fullDetails() HeartbeatDetails {
	at := time.Date(2026, 11, 13, 10, 2, 11, 0, time.UTC)
	return HeartbeatDetails{
		V:     1,
		Epoch: 12, Revision: 7, Judging: 31, Phase: "run", State: "waiting", Reason: "NAMESPACE_HELD", Since: at,
		Lanes: LaneDetails{Busy: 1, Parked: 2, Wedged: 3, OldestStepSeconds: 4},
		Broker: &BrokerDetails{CheckedAt: at, Object: "jetstream:ORDERS/payments", MaxDeliveries: 10, LeaseSeconds: 60,
			RetentionSeconds: 604800, Pending: 412, Unacknowledged: 1, Redelivered: 3, DeadLetterDepth: -1},
		Warnings: []string{"DELIVERY_LIMIT_LOW"}, ConfirmedAt: at, LastAcceptedAt: at, LastPollOkAt: at,
		Image: "ghcr.io/orkstack/source-nats-jetstream@sha256:" + strings.Repeat("7", 64),
		Held:  "unread=spec", HeldSince: at, RefusedSince: at,
	}
}

// TestHeartbeatDetails_RoundTrip: what an attempt beats is what the next reads.
func TestHeartbeatDetails_RoundTrip(t *testing.T) {
	want := fullDetails()
	raw := want.Encode()
	got, ok := ReadHeartbeatDetails(raw)
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("%s\nread as %+v, %v", raw, got, ok)
	}
	for _, field := range []string{`"v":1`, `"oldestStepSeconds":4`, `"deadLetterDepth":-1`, `"lastPollOkAt"`, `"refusedSince"`, `"heldSince"`} {
		if !bytes.Contains(raw, []byte(field)) {
			t.Errorf("%s has no %s", raw, field)
		}
	}
	if raw := (HeartbeatDetails{Epoch: 1, Phase: "admit"}).Encode(); bytes.Contains(raw, []byte("0001-01-01")) || bytes.Contains(raw, []byte("broker")) {
		t.Errorf("what is not known yet is beaten: %s", raw)
	}
}

// TestHeartbeatDetails_Bounded: details are never refused and never over 4 KiB:
// what an image reported is cut first, and what the workflow reads (the epoch,
// the state, the three instants carried to the next attempt) is always there.
func TestHeartbeatDetails_Bounded(t *testing.T) {
	d := fullDetails()
	d.Broker.Object = strings.Repeat("é\x00\"\\", 4000)
	d.Image = strings.Repeat("i", 9000)
	d.Reason = strings.Repeat("R", 9000)
	for range 500 {
		d.Warnings = append(d.Warnings, strings.Repeat("W", 100))
	}
	raw := d.Encode()
	if len(raw) > MaxHeartbeatBytes {
		t.Fatalf("%d bytes", len(raw))
	}
	got, ok := ReadHeartbeatDetails(raw)
	if !ok || got.Epoch != d.Epoch || got.State != d.State || !got.RefusedSince.Equal(d.RefusedSince) || !got.HeldSince.Equal(d.HeldSince) || got.Held != d.Held {
		t.Fatalf("read as %+v, %v", got, ok)
	}
}

// TestReadHeartbeatDetails_NotItsOwn: details of another format, of no shape or
// over their bound are not read: an attempt starts with none of them. A field it
// does not know is another release's, and changes nothing.
func TestReadHeartbeatDetails_NotItsOwn(t *testing.T) {
	for _, raw := range []string{"", "null", "[]", `{"v":2,"epoch":1}`, `{"epoch":1}`, `{"v":1,"epoch":"x"}`, `{"v":1} {"v":1}`, `{"v":1,"held":"` + strings.Repeat("x", MaxHeartbeatBytes) + `"}`} {
		if got, ok := ReadHeartbeatDetails([]byte(raw)); ok {
			t.Errorf("%.40q read as %+v", raw, got)
		}
	}
	if got, ok := ReadHeartbeatDetails([]byte(`{"v":1,"epoch":3,"later":{"a":1}}`)); !ok || got.Epoch != 3 {
		t.Errorf("a later field: %+v, %v", got, ok)
	}
	// An instant no beat can carry is none, as it is once beaten again.
	got, ok := ReadHeartbeatDetails([]byte(`{"v":1,"epoch":3,"heldSince":"0000-01-01T00:00:00Z","refusedSince":"0000-12-31T23:59:59Z","broker":{"checkedAt":"0000-06-01T00:00:00Z"}}`))
	if !ok || !got.HeldSince.IsZero() || !got.RefusedSince.IsZero() || !got.Broker.CheckedAt.IsZero() {
		t.Errorf("an instant before year 1: %+v, %v", got, ok)
	}
}

func FuzzReadHeartbeatDetails(f *testing.F) {
	d := fullDetails()
	f.Add(d.Encode())
	f.Add([]byte(`{"v":1}`))
	f.Add([]byte(`{"v":1,"broker":{"object":"\ud800"}}`))
	f.Add([]byte(`{"v":1,"since":"9999-12-31T23:59:59.999999999Z","lanes":{"busy":-9223372036854775808}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		got, ok := ReadHeartbeatDetails(raw)
		if !ok {
			return
		}
		again, ok := ReadHeartbeatDetails(got.Encode())
		if !ok || again.Epoch != got.Epoch || !again.RefusedSince.Equal(got.RefusedSince) || !again.HeldSince.Equal(got.HeldSince) ||
			!again.Since.Equal(got.Since) || !again.ConfirmedAt.Equal(got.ConfirmedAt) || !again.LastAcceptedAt.Equal(got.LastAcceptedAt) || !again.LastPollOkAt.Equal(got.LastPollOkAt) {
			t.Fatalf("%q read as %+v, and its own encoding as %+v, %v", raw, got, again, ok)
		}
	})
}
