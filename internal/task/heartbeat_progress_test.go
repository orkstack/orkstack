package task

import (
	"encoding/json/v2"
	"reflect"
	"sort"
	"testing"

	"go.temporal.io/sdk/converter"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// TestHeartbeatProgress_JSONShapeIsTheOrkersShape pins the wire names
// an Orker and an engine of different builds agree on, round-trips the detail
// through Temporal's default converter the way RecordHeartbeat and the
// engine's describe decode do, and proves an old Orker's beat still decodes.
func TestHeartbeatProgress_JSONShapeIsTheOrkersShape(t *testing.T) {
	full := HeartbeatProgress{Phase: "run", ContainerID: "3f2a9c1d0b4e7f00", BytesLogged: 432013312, OrkerID: "orker-7"}
	dc := converter.GetDefaultDataConverter()
	payloads, err := dc.ToPayloads(full)
	must.OKf(t, err, "ToPayloads")
	var back HeartbeatProgress
	must.OKf(t, dc.FromPayloads(payloads, &back), "FromPayloads")
	if !reflect.DeepEqual(back, full) {
		t.Fatalf("round trip = %+v, want %+v", back, full)
	}
	var keys map[string]any
	if err := json.Unmarshal(payloads.GetPayloads()[0].GetData(), &keys); err != nil {
		t.Fatalf("detail is not a JSON object: %v", err)
	}
	var got []string
	for k := range keys {
		got = append(got, k)
	}
	sort.Strings(got)
	want := []string{"bytes_logged", "container_id", "orker_id", "phase"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON keys = %v, want exactly %v", got, want)
	}

	var old HeartbeatProgress
	if err := json.Unmarshal([]byte(`{"phase":"run"}`), &old); err != nil {
		t.Fatalf("an old Orker's beat does not decode: %v", err)
	}
	if !reflect.DeepEqual(old, HeartbeatProgress{Phase: "run"}) {
		t.Fatalf("old beat = %+v, want phase run and every other field empty", old)
	}
	t.Logf("wire keys %v; {\"phase\":\"run\"} decodes as %+v", got, old)
}

// TestHeartbeatStaleAfter_LeavesRoomBeforeTheDeadWorkerWindow holds the
// ordering that makes the stale flag useful: a healthy tick never trips it,
// and it trips before Temporal acts.
func TestHeartbeatStaleAfter_LeavesRoomBeforeTheDeadWorkerWindow(t *testing.T) {
	if !(MaxHeartbeatInterval < HeartbeatStaleAfter && HeartbeatStaleAfter < HeartbeatTimeout) {
		t.Fatalf("want MaxHeartbeatInterval (%v) < HeartbeatStaleAfter (%v) < HeartbeatTimeout (%v)",
			MaxHeartbeatInterval, HeartbeatStaleAfter, HeartbeatTimeout)
	}
	t.Logf("MaxHeartbeatInterval %v < HeartbeatStaleAfter %v < HeartbeatTimeout %v",
		MaxHeartbeatInterval, HeartbeatStaleAfter, HeartbeatTimeout)
}

// TestHeartbeatProgress_UsageIsMixedFleetSafe: a beat carrying usage
// round-trips through Temporal's converter with its usage byte for byte; an
// engine older than the usage key (a detail struct without it) decodes the
// beat's other fields and ignores the key; and the usage decodes on its own.
func TestHeartbeatProgress_UsageIsMixedFleetSafe(t *testing.T) {
	usage, err := json.Marshal(HeartbeatUsage{CPUCores: new(0.5), CPUSeconds: 12.25, Memory: 1 << 20, Limit: 1 << 30, AgeMillis: 900, Interval: 5000, Tick: 5000})
	must.OKf(t, err, "encode usage")
	beat := HeartbeatProgress{Phase: "run", OrkerID: "orker-7", Started: true, Usage: usage}
	dc := converter.GetDefaultDataConverter()
	payloads, err := dc.ToPayloads(beat)
	must.OKf(t, err, "ToPayloads")
	var back HeartbeatProgress
	must.OKf(t, dc.FromPayloads(payloads, &back), "FromPayloads")
	if !reflect.DeepEqual(back, beat) {
		t.Fatalf("round trip = %+v, want %+v", back, beat)
	}

	// The detail as a v0.3.x engine declares it: no usage key.
	type olderProgress struct {
		Phase       string `json:"phase"`
		ContainerID string `json:"container_id,omitempty"`
		BytesLogged int64  `json:"bytes_logged,omitzero"`
		OrkerID     string `json:"orker_id,omitempty"`
		Ordinal     int32  `json:"ordinal,omitzero"`
	}
	var older olderProgress
	must.OKf(t, dc.FromPayloads(payloads, &older), "an older engine decodes a beat with usage")
	if older != (olderProgress{Phase: "run", OrkerID: "orker-7"}) {
		t.Fatalf("older engine read %+v", older)
	}

	var u HeartbeatUsage
	must.OKf(t, json.Unmarshal(back.Usage, &u), "decode usage")
	if u.CPUSeconds != 12.25 || u.Memory != 1<<20 || u.Tick != 5000 {
		t.Fatalf("usage = %+v", u)
	}
	t.Logf("beat %s", payloads.GetPayloads()[0].GetData())
}
