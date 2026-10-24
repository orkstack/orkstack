package task

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"go.temporal.io/sdk/converter"
)

// TestHeartbeatProgress_JSONShapeIsTheOrkersShape pins the wire names
// an Orker and an engine of different builds agree on, round-trips the detail
// through Temporal's default converter the way RecordHeartbeat and the
// engine's describe decode do, and proves an old Orker's beat still decodes.
func TestHeartbeatProgress_JSONShapeIsTheOrkersShape(t *testing.T) {
	full := HeartbeatProgress{Phase: "run", ContainerID: "3f2a9c1d0b4e7f00", BytesLogged: 432013312, OrkerID: "orker-7"}
	dc := converter.GetDefaultDataConverter()
	payloads, err := dc.ToPayloads(full)
	if err != nil {
		t.Fatalf("ToPayloads: %v", err)
	}
	var back HeartbeatProgress
	if err := dc.FromPayloads(payloads, &back); err != nil {
		t.Fatalf("FromPayloads: %v", err)
	}
	if back != full {
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
	if old != (HeartbeatProgress{Phase: "run"}) {
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
