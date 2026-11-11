package task

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/converter"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/stablejson"

	"github.com/orkstack/orkstack/internal/testsupport/must"
)

// legacyInputSize is the measure CheckInputSize took before PayloadSize was
// generalised from *ExecuteTaskInput to any message, spelled out as it was.
func legacyInputSize(in *orkstackv1.ExecuteTaskInput) (int64, error) {
	payload, err := converter.GetDefaultDataConverter().ToPayload(in)
	if err != nil {
		return 0, err
	}
	raw := payload.GetData()
	var compact bytes.Buffer
	if err := stablejson.Compact(&compact, raw); err != nil {
		return int64(len(raw)), nil //nolint:nilerr // the raw length is the answer
	}
	return int64(compact.Len() + whitespaceSites(compact.Bytes())), nil
}

// TestPayloadSize_IsWhatCheckInputSizeMeasured: over every ExecuteTask input
// the committed history fixtures recorded, the exported measure equals the
// one CheckInputSize used before the change, so no decision it recorded can
// move.
func TestPayloadSize_IsWhatCheckInputSizeMeasured(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "engine", "testdata", "history", "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("glob fixtures: %v (%d)", err, len(paths))
	}
	compared := 0
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		must.OK(t, err)
		history := &historypb.History{}
		must.OKf(t, (temporalproto.CustomJSONUnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, history), "decode %s", path)
		for _, event := range history.GetEvents() {
			attrs := event.GetActivityTaskScheduledEventAttributes()
			if attrs == nil || attrs.GetActivityType().GetName() != ActivityExecuteTask {
				continue
			}
			var in *orkstackv1.ExecuteTaskInput
			must.OKf(t, converter.GetDefaultDataConverter().FromPayload(attrs.GetInput().GetPayloads()[0], &in), "%s: decode an ExecuteTask input", path)
			got, err := PayloadSize(in)
			must.OK(t, err)
			want, err := legacyInputSize(in)
			must.OK(t, err)
			if got != want {
				t.Errorf("%s/%s: PayloadSize = %d, the measure before = %d", filepath.Base(path), in.GetTaskId(), got, want)
			}
			compared++
		}
	}
	if compared == 0 {
		t.Fatal("no recorded ExecuteTask input was compared")
	}
	t.Logf("%d recorded inputs compared", compared)
}
