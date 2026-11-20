package sourcehost

import (
	"testing"

	"github.com/orkstack/orkstack/internal/orker/servicehost"
)

func TestJudgeStatus_TheBrokerBlock(t *testing.T) {
	const valid = `{"object":"dir:/tmp/b","maxDeliveries":5,"messageLimitBytes":1048576,"leaseSeconds":60}`
	tests := []struct {
		name       string
		block      string
		ok         bool
		atMostOnce bool
		wantOK     bool
		wantReason string
		wantField  string
		wantLimit  int
	}{
		{name: "an accepted limit", block: valid, ok: true, wantOK: true, wantLimit: 5},
		{name: "the lowest accepted limit", block: `{"maxDeliveries":2,"messageLimitBytes":1}`, ok: true, wantOK: true, wantLimit: 2},
		{name: "the highest accepted limit", block: `{"maxDeliveries":100,"messageLimitBytes":1}`, ok: true, wantOK: true, wantLimit: 100},
		{name: "a limit of a billion is no limit", block: `{"maxDeliveries":1000000000,"messageLimitBytes":1}`, ok: true,
			wantReason: "OBJECT_INVALID", wantField: "broker.maxDeliveries"},
		{name: "a limit of one", block: `{"maxDeliveries":1,"messageLimitBytes":1}`, ok: true,
			wantReason: "OBJECT_INVALID", wantField: "broker.maxDeliveries"},
		{name: "a negative limit", block: `{"maxDeliveries":-1,"messageLimitBytes":1}`, ok: true,
			wantReason: "OBJECT_INVALID", wantField: "broker.maxDeliveries"},
		{name: "a fractional limit", block: `{"maxDeliveries":5.5,"messageLimitBytes":1}`, ok: true,
			wantReason: "OBJECT_INVALID", wantField: "broker"},
		{name: "no limit, at least once", block: `{"messageLimitBytes":1}`, ok: true,
			wantReason: "OBJECT_INVALID", wantField: "broker.maxDeliveries"},
		{name: "no limit, at most once", block: `{"messageLimitBytes":1}`, ok: true, atMostOnce: true, wantOK: true},
		{name: "no message limit", block: `{"maxDeliveries":5}`, ok: true, wantReason: "MESSAGE_LIMIT", wantField: "broker.messageLimitBytes"},
		{name: "a message limit of zero", block: `{"maxDeliveries":5,"messageLimitBytes":0}`, ok: true,
			wantReason: "MESSAGE_LIMIT", wantField: "broker.messageLimitBytes"},
		{name: "no block", ok: true, wantReason: "OBJECT_INVALID", wantField: "broker"},
		{name: "a block that is no object", block: `[1]`, ok: true, wantReason: "OBJECT_INVALID", wantField: "broker"},
		{name: "a limit given as text", block: `{"maxDeliveries":"5","messageLimitBytes":1}`, ok: true,
			wantReason: "OBJECT_INVALID", wantField: "broker"},
		{name: "a check that is not ok is the image's word", ok: false, wantReason: "UNREACHABLE"},
		{name: "a check that is not ok keeps a limit it reports", block: valid, ok: false, wantReason: "UNREACHABLE", wantLimit: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := servicehost.Status{Ready: true, OK: tt.ok}
			if !tt.ok {
				s.Reason = "UNREACHABLE"
			}
			if tt.block != "" {
				s.Block = []byte(tt.block)
			}
			got, limit := JudgeStatus(s, tt.atMostOnce)
			if got.OK != tt.wantOK || got.Reason != tt.wantReason || got.Field != tt.wantField {
				t.Fatalf("JudgeStatus = ok %v reason %q field %q; want ok %v reason %q field %q",
					got.OK, got.Reason, got.Field, tt.wantOK, tt.wantReason, tt.wantField)
			}
			if limit != tt.wantLimit {
				t.Fatalf("limit = %d; want %d", limit, tt.wantLimit)
			}
			if !got.Ready || got.Fatal {
				t.Fatalf("JudgeStatus changed ready or fatal: %+v", got)
			}
		})
	}
}
