package engine

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
)

// TestUnscopedOrkers: every live Orker whose verdict is not scoped is
// listed, an older build that reports none among them; an Orker that did not
// answer is not judged.
func TestUnscopedOrkers(t *testing.T) {
	orker := func(id string, scope orkstackv1.StoreScope, detail string) FleetOrker {
		return FleetOrker{Presence: OrkerPresence{Identity: id},
			Described: &orkstackv1.DescribeOrkerResult{OrkerId: id, StoreScope: scope, StoreScopeDetail: detail}}
	}
	fleet := []FleetOrker{
		orker("a", orkstackv1.StoreScope_STORE_SCOPE_SCOPED, ""),
		orker("b", orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED, "reached: put metadata/identity/"),
		orker("c", orkstackv1.StoreScope_STORE_SCOPE_UNKNOWN, "list metadata/audit/: SlowDown"),
		orker("d", orkstackv1.StoreScope_STORE_SCOPE_UNSPECIFIED, ""),
		{Presence: OrkerPresence{Identity: "e"}, Err: errors.New("no answer")},
	}
	got := UnscopedOrkers(fleet)
	var ids []string
	for _, u := range got {
		ids = append(ids, u.OrkerID)
	}
	if want := []string{"b", "c", "d"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("unscoped = %q, want %q", ids, want)
	}
	if got[0].Verdict != "unscoped" || got[1].Verdict != "no verdict" || got[2].Verdict != "not reported" {
		t.Fatalf("verdicts = %+v", got)
	}
}

type fixedFleet struct {
	orkers []*orkstackv1.DescribeOrkerResult
	err    error
}

func (f fixedFleet) ObserveOrkers(context.Context) ([]OrkerPresence, error) {
	var out []OrkerPresence
	for _, o := range f.orkers {
		out = append(out, OrkerPresence{Identity: o.GetOrkerId()})
	}
	return out, f.err
}

func (f fixedFleet) DescribeOrkerByID(_ context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	for _, o := range f.orkers {
		if o.GetOrkerId() == id {
			return o, nil
		}
	}
	return nil, errors.New("gone")
}

// TestReportStoreScope_LogsEachUnscopedOrkerAtError: one pass logs every
// such Orker at error, naming it and what it found, and nothing when the
// fleet is scoped; a fleet that cannot be read is a warning.
func TestReportStoreScope_LogsEachUnscopedOrkerAtError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	fleet := fixedFleet{orkers: []*orkstackv1.DescribeOrkerResult{
		{OrkerId: "a", StoreScope: orkstackv1.StoreScope_STORE_SCOPE_SCOPED},
		{OrkerId: "b", StoreScope: orkstackv1.StoreScope_STORE_SCOPE_UNSCOPED, StoreScopeDetail: "reached: put metadata/identity/"},
	}}
	ReportStoreScope(context.Background(), fleet, logger)
	out := buf.String()
	if strings.Count(out, "level=ERROR") != 1 || !strings.Contains(out, "orker=b") || !strings.Contains(out, "metadata/identity/") {
		t.Fatalf("logged %q", out)
	}

	buf.Reset()
	ReportStoreScope(context.Background(), fixedFleet{orkers: fleet.orkers[:1]}, logger)
	if buf.Len() != 0 {
		t.Fatalf("a scoped fleet logged %q", buf.String())
	}

	// An Orker that is listed but does not answer is named at warn.
	buf.Reset()
	gone := fixedFleet{orkers: fleet.orkers[:1]}
	ReportStoreScope(context.Background(), presenceOnly{gone, "c"}, logger)
	if out := buf.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "orker=c") {
		t.Fatalf("an Orker that did not answer logged %q", out)
	}

	buf.Reset()
	ReportStoreScope(context.Background(), fixedFleet{err: errors.New("cluster down")}, logger)
	if out := buf.String(); !strings.Contains(out, "level=WARN") || strings.Contains(out, "level=ERROR") {
		t.Fatalf("an unreadable fleet logged %q", out)
	}
}

// presenceOnly lists one more Orker than fleet can describe.
type presenceOnly struct {
	fleet fixedFleet
	extra string
}

func (p presenceOnly) ObserveOrkers(ctx context.Context) ([]OrkerPresence, error) {
	out, err := p.fleet.ObserveOrkers(ctx)
	return append(out, OrkerPresence{Identity: p.extra}), err
}

func (p presenceOnly) DescribeOrkerByID(ctx context.Context, id string) (*orkstackv1.DescribeOrkerResult, error) {
	return p.fleet.DescribeOrkerByID(ctx, id)
}
