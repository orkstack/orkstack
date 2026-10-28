package listfilter_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

// The properties below are the ones docs/UI_CLI_PARITY.md "Filtering lists"
// states: the grammar round-trips, a refusal is the server's own named error,
// a canonical set is a fixed point of compiling (a page token binds its
// Key), and a matcher decides a row as the table of matching rules says.

// FuzzParse_AcceptedTextIsAFilterThatFormatsBackToItself: whatever text Parse
// accepts is a filter the compiler's input rules allow (a field, an operator
// and the value count that operator takes), and writing it in the grammar
// and parsing it again gives the same filter. Every refusal says something.
func FuzzParse_AcceptedTextIsAFilterThatFormatsBackToItself(f *testing.F) {
	for _, row := range loadVectors(f).Grammar {
		f.Add(row.Text)
	}
	for _, seed := range []string{
		"", "=", "a", "a=", `a\`, `a=\`, "a!", "a>", "a>=", "a=,", "a=b,,c", `a=b\,c`, `\=a=b`, "a==b", "a<=b", "a=\\=b",
		"labels.team=a,b", "created_at>=2026-10-09T00:00:00Z", "a~b,c", "a=b,\\=c", "a\x00=b", "\xff=\xfe,\xfd",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		parsed, err := listfilter.Parse(text)
		if err != nil {
			if err.Error() == "" {
				t.Fatalf("Parse(%q) refused with an empty message", text)
			}
			return
		}
		requireParseProducible(t, text, parsed)
		formatted := listfilter.Format(parsed)
		back, err := listfilter.Parse(formatted)
		if err != nil {
			t.Fatalf("Parse(%q) = %v, but Parse(Format(it)) = Parse(%q) was refused: %v", text, parsed, formatted, err)
		}
		if !proto.Equal(parsed, back) {
			t.Fatalf("Parse(%q) = %v, Parse(Format(it)) = Parse(%q) = %v", text, parsed, formatted, back)
		}
		if again := listfilter.Format(back); again != formatted {
			t.Fatalf("Format is not a fixed point: %q then %q", formatted, again)
		}
	})
}

// requireParseProducible checks the shape the grammar promises of a parsed
// filter.
func requireParseProducible(t *testing.T, text string, f *orkstackv1.FieldFilter) {
	t.Helper()
	if f.GetField() == "" {
		t.Fatalf("Parse(%q) accepted an empty field", text)
	}
	values := f.GetValues()
	switch f.GetOp() {
	case orkstackv1.FilterOperator_FILTER_OPERATOR_IN:
		if len(values) < 2 || slices.Contains(values, "") {
			t.Fatalf("Parse(%q) = in with values %q; an in holds two or more non-empty items", text, values)
		}
	case orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS,
		orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX,
		orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER,
		orkstackv1.FilterOperator_FILTER_OPERATOR_BEFORE:
		if len(values) != 1 {
			t.Fatalf("Parse(%q) = %v; %v takes exactly one value", text, f, f.GetOp())
		}
	default:
		t.Fatalf("Parse(%q) produced operator %v", text, f.GetOp())
	}
}

// FuzzFormat_ParsesBackToTheSameFilter is the other direction, from the
// structure: every filter Parse can produce (a field, an operator, and the
// value count the operator takes) is written by Format as text that Parse
// reads as that exact filter, whatever bytes the field and the values hold.
func FuzzFormat_ParsesBackToTheSameFilter(f *testing.F) {
	f.Add("status", uint8(0), "FAILED", "", "", uint8(1))
	f.Add("labels.team", uint8(2), "a,b", `c\d`, "=e", uint8(3))
	f.Add(`a\b=c`, uint8(1), "=", "", "", uint8(1))
	f.Add("a!b", uint8(5), "\xff", "", "", uint8(1))
	f.Fuzz(func(t *testing.T, field string, opSel uint8, v1, v2, v3 string, n uint8) {
		if field == "" {
			return
		}
		ops := []orkstackv1.FilterOperator{
			orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS,
			orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS,
			orkstackv1.FilterOperator_FILTER_OPERATOR_IN,
			orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS,
			orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX,
			orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER,
			orkstackv1.FilterOperator_FILTER_OPERATOR_BEFORE,
		}
		op := ops[int(opSel)%len(ops)]
		values := []string{v1}
		if op == orkstackv1.FilterOperator_FILTER_OPERATOR_IN {
			values = []string{v1, v2, v3}[:2+int(n)%2]
			if slices.Contains(values, "") {
				return // an in holds no empty item
			}
		}
		want := &orkstackv1.FieldFilter{Field: field, Op: op, Values: values}
		text := listfilter.Format(want)
		got, err := listfilter.Parse(text)
		if err != nil {
			t.Fatalf("Format(%v) = %q, which Parse refused: %v", want, text, err)
		}
		if !proto.Equal(got, want) {
			t.Fatalf("Format(%v) = %q, which Parse read as %v", want, text, got)
		}
	})
}

// allSpecs are the allowlists of every filtered list, in a stable order, and
// the test-built runs allowlist (enum values, requires, a time interval).
func allSpecs(tb testing.TB) []*listfilter.Spec {
	tb.Helper()
	return []*listfilter.Spec{
		specOf(tb, &orkstackv1.ListNamespacesRequest{}),
		runsSpec(tb),
		specOf(tb, &orkstackv1.ListWebhooksRequest{}),
		specOf(tb, &orkstackv1.ListTriggersRequest{}),
		specOf(tb, &orkstackv1.ListSchedulesRequest{}),
		specOf(tb, &orkstackv1.ListPoolsRequest{}),
		specOf(tb, &orkstackv1.ListActivitiesRequest{}),
		specOf(tb, &orkstackv1.ListOrkersRequest{}),
		specOf(tb, &orkstackv1.ListReleasesRequest{}),
		specOf(tb, &orkstackv1.ListDeliveriesRequest{}),
		specOf(tb, &orkstackv1.ListAuditRecordsRequest{}),
		specOf(tb, &orkstackv1.ListOrkflowsRequest{}),
	}
}

// compileSeeds are filter texts drawn from the canonical vectors and the
// compile tests' tables.
var compileSeeds = []string{
	"name=a", "name=a,b", "name~zz", "description^x", "labels.team=payments", "labels.team!=x", "labels.=x", "labels=x",
	"status=failed", "status=FAILED,CANCELED", "status!=running", "status=parent-run", "status=nope",
	"orkflow_name=Bad", "orkflow_name=", "origin.kind=trigger", "origin.id=abc", "origin.kind=SUBMIT",
	"created_at>=2026-10-09T00:00:00Z", "created_at<2026-10-09T01:00:00+01:00", "created_at<2026-10-08T00:00:00Z",
	"created_at>=2026-10-09", "created_at>=0001-01-01T00:00:00+01:00", "created_at<9999-12-31T23:59:59.999999999-23:59",
	"updated_at>=2026-10-09T00:00:00.123456789Z", "drained=true", "cordoned=maybe", "orker_id=a/b", "hold.reason=",
}

// FuzzCompile_ACompiledSetIsCanonicalAndTheOrderOfItsFiltersDoesNotMatter
// states §"Canonical form and page tokens": a set that compiles is written
// back as texts that parse and compile to the same set (the same Key), the
// same filters in another order, or repeated, are the same set, a refusal is
// the compiler's own error naming the filter, and the set a refusal or an
// acceptance was reached on does not depend on the order.
func FuzzCompile_ACompiledSetIsCanonicalAndTheOrderOfItsFiltersDoesNotMatter(f *testing.F) {
	specs := allSpecs(f)
	for i := range specs {
		for _, seed := range compileSeeds {
			f.Add(uint8(i), seed, "", "")
		}
		f.Add(uint8(i), "created_at>=2026-10-09T00:00:00Z", "created_at>=2026-10-10T00:00:00Z", "created_at<2026-10-12T00:00:00Z")
		f.Add(uint8(i), "status=failed,running,FAILED", "status=RUNNING", "origin.kind=trigger")
	}
	f.Fuzz(func(t *testing.T, which uint8, a, b, c string) {
		spec := specs[int(which)%len(specs)]
		var filters []*orkstackv1.FieldFilter
		for _, text := range []string{a, b, c} {
			if text == "" {
				continue
			}
			parsed, err := listfilter.Parse(text)
			if err != nil {
				return
			}
			filters = append(filters, parsed)
		}
		set, err := spec.Compile(filters)
		reversed := slices.Clone(filters)
		slices.Reverse(reversed)
		other, otherErr := spec.Compile(reversed)
		if (err == nil) != (otherErr == nil) {
			t.Fatalf("%v compiles to %v but reversed to %v", texts(filters), err, otherErr)
		}
		if err != nil {
			requireNamedRefusal(t, filters, err)
			return
		}
		if set.Key() != other.Key() || !slices.Equal(set.Texts(), other.Texts()) {
			t.Fatalf("%v: order changed the set: %q then %q", texts(filters), set.Texts(), other.Texts())
		}
		doubled, err := spec.Compile(append(slices.Clone(filters), filters...))
		if err != nil || doubled.Key() != set.Key() {
			if err == nil || !strings.Contains(err.Error(), "at most") {
				t.Fatalf("%v: repeating its filters changed the set: %q, %v", texts(filters), doubled.Texts(), err)
			}
		}

		canonical := set.Texts()
		if !slices.IsSorted(canonical) || len(slices.Compact(slices.Clone(canonical))) != len(canonical) {
			t.Fatalf("%v: canonical texts %q are not sorted and unique", texts(filters), canonical)
		}
		var again []*orkstackv1.FieldFilter
		for _, text := range canonical {
			parsed, err := listfilter.Parse(text)
			if err != nil {
				t.Fatalf("%v compiled to %q, which does not parse: %v", texts(filters), text, err)
			}
			again = append(again, parsed)
		}
		recompiled, err := spec.Compile(again)
		if err != nil {
			t.Fatalf("%v compiled to %q, which does not compile again: %v", texts(filters), canonical, err)
		}
		if recompiled.Key() != set.Key() || !slices.Equal(recompiled.Texts(), canonical) {
			t.Fatalf("%v compiled to %q, which compiles to %q", texts(filters), canonical, recompiled.Texts())
		}
		viaWire, err := spec.Compile(set.Proto())
		if err != nil || viaWire.Key() != set.Key() {
			t.Fatalf("%v: the set's own wire filters compile to %q, %v", texts(filters), viaWire.Texts(), err)
		}
		if (set.Key() == "") != set.Empty() || set.Len() != len(canonical) {
			t.Fatalf("%v: Key %q, Empty %v, Len %d disagree with texts %q", texts(filters), set.Key(), set.Empty(), set.Len(), canonical)
		}
	})
}

func texts(filters []*orkstackv1.FieldFilter) []string {
	out := make([]string, len(filters))
	for i, f := range filters {
		out[i] = listfilter.Format(f)
	}
	return out
}

// requireNamedRefusal checks the refusal is the compiler's error, with the
// message the API answers with and an index that points into the request.
func requireNamedRefusal(t *testing.T, filters []*orkstackv1.FieldFilter, err error) {
	t.Helper()
	var refused *listfilter.Error
	if !errors.As(err, &refused) {
		t.Fatalf("%v: refused with %T %v, not a *listfilter.Error", texts(filters), err, err)
	}
	if refused.Message == "" || refused.Message != err.Error() {
		t.Fatalf("%v: refusal message %q", texts(filters), refused.Message)
	}
	if refused.Index < -1 || refused.Index >= len(filters) {
		t.Fatalf("%v: refusal index %d is outside the request", texts(filters), refused.Index)
	}
	if refused.Index >= 0 && !strings.HasPrefix(refused.Message, "filters[") {
		t.Fatalf("%v: a refusal of filter %d does not say which: %q", texts(filters), refused.Index, refused.Message)
	}
}

// FuzzCompile_WireFiltersAreRefusedByNameNeverByPanic hands the compiler
// filters no parser would produce: an unset or unknown operator, the wrong
// value count, any bytes in the values.
func FuzzCompile_WireFiltersAreRefusedByNameNeverByPanic(f *testing.F) {
	specs := allSpecs(f)
	f.Add(uint8(0), "name", int32(1), "a", "b", uint8(1))
	f.Add(uint8(0), "name", int32(0), "a", "", uint8(1))
	f.Add(uint8(0), "name", int32(99), "a", "", uint8(1))
	f.Add(uint8(0), "name", int32(3), "a", "", uint8(1))
	f.Add(uint8(0), "created_at", int32(6), "2026-10-09T00:00:00Z", "", uint8(1))
	f.Add(uint8(0), "labels.k", int32(2), "\xff", "", uint8(1))
	f.Add(uint8(1), "status", int32(3), "failed", "FAILED", uint8(2))
	f.Fuzz(func(t *testing.T, which uint8, field string, op int32, v1, v2 string, n uint8) {
		spec := specs[int(which)%len(specs)]
		values := []string{v1, v2}[:int(n)%3]
		filters := []*orkstackv1.FieldFilter{{Field: field, Op: orkstackv1.FilterOperator(op), Values: values}}
		set, err := spec.Compile(filters)
		if err != nil {
			var refused *listfilter.Error
			if !errors.As(err, &refused) || refused.Message == "" || refused.Index != 0 && refused.Index != -1 {
				t.Fatalf("Compile(%v) = %T %v", filters, err, err)
			}
			return
		}
		if set.Len() != 1 {
			t.Fatalf("Compile(%v) kept %d filters, want 1", filters, set.Len())
		}
		parsed, err := listfilter.Parse(set.Texts()[0])
		if err != nil {
			t.Fatalf("Compile(%v) = %q, which does not parse: %v", filters, set.Texts()[0], err)
		}
		if _, err := spec.Compile([]*orkstackv1.FieldFilter{parsed}); err != nil {
			t.Fatalf("Compile(%v) = %q, which does not compile again: %v", filters, set.Texts()[0], err)
		}
	})
}

// namespaceRow builds the row the matching fuzz targets read.
func namespaceRow(name, description, labelKey, labelValue, pool, holdReason string, sec int64, nanos int32, hasTime bool) *orkstackv1.Namespace {
	row := &orkstackv1.Namespace{
		Name:        name,
		Description: description,
		DefaultPool: pool,
	}
	if labelKey != "" {
		row.Labels = map[string]string{labelKey: labelValue}
	}
	if holdReason != "" {
		row.Hold = &orkstackv1.NamespaceHold{Reason: holdReason}
	}
	if hasTime {
		row.CreatedAt = &timestamppb.Timestamp{Seconds: sec, Nanos: nanos}
	}
	return row
}

// rowValue is a namespace row read the way the matching-rules table says:
// text as it is, an unset text as "", an absent label key as "", a time as
// microseconds and an unset time as never matching.
func rowValue(row *orkstackv1.Namespace, path string) (text string, micros int64, isTime, set bool) {
	switch {
	case path == "name":
		return row.GetName(), 0, false, true
	case path == "description":
		return row.GetDescription(), 0, false, true
	case path == "default_pool":
		return row.GetDefaultPool(), 0, false, true
	case path == "hold.reason":
		return row.GetHold().GetReason(), 0, false, true
	case path == "require_signed_by":
		return row.GetRequireSignedBy(), 0, false, true
	case path == "telemetry.sink":
		return row.GetTelemetry().GetSink(), 0, false, true
	case strings.HasPrefix(path, "labels."):
		return row.GetLabels()[strings.TrimPrefix(path, "labels.")], 0, false, true
	case path == "created_at":
		if row.GetCreatedAt() == nil {
			return "", 0, true, false
		}
		return "", row.GetCreatedAt().GetSeconds()*1_000_000 + int64(row.GetCreatedAt().GetNanos())/1000, true, true
	case path == "updated_at":
		return "", 0, true, false
	}
	panic("rowValue: unexpected path " + path)
}

// holds is the documented meaning of one canonical filter on a row.
func holds(t *testing.T, f *orkstackv1.FieldFilter, row *orkstackv1.Namespace) bool {
	t.Helper()
	text, micros, isTime, set := rowValue(row, f.GetField())
	values := f.GetValues()
	if isTime {
		if !set {
			return false
		}
		bound, err := time.Parse(time.RFC3339Nano, values[0])
		if err != nil {
			t.Fatalf("canonical time %q does not parse: %v", values[0], err)
		}
		if f.GetOp() == orkstackv1.FilterOperator_FILTER_OPERATOR_AT_OR_AFTER {
			return micros >= bound.UnixMicro()
		}
		return micros < bound.UnixMicro()
	}
	switch f.GetOp() {
	case orkstackv1.FilterOperator_FILTER_OPERATOR_EQUALS:
		return text == values[0]
	case orkstackv1.FilterOperator_FILTER_OPERATOR_NOT_EQUALS:
		return text != values[0]
	case orkstackv1.FilterOperator_FILTER_OPERATOR_IN:
		return slices.Contains(values, text)
	case orkstackv1.FilterOperator_FILTER_OPERATOR_CONTAINS:
		return strings.Contains(text, values[0])
	case orkstackv1.FilterOperator_FILTER_OPERATOR_PREFIX:
		return strings.HasPrefix(text, values[0])
	}
	t.Fatalf("canonical filter %v has operator %v", f, f.GetOp())
	return false
}

// FuzzMatch_DecidesARowAsTheMatchingRulesSayAndEveryPushdownIsSound compiles
// up to three filters over the namespaces allowlist and reads a row with the
// documented rules (text verbatim and case-sensitive, an unset reading as "",
// an absent label reading "", an unset time never matching, instants compared
// at microsecond precision). The row matches the set exactly when it matches
// every filter; the single-value and time-interval narrowings a list reads
// with can never exclude a row the set matches; and a subset of the filters
// can only match more.
func FuzzMatch_DecidesARowAsTheMatchingRulesSayAndEveryPushdownIsSound(f *testing.F) {
	spec := specOf(f, &orkstackv1.ListNamespacesRequest{})
	f.Add("name=a", "created_at>=2026-10-09T00:00:00Z", "labels.team=x", "a", "d", "team", "x", "p", "r", int64(1791504000), int32(500), true)
	f.Add("name~b", "description^d", "hold.reason!=", "ab", "dd", "k", "", "", "why", int64(0), int32(0), false)
	f.Add("created_at<2026-10-09T00:00:00.000001Z", "created_at>=1970-01-01T00:00:00Z", "default_pool=in,out", "", "", "", "", "in", "", int64(-1), int32(999999999), true)
	f.Add("labels.k=,", "labels.k!=v", "labels.k~", "n", "", "k", "v", "", "", int64(1), int32(1), true)
	f.Fuzz(func(t *testing.T, a, b, c, name, description, labelKey, labelValue, pool, holdReason string, sec int64, nanos int32, hasTime bool) {
		if nanos < 0 || nanos > 999_999_999 || sec < -62135596800 || sec > 253402300799 {
			return // not a valid google.protobuf.Timestamp
		}
		var filters []*orkstackv1.FieldFilter
		for _, text := range []string{a, b, c} {
			if text == "" {
				continue
			}
			parsed, err := listfilter.Parse(text)
			if err != nil {
				return
			}
			filters = append(filters, parsed)
		}
		set, err := spec.Compile(filters)
		if err != nil {
			return
		}
		row := namespaceRow(name, description, labelKey, labelValue, pool, holdReason, sec, nanos, hasTime)

		want := true
		for _, canonical := range set.Proto() {
			want = want && holds(t, canonical, row)
		}
		if got := set.Match(row); got != want {
			t.Fatalf("filters %q on %v: Match = %v, the documented rules say %v", set.Texts(), row, got, want)
		}
		if !want {
			return
		}
		// The row matches: every narrowing must still contain it.
		for _, canonical := range set.Proto() {
			path := canonical.GetField()
			text, micros, isTime, _ := rowValue(row, path)
			if isTime {
				from, before := set.Interval(path)
				if !from.IsZero() && micros < from.UnixMicro() || !before.IsZero() && micros >= before.UnixMicro() {
					t.Fatalf("filters %q: Interval(%s) = [%v, %v) excludes the matching row at %dµs", set.Texts(), path, from, before, micros)
				}
				continue
			}
			if v, ok := set.Equality(path); ok && text != v {
				t.Fatalf("filters %q: Equality(%s) = %q, but the matching row holds %q", set.Texts(), path, v, text)
			}
		}
		for _, canonical := range set.Proto() {
			if !set.Restrict(canonical.GetField()).Match(row) {
				t.Fatalf("filters %q: the subset on %s rejects a row the whole set accepts", set.Texts(), canonical.GetField())
			}
		}
	})
}
