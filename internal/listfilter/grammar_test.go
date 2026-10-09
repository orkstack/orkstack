package listfilter_test

import (
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

func asError(err error, target **listfilter.Error) bool { return errors.As(err, target) }

func TestParse_SharedVectors(t *testing.T) {
	for _, row := range loadVectors(t).Grammar {
		got, err := listfilter.Parse(row.Text)
		if row.Error != "" {
			if err == nil {
				t.Errorf("Parse(%q) = %v; want an error containing %q", row.Text, got, row.Error)
			} else if !strings.Contains(err.Error(), row.Error) {
				t.Errorf("Parse(%q) error %q; want it to contain %q", row.Text, err, row.Error)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q): %v", row.Text, err)
			continue
		}
		want := &orkstackv1.FieldFilter{Field: row.Filter.Field, Op: opOf(t, row.Filter.Op), Values: row.Filter.Values}
		if !proto.Equal(got, want) {
			t.Errorf("Parse(%q) = %v; want %v", row.Text, got, want)
		}
	}
}

func TestFormat_SharedVectors(t *testing.T) {
	for _, row := range loadVectors(t).Grammar {
		if row.Filter == nil {
			continue
		}
		want := row.Text
		if row.Formatted != "" {
			want = row.Formatted
		}
		f := &orkstackv1.FieldFilter{Field: row.Filter.Field, Op: opOf(t, row.Filter.Op), Values: row.Filter.Values}
		if got := listfilter.Format(f); got != want {
			t.Errorf("Format(%v) = %q; want %q", f, got, want)
		}
	}
}

func TestParseFormat_RoundTripsEveryVector(t *testing.T) {
	for _, row := range loadVectors(t).Grammar {
		if row.Filter == nil {
			continue
		}
		f := &orkstackv1.FieldFilter{Field: row.Filter.Field, Op: opOf(t, row.Filter.Op), Values: row.Filter.Values}
		back, err := listfilter.Parse(listfilter.Format(f))
		if err != nil {
			t.Errorf("Parse(Format(%v)): %v", f, err)
			continue
		}
		if !proto.Equal(back, f) {
			t.Errorf("Parse(Format(%v)) = %v", f, back)
		}
	}
}

func TestOperatorLabel(t *testing.T) {
	want := map[orkstackv1.FilterOperator]string{
		1: "equals (=)", 2: "not equals (!=)", 3: "in (=a,b)", 4: "contains (~)",
		5: "prefix (^)", 6: "at or after (>=)", 7: "before (<)",
	}
	for op, label := range want {
		if got := listfilter.OperatorLabel(op); got != label {
			t.Errorf("OperatorLabel(%v) = %q; want %q", op, got, label)
		}
	}
}
