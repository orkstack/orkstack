package api_test

import (
	"testing"

	orkstackv1 "github.com/orkstack/orkstack/gen/go/orkstack/v1"
	"github.com/orkstack/orkstack/internal/listfilter"
)

// filters parses CLI-grammar texts into wire filters, as orkctl --filter
// does, failing the test on a syntax error.
func filters(t *testing.T, texts ...string) []*orkstackv1.FieldFilter {
	t.Helper()
	if len(texts) == 0 {
		return nil
	}
	out := make([]*orkstackv1.FieldFilter, len(texts))
	for i, text := range texts {
		f, err := listfilter.Parse(text)
		if err != nil {
			t.Fatalf("Parse(%q): %v", text, err)
		}
		out[i] = f
	}
	return out
}
