package eventsources

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// TestMessageSentence: every reason of a message has one fixed sentence, which
// holds nothing to fill in; a reason of another release has none.
func TestMessageSentence(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "reasons.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		reason, _ := strconv.Unquote(lit.Value)
		if !strings.HasPrefix(reason, "MESSAGE_") {
			return true
		}
		found++
		if s := MessageSentence(reason); s == "" || strings.ContainsAny(s, "%{}") || !strings.HasSuffix(s, ".") {
			t.Errorf("MessageSentence(%s) = %q", reason, s)
		}
		return true
	})
	if found != 17 {
		t.Errorf("%d reasons of a message read; want 17", found)
	}
	if s := MessageSentence(ReasonPaused); s == "" {
		t.Error("a message given back for a paused source has no sentence")
	}
	for _, reason := range []string{"", "MESSAGE_OF_LATER", ReasonDisabled} {
		if s := MessageSentence(reason); s != "" {
			t.Errorf("MessageSentence(%q) = %q; want none", reason, s)
		}
	}
}
