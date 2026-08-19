package dsl

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// TestUnified_RendersTheChangeBetweenTwoVersions pins the exact text both
// clients render. It is one golden, not two renderings that "look the same".
func TestUnified_RendersTheChangeBetweenTwoVersions(t *testing.T) {
	from := `name: deploy
version: "1.0.0"
tasks:
  - id: build
    plugin: shell
    with:
      op: exec
      command: make build
`
	to := `name: deploy
version: "1.1.0"
tasks:
  - id: build
    plugin: shell
    with:
      op: exec
      command: make build
  - id: ship
    plugin: shell
    needs: [build]
    with:
      op: exec
      command: make ship
`
	want := `--- a/deploy@1.0.0
+++ b/deploy@1.1.0
@@ -1,8 +1,14 @@
 name: deploy
-version: "1.0.0"
+version: "1.1.0"
 tasks:
   - id: build
     plugin: shell
     with:
       op: exec
       command: make build
+  - id: ship
+    plugin: shell
+    needs: [build]
+    with:
+      op: exec
+      command: make ship
`
	got := Unified("deploy@1.0.0", "deploy@1.1.0", from, to)
	if got != want {
		t.Fatalf("Unified diff\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

// TestUnified_IsEmptyWhenNothingChanged: an empty diff is the honest rendering
// of no change. Emitting headers for two identical versions would make every
// client print a file's worth of nothing.
func TestUnified_IsEmptyWhenNothingChanged(t *testing.T) {
	document := "name: deploy\nversion: \"1.0.0\"\n"
	if got := Unified("a", "b", document, document); got != "" {
		t.Fatalf("Unified(identical) = %q, want \"\"", got)
	}
	// Byte-identical includes the trailing newline: a document stored without
	// one is not the same document, and the store will refuse to re-apply the
	// version. The diff has to be able to SHOW that, or the refusal is
	// undiagnosable.
	truncated := Unified("a", "b", document, strings.TrimSuffix(document, "\n"))
	if truncated == "" {
		t.Fatal("Unified() reported no change between documents that differ in their final newline")
	}
	if !strings.Contains(truncated, `\ No newline at end of file`) {
		t.Fatalf("a missing final newline is not marked:\n%s", truncated)
	}
}

// TestUnified_HunksAreSeparatedAndContextIsThreeLines checks the property that
// makes the output a unified diff rather than a pair of file dumps: distant
// changes are separate hunks, each with three lines of context.
func TestUnified_HunksAreSeparatedAndContextIsThreeLines(t *testing.T) {
	from := make([]string, 0, 30)
	for i := range 30 {
		from = append(from, fmt.Sprintf("line %02d", i))
	}
	to := append([]string(nil), from...)
	to[1] = "line 01 changed"
	to[25] = "line 25 changed"

	got := Unified("a", "b", strings.Join(from, "\n")+"\n", strings.Join(to, "\n")+"\n")
	headers := 0
	for _, row := range strings.Split(got, "\n") {
		if strings.HasPrefix(row, "@@") {
			headers++
		}
	}
	if headers != 2 {
		t.Fatalf("two distant changes produced %d hunks, want 2:\n%s", headers, got)
	}
	if strings.Contains(got, " line 10") {
		t.Fatalf("an unchanged line far from every change is in the diff:\n%s", got)
	}
	for _, want := range []string{"-line 01", "+line 01 changed", "-line 25", "+line 25 changed"} {
		if !strings.Contains(got, want) {
			t.Errorf("diff does not contain %q:\n%s", want, got)
		}
	}
}

// TestUnified_AppliesBackToTheNewVersion is the property that makes the diff
// TRUE rather than merely plausible: replaying the hunks over the old document
// reproduces the new one, byte for byte. A diff that renders nicely and
// describes the wrong change would pass every assertion above and fail this.
func TestUnified_AppliesBackToTheNewVersion(t *testing.T) {
	cases := []struct{ name, from, to string }{
		{"insert at the head", "b\nc\n", "a\nb\nc\n"},
		{"insert at the tail", "a\nb\n", "a\nb\nc\n"},
		{"delete everything", "a\nb\nc\n", ""},
		{"create from nothing", "", "a\nb\nc\n"},
		{"replace one line", "a\nb\nc\n", "a\nB\nc\n"},
		{"two distant edits", longDocument(t, map[int]string{2: "two", 40: "forty"}), longDocument(t, map[int]string{2: "TWO", 40: "FORTY"})},
		{"adjacent edits merge", longDocument(t, nil), longDocument(t, map[int]string{10: "x", 11: "y"})},
		{"reorder tasks", "a\nb\nc\nd\n", "a\nc\nb\nd\n"},
		{"trailing newline added", "a\nb", "a\nb\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diff := Unified("from", "to", tc.from, tc.to)
			if diff == "" {
				if tc.from != tc.to {
					t.Fatalf("Unified() = \"\" for two different documents")
				}
				return
			}
			patched := applyUnified(t, tc.from, diff)
			if patched != tc.to {
				t.Fatalf("applying the diff did not reproduce the new version\n"+
					"--- diff ---\n%s\n--- want ---\n%q\n--- got ---\n%q", diff, tc.to, patched)
			}
		})
	}
}

func longDocument(t *testing.T, edits map[int]string) string {
	t.Helper()
	lines := make([]string, 0, 50)
	for i := range 50 {
		if replacement, ok := edits[i]; ok {
			lines = append(lines, replacement)
			continue
		}
		lines = append(lines, fmt.Sprintf("line %02d", i))
	}
	return strings.Join(lines, "\n") + "\n"
}

// applyUnified is a minimal unified-diff patcher used only to check the diff
// against itself. It reads the hunk headers rather than trusting them, so a
// wrong line number fails here instead of being reproduced.
//
// It understands `\ No newline at end of file`, because the diff emits it and
// a patcher that ignored it would "reproduce" a document one byte off and call
// the diff correct.
func applyUnified(t *testing.T, from, diff string) string {
	t.Helper()
	source := splitLines(from)
	var out []string
	cursor := 0 // 0-based index into source
	newTrailingNewline := true
	lastKind := byte(0)
	for _, block := range strings.Split(diff, "\n") {
		switch {
		case strings.HasPrefix(block, "--- "), strings.HasPrefix(block, "+++ "):
			continue
		case strings.HasPrefix(block, "@@"):
			start := hunkStart(t, block)
			if start < cursor {
				t.Fatalf("hunk header %q moves backwards from line %d", block, cursor)
			}
			for _, l := range source[cursor:start] {
				out = append(out, l.text)
			}
			cursor = start
		case block == noNewlineMarker:
			// The marker describes the row above it: the NEW document lacks a
			// final newline only when that row is in the new document.
			if lastKind == '+' || lastKind == ' ' {
				newTrailingNewline = false
			}
			continue
		case block == "":
			continue
		default:
			lastKind = block[0]
			switch block[0] {
			case ' ':
				if cursor >= len(source) || source[cursor].text != block[1:] {
					t.Fatalf("context row %q does not match source line %d", block, cursor+1)
				}
				out = append(out, source[cursor].text)
				cursor++
			case '-':
				if cursor >= len(source) || source[cursor].text != block[1:] {
					t.Fatalf("deleted row %q does not match source line %d", block, cursor+1)
				}
				cursor++
			case '+':
				out = append(out, block[1:])
			default:
				t.Fatalf("unknown diff row %q", block)
			}
		}
	}
	for _, l := range source[cursor:] {
		out = append(out, l.text)
	}
	if len(out) == 0 {
		return ""
	}
	if !newTrailingNewline {
		return strings.Join(out, "\n")
	}
	return strings.Join(out, "\n") + "\n"
}

// hunkStart reads the old-file start line out of an "@@ -l,s +l,s @@" header
// and returns it as a 0-based index.
func hunkStart(t *testing.T, header string) int {
	t.Helper()
	fields := strings.Fields(header)
	if len(fields) < 2 || !strings.HasPrefix(fields[1], "-") {
		t.Fatalf("malformed hunk header %q", header)
	}
	numbers := strings.SplitN(strings.TrimPrefix(fields[1], "-"), ",", 2)
	start, err := strconv.Atoi(numbers[0])
	if err != nil {
		t.Fatalf("malformed hunk header %q: %v", header, err)
	}
	count := 1
	if len(numbers) == 2 {
		if count, err = strconv.Atoi(numbers[1]); err != nil {
			t.Fatalf("malformed hunk header %q: %v", header, err)
		}
	}
	if count == 0 {
		// A hunk that consumes nothing from the old file starts after `start`.
		return start
	}
	return start - 1
}
