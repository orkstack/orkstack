package dsl

import (
	"fmt"
	"strings"
)

// Unified renders the unified diff between two DSL documents.
//
// WHY THE SERVER DIFFS. Two clients read this text — `orkctl orkflow diff` and
// the UI's version page — and a third will follow. Each computing its own diff
// means each carries a diff dependency, and the two renderings drift: the CLI
// and the page disagree about what changed between the same two versions,
// which is a question with one right answer. One implementation here, golden
// tested once, is rendered verbatim by every client. It also keeps the answer
// available to a client that cannot diff at all (a curl, a webhook), and the
// bytes it compares are the bytes the server stored rather than whatever a
// client believes it applied.
//
// The output is standard unified-diff text with the usual three lines of
// context: `--- a/…`, `+++ b/…`, one `@@ -l,s +l,s @@` header per hunk, and
// ' ', '-', '+' prefixed rows. Identical inputs produce "" — an empty diff is
// the honest rendering of no change, and callers print "no difference" rather
// than an empty file's worth of headers.
//
// It is deliberately NOT a semantic diff of the compiled orkflow. What a user
// edited is the YAML, and what a review needs to see is the text they will
// edit again — a diff of two task graphs would be a second, different answer
// to a question nobody asked.
func Unified(fromLabel, toLabel, from, to string) string {
	if from == to {
		return ""
	}
	fromLines, toLines := splitLines(from), splitLines(to)
	hunks := hunks(diffOps(fromLines, toLines))
	if len(hunks) == 0 {
		return ""
	}
	var out strings.Builder
	fmt.Fprintf(&out, "--- a/%s\n", fromLabel)
	fmt.Fprintf(&out, "+++ b/%s\n", toLabel)
	for _, h := range hunks {
		fmt.Fprintf(&out, "@@ -%s +%s @@\n", span(h.fromStart, h.fromCount), span(h.toStart, h.toCount))
		for _, row := range h.rows {
			out.WriteString(string(row.kind))
			out.WriteString(row.line.text)
			out.WriteByte('\n')
			if row.line.noNewline {
				out.WriteString(noNewlineMarker)
				out.WriteByte('\n')
			}
		}
	}
	return out.String()
}

// diffContext is how many unchanged lines surround each change, matching
// `diff -u`'s default so the output is recognisable and machine-parsable.
const diffContext = 3

// noNewlineMarker is the standard row `diff -u` emits when the line above it
// ended the file without a newline.
const noNewlineMarker = `\ No newline at end of file`

// line is one line of a document, and whether it ended the document without a
// trailing newline.
//
// The flag is part of the line's IDENTITY, not decoration: "a\nb" and "a\nb\n"
// are two different documents, and comparing only the text would report them
// as identical — which for this repo is worse than cosmetic, because the store
// refuses a second apply of the same version with different source bytes and
// the user's only diagnosis is this diff.
type line struct {
	text      string
	noNewline bool
}

// splitLines splits a document into lines, treating a trailing newline as a
// terminator rather than as an empty final line: "a\n" is one line, and adding
// text after it must not report the last line as changed when it did not.
func splitLines(document string) []line {
	if document == "" {
		return nil
	}
	texts := strings.Split(document, "\n")
	complete := false
	if last := len(texts) - 1; last >= 0 && texts[last] == "" {
		texts = texts[:last]
		complete = true
	}
	lines := make([]line, len(texts))
	for i, text := range texts {
		lines[i] = line{text: text}
	}
	if !complete && len(lines) > 0 {
		lines[len(lines)-1].noNewline = true
	}
	return lines
}

// op is one row of the edit script.
type op struct {
	kind byte // ' ' keep, '-' delete from `from`, '+' insert from `to`
	line line
}

// diffOps is a line-level longest-common-subsequence diff.
//
// The table is O(n·m) in memory, which is right for the input: a DSL document
// is tens to hundreds of lines, and the alternative (Myers with a linear-space
// refinement) is several times the code for a saving nothing here can measure.
// The guard below keeps that promise honest — a pathological input falls back
// to "everything replaced" rather than allocating a huge table.
func diffOps(from, to []line) []op {
	const maxCells = 4 << 20 // ~2000x2000 lines
	if len(from)*len(to) > maxCells {
		ops := make([]op, 0, len(from)+len(to))
		for _, l := range from {
			ops = append(ops, op{kind: '-', line: l})
		}
		for _, l := range to {
			ops = append(ops, op{kind: '+', line: l})
		}
		return ops
	}
	// lcs[i][j] = length of the longest common subsequence of from[i:] and to[j:].
	lcs := make([][]int, len(from)+1)
	for i := range lcs {
		lcs[i] = make([]int, len(to)+1)
	}
	for i := len(from) - 1; i >= 0; i-- {
		for j := len(to) - 1; j >= 0; j-- {
			if from[i] == to[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
				continue
			}
			lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
		}
	}
	var ops []op
	i, j := 0, 0
	for i < len(from) && j < len(to) {
		switch {
		case from[i] == to[j]:
			ops = append(ops, op{kind: ' ', line: from[i]})
			i, j = i+1, j+1
		case lcs[i+1][j] >= lcs[i][j+1]:
			ops = append(ops, op{kind: '-', line: from[i]})
			i++
		default:
			ops = append(ops, op{kind: '+', line: to[j]})
			j++
		}
	}
	for ; i < len(from); i++ {
		ops = append(ops, op{kind: '-', line: from[i]})
	}
	for ; j < len(to); j++ {
		ops = append(ops, op{kind: '+', line: to[j]})
	}
	return ops
}

// hunk is one contiguous run of changes with its context.
type hunk struct {
	fromStart, fromCount int
	toStart, toCount     int
	rows                 []op
}

// hunks groups an edit script into unified-diff hunks with diffContext lines of
// surrounding context, merging hunks whose contexts overlap.
func hunks(ops []op) []hunk {
	changed := make([]int, 0, len(ops))
	for index, o := range ops {
		if o.kind != ' ' {
			changed = append(changed, index)
		}
	}
	if len(changed) == 0 {
		return nil
	}
	// Line numbers are 1-based and count only the side a row belongs to.
	fromNo, toNo := make([]int, len(ops)), make([]int, len(ops))
	f, t := 0, 0
	for index, o := range ops {
		fromNo[index], toNo[index] = f, t
		if o.kind != '+' {
			f++
		}
		if o.kind != '-' {
			t++
		}
	}

	var out []hunk
	for start := 0; start < len(changed); {
		first := changed[start]
		last := first
		end := start
		for end+1 < len(changed) && changed[end+1]-last <= 2*diffContext+1 {
			end++
			last = changed[end]
		}
		lo := max(first-diffContext, 0)
		hi := min(last+diffContext, len(ops)-1)

		h := hunk{fromStart: fromNo[lo] + 1, toStart: toNo[lo] + 1}
		for index := lo; index <= hi; index++ {
			o := ops[index]
			h.rows = append(h.rows, o)
			if o.kind != '+' {
				h.fromCount++
			}
			if o.kind != '-' {
				h.toCount++
			}
		}
		// An empty side starts at line 0 by convention (`diff -u` of an empty
		// file), so a pure insertion does not claim to start at line 1 of a
		// file that has none.
		if h.fromCount == 0 {
			h.fromStart = fromNo[lo]
		}
		if h.toCount == 0 {
			h.toStart = toNo[lo]
		}
		out = append(out, h)
		start = end + 1
	}
	return out
}

// span renders a hunk header's range, collapsing a one-line range to its
// number as `diff -u` does.
func span(start, count int) string {
	if count == 1 {
		return fmt.Sprintf("%d", start)
	}
	return fmt.Sprintf("%d,%d", start, count)
}
