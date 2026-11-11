package main

import (
	"bufio"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
)

// The proof job's job records (.github/workflows/proof.yml, "job records")
// keep a bounded log of every job that did not succeed: what the merger's
// classification reads from a log, and the log's last lines for a reader. A job's whole log can be megabytes, and the records are
// copied into the merged proof (scripts/proof-collect.sh).
const (
	// jobLogTailLines is how many of the log's last lines are kept.
	jobLogTailLines = 200
	// jobLogLineWidth cuts a kept tail line, as a ledger cuts a failing
	// test's output (failureTailWidth).
	jobLogLineWidth = failureTailWidth
	// jobLogProgressBytes bounds the lines the merger reads (mergerReads),
	// which are kept whole, and the length of any one line: an integration
	// shard prints two proofrun lines of about 150 bytes for each of its
	// tests, a few hundred tests at most.
	jobLogProgressBytes = 1 << 20
)

// errJobLogProgress is a log whose lines the merger reads pass the bound
// together. Half of them could read as a lost runner where the whole reads as
// a failing test, so such a log is refused, never cut, and the job's record
// fails closed.
var errJobLogProgress = fmt.Errorf("its proofrun lines exceed %d bytes", jobLogProgressBytes)

// errJobLogLine is a log with a line longer than the bound: what follows the
// cut could be a line the merger reads, so it is refused the same way.
var errJobLogLine = fmt.Errorf("it has a line of more than %d bytes", jobLogProgressBytes)

// terminalControl is what a log line carries for a terminal and not for a
// reader: an escape sequence (CSI, OSC, or a lone ESC with its one byte) and
// any other control byte but a tab.
var terminalControl = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]|\x1b\\][^\x07\x1b]*(?:\x07|\x1b\\\\)|\x1b.?|[\x00-\x08\x0a-\x1f\x7f]")

// plainLogLine is line without its terminal controls.
func plainLogLine(line string) string {
	for i := 0; i < len(line); i++ {
		if c := line[i]; (c < 0x20 && c != '\t') || c == 0x7f {
			return terminalControl.ReplaceAllString(line, "")
		}
	}
	return line
}

// keptLine is one line of the bounded log, by its index in the whole.
type keptLine struct {
	index int
	text  string
}

// boundJobLog copies the part of a GitHub job log that the merger reads, in
// the log's own order: every line it reads anything from (mergerReads: a
// `proofrun: start` or `proofrun: outcome` line, or a test event in JSON)
// wherever it is, byte for byte, so logProgress and attemptProgress read from
// the bounded log what they read from the whole; and the last jobLogTailLines lines, each without its
// terminal control sequences and cut at jobLogLineWidth bytes. A run of lines
// left out is one line that says how many. It reads the log once and holds at
// most the bound in memory. A log whose merger lines pass jobLogProgressBytes
// together (errJobLogProgress), or with a line longer than that
// (errJobLogLine), is refused and nothing is written.
func boundJobLog(r io.Reader, w io.Writer) error {
	br := bufio.NewReaderSize(r, 64<<10)
	var progress, tail []keptLine
	progressBytes, n := 0, 0
	for {
		line, whole, err := readBoundedLine(br, jobLogProgressBytes)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if !whole {
			return errJobLogLine
		}
		if line == "" && errors.Is(err, io.EOF) {
			break
		}
		if mergerReads(line) {
			progressBytes += len(line) + 1
			if progressBytes > jobLogProgressBytes {
				return errJobLogProgress
			}
			progress = append(progress, keptLine{n, line})
		} else {
			text := plainLogLine(line)
			if len(text) > jobLogLineWidth {
				text = strings.ToValidUTF8(text[:jobLogLineWidth], "") + "…"
			}
			// A line the merger reads nothing from must not become one by its
			// cleanup (a control byte that made a JSON event invalid, a cut
			// that ends a token): it is left out instead.
			if !mergerReads(text) {
				tail = append(tail, keptLine{n, text})
				if len(tail) > jobLogTailLines {
					tail = tail[1:]
				}
			}
		}
		n++
		if errors.Is(err, io.EOF) {
			break
		}
	}
	// A tail line before the last jobLogTailLines lines of the whole is not
	// one of them: the proofrun lines among those count.
	kept := progress
	for _, l := range tail {
		if l.index >= n-jobLogTailLines {
			kept = append(kept, l)
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].index < kept[j].index })
	bw := bufio.NewWriter(w)
	next := 0
	for _, l := range kept {
		if gap := l.index - next; gap > 0 {
			fmt.Fprintf(bw, "[proofrun job-log: %d lines left out]\n", gap)
		}
		bw.WriteString(l.text)
		bw.WriteByte('\n')
		next = l.index + 1
	}
	if gap := n - next; gap > 0 {
		fmt.Fprintf(bw, "[proofrun job-log: %d lines left out]\n", gap)
	}
	return bw.Flush()
}

// readBoundedLine reads one line without its newline, as strings.Split on
// "\n" would give it. A line longer than max bytes is not read whole: whole
// is false, and the reader is left inside it. io.EOF comes with the last line,
// or alone after it.
func readBoundedLine(br *bufio.Reader, max int) (line string, whole bool, err error) {
	var b strings.Builder
	for {
		frag, err := br.ReadSlice('\n')
		frag = bytesTrimNewline(frag)
		if b.Len()+len(frag) > max {
			return "", false, nil
		}
		b.Write(frag)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return b.String(), true, err
	}
}

// bytesTrimNewline is frag without its one trailing newline.
func bytesTrimNewline(frag []byte) []byte {
	if n := len(frag); n > 0 && frag[n-1] == '\n' {
		return frag[:n-1]
	}
	return frag
}

// cmdJobLog is `proofrun job-log`: it bounds the job log on standard input
// onto standard output (boundJobLog). Exit 1 when the log's proofrun lines
// pass the bound, or it cannot be read, with nothing written to standard
// output; exit 2 for an invalid invocation.
func cmdJobLog(args []string) int {
	fs := flag.NewFlagSet("job-log", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "proofrun job-log <LOG >BOUNDED")
		return 2
	}
	var out strings.Builder
	if err := boundJobLog(stdin, &out); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun job-log:", err)
		return 1
	}
	if _, err := io.WriteString(stdout, out.String()); err != nil {
		fmt.Fprintln(os.Stderr, "proofrun job-log:", err)
		return 1
	}
	return 0
}

// mergerReads says the merger reads something from a log line: a proofrun
// progress line (logProgress) or a test event in JSON (attemptProgress),
// decided on the line as it is.
func mergerReads(line string) bool {
	if strings.Contains(line, "proofrun: ") && (progressStart.MatchString(line) || progressOutcome.MatchString(line)) {
		return true
	}
	if t := strings.TrimSpace(line); strings.HasPrefix(t, "{") {
		var ev event
		return json.Unmarshal([]byte(line), &ev) == nil && ev.Test != ""
	}
	return false
}
