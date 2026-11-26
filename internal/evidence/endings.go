package evidence

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"strconv"
	"time"
)

// Ending is one ending of a run that was resumed: how the run stood when it
// ended under this scope, and the member holding the archive body of it.
type Ending struct {
	// Scope is 0 for the run's first ending, else that of the resume it
	// followed: the sequence the stream goes on at after that resume.
	Scope uint64 `json:"scope"`
	// Path is run.jsonl for the run's newest ending, EndingPath(Scope) for an
	// earlier one.
	Path string `json:"path"`
	// RunStatus, Reason and RunFinishedAt are how the run ended then; empty
	// when the ending's body is not in the bundle.
	RunStatus     string    `json:"run_status,omitzero"`
	Reason        string    `json:"reason,omitzero"`
	RunFinishedAt time.Time `json:"run_finished_at,omitzero"`
}

// runPath is the member holding the body of a run's newest ending.
const runPath = "run.jsonl"

// EndingPath is the member of an earlier ending's archive body.
func EndingPath(scope uint64) string { return fmt.Sprintf("endings/%020d.jsonl", scope) }

// checkEndings holds a manifest's endings to what the engine writes: the first
// ending first, the newest last, each with its member.
func checkEndings(m *Manifest) error {
	if len(m.Endings) == 0 {
		return nil
	}
	listed := map[string]bool{}
	for _, member := range m.Members {
		listed[member.Path] = true
	}
	for i, e := range m.Endings {
		path := EndingPath(e.Scope)
		if i == len(m.Endings)-1 {
			path = runPath
		}
		switch {
		case len(m.Endings) == 1, (i == 0) != (e.Scope == 0), i > 0 && e.Scope <= m.Endings[i-1].Scope:
			return fmt.Errorf("evidence: MANIFEST.json endings are not the run's first and later ones, in order")
		case e.Path != path || !listed[path]:
			return fmt.Errorf("evidence: MANIFEST.json ending %d is not the member %s", e.Scope, path)
		}
	}
	return nil
}

// maxStreamLine bounds one line of an archive body as it is checked. A line
// holds one run or one event, and neither comes near it.
const maxStreamLine = 64 << 20

// streamCheck holds an archive body of a resumed run, as it is hashed, to the
// tiling of its event stream: every number from 1 to its last event is an
// event, is named by a gap line, or is skipped right before the number a resume
// made the stream go on at, and it reaches the ending it is of.
type streamCheck struct {
	scopes map[uint64]bool
	// reach is the number the stream must reach: the ending's own scope.
	reach   int64
	run     bool
	line    []byte
	last    int64
	failure *Failure
}

func newStreamCheck(m *Manifest, path string) *streamCheck {
	c := &streamCheck{scopes: map[uint64]bool{}, reach: 1}
	for _, e := range m.Endings {
		c.scopes[e.Scope] = e.Scope > 0
		if e.Path == path {
			c.reach = max(c.reach, int64(e.Scope))
		}
	}
	return c
}

// Write takes the body's bytes, a line at a time. It never fails: what it
// finds is a failure of the report.
func (c *streamCheck) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 && c.failure == nil {
		head, rest, ended := bytes.Cut(p, []byte("\n"))
		if len(c.line)+len(head) > maxStreamLine {
			c.fail(FailureBadStream, "", "a line above 64 MiB")
			break
		}
		c.line = append(c.line, head...)
		p = rest
		if ended {
			c.take(c.line)
			c.line = c.line[:0]
		}
	}
	return n, nil
}

// end checks what follows the last newline and returns what was found.
func (c *streamCheck) end() *Failure {
	if c.failure == nil {
		c.take(c.line)
	}
	switch {
	case c.failure != nil:
	case !c.run:
		c.fail(FailureBadStream, "", "no run line")
	case c.last < c.reach:
		c.fail(FailureEventSequence, strconv.FormatInt(c.reach, 10), strconv.FormatInt(c.last, 10))
	}
	return c.failure
}

func (c *streamCheck) fail(reason, want, got string) {
	c.failure, c.line = &Failure{Reason: reason, Want: want, Got: got}, nil
}

func (c *streamCheck) take(line []byte) {
	if len(bytes.TrimSpace(line)) == 0 {
		return
	}
	var record struct {
		Kind  string `json:"kind"`
		Event struct {
			Sequence int64  `json:"sequence,string"`
			Type     string `json:"type"`
		} `json:"event"`
		Gap struct {
			First int64 `json:"first"`
			Last  int64 `json:"last"`
		} `json:"gap"`
	}
	if err := json.Unmarshal(line, &record); err != nil {
		c.fail(FailureBadStream, "", "a line that is not an archive line")
		return
	}
	first, last := record.Event.Sequence, record.Event.Sequence
	switch record.Kind {
	case "run":
		c.run = true
		return
	case "gap":
		first, last = record.Gap.First, record.Gap.Last
		if first > c.last+1 && c.scopes[uint64(first)] {
			first = c.last + 1 // the event the stream went on at is the gap's
		}
	case "event":
		if first > c.last+1 && c.resumedAt(first, record.Event.Type) {
			first = c.last + 1
		}
	default:
		c.fail(FailureBadStream, "", fmt.Sprintf("a line of kind %q", record.Kind))
		return
	}
	if first != c.last+1 || last < first {
		c.fail(FailureEventSequence, strconv.FormatInt(c.last+1, 10), strconv.FormatInt(first, 10))
		return
	}
	c.last = last
}

// resumedAt reports whether a resume made the stream go on at this event: its
// RUN_RESUMED, or the end of a run whose resume could not go on.
func (c *streamCheck) resumedAt(sequence int64, eventType string) bool {
	if !c.scopes[uint64(sequence)] {
		return false
	}
	switch eventType {
	case "EVENT_TYPE_RUN_RESUMED", "EVENT_TYPE_RUN_FAILED", "EVENT_TYPE_RUN_CANCELED":
		return true
	}
	return false
}
