package orker

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/orkstack/orkstack/pkg/logstream"
)

// attemptMask is one attempt's guard against its secrets reaching what
// leaves the container (docs/SECURITY_MODEL.md, "Masking and its limits"):
// the masker in front of both streams, the whole-text masking of every
// failure and [ork] line, and the list a task's values.json is checked
// against. It is a guard against accidents, not a boundary.
//
// A masker is installed only once a value with a form is registered, so a
// task with no secret, or whose secrets all say mask: false, is captured
// byte-exact as before.
type attemptMask struct {
	logs   bool // orker.log.mask_secrets
	values bool // orker.values.refuse_secrets
	note   func(line string)
	// finder builds the values check's automaton: logstream.NewSecretFinder,
	// or a test's, to prove a check that cannot be built fails the attempt.
	finder func([]logstream.NamedValue) (*logstream.SecretFinder, error)

	mu      sync.Mutex
	masker  *logstream.Masker
	writers []streamWriter
	masked  map[string]int64 // replacements the closed writers made, by stream
	// checked is what values.json is held to: every secret that does not say
	// mask: false, in declaration order.
	checked []logstream.NamedValue
	// sizes is every registered secret's own forms, for a refusal's
	// contributors.
	sizes []maskContributor
}

// The two switches, orker.log.mask_secrets and orker.values.refuse_secrets.
// Both are on unless set false, and every Orker of a pool has the same.
const (
	MaskSecretsVar          = "ORK_ORKER_LOG_MASK_SECRETS"
	DefaultMaskSecrets      = true
	RefuseSecretValuesVar   = "ORK_ORKER_VALUES_REFUSE_SECRETS"
	DefaultRefuseSecretVals = true
)

// orker.refuse_readable_engine_keys: an Orker that can open a file one of
// the engine's key settings names refuses to start, where by default it
// logs an error (docs/SECURITY_MODEL.md, "The Orker's own sources").
const (
	RefuseReadableEngineKeysVar     = "ORK_ORKER_REFUSE_READABLE_ENGINE_KEYS"
	DefaultRefuseReadableEngineKeys = false
)

// streamWriter is one stream's masking writer and the stream it masks.
type streamWriter struct {
	stream string
	w      *logstream.MaskWriter
}

// The streams a mask writer is named by, as its metric labels them.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// maskContributor is one secret's share of the masker's set, named by its
// declared name: never its value.
type maskContributor struct {
	name         string
	forms, bytes int
}

// newAttemptMask returns the attempt's mask under the Orker's two switches.
// note writes one line to the task's own log.
func (d *OCIDispatcher) newAttemptMask(note func(string)) *attemptMask {
	if note == nil {
		note = func(string) {}
	}
	finder := d.valuesFinder
	if finder == nil {
		finder = logstream.NewSecretFinder
	}
	return &attemptMask{logs: !d.LogUnmasked, values: !d.AllowSecretValues, note: note, finder: finder}
}

// secretValue is one resolved secret, for register.
type secretValue struct {
	name     string
	value    []byte
	unmasked bool
}

// register takes every resolved secret of the attempt, before the container
// can print one. It fails MASK_CAPACITY_EXCEEDED when their forms do not fit
// the masker's bounds, or the check's: an attempt that cannot be masked for
// its whole life is told so before it does any work.
func (m *attemptMask) register(secrets []secretValue) *Failure {
	if m == nil {
		hidden := 0
		for _, s := range secrets {
			if !s.unmasked {
				hidden++
			}
		}
		if hidden == 0 {
			return nil
		}
		// Every attempt has a mask (executeAttempt); a path without one
		// would run the container unmasked and unchecked, so it fails.
		return Fail(ReasonWorkdirFailed, "this attempt has no masker for its %d secrets; nothing was started", hidden)
	}
	// The notes are written once the lock is let go: writing one masks it,
	// which takes the lock.
	var notes []string
	defer func() {
		for _, line := range notes {
			m.note(line)
		}
	}()
	m.mu.Lock()
	defer m.mu.Unlock()
	// Every secret's share first, so a refusal names the largest of all of
	// them, whichever one passed the bound.
	for _, s := range secrets {
		if s.unmasked {
			continue
		}
		size := 0
		for _, f := range logstream.MaskForms(s.value) {
			size += len(f)
		}
		m.sizes = append(m.sizes, maskContributor{name: s.name, forms: len(logstream.MaskForms(s.value)), bytes: size})
	}
	for _, s := range secrets {
		if s.unmasked {
			// Its author said it is not one to hide: neither masked nor
			// looked for. Another secret holding the same bytes still masks
			// them.
			continue
		}
		forms := logstream.MaskForms(s.value)
		if m.values {
			m.checked = append(m.checked, logstream.NamedValue{Name: s.name, Value: s.value})
		}
		if len(forms) == 0 {
			if m.logs {
				notes = append(notes, fmt.Sprintf("[ork] secret %q is shorter than %d bytes and is not masked in logs",
					s.name, logstream.MaskMinFormBytes))
			}
			if m.values {
				notes = append(notes, fmt.Sprintf("[ork] secret %q is shorter than %d bytes and is not looked for in values.json",
					s.name, logstream.MaskMinFormBytes))
			}
			continue
		}
		if !m.logs {
			continue
		}
		if m.masker == nil {
			m.masker = logstream.NewMasker()
		}
		if _, err := m.masker.Add("secret:"+s.name, s.value); err != nil {
			return m.capacityFailure(err)
		}
	}
	if m.values && len(m.checked) > 0 {
		// The check's own automaton must fit too: it is built again at the
		// check, and what does not fit there fails the attempt, so a set
		// that cannot fit is refused now, before the container runs.
		if _, err := m.finder(m.checked); err != nil {
			return m.capacityFailure(err)
		}
	}
	return nil
}

// capacityFailure names the bound and the largest contributors by their
// declared names and sizes, never a value.
func (m *attemptMask) capacityFailure(err error) *Failure {
	var capacity *logstream.MaskCapacityError
	if !errors.As(err, &capacity) {
		return FailCause(ReasonMaskCapacityExceeded, err, "the task's secrets cannot be masked: %v", err)
	}
	largest := append([]maskContributor(nil), m.sizes...)
	sort.SliceStable(largest, func(i, j int) bool { return largest[i].bytes > largest[j].bytes })
	if len(largest) > 3 {
		largest = largest[:3]
	}
	names := make([]string, 0, len(largest))
	for _, c := range largest {
		names = append(names, fmt.Sprintf("secret %q (%d forms, %d bytes)", c.name, c.forms, c.bytes))
	}
	return Fail(ReasonMaskCapacityExceeded,
		"the task's secrets would need %d masking forms of %d bytes, past the bounds of %d forms and %d bytes; "+
			"the largest: %s. Give the task fewer or smaller secrets, or set mask: false on one that is "+
			"configuration rather than a secret",
		capacity.Forms, capacity.Bytes, capacity.MaxForms, capacity.MaxBytes, strings.Join(names, ", "))
}

// writer returns w behind the attempt's masker, one stream's own state: w
// itself when nothing is masked, so the capture stays byte-exact.
func (m *attemptMask) writer(stream string, w io.Writer) io.Writer {
	if m == nil {
		return w
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.masker == nil {
		return w
	}
	mw := m.masker.Writer(w)
	m.writers = append(m.writers, streamWriter{stream: stream, w: mw})
	return mw
}

// closeStreams masks and passes on what each stream still holds: a task
// killed in the middle of printing a secret leaves `***`, not all but its
// last byte. It returns the first error a stream's writer met.
func (m *attemptMask) closeStreams() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	writers := m.writers
	m.writers = nil
	m.mu.Unlock()
	var first error
	for _, sw := range writers {
		if err := sw.w.Close(); err != nil && first == nil {
			first = err
		}
		m.mu.Lock()
		if m.masked == nil {
			m.masked = map[string]int64{}
		}
		m.masked[sw.stream] += sw.w.Masked()
		m.mu.Unlock()
	}
	return first
}

// text masks a message the Orker composed whole: a failure, an [ork] line.
func (m *attemptMask) text(s string) string {
	if m == nil {
		return s
	}
	m.mu.Lock()
	masker := m.masker
	m.mu.Unlock()
	if masker == nil {
		return s
	}
	return masker.Mask(s)
}

// active reports whether any value is masked.
func (m *attemptMask) active() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.masker != nil
}

// line masks one framed log line the Orker wrote itself. A container's own
// lines were masked upstream, as bytes, before they were framed. Every line
// the Orker composes begins "[ork]"; one that did not would leave unmasked,
// so a new line of the Orker's keeps the prefix.
func (m *attemptMask) line(s string) string {
	if strings.HasPrefix(s, "[ork]") {
		return m.text(s)
	}
	return s
}

// failure masks every text of f that leaves the attempt: its message and its
// scan notes. Its cause is kept for errors.Is and never reported.
func (m *attemptMask) failure(f *Failure) *Failure {
	if m == nil || f == nil {
		return f
	}
	f.Message = m.text(f.Message)
	if f.Cause != nil {
		if prior, ok := f.Cause.(*maskedCause); ok {
			prior.text = m.text(prior.text)
		} else {
			f.Cause = &maskedCause{text: m.text(f.Cause.Error()), err: f.Cause}
		}
	}
	if f.Detail != nil {
		for i, note := range f.Detail.GetScanNotes() {
			f.Detail.ScanNotes[i] = m.text(note)
		}
	}
	return f
}

// maskedCause is a failure's cause whose text is masked; errors.Is and As
// still reach the cause it wraps.
type maskedCause struct {
	text string
	err  error
}

func (c *maskedCause) Error() string { return c.text }
func (c *maskedCause) Unwrap() error { return c.err }

// MaskStats is one attempt's masking, for its record and its metrics: how
// many runs of bytes were replaced, and the size of the set. Never which
// secret, or where.
type MaskStats struct {
	// Occurrences is the sum of ByStream.
	Occurrences int64
	// ByStream is the replacements made in each stream (StreamStdout,
	// StreamStderr); nil when nothing was masked.
	ByStream     map[string]int64
	Forms, Bytes int
}

func (m *attemptMask) stats() MaskStats {
	if m == nil {
		return MaskStats{}
	}
	// A writer's lock is never taken under m.mu: a Write holds it while its
	// sink masks an [ork] line, which takes m.mu.
	m.mu.Lock()
	stats := MaskStats{}
	for stream, n := range m.masked {
		stats.add(stream, n)
	}
	writers := append([]streamWriter(nil), m.writers...)
	masker := m.masker
	m.mu.Unlock()
	for _, sw := range writers {
		stats.add(sw.stream, sw.w.Masked())
	}
	if masker != nil {
		stats.Forms, stats.Bytes = masker.Size()
	}
	return stats
}

func (s *MaskStats) add(stream string, n int64) {
	if n == 0 {
		return
	}
	if s.ByStream == nil {
		s.ByStream = map[string]int64{}
	}
	s.ByStream[stream] += n
	s.Occurrences += n
}

// checkValues fails the attempt VALUES_CONTAIN_SECRET when values.json holds
// one of its secrets, in any spelling the masker knows, in its bytes or in
// any decoded string value or object key. It names the secret, and where by
// position only, never a key's text and never the value. It fails closed: a
// check that cannot be built, or a document it cannot read, fails the
// attempt.
func (m *attemptMask) checkValues(raw []byte) *Failure {
	if m == nil || !m.values {
		return nil
	}
	m.mu.Lock()
	checked := m.checked
	m.mu.Unlock()
	if len(checked) == 0 {
		return nil
	}
	finder, err := m.finder(checked)
	if err != nil {
		return m.capacityFailure(err)
	}
	name, path, failure := findSecretInValues(finder, raw)
	if failure != nil {
		return failure
	}
	if name == "" {
		return nil
	}
	return Fail(ReasonValuesContainSecret,
		"%s holds the value of secret %q (at %s); a result is copied into the run's history and its "+
			"store, where whoever reads the run reads it. Return a reference to the secret, not its value",
		ContainerValuesJSON, name, path)
}

// findSecretInValues reads raw as the strict reader does and reports the
// first secret it holds and where, as a path of array indexes and object
// member ordinals ($[2].#3: the third member of the object at index 2).
func findSecretInValues(finder *logstream.SecretFinder, raw []byte) (name, path string, failure *Failure) {
	type frame struct {
		object  bool
		n       int  // elements, or members, begun so far
		wantKey bool // an object's next string is a member's name
	}
	var stack []frame
	render := func() string {
		var b strings.Builder
		b.WriteString("$")
		for _, f := range stack {
			if f.object {
				b.WriteString(".#" + strconv.Itoa(f.n))
			} else {
				b.WriteString("[" + strconv.Itoa(f.n-1) + "]")
			}
		}
		return b.String()
	}
	// begin records that a value starts in the frame on top: the next
	// element of an array, or the value of the member just named.
	begin := func() {
		if len(stack) == 0 {
			return
		}
		top := &stack[len(stack)-1]
		if top.object {
			top.wantKey = true
		} else {
			top.n++
		}
	}
	dec := jsontext.NewDecoder(bytes.NewReader(raw))
	for {
		before := dec.InputOffset()
		tok, err := dec.ReadToken()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", "", FailCause(ReasonValuesMalformed, err,
				"%s could not be read for the secret check: %v", ContainerValuesJSON, err)
		}
		after := dec.InputOffset()
		switch kind := tok.Kind(); kind {
		case '{', '[':
			begin()
			stack = append(stack, frame{object: kind == '{', wantKey: kind == '{'})
		case '}', ']':
			stack = stack[:len(stack)-1]
		default:
			if top := len(stack) - 1; top >= 0 && stack[top].object && stack[top].wantKey {
				stack[top].n++
				stack[top].wantKey = false
			} else {
				begin()
			}
			// The token's own bytes: what lies before it is whitespace and
			// the separator that ended the token before.
			token := bytes.TrimLeft(raw[before:after], " \t\r\n,:")
			if found, ok := finder.Find(token); ok {
				return found, render(), nil
			}
			if kind == '"' {
				if found, ok := finder.Find([]byte(tok.String())); ok {
					return found, render(), nil
				}
			}
		}
	}
	// What spans tokens: a value written across a key and its string, say.
	if found, ok := finder.Find(raw); ok {
		return found, "$", nil
	}
	return "", "", nil
}
