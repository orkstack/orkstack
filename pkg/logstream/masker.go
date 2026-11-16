package logstream

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// The bounds of one masker's set of forms: a value whose forms would take
// the set past either is refused whole.
const (
	MaskMaxForms     = 1024
	MaskMaxFormBytes = 512 << 10
)

// MaskReplacement is what every masked run of bytes becomes.
const MaskReplacement = "***"

// MaskCapacityError is an Add refused because the set would pass a bound.
// It names the totals the set would have had, never a value.
type MaskCapacityError struct {
	Forms, Bytes       int
	MaxForms, MaxBytes int
}

func (e *MaskCapacityError) Error() string {
	return fmt.Sprintf("masking would need %d forms of %d bytes, past the bounds of %d forms and %d bytes",
		e.Forms, e.Bytes, e.MaxForms, e.MaxBytes)
}

// Masker replaces every registered secret in a task's output with
// MaskReplacement. One masker serves an
// attempt; each output stream gets its own Writer, with its own match
// state, and the writers share only the set of forms.
//
// A form is held by whoever registered a value that produces it (Add's
// holder) and leaves with its last holder (Drop), so two holders of equal
// values never unmask each other. Forms may be added and dropped while the
// streams run: the automaton is rebuilt outside the writers' locks, which
// are taken only to swap it in and to read the held bytes again.
//
// It is a guard against accidents, not a boundary: a task that means to
// disclose a value can transform it first.
type Masker struct {
	mu      sync.Mutex
	held    map[string]map[string]struct{} // holder → its forms
	holders map[string]int                 // form → how many holders
	bytes   int
	match   *matcher
	gen     uint64 // counts rebuilds; a writer never takes an older automaton
	writers map[*MaskWriter]struct{}
}

// NewMasker returns a masker with no form: its writers pass every byte
// through unchanged until a form is added.
func NewMasker() *Masker {
	return &Masker{
		held:    map[string]map[string]struct{}{},
		holders: map[string]int{},
		match:   newMatcher(nil),
		writers: map[*MaskWriter]struct{}{},
	}
}

// Add registers value's forms (MaskForms) under holder. It reports whether
// value has any form; one that has none — a value shorter than
// MaskMinFormBytes — is left unmasked, and the caller says so. A value that
// has forms is still masked only in the spellings MaskForms lists: a long
// value in fewer of them, a multi-line value line by line, and a line
// shorter than MaskMinFormBytes not at all. An Add that
// would take the set past MaskMaxForms or MaskMaxFormBytes changes nothing
// and returns a *MaskCapacityError.
//
// Add returns once every writer holds the new forms, so a value is masked
// from the moment Add returns: register it before anything can print it.
// It waits for each writer's current Write, and so for that writer's sink: a
// stalled sink stalls Add, and Drop, until it moves, and a sink that calls
// Add or Drop from inside its own Write deadlocks.
func (m *Masker) Add(holder string, value []byte) (bool, error) {
	forms := MaskForms(value)
	if len(forms) == 0 {
		return false, nil
	}
	m.mu.Lock()
	mine := m.held[holder]
	newForms, newBytes := 0, 0
	seen := map[string]bool{}
	for _, f := range forms {
		key := string(f)
		if m.holders[key] == 0 && !seen[key] {
			newForms++
			newBytes += len(f)
		}
		seen[key] = true
	}
	total, totalBytes := len(m.holders)+newForms, m.bytes+newBytes
	if total > MaskMaxForms || totalBytes > MaskMaxFormBytes {
		m.mu.Unlock()
		return false, &MaskCapacityError{Forms: total, Bytes: totalBytes, MaxForms: MaskMaxForms, MaxBytes: MaskMaxFormBytes}
	}
	if mine == nil {
		mine = map[string]struct{}{}
		m.held[holder] = mine
	}
	for _, f := range forms {
		key := string(f)
		if _, ok := mine[key]; ok {
			continue
		}
		mine[key] = struct{}{}
		if m.holders[key] == 0 {
			m.bytes += len(key)
		}
		m.holders[key]++
	}
	if newForms == 0 {
		m.mu.Unlock()
		return true, nil
	}
	m.rebuildAndUnlock()
	return true, nil
}

// Drop removes holder from every form it holds; a form left with no holder
// is no longer masked.
func (m *Masker) Drop(holder string) {
	m.mu.Lock()
	mine, ok := m.held[holder]
	if !ok {
		m.mu.Unlock()
		return
	}
	delete(m.held, holder)
	changed := false
	for key := range mine {
		if m.holders[key]--; m.holders[key] <= 0 {
			delete(m.holders, key)
			m.bytes -= len(key)
			changed = true
		}
	}
	if !changed {
		m.mu.Unlock()
		return
	}
	m.rebuildAndUnlock()
}

// Size is the set's size: how many forms, of how many bytes.
func (m *Masker) Size() (forms, bytes int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.holders), m.bytes
}

// rebuildAndUnlock builds the automaton over the current forms, which m.mu
// guards, releases m.mu, and swaps the automaton into every writer. No
// writer's lock is held while building, and no lock of the masker while a
// writer passes its released bytes on, so a slow or reentrant sink stalls
// nobody else. A writer keeps the newest automaton it was given.
func (m *Masker) rebuildAndUnlock() {
	forms := make([][]byte, 0, len(m.holders))
	for key := range m.holders {
		forms = append(forms, []byte(key))
	}
	sort.Slice(forms, func(i, j int) bool { return string(forms[i]) < string(forms[j]) })
	match := newMatcher(forms)
	m.gen++
	m.match = match
	gen := m.gen
	writers := make([]*MaskWriter, 0, len(m.writers))
	for w := range m.writers {
		writers = append(writers, w)
	}
	m.mu.Unlock()
	for _, w := range writers {
		w.swap(match, gen)
	}
}

// Writer returns a writer that masks one stream before it reaches w. Close
// it when the stream ends: what it still holds is masked, not released, and
// the masker stops updating it; a writer never closed stays registered with
// the masker until the masker is dropped.
func (m *Masker) Writer(w io.Writer) *MaskWriter {
	m.mu.Lock()
	defer m.mu.Unlock()
	mw := &MaskWriter{owner: m, w: w, match: m.match, gen: m.gen}
	m.writers[mw] = struct{}{}
	return mw
}

// Mask returns s with every form masked, for a message the Orker composes
// whole (a failure, an [ork] line). A tail of s that only begins a form is
// masked when it is at least MaskMinFormBytes long: a message cut short by
// a bound must not end in most of a secret.
func (m *Masker) Mask(s string) string {
	m.mu.Lock()
	match := m.match
	m.mu.Unlock()
	if match.empty() {
		return s
	}
	var out strings.Builder
	st := maskStream{match: match}
	st.feed([]byte(s))
	st.finish(MaskMinFormBytes)
	st.emit(&out, true)
	return out.String()
}

// MaskWriter masks one output stream. It is safe for concurrent use, though
// a stream is normally written by one goroutine.
type MaskWriter struct {
	owner *Masker
	mu    sync.Mutex
	w     io.Writer
	match *matcher
	gen   uint64
	st    maskStream
	err   error
	done  bool
	out   []byte
}

// Write masks p and passes on every byte that can no longer become part of
// an occurrence. It holds only the longest tail that still begins a form,
// which never spans a line terminator, so a complete line is never held.
func (w *MaskWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	if w.done {
		return 0, io.ErrClosedPipe
	}
	if w.match.empty() && len(w.st.buf) == 0 {
		n, err := w.w.Write(p)
		if err == nil && n < len(p) {
			err = io.ErrShortWrite
		}
		if err != nil {
			w.err = err
		}
		return n, err
	}
	w.st.match = w.match
	w.st.feed(p)
	return len(p), w.flushLocked(false)
}

// Err is the first error the underlying writer returned, nil if none: from a
// Write, a Close, or bytes a swap released (Add, Drop), which no Write call
// reports until the next one. Bytes that failed to pass on are not retried.
func (w *MaskWriter) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// Masked is how many runs of bytes the writer has replaced so far: each
// MaskReplacement it passed on. A run cut at the held bound counts once for
// each part. It says how often, never what or where.
func (w *MaskWriter) Masked() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.st.masked
}

// Close masks what the stream still holds, passes it on, and detaches the
// writer from its masker. It returns the writer's first error, so a stream
// that lost output says so at its end. It does not close the underlying
// writer.
func (w *MaskWriter) Close() error {
	w.mu.Lock()
	if w.done {
		w.mu.Unlock()
		return w.err
	}
	w.done = true
	err := w.err
	if err == nil {
		w.st.finish(0)
		err = w.flushLocked(true)
	}
	w.mu.Unlock()
	w.owner.mu.Lock()
	delete(w.owner.writers, w)
	w.owner.mu.Unlock()
	return err
}

// swap installs automaton gen and reads the held bytes again under it; an
// automaton older than the writer's is ignored. A failure to pass bytes on
// is kept, and the next Write returns it.
func (w *MaskWriter) swap(match *matcher, gen uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if gen <= w.gen {
		return
	}
	w.match, w.gen = match, gen
	if w.done || w.err != nil {
		return
	}
	w.st.rescan(match)
	_ = w.flushLocked(false) // a failure is kept in w.err for the next Write
}

func (w *MaskWriter) flushLocked(all bool) error {
	w.out = w.out[:0]
	w.out = w.st.appendEmit(w.out, all)
	if len(w.out) == 0 {
		return nil
	}
	n, err := w.w.Write(w.out)
	if err == nil && n < len(w.out) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.err = err
	}
	return err
}

// maskStream is one stream's match state: the bytes not yet passed on, the
// runs of them inside an occurrence, and the automaton's state.
type maskStream struct {
	match *matcher
	state int32
	buf   []byte // bytes not yet passed on; buf[0] is at offset base
	base  int64
	runs  []maskRun // sorted, disjoint, not touching; offsets are absolute
	// cut, when hasCut, is the end of the held rest of a run cut at
	// maskMaxHeld: those bytes lie inside an occurrence that began among
	// bytes already passed on, so a rescan cannot find it again and must
	// keep them masked.
	cut    int64
	hasCut bool
	// masked counts the replacements passed on.
	masked int64
}

// maskRun is the bytes [start, end] that lie inside an occurrence.
type maskRun struct{ start, end int64 }

func (s *maskStream) pos() int64 { return s.base + int64(len(s.buf)) }

func (s *maskStream) feed(p []byte) {
	for _, b := range p {
		s.buf = append(s.buf, b)
		s.state = s.match.step(s.state, b)
		if n := s.match.nodes[s.state].out; n > 0 {
			end := s.pos() - 1
			s.addRun(end-int64(n)+1, end)
		}
	}
}

// addRun adds [start, end], merged with every run it overlaps or touches.
// Occurrences are found in the order they end, so the new run ends at or
// after every run held: it can merge only with a suffix of the list.
func (s *maskStream) addRun(start, end int64) {
	start = max(start, s.base)
	for len(s.runs) > 0 {
		last := s.runs[len(s.runs)-1]
		if last.end+1 < start {
			break
		}
		start, end = min(start, last.start), max(end, last.end)
		s.runs = s.runs[:len(s.runs)-1]
	}
	s.runs = append(s.runs, maskRun{start, end})
}

// rescan reads every byte not yet passed on again under match, from the
// automaton's root, and finds its runs afresh: a form added mid-stream masks
// what is still held (every occurrence of a form kept is found again), and a
// form dropped releases what only it was masking. Bytes already passed on
// are not read again, so an occurrence of a new form that began among them
// is not masked: a value is registered before anything can print it.
func (s *maskStream) rescan(match *matcher) {
	s.match, s.state = match, 0
	s.runs = s.runs[:0]
	if s.hasCut && s.cut >= s.base {
		s.runs = append(s.runs, maskRun{s.base, s.cut})
	}
	held := append([]byte(nil), s.buf...)
	s.buf = s.buf[:0]
	s.feed(held)
}

// finish ends the stream: the tail that only begins a form becomes a run of
// its own when it is at least minHeld bytes long (0: always).
func (s *maskStream) finish(minHeld int) {
	if s.match.empty() {
		return
	}
	depth := int64(s.match.nodes[s.state].depth)
	if depth > 0 && depth >= int64(minHeld) {
		s.addRun(s.pos()-depth, s.pos()-1)
	}
	s.state = 0
}

// maskMaxHeld bounds the bytes a stream holds for a run the held tail could
// still extend: past it the run is passed on as one replacement and the rest
// of it masked again, so a secret repeated without end holds no more than
// this and the tail. A longer run then reads as "******".
const maskMaxHeld = 64 << 10

// releasable is the offset before which every byte is final: the start of
// the tail that still begins a form, or of a run that tail could extend
// while no more than maskMaxHeld bytes are held.
func (s *maskStream) releasable(all bool) int64 {
	if all {
		return s.pos()
	}
	// Every run that reaches the held tail, or touches it, can still be
	// joined by an occurrence that starts there; runs are sorted, so they
	// are a suffix of the list.
	hold := s.pos() - int64(s.match.nodes[s.state].depth)
	if len(s.buf) > maskMaxHeld {
		return hold
	}
	tail := hold
	for i := len(s.runs) - 1; i >= 0 && s.runs[i].end+1 >= tail; i-- {
		hold = min(hold, s.runs[i].start)
	}
	return hold
}

// appendEmit appends what may be passed on to out, each run as one
// MaskReplacement, and forgets it.
func (s *maskStream) appendEmit(out []byte, all bool) []byte {
	until := s.releasable(all)
	at := s.base
	i := 0
	for ; i < len(s.runs) && s.runs[i].start < until; i++ {
		r := s.runs[i]
		out = append(out, s.buf[at-s.base:r.start-s.base]...)
		out = append(out, MaskReplacement...)
		s.masked++
		if r.end >= until {
			// A run cut at the bound: its rest stays held, masked, a swap
			// included.
			s.runs[i].start = until
			s.cut, s.hasCut = r.end, true
			at = until
			break
		}
		at = r.end + 1
	}
	if at < until {
		out = append(out, s.buf[at-s.base:until-s.base]...)
		at = until
	}
	s.runs = append(s.runs[:0], s.runs[i:]...)
	n := copy(s.buf, s.buf[at-s.base:])
	s.buf = s.buf[:n]
	s.base = at
	return out
}

func (s *maskStream) emit(w io.Writer, all bool) {
	_, _ = w.Write(s.appendEmit(nil, all))
}
