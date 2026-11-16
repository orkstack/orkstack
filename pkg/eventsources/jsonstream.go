package eventsources

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// The bounds of reading a body as JSON (docs/EVENT_SOURCES.md, "Reading the
// body as JSON"). Each is fixed: a body over one is rejected, never read in
// part.
const (
	// MaxJSONDepth is the deepest nesting a body may have.
	MaxJSONDepth = 64
	// MaxOpenKeyBytes bounds the key names of the objects open at once,
	// which the reader keeps to refuse a key written twice.
	MaxOpenKeyBytes = 1 << 20
	// MaxSelectedBytes bounds one selected value, as JSON text: a value
	// over it is "too large", never returned.
	MaxSelectedBytes = 4 << 10
	// maxPointers is the most pointers one read selects: 16 tests and 64
	// mapped inputs fit with room.
	maxPointers = 128
)

// MessageError is a message's rejection: one MESSAGE_* reason, and where in
// the body it was found. Its text never quotes the body or a key.
type MessageError struct {
	Reason string
	// Offset is the byte offset of the fault, for a reason about the body;
	// -1 when it is not about a place in it.
	Offset int
	// Detail is OrkStack's own words for what is wrong.
	Detail string
}

func (e *MessageError) Error() string {
	if e.Offset >= 0 {
		return fmt.Sprintf("%s: %s at byte %d", e.Reason, e.Detail, e.Offset)
	}
	return e.Reason + ": " + e.Detail
}

// Selected is what one read selected: for each pointer asked, the value's
// JSON text, or nothing, or "too large".
type Selected struct {
	values   map[string][]byte
	tooLarge map[string]bool
}

// Value returns the JSON text the pointer selected. ok is false when it
// selected nothing or a value over MaxSelectedBytes.
func (s Selected) Value(pointer string) ([]byte, bool) {
	v, ok := s.values[pointer]
	return v, ok
}

// TooLarge reports whether the pointer selected a value over
// MaxSelectedBytes.
func (s Selected) TooLarge(pointer string) bool { return s.tooLarge[pointer] }

// ParsePointer splits an RFC 6901 JSON Pointer into its reference tokens,
// unescaped. "" is the whole document.
func ParsePointer(pointer string) ([]string, error) {
	if pointer == "" {
		return nil, nil
	}
	if pointer[0] != '/' {
		return nil, fmt.Errorf("a JSON Pointer is empty or begins with /")
	}
	parts := strings.Split(pointer[1:], "/")
	for i, part := range parts {
		for j := 0; j < len(part); j++ {
			if part[j] == '~' && (j+1 == len(part) || (part[j+1] != '0' && part[j+1] != '1')) {
				return nil, fmt.Errorf("a ~ in a JSON Pointer is written ~0 or ~1")
			}
		}
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
	}
	return parts, nil
}

// ReadJSON validates body as one strict JSON value (AGENTS.md §3.1: valid
// UTF-8, one value, no key written twice in one object) in one pass that
// builds no tree, and returns the values the pointers select. What it
// allocates does not grow with the body: it keeps the path it is on, the key
// names of the objects open on that path, and the selected values.
func ReadJSON(body []byte, pointers []string) (Selected, *MessageError) {
	r := reader{body: body}
	if err := r.compile(pointers); err != nil {
		return Selected{}, err
	}
	r.skipSpace()
	if err := r.value(0); err != nil {
		return Selected{}, err
	}
	r.skipSpace()
	if r.pos != len(r.body) {
		return Selected{}, r.fail("a second value follows the first")
	}
	return r.selected, nil
}

// pointerPath is one compiled pointer.
type pointerPath struct {
	text   string
	tokens []string
	// index is each token read as an array index, -1 where it is none.
	index []int
}

// bits is a set of pointer indices.
type bits [maxPointers / 64]uint64

func (b *bits) set(i int)     { b[i/64] |= 1 << (i % 64) }
func (b bits) has(i int) bool { return b[i/64]&(1<<(i%64)) != 0 }

// next is the first member at or after from, or -1.
func (b bits) next(from int) int {
	for i := from; i < maxPointers; i++ {
		if b[i/64] == 0 {
			i = (i/64+1)*64 - 1
			continue
		}
		if b.has(i) {
			return i
		}
	}
	return -1
}

// openObject is one object on the current path. Its key names, unescaped,
// lie end to end in the reader's arena from start, each followed by its
// length; seen holds a hash of each, so a key written twice is found without
// a map entry the garbage collector has to follow.
type openObject struct {
	start int
	seen  map[uint64]struct{}
}

type reader struct {
	body     []byte
	pos      int
	pointers []pointerPath
	// alive[d] holds the pointers whose first d tokens match the path to
	// the value read at depth d.
	alive   [MaxJSONDepth + 2]bits
	objects []openObject
	arena   []byte
	seed    maphash.Seed
	scratch []byte
	// selected is what the pointers selected.
	selected Selected
}

func (r *reader) compile(pointers []string) *MessageError {
	if len(pointers) > maxPointers {
		return &MessageError{Reason: ReasonMessageUnprocessable, Offset: -1,
			Detail: fmt.Sprintf("%d selectors; at most %d are read at once", len(pointers), maxPointers)}
	}
	r.seed = maphash.MakeSeed()
	for i, p := range pointers {
		tokens, err := ParsePointer(p)
		if err != nil {
			return &MessageError{Reason: ReasonMessageUnprocessable, Offset: -1, Detail: err.Error()}
		}
		index := make([]int, len(tokens))
		for j, tok := range tokens {
			index[j] = arrayIndex(tok)
		}
		r.pointers = append(r.pointers, pointerPath{text: p, tokens: tokens, index: index})
		r.alive[0].set(i)
	}
	return nil
}

func (r *reader) fail(detail string) *MessageError {
	return &MessageError{Reason: ReasonMessageNotJSON, Offset: r.pos, Detail: detail}
}

func (r *reader) skipSpace() {
	for r.pos < len(r.body) {
		switch r.body[r.pos] {
		case ' ', '\t', '\n', '\r':
			r.pos++
		default:
			return
		}
	}
}

// value reads one value at depth and records it for each pointer whose path
// ends here.
func (r *reader) value(depth int) *MessageError {
	start := r.pos
	if err := r.valueBody(depth); err != nil {
		return err
	}
	live := r.alive[depth]
	for i := live.next(0); i >= 0; i = live.next(i + 1) {
		if len(r.pointers[i].tokens) == depth {
			r.record(r.pointers[i].text, r.body[start:r.pos])
		}
	}
	return nil
}

func (r *reader) record(pointer string, raw []byte) {
	if len(raw) > MaxSelectedBytes {
		if r.selected.tooLarge == nil {
			r.selected.tooLarge = map[string]bool{}
		}
		r.selected.tooLarge[pointer] = true
		return
	}
	if r.selected.values == nil {
		r.selected.values = map[string][]byte{}
	}
	r.selected.values[pointer] = raw
}

func (r *reader) valueBody(depth int) *MessageError {
	if r.pos >= len(r.body) {
		return r.fail("the body ends where a value was expected")
	}
	switch c := r.body[r.pos]; {
	case c == '{':
		return r.object(depth)
	case c == '[':
		return r.array(depth)
	case c == '"':
		_, err := r.str(false)
		return err
	case c == '-' || (c >= '0' && c <= '9'):
		return r.number()
	case c == 't':
		return r.literal("true")
	case c == 'f':
		return r.literal("false")
	case c == 'n':
		return r.literal("null")
	default:
		return r.fail("a character that begins no JSON value")
	}
}

func (r *reader) literal(word string) *MessageError {
	if !bytes.HasPrefix(r.body[r.pos:], []byte(word)) {
		return r.fail("a character that begins no JSON value")
	}
	r.pos += len(word)
	return nil
}

func (r *reader) enter(depth int) *MessageError {
	if depth+1 > MaxJSONDepth {
		return &MessageError{Reason: ReasonMessageJSONTooDeep, Offset: r.pos,
			Detail: fmt.Sprintf("nested deeper than %d levels", MaxJSONDepth)}
	}
	return nil
}

// descend sets the live pointers of the child at depth+1 reached by the key
// (an object's member) or the index (an array's element).
func (r *reader) descend(depth int, key []byte, index int, isIndex bool) {
	var next bits
	live := r.alive[depth]
	for i := live.next(0); i >= 0; i = live.next(i + 1) {
		p := &r.pointers[i]
		if len(p.tokens) <= depth {
			continue
		}
		if isIndex {
			if p.index[depth] == index {
				next.set(i)
			}
		} else if string(key) == p.tokens[depth] {
			next.set(i)
		}
	}
	r.alive[depth+1] = next
}

// arrayIndex is an RFC 6901 array index: digits with no leading zero, or -1.
func arrayIndex(tok string) int {
	if tok == "" || len(tok) > 9 || (len(tok) > 1 && tok[0] == '0') {
		return -1
	}
	n := 0
	for i := 0; i < len(tok); i++ {
		if tok[i] < '0' || tok[i] > '9' {
			return -1
		}
		n = n*10 + int(tok[i]-'0')
	}
	return n
}

func (r *reader) array(depth int) *MessageError {
	if err := r.enter(depth); err != nil {
		return err
	}
	r.pos++
	r.skipSpace()
	if r.pos < len(r.body) && r.body[r.pos] == ']' {
		r.pos++
		return nil
	}
	for index := 0; ; index++ {
		r.descend(depth, nil, index, true)
		r.skipSpace()
		if err := r.value(depth + 1); err != nil {
			return err
		}
		r.skipSpace()
		if r.pos >= len(r.body) {
			return r.fail("an array is not closed")
		}
		switch r.body[r.pos] {
		case ',':
			r.pos++
		case ']':
			r.pos++
			return nil
		default:
			return r.fail("an array's values are separated by commas")
		}
	}
}

func (r *reader) object(depth int) *MessageError {
	if err := r.enter(depth); err != nil {
		return err
	}
	r.pos++
	open := r.openObject()
	defer r.closeObject()
	r.skipSpace()
	if r.pos < len(r.body) && r.body[r.pos] == '}' {
		r.pos++
		return nil
	}
	for {
		r.skipSpace()
		if r.pos >= len(r.body) || r.body[r.pos] != '"' {
			return r.fail("an object's keys are strings")
		}
		keyAt := r.pos
		key, err := r.str(true)
		if err != nil {
			return err
		}
		if err := r.addKey(open, key, keyAt); err != nil {
			return err
		}
		r.descend(depth, key, 0, false)
		r.skipSpace()
		if r.pos >= len(r.body) || r.body[r.pos] != ':' {
			return r.fail("a key is followed by a colon")
		}
		r.pos++
		r.skipSpace()
		if err := r.value(depth + 1); err != nil {
			return err
		}
		r.skipSpace()
		if r.pos >= len(r.body) {
			return r.fail("an object is not closed")
		}
		switch r.body[r.pos] {
		case ',':
			r.pos++
		case '}':
			r.pos++
			return nil
		default:
			return r.fail("an object's members are separated by commas")
		}
	}
}

// openObject pushes an object onto the path, reusing the key set a closed
// object at this depth left behind, so reading many objects allocates no
// more than reading one.
func (r *reader) openObject() int {
	i := len(r.objects)
	if i < cap(r.objects) {
		r.objects = r.objects[:i+1]
		clear(r.objects[i].seen)
	} else {
		r.objects = append(r.objects, openObject{seen: map[uint64]struct{}{}})
	}
	r.objects[i].start = len(r.arena)
	return i
}

func (r *reader) closeObject() {
	i := len(r.objects) - 1
	r.arena = r.arena[:r.objects[i].start]
	r.objects = r.objects[:i]
}

// addKey records key in the object at index open, refusing one it holds
// already and keys past the budget.
func (r *reader) addKey(open int, key []byte, at int) *MessageError {
	if len(r.arena)+len(key) > MaxOpenKeyBytes {
		return &MessageError{Reason: ReasonMessageJSONTooLarge, Offset: at,
			Detail: fmt.Sprintf("the key names of the objects open at once total more than %d bytes", MaxOpenKeyBytes)}
	}
	obj := &r.objects[open]
	h := maphash.Bytes(r.seed, key)
	if _, seen := obj.seen[h]; seen && r.holdsKey(open, key) {
		return &MessageError{Reason: ReasonMessageNotJSON, Offset: at, Detail: "a key is written twice in one object"}
	}
	n := len(key)
	r.arena = append(r.arena, key...)
	r.arena = append(r.arena, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	obj.seen[h] = struct{}{}
	return nil
}

// holdsKey walks the object's keys back to front, for a key whose hash it
// has seen: a hash alone never refuses a body.
func (r *reader) holdsKey(open int, key []byte) bool {
	start := r.objects[open].start
	end := len(r.arena)
	for end > start {
		n := int(r.arena[end-4])<<24 | int(r.arena[end-3])<<16 | int(r.arena[end-2])<<8 | int(r.arena[end-1])
		keyStart := end - 4 - n
		if bytes.Equal(r.arena[keyStart:end-4], key) {
			return true
		}
		end = keyStart
	}
	return false
}

// str reads a string. With unescape it returns the string's content with
// JSON's escapes undone, in a buffer valid until the next call.
func (r *reader) str(unescape bool) ([]byte, *MessageError) {
	r.pos++ // the opening quote
	r.scratch = r.scratch[:0]
	for {
		if r.pos >= len(r.body) {
			return nil, r.fail("a string is not closed")
		}
		c := r.body[r.pos]
		switch {
		case c == '"':
			r.pos++
			return r.scratch, nil
		case c == '\\':
			if err := r.escape(unescape); err != nil {
				return nil, err
			}
		case c < 0x20:
			return nil, r.fail("a control character in a string")
		case c < utf8.RuneSelf:
			if unescape {
				r.scratch = append(r.scratch, c)
			}
			r.pos++
		default:
			rn, size := utf8.DecodeRune(r.body[r.pos:])
			if rn == utf8.RuneError && size <= 1 {
				return nil, r.fail("a string is not valid UTF-8")
			}
			if unescape {
				r.scratch = append(r.scratch, r.body[r.pos:r.pos+size]...)
			}
			r.pos += size
		}
	}
}

func (r *reader) escape(unescape bool) *MessageError {
	if r.pos+1 >= len(r.body) {
		return r.fail("a string is not closed")
	}
	var out byte
	switch r.body[r.pos+1] {
	case '"', '\\', '/':
		out = r.body[r.pos+1]
	case 'b':
		out = '\b'
	case 'f':
		out = '\f'
	case 'n':
		out = '\n'
	case 'r':
		out = '\r'
	case 't':
		out = '\t'
	case 'u':
		rn, err := r.unicodeEscape()
		if err != nil {
			return err
		}
		if unescape {
			r.scratch = utf8.AppendRune(r.scratch, rn)
		}
		return nil
	default:
		return r.fail("an escape JSON does not define")
	}
	if unescape {
		r.scratch = append(r.scratch, out)
	}
	r.pos += 2
	return nil
}

// unicodeEscape reads \uXXXX, and its pair when it is a high surrogate. A
// surrogate that is not one half of a pair is not strict JSON.
func (r *reader) unicodeEscape() (rune, *MessageError) {
	first, ok := hex4(r.body, r.pos+2)
	if !ok {
		return 0, r.fail("a \\u escape needs four hexadecimal digits")
	}
	if !utf16.IsSurrogate(first) {
		r.pos += 6
		return first, nil
	}
	if first >= 0xDC00 {
		return 0, r.fail("a lone surrogate in a string")
	}
	at := r.pos + 6
	if at+1 >= len(r.body) || r.body[at] != '\\' || r.body[at+1] != 'u' {
		return 0, r.fail("a lone surrogate in a string")
	}
	second, ok := hex4(r.body, at+2)
	if !ok || second < 0xDC00 || second > 0xDFFF {
		return 0, r.fail("a lone surrogate in a string")
	}
	r.pos = at + 6
	return utf16.DecodeRune(first, second), nil
}

func hex4(b []byte, at int) (rune, bool) {
	if at+4 > len(b) {
		return 0, false
	}
	var n rune
	for _, c := range b[at : at+4] {
		var d byte
		switch {
		case c >= '0' && c <= '9':
			d = c - '0'
		case c >= 'a' && c <= 'f':
			d = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			d = c - 'A' + 10
		default:
			return 0, false
		}
		n = n<<4 | rune(d)
	}
	return n, true
}

// number checks the JSON number grammar; the value's text is kept as it
// is, never converted.
func (r *reader) number() *MessageError {
	if r.body[r.pos] == '-' {
		r.pos++
	}
	switch {
	case r.pos < len(r.body) && r.body[r.pos] == '0':
		r.pos++
	case r.pos < len(r.body) && r.body[r.pos] >= '1' && r.body[r.pos] <= '9':
		r.digits()
	default:
		return r.fail("a number needs a digit")
	}
	if r.pos < len(r.body) && r.body[r.pos] == '.' {
		r.pos++
		if !r.digits() {
			return r.fail("a fraction needs a digit")
		}
	}
	if r.pos < len(r.body) && (r.body[r.pos] == 'e' || r.body[r.pos] == 'E') {
		r.pos++
		if r.pos < len(r.body) && (r.body[r.pos] == '+' || r.body[r.pos] == '-') {
			r.pos++
		}
		if !r.digits() {
			return r.fail("an exponent needs a digit")
		}
	}
	return nil
}

func (r *reader) digits() bool {
	start := r.pos
	for r.pos < len(r.body) && r.body[r.pos] >= '0' && r.body[r.pos] <= '9' {
		r.pos++
	}
	return r.pos > start
}

// strconvUnquote undoes a JSON string's escapes; raw is the string's JSON
// text, quotes included, already validated.
func strconvUnquote(raw []byte) (string, bool) {
	r := reader{body: raw}
	if len(raw) == 0 || raw[0] != '"' {
		return "", false
	}
	s, err := r.str(true)
	if err != nil || r.pos != len(raw) {
		return "", false
	}
	return string(s), true
}
