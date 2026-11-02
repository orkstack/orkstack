package dsl

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"
)

// SVG path data (an activity icon's d), held to the SVG 2 path-data grammar
// by a strict scanner. Nothing here builds markup: the engine only decides
// whether a d is acceptable, and an acceptable d is made of command letters,
// numbers and separators alone, so no client can read one as markup, a URL,
// an entity or CSS.

// pathArity is each command letter's argument count. Z takes none; every
// other command takes whole groups of its arity, repeated.
var pathArity = map[byte]int{
	'M': 2, 'm': 2, 'L': 2, 'l': 2, 'T': 2, 't': 2,
	'H': 1, 'h': 1, 'V': 1, 'v': 1,
	'C': 6, 'c': 6,
	'S': 4, 's': 4, 'Q': 4, 'q': 4,
	'A': 7, 'a': 7,
	'Z': 0, 'z': 0,
}

// isPathSpace is SVG's wsp: tab, line feed, form feed, carriage return and
// space.
func isPathSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\f' || b == '\r'
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

// startsNumber is a byte a number may begin with.
func startsNumber(b byte) bool { return isDigit(b) || b == '+' || b == '-' || b == '.' }

// pathScanner walks one d.
type pathScanner struct {
	d string
	i int
}

func (p *pathScanner) done() bool { return p.i >= len(p.d) }

func (p *pathScanner) spaces() {
	for !p.done() && isPathSpace(p.d[p.i]) {
		p.i++
	}
}

// separator consumes SVG's comma_wsp?, reporting whether it held a comma.
func (p *pathScanner) separator() bool {
	p.spaces()
	if !p.done() && p.d[p.i] == ',' {
		p.i++
		p.spaces()
		return true
	}
	return false
}

// unexpected names what stands at the scanner's position: the character
// there, quoted, or a byte that begins none.
func (p *pathScanner) unexpected(want string) error {
	if p.done() {
		return fmt.Errorf("ends where %s was expected", want)
	}
	r, size := utf8.DecodeRuneInString(p.d[p.i:])
	if r == utf8.RuneError && size <= 1 {
		return fmt.Errorf("has the byte 0x%02x at byte %d where %s was expected", p.d[p.i], p.i, want)
	}
	return fmt.Errorf("has %q at byte %d where %s was expected", r, p.i, want)
}

// maxIconCoordinate bounds every number an icon holds: its grid is 16 units,
// and nothing it draws needs a number past this.
const maxIconCoordinate = 10000

// number reads one number, greedily, and holds it to a finite float64:
// sign? (digits ("." digits?)? | "." digits) (("e"|"E") sign? digits)?
func (p *pathScanner) number() error {
	start := p.i
	if !p.done() && (p.d[p.i] == '+' || p.d[p.i] == '-') {
		p.i++
	}
	whole := p.i
	for !p.done() && isDigit(p.d[p.i]) {
		p.i++
	}
	digits := p.i > whole
	if !p.done() && p.d[p.i] == '.' {
		p.i++
		fraction := p.i
		for !p.done() && isDigit(p.d[p.i]) {
			p.i++
		}
		digits = digits || p.i > fraction
	}
	if !digits {
		p.i = start
		return p.unexpected("a number")
	}
	// An exponent only when digits follow it: "1e" is the number 1 and an
	// e that is no command.
	if !p.done() && (p.d[p.i] == 'e' || p.d[p.i] == 'E') {
		at := p.i + 1
		if at < len(p.d) && (p.d[at] == '+' || p.d[at] == '-') {
			at++
		}
		if at < len(p.d) && isDigit(p.d[at]) {
			p.i = at
			for !p.done() && isDigit(p.d[p.i]) {
				p.i++
			}
		}
	}
	value, err := strconv.ParseFloat(p.d[start:p.i], 64)
	if err != nil || math.IsInf(value, 0) || math.IsNaN(value) || math.Abs(value) > maxIconCoordinate {
		return fmt.Errorf("has the number %q at byte %d, which is not a number from -%d to %d",
			p.d[start:p.i], start, maxIconCoordinate, maxIconCoordinate)
	}
	return nil
}

// flag reads one arc flag: the single byte 0 or 1.
func (p *pathScanner) flag() error {
	if p.done() || (p.d[p.i] != '0' && p.d[p.i] != '1') {
		return p.unexpected("an arc flag (0 or 1)")
	}
	p.i++
	return nil
}

// CheckIconPathData holds an icon path's d to the SVG 2 path-data grammar:
// optional space around it; a moveto first; then commands, each followed by
// whole groups of its arguments (a moveto's later pairs are linetos);
// numbers that are finite; arc flags of one byte; a comma only between two
// arguments. Anything else — any other byte, a number after Z, a trailing or
// doubled comma — refuses it. Pure.
func CheckIconPathData(d string) error {
	p := &pathScanner{d: d}
	p.spaces()
	if p.done() {
		return errors.New("is empty: a path is SVG path data that begins with M or m")
	}
	first := true
	for {
		p.spaces()
		if p.done() {
			return nil
		}
		command := p.d[p.i]
		arity, isCommand := pathArity[command]
		switch {
		case !isCommand && first:
			return p.unexpected("a moveto (M or m)")
		case !isCommand:
			return p.unexpected("a path command (one of MmLlHhVvCcSsQqTtAaZz)")
		case first && command != 'M' && command != 'm':
			return fmt.Errorf("begins with %q: path data begins with a moveto (M or m)", command)
		}
		first = false
		p.i++
		if arity == 0 {
			continue
		}
		p.spaces()
		for {
			for k := range arity {
				if k > 0 {
					p.separator()
				}
				var err error
				if (command == 'A' || command == 'a') && (k == 3 || k == 4) {
					err = p.flag()
				} else {
					err = p.number()
				}
				if err != nil {
					return fmt.Errorf("%c command: %w", command, err)
				}
			}
			comma := p.separator()
			if !p.done() && startsNumber(p.d[p.i]) {
				continue
			}
			if comma {
				return fmt.Errorf("%c command: %w", command, p.unexpected("a number after the comma"))
			}
			break
		}
	}
}
