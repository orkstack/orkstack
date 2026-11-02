package main

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// evalGHExpr evaluates the subset of GitHub's expression language a job's
// `if:` is written in, so that a test can ask what a guard admits instead of
// reading its text: dotted context paths (looked up in ctx, an absent one is
// the empty string, as GitHub's null), string and number literals, true and
// false, `==`, `!=`, `<`, `&&`, `||`, `!`, parentheses and startsWith.
// Strings compare without regard to case, as GitHub's do; two numbers compare
// as numbers. Anything else is an error, so a guard that grows a construct
// the evaluator does not know fails its test rather than passing it.
func evalGHExpr(expr string, ctx map[string]string) (bool, error) {
	p := &ghParser{src: expr, ctx: ctx}
	v, err := p.or()
	if err != nil {
		return false, err
	}
	p.skip()
	if p.pos != len(p.src) {
		return false, fmt.Errorf("unexpected %q at %d", p.src[p.pos:], p.pos)
	}
	return v.truthy(), nil
}

type ghValue struct {
	s      string
	b      bool
	isBool bool
}

func (v ghValue) truthy() bool {
	if v.isBool {
		return v.b
	}
	return v.s != "" && v.s != "0"
}

type ghParser struct {
	src string
	pos int
	ctx map[string]string
}

func (p *ghParser) skip() {
	for p.pos < len(p.src) && strings.ContainsRune(" \t\r\n", rune(p.src[p.pos])) {
		p.pos++
	}
}

func (p *ghParser) eat(tok string) bool {
	p.skip()
	if strings.HasPrefix(p.src[p.pos:], tok) {
		p.pos += len(tok)
		return true
	}
	return false
}

func (p *ghParser) or() (ghValue, error) {
	v, err := p.and()
	for err == nil && p.eat("||") {
		var r ghValue
		if r, err = p.and(); err == nil {
			v = ghValue{b: v.truthy() || r.truthy(), isBool: true}
		}
	}
	return v, err
}

func (p *ghParser) and() (ghValue, error) {
	v, err := p.cmp()
	for err == nil && p.eat("&&") {
		var r ghValue
		if r, err = p.cmp(); err == nil {
			v = ghValue{b: v.truthy() && r.truthy(), isBool: true}
		}
	}
	return v, err
}

func (p *ghParser) cmp() (ghValue, error) {
	l, err := p.unary()
	if err != nil {
		return l, err
	}
	for _, op := range []string{"==", "!=", "<"} {
		if !p.eat(op) {
			continue
		}
		r, err := p.unary()
		if err != nil {
			return r, err
		}
		ln, lerr := strconv.ParseFloat(l.s, 64)
		rn, rerr := strconv.ParseFloat(r.s, 64)
		numeric := !l.isBool && !r.isBool && lerr == nil && rerr == nil
		switch op {
		case "<":
			if !numeric {
				return ghValue{}, fmt.Errorf("< compares %q and %q, which are not both numbers", l.s, r.s)
			}
			return ghValue{b: ln < rn, isBool: true}, nil
		default:
			eq := strings.EqualFold(l.s, r.s)
			if numeric {
				eq = ln == rn
			}
			if l.isBool || r.isBool {
				eq = l.truthy() == r.truthy()
			}
			return ghValue{b: eq == (op == "=="), isBool: true}, nil
		}
	}
	return l, nil
}

func (p *ghParser) unary() (ghValue, error) {
	if p.skip(); p.pos < len(p.src) && p.src[p.pos] == '!' && !strings.HasPrefix(p.src[p.pos:], "!=") {
		p.pos++
		v, err := p.unary()
		return ghValue{b: !v.truthy(), isBool: true}, err
	}
	return p.primary()
}

func (p *ghParser) primary() (ghValue, error) {
	p.skip()
	if p.pos >= len(p.src) {
		return ghValue{}, fmt.Errorf("the expression ends early")
	}
	switch c := p.src[p.pos]; {
	case c == '(':
		p.pos++
		v, err := p.or()
		if err != nil {
			return v, err
		}
		if !p.eat(")") {
			return v, fmt.Errorf("missing ) at %d", p.pos)
		}
		return v, nil
	case c == '\'':
		var b strings.Builder
		for i := p.pos + 1; i < len(p.src); i++ {
			if p.src[i] != '\'' {
				b.WriteByte(p.src[i])
				continue
			}
			if i+1 < len(p.src) && p.src[i+1] == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			p.pos = i + 1
			return ghValue{s: b.String()}, nil
		}
		return ghValue{}, fmt.Errorf("an unterminated string")
	case c >= '0' && c <= '9':
		start := p.pos
		for p.pos < len(p.src) && (p.src[p.pos] >= '0' && p.src[p.pos] <= '9' || p.src[p.pos] == '.') {
			p.pos++
		}
		return ghValue{s: p.src[start:p.pos]}, nil
	case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
		start := p.pos
		for p.pos < len(p.src) && (p.src[p.pos] == '_' || p.src[p.pos] == '.' || p.src[p.pos] == '-' ||
			p.src[p.pos] >= 'a' && p.src[p.pos] <= 'z' || p.src[p.pos] >= 'A' && p.src[p.pos] <= 'Z' || p.src[p.pos] >= '0' && p.src[p.pos] <= '9') {
			p.pos++
		}
		name := p.src[start:p.pos]
		switch {
		case name == "true" || name == "false":
			return ghValue{b: name == "true", isBool: true}, nil
		case p.eat("("):
			if !strings.EqualFold(name, "startsWith") {
				return ghValue{}, fmt.Errorf("the function %s is not one the evaluator knows", name)
			}
			a, err := p.or()
			if err != nil || !p.eat(",") {
				return a, fmt.Errorf("startsWith takes two arguments (%v)", err)
			}
			b, err := p.or()
			if err != nil || !p.eat(")") {
				return b, fmt.Errorf("startsWith is not closed (%v)", err)
			}
			return ghValue{b: strings.HasPrefix(strings.ToLower(a.s), strings.ToLower(b.s)), isBool: true}, nil
		}
		return ghValue{s: p.ctx[name]}, nil
	}
	return ghValue{}, fmt.Errorf("unexpected %q at %d", p.src[p.pos:], p.pos)
}

func TestEvalGHExpr_EvaluatesTheGuardsConstructs(t *testing.T) {
	ctx := map[string]string{"a.b": "Foo", "n": "2", "branch": "validate/full/x", "empty": ""}
	for _, c := range []struct {
		expr string
		want bool
		err  bool
	}{
		{"a.b == 'foo'", true, false}, // case-insensitive
		{"a.b != 'foo'", false, false},
		{"a.b == 'bar' || n < 3", true, false},
		{"a.b == 'bar' && n < 3", false, false},
		{"true && (false || a.b == 'foo')", true, false},
		{"n < 2", false, false},
		{"n == 2", true, false},
		{"startsWith(branch, 'validate/full/')", true, false},
		{"startsWith(branch, 'validate/other')", false, false},
		{"missing == ''", true, false}, // an absent path is null
		{"!(a.b == 'foo')", false, false},
		{"a.b == 'x''y'", false, false},
		{"empty", false, false},
		{"contains(a.b, 'F')", false, true}, // a function the evaluator does not know
		{"a.b == ", false, true},
		{"(a.b == 'foo'", false, true},
		{"a.b == 'foo' extra", false, true},
		{"'unterminated", false, true},
	} {
		got, err := evalGHExpr(c.expr, ctx)
		if (err != nil) != c.err || got != c.want {
			t.Errorf("%q = %v, %v; want %v, error %v", c.expr, got, err, c.want, c.err)
		}
	}
}
