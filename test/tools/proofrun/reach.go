package main

import (
	"go/token"
	"sort"
)

// A declGraph is the reference graph of one package's top-level
// declarations in one build configuration: what each declaration mentions,
// and so what a test reaches through the package.
//
// A func, type, var or const is a node named by its identifier. A mention of
// that identifier, a selector's name included, is a reference to it: matching
// by name can only over-select, and a package-qualified or dot-imported use
// (an external test package naming an export_test.go helper) needs it.
//
// A method is a node named "T.m", its receiver type and its name. It is never
// reached by its name alone: an unrelated function, local variable, field or
// another type's method of that name says nothing about it. Without type
// checking, a reach arrives at a method T.m when
//
//   - a declaration it has passed selects the name m (x.m, T.m or (*T).m) and
//     a declaration it has passed mentions T (a composite literal, a
//     constructor's result type, a field); or
//   - m is exported and the reach mentions T, selector or not: another
//     package's code can call it through an interface the type satisfies
//     (Get, Put, String, Close, a fake's Describe), and that call is in no
//     declaration of this package.
//
// An unexported method is called by this package's declarations only, so its
// selector is always visible. So is any method a _test.go file adds to a type
// a non-test file declares (an export_test.go hook): it exists in the test
// build only, so production code does not call it, and a test can hold a
// value of that type that another package made, never naming the type. Such a
// hook is reached by its selector alone, as is a method of a receiver that is
// not a declared type of the package (an alias, a type in a file of another
// build configuration): the mention of the type cannot be relied on, and the
// name is all there is.
type declGraph struct {
	idents   map[string]map[string]bool // node -> every identifier it mentions
	selects  map[string]map[string]bool // node -> the names its selector expressions select
	byRecv   map[string][]methodNode    // receiver type -> its methods
	bySel    map[string][]methodNode    // method name -> the methods so named
	methods  map[string]bool            // method nodes known
	declared map[string]bool            // types declared in the package, aliases excluded
	prod     map[string]bool            // the declared types a non-test file declares

	frozen      bool
	sortedIdent map[string][]string
	sortedSel   map[string][]string
}

// methodNode is one method of a declGraph.
type methodNode struct {
	node string // "T.m"
	recv string // T
	name string // m
	test bool   // declared in a _test.go file
}

func newDeclGraph() *declGraph {
	return &declGraph{
		idents: map[string]map[string]bool{}, selects: map[string]map[string]bool{},
		byRecv: map[string][]methodNode{}, bySel: map[string][]methodNode{},
		methods: map[string]bool{}, declared: map[string]bool{}, prod: map[string]bool{},
	}
}

// add records one declaration.
func (g *declGraph) add(d decl) {
	g.frozen = false
	if g.idents[d.Node] == nil {
		g.idents[d.Node] = map[string]bool{}
		g.selects[d.Node] = map[string]bool{}
	}
	for r := range d.Refs {
		g.idents[d.Node][r] = true
	}
	for s := range d.Sels {
		g.selects[d.Node][s] = true
	}
	switch {
	case d.Recv != "":
		g.addMethod(methodNode{node: d.Node, recv: d.Recv, name: d.Name, test: d.InTest})
	case d.IsType && !d.Alias:
		g.declared[d.Name] = true
		if !d.InTest {
			g.prod[d.Name] = true
		}
	}
}

// addMethod makes m a method of its receiver, unless it is one already. A
// deleted method has no declaration to add, and still has to be reachable: the
// tests that selected it by name are the evidence for removing it.
func (g *declGraph) addMethod(m methodNode) {
	if g.methods[m.node] {
		return
	}
	g.frozen = false
	g.methods[m.node] = true
	g.byRecv[m.recv] = append(g.byRecv[m.recv], m)
	g.bySel[m.name] = append(g.bySel[m.name], m)
}

// hook reports whether m is a method a _test.go file adds to a type a non-test
// file declares.
func (g *declGraph) hook(m methodNode) bool {
	return m.test && g.prod[m.recv]
}

// reachedByType reports whether the mention of m's receiver type reaches m
// with no selector: an exported method can be called through an interface by
// another package. A hook cannot.
func (g *declGraph) reachedByType(m methodNode) bool {
	return token.IsExported(m.name) && !g.hook(m)
}

// reachedBySelector reports whether a selector of m's name reaches m, given
// the types the reach has mentioned: it needs the mention of the receiver, but
// for a receiver that is no declared type of the package and for a hook.
func (g *declGraph) reachedBySelector(m methodNode, mentioned map[string]bool) bool {
	return mentioned[m.recv] || !g.declared[m.recv] || g.hook(m)
}

func sortedSet(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func sortMethods(by map[string][]methodNode) {
	for _, ms := range by {
		sort.Slice(ms, func(i, j int) bool { return ms[i].node < ms[j].node })
	}
}

// freeze sorts everything a reach iterates, once, so that which declaration
// a reach names first never depends on map order.
func (g *declGraph) freeze() {
	if g.frozen {
		return
	}
	g.sortedIdent = make(map[string][]string, len(g.idents))
	g.sortedSel = make(map[string][]string, len(g.selects))
	for n, set := range g.idents {
		g.sortedIdent[n] = sortedSet(set)
	}
	for n, set := range g.selects {
		g.sortedSel[n] = sortedSet(set)
	}
	sortMethods(g.byRecv)
	sortMethods(g.bySel)
	g.frozen = true
}

// reaches reports which target `from` reaches through the graph, or "". A
// target is a node, or the plain name of one that no longer exists on this
// side (a deleted helper a test still mentions).
func (g *declGraph) reaches(from string, targets map[string]bool) string {
	g.freeze()
	seen := map[string]bool{from: true}
	mentioned := map[string]bool{} // identifiers a reached declaration mentions
	selected := map[string]bool{}  // names a reached declaration selects
	queue := []string{from}
	found := ""
	reach := func(n string) {
		if seen[n] {
			return
		}
		seen[n] = true
		queue = append(queue, n)
		if found == "" && targets[n] {
			found = n
		}
	}
	for len(queue) > 0 && found == "" {
		cur := queue[0]
		queue = queue[1:]
		for _, r := range g.sortedIdent[cur] {
			if targets[r] {
				return r
			}
			if !mentioned[r] {
				mentioned[r] = true
				for _, m := range g.byRecv[r] {
					if g.reachedByType(m) || selected[m.name] {
						reach(m.node)
					}
				}
			}
			if g.idents[r] != nil {
				reach(r)
			}
		}
		for _, s := range g.sortedSel[cur] {
			if selected[s] {
				continue
			}
			selected[s] = true
			for _, m := range g.bySel[s] {
				if g.reachedBySelector(m, mentioned) {
					reach(m.node)
				}
			}
		}
	}
	return found
}
