package logstream

import "sort"

// matcher is an Aho-Corasick automaton over a set of forms: one pass over a
// stream finds every occurrence of every form, at a cost per byte that does
// not depend on how many forms there are. The root's transitions are a dense
// row; below it the trie is mostly chains, so each node keeps its edges
// sorted and falls back along its failure link. It is immutable once built,
// so the writers of a masker share it.
type matcher struct {
	root  [256]int32
	nodes []matchNode
}

type matchNode struct {
	edges []matchEdge // sorted by b
	fail  int32
	depth int32
	// out is the length of the longest form that ends at this node or at a
	// node its failure chain reaches: every byte of the last out bytes lies
	// inside an occurrence.
	out int32
}

type matchEdge struct {
	b  byte
	to int32
}

// newMatcher builds the automaton over forms. A nil or empty set builds one
// that matches nothing and holds nothing.
func newMatcher(forms [][]byte) *matcher {
	m := &matcher{nodes: []matchNode{{}}}
	for _, f := range forms {
		if len(f) == 0 {
			continue
		}
		at := int32(0)
		for _, b := range f {
			next := m.child(at, b)
			if next < 0 {
				next = int32(len(m.nodes))
				m.nodes = append(m.nodes, matchNode{depth: m.nodes[at].depth + 1})
				m.addEdge(at, b, next)
			}
			at = next
		}
		m.nodes[at].out = max(m.nodes[at].out, int32(len(f)))
	}
	for i := range m.nodes {
		sort.Slice(m.nodes[i].edges, func(a, b int) bool { return m.nodes[i].edges[a].b < m.nodes[i].edges[b].b })
	}
	// Breadth first: a node's failure link is the deepest proper suffix of
	// its path that is also a path, found from its parent's link.
	queue := make([]int32, 0, len(m.nodes))
	for b := 0; b < 256; b++ {
		if to := m.child(0, byte(b)); to > 0 {
			m.root[b] = to
			m.nodes[to].fail = 0
			queue = append(queue, to)
		}
	}
	for len(queue) > 0 {
		at := queue[0]
		queue = queue[1:]
		for _, e := range m.nodes[at].edges {
			fail := m.nodes[at].fail
			for {
				if to := m.child(fail, e.b); to >= 0 && to != e.to {
					m.nodes[e.to].fail = to
					break
				}
				if fail == 0 {
					m.nodes[e.to].fail = 0
					break
				}
				fail = m.nodes[fail].fail
			}
			m.nodes[e.to].out = max(m.nodes[e.to].out, m.nodes[m.nodes[e.to].fail].out)
			queue = append(queue, e.to)
		}
	}
	return m
}

// child is the trie edge from at on b, or -1. Used while building, before
// the root row exists.
func (m *matcher) child(at int32, b byte) int32 {
	edges := m.nodes[at].edges
	i := sort.Search(len(edges), func(i int) bool { return edges[i].b >= b })
	if i < len(edges) && edges[i].b == b {
		return edges[i].to
	}
	for _, e := range edges { // edges are sorted only once the build ends
		if e.b == b {
			return e.to
		}
	}
	return -1
}

func (m *matcher) addEdge(at int32, b byte, to int32) {
	m.nodes[at].edges = append(m.nodes[at].edges, matchEdge{b: b, to: to})
}

// step is the state after reading b in state at.
func (m *matcher) step(at int32, b byte) int32 {
	for at != 0 {
		edges := m.nodes[at].edges
		lo, hi := 0, len(edges)
		for lo < hi {
			mid := int(uint(lo+hi) >> 1)
			if edges[mid].b < b {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if lo < len(edges) && edges[lo].b == b {
			return edges[lo].to
		}
		at = m.nodes[at].fail
	}
	return m.root[b]
}

// empty reports whether the automaton holds no form.
func (m *matcher) empty() bool { return m == nil || len(m.nodes) <= 1 }
